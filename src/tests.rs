use crate::api;
use crate::crawler::*;
use crate::discovery::{DiscoveryBudget, DiscoverySource, Found, Lead, Query, federate};
use crate::scoring::{LinkContext, ON_TOPIC, Shape, Terms, score_link};
use crate::sources::{brave::Brave, gdelt::Gdelt, wayback::Wayback, wikidata::Wikidata};
use crate::net::{self, NetPolicy};
use crate::structured;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use reqwest::dns::Resolve;
use tokio::net::TcpListener;
use url::Url;

// fixture server
//
// The suite used to crawl live sites, so it was non-hermetic: it failed offline
// and whenever a third party changed their markup. Everything below runs against
// a local server with known content and a request counter.


struct Fixture {
    base: String,
    addr: SocketAddr,
    hits: Arc<Mutex<HashMap<String, usize>>>,
    /// Raw request text, so a test can assert on what was actually sent.
    requests: Arc<Mutex<Vec<String>>>,
}

impl Fixture {
    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base, path)
    }

    fn hits(&self, path: &str) -> usize {
        self.hits.lock().unwrap().get(path).copied().unwrap_or(0)
    }

    /// The same server under a second name, so a crawl can span two hosts.
    /// `host_of` ignores the port, so a second port would still be one host.
    fn aliased(&self, host: &str) -> String {
        format!("http://{host}:{}", self.addr.port())
    }

    fn sent(&self) -> String {
        self.requests.lock().unwrap().join("\n")
    }
}

fn body_for(path: &str) -> (u16, &'static str, Vec<u8>) {
    match path {
        "/robots.txt" => (
            200,
            "text/plain",
            "User-agent: *\nDisallow: /secret\nSitemap: /sitemap.xml\n".into(),
        ),
        "/sitemap.xml" => (
            200,
            "application/xml",
            r#"<?xml version="1.0" encoding="UTF-8"?>
               <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
                 <sitemap><loc>/sm-plain.xml</loc><lastmod>2026-08-20</lastmod></sitemap>
                 <sitemap><loc>/sm-gzipped.xml.gz</loc></sitemap>
               </sitemapindex>"#
                .into(),
        ),
        "/sm-plain.xml" => (
            200,
            "application/xml",
            r#"<?xml version="1.0" encoding="UTF-8"?>
               <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
                 <url><loc>/alpha</loc><lastmod>2026-08-21</lastmod></url>
                 <url><loc>/nav-noise</loc></url>
                 <url><loc>/d1?utm_source=sitemap</loc></url>
               </urlset>"#
                .into(),
        ),
        "/sm-gzipped.xml.gz" => {
            use std::io::Write;
            let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
               <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
                 <url><loc>/d2</loc></url>
                 <url><loc>/d3</loc></url>
               </urlset>"#;
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(xml.as_bytes()).unwrap();
            (200, "application/gzip", encoder.finish().unwrap())
        }
        "/" => (
            200,
            "text/html",
            r#"<html><head><title>Home</title>
               <meta name="description" content="the root page">
               <link rel="canonical" href="https://example.com/">
               </head>
               <body>
                 <nav><a href="/nav-noise">skip me</a> navigation clutter</nav>
                 <main>
                   <p>Welcome to the index.</p>
                   <a href="/alpha">alpha</a>
                   <a href="/alpha#section">alpha again</a>
                   <a href="/alpha?utm_source=x">alpha tracked</a>
                   <a href="/missing">missing</a>
                   <a href="/report.pdf">a pdf</a>
                   <a href="/secret">secret</a>
                   <a href="/d1">depth one</a>
                   <a href="https://google-analytics.com/collect">tracker</a>
                 </main>
                 <footer>footer clutter</footer>
               </body></html>"#
                .into(),
        ),
        "/alpha" => (
            200,
            "text/html",
            r#"<html><body><nav>nav clutter</nav><main>
               <p>The Needle appears here in the alpha page.</p>
               <script>var junk = "needle in script";</script>
               </main></body></html>"#
                .into(),
        ),
        "/nav-noise" => (200, "text/html", "<html><body><main>noise</main></body></html>".into()),
        // A chain, so the depth guard rail has something to actually stop.
        "/d1" => (200, "text/html", r#"<html><body><main>one <a href="/d2">two</a></main></body></html>"#.into()),
        "/d2" => (200, "text/html", r#"<html><body><main>two <a href="/d3">three</a></main></body></html>"#.into()),
        "/d3" => (200, "text/html", "<html><body><main>three</main></body></html>".into()),
        "/secret" => (200, "text/html", "<html><body><main>classified</main></body></html>".into()),
        // A hub page of the kind a focused crawl has to triage: the useful
        // link is neither first nor obviously labelled.
        "/hub" => (
            200,
            "text/html",
            r#"<html><body><main>
                 <a href="/cookie-policy">Cookie policy</a>
                 <a href="/contact">Contact us</a>
                 <a href="/press/2019/">Press releases 2019</a>
               </main></body></html>"#
                .into(),
        ),
        "/cookie-policy" => (
            200,
            "text/html",
            "<html><body><main>We use cookies on this website.</main></body></html>".into(),
        ),
        "/contact" => (200, "text/html", "<html><body><main>Contact us.</main></body></html>".into()),
        "/press/2019/" => (
            200,
            "text/html",
            r#"<html><body><main><p>Japarov visited Ankara in 2019.</p>
                 <a href="/press/2019/page/2" rel="next">Next</a></main></body></html>"#
                .into(),
        ),
        "/press/2019/page/2" => (
            200,
            "text/html",
            "<html><body><main>Japarov met the Turkish president.</main></body></html>".into(),
        ),
        // A dull index that names nothing, holding a link that names nothing,
        // leading to the page that does. Without tunnelling this is a dead end.
        "/dull" => (
            200,
            "text/html",
            r#"<html><body><main>Site index. <a href="/q/target">Read more</a></main></body></html>"#
                .into(),
        ),
        // Non-Latin text long enough to force truncation, and a Turkish capital
        // İ, whose lowercase form is two characters. Both used to panic.
        "/cyrillic" => (
            200,
            "text/html",
            format!(
                r#"<html><body><main><p>İstanbul ziyareti.</p>
                   <a href="/q/target">{}</a></main></body></html>"#,
                "Садыр Жапаров встретился с делегацией ".repeat(20)
            )
            .into_bytes(),
        ),
        "/q/target" => (
            200,
            "text/html",
            "<html><body><main>Садыр Жапаров met the delegation.</main></body></html>".into(),
        ),
        "/report.pdf" => (200, "application/pdf", "%PDF-1.4 not html".into()),
        // Served as text/plain on purpose: GDELT does exactly this, and the
        // HTML content-type gate must not be in the way of a JSON fetch.
        "/api.json" => (200, "text/plain", r#"{"ok":true,"items":[1,2,3]}"#.into()),
        "/api-bad.json" => (200, "application/json", "not json at all".into()),
        // GDELT-shaped responses. The last two are the cases its documentation
        // does not mention: no matches, and the rate-limit notice.
        "/gdelt" => (
            200,
            "application/json",
            r#"{"articles": [
                 {"url": "https://example.org/first", "title": "First report",
                  "seendate": "20260827T081500Z", "domain": "example.org", "language": "English"},
                 {"url": "https://example.org/second", "title": "Second report",
                  "seendate": "20260826T101500Z", "domain": "example.org", "language": "Spanish"},
                 {"url": "https://example.org/third?utm_source=gdelt", "title": "Third report",
                  "seendate": "20260825T090000Z", "domain": "example.org", "language": "Serbian"}
               ]}"#
                .into(),
        ),
        "/gdelt-empty" => (200, "application/json", "{}".into()),
        // Wikidata answers two different calls on one endpoint, told apart by
        // the `action` parameter; the fixture routes on path, so they are split
        // into two paths here and the source is pointed at each in turn.
        "/wd-search" => (
            200,
            "application/json",
            r#"{"search": [{"id": "Q25597826", "label": "Sadyr Japarov",
                            "description": "President of Kyrgyzstan"}]}"#
                .into(),
        ),
        "/wd-search-empty" => (200, "application/json", r#"{"search": []}"#.into()),
        "/wd-entity" => (
            200,
            "application/json",
            r#"{"entities": {"Q25597826": {
                 "labels": {"en": {"language": "en", "value": "Sadyr Japarov"}},
                 "aliases": {"en": [{"language": "en", "value": "Sadyr Zhaparov"}]},
                 "claims": {"P856": [{"mainsnak": {"datavalue": {"value": "https://president.kg/"}}}]},
                 "sitelinks": {
                   "enwiki": {"site": "enwiki", "title": "Sadyr Japarov",
                              "url": "https://en.wikipedia.org/wiki/Sadyr_Japarov"},
                   "kywiki": {"site": "kywiki", "title": "Жапаров Садыр",
                              "url": "https://ky.wikipedia.org/wiki/Sadyr"},
                   "commonswiki": {"site": "commonswiki", "title": "Category:Sadyr",
                                   "url": "https://commons.wikimedia.org/wiki/Category:Sadyr"}
                 }}}}"#
                .into(),
        ),
        // CDX: array of arrays whose first row names the columns.
        "/cdx" => (
            200,
            "application/json",
            r#"[["urlkey","timestamp","original","mimetype","statuscode","digest","length"],
                ["kg,gov,mfa)/news/1","20190104061944","http://mfa.gov.kg/news/1","text/html","200","AAA","100"],
                ["kg,gov,mfa)/news/2","20200215120000","http://mfa.gov.kg/news/2","text/html","200","BBB","200"]]"#
                .into(),
        ),
        "/cdx-empty" => (200, "application/json", "[]".into()),
        "/brave" => (
            200,
            "application/json",
            r#"{"web": {"results": [
                 {"url": "https://example.org/one", "title": "One", "age": "2 days ago",
                  "language": "en"},
                 {"url": "https://example.org/two", "title": "Two"}
               ]}}"#
                .into(),
        ),
        "/gdelt-limited" => (
            200,
            "text/plain",
            "Please limit requests to one every 5 seconds or contact us for larger queries."
                .into(),
        ),
        _ => (404, "text/html", "<html><body>not found</body></html>".into()),
    }
}

async fn fixture() -> Fixture {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hits: Arc<Mutex<HashMap<String, usize>>> = Arc::new(Mutex::new(HashMap::new()));
    let counter = Arc::clone(&hits);
    let requests: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let recorder = Arc::clone(&requests);

    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let counter = Arc::clone(&counter);
            let recorder = Arc::clone(&recorder);
            tokio::spawn(async move {
                let mut buf = vec![0u8; 2048];
                let Ok(n) = socket.read(&mut buf).await else { return };
                let request = String::from_utf8_lossy(&buf[..n]);
                let target = request
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap_or("/")
                    .to_string();
                // Routed and counted by path alone; the query string is still
                // in the recorded request for tests that assert on it.
                let (path, query) = match target.split_once('?') {
                    Some((path, query)) => (path.to_string(), query.to_string()),
                    None => (target.clone(), String::new()),
                };

                recorder.lock().unwrap().push(request.to_string());
                let hit = {
                    let mut hits = counter.lock().unwrap();
                    let count = hits.entry(path.clone()).or_insert(0);
                    *count += 1;
                    *count
                };

                let (status, content_type, body) = match path.as_str() {
                    // Fails once, then succeeds — a transient failure needs
                    // state, which a pure `body_for` cannot have.
                    "/flaky.json" if hit == 1 => (503, "application/json", b"{}".to_vec()),
                    "/flaky.json" => (200, "application/json", br#"{"recovered":true}"#.to_vec()),
                    // Wikidata serves both of its calls from one endpoint and
                    // tells them apart by `action`, so the fixture does too.
                    "/wd" if query.contains("wbsearchentities") => body_for("/wd-search"),
                    "/wd" if query.contains("wbgetentities") => body_for("/wd-entity"),
                    _ => body_for(&path),
                };
                let header = format!(
                    "HTTP/1.1 {status} OK\r\nContent-Type: {content_type}\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = socket.write_all(header.as_bytes()).await;
                let _ = socket.write_all(&body).await;
                let _ = socket.shutdown().await;
            });
        }
    });

    Fixture {
        base: format!("http://{addr}"),
        addr,
        hits,
        requests,
    }
}

fn test_config() -> CrawlConfig {
    CrawlConfig {
        per_host_delay: Duration::ZERO,
        max_concurrency: 4,
        ..CrawlConfig::default()
    }
}

/// Pins `name` to the fixture's address, so a test can use a hostname the
/// resolver never sees. Mirrors `build_client`, minus the guards this test
/// deliberately does not exercise.
fn client_resolving(name: &str, addr: SocketAddr) -> reqwest::Client {
    net::init(NetPolicy::permissive());
    reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .timeout(Duration::from_secs(5))
        .resolve(name, addr)
        .build()
        .unwrap()
}

fn test_client() -> reqwest::Client {
    // The fixture server is on loopback, which the default policy refuses. The
    // guard itself is exercised directly, below.
    net::init(NetPolicy::permissive());

    build_client(Duration::from_secs(5)).unwrap()
}


// *** URL handling ***
#[test]
fn canonicalize_rejects_junk_instead_of_panicking() {
    // These inputs previously reached `Url::parse(..).unwrap()` and aborted the
    // whole process, taking the MCP session down with it.
    for bad in ["", "   ", "not a url", "exa mple.com", "ftp://files.example.com"] {
        assert!(canonicalize(bad).is_err(), "expected an error for {bad:?}");
    }
}

#[test]
fn canonicalize_adds_scheme_and_normalizes() {
    assert_eq!(canonicalize("example.com").unwrap().as_str(), "https://example.com/");
    assert_eq!(
        canonicalize("https://example.com/p#anchor").unwrap().as_str(),
        "https://example.com/p"
    );
    assert_eq!(
        canonicalize("https://example.com/p?utm_source=news&id=7").unwrap().as_str(),
        "https://example.com/p?id=7"
    );
    // Only tracking params present, so the query disappears entirely.
    assert_eq!(
        canonicalize("https://example.com/p?fbclid=abc").unwrap().as_str(),
        "https://example.com/p"
    );
}

#[test]
fn equivalent_urls_share_a_dedup_key() {
    let variants = [
        "https://example.com/page",
        "https://example.com/page/",
        "https://www.example.com/page",
        "https://example.com/page#top",
        "https://example.com/page?utm_campaign=spring",
    ];
    let keys: Vec<String> = variants
        .iter()
        .map(|v| dedup_key(&canonicalize(v).unwrap()))
        .collect();
    assert!(
        keys.windows(2).all(|w| w[0] == w[1]),
        "expected one key, got {keys:?}"
    );
}

#[test]
fn distinct_urls_keep_distinct_keys() {
    let a = dedup_key(&canonicalize("https://example.com/a").unwrap());
    let b = dedup_key(&canonicalize("https://example.com/b").unwrap());
    assert_ne!(a, b);
}


// *** extraction ***

#[test]
fn extract_text_drops_chrome_and_scripts() {
    let html = r#"<html><body>
        <nav>navigation clutter</nav>
        <main><p>Real content here.</p><script>var x = "script junk";</script></main>
        <footer>footer clutter</footer>
    </body></html>"#;
    let text = extract_text(html);
    assert!(text.contains("Real content here."));
    assert!(!text.contains("navigation clutter"));
    assert!(!text.contains("footer clutter"));
    assert!(!text.contains("script junk"));
}

#[test]
fn extract_links_dedups_and_resolves() {
    let base = canonicalize("https://example.com/dir/page").unwrap();
    let html = r#"<a href="/a">1</a><a href="a">2</a><a href="/a#x">3</a>
                  <a href="/a?utm_source=q">4</a><a href="mailto:x@y.z">5</a>"#;
    let links = extract_links(html, &base);
    let strings: Vec<String> = links.iter().map(|u| u.to_string()).collect();

    assert!(strings.contains(&"https://example.com/a".to_string()));
    assert!(strings.contains(&"https://example.com/dir/a".to_string()));
    // mailto: is not fetchable and must not enter the frontier.
    assert!(!strings.iter().any(|s| s.starts_with("mailto")));
    // /a, /a#x and /a?utm_source=q all collapse to one entry.
    assert_eq!(links.len(), 2, "got {strings:?}");
}

#[test]
fn extract_metadata_reports_present_fields_only() {
    let html = r#"<html><head><title>A Title</title>
        <meta name="description" content="A description">
        <link rel="canonical" href="https://example.com/canon"></head><body></body></html>"#;
    let meta = extract_metadata(html);
    assert!(meta.contains("Title: A Title"));
    assert!(meta.contains("Description: A description"));
    assert!(meta.contains("Canonical: https://example.com/canon"));
    // Absent fields are omitted rather than printed empty.
    assert!(!meta.contains("Author:"));
}


// *** fetching ***
#[tokio::test]
async fn fetch_rejects_error_status() {
    let server = fixture().await;
    let url = canonicalize(&server.url("/missing")).unwrap();
    let result = fetch_page(&test_client(), &url, 5_000_000).await;
    assert!(matches!(result, Err(CrawlError::Status(404))), "got {result:?}");
}

#[tokio::test]
async fn fetch_rejects_non_html_content_type() {
    let server = fixture().await;
    let url = canonicalize(&server.url("/report.pdf")).unwrap();
    let result = fetch_page(&test_client(), &url, 5_000_000).await;
    assert!(matches!(result, Err(CrawlError::ContentType(_))), "got {result:?}");
}

#[tokio::test]
async fn fetch_rejects_oversized_body() {
    let server = fixture().await;
    let url = canonicalize(&server.url("/")).unwrap();
    let result = fetch_page(&test_client(), &url, 16).await;
    assert!(matches!(result, Err(CrawlError::TooLarge(16))), "got {result:?}");
}

// *** crawl ***
#[tokio::test]
async fn crawl_reports_only_pages_it_actually_fetched() {
    let server = fixture().await;
    let outcome = crawl(&test_client(), &server.url("/"), &test_config()).await.unwrap();
    let urls = outcome.urls();

    assert!(urls.iter().any(|u| u.ends_with("/alpha")));
    // /missing 404s and /report.pdf is not HTML: both belong in `failed`, and
    // neither may appear in the visited list.
    assert!(!urls.iter().any(|u| u.contains("/missing")));
    assert!(!urls.iter().any(|u| u.contains("report.pdf")));
    assert!(outcome.failed.iter().any(|(u, _)| u.contains("/missing")));
    assert!(outcome.failed.iter().any(|(u, _)| u.contains("report.pdf")));
}

#[tokio::test]
async fn crawl_honors_robots_txt() {
    let server = fixture().await;
    let outcome = crawl(&test_client(), &server.url("/"), &test_config()).await.unwrap();

    assert!(!outcome.urls().iter().any(|u| u.contains("/secret")));
    assert!(outcome.robots_skipped >= 1);
    assert_eq!(server.hits("/secret"), 0, "disallowed path was requested anyway");
}

#[tokio::test]
async fn crawl_can_ignore_robots_when_asked() {
    let server = fixture().await;
    let cfg = CrawlConfig { respect_robots: false, ..test_config() };
    let outcome = crawl(&test_client(), &server.url("/"), &cfg).await.unwrap();
    assert!(outcome.urls().iter().any(|u| u.contains("/secret")));
}

#[tokio::test]
async fn crawl_never_fetches_the_same_page_twice() {
    let server = fixture().await;
    let _ = crawl(&test_client(), &server.url("/"), &test_config()).await.unwrap();
    // The index links to /alpha three ways: bare, with a fragment, and with a
    // tracking parameter. Canonicalization collapses them to one request.
    assert_eq!(server.hits("/alpha"), 1, "hits: {:?}", server.hits.lock().unwrap());
}

#[tokio::test]
async fn crawl_skips_blocked_domains() {
    let server = fixture().await;
    let outcome = crawl(&test_client(), &server.url("/"), &test_config()).await.unwrap();
    assert!(!outcome.urls().iter().any(|u| u.contains("google-analytics")));
    assert!(!outcome.failed.iter().any(|(u, _)| u.contains("google-analytics")));
}

#[tokio::test]
async fn crawl_stops_at_the_page_budget() {
    let server = fixture().await;
    let cfg = CrawlConfig { max_pages: 2, ..test_config() };
    let outcome = crawl(&test_client(), &server.url("/"), &cfg).await.unwrap();
    assert!(outcome.pages.len() + outcome.failed.len() <= 2, "budget overrun");
    assert!(outcome.budget_hit);
}

#[tokio::test]
async fn crawl_respects_depth_guard() {
    let server = fixture().await;

    // max_depth counts link hops from the seed: 1 reaches /d1 but not /d2.
    let cfg = CrawlConfig { max_depth: 1, ..test_config() };
    let shallow = crawl(&test_client(), &server.url("/"), &cfg).await.unwrap();
    assert!(shallow.urls().iter().any(|u| u.ends_with("/d1")));
    assert!(!shallow.urls().iter().any(|u| u.ends_with("/d2")));

    let cfg = CrawlConfig { max_depth: 2, ..test_config() };
    let deeper = crawl(&test_client(), &server.url("/"), &cfg).await.unwrap();
    assert!(deeper.urls().iter().any(|u| u.ends_with("/d2")));
    assert!(!deeper.urls().iter().any(|u| u.ends_with("/d3")));
}

#[tokio::test]
async fn crawl_same_domain_stays_on_the_seed_host() {
    let server = fixture().await;
    let outcome = crawl_same_domain(&test_client(), &server.url("/"), &test_config())
        .await
        .unwrap();
    assert!(!outcome.urls().is_empty());
    assert!(
        outcome.urls().iter().all(|u| u.contains("127.0.0.1")),
        "left the seed host: {:?}",
        outcome.urls()
    );
}

#[tokio::test]
async fn crawl_rejects_a_malformed_seed() {
    let result = crawl(&test_client(), "not a url", &test_config()).await;
    assert!(matches!(result, Err(CrawlError::BadUrl(_))));
}

// *** search ***
#[tokio::test]
async fn search_matches_case_insensitively_without_refetching() {
    let server = fixture().await;
    let (matches, outcome) =
        search_site(&test_client(), &server.url("/"), "needle", &test_config())
            .await
            .unwrap();

    assert!(matches.iter().any(|m| m.url.as_str().ends_with("/alpha")));
    assert!(!matches[0].snippet.is_empty());

    // Every page is fetched exactly once. The old implementation crawled first,
    // threw the HTML away, then downloaded every page a second time to search it.
    for page in &outcome.pages {
        assert_eq!(
            server.hits(page.url.path()),
            1,
            "{} was fetched more than once",
            page.url
        );
    }
}

#[tokio::test]
async fn search_ignores_text_inside_scripts() {
    let server = fixture().await;
    let (matches, _) = search_site(&test_client(), &server.url("/alpha"), "needle", &test_config())
        .await
        .unwrap();
    assert_eq!(matches[0].hits, 1);
}

// ---------------------------------------------------------------------------
// sitemaps
//
// The entry point that makes client-rendered sites reachable at all: their HTML
// carries no links, but their sitemap lists every page.
// ---------------------------------------------------------------------------

use crate::sitemap::{self, SitemapConfig};

fn sitemap_config() -> SitemapConfig {
    SitemapConfig {
        per_host_delay: Duration::ZERO,
        ..SitemapConfig::default()
    }
}

#[tokio::test]
async fn discovers_the_sitemap_named_in_robots_txt() {
    let server = fixture().await;
    let found = sitemap::discover(&test_client(), &server.url("/")).await.unwrap();
    assert_eq!(found.len(), 1);
    assert!(found[0].as_str().ends_with("/sitemap.xml"));
}

#[tokio::test]
async fn walks_an_index_into_its_child_sitemaps() {
    let server = fixture().await;
    let outcome = sitemap::collect(&test_client(), &server.url("/"), &sitemap_config())
        .await
        .unwrap();

    // One index plus both children.
    assert_eq!(outcome.sitemaps_read, 3);
    let urls = outcome.urls();
    assert!(urls.iter().any(|u| u.ends_with("/alpha")));
    assert!(urls.iter().any(|u| u.ends_with("/nav-noise")));
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
}

#[tokio::test]
async fn reads_gzipped_child_sitemaps() {
    let server = fixture().await;
    let outcome = sitemap::collect(&test_client(), &server.url("/"), &sitemap_config())
        .await
        .unwrap();
    let urls = outcome.urls();
    // /d2 and /d3 exist only inside the gzipped child.
    assert!(urls.iter().any(|u| u.ends_with("/d2")), "gunzip failed: {urls:?}");
    assert!(urls.iter().any(|u| u.ends_with("/d3")));
}

#[tokio::test]
async fn sitemap_urls_are_canonicalized() {
    let server = fixture().await;
    let outcome = sitemap::collect(&test_client(), &server.url("/"), &sitemap_config())
        .await
        .unwrap();
    // The sitemap lists /d1?utm_source=sitemap; the tracking param is stripped.
    assert!(outcome.urls().iter().any(|u| u.ends_with("/d1")));
    assert!(!outcome.urls().iter().any(|u| u.contains("utm_source")));
}

#[tokio::test]
async fn sitemap_captures_lastmod() {
    let server = fixture().await;
    let outcome = sitemap::collect(&test_client(), &server.url("/"), &sitemap_config())
        .await
        .unwrap();
    let alpha = outcome
        .entries
        .iter()
        .find(|e| e.url.path() == "/alpha")
        .expect("/alpha missing");
    assert_eq!(alpha.lastmod.as_deref(), Some("2026-08-21"));
}

#[tokio::test]
async fn sitemap_filter_applies_while_walking() {
    let server = fixture().await;
    let cfg = SitemapConfig { contains: Some("d".into()), ..sitemap_config() };
    let outcome = sitemap::collect(&test_client(), &server.url("/"), &cfg).await.unwrap();

    let urls = outcome.urls();
    assert!(urls.iter().all(|u| u.contains("/d")), "unfiltered: {urls:?}");
    assert!(!urls.iter().any(|u| u.ends_with("/alpha")));
}

#[tokio::test]
async fn sitemap_respects_the_url_budget() {
    let server = fixture().await;
    let cfg = SitemapConfig { max_urls: 2, ..sitemap_config() };
    let outcome = sitemap::collect(&test_client(), &server.url("/"), &cfg).await.unwrap();
    assert_eq!(outcome.entries.len(), 2);
    assert!(outcome.truncated);
}

#[tokio::test]
async fn sitemap_accepts_a_direct_sitemap_url() {
    let server = fixture().await;
    let outcome = sitemap::collect(&test_client(), &server.url("/sm-plain.xml"), &sitemap_config())
        .await
        .unwrap();
    assert_eq!(outcome.sitemaps_read, 1);
    assert_eq!(outcome.entries.len(), 3);
}

#[tokio::test]
async fn crawl_can_seed_itself_from_the_sitemap() {
    let server = fixture().await;

    // Depth 0 means no link following at all, so every page fetched beyond the
    // seed must have come from the sitemap.
    let cfg = CrawlConfig { max_depth: 0, seed_from_sitemap: true, ..test_config() };
    let outcome = crawl(&test_client(), &server.url("/"), &cfg).await.unwrap();

    let urls = outcome.urls();
    assert!(urls.iter().any(|u| u.ends_with("/alpha")), "got {urls:?}");
    assert!(urls.iter().any(|u| u.ends_with("/d2")));

    // Without seeding, the same crawl reaches only the seed.
    let cfg = CrawlConfig { max_depth: 0, ..test_config() };
    let plain = crawl(&test_client(), &server.url("/"), &cfg).await.unwrap();
    assert_eq!(plain.pages.len(), 1);
}


// *** structured data ***

#[test]
fn json_ld_flattens_arrays_and_graphs() {
    let html = r#"<html><head>
        <script type="application/ld+json">
          {"@context":"https://schema.org","@graph":[
            {"@type":"NewsArticle","headline":"A visit","datePublished":"2026-03-01"},
            {"@type":"Person","name":"A Name"}]}
        </script>
        <script type="application/ld+json">
          [{"@type":"Organization","name":"A Ministry"}]
        </script></head><body></body></html>"#;

    let data = structured::extract(html);
    // Three records, not two blocks: the @graph and the array are containers.
    assert_eq!(data.json_ld.len(), 3);
    assert_eq!(data.schema_types(), ["NewsArticle", "Person", "Organization"]);

    let rendered = structured::render_records(&data.json_ld);
    assert!(rendered.contains("headline: A visit"), "got {rendered}");
    assert!(rendered.contains("datePublished: 2026-03-01"));
}

#[test]
fn json_ld_survives_wrappers_and_broken_blocks() {
    let html = r#"<script type="application/ld+json">//<![CDATA[
                    {"@type":"Event","name":"Wrapped"}
                  //]]></script>
                  <script type="application/ld+json">{"@type":"Event", oops}</script>
                  <script type="APPLICATION/LD+JSON">{"@type":"Place","name":"Cased"}</script>"#;

    let data = structured::extract(html);
    let names: Vec<String> = data
        .json_ld
        .iter()
        .filter_map(|r| r.get("name").and_then(|v| v.as_str()).map(str::to_string))
        .collect();
    // One block is malformed; it must not take the valid ones down with it.
    assert_eq!(names, ["Wrapped", "Cased"]);
}

#[test]
fn next_data_is_read_from_its_script_id() {
    let html = r#"<html><body><div id="__next"></div>
        <script id="__NEXT_DATA__" type="application/json">
          {"props":{"pageProps":{"product":{"title":"A Thing","href":"/p/1"}}},"buildId":"abc"}
        </script></body></html>"#;

    let data = structured::extract(html);
    assert_eq!(data.embedded.len(), 1);
    assert_eq!(data.embedded[0].name, "__NEXT_DATA__");

    let outline = structured::render_outline(&data.embedded[0].value, 3);
    assert!(outline.contains("props: object (1 keys)"), "got {outline}");
    assert!(outline.contains("buildId: abc"));
    // Depth 3 stops before the leaf, which is the point of an outline.
    assert!(!outline.contains("A Thing"));
}

#[test]
fn inline_state_assignment_is_read_only_when_it_is_json() {
    let json = r#"<script>window.__INITIAL_STATE__ = {"items":[{"url":"/a"}]};</script>"#;
    let data = structured::extract(json);
    assert_eq!(data.embedded.len(), 1);
    assert_eq!(data.embedded[0].name, "__INITIAL_STATE__");

    // Nuxt 2 assigns a program, not data. Nothing to take is the right answer.
    let program = r#"<script>window.__NUXT__=(function(a,b){return {x:a}}(1,2));</script>"#;
    assert!(structured::extract(program).embedded.is_empty());
}

#[test]
fn braces_inside_strings_do_not_end_the_span() {
    let html = r#"<script>window.__PRELOADED_STATE__ = {"a":"} not the end \" still","b":2};
                  var after = 1;</script>"#;
    let data = structured::extract(html);
    assert_eq!(data.embedded[0].value["b"], 2);
}

#[test]
fn state_urls_are_harvested_and_stripped_of_assets() {
    let base = canonicalize("https://example.com/").unwrap();
    let html = r#"<script id="__NEXT_DATA__" type="application/json">
        {"props":{"items":[{"href":"/p/1"},{"href":"/p/1?utm_source=x"},
                           {"href":"https://example.com/p/2"}],
                  "logo":"/_next/static/chunks/main.js","blurb":"just some text /with a slash"},
         "page":"/[[...slug]]","assetPrefix":"/img/hero.png"}</script>"#;

    let data = structured::extract(html);
    let urls: Vec<String> = structured::urls_in(&data.embedded[0].value, &base, 100)
        .iter()
        .map(|u| u.to_string())
        .collect();

    assert!(urls.contains(&"https://example.com/p/1".to_string()), "got {urls:?}");
    assert!(urls.contains(&"https://example.com/p/2".to_string()));
    // Tracking params collapse into the same entry; build assets and the route
    // template the framework stores next to them never qualify at all.
    assert!(!urls.iter().any(|u| u.contains("slug")), "got {urls:?}");
    assert_eq!(urls.len(), 2, "got {urls:?}");
}

#[test]
fn metadata_falls_back_to_json_ld_when_meta_tags_are_missing() {
    let html = r#"<html><head><script type="application/ld+json">
        {"@type":"NewsArticle","headline":"From the record",
         "datePublished":"2026-02-09","author":{"@type":"Person","name":"A Reporter"}}
        </script></head><body></body></html>"#;

    let meta = extract_metadata(html);
    assert!(meta.contains("Title: From the record"), "got {meta}");
    assert!(meta.contains("Author: A Reporter"));
    assert!(meta.contains("Published: 2026-02-09"));
    assert!(meta.contains("Schema types: NewsArticle"));
}

#[test]
fn meta_tags_win_over_the_record_when_both_are_present() {
    let html = r#"<html><head><title>From the tag</title>
        <script type="application/ld+json">{"@type":"Article","headline":"From the record"}
        </script></head><body></body></html>"#;
    assert!(extract_metadata(html).contains("Title: From the tag"));
}


// *** federated frontier ***

#[tokio::test]
async fn crawl_from_merges_seeds_into_one_budget_and_keeps_provenance() {
    let server = fixture().await;
    let client = test_client();

    let seeds = vec![
        Seed::new(canonicalize(&server.url("/alpha")).unwrap(), Origin::Seed),
        Seed::new(canonicalize(&server.url("/d1")).unwrap(), Origin::Source("fixture".into())),
    ];
    let cfg = CrawlConfig { max_depth: 0, ..test_config() };
    let outcome = crawl_from(&client, seeds, &cfg).await.unwrap();

    let urls = outcome.urls();
    assert!(urls.iter().any(|u| u.ends_with("/alpha")), "got {urls:?}");
    assert!(urls.iter().any(|u| u.ends_with("/d1")));

    // Each page still knows which backend put it in the frontier.
    let origin_of = |suffix: &str| {
        outcome
            .pages
            .iter()
            .find(|p| p.url.as_str().ends_with(suffix))
            .map(|p| p.origin.clone())
            .unwrap()
    };
    assert_eq!(origin_of("/alpha"), Origin::Seed);
    assert_eq!(origin_of("/d1"), Origin::Source("fixture".into()));
}

#[tokio::test]
async fn a_seed_repeated_across_sources_is_fetched_once() {
    let server = fixture().await;
    let seeds = vec![
        Seed::new(canonicalize(&server.url("/alpha")).unwrap(), Origin::Seed),
        // Same page, tracking param and fragment differ.
        Seed::new(canonicalize(&server.url("/alpha?utm_source=x")).unwrap(), Origin::Source("a".into())),
        Seed::new(canonicalize(&server.url("/alpha#top")).unwrap(), Origin::Source("b".into())),
    ];
    let cfg = CrawlConfig { max_depth: 0, ..test_config() };
    let outcome = crawl_from(&test_client(), seeds, &cfg).await.unwrap();

    assert_eq!(outcome.pages.len(), 1);
    assert_eq!(server.hits("/alpha"), 1);
    // First source to name it owns it.
    assert_eq!(outcome.pages[0].origin, Origin::Seed);
}

#[tokio::test]
async fn followed_links_record_the_page_they_came_from() {
    let server = fixture().await;
    let cfg = CrawlConfig { max_depth: 2, ..test_config() };
    let outcome = crawl(&test_client(), &server.url("/d1"), &cfg).await.unwrap();

    let d2 = outcome.pages.iter().find(|p| p.url.as_str().ends_with("/d2")).unwrap();
    match &d2.origin {
        Origin::Link { from } => assert!(from.as_str().ends_with("/d1"), "got {from}"),
        other => panic!("expected a link origin, got {other}"),
    }
    assert_eq!(outcome.pages[0].origin, Origin::Seed);
}

#[tokio::test]
async fn sitemap_seeded_urls_are_labelled_as_such() {
    let server = fixture().await;
    let cfg = CrawlConfig { max_depth: 0, seed_from_sitemap: true, ..test_config() };
    let outcome = crawl(&test_client(), &server.url("/"), &cfg).await.unwrap();

    let alpha = outcome.pages.iter().find(|p| p.url.as_str().ends_with("/alpha")).unwrap();
    assert_eq!(alpha.origin, Origin::Sitemap);

    let counts = outcome.origin_counts();
    assert!(counts.contains(&("seed".to_string(), 1)), "got {counts:?}");
    assert!(counts.iter().any(|(kind, n)| kind == "sitemap" && *n > 1), "got {counts:?}");
}

#[tokio::test]
async fn same_domain_only_admits_every_seeded_host() {
    let server = fixture().await;
    // Two names for one server: with a single `seed_host` the second host's
    // pages would be discarded as off-domain.
    let client = client_resolving("crawler.test", server.addr);
    let seeds = vec![
        Seed::new(canonicalize(&server.url("/")).unwrap(), Origin::Seed),
        Seed::new(canonicalize(&format!("{}/d1", server.aliased("crawler.test"))).unwrap(), Origin::Seed),
    ];

    let cfg = CrawlConfig { max_depth: 1, same_domain_only: true, ..test_config() };
    let outcome = crawl_from(&client, seeds, &cfg).await.unwrap();

    let urls = outcome.urls();
    let on_alias = |suffix: &str| {
        urls.iter().any(|u| u.contains("crawler.test") && u.ends_with(suffix))
    };
    assert!(on_alias("/d1"), "got {urls:?}");
    // And a link followed from the aliased host stays admitted.
    assert!(on_alias("/d2"), "got {urls:?}");
    // The tracker link on the seed page is still blocked.
    assert!(!urls.iter().any(|u| u.contains("google-analytics")));
}

#[tokio::test]
async fn crawl_from_rejects_an_empty_seed_list() {
    assert!(crawl_from(&test_client(), Vec::new(), &test_config()).await.is_err());
}

// *** JSON API fetching ***

#[tokio::test]
async fn fetch_json_accepts_a_body_served_as_text_plain() {
    let server = fixture().await;
    let url = canonicalize(&server.url("/api.json")).unwrap();
    let value = api::fetch_json(&test_client(), &url, &[], 1024 * 1024).await.unwrap();
    assert_eq!(value["ok"], true);
    assert_eq!(value["items"][2], 3);
}

#[tokio::test]
async fn fetch_json_reports_a_malformed_body_as_a_parse_failure() {
    let server = fixture().await;
    let url = canonicalize(&server.url("/api-bad.json")).unwrap();
    let err = api::fetch_json(&test_client(), &url, &[], 1024 * 1024).await.unwrap_err();
    assert!(matches!(err, CrawlError::Json(_)), "got {err}");
}

#[tokio::test]
async fn fetch_json_retries_a_transient_failure() {
    let server = fixture().await;
    let url = canonicalize(&server.url("/flaky.json")).unwrap();
    let value = api::fetch_json(&test_client(), &url, &[], 1024 * 1024).await.unwrap();

    assert_eq!(value["recovered"], true);
    // Once refused, once succeeded — not once, and not three times.
    assert_eq!(server.hits("/flaky.json"), 2);
}

#[tokio::test]
async fn fetch_json_does_not_retry_a_client_error() {
    let server = fixture().await;
    let url = canonicalize(&server.url("/nope.json")).unwrap();
    let err = api::fetch_json(&test_client(), &url, &[], 1024 * 1024).await.unwrap_err();

    assert!(matches!(err, CrawlError::Status(404)), "got {err}");
    // A 404 will still be a 404 next time; retrying it only wastes budget.
    assert_eq!(server.hits("/nope.json"), 1);
}

#[tokio::test]
async fn fetch_json_sends_the_headers_it_is_given() {
    let server = fixture().await;
    let url = canonicalize(&server.url("/api.json")).unwrap();
    api::fetch_json(&test_client(), &url, &[("X-Subscription-Token", "s3cret")], 1024 * 1024)
        .await
        .unwrap();
    assert!(server.sent().contains("x-subscription-token: s3cret"), "got {}", server.sent());
}

#[tokio::test]
async fn fetch_json_stops_at_the_body_limit() {
    let server = fixture().await;
    let url = canonicalize(&server.url("/api.json")).unwrap();
    let err = api::fetch_json(&test_client(), &url, &[], 8).await.unwrap_err();
    assert!(matches!(err, CrawlError::TooLarge(8)), "got {err}");
}

// *** destination policy ***

#[test]
fn only_public_unicast_addresses_are_reachable() {
    let public = [
        "8.8.8.8", "1.1.1.1", "93.184.216.34", "2606:4700:4700::1111",
    ];
    let blocked = [
        // The cloud metadata endpoint is the reason this exists.
        "169.254.169.254",
        "127.0.0.1", "0.0.0.0", "10.0.0.1", "172.16.0.1", "192.168.1.1",
        // Carrier-grade NAT, benchmarking, reserved, broadcast.
        "100.64.0.1", "198.18.0.1", "240.0.0.1", "255.255.255.255",
        "::1", "::", "fe80::1", "fc00::1", "2001:db8::1",
        // An IPv4 destination wearing a v6 costume.
        "::ffff:127.0.0.1", "::ffff:169.254.169.254",
    ];

    for ip in public {
        assert!(net::is_public(ip.parse().unwrap()), "{ip} should be reachable");
    }
    for ip in blocked {
        assert!(!net::is_public(ip.parse().unwrap()), "{ip} should be refused");
    }
}

#[test]
fn strict_policy_refuses_address_literals_and_odd_schemes() {
    let strict = NetPolicy::strict();
    assert!(strict.check_url(&Url::parse("https://example.com/x").unwrap()).is_ok());
    assert!(strict.check_url(&Url::parse("http://169.254.169.254/latest/meta-data/").unwrap()).is_err());
    assert!(strict.check_url(&Url::parse("http://[::1]:8080/").unwrap()).is_err());
    assert!(strict.check_url(&Url::parse("file:///etc/passwd").unwrap()).is_err());

    // A hostname cannot be judged here — that is the resolver's job.
    assert!(strict.check_url(&Url::parse("http://localhost/").unwrap()).is_ok());

    // Opting in is what makes intranet use possible at all.
    assert!(NetPolicy::permissive().check_url(&Url::parse("http://127.0.0.1:8080/").unwrap()).is_ok());
}

#[tokio::test]
async fn the_resolver_refuses_a_name_that_resolves_to_loopback() {
    let name = reqwest::dns::Name::from_str("localhost").unwrap();
    let refused = net::resolver(NetPolicy::strict()).resolve(name).await;
    let err = match refused {
        Ok(_) => panic!("localhost should not resolve to anything reachable"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("non-public"), "got {err}");

    // The same name resolves normally once intranet use is opted into.
    let name = reqwest::dns::Name::from_str("localhost").unwrap();
    let addrs = net::resolver(NetPolicy::permissive()).resolve(name).await.unwrap();
    assert!(addrs.count() > 0);
}

#[tokio::test]
async fn a_blocked_destination_fails_before_any_request_is_made() {
    let strict = NetPolicy::strict();
    let url = Url::parse("http://169.254.169.254/latest/meta-data/").unwrap();
    // The guard is a pure check on the URL, so it holds whether or not anything
    // is listening at the other end.
    assert!(strict.check_url(&url).is_err());
}


// *** discovery ***

/// A source that answers from memory, to test the federation itself rather
/// than any backend. Written outside the module that defines the trait, which
/// is the claim the trait is making: adding a source is one file.
#[derive(Debug)]
struct Canned {
    name: &'static str,
    leads: Vec<(&'static str, f32)>,
    fails: bool,
    delay: Duration,
}

impl Canned {
    fn new(name: &'static str, leads: Vec<(&'static str, f32)>) -> Self {
        Self { name, leads, fails: false, delay: Duration::ZERO }
    }
}

#[async_trait::async_trait]
impl DiscoverySource for Canned {
    fn name(&self) -> Arc<str> {
        Arc::from(self.name)
    }

    async fn discover(
        &self,
        _client: &reqwest::Client,
        _query: &Query,
        _budget: &DiscoveryBudget,
    ) -> Result<Found, CrawlError> {
        tokio::time::sleep(self.delay).await;
        if self.fails {
            return Err(CrawlError::Transport("backend is down".into()));
        }
        Ok(Found::leads(self
            .leads
            .iter()
            .map(|(url, score)| Lead {
                url: canonicalize(url).unwrap(),
                score: *score,
                title: None,
                seen: None,
                domain: None,
                language: None,
                sources: vec![Arc::from(self.name)],
            })
            .collect()))
    }
}

fn test_query() -> Query {
    Query { text: "state visit".into(), since: None, until: None, sites: Vec::new() }
}

#[tokio::test]
async fn gdelt_scores_by_rank_and_keeps_what_it_was_told() {
    let server = fixture().await;
    let gdelt = Gdelt::at(&server.url("/gdelt")).unwrap();
    let leads = gdelt
        .discover(&test_client(), &test_query(), &DiscoveryBudget::default())
        .await
        .unwrap()
        .leads;

    assert_eq!(leads.len(), 3);
    // GDELT reports no relevance number, so its own ordering is the signal.
    assert!(leads[0].score > leads[1].score && leads[1].score > leads[2].score);
    assert_eq!(leads[0].score, 1.0);

    assert_eq!(leads[1].title.as_deref(), Some("Second report"));
    assert_eq!(leads[1].language.as_deref(), Some("Spanish"));
    assert_eq!(leads[1].domain.as_deref(), Some("example.org"));
    assert_eq!(leads[1].seen.as_deref(), Some("20260826T101500Z"));
    // Canonicalized on the way in, like every other URL in the crawler.
    assert_eq!(leads[2].url.as_str(), "https://example.org/third");
}

#[tokio::test]
async fn gdelt_sends_the_query_it_was_given() {
    let server = fixture().await;
    let gdelt = Gdelt::at(&server.url("/gdelt")).unwrap();
    let query = Query {
        text: "\"state visit\"".into(),
        since: Some("20260101".into()),
        until: Some("20260201".into()),
        sites: Vec::new(),
    };
    let budget = DiscoveryBudget { max_candidates: 10, ..DiscoveryBudget::default() };
    gdelt.discover(&test_client(), &query, &budget).await.unwrap();

    let sent = server.sent();
    assert!(sent.contains("query=%22state+visit%22"), "got {sent}");
    assert!(sent.contains("format=json") && sent.contains("mode=artlist"), "got {sent}");
    assert!(sent.contains("maxrecords=10"), "got {sent}");
    // A date becomes a whole timestamp, which is all GDELT accepts.
    assert!(sent.contains("startdatetime=20260101000000"), "got {sent}");
    assert!(sent.contains("enddatetime=20260201235959"), "got {sent}");
}

#[tokio::test]
async fn gdelt_reads_a_missing_articles_key_as_no_results() {
    let server = fixture().await;
    let gdelt = Gdelt::at(&server.url("/gdelt-empty")).unwrap();
    let leads = gdelt
        .discover(&test_client(), &test_query(), &DiscoveryBudget::default())
        .await
        .unwrap()
        .leads;
    // `{}` means nothing matched. Reporting that as a failure would turn every
    // empty search into an error.
    assert!(leads.is_empty());
}

#[tokio::test]
async fn gdelt_recognises_a_rate_limit_answered_as_success() {
    let server = fixture().await;
    let gdelt = Gdelt::at(&server.url("/gdelt-limited")).unwrap();
    let err = gdelt
        .discover(&test_client(), &test_query(), &DiscoveryBudget::default())
        .await
        .unwrap_err();

    // 200 OK carrying a sentence of prose. Parsed blindly it reads as malformed
    // JSON, which would send the caller looking for the wrong problem.
    assert!(matches!(err, CrawlError::RateLimited(_)), "got {err}");
}

#[tokio::test]
async fn gdelt_refuses_an_empty_query() {
    let server = fixture().await;
    let gdelt = Gdelt::at(&server.url("/gdelt")).unwrap();
    let query = Query { text: "   ".into(), since: None, until: None, sites: Vec::new() };
    assert!(gdelt.discover(&test_client(), &query, &DiscoveryBudget::default()).await.is_err());
    assert_eq!(server.hits("/gdelt"), 0, "an empty query should cost no request");
}

#[tokio::test]
async fn federate_merges_one_url_found_by_two_sources() {
    let sources: Vec<Box<dyn DiscoverySource>> = vec![
        Box::new(Canned::new("alpha", vec![("https://example.org/shared", 0.4)])),
        Box::new(Canned::new(
            "beta",
            vec![("https://example.org/shared?utm_source=x", 0.9), ("https://example.org/only", 0.5)],
        )),
    ];
    let outcome = federate(&test_client(), &sources, &test_query(), &DiscoveryBudget::default()).await;

    assert_eq!(outcome.leads.len(), 2, "got {:?}", outcome.leads);
    let shared = &outcome.leads[0];
    assert_eq!(shared.url.as_str(), "https://example.org/shared");
    // Best score wins; agreement is recorded rather than added to the score.
    assert_eq!(shared.score, 0.9);
    assert_eq!(shared.sources.len(), 2);
    assert!(shared.sources.iter().any(|s| &**s == "alpha"));
    assert!(shared.sources.iter().any(|s| &**s == "beta"));
}

#[tokio::test]
async fn federate_keeps_going_when_a_source_fails_or_hangs() {
    let mut broken = Canned::new("broken", vec![]);
    broken.fails = true;
    let mut slow = Canned::new("slow", vec![("https://example.org/late", 1.0)]);
    slow.delay = Duration::from_secs(30);

    let sources: Vec<Box<dyn DiscoverySource>> = vec![
        Box::new(broken),
        Box::new(slow),
        Box::new(Canned::new("working", vec![("https://example.org/found", 0.7)])),
    ];
    let budget = DiscoveryBudget { deadline: Duration::from_millis(80), ..Default::default() };
    let outcome = federate(&test_client(), &sources, &test_query(), &budget).await;

    // One backend down and one hanging must not cost the results of the third.
    assert_eq!(outcome.leads.len(), 1);
    assert_eq!(outcome.leads[0].url.as_str(), "https://example.org/found");
    assert_eq!(outcome.failures.len(), 2);
    assert!(outcome.failures.iter().any(|(name, why)| name == "slow" && why.contains("timed out")));
    assert!(outcome.failures.iter().any(|(name, _)| name == "broken"));
}

#[tokio::test]
async fn federate_cuts_to_the_candidate_budget_keeping_the_best() {
    let sources: Vec<Box<dyn DiscoverySource>> = vec![Box::new(Canned::new(
        "alpha",
        vec![
            ("https://example.org/low", 0.1),
            ("https://example.org/high", 0.9),
            ("https://example.org/mid", 0.5),
        ],
    ))];
    let budget = DiscoveryBudget { max_candidates: 2, ..Default::default() };
    let outcome = federate(&test_client(), &sources, &test_query(), &budget).await;

    let urls: Vec<&str> = outcome.leads.iter().map(|l| l.url.as_str()).collect();
    assert_eq!(urls, ["https://example.org/high", "https://example.org/mid"]);
}

#[tokio::test]
async fn one_source_cannot_swallow_the_candidate_budget() {
    // A source that returns many leads at one score, against one that returns
    // few. This is Wikidata's sixty-one language editions versus an archive
    // index, which live testing showed crowding the archive out entirely.
    let many: Vec<(&'static str, f32)> = vec![
        ("https://example.org/a", 0.7),
        ("https://example.org/b", 0.7),
        ("https://example.org/c", 0.7),
        ("https://example.org/d", 0.7),
        ("https://example.org/e", 0.7),
    ];
    let sources: Vec<Box<dyn DiscoverySource>> = vec![
        Box::new(Canned::new("flood", many)),
        Box::new(Canned::new("archive", vec![("https://archive.test/one", 0.5)])),
    ];
    let budget = DiscoveryBudget { max_candidates: 4, ..Default::default() };
    let outcome = federate(&test_client(), &sources, &test_query(), &budget).await;

    assert_eq!(outcome.leads.len(), 4);
    let from_archive = outcome
        .leads
        .iter()
        .filter(|l| l.sources.iter().any(|s| &**s == "archive"))
        .count();
    // Lower-scoring but from the only other source: it must survive the cut.
    assert_eq!(from_archive, 1, "got {:?}", outcome.leads.iter().map(|l| l.url.as_str()).collect::<Vec<_>>());
}

#[tokio::test]
async fn a_lead_becomes_a_seed_that_remembers_its_source() {
    let sources: Vec<Box<dyn DiscoverySource>> =
        vec![Box::new(Canned::new("alpha", vec![("https://example.org/one", 0.6)]))];
    let outcome = federate(&test_client(), &sources, &test_query(), &DiscoveryBudget::default()).await;

    let seeds = outcome.seeds();
    assert_eq!(seeds[0].score, 0.6);
    assert_eq!(seeds[0].origin, Origin::Source("alpha".into()));
}

#[tokio::test]
async fn discovered_seeds_are_fetched_best_first() {
    let server = fixture().await;
    // Deliberately queued worst-first; only the score should decide.
    let seeds = vec![
        Seed::scored(canonicalize(&server.url("/d3")).unwrap(), Origin::Source("s".into()), 0.1),
        Seed::scored(canonicalize(&server.url("/alpha")).unwrap(), Origin::Source("s".into()), 0.9),
        Seed::scored(canonicalize(&server.url("/d1")).unwrap(), Origin::Source("s".into()), 0.5),
    ];
    // One worker and a budget of two, so the order is the whole result.
    let cfg = CrawlConfig { max_pages: 2, max_concurrency: 1, max_depth: 0, ..test_config() };
    let outcome = crawl_from(&test_client(), seeds, &cfg).await.unwrap();

    let urls = outcome.urls();
    assert!(urls.iter().any(|u| u.ends_with("/alpha")), "got {urls:?}");
    assert!(urls.iter().any(|u| u.ends_with("/d1")), "got {urls:?}");
    assert!(!urls.iter().any(|u| u.ends_with("/d3")), "lowest score should have been dropped");
}


// *** wikidata ***

#[tokio::test]
async fn wikidata_returns_the_official_site_and_every_language_edition() {
    let server = fixture().await;
    let wd = Wikidata::at(&server.url("/wd")).unwrap();
    let found = wd
        .discover(&test_client(), &test_query(), &DiscoveryBudget::default())
        .await
        .unwrap();
    let leads = found.leads;

    let urls: Vec<&str> = leads.iter().map(|l| l.url.as_str()).collect();
    // The primary source outranks anything written about it.
    assert_eq!(urls[0], "https://president.kg/");
    assert_eq!(leads[0].score, 1.0);

    // Every language edition, which is the part a keyword search will not give.
    assert!(urls.contains(&"https://en.wikipedia.org/wiki/Sadyr_Japarov"), "got {urls:?}");
    assert!(urls.contains(&"https://ky.wikipedia.org/wiki/Sadyr"), "got {urls:?}");
    let ky = leads.iter().find(|l| l.url.as_str().contains("ky.wikipedia")).unwrap();
    assert_eq!(ky.language.as_deref(), Some("ky"));

    // commonswiki is not a language edition and must not be labelled as one,
    // nor outrank real articles by sorting earlier than them.
    let commons = leads.iter().find(|l| l.url.as_str().contains("commons")).unwrap();
    assert_eq!(commons.language, None);
    let english = leads.iter().find(|l| l.url.as_str().contains("en.wikipedia")).unwrap();
    assert!(english.score > ky.score && ky.score > commons.score);

    // Resolve, then read: two calls, not one and not three.
    assert_eq!(server.hits("/wd"), 2);
}

#[tokio::test]
async fn wikidata_treats_an_unknown_name_as_no_results() {
    let server = fixture().await;
    let wd = Wikidata::at(&server.url("/wd-search-empty")).unwrap();
    let leads = wd
        .discover(&test_client(), &test_query(), &DiscoveryBudget::default())
        .await
        .unwrap()
        .leads;
    assert!(leads.is_empty());
    // One request, not two: there was no entity to look up.
    assert_eq!(server.hits("/wd-search-empty"), 1);
}

#[tokio::test]
async fn wikidata_needs_two_requests_and_says_so_by_doing_nothing() {
    let server = fixture().await;
    let wd = Wikidata::at(&server.url("/wd-search")).unwrap();
    let budget = DiscoveryBudget { max_requests: 1, ..DiscoveryBudget::default() };
    assert!(wd.discover(&test_client(), &test_query(), &budget).await.unwrap().leads.is_empty());
    assert_eq!(server.hits("/wd-search"), 0, "a budget it cannot meet should cost nothing");
}

// *** wayback ***

#[tokio::test]
async fn wayback_turns_captures_into_replay_urls() {
    let server = fixture().await;
    let wayback = Wayback::at(&server.url("/cdx")).unwrap();
    let query = Query { sites: vec!["mfa.gov.kg".into()], ..test_query() };
    let leads = wayback
        .discover(&test_client(), &query, &DiscoveryBudget::default())
        .await
        .unwrap()
        .leads;

    assert_eq!(leads.len(), 2, "the header row is not a result");
    assert_eq!(
        leads[0].url.as_str(),
        "https://web.archive.org/web/20190104061944/http://mfa.gov.kg/news/1"
    );
    // The original is what stopped resolving; it is kept as the label.
    assert_eq!(leads[0].title.as_deref(), Some("http://mfa.gov.kg/news/1"));
    assert_eq!(leads[0].seen.as_deref(), Some("20190104061944"));
    assert_eq!(leads[0].domain.as_deref(), Some("mfa.gov.kg"));
}

#[tokio::test]
async fn wayback_asks_only_for_html_captures_that_worked() {
    let server = fixture().await;
    let wayback = Wayback::at(&server.url("/cdx")).unwrap();
    let query = Query {
        sites: vec!["mfa.gov.kg".into()],
        since: Some("20190101".into()),
        until: Some("20211231".into()),
        ..test_query()
    };
    wayback.discover(&test_client(), &query, &DiscoveryBudget::default()).await.unwrap();

    let sent = server.sent();
    assert!(sent.contains("url=mfa.gov.kg*"), "got {sent}");
    assert!(sent.contains("collapse=urlkey"), "got {sent}");
    assert!(sent.contains("filter=statuscode%3A200"), "got {sent}");
    assert!(sent.contains("filter=mimetype%3Atext%2Fhtml"), "got {sent}");
    assert!(sent.contains("from=20190101") && sent.contains("to=20211231"), "got {sent}");
}

#[tokio::test]
async fn wayback_has_nothing_to_say_about_a_text_only_query() {
    let server = fixture().await;
    let wayback = Wayback::at(&server.url("/cdx")).unwrap();
    // No site named, so nothing to expand. That is an empty answer, not an
    // error — the round should not report a failure for it.
    let leads = wayback
        .discover(&test_client(), &test_query(), &DiscoveryBudget::default())
        .await
        .unwrap()
        .leads;
    assert!(leads.is_empty());
    assert_eq!(server.hits("/cdx"), 0);
}

#[tokio::test]
async fn wayback_survives_an_empty_index() {
    let server = fixture().await;
    let wayback = Wayback::at(&server.url("/cdx-empty")).unwrap();
    let query = Query { sites: vec!["mfa.gov.kg".into()], ..test_query() };
    let leads = wayback
        .discover(&test_client(), &query, &DiscoveryBudget::default())
        .await
        .unwrap()
        .leads;
    assert!(leads.is_empty());
}

// *** brave ***

#[tokio::test]
async fn brave_sends_its_key_as_a_header_and_ranks_by_position() {
    let server = fixture().await;
    let brave = Brave::at(&server.url("/brave"), "s3cret").unwrap();
    let leads = brave
        .discover(&test_client(), &test_query(), &DiscoveryBudget::default())
        .await
        .unwrap()
        .leads;

    assert_eq!(leads.len(), 2);
    assert_eq!(leads[0].score, 1.0);
    assert!(leads[0].score > leads[1].score);
    assert_eq!(leads[0].seen.as_deref(), Some("2 days ago"));
    assert_eq!(leads[0].domain.as_deref(), Some("example.org"));

    // The credential travels in a header, never in the query string, and never
    // through a tool schema.
    let sent = server.sent();
    assert!(sent.contains("x-subscription-token: s3cret"), "got {sent}");
    assert!(!sent.contains("q=state+visit&key"), "the key must not be a query parameter");
}

#[test]
fn brave_is_absent_rather_than_broken_without_a_key() {
    // Safe to assert unconditionally: the suite does not set the variable, and
    // a source that fails identically on every call is worse than no source.
    unsafe { std::env::remove_var(crate::sources::brave::API_KEY_ENV) };
    assert!(Brave::from_env().is_none());
}


// *** link scoring ***

fn subject() -> Terms {
    // The same person, as three different outlets would write the name.
    Terms::new(["Sadyr Japarov", "Sadyr Zhaparov", "Садыр Жапаров"])
}

fn link(url: &str, anchor: &str) -> LinkContext {
    LinkContext {
        url: canonicalize(url).unwrap(),
        anchor: anchor.to_string(),
        nearby: String::new(),
        rel_next: false,
    }
}

#[test]
fn a_name_matches_across_scripts_and_spellings() {
    let terms = subject();
    // Each spelling finds the subject, which is the entire point of carrying
    // the alias set: outlets do not agree on romanisation.
    assert_eq!(terms.match_strength("Sadyr Japarov arrived"), 1.0);
    assert_eq!(terms.match_strength("SADYR ZHAPAROV ARRIVED"), 1.0);
    assert_eq!(terms.match_strength("Садыр Жапаров прибыл"), 1.0);
    // A surname alone is partial credit, not nothing.
    let partial = terms.match_strength("Japarov met the delegation");
    assert!(partial > 0.0 && partial < 1.0, "got {partial}");
    assert_eq!(terms.match_strength("the cabinet met today"), 0.0);
}

#[test]
fn accents_fold_so_one_romanisation_finds_another() {
    let terms = Terms::new(["Sadir Japarov"]);
    // Azerbaijani writes it Sadır; Czech, Žaparov. Folding is what lets one
    // spelling in the query match another in the anchor text.
    assert_eq!(terms.match_strength("Sadır Japarov"), 1.0);
    assert_eq!(Terms::new(["Zaparov"]).match_strength("Žaparov"), 1.0);
}

#[test]
fn url_shape_tells_an_archive_from_a_dead_end() {
    let shape = |u: &str| Shape::of(&canonicalize(u).unwrap());

    assert!(shape("https://x.test/press-releases/2019/").archive);
    assert!(shape("https://x.test/press-releases/2019/").dated);
    assert!(shape("https://x.test/news?page=47").paginated);
    assert!(shape("https://x.test/news/page/47").paginated);
    assert!(shape("https://x.test/cookie-policy").dead_end);
    assert!(shape("https://x.test/assets/app.js").asset);

    // A bare article path is none of these, and that is fine.
    let plain = shape("https://x.test/some/page");
    assert!(!plain.archive && !plain.paginated && !plain.dead_end);
}

#[test]
fn the_archive_outranks_the_cookie_notice() {
    let terms = subject();
    let archive = score_link(&link("https://x.test/press-releases/2019/", "Press releases"), &terms, 0.0).unwrap();
    let cookies = score_link(&link("https://x.test/cookie-policy", "Cookie policy"), &terms, 0.0).unwrap();
    let plain = score_link(&link("https://x.test/about", "About"), &terms, 0.0).unwrap();

    // This is the whole behaviour in one assertion: given a budget, the crawl
    // spends it on the archive rather than the cookie notice.
    assert!(archive > plain, "archive {archive} vs plain {plain}");
    assert!(plain > cookies, "plain {plain} vs cookies {cookies}");
    assert!(archive >= ON_TOPIC);
    assert!(cookies < ON_TOPIC);
}

#[test]
fn a_link_named_in_the_subjects_own_language_scores() {
    let terms = subject();
    // Before aliases reached the scorer this was zero, and the Kyrgyz and
    // Russian coverage — most of what exists — was invisible.
    let cyrillic = score_link(&link("https://x.test/n/1", "Садыр Жапаров встретился"), &terms, 0.0).unwrap();
    assert!(cyrillic >= ON_TOPIC, "got {cyrillic}");
}

#[test]
fn pagination_is_followed_because_nothing_else_indexes_it() {
    let terms = Terms::default();
    let mut next = link("https://x.test/news", "Next");
    next.rel_next = true;

    // Page 47 of an archive is not in any search index. Both the query form
    // and the rel attribute have to be enough on their own, since an unfocused
    // crawl has no subject to match against.
    assert!(score_link(&link("https://x.test/news?page=47", "47"), &terms, 0.0).unwrap() >= ON_TOPIC);
    assert!(score_link(&next, &terms, 0.0).unwrap() >= ON_TOPIC);
}

#[test]
fn assets_are_never_worth_a_request() {
    // Not a low score — not a page at all, however well its URL matches.
    assert_eq!(score_link(&link("https://x.test/app.js", "Sadyr Japarov"), &subject(), 1.0), None);
}

#[test]
fn a_relevant_parent_lifts_its_links() {
    let terms = subject();
    let anonymous = link("https://x.test/n/1", "Read more");
    let from_nowhere = score_link(&anonymous, &terms, 0.0).unwrap();
    let from_a_good_page = score_link(&anonymous, &terms, 1.0).unwrap();

    // How a crawl stays on a thread instead of leaving it at the first hop.
    assert!(from_a_good_page > from_nowhere);
}

// *** focused crawling ***

#[tokio::test]
async fn a_focused_crawl_spends_its_budget_on_the_archive() {
    let server = fixture().await;
    let cfg = CrawlConfig {
        max_pages: 3,
        max_concurrency: 1,
        max_depth: 2,
        focus: Some(Terms::new(["Japarov"])),
        ..test_config()
    };
    let outcome = crawl(&test_client(), &server.url("/hub"), &cfg).await.unwrap();

    let urls = outcome.urls();
    // The press archive is listed last on the page and fetched first anyway.
    assert!(urls.iter().any(|u| u.ends_with("/press/2019/")), "got {urls:?}");
    assert!(!urls.iter().any(|u| u.contains("cookie-policy")), "got {urls:?}");
    assert_eq!(server.hits("/cookie-policy"), 0, "a dead end should cost nothing");
}

#[tokio::test]
async fn a_focused_crawl_follows_the_next_page() {
    let server = fixture().await;
    let cfg = CrawlConfig {
        max_pages: 10,
        max_depth: 2,
        focus: Some(Terms::new(["Japarov"])),
        ..test_config()
    };
    let outcome = crawl(&test_client(), &server.url("/hub"), &cfg).await.unwrap();

    // Page two exists only behind a rel="next" link. No index lists it.
    assert!(outcome.urls().iter().any(|u| u.ends_with("/press/2019/page/2")), "got {:?}", outcome.urls());
}

#[tokio::test]
async fn tunnelling_reaches_what_sits_behind_a_dull_page() {
    let server = fixture().await;
    let focus = Terms::new(["Садыр Жапаров"]);

    // "Read more" on a page that names nothing: the link scores below the
    // threshold, and the page it leads to is the one that matters.
    let cfg = CrawlConfig {
        max_pages: 10,
        max_depth: 2,
        focus: Some(focus.clone()),
        tunnel_slack: 2,
        ..test_config()
    };
    let reached = crawl(&test_client(), &server.url("/dull"), &cfg).await.unwrap();
    assert!(reached.urls().iter().any(|u| u.ends_with("/q/target")), "got {:?}", reached.urls());

    // With no allowance the same crawl prunes at the first dull hop, which is
    // exactly the failure tunnelling exists to prevent.
    let strict = CrawlConfig { tunnel_slack: 0, ..cfg };
    let pruned = crawl(&test_client(), &server.url("/dull"), &strict).await.unwrap();
    assert!(!pruned.urls().iter().any(|u| u.ends_with("/q/target")), "got {:?}", pruned.urls());
}

#[tokio::test]
async fn relevance_is_measured_from_the_page_that_arrived() {
    let server = fixture().await;
    let cfg = CrawlConfig {
        max_pages: 5,
        max_depth: 1,
        focus: Some(Terms::new(["Japarov"])),
        ..test_config()
    };
    let outcome = crawl(&test_client(), &server.url("/hub"), &cfg).await.unwrap();

    let press = outcome.pages.iter().find(|p| p.url.as_str().ends_with("/press/2019/")).unwrap();
    let hub = outcome.pages.iter().find(|p| p.url.as_str().ends_with("/hub")).unwrap();
    // The hub's link text promised the archive; its own body says nothing.
    assert!(press.relevance > hub.relevance, "press {} hub {}", press.relevance, hub.relevance);
}

#[tokio::test]
async fn an_unfocused_crawl_is_still_breadth_first() {
    let server = fixture().await;
    // No subject, so every link scores equally and insertion order decides.
    // The heap must not reorder what the old queue would have kept.
    let cfg = CrawlConfig { max_pages: 2, max_concurrency: 1, max_depth: 2, ..test_config() };
    let outcome = crawl(&test_client(), &server.url("/d1"), &cfg).await.unwrap();

    let urls = outcome.urls();
    assert!(urls[0].ends_with("/d1"), "got {urls:?}");
    assert!(urls[1].ends_with("/d2"), "got {urls:?}");
}


// *** unicode safety ***
//
// Every string here comes off the web, and the web is not ASCII. Both of these
// panicked the worker thread before they were fixed — one on an anchor long
// enough to truncate, one on a search offset taken from folded text.

#[tokio::test]
async fn a_non_latin_page_does_not_panic_the_crawl() {
    let server = fixture().await;
    let cfg = CrawlConfig {
        max_depth: 1,
        focus: Some(Terms::new(["Садыр Жапаров"])),
        ..test_config()
    };
    let outcome = crawl(&test_client(), &server.url("/cyrillic"), &cfg).await.unwrap();

    // The anchor is far past the truncation limit and made of multi-byte
    // characters, so cutting it on a byte index lands mid-character.
    assert!(outcome.urls().iter().any(|u| u.ends_with("/q/target")), "got {:?}", outcome.urls());
}

#[tokio::test]
async fn search_quotes_the_original_text_not_the_folded_one() {
    let server = fixture().await;
    let cfg = CrawlConfig { max_depth: 0, ..test_config() };
    let (matches, _) =
        search_site(&test_client(), &server.url("/cyrillic"), "ZIYARET", &cfg).await.unwrap();

    let snippet = &matches.first().expect("the page contains the term").snippet;
    // `İ` lowercases to two characters, so every offset after it shifts. Taken
    // from the folded string and applied to the original, the quote lands in
    // the wrong place — or between the bytes of a character.
    assert!(snippet.contains("ziyareti"), "got {snippet}");
    assert!(snippet.contains("İstanbul"), "got {snippet}");
}

#[tokio::test]
async fn an_empty_search_term_is_refused() {
    let server = fixture().await;
    let cfg = CrawlConfig { max_depth: 0, ..test_config() };
    // Otherwise it matches between every pair of characters and reports a hit
    // count one larger than the page length.
    assert!(search_site(&test_client(), &server.url("/"), "   ", &cfg).await.is_err());
}


// *** Persian and Arabic ***
//
// Persian and Arabic keyboards produce different code points for what is the
// same letter, and both spellings of a name circulate. These were all misses
// before Arabic-script folding existed.

#[test]
fn persian_and_arabic_spellings_of_one_word_match() {
    let cases = [
        // Farsi yeh against Arabic yeh.
        ("صادیر جاپاروف", "صادير جاپاروف امروز"),
        // Farsi kaf against Arabic kaf.
        ("کوروش", "كوروش"),
        // Alef with hamza against bare alef.
        ("ایران", "إيران"),
        // Heh against teh marbuta.
        ("مدرسه", "مدرسة"),
        // A single fatha must not split the word in two.
        ("سلام", "سَلام"),
        // Persian digits against ASCII.
        ("۱۴۰۲", "1402"),
        // Zero-width non-joiner against a space.
        ("می‌رود", "می رود"),
    ];
    for (term, text) in cases {
        assert_eq!(Terms::new([term]).match_strength(text), 1.0, "{term} should match {text}");
    }
}

#[test]
fn hijri_years_count_as_dates() {
    let dated = |u: &str| Shape::of(&canonicalize(u).unwrap()).dated;
    // Iranian archives are sliced by the Solar Hijri calendar, in either digit set.
    assert!(dated("https://x.ir/news/1402/"));
    assert!(dated("https://x.ir/news/%DB%B1%DB%B4%DB%B0%DB%B2/"));
    assert!(dated("https://x.test/news/2023/"));
    assert!(!dated("https://x.test/news/1750/"));
}

#[test]
fn a_persian_archive_link_is_on_topic() {
    let terms = Terms::new(["صادیر جاپاروف"]);
    // Percent-encoded on the wire; decoded, it is an archive folder and a
    // Persian-calendar year — which is where a ministry's history sits.
    let archive = score_link(&link("https://x.ir/آرشیو/۱۴۰۲/", "آرشیو"), &terms, 0.0).unwrap();
    assert!(archive >= ON_TOPIC, "got {archive}");

    let named = score_link(&link("https://x.ir/news/1", "صادیر جاپاروف با هیئت دیدار کرد"), &terms, 0.0).unwrap();
    assert!(named >= ON_TOPIC, "got {named}");
}
