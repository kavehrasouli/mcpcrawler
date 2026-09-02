use crate::api;
use crate::crawler::*;
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
        "/report.pdf" => (200, "application/pdf", "%PDF-1.4 not html".into()),
        // Served as text/plain on purpose: GDELT does exactly this, and the
        // HTML content-type gate must not be in the way of a JSON fetch.
        "/api.json" => (200, "text/plain", r#"{"ok":true,"items":[1,2,3]}"#.into()),
        "/api-bad.json" => (200, "application/json", "not json at all".into()),
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
                let path = request
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap_or("/")
                    .to_string();

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
        (canonicalize(&server.url("/alpha")).unwrap(), Origin::Seed),
        (canonicalize(&server.url("/d1")).unwrap(), Origin::Source("fixture".into())),
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
        (canonicalize(&server.url("/alpha")).unwrap(), Origin::Seed),
        // Same page, tracking param and fragment differ.
        (canonicalize(&server.url("/alpha?utm_source=x")).unwrap(), Origin::Source("a".into())),
        (canonicalize(&server.url("/alpha#top")).unwrap(), Origin::Source("b".into())),
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
        (canonicalize(&server.url("/")).unwrap(), Origin::Seed),
        (canonicalize(&format!("{}/d1", server.aliased("crawler.test"))).unwrap(), Origin::Seed),
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
