use crate::crawler::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

// fixture server
//
// The suite used to crawl live sites, so it was non-hermetic: it failed offline
// and whenever a third party changed their markup. Everything below runs against
// a local server with known content and a request counter.


struct Fixture {
    base: String,
    hits: Arc<Mutex<HashMap<String, usize>>>,
}

impl Fixture {
    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base, path)
    }

    fn hits(&self, path: &str) -> usize {
        self.hits.lock().unwrap().get(path).copied().unwrap_or(0)
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
        _ => (404, "text/html", "<html><body>not found</body></html>".into()),
    }
}

async fn fixture() -> Fixture {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hits: Arc<Mutex<HashMap<String, usize>>> = Arc::new(Mutex::new(HashMap::new()));
    let counter = Arc::clone(&hits);

    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let counter = Arc::clone(&counter);
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

                *counter.lock().unwrap().entry(path.clone()).or_insert(0) += 1;

                let (status, content_type, body) = body_for(&path);
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
        hits,
    }
}

fn test_config() -> CrawlConfig {
    CrawlConfig {
        per_host_delay: Duration::ZERO,
        max_concurrency: 4,
        ..CrawlConfig::default()
    }
}

fn test_client() -> reqwest::Client {
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
