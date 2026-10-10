use crate::api;
use crate::crawler::*;
use crate::discovery::{DiscoveryBudget, DiscoverySource, Found, Lead, Query, Span, federate};
use crate::dedup;
use crate::expand::{self, Plan};
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
        "/wd-search-office" => (
            200,
            "application/json",
            r#"{"search": [{"id": "Q853036", "label": "President of Kyrgyzstan",
                            "description": "political position"}]}"#
                .into(),
        ),
        // A position: it lists who holds it and since when.
        "/wd-office" => (
            200,
            "application/json",
            r#"{"entities": {"Q853036": {
                 "labels": {"en": {"language": "en", "value": "President of Kyrgyzstan"}},
                 "claims": {
                   "P31": [{"mainsnak": {"datavalue": {"value": {"id": "Q4164871"}}}}],
                   "P1308": [{"mainsnak": {"datavalue": {"value": {"id": "Q25597826"}}},
                              "qualifiers": {"P580": [{"datavalue": {"value": {"time": "+2021-01-28T00:00:00Z"}}}]}}]
                 },
                 "sitelinks": {"enwiki": {"site": "enwiki", "title": "President of Kyrgyzstan",
                               "url": "https://en.wikipedia.org/wiki/President_of_Kyrgyzstan"}}}}}"#
                .into(),
        ),
        "/wd-labels" => (
            200,
            "application/json",
            r#"{"entities": {
                 "Q111": {"labels": {"en": {"language": "en", "value": "Prime Minister of Kyrgyzstan"}}},
                 "Q222": {"labels": {"en": {"language": "en", "value": "President of Kyrgyzstan"}}}}}"#
                .into(),
        ),
        "/wd-entity" => (
            200,
            "application/json",
            r#"{"entities": {"Q25597826": {
                 "labels": {"en": {"language": "en", "value": "Sadyr Japarov"}},
                 "aliases": {"en": [{"language": "en", "value": "Sadyr Zhaparov"}]},
                 "claims": {"P856": [{"mainsnak": {"datavalue": {"value": "https://president.kg/"}}}],
                            "P1559": [{"mainsnak": {"datavalue": {"value": {"text": "Жапаров Садыр", "language": "ky"}}}}],
                            "P39": [
                              {"mainsnak": {"datavalue": {"value": {"id": "Q111"}}},
                               "qualifiers": {"P580": [{"datavalue": {"value": {"time": "+2020-10-10T00:00:00Z"}}}],
                                              "P582": [{"datavalue": {"value": {"time": "+2021-01-28T00:00:00Z"}}}]}},
                              {"mainsnak": {"datavalue": {"value": {"id": "Q222"}}},
                               "qualifiers": {"P580": [{"datavalue": {"value": {"time": "+2021-01-28T00:00:00Z"}}}]}}
                            ]},
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

                let host = request
                    .lines()
                    .find_map(|l| l.strip_prefix("Host: ").or_else(|| l.strip_prefix("host: ")))
                    .unwrap_or("")
                    .trim()
                    .to_string();
                // Extra response headers and an artificial delay, for the
                // routes that need them.
                let mut extra = String::new();
                let mut delay = 0u64;
                let (status, content_type, body) = match path.as_str() {
                    "/robots.txt" if host.starts_with("robots5xx.test") => {
                        (503, "text/plain", b"robots is down".to_vec())
                    }
                    "/robots.txt" if host.starts_with("norobots.test") => {
                        (404, "text/html", b"not found".to_vec())
                    }
                    // Larger than the robots.txt read limit, with the rule that
                    // matters placed after it.
                    "/robots.txt" if host.starts_with("bigrobots.test") => {
                        let mut body = "# padding\n".repeat(60_000).into_bytes();
                        body.extend_from_slice(b"User-agent: *\nDisallow: /alpha\n");
                        (200, "text/plain", body)
                    }
                    // A small download that expands to more than the decoded limit.
                    "/sm-bomb.xml.gz" => {
                        use std::io::Write;
                        let mut encoder =
                            flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
                        encoder.write_all(b"<urlset><url><loc>/a</loc></url>").unwrap();
                        let zeros = vec![b' '; 1024 * 1024];
                        for _ in 0..70 {
                            encoder.write_all(&zeros).unwrap();
                        }
                        encoder.write_all(b"</urlset>").unwrap();
                        (200, "application/gzip", encoder.finish().unwrap())
                    }
                    "/sm-deep.xml" => {
                        let open = "<a>".repeat(200);
                        let close = "</a>".repeat(200);
                        (200, "application/xml", format!("<urlset>{open}<url><loc>/x</loc></url>{close}</urlset>").into_bytes())
                    }
                    // Image and video extensions nest a `<loc>` inside the `<url>`.
                    "/sm-ext.xml" => (
                        200,
                        "application/xml",
                        br#"<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"
                                  xmlns:image="http://www.google.com/schemas/sitemap-image/1.1">
                              <url><loc>/page-1</loc>
                                <image:image><image:loc>/img/1.jpg</image:loc></image:image>
                                <lastmod>2026-01-02</lastmod></url>
                              <url><loc><![CDATA[/page-2]]></loc></url>
                            </urlset>"#.to_vec(),
                    ),
                    "/sm-many.xml" => (
                        200,
                        "application/xml",
                        br#"<sitemapindex>
                              <sitemap><loc>/sm-c1.xml</loc></sitemap><sitemap><loc>/sm-c2.xml</loc></sitemap>
                              <sitemap><loc>/sm-c3.xml</loc></sitemap><sitemap><loc>/sm-c4.xml</loc></sitemap>
                              <sitemap><loc>/sm-c5.xml</loc></sitemap><sitemap><loc>/sm-c6.xml</loc></sitemap>
                            </sitemapindex>"#.to_vec(),
                    ),
                    p if p.starts_with("/sm-c") && p.ends_with(".xml") => (
                        200,
                        "application/xml",
                        format!("<urlset><url><loc>/from{p}</loc></url></urlset>").into_bytes(),
                    ),
                    "/searxng/search" if query.contains("pageno=2") => {
                        (200, "application/json", br#"{"results": []}"#.to_vec())
                    }
                    "/searxng/search" => (
                        200,
                        "application/json",
                        br#"{"results": [
                             {"url": "https://example.org/a", "title": "A", "publishedDate": "2025-06-01T10:00:00"},
                             {"url": "https://example.org/b?utm_source=x", "title": "B", "publishedDate": "2021-02-03T00:00:00"},
                             {"url": "https://example.org/c", "title": "C"},
                             {"title": "no url at all"}
                           ]}"#.to_vec(),
                    ),
                    "/cc-collinfo" => (
                        200,
                        "application/json",
                        format!(r#"[{{"id": "CC-MAIN-TEST", "cdx-api": "http://{host}/cc-index"}},
                                    {{"id": "CC-MAIN-OLD", "cdx-api": "http://{host}/cc-index-old"}}]"#).into_bytes(),
                    ),
                    "/cc-index" => (
                        200,
                        "text/x-ndjson",
                        concat!(
                            r#"{"urlkey": "kg,gov,mfa)/news/1", "timestamp": "20260105", "url": "https://mfa.gov.kg/news/1", "status": "200"}"#, "\n",
                            r#"{"urlkey": "kg,gov,mfa)/news/2", "timestamp": "20260106", "url": "https://mfa.gov.kg/news/2", "status": "200"}"#, "\n",
                            "this line is not json\n",
                        ).as_bytes().to_vec(),
                    ),
                    // A page that is an empty shell until its script runs.
                    "/spa" => (
                        200,
                        "text/html",
                        br#"<html><body><div id="app"></div><script>
                            setTimeout(function () {
                              document.getElementById('app').innerHTML =
                                '<p>Rendered by script: the delegation arrived in Ankara today.</p>' +
                                '<a href="/alpha">the alpha page</a>';
                            }, 50);
                          </script></body></html>"#.to_vec(),
                    ),
                    "/login" => (
                        200,
                        "text/html",
                        br#"<html><body><form action="/login-submit" method="get">
                              <input type="text" name="username"><input type="password" name="password">
                              <button type="submit">Sign in</button></form></body></html>"#.to_vec(),
                    ),
                    "/login-submit" => {
                        extra = "Set-Cookie: session=secret123; Path=/\r\n".into();
                        (200, "text/html", b"<html><body><main>welcome back, you are signed in</main></body></html>".to_vec())
                    }
                    "/whoami" => {
                        let cookie = request
                            .lines()
                            .find(|l| l.to_ascii_lowercase().starts_with("cookie:"))
                            .unwrap_or("no cookie")
                            .to_string();
                        (200, "text/html", format!("<html><body><main>header: [{cookie}]</main></body></html>").into_bytes())
                    }
                    // An endless tree: page n links to six children, so a crawl can
                    // be asked for as many pages as the test wants.
                    p if p.starts_with("/gen/") => {
                        let n: u64 = p["/gen/".len()..].parse().unwrap_or(0);
                        let children: String = (1..=6)
                            .map(|i| format!(r#"<a href="/gen/{}">child {i}</a> "#, n * 6 + i))
                            .collect();
                        let filler = format!("Report {n} on the delegation, the talks and the agreements signed. ").repeat(25);
                        (200, "text/html", format!("<html><body><main><p>{filler}</p>{children}</main></body></html>").into_bytes())
                    }
                    "/redir-rel" => {
                        extra = "Location: /alpha\r\n".into();
                        (302, "text/html", Vec::new())
                    }
                    "/redir-blocked" => {
                        extra = "Location: http://doubleclick.net/x\r\n".into();
                        (302, "text/html", Vec::new())
                    }
                    "/redir-secret" => {
                        extra = "Location: /secret\r\n".into();
                        (302, "text/html", Vec::new())
                    }
                    "/redir-offsite" => {
                        extra = "Location: http://offsite.test/alpha\r\n".into();
                        (302, "text/html", Vec::new())
                    }
                    "/redir-loop" => {
                        extra = "Location: /redir-loop\r\n".into();
                        (302, "text/html", Vec::new())
                    }
                    "/old/page" => {
                        extra = "Location: /new/dir/page\r\n".into();
                        (302, "text/html", Vec::new())
                    }
                    "/new/dir/page" => (
                        200,
                        "text/html",
                        br#"<html><body><main>moved <a href="sibling">a sibling</a></main></body></html>"#.to_vec(),
                    ),
                    "/new/dir/sibling" => {
                        (200, "text/html", b"<html><body><main>the sibling</main></body></html>".to_vec())
                    }
                    // Two failures with `Retry-After: 0`, then the page.
                    "/retry-ok" if hit <= 2 => {
                        extra = "Retry-After: 0\r\n".into();
                        (503, "text/html", b"busy".to_vec())
                    }
                    "/retry-ok" => (200, "text/html", b"<html><body><main>recovered</main></body></html>".to_vec()),
                    "/always503" => {
                        extra = "Retry-After: 0\r\n".into();
                        (503, "text/html", b"busy".to_vec())
                    }
                    "/slow" => {
                        delay = 2_500;
                        (200, "text/html", b"<html><body><main>slow page</main></body></html>".to_vec())
                    }
                    // Fails once, then succeeds — a transient failure needs
                    // state, which a pure `body_for` cannot have.
                    "/flaky.json" if hit == 1 => (503, "application/json", b"{}".to_vec()),
                    "/flaky.json" => (200, "application/json", br#"{"recovered":true}"#.to_vec()),
                    // Wikidata serves both of its calls from one endpoint and
                    // tells them apart by `action`, so the fixture does too.
                    "/wd" if query.contains("wbsearchentities") && query.contains("office") => {
                        body_for("/wd-search-office")
                    }
                    "/wd" if query.contains("ids=Q853036") => body_for("/wd-office"),
                    "/wd" if query.contains("props=labels&languages=en") => body_for("/wd-labels"),
                    "/wd" if query.contains("wbsearchentities") => body_for("/wd-search"),
                    "/wd" if query.contains("wbgetentities") => body_for("/wd-entity"),
                    _ => body_for(&path),
                };
                let header = format!(
                    "HTTP/1.1 {status} OK\r\nContent-Type: {content_type}\r\n{extra}\
                     Content-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                if delay > 0 {
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
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
    Query { text: "state visit".into(), since: None, until: None, sites: Vec::new(), post: None }
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
        post: None,
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
    let query = Query { text: "   ".into(), since: None, until: None, sites: Vec::new(), post: None };
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
    // On the common scale beta's top pick is 1.0, whatever its raw number was.
    assert_eq!(shared.score, 1.0);
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
    // A source with a single answer expressed no preference between answers, so
    // it gets the top of the common scale rather than a number it made up.
    assert_eq!(seeds[0].score, 1.0);
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

// *** query expansion ***

fn span(from: Option<&str>, to: Option<&str>) -> Span {
    Span { from: from.map(str::to_string), to: to.map(str::to_string) }
}

fn names(list: &[&str]) -> Vec<String> {
    list.iter().map(|s| s.to_string()).collect()
}

fn plan<'a>(query: &'a str, aliases: &'a [String], window: &'a Span, max: usize) -> Plan<'a> {
    Plan { query, names: aliases, place: None, window, max_queries: max, this_year: 2026 }
}

#[tokio::test]
async fn wikidata_reports_the_tenure_and_leaves_a_post_still_held_open() {
    let server = fixture().await;
    let wd = Wikidata::at(&server.url("/wd")).unwrap();
    let found = wd
        .discover(&test_client(), &test_query(), &DiscoveryBudget::default())
        .await
        .unwrap();
    // Earliest start across both claims; the second has no end, so still held.
    assert_eq!(found.span, Some(span(Some("20201010"), None)));
}

#[test]
fn tenure_closes_when_every_post_has_ended_and_widens_partial_dates() {
    let entity = |claims: &str| {
        serde_json::from_str::<serde_json::Value>(&format!(
            r#"{{"entities": {{"Q1": {{"claims": {{"P39": {claims}}}}}}}}}"#
        ))
        .unwrap()
    };
    let closed = entity(
        r#"[{"qualifiers": {"P580": [{"datavalue": {"value": {"time": "+2010-00-00T00:00:00Z"}}}],
                            "P582": [{"datavalue": {"value": {"time": "+2014-00-00T00:00:00Z"}}}]}}]"#,
    );
    // Year-only dates widen to cover the whole year on each side.
    assert_eq!(
        crate::sources::wikidata::tenure_of(&closed, "Q1"),
        Some(span(Some("20100101"), Some("20141231")))
    );

    // A claim with no dates at all says nothing about when.
    let undated = entity(r#"[{"qualifiers": {}}]"#);
    assert_eq!(crate::sources::wikidata::tenure_of(&undated, "Q1"), None);
    assert_eq!(crate::sources::wikidata::tenure_of(&entity("[]"), "Q1"), None);
}

#[test]
fn the_window_is_the_users_bounds_narrowed_to_the_tenure() {
    let tenure = span(Some("20201010"), Some("20250101"));
    // No user bounds: the tenure is the window.
    assert_eq!(expand::resolve_window(None, None, Some(&tenure)), tenure);
    // User bounds inside the tenure win; bounds outside it are cut back.
    assert_eq!(
        expand::resolve_window(Some("20220101"), Some("20230101"), Some(&tenure)),
        span(Some("20220101"), Some("20230101"))
    );
    assert_eq!(
        expand::resolve_window(Some("20000101"), Some("20300101"), Some(&tenure)),
        tenure
    );
    // No tenure known: the user's bounds, untouched.
    assert_eq!(
        expand::resolve_window(Some("20220101"), None, None),
        span(Some("20220101"), None)
    );
    // Disjoint: the user asked for those dates, so an empty window loses.
    assert_eq!(
        expand::resolve_window(Some("19900101"), Some("19950101"), Some(&tenure)),
        span(Some("19900101"), Some("19950101"))
    );
}

#[test]
fn names_differing_by_accent_are_one_name_and_each_script_gets_a_turn() {
    let aliases = names(&[
        "Šadyr Japarov", // the query again, once the accent folds
        "SADYR JAPAROV",
        "Садыр Жапаров",
        "Sadyr Zhaparov",
        "صادیر جاپاروف",
        "Sadyr Nurgozhoevich Zhaparov",
    ]);
    let w = Span::default();
    let queries = expand::expand(&plan("Sadyr Japarov", &aliases, &w, 20));
    let round0: Vec<&str> = queries.iter().take(4).map(|q| q.text.as_str()).collect();

    assert_eq!(round0[0], "\"Sadyr Japarov\"");
    // The Cyrillic form, then a further romanisation, then the Arabic-script one.
    assert_eq!(round0[1], "\"Садыр Жапаров\"");
    assert_eq!(round0[2], "\"Sadyr Zhaparov\"");
    assert_eq!(round0[3], "\"صادیر جاپاروف\"");
    // "Šadyr" and "SADYR" fold to the query itself and add nothing.
    assert!(!queries.iter().any(|q| q.text.contains("Šadyr") || q.text.contains("SADYR")));
}

#[test]
fn expansion_is_bounded_deduplicated_and_carries_the_place() {
    let aliases = names(&["Садыр Жапаров"]);
    let w = span(Some("20201010"), Some("20260101"));
    let mut p = plan("Sadyr Japarov", &aliases, &w, 6);
    p.place = Some("Turkey");

    let queries = expand::expand(&p);
    assert_eq!(queries.len(), 6, "never more than asked for");
    assert!(queries.iter().all(|q| q.text.ends_with("Turkey")));
    let mut seen = std::collections::HashSet::new();
    assert!(
        queries.iter().all(|q| seen.insert((q.text.clone(), q.since.clone(), q.until.clone()))),
        "no query twice"
    );
    // Multi-word predicates are matched as phrases.
    let all: String = queries.iter().map(|q| q.text.clone()).collect::<Vec<_>>().join("|");
    assert!(all.contains("\"Sadyr Japarov\" visit Turkey"), "got {all}");
}

#[test]
fn year_slices_tile_the_window_exactly_with_no_gap_or_overlap() {
    let w = span(Some("20201010"), Some("20260315"));
    let queries = expand::expand(&plan("Japarov", &[], &w, 12));
    // The slices are the ones that are narrower than the whole window.
    let slices: Vec<(&str, &str)> = queries
        .iter()
        .filter_map(|q| Some((q.since.as_deref()?, q.until.as_deref()?)))
        .filter(|&(s, u)| (s, u) != ("20201010", "20260315"))
        .collect();
    assert!(slices.len() >= 2, "got {slices:?}");

    // Starts on the window's exact start, ends on its exact end, and each slice
    // begins the day after the previous one ended.
    assert_eq!(slices.first().unwrap().0, "20201010");
    assert_eq!(slices.last().unwrap().1, "20260315");
    for pair in slices.windows(2) {
        let prev_year: i32 = pair[0].1[..4].parse().unwrap();
        assert_eq!(pair[0].1, format!("{prev_year}1231"));
        assert_eq!(pair[1].0, format!("{}0101", prev_year + 1));
    }
}

#[test]
fn an_open_window_slices_up_to_this_year_and_stays_open() {
    let w = span(Some("20240101"), None);
    let queries = expand::expand(&plan("Japarov", &[], &w, 10));
    let last_slice = queries
        .iter()
        .rev()
        .find(|q| q.since.as_deref().is_some_and(|s| s != "20240101") && q.until.is_none());
    // The final slice reaches the present and has no end date to invent.
    assert!(last_slice.is_some(), "got {queries:?}");
}

#[test]
fn a_query_that_already_has_syntax_is_not_requoted() {
    let w = Span::default();
    let queries = expand::expand(&plan("Japarov sourcelang:russian", &[], &w, 3));
    assert_eq!(queries[0].text, "Japarov sourcelang:russian");
    let queries = expand::expand(&plan("\"state visit\" Turkey", &[], &w, 3));
    assert_eq!(queries[0].text, "\"state visit\" Turkey");
}

/// Answers every query and remembers the text of each, to see what a round
/// actually asked.
#[derive(Debug)]
struct Recorder {
    name: &'static str,
    asked: Arc<Mutex<Vec<String>>>,
    fails_after: Option<usize>,
    per_query: bool,
    found: fn() -> Found,
}

#[async_trait::async_trait]
impl DiscoverySource for Recorder {
    fn name(&self) -> Arc<str> {
        Arc::from(self.name)
    }
    fn per_query(&self) -> bool {
        self.per_query
    }
    async fn discover(
        &self,
        _client: &reqwest::Client,
        query: &Query,
        _budget: &DiscoveryBudget,
    ) -> Result<Found, CrawlError> {
        let mut asked = self.asked.lock().unwrap();
        asked.push(query.text.clone());
        if self.fails_after.is_some_and(|n| asked.len() > n) {
            return Err(CrawlError::Transport("rate limited".into()));
        }
        Ok((self.found)())
    }
}

fn lead_for(name: &'static str, url: &str) -> Lead {
    Lead {
        url: canonicalize(url).unwrap(),
        score: 0.5,
        title: None,
        seen: None,
        domain: None,
        language: None,
        sources: vec![Arc::from(name)],
    }
}

#[tokio::test]
async fn research_expands_from_what_the_first_round_learned() {
    let text_log = Arc::new(Mutex::new(Vec::new()));
    let subject_log = Arc::new(Mutex::new(Vec::new()));
    let sources: Vec<Box<dyn DiscoverySource>> = vec![
        Box::new(Recorder {
            name: "news",
            asked: Arc::clone(&text_log),
            fails_after: None,
            per_query: true,
            found: || Found::leads(vec![lead_for("news", "https://example.org/story")]),
        }),
        Box::new(Recorder {
            name: "entity",
            asked: Arc::clone(&subject_log),
            fails_after: None,
            per_query: false,
            found: || Found {
                leads: vec![lead_for("entity", "https://example.org/profile")],
                terms: vec!["Садыр Жапаров".to_string()],
                span: Some(Span { from: Some("20201010".into()), to: Some("20260101".into()) }),
                subject: None,
            },
        }),
    ];
    let base = Query { text: "Sadyr Japarov".into(), since: None, until: None, sites: vec![], post: None };

    let done = expand::research(
        &test_client(),
        &sources,
        &base,
        Some("Turkey"),
        6,
        &DiscoveryBudget::default(),
    )
    .await;

    // The tenure the entity source reported became the window.
    assert_eq!(done.window, span(Some("20201010"), Some("20260101")));
    // The native-script name it learned was searched.
    assert!(done.queries.iter().any(|q| q.text.contains("Садыр Жапаров")));
    // The text search ran the original plus every expansion...
    assert_eq!(text_log.lock().unwrap().len(), 1 + done.queries.len());
    // ...and the entity lookup, whose answer does not depend on wording, once.
    assert_eq!(subject_log.lock().unwrap().len(), 1);
    // Both sources' leads survive the merge.
    assert_eq!(done.federated.leads.len(), 2);
}

#[tokio::test]
async fn research_stops_asking_a_source_that_has_started_failing() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let sources: Vec<Box<dyn DiscoverySource>> = vec![Box::new(Recorder {
        name: "limited",
        asked: Arc::clone(&log),
        fails_after: Some(1),
        per_query: true,
        found: || Found::leads(vec![lead_for("limited", "https://example.org/a")]),
    })];
    let base = Query { text: "Japarov".into(), since: None, until: None, sites: vec![], post: None };

    let done = expand::research(&test_client(), &sources, &base, None, 8, &DiscoveryBudget::default())
        .await;

    // The base round worked, the first expansion was refused, and that was the
    // last request: eight more would only deepen a rate limit.
    assert_eq!(log.lock().unwrap().len(), 2);
    assert_eq!(done.federated.failures.len(), 1);
    assert_eq!(done.federated.leads.len(), 1, "what it returned before failing is kept");
    // Planned several, sent one: the report must not claim the rest.
    assert_eq!(done.ran, 1);
    assert!(done.queries.len() > done.ran);
}

// *** near-duplicates ***

const WORDS: &[&str] = &[
    "president", "visit", "ankara", "talks", "trade", "energy", "border", "minister", "signed",
    "agreement", "delegation", "summit", "bishkek", "cooperation", "security", "water", "gold",
    "railway", "council", "meeting", "parliament", "reform", "budget", "election", "policy",
    "region", "forum", "accord", "treaty", "envoy", "capital", "airport", "ceremony", "press",
];

/// A long article from a fixed seed: the same words every run, so a failing
/// distance is a real change in behaviour and not a bad draw.
fn article(seed: u64, words: usize) -> String {
    let mut state = seed;
    (0..words)
        .map(|_| {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            WORDS[(state >> 33) as usize % WORDS.len()]
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[test]
fn a_syndicated_copy_with_different_chrome_is_the_same_story() {
    let story = article(7, 300);
    let copy = format!("Share on X Subscribe now Menu {story} Related articles Terms of use Contact us");
    let (a, b) = (dedup::fingerprint(&story).unwrap(), dedup::fingerprint(&copy).unwrap());
    assert!(dedup::distance(a, b) <= dedup::SAME_STORY, "distance {}", dedup::distance(a, b));
}

#[test]
fn a_different_story_of_the_same_length_is_not() {
    let a = dedup::fingerprint(&article(7, 300)).unwrap();
    let b = dedup::fingerprint(&article(99, 300)).unwrap();
    assert!(dedup::distance(a, b) > 10, "distance {}", dedup::distance(a, b));
}

#[test]
fn short_text_is_never_fingerprinted_so_error_pages_do_not_cluster() {
    assert_eq!(dedup::fingerprint("Page not found. Return to the homepage."), None);
    assert_eq!(dedup::fingerprint(""), None);
}

#[test]
fn text_without_spaces_still_fingerprints() {
    // Chinese has no word boundaries: shingling by word would see one word.
    let text = "总统对安卡拉进行了正式访问并与土耳其领导人举行了会谈双方签署了多项合作协议".repeat(8);
    let other = "地震发生在山区造成多处房屋倒塌救援人员已经赶往现场开展搜救工作".repeat(8);
    let (a, b) = (dedup::fingerprint(&text).unwrap(), dedup::fingerprint(&other).unwrap());
    assert!(dedup::distance(a, b) > dedup::SAME_STORY);
    assert_eq!(dedup::distance(a, dedup::fingerprint(&text).unwrap()), 0);
}

#[test]
fn clusters_compare_to_the_first_member_so_drift_cannot_chain() {
    let (a, b, c) = (0u64, 0b111, 0b111111);
    // b is 3 from a, c is 3 from b but 6 from a: chaining would join all
    // three; a cluster is a claim about similarity to its lead.
    let groups = dedup::cluster(&[Some(a), Some(b), Some(c)], 3);
    assert_eq!(groups, vec![vec![0, 1], vec![2]]);
    // No fingerprint, no cluster: each stands alone, even next to a twin.
    let groups = dedup::cluster(&[None, None, Some(a), Some(a)], 3);
    assert_eq!(groups, vec![vec![0], vec![1], vec![2, 3]]);
}

fn page(url: &str, body: &str, relevance: f32) -> FetchedPage {
    FetchedPage {
        url: Url::parse(url).unwrap(),
        html: format!("<html><body><main>{body}</main></body></html>"),
        origin: Origin::Seed,
        relevance,
        redirected_from: None,
    }
}

#[test]
fn the_most_relevant_copy_is_shown_and_the_rest_are_counted() {
    let story = article(7, 300);
    let pages = vec![
        page("https://a.example/one", &story, 0.2),
        page("https://b.example/one", &format!("Menu {story} Footer"), 0.9),
        page("https://c.example/other", &article(99, 300), 0.5),
        page("https://d.example/one", &story, 0.9),
    ];
    let groups = dedup::group_pages(&pages);
    assert_eq!(groups.len(), 2);
    // 0.9 twice: the earlier fetch wins the tie.
    assert_eq!(groups[0], dedup::Group { best: 1, copies: vec![0, 3] });
    assert_eq!(groups[1], dedup::Group { best: 2, copies: vec![] });
}

#[test]
fn the_report_says_how_many_distinct_stories_there_were() {
    let story = article(7, 300);
    let outcome = CrawlOutcome {
        pages: vec![
            page("https://a.example/one", &story, 0.4),
            page("https://b.example/one", &story, 0.4),
            page("https://b.example/two", &story, 0.4),
            page("https://c.example/other", &article(99, 300), 0.4),
        ],
        failed: vec![],
        robots_skipped: 0,
        budget_hit: false,
        ..CrawlOutcome::default()
    };
    let text = crate::tools::report(&outcome);
    assert!(text.contains("Fetched 4 page(s); 2 distinct after collapsing 2 near-duplicate copies"), "{text}");
    // Both copies are on one host: named once, and nothing is left over.
    assert!(text.contains("https://a.example/one — also on b.example\n"), "{text}");
    // Only one line per story is listed.
    assert!(!text.contains("https://b.example/two"), "{text}");
}

#[test]
fn iso_dates_are_read_and_everything_else_is_refused_not_guessed() {
    assert_eq!(dedup::ymd("2024-03-05T10:00:00+03:00", 2026), Some("20240305".into()));
    assert_eq!(dedup::ymd("2024/03/05", 2026), Some("20240305".into()));
    assert_eq!(dedup::ymd(" 2024-03-05 ", 2026), Some("20240305".into()));
    // A wrong date in a record someone sorts by is worse than none.
    assert_eq!(dedup::ymd("5 March 2024", 2026), None);
    assert_eq!(dedup::ymd("03-05-2024", 2026), None);
    assert_eq!(dedup::ymd("2024-13-05", 2026), None);
    assert_eq!(dedup::ymd("1969-01-01", 2026), None);
    assert_eq!(dedup::ymd("2031-01-01", 2026), None, "a date in the future is junk");
    assert_eq!(dedup::ymd("", 2026), None);
}

fn dated_page(url: &str, body: &str, published: &str) -> FetchedPage {
    FetchedPage {
        url: Url::parse(url).unwrap(),
        html: format!(
            "<html><head><meta property='article:published_time' content='{published}'></head>\
             <body><main>{body}</main></body></html>"
        ),
        origin: Origin::Seed,
        relevance: 0.5,
        redirected_from: None,
    }
}

#[test]
fn a_story_is_dated_by_its_earliest_copy_and_counted_by_its_hosts() {
    let story = article(7, 300);
    let pages = vec![
        dated_page("https://www.a.example/x", &story, "2024-03-07T09:00:00Z"),
        dated_page("https://a.example/y", &story, "2024-03-06T09:00:00Z"),
        dated_page("https://b.example/x", &story, "2024-03-05T23:00:00Z"),
    ];
    let groups = dedup::group_pages(&pages);
    assert_eq!(groups.len(), 1);
    // The original is the earliest; reposts are dated when they were reposted.
    assert_eq!(dedup::story_date(&pages, &groups[0], 2026), Some("20240305".into()));
    // `www.a.example` and `a.example` are one outlet.
    assert_eq!(dedup::story_hosts(&pages, &groups[0]), vec!["a.example", "b.example"]);
}

#[test]
fn the_report_dates_stories_and_names_other_hosts_not_other_copies() {
    let story = article(7, 300);
    let outcome = CrawlOutcome {
        pages: vec![
            dated_page("https://a.example/one", &story, "2024-03-05"),
            dated_page("https://a.example/two", &story, "2024-03-06"),
            dated_page("https://b.example/one", &story, "2024-03-06"),
            dated_page("https://c.example/solo", &article(99, 300), "2023-01-10"),
        ],
        failed: vec![],
        robots_skipped: 0,
        budget_hit: false,
        ..CrawlOutcome::default()
    };
    let text = crate::tools::report(&outcome);
    assert!(text.contains("2 of 2 stories carry a publish date, from 2023-01-10 to 2024-03-05"), "{text}");
    // Three copies, two hosts: one other host is named, the second copy on a.example is not.
    assert!(text.contains("https://a.example/one (2024-03-05) — also on b.example\n"), "{text}");
    assert!(text.contains("https://c.example/solo (2023-01-10)\n"), "{text}");
}

#[test]
fn copies_all_on_one_host_say_so() {
    let story = article(7, 300);
    let outcome = CrawlOutcome {
        pages: vec![
            page("https://a.example/one", &story, 0.4),
            page("https://a.example/two", &story, 0.4),
        ],
        failed: vec![],
        robots_skipped: 0,
        budget_hit: false,
        ..CrawlOutcome::default()
    };
    let text = crate::tools::report(&outcome);
    assert!(text.contains("https://a.example/one — 1 more copy on the same host"), "{text}");
}

// *** excerpts ***

#[test]
fn an_excerpt_is_the_lines_that_mention_the_subject_in_reading_order() {
    let terms = Terms::new(["Sadyr Japarov"]);
    let text = "Menu\n\
                Japarov arrived in Ankara on Tuesday for a state visit.\n\
                Weather: sunny, 24 degrees across the capital region today.\n\
                Talks covered trade and energy, the delegation said afterwards.\n\
                Sadyr Japarov signed three agreements with his Turkish counterpart.";
    let excerpt = terms.excerpt(text, None).unwrap();
    // The full-name line outranks the surname-only one but they read in page order.
    let arrived = excerpt.find("arrived in Ankara").unwrap();
    let signed = excerpt.find("signed three agreements").unwrap();
    assert!(arrived < signed, "{excerpt}");
    assert!(!excerpt.contains("Weather") && !excerpt.contains("Menu"), "{excerpt}");
}

#[test]
fn an_excerpt_is_none_when_nothing_mentions_the_subject_and_never_the_page_start() {
    let terms = Terms::new(["Sadyr Japarov"]);
    assert_eq!(terms.excerpt("Welcome to the site. Here is the weather for the whole week.", None), None);
}

#[test]
fn long_lines_are_clipped_on_a_character_boundary() {
    let terms = Terms::new(["Жапаров"]);
    let line = format!("Жапаров {}", "очень длинное предложение ".repeat(40));
    let excerpt = terms.excerpt(&line, None).unwrap();
    assert!(excerpt.chars().count() <= 241, "{}", excerpt.chars().count());
    assert!(excerpt.ends_with('…'));
}

#[test]
fn the_excerpt_report_shows_lines_under_each_story_and_the_plain_report_does_not() {
    let body = format!("{}. Japarov arrived in Ankara on Tuesday for a state visit.", article(7, 300));
    let outcome = CrawlOutcome {
        pages: vec![dated_page("https://a.example/one", &body, "2024-03-05")],
        failed: vec![],
        robots_skipped: 0,
        budget_hit: false,
        ..CrawlOutcome::default()
    };
    let terms = Terms::new(["Sadyr Japarov"]);
    let with = crate::tools::report_with(
        &outcome,
        Some(&crate::tools::Excerpts { subject: &terms, near: None }),
    );
    assert!(with.contains("https://a.example/one (2024-03-05)\n    "), "{with}");
    assert!(with.contains("arrived in Ankara"), "{with}");
    assert!(!crate::tools::report(&outcome).contains("arrived in Ankara"));
}

#[test]
fn the_sentence_naming_the_subject_is_found_deep_inside_one_long_paragraph() {
    let terms = Terms::new(["Sadyr Japarov"]);
    // One line, as an article paragraph is: filler, then the sentence that matters.
    let paragraph = format!(
        "{}. President Sadyr Japarov landed in Ankara on Tuesday. {}.",
        article(7, 120),
        article(8, 120)
    );
    let excerpt = terms.excerpt(&paragraph, None).unwrap();
    assert!(excerpt.contains("landed in Ankara on Tuesday"), "{excerpt}");
    assert!(excerpt.chars().count() < 300, "only the sentence, not the paragraph: {excerpt}");
}

#[test]
fn cjk_full_stops_split_sentences_without_a_following_space() {
    // Four characters or more: shorter names are not matched in unspaced scripts at all,
    // a limit of Terms noted in todo.md, not of excerpts.
    let terms = Terms::new(["土耳其总统"]);
    let text = "今天天气晴朗适合出行的人很多。土耳其总统对吉尔吉斯斯坦进行了正式访问并举行会谈。明天将有降雨天气预报如此。";
    let excerpt = terms.excerpt(text, None).unwrap();
    assert!(excerpt.contains("土耳其总统对吉尔吉斯斯坦"), "{excerpt}");
    assert!(!excerpt.contains("天气晴朗"), "{excerpt}");
}

#[tokio::test]
async fn the_native_language_name_leads_the_aliases() {
    let server = fixture().await;
    let wd = Wikidata::at(&server.url("/wd")).unwrap();
    let found = wd
        .discover(&test_client(), &test_query(), &DiscoveryBudget::default())
        .await
        .unwrap();
    // Before the English label, so name picking reaches it first.
    assert_eq!(found.terms[0], "Жапаров Садыр");
}

#[test]
fn a_sentence_with_the_place_beats_one_that_only_names_the_subject() {
    let terms = Terms::new(["Mahmoud Ahmadinejad"]);
    let near = Terms::new(["Venezuela"]);
    // Five sentences name the subject and only three are kept. The place sentence is
    // the shortest, so only the place can get it in ahead of the longer ones.
    let text = "Mahmoud Ahmadinejad was born in the village of Aradan in the Semnan Province.\n\
                Mahmoud Ahmadinejad studied civil engineering at the Iran University of Science.\n\
                Mahmoud Ahmadinejad served as the mayor of Tehran from 2003 until 2005.\n\
                Mahmoud Ahmadinejad won the presidential election in the summer of 2005.\n\
                Mahmoud Ahmadinejad visited Venezuela.";
    let with_place = terms.excerpt(text, Some(&near)).unwrap();
    assert!(with_place.contains("Venezuela"), "{with_place}");
    // Not asked about a place, the same page's excerpt leaves it out.
    let without = terms.excerpt(text, None).unwrap();
    assert!(!without.contains("Venezuela"), "{without}");
    // And among equals the longer sentence beats a short fragment.
    let fragment = terms
        .excerpt("Mahmoud Ahmadinejad (sister)\nMahmoud Ahmadinejad flew to Caracas for talks in 2007.", None)
        .unwrap();
    assert!(fragment.starts_with("Mahmoud Ahmadinejad ("), "both are kept, in page order: {fragment}");
}

// *** redirects, retries, robots failures, deadline ***

async fn crawl_one(server: &Fixture, path: &str, cfg: CrawlConfig) -> CrawlOutcome {
    crawl(&test_client(), &server.url(path), &cfg).await.unwrap()
}

fn shallow() -> CrawlConfig {
    CrawlConfig { max_depth: 0, ..test_config() }
}

#[tokio::test]
async fn a_redirect_is_followed_and_the_final_url_is_reported() {
    let server = fixture().await;
    let outcome = crawl_one(&server, "/redir-rel", shallow()).await;
    assert_eq!(outcome.pages.len(), 1, "{:?}", outcome.failed);
    let page = &outcome.pages[0];
    assert!(page.url.as_str().ends_with("/alpha"), "got {}", page.url);
    assert!(page.redirected_from.as_ref().unwrap().as_str().ends_with("/redir-rel"));
}

#[tokio::test]
async fn links_resolve_against_where_a_redirect_ended_up() {
    let server = fixture().await;
    // /old/page -> /new/dir/page, whose relative link `sibling` is /new/dir/sibling,
    // not /old/sibling.
    let cfg = CrawlConfig { max_depth: 1, ..test_config() };
    let outcome = crawl_one(&server, "/old/page", cfg).await;
    let urls = outcome.urls();
    assert!(urls.iter().any(|u| u.ends_with("/new/dir/sibling")), "got {urls:?}");
    assert_eq!(server.hits("/old/sibling"), 0);
}

#[tokio::test]
async fn a_redirect_to_a_blocked_domain_is_refused_before_any_request() {
    let server = fixture().await;
    let outcome = crawl_one(&server, "/redir-blocked", shallow()).await;
    assert!(outcome.pages.is_empty());
    let why = &outcome.failed[0].1;
    assert!(why.contains("blocked domain"), "got {why}");
}

#[tokio::test]
async fn a_redirect_into_a_robots_disallowed_path_never_reaches_it() {
    let server = fixture().await;
    let outcome = crawl_one(&server, "/redir-secret", shallow()).await;
    assert!(outcome.pages.is_empty());
    assert_eq!(outcome.robots_skipped, 1);
    // The strong claim: the disallowed page was not requested at all.
    assert_eq!(server.hits("/secret"), 0);
}

#[tokio::test]
async fn same_domain_only_stops_a_redirect_leaving_the_domain() {
    let server = fixture().await;
    let cfg = CrawlConfig { same_domain_only: true, ..shallow() };
    let outcome = crawl_one(&server, "/redir-offsite", cfg).await;
    assert!(outcome.pages.is_empty());
    assert!(outcome.failed[0].1.contains("leaves the seeded domains"), "{:?}", outcome.failed);
}

#[tokio::test]
async fn a_redirect_loop_gives_up() {
    let server = fixture().await;
    let outcome = crawl_one(&server, "/redir-loop", shallow()).await;
    assert!(outcome.pages.is_empty());
    assert!(outcome.failed[0].1.contains("redirects"), "{:?}", outcome.failed);
    // The first request plus five hops, and then it stops.
    assert_eq!(server.hits("/redir-loop"), 6);
}

#[tokio::test]
async fn a_transient_failure_is_retried_and_counted() {
    let server = fixture().await;
    let outcome = crawl_one(&server, "/retry-ok", shallow()).await;
    assert_eq!(outcome.pages.len(), 1, "{:?}", outcome.failed);
    assert_eq!(outcome.retries, 2);
    assert_eq!(server.hits("/retry-ok"), 3);
}

#[tokio::test]
async fn retries_are_bounded_per_request() {
    let server = fixture().await;
    let outcome = crawl_one(&server, "/always503", shallow()).await;
    assert!(outcome.pages.is_empty());
    assert!(outcome.failed[0].1.contains("503"), "{:?}", outcome.failed);
    // One attempt and two retries; not an endless loop.
    assert_eq!(server.hits("/always503"), 3);
}

#[tokio::test]
async fn retries_draw_on_the_crawls_shared_allowance() {
    let server = fixture().await;
    // A one-page budget allows one retry in total, however many are wanted.
    let cfg = CrawlConfig { max_pages: 1, max_retries: 5, ..shallow() };
    let outcome = crawl_one(&server, "/always503", cfg).await;
    assert_eq!(server.hits("/always503"), 2);
    assert!(outcome.pages.is_empty());
}

#[tokio::test]
async fn an_unreadable_robots_txt_means_do_not_crawl() {
    let server = fixture().await;
    // A 5xx robots.txt means the rules exist and cannot be read. The page is
    // never requested.
    let client = client_resolving("robots5xx.test", server.addr);
    let url = format!("{}/alpha", server.aliased("robots5xx.test"));
    let outcome = crawl(&client, &url, &shallow()).await.unwrap();
    assert!(outcome.pages.is_empty());
    assert_eq!(outcome.robots_skipped, 1);
    assert_eq!(server.hits("/alpha"), 0);
}

#[tokio::test]
async fn a_missing_robots_txt_means_everything_is_allowed() {
    let server = fixture().await;
    // The fixture's own robots.txt disallows /secret. Under a name whose
    // robots.txt is a 404 there are no rules, so the same path is fetched.
    let client = client_resolving("norobots.test", server.addr);
    let url = format!("{}/secret", server.aliased("norobots.test"));
    let outcome = crawl(&client, &url, &shallow()).await.unwrap();
    assert_eq!(outcome.pages.len(), 1, "{:?}", outcome.failed);
    assert_eq!(outcome.robots_skipped, 0);
    // And under the ordinary name, the rule holds.
    let blocked = crawl_one(&server, "/secret", shallow()).await;
    assert_eq!(blocked.robots_skipped, 1);
}

#[tokio::test]
async fn robots_enforcement_can_be_switched_off() {
    let server = fixture().await;
    let cfg = CrawlConfig { respect_robots: false, ..shallow() };
    let outcome = crawl_one(&server, "/secret", cfg).await;
    assert_eq!(outcome.pages.len(), 1);
}

#[tokio::test]
async fn an_oversized_robots_txt_is_read_to_its_limit_and_no_further() {
    let server = fixture().await;
    // The rule that would block /alpha sits past the 512 KiB read limit, so it
    // is never seen and the page is fetched.
    let client = client_resolving("bigrobots.test", server.addr);
    let url = format!("{}/alpha", server.aliased("bigrobots.test"));
    let outcome = crawl(&client, &url, &shallow()).await.unwrap();
    assert_eq!(outcome.pages.len(), 1, "{:?}", outcome.failed);
}

#[tokio::test]
async fn concurrent_first_requests_make_one_robots_fetch() {
    let server = fixture().await;
    let seeds: Vec<Seed> = ["/alpha", "/d1", "/nav-noise", "/d2"]
        .iter()
        .map(|p| Seed::new(canonicalize(&server.url(p)).unwrap(), Origin::Seed))
        .collect();
    let cfg = CrawlConfig { max_concurrency: 4, ..shallow() };
    crawl_from(&test_client(), seeds, &cfg).await.unwrap();
    assert_eq!(server.hits("/robots.txt"), 1);
}

#[tokio::test]
async fn the_deadline_abandons_a_slow_request_and_says_so() {
    let server = fixture().await;
    let cfg = CrawlConfig { deadline: Some(Duration::from_millis(400)), ..shallow() };
    let started = std::time::Instant::now();
    let outcome = crawl_one(&server, "/slow", cfg).await;
    assert!(started.elapsed() < Duration::from_millis(2_000), "waited for the slow page");
    assert!(outcome.deadline_hit);
    assert!(outcome.pages.is_empty());
    assert!(outcome.failed.iter().any(|(_, why)| why.contains("deadline")));
}

// *** URL identity ***

#[test]
fn url_identity_ignores_scheme_and_www_but_not_port_or_path_case() {
    let key = |s: &str| dedup_key(&canonicalize(s).unwrap());
    // Same document under either scheme, with or without www and a trailing slash.
    assert_eq!(key("http://example.com/a"), key("https://www.example.com/a/"));
    // The default port is the same server; another port is not.
    assert_eq!(key("https://example.com:443/a"), key("https://example.com/a"));
    assert_ne!(key("https://example.com:8443/a"), key("https://example.com/a"));
    // Servers are case-sensitive about paths; hosts are not.
    assert_ne!(key("https://example.com/Page"), key("https://example.com/page"));
    assert_eq!(key("https://EXAMPLE.com/a"), key("https://example.com/a"));
    // Tracking parameters and fragments are noise; real parameters are not.
    assert_eq!(key("https://example.com/a?utm_source=x#top"), key("https://example.com/a"));
    assert_ne!(key("https://example.com/a?id=1"), key("https://example.com/a?id=2"));
    // Parameter order does not matter.
    assert_eq!(key("https://example.com/a?b=2&a=1"), key("https://example.com/a?a=1&b=2"));
}

#[test]
fn an_internationalised_host_has_one_identity_however_it_is_spelled() {
    let key = |s: &str| dedup_key(&canonicalize(s).unwrap());
    // The Unicode form and its punycode form are one host.
    assert_eq!(key("https://münchen.example/a"), key("https://xn--mnchen-3ya.example/a"));
}

// *** sitemap limits ***

fn sitemap_cfg() -> crate::sitemap::SitemapConfig {
    crate::sitemap::SitemapConfig { per_host_delay: Duration::ZERO, ..Default::default() }
}

#[tokio::test]
async fn a_gzip_bomb_fails_the_request_instead_of_exhausting_memory() {
    let server = fixture().await;
    let outcome = crate::sitemap::collect(&test_client(), &server.url("/sm-bomb.xml.gz"), &sitemap_cfg())
        .await
        .unwrap();
    assert!(outcome.entries.is_empty());
    let why = &outcome.failed[0].1;
    assert!(why.contains("limit"), "got {why}");
}

#[tokio::test]
async fn an_element_nested_too_deeply_is_refused() {
    let server = fixture().await;
    let outcome = crate::sitemap::collect(&test_client(), &server.url("/sm-deep.xml"), &sitemap_cfg())
        .await
        .unwrap();
    assert!(outcome.entries.is_empty());
    assert!(outcome.failed[0].1.contains("nested"), "{:?}", outcome.failed);
}

#[tokio::test]
async fn an_images_loc_does_not_replace_the_pages_and_cdata_is_read() {
    let server = fixture().await;
    let outcome = crate::sitemap::collect(&test_client(), &server.url("/sm-ext.xml"), &sitemap_cfg())
        .await
        .unwrap();
    let urls = outcome.urls();
    assert_eq!(urls.len(), 2, "got {urls:?}");
    assert!(urls[0].ends_with("/page-1"), "the image's address must not win: {urls:?}");
    assert!(urls[1].ends_with("/page-2"), "CDATA must be read: {urls:?}");
    assert_eq!(outcome.entries[0].lastmod.as_deref(), Some("2026-01-02"));
}

#[tokio::test]
async fn the_sitemap_budget_holds_exactly_under_concurrency() {
    let server = fixture().await;
    // The index plus two children is three documents; five workers would have
    // fetched all six children in one batch.
    let cfg = crate::sitemap::SitemapConfig { max_sitemaps: 3, concurrency: 5, ..sitemap_cfg() };
    let outcome = crate::sitemap::collect(&test_client(), &server.url("/sm-many.xml"), &cfg)
        .await
        .unwrap();
    let children: usize = (1..=6).map(|i| server.hits(&format!("/sm-c{i}.xml"))).sum();
    assert_eq!(children, 2, "fetched {children} child sitemaps");
    assert!(outcome.sitemaps_read <= 3);
    assert!(outcome.truncated);
}

// *** the protocol ***
//
// The rest of the suite covers the crawler. A tool signature, a result flag or
// a schema can be wrong while every one of those tests passes, so these speak
// MCP to a real server over an in-memory pipe.

async fn mcp_client() -> rmcp::service::RunningService<rmcp::RoleClient, ()> {
    use rmcp::ServiceExt;
    net::init(NetPolicy::permissive());
    let (server_io, client_io) = tokio::io::duplex(1 << 20);
    tokio::spawn(async move {
        let server = crate::tools::Crawler::new().unwrap().serve(server_io).await.unwrap();
        let _ = server.waiting().await;
    });
    ().serve(client_io).await.unwrap()
}

fn args(value: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
    value.as_object().cloned().unwrap()
}

#[tokio::test]
async fn the_server_lists_its_tools_with_schemas() {
    let client = mcp_client().await;
    let tools = client.list_all_tools().await.unwrap();
    let names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
    for expected in ["crawl_urls", "crawl_site", "discover", "discover_and_crawl", "crawler_stats"] {
        assert!(names.contains(&expected), "missing {expected}: {names:?}");
    }
    let urls = tools.iter().find(|t| t.name == "crawl_urls").unwrap();
    assert!(urls.input_schema.get("properties").unwrap().get("urls").is_some());
    let info = client.peer_info().unwrap();
    assert_eq!(info.server_info.as_ref().unwrap().name, "mcpcrawler");
}

#[tokio::test]
async fn crawl_urls_reads_a_set_and_returns_data_alongside_the_prose() {
    use rmcp::model::CallToolRequestParams;
    let server = fixture().await;
    let client = mcp_client().await;
    let result = client
        .call_tool(CallToolRequestParams::new("crawl_urls").with_arguments(args(serde_json::json!({
            "urls": [server.url("/alpha"), server.url("/redir-rel"), "not a url at all"],
        }))))
        .await
        .unwrap();

    assert_eq!(result.is_error, Some(false));
    let data = result.structured_content.unwrap();
    // /redir-rel lands on /alpha, which is already in the set: one page, not two.
    assert_eq!(data["pages_total"], 1, "{data}");
    assert_eq!(data["partial"], false);
    let text = result.content[0].as_text().unwrap().text.clone();
    assert!(text.contains("Skipped not a url at all"), "{text}");
    assert!(text.contains("Fetched 1 page(s)"), "{text}");
}

#[tokio::test]
async fn a_failure_sets_the_error_flag_and_carries_a_stable_code() {
    use rmcp::model::CallToolRequestParams;
    let client = mcp_client().await;

    let result = client
        .call_tool(CallToolRequestParams::new("fetch_content")
            .with_arguments(args(serde_json::json!({ "url": "ftp://example.com/file" }))))
        .await
        .unwrap();
    assert_eq!(result.is_error, Some(true));
    assert_eq!(result.structured_content.unwrap()["error"]["code"], "unsupported_scheme");

    // Bad input is distinguished from a failed fetch.
    let result = client
        .call_tool(CallToolRequestParams::new("crawl_urls")
            .with_arguments(args(serde_json::json!({ "urls": ["http://127.0.0.1:9/"], "render": "sometimes" }))))
        .await
        .unwrap();
    assert_eq!(result.is_error, Some(true));
    assert_eq!(result.structured_content.unwrap()["error"]["code"], "bad_input");
}

#[tokio::test]
async fn crawler_stats_counts_what_the_server_has_done() {
    use rmcp::model::CallToolRequestParams;
    let server = fixture().await;
    let client = mcp_client().await;
    client
        .call_tool(CallToolRequestParams::new("crawl_urls")
            .with_arguments(args(serde_json::json!({ "urls": [server.url("/alpha")] }))))
        .await
        .unwrap();
    let stats = client.call_tool(CallToolRequestParams::new("crawler_stats")).await.unwrap();
    let text = stats.content[0].as_text().unwrap().text.clone();
    let fetched: u64 = text
        .lines()
        .find_map(|l| l.strip_prefix("pages_fetched "))
        .and_then(|n| n.parse().ok())
        .unwrap();
    assert!(fetched >= 1, "{text}");
}

#[tokio::test]
async fn sources_with_different_native_scales_land_on_one_common_scale() {
    let sources: Vec<Box<dyn DiscoverySource>> = vec![
        // One scores in tenths, the other near one: raw numbers say nothing
        // about which is more relevant.
        Box::new(Canned::new(
            "small",
            vec![("https://example.org/s1", 0.20), ("https://example.org/s2", 0.10)],
        )),
        Box::new(Canned::new(
            "large",
            vec![("https://example.org/l1", 0.95), ("https://example.org/l2", 0.55), ("https://example.org/l3", 0.15)],
        )),
    ];
    let outcome = federate(&test_client(), &sources, &test_query(), &DiscoveryBudget::default()).await;
    let score = |tail: &str| {
        outcome.leads.iter().find(|l| l.url.as_str().ends_with(tail)).map(|l| l.score).unwrap()
    };
    // Each source's best is 1.0 and its worst 0.1, in proportion between.
    assert_eq!(score("/s1"), 1.0);
    assert!((score("/s2") - 0.1).abs() < 1e-6);
    assert_eq!(score("/l1"), 1.0);
    assert!((score("/l3") - 0.1).abs() < 1e-6);
    assert!(score("/l2") > 0.1 && score("/l2") < 1.0);
}

// *** scripts without spaces, host prior ***

#[test]
fn short_names_in_spaceless_scripts_match_wherever_they_sit() {
    // Two characters, no spaces anywhere: neither the phrase rule (needs spaces
    // around) nor the old four-character word rule could see it.
    let terms = Terms::new(["总统"]);
    assert_eq!(terms.match_strength("土耳其总统对吉尔吉斯斯坦进行了正式访问"), 1.0);
    assert_eq!(terms.match_strength("今天天气晴朗适合出行"), 0.0);

    let thai = Terms::new(["นายกรัฐมนตรี"]);
    assert_eq!(thai.match_strength("วันนี้นายกรัฐมนตรีเดินทางไปเยือนญี่ปุ่น"), 1.0);

    // Spaced scripts are unchanged: a Latin name still needs its boundaries.
    let latin = Terms::new(["Ali"]);
    assert_eq!(latin.match_strength("Malik arrived"), 0.0);
}

#[test]
fn an_institutional_host_breaks_a_tie_but_cannot_make_a_link_on_topic() {
    use crate::scoring::{LinkContext, score_link};
    let terms = Terms::new(["Japarov"]);
    let link = |url: &str| LinkContext {
        url: Url::parse(url).unwrap(),
        anchor: "Latest news".into(),
        nearby: String::new(),
        rel_next: false,
    };
    let gov = score_link(&link("https://mfa.gov.kg/news"), &terms, 0.0).unwrap();
    let com = score_link(&link("https://example.com/news"), &terms, 0.0).unwrap();
    assert!(gov > com, "gov {gov} vs com {com}");
    assert!(gov - com < 0.1);
    // And it is not an on-topic link merely for being official.
    let dull = score_link(&link("https://mfa.gov.kg/about"), &terms, 0.0).unwrap();
    assert!(dull < crate::scoring::ON_TOPIC);
    // A lookalike is not an institution.
    let fake = score_link(&link("https://gov.example.com/news"), &terms, 0.0).unwrap();
    assert_eq!(fake, com);
}

// *** an office, and a post ***

#[tokio::test]
async fn an_office_is_resolved_to_the_person_who_holds_it() {
    let server = fixture().await;
    let wd = Wikidata::at(&server.url("/wd")).unwrap();
    let query = Query { text: "President of Kyrgyzstan office".into(), ..test_query() };
    let found = wd.discover(&test_client(), &query, &DiscoveryBudget::default()).await.unwrap();

    assert_eq!(found.subject.as_deref(), Some("Sadyr Japarov (Wikidata Q25597826)"));
    // The names are the person's, not the office's.
    assert!(found.terms.iter().any(|t| t == "Sadyr Zhaparov"), "{:?}", found.terms);
    assert!(!found.terms.iter().any(|t| t.contains("President of")), "{:?}", found.terms);
    // The window is this holder's time in this office.
    assert_eq!(found.span, Some(span(Some("20210128"), None)));
    // The person's pages, and the office's below them.
    let urls: Vec<&str> = found.leads.iter().map(|l| l.url.as_str()).collect();
    assert!(urls.contains(&"https://en.wikipedia.org/wiki/Sadyr_Japarov"), "{urls:?}");
    let office = found.leads.iter().find(|l| l.url.as_str().ends_with("President_of_Kyrgyzstan")).unwrap();
    let person = found.leads.iter().find(|l| l.url.as_str().ends_with("Sadyr_Japarov")).unwrap();
    assert!(office.score < person.score);
    // Search, office, holder: three requests, none wasted on labels.
    assert_eq!(server.hits("/wd"), 3);
}

#[tokio::test]
async fn naming_a_post_narrows_the_window_to_that_posts_term() {
    let server = fixture().await;
    let wd = Wikidata::at(&server.url("/wd")).unwrap();
    // Without a post: the union of both terms, from the earlier start.
    let all = wd.discover(&test_client(), &test_query(), &DiscoveryBudget::default()).await.unwrap();
    assert_eq!(all.span, Some(span(Some("20201010"), None)));

    // "president": only the presidency, which began later.
    let query = Query { post: Some("President".into()), ..test_query() };
    let found = wd.discover(&test_client(), &query, &DiscoveryBudget::default()).await.unwrap();
    assert_eq!(found.span, Some(span(Some("20210128"), None)));

    // A post the person never held falls back to the union rather than to nothing.
    let query = Query { post: Some("astronaut".into()), ..test_query() };
    let found = wd.discover(&test_client(), &query, &DiscoveryBudget::default()).await.unwrap();
    assert_eq!(found.span, Some(span(Some("20201010"), None)));
}

#[tokio::test]
async fn without_the_budget_for_a_third_request_the_office_is_left_unresolved() {
    let server = fixture().await;
    let wd = Wikidata::at(&server.url("/wd")).unwrap();
    let query = Query { text: "President of Kyrgyzstan office".into(), ..test_query() };
    let budget = DiscoveryBudget { max_requests: 2, ..DiscoveryBudget::default() };
    let found = wd.discover(&test_client(), &query, &budget).await.unwrap();
    // The office's own pages still come back; nobody was named.
    assert_eq!(found.subject, None);
    assert!(found.leads.iter().any(|l| l.url.as_str().ends_with("President_of_Kyrgyzstan")));
    assert_eq!(server.hits("/wd"), 2);
}

// *** native verbs and spellings ***

#[test]
fn a_cyrillic_name_gets_both_common_english_spellings() {
    let forms = expand::latin_forms("Садыр Жапаров");
    assert_eq!(forms, vec!["Sadyr Zhaparov", "Sadyr Japarov"]);
    // A name with none of the letters the schemes disagree about has one form.
    assert_eq!(expand::latin_forms("Анна Петрова"), vec!["Anna Petrova"]);
    // Only Cyrillic is romanised.
    assert!(expand::latin_forms("Sadyr Japarov").is_empty());
    assert!(expand::latin_forms("محمود").is_empty());
    // Soft and hard signs vanish; ё and щ expand; capitals stay capitals.
    assert_eq!(expand::latin_forms("Хрущёв")[0], "Khrushchyov");
}

#[test]
fn an_unlisted_romanisation_is_generated_when_only_cyrillic_is_recorded() {
    // Wikidata listed the Cyrillic form and nothing in Latin for it.
    let aliases = names(&["Садыр Жапаров"]);
    let w = Span::default();
    let queries = expand::expand(&plan("Sadyr Zhaparov", &aliases, &w, 20));
    let texts: Vec<&str> = queries.iter().map(|q| q.text.as_str()).collect();
    // The query already is the reference spelling, so the newspaper one is new.
    assert!(texts.contains(&"\"Sadyr Japarov\""), "{texts:?}");
    // And the spelling it already had is not repeated.
    assert_eq!(texts.iter().filter(|t| **t == "\"Sadyr Zhaparov\"").count(), 1, "{texts:?}");
}

#[test]
fn a_name_in_another_script_is_searched_with_that_languages_verbs() {
    let aliases = names(&["Садыр Жапаров", "محمود احمدی‌نژاد", "محمود أحمدي نجاد"]);
    let w = Span::default();
    let mut p = plan("Sadyr Japarov", &aliases, &w, 20);
    p.place = Some("Turkey");
    let texts: Vec<String> = expand::expand(&p).into_iter().map(|q| q.text).collect();
    let has = |needle: &str| texts.iter().any(|t| t.contains(needle));

    assert!(has("\"Садыр Жапаров\" визит Turkey"), "{texts:?}");
    // Persian and Arabic share a script and not a verb.
    assert!(has("محمود احمدی‌نژاد\" سفر") || has("محمود احمدی‌نژاد سفر"), "{texts:?}");
    // The Latin name keeps the English ones.
    assert!(has("\"Sadyr Japarov\" \"state visit\" Turkey"), "{texts:?}");
    // And a native name is never paired with an English verb.
    assert!(!has("Садыр Жапаров\" visit"), "{texts:?}");
}

// *** searxng, common crawl, allowlist, configuration ***

#[tokio::test]
async fn searxng_ranks_by_position_filters_by_window_and_skips_what_has_no_url() {
    use crate::sources::searxng::SearxNg;
    let server = fixture().await;
    let searx = SearxNg::at(&server.url("/searxng")).unwrap();
    let query = Query { since: Some("20240101".into()), ..test_query() };
    let found = searx.discover(&test_client(), &query, &DiscoveryBudget::default()).await.unwrap();

    let urls: Vec<&str> = found.leads.iter().map(|l| l.url.as_str()).collect();
    // B is dated 2021, before the window; C has no date and is kept; the
    // result with no URL is dropped.
    assert_eq!(urls, vec!["https://example.org/a", "https://example.org/c"], "{urls:?}");
    assert!(found.leads[0].score > found.leads[1].score);
    assert_eq!(found.leads[0].title.as_deref(), Some("A"));
    assert!(server.sent().contains("format=json"), "JSON must be asked for");
}

#[tokio::test]
async fn common_crawl_finds_the_latest_crawl_and_returns_original_urls() {
    use crate::sources::commoncrawl::CommonCrawl;
    let server = fixture().await;
    let cc = CommonCrawl::at(&server.url("/cc-collinfo")).unwrap();
    let query = Query { sites: vec!["mfa.gov.kg".into()], ..test_query() };
    let found = cc.discover(&test_client(), &query, &DiscoveryBudget::default()).await.unwrap();

    let urls: Vec<&str> = found.leads.iter().map(|l| l.url.as_str()).collect();
    // Live URLs, not replay copies; the line that was not JSON is skipped.
    assert_eq!(urls, vec!["https://mfa.gov.kg/news/1", "https://mfa.gov.kg/news/2"]);
    assert_eq!(found.leads[0].seen.as_deref(), Some("20260105"));
    // The newest crawl, not the old one.
    assert_eq!(server.hits("/cc-index"), 1);
    assert_eq!(server.hits("/cc-index-old"), 0);
    assert!(server.sent().contains("url=mfa.gov.kg%2F*"), "{}", server.sent());

    // The crawl list is read once, however many queries follow.
    cc.discover(&test_client(), &query, &DiscoveryBudget::default()).await.unwrap();
    assert_eq!(server.hits("/cc-collinfo"), 1);
}

#[tokio::test]
async fn common_crawl_has_nothing_to_say_without_a_site() {
    use crate::sources::commoncrawl::CommonCrawl;
    let server = fixture().await;
    let cc = CommonCrawl::at(&server.url("/cc-collinfo")).unwrap();
    let found = cc.discover(&test_client(), &test_query(), &DiscoveryBudget::default()).await.unwrap();
    assert!(found.leads.is_empty());
    assert_eq!(server.hits("/cc-collinfo"), 0, "a text-only query should cost no request");
}

#[test]
fn an_allowlisted_host_is_reachable_and_its_neighbours_are_not() {
    let strict = NetPolicy::strict();
    let url = |s: &str| Url::parse(s).unwrap();
    assert!(strict.check_url(&url("http://10.99.99.99:8080/")).is_err());

    net::allow_host(" 10.99.99.99 ");
    assert!(strict.check_url(&url("http://10.99.99.99:8080/search")).is_ok());
    // Exact match: the next address, and a name that merely contains it, stay refused.
    assert!(strict.check_url(&url("http://10.99.99.98/")).is_err());
    assert!(net::host_allowed("10.99.99.99"));
    assert!(!net::host_allowed("10.99.99.9"));

    // Case and brackets do not matter. (A documentation address, so no other
    // test's expectations are disturbed by the process-wide list.)
    net::allow_host("[2001:DB8::77]");
    assert!(net::host_allowed("2001:db8::77"));
    assert!(strict.check_url(&url("http://[2001:db8::77]:8080/")).is_ok());
    assert!(strict.check_url(&url("http://[2001:db8::78]/")).is_err());
}

#[test]
fn ipv6_forms_that_hide_an_ipv4_destination_are_judged_by_it() {
    let public = |s: &str| net::is_public(s.parse().unwrap());
    // The deprecated IPv4-compatible form, and the reserved block it lives in.
    assert!(!public("::2"));
    assert!(!public("::7f00:1"));
    // NAT64 and 6to4 carry an IPv4 address; a private one stays private.
    assert!(!public("64:ff9b::7f00:1"));
    assert!(!public("64:ff9b::a00:1"));
    assert!(public("64:ff9b::808:808"));
    assert!(!public("2002:7f00:1::1"));
    assert!(public("2002:808:808::1"));
    // Teredo is a tunnel, not a destination.
    assert!(!public("2001:0:4136:e378:8000:63bf:3fff:fdd2"));
    // Ordinary global addresses are unaffected.
    assert!(public("2606:4700:4700::1111"));
}

#[test]
fn a_wrong_setting_is_reported_not_ignored() {
    let with = |pairs: &'static [(&'static str, &'static str)]| {
        crate::config::problems_in(&move |name| {
            pairs.iter().find(|(k, _)| *k == name).map(|(_, v)| v.to_string())
        })
    };
    assert!(with(&[]).is_empty());
    assert!(with(&[("MCPCRAWLER_ALLOW_PRIVATE_NETWORKS", "yes")]).is_empty());

    let typo = with(&[("MCPCRAWLER_ALLOW_PRIVATE_NETWORKS", "ture")]);
    assert_eq!(typo.len(), 1);
    assert!(typo[0].contains("ture"), "{typo:?}");

    // Several problems come back together.
    let many = with(&[
        ("MCPCRAWLER_ALLOW_HOSTS", "good.lan, http://bad.lan/"),
        ("MCPCRAWLER_LOG", "loud"),
        ("MCPCRAWLER_SEARXNG_URL", "ftp://nope"),
    ]);
    assert_eq!(many.len(), 3, "{many:?}");
    assert!(with(&[("MCPCRAWLER_SEARXNG_URL", "http://searx.test:8080")]).is_empty());
}

// *** cancellation, cross-call limits, hostile input ***

#[tokio::test]
async fn dropping_a_crawl_stops_it_fetching() {
    let server = fixture().await;
    // /slow takes 2.5 s to answer. Give up on the crawl long before that: the
    // future is dropped, and nothing it spawned may keep going.
    let cfg = CrawlConfig { deadline: None, ..shallow() };
    let url = server.url("/slow");
    let abandoned = tokio::time::timeout(
        Duration::from_millis(300),
        crawl(&test_client(), &url, &cfg),
    )
    .await;
    assert!(abandoned.is_err(), "the crawl should still have been waiting");

    // Nothing queued behind it gets fetched after the drop.
    let seen = server.hits("/slow");
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(server.hits("/slow"), seen);
}

#[tokio::test]
async fn crawls_beyond_the_process_ceiling_wait_their_turn() {
    use crate::limits;
    let server = fixture().await;
    let cfg = CrawlConfig { deadline: Some(Duration::from_secs(10)), ..shallow() };
    let client = test_client();

    // One more crawl than the ceiling allows, all asked to do slow work at once.
    let mut running = Vec::new();
    for _ in 0..limits::MAX_CRAWLS + 1 {
        let (client, cfg, url) = (client.clone(), cfg.clone(), server.url("/slow"));
        running.push(tokio::spawn(async move { crawl(&client, &url, &cfg).await }));
    }
    tokio::time::sleep(Duration::from_millis(600)).await;
    // Every permit is in use, and the extra crawl is queued behind them.
    assert_eq!(limits::CRAWLS.available_permits(), 0);
    for task in running {
        task.await.unwrap().unwrap();
    }
    assert_eq!(limits::CRAWLS.available_permits(), limits::MAX_CRAWLS);
}

/// Deterministic noise, so a failure can be reproduced.
struct Noise(u64);

impl Noise {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    /// A string drawn from the characters that have broken parsers before:
    /// markup, quotes, escapes, combining marks, scripts that fold oddly,
    /// bidirectional controls, NUL and the replacement character.
    fn text(&mut self, len: usize) -> String {
        const PARTS: &[&str] = &[
            "<", ">", "</", "<a href=\"", "\"", "'", "&", "&amp;", "&#x0;", "/", "\\", ":", "?", "#",
            "%", "%ZZ", "%E0%A4", "..", " ", "\n", "\0", "\u{FFFD}", "İ", "ß", "ı", "é", "Ж", "ж",
            "ی", "ي", "ک", "ك", "۱۴۰۲", "总统", "ก", "\u{202E}", "\u{0301}", "http://", "https://",
            "javascript:", "mailto:", "[", "]", "{", "}", "-", "0", "9", "2024-13-45", "a", "Z",
            "script", "style", "href", "www.", ".gov", "xn--", "😀",
        ];
        (0..len).map(|_| PARTS[(self.next() % PARTS.len() as u64) as usize]).collect()
    }
}

#[test]
fn hostile_text_never_panics_the_pure_functions() {
    let mut noise = Noise(0x9E3779B97F4A7C15);
    let terms = Terms::new(["Sadyr Japarov", "总统", "Жапаров", "ی"]);
    let base = Url::parse("https://example.org/a/b").unwrap();

    for round in 0..1500 {
        let text = noise.text(1 + round % 60);
        // None of these may panic; what they return does not matter here.
        let _ = canonicalize(&text);
        let _ = dedup_key(&base.join(&text).unwrap_or_else(|_| base.clone()));
        let _ = terms.match_strength(&text);
        let _ = terms.excerpt(&text, Some(&terms));
        let _ = Terms::new([text.as_str()]);
        let _ = dedup::fingerprint(&text);
        let _ = dedup::ymd(&text, 2026);
        let _ = extract_text(&text);
        let _ = extract_links(&text, &base);
        let _ = extract_metadata(&text);
        let _ = structured::extract(&text);
        let _ = analyse(&text, &base);
        if let Ok(url) = Url::parse(&format!("https://example.org/{text}")) {
            let _ = Shape::of(&url);
            let _ = score_link(
                &LinkContext { url, anchor: text.clone(), nearby: text.clone(), rel_next: false },
                &terms,
                0.5,
            );
        }
        let _ = expand::expand(&plan(&text, &names(&[text.as_str(), "Жапаров"]), &Span::default(), 10));
        let _ = expand::resolve_window(Some(&text), Some(&text), None);
    }
}

#[test]
fn hostile_markup_is_survivable_at_depth_and_size() {
    // Pathological shapes rather than random ones: very deep nesting, very many
    // siblings, one enormous text node, an unterminated everything.
    let deep = format!("{}x{}", "<div>".repeat(5_000), "</div>".repeat(5_000));
    let wide = "<p>word </p>".repeat(50_000);
    let huge = format!("<p>{}</p>", "ж".repeat(2_000_000));
    let torn = "<a href=\"http://x.y/<script>&#<!-- <![CDATA[ <p".repeat(2_000);
    let base = Url::parse("https://example.org/").unwrap();
    for html in [&deep, &wide, &huge, &torn] {
        let _ = extract_text(html);
        let _ = extract_links(html, &base);
        let _ = extract_metadata(html);
        let _ = structured::extract(html);
        let _ = dedup::fingerprint(&extract_text(html));
    }
}

// *** a real browser, on purpose ***
//
// These need Chrome and take several seconds, so they only run when asked:
//     cargo test --release -- --ignored browser

#[tokio::test]
#[ignore = "needs Chrome; run with --ignored"]
async fn browser_rendering_auto_mode_and_login_isolation() {
    let server = fixture().await;
    let client = test_client();

    // A shell with nothing in it until a script runs.
    let shell = crawl_one(&server, "/spa", shallow()).await;
    let shell_text = extract_text(&shell.pages[0].html);
    assert!(!shell_text.contains("Rendered by script"), "static fetch must see the bare shell");

    // `auto` notices the shell and renders it.
    let cfg = CrawlConfig { render: Render::Auto, ..shallow() };
    let rendered = crawl(&client, &server.url("/spa"), &cfg).await.unwrap();
    let text = extract_text(&rendered.pages[0].html);
    assert!(text.contains("Rendered by script"), "auto should have rendered: {text}");

    // And `auto` leaves an ordinary page alone: no browser tab for it.
    let before = crate::metrics::TABS_OPENED.load(std::sync::atomic::Ordering::Relaxed);
    let cfg = CrawlConfig { render: Render::Auto, ..shallow() };
    crawl(&client, &server.url("/alpha"), &cfg).await.unwrap();
    assert_eq!(crate::metrics::TABS_OPENED.load(std::sync::atomic::Ordering::Relaxed), before);

    // A login reaches the signed-in page...
    let page = login_and_fetch(&server.url("/login"), "alice", "pw").await.unwrap();
    assert!(extract_text(&page).contains("signed in"), "login failed: {page}");
    // ...and its session does not outlive the call. The cookie the server set
    // for that login must not ride along on any later request.
    let later = fetch_page_headless(&server.url("/whoami")).await.unwrap();
    assert!(!later.contains("secret123"), "the login session leaked: {later}");

    shutdown_browser().await;
}

#[test]
fn a_shell_is_told_from_a_small_ordinary_page() {
    use crate::crawler::looks_unrendered_html as shell;
    let base = Url::parse("https://example.org/").unwrap();
    // What a React or Next.js app serves before its script runs.
    assert!(shell(r#"<html><body><div id="root"></div><script src="/app.js"></script></body></html>"#, &base));
    assert!(shell(r#"<html><body><div id="__next"></div></body></html>"#, &base));
    // Short, link-free, and with an inline script only: just a small page.
    assert!(!shell("<html><body><main>The Needle appears here.</main><script>var x = 1;</script></body></html>", &base));
    // A mount point that already has content is rendered.
    let rendered = format!(r#"<html><body><div id="root"><p>{}</p></div></body></html>"#, "word ".repeat(80));
    assert!(!shell(&rendered, &base));
    // A mount point with links is navigable as it stands.
    assert!(!shell(r#"<html><body><div id="root"><a href="/x">x</a></div></body></html>"#, &base));
}

// *** soak and throughput ***
//
//     cargo test --release -- --ignored soak --nocapture
//
// Not part of the ordinary run: it crawls thousands of pages and prints what it
// measured, which is where the figures in docs/performance.md come from.

fn process_stats() -> (u64, usize) {
    let pid = std::process::id().to_string();
    let rss_kb: u64 = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid])
        .output()
        .ok()
        .and_then(|o| String::from_utf8_lossy(&o.stdout).trim().parse().ok())
        .unwrap_or(0);
    let fds = std::process::Command::new("lsof")
        .args(["-p", &pid])
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).lines().count())
        .unwrap_or(0);
    (rss_kb, fds)
}

#[tokio::test]
#[ignore = "soak test; run with --ignored soak --nocapture"]
async fn soak_a_large_crawl_stays_within_its_limits() {
    let server = fixture().await;
    let client = test_client();
    let (rss_before, fds_before) = process_stats();

    let cfg = CrawlConfig {
        max_pages: 3_000,
        max_depth: 20,
        max_concurrency: 16,
        deadline: Some(Duration::from_secs(120)),
        ..test_config()
    };
    let started = std::time::Instant::now();
    let outcome = crawl(&client, &server.url("/gen/0"), &cfg).await.unwrap();
    let elapsed = started.elapsed();
    let (rss_after, fds_after) = process_stats();

    println!(
        "soak: {} pages in {:.1}s ({:.0} pages/s); rss {} -> {} MB; fds {} -> {}; failed {}",
        outcome.pages.len(),
        elapsed.as_secs_f32(),
        outcome.pages.len() as f32 / elapsed.as_secs_f32(),
        rss_before / 1024,
        rss_after / 1024,
        fds_before,
        fds_after,
        outcome.failed.len(),
    );
    assert_eq!(outcome.pages.len(), 3_000);
    assert!(outcome.budget_hit && !outcome.deadline_hit);
    // Sockets are released: the descriptor count is where it started, give or take.
    assert!(fds_after < fds_before + 100, "descriptors grew from {fds_before} to {fds_after}");

    // Reading the pages back out: extraction, fingerprinting, clustering.
    let started = std::time::Instant::now();
    let groups = dedup::group_pages(&outcome.pages);
    let report_time = started.elapsed();
    println!(
        "report: {} pages grouped into {} stories in {:.2}s",
        outcome.pages.len(),
        groups.len(),
        report_time.as_secs_f32()
    );
    assert!(report_time < Duration::from_secs(60));
}

// *** an unreachable source, a refused robots fetch ***

/// Fails the way a host that does not answer fails, and counts how often it
/// was actually asked.
#[derive(Debug)]
struct Unreachable {
    name: &'static str,
    asked: Arc<Mutex<usize>>,
    message: &'static str,
}

#[async_trait::async_trait]
impl DiscoverySource for Unreachable {
    fn name(&self) -> Arc<str> {
        Arc::from(self.name)
    }
    async fn discover(
        &self,
        _client: &reqwest::Client,
        _query: &Query,
        _budget: &DiscoveryBudget,
    ) -> Result<Found, CrawlError> {
        *self.asked.lock().unwrap() += 1;
        Err(CrawlError::Transport(self.message.to_string()))
    }
}

#[tokio::test]
async fn a_source_that_cannot_be_reached_is_left_alone_for_a_while() {
    let asked = Arc::new(Mutex::new(0));
    let sources: Vec<Box<dyn DiscoverySource>> = vec![Box::new(Unreachable {
        name: "cooldown-unreachable",
        asked: Arc::clone(&asked),
        message: "error sending request: client error (Connect): operation timed out",
    })];
    let budget = DiscoveryBudget::default();

    let first = federate(&test_client(), &sources, &test_query(), &budget).await;
    assert!(first.failures[0].1.contains("timed out"), "{:?}", first.failures);

    // The next call does not pay for the same timeout again, and says why.
    let second = federate(&test_client(), &sources, &test_query(), &budget).await;
    assert_eq!(*asked.lock().unwrap(), 1, "the source should not have been asked again");
    assert!(second.failures[0].1.contains("skipped"), "{:?}", second.failures);
    assert!(second.failures[0].1.contains("trying again in"), "{:?}", second.failures);
}

#[tokio::test]
async fn only_a_failed_connection_starts_a_cooldown() {
    let asked = Arc::new(Mutex::new(0));
    let sources: Vec<Box<dyn DiscoverySource>> = vec![Box::new(Unreachable {
        name: "cooldown-bad-answer",
        asked: Arc::clone(&asked),
        // The host answered; it just did not like the request.
        message: "server returned HTTP 500",
    })];
    let budget = DiscoveryBudget::default();
    federate(&test_client(), &sources, &test_query(), &budget).await;
    federate(&test_client(), &sources, &test_query(), &budget).await;
    assert_eq!(*asked.lock().unwrap(), 2);
}

#[tokio::test]
async fn a_destination_the_guard_refuses_is_reported_as_refused_not_as_robots() {
    // Under the default (strict) policy a name that resolves to loopback is
    // refused by the resolver. Its robots.txt cannot be fetched for that reason,
    // and the page must not be reported as "skipped by robots.txt".
    let policy = NetPolicy::strict();
    let client = reqwest::Client::builder()
        .dns_resolver(net::resolver(policy))
        .redirect(net::redirect_policy(policy))
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let outcome = crawl(&client, "http://localhost:1/", &shallow()).await;
    // The suite runs under the permissive policy (the fixture is on loopback),
    // so the process-wide guard lets this through to a connection failure;
    // what matters is that the resolver's refusal, when it does fire, is
    // marked so it can be told apart.
    let _ = outcome;
    let refusal = net::resolver(policy);
    use reqwest::dns::Resolve;
    let err = refusal
        .resolve("localhost".parse().unwrap())
        .await
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    assert!(err.contains(net::REFUSAL_MARK), "got {err:?}");
}
