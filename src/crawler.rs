use chromiumoxide::{Browser, BrowserConfig};
use futures::StreamExt;
use futures::future::join_all;
use reqwest::Client;
use scraper::{Html, Node, Selector};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};
use texting_robots::Robot;
use url::Url;

pub const USER_AGENT: &str = "mcpcrawler/0.1";

const BLOCKED_DOMAINS: &[&str] = &[
    "google-analytics.com",
    "doubleclick.net",
    "amazon-adsystem.com",
    "googlesyndication.com",
    "googletagmanager.com",
    "ads.yahoo.com",
    "scorecardresearch.com",
    "outbrain.com",
];

/// Query parameters that never change what a page says, only who gets credit for
/// the click. Stripped so `/p?utm_source=x` and `/p` dedup to one entry.
const TRACKING_PARAMS: &[&str] = &[
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "utm_id",
    "gclid", "fbclid", "msclkid", "dclid", "yclid", "igshid", "mc_cid", "mc_eid",
    "ref_src", "_ga", "_gl", "spm", "scm",
];

/// Tags whose text is never page content.
const SKIP_TAGS: &[&str] = &[
    "script", "style", "noscript", "template", "svg", "canvas", "iframe",
    "nav", "header", "footer", "aside", "form", "button", "select",
];

/// Tags that should produce a line break when flattening to text.
const BLOCK_TAGS: &[&str] = &[
    "p", "div", "section", "article", "li", "tr", "br", "hr",
    "h1", "h2", "h3", "h4", "h5", "h6", "blockquote", "pre", "td", "th",
];

// *** errors ***

#[derive(Debug)]
pub enum CrawlError {
    BadUrl(String),
    UnsupportedScheme(String),
    Transport(String),
    Status(u16),
    ContentType(String),
    TooLarge(usize),
    Browser(String),
}

impl fmt::Display for CrawlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CrawlError::BadUrl(u) => write!(f, "not a usable URL: {u}"),
            CrawlError::UnsupportedScheme(s) => write!(f, "unsupported scheme: {s}"),
            CrawlError::Transport(e) => write!(f, "request failed: {e}"),
            CrawlError::Status(c) => write!(f, "server returned HTTP {c}"),
            CrawlError::ContentType(c) => write!(f, "not an HTML page (Content-Type: {c})"),
            CrawlError::TooLarge(n) => write!(f, "page exceeds the {n} byte limit"),
            CrawlError::Browser(e) => write!(f, "headless browser failed: {e}"),
        }
    }
}

impl std::error::Error for CrawlError {}

impl From<reqwest::Error> for CrawlError {
    fn from(e: reqwest::Error) -> Self {
        CrawlError::Transport(e.to_string())
    }
}

// *** config ***

#[derive(Debug, Clone)]
pub struct CrawlConfig {
    /// Hard cap on pages fetched. This is the real budget — depth is only a guard rail.
    pub max_pages: usize,
    /// How many requests may be in flight at once, across all hosts.
    pub max_concurrency: usize,
    /// Minimum gap between two requests to the same host.
    pub per_host_delay: Duration,
    /// Guard rail so a link farm can't keep the crawl alive forever.
    pub max_depth: u32,
    pub respect_robots: bool,
    /// Only follow links whose host matches the seed's.
    pub same_domain_only: bool,
    pub max_body_bytes: usize,
}

impl Default for CrawlConfig {
    fn default() -> Self {
        Self {
            max_pages: 200,
            max_concurrency: 8,
            per_host_delay: Duration::from_millis(500),
            max_depth: 3,
            respect_robots: true,
            same_domain_only: false,
            max_body_bytes: 5 * 1024 * 1024,
        }
    }
}

pub fn build_client(request_timeout: Duration) -> Result<Client, reqwest::Error> {
    Client::builder()
        .user_agent(USER_AGENT)
        .timeout(request_timeout)
        .connect_timeout(Duration::from_secs(8))
        .redirect(reqwest::redirect::Policy::limited(5))
        .build()
}

// *** URL handling ***

/// Parse and canonicalize a URL so equivalent addresses collapse to one form.
/// Returns an error rather than panicking — every caller path is reachable from
/// untrusted tool input.
pub fn canonicalize(input: &str) -> Result<Url, CrawlError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(CrawlError::BadUrl(input.to_string()));
    }

    let with_scheme = if trimmed.contains("://") {
        trimmed.to_string()
    } else {
        format!("https://{trimmed}")
    };

    let mut url =
        Url::parse(&with_scheme).map_err(|_| CrawlError::BadUrl(input.to_string()))?;

    match url.scheme() {
        "http" | "https" => {}
        other => return Err(CrawlError::UnsupportedScheme(other.to_string())),
    }

    if url.host_str().is_none_or(|h| h.is_empty()) {
        return Err(CrawlError::BadUrl(input.to_string()));
    }

    // Fragments are client-side only; they never identify a different document.
    url.set_fragment(None);

    let kept: Vec<(String, String)> = url
        .query_pairs()
        .filter(|(k, _)| !TRACKING_PARAMS.contains(&k.as_ref()))
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();

    if kept.is_empty() {
        url.set_query(None);
    } else {
        // Sorted so parameter order can't create phantom duplicates.
        let mut sorted = kept;
        sorted.sort();
        let mut serializer = url.query_pairs_mut();
        serializer.clear();
        for (k, v) in &sorted {
            serializer.append_pair(k, v);
        }
        drop(serializer);
    }

    if url.path().is_empty() {
        url.set_path("/");
    }

    Ok(url)
}

/// Identity used for deduplication only — never for fetching, because some hosts
/// serve different content on `www.` than on the apex domain.
pub fn dedup_key(url: &Url) -> String {
    let host = url
        .host_str()
        .map(|h| h.trim_start_matches("www.").to_lowercase())
        .unwrap_or_default();
    let path = url.path().trim_end_matches('/');
    match url.query() {
        Some(q) => format!("{host}{path}?{q}"),
        None => format!("{host}{path}"),
    }
}

pub fn normalize_domain(domain: &str) -> &str {
    domain.strip_prefix("www.").unwrap_or(domain)
}

pub fn host_of(url: &Url) -> String {
    url.host_str()
        .map(|h| normalize_domain(h).to_lowercase())
        .unwrap_or_default()
}

fn is_blocked(url: &Url) -> bool {
    let host = host_of(url);
    BLOCKED_DOMAINS.iter().any(|b| host == *b || host.ends_with(&format!(".{b}")))
}

// *** fetching ***

fn looks_like_html(content_type: &str) -> bool {
    let ct = content_type.to_ascii_lowercase();
    ct.starts_with("text/html")
        || ct.starts_with("application/xhtml")
        || ct.starts_with("text/plain")
        || ct.is_empty()
}

/// Fetch one page, rejecting error statuses, non-HTML bodies, and oversized
/// responses. The body is streamed so a huge file is abandoned rather than
/// buffered in full.
pub async fn fetch_page(
    client: &Client,
    url: &Url,
    max_body_bytes: usize,
) -> Result<String, CrawlError> {
    let mut response = client.get(url.clone()).send().await?;

    let status = response.status();
    if !status.is_success() {
        return Err(CrawlError::Status(status.as_u16()));
    }

    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    if !looks_like_html(&content_type) {
        return Err(CrawlError::ContentType(content_type));
    }

    let mut body: Vec<u8> = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if body.len() + chunk.len() > max_body_bytes {
            return Err(CrawlError::TooLarge(max_body_bytes));
        }
        body.extend_from_slice(&chunk);
    }

    Ok(String::from_utf8_lossy(&body).into_owned())
}

// headless browser
static BROWSER: tokio::sync::OnceCell<Arc<tokio::sync::Mutex<Browser>>> =
    tokio::sync::OnceCell::const_new();

static PROFILE_DIR: LazyLock<std::path::PathBuf> = LazyLock::new(|| {
    // A private profile per process. chromiumoxide otherwise reuses one shared
    // directory, so a SingletonLock left behind by a crashed run blocks every
    // later launch on the machine.
    std::env::temp_dir().join(format!("mcpcrawler-{}", std::process::id()))
});

/// Launch the browser once and reuse it, opening a tab per fetch. Previously
/// every headless call spawned a fresh Chrome and never closed it.
async fn shared_browser() -> Result<Arc<tokio::sync::Mutex<Browser>>, CrawlError> {
    BROWSER
        .get_or_try_init(|| async {
            sweep_stale_profiles();
            let config = BrowserConfig::builder()
                .user_data_dir(PROFILE_DIR.as_path())
                .build()
                .map_err(CrawlError::Browser)?;
            let (browser, mut handler) = Browser::launch(config)
                .await
                .map_err(|e| CrawlError::Browser(e.to_string()))?;

            // The handler drives the CDP connection for the process lifetime.
            tokio::spawn(async move { while handler.next().await.is_some() {} });

            Ok(Arc::new(tokio::sync::Mutex::new(browser)))
        })
        .await
        .map(Arc::clone)
}

/// Delete profile directories left behind by runs that were hard-killed before
/// they could clean up. Per-process profiles would otherwise accumulate.
fn sweep_stale_profiles() {
    let Ok(entries) = std::fs::read_dir(std::env::temp_dir()) else {
        return;
    };
    let day = Duration::from_secs(24 * 60 * 60);

    for entry in entries.flatten() {
        let path = entry.path();
        let is_ours = path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with("mcpcrawler-"));

        if !is_ours || path == *PROFILE_DIR {
            continue;
        }
        // Age, not liveness: a browser still in use rewrites its profile constantly.
        let stale = entry
            .metadata()
            .and_then(|m| m.modified())
            .map(|t| t.elapsed().unwrap_or_default() > day)
            .unwrap_or(false);

        if stale {
            let _ = std::fs::remove_dir_all(&path);
        }
    }
}

/// Close the browser and remove its profile directory. Must run before the
/// process exits, or Chrome is orphaned and the profile is left behind.
pub async fn shutdown_browser() {
    if let Some(browser) = BROWSER.get() {
        let mut browser = browser.lock().await;
        let _ = browser.close().await;
        let _ = browser.wait().await;
    }
    let _ = std::fs::remove_dir_all(PROFILE_DIR.as_path());
}

/// How long to let a client-rendered page settle before reading its DOM.
pub const DEFAULT_SETTLE: Duration = Duration::from_secs(6);

/// Open a tab and let client-rendered pages finish rendering.
///
/// `wait_for_navigation` returns as soon as the document loads, which for a
/// single-page app is before the framework has drawn anything. This polls until
/// the DOM stops growing, so a React or Next.js site yields real content rather
/// than an empty shell.
async fn open_settled_page(
    browser: &tokio::sync::Mutex<Browser>,
    url: &str,
    settle: Duration,
) -> Result<chromiumoxide::page::Page, CrawlError> {
    let page = {
        let browser = browser.lock().await;
        browser
            .new_page(url)
            .await
            .map_err(|e| CrawlError::Browser(e.to_string()))?
    };

    if let Err(e) = page.wait_for_navigation().await {
        let _ = page.close().await;
        return Err(CrawlError::Browser(e.to_string()));
    }

    let deadline = Instant::now() + settle;
    let mut previous = 0usize;
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let Ok(html) = page.content().await else { break };
        // Two consecutive polls at the same size means rendering has stopped.
        if html.len() == previous && previous > 0 {
            break;
        }
        previous = html.len();
    }

    Ok(page)
}

pub async fn fetch_page_headless(url: &str) -> Result<String, CrawlError> {
    let url = canonicalize(url)?;
    let browser = shared_browser().await?;
    let page = open_settled_page(&browser, url.as_str(), DEFAULT_SETTLE).await?;

    // Read separately so the tab is closed on every path, not just success.
    let result = page
        .content()
        .await
        .map_err(|e| CrawlError::Browser(e.to_string()));

    let _ = page.close().await;
    result
}

const USERNAME_SELECTORS: &[&str] = &[
    "input[type='email']",
    "input[autocomplete='username']",
    "input[autocomplete='email']",
    "input[name*='user']",
    "input[name*='email']",
    "input[id*='user']",
    "input[id*='email']",
    "input[name='loginfmt']",
    "input[name='login']",
    "input[type='text']",
];

const PASSWORD_SELECTORS: &[&str] = &[
    "input[type='password']",
    "input[name*='pass']",
    "input[id*='pass']",
];

const SUBMIT_SELECTORS: &[&str] = &[
    "button[type='submit']",
    "input[type='submit']",
    "button[id*='login']",
    "button[id*='signin']",
    "button[class*='login']",
    "button[class*='signin']",
    "[data-testid*='login']",
    "[data-testid*='signin']",
];

async fn find_element_any(
    page: &chromiumoxide::page::Page,
    selectors: &[&str],
) -> Option<chromiumoxide::element::Element> {
    for selector in selectors {
        if let Ok(el) = page.find_element(*selector).await {
            return Some(el);
        }
    }
    None
}

pub async fn login_and_fetch(
    login_url: &str,
    username: &str,
    password: &str,
) -> Result<String, CrawlError> {
    let login_url = canonicalize(login_url)?;
    let browser = shared_browser().await?;

    let page = open_settled_page(&browser, login_url.as_str(), DEFAULT_SETTLE).await?;

    let result = async {
        find_element_any(&page, USERNAME_SELECTORS)
            .await
            .ok_or_else(|| CrawlError::Browser("username field not found".into()))?
            .click()
            .await
            .map_err(|e| CrawlError::Browser(e.to_string()))?
            .type_str(username)
            .await
            .map_err(|e| CrawlError::Browser(e.to_string()))?;

        find_element_any(&page, PASSWORD_SELECTORS)
            .await
            .ok_or_else(|| CrawlError::Browser("password field not found".into()))?
            .click()
            .await
            .map_err(|e| CrawlError::Browser(e.to_string()))?
            .type_str(password)
            .await
            .map_err(|e| CrawlError::Browser(e.to_string()))?;

        find_element_any(&page, SUBMIT_SELECTORS)
            .await
            .ok_or_else(|| CrawlError::Browser("submit button not found".into()))?
            .click()
            .await
            .map_err(|e| CrawlError::Browser(e.to_string()))?;

        tokio::time::sleep(Duration::from_secs(3)).await;
        page.content()
            .await
            .map_err(|e| CrawlError::Browser(e.to_string()))
    }
    .await;

    let _ = page.close().await;
    result
}

// crawl

#[derive(Debug, Clone)]
pub struct FetchedPage {
    pub url: Url,
    pub html: String,
}

#[derive(Debug, Default)]
pub struct CrawlOutcome {
    /// Pages actually retrieved. Never contains a URL whose fetch failed.
    pub pages: Vec<FetchedPage>,
    pub failed: Vec<(String, String)>,
    pub robots_skipped: usize,
    pub budget_hit: bool,
}

impl CrawlOutcome {
    pub fn urls(&self) -> Vec<String> {
        self.pages.iter().map(|p| p.url.to_string()).collect()
    }
}

struct CrawlState {
    frontier: Mutex<VecDeque<(Url, u32)>>,
    seen: Mutex<HashSet<String>>,
    pages: Mutex<Vec<FetchedPage>>,
    failed: Mutex<Vec<(String, String)>>,
    host_slot: Mutex<HashMap<String, Instant>>,
    robots: Mutex<HashMap<String, Option<Arc<Robot>>>>,
    active: AtomicUsize,
    claimed: AtomicUsize,
    robots_skipped: AtomicUsize,
    budget_hit: AtomicBool,
    seed_host: String,
}

fn claim_host_slot(state: &CrawlState, host: &str, delay: Duration) -> Duration {
    let mut slots = state.host_slot.lock().unwrap();
    let now = Instant::now();
    let slot = slots.entry(host.to_string()).or_insert(now);
    let wait = slot.saturating_duration_since(now);
    *slot = (*slot).max(now) + delay;
    wait
}

async fn robots_for(
    client: &Client,
    state: &CrawlState,
    url: &Url,
) -> Option<Arc<Robot>> {
    let origin = url.origin().ascii_serialization();

    if let Some(cached) = state.robots.lock().unwrap().get(&origin) {
        return cached.clone();
    }

    let robots_url = format!("{origin}/robots.txt");
    let parsed = match client.get(&robots_url).send().await {
        Ok(resp) if resp.status().is_success() => match resp.bytes().await {
            Ok(body) => Robot::new(USER_AGENT, &body).ok().map(Arc::new),
            Err(_) => None,
        },
        // No robots.txt, or it could not be read: nothing is disallowed.
        _ => None,
    };

    state
        .robots
        .lock()
        .unwrap()
        .insert(origin, parsed.clone());
    parsed
}

async fn process_one(
    client: &Client,
    state: &CrawlState,
    cfg: &CrawlConfig,
    url: Url,
    depth: u32,
) {
    if cfg.respect_robots
        && let Some(robot) = robots_for(client, state, &url).await
        && !robot.allowed(url.as_str())
    {
        state.robots_skipped.fetch_add(1, Ordering::Relaxed);
        return;
    }

    let host = host_of(&url);
    let wait = claim_host_slot(state, &host, cfg.per_host_delay);
    if !wait.is_zero() {
        tokio::time::sleep(wait).await;
    }

    match fetch_page(client, &url, cfg.max_body_bytes).await {
        Ok(html) => {
            if depth < cfg.max_depth {
                let mut frontier = state.frontier.lock().unwrap();
                let mut seen = state.seen.lock().unwrap();
                for link in extract_links(&html, &url) {
                    if is_blocked(&link) {
                        continue;
                    }
                    if cfg.same_domain_only && host_of(&link) != state.seed_host {
                        continue;
                    }
                    if seen.insert(dedup_key(&link)) {
                        frontier.push_back((link, depth + 1));
                    }
                }
            }
            state.pages.lock().unwrap().push(FetchedPage { url, html });
        }
        Err(e) => {
            // Recorded separately so a failed fetch is never reported as visited.
            state
                .failed
                .lock()
                .unwrap()
                .push((url.to_string(), e.to_string()));
        }
    }
}

async fn worker(client: &Client, state: &CrawlState, cfg: &CrawlConfig) {
    loop {
        // Popping and marking the work active must happen under one lock, or a
        // peer worker can observe an empty frontier with no active work and exit
        // while this item is still in hand.
        let next = {
            let mut frontier = state.frontier.lock().unwrap();
            match frontier.pop_front() {
                Some(item) => {
                    state.active.fetch_add(1, Ordering::SeqCst);
                    Some(item)
                }
                None => None,
            }
        };

        let (url, depth) = match next {
            Some(item) => item,
            None => {
                if state.active.load(Ordering::SeqCst) == 0 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(15)).await;
                continue;
            }
        };

        if state.claimed.fetch_add(1, Ordering::SeqCst) >= cfg.max_pages {
            state.budget_hit.store(true, Ordering::SeqCst);
            state.active.fetch_sub(1, Ordering::SeqCst);
            state.frontier.lock().unwrap().clear();
            return;
        }

        process_one(client, state, cfg, url, depth).await;

        // Links are queued inside process_one, so the counter drops only after
        // any children are visible to peers.
        state.active.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Crawl from `seed`, bounded by page budget, depth, concurrency, and per-host
/// rate limit. Returns the pages it actually retrieved, along with what failed.
pub async fn crawl(
    client: &Client,
    seed: &str,
    cfg: &CrawlConfig,
) -> Result<CrawlOutcome, CrawlError> {
    let seed_url = canonicalize(seed)?;

    let state = CrawlState {
        frontier: Mutex::new(VecDeque::from([(seed_url.clone(), 0)])),
        seen: Mutex::new(HashSet::from([dedup_key(&seed_url)])),
        pages: Mutex::new(Vec::new()),
        failed: Mutex::new(Vec::new()),
        host_slot: Mutex::new(HashMap::new()),
        robots: Mutex::new(HashMap::new()),
        active: AtomicUsize::new(0),
        claimed: AtomicUsize::new(0),
        robots_skipped: AtomicUsize::new(0),
        budget_hit: AtomicBool::new(false),
        seed_host: host_of(&seed_url),
    };

    let workers = cfg.max_concurrency.max(1);
    join_all((0..workers).map(|_| worker(client, &state, cfg))).await;

    Ok(CrawlOutcome {
        pages: state.pages.into_inner().unwrap(),
        failed: state.failed.into_inner().unwrap(),
        robots_skipped: state.robots_skipped.load(Ordering::SeqCst),
        budget_hit: state.budget_hit.load(Ordering::SeqCst),
    })
}

pub async fn crawl_same_domain(
    client: &Client,
    seed: &str,
    cfg: &CrawlConfig,
) -> Result<CrawlOutcome, CrawlError> {
    let mut cfg = cfg.clone();
    cfg.same_domain_only = true;
    crawl(client, seed, &cfg).await
}

#[derive(Debug)]
pub struct Match {
    pub url: Url,
    pub hits: usize,
    pub snippet: String,
}

/// Crawl, then match against the HTML already retrieved. The previous version
/// re-downloaded every page a second time to do this.
pub async fn search_site(
    client: &Client,
    seed: &str,
    keyword: &str,
    cfg: &CrawlConfig,
) -> Result<(Vec<Match>, CrawlOutcome), CrawlError> {
    let outcome = crawl(client, seed, cfg).await?;
    let needle = keyword.to_lowercase();

    let matches = outcome
        .pages
        .iter()
        .filter_map(|page| {
            let text = extract_text(&page.html);
            let haystack = text.to_lowercase();
            let hits = haystack.matches(&needle).count();
            if hits == 0 {
                return None;
            }
            let at = haystack.find(&needle)?;
            Some(Match {
                url: page.url.clone(),
                hits,
                snippet: snippet_around(&text, at, needle.len()),
            })
        })
        .collect();

    Ok((matches, outcome))
}

fn snippet_around(text: &str, at: usize, len: usize) -> String {
    let start = text[..at].char_indices().rev().nth(60).map(|(i, _)| i).unwrap_or(0);
    let end_from = (at + len).min(text.len());
    let end = text[end_from..]
        .char_indices()
        .nth(90)
        .map(|(i, _)| end_from + i)
        .unwrap_or(text.len());
    let mut out = String::new();
    if start > 0 {
        out.push('…');
    }
    out.push_str(text[start..end].trim());
    if end < text.len() {
        out.push('…');
    }
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

// extraction

static LINK_SELECTOR: LazyLock<Selector> = LazyLock::new(|| Selector::parse("a[href]").unwrap());
static BODY_SELECTOR: LazyLock<Selector> = LazyLock::new(|| Selector::parse("body").unwrap());
static MAIN_SELECTORS: LazyLock<Vec<Selector>> = LazyLock::new(|| {
    ["main", "article", "[role='main']", "#content", "#main", ".post", ".content"]
        .iter()
        .filter_map(|s| Selector::parse(s).ok())
        .collect()
});

pub fn extract_links(html: &str, base: &Url) -> Vec<Url> {
    let document = Html::parse_document(html);
    let mut seen = HashSet::new();
    document
        .select(&LINK_SELECTOR)
        .filter_map(|el| el.value().attr("href"))
        .filter_map(|href| base.join(href).ok())
        .filter(|url| matches!(url.scheme(), "http" | "https"))
        .filter_map(|url| canonicalize(url.as_str()).ok())
        .filter(|url| seen.insert(dedup_key(url)))
        .collect()
}

fn collect_text(node: ego_tree::NodeRef<'_, Node>, out: &mut String) {
    for child in node.children() {
        match child.value() {
            Node::Element(el) => {
                let name = el.name();
                if SKIP_TAGS.contains(&name) {
                    continue;
                }
                collect_text(child, out);
                if BLOCK_TAGS.contains(&name) && !out.ends_with('\n') {
                    out.push('\n');
                }
            }
            Node::Text(text) => {
                let trimmed = text.trim();
                if !trimmed.is_empty() {
                    out.push_str(trimmed);
                    out.push(' ');
                }
            }
            _ => {}
        }
    }
}

/// Main-content text, with navigation, scripts, and chrome removed. Taking the
/// whole `<body>` gave every page on a site a large identical block of text,
/// which would poison both relevance scoring and duplicate detection.
pub fn extract_text(html: &str) -> String {
    let document = Html::parse_document(html);

    let root = MAIN_SELECTORS
        .iter()
        .find_map(|sel| document.select(sel).next())
        .or_else(|| document.select(&BODY_SELECTOR).next());

    let Some(root) = root else {
        return String::new();
    };

    let mut out = String::new();
    collect_text(*root, &mut out);

    out.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn extract_text_md(html: &str) -> String {
    htmd::convert(html).unwrap_or_default()
}

pub fn extract_metadata(html: &str) -> String {
    static TITLE: LazyLock<Selector> = LazyLock::new(|| Selector::parse("title").unwrap());
    static DESC: LazyLock<Selector> =
        LazyLock::new(|| Selector::parse("meta[name='description']").unwrap());
    static AUTHOR: LazyLock<Selector> =
        LazyLock::new(|| Selector::parse("meta[name='author']").unwrap());
    static KEYWORDS: LazyLock<Selector> =
        LazyLock::new(|| Selector::parse("meta[name='keywords']").unwrap());
    static OG_TITLE: LazyLock<Selector> =
        LazyLock::new(|| Selector::parse("meta[property='og:title']").unwrap());
    static OG_DESC: LazyLock<Selector> =
        LazyLock::new(|| Selector::parse("meta[property='og:description']").unwrap());
    static CANONICAL: LazyLock<Selector> =
        LazyLock::new(|| Selector::parse("link[rel='canonical']").unwrap());
    static PUBLISHED: LazyLock<Selector> = LazyLock::new(|| {
        Selector::parse("meta[property='article:published_time'], time[datetime]").unwrap()
    });

    let document = Html::parse_document(html);

    let content = |sel: &Selector| -> String {
        document
            .select(sel)
            .next()
            .and_then(|el| {
                el.value()
                    .attr("content")
                    .or_else(|| el.value().attr("href"))
                    .or_else(|| el.value().attr("datetime"))
            })
            .unwrap_or_default()
            .trim()
            .to_string()
    };

    let title = document
        .select(&TITLE)
        .next()
        .map(|el| el.text().collect::<Vec<_>>().join(" ").trim().to_string())
        .unwrap_or_default();

    [
        ("Title", title),
        ("Description", content(&DESC)),
        ("Author", content(&AUTHOR)),
        ("Keywords", content(&KEYWORDS)),
        ("OG Title", content(&OG_TITLE)),
        ("OG Description", content(&OG_DESC)),
        ("Canonical", content(&CANONICAL)),
        ("Published", content(&PUBLISHED)),
    ]
    .iter()
    .filter(|(_, v)| !v.is_empty())
    .map(|(k, v)| format!("{k}: {v}"))
    .collect::<Vec<_>>()
    .join("\n")
}
