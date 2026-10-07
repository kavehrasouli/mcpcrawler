use chromiumoxide::{Browser, BrowserConfig};
use futures::StreamExt;
use futures::future::join_all;
use reqwest::Client;
use crate::scoring::{LinkContext, ON_TOPIC, Terms, score_link};
use scraper::{Html, Node, Selector};
use std::cmp::Ordering as CmpOrdering;
use std::collections::{BinaryHeap, HashMap, HashSet};
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

/// How much anchor text and surrounding text to keep for scoring. Both are
/// signals, not content — a whole paragraph adds noise, not precision.
const ANCHOR_CHARS: usize = 200;
const NEARBY_CHARS: usize = 400;

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
    Parse(String),
    Blocked(String),
    Json(String),
    RateLimited(String),
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
            CrawlError::Parse(e) => write!(f, "could not parse XML: {e}"),
            CrawlError::Blocked(why) => write!(f, "destination refused: {why}"),
            CrawlError::Json(e) => write!(f, "response was not the expected JSON: {e}"),
            CrawlError::RateLimited(who) => write!(f, "rate limited: {who}"),
        }
    }
}

impl std::error::Error for CrawlError {}

impl From<reqwest::Error> for CrawlError {
    fn from(e: reqwest::Error) -> Self {
        // reqwest's own Display says only "error sending request". The reason —
        // a refused destination, a DNS failure, a TLS error — is down the
        // source chain, and without it every guard rejection is indistinguish-
        // able from the network being down.
        let mut message = e.to_string();
        let mut source = std::error::Error::source(&e);
        while let Some(cause) = source {
            message.push_str(&format!(": {cause}"));
            source = cause.source();
        }
        CrawlError::Transport(message)
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
    /// Seed the frontier from the site's sitemap instead of relying on links
    /// found in the seed page. Necessary for client-rendered sites, whose HTML
    /// contains no links at all.
    pub seed_from_sitemap: bool,
    /// What the crawl is looking for. With `None` the frontier stays
    /// breadth-first and every link is equal, which is the right behaviour for
    /// "fetch this site" and the wrong one for "find what this site says about
    /// X".
    pub focus: Option<Terms>,
    /// Consecutive off-topic hops a branch may take before it is pruned.
    ///
    /// Zero would be pure focused crawling, and it is a trap: the page you want
    /// is routinely two hops behind an index page that says nothing itself, so
    /// pruning on the first bad score cuts exactly the paths that lead to
    /// archives. This is the tunnelling allowance.
    pub tunnel_slack: u32,
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
            seed_from_sitemap: false,
            focus: None,
            tunnel_slack: 2,
        }
    }
}

pub fn build_client(request_timeout: Duration) -> Result<Client, reqwest::Error> {
    let policy = crate::net::policy();
    Client::builder()
        .user_agent(USER_AGENT)
        .timeout(request_timeout)
        .connect_timeout(Duration::from_secs(8))
        // Both guards are needed: the resolver judges hosts that get looked up,
        // the redirect policy judges hops that name an address outright.
        .dns_resolver(crate::net::resolver(policy))
        .redirect(crate::net::redirect_policy(policy))
        .build()
}

/// Refuse a destination the policy disallows before spending a request on it.
pub(crate) fn allowed(url: &Url) -> Result<(), CrawlError> {
    crate::net::policy().check_url(url).map_err(CrawlError::Blocked)
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
/// Fetch a URL as raw bytes, with the status and size guards but no content-type
/// gate. Sitemaps are XML and frequently gzipped, so they cannot go through
/// `fetch_page`.
pub async fn fetch_bytes(
    client: &Client,
    url: &Url,
    max_body_bytes: usize,
) -> Result<Vec<u8>, CrawlError> {
    allowed(url)?;
    let mut response = client.get(url.clone()).send().await?;

    let status = response.status();
    if !status.is_success() {
        return Err(CrawlError::Status(status.as_u16()));
    }

    let mut body: Vec<u8> = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if body.len() + chunk.len() > max_body_bytes {
            return Err(CrawlError::TooLarge(max_body_bytes));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

pub async fn fetch_page(
    client: &Client,
    url: &Url,
    max_body_bytes: usize,
) -> Result<String, CrawlError> {
    allowed(url)?;
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
    /// How this URL entered the frontier.
    pub origin: Origin,
    /// How much of the subject the fetched page turned out to contain, 0 to 1.
    /// Zero for an unfocused crawl, which has no subject to measure against.
    pub relevance: f32,
}

/// Where a candidate URL came from.
///
/// A single-seed crawl barely needs this — everything is the seed or a link off
/// it. Federated discovery does: candidates arrive from several backends at
/// once, and a merged frontier that cannot say which backend produced a URL has
/// thrown away the one thing that makes the merge auditable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Origin {
    /// Supplied by the caller.
    Seed,
    /// Listed in the site's sitemap.
    Sitemap,
    /// Linked from a page this crawl already fetched.
    Link { from: Url },
    /// Returned by a named discovery source. Constructed by the federation
    /// layer, which is why nothing in the crawler itself builds one yet.
    #[allow(dead_code)]
    Source(Arc<str>),
}

impl Origin {
    /// The grouping label, without the parent URL a `Link` carries — a summary
    /// wants "412 from links", not 412 distinct parents.
    pub fn kind(&self) -> String {
        match self {
            Origin::Seed => "seed".to_string(),
            Origin::Sitemap => "sitemap".to_string(),
            Origin::Link { .. } => "link".to_string(),
            Origin::Source(name) => format!("source:{name}"),
        }
    }
}

impl fmt::Display for Origin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Origin::Link { from } => write!(f, "link from {from}"),
            other => write!(f, "{}", other.kind()),
        }
    }
}

/// A URL entering the frontier from outside the crawl.
#[derive(Debug, Clone)]
pub struct Seed {
    pub url: Url,
    pub origin: Origin,
    /// How promising the caller thinks this URL is; higher is better.
    ///
    /// The frontier is still FIFO, so today this only decides the order seeds
    /// are queued in — which is already worth having, because it means a
    /// federated round fetches its best candidates before it runs out of
    /// budget. Phase 4 replaces the queue with a priority queue and this
    /// becomes the ordering key for followed links as well.
    pub score: f32,
}

impl Seed {
    /// A seed nobody has scored. Ordering among these is the order given.
    pub fn new(url: Url, origin: Origin) -> Self {
        Self { url, origin, score: 0.0 }
    }

    pub fn scored(url: Url, origin: Origin, score: f32) -> Self {
        Self { url, origin, score }
    }
}

/// A URL waiting to be fetched, with everything the crawl knows about it.
#[derive(Debug, Clone)]
struct Candidate {
    url: Url,
    depth: u32,
    origin: Origin,
    /// How promising this URL looked before it was fetched.
    score: f32,
    /// Remaining tunnelling allowance — consecutive off-topic hops this branch
    /// may still take. Spent down on a poor link, restored by a good one.
    slack: u32,
    /// Insertion order, used only to break ties. With no focus every score is
    /// equal, so this alone decides the order and the crawl is plain
    /// breadth-first, exactly as it was before the queue became a heap.
    seq: usize,
}

// Ordering is what makes the frontier a priority queue: `BinaryHeap` pops the
// greatest, so "greatest" must mean "most worth fetching next".
impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == CmpOrdering::Equal
    }
}

impl Eq for Candidate {}

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

    /// Fetched pages counted by where their URL came from, largest group first.
    pub fn origin_counts(&self) -> Vec<(String, usize)> {
        let mut counts: HashMap<String, usize> = HashMap::new();
        for page in &self.pages {
            *counts.entry(page.origin.kind()).or_default() += 1;
        }
        let mut counts: Vec<(String, usize)> = counts.into_iter().collect();
        counts.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        counts
    }
}

struct CrawlState {
    frontier: Mutex<BinaryHeap<Candidate>>,
    seen: Mutex<HashSet<String>>,
    pages: Mutex<Vec<FetchedPage>>,
    failed: Mutex<Vec<(String, String)>>,
    host_slot: HostSlots,
    robots: Mutex<HashMap<String, Option<Arc<Robot>>>>,
    active: AtomicUsize,
    claimed: AtomicUsize,
    /// Handed out in order so equally scored candidates keep their arrival
    /// order.
    queued: AtomicUsize,
    robots_skipped: AtomicUsize,
    budget_hit: AtomicBool,
    /// Every host the caller seeded. With one seed this is the old `seed_host`;
    /// with a federated frontier there is no single host to compare against.
    seed_hosts: HashSet<String>,
}

pub type HostSlots = Mutex<HashMap<String, Instant>>;

/// Reserve the next slot for `host`, returning how long to wait before using it.
/// Claiming and waiting are separate so concurrent callers queue behind each
/// other instead of all sleeping the same interval and then firing together.
pub fn claim_slot(slots: &HostSlots, host: &str, delay: Duration) -> Duration {
    let mut slots = slots.lock().unwrap();
    let now = Instant::now();
    let slot = slots.entry(host.to_string()).or_insert(now);
    let wait = slot.saturating_duration_since(now);
    *slot = (*slot).max(now) + delay;
    wait
}

/// Wait out this host's rate limit before issuing a request.
pub async fn throttle(slots: &HostSlots, host: &str, delay: Duration) {
    let wait = claim_slot(slots, host, delay);
    if !wait.is_zero() {
        tokio::time::sleep(wait).await;
    }
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

async fn process_one(client: &Client, state: &CrawlState, cfg: &CrawlConfig, item: Candidate) {
    let Candidate { url, depth, origin, score, slack, .. } = item;
    if cfg.respect_robots
        && let Some(robot) = robots_for(client, state, &url).await
        && !robot.allowed(url.as_str())
    {
        state.robots_skipped.fetch_add(1, Ordering::Relaxed);
        return;
    }

    throttle(&state.host_slot, &host_of(&url), cfg.per_host_delay).await;

    match fetch_page(client, &url, cfg.max_body_bytes).await {
        Ok(html) => {
            let relevance = match &cfg.focus {
                // Measured from the page that actually arrived, not inherited
                // from how good its link looked. An index page whose link text
                // promised everything and whose body says nothing should not
                // lend its promise to its children.
                Some(terms) if depth < cfg.max_depth => {
                    let page = analyse(&html, &url);
                    let relevance = terms.match_strength(&page.text);
                    queue_links(state, cfg, &url, depth, slack, relevance, page.links, terms);
                    relevance
                }
                Some(terms) => terms.match_strength(&extract_text(&html)),
                None => {
                    if depth < cfg.max_depth {
                        let page = analyse(&html, &url);
                        queue_links(
                            state,
                            cfg,
                            &url,
                            depth,
                            slack,
                            0.0,
                            page.links,
                            &Terms::default(),
                        );
                    }
                    score
                }
            };

            state.pages.lock().unwrap().push(FetchedPage {
                url,
                html,
                origin,
                relevance,
            });
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

/// Queue a page's links, deciding for each whether it is worth a request.
///
/// A link that looks on-topic restores the branch's full tunnelling allowance.
/// One that does not spends a unit of it, and when a branch runs out it is
/// dropped — so the crawl will walk through two dull pages to reach a good one
/// but will not wander indefinitely.
#[allow(clippy::too_many_arguments)]
fn queue_links(
    state: &CrawlState,
    cfg: &CrawlConfig,
    parent: &Url,
    depth: u32,
    slack: u32,
    relevance: f32,
    links: Vec<LinkContext>,
    terms: &Terms,
) {
    let focused = !terms.is_empty();
    let mut frontier = state.frontier.lock().unwrap();
    let mut seen = state.seen.lock().unwrap();

    for link in links {
        if is_blocked(&link.url) {
            continue;
        }
        if cfg.same_domain_only && !state.seed_hosts.contains(&host_of(&link.url)) {
            continue;
        }

        let (score, slack) = if focused {
            let Some(score) = score_link(&link, terms, relevance) else {
                continue;
            };
            if score >= ON_TOPIC {
                (score, cfg.tunnel_slack)
            } else if slack == 0 {
                // Out of allowance on an unpromising branch: stop here.
                continue;
            } else {
                (score, slack - 1)
            }
        } else {
            (0.0, cfg.tunnel_slack)
        };

        if seen.insert(dedup_key(&link.url)) {
            frontier.push(Candidate {
                url: link.url,
                depth: depth + 1,
                origin: Origin::Link { from: parent.clone() },
                score,
                slack,
                seq: state.queued.fetch_add(1, Ordering::SeqCst),
            });
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
            match frontier.pop() {
                Some(item) => {
                    state.active.fetch_add(1, Ordering::SeqCst);
                    Some(item)
                }
                None => None,
            }
        };

        let item = match next {
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

        process_one(client, state, cfg, item).await;

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
    crawl_from(client, vec![Seed::new(seed_url, Origin::Seed)], cfg).await
}

/// Crawl from many seeds at once, each carrying where it came from.
///
/// This is the entry point a federated frontier needs: discovery sources return
/// candidate URLs across many hosts, and they all have to enter one crawl with
/// one shared budget, one visited set and one rate limiter — running a separate
/// crawl per source would multiply every limit by the number of sources.
pub async fn crawl_from(
    client: &Client,
    mut seeds: Vec<Seed>,
    cfg: &CrawlConfig,
) -> Result<CrawlOutcome, CrawlError> {
    if seeds.is_empty() {
        return Err(CrawlError::BadUrl("no seed URLs".to_string()));
    }

    // Best first, so a budget that runs out takes the tail rather than an
    // arbitrary slice. `sort_by` is stable, so equal scores keep their order.
    seeds.sort_by(|a, b| b.score.total_cmp(&a.score));

    let mut frontier: BinaryHeap<Candidate> = BinaryHeap::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut seed_hosts: HashSet<String> = HashSet::new();
    let mut queued = 0usize;

    for seed in seeds {
        seed_hosts.insert(host_of(&seed.url));
        if seen.insert(dedup_key(&seed.url)) {
            frontier.push(Candidate {
                url: seed.url,
                depth: 0,
                origin: seed.origin,
                score: seed.score,
                slack: cfg.tunnel_slack,
                seq: queued,
            });
            queued += 1;
        }
    }

    if cfg.seed_from_sitemap {
        // Every seed host contributes, but out of one shared budget rather than
        // `max_pages` each — the crawl can only ever fetch `max_pages` anyway.
        let hosts: Vec<Url> = distinct_host_roots(&frontier);
        for root in hosts {
            let remaining = cfg.max_pages.saturating_sub(frontier.len());
            if remaining == 0 {
                break;
            }
            let sitemap_cfg = crate::sitemap::SitemapConfig {
                max_urls: remaining,
                per_host_delay: cfg.per_host_delay,
                ..Default::default()
            };
            let Ok(outcome) = crate::sitemap::collect(client, root.as_str(), &sitemap_cfg).await
            else {
                continue;
            };
            for entry in outcome.entries {
                if cfg.same_domain_only && !seed_hosts.contains(&host_of(&entry.url)) {
                    continue;
                }
                // A sitemap lists URLs with no anchor text and no context, so
                // the URL itself is all there is to judge them on.
                let score = match &cfg.focus {
                    Some(terms) => score_link(
                        &LinkContext {
                            url: entry.url.clone(),
                            anchor: String::new(),
                            nearby: String::new(),
                            rel_next: false,
                        },
                        terms,
                        0.0,
                    )
                    .unwrap_or(0.0),
                    None => 0.0,
                };
                if seen.insert(dedup_key(&entry.url)) {
                    frontier.push(Candidate {
                        url: entry.url,
                        depth: 0,
                        origin: Origin::Sitemap,
                        score,
                        slack: cfg.tunnel_slack,
                        seq: queued,
                    });
                    queued += 1;
                }
            }
        }
    }

    let state = CrawlState {
        frontier: Mutex::new(frontier),
        seen: Mutex::new(seen),
        pages: Mutex::new(Vec::new()),
        failed: Mutex::new(Vec::new()),
        host_slot: Mutex::new(HashMap::new()),
        robots: Mutex::new(HashMap::new()),
        active: AtomicUsize::new(0),
        claimed: AtomicUsize::new(0),
        robots_skipped: AtomicUsize::new(0),
        budget_hit: AtomicBool::new(false),
        queued: AtomicUsize::new(queued),
        seed_hosts,
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

/// One URL per distinct host, so a sitemap is looked up once per site however
/// many seeds landed on it.
fn distinct_host_roots(frontier: &BinaryHeap<Candidate>) -> Vec<Url> {
    let mut seen = HashSet::new();
    frontier
        .iter()
        .filter(|c| seen.insert(host_of(&c.url)))
        .map(|c| c.url.clone())
        .collect()
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
    if keyword.trim().is_empty() {
        return Err(CrawlError::BadUrl("empty search term".to_string()));
    }

    let outcome = crawl(client, seed, cfg).await?;
    let needle = keyword.to_lowercase();

    let matches = outcome
        .pages
        .iter()
        .filter_map(|page| {
            let text = extract_text(&page.html);
            let (haystack, offsets) = lowercase_with_offsets(&text);
            let hits = haystack.matches(&needle).count();
            if hits == 0 {
                return None;
            }
            let at = haystack.find(&needle)?;
            // Back to where the match sits in the *original* text. Lowercasing
            // is not length-preserving — Turkish `İ` becomes two characters —
            // so an offset taken from the folded string does not address the
            // string it came from.
            let start = *offsets.get(at)?;
            let end = *offsets.get(at + needle.len()).unwrap_or(&text.len());
            Some(Match {
                url: page.url.clone(),
                hits,
                snippet: snippet_around(&text, start, end.saturating_sub(start)),
            })
        })
        .collect();

    Ok((matches, outcome))
}

/// Lowercase `text`, and for every byte of the result record which byte of the
/// original it came from. The map is what makes a case-insensitive search
/// reportable: you can find in the folded text and still quote the real one.
fn lowercase_with_offsets(text: &str) -> (String, Vec<usize>) {
    let mut lower = String::with_capacity(text.len());
    let mut offsets: Vec<usize> = Vec::with_capacity(text.len() + 1);

    for (at, c) in text.char_indices() {
        for folded in c.to_lowercase() {
            let before = lower.len();
            lower.push(folded);
            offsets.resize(lower.len(), at);
            debug_assert!(lower.len() > before);
        }
    }
    offsets.push(text.len());
    (lower, offsets)
}

fn snippet_around(text: &str, at: usize, len: usize) -> String {
    let at = floor_boundary(text, at);
    let start = text[..at].char_indices().rev().nth(60).map(|(i, _)| i).unwrap_or(0);
    let end_from = floor_boundary(text, (at + len).min(text.len()));
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
    links_in(&Html::parse_document(html), base)
        .into_iter()
        .map(|link| link.url)
        .collect()
}

/// A page read once: its links with the context needed to score them, and its
/// main text. Parsing is the expensive part of a fetch, and the frontier needs
/// both, so doing it twice would double the cost of every page crawled.
pub struct PageAnalysis {
    pub links: Vec<LinkContext>,
    pub text: String,
}

pub fn analyse(html: &str, base: &Url) -> PageAnalysis {
    let document = Html::parse_document(html);
    PageAnalysis { links: links_in(&document, base), text: text_of(&document) }
}

fn links_in(document: &Html, base: &Url) -> Vec<LinkContext> {
    let mut seen = HashSet::new();
    let mut links = Vec::new();

    for el in document.select(&LINK_SELECTOR) {
        let Some(href) = el.value().attr("href") else { continue };
        let Ok(joined) = base.join(href) else { continue };
        if !matches!(joined.scheme(), "http" | "https") {
            continue;
        }
        let Ok(url) = canonicalize(joined.as_str()) else { continue };
        if !seen.insert(dedup_key(&url)) {
            continue;
        }

        // The sentence around a link says more about it than three words of
        // anchor text, and costs nothing — the parent is already in hand.
        let nearby = el
            .parent()
            .and_then(scraper::ElementRef::wrap)
            .map(|parent| trimmed_text(parent.text(), NEARBY_CHARS))
            .unwrap_or_default();

        links.push(LinkContext {
            url,
            anchor: trimmed_text(el.text(), ANCHOR_CHARS),
            nearby,
            rel_next: el
                .value()
                .attr("rel")
                .is_some_and(|rel| rel.to_ascii_lowercase().split_whitespace().any(|r| r == "next")),
        });
    }
    links
}

/// The largest char boundary at or below `limit`. Truncating on a raw byte
/// index panics the moment a page uses anything outside ASCII, which — for a
/// crawler whose whole point is multilingual coverage — is most of them.
fn floor_boundary(text: &str, limit: usize) -> usize {
    let mut cutoff = limit.min(text.len());
    while cutoff > 0 && !text.is_char_boundary(cutoff) {
        cutoff -= 1;
    }
    cutoff
}

fn trimmed_text<'a>(parts: impl Iterator<Item = &'a str>, limit: usize) -> String {
    let mut out = String::new();
    for part in parts {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(part);
        if out.len() >= limit {
            out.truncate(floor_boundary(&out, limit));
            break;
        }
    }
    out
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
    text_of(&Html::parse_document(html))
}

fn text_of(document: &Html) -> String {
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

static PUBLISHED: LazyLock<Selector> = LazyLock::new(|| {
    Selector::parse("meta[property='article:published_time'], time[datetime]").unwrap()
});

/// When the page says it was published, exactly as it says it. A meta tag or
/// `<time>` element first; the JSON-LD record for pages that render client-side
/// and carry nothing else. Normalising the format is the caller's business.
pub fn published_raw(html: &str) -> Option<String> {
    let document = Html::parse_document(html);
    document
        .select(&PUBLISHED)
        .next()
        .and_then(|el| el.value().attr("content").or_else(|| el.value().attr("datetime")))
        .map(str::trim)
        .filter(|date| !date.is_empty())
        .map(str::to_string)
        .or_else(|| crate::structured::extract(html).field(&["datePublished", "dateCreated"]))
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

    // A page that renders its content client-side often has no meta tags worth
    // reading, yet still publishes the same facts as JSON-LD for search engines.
    // Fall back to the record rather than reporting the field as absent.
    let structured = crate::structured::extract(html);
    let or_schema = |value: String, keys: &[&str]| -> String {
        if value.is_empty() {
            structured.field(keys).unwrap_or_default()
        } else {
            value
        }
    };

    [
        ("Title", or_schema(title, &["headline", "name"])),
        ("Description", or_schema(content(&DESC), &["description"])),
        ("Author", or_schema(content(&AUTHOR), &["author", "creator"])),
        ("Keywords", content(&KEYWORDS)),
        ("OG Title", content(&OG_TITLE)),
        ("OG Description", content(&OG_DESC)),
        ("Canonical", or_schema(content(&CANONICAL), &["url"])),
        ("Published", or_schema(content(&PUBLISHED), &["datePublished", "dateCreated"])),
        ("Schema types", structured.schema_types().join(", ")),
    ]
    .iter()
    .filter(|(_, v)| !v.is_empty())
    .map(|(k, v)| format!("{k}: {v}"))
    .collect::<Vec<_>>()
    .join("\n")
}
