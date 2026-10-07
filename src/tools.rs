use crate::crawler::{
    CrawlConfig, CrawlError, CrawlOutcome, build_client, canonicalize, crawl, crawl_from,
    crawl_same_domain,
    extract_links, extract_metadata, extract_text, extract_text_md, fetch_page,
    fetch_page_headless, login_and_fetch, search_site,
};
use reqwest::Client;
use rmcp::{
    ServerHandler,
    handler::server::wrapper::Parameters,
    model::{Implementation, ServerCapabilities, ServerInfo},
    schemars, tool, tool_handler, tool_router,
};
use crate::discovery::{DiscoveryBudget, DiscoverySource, Federated, Query, federate};
use crate::dedup::{self, Group};
use crate::expand;
use crate::scoring::Terms;
use crate::sitemap::{self, SitemapConfig};
use crate::sources::{brave::Brave, gdelt::Gdelt, wayback::Wayback, wikidata::Wikidata};
use crate::structured::{self, StructuredData};
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

/// Ceiling on any single tool response. A large crawl used to return every URL
/// joined into one string, which can be megabytes of model context.
const MAX_OUTPUT_CHARS: usize = 60_000;
/// Ceiling on how many URLs are listed before the rest are summarized.
const MAX_LISTED: usize = 400;

const DEFAULT_TIMEOUT_SECS: u64 = 15;

/// How deep a state blob is outlined: enough to show where the data sits, not
/// so much that the outline becomes the dump it exists to avoid.
const STATE_OUTLINE_DEPTH: u32 = 3;
/// Ceiling on URLs harvested out of one state blob.
const MAX_STATE_URLS: usize = 1_000;

fn cap(mut text: String) -> String {
    if text.chars().count() <= MAX_OUTPUT_CHARS {
        return text;
    }
    let cutoff = text
        .char_indices()
        .nth(MAX_OUTPUT_CHARS)
        .map(|(i, _)| i)
        .unwrap_or(text.len());
    text.truncate(cutoff);
    text.push_str("\n\n[truncated — output exceeded the size limit]");
    text
}

fn list_urls(urls: &[String]) -> String {
    if urls.len() <= MAX_LISTED {
        return urls.join("\n");
    }
    let mut out = urls[..MAX_LISTED].join("\n");
    out.push_str(&format!(
        "\n… and {} more (not listed)",
        urls.len() - MAX_LISTED
    ));
    out
}

/// Brave is registered only when a key is configured: a backend that reports
/// the same failure on every call is worse than one that is simply absent.
/// `None` for an absent or blank subject, which keeps the frontier
/// breadth-first rather than scoring everything at zero.
fn focus_from(about: Option<&str>) -> Option<Terms> {
    let terms = Terms::new(about.into_iter());
    Some(terms).filter(|t| !t.is_empty())
}

fn built_in_sources() -> Vec<Box<dyn DiscoverySource>> {
    let mut sources: Vec<Box<dyn DiscoverySource>> = vec![
        Box::new(Gdelt::new()),
        Box::new(Wikidata::new()),
        Box::new(Wayback::new()),
    ];
    if let Some(brave) = Brave::from_env() {
        sources.push(Box::new(brave));
    }
    sources
}

/// One discovery round, expanded or not, with a line saying what was run.
///
/// Expansion is opt-in and says what it did: a result list that silently came
/// from eight queries over a narrowed window reads as if it came from the one
/// the caller typed.
async fn run_discovery(
    client: &reqwest::Client,
    sources: &[Box<dyn DiscoverySource>],
    input: &DiscoverInput,
) -> Result<(Federated, String), String> {
    let query = input.window()?;
    let budget = input.budget();
    if input.expand != Some(true) {
        return Ok((federate(client, sources, &query, &budget).await, String::new()));
    }

    let max = input.max_queries.unwrap_or(expand::DEFAULT_QUERIES);
    let done = expand::research(client, sources, &query, input.place.as_deref(), max, &budget).await;
    let bound = |d: &Option<String>| d.clone().unwrap_or_else(|| "open".to_string());
    let mut note = format!(
        "Expanded: {} more quer{} over {}–{}, plus the original.\n",
        done.queries.len(),
        if done.queries.len() == 1 { "y" } else { "ies" },
        bound(&done.window.from),
        bound(&done.window.to),
    );
    for q in &done.queries {
        note.push_str(&format!("  - {} [{}–{}]\n", q.text, bound(&q.since), bound(&q.until)));
    }
    note.push('\n');
    Ok((done.federated, note))
}

fn discovery_report(outcome: &Federated) -> String {
    let mut out = format!(
        "{} candidate URL(s).{}\n\n",
        outcome.leads.len(),
        match outcome.failures.len() {
            0 => String::new(),
            n => format!(" {n} source(s) failed — results are partial."),
        }
    );

    for (name, why) in &outcome.failures {
        out.push_str(&format!("{name} failed: {why}\n"));
    }
    if !outcome.failures.is_empty() {
        out.push('\n');
    }

    for lead in &outcome.leads {
        out.push_str(&format!("[{:.2}] {}\n", lead.score, lead.url));
        let mut detail: Vec<String> = Vec::new();
        if let Some(title) = &lead.title {
            detail.push(title.clone());
        }
        for field in [&lead.domain, &lead.language, &lead.seen] {
            if let Some(value) = field {
                detail.push(value.clone());
            }
        }
        detail.push(format!(
            "via {}",
            lead.sources.iter().map(|s| s.to_string()).collect::<Vec<_>>().join(" + ")
        ));
        out.push_str(&format!("       {}\n", detail.join(" — ")));
    }

    out
}

fn structured_report(data: &StructuredData, base: &Url) -> String {
    if data.is_empty() {
        return "No structured data: the page publishes no JSON-LD records and no embedded \
                framework state. Use fetch_content for its text, or headless=true if it builds \
                itself client-side."
            .to_string();
    }

    let mut out = format!(
        "{} JSON-LD record(s); {} embedded state blob(s).\n\n",
        data.json_ld.len(),
        data.embedded.len()
    );

    if !data.json_ld.is_empty() {
        out.push_str("## JSON-LD\n\n");
        out.push_str(&structured::render_records(&data.json_ld));
    }

    for state in &data.embedded {
        out.push_str(&format!(
            "## {} — {:.1} KB of JSON\n\n{}\n\n",
            state.name,
            state.raw_len as f64 / 1024.0,
            structured::render_outline(&state.value, STATE_OUTLINE_DEPTH)
        ));

        // The point of the blob on a link-less page: the URLs its HTML omits.
        let urls: Vec<String> = structured::urls_in(&state.value, base, MAX_STATE_URLS)
            .iter()
            .map(Url::to_string)
            .collect();
        if !urls.is_empty() {
            out.push_str(&format!(
                "### {} URL(s) referenced in {}\n\n{}\n\n",
                urls.len(),
                state.name,
                list_urls(&urls)
            ));
        }
    }

    out
}

/// ` — also on a.com, b.com (+3)`: the hosts carrying the same story, which is
/// itself the evidence that it was syndicated.
fn copies_note(outcome: &CrawlOutcome, group: &Group) -> String {
    if group.copies.is_empty() {
        return String::new();
    }
    let mut hosts: Vec<&str> = Vec::new();
    for copy in &group.copies {
        if let Some(host) = outcome.pages[*copy].url.host_str()
            && !hosts.contains(&host)
        {
            hosts.push(host);
        }
    }
    let shown = hosts.len().min(3);
    // Copies not accounted for by a named host, which may be several per host.
    let more = group
        .copies
        .iter()
        .filter(|c| {
            outcome.pages[**c].url.host_str().is_none_or(|h| !hosts[..shown].contains(&h))
        })
        .count();
    let mut note = format!(" — also on {}", hosts[..shown].join(", "));
    if more > 0 {
        note.push_str(&format!(" (+{more})"));
    }
    note
}

pub(crate) fn report(outcome: &CrawlOutcome) -> String {
    let urls = outcome.urls();
    let focused = outcome.pages.iter().any(|p| p.relevance > 0.0);
    // Where the URLs came from, not which URL came from where: naming the
    // parent of every link would be most of the output and none of the value.
    let origins = match outcome.origin_counts().as_slice() {
        [] => String::new(),
        counts => format!(
            "\nFound via: {}.",
            counts
                .iter()
                .map(|(kind, count)| format!("{count} {kind}"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    };

    // One story syndicated onto fifty sites is one report, not fifty.
    let groups = dedup::group_pages(&outcome.pages);
    let copies: usize = groups.iter().map(|g| g.copies.len()).sum();
    let distinct = match copies {
        0 => String::new(),
        n => format!(
            " {} distinct after collapsing {n} near-duplicate cop{}.",
            groups.len(),
            if n == 1 { "y" } else { "ies" }
        ),
    };

    let mut out = format!(
        "Fetched {} page(s);{distinct} {} failed; {} skipped by robots.txt.{}{}\n\n",
        urls.len(),
        outcome.failed.len(),
        outcome.robots_skipped,
        if outcome.budget_hit {
            " Page budget reached — results are partial."
        } else {
            ""
        },
        origins
    );

    // Best first for a focused crawl: the relevant pages are the answer, and
    // the order they happened to be fetched in is not interesting.
    let mut shown: Vec<&Group> = groups.iter().collect();
    if focused {
        shown.sort_by(|a, b| {
            outcome.pages[b.best].relevance.total_cmp(&outcome.pages[a.best].relevance)
        });
    }
    let listed = shown.len().min(MAX_LISTED);
    for group in &shown[..listed] {
        let page = &outcome.pages[group.best];
        if focused {
            out.push_str(&format!("[{:.2}] ", page.relevance));
        }
        out.push_str(page.url.as_str());
        out.push_str(&copies_note(outcome, group));
        out.push('\n');
    }
    if shown.len() > listed {
        out.push_str(&format!("… and {} more (not listed)\n", shown.len() - listed));
    }

    if !outcome.failed.is_empty() {
        out.push_str("\n\nFailed:\n");
        for (url, err) in outcome.failed.iter().take(50) {
            out.push_str(&format!("{url} — {err}\n"));
        }
        if outcome.failed.len() > 50 {
            out.push_str(&format!("… and {} more\n", outcome.failed.len() - 50));
        }
    }

    cap(out)
}

// ***tool inputs***

/// Crawl limits. All optional — the defaults are deliberately conservative so a
/// crawl cannot run away.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct CrawlInput {
    #[schemars(description = "The URL to crawl")]
    pub url: String,
    #[schemars(description = "Maximum pages to fetch. This is the real budget. Default 200.")]
    #[serde(default)]
    pub max_pages: Option<usize>,
    #[schemars(description = "Maximum link hops from the seed. Safety guard rail. Default 3.")]
    #[serde(default)]
    pub depth: Option<u32>,
    #[schemars(description = "Concurrent requests in flight. Default 8.")]
    #[serde(default)]
    pub concurrency: Option<usize>,
    #[schemars(description = "Milliseconds to wait between requests to the same host. Default 500.")]
    #[serde(default)]
    pub per_host_delay_ms: Option<u64>,
    #[schemars(description = "Honor robots.txt. Default true.")]
    #[serde(default)]
    pub respect_robots: Option<bool>,
    #[schemars(
        description = "Seed the crawl from the site's sitemap. Required for client-rendered \
                       sites, whose HTML contains no links to follow. Default false."
    )]
    #[serde(default)]
    pub seed_from_sitemap: Option<bool>,
    #[schemars(
        description = "What the crawl is looking for. Given this, links are scored before they \
                       are opened — archive indexes and paginated lists are preferred, cookie \
                       and login pages avoided — so the budget goes to pages about the subject \
                       rather than to whatever a page happens to list first. Without it the \
                       crawl is plain breadth-first."
    )]
    #[serde(default)]
    pub about: Option<String>,
}

impl CrawlInput {
    fn config(&self) -> CrawlConfig {
        let d = CrawlConfig::default();
        CrawlConfig {
            max_pages: self.max_pages.unwrap_or(d.max_pages).clamp(1, 10_000),
            max_concurrency: self.concurrency.unwrap_or(d.max_concurrency).clamp(1, 64),
            per_host_delay: self
                .per_host_delay_ms
                .map(Duration::from_millis)
                .unwrap_or(d.per_host_delay),
            max_depth: self.depth.unwrap_or(d.max_depth).clamp(1, 20),
            respect_robots: self.respect_robots.unwrap_or(d.respect_robots),
            seed_from_sitemap: self.seed_from_sitemap.unwrap_or(false),
            focus: focus_from(self.about.as_deref()),
            ..d
        }
    }
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SearchInput {
    #[schemars(description = "The URL to crawl")]
    pub url: String,
    #[schemars(description = "The keyword to search for. Matching is case-insensitive.")]
    pub keyword: String,
    #[schemars(description = "Maximum pages to fetch. Default 200.")]
    #[serde(default)]
    pub max_pages: Option<usize>,
    #[schemars(description = "Maximum link hops from the seed. Default 3.")]
    #[serde(default)]
    pub depth: Option<u32>,
    #[schemars(description = "Restrict the crawl to the seed's domain. Default false.")]
    #[serde(default)]
    pub same_domain_only: Option<bool>,
}

impl SearchInput {
    fn config(&self) -> CrawlConfig {
        let d = CrawlConfig::default();
        CrawlConfig {
            max_pages: self.max_pages.unwrap_or(d.max_pages).clamp(1, 10_000),
            max_depth: self.depth.unwrap_or(d.max_depth).clamp(1, 20),
            same_domain_only: self.same_domain_only.unwrap_or(false),
            // The keyword already says what the crawl is looking for, so
            // spending the budget on pages likely to contain it is free.
            focus: focus_from(Some(&self.keyword)),
            ..d
        }
    }
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct FetchInput {
    #[schemars(description = "The URL to fetch content from")]
    pub url: String,
    #[schemars(description = "Render with a headless browser first, for JS-built pages. Default false.")]
    #[serde(default)]
    pub headless: Option<bool>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SitemapInput {
    #[schemars(description = "A site (its sitemaps are discovered via robots.txt) or a sitemap URL")]
    pub url: String,
    #[schemars(
        description = "Keep only URLs containing this substring, case-insensitive. Applied while \
                       walking, so a filtered scan reaches much deeper into a large index."
    )]
    #[serde(default)]
    pub contains: Option<String>,
    #[schemars(description = "Maximum URLs to collect. Default 5000.")]
    #[serde(default)]
    pub max_urls: Option<usize>,
    #[schemars(description = "Maximum sitemap documents to read. Default 50.")]
    #[serde(default)]
    pub max_sitemaps: Option<usize>,
}

impl SitemapInput {
    fn config(&self) -> SitemapConfig {
        let d = SitemapConfig::default();
        SitemapConfig {
            max_urls: self.max_urls.unwrap_or(d.max_urls).clamp(1, 200_000),
            max_sitemaps: self.max_sitemaps.unwrap_or(d.max_sitemaps).clamp(1, 2_000),
            contains: self.contains.clone().filter(|c| !c.is_empty()),
            ..d
        }
    }
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DiscoverInput {
    #[schemars(
        description = "What to look for. Quote a phrase to match it as a phrase. GDELT also \
                       accepts its own operators, such as sourcelang:spanish or domain:un.org."
    )]
    pub query: String,
    #[schemars(description = "Earliest date to consider, as YYYYMMDD. Optional.")]
    #[serde(default)]
    pub since: Option<String>,
    #[schemars(description = "Latest date to consider, as YYYYMMDD. Optional.")]
    #[serde(default)]
    pub until: Option<String>,
    #[schemars(description = "Maximum candidate URLs to return. Default 50, max 250.")]
    #[serde(default)]
    pub max_results: Option<usize>,
    #[schemars(
        description = "Hosts to expand wholesale, e.g. mfa.gov.kg. Archive sources (Wayback) \
                       search by site rather than by text, so they contribute only when this \
                       is given."
    )]
    #[serde(default)]
    pub sites: Option<Vec<String>>,
    #[schemars(
        description = "Search for the subject under its other names and across the years of its \
                       term, not just the query as typed. Best with a person's name: their \
                       aliases, native-script names and term dates come from Wikidata. Slower — \
                       the news index answers one request every five seconds. Default false."
    )]
    #[serde(default)]
    pub expand: Option<bool>,
    #[schemars(description = "With expand: a place the subject's activity is about, added to every variant.")]
    #[serde(default)]
    pub place: Option<String>,
    #[schemars(description = "With expand: how many query variants to run. Default 8, max 20.")]
    #[serde(default)]
    pub max_queries: Option<usize>,
}

impl DiscoverInput {
    fn budget(&self) -> DiscoveryBudget {
        let d = DiscoveryBudget::default();
        DiscoveryBudget {
            max_candidates: self.max_results.unwrap_or(d.max_candidates).clamp(1, 250),
            ..d
        }
    }

    /// Both bounds are optional, but a malformed one is a mistake worth
    /// reporting rather than quietly dropping — a search silently run over all
    /// of time looks like a working search.
    fn window(&self) -> Result<Query, String> {
        for (label, value) in [("since", &self.since), ("until", &self.until)] {
            if let Some(value) = value
                && !(value.len() == 8 && value.chars().all(|c| c.is_ascii_digit()))
            {
                return Err(format!("{label} must be a YYYYMMDD date, got \"{value}\""));
            }
        }
        Ok(Query {
            text: self.query.clone(),
            since: self.since.clone(),
            until: self.until.clone(),
            sites: self.sites.clone().unwrap_or_default(),
        })
    }
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ResearchInput {
    #[schemars(description = "What to look for, as in discover.")]
    pub query: String,
    #[schemars(description = "Earliest date to consider, as YYYYMMDD. Optional.")]
    #[serde(default)]
    pub since: Option<String>,
    #[schemars(description = "Latest date to consider, as YYYYMMDD. Optional.")]
    #[serde(default)]
    pub until: Option<String>,
    #[schemars(description = "Candidate URLs to gather before fetching. Default 50, max 250.")]
    #[serde(default)]
    pub max_results: Option<usize>,
    #[schemars(description = "Pages to actually fetch, best-scoring first. Default 25.")]
    #[serde(default)]
    pub max_pages: Option<usize>,
    #[schemars(description = "Hosts to expand wholesale, as in discover.")]
    #[serde(default)]
    pub sites: Option<Vec<String>>,
    #[schemars(
        description = "How many link hops to follow from each candidate. Default 2 — links are \
                       scored before they are opened, so following them is productive rather \
                       than indiscriminate."
    )]
    #[serde(default)]
    pub depth: Option<u32>,
    #[schemars(description = "Search under the subject's other names and across its term, as in discover. Default false.")]
    #[serde(default)]
    pub expand: Option<bool>,
    #[schemars(description = "With expand: a place the subject's activity is about.")]
    #[serde(default)]
    pub place: Option<String>,
    #[schemars(description = "With expand: how many query variants to run. Default 8, max 20.")]
    #[serde(default)]
    pub max_queries: Option<usize>,
}

impl ResearchInput {
    fn discovery(&self) -> DiscoverInput {
        DiscoverInput {
            query: self.query.clone(),
            since: self.since.clone(),
            until: self.until.clone(),
            max_results: self.max_results,
            sites: self.sites.clone(),
            expand: self.expand,
            place: self.place.clone(),
            max_queries: self.max_queries,
        }
    }

    /// Following links from a federated frontier is only worth doing because
    /// the links are scored first: without a subject to score against, every
    /// hop multiplies the work by whatever a page happens to link to. With one,
    /// the hops are how the crawl reaches the paginated archives no index
    /// covers.
    fn config(&self, focus: Terms) -> CrawlConfig {
        CrawlConfig {
            max_pages: self.max_pages.unwrap_or(25).clamp(1, 1_000),
            max_depth: self.depth.unwrap_or(2).clamp(0, 10),
            focus: Some(focus).filter(|terms| !terms.is_empty()),
            ..CrawlConfig::default()
        }
    }
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct LoginInput {
    #[schemars(description = "The URL to login to")]
    pub url: String,
}

// *** server ***

#[derive(Debug, Clone)]
pub struct Crawler {
    client: Client,
    /// Held on the server rather than built per call, so per-source rate limits
    /// persist between tool invocations — which is when they get breached.
    sources: Arc<Vec<Box<dyn DiscoverySource>>>,
}

impl Crawler {
    pub fn new() -> Self {
        Self {
            client: build_client(Duration::from_secs(DEFAULT_TIMEOUT_SECS))
                .expect("HTTP client configuration is static and always valid"),
            sources: Arc::new(built_in_sources()),
        }
    }

    /// Retrieve HTML for a single URL, via the browser when asked.
    async fn html_for(&self, url: &str, headless: bool) -> Result<String, CrawlError> {
        if headless {
            fetch_page_headless(url).await
        } else {
            let parsed = canonicalize(url)?;
            fetch_page(&self.client, &parsed, CrawlConfig::default().max_body_bytes).await
        }
    }
}

#[tool_router]
impl Crawler {

    #[tool(description = "Crawl a website and return the URLs that were successfully fetched")]
    async fn crawl_site(&self, Parameters(input): Parameters<CrawlInput>) -> String {
        match crawl(&self.client, &input.url, &input.config()).await {
            Ok(outcome) => report(&outcome),
            Err(e) => format!("Crawl failed: {e}"),
        }
    }

    #[tool(description = "Crawl a website, following only links on the same domain")]
    async fn crawl_site_same_domain(&self, Parameters(input): Parameters<CrawlInput>) -> String {
        match crawl_same_domain(&self.client, &input.url, &input.config()).await {
            Ok(outcome) => report(&outcome),
            Err(e) => format!("Crawl failed: {e}"),
        }
    }

    #[tool(description = "Fetch the readable text content of a single URL")]
    async fn fetch_content(&self, Parameters(input): Parameters<FetchInput>) -> String {
        match self.html_for(&input.url, input.headless.unwrap_or(false)).await {
            Ok(html) => cap(extract_text(&html)),
            Err(e) => format!("Failed to fetch {}: {e}", input.url),
        }
    }

    #[tool(description = "Fetch the content of a single URL as markdown")]
    async fn fetch_content_in_md(&self, Parameters(input): Parameters<FetchInput>) -> String {
        match self.html_for(&input.url, input.headless.unwrap_or(false)).await {
            Ok(html) => cap(extract_text_md(&html)),
            Err(e) => format!("Failed to fetch {}: {e}", input.url),
        }
    }

    #[tool(description = "Crawl a website and return pages whose text contains a keyword, with match counts and snippets")]
    async fn search_site_keyword(&self, Parameters(input): Parameters<SearchInput>) -> String {
        match search_site(&self.client, &input.url, &input.keyword, &input.config()).await {
            Ok((matches, outcome)) => {
                let mut out = format!(
                    "{} of {} fetched page(s) mention \"{}\".{}\n\n",
                    matches.len(),
                    outcome.pages.len(),
                    input.keyword,
                    if outcome.budget_hit {
                        " Page budget reached — results are partial."
                    } else {
                        ""
                    }
                );
                for m in &matches {
                    out.push_str(&format!("{}\n  {} hit(s): {}\n\n", m.url, m.hits, m.snippet));
                }
                cap(out)
            }
            Err(e) => format!("Search failed: {e}"),
        }
    }

    #[tool(description = "Extract all links from a single URL")]
    async fn extract_all_links(&self, Parameters(input): Parameters<FetchInput>) -> String {
        let base = match canonicalize(&input.url) {
            Ok(url) => url,
            Err(e) => return format!("Failed to read {}: {e}", input.url),
        };
        match self.html_for(&input.url, input.headless.unwrap_or(false)).await {
            Ok(html) => {
                let links: Vec<String> =
                    extract_links(&html, &base).iter().map(Url::to_string).collect();
                cap(format!("{} link(s)\n\n{}", links.len(), list_urls(&links)))
            }
            Err(e) => format!("Failed to fetch {}: {e}", input.url),
        }
    }

    #[tool(description = "Extract metadata (title, description, author, canonical URL, publish date) from a URL")]
    async fn extract_meta(&self, Parameters(input): Parameters<FetchInput>) -> String {
        match self.html_for(&input.url, input.headless.unwrap_or(false)).await {
            Ok(html) => cap(extract_metadata(&html)),
            Err(e) => format!("Failed to fetch {}: {e}", input.url),
        }
    }

    #[tool(
        description = "Extract the structured data a page already ships in its HTML: schema.org \
                       JSON-LD records, and embedded framework state (__NEXT_DATA__, Nuxt, a \
                       preloaded Redux or Apollo store). This is how to read a client-rendered \
                       page's data without paying for a browser, and it returns records — dates, \
                       names, prices, identifiers — rather than prose."
    )]
    async fn extract_structured_data(&self, Parameters(input): Parameters<FetchInput>) -> String {
        let base = match canonicalize(&input.url) {
            Ok(url) => url,
            Err(e) => return format!("Failed to read {}: {e}", input.url),
        };
        match self.html_for(&input.url, input.headless.unwrap_or(false)).await {
            Ok(html) => cap(structured_report(&structured::extract(&html), &base)),
            Err(e) => format!("Failed to fetch {}: {e}", input.url),
        }
    }

    #[tool(
        description = "List the URLs a site publishes in its sitemap. Far cheaper and far more \
                       complete than crawling, and it is the only way to enumerate a \
                       client-rendered site whose HTML contains no links. Use `contains` to \
                       filter a large index."
    )]
    async fn list_sitemap_urls(&self, Parameters(input): Parameters<SitemapInput>) -> String {
        match sitemap::collect(&self.client, &input.url, &input.config()).await {
            Ok(outcome) => {
                let urls = outcome.urls();
                let mut out = format!(
                    "{} URL(s) from {} sitemap(s).{}{}\n\n",
                    urls.len(),
                    outcome.sitemaps_read,
                    match &input.contains {
                        Some(c) if !c.is_empty() => format!(" Filtered by \"{c}\"."),
                        _ => String::new(),
                    },
                    match (outcome.truncated, outcome.sitemaps_pending) {
                        (true, 0) => " Stopped at the URL budget — raise max_urls for more.".into(),
                        (_, pending) if pending > 0 => format!(
                            " Partial: {pending} sitemap(s) left unread. Raise max_urls and \
                             max_sitemaps, or narrow the search with `contains`."
                        ),
                        _ => String::new(),
                    }
                );
                // lastmod tells the model which pages are worth revisiting.
                let lines: Vec<String> = outcome
                    .entries
                    .iter()
                    .map(|e| match &e.lastmod {
                        Some(when) => format!("{}  [{when}]", e.url),
                        None => e.url.to_string(),
                    })
                    .collect();
                out.push_str(&list_urls(&lines));

                if !outcome.failed.is_empty() {
                    out.push_str("\n\nFailed:\n");
                    for (url, err) in outcome.failed.iter().take(20) {
                        out.push_str(&format!("{url} — {err}\n"));
                    }
                }
                cap(out)
            }
            Err(e) => format!("Could not read sitemap: {e}"),
        }
    }

    #[tool(
        description = "Find candidate URLs about a topic when you have no seed URL to start \
                       from. Fans the query across federated discovery sources — currently \
                       GDELT's worldwide multilingual news index — and returns ranked URLs with \
                       the source that found each one. Use it before the crawl tools, then feed \
                       the URLs you want into fetch_content, extract_structured_data or \
                       crawl_site."
    )]
    async fn discover(&self, Parameters(input): Parameters<DiscoverInput>) -> String {
        let (outcome, note) = match run_discovery(&self.client, &self.sources, &input).await {
            Ok(done) => done,
            Err(e) => return e,
        };
        cap(format!("{note}{}", discovery_report(&outcome)))
    }

    #[tool(
        description = "Discover candidate URLs for a topic and fetch them in one step: federates \
                       the query across discovery sources, merges the results into a single \
                       crawl frontier best-scoring first, and fetches within one page budget. \
                       Use when you want the pages themselves rather than a list to triage."
    )]
    async fn discover_and_crawl(&self, Parameters(input): Parameters<ResearchInput>) -> String {
        let discovery = input.discovery();
        let (outcome, note) = match run_discovery(&self.client, &self.sources, &discovery).await {
            Ok(done) => done,
            Err(e) => return e,
        };
        let failures = outcome.failures.clone();

        // The query, plus every name discovery learned for the same subject.
        // This is what lets a link whose anchor text is in the subject's own
        // language score above zero.
        let mut names = vec![input.query.clone()];
        names.extend(outcome.terms.clone());
        let focus = Terms::new(&names);
        let aliases = names.len() - 1;

        let seeds = outcome.seeds();
        if seeds.is_empty() {
            return format!("No candidate URLs for \"{}\".", input.query);
        }

        let found = seeds.len();
        match crawl_from(&self.client, seeds, &input.config(focus)).await {
            Ok(crawled) => {
                let mut out = note;
                out.push_str(&format!(
                    "{found} candidate(s) discovered; scoring links against {} name(s) for the \
                     subject.\n",
                    aliases + 1
                ));
                for (name, why) in &failures {
                    out.push_str(&format!("{name} failed: {why}\n"));
                }
                out.push('\n');
                out.push_str(&report(&crawled));
                cap(out)
            }
            Err(e) => format!("Discovery found {found} URL(s) but the crawl failed: {e}"),
        }
    }

    #[tool(
        description = "Login to a website using credentials from passmanager. The master password is read from the MCPCRAWLER_MASTER_PASSWORD environment variable, never passed as an argument."
    )]
    async fn login_to_site(&self, Parameters(input): Parameters<LoginInput>) -> String {
        let Ok(master_password) = std::env::var("MCPCRAWLER_MASTER_PASSWORD") else {
            return "MCPCRAWLER_MASTER_PASSWORD is not set. Set it in the server's environment \
                    so the master password never passes through the model's context."
                .to_string();
        };

        match crate::passmanager::get_credential(&input.url, &master_password).await {
            Some((username, password)) => match login_and_fetch(&input.url, &username, &password).await {
                Ok(html) => cap(extract_text(&html)),
                Err(e) => format!("Login failed: {e}"),
            },
            None => format!("No credentials found for {}", input.url),
        }
    }
}

#[tool_handler]
impl ServerHandler for Crawler {
    fn get_info(&self) -> ServerInfo {
        // Without this the client is told it is talking to "rmcp", because the
        // SDK's own build environment is what `Implementation::default()` reads.
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(
                env!("CARGO_PKG_NAME"),
                env!("CARGO_PKG_VERSION"),
            ))
            .with_instructions(
            "A web crawler. Crawls are bounded by a page budget (max_pages), a per-host \
             rate limit, and robots.txt — depth is only a guard rail, not the main control. \
             Tools report fetched and failed URLs separately.",
        )
    }
}
