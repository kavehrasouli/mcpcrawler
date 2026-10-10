use crate::crawler::{
    CrawlConfig, CrawlError, CrawlOutcome, Origin, Render, Seed, build_client, canonicalize, crawl,
    crawl_from, crawl_same_domain,
    extract_links, extract_metadata, extract_text, extract_text_md, fetch_page,
    fetch_page_headless, login_and_fetch, search_site,
};
use reqwest::Client;
use crate::reply::{fail, fail_with, ok, ok_with};
use rmcp::{
    ServerHandler,
    handler::server::wrapper::Parameters,
    model::{CallToolResult, Implementation, ServerCapabilities, ServerInfo},
    schemars, tool, tool_handler, tool_router,
};
use crate::discovery::{DiscoveryBudget, DiscoverySource, Federated, Query, federate};
use crate::dedup::{self, Group};
use crate::expand;
use crate::scoring::Terms;
use crate::sitemap::{self, SitemapConfig};
use crate::sources::{
    brave::Brave, commoncrawl::CommonCrawl, gdelt::Gdelt, searxng::SearxNg, wayback::Wayback,
    wikidata::Wikidata,
};
use crate::structured::{self, StructuredData};
use serde::Deserialize;
use serde_json::{Value, json};
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
    let terms = Terms::new(about);
    Some(terms).filter(|t| !t.is_empty())
}

fn built_in_sources() -> Vec<Box<dyn DiscoverySource>> {
    let mut sources: Vec<Box<dyn DiscoverySource>> = vec![
        Box::new(Gdelt::new()),
        Box::new(Wikidata::new()),
        Box::new(Wayback::new()),
        Box::new(CommonCrawl::new()),
    ];
    if let Some(brave) = Brave::from_env() {
        sources.push(Box::new(brave));
    }
    // A bad URL was already reported at startup, by `config::validate`.
    if let Ok(Some(searx)) = SearxNg::from_env() {
        sources.push(Box::new(searx));
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
) -> Result<(Federated, String), CallToolResult> {
    let query = input.window().map_err(|e| fail("bad_input", e, None))?;
    let budget = input.budget();
    if input.expand != Some(true) {
        return Ok((federate(client, sources, &query, &budget).await, String::new()));
    }

    let max = input.max_queries.unwrap_or(expand::DEFAULT_QUERIES);
    let done = expand::research(client, sources, &query, input.place.as_deref(), max, &budget).await;
    let bound = |d: &Option<String>| d.clone().unwrap_or_else(|| "open".to_string());
    let planned = done.queries.len();
    let mut note = if done.ran == 0 && planned > 0 {
        // Said plainly: a list of queries that reads as if they were run is
        // the same as claiming the results came from them.
        format!(
            "Expansion planned {planned} more quer{} but none ran: every text-search source had \
             already failed.\n",
            if planned == 1 { "y" } else { "ies" }
        )
    } else {
        format!(
            "Expanded: {} of {planned} planned quer{} ran, over {}–{}, plus the original.\n",
            done.ran,
            if planned == 1 { "y" } else { "ies" },
            bound(&done.window.from),
            bound(&done.window.to),
        )
    };
    for q in done.queries.iter().take(done.ran) {
        note.push_str(&format!("  - {} [{}–{}]\n", q.text, bound(&q.since), bound(&q.until)));
    }
    note.push('\n');
    Ok((done.federated, note))
}

fn discovery_report(outcome: &Federated) -> String {
    let mut out = String::new();
    if let Some(subject) = &outcome.subject {
        out.push_str(&format!("Resolved the query to {subject}.\n"));
    }
    out.push_str(&format!(
        "{} candidate URL(s).{}\n\n",
        outcome.leads.len(),
        match outcome.failures.len() {
            0 => String::new(),
            n => format!(" {n} source(s) failed — results are partial."),
        }
    ));

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
        for value in [&lead.domain, &lead.language, &lead.seen].into_iter().flatten() {
            detail.push(value.clone());
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

/// `20240305` -> `2024-03-05`.
fn iso(date: &str) -> String {
    format!("{}-{}-{}", &date[..4], &date[4..6], &date[6..8])
}

/// ` — also on a.com, b.com (+3)`: the other hosts carrying the same story,
/// which is itself the evidence that it was syndicated. Hosts, not copies:
/// three copies on one site are one outlet repeating itself, and say so.
fn copies_note(outcome: &CrawlOutcome, group: &Group) -> String {
    if group.copies.is_empty() {
        return String::new();
    }
    let hosts = dedup::story_hosts(&outcome.pages, group);
    let others = &hosts[hosts.len().min(1)..];
    if others.is_empty() {
        return format!(" — {} more cop{} on the same host", group.copies.len(),
            if group.copies.len() == 1 { "y" } else { "ies" });
    }
    let mut note = format!(" — also on {}", others[..others.len().min(3)].join(", "));
    if others.len() > 3 {
        note.push_str(&format!(" (+{})", others.len() - 3));
    }
    note
}

#[cfg(test)]
pub(crate) fn report(outcome: &CrawlOutcome) -> String {
    report_with(outcome, None)
}

/// What to excerpt for: the subject, and optionally a place that makes a
/// sentence worth more when it appears alongside.
pub(crate) struct Excerpts<'a> {
    pub subject: &'a Terms,
    pub near: Option<&'a Terms>,
}

/// Most stories listed when each carries an excerpt. A hundred paragraphs is
/// not a list a model can read; they are best-first, so the cut loses the
/// least relevant.
const MAX_EXCERPTED: usize = 25;

/// As [`report`], and with `Some(terms)` each story is followed by the lines
/// that mention the subject.
pub(crate) fn report_with(outcome: &CrawlOutcome, excerpts: Option<&Excerpts>) -> String {
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
        match (outcome.budget_hit, outcome.deadline_hit) {
            (_, true) => " Time limit reached — results are partial.",
            (true, false) => " Page budget reached — results are partial.",
            _ => "",
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
    let year = expand::this_year() + 1;
    let dates: Vec<Option<String>> =
        groups.iter().map(|g| dedup::story_date(&outcome.pages, g, year)).collect();
    let dated: Vec<&String> = dates.iter().flatten().collect();
    if let (Some(first), Some(last)) = (dated.iter().min(), dated.iter().max()) {
        out.push_str(&format!(
            "{} of {} stories carry a publish date, from {} to {}.\n",
            dated.len(),
            groups.len(),
            iso(first),
            iso(last)
        ));
    }

    let listed = shown.len().min(if excerpts.is_some() { MAX_EXCERPTED } else { MAX_LISTED });
    for group in &shown[..listed] {
        let page = &outcome.pages[group.best];
        if focused {
            out.push_str(&format!("[{:.2}] ", page.relevance));
        }
        out.push_str(page.url.as_str());
        if let Some(from) = &page.redirected_from {
            out.push_str(&format!(" (redirected from {from})"));
        }
        let at = groups.iter().position(|g| std::ptr::eq(g, *group));
        if let Some(date) = at.and_then(|i| dates[i].as_deref()) {
            out.push_str(&format!(" ({})", iso(date)));
        }
        out.push_str(&copies_note(outcome, group));
        out.push('\n');
        if let Some(wanted) = excerpts
            && let Some(lines) = wanted.subject.excerpt(&extract_text(&page.html), wanted.near)
        {
            out.push_str(&format!("    {lines}\n"));
        }
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

/// The same facts as the report, as data: final URLs, whether the result is
/// partial, and what was not fetched.
fn crawl_data(outcome: &CrawlOutcome) -> Value {
    json!({
        "pages_total": outcome.pages.len(),
        "pages": outcome.pages.iter().take(MAX_LISTED).map(|p| json!({
            "url": p.url.as_str(),
            "redirected_from": p.redirected_from.as_ref().map(Url::as_str),
            "relevance": p.relevance,
            "origin": p.origin.kind(),
        })).collect::<Vec<_>>(),
        "failed_total": outcome.failed.len(),
        "failed": outcome.failed.iter().take(50)
            .map(|(url, error)| json!({ "url": url, "error": error }))
            .collect::<Vec<_>>(),
        "robots_skipped": outcome.robots_skipped,
        "budget_hit": outcome.budget_hit,
        "deadline_hit": outcome.deadline_hit,
        "retries": outcome.retries,
        "partial": outcome.budget_hit || outcome.deadline_hit,
    })
}

/// Apply the limits every crawl-shaped tool shares.
fn apply_run_limits(
    cfg: &mut CrawlConfig,
    max_seconds: Option<u64>,
    render: Option<&str>,
) -> Result<(), String> {
    if let Some(seconds) = max_seconds {
        cfg.deadline = Some(Duration::from_secs(seconds.clamp(1, 1_800)));
    }
    cfg.render = match render.map(|r| r.trim().to_ascii_lowercase()).as_deref() {
        None | Some("") | Some("never") => Render::Never,
        Some("auto") => Render::Auto,
        Some("always") => Render::Always,
        Some(other) => {
            return Err(format!("render must be never, auto or always, got \"{other}\""));
        }
    };
    Ok(())
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
    #[schemars(
        description = "Wall-clock limit for the whole crawl, in seconds. A request still in \
                       flight when it passes is abandoned. Default 300, max 1800."
    )]
    #[serde(default)]
    pub max_seconds: Option<u64>,
    #[schemars(
        description = "Use the headless browser: never (default), auto (only for a page that \
                       comes back with no links and almost no text — a client-rendered shell), \
                       or always. A browser tab costs far more than a request."
    )]
    #[serde(default)]
    pub render: Option<String>,
}

impl CrawlInput {
    fn config(&self) -> Result<CrawlConfig, String> {
        let d = CrawlConfig::default();
        let mut cfg = CrawlConfig {
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
        };
        apply_run_limits(&mut cfg, self.max_seconds, self.render.as_deref())?;
        Ok(cfg)
    }
}

/// A set of URLs to read together, typically a model's own search results.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct UrlsInput {
    #[schemars(
        description = "The URLs to read, up to 200 — typically the results of a web search. \
                       They are read as one set: one page budget, one rate limiter, and \
                       near-duplicate copies of a story collapsed across all of them."
    )]
    pub urls: Vec<String>,
    #[schemars(
        description = "What you are looking for. Used to score any links followed and to pick \
                       the excerpts."
    )]
    #[serde(default)]
    pub about: Option<String>,
    #[schemars(
        description = "Under each story, the sentences that mention `about`. At most 25 stories. \
                       Default false."
    )]
    #[serde(default)]
    pub excerpts: Option<bool>,
    #[schemars(description = "Link hops from each URL. Default 0: read exactly these pages.")]
    #[serde(default)]
    pub depth: Option<u32>,
    #[schemars(description = "Pages to fetch in total. Default 50.")]
    #[serde(default)]
    pub max_pages: Option<usize>,
    #[schemars(description = "Follow links only within the hosts of the given URLs. Default true.")]
    #[serde(default)]
    pub same_domain_only: Option<bool>,
    #[schemars(description = "Wall-clock limit in seconds. Default 300, max 1800.")]
    #[serde(default)]
    pub max_seconds: Option<u64>,
    #[schemars(description = "never (default), auto or always: use the headless browser.")]
    #[serde(default)]
    pub render: Option<String>,
}

/// More than this and the set is a crawl, which has a tool of its own.
const MAX_URLS_PER_CALL: usize = 200;

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
    #[schemars(
        description = "Which post the question is about, as a word from its English name \
                       (\"president\"). Someone who held several offices has a term for each; \
                       this bounds the search to the matching one instead of their whole career."
    )]
    #[serde(default)]
    pub post: Option<String>,
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
            post: self.post.clone().filter(|p| !p.trim().is_empty()),
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
    #[schemars(
        description = "Under each story, include the lines of the page that mention the \
                       subject, so dates, places and who was present can be read without \
                       fetching the page again. Lists at most 25 stories. Default false."
    )]
    #[serde(default)]
    pub excerpts: Option<bool>,
    #[schemars(description = "Which post the question is about, as in discover.")]
    #[serde(default)]
    pub post: Option<String>,
    #[schemars(description = "Wall-clock limit for the crawl stage in seconds. Default 300, max 1800.")]
    #[serde(default)]
    pub max_seconds: Option<u64>,
    #[schemars(description = "never (default), auto or always: use the headless browser.")]
    #[serde(default)]
    pub render: Option<String>,
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
            post: self.post.clone(),
        }
    }

    /// Following links from a federated frontier is only worth doing because
    /// the links are scored first: without a subject to score against, every
    /// hop multiplies the work by whatever a page happens to link to. With one,
    /// the hops are how the crawl reaches the paginated archives no index
    /// covers.
    fn config(&self, focus: Terms) -> Result<CrawlConfig, String> {
        let mut cfg = CrawlConfig {
            max_pages: self.max_pages.unwrap_or(25).clamp(1, 1_000),
            max_depth: self.depth.unwrap_or(2).clamp(0, 10),
            focus: Some(focus).filter(|terms| !terms.is_empty()),
            ..CrawlConfig::default()
        };
        apply_run_limits(&mut cfg, self.max_seconds, self.render.as_deref())?;
        Ok(cfg)
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
    /// Fails, rather than panics, when the HTTP stack cannot start — a missing
    /// TLS backend, say — so the process can say why on its way out.
    pub fn new() -> Result<Self, reqwest::Error> {
        Ok(Self {
            client: build_client(Duration::from_secs(DEFAULT_TIMEOUT_SECS))?,
            sources: Arc::new(built_in_sources()),
        })
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
    async fn crawl_site(&self, Parameters(input): Parameters<CrawlInput>) -> CallToolResult {
        let cfg = match input.config() {
            Ok(cfg) => cfg,
            Err(e) => return fail("bad_input", e, None),
        };
        match crawl(&self.client, &input.url, &cfg).await {
            Ok(outcome) => crawl_reply(&outcome, None, String::new()),
            Err(e) => fail_with(&e, "Crawl failed", Some(&input.url)),
        }
    }

    #[tool(description = "Crawl a website, following only links on the same domain")]
    async fn crawl_site_same_domain(&self, Parameters(input): Parameters<CrawlInput>) -> CallToolResult {
        let cfg = match input.config() {
            Ok(cfg) => cfg,
            Err(e) => return fail("bad_input", e, None),
        };
        match crawl_same_domain(&self.client, &input.url, &cfg).await {
            Ok(outcome) => crawl_reply(&outcome, None, String::new()),
            Err(e) => fail_with(&e, "Crawl failed", Some(&input.url)),
        }
    }

    #[tool(
        description = "Read a set of URLs together — typically the results of your own web \
                       search. They share one page budget and one rate limiter, near-duplicate \
                       copies of the same story are collapsed across all of them, each story is \
                       dated, and the report names the independent hosts that carried it. Give \
                       `about` and `excerpts` to get, under each story, the sentences that \
                       mention your subject. Use this after searching; use discover when you \
                       have no URLs yet."
    )]
    async fn crawl_urls(&self, Parameters(input): Parameters<UrlsInput>) -> CallToolResult {
        if input.urls.is_empty() {
            return fail("bad_input", "urls is empty".to_string(), None);
        }
        if input.urls.len() > MAX_URLS_PER_CALL {
            return fail(
                "bad_input",
                format!("{} URLs given; the limit is {MAX_URLS_PER_CALL}", input.urls.len()),
                None,
            );
        }

        let mut seeds = Vec::new();
        let mut rejected: Vec<(String, String)> = Vec::new();
        for raw in &input.urls {
            match canonicalize(raw) {
                Ok(url) => seeds.push(Seed::new(url, Origin::Seed)),
                Err(e) => rejected.push((raw.clone(), e.to_string())),
            }
        }
        if seeds.is_empty() {
            return fail("bad_input", "none of the URLs could be used".to_string(), None);
        }

        let focus = Terms::new(input.about.iter());
        let mut cfg = CrawlConfig {
            max_pages: input.max_pages.unwrap_or(50).clamp(1, 1_000),
            max_depth: input.depth.unwrap_or(0).clamp(0, 10),
            same_domain_only: input.same_domain_only.unwrap_or(true),
            focus: Some(focus.clone()).filter(|t| !t.is_empty()),
            ..CrawlConfig::default()
        };
        if let Err(e) = apply_run_limits(&mut cfg, input.max_seconds, input.render.as_deref()) {
            return fail("bad_input", e, None);
        }

        let mut note = String::new();
        for (raw, why) in &rejected {
            note.push_str(&format!("Skipped {raw}: {why}\n"));
        }
        match crawl_from(&self.client, seeds, &cfg).await {
            Ok(outcome) => {
                let wanted = (input.excerpts == Some(true) && !focus.is_empty())
                    .then_some(Excerpts { subject: &focus, near: None });
                crawl_reply(&outcome, wanted.as_ref(), note)
            }
            Err(e) => fail_with(&e, "Crawl failed", None),
        }
    }

    #[tool(description = "Fetch the readable text content of a single URL")]
    async fn fetch_content(&self, Parameters(input): Parameters<FetchInput>) -> CallToolResult {
        match self.html_for(&input.url, input.headless.unwrap_or(false)).await {
            Ok(html) => ok(cap(extract_text(&html))),
            Err(e) => fail_with(&e, "Failed to fetch", Some(&input.url)),
        }
    }

    #[tool(description = "Fetch the content of a single URL as markdown")]
    async fn fetch_content_in_md(&self, Parameters(input): Parameters<FetchInput>) -> CallToolResult {
        match self.html_for(&input.url, input.headless.unwrap_or(false)).await {
            Ok(html) => ok(cap(extract_text_md(&html))),
            Err(e) => fail_with(&e, "Failed to fetch", Some(&input.url)),
        }
    }

    #[tool(description = "Crawl a website and return pages whose text contains a keyword, with match counts and snippets")]
    async fn search_site_keyword(&self, Parameters(input): Parameters<SearchInput>) -> CallToolResult {
        match search_site(&self.client, &input.url, &input.keyword, &input.config()).await {
            Ok((matches, outcome)) => {
                let mut out = format!(
                    "{} of {} fetched page(s) mention \"{}\".{}\n\n",
                    matches.len(),
                    outcome.pages.len(),
                    input.keyword,
                    match (outcome.budget_hit, outcome.deadline_hit) {
                        (_, true) => " Time limit reached — results are partial.",
                        (true, false) => " Page budget reached — results are partial.",
                        _ => "",
                    }
                );
                for m in &matches {
                    out.push_str(&format!("{}\n  {} hit(s): {}\n\n", m.url, m.hits, m.snippet));
                }
                let data = json!({
                    "matches": matches.iter().map(|m| json!({
                        "url": m.url.to_string(), "hits": m.hits, "snippet": m.snippet,
                    })).collect::<Vec<_>>(),
                    "pages_fetched": outcome.pages.len(),
                    "partial": outcome.budget_hit || outcome.deadline_hit,
                });
                ok_with(cap(out), data)
            }
            Err(e) => fail_with(&e, "Search failed", Some(&input.url)),
        }
    }

    #[tool(description = "Extract all links from a single URL")]
    async fn extract_all_links(&self, Parameters(input): Parameters<FetchInput>) -> CallToolResult {
        let base = match canonicalize(&input.url) {
            Ok(url) => url,
            Err(e) => return fail_with(&e, "Failed to read", Some(&input.url)),
        };
        match self.html_for(&input.url, input.headless.unwrap_or(false)).await {
            Ok(html) => {
                let links: Vec<String> =
                    extract_links(&html, &base).iter().map(Url::to_string).collect();
                ok_with(
                    cap(format!("{} link(s)\n\n{}", links.len(), list_urls(&links))),
                    json!({ "links_total": links.len(), "links": links.iter().take(MAX_LISTED).collect::<Vec<_>>() }),
                )
            }
            Err(e) => fail_with(&e, "Failed to fetch", Some(&input.url)),
        }
    }

    #[tool(description = "Extract metadata (title, description, author, canonical URL, publish date) from a URL")]
    async fn extract_meta(&self, Parameters(input): Parameters<FetchInput>) -> CallToolResult {
        match self.html_for(&input.url, input.headless.unwrap_or(false)).await {
            Ok(html) => ok(cap(extract_metadata(&html))),
            Err(e) => fail_with(&e, "Failed to fetch", Some(&input.url)),
        }
    }

    #[tool(
        description = "Extract the structured data a page already ships in its HTML: schema.org \
                       JSON-LD records, and embedded framework state (__NEXT_DATA__, Nuxt, a \
                       preloaded Redux or Apollo store). This is how to read a client-rendered \
                       page's data without paying for a browser, and it returns records — dates, \
                       names, prices, identifiers — rather than prose."
    )]
    async fn extract_structured_data(&self, Parameters(input): Parameters<FetchInput>) -> CallToolResult {
        let base = match canonicalize(&input.url) {
            Ok(url) => url,
            Err(e) => return fail_with(&e, "Failed to read", Some(&input.url)),
        };
        match self.html_for(&input.url, input.headless.unwrap_or(false)).await {
            Ok(html) => ok(cap(structured_report(&structured::extract(&html), &base))),
            Err(e) => fail_with(&e, "Failed to fetch", Some(&input.url)),
        }
    }

    #[tool(
        description = "List the URLs a site publishes in its sitemap. Far cheaper and far more \
                       complete than crawling, and it is the only way to enumerate a \
                       client-rendered site whose HTML contains no links. Use `contains` to \
                       filter a large index."
    )]
    async fn list_sitemap_urls(&self, Parameters(input): Parameters<SitemapInput>) -> CallToolResult {
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
                    if outcome.truncated { " Stopped at a limit — results are partial." } else { "" },
                );
                out.push_str(&list_urls(&urls));
                if outcome.sitemaps_pending > 0 {
                    out.push_str(&format!(
                        "\n\n{} further sitemap(s) were found but not read.",
                        outcome.sitemaps_pending
                    ));
                }
                if !outcome.failed.is_empty() {
                    out.push_str("\n\nFailed:\n");
                    for (url, err) in outcome.failed.iter().take(20) {
                        out.push_str(&format!("{url} — {err}\n"));
                    }
                }
                let data = json!({
                    "urls_total": urls.len(),
                    "urls": outcome.entries.iter().take(MAX_LISTED)
                        .map(|e| json!({ "url": e.url.as_str(), "lastmod": e.lastmod }))
                        .collect::<Vec<_>>(),
                    "sitemaps_read": outcome.sitemaps_read,
                    "sitemaps_pending": outcome.sitemaps_pending,
                    "failed": outcome.failed.iter().take(20)
                        .map(|(url, error)| json!({ "url": url, "error": error })).collect::<Vec<_>>(),
                    "partial": outcome.truncated,
                });
                ok_with(cap(out), data)
            }
            Err(e) => fail_with(&e, "Could not read sitemap", Some(&input.url)),
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
    async fn discover(&self, Parameters(input): Parameters<DiscoverInput>) -> CallToolResult {
        let (outcome, note) = match run_discovery(&self.client, &self.sources, &input).await {
            Ok(done) => done,
            Err(e) => return e,
        };
        let data = json!({
            "candidates": outcome.leads.iter().map(|l| json!({
                "url": l.url.as_str(), "score": l.score, "title": l.title,
                "sources": l.sources.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            })).collect::<Vec<_>>(),
            "failures": outcome.failures.iter()
                .map(|(name, why)| json!({ "source": name, "error": why })).collect::<Vec<_>>(),
            "partial": !outcome.failures.is_empty(),
        });
        ok_with(cap(format!("{note}{}", discovery_report(&outcome))), data)
    }

    #[tool(
        description = "Discover candidate URLs for a topic and fetch them in one step: federates \
                       the query across discovery sources, merges the results into a single \
                       crawl frontier best-scoring first, and fetches within one page budget. \
                       Use when you want the pages themselves rather than a list to triage."
    )]
    async fn discover_and_crawl(&self, Parameters(input): Parameters<ResearchInput>) -> CallToolResult {
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
        let place_terms = input.place.as_deref().map(|p| Terms::new([p])).filter(|t| !t.is_empty());
        let excerpt_terms = (input.excerpts == Some(true)).then(|| (focus.clone(), place_terms));
        let aliases = names.len() - 1;

        let seeds = outcome.seeds();
        if seeds.is_empty() {
            let why = if failures.is_empty() {
                format!("No candidate URLs for \"{}\".", input.query)
            } else {
                format!(
                    "No candidate URLs for \"{}\": {} source(s) failed.",
                    input.query,
                    failures.len()
                )
            };
            return fail("no_candidates", format!("{note}{why}"), None);
        }

        let cfg = match input.config(focus) {
            Ok(cfg) => cfg,
            Err(e) => return fail("bad_input", e, None),
        };
        let found = seeds.len();
        match crawl_from(&self.client, seeds, &cfg).await {
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
                let wanted = excerpt_terms
                    .as_ref()
                    .map(|(subject, near)| Excerpts { subject, near: near.as_ref() });
                crawl_reply(&crawled, wanted.as_ref(), out)
            }
            Err(e) => fail_with(&e, &format!("Discovery found {found} URL(s) but the crawl failed"), None),
        }
    }

    #[tool(
        description = "Login to a website using credentials from passmanager. The master password is read from the MCPCRAWLER_MASTER_PASSWORD environment variable, never passed as an argument. The session lasts for this call only: nothing is kept for later calls."
    )]
    async fn login_to_site(&self, Parameters(input): Parameters<LoginInput>) -> CallToolResult {
        let Ok(master_password) = std::env::var("MCPCRAWLER_MASTER_PASSWORD") else {
            return fail(
                "credentials_unconfigured",
                "MCPCRAWLER_MASTER_PASSWORD is not set. Set it in the server's environment so \
                 the master password never passes through the model's context."
                    .to_string(),
                None,
            );
        };

        match crate::passmanager::get_credential(&input.url, &master_password).await {
            Ok((username, password)) => {
                match login_and_fetch(&input.url, &username, &password).await {
                    Ok(html) => ok(cap(extract_text(&html))),
                    Err(e) => fail_with(&e, "Login failed", Some(&input.url)),
                }
            }
            Err(e) => fail(e.code(), e.to_string(), Some(&input.url)),
        }
    }

    #[tool(
        description = "Counters for this server since it started: pages fetched, failures by \
                       class, retries, redirects followed and refused, robots.txt skips, and \
                       crawls that stopped at their page or time budget."
    )]
    async fn crawler_stats(&self) -> CallToolResult {
        ok(crate::metrics::snapshot())
    }
}

/// A crawl's report as a tool result: prose for the model, the same facts as
/// data for a client.
fn crawl_reply(outcome: &CrawlOutcome, excerpts: Option<&Excerpts>, preface: String) -> CallToolResult {
    let text = cap(format!("{preface}{}", report_with(outcome, excerpts)));
    ok_with(text, crawl_data(outcome))
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
            "A web crawler. Crawls are bounded by a page budget (max_pages), a time limit \
             (max_seconds), a per-host rate limit, and robots.txt — depth is only a guard \
             rail, not the main control. Tools report fetched and failed URLs separately, \
             and failures set isError with a stable code. Schema version 2.",
        )
    }
}
