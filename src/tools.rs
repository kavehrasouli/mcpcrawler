use crate::crawler::{
    CrawlConfig, CrawlError, CrawlOutcome, build_client, canonicalize, crawl, crawl_same_domain,
    extract_links, extract_metadata, extract_text, extract_text_md, fetch_page,
    fetch_page_headless, login_and_fetch, search_site,
};
use reqwest::Client;
use rmcp::{
    ServerHandler,
    model::{ServerCapabilities, ServerInfo},
    schemars, tool,
};
use serde::Deserialize;
use std::time::Duration;
use url::Url;

/// Ceiling on any single tool response. A large crawl used to return every URL
/// joined into one string, which can be megabytes of model context.
const MAX_OUTPUT_CHARS: usize = 60_000;
/// Ceiling on how many URLs are listed before the rest are summarized.
const MAX_LISTED: usize = 400;

const DEFAULT_TIMEOUT_SECS: u64 = 15;

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

fn report(outcome: &CrawlOutcome) -> String {
    let urls = outcome.urls();
    let mut out = format!(
        "Fetched {} page(s); {} failed; {} skipped by robots.txt.{}\n\n",
        urls.len(),
        outcome.failed.len(),
        outcome.robots_skipped,
        if outcome.budget_hit {
            " Page budget reached — results are partial."
        } else {
            ""
        }
    );

    out.push_str(&list_urls(&urls));

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
pub struct LoginInput {
    #[schemars(description = "The URL to login to")]
    pub url: String,
}

// *** server ***

#[derive(Debug, Clone)]
pub struct Crawler {
    client: Client,
}

impl Crawler {
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

#[tool(tool_box)]
impl Crawler {
    pub fn new() -> Self {
        Self {
            client: build_client(Duration::from_secs(DEFAULT_TIMEOUT_SECS))
                .expect("HTTP client configuration is static and always valid"),
        }
    }

    #[tool(description = "Crawl a website and return the URLs that were successfully fetched")]
    async fn crawl_site(&self, #[tool(aggr)] input: CrawlInput) -> String {
        match crawl(&self.client, &input.url, &input.config()).await {
            Ok(outcome) => report(&outcome),
            Err(e) => format!("Crawl failed: {e}"),
        }
    }

    #[tool(description = "Crawl a website, following only links on the same domain")]
    async fn crawl_site_same_domain(&self, #[tool(aggr)] input: CrawlInput) -> String {
        match crawl_same_domain(&self.client, &input.url, &input.config()).await {
            Ok(outcome) => report(&outcome),
            Err(e) => format!("Crawl failed: {e}"),
        }
    }

    #[tool(description = "Fetch the readable text content of a single URL")]
    async fn fetch_content(&self, #[tool(aggr)] input: FetchInput) -> String {
        match self.html_for(&input.url, input.headless.unwrap_or(false)).await {
            Ok(html) => cap(extract_text(&html)),
            Err(e) => format!("Failed to fetch {}: {e}", input.url),
        }
    }

    #[tool(description = "Fetch the content of a single URL as markdown")]
    async fn fetch_content_in_md(&self, #[tool(aggr)] input: FetchInput) -> String {
        match self.html_for(&input.url, input.headless.unwrap_or(false)).await {
            Ok(html) => cap(extract_text_md(&html)),
            Err(e) => format!("Failed to fetch {}: {e}", input.url),
        }
    }

    #[tool(description = "Crawl a website and return pages whose text contains a keyword, with match counts and snippets")]
    async fn search_site_keyword(&self, #[tool(aggr)] input: SearchInput) -> String {
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
    async fn extract_all_links(&self, #[tool(aggr)] input: FetchInput) -> String {
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
    async fn extract_meta(&self, #[tool(aggr)] input: FetchInput) -> String {
        match self.html_for(&input.url, input.headless.unwrap_or(false)).await {
            Ok(html) => cap(extract_metadata(&html)),
            Err(e) => format!("Failed to fetch {}: {e}", input.url),
        }
    }

    #[tool(
        description = "Login to a website using credentials from passmanager. The master password is read from the MCPCRAWLER_MASTER_PASSWORD environment variable, never passed as an argument."
    )]
    async fn login_to_site(&self, #[tool(aggr)] input: LoginInput) -> String {
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

#[tool(tool_box)]
impl ServerHandler for Crawler {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            instructions: Some(
                "A web crawler. Crawls are bounded by a page budget (max_pages), a per-host \
                 rate limit, and robots.txt — depth is only a guard rail, not the main control. \
                 Tools report fetched and failed URLs separately."
                    .into(),
            ),
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            ..Default::default()
        }
    }
}
