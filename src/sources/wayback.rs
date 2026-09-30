//! Wayback CDX — what the live web no longer has.
//!
//! The reason this source exists is the page that was deleted in 2019. A
//! ministry reorganises its site, a news outlet purges its archive, a
//! government changes and the previous administration's press releases stop
//! resolving. None of that is reachable by crawling, and none of it is in a
//! search index, but the Internet Archive kept a copy and will list every
//! capture it has for a host.
//!
//! It searches by **URL pattern, not by text** — there is no keyword to give
//! it. So it contributes only once something upstream has named a site, which
//! is `Query::sites`, and is why Wikidata's official-website claim matters
//! beyond being one more link.

use crate::api::fetch_json;
use crate::crawler::{CrawlError, canonicalize};
use crate::discovery::{DiscoveryBudget, DiscoverySource, Found, Lead, Query};
use async_trait::async_trait;
use reqwest::Client;
use serde_json::Value;
use std::sync::Arc;
use url::Url;

const ENDPOINT: &str = "https://web.archive.org/cdx/search/cdx";
const REPLAY: &str = "https://web.archive.org/web";
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

/// Older captures are not worse, so rank cannot mean what it means elsewhere.
/// Every capture gets the same middling score and lets corroboration and the
/// crawler's own scoring sort it out.
const CAPTURE_SCORE: f32 = 0.5;

#[derive(Debug)]
pub struct Wayback {
    endpoint: Url,
}

impl Wayback {
    pub fn new() -> Self {
        Self::at(ENDPOINT).expect("the built-in Wayback endpoint is a valid URL")
    }

    pub fn at(endpoint: &str) -> Result<Self, CrawlError> {
        Ok(Self { endpoint: canonicalize(endpoint)? })
    }

    fn request_url(&self, site: &str, query: &Query, per_site: usize) -> Url {
        let mut url = self.endpoint.clone();
        url.query_pairs_mut()
            .append_pair("url", &format!("{site}*"))
            .append_pair("output", "json")
            .append_pair("limit", &per_site.to_string())
            // One row per distinct URL, its HTML captures only, and only the
            // ones that actually returned a page. Without these the answer is
            // mostly JavaScript files and redirects.
            .append_pair("collapse", "urlkey")
            .append_pair("filter", "statuscode:200")
            .append_pair("filter", "mimetype:text/html");

        if let Some(since) = query.since.as_deref() {
            url.query_pairs_mut().append_pair("from", since);
        }
        if let Some(until) = query.until.as_deref() {
            url.query_pairs_mut().append_pair("to", until);
        }
        url
    }
}

impl Default for Wayback {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DiscoverySource for Wayback {
    fn name(&self) -> Arc<str> {
        Arc::from("wayback")
    }

    async fn discover(
        &self,
        client: &Client,
        query: &Query,
        budget: &DiscoveryBudget,
    ) -> Result<Found, CrawlError> {
        // Nothing to expand is not a failure — it is this source having
        // nothing to say about a text-only query.
        if query.sites.is_empty() || budget.max_requests == 0 {
            return Ok(Found::default());
        }

        let sites: Vec<&String> = query.sites.iter().take(budget.max_requests).collect();
        let per_site = (budget.max_candidates / sites.len().max(1)).max(1);

        let mut leads = Vec::new();
        for site in sites {
            let url = self.request_url(site, query, per_site);
            let rows = fetch_json(client, &url, &[], MAX_BODY_BYTES).await?;
            leads.extend(leads_from(&rows, self.name()));
        }
        Ok(Found::leads(leads))
    }
}

/// CDX answers with an array of arrays whose **first row names the columns**.
/// Reading it positionally without dropping that row turns the header into a
/// result called "original".
fn leads_from(response: &Value, source: Arc<str>) -> Vec<Lead> {
    let Some(rows) = response.as_array() else {
        return Vec::new();
    };
    let Some(header) = rows.first().and_then(Value::as_array) else {
        return Vec::new();
    };

    let column = |name: &str| {
        header
            .iter()
            .position(|c| c.as_str() == Some(name))
    };
    let (Some(timestamp_at), Some(original_at)) = (column("timestamp"), column("original")) else {
        return Vec::new();
    };

    rows.iter()
        .skip(1)
        .filter_map(|row| {
            let row = row.as_array()?;
            let timestamp = row.get(timestamp_at)?.as_str()?;
            let original = row.get(original_at)?.as_str()?;
            // The replay URL, not the original: the original is what stopped
            // resolving, which is the entire reason to ask the archive.
            let url = canonicalize(&format!("{REPLAY}/{timestamp}/{original}")).ok()?;

            Some(Lead {
                url,
                score: CAPTURE_SCORE,
                title: Some(original.to_string()),
                seen: Some(timestamp.to_string()),
                domain: Url::parse(original).ok().and_then(|u| u.host_str().map(str::to_string)),
                language: None,
                sources: vec![Arc::clone(&source)],
            })
        })
        .collect()
}
