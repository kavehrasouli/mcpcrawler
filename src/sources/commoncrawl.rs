//! Common Crawl — the index of a very large public crawl, queried by site.
//!
//! Like Wayback it searches by host and not by text, so it contributes only
//! when `Query::sites` names one. Unlike Wayback it returns the *original*
//! URLs: these pages are live and can be fetched directly, where a Wayback
//! replay is a copy of something that may be gone. The two overlap and neither
//! is complete, which is the argument for asking both.
//!
//! The index is addressed per crawl, so the first step is to ask which crawls
//! exist (`collinfo.json`, newest first) and use the latest. That list is read
//! once and kept. Of the free indexes this is the least dependable — it is
//! slow, and a crawl's index server is sometimes down — so a failure here is
//! reported and the other sources' results are returned regardless.

use crate::api::{fetch_body, fetch_json};
use crate::crawler::{CrawlError, canonicalize};
use crate::discovery::{DiscoveryBudget, DiscoverySource, Found, Lead, Query};
use async_trait::async_trait;
use reqwest::Client;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::OnceCell;
use url::Url;

const COLLINFO: &str = "https://index.commoncrawl.org/collinfo.json";
const MAX_BODY_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug)]
pub struct CommonCrawl {
    collinfo: Url,
    /// The newest crawl's index endpoint, found once.
    index: OnceCell<Url>,
}

impl CommonCrawl {
    pub fn new() -> Self {
        Self::at(COLLINFO).expect("the built-in Common Crawl endpoint is a valid URL")
    }

    pub fn at(collinfo: &str) -> Result<Self, CrawlError> {
        Ok(Self { collinfo: canonicalize(collinfo)?, index: OnceCell::new() })
    }

    async fn latest_index(&self, client: &Client) -> Result<&Url, CrawlError> {
        self.index
            .get_or_try_init(|| async {
                let list = fetch_json(client, &self.collinfo, &[], MAX_BODY_BYTES).await?;
                list.as_array()
                    .and_then(|crawls| crawls.first())
                    .and_then(|crawl| crawl.get("cdx-api"))
                    .and_then(Value::as_str)
                    .and_then(|api| canonicalize(api).ok())
                    .ok_or_else(|| CrawlError::Json("collinfo.json lists no usable crawl".to_string()))
            })
            .await
    }
}

impl Default for CommonCrawl {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DiscoverySource for CommonCrawl {
    fn name(&self) -> Arc<str> {
        Arc::from("commoncrawl")
    }

    fn per_query(&self) -> bool {
        false
    }

    async fn discover(
        &self,
        client: &Client,
        query: &Query,
        budget: &DiscoveryBudget,
    ) -> Result<Found, CrawlError> {
        // Nothing to look up without a site; not a failure.
        if query.sites.is_empty() || budget.max_requests < 2 {
            return Ok(Found::default());
        }
        let index = self.latest_index(client).await?;

        let source = self.name();
        let mut leads = Vec::new();
        // One request spent finding the crawl; the rest go to sites.
        for site in query.sites.iter().take(budget.max_requests - 1) {
            let mut url = index.clone();
            url.query_pairs_mut()
                .append_pair("url", &format!("{}/*", site.trim().trim_end_matches('/')))
                .append_pair("output", "json")
                .append_pair("limit", &budget.max_candidates.to_string())
                .append_pair("filter", "status:200")
                .append_pair("filter", "mime:text/html")
                .append_pair("collapse", "urlkey");
            let body = fetch_body(client, &url, &[], MAX_BODY_BYTES).await?;

            // One JSON object per line.
            let rows: Vec<Value> = String::from_utf8_lossy(&body)
                .lines()
                .filter_map(|line| serde_json::from_str(line).ok())
                .collect();
            let total = rows.len().max(1) as f32;
            for (rank, row) in rows.iter().enumerate() {
                let Some(page) = row.get("url").and_then(Value::as_str).and_then(|u| canonicalize(u).ok())
                else {
                    continue;
                };
                leads.push(Lead {
                    url: page,
                    score: 1.0 - (rank as f32 / total) * 0.9,
                    title: None,
                    seen: row.get("timestamp").and_then(Value::as_str).map(str::to_string),
                    domain: Some(site.clone()),
                    language: None,
                    sources: vec![Arc::clone(&source)],
                });
            }
        }
        leads.truncate(budget.max_candidates);
        Ok(Found::leads(leads))
    }
}
