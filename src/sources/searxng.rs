//! SearXNG — a meta-search engine you run yourself.
//!
//! It asks several public search engines at once and returns one JSON list, and
//! because it runs on your own machine its rate limit is yours to set. That
//! makes it the way to get general web search with no account and no key.
//!
//! **Setup.** Run an instance, enable the JSON output format in its
//! `settings.yml` (`search.formats` must include `json`; without it the server
//! answers 403), and point `MCPCRAWLER_SEARXNG_URL` at it. An instance on
//! `localhost` or a private address is private-network traffic, so the host is
//! added to the destination allowlist automatically — and only that host.
//!
//! **Dates.** SearXNG filters by coarse ranges (day, week, month, year), not by
//! a window, so the window is applied afterwards to results that carry a
//! publication date. A result with no date is kept: it is unknown, not wrong.

use crate::api::fetch_json;
use crate::crawler::{CrawlError, canonicalize};
use crate::dedup::ymd;
use crate::discovery::{DiscoveryBudget, DiscoverySource, Found, Lead, Query};
use async_trait::async_trait;
use reqwest::Client;
use serde_json::Value;
use std::sync::Arc;
use url::Url;

pub const URL_ENV: &str = "MCPCRAWLER_SEARXNG_URL";
const MAX_BODY_BYTES: usize = 4 * 1024 * 1024;
/// Pages of results asked for at most. Each is a round of searches by the
/// engines behind it.
const MAX_PAGES: usize = 2;

#[derive(Debug)]
pub struct SearxNg {
    endpoint: Url,
}

impl SearxNg {
    /// `None` when no instance is configured; `Err` when the setting is not a
    /// usable URL, so a typo is reported at startup and not on every query.
    pub fn from_env() -> Result<Option<Self>, String> {
        Self::from_setting(std::env::var(URL_ENV).ok().as_deref())
    }

    /// As [`from_env`](Self::from_env), for a value already in hand.
    pub fn from_setting(raw: Option<&str>) -> Result<Option<Self>, String> {
        let Some(raw) = raw.map(str::trim).filter(|r| !r.is_empty()) else { return Ok(None) };
        let base = canonicalize(raw).map_err(|e| format!("{URL_ENV} is not a usable URL: {e}"))?;
        if let Some(host) = base.host_str() {
            // The operator named this host deliberately; nothing else is opened.
            crate::net::allow_host(host);
        }
        Self::at(base.as_str()).map(Some).map_err(|e| e.to_string())
    }

    /// `base` is the instance's root; `/search` is added.
    pub fn at(base: &str) -> Result<Self, CrawlError> {
        let mut endpoint = canonicalize(base)?;
        if !endpoint.path().ends_with("/search") {
            let path = format!("{}/search", endpoint.path().trim_end_matches('/'));
            endpoint.set_path(&path);
        }
        Ok(Self { endpoint })
    }

    fn page_url(&self, text: &str, page: usize) -> Url {
        let mut url = self.endpoint.clone();
        url.query_pairs_mut()
            .append_pair("q", text)
            .append_pair("format", "json")
            .append_pair("pageno", &page.to_string());
        url
    }
}

#[async_trait]
impl DiscoverySource for SearxNg {
    fn name(&self) -> Arc<str> {
        Arc::from("searxng")
    }

    async fn discover(
        &self,
        client: &Client,
        query: &Query,
        budget: &DiscoveryBudget,
    ) -> Result<Found, CrawlError> {
        let Some(text) = query.text() else {
            return Err(CrawlError::BadUrl("empty query".to_string()));
        };

        let mut results: Vec<Value> = Vec::new();
        for page in 1..=MAX_PAGES.min(budget.max_requests) {
            let body = fetch_json(client, &self.page_url(text, page), &[], MAX_BODY_BYTES).await?;
            let batch = body.get("results").and_then(Value::as_array).cloned().unwrap_or_default();
            let empty = batch.is_empty();
            results.extend(batch);
            if empty || results.len() >= budget.max_candidates {
                break;
            }
        }

        let source = self.name();
        let total = results.len().max(1) as f32;
        let mut leads = Vec::new();
        for (rank, result) in results.iter().enumerate() {
            let Some(url) = result.get("url").and_then(Value::as_str).and_then(|u| canonicalize(u).ok())
            else {
                continue;
            };
            let published = result.get("publishedDate").and_then(Value::as_str);
            if let Some(date) = published.and_then(|d| ymd(d, 9999))
                && (query.since.as_deref().is_some_and(|s| date.as_str() < s)
                    || query.until.as_deref().is_some_and(|u| date.as_str() > u))
            {
                continue;
            }
            leads.push(Lead {
                url,
                // The engine's own order is the signal; it reports no number
                // that means anything across engines.
                score: 1.0 - (rank as f32 / total) * 0.9,
                title: result.get("title").and_then(Value::as_str).map(str::to_string),
                seen: published.map(str::to_string),
                domain: None,
                language: None,
                sources: vec![Arc::clone(&source)],
            });
        }
        leads.truncate(budget.max_candidates);
        Ok(Found::leads(leads))
    }
}
