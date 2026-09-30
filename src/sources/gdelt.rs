//! GDELT DOC 2.0 — worldwide news article search.
//!
//! First source for a reason: it needs no API key, it indexes broadcast, print
//! and web news in over a hundred languages, and it reaches syndicated regional
//! coverage that a general web search never surfaces. For "what did this person
//! do in this country", that multilingual tail is most of the answer.
//!
//! Two things about this API are worth knowing before reading the code, and
//! neither is in its documentation:
//!
//! 1. It asks for **one request every five seconds**, and enforces that by
//!    answering `200 OK` with a sentence of English prose instead of JSON. A
//!    naive client reads that as malformed JSON and reports the wrong problem.
//! 2. A query that matches nothing returns `{}` — the `articles` key is absent
//!    rather than empty, so treating a missing key as an error would turn every
//!    empty result into a failure.

use crate::api::{fetch_body, parse_json};
use crate::crawler::{CrawlError, HostSlots, canonicalize, host_of, throttle};
use crate::discovery::{DiscoveryBudget, DiscoverySource, Found, Lead, Query};
use async_trait::async_trait;
use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use url::Url;

const ENDPOINT: &str = "https://api.gdeltproject.org/api/v2/doc/doc";

/// What GDELT asks for, and what it answers with when it does not get it.
const MIN_REQUEST_GAP: Duration = Duration::from_secs(5);
const RATE_LIMIT_MARKER: &str = "limit requests to one every";

/// The API caps a single response at 250 articles.
const MAX_RECORDS: usize = 250;

/// Response bodies are article metadata only, never page content.
const MAX_BODY_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug)]
pub struct Gdelt {
    endpoint: Url,
    /// Held per instance rather than per call, so the five-second gap survives
    /// across tool invocations — which is exactly when it gets breached.
    slots: HostSlots,
}

/// Override for the endpoint, so the server can be pointed at a mirror, a
/// proxy, or a local stand-in without a rebuild.
pub const ENDPOINT_ENV: &str = "MCPCRAWLER_GDELT_ENDPOINT";

impl Gdelt {
    pub fn new() -> Self {
        match std::env::var(ENDPOINT_ENV) {
            Ok(endpoint) => Self::at(&endpoint)
                .unwrap_or_else(|e| panic!("{ENDPOINT_ENV} is not a usable URL: {e}")),
            Err(_) => Self::at(ENDPOINT).expect("the built-in GDELT endpoint is a valid URL"),
        }
    }

    /// Point at a different endpoint. The test suite serves GDELT-shaped
    /// responses locally; hitting the real API from tests would be neither
    /// hermetic nor polite.
    pub fn at(endpoint: &str) -> Result<Self, CrawlError> {
        Ok(Self {
            endpoint: canonicalize(endpoint)?,
            slots: Mutex::new(HashMap::new()),
        })
    }

    fn request_url(&self, query: &Query, budget: &DiscoveryBudget) -> Url {
        let records = budget.max_candidates.clamp(1, MAX_RECORDS).to_string();
        let mut url = self.endpoint.clone();
        {
            let mut params = url.query_pairs_mut();
            params
                .append_pair("query", &query.text)
                .append_pair("mode", "artlist")
                .append_pair("format", "json")
                .append_pair("maxrecords", &records)
                // Relevance rather than recency: a crawl budget spent on the
                // newest matches is a crawl budget spent on today's news.
                .append_pair("sort", "HybridRel");

            // GDELT wants whole timestamps, so a date becomes its first or last
            // second. Bounding the query is what keeps a term-limited search
            // from spending its records outside the window.
            if let Some(since) = query.since.as_deref() {
                params.append_pair("startdatetime", &format!("{since}000000"));
            }
            if let Some(until) = query.until.as_deref() {
                params.append_pair("enddatetime", &format!("{until}235959"));
            }
        }
        url
    }
}

impl Default for Gdelt {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DiscoverySource for Gdelt {
    fn name(&self) -> Arc<str> {
        Arc::from("gdelt")
    }

    async fn discover(
        &self,
        client: &Client,
        query: &Query,
        budget: &DiscoveryBudget,
    ) -> Result<Found, CrawlError> {
        if query.text.trim().is_empty() {
            return Err(CrawlError::BadUrl("empty query".to_string()));
        }
        if budget.max_requests == 0 {
            return Ok(Found::default());
        }

        let url = self.request_url(query, budget);
        throttle(&self.slots, &host_of(&url), MIN_REQUEST_GAP).await;

        let body = fetch_body(client, &url, &[], MAX_BODY_BYTES).await?;

        // Checked before parsing, because the rate-limit notice arrives as a
        // successful response whose body happens not to be JSON.
        let head = String::from_utf8_lossy(&body[..body.len().min(400)]);
        if head.contains(RATE_LIMIT_MARKER) {
            return Err(CrawlError::RateLimited(
                "GDELT allows one request every 5 seconds".to_string(),
            ));
        }

        Ok(Found::leads(leads_from(&parse_json(&body)?, self.name())))
    }
}

/// GDELT reports no relevance number, so its own ordering is the only signal it
/// gives about relative quality. Rank becomes the score: first result 1.0,
/// last just above 0.
fn leads_from(response: &Value, source: Arc<str>) -> Vec<Lead> {
    let articles = response
        .get("articles")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();

    let total = articles.len() as f32;
    articles
        .iter()
        .enumerate()
        .filter_map(|(rank, article)| {
            let url = canonicalize(article.get("url")?.as_str()?).ok()?;
            let field = |key: &str| {
                article
                    .get(key)
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
                    .map(str::to_string)
            };

            Some(Lead {
                url,
                score: 1.0 - (rank as f32 / total),
                title: field("title"),
                seen: field("seendate"),
                domain: field("domain"),
                language: field("language"),
                sources: vec![Arc::clone(&source)],
            })
        })
        .collect()
}
