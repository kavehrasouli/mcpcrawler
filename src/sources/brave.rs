//! Brave Search — ordinary web search, as the general-recall backstop.
//!
//! The other sources are each strong in one direction and blind elsewhere:
//! GDELT knows news and not much else, Wikidata knows entities, Wayback knows
//! what was deleted. A general web index is what catches the material that
//! belongs to none of those categories — a think tank's PDF, a parliamentary
//! transcript, a conference agenda.
//!
//! It is also the first source that needs a credential. The key is read from
//! the environment at startup and never appears in a tool schema, so it cannot
//! travel through the model's context — the same rule the master password
//! follows.

use crate::api::fetch_json;
use crate::crawler::{CrawlError, canonicalize};
use crate::discovery::{DiscoveryBudget, DiscoverySource, Found, Lead, Query};
use async_trait::async_trait;
use reqwest::Client;
use serde_json::Value;
use std::sync::Arc;
use url::Url;

const ENDPOINT: &str = "https://api.search.brave.com/res/v1/web/search";
const MAX_BODY_BYTES: usize = 4 * 1024 * 1024;

/// Where the subscription token is read from. Absent, the source is not
/// registered at all — a backend that reports the same failure on every call
/// is worse than one that is simply not there.
pub const API_KEY_ENV: &str = "MCPCRAWLER_BRAVE_API_KEY";

/// The API caps a page of results at 20.
const MAX_RESULTS: usize = 20;

#[derive(Debug)]
pub struct Brave {
    endpoint: Url,
    key: String,
}

impl Brave {
    /// `None` when no key is configured.
    pub fn from_env() -> Option<Self> {
        let key = std::env::var(API_KEY_ENV).ok()?;
        let key = key.trim().to_string();
        if key.is_empty() {
            return None;
        }
        Some(Self::at(ENDPOINT, &key).expect("the built-in Brave endpoint is a valid URL"))
    }

    pub fn at(endpoint: &str, key: &str) -> Result<Self, CrawlError> {
        Ok(Self { endpoint: canonicalize(endpoint)?, key: key.to_string() })
    }

    fn request_url(&self, text: &str, budget: &DiscoveryBudget) -> Url {
        let count = budget.max_candidates.clamp(1, MAX_RESULTS).to_string();
        let mut url = self.endpoint.clone();
        url.query_pairs_mut()
            .append_pair("q", text)
            .append_pair("count", &count);
        url
    }
}

#[async_trait]
impl DiscoverySource for Brave {
    fn name(&self) -> Arc<str> {
        Arc::from("brave")
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
        if budget.max_requests == 0 {
            return Ok(Found::default());
        }

        let url = self.request_url(text, budget);
        let headers = [
            ("Accept", "application/json"),
            ("X-Subscription-Token", self.key.as_str()),
        ];
        let response = fetch_json(client, &url, &headers, MAX_BODY_BYTES).await?;
        Ok(Found::leads(leads_from(&response, self.name())))
    }
}

/// Like GDELT, Brave reports no relevance number and ranks its own results, so
/// position is the signal.
fn leads_from(response: &Value, source: Arc<str>) -> Vec<Lead> {
    let results = response
        .get("web")
        .and_then(|w| w.get("results"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();

    let total = results.len() as f32;
    results
        .iter()
        .enumerate()
        .filter_map(|(rank, result)| {
            let url = canonicalize(result.get("url")?.as_str()?).ok()?;
            let field = |key: &str| {
                result
                    .get(key)
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
                    .map(str::to_string)
            };
            Some(Lead {
                url: url.clone(),
                score: 1.0 - (rank as f32 / total),
                title: field("title"),
                seen: field("age"),
                domain: url.host_str().map(str::to_string),
                language: field("language"),
                sources: vec![Arc::clone(&source)],
            })
        })
        .collect()
}
