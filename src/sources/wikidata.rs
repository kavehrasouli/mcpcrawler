//! Wikidata — the identity spine.
//!
//! "President of Kyrgyzstan" is a description, not an identifier. Wikidata
//! turns it into a Q-ID, and with the Q-ID comes the machine-readable spine of
//! everything downstream: labels in a hundred languages, the alias sets that
//! carry the transliterations outlets disagree about (*Sadyr Japarov* and
//! *Sadyr Zhaparov* are the same person), the dates a position was held, and
//! the official website.
//!
//! As a discovery source it contributes two kinds of URL:
//!
//! - **The official website**, which is a primary-source domain, free.
//! - **Every language edition of the entity's Wikipedia article.** This is the
//!   part a keyword search will not give you. One person had 61 of them, and
//!   the Kyrgyz and Russian articles cite local coverage that the English one
//!   has never heard of.
//!
//! Two requests: one to resolve the name, one to read the entity.

use crate::api::fetch_json;
use crate::crawler::{CrawlError, canonicalize};
use crate::discovery::{DiscoveryBudget, DiscoverySource, Found, Lead, Query};
use async_trait::async_trait;
use reqwest::Client;
use serde_json::Value;
use std::sync::Arc;
use url::Url;

const ENDPOINT: &str = "https://www.wikidata.org/w/api.php";
const MAX_BODY_BYTES: usize = 8 * 1024 * 1024;

/// Requests this source needs before it can answer at all.
const REQUESTS_NEEDED: usize = 2;

/// `P856`, official website.
const OFFICIAL_WEBSITE: &str = "P856";

/// A primary source outranks any encyclopaedia article about it.
const OFFICIAL_SITE_SCORE: f32 = 1.0;
/// The English edition is usually the fullest and is the one most likely to
/// cite sources in several languages, so it leads the other editions.
const ENGLISH_ARTICLE_SCORE: f32 = 0.85;
/// Every other language edition. Coverage of a non-anglophone subject lives
/// mostly outside English, so these are worth real weight, not a token one.
const ARTICLE_SCORE: f32 = 0.7;
/// Commons, Wikiquote, Wikisource and friends. Real pages, but media
/// categories and quotation lists are not reporting, and left at the same
/// score as an article they outrank one purely by sorting earlier.
const COMPANION_WIKI_SCORE: f32 = 0.4;

#[derive(Debug)]
pub struct Wikidata {
    endpoint: Url,
}

impl Wikidata {
    pub fn new() -> Self {
        Self::at(ENDPOINT).expect("the built-in Wikidata endpoint is a valid URL")
    }

    pub fn at(endpoint: &str) -> Result<Self, CrawlError> {
        Ok(Self { endpoint: canonicalize(endpoint)? })
    }

    fn search_url(&self, text: &str) -> Url {
        let mut url = self.endpoint.clone();
        url.query_pairs_mut()
            .append_pair("action", "wbsearchentities")
            .append_pair("search", text)
            .append_pair("language", "en")
            .append_pair("uselang", "en")
            .append_pair("format", "json")
            .append_pair("limit", "1");
        url
    }

    fn entity_url(&self, id: &str) -> Url {
        let mut url = self.endpoint.clone();
        url.query_pairs_mut()
            .append_pair("action", "wbgetentities")
            .append_pair("ids", id)
            .append_pair("format", "json")
            // `sitelinks/urls` is what makes the language editions usable
            // directly rather than as titles needing reassembly.
            .append_pair("props", "labels|aliases|claims|sitelinks/urls");
        url
    }
}

impl Default for Wikidata {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DiscoverySource for Wikidata {
    fn name(&self) -> Arc<str> {
        Arc::from("wikidata")
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
        if budget.max_requests < REQUESTS_NEEDED {
            return Ok(Found::default());
        }

        let found = fetch_json(client, &self.search_url(text), &[], MAX_BODY_BYTES).await?;
        let Some(id) = first_entity_id(&found) else {
            // No entity by that name is an ordinary answer, not a failure.
            return Ok(Found::default());
        };

        let entity = fetch_json(client, &self.entity_url(&id), &[], MAX_BODY_BYTES).await?;
        Ok(Found {
            leads: leads_from(&entity, &id, self.name()),
            terms: names_of(&entity, &id),
        })
    }
}

fn first_entity_id(response: &Value) -> Option<String> {
    let hit = response.get("search")?.as_array()?.first()?;
    Some(hit.get("id")?.as_str()?.to_string())
}

fn leads_from(response: &Value, id: &str, source: Arc<str>) -> Vec<Lead> {
    let Some(entity) = response.get("entities").and_then(|e| e.get(id)) else {
        return Vec::new();
    };

    let label = entity
        .get("labels")
        .and_then(|l| l.get("en"))
        .and_then(|l| l.get("value"))
        .and_then(Value::as_str)
        .map(str::to_string);

    let mut leads = Vec::new();

    // The official website first: a ministry's own archive beats any article
    // written about it.
    if let Some(sites) = entity.get("claims").and_then(|c| c.get(OFFICIAL_WEBSITE)) {
        for claim in sites.as_array().map(Vec::as_slice).unwrap_or_default() {
            let Some(value) = claim
                .get("mainsnak")
                .and_then(|s| s.get("datavalue"))
                .and_then(|d| d.get("value"))
                .and_then(Value::as_str)
            else {
                continue;
            };
            if let Ok(url) = canonicalize(value) {
                leads.push(Lead {
                    url,
                    score: OFFICIAL_SITE_SCORE,
                    title: label.clone().map(|l| format!("{l} — official website")),
                    seen: None,
                    domain: None,
                    language: None,
                    sources: vec![Arc::clone(&source)],
                });
            }
        }
    }

    // Then every language edition. Coverage of a non-anglophone subject lives
    // mostly outside English, and each article is a different set of citations.
    let sitelinks = entity.get("sitelinks").and_then(Value::as_object);
    for (site, link) in sitelinks.into_iter().flatten() {
        let Some(url) = link.get("url").and_then(Value::as_str) else {
            continue;
        };
        // `enwiki` -> `en`; `commonswiki` and friends have no language prefix
        // worth reporting.
        let language = site.strip_suffix("wiki").filter(|l| l.len() <= 3);
        let Ok(url) = canonicalize(url) else { continue };

        let score = match language {
            Some("en") => ENGLISH_ARTICLE_SCORE,
            Some(_) => ARTICLE_SCORE,
            None => COMPANION_WIKI_SCORE,
        };

        leads.push(Lead {
            url,
            score,
            title: link.get("title").and_then(Value::as_str).map(str::to_string),
            seen: None,
            domain: None,
            language: language.map(str::to_string),
            sources: vec![Arc::clone(&source)],
        });
    }

    leads
}

/// Every name the entity is known by: its label in each language, and each
/// alias.
///
/// This is the highest-value thing Wikidata returns and it is not a URL. One
/// person is *Sadyr Japarov*, *Sadyr Zhaparov*, *Садыр Жапаров* and *Sadır
/// Japarov*, and outlets do not agree on which. Handed to the crawler's link
/// scoring, each of these turns a link that previously scored zero into one
/// worth opening.
fn names_of(response: &Value, id: &str) -> Vec<String> {
    let Some(entity) = response.get("entities").and_then(|e| e.get(id)) else {
        return Vec::new();
    };

    let mut names: Vec<String> = Vec::new();
    let mut push = |value: Option<&str>| {
        if let Some(name) = value.map(str::trim).filter(|n| !n.is_empty()) {
            let name = name.to_string();
            if !names.contains(&name) {
                names.push(name);
            }
        }
    };

    if let Some(labels) = entity.get("labels").and_then(Value::as_object) {
        for label in labels.values() {
            push(label.get("value").and_then(Value::as_str));
        }
    }
    if let Some(languages) = entity.get("aliases").and_then(Value::as_object) {
        for aliases in languages.values() {
            for alias in aliases.as_array().map(Vec::as_slice).unwrap_or_default() {
                push(alias.get("value").and_then(Value::as_str));
            }
        }
    }
    names
}
