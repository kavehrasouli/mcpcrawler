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
//!
//! **An office is not a person.** "President of Kyrgyzstan" resolves to the
//! *position*, which lists its officeholder and when they took up the post.
//! When the name turns out to be a position, one more request reads the holder,
//! and the person is what is searched for — with the window bounded by that
//! holder's time in that office, not by their whole career.

use crate::api::fetch_json;
use crate::crawler::{CrawlError, canonicalize};
use crate::discovery::{DiscoveryBudget, DiscoverySource, Found, Lead, Query, Span};
use std::collections::HashSet;
use async_trait::async_trait;
use reqwest::Client;
use serde_json::Value;
use std::sync::Arc;
use url::Url;

const ENDPOINT: &str = "https://www.wikidata.org/w/api.php";
const MAX_BODY_BYTES: usize = 8 * 1024 * 1024;

/// Requests this source needs before it can answer at all.
const REQUESTS_NEEDED: usize = 2;

/// `P39`, position held — with `P580` and `P582` as the qualifiers for when.
const POSITION_HELD: &str = "P39";
/// `P1308`, officeholder: on a position, who holds it.
const OFFICEHOLDER: &str = "P1308";
/// `P31` and `Q4164871`: instance of "position".
const INSTANCE_OF: &str = "P31";
const POSITION_CLASS: &str = "Q4164871";
/// An office's own pages rank below its holder's: they describe the post, and
/// the holder's are about the person doing what the question asks about.
const OFFICE_DISCOUNT: f32 = 0.8;
const START_TIME: &str = "P580";
const END_TIME: &str = "P582";

/// `P1559`, name in native language: monolingual text, `{text, language}`.
const NATIVE_NAME: &str = "P1559";

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

    /// English labels only, for a handful of entities at once.
    fn labels_url(&self, ids: &[String]) -> Url {
        let mut url = self.endpoint.clone();
        url.query_pairs_mut()
            .append_pair("action", "wbgetentities")
            .append_pair("ids", &ids.join("|"))
            .append_pair("format", "json")
            .append_pair("props", "labels")
            .append_pair("languages", "en");
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

    fn per_query(&self) -> bool {
        false
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
        let Some(mut id) = first_entity_id(&found) else {
            // No entity by that name is an ordinary answer, not a failure.
            return Ok(Found::default());
        };

        let mut entity = fetch_json(client, &self.entity_url(&id), &[], MAX_BODY_BYTES).await?;
        let mut requests = REQUESTS_NEEDED;
        let mut office_leads: Vec<Lead> = Vec::new();
        let mut held: Option<Span> = None;
        let mut subject: Option<String> = None;

        // The name was an office: read who holds it, and search for them.
        if is_position(&entity, &id)
            && requests < budget.max_requests
            && let Some((holder, term)) = officeholder(&entity, &id)
        {
            office_leads = leads_from(&entity, &id, self.name())
                .into_iter()
                .map(|mut lead| {
                    lead.score *= OFFICE_DISCOUNT;
                    lead
                })
                .collect();
            let person = fetch_json(client, &self.entity_url(&holder), &[], MAX_BODY_BYTES).await?;
            requests += 1;
            subject = Some(match label_of(&person, &holder) {
                Some(label) => format!("{label} (Wikidata {holder})"),
                None => format!("Wikidata {holder}"),
            });
            entity = person;
            id = holder;
            held = Some(term);
        }

        // A person with several posts: the one the question names.
        let mut span = held;
        if span.is_none() {
            let wanted = match query.post.as_deref().map(str::trim).filter(|p| !p.is_empty()) {
                Some(post) if requests < budget.max_requests => {
                    let ids = position_ids(&entity, &id);
                    if ids.is_empty() {
                        None
                    } else {
                        let labels =
                            fetch_json(client, &self.labels_url(&ids), &[], MAX_BODY_BYTES).await?;
                        Some(posts_named(&labels, post))
                    }
                }
                _ => None,
            };
            span = tenure_of_posts(&entity, &id, wanted.as_ref().filter(|w| !w.is_empty()));
        }

        let mut leads = leads_from(&entity, &id, self.name());
        leads.extend(office_leads);
        Ok(Found { leads, terms: names_of(&entity, &id), span, subject })
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

    // The name in the subject's own language comes first. Labels arrive in
    // language-code order, which puts Amharic ahead of Persian for an Iranian
    // president; the property that says which language the person writes their
    // own name in is a better pick than the alphabet.
    if let Some(native) = entity
        .get("claims")
        .and_then(|c| c.get(NATIVE_NAME))
        .and_then(Value::as_array)
    {
        for claim in native {
            push(
                claim
                    .get("mainsnak")
                    .and_then(|s| s.get("datavalue"))
                    .and_then(|d| d.get("value"))
                    .and_then(|v| v.get("text"))
                    .and_then(Value::as_str),
            );
        }
    }
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

/// When the entity held office: the earliest start and the latest end across
/// every `P39` claim.
///
/// One claim without an end date is a post still held, and that leaves the
/// span open at the far end rather than closed at the last date on record. A
/// subject with several posts gets the union of them, which is wider than any
/// one post — a deliberate trade, since a window that is too narrow loses
/// coverage silently and one that is too wide only costs a few queries. Naming
/// the post (see [`tenure_of_posts`]) narrows it.
#[cfg(test)]
pub(crate) fn tenure_of(response: &Value, id: &str) -> Option<Span> {
    tenure_of_posts(response, id, None)
}

/// As [`tenure_of`], counting only the claims for the posts in `wanted`
/// (Wikidata Q-IDs). `None` counts them all.
pub(crate) fn tenure_of_posts(
    response: &Value,
    id: &str,
    wanted: Option<&HashSet<String>>,
) -> Option<Span> {
    let entity = response.get("entities").and_then(|e| e.get(id))?;
    let claims = entity.get("claims")?.get(POSITION_HELD)?.as_array()?;

    let mut from: Option<String> = None;
    let mut to: Option<String> = None;
    let mut ongoing = false;
    let mut counted = 0;
    for claim in claims {
        if let Some(wanted) = wanted
            && !claim_value_id(claim).is_some_and(|post| wanted.contains(post))
        {
            continue;
        }
        counted += 1;
        let (start, end) = dates_of(claim);
        if let Some(start) = start {
            from = Some(from.map_or(start.clone(), |f| f.min(start)));
        }
        match end {
            Some(end) => to = Some(to.map_or(end.clone(), |t| t.max(end))),
            None => ongoing = true,
        }
    }
    if counted == 0 || (from.is_none() && to.is_none()) {
        return None;
    }
    Some(Span { from, to: if ongoing { None } else { to } })
}

/// The start and end of one claim, from its `P580` and `P582` qualifiers.
fn dates_of(claim: &Value) -> (Option<String>, Option<String>) {
    let qualifier = |property: &str| -> Option<&str> {
        claim
            .get("qualifiers")?
            .get(property)?
            .as_array()?
            .first()?
            .get("datavalue")?
            .get("value")?
            .get("time")?
            .as_str()
    };
    (
        qualifier(START_TIME).and_then(|t| date_of(t, false)),
        qualifier(END_TIME).and_then(|t| date_of(t, true)),
    )
}

/// The entity a claim points at, for claims whose value is another entity.
fn claim_value_id(claim: &Value) -> Option<&str> {
    claim.get("mainsnak")?.get("datavalue")?.get("value")?.get("id")?.as_str()
}

/// Every post the entity has held, once each.
fn position_ids(response: &Value, id: &str) -> Vec<String> {
    let Some(claims) = response
        .get("entities")
        .and_then(|e| e.get(id))
        .and_then(|e| e.get("claims"))
        .and_then(|c| c.get(POSITION_HELD))
        .and_then(Value::as_array)
    else {
        return Vec::new();
    };
    let mut ids: Vec<String> = Vec::new();
    for claim in claims {
        if let Some(post) = claim_value_id(claim)
            && !ids.iter().any(|seen| seen == post)
        {
            ids.push(post.to_string());
        }
    }
    ids
}

/// The posts, among those labelled in `labels`, whose English name contains
/// `post`. Case-insensitive.
fn posts_named(labels: &Value, post: &str) -> HashSet<String> {
    let needle = post.to_lowercase();
    labels
        .get("entities")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
        .filter(|(_, entity)| {
            entity
                .get("labels")
                .and_then(|l| l.get("en"))
                .and_then(|l| l.get("value"))
                .and_then(Value::as_str)
                .is_some_and(|name| name.to_lowercase().contains(&needle))
        })
        .map(|(id, _)| id.clone())
        .collect()
}

/// Whether the entity is an office rather than a person: an instance of
/// "position", or something that lists an officeholder and holds no posts.
fn is_position(response: &Value, id: &str) -> bool {
    let Some(claims) = response.get("entities").and_then(|e| e.get(id)).and_then(|e| e.get("claims"))
    else {
        return false;
    };
    let is_class = claims
        .get(INSTANCE_OF)
        .and_then(Value::as_array)
        .is_some_and(|c| c.iter().any(|claim| claim_value_id(claim) == Some(POSITION_CLASS)));
    is_class || (claims.get(OFFICEHOLDER).is_some() && claims.get(POSITION_HELD).is_none())
}

/// Who holds an office, and for how long: the holder with no end date, or
/// failing that the one whose term ended last.
fn officeholder(response: &Value, id: &str) -> Option<(String, Span)> {
    let claims = response
        .get("entities")?
        .get(id)?
        .get("claims")?
        .get(OFFICEHOLDER)?
        .as_array()?;
    let mut best: Option<(String, Span)> = None;
    for claim in claims {
        let Some(holder) = claim_value_id(claim) else { continue };
        let (from, to) = dates_of(claim);
        let candidate = (holder.to_string(), Span { from, to });
        best = Some(match best {
            None => candidate,
            Some(current) => {
                // Still in post beats finished; later end beats earlier.
                let better = match (&candidate.1.to, &current.1.to) {
                    (None, Some(_)) => true,
                    (Some(a), Some(b)) => a > b,
                    _ => false,
                };
                if better { candidate } else { current }
            }
        });
    }
    best
}

fn label_of(response: &Value, id: &str) -> Option<String> {
    response
        .get("entities")?
        .get(id)?
        .get("labels")?
        .get("en")?
        .get("value")?
        .as_str()
        .map(str::to_string)
}

/// `+2021-01-28T00:00:00Z` -> `20210128`. Wikidata marks a date known only to
/// the month or the year with zeros (`+2021-00-00`), which is widened to the
/// whole month or year so the window never excludes a day it might contain.
/// Dates before year 1 or with a malformed shape give `None`.
fn date_of(time: &str, end_of_period: bool) -> Option<String> {
    let date = time.strip_prefix('+')?.split('T').next()?;
    let mut parts = date.split('-');
    let (year, month, day) = (parts.next()?, parts.next()?, parts.next()?);
    if year.len() != 4 || !year.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let month = match (month, end_of_period) {
        ("00", false) => "01",
        ("00", true) => "12",
        (m, _) => m,
    };
    let day = match (day, end_of_period) {
        ("00", false) => "01",
        ("00", true) => "31",
        (d, _) => d,
    };
    let all_digits = |s: &str| s.len() == 2 && s.chars().all(|c| c.is_ascii_digit());
    (all_digits(month) && all_digits(day)).then(|| format!("{year}{month}{day}"))
}
