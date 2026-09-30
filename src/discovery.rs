//! Source federation: turning a question into candidate URLs.
//!
//! Every tool before this one takes a `url` as its first argument, because the
//! design assumed you already know where to look. The questions this project
//! exists to answer do not come with a seed: *every visit the president of X
//! made to Y* is scattered across a ministry's press archive, a wire story that
//! ran in two hundred syndicated copies, and a page deleted in 2019 that now
//! lives only in the Wayback Machine. None of those are reachable by walking
//! links outward from a homepage.
//!
//! So discovery is not a crawl problem. It is a federation problem: fan one
//! query across every backend that indexes something the others do not, and
//! merge what comes back into a single frontier that still remembers which
//! backend found what.
//!
//! **On robots.txt.** Nothing in this module consults it. A documented API is
//! an interface published for programmatic use, and its terms are its terms —
//! `robots.txt` governs crawling a site's pages, which is a different act. It
//! still applies in full to every page the crawler fetches from these results.

use crate::crawler::{CrawlError, Origin, Seed, dedup_key};
use async_trait::async_trait;
use futures::future::join_all;
use reqwest::Client;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

/// What to look for.
#[derive(Debug, Clone)]
pub struct Query {
    pub text: String,
    /// Inclusive window as `YYYYMMDD` dates. Sources widen or narrow it to
    /// whatever granularity their API accepts, and ignore it if they have no
    /// notion of time at all.
    pub since: Option<String>,
    pub until: Option<String>,
    /// Hosts worth expanding wholesale.
    ///
    /// Not every backend searches text. An archive index — Wayback, Common
    /// Crawl — takes a URL pattern and nothing else, so it has nothing to do
    /// until something upstream names a site. Wikidata's official-website
    /// claim is one way to fill this; the domains of an earlier round's
    /// results are another.
    pub sites: Vec<String>,
}

impl Query {
    /// The query text, trimmed, or `None` when there is nothing to search for.
    pub fn text(&self) -> Option<&str> {
        Some(self.text.trim()).filter(|t| !t.is_empty())
    }
}

/// What one federation round is allowed to spend.
///
/// Without this a round is unbounded in exactly the direction that hurts: the
/// page budget governs what the crawler fetches *afterwards*, and says nothing
/// about how many API calls were made to decide what to fetch. Phase 3 turns
/// one phrase into a few dozen queries, so the multiplier is real.
#[derive(Debug, Clone)]
pub struct DiscoveryBudget {
    /// API requests one source may make. Most sources answer in one.
    pub max_requests: usize,
    /// Candidates kept after merging. Sources may return more; the merge cuts.
    pub max_candidates: usize,
    /// Wall clock for one source. Applied per source, not to the round, so one
    /// slow backend cannot cost you the results the others already returned.
    pub deadline: Duration,
}

impl Default for DiscoveryBudget {
    fn default() -> Self {
        Self {
            // Two is the floor for a source that must look an entity up
            // before it can ask about it, which Wikidata does.
            max_requests: 4,
            max_candidates: 50,
            // Archive indexes are slow by nature: a whole-domain CDX query
            // answers in about seven seconds healthy, and minutes when not.
            deadline: Duration::from_secs(60),
        }
    }
}

/// One candidate URL, with what the source that found it knew about it.
#[derive(Debug, Clone)]
pub struct Lead {
    pub url: Url,
    /// Higher is more promising, on an arbitrary scale each source sets for
    /// itself. Comparable within a source; only roughly comparable across them.
    pub score: f32,
    pub title: Option<String>,
    /// When the source saw it, in whatever format the source reports.
    pub seen: Option<String>,
    pub domain: Option<String>,
    pub language: Option<String>,
    /// Every source that returned this URL. A URL two independent backends
    /// found is a different thing from one that only appeared once — counting
    /// that properly is Phase 5's job, but the evidence has to survive to
    /// reach it.
    pub sources: Vec<Arc<str>>,
}

impl Lead {
    /// Hand this lead to the crawler, keeping where it came from.
    pub fn into_seed(self) -> Seed {
        let origin = match self.sources.first() {
            Some(name) => Origin::Source(Arc::clone(name)),
            None => Origin::Seed,
        };
        Seed::scored(self.url, origin, self.score)
    }
}

/// What a source found: candidate URLs, and any further names for the subject
/// it learned on the way.
#[derive(Debug, Default)]
pub struct Found {
    pub leads: Vec<Lead>,
    /// Other names for the same subject — aliases, transliterations,
    /// native-script forms.
    ///
    /// These are worth as much as the URLs. A crawler that does not know
    /// *Садыр Жапаров* is *Sadyr Japarov* scores every link written in the
    /// subject's own language at zero, which is most of the coverage that
    /// exists for a non-anglophone subject.
    pub terms: Vec<String>,
}

impl Found {
    pub fn leads(leads: Vec<Lead>) -> Self {
        Self { leads, terms: Vec::new() }
    }
}

/// A backend that can turn a query into candidate URLs.
///
/// One trait, one file per implementation: adding Wikidata or Wayback later
/// should not touch anything that already works.
#[async_trait]
pub trait DiscoverySource: Debug + Send + Sync {
    /// Stable identifier. It becomes `Origin::Source(name)` on every URL this
    /// source contributes, so it ends up in crawl reports — keep it short.
    fn name(&self) -> Arc<str>;

    async fn discover(
        &self,
        client: &Client,
        query: &Query,
        budget: &DiscoveryBudget,
    ) -> Result<Found, CrawlError>;
}

/// The result of one federation round.
#[derive(Debug, Default)]
pub struct Federated {
    /// Merged and ranked, best first.
    pub leads: Vec<Lead>,
    /// Every name the round learned for the subject, deduplicated. What the
    /// crawler's link scoring is given to match against.
    pub terms: Vec<String>,
    /// Sources that failed, and why. A round does not fail because one backend
    /// is down — it reports the gap and returns what the others found.
    pub failures: Vec<(String, String)>,
}

impl Federated {
    pub fn seeds(self) -> Vec<Seed> {
        self.leads.into_iter().map(Lead::into_seed).collect()
    }
}

/// Run every source against one query, in parallel, and merge the results.
pub async fn federate(
    client: &Client,
    sources: &[Box<dyn DiscoverySource>],
    query: &Query,
    budget: &DiscoveryBudget,
) -> Federated {
    let rounds = sources.iter().map(|source| async move {
        let name = source.name();
        // Per source, so a backend that hangs costs only its own results.
        match tokio::time::timeout(budget.deadline, source.discover(client, query, budget)).await {
            Ok(Ok(leads)) => (name, Ok(leads)),
            Ok(Err(e)) => (name, Err(e.to_string())),
            Err(_) => (name, Err(format!("timed out after {:?}", budget.deadline))),
        }
    });

    let mut found: Vec<Lead> = Vec::new();
    let mut terms: Vec<String> = Vec::new();
    let mut failures = Vec::new();
    for (name, result) in join_all(rounds).await {
        match result {
            Ok(answer) => {
                found.extend(answer.leads);
                for term in answer.terms {
                    if !terms.contains(&term) {
                        terms.push(term);
                    }
                }
            }
            Err(why) => failures.push((name.to_string(), why)),
        }
    }

    Federated { leads: merge(found, budget.max_candidates), terms, failures }
}

/// Collapse duplicate URLs, keeping every source that found one.
///
/// The merged score is the best any source gave, not a sum: a URL is not more
/// relevant because two backends agree, it is more *corroborated*, and those
/// are different claims. The corroboration is recorded in `sources` for the
/// stage that knows what to do with it.
fn merge(leads: Vec<Lead>, max_candidates: usize) -> Vec<Lead> {
    let mut by_url: HashMap<String, Lead> = HashMap::new();
    let mut order: Vec<String> = Vec::new();

    for lead in leads {
        let key = dedup_key(&lead.url);
        match by_url.get_mut(&key) {
            Some(existing) => {
                if lead.score > existing.score {
                    existing.score = lead.score;
                }
                for source in lead.sources {
                    if !existing.sources.contains(&source) {
                        existing.sources.push(source);
                    }
                }
                // Keep whatever detail the first source lacked.
                existing.title = existing.title.take().or(lead.title);
                existing.seen = existing.seen.take().or(lead.seen);
                existing.domain = existing.domain.take().or(lead.domain);
                existing.language = existing.language.take().or(lead.language);
            }
            None => {
                order.push(key.clone());
                by_url.insert(key, lead);
            }
        }
    }

    let merged: Vec<Lead> = order.into_iter().filter_map(|k| by_url.remove(&k)).collect();
    share_out(merged, max_candidates)
}

/// Fill the candidate budget by taking turns between sources, best first
/// within each.
///
/// Taking the global top N instead looks right and is not. Scores are only
/// comparable *within* a source — each sets its own scale — so a global sort
/// lets one backend's ranking decide the whole round. Live, that is not
/// theoretical: Wikidata returns every language edition of an article at one
/// score, and sixty-one of those swallowed a budget of ten whole, leaving the
/// archive results that were the entire reason for asking with nothing. Taking
/// turns means every backend is represented in what gets crawled, which is the
/// point of federating at all.
fn share_out(leads: Vec<Lead>, max_candidates: usize) -> Vec<Lead> {
    let mut by_source: Vec<(Arc<str>, Vec<Lead>)> = Vec::new();
    for lead in leads {
        // Grouped under the source that scored it highest, which is the one
        // that thinks it is most relevant.
        let owner = Arc::clone(&lead.sources[0]);
        match by_source.iter_mut().find(|(name, _)| *name == owner) {
            Some((_, group)) => group.push(lead),
            None => by_source.push((owner, vec![lead])),
        }
    }
    for (_, group) in &mut by_source {
        group.sort_by(|a, b| b.score.total_cmp(&a.score));
    }

    let mut taken = Vec::with_capacity(max_candidates);
    let mut round = 0;
    while taken.len() < max_candidates {
        let mut handed_out = false;
        for (_, group) in &mut by_source {
            if taken.len() >= max_candidates {
                break;
            }
            if round < group.len() {
                taken.push(group[round].clone());
                handed_out = true;
            }
        }
        // Every source exhausted: nothing left to hand out.
        if !handed_out {
            break;
        }
        round += 1;
    }
    taken
}
