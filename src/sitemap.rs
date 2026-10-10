//! Sitemap discovery and traversal.
//!
//! Large sites publish their own URL inventory in `sitemap.xml` precisely so
//! crawlers do not have to guess. It is the cheapest entry point available and
//! frequently the only workable one: a client-rendered storefront can serve a
//! shell with zero links in its HTML while its sitemap index lists millions of
//! pages, all reachable with plain GETs and no browser.

use crate::crawler::{
    CrawlError, HostSlots, USER_AGENT, canonicalize, fetch_bytes, host_of, read_truncated,
    throttle,
};
use crate::limits;
use futures::future::join_all;
use quick_xml::Reader;
use quick_xml::events::Event;
use reqwest::Client;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Read;
use std::sync::Mutex;
use std::time::Duration;
use texting_robots::Robot;
use url::Url;

/// Conventional locations to try when `robots.txt` names no sitemap.
const FALLBACK_PATHS: &[&str] = &["/sitemap.xml", "/sitemap_index.xml", "/sitemap-index.xml"];

#[derive(Debug, Clone)]
pub struct SitemapEntry {
    pub url: Url,
    pub lastmod: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SitemapConfig {
    /// Stop after collecting this many URLs. A single index can list millions.
    pub max_urls: usize,
    /// Stop after reading this many sitemap documents.
    pub max_sitemaps: usize,
    /// How deep nested sitemap indexes may go.
    pub max_depth: u32,
    pub concurrency: usize,
    pub per_host_delay: Duration,
    pub max_body_bytes: usize,
    /// What one document may expand to once decompressed.
    pub max_decoded_bytes: usize,
    /// Decompressed bytes across every document in the walk. Without it a site
    /// can hand over a thousand files that are each under the limit.
    pub max_total_bytes: usize,
    /// Keep only URLs containing this substring, matched case-insensitively.
    /// Applied while walking, so a filtered scan can cover far more of a large
    /// index within the same `max_urls` budget.
    pub contains: Option<String>,
}

impl Default for SitemapConfig {
    fn default() -> Self {
        Self {
            max_urls: 5_000,
            max_sitemaps: 50,
            max_depth: 3,
            concurrency: 5,
            per_host_delay: Duration::from_millis(300),
            // Sitemaps are far larger than pages; a gzipped child can be tens of MB.
            max_body_bytes: 64 * 1024 * 1024,
            max_decoded_bytes: limits::MAX_DECODED_BYTES,
            max_total_bytes: 4 * limits::MAX_DECODED_BYTES,
            contains: None,
        }
    }
}

#[derive(Debug, Default)]
pub struct SitemapOutcome {
    pub entries: Vec<SitemapEntry>,
    pub sitemaps_read: usize,
    /// Child sitemaps discovered but not read, because a budget ran out.
    pub sitemaps_pending: usize,
    pub failed: Vec<(String, String)>,
    pub truncated: bool,
}

impl SitemapOutcome {
    pub fn urls(&self) -> Vec<String> {
        self.entries.iter().map(|e| e.url.to_string()).collect()
    }
}

// ---------------------------------------------------------------------------
// discovery
// ---------------------------------------------------------------------------

/// Find a site's sitemaps: those declared in `robots.txt` first, then the
/// conventional paths.
pub async fn discover(client: &Client, site: &str) -> Result<Vec<Url>, CrawlError> {
    let base = canonicalize(site)?;
    let origin = base.origin().ascii_serialization();
    let mut found: Vec<Url> = Vec::new();

    // Read to a fixed size, like every other robots.txt fetch: a sitemap
    // directive past the limit is not worth an unbounded read.
    if let Ok(response) = client.get(format!("{origin}/robots.txt")).send().await
        && response.status().is_success()
        && let body = read_truncated(response, limits::MAX_ROBOTS_BYTES).await
        && let Ok(robot) = Robot::new(USER_AGENT, &body)
    {
        found.extend(
            robot
                .sitemaps
                .iter()
                .filter_map(|s| base.join(s).ok())
                .filter_map(|u| canonicalize(u.as_str()).ok()),
        );
    }

    if found.is_empty() {
        found.extend(
            FALLBACK_PATHS
                .iter()
                .filter_map(|path| canonicalize(&format!("{origin}{path}")).ok()),
        );
    }

    let mut seen = HashSet::new();
    found.retain(|u| seen.insert(u.to_string()));
    Ok(found)
}

/// Whether a URL is itself a sitemap rather than a site to look one up for.
fn looks_like_sitemap(url: &Url) -> bool {
    let path = url.path().to_ascii_lowercase();
    path.ends_with(".xml") || path.ends_with(".xml.gz") || path.contains("sitemap")
}

// ---------------------------------------------------------------------------
// parsing
// ---------------------------------------------------------------------------

enum Document {
    /// A `<sitemapindex>`: these entries are more sitemaps to read.
    Index(Vec<SitemapEntry>),
    /// A `<urlset>`: these entries are pages.
    Urls(Vec<SitemapEntry>),
}

/// Transparently gunzip, refusing a document that expands past `limit`.
///
/// Child sitemaps are usually served as `.gz` files, which is content encoding
/// the HTTP layer does not undo for us. The limit is on the *decoded* size: a
/// few kilobytes of gzip can stand for gigabytes, and a cap on the bytes that
/// crossed the wire never sees them. Data that only looks like gzip is passed
/// through as it came.
fn maybe_gunzip(bytes: Vec<u8>, limit: usize) -> Result<Vec<u8>, CrawlError> {
    if !bytes.starts_with(&[0x1f, 0x8b]) {
        return Ok(bytes);
    }
    let mut decoded = Vec::new();
    // One byte past the limit is enough to know it was exceeded.
    let mut reader = flate2::read::GzDecoder::new(&bytes[..]).take(limit as u64 + 1);
    match reader.read_to_end(&mut decoded) {
        Ok(_) if decoded.len() > limit => Err(CrawlError::TooLarge(limit)),
        Ok(_) => Ok(decoded),
        Err(_) => Ok(bytes),
    }
}

/// `base` is the sitemap's own URL. The spec calls for absolute `<loc>` values, but
/// relative ones appear in the wild and resolving them costs nothing.
fn parse(xml: &str, base: &Url) -> Result<Document, CrawlError> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut is_index = false;
    let mut entries = Vec::new();
    // The open elements, by local name. `<loc>` counts only as a direct child
    // of `<url>` or `<sitemap>`: image, video and news extensions each nest a
    // `<loc>` of their own, and taking any `<loc>` let an image's address
    // overwrite the page's.
    let mut path: Vec<String> = Vec::new();
    let mut loc: Option<String> = None;
    let mut lastmod: Option<String> = None;
    let mut events = 0usize;

    let field = |path: &[String], text: String, loc: &mut Option<String>, lastmod: &mut Option<String>| {
        let [.., parent, name] = path else { return };
        if parent != "url" && parent != "sitemap" {
            return;
        }
        match name.as_str() {
            "loc" => *loc = Some(text),
            "lastmod" => *lastmod = Some(text),
            _ => {}
        }
    };

    loop {
        events += 1;
        // A document of nothing but tiny elements fits comfortably under a byte
        // limit and still costs unbounded work to walk.
        if events > limits::MAX_XML_EVENTS {
            return Err(CrawlError::Parse("document has too many elements".to_string()));
        }
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                // Sitemaps are namespaced, so match on the local name only.
                let name = e.local_name().as_ref().to_string();
                if name == "sitemapindex" {
                    is_index = true;
                }
                path.push(name);
                if path.len() > limits::MAX_XML_DEPTH {
                    return Err(CrawlError::Parse("elements nested too deeply".to_string()));
                }
            }
            Ok(Event::Text(e)) => {
                let raw = e.into_inner();
                let text = match quick_xml::escape::unescape(&raw) {
                    Ok(unescaped) => unescaped.trim().to_string(),
                    Err(_) => raw.trim().to_string(),
                };
                if !text.is_empty() {
                    field(&path, text, &mut loc, &mut lastmod);
                }
            }
            // `<loc><![CDATA[…]]></loc>` is common in generated sitemaps.
            Ok(Event::CData(e)) => {
                let text = e.into_inner().trim().to_string();
                if !text.is_empty() {
                    field(&path, text, &mut loc, &mut lastmod);
                }
            }
            Ok(Event::End(e)) => {
                let name = e.local_name().as_ref().to_string();
                if (name == "sitemap" || name == "url") && path.len() >= 2 {
                    if let Some(raw) = loc.take()
                        && let Ok(joined) = base.join(&raw)
                        && let Ok(url) = canonicalize(joined.as_str())
                    {
                        entries.push(SitemapEntry { url, lastmod: lastmod.clone() });
                    }
                    lastmod = None;
                }
                path.pop();
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(CrawlError::Parse(e.to_string())),
            _ => {}
        }
    }

    Ok(if is_index {
        Document::Index(entries)
    } else {
        Document::Urls(entries)
    })
}

// ---------------------------------------------------------------------------
// traversal
// ---------------------------------------------------------------------------

/// Read a site's sitemaps and return the URLs they list.
///
/// `start` may be either a site (its sitemaps are discovered) or a sitemap URL
/// (read directly). Nested indexes are followed breadth-first so the shallowest,
/// most general sitemaps are read before deeper ones.
pub async fn collect(
    client: &Client,
    start: &str,
    cfg: &SitemapConfig,
) -> Result<SitemapOutcome, CrawlError> {
    let parsed = canonicalize(start)?;
    let roots = if looks_like_sitemap(&parsed) {
        vec![parsed]
    } else {
        discover(client, start).await?
    };

    let slots: HostSlots = Mutex::new(HashMap::new());
    let needle = cfg.contains.as_ref().map(|c| c.to_lowercase());

    let mut queue: VecDeque<(Url, u32)> = roots.into_iter().map(|u| (u, 0)).collect();
    let mut seen: HashSet<String> = queue.iter().map(|(u, _)| u.to_string()).collect();
    let mut outcome = SitemapOutcome::default();
    let mut decoded_total = 0usize;

    while !queue.is_empty() {
        // Every attempt spends the sitemap budget, successful or not: a failed
        // fetch still cost a request.
        let attempts = outcome.sitemaps_read + outcome.failed.len();
        if outcome.entries.len() >= cfg.max_urls
            || attempts >= cfg.max_sitemaps
            || decoded_total >= cfg.max_total_bytes
        {
            outcome.truncated = true;
            break;
        }

        // One batch at a time keeps concurrency bounded without a worker pool;
        // every task here is waiting on the network. The batch is cut to what
        // is left of the budget, so the limit holds exactly and not "to within
        // one batch".
        let room = cfg.max_sitemaps - attempts;
        let batch: Vec<(Url, u32)> = (0..cfg.concurrency.max(1).min(room))
            .filter_map(|_| queue.pop_front())
            .collect();

        let results = join_all(batch.into_iter().map(|(url, depth)| {
            let slots = &slots;
            async move {
                throttle(slots, &host_of(&url), cfg.per_host_delay).await;
                let fetched = fetch_bytes(client, &url, cfg.max_body_bytes).await;
                (url, depth, fetched)
            }
        }))
        .await;

        for (url, depth, fetched) in results {
            let bytes = match fetched {
                Ok(bytes) => bytes,
                Err(e) => {
                    outcome.failed.push((url.to_string(), e.to_string()));
                    continue;
                }
            };

            let decoded = match maybe_gunzip(bytes, cfg.max_decoded_bytes) {
                Ok(decoded) => decoded,
                Err(e) => {
                    outcome.failed.push((url.to_string(), e.to_string()));
                    continue;
                }
            };
            decoded_total += decoded.len();
            let xml = String::from_utf8_lossy(&decoded).into_owned();
            let document = match parse(&xml, &url) {
                Ok(document) => document,
                Err(e) => {
                    outcome.failed.push((url.to_string(), e.to_string()));
                    continue;
                }
            };
            outcome.sitemaps_read += 1;

            match document {
                Document::Index(children) => {
                    if depth >= cfg.max_depth {
                        outcome.sitemaps_pending += children.len();
                        continue;
                    }
                    for child in children {
                        if seen.insert(child.url.to_string()) {
                            queue.push_back((child.url, depth + 1));
                        }
                    }
                }
                Document::Urls(urls) => {
                    for entry in urls {
                        if outcome.entries.len() >= cfg.max_urls {
                            outcome.truncated = true;
                            break;
                        }
                        // Filtering here rather than after collection means a
                        // narrow search can scan much deeper into a large index.
                        let keep = needle
                            .as_ref()
                            .is_none_or(|n| entry.url.as_str().to_lowercase().contains(n));
                        if keep {
                            outcome.entries.push(entry);
                        }
                    }
                }
            }
        }
    }

    outcome.sitemaps_pending += queue.len();
    Ok(outcome)
}
