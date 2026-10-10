//! Counters and structured logs.
//!
//! Counters are plain atomics, read by the `crawler_stats` tool, so a long
//! session can be asked what it has been doing without a metrics server. Logs
//! go to stderr — stdout belongs to the protocol — as one JSON object per line,
//! and only when `MCPCRAWLER_LOG` asks for them.
//!
//! **Redaction.** Nothing here accepts a credential, and URLs are logged
//! without their query string or userinfo: a query is where tokens end up.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use url::Url;

macro_rules! counters {
    ($($name:ident),+ $(,)?) => {
        $(pub static $name: AtomicU64 = AtomicU64::new(0);)+

        /// Every counter as `name value` lines.
        pub fn snapshot() -> String {
            let mut out = String::new();
            $(out.push_str(&format!(
                "{} {}\n",
                stringify!($name).to_lowercase(),
                $name.load(Ordering::Relaxed)
            ));)+
            out
        }
    };
}

counters!(
    CRAWLS_STARTED,
    PAGES_FETCHED,
    FETCH_FAILURES,
    STATUS_2XX,
    STATUS_3XX,
    STATUS_4XX,
    STATUS_5XX,
    RETRIES,
    REDIRECTS_FOLLOWED,
    REDIRECTS_REFUSED,
    ROBOTS_SKIPS,
    BUDGET_EXITS,
    DEADLINE_EXITS,
    BYTES_FETCHED,
    TABS_OPENED,
    DISCOVERY_ROUNDS,
    SOURCE_FAILURES,
);

pub fn inc(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

pub fn add(counter: &AtomicU64, n: u64) {
    counter.fetch_add(n, Ordering::Relaxed);
}

/// Count a response by class.
pub fn status(code: u16) {
    match code {
        200..=299 => inc(&STATUS_2XX),
        300..=399 => inc(&STATUS_3XX),
        400..=499 => inc(&STATUS_4XX),
        _ => inc(&STATUS_5XX),
    }
}

/// A short identifier for one crawl, so its log lines can be grouped.
pub fn crawl_id() -> String {
    format!("c{}", CRAWLS_STARTED.fetch_add(1, Ordering::Relaxed) + 1)
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Level {
    Error,
    Warn,
    Info,
    Debug,
}

fn threshold() -> Option<Level> {
    match std::env::var("MCPCRAWLER_LOG").ok()?.trim().to_ascii_lowercase().as_str() {
        "error" => Some(Level::Error),
        "warn" => Some(Level::Warn),
        "info" => Some(Level::Info),
        "debug" => Some(Level::Debug),
        _ => None,
    }
}

/// A URL safe to write to a log: no userinfo, no query, no fragment.
pub fn redact(url: &Url) -> String {
    let mut clean = url.clone();
    let _ = clean.set_username("");
    let _ = clean.set_password(None);
    clean.set_query(None);
    clean.set_fragment(None);
    clean.to_string()
}

fn emit(level: Level, name: &str, crawl: &str, fields: &[(&str, String)]) {
    if threshold().is_none_or(|t| level > t) {
        return;
    }
    let at = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0);
    let mut object = serde_json::Map::new();
    object.insert("ts_ms".into(), at.into());
    object.insert("event".into(), name.into());
    object.insert("crawl".into(), crawl.into());
    for (key, value) in fields {
        object.insert((*key).into(), value.clone().into());
    }
    eprintln!("{}", serde_json::Value::Object(object));
}

pub fn info(name: &str, crawl: &str, fields: &[(&str, String)]) {
    emit(Level::Info, name, crawl, fields);
}

pub fn warn(name: &str, crawl: &str, fields: &[(&str, String)]) {
    emit(Level::Warn, name, crawl, fields);
}

pub fn debug(name: &str, crawl: &str, fields: &[(&str, String)]) {
    emit(Level::Debug, name, crawl, fields);
}
