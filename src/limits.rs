//! Ceilings that hold across calls, not just within one.
//!
//! Every crawl has its own page budget, concurrency and deadline, and those
//! bound a single call. They say nothing about several calls at once: four
//! callers each running eight workers is thirty-two requests in flight, each
//! with its own browser tab if they asked for one. The limits here are
//! process-wide, so the per-call limits cannot be multiplied past them.

use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Semaphore;

/// HTTP requests in flight across every crawl in the process.
pub const MAX_HTTP: usize = 32;
/// Browser tabs open at once. A tab costs far more than a socket — memory for a
/// whole rendering engine — so it has its own, much smaller, ceiling.
pub const MAX_TABS: usize = 3;
/// Crawl calls running at once. Further calls wait their turn.
pub const MAX_CRAWLS: usize = 4;

/// URLs a single frontier may hold. A link farm can offer millions; past this
/// they are dropped, not queued.
pub const MAX_FRONTIER: usize = 50_000;

/// `robots.txt` is read to this size and no further. The convention is that a
/// crawler parses at least 500 KiB; anything beyond is ignored, not an error.
pub const MAX_ROBOTS_BYTES: usize = 512 * 1024;
/// What one compressed body may expand to. The sitemap protocol caps a file at
/// 50 MB uncompressed, so this leaves room and no more. Compression ratios of a
/// thousand to one are ordinary for repetitive XML, so a size limit on the wire
/// says nothing about the size in memory.
pub const MAX_DECODED_BYTES: usize = 64 * 1024 * 1024;
/// The rendered DOM of one page. A page script can grow this without bound.
pub const MAX_DOM_BYTES: usize = 20 * 1024 * 1024;
/// XML events and nesting accepted from one document.
pub const MAX_XML_EVENTS: usize = 5_000_000;
pub const MAX_XML_DEPTH: usize = 64;

pub static HTTP: Semaphore = Semaphore::const_new(MAX_HTTP);
pub static TABS: Semaphore = Semaphore::const_new(MAX_TABS);
pub static CRAWLS: Semaphore = Semaphore::const_new(MAX_CRAWLS);

static SHUTTING_DOWN: AtomicBool = AtomicBool::new(false);

/// Ask every crawl to stop taking new work. Set on SIGINT, SIGTERM and client
/// disconnect, before the browser is torn down, so nothing starts a fetch into
/// a process that is leaving.
pub fn begin_shutdown() {
    SHUTTING_DOWN.store(true, Ordering::SeqCst);
}

pub fn shutting_down() -> bool {
    SHUTTING_DOWN.load(Ordering::SeqCst)
}
