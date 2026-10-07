//! Near-duplicate detection: counting stories, not copies.
//!
//! A single wire report of a state visit appears, near-verbatim, on hundreds
//! of sites. Counted as pages it looks like hundreds of sources agreeing; it is
//! one source, repeated. Anything that later asks "how many independent
//! reports attest this" has to collapse the copies first, or one syndicated
//! story outweighs three genuinely separate ones.
//!
//! The fingerprint is a 64-bit SimHash over three-word shingles: similar texts
//! give fingerprints that differ in few bits, so closeness is a popcount rather
//! than a comparison of two documents. A distance of 3 or less is "the same
//! story" — a different headline, a byline, a footer and a "related articles"
//! box all move a handful of shingles out of hundreds and flip very few bits.

use crate::crawler::{FetchedPage, extract_text, normalize_domain, published_raw};
use crate::scoring::normalise;

/// Bits that may differ and still be one story.
pub const SAME_STORY: u32 = 3;
/// Fewer words than this and the fingerprint is noise: a "page not found" or a
/// cookie notice would match every other short page on any site.
const MIN_WORDS: usize = 25;
/// Words per shingle. One gives a bag of words that ignores order; much more
/// and a single edit disturbs too many shingles.
const SHINGLE: usize = 3;

/// `None` for text too short to fingerprint, which is never clustered.
pub fn fingerprint(text: &str) -> Option<u64> {
    let normalised = normalise(text);
    let words: Vec<&str> = normalised.split(' ').filter(|w| !w.is_empty()).collect();

    // Scripts written without spaces (Chinese, Japanese) normalise to a few
    // enormous "words"; shingling those by word gives one shingle. Fall back
    // to character runs, which is what a word is in those scripts anyway.
    let shingles: Vec<String> = if words.len() >= MIN_WORDS {
        words.windows(SHINGLE).map(|w| w.join(" ")).collect()
    } else {
        let chars: Vec<char> = normalised.chars().filter(|c| *c != ' ').collect();
        if chars.len() < MIN_WORDS * 4 {
            return None;
        }
        chars.windows(SHINGLE + 2).map(|w| w.iter().collect()).collect()
    };

    let mut votes = [0i32; 64];
    for shingle in &shingles {
        let hash = hash64(shingle.as_bytes());
        for (bit, vote) in votes.iter_mut().enumerate() {
            *vote += if hash >> bit & 1 == 1 { 1 } else { -1 };
        }
    }
    Some(votes.iter().enumerate().fold(0u64, |acc, (bit, v)| acc | (u64::from(*v > 0) << bit)))
}

pub fn distance(a: u64, b: u64) -> u32 {
    (a ^ b).count_ones()
}

/// FNV-1a, then a finaliser. Written out rather than taken from the standard
/// hasher, whose output is allowed to change between Rust releases — a
/// fingerprint that stops matching its own earlier values after a toolchain
/// update is a bug nobody can see.
fn hash64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    // FNV mixes the high bits poorly on short inputs; SimHash votes on every
    // bit, so they have to be good.
    hash ^= hash >> 30;
    hash = hash.wrapping_mul(0xbf58476d1ce4e5b9);
    hash ^= hash >> 27;
    hash = hash.wrapping_mul(0x94d049bb133111eb);
    hash ^ hash >> 31
}

/// Group indices whose fingerprints are within `threshold` of a cluster's
/// first member. Entries with no fingerprint each stand alone.
///
/// Compared against the first member and not against any member: chaining
/// ("A is close to B, B to C") lets a slow drift join texts that have nothing
/// in common, which is the opposite of the claim a cluster makes.
pub fn cluster(prints: &[Option<u64>], threshold: u32) -> Vec<Vec<usize>> {
    let mut clusters: Vec<(Option<u64>, Vec<usize>)> = Vec::new();
    for (index, print) in prints.iter().enumerate() {
        let home = print.and_then(|p| {
            clusters
                .iter()
                .position(|(lead, _)| lead.is_some_and(|l| distance(l, p) <= threshold))
        });
        match home {
            Some(at) => clusters[at].1.push(index),
            None => clusters.push((*print, vec![index])),
        }
    }
    clusters.into_iter().map(|(_, members)| members).collect()
}

/// One story and every copy of it.
#[derive(Debug, PartialEq, Eq)]
pub struct Group {
    /// The copy to show: the most relevant, the earliest fetched on a tie.
    pub best: usize,
    /// The other copies, in fetch order.
    pub copies: Vec<usize>,
}

/// Cluster fetched pages by the text they carry, not the markup around it.
pub fn group_pages(pages: &[FetchedPage]) -> Vec<Group> {
    let prints: Vec<Option<u64>> =
        pages.iter().map(|page| fingerprint(&extract_text(&page.html))).collect();

    cluster(&prints, SAME_STORY)
        .into_iter()
        .map(|members| {
            let best = *members
                .iter()
                .reduce(|best, candidate| {
                    // Strictly greater, so an equal score keeps the earlier page.
                    if pages[*candidate].relevance > pages[*best].relevance {
                        candidate
                    } else {
                        best
                    }
                })
                .expect("a cluster has at least one member");
            let copies = members.into_iter().filter(|m| *m != best).collect();
            Group { best, copies }
        })
        .collect()
}

/// `2024-03-05T10:00:00+03:00` -> `20240305`.
///
/// Only an ISO-shaped date, with `-` or `/` between the parts: a page that
/// says "5 March" is not guessed at, because a wrong date is worse than none
/// in a record someone will sort by. The time and zone are dropped, so a story
/// published near midnight can land a day off. Years outside 1990 to
/// `latest_year` are refused as the junk they nearly always are.
pub fn ymd(raw: &str, latest_year: i32) -> Option<String> {
    let head: String = raw.trim().chars().take(10).collect();
    let mut parts = head.split(['-', '/']);
    let (year, month, day) = (parts.next()?, parts.next()?, parts.next()?);
    let digits = |s: &str, len: usize| s.len() == len && s.chars().all(|c| c.is_ascii_digit());
    if !(digits(year, 4) && digits(month, 2) && digits(day, 2)) {
        return None;
    }
    let (y, m, d): (i32, u32, u32) = (year.parse().ok()?, month.parse().ok()?, day.parse().ok()?);
    ((1990..=latest_year).contains(&y) && (1..=12).contains(&m) && (1..=31).contains(&d))
        .then(|| format!("{year}{month}{day}"))
}

/// When a story first appeared: the earliest date among its copies. A
/// syndicated copy is dated when it was reposted, so the original is the
/// earliest.
pub fn story_date(pages: &[FetchedPage], group: &Group, latest_year: i32) -> Option<String> {
    std::iter::once(&group.best)
        .chain(&group.copies)
        .filter_map(|&i| ymd(&published_raw(&pages[i].html)?, latest_year))
        .min()
}

/// The distinct hosts carrying a story, the shown copy's first.
///
/// This is what independence means here: two copies on one host are one
/// outlet repeating itself. It is host-level, not organisation-level —
/// `news.example.com` and `example.com` count as two, and two outlets owned by
/// one group count as two — so it overstates independence, never understates.
pub fn story_hosts(pages: &[FetchedPage], group: &Group) -> Vec<String> {
    let mut hosts: Vec<String> = Vec::new();
    for &i in std::iter::once(&group.best).chain(&group.copies) {
        if let Some(host) = pages[i].url.host_str() {
            let host = normalize_domain(host).to_string();
            if !hosts.contains(&host) {
                hosts.push(host);
            }
        }
    }
    hosts
}
