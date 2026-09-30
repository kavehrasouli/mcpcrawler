//! Fetching JSON APIs.
//!
//! Discovery sources are APIs, not pages, and [`fetch_page`](crate::crawler::fetch_page)
//! is deliberately wrong for them on three counts: it rejects anything that is
//! not HTML (GDELT serves JSON as `text/plain`), it cannot carry an auth
//! header, and it gives up on the first `429`. Rate limits are the normal
//! operating condition of a free API, so retrying with backoff belongs in the
//! fetch rather than in every caller.

// The federation layer these exist for lands in Phase 2; nothing in the crawler
// itself calls an API yet.
#![allow(dead_code)]

use crate::crawler::{CrawlError, allowed};
use reqwest::Client;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use url::Url;

/// Attempts for a request whose failure retrying could actually fix.
const MAX_ATTEMPTS: u32 = 3;
/// However long a server asks us to wait, we stop waiting here.
const MAX_RETRY_WAIT: Duration = Duration::from_secs(10);

/// Fetch and parse a JSON API response, retrying what is worth retrying.
pub async fn fetch_json(
    client: &Client,
    url: &Url,
    headers: &[(&str, &str)],
    max_body_bytes: usize,
) -> Result<serde_json::Value, CrawlError> {
    let body = fetch_body(client, url, headers, max_body_bytes).await?;
    parse_json(&body)
}

/// Parse a body as JSON, quoting what actually arrived when it is not.
///
/// The snippet is not decoration. An API that answers a rate limit with `200`
/// and a sentence of prose — GDELT does exactly this — otherwise surfaces as
/// "expected value at line 1 column 1", which tells the caller nothing about
/// what went wrong or how to fix it.
pub fn parse_json(body: &[u8]) -> Result<serde_json::Value, CrawlError> {
    serde_json::from_slice(body).map_err(|e| {
        let text = String::from_utf8_lossy(body);
        let snippet: String = text.trim().chars().take(200).collect();
        CrawlError::Json(format!("{e}; body began: {snippet}"))
    })
}

/// The raw bytes of an API response, with the same retry behaviour as
/// [`fetch_json`]. Sources whose API can answer with something other than JSON
/// need to see the body before it is parsed.
pub async fn fetch_body(
    client: &Client,
    url: &Url,
    headers: &[(&str, &str)],
    max_body_bytes: usize,
) -> Result<Vec<u8>, CrawlError> {
    allowed(url)?;

    let mut last_status = None;
    for attempt in 1..=MAX_ATTEMPTS {
        let mut request = client.get(url.clone());
        for (name, value) in headers {
            request = request.header(*name, *value);
        }

        match request.send().await {
            Ok(mut response) => {
                let status = response.status();
                if status.is_success() {
                    let mut body: Vec<u8> = Vec::new();
                    while let Some(chunk) = response.chunk().await? {
                        if body.len() + chunk.len() > max_body_bytes {
                            return Err(CrawlError::TooLarge(max_body_bytes));
                        }
                        body.extend_from_slice(&chunk);
                    }
                    return Ok(body);
                }

                last_status = Some(status.as_u16());
                if attempt == MAX_ATTEMPTS || !worth_retrying(status.as_u16()) {
                    return Err(CrawlError::Status(status.as_u16()));
                }
                // A server that says how long to wait knows better than the
                // backoff curve does.
                let wait = retry_after(&response).unwrap_or_else(|| backoff(attempt));
                tokio::time::sleep(wait).await;
            }
            Err(e) => {
                // A refused connection or a blown deadline may be transient; a
                // malformed request never is.
                if attempt == MAX_ATTEMPTS || !(e.is_timeout() || e.is_connect()) {
                    return Err(e.into());
                }
                tokio::time::sleep(backoff(attempt)).await;
            }
        }
    }

    Err(CrawlError::Status(last_status.unwrap_or(0)))
}

fn worth_retrying(status: u16) -> bool {
    matches!(status, 429 | 500 | 502 | 503 | 504)
}

fn retry_after(response: &reqwest::Response) -> Option<Duration> {
    let seconds: u64 = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()?;
    Some(Duration::from_secs(seconds).min(MAX_RETRY_WAIT))
}

/// Exponential backoff with enough jitter that concurrent retries do not all
/// fire on the same tick. The clock is the entropy source — a random-number
/// dependency would be a lot of crate for a quarter-second of spread.
fn backoff(attempt: u32) -> Duration {
    let base = Duration::from_millis(250 * (1_u64 << (attempt - 1)));
    let jitter = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::from(d.subsec_nanos()) % 250)
        .unwrap_or(0);
    base + Duration::from_millis(jitter)
}
