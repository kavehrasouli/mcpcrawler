//! Tool responses: text for the model, structure for everything else.
//!
//! A tool that failed used to return a sentence as if it had succeeded, so a
//! client could not tell "no pages matched" from "the request was refused".
//! Failures now set `is_error` and carry a stable machine-readable code next to
//! the message; successes can carry the same facts as data — final URLs,
//! whether the result is partial — alongside the prose.

use crate::crawler::CrawlError;
use rmcp::model::{CallToolResult, ContentBlock};
use serde_json::{Value, json};

pub fn ok(text: String) -> CallToolResult {
    CallToolResult::success(vec![ContentBlock::text(text)])
}

/// A successful result that also carries structured data.
pub fn ok_with(text: String, data: Value) -> CallToolResult {
    let mut result = ok(text);
    result.structured_content = Some(data);
    result
}

/// A failure the caller should see. `code` is stable; `message` is for people.
pub fn fail(code: &str, message: String, url: Option<&str>) -> CallToolResult {
    let mut result = CallToolResult::error(vec![ContentBlock::text(format!("[{code}] {message}"))]);
    result.structured_content =
        Some(json!({ "error": { "code": code, "message": message, "url": url } }));
    result
}

/// A failure that came from the crawler, with the code its kind maps to.
pub fn fail_with(e: &CrawlError, doing: &str, url: Option<&str>) -> CallToolResult {
    fail(e.code(), format!("{doing}: {e}"), url)
}
