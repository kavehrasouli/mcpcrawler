//! Structured data already present in static HTML.
//!
//! A page built with React or Next.js serves a shell with no `<a>` tags, but it
//! is rarely empty of *data*: the framework ships the props it rendered from as
//! JSON in a `<script>` tag, and publishers separately emit schema.org records
//! as JSON-LD so search engines do not have to infer them. Both sit in the
//! bytes a plain GET already returns, so reading them costs one request and no
//! browser — the cheap half of reaching a client-rendered site, and the half
//! that yields records rather than prose.

use crate::crawler::{canonicalize, dedup_key};
use scraper::{Html, Selector};
use serde_json::Value;
use std::collections::HashSet;
use std::sync::LazyLock;
use url::Url;

/// Globals a single-page app publishes its server state under. All of them are
/// assigned in an inline script; `__NEXT_DATA__` and `__NUXT_DATA__` also appear
/// as `<script id=…  type="application/json">`, which is the easier path.
const STATE_GLOBALS: &[&str] = &[
    "__NEXT_DATA__",
    "__NUXT_DATA__",
    "__NUXT__",
    "__INITIAL_STATE__",
    "__PRELOADED_STATE__",
    "__APOLLO_STATE__",
    "__remixContext",
];

/// Build artifacts referenced from the same state blob as the content URLs.
/// They are numerous, uniform, and never worth fetching.
const ASSET_EXTENSIONS: &[&str] = &[
    ".js", ".mjs", ".css", ".map", ".png", ".jpg", ".jpeg", ".gif", ".webp", ".avif", ".svg",
    ".ico", ".woff", ".woff2", ".ttf", ".otf", ".mp4", ".webm", ".mp3", ".pdf", ".zip",
];

/// Lines of flattened output per JSON-LD record. A `Product` with every offer
/// expanded can run to hundreds; the first dozens carry the identifying facts.
const MAX_RECORD_LINES: usize = 60;
/// Lines of shape outline per state blob.
const MAX_OUTLINE_LINES: usize = 120;
/// Characters of any single string value.
const MAX_VALUE_CHARS: usize = 300;

#[derive(Debug, Clone)]
pub struct StructuredData {
    /// schema.org records, one entry per node — arrays and `@graph` containers
    /// are flattened away so callers never have to unwrap them.
    pub json_ld: Vec<Value>,
    /// Framework state blobs, in document order.
    pub embedded: Vec<EmbeddedState>,
}

#[derive(Debug, Clone)]
pub struct EmbeddedState {
    /// The script id or global it was published under, e.g. `__NEXT_DATA__`.
    pub name: String,
    pub value: Value,
    /// Size of the raw JSON text, which the outline deliberately elides.
    pub raw_len: usize,
}

impl StructuredData {
    pub fn is_empty(&self) -> bool {
        self.json_ld.is_empty() && self.embedded.is_empty()
    }

    /// The `@type` of every record, deduplicated. Cheap signal for whether a
    /// page is worth reading in full.
    pub fn schema_types(&self) -> Vec<String> {
        let mut seen = HashSet::new();
        let mut types = Vec::new();
        for node in &self.json_ld {
            for t in type_names(node) {
                if seen.insert(t.clone()) {
                    types.push(t);
                }
            }
        }
        types
    }

    /// First non-empty value at any of `keys`, searched across every record.
    /// Objects are unwrapped by their `name` field, which is how schema.org
    /// expresses `author` and `publisher`.
    pub fn field(&self, keys: &[&str]) -> Option<String> {
        self.json_ld
            .iter()
            .flat_map(|node| keys.iter().filter_map(move |k| node.get(*k)))
            .filter_map(named_value)
            .find(|v| !v.is_empty())
    }
}

pub fn extract(html: &str) -> StructuredData {
    static SCRIPT: LazyLock<Selector> = LazyLock::new(|| Selector::parse("script").unwrap());

    let document = Html::parse_document(html);
    let mut json_ld = Vec::new();
    let mut embedded: Vec<EmbeddedState> = Vec::new();

    for el in document.select(&SCRIPT) {
        let attr = |name: &str| el.value().attr(name).unwrap_or_default().trim().to_lowercase();
        let text = el.text().collect::<String>();

        if attr("type") == "application/ld+json" {
            if let Ok(value) = serde_json::from_str::<Value>(strip_wrappers(&text)) {
                push_records(value, &mut json_ld);
            }
            continue;
        }

        let id = el.value().attr("id").unwrap_or_default();
        if STATE_GLOBALS.contains(&id) {
            if let Ok(value) = serde_json::from_str::<Value>(strip_wrappers(&text)) {
                let raw_len = text.trim().len();
                push_state(id.to_string(), value, raw_len, &mut embedded);
            }
            continue;
        }

        for global in STATE_GLOBALS {
            if let Some(raw) = assigned_json(&text, global)
                && let Ok(value) = serde_json::from_str::<Value>(raw)
            {
                push_state(global.to_string(), value, raw.len(), &mut embedded);
            }
        }
    }

    StructuredData { json_ld, embedded }
}

/// Sites wrap JSON-LD in the comment and CDATA guards that older XHTML needed.
fn strip_wrappers(raw: &str) -> &str {
    let mut text = raw.trim();
    for (open, close) in [("<!--", "-->"), ("//<![CDATA[", "//]]>"), ("<![CDATA[", "]]>")] {
        if let Some(inner) = text.strip_prefix(open) {
            text = inner.strip_suffix(close).unwrap_or(inner).trim();
        }
    }
    text
}

/// A JSON-LD block may be one record, an array of them, or a `@graph` container
/// holding the records the page is actually about. Flatten all three to records.
fn push_records(value: Value, out: &mut Vec<Value>) {
    match value {
        Value::Array(items) => items.into_iter().for_each(|item| push_records(item, out)),
        Value::Object(mut map) => {
            if let Some(graph) = map.remove("@graph") {
                push_records(graph, out);
            }
            // What remains is a container, not a record, if it held nothing but
            // the graph and its context.
            if map.keys().any(|k| k != "@context") {
                out.push(Value::Object(map));
            }
        }
        _ => {}
    }
}

/// A page can publish the same global twice — once as a script id, once inline.
/// The first one read is the one that parsed, so later duplicates are dropped.
fn push_state(name: String, value: Value, raw_len: usize, out: &mut Vec<EmbeddedState>) {
    if out.iter().any(|s| s.name == name) {
        return;
    }
    out.push(EmbeddedState { name, value, raw_len });
}

/// The JSON assigned to `global` in an inline script, if it is JSON at all.
/// Nuxt 2 emits `window.__NUXT__=(function(a,b){…}(…))`, which is a program and
/// not data; the brace scan below simply finds nothing to take, which is right.
fn assigned_json<'a>(script: &'a str, global: &str) -> Option<&'a str> {
    let mut from = 0;
    while let Some(at) = script[from..].find(global) {
        let after = from + at + global.len();
        from = after;
        let rest = script[after..].trim_start();
        let Some(rest) = rest.strip_prefix('=') else {
            continue;
        };
        if let Some(span) = json_span(rest.trim_start()) {
            return Some(span);
        }
    }
    None
}

/// The balanced object or array at the head of `s`. Scanning bytes is safe here:
/// no continuation byte of a multi-byte character can collide with ASCII.
fn json_span(s: &str) -> Option<&str> {
    let bytes = s.as_bytes();
    let (open, close) = match bytes.first()? {
        b'{' => (b'{', b'}'),
        b'[' => (b'[', b']'),
        _ => return None,
    };

    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;

    for (i, &b) in bytes.iter().enumerate() {
        if in_string {
            match b {
                _ if escaped => escaped = false,
                b'\\' => escaped = true,
                b'"' => in_string = false,
                _ => {}
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b if b == open => depth += 1,
            b if b == close => {
                depth -= 1;
                if depth == 0 {
                    return Some(&s[..=i]);
                }
            }
            _ => {}
        }
    }
    None
}

// *** reading values out ***

fn type_names(node: &Value) -> Vec<String> {
    match node.get("@type") {
        Some(Value::String(s)) => vec![s.clone()],
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect(),
        _ => Vec::new(),
    }
}

/// A schema.org field is either a scalar or an object standing in for one —
/// `author` is usually `{"@type": "Person", "name": "…"}`.
fn named_value(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.trim().to_string()),
        Value::Number(n) => Some(n.to_string()),
        Value::Object(_) => value
            .get("name")
            .or_else(|| value.get("headline"))
            .and_then(named_value),
        Value::Array(items) => items.iter().find_map(named_value),
        _ => None,
    }
}

/// The URLs buried in a JSON blob. This is what makes state worth reading on a
/// client-rendered site: the links its HTML does not contain are usually right
/// here, as strings in the props.
pub fn urls_in(value: &Value, base: &Url, limit: usize) -> Vec<Url> {
    let mut seen = HashSet::new();
    let mut urls = Vec::new();
    collect_urls(value, base, limit, &mut seen, &mut urls);
    urls
}

fn collect_urls(
    value: &Value,
    base: &Url,
    limit: usize,
    seen: &mut HashSet<String>,
    out: &mut Vec<Url>,
) {
    if out.len() >= limit {
        return;
    }
    match value {
        Value::String(s) => {
            if let Some(url) = url_like(s, base)
                && seen.insert(dedup_key(&url))
            {
                out.push(url);
            }
        }
        Value::Array(items) => items
            .iter()
            .for_each(|item| collect_urls(item, base, limit, seen, out)),
        Value::Object(map) => map
            .values()
            .for_each(|item| collect_urls(item, base, limit, seen, out)),
        _ => {}
    }
}

fn url_like(s: &str, base: &Url) -> Option<Url> {
    let s = s.trim();
    let absolute = s.starts_with("http://") || s.starts_with("https://");
    let rooted = s.starts_with('/') && s.len() > 1;
    if !absolute && !rooted {
        return None;
    }
    if s.contains(char::is_whitespace) || s.contains('<') {
        return None;
    }
    // Route templates live in the same props as real links — Next.js stores its
    // own page as `/[[...slug]]` — and resolve to URLs that were never pages.
    if s.contains(['[', ']', '{', '}', '*']) || s.contains("/:") {
        return None;
    }

    let joined = base.join(s).ok()?;
    let path = joined.path().to_lowercase();
    if ASSET_EXTENSIONS.iter().any(|ext| path.ends_with(ext)) {
        return None;
    }
    canonicalize(joined.as_str()).ok()
}

// *** rendering ***

/// Records flattened to `path: value` lines. Schema-agnostic on purpose: a
/// whitelist of interesting keys would silently drop whatever a site publishes
/// that the whitelist never anticipated.
pub fn render_records(records: &[Value]) -> String {
    let mut out = String::new();
    for (i, record) in records.iter().enumerate() {
        let types = type_names(record);
        let label = if types.is_empty() {
            "(untyped)".to_string()
        } else {
            types.join(", ")
        };
        out.push_str(&format!("[{}] @type: {label}\n", i + 1));

        let mut lines = Vec::new();
        flatten("", record, &mut lines);
        for line in &lines {
            out.push_str(&format!("  {line}\n"));
        }
        if lines.len() >= MAX_RECORD_LINES {
            out.push_str("  … (record truncated)\n");
        }
        out.push('\n');
    }
    out
}

fn flatten(prefix: &str, value: &Value, out: &mut Vec<String>) {
    if out.len() >= MAX_RECORD_LINES {
        return;
    }
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                if key == "@context" || key == "@type" {
                    continue;
                }
                let path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                flatten(&path, child, out);
            }
        }
        Value::Array(items) => {
            for (i, child) in items.iter().enumerate() {
                flatten(&format!("{prefix}[{i}]"), child, out);
            }
        }
        Value::Null => {}
        scalar => out.push(format!("{prefix}: {}", short(scalar))),
    }
}

/// Shape of a state blob rather than its contents: a Next.js `pageProps` can be
/// hundreds of kilobytes, and dumping it would spend the whole response budget
/// on one page. The outline says where the data is; `urls_in` takes the links.
pub fn render_outline(value: &Value, max_depth: u32) -> String {
    let mut lines = Vec::new();
    outline_into(value, 0, max_depth, &mut lines);
    lines.join("\n")
}

fn outline_into(value: &Value, depth: u32, max_depth: u32, out: &mut Vec<String>) {
    let indent = "  ".repeat(depth as usize);
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                if out.len() >= MAX_OUTLINE_LINES {
                    out.push(format!("{indent}… (outline truncated)"));
                    return;
                }
                out.push(format!("{indent}{key}: {}", shape(child)));
                if depth + 1 < max_depth {
                    outline_into(child, depth + 1, max_depth, out);
                }
            }
        }
        // One element is enough to show what a list of ten thousand holds.
        Value::Array(items) => {
            if let Some(first) = items.first() {
                if out.len() >= MAX_OUTLINE_LINES {
                    return;
                }
                out.push(format!("{indent}[0]: {}", shape(first)));
                if depth + 1 < max_depth {
                    outline_into(first, depth + 1, max_depth, out);
                }
            }
        }
        _ => {}
    }
}

fn shape(value: &Value) -> String {
    match value {
        Value::Object(map) => format!("object ({} keys)", map.len()),
        Value::Array(items) => format!("array ({} items)", items.len()),
        scalar => short(scalar),
    }
}

fn short(value: &Value) -> String {
    let text = match value {
        Value::String(s) => s.split_whitespace().collect::<Vec<_>>().join(" "),
        other => other.to_string(),
    };
    if text.chars().count() <= MAX_VALUE_CHARS {
        return text;
    }
    let cutoff = text
        .char_indices()
        .nth(MAX_VALUE_CHARS)
        .map(|(i, _)| i)
        .unwrap_or(text.len());
    format!("{}…", &text[..cutoff])
}
