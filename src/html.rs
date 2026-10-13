//! A guard in front of the HTML parser.
//!
//! Real pages are shallow: a few dozen levels, a few hundred at the extreme. A
//! page nested twenty thousand levels deep is eighty kilobytes of `<div>` and
//! nothing a person wrote, and it is a weapon in two ways. The parser slows
//! down roughly with the *square* of the depth — minutes of CPU for a page that
//! fits comfortably under the size limit — and anything that walks the tree
//! recursively overflows its stack, which in Rust aborts the whole process and
//! cannot be caught.
//!
//! So depth is measured *before* parsing, in one linear pass, and a page over
//! the limit is refused. The pass is an estimate, not a parser: it reads tags,
//! skips what a parser would skip (comments, the bodies of `<script>` and
//! `<style>`), and does not count elements whose end tags are optional, so a
//! long list of unclosed `<li>` or `<p>` is not mistaken for nesting. Where it
//! disagrees with the real parser it should err towards *accepting*, since
//! refusing real pages is the cost of the defence; the other defences (an
//! iterative walk, larger worker stacks) are what cover a crafted page that
//! slips through.

use scraper::Html;

/// Deeper than this and a page is refused. Generous: ordinary pages are far
/// under a hundred.
pub const MAX_NESTING: usize = 512;

/// Elements with no end tag at all.
const VOID: &[&str] = &[
    "area", "base", "basefont", "br", "col", "embed", "frame", "hr", "img", "input", "keygen",
    "link", "meta", "param", "source", "track", "wbr",
];

/// Elements the parser closes by itself when the next one begins, so writing
/// them without end tags — as HTML allows — never makes them nest.
const OPTIONAL_END: &[&str] = &[
    "p", "li", "dt", "dd", "tr", "td", "th", "thead", "tbody", "tfoot", "option", "optgroup",
    "colgroup", "caption", "rt", "rp",
];

/// Elements whose content is text, not markup, up to their own end tag.
const RAW_TEXT: &[&str] = &["script", "style", "textarea", "title", "xmp"];

/// Whether the document is nested past [`MAX_NESTING`].
pub fn too_deep(html: &str) -> bool {
    deepest(html, MAX_NESTING) > MAX_NESTING
}

/// Parse a document, or an empty one if it is too deeply nested to parse
/// safely. Every extraction in the crate goes through here, so a hostile page
/// yields nothing instead of a crash or a stall.
pub fn parse(html: &str) -> Html {
    if too_deep(html) {
        Html::parse_document("")
    } else {
        Html::parse_document(html)
    }
}

/// The deepest nesting seen, stopping as soon as it passes `limit`.
fn deepest(html: &str, limit: usize) -> usize {
    let b = html.as_bytes();
    let mut i = 0;
    let mut depth = 0usize;
    let mut max = 0usize;

    while i < b.len() {
        if b[i] != b'<' {
            i += 1;
            continue;
        }
        let rest = &b[i + 1..];
        if rest.starts_with(b"!--") {
            i = find(b, i + 4, b"-->").map_or(b.len(), |p| p + 3);
            continue;
        }
        match rest.first() {
            Some(b'/') => {
                let (name, end) = read_name(b, i + 2);
                if counts(&name) {
                    depth = depth.saturating_sub(1);
                }
                i = skip_to_gt(b, end);
            }
            Some(c) if c.is_ascii_alphabetic() => {
                let (name, after_name) = read_name(b, i + 1);
                let (end, self_closing) = skip_attributes(b, after_name);
                if !self_closing && RAW_TEXT.contains(&name.as_str()) {
                    // Everything up to the matching end tag is text.
                    let close = format!("</{name}");
                    i = find_ci(b, end, close.as_bytes()).unwrap_or(b.len());
                    continue;
                }
                if counts(&name) && !self_closing {
                    depth += 1;
                    max = max.max(depth);
                    if max > limit {
                        return max;
                    }
                }
                i = end;
            }
            // `<!doctype`, `<?xml`, a stray `<`: not an element.
            _ => i += 1,
        }
    }
    max
}

fn counts(name: &str) -> bool {
    !VOID.contains(&name) && !OPTIONAL_END.contains(&name)
}

/// A tag name, lowercased, and the position after it.
fn read_name(b: &[u8], start: usize) -> (String, usize) {
    let mut end = start;
    while end < b.len() && (b[end].is_ascii_alphanumeric() || b[end] == b'-' || b[end] == b':') {
        end += 1;
    }
    (String::from_utf8_lossy(&b[start..end]).to_ascii_lowercase(), end)
}

fn skip_to_gt(b: &[u8], from: usize) -> usize {
    find(b, from, b">").map_or(b.len(), |p| p + 1)
}

/// From just after a tag name to just after its `>`, reading attributes the way
/// the HTML parser does: a quote delimits a value only where a value begins,
/// so a stray quote inside an unquoted value or a name cannot swallow the
/// markup that follows. Returns the position after `>` and whether the tag was
/// written `/>`.
fn skip_attributes(b: &[u8], mut i: usize) -> (usize, bool) {
    let is_space = |c: u8| matches!(c, b' ' | b'\t' | b'\n' | b'\r' | 0x0c);
    let mut self_closing = false;
    while i < b.len() {
        match b[i] {
            b'>' => return (i + 1, self_closing),
            b'/' => {
                self_closing = b.get(i + 1) == Some(&b'>');
                i += 1;
            }
            c if is_space(c) => i += 1,
            _ => {
                self_closing = false;
                // An attribute name.
                while i < b.len() && !is_space(b[i]) && !matches!(b[i], b'=' | b'>' | b'/') {
                    i += 1;
                }
                while i < b.len() && is_space(b[i]) {
                    i += 1;
                }
                if b.get(i) == Some(&b'=') {
                    i += 1;
                    while i < b.len() && is_space(b[i]) {
                        i += 1;
                    }
                    match b.get(i) {
                        Some(&q) if q == b'"' || q == b'\'' => {
                            i += 1;
                            i = find(b, i, &[q]).map_or(b.len(), |p| p + 1);
                        }
                        _ => {
                            while i < b.len() && !is_space(b[i]) && b[i] != b'>' {
                                i += 1;
                            }
                        }
                    }
                }
            }
        }
    }
    (b.len(), false)
}

fn find(haystack: &[u8], from: usize, needle: &[u8]) -> Option<usize> {
    haystack.get(from..)?.windows(needle.len()).position(|w| w == needle).map(|p| p + from)
}

fn find_ci(haystack: &[u8], from: usize, needle: &[u8]) -> Option<usize> {
    haystack
        .get(from..)?
        .windows(needle.len())
        .position(|w| w.eq_ignore_ascii_case(needle))
        .map(|p| p + from)
}
