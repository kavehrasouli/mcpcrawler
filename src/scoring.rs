//! Deciding which link to open next.
//!
//! A crawler that follows links in the order it found them spends its budget on
//! whatever a page happens to list first, which is usually navigation: cookie
//! notices, contact forms, language switchers. The pages worth having —
//! `/press-releases/2019/`, page 47 of a news archive — sit further down, and
//! frequently behind an index page that says nothing itself.
//!
//! So every link is judged *before* it is fetched, from four things that cost
//! nothing because the parent page is already parsed: the anchor text, the
//! sentence around it, the tokens in the URL, and how relevant the page that
//! linked it turned out to be. Classic focused crawling, in Chakrabarti's 1999
//! shape.
//!
//! The scoring is deliberately not clever. It is a weighted sum with a fixed
//! vocabulary, because the alternative — a model call per link — would cost
//! more than fetching the page it is trying to avoid fetching.

use percent_encoding::percent_decode_str;
use std::collections::HashSet;
use std::sync::LazyLock;
use url::Url;

/// Weights, summing to 1.0 before shape bonuses and penalties.
const ANCHOR_WEIGHT: f32 = 0.45;
const NEARBY_WEIGHT: f32 = 0.15;
const URL_WEIGHT: f32 = 0.25;
const PARENT_WEIGHT: f32 = 0.15;

/// What a link has to score to count as on-topic. Below this it is only
/// followed on tunnelling slack.
pub const ON_TOPIC: f32 = 0.25;

/// Paths that hold the material an archive crawl is after. Deliberately short:
/// a long list of guesses is a long list of ways to be confidently wrong.
///
/// Not only English. A Persian ministry's archive is at `/آرشیو/`, and a crawler
/// that only recognises `/archive/` is blind to exactly the sites its
/// multilingual alias handling exists to reach. Written here in their natural
/// spelling and normalised once at startup, so the entries do not have to be
/// hand-folded to match.
const ARCHIVE_TOKENS: &[&str] = &[
    "press", "news", "archive", "archives", "release", "releases", "statement",
    "statements", "bulletin", "media", "speech", "speeches", "briefing",
    "announcement", "announcements", "publication", "publications", "visit",
    "visits", "meeting", "meetings", "report", "reports", "story", "stories",
    "article", "articles", "document", "documents", "transcript", "transcripts",
    // Persian and Arabic: archive, news, report, announcement, statement, media.
    "آرشیو", "بایگانی", "اخبار", "خبر", "گزارش", "اطلاعیه", "بیانیه", "رسانه",
    "مقاله", "مقالات", "سخنرانی", "دیدار", "سفر", "الأخبار", "أرشيف", "بيان",
    // Russian and Ukrainian.
    "новости", "новина", "новини", "архив", "архів", "пресс", "прес",
    "сообщение", "заявление", "заявa", "статья", "статті", "визит",
];

/// The token lists, folded the same way page text is, so a comparison is
/// between like and like.
static ARCHIVE_SET: LazyLock<HashSet<String>> = LazyLock::new(|| normalised_set(ARCHIVE_TOKENS));
static DEAD_END_SET: LazyLock<HashSet<String>> = LazyLock::new(|| normalised_set(DEAD_END_TOKENS));

fn normalised_set(tokens: &[&str]) -> HashSet<String> {
    tokens.iter().map(|t| normalise(t).trim().to_string()).collect()
}

/// Query keys that page through a list. The whole point of following these is
/// that no search engine indexes page 47.
const PAGE_KEYS: &[&str] = &["page", "p", "offset", "start", "from", "pagenum", "pg"];

/// Paths that are never the answer. Each of these reliably eats budget.
const DEAD_END_TOKENS: &[&str] = &[
    "cookie", "cookies", "privacy", "terms", "legal", "disclaimer", "copyright",
    "login", "signin", "sign-in", "register", "signup", "sign-up", "account",
    "cart", "checkout", "basket", "subscribe", "newsletter", "advertise",
    "advertising", "careers", "jobs", "vacancy", "vacancies", "faq", "help",
    "support", "accessibility", "share", "print", "download-app", "app-store",
];

/// Extensions that are not pages. `.pdf` is deliberately absent — a ministry's
/// communiqués are frequently PDFs, and this crawler should want them.
const ASSET_EXTENSIONS: &[&str] = &[
    ".css", ".js", ".mjs", ".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg",
    ".ico", ".woff", ".woff2", ".ttf", ".mp4", ".webm", ".mp3", ".zip", ".gz",
];

/// Bonus for a link that looks like an archive index or a page of one.
const ARCHIVE_BONUS: f32 = 0.20;
const PAGINATION_BONUS: f32 = 0.30;
/// A year in the path is the signature of a date-sliced archive.
const YEAR_BONUS: f32 = 0.15;
/// `rel="next"` is a site telling you where the rest of the list is.
const REL_NEXT_BONUS: f32 = 0.35;
const DEAD_END_PENALTY: f32 = 0.60;

/// The names a subject goes by.
///
/// One person is *Sadyr Japarov*, *Sadyr Zhaparov* and *Садыр Жапаров*
/// depending on who is writing, and outlets do not agree. Wikidata hands back
/// the full set; without it a link whose anchor text is the Cyrillic form
/// scores zero, which is precisely the coverage a non-anglophone subject has
/// most of.
#[derive(Debug, Clone, Default)]
pub struct Terms {
    /// Normalised whole phrases.
    phrases: Vec<String>,
    /// Every distinct word across all phrases, long enough to be worth
    /// matching on its own. "Japarov" alone in an anchor is a real signal;
    /// "of" is not.
    words: Vec<String>,
}

/// Words too common to carry meaning. Not a full stop-word list — just the
/// ones that show up inside the kind of phrase this takes.
const NOISE_WORDS: &[&str] = &[
    "the", "of", "and", "in", "to", "a", "an", "for", "on", "at", "by", "with",
    "from", "his", "her", "their", "its",
];

/// Shortest word worth matching alone.
const MIN_WORD_LEN: usize = 4;

impl Terms {
    /// Build from a query and whatever aliases discovery turned up.
    pub fn new<I, S>(phrases: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut seen_phrases = HashSet::new();
        let mut seen_words = HashSet::new();
        let mut terms = Terms::default();

        for phrase in phrases {
            let normalised = normalise(phrase.as_ref());
            if normalised.is_empty() {
                continue;
            }
            for word in normalised.split(' ') {
                if word.chars().count() >= MIN_WORD_LEN
                    && !NOISE_WORDS.contains(&word)
                    && seen_words.insert(word.to_string())
                {
                    terms.words.push(word.to_string());
                }
            }
            if seen_phrases.insert(normalised.clone()) {
                terms.phrases.push(normalised);
            }
        }
        terms
    }

    pub fn is_empty(&self) -> bool {
        self.phrases.is_empty()
    }

    /// How much of the subject appears in `text`, from 0 to 1.
    ///
    /// A whole phrase is worth everything; individual distinctive words are
    /// worth a share, because "Japarov met the Turkish president" is a hit even
    /// though the full name never appears.
    pub fn match_strength(&self, text: &str) -> f32 {
        if self.phrases.is_empty() {
            return 0.0;
        }
        let haystack = normalise(text);
        if haystack.is_empty() {
            return 0.0;
        }
        if self.phrases.iter().any(|p| haystack.contains(p.as_str())) {
            return 1.0;
        }
        if self.words.is_empty() {
            return 0.0;
        }
        let hits = self.words.iter().filter(|w| haystack.contains(w.as_str())).count();
        // Partial credit, but never as much as a full phrase match.
        (hits as f32 / self.words.len() as f32).min(0.9)
    }
}

/// Lowercase, fold the Latin accents transliteration disagrees about, and
/// reduce everything else to single spaces.
///
/// Scripts that are not Latin pass through unchanged, which is correct:
/// lowercasing is meaningful for Cyrillic and Greek, and a no-op for Arabic.
/// This is not a substitute for full Unicode normalisation — it is the part
/// that matters for matching names across romanisations.
fn normalise(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push(' ');
    let mut last_was_space = true;

    for raw in text.chars().flat_map(char::to_lowercase) {
        if is_droppable(raw) {
            continue;
        }
        let folded = fold(raw);
        if folded.is_empty() {
            if !last_was_space {
                out.push(' ');
                last_was_space = true;
            }
            continue;
        }
        out.push_str(folded);
        last_was_space = false;
    }
    if !last_was_space {
        out.push(' ');
    }
    out
}

/// Marks that qualify a letter without changing which letter it is: Arabic
/// harakat, the tatweel that stretches a word for justification, and Latin
/// combining accents in decomposed text.
///
/// Dropped outright rather than treated as separators. A single fatha in
/// `سَلام` would otherwise split one word into two and the name would not match
/// at all.
fn is_droppable(c: char) -> bool {
    matches!(c,
        // Latin combining diacritics, for text that arrives decomposed.
        '\u{0300}'..='\u{036F}'
        // Arabic harakat and Quranic annotation marks.
        | '\u{064B}'..='\u{065F}'
        | '\u{0670}'
        | '\u{06D6}'..='\u{06ED}'
        // Tatweel: a typographic stretch, never part of the word.
        | '\u{0640}'
    )
}

/// Accent folding for the Latin ranges that romanised names actually use.
/// Returns an empty string for anything that should read as a separator.
fn fold(c: char) -> &'static str {
    match c {
        'a'..='z' | '0'..='9' => return leak_ascii(c),
        'à' | 'á' | 'â' | 'ã' | 'ä' | 'å' | 'ā' | 'ă' | 'ą' => "a",
        'ç' | 'ć' | 'č' | 'ĉ' | 'ċ' => "c",
        'ď' | 'đ' => "d",
        'è' | 'é' | 'ê' | 'ë' | 'ē' | 'ĕ' | 'ė' | 'ę' | 'ě' => "e",
        'ĝ' | 'ğ' | 'ġ' | 'ģ' => "g",
        'ĥ' | 'ħ' => "h",
        'ì' | 'í' | 'î' | 'ï' | 'ī' | 'ĭ' | 'į' | 'ı' => "i",
        'ĵ' => "j",
        'ķ' => "k",
        'ĺ' | 'ļ' | 'ľ' | 'ł' => "l",
        'ñ' | 'ń' | 'ņ' | 'ň' => "n",
        'ò' | 'ó' | 'ô' | 'õ' | 'ö' | 'ø' | 'ō' | 'ŏ' | 'ő' => "o",
        'ŕ' | 'ŗ' | 'ř' => "r",
        'ś' | 'ŝ' | 'ş' | 'š' => "s",
        'ţ' | 'ť' | 'ŧ' => "t",
        'ù' | 'ú' | 'û' | 'ü' | 'ũ' | 'ū' | 'ŭ' | 'ů' | 'ű' | 'ų' => "u",
        'ŵ' => "w",
        'ý' | 'ÿ' | 'ŷ' => "y",
        'ź' | 'ż' | 'ž' => "z",
        'ß' => "ss",
        'æ' => "ae",
        'œ' => "oe",
        'þ' => "th",

        // Arabic script. Persian and Arabic keyboards produce different code
        // points for letters that are the same letter — an Iranian site writes
        // `ی` and `ک` where an Arabic-configured system writes `ي` and `ك`, and
        // both spellings of a name are in circulation. This is the same problem
        // as romanisation, one script over.
        'ي' | 'ی' | 'ى' | 'ئ' => "ی",
        'ك' | 'ک' => "ک",
        // Teh marbuta ends a word where a plain heh would in Persian.
        'ة' | 'ه' | 'ۀ' => "ه",
        'أ' | 'إ' | 'آ' | 'ٱ' | 'ا' | 'ﺍ' => "ا",
        'ؤ' => "و",
        // Arabic-Indic and Persian digits. A Persian date is written ۱۴۰۲, and
        // the same page's URL may well say 1402.
        '\u{0660}'..='\u{0669}' => leak_ascii(digit(c, '\u{0660}')),
        '\u{06F0}'..='\u{06F9}' => leak_ascii(digit(c, '\u{06F0}')),
        // Any other letter — Cyrillic, Greek, Arabic, CJK — is kept as-is.
        c if c.is_alphanumeric() => return leak_char(c),
        _ => "",
    }
}

/// The ASCII digit corresponding to `c` in a script's own digit range.
fn digit(c: char, zero: char) -> char {
    char::from(b'0' + (c as u32 - zero as u32) as u8)
}

/// ASCII letters and digits, without allocating.
fn leak_ascii(c: char) -> &'static str {
    const ALPHABET: &str = "abcdefghijklmnopqrstuvwxyz0123456789";
    let offset = match c {
        'a'..='z' => c as usize - 'a' as usize,
        _ => 26 + (c as usize - '0' as usize),
    };
    &ALPHABET[offset..offset + 1]
}

/// Non-Latin characters are kept verbatim. Leaking is bounded by the number of
/// distinct characters a crawl actually encounters, and buys a `&'static str`
/// return everywhere else.
fn leak_char(c: char) -> &'static str {
    use std::collections::HashMap;
    use std::sync::{LazyLock, Mutex};
    static KEPT: LazyLock<Mutex<HashMap<char, &'static str>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));

    let mut kept = KEPT.lock().unwrap();
    kept.entry(c)
        .or_insert_with(|| Box::leak(c.to_string().into_boxed_str()))
}

/// Everything known about a link at the moment of deciding whether to open it.
#[derive(Debug, Clone)]
pub struct LinkContext {
    pub url: Url,
    pub anchor: String,
    /// Text around the link — usually its parent element.
    pub nearby: String,
    /// The link carries `rel="next"`.
    pub rel_next: bool,
}

/// What the shape of a URL suggests, independent of the subject.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Shape {
    pub archive: bool,
    pub paginated: bool,
    pub dated: bool,
    pub dead_end: bool,
    pub asset: bool,
}

impl Shape {
    pub fn of(url: &Url) -> Self {
        // A non-ASCII path arrives percent-encoded, so `/آرشیو/` reaches here as
        // `/%D8%A2%D8%B1%D8%B4%DB%8C%D9%88/`. Without decoding, every path
        // outside ASCII is an opaque blob and none of these signals fire.
        let decoded = percent_decode_str(url.path()).decode_utf8_lossy().into_owned();
        let path = decoded.to_lowercase();
        let folded = normalise(&decoded);
        let segments: Vec<&str> = folded.split_whitespace().collect();

        let dated = segments.iter().any(|s| is_year(s))
            || url.query().is_some_and(|q| {
                q.split('&').any(|pair| {
                    pair.split_once('=').is_some_and(|(k, v)| {
                        matches!(k.to_lowercase().as_str(), "year" | "y" | "date") || is_year(v)
                    })
                })
            });

        let paginated = url.query().is_some_and(|q| {
            q.split('&').any(|pair| {
                pair.split_once('=')
                    .is_some_and(|(k, v)| PAGE_KEYS.contains(&k.to_lowercase().as_str())
                        && v.chars().all(|c| c.is_ascii_digit())
                        && !v.is_empty())
            })
        }) || segments.windows(2).any(|w| w[0] == "page" && w[1].chars().all(|c| c.is_ascii_digit()));

        Shape {
            archive: segments.iter().any(|s| ARCHIVE_SET.contains(*s)),
            paginated,
            dated,
            dead_end: segments.iter().any(|s| DEAD_END_SET.contains(*s)),
            asset: ASSET_EXTENSIONS.iter().any(|ext| path.ends_with(ext)),
        }
    }
}

/// A four-digit year in any calendar a news archive is likely to be sliced by:
/// Gregorian (19xx, 20xx), and Solar and Lunar Hijri (13xx, 14xx) — an Iranian
/// ministry files its press releases under `/۱۴۰۲/`, not `/2023/`. Persian and
/// Arabic-Indic digits have already been folded to ASCII by the time a path
/// segment gets here.
fn is_year(token: &str) -> bool {
    token.len() == 4
        && token.chars().all(|c| c.is_ascii_digit())
        && matches!(&token[..2], "13" | "14" | "19" | "20")
}

/// Score a link, or `None` if it should never be fetched at all.
///
/// The scale runs from -1 to 1, not 0 to 1. A known dead end has to sort
/// *below* a page that is merely unremarkable — if both floor at zero the
/// frontier cannot tell "definitely not worth it" from "no information", and
/// spends the budget on cookie notices whenever it runs short of better ideas.
///
/// `parent` is how relevant the page holding this link turned out to be. It
/// carries weight because a link on an obviously on-topic page is more likely
/// to be on-topic itself, which is what lets a crawl stay on a thread rather
/// than wander off it at the first hop.
pub fn score_link(link: &LinkContext, terms: &Terms, parent: f32) -> Option<f32> {
    let shape = Shape::of(&link.url);
    if shape.asset {
        // Not a low score — not a page. A stylesheet is never the answer.
        return None;
    }

    let mut score = terms.match_strength(&link.anchor) * ANCHOR_WEIGHT
        + terms.match_strength(&link.nearby) * NEARBY_WEIGHT
        + terms.match_strength(link.url.as_str()) * URL_WEIGHT
        + parent.clamp(0.0, 1.0) * PARENT_WEIGHT;

    // Shape bonuses apply whether or not the subject is named: an archive index
    // rarely mentions the subject in its own link text, and it is exactly the
    // page that leads to the ones that do.
    if shape.archive {
        score += ARCHIVE_BONUS;
    }
    if shape.paginated {
        score += PAGINATION_BONUS;
    }
    if shape.dated {
        score += YEAR_BONUS;
    }
    if link.rel_next {
        score += REL_NEXT_BONUS;
    }
    if shape.dead_end {
        score -= DEAD_END_PENALTY;
    }

    Some(score.clamp(-1.0, 1.0))
}
