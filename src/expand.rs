//! Query expansion: one phrase becomes the handful that cover the topic.
//!
//! Most of a search's recall is decided before any request is made. A
//! non-anglophone subject is covered mostly in their own language and the
//! destination's; one name has several valid romanisations that outlets do not
//! agree on; a search backend caps the results of one query, so the same query
//! run year by year across a term returns far more than one unbounded query.
//!
//! This module is pure — it turns names, a place and a time window into
//! `Query` values and makes no request. [`research`] is the part that runs
//! them.
//!
//! **Languages.** A name written in a non-Latin script is paired with the verbs
//! of that script's main language (see [`native_predicates`]) instead of the
//! English ones, because the press that writes the name in Cyrillic writes the
//! rest of the sentence in Russian. Those words are a short, deliberately
//! plain list — "visit", "official visit", "talks" — and are the author's
//! translations, not reviewed by a speaker of each language. A script is not a
//! language (Cyrillic is also Kyrgyz, Ukrainian, Bulgarian), so this is a
//! best guess at the commonest one, and Latin-script names in French, Turkish
//! or Spanish press get the English verbs only.
//!
//! **Spellings.** Wikidata lists the romanisations people have recorded.
//! Where only a Cyrillic form is listed, [`latin_forms`] adds the two common
//! English renderings of it — *Zhaparov* and *Japarov* are the same name.

use crate::discovery::{
    DiscoveryBudget, DiscoverySource, Federated, Query, Span, federate_refs, merge,
};
use crate::scoring::normalise;
use reqwest::Client;
use std::collections::HashSet;

/// Hard ceiling on queries one request can ask for. Each costs a round of API
/// calls, and the rate-limited backends answer one per five seconds.
pub const MAX_QUERIES: usize = 20;
pub const DEFAULT_QUERIES: usize = 8;
/// Distinct names tried per round. More than this is mostly the same name in
/// a language the press does not write in.
const MAX_NAMES: usize = 4;
/// Longer than this is a description or a sentence, not a name.
const MAX_NAME_CHARS: usize = 60;

/// The English verbs, used for Latin-script names. Native-script names use
/// [`native_predicates`].
const PREDICATES: &[&str] = &[
    "visit",
    "state visit",
    "official visit",
    "trip to",
    "talks in",
    "arrived in",
    "met with",
    "summit",
];

pub struct Plan<'a> {
    /// The user's own text. Always the primary name.
    pub query: &'a str,
    /// Other names for the subject, from discovery.
    pub names: &'a [String],
    pub place: Option<&'a str>,
    /// Already resolved: see [`resolve_window`].
    pub window: &'a Span,
    pub max_queries: usize,
    /// For a window with no end. Passed in, not read from the clock, so the
    /// expansion is a pure function and a test can fix it.
    pub this_year: i32,
}

/// The window a round is bounded to: the user's explicit bounds, narrowed to
/// the subject's tenure where both exist.
///
/// "Bound every query by the term dates so budget isn't spent outside the
/// window." If the two do not overlap the user's bounds win — an empty window
/// would silently return nothing, and the user asked for those dates.
pub fn resolve_window(since: Option<&str>, until: Option<&str>, tenure: Option<&Span>) -> Span {
    let user = Span { from: since.map(str::to_string), to: until.map(str::to_string) };
    let Some(tenure) = tenure else { return user };

    // `YYYYMMDD` strings order the same way the dates do.
    let from = [user.from.clone(), tenure.from.clone()].into_iter().flatten().max();
    let to = [user.to.clone(), tenure.to.clone()].into_iter().flatten().min();
    if let (Some(f), Some(t)) = (&from, &to)
        && f > t
    {
        return user;
    }
    Span { from, to }
}

pub fn this_year() -> i32 {
    // 365.2425 days, in seconds. Exact enough to name a year.
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    1970 + (secs / 31_556_952) as i32
}

pub fn expand(plan: &Plan) -> Vec<Query> {
    let max = plan.max_queries.clamp(1, MAX_QUERIES);
    let names = pick_names(plan.query, plan.names, MAX_NAMES);
    let place = plan.place.map(str::trim).filter(|p| !p.is_empty());
    let mut out = Builder::new(max);

    // Round 0: every name, over the whole window.
    for name in &names {
        let text = match place {
            Some(p) => format!("{} {p}", phrase(name)),
            None => phrase(name),
        };
        out.push(text, plan.window.from.clone(), plan.window.to.clone());
    }

    // Year slices of the primary name. Half of what is left, so slicing
    // cannot starve the predicate variants and the reverse.
    let remaining = max.saturating_sub(out.len());
    if let Some(from_year) = plan.window.from.as_deref().and_then(year_of) {
        let to_year = plan.window.to.as_deref().and_then(year_of).unwrap_or(plan.this_year);
        let slots = remaining / 2;
        if to_year > from_year && slots >= 2 {
            let primary = match place {
                Some(p) => format!("{} {p}", phrase(&names[0])),
                None => phrase(&names[0]),
            };
            for (first, last) in year_buckets(from_year, to_year, slots) {
                // The window's own edges are exact dates; only the interior
                // boundaries are whole calendar years.
                let since = if first == from_year {
                    plan.window.from.clone()
                } else {
                    Some(format!("{first}0101"))
                };
                let until = match (&plan.window.to, last == to_year) {
                    (Some(to), true) => Some(to.clone()),
                    (None, true) => None,
                    _ => Some(format!("{last}1231")),
                };
                out.push(primary.clone(), since, until);
            }
        }
    }

    // Predicate variants fill what is left: each predicate across every name
    // before the next predicate, so a small budget still spans the names. A
    // name in another script takes that script's verbs in the same position.
    for (i, predicate) in PREDICATES.iter().enumerate() {
        for name in &names {
            let native = native_predicates(name);
            let verb = native.get(i % native.len().max(1)).copied().unwrap_or(predicate);
            let text = match place {
                Some(p) => format!("{} {} {p}", phrase(name), phrase(verb)),
                None => format!("{} {}", phrase(name), phrase(verb)),
            };
            out.push(text, plan.window.from.clone(), plan.window.to.clone());
        }
    }

    out.queries
}

struct Builder {
    queries: Vec<Query>,
    seen: HashSet<(String, Option<String>, Option<String>)>,
    max: usize,
}

impl Builder {
    fn new(max: usize) -> Self {
        Self { queries: Vec::new(), seen: HashSet::new(), max }
    }

    fn len(&self) -> usize {
        self.queries.len()
    }

    fn push(&mut self, text: String, since: Option<String>, until: Option<String>) {
        if self.queries.len() >= self.max {
            return;
        }
        if self.seen.insert((text.to_lowercase(), since.clone(), until.clone())) {
            self.queries.push(Query { text, since, until, sites: Vec::new(), post: None });
        }
    }
}

/// Quote a multi-word name so a backend matches it as a phrase. Text that
/// already carries quotes or an operator (`sourcelang:russian`) is the user's
/// own syntax and is left alone.
fn phrase(text: &str) -> String {
    let text = text.trim();
    if text.contains('"') || text.contains(':') || !text.contains(char::is_whitespace) {
        text.to_string()
    } else {
        format!("\"{text}\"")
    }
}

fn year_of(date: &str) -> Option<i32> {
    date.get(..4)?.parse().ok()
}

/// Split `from..=to` into at most `slots` contiguous, non-overlapping runs of
/// years. More years than slots means each run covers several: the whole term
/// is still searched, just coarser.
fn year_buckets(from: i32, to: i32, slots: usize) -> Vec<(i32, i32)> {
    let years = (to - from + 1) as usize;
    let size = years.div_ceil(slots.max(1)) as i32;
    let mut buckets = Vec::new();
    let mut start = from;
    while start <= to {
        let end = (start + size - 1).min(to);
        buckets.push((start, end));
        start = end + 1;
    }
    buckets
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Script {
    Latin,
    Cyrillic,
    Arabic,
    Han,
    Kana,
    Hangul,
    Devanagari,
    Hebrew,
    Greek,
    Other,
}

fn script_of(name: &str) -> Script {
    let Some(c) = name.chars().find(|c| c.is_alphabetic()) else { return Script::Other };
    match c as u32 {
        0x41..=0x24F | 0x1E00..=0x1EFF => Script::Latin,
        0x370..=0x3FF => Script::Greek,
        0x400..=0x52F => Script::Cyrillic,
        0x590..=0x5FF => Script::Hebrew,
        0x600..=0x6FF | 0x750..=0x77F | 0xFB50..=0xFDFF | 0xFE70..=0xFEFF => Script::Arabic,
        0x900..=0x97F => Script::Devanagari,
        0x3040..=0x30FF => Script::Kana,
        0x4E00..=0x9FFF => Script::Han,
        0x1100..=0x11FF | 0xAC00..=0xD7AF => Script::Hangul,
        _ => Script::Other,
    }
}

/// The verbs a name's own press uses, by the script the name is written in.
/// Empty for Latin script, which gets the English ones.
///
/// Arabic script splits by the letters it uses: Persian writes `پ چ ژ گ` and
/// its own forms of `ی` and `ک`, Arabic does not, and the two say "visit"
/// differently.
fn native_predicates(name: &str) -> &'static [&'static str] {
    match script_of(name) {
        Script::Cyrillic => &["визит", "официальный визит", "поездка", "переговоры"],
        Script::Arabic => {
            if name.chars().any(|c| matches!(c, 'پ' | 'چ' | 'ژ' | 'گ' | 'ی' | 'ک')) {
                &["سفر", "دیدار", "بازدید", "مذاکرات"]
            } else {
                &["زيارة", "زيارة رسمية", "محادثات", "لقاء"]
            }
        }
        Script::Han => &["访问", "会见", "出访", "会谈"],
        Script::Kana => &["訪問", "会談", "会見"],
        Script::Hangul => &["방문", "회담", "정상회담"],
        Script::Greek => &["επίσκεψη", "συνάντηση"],
        Script::Hebrew => &["ביקור", "פגישה"],
        Script::Devanagari => &["यात्रा", "दौरा", "मुलाकात"],
        Script::Latin | Script::Other => &[],
    }
}

/// English renderings of a Cyrillic name: the scheme most reference works use
/// (*Zhaparov*, *Khrushchev*) and the plainer one most newspapers do (*Japarov*,
/// *Hrushchev*). They differ only where the scripts disagree about a sound, so
/// a name with none of those letters yields one form, not two. Empty for a
/// name that is not Cyrillic.
pub fn latin_forms(name: &str) -> Vec<String> {
    if script_of(name) != Script::Cyrillic {
        return Vec::new();
    }
    let reference = romanise(name, true);
    let plain = romanise(name, false);
    if reference == plain { vec![reference] } else { vec![reference, plain] }
}

fn romanise(name: &str, reference: bool) -> String {
    let mut out = String::with_capacity(name.len());
    for c in name.chars() {
        let lower = c.to_lowercase().next().unwrap_or(c);
        let piece: &str = match lower {
            'а' => "a", 'б' => "b", 'в' => "v", 'г' => "g", 'д' => "d", 'е' => "e", 'ё' => "yo",
            'ж' => if reference { "zh" } else { "j" },
            'з' => "z", 'и' => "i", 'й' => "y", 'к' => "k", 'л' => "l", 'м' => "m", 'н' => "n",
            'о' => "o", 'п' => "p", 'р' => "r", 'с' => "s", 'т' => "t", 'у' => "u", 'ф' => "f",
            'х' => if reference { "kh" } else { "h" },
            'ц' => "ts", 'ч' => "ch", 'ш' => "sh",
            'щ' => if reference { "shch" } else { "sch" },
            'ъ' | 'ь' => "", 'ы' => "y", 'э' => "e", 'ю' => "yu", 'я' => "ya",
            // Ukrainian, Belarusian, Kazakh, Kyrgyz and their neighbours.
            'і' => "i", 'ї' => "yi", 'є' => "ye", 'ґ' => "g", 'ў' => "u",
            'ү' | 'ұ' => "u", 'ө' => "o", 'ң' => "ng", 'қ' => "q", 'ғ' => "gh", 'һ' => "h", 'ә' => "a",
            _ => {
                out.push(c);
                continue;
            }
        };
        if c.is_uppercase() && !piece.is_empty() {
            let mut chars = piece.chars();
            if let Some(first) = chars.next() {
                out.extend(first.to_uppercase());
                out.push_str(chars.as_str());
            }
        } else {
            out.push_str(piece);
        }
    }
    out
}

/// The names worth a query of their own: the user's text first, then
/// alternating between a script not yet used and a further romanisation.
///
/// A subject's Wikidata record can carry a hundred labels, most of them the
/// same name in a script the press never writes it in. One query per distinct
/// script is what buys the subject's own-language coverage; the alternating
/// Latin pick is what buys the romanisations outlets disagree about. Names that
/// differ only by case or accent are one name — they would match the same
/// articles.
fn pick_names(query: &str, aliases: &[String], max: usize) -> Vec<String> {
    let mut chosen = vec![query.trim().to_string()];
    let mut seen: HashSet<String> = HashSet::from([normalise(query)]);
    let mut scripts = vec![script_of(query)];

    let mut pool: Vec<(Script, String)> = Vec::new();
    for alias in aliases {
        let alias = alias.trim();
        if alias.is_empty() || alias.chars().count() > MAX_NAME_CHARS {
            continue;
        }
        if seen.insert(normalise(alias)) {
            pool.push((script_of(alias), alias.to_string()));
        }
    }

    // Spellings the record does not list, behind the ones it does. A Cyrillic
    // alias with no recorded romanisation is the usual reason.
    for alias in aliases {
        for form in latin_forms(alias.trim()) {
            if form.chars().count() <= MAX_NAME_CHARS && seen.insert(normalise(&form)) {
                pool.push((Script::Latin, form));
            }
        }
    }

    let mut want_new_script = true;
    while chosen.len() < max && !pool.is_empty() {
        let at = if want_new_script {
            pool.iter().position(|(s, _)| !scripts.contains(s))
        } else {
            pool.iter().position(|(s, _)| *s == Script::Latin)
        }
        // Nothing of the kind wanted: take whatever is next, rather than stop
        // short with budget unspent.
        .unwrap_or(0);
        let (script, name) = pool.remove(at);
        if !scripts.contains(&script) {
            scripts.push(script);
        }
        chosen.push(name);
        want_new_script = !want_new_script;
    }
    chosen
}

/// What an expanded round did, for the tool to report.
pub struct Researched {
    pub federated: Federated,
    /// The window every expanded query was bounded to.
    pub window: Span,
    /// The expanded queries planned, not counting the original.
    pub queries: Vec<Query>,
    /// How many of them were actually sent. Fewer than planned when every
    /// text-searching source had failed by then; they run in order, so these
    /// are the first `ran`.
    pub ran: usize,
}

/// Run `base` across every source, then the expansions across the sources
/// whose answer depends on the wording.
///
/// The base round runs first and on everything: it is where Wikidata learns
/// the subject's names and tenure, which is what the expansion is built from.
/// A source that fails in one round is dropped from the rest. Re-asking a
/// rate-limited API a further seven times is the one way to make that worse.
pub async fn research(
    client: &Client,
    sources: &[Box<dyn DiscoverySource>],
    base: &Query,
    place: Option<&str>,
    max_queries: usize,
    budget: &DiscoveryBudget,
) -> Researched {
    let all: Vec<&dyn DiscoverySource> = sources.iter().map(|s| s.as_ref()).collect();
    let first = federate_refs(client, &all, base, budget).await;

    let window = resolve_window(base.since.as_deref(), base.until.as_deref(), first.span.as_ref());
    let plan = Plan {
        query: &base.text,
        names: &first.terms,
        place,
        window: &window,
        max_queries,
        this_year: this_year(),
    };
    let mut skip: HashSet<(String, Option<String>, Option<String>)> = HashSet::from([(
        base.text.to_lowercase(),
        base.since.clone(),
        base.until.clone(),
    )]);
    let queries: Vec<Query> = expand(&plan)
        .into_iter()
        .filter(|q| skip.insert((q.text.to_lowercase(), q.since.clone(), q.until.clone())))
        .collect();

    let mut live: Vec<&dyn DiscoverySource> =
        sources.iter().filter(|s| s.per_query()).map(|s| s.as_ref()).collect();
    // The base round's failures are already known; do not repeat them.
    live.retain(|s| !first.failures.iter().any(|(name, _)| *name == *s.name()));

    let mut leads = first.leads;
    let mut terms = first.terms;
    let mut failures = first.failures;
    let span = first.span;
    let subject = first.subject;
    let mut ran = 0;
    for query in &queries {
        if live.is_empty() {
            break;
        }
        ran += 1;
        let round = federate_refs(client, &live, query, budget).await;
        leads.extend(round.leads);
        for term in round.terms {
            if !terms.contains(&term) {
                terms.push(term);
            }
        }
        for (name, why) in round.failures {
            live.retain(|s| *s.name() != *name);
            if !failures.iter().any(|(n, _)| *n == name) {
                failures.push((name, why));
            }
        }
    }

    Researched {
        federated: Federated {
            leads: merge(leads, budget.max_candidates),
            terms,
            span,
            subject,
            failures,
        },
        window,
        queries,
        ran,
    }
}
