use crate::scoring::{LinkContext, Terms, score_link};
use crate::crawler::canonicalize;

#[test]
fn probe() {
    let cases: &[(&str, &str, &str)] = &[
        ("identical",        "صادیر جاپاروف", "صادیر جاپاروف امروز"),
        ("farsi vs arabic yeh", "صادیر جاپاروف", "صادير جاپاروف امروز"),
        ("farsi vs arabic kaf", "کوروش",        "كوروش"),
        ("zwnj compound",    "می‌رود",        "می رود"),
        ("diacritics",       "سلام",          "سَلام"),
        ("arabic-indic digits", "۱۴۰۲",        "1402"),
        ("alef variants",    "ایران",          "إيران"),
        ("heh/teh marbuta",  "خامنه‌ای",       "خامنة اي"),
    ];
    for (name, term, text) in cases {
        let t = Terms::new([*term]);
        println!("{name:24} term={term:14} text={text:20} -> {:.2}", t.match_strength(text));
    }

    println!("\n--- link scoring ---");
    let terms = Terms::new(["صادیر جاپاروف"]);
    for (url, anchor) in [
        ("https://x.ir/news/1", "صادیر جاپاروف با هیئت دیدار کرد"),
        ("https://x.ir/آرشیو/۱۴۰۲/", "آرشیو"),
        ("https://x.ir/cookie-policy", "سیاست کوکی"),
    ] {
        let l = LinkContext { url: canonicalize(url).unwrap(), anchor: anchor.into(), nearby: String::new(), rel_next: false };
        println!("{:.2}  {anchor}", score_link(&l, &terms, 0.0).unwrap_or(-9.0));
    }
}
