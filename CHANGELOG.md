# Changelog

Tool schemas are versioned with the crate. The rules:

- **Patch** (0.1.x → 0.1.y): fixes only. No tool, parameter or result field is added, removed or changed.
- **Minor** (0.x → 0.y): may add tools, optional parameters and result fields. A parameter or field that exists keeps its meaning. Anything removed is announced as deprecated here one minor version before it goes.
- Until 1.0, a minor version may also change defaults (a page budget, a timeout) when the old one was unsafe; such a change is listed under **Changed defaults**.
- The `serverInfo` handshake reports the crate version, and the server instructions name the schema generation ("Schema version 2").

## Unreleased — schema version 2

### Added
- `crawl_urls`: read a set of URLs (a model's own search results) as one crawl, with near-duplicates collapsed across the set.
- `crawler_stats`: counters since startup.
- `discover` / `discover_and_crawl`: `expand`, `place`, `max_queries`, `post`, and (`discover_and_crawl`) `excerpts`.
- Crawl tools: `max_seconds` (wall-clock limit) and `render` (`never` / `auto` / `always`).
- Results carry structured data beside the prose: final URLs, `redirected_from`, `partial`, `deadline_hit`.
- Sources: SearXNG (`MCPCRAWLER_SEARXNG_URL`), Common Crawl.
- `MCPCRAWLER_ALLOW_HOSTS`, `MCPCRAWLER_LOG`; startup validation of every setting.

### Changed
- **Failures set `isError` and carry a stable `error.code`.** They were plain sentences returned as if successful. Clients that matched on the text "Failed to fetch" should read `isError` and `structuredContent.error.code` instead.
- `crawl_site` and friends report the final URL after redirects, and links resolve against it.
- `Crawler::new` returns a `Result`.
- A discovery source that could not be reached is skipped for two minutes, and the skip says so, instead of costing a full connect timeout on every call.

### Changed defaults
- Crawls stop after 300 seconds (`max_seconds`).
- A `robots.txt` that cannot be read because of a server error or no answer now means "do not crawl that site" (it used to mean "allowed"). A missing one (4xx) still means allowed.
- Discovery scores are rescaled per source: each source's best is 1.0 and its worst 0.1.
- `login_to_site` sessions no longer persist after the call.

### Fixed
- **SIGTERM left the server hung and Chrome running.** Returning from `main` waited on tokio's blocking stdin read, which does not finish while the client still holds the pipe. The process now exits explicitly once the browser is closed, and closing is bounded and falls back to killing Chrome. Regression test: `tests/shutdown.rs`.
- A destination refused by the network guard was reported as "skipped by robots.txt"; it is now reported as refused.
- `render=auto` opened a browser tab for any short page with no links; it now needs an app mount point or script bundle as well.

### Security
- Dependencies: `h2` 0.4.14 → 0.4.20 (RUSTSEC-2026-0258, unbounded empty DATA frames from a server), `rustls` → 0.23.45 (RUSTSEC-2026-0285), `anyhow` → 1.0.104 (RUSTSEC-2026-0190), and `scraper` 0.22 → 0.27, which removes the unmaintained `fxhash` (RUSTSEC-2025-0057). `cargo audit` and `cargo deny` are clean.
- Every redirect hop is checked against the blocked-domain list, `same_domain_only` and `robots.txt`; it used to be followed without asking.
- IPv6 addresses that embed an IPv4 destination (IPv4-compatible, NAT64, 6to4) and Teredo are judged by it or refused. `::2` was previously treated as public.
- Decompressed sitemaps, `robots.txt`, rendered DOM size, XML nesting and element count are bounded.
- Process-wide ceilings on concurrent requests (32), browser tabs (3) and crawls (4); frontier size capped.
