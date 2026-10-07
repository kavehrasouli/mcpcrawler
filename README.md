# mcpcrawler

A web crawler built in Rust, exposed as a Model Context Protocol (MCP) server.

Crawls are bounded by a **page budget**, a **per-host rate limit**, and **robots.txt**.
Link depth is only a guard rail, not the main control.

## Tools

| Tool | Purpose |
|---|---|
| `discover` | Find candidate URLs for a topic with no seed URL, from federated sources |
| `discover_and_crawl` | Same, then fetch the best candidates in one page budget |
| `crawl_site` | Crawl from a URL, return the pages actually fetched (`about` to focus it) |
| `crawl_site_same_domain` | Same, but only follows links on the seed's domain |
| `fetch_content` | Readable text of one URL (`headless` for JS-rendered pages) |
| `fetch_content_in_md` | Same, as markdown |
| `extract_all_links` | All links on a page, canonicalized and deduplicated |
| `search_site_keyword` | Crawl and return pages matching a keyword, with hit counts and snippets |
| `extract_meta` | Title, description, author, canonical URL, publish date |
| `extract_structured_data` | schema.org JSON-LD records and embedded framework state (`__NEXT_DATA__`, Nuxt, Redux) |
| `list_sitemap_urls` | Enumerate a site's URLs from its sitemap |
| `login_to_site` | Log in with credentials from `passmanager` |

### Crawl options

All optional, with conservative defaults so a crawl cannot run away.

| Option | Default | Meaning |
|---|---|---|
| `max_pages` | 200 | Hard cap on pages fetched — the real budget |
| `depth` | 3 | Maximum link hops from the seed |
| `concurrency` | 8 | Requests in flight at once |
| `per_host_delay_ms` | 500 | Minimum gap between requests to one host |
| `respect_robots` | true | Honor `robots.txt` |
| `seed_from_sitemap` | false | Seed the frontier from the site's sitemap |

### Client-rendered sites

A site built with React or Next.js serves a shell with no links in its HTML, so
following links reaches nothing. Two ways through:

```
# cheapest: the site's own URL inventory, no browser
list_sitemap_urls url=https://example.com contains=laptop

# or seed a crawl from it
crawl_site_same_domain url=https://example.com seed_from_sitemap=true
```

`list_sitemap_urls` reads `robots.txt` for `Sitemap:` directives, falls back to the
conventional paths, walks nested sitemap indexes, and transparently gunzips children.
Use `contains` to filter while walking — that reaches much deeper into a large index
than filtering afterwards would.

A third route needs neither browser nor sitemap: most client-rendered pages still ship
their data as JSON in the static HTML, either as schema.org JSON-LD or as the props the
framework hydrated from.

```
extract_structured_data url=https://en.wikipedia.org/wiki/State_visit
```

`extract_structured_data` reads both. JSON-LD comes back as flattened records — a Wikipedia
article yields its Wikidata Q-ID, a news page its headline, author and publish date. State
blobs (`__NEXT_DATA__`, `__NUXT__`, `__INITIAL_STATE__`, `__PRELOADED_STATE__`,
`__APOLLO_STATE__`, `__remixContext`) come back as a shape outline plus every content URL
referenced inside them, which on a page with no `<a>` tags is the only link list there is.
A blob can be hundreds of kilobytes, so the outline is deliberately not a dump.

`extract_meta` uses the same records to fill in fields the meta tags omit.

Rendering with `headless=true` also works, but it is the expensive fallback: on one
large storefront the rendered homepage yielded 1,830 links while its sitemap listed
millions.

Crawl tools report fetched and failed URLs **separately** — a URL that 404s or returns
a non-HTML body appears under `Failed`, never in the visited list. Each report also ends
with a `Found via:` line counting where the fetched URLs came from — seed, sitemap, or a
followed link.

## Discovery

Every other tool takes a `url` as its first argument, which assumes you already know where
to look. `discover` does not:

```
discover query="\"state visit\" Kyrgyzstan" since=20260101 until=20260901
```

It fans the query across discovery sources, merges the answers into one ranked, deduplicated
list, and reports which source found each URL. A URL two sources found keeps both names —
corroboration is recorded rather than folded into the score.

| Source | Key needed | Reaches |
|---|---|---|
| GDELT DOC 2.0 | no | worldwide news in 100+ languages, including syndicated regional coverage |
| Wikidata | no | the entity's official website and every language edition of its article |
| Wayback CDX | no | pages that were deleted and exist nowhere else |
| Brave Search | `MCPCRAWLER_BRAVE_API_KEY` | general web search — PDFs, transcripts, agendas |

Brave is registered only when its key is present in the server's environment; without it the
other three run and it is simply absent. The key never appears in a tool schema, so it cannot
travel through the model's context.

Sources are not the same shape. GDELT and Brave search text. Wayback searches by **site**, not
by keyword, so it contributes only when `sites` names a host — Wikidata's official-website
claim is one way to learn one. Wikidata takes a name and returns identity.

The candidate budget is shared out by taking turns between sources rather than by global
score, because scores are only comparable within a source. Without that, Wikidata's sixty-odd
language editions swallow a budget of ten whole and the archive results never appear.

`discover_and_crawl` does the same and then fetches, best-scoring first, under one page
budget. Candidates are fetched at depth 0 — discovery already returns URLs from many hosts,
so following links from each would multiply the work by a factor nobody asked for.

Adding a source is one file: implement `DiscoverySource` in [src/sources/](src/sources/) and
add it to the list in `Crawler::new`.

Note that discovery sources talk to documented APIs, so `robots.txt` is not consulted for
those requests. It still governs every page the crawler fetches from the results.

## Focused crawling

Given a subject, the crawler scores every link *before* opening it, so the page budget goes
where the answer is likely to be:

```
crawl_site_same_domain url=https://example.gov about="state visit Kyrgyzstan"
```

Four signals, all free because the page is already parsed: the anchor text, the sentence
around the link, the tokens in the URL, and how relevant the page holding the link turned out
to be. On top of that, URL shape — `/press-releases/2019/` and `?page=47` are preferred,
`/cookie-policy` and `/login` are pushed below neutral, and assets are never fetched at all.

Three things make it work in practice:

- **Tunnelling.** The page you want is routinely two hops behind an index page that says
  nothing itself. Pruning on the first low score cuts exactly the paths that lead to archives,
  so a branch gets an allowance of consecutive off-topic hops (`tunnel_slack`, default 2)
  before it is dropped.
- **Pagination.** `rel="next"` and `?page=N` get a deliberate boost. Page 47 of a press
  archive is in no search index, and that is where a ministry's history lives.
- **Alias matching.** `discover_and_crawl` feeds the crawler every name discovery learned for
  the subject. On one live query Wikidata returned **60** — *Sadyr Japarov*, *Садыр Жапаров*,
  *صادیر جاپاروف* — and a link written in any of them now scores. Without this the crawler
  sees a non-anglophone subject's own coverage as irrelevant, which is most of the coverage
  that exists.
- **Arabic script.** Persian and Arabic keyboards produce different code points for the same
  letter (`ی`/`ي`, `ک`/`ك`), and both spellings circulate. These fold together, along with alef
  variants, teh marbuta, diacritics and Persian digits. Non-ASCII URL paths are decoded, so an
  archive at `/آرشیو/۱۴۰۲/` is recognised as one — including its Solar Hijri year.

Without `about`, the frontier stays breadth-first and every link is equal, which is the right
behaviour for "fetch this site" and the wrong one for "find what this site says about X".
`search_site_keyword` focuses on its keyword automatically.

## Query expansion

`discover` and `discover_and_crawl` take `expand=true` to search for a subject under more than
the words typed:

```
discover query="Sadyr Japarov" place="Turkey" expand=true
```

The first round runs on every source as usual. Wikidata answers it with the subject's other
names and, where it records them, the dates the subject held office. Those build the variants:

- **Names.** The query, then alternately a script not yet used and a further romanisation, up
  to four. Names that differ only by case or accent count as one. Native-script aliases are
  what reach a non-anglophone subject's own press.
- **Window.** Your `since`/`until`, narrowed to the term. If they do not overlap, yours win.
- **Year slices.** The primary name run over contiguous slices of the window, so a backend's
  per-query result cap applies to each slice rather than to the whole term.
- **Predicates.** `visit`, `"state visit"`, `"trip to"` and so on, each across every name.

Only the text-searching sources rerun; Wikidata and Wayback answer the same whatever the
wording, so they run once. A source that fails in one round is not asked again. The output
lists every variant it ran. `max_queries` (default 8, max 20) bounds the cost: GDELT answers one
request every five seconds, so eight variants take about forty.

The predicate words are English. Language coverage comes from the names.

## Near-duplicates

One wire story about a state visit appears nearly verbatim on hundreds of sites. Crawl
reports collapse these: pages are fingerprinted on the text they carry (a 64-bit SimHash over
three-word shingles, so a different headline, byline or footer does not matter), and copies
within 3 bits of a story's first page are one story.

```
Fetched 40 page(s); 12 distinct after collapsing 28 near-duplicate copies. ...
[0.82] https://example.org/visit — also on b.example, c.example (+9)
```

The most relevant copy is shown, the earliest fetched on a tie, and the *other hosts* carrying
the story are named — which is itself the evidence that it was syndicated. Hosts, not copies:
three copies on one site are one outlet repeating itself, and the report says so. `www.` is
ignored; beyond that independence is by host, so two outlets under one owner still count as two.

Each story is dated by its earliest copy, from `article:published_time`, a `<time>` element or
the JSON-LD record. Only ISO-shaped dates are read (`2024-03-05T10:00:00Z`); "5 March" is not
guessed at, because a wrong date in something you sort by is worse than none. The report opens
with how many stories carry a date and the range they span. Pages under 25 words
are never clustered, so error pages and cookie notices do not match each other.

## Excerpts

`discover_and_crawl` takes `excerpts=true` to print, under each story, the sentences of the page
that mention the subject — up to three, in reading order — so a model can read off dates, places
and who was present without fetching every page again. Lists at most 25 stories, best first.
Nothing is extracted by the crawler itself: it hands over deduplicated, dated stories with
their independent hosts, and reading the facts out of them is the caller's job.

## Destinations

By default the crawler will only connect to public unicast addresses. Loopback, private,
link-local (which is where cloud metadata endpoints live), CGNAT, reserved and multicast
destinations are refused, in both IPv4 and IPv6 forms, and every redirect hop is checked
the same way. The check happens at DNS resolution, so a hostname cannot resolve to a
public address for the check and a private one for the connection.

To crawl an intranet deliberately, set `MCPCRAWLER_ALLOW_PRIVATE_NETWORKS=1` in the
server's environment.

## Usage

```bash
cargo build --release
./target/release/mcpcrawler
```

Connect via Claude Desktop by adding to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "crawler": {
      "command": "/path/to/mcpcrawler",
      "env": {
        "PASSMANAGER_PATH": "/path/to/passmanager",
        "MCPCRAWLER_MASTER_PASSWORD": "…"
      }
    }
  }
}
```