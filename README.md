# mcpcrawler

A web crawler built in Rust, exposed as a Model Context Protocol (MCP) server.

Crawls are bounded by a **page budget**, a **per-host rate limit**, and **robots.txt**.
Link depth is only a guard rail, not the main control.

## Tools

| Tool | Purpose |
|---|---|
| `crawl_site` | Crawl from a URL, return the pages actually fetched |
| `crawl_site_same_domain` | Same, but only follows links on the seed's domain |
| `fetch_content` | Readable text of one URL (`headless` for JS-rendered pages) |
| `fetch_content_in_md` | Same, as markdown |
| `extract_all_links` | All links on a page, canonicalized and deduplicated |
| `search_site_keyword` | Crawl and return pages matching a keyword, with hit counts and snippets |
| `extract_meta` | Title, description, author, canonical URL, publish date |
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

Rendering with `headless=true` also works, but it is the expensive fallback: on one
large storefront the rendered homepage yielded 1,830 links while its sitemap listed
millions.

Crawl tools report fetched and failed URLs **separately** — a URL that 404s or returns
a non-HTML body appears under `Failed`, never in the visited list.

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