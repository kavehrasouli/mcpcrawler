# mcpcrawler

A web crawler built in Rust, exposed as Model Context Protocl (MCP) server.

## Tools
- `crawl_site` — crawls a URL up to a given depth and returns all visited URLs.

- `crawl_site_same_domain` — crawls a URL but only follows links on the same domain

- `fetch_content` — fetches the text content of a single URL

- `fetch_content_in_md` - fetches the text content of a single URL in markdown format (.md)

- `search_site_keyword` — crawls a website and returns only URLs containing a specific keyword

## Planned
- [ ] JavaScript rendering support via headless browser

## Usage
Build and run:
```bash
cargo build --release
./target/release/mcpcrawler
```
