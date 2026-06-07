# mcpcrawler

A web crawler built in Rust, exposed as a Model Context Protocol (MCP) server.

## Tools
- `crawl_site` — crawls a URL up to a given depth and returns all visited URLs
- `crawl_site_same_domain` — crawls a URL but only follows links on the same domain
- `fetch_content` — fetches the plain text content of a single URL (supports headless mode for JS-rendered pages)
- `fetch_content_in_md` — fetches the content of a single URL in markdown format (supports headless mode)
- `extract_all_links` — extracts all links from a URL (supports headless mode)
- `search_site_keyword` — crawls a website and returns only URLs containing a specific keyword
- `extract_meta` — extract metadata from a URL

## Usage
Build and run:
```bash
cargo build --release
./target/release/mcpcrawler
```

Connect via Claude Desktop by adding to `claude_desktop_config.json`:
```json
{
  "mcpServers": {
    "crawler": {
      "command": "/path/to/mcpcrawler"
    }
  }
}
```