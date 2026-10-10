# Limits and measured behaviour

## Documented maximums

| Limit | Value | Where it applies |
|---|---|---|
| HTTP requests in flight | 32 | whole process |
| Browser tabs open | 3 | whole process |
| Crawls running at once | 4 (others wait) | whole process |
| Pages per crawl | 10,000 | `max_pages` (default 200; `crawl_urls` 50; `discover_and_crawl` 25) |
| URLs queued in one frontier | 50,000 | per crawl |
| Wall-clock time per crawl | 1,800 s | `max_seconds` (default 300) |
| Redirect hops followed | 5 | per URL, each hop checked |
| Retries | 2 per request; at most `max_pages` per crawl | 429, 5xx, refused/timed-out connections |
| Page body | 5 MiB | per page |
| Sitemap, decoded | 64 MiB each, 256 MiB per walk | gzip bombs fail the request |
| Sitemap files read | 50 (exact) | `max_sitemaps` |
| `robots.txt` | read to 512 KiB | longer is truncated |
| Rendered DOM | 20 MiB | per page |
| XML | 64 levels deep, 5,000,000 events | per document |
| URLs per `crawl_urls` call | 200 | |

## Measured

`cargo test --release -- --ignored soak --nocapture` crawls a generated tree of pages from a **local** fixture server with no per-host delay, so it measures the crawler's own overhead and not the network.

On a development MacBook (one run, 3,000 pages, 16 workers):

- 3,000 pages in 0.2 s (about 12,000 pages/s);
- resident memory 10 MB before, 28 MB after;
- open file descriptors 17 before, 17 after — sockets are released;
- reading the pages back (text extraction, SimHash, clustering): 0.14 s for 3,000.

Against real sites the limits that matter are the per-host delay (500 ms by default, so about two pages a second per host) and the other side's speed, not this overhead.

## What has not been measured

- Behaviour at the documented maximums on a long-running hosted deployment: no soak of hours, no memory profile under a mix of tool calls, no test with the browser tabs busy while crawls run.
- Pages with very large DOMs through the headless browser.
- Any CI baseline: there is no regression check on these numbers yet.
