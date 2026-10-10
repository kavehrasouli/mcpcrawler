# What mcpcrawler keeps, sends and logs

This describes what the software does, so that anyone running it can decide whether that fits their obligations. It is not legal advice, and a hosted deployment — where other people's requests pass through it — needs a review this document cannot replace.

## What it stores

**Nothing persistent.** There is no database, cache or on-disk history. Fetched pages, extracted text, URLs and discovery results live in memory for the duration of one tool call and are returned to the client. The only files written are the headless browser's temporary profile directory, which is deleted on exit.

What the *client* does with the results — including keeping them in a conversation transcript — is outside this program.

## What it sends, and to whom

- **To the sites being crawled:** requests for the URLs asked for, with a fixed user agent and no cookies (except inside a `login_to_site` call, below). It fetches `robots.txt` for every site and honors it unless `respect_robots=false` is passed.
- **To discovery services**, when `discover` or `discover_and_crawl` is used: the query text, and any date range, go to GDELT, Wikidata and Common Crawl's index; a site name goes to the Internet Archive's index (Wayback) and Common Crawl; the query also goes to Brave Search and to a SearXNG instance, each only if configured. **A query is sent to third parties.** Do not put anything in a query that should not leave the machine.
- **To a credential helper**, for `login_to_site`: the site name and the master password, over a pipe to a local process.

## Credentials

- The master password is read from `MCPCRAWLER_MASTER_PASSWORD` at call time. It is never a tool parameter, so it does not pass through the model's context or appear in a transcript.
- The helper returns a username and password, which are typed into the page and then dropped. They are not returned to the client and not logged.
- The login happens in a browser context of its own, discarded when the call ends: cookies, local storage and cache from the logged-in session do not persist and are not visible to any other call.
- Errors about credentials say which step failed (`credentials_unavailable`, `credentials_not_found`, …) and never include the password, the username or anything the helper printed.

## Logs and metrics

- Logging is **off** unless `MCPCRAWLER_LOG` is set. When on, it writes one JSON object per line to standard error.
- URLs are logged **without their query string, fragment or userinfo**, because that is where tokens end up. Page content is never logged.
- `crawler_stats` returns counters only (pages fetched, failures by class, retries, and so on) — no URLs, no content.

## Robots, terms of service, and what is on the page

The crawler honors `robots.txt` by default, including treating an unreadable one as a prohibition. That is a courtesy and a convention, not a licence: a site's terms of service, copyright, and rules about personal data apply to what you do with what you fetch whether or not `robots.txt` allows the request. Pages can contain personal data. Nothing here filters it.

## Retention and deletion

Since nothing is stored, there is nothing to delete on the server side. For a hosted deployment, the questions that remain are about the surrounding system — request logs at the proxy, the client's transcripts — and need answering there.
