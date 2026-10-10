# Installing and running mcpcrawler

mcpcrawler is an MCP server that speaks over standard input and output. A client starts it, talks to it, and stops it. There is no port to open and nothing to keep running.

## Supported systems

Developed and tested on macOS. The code is portable and CI builds and tests it on Linux, macOS and Windows (see `.github/workflows/ci.yml` — **that workflow has not yet run on GitHub**; until it has, treat Linux and Windows as expected to work, not as verified). Signal handling for graceful shutdown is Unix-only; on Windows the browser is closed when the client disconnects, not on Ctrl-C.

**Rust:** 1.88 or newer (declared as `rust-version`; CI checks it, which has also not run yet).

## Install

```
git clone <repository>
cd mcpcrawler
cargo build --release --locked
```

The binary is `target/release/mcpcrawler`. `--locked` builds exactly the dependency versions in `Cargo.lock`.

### Register with a client

Claude Code:

```
claude mcp add mcpcrawler -- /absolute/path/to/target/release/mcpcrawler
```

Claude Desktop, in `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "mcpcrawler": {
      "command": "/absolute/path/to/target/release/mcpcrawler",
      "env": { "MCPCRAWLER_LOG": "warn" }
    }
  }
}
```

Settings are environment variables; the full list, with defaults, is at the top of `src/config.rs` and in the README. A setting that is present and wrong stops the server at startup with a message naming it.

## The headless browser

Only `render=auto|always`, `headless=true` and `login_to_site` use a browser. Everything else needs none.

- **Discovery.** Chrome or Chromium must be installed. It is found through the `CHROME` environment variable, then by looking for `chrome`, `google-chrome`, `google-chrome-stable`, `chromium` and `chromium-browser` on `PATH`, then in the usual install locations.
- **One browser, many tabs.** The server starts a single browser on first use and keeps it. At most three tabs are open at once, across every call.
- **Profile.** A private, per-process profile directory in the system temp directory, removed on exit. A profile left behind by a killed process is swept up at the next launch.
- **Logins.** Each `login_to_site` call gets its own browser context, discarded when the call ends. Cookies and storage from one login are never visible to another call.

### Containers and the Chrome sandbox

Chrome will not start as root with its sandbox on, which is how most containers run. Setting `MCPCRAWLER_BROWSER_NO_SANDBOX=1` turns the sandbox off. That is a real loss of protection against a hostile page, so prefer running the container as a non-root user and leaving the sandbox on if your runtime allows it (it needs `--cap-add=SYS_ADMIN` or a seccomp profile that permits user namespaces).

The `Dockerfile` in this directory is a starting point. **It has not been built or run** — the container runtime was not available when it was written — so expect to adjust it.

## Stopping, upgrading, rolling back

**Stopping.** On SIGINT, SIGTERM, or when the client disconnects, the server stops taking new fetches, then closes the browser and deletes its profile. A request already in flight when a crawl's `max_seconds` passes is abandoned. A credential helper process is killed when its call ends, however it ends.

**Upgrading.** The server holds no state on disk, so an upgrade is: build the new version, point the client at it, restart the client. Tool schemas are versioned; `CHANGELOG.md` says what changed and what clients should read differently. A client built for schema version 1 will see failures as `isError` results with a code, where it used to see plain text.

**Rolling back.** Point the client back at the previous binary. Keep the previous `target/release/mcpcrawler` (or build tag) until the new one has been used for a while.

## Network policy

By default the crawler connects only to public addresses: loopback, private, link-local (which is where cloud metadata lives), carrier-grade NAT, reserved and multicast destinations are refused, including IPv6 forms that embed an IPv4 destination, and every redirect hop is checked. To reach one internal service without opening the whole network, name it:

```
MCPCRAWLER_ALLOW_HOSTS=searxng.internal,10.0.0.5
```

`MCPCRAWLER_ALLOW_PRIVATE_NETWORKS=1` opens everything and is for deliberate intranet crawling only.

## A user agent and contact

Requests identify as `mcpcrawler/0.1`. The user agent is not configurable and carries no contact address. If you crawl sites that expect one, that is a gap to close before doing so at any scale.
