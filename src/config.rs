//! Configuration, in one place, checked before anything runs.
//!
//! Everything is an environment variable, because an MCP server is launched by
//! a client that passes its environment and nothing else. A value that is set
//! and wrong is an error at startup — the alternative is a server that quietly
//! ignores `MCPCRAWLER_ALLOW_PRIVATE_NETWORKS=ture` and stays strict while its
//! operator believes otherwise.
//!
//! | Variable | Meaning | Default |
//! |---|---|---|
//! | `MCPCRAWLER_ALLOW_PRIVATE_NETWORKS` | `1`/`true`/`yes` to permit loopback, private and link-local destinations | off |
//! | `MCPCRAWLER_ALLOW_HOSTS` | comma-separated hosts exempt from that refusal, and only those | none |
//! | `MCPCRAWLER_BRAVE_API_KEY` | enables the Brave Search source | absent |
//! | `MCPCRAWLER_SEARXNG_URL` | enables the SearXNG source; its host is allowlisted | absent |
//! | `MCPCRAWLER_MASTER_PASSWORD` | master password for `login_to_site`, read at call time | absent |
//! | `PASSMANAGER_PATH` | the credential helper program | `passmanager` |
//! | `MCPCRAWLER_BROWSER_NO_SANDBOX` | `1`/`true`/`yes` runs Chrome without its sandbox (containers only) | off |
//! | `CHROME` | path to the Chrome or Chromium binary, if it is not on `PATH` | searched |
//! | `MCPCRAWLER_LOG` | `error`, `warn`, `info` or `debug`: JSON log lines on stderr | off |

use crate::net::{ALLOW_HOSTS_ENV, ALLOW_PRIVATE_ENV};
use crate::sources::searxng::{self, SearxNg};

/// Whether a boolean setting is on. Unset, empty and unrecognised read as off;
/// [`problems_in`] is what reports the unrecognised ones.
pub fn flag(name: &str) -> bool {
    std::env::var(name)
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false)
}

/// Every problem found, so an operator fixes them in one go.
pub fn validate() -> Result<(), Vec<String>> {
    let problems = problems_in(&|name| std::env::var(name).ok());
    if problems.is_empty() { Ok(()) } else { Err(problems) }
}

/// The checks, over any source of settings, so they can be tested without
/// touching the process environment.
pub fn problems_in(setting: &dyn Fn(&str) -> Option<String>) -> Vec<String> {
    let mut problems = Vec::new();

    for name in [ALLOW_PRIVATE_ENV, crate::crawler::NO_SANDBOX_ENV] {
        if let Some(value) = setting(name) {
            let v = value.trim().to_ascii_lowercase();
            if !matches!(v.as_str(), "" | "0" | "1" | "true" | "false" | "yes" | "no") {
                problems.push(format!("{name} must be 1, true, yes, 0, false or no, got \"{value}\""));
            }
        }
    }

    if let Some(list) = setting(ALLOW_HOSTS_ENV) {
        for host in list.split(',').map(str::trim).filter(|h| !h.is_empty()) {
            // A host, not a URL: a scheme or a path here is a mistake that would
            // otherwise allow nothing.
            if host.contains("://") || host.contains('/') {
                problems.push(format!("{ALLOW_HOSTS_ENV} takes bare hosts, got \"{host}\""));
            }
        }
    }

    if let Some(value) = setting("MCPCRAWLER_LOG") {
        let v = value.trim().to_ascii_lowercase();
        if !matches!(v.as_str(), "" | "error" | "warn" | "info" | "debug") {
            problems.push(format!("MCPCRAWLER_LOG must be error, warn, info or debug, got \"{value}\""));
        }
    }

    if let Err(e) = SearxNg::from_setting(setting(searxng::URL_ENV).as_deref()) {
        problems.push(e);
    }

    problems
}
