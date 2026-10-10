mod api;
mod config;
mod crawler;
mod dedup;
mod discovery;
mod expand;
mod limits;
mod metrics;
mod net;
mod passmanager;
mod reply;
mod scoring;
mod sitemap;
mod sources;
mod structured;
mod tools;

use rmcp::ServiceExt;
use tokio::io::{stdin, stdout};
use tools::Crawler;

#[cfg(test)]
mod tests;

#[tokio::main]
async fn main() -> std::process::ExitCode {
    // A setting that is present and wrong is an error now, not a surprise later.
    net::init_allowed_hosts_from_env();
    if let Err(problems) = config::validate() {
        for problem in problems {
            eprintln!("mcpcrawler: configuration error: {problem}");
        }
        return std::process::ExitCode::FAILURE;
    }

    // Fixed for the life of the process, before anything can fetch.
    net::init(net::NetPolicy::from_env());

    let crawler = match Crawler::new() {
        Ok(crawler) => crawler,
        Err(e) => {
            eprintln!("mcpcrawler: could not start the HTTP client: {e}");
            return std::process::ExitCode::FAILURE;
        }
    };
    let service = match crawler.serve((stdin(), stdout())).await {
        Ok(service) => service,
        Err(e) => {
            eprintln!("mcpcrawler: failed to start: {e}");
            return std::process::ExitCode::FAILURE;
        }
    };

    // Whichever arrives first, the headless browser still gets torn down. Without
    // this Chrome is orphaned and its profile lock blocks the next launch. SIGTERM
    // matters as much as SIGINT: that is how most supervisors stop a stdio server.
    let outcome = tokio::select! {
        result = service.waiting() => result.err().map(|e| e.to_string()),
        _ = tokio::signal::ctrl_c() => None,
        _ = terminated() => None,
    };

    // Crawls stop taking work first, so nothing starts a fetch into a process
    // that is leaving; then the browser goes.
    limits::begin_shutdown();
    crawler::shutdown_browser().await;

    let code = match outcome {
        Some(e) => {
            eprintln!("mcpcrawler: server stopped: {e}");
            1
        }
        None => 0,
    };
    // Exit here rather than return. Returning drops the runtime, and the runtime
    // waits for tokio's stdin reader — a blocking read that does not finish
    // while the client still holds the pipe open, which is exactly the case on
    // SIGTERM. Everything that needed closing has been closed above.
    std::process::exit(code)
}

#[cfg(unix)]
async fn terminated() {
    use tokio::signal::unix::{SignalKind, signal};
    match signal(SignalKind::terminate()) {
        Ok(mut sig) => {
            sig.recv().await;
        }
        Err(_) => std::future::pending().await,
    }
}

#[cfg(not(unix))]
async fn terminated() {
    std::future::pending().await
}
