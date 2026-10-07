mod api;
mod crawler;
mod dedup;
mod discovery;
mod expand;
mod net;
mod passmanager;
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
    // Fixed for the life of the process, before anything can fetch.
    net::init(net::NetPolicy::from_env());

    let service = match Crawler::new().serve((stdin(), stdout())).await {
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

    crawler::shutdown_browser().await;

    match outcome {
        Some(e) => {
            eprintln!("mcpcrawler: server stopped: {e}");
            std::process::ExitCode::FAILURE
        }
        None => std::process::ExitCode::SUCCESS,
    }
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
