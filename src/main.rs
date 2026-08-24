mod crawler;
mod passmanager;
mod tools;

use rmcp::ServiceExt;
use tokio::io::{stdin, stdout};
use tools::Crawler;

#[cfg(test)]
mod tests;

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let service = match Crawler::new().serve((stdin(), stdout())).await {
        Ok(service) => service,
        Err(e) => {
            eprintln!("mcpcrawler: failed to start: {e}");
            return std::process::ExitCode::FAILURE;
        }
    };

    if let Err(e) = service.waiting().await {
        eprintln!("mcpcrawler: server stopped: {e}");
        return std::process::ExitCode::FAILURE;
    }

    std::process::ExitCode::SUCCESS
}
