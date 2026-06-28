mod crawler;
mod tools;
mod passmanager;

use tools::Crawler;
use rmcp::ServiceExt;
use tokio::io::{stdin, stdout};

#[cfg(test)]
mod tests;

#[tokio::main]
async fn main() {
    let service = Crawler::new();
    let transport = (stdin(), stdout());
    service.serve(transport).await.unwrap().waiting().await.unwrap();
}