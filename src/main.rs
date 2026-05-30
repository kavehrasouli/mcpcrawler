mod crawler;
mod tools;

use tools::Crawler;
use rmcp::ServiceExt;
use tokio::io::{stdin, stdout};

#[cfg(test)]
mod tests;

#[tokio::main]
async fn main() {
    let service = Crawler::new();
    let transport = (stdin(), stdout());
    service.serve(transport).await.unwrap();
}