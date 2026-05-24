mod crawler;
mod tools;

use tools::Crawler;
use rmcp::ServiceExt;
use tokio::io::{stdin, stdout};

#[tokio::main]
async fn main() {
    let service = Crawler::new();
    let transport = (stdin(), stdout());
    service.serve(transport).await.unwrap();
}