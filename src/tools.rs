use rmcp::{ServerHandler, model::ServerInfo, schemars, tool};
use serde::Deserialize;
use reqwest::Client;
use crate::crawler::crawl;
use std::sync::{Arc, Mutex};

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct CrawlInput {
    #[schemars(description = "The URL to crawl")]
    pub url: String,
    #[schemars(description = "How deep to follow links")]
    pub depth: u32,
}

#[derive(Debug, Clone)]
pub struct Crawler {
    client: Client,
}


#[tool(tool_box)]
impl Crawler {
    pub fn new() -> Self {
        Self {client: Client::new()}
    }
    #[tool(description = "Crawl a website and return all visited URLs")]
    async fn crawl_site(&self, #[tool(aggr)] input: CrawlInput) -> String {
        let mut visited = Arc::new(Mutex::new(Vec::new()));
        crawl(&self.client, &input.url, input.depth, visited.clone()).await;
        visited.lock().unwrap().join("\n") //implicit return
    }
}

#[tool(tool_box)]
impl ServerHandler for Crawler {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            instructions: Some("A web crawler".into()),
            ..Default::default()
        }
    }
}