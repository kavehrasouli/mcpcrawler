use rmcp::{ServerHandler, model::ServerInfo, schemars, tool};
use serde::Deserialize;
use reqwest::Client;
use crate::crawler::{crawl, fetch_page, extract_links, extract_text, extract_text_md, search_site, crawl_same_domain};
use std::sync::{Arc, Mutex};

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct CrawlInput {
    #[schemars(description = "The URL to crawl")]
    pub url: String,
    #[schemars(description = "How deep to follow links")]
    pub depth: u32,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SearchInput {
    #[schemars(description = "The URL to crawl")]
    pub url: String,
    #[schemars(description = "How deep to follow links")]
    pub depth: u32,
    #[schemars(description = "The keyword to search for")]
    pub keyword: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct FetchInput {
    #[schemars(description = "The URL to fetch content from")]
    pub url: String,
    pub headless: bool,
}

#[derive(Debug, Clone)]
pub struct Crawler {
    client: Client,
}

#[tool(tool_box)]
impl Crawler {
    pub fn new() -> Self {
        Self {
            client: Client::builder()
                .user_agent("mcpcrawler/0.1")
                .build()
                .unwrap()
        }
    }

    #[tool(description = "Crawl a website and return all visited URLs")]
    async fn crawl_site(&self, #[tool(aggr)] input: CrawlInput) -> String {
        let visited = Arc::new(Mutex::new(Vec::new()));
        crawl(&self.client, &input.url, input.depth, visited.clone()).await;
        visited.lock().unwrap().join("\n") //implicit return
    }

    #[tool(description = "Crawl only same domains")]
    async fn crawl_site_same_domain(&self, #[tool(aggr)] input: CrawlInput) -> String {
        crawl_same_domain(&self.client, &input.url, input.depth)
            .await
            .join("\n")
    }

    #[tool(description = "Fetch the content of a URL")]
    async fn fetch_content(&self, #[tool(aggr)] input: FetchInput) -> String {
        let html = if input.headless {
            match fetch_page_headless(&input.url).await {
                Ok(html) => html,
                Err(_)   => return "Failed to fetch page".to_string(),
            }
        } else {
            match fetch_page(&self.client, &input.url).await {
                Ok(html) => html,
                Err(_)   => return "Failed to fetch page".to_string(),
            }
        };
        extract_text(&html)
    }

    #[tool(description = "Fetch the content of a URL in .md format")]
    async fn fetch_content_in_md(&self, #[tool(aggr)] input: FetchInput) -> String {
        let html = if input.headless {
            match fetch_page_headless(&input.url).await {
                Ok(html) => html,
                Err(_)   => return "Failed to fetch page in markdown in headless".to_string(),
            }
        } else {
            match fetch_page(&self.client, &input.url).await {
                Ok(html) => html,
                Err(_)   => return "Failed to fetch page in markdown".to_string(),
            }
        };
        extract_text_md(&html)
    }

    #[tool(description = "Crawl a website and return visited URLs containing a specific keyword")]
    async fn search_site_keyword(&self, #[tool(aggr)] input: SearchInput) -> String {
        search_site(&self.client, &input.url, input.depth, &input.keyword)
            .await
            .join("\n")
    }

    #[tool(description = "Extract all links from a URL")]
    async fn extract_all_links(&self, #[tool(aggr)] input: FetchInput) -> String {
        let html = if input.headless {
            match fetch_page_headless(&input.url).await {
                Ok(html) => html,
                Err(_)   => return "Failed to extract links in headless".to_string(),
            }
        } else {
            match fetch_page(&self.client, &input.url).await {
                Ok(html) => html,
                Err(_)   => return "Failed to extract links".to_string(),
            }
        };
        extract_links(&html, &input.url).join("\n")
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