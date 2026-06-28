use rmcp::{ServerHandler, model::{ServerCapabilities, ServerInfo}, schemars, tool};
use serde::Deserialize;
use reqwest::Client;
use crate::crawler::{crawl, fetch_page, fetch_page_headless, extract_links, extract_text, extract_text_md, search_site, crawl_same_domain, extract_metadata, login_and_fetch};
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

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct LoginInput {
    #[schemars(description = "The URL to login to")]
    pub url: String,
    #[schemars(description = "Master password to decrypy stored credentials")]
    pub master_password: String
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
                Err(e)   => return format!("Failed to fetch page headless: {e:?}"),
            }
        } else {
            match fetch_page(&self.client, &input.url).await {
                Ok(html) => html,
                Err(e)   => return format!("Failed to fetch page: {e:?}"),
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

    #[tool(description = "Extract metadata from a URL")]
    async fn extract_meta(&self, #[tool(aggr)] input: FetchInput) -> String {
        match fetch_page(&self.client, &input.url).await {
            Ok(html) => extract_metadata(&html),
            Err(_)   => "Failed to fetch page".to_string(),
        }
    }

    #[tool(description = "Login to a website using credentials stored in passmanager")]
    async fn login_to_site(&self, #[tool(aggr)] input: LoginInput) -> String {
        match crate::passmanager::get_credential(&input.url, &input.master_password).await {
            Some((username, password)) => {
                match login_and_fetch(
                    &input.url,
                    &input.url,
                    &username,
                    &password,
                ).await {
                    Ok(html) => extract_text(&html),
                    Err(e)   => format!("Login failed: {}", e),
                }
            }
            None => format!("No credentials found for {}", input.url),
        }
    }
}

#[tool(tool_box)]
impl ServerHandler for Crawler {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            instructions: Some("A web crawler".into()),
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            ..Default::default()
        }
    }
}