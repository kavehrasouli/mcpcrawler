use reqwest::Client;
use scraper::{Html, Selector};
use url::Url;
use std::pin::Pin;
use futures::future::join_all;
use std::sync::{Arc, Mutex};

// fetch_page:
// takes a URL, makes a HTTP GET request to it,
// and returns the raw HTML as a String.
pub async fn fetch_page(client: &Client, url: &str) -> Result<String, reqwest::Error> 
{
    let body = client
        .get(url)   // prepare - build the request
        .send()     // send the request
        .await?
        .text()     // read the body as a string
        .await?;

    Ok(body)
}

// extract_links: 
// This function takes the HTML string that fetch_page returns, 
// parses it, and pulls out all the URLs from <a href="..."> tags.
pub fn extract_links(html: &str, base_url: &str) -> Vec<String> 
{
    let document = Html::parse_document(html);
    let selector = Selector::parse("a[href]").unwrap();
    let base     = Url::parse(base_url).unwrap();
    document
        .select(&selector)
        .filter_map(|x| x.value().attr("href"))
        .filter_map(|href| base.join(href).ok())
        .map(|url| url.to_string())
        .collect()
}

pub fn crawl<'a>(
    client: &'a Client,     // HTTP client
    url: &'a str,           // current URL to crawl
    depth: u32,             // depth to follow links
    visited: Arc<Mutex<Vec<String>>>    // shared list of URLs
) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
    let visited = Arc::clone(&visited); // make a clone for async block to own
    Box::pin(async move {               // wrap in a pinned heap-allocated future
        if depth == 0 {return;}         // base case
        {   // open scope to control when the lock is dropped
            let mut guard = visited.lock().unwrap();    // lock the mutex, get mutable access
            if guard.contains(&url.to_string()) {return;}   // skip if already visited
            guard.push(url.to_string());    // mark current URL as visited
        } // lock dropped here

        let html = match fetch_page(client, url).await {
            Ok(html) => html,
            Err(_) => return,
        };

        let links = extract_links(&html, url);

        let futures: Vec<_> = links
            .iter()
            .filter(|link| !visited.lock().unwrap().contains(*link))
            .map(|link| crawl(client, link, depth-1, visited.clone()))
            .collect();
        join_all(futures).await;
    })
}

pub async fn search_site(
    client: &Client,
    url:    &str,
    depth:  u32,
    keyword: &str
) -> Vec<String> {
    let visited = Arc::new(Mutex::new(Vec::new()));
    crawl(client, url, depth, visited.clone()).await;

    let mut results = Vec::new();
    let urls = visited.lock().unwrap().clone();

    for url in urls.iter() {
        let html = match fetch_page(client, url).await {
            Ok(html) => html,
            Err(_) => continue,
        };
        let text = extract_text(&html);
        if text.contains(keyword) {
            results.push(url.clone());
        }
    }
    results
}

pub fn extract_text(html: &str) -> String {
    let document = Html::parse_document(html); // parse the raw HTML
    let selector = Selector::parse("body").unwrap(); // build a CSS selector targeting <body>
    // find the first <body> element in the document (there is only ever one!)
    document
        .select(&selector)
        .next()                 // get the first match as Option<ElementRef>
        .map(|x| x              // if body exists, extract its text
            .text()             // iterator over all text nodes inside body
            .collect::<Vec<_>>()  // gather text nodes into a Vec
            .join(" ")          // join them into one String with spaces
            )
        .unwrap_or_default()    // if no body found, return empty String
}

pub fn extract_text_md(html: &str) -> String {
    htmd::convert(html).unwrap_or_default()
}