use reqwest::Client;
use scraper::{Html, Selector};
use url::Url;
use std::pin::Pin;
use futures::future::join_all;
use std::sync::{Arc, Mutex};
use futures::StreamExt;
use chromiumoxide::{Browser, BrowserConfig, page::Page};


const BLOCKED_DOMAINS: &[&str] = &[
    "google-analytics.com",
    "doubleclick.net",
    "amazon-adsystem.com",
    "googlesyndication.com",
    "googletagmanager.com",
    "ads.yahoo.com",
    "scorecardresearch.com",
    "outbrain.com",
];

pub fn normalize_url(url: &str) -> String {
    if url.starts_with("http://") || url.starts_with("https://"){
        url.to_string()
    } else {
        format!("https://{}", url)
    }
}

fn is_blocked(url: &str) -> bool {
    Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(|h| normalize_domain(h).to_string()))
        .map(|d| BLOCKED_DOMAINS.iter().any(|blocked| d.contains(blocked)))
        .unwrap_or(false)
}

pub fn normalize_domain(domain: &str) -> &str {
    domain.strip_prefix("www.").unwrap_or(domain)
}

pub async fn fetch_page(client: &Client, url: &str) -> Result<String, reqwest::Error>
{
    let url = normalize_url(url);
    let body = client
        .get(&url)
        .send()
        .await?
        .text()
        .await?;

    Ok(body)
}

pub async fn fetch_page_headless(url: &str) -> Result<String, Box<dyn std::error::Error>> {
    let url = normalize_url(url);
    let (browser, mut handler) =
        Browser::launch(BrowserConfig::builder().build()?).await?;

    let handle = tokio::spawn(async move {
        while let Some(_) = handler.next().await {}
    });

    let page = browser.new_page(&url).await?;
    let html = page.wait_for_navigation().await?.content().await?;
    handle.abort();
    Ok(html)
}


const USERNAME_SELECTORS: &[&str] = &[
    "input[type='email']",
    "input[autocomplete='username']",
    "input[autocomplete='email']",
    "input[name*='user']",
    "input[name*='email']",
    "input[id*='user']",
    "input[id*='email']",
    "input[name='loginfmt']",
    "input[name='login']",
    "input[type='text']",
];

const PASSWORD_SELECTORS: &[&str] = &[
    "input[type='password']",
    "input[name*='pass']",
    "input[id*='pass']",
];

const SUBMIT_SELECTORS: &[&str] = &[
    "button[type='submit']",
    "input[type='submit']",
    "button[id*='login']",
    "button[id*='signin']",
    "button[class*='login']",
    "button[class*='signin']",
    "[data-testid*='login']",
    "[data-testid*='signin']",
];

async fn find_element_any(page: &Page, selectors: &[&str])
    -> Option<chromiumoxide::element::Element>
{
    for selector in selectors {
        if let Ok(el) = page.find_element(*selector).await {
            return Some(el);
        }
    }
    None
}

pub async fn login_and_fetch(
    login_url: &str,
    url: &str,
    username: &str,
    password: &str,
) -> Result<String, Box<dyn std::error::Error>>
{
    let (browser, mut handler) =
        Browser::launch(BrowserConfig::builder().build()?).await?;

    let handle = tokio::spawn(async move {
        while let Some(_) = handler.next().await {}
    });

    let page = browser.new_page(login_url).await?;
    page.wait_for_navigation().await?;

    find_element_any(&page, USERNAME_SELECTORS)
        .await
        .ok_or("username field not found")?
        .click().await?
        .type_str(username).await?;

    find_element_any(&page, PASSWORD_SELECTORS)
        .await
        .ok_or("password field not found")?
        .click().await?
        .type_str(password).await?;

    find_element_any(&page, SUBMIT_SELECTORS)
        .await
        .ok_or("submit button not found")?
        .click().await?;

    page.wait_for_navigation().await?;

    let page = browser.new_page(url).await?;
    let html = page.wait_for_navigation().await?.content().await?;

    handle.abort();
    Ok(html)
}

pub fn extract_links(html: &str, base_url: &str) -> Vec<String>
{
    let base_url = normalize_url(base_url);
    let document = Html::parse_document(html);
    let selector = Selector::parse("a[href]").unwrap();
    let base     = Url::parse(&base_url).unwrap();
    document
        .select(&selector)
        .filter_map(|x| x.value().attr("href"))
        .filter_map(|href| base.join(href).ok())
        .map(|url| url.to_string())
        .collect()
}

pub fn crawl<'a>(
    client: &'a Client,
    url: &'a str,
    depth: u32,
    visited: Arc<Mutex<Vec<String>>>
) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
    let visited = Arc::clone(&visited);
    Box::pin(async move {
        if depth == 0 {return;}
        {
            let mut guard = visited.lock().unwrap();
            if guard.contains(&url.to_string()) {return;}
            guard.push(url.to_string());
        }

        let html = match fetch_page(client, url).await {
            Ok(html) => html,
            Err(_) => return,
        };

        let links = extract_links(&html, url);

        let futures: Vec<_> = links
            .iter()
            .filter(|link| !visited.lock().unwrap().contains(*link))
            .filter(|link| !is_blocked(link))
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

fn crawl_same_domain_inner<'a>(
    client: &'a Client,
    url: &'a str,
    depth: u32,
    visited: Arc<Mutex<Vec<String>>>,
    base_domain: &'a str
) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
    let visited = Arc::clone(&visited);
    Box::pin(async move {
        if depth == 0 {return;}
        {
            let mut guard = visited.lock().unwrap();
            if guard.contains(&url.to_string()) {return;}
            guard.push(url.to_string());
        }

        let html = match fetch_page(client, url).await {
            Ok(html) => html,
            Err(_) => return,
        };

        let links = extract_links(&html, url);

        let futures: Vec<_> = links
            .iter()
            .filter(|link| !visited.lock().unwrap().contains(*link))
            .filter(|link| Url::parse(link)
                .ok()
                .and_then(|u| u.host_str().map(|h| normalize_domain(h).to_string()))
                .map(|d| d == base_domain)
                .unwrap_or(false)
            )
            .filter(|link| !is_blocked(link))
        .map(|link| crawl_same_domain_inner(client, link, depth-1, visited.clone(), base_domain))
        .collect();

    join_all(futures).await;
    })
}

pub async fn crawl_same_domain(
    client: &Client,
    url: &str,
    depth: u32
) -> Vec<String> {
    let url = normalize_url(url);
    let base = Url::parse(&url).unwrap();
    let domain = normalize_domain(base.host_str().unwrap_or("")).to_string();
    let visited = Arc::new(Mutex::new(Vec::new()));
    crawl_same_domain_inner(client, &url, depth, visited.clone(), &domain).await;
    visited.lock().unwrap().clone()
}

pub fn extract_text(html: &str) -> String {
    let document = Html::parse_document(html);
    let selector = Selector::parse("body").unwrap();

    document
        .select(&selector)
        .next()
        .map(|x| x
            .text()
            .collect::<Vec<_>>()
            .join(" ")
            )
        .unwrap_or_default()
}

pub fn extract_text_md(html: &str) -> String {
    htmd::convert(html).unwrap_or_default()
}

pub fn extract_metadata(html: &str) -> String {
    let document = Html::parse_document(html);
    let title_selector    = Selector::parse("title").unwrap();
    let desc_selector     = Selector::parse("meta[name='description']").unwrap();
    let author_selector   = Selector::parse("meta[name='author']").unwrap();
    let keywords_selector = Selector::parse("meta[name='keywords']").unwrap();
    let og_title_selector = Selector::parse("meta[property='og:title']").unwrap();
    let og_desc_selector  = Selector::parse("meta[property='og:description']").unwrap();

    let title = document
                .select(&title_selector)
                .next()
                .map(|x| x
                    .text()
                    .collect::<Vec<_>>()
                    .join(" "))
                .unwrap_or_default();

    let authr = document
        .select(&author_selector)
        .next()
        .and_then(|x| x.value().attr("content"))
        .unwrap_or_default()
        .to_string();

    let descr = document
        .select(&desc_selector)
        .next()
        .and_then(|x| x.value().attr("content"))
        .unwrap_or_default()
        .to_string();
    
    let kwrds = document
        .select(&keywords_selector)
        .next()
        .and_then(|x| x.value().attr("content"))
        .unwrap_or_default()
        .to_string();
    
    let ogtitle = document
        .select(&og_title_selector)
        .next()
        .and_then(|x| x.value().attr("content"))
        .unwrap_or_default()
        .to_string();
    
    let ogdesc = document
        .select(&og_desc_selector)
        .next()
        .and_then(|x| x.value().attr("content"))
        .unwrap_or_default()
        .to_string();
    
    format!(
        "Title: {}\nDescription: {}\nAuthor: {}\nKeywords: {}\nOG Title: {}\nOG Description: {}",
        title, descr, authr, kwrds, ogtitle, ogdesc
    )
}
