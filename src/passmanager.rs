use rmcp::{ServiceExt, model::CallToolRequestParam};
use tokio::process::Command;
use std::process::Stdio;

pub async fn get_credential(site: &str, master_password: &str) -> Option<(String, String)> {
    let path = std::env::var("PASSMANAGER_PATH")
        .unwrap_or_else(|_| "passmanager".to_string());

    let mut child = Command::new(&path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn().ok()?;
    
    let stdin  = child.stdin.take()?;
    let stdout = child.stdout.take()?;
    
    let client = ().serve((stdout, stdin)).await.ok()?;

    let result = client.call_tool(CallToolRequestParam {
        name: "get_credential".into(),
        arguments: Some(serde_json::json!({
            "site": site,
            "master_password": master_password
        }).as_object().cloned().unwrap()),
    }).await.ok()?;

    let text = result.content.first()?.as_text()?.text.clone();

    let mut username = None;
    let mut password = None;
    for line in text.lines() {
        if let Some(u) = line.strip_prefix("username: ") {
            username = Some(u.to_string());
        } else if let Some(p) = line.strip_prefix("password: ") {
            password = Some(p.to_string());
        }
    }
    Some((username?, password?))
}