use rmcp::{ServiceExt, model::CallToolRequestParams};
use std::fmt;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;

/// Longest the credential helper is given, start to finish. A hung helper must
/// not hold a tool call open, nor the master password in its memory.
const HELPER_TIMEOUT: Duration = Duration::from_secs(20);

/// Why a credential could not be had. None of these carry the password, the
/// username, or anything the helper printed: the message is safe to show.
#[derive(Debug)]
pub enum CredentialError {
    /// The helper program could not be started.
    Unavailable(String),
    /// It started but did not speak the protocol, or died mid-conversation.
    Protocol,
    /// It answered, and has nothing stored for this site.
    NotFound,
    TimedOut,
}

impl CredentialError {
    pub fn code(&self) -> &'static str {
        match self {
            CredentialError::Unavailable(_) => "credentials_unavailable",
            CredentialError::Protocol => "credentials_protocol",
            CredentialError::NotFound => "credentials_not_found",
            CredentialError::TimedOut => "credentials_timeout",
        }
    }
}

impl fmt::Display for CredentialError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CredentialError::Unavailable(path) => write!(f, "could not start the credential helper `{path}`"),
            CredentialError::Protocol => write!(f, "the credential helper did not answer correctly"),
            CredentialError::NotFound => write!(f, "no credentials are stored for this site"),
            CredentialError::TimedOut => write!(f, "the credential helper did not answer in time"),
        }
    }
}

pub async fn get_credential(
    site: &str,
    master_password: &str,
) -> Result<(String, String), CredentialError> {
    tokio::time::timeout(HELPER_TIMEOUT, ask(site, master_password))
        .await
        .map_err(|_| CredentialError::TimedOut)?
}

async fn ask(site: &str, master_password: &str) -> Result<(String, String), CredentialError> {
    let path = std::env::var("PASSMANAGER_PATH").unwrap_or_else(|_| "passmanager".to_string());

    // `kill_on_drop`: however this function ends — an answer, an error, the
    // timeout dropping the future — the helper does not outlive it.
    let mut child = Command::new(&path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|_| CredentialError::Unavailable(path.clone()))?;

    let stdin = child.stdin.take().ok_or(CredentialError::Protocol)?;
    let stdout = child.stdout.take().ok_or(CredentialError::Protocol)?;

    let client = ().serve((stdout, stdin)).await.map_err(|_| CredentialError::Protocol)?;

    let arguments = serde_json::json!({
        "site": site,
        "master_password": master_password
    })
    .as_object()
    .cloned()
    .ok_or(CredentialError::Protocol)?;

    let result = client
        .call_tool(CallToolRequestParams::new("get_credential").with_arguments(arguments))
        .await
        .map_err(|_| CredentialError::Protocol)?;

    let text = result
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.clone())
        .ok_or(CredentialError::Protocol)?;

    let mut username = None;
    let mut password = None;
    for line in text.lines() {
        if let Some(u) = line.strip_prefix("username: ") {
            username = Some(u.to_string());
        } else if let Some(p) = line.strip_prefix("password: ") {
            password = Some(p.to_string());
        }
    }
    match (username, password) {
        (Some(u), Some(p)) => Ok((u, p)),
        _ => Err(CredentialError::NotFound),
    }
}
