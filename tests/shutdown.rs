//! Does the server leave anything running when it stops?
//!
//! Drives the real binary over its real stdio. It starts a headless browser,
//! stops the server three ways — SIGTERM, SIGINT, and the client hanging up —
//! and checks each time that the process exits promptly, that no Chrome
//! belonging to it is left, and that its profile directory is gone.
//!
//! This was written after the first version of the check found that SIGTERM
//! left the server hung and Chrome running. Needs Chrome and a Unix `pgrep`:
//!
//!     cargo test --release --test shutdown -- --ignored

#![cfg(unix)]

use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn rpc(
    stdin: &mut std::process::ChildStdin,
    out: &mut BufReader<std::process::ChildStdout>,
    id: u32,
    method: &str,
    params: serde_json::Value,
) -> serde_json::Value {
    let msg = serde_json::json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    writeln!(stdin, "{msg}").unwrap();
    stdin.flush().unwrap();
    loop {
        let mut line = String::new();
        assert!(out.read_line(&mut line).unwrap() > 0, "server closed its output");
        let value: serde_json::Value = serde_json::from_str(&line).unwrap();
        if value["id"] == id {
            return value;
        }
    }
}

/// Chrome processes launched with this server's private profile directory.
fn browsers_of(server_pid: u32) -> usize {
    let out = Command::new("pgrep")
        .args(["-f", &format!("mcpcrawler-{server_pid}")])
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout).split_whitespace().count()
}

/// A page to render, served from loopback for as long as the test runs.
fn serve_a_page() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut stream = stream;
            let body = "<html><body><main>a page to render</main></body></html>";
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
    port
}

fn stop_by(how: &str) {
    let port = serve_a_page();
    let mut child = Command::new(env!("CARGO_BIN_EXE_mcpcrawler"))
        .env("MCPCRAWLER_ALLOW_PRIVATE_NETWORKS", "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let pid = child.id();
    let mut stdin = child.stdin.take().unwrap();
    let mut out = BufReader::new(child.stdout.take().unwrap());

    rpc(&mut stdin, &mut out, 1, "initialize", serde_json::json!({
        "protocolVersion": "2025-06-18", "capabilities": {},
        "clientInfo": { "name": "shutdown-test", "version": "0" },
    }));
    writeln!(stdin, r#"{{"jsonrpc":"2.0","method":"notifications/initialized"}}"#).unwrap();
    // Open a real browser.
    let reply = rpc(&mut stdin, &mut out, 2, "tools/call", serde_json::json!({
        "name": "crawl_urls",
        "arguments": { "urls": [format!("http://127.0.0.1:{port}/")], "render": "always", "max_pages": 1 },
    }));
    assert_eq!(reply["result"]["isError"], false, "{reply}");

    let profile = std::env::temp_dir().join(format!("mcpcrawler-{pid}"));
    assert!(browsers_of(pid) > 0, "no browser was started");
    assert!(profile.is_dir());

    let started = Instant::now();
    match how {
        "SIGTERM" => assert!(Command::new("kill").args(["-TERM", &pid.to_string()]).status().unwrap().success()),
        "SIGINT" => assert!(Command::new("kill").args(["-INT", &pid.to_string()]).status().unwrap().success()),
        _ => drop(stdin), // the client hangs up
    }
    // Poll for the exit rather than wait forever: a hang is the failure.
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        assert!(started.elapsed() < Duration::from_secs(20), "{how}: still running after 20s");
        std::thread::sleep(Duration::from_millis(100));
    };
    assert!(status.success(), "{how}: exited with {status}");

    std::thread::sleep(Duration::from_millis(800));
    assert_eq!(browsers_of(pid), 0, "{how}: a browser was left running");
    assert!(!profile.exists(), "{how}: the profile directory was left behind");
}

#[test]
#[ignore = "needs Chrome; run with --ignored"]
fn stopping_leaves_no_browser_and_no_profile() {
    for how in ["SIGTERM", "SIGINT", "disconnect"] {
        stop_by(how);
    }
}
