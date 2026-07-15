//! `polyflare login`: the Codex OAuth **authorization_code + PKCE** onboarding flow (native account
//! registration, the alternative to importing from codex-lb).
//!
//! # Headless by design
//! The OpenAI app registration pins the redirect to `http://localhost:1455/auth/callback`, so the
//! browser's redirect only reaches PolyFlare when they're on the same machine. This flow offers
//! BOTH paths at once and races them:
//! - a best-effort loopback listener on `127.0.0.1:1455` catches the redirect automatically (local);
//! - OR you paste the full redirected URL from your browser's address bar (remote/headless — the
//!   same trick codex-lb's `manual_callback` and CLIProxyAPI's paste fallback use).
//!
//! The authorize URL is always printed (never auto-opened unless `open_browser`), so this works
//! under launchd / over SSH with no local browser. `state` is validated (CSRF) before the exchange.
//!
//! The reusable primitives here — the listener, `parse_callback`, and (in `oauth`) PKCE/state gen —
//! are deliberately free functions, not baked into a Codex struct, so a second provider can lift
//! them without inheriting Codex specifics.

use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

use crate::oauth::{
    generate_pkce, generate_state, OAuthClient, OAuthError, Refreshed, CALLBACK_PORT, REDIRECT_URI,
};

/// How long to wait for the callback (or a pasted URL) before giving up.
const LOGIN_TIMEOUT: Duration = Duration::from_secs(600);

#[derive(Debug, thiserror::Error)]
pub enum LoginError {
    #[error("oauth error: {0}")]
    OAuth(#[from] OAuthError),
    #[error("login timed out waiting for the browser redirect")]
    Timeout,
    #[error("state mismatch on the OAuth callback — possible CSRF; aborting")]
    StateMismatch,
    #[error("io error: {0}")]
    Io(String),
}

/// Run the interactive login and return the freshly obtained tokens + identity claims. Prints the
/// authorize URL to stdout; opens a browser only when `open_browser`.
pub async fn run_login(oauth: &OAuthClient, open_browser: bool) -> Result<Refreshed, LoginError> {
    let (verifier, challenge) = generate_pkce();
    let state = generate_state();
    let authorize_url = oauth.build_authorize_url(&state, &challenge);

    // Best-effort local callback listener; a busy/unavailable port just means "use the paste path".
    let listener = TcpListener::bind(("127.0.0.1", CALLBACK_PORT)).await.ok();

    println!("\nTo authenticate, open this URL in your browser:\n\n  {authorize_url}\n");
    if open_browser {
        open_url(&authorize_url);
    }
    if listener.is_none() {
        println!("(Local port {CALLBACK_PORT} is unavailable — use the paste option below.)");
    }
    println!(
        "If PolyFlare is running on a remote/headless host, that redirect can't reach it. After \
         authorizing,\ncopy the FULL redirected URL from your browser's address bar and paste it \
         here, then press Enter.\n(Local users can ignore this — the callback is caught \
         automatically. Tip: `ssh -L {CALLBACK_PORT}:127.0.0.1:{CALLBACK_PORT} <host>` also works.)\n"
    );

    // Race: the local callback, a pasted URL, and a timeout — whichever fires first wins.
    let (code, returned_state) = tokio::select! {
        r = wait_for_callback(listener.as_ref()) => r?,
        r = read_pasted_callback() => r?,
        _ = tokio::time::sleep(LOGIN_TIMEOUT) => return Err(LoginError::Timeout),
    };

    if returned_state != state {
        return Err(LoginError::StateMismatch);
    }
    Ok(oauth.exchange_code(&code, &verifier, REDIRECT_URI).await?)
}

/// Accept connections on the loopback listener until one carries `code`+`state`, replying with a
/// tiny success page. With no listener (bind failed) this never resolves, ceding to the paste path.
async fn wait_for_callback(listener: Option<&TcpListener>) -> Result<(String, String), LoginError> {
    let listener = match listener {
        Some(l) => l,
        None => return std::future::pending().await,
    };
    loop {
        let (mut stream, _) = listener
            .accept()
            .await
            .map_err(|e| LoginError::Io(e.to_string()))?;

        // Read just the request line: `GET /auth/callback?code=..&state=.. HTTP/1.1`.
        let mut request_line = String::new();
        {
            let mut reader = BufReader::new(&mut stream);
            reader
                .read_line(&mut request_line)
                .await
                .map_err(|e| LoginError::Io(e.to_string()))?;
        }

        // Always reply 200 so the browser shows something, then close.
        let page = "<html><body style=\"font-family:sans-serif\"><h2>PolyFlare: \
                    authentication received.</h2><p>You can close this tab.</p></body></html>";
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            page.len(),
            page
        );
        let _ = stream.write_all(response.as_bytes()).await;
        let _ = stream.flush().await;

        if let Some(path) = request_line.split_whitespace().nth(1) {
            if let Some(pair) = parse_callback(path) {
                return Ok(pair);
            }
        }
        // A stray request (e.g. favicon) — keep waiting for the real callback.
    }
}

/// Read lines from stdin until one parses to `code`+`state`. On EOF (no tty) this never resolves,
/// ceding to the local callback / timeout.
async fn read_pasted_callback() -> Result<(String, String), LoginError> {
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    loop {
        match lines
            .next_line()
            .await
            .map_err(|e| LoginError::Io(e.to_string()))?
        {
            Some(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                if let Some(pair) = parse_callback(line) {
                    return Ok(pair);
                }
                eprintln!(
                    "Couldn't find code/state in that input — paste the FULL redirected URL:"
                );
            }
            None => return std::future::pending().await,
        }
    }
}

/// Extract `(code, state)` from a full callback URL OR a request path (`/auth/callback?...`).
fn parse_callback(input: &str) -> Option<(String, String)> {
    let url_str = if input.contains("://") {
        input.to_string()
    } else if let Some(rest) = input.strip_prefix('/') {
        format!("http://localhost/{rest}")
    } else {
        format!("http://localhost/?{input}")
    };
    let url = reqwest::Url::parse(&url_str).ok()?;
    let mut code = None;
    let mut state = None;
    for (k, v) in url.query_pairs() {
        match k.as_ref() {
            "code" => code = Some(v.into_owned()),
            "state" => state = Some(v.into_owned()),
            _ => {}
        }
    }
    Some((code?, state?))
}

/// Best-effort browser launch (only when `--open` is passed). Never fails the login.
fn open_url(url: &str) {
    #[cfg(target_os = "macos")]
    let _ = std::process::Command::new("open").arg(url).spawn();
    #[cfg(all(unix, not(target_os = "macos")))]
    let _ = std::process::Command::new("xdg-open").arg(url).spawn();
    #[cfg(target_os = "windows")]
    let _ = std::process::Command::new("cmd")
        .args(["/C", "start", "", url])
        .spawn();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_callback_from_full_url() {
        let (code, state) =
            parse_callback("http://localhost:1455/auth/callback?code=abc123&state=xyz789").unwrap();
        assert_eq!(code, "abc123");
        assert_eq!(state, "xyz789");
    }

    #[test]
    fn parse_callback_from_request_path() {
        let (code, state) = parse_callback("/auth/callback?state=s1&code=c1").unwrap();
        assert_eq!(code, "c1");
        assert_eq!(state, "s1");
    }

    #[test]
    fn parse_callback_url_decodes_values() {
        // A `code` that arrived percent-encoded must come back decoded.
        let (code, _) = parse_callback("/auth/callback?code=a%2Bb%2Fc&state=s").unwrap();
        assert_eq!(code, "a+b/c");
    }

    #[test]
    fn parse_callback_rejects_missing_params() {
        assert!(parse_callback("/auth/callback?code=only").is_none());
        assert!(parse_callback("/auth/callback?state=only").is_none());
        assert!(parse_callback("not a url at all").is_none());
    }
}
