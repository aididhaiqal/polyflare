//! Anthropic subscription usage and limits, read from the free `GET /api/oauth/usage` endpoint the
//! Claude Code CLI itself polls. Authenticated with the account's own OAuth bearer plus the
//! `anthropic-beta: oauth-2025-04-20` header — the same grant that serves inference, so no wider
//! scope is required to READ usage (whether it is required is exactly what the probe confirms).
//!
//! This first cut returns the raw JSON body. Typed parsing, a poll loop, and dashboard surfacing
//! build on top of it once the live payload's real shape (which windows, whether a plan name is
//! present) is known rather than assumed from better-ccflare's types.

use std::time::Duration;

use polyflare_store::{Store, TokenCipher};

/// The `anthropic-beta` token the OAuth usage endpoint expects. Mirrors
/// `claude_wire::OAUTH_BETA`; kept as a local constant so this module has no cross-crate coupling.
const OAUTH_BETA: &str = "oauth-2025-04-20";

/// A recent Claude Code CLI version. The usage endpoint is CLI-facing and may gate on a
/// `claude-code/<semver>` User-Agent, so a real one is sent rather than a generic agent.
const CLAUDE_CODE_UA: &str = "claude-code/2.1.209";

/// The OAuth endpoints sit at the API root (`/api/oauth/...`), not under any versioned path.
fn oauth_url(upstream_base: &str, path: &str) -> String {
    format!("{}/api/oauth/{}", upstream_base.trim_end_matches('/'), path)
}

/// Fetch one account's usage payload. Returns `(http_status, body_text)`; the caller decides how to
/// present or parse it. The access token is decrypted only for the duration of the call.
pub async fn fetch_usage_raw(
    store: &Store,
    cipher: &TokenCipher,
    upstream_base: &str,
    account_id: &str,
) -> Result<(u16, String), Box<dyn std::error::Error>> {
    fetch_oauth_raw(store, cipher, upstream_base, account_id, "usage").await
}

/// Fetch a raw `GET /api/oauth/{path}` body for an account, authenticated with its OAuth bearer and
/// the oauth beta header. Used both for `usage` and for probing whether a profile endpoint exists.
pub async fn fetch_oauth_raw(
    store: &Store,
    cipher: &TokenCipher,
    upstream_base: &str,
    account_id: &str,
    path: &str,
) -> Result<(u16, String), Box<dyn std::error::Error>> {
    let tokens = store
        .accounts()
        .decrypt_tokens(account_id, cipher)
        .await?
        .ok_or("account not found, or it has no stored tokens")?;
    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .build()?;
    let resp = http
        .get(oauth_url(upstream_base, path))
        .header("Authorization", format!("Bearer {}", tokens.access_token))
        .header("anthropic-beta", OAUTH_BETA)
        .header("User-Agent", CLAUDE_CODE_UA)
        .header("Accept", "application/json")
        .header("Content-Type", "application/json")
        .send()
        .await?;
    let status = resp.status().as_u16();
    let body = resp.text().await?;
    Ok((status, body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oauth_url_is_rooted_and_trims_a_trailing_slash() {
        assert_eq!(
            oauth_url("https://api.anthropic.com", "usage"),
            "https://api.anthropic.com/api/oauth/usage"
        );
        assert_eq!(
            oauth_url("https://api.anthropic.com/", "profile"),
            "https://api.anthropic.com/api/oauth/profile"
        );
    }
}
