//! Core value types threaded through the request path.

use std::pin::Pin;

use bytes::Bytes;
use futures_core::Stream;

/// A request prepared for a specific backend. In M1 this is a thin wrapper over the
/// raw request JSON plus the target model; continuity/translation enrich it later.
#[derive(Debug, Clone)]
pub struct PreparedRequest {
    pub body: serde_json::Value,
    pub model: String,
}

/// Errors an executor can surface.
#[derive(Debug, thiserror::Error)]
pub enum ExecError {
    #[error("upstream request failed: {0}")]
    Upstream(String),
    #[error("stream error: {0}")]
    Stream(String),
}

/// A non-buffering streaming response body: pinned, boxed, `Send` stream of byte chunks.
pub type ResponseStream = Pin<Box<dyn Stream<Item = Result<Bytes, ExecError>> + Send>>;

/// A credential/endpoint an executor uses to reach an upstream. M1 = single account from config.
#[derive(Clone)]
pub struct Account {
    pub id: String,
    pub base_url: String,
    pub bearer_token: String,
}

// `bearer_token` is a secret and must never be printed in clear via `{:?}`.
impl std::fmt::Debug for Account {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Account")
            .field("id", &self.id)
            .field("base_url", &self.base_url)
            .field("bearer_token", &"***")
            .finish()
    }
}

/// Per-request context threaded through selection/continuity. Minimal in M1.
#[derive(Debug, Clone, Default)]
pub struct RequestCtx {
    pub session_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_debug_redacts_bearer_token() {
        let account = Account {
            id: "acct-1".into(),
            base_url: "https://example.test".into(),
            bearer_token: "super-secret-token-value".into(),
        };

        let debug_output = format!("{account:?}");

        assert!(
            !debug_output.contains("super-secret-token-value"),
            "Debug output must never contain the raw bearer token: {debug_output}"
        );
        assert!(
            debug_output.contains("***"),
            "Debug output must contain the redaction marker: {debug_output}"
        );
        assert!(
            debug_output.contains("acct-1"),
            "Debug output must still contain the id: {debug_output}"
        );
        assert!(
            debug_output.contains("https://example.test"),
            "Debug output must still contain the base_url: {debug_output}"
        );
    }
}
