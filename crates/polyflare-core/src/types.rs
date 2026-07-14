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
#[derive(Debug, Clone)]
pub struct Account {
    pub id: String,
    pub base_url: String,
    pub bearer_token: String,
}

/// Per-request context threaded through selection/continuity. Minimal in M1.
#[derive(Debug, Clone, Default)]
pub struct RequestCtx {
    pub session_id: Option<String>,
}
