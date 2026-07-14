//! Application state and router construction.

use std::sync::Arc;

use axum::extract::DefaultBodyLimit;
use axum::routing::post;
use axum::Router;

use polyflare_codex::oauth::OAuthClient;
use polyflare_core::{Executor, Selector};
use polyflare_store::{Store, TokenCipher};

use crate::ingress::responses_handler;

/// Raised request-body limit: axum's `Json` extractor default (2 MB) 413s real
/// OpenAI-Responses requests. 100 MB is generous for real Codex turns while bounded.
const MAX_REQUEST_BODY_BYTES: usize = 100 * 1024 * 1024;

/// Shared server state: the executor, the account selector, the store + at-rest cipher, the
/// OAuth refresher, and the shared upstream base URL. Wrapped in `Arc` by the caller.
pub struct AppState {
    pub executor: Arc<dyn Executor>,
    pub selector: Arc<dyn Selector>,
    pub store: Store,
    pub cipher: TokenCipher,
    pub oauth: OAuthClient,
    pub upstream_base_url: String,
}

pub fn build_app(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/responses", post(responses_handler))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
        .with_state(state)
}
