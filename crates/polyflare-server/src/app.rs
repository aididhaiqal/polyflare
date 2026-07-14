//! Application state and router construction.

use std::sync::Arc;

use axum::extract::DefaultBodyLimit;
use axum::routing::post;
use axum::Router;

use polyflare_core::{Account, Executor};

use crate::ingress::responses_handler;

/// Raised request-body limit: axum's `Json` extractor default (2 MB) 413s real
/// OpenAI-Responses requests from long conversations / file reads. 100 MB is generous
/// for real Codex turns while still bounded against unbounded buffering.
/// M2 should revisit this — ideally stream the request body instead of buffering it.
const MAX_REQUEST_BODY_BYTES: usize = 100 * 1024 * 1024;

pub struct AppState {
    pub executor: Arc<dyn Executor>,
    pub account: Account,
}

pub fn build_app(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/responses", post(responses_handler))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
        .with_state(state)
}
