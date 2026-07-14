//! Application state and router construction.

use std::sync::Arc;

use axum::extract::DefaultBodyLimit;
use axum::routing::post;
use axum::Router;

use polyflare_codex::oauth::OAuthClient;
use polyflare_core::{Continuity, Executor, Provider, Selector};
use polyflare_store::{Store, TokenCipher};

use crate::ingress::{messages_handler, responses_handler};

/// Raised request-body limit: axum's `Json` extractor default (2 MB) 413s real
/// OpenAI-Responses requests. 100 MB is generous for real Codex turns while bounded.
const MAX_REQUEST_BODY_BYTES: usize = 100 * 1024 * 1024;

/// Shared server state: the per-provider executors, the account selector, the continuity engine,
/// the store + at-rest cipher, the OAuth refresher, and the per-provider upstream base URLs.
/// Wrapped in `Arc` by the caller.
pub struct AppState {
    pub codex_executor: Arc<dyn Executor>,
    pub anthropic_executor: Arc<dyn Executor>,
    pub selector: Arc<dyn Selector>,
    pub continuity: Arc<dyn Continuity>,
    pub store: Store,
    pub cipher: TokenCipher,
    pub oauth: OAuthClient,
    pub upstream_base_url: String,
    pub anthropic_upstream_base_url: String,
}

impl AppState {
    /// The executor that serves `provider`'s pool.
    pub fn executor_for(&self, provider: Provider) -> &Arc<dyn Executor> {
        match provider {
            Provider::Codex => &self.codex_executor,
            Provider::Anthropic => &self.anthropic_executor,
        }
    }

    /// The upstream base URL for `provider`'s pool.
    pub fn upstream_base_url_for(&self, provider: Provider) -> &str {
        match provider {
            Provider::Codex => &self.upstream_base_url,
            Provider::Anthropic => &self.anthropic_upstream_base_url,
        }
    }
}

pub fn build_app(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/responses", post(responses_handler))
        .route("/v1/messages", post(messages_handler))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
        .with_state(state)
}
