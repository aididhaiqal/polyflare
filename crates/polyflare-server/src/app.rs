//! Application state and router construction.

use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post};
use axum::Router;

use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexVersionCache;
use polyflare_core::{Continuity, Executor, Provider, Selector};

use crate::account_cache::AccountCache;
use polyflare_store::{Store, TokenCipher};

use crate::ingress::{messages_handler, responses_handler};
use crate::refresh_locks::RefreshLocks;

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
    /// Per-account OAuth refresh singleflight coordination (F2): serializes concurrent
    /// refresh-if-stale attempts for the SAME account so only one call reaches the OAuth
    /// endpoint with a given refresh token.
    pub refresh_locks: RefreshLocks,
    /// M5 capture-fixture mechanism (`POLYFLARE_CAPTURE_FINGERPRINT`; see
    /// `crate::fingerprint_capture`): when `Some`, the ingress appends every request's
    /// content-safe structural HTTP fingerprint to this path. `None` (the default) disables it
    /// entirely — the ingress never calls into `fingerprint_capture` at all.
    pub capture_fingerprint_path: Option<PathBuf>,
    /// Resolves the live `codex-rs` release version for the SYNTHESIZED (translated) egress
    /// User-Agent, so it tracks the real fleet instead of a hardcoded constant. Read on the hot
    /// path via `cached_or_fallback()` (sync, no I/O); warmed by a background task in `serve`.
    pub codex_version: Arc<CodexVersionCache>,
    /// In-memory cache of the selector-input account snapshots (see `crate::account_cache`): serves
    /// selection from memory instead of re-running the O(accounts) store query per request.
    /// Invalidated on account-state writes (status/tokens/add).
    pub account_cache: Arc<AccountCache>,
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
        // Model catalog (read-only GETs): real Codex models (bootstrap floor for now) merged with
        // PolyFlare's synthetic aliases. Routing is by method+path, so these never conflict with
        // the `/v1/*` POSTs above.
        .route("/models", get(crate::catalog::codex_models_handler))
        .route(
            "/backend-api/codex/models",
            get(crate::catalog::codex_models_handler),
        )
        .route("/v1/models", get(crate::catalog::v1_models_handler))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
        .with_state(state)
}
