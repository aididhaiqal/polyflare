//! Application state and router construction.

use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::DefaultBodyLimit;
use axum::routing::{get, patch, post};
use axum::Router;

use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexVersionCache;
use polyflare_core::{Continuity, Executor, Provider, Selector};

use crate::account_cache::AccountCache;
use polyflare_store::{Store, TokenCipher};

use crate::ingress::{
    messages_handler, pooled_messages_handler, pooled_responses_handler, responses_handler,
};
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
    /// The DEFAULT (global) routing selector — used for the bare paths and for any pool without an
    /// explicit override in `pool_selectors`.
    pub selector: Arc<dyn Selector>,
    /// Per-pool routing-strategy overrides (pool slug → selector). Empty by default; populated from
    /// `POLYFLARE_POOL_STRATEGY`. A pool absent here routes with `selector`.
    pub pool_selectors: std::collections::HashMap<String, Arc<dyn Selector>>,
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
    /// In-memory cache of resolved (account row + decrypted tokens) keyed by account id (see
    /// `crate::token_cache`): keeps the per-request account read+decrypt off SQLite. TTL'd +
    /// invalidated on the store account-generation bump; tokens are zeroized on eviction.
    pub token_cache: Arc<crate::token_cache::TokenCache>,
    /// Live per-account routing runtime state (see `crate::runtime_state`): failure-driven
    /// `error_count`/`cooldown_until`/`last_error_at`/`last_selected_at` overlaid onto snapshots at
    /// selection time. In-memory only (churns per request); resets on restart.
    pub runtime: Arc<crate::runtime_state::RuntimeStates>,
    /// Admin token gating every `/api/*` dashboard route (see `crate::auth::require_admin`), from
    /// `POLYFLARE_ADMIN_TOKEN`. `None` ⇒ the dashboard API is disabled (503), not silently open.
    pub admin_token: Option<String>,
    /// Enables the live log stream (`POLYFLARE_LIVE_LOGS`). Consumed by a later task
    /// (`/api/logs/stream`); present now so `AppState` construction doesn't churn again then.
    pub live_logs: bool,
    /// Content-free live-log bus (see `crate::log_bus`): a broadcast channel + ring buffer fed
    /// from the `RequestLog` content-safety chokepoint (`crate::ingress`'s route wrappers). A
    /// later task exposes this over `/api/logs/stream`; present now so publishing starts
    /// immediately at the chokepoint instead of only once the SSE endpoint lands.
    pub log_bus: std::sync::Arc<crate::log_bus::LogBus>,
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

    /// The routing selector for a request narrowed to `pool`: the pool's configured strategy
    /// override if any, else the global default. The bare paths pass `None` ⇒ the default.
    pub fn selector_for(&self, pool: Option<&str>) -> &Arc<dyn Selector> {
        pool.and_then(|p| self.pool_selectors.get(p))
            .unwrap_or(&self.selector)
    }
}

pub fn build_app(state: Arc<AppState>) -> Router {
    // The dashboard API: every route here sits behind `require_admin` (POLYFLARE_ADMIN_TOKEN via
    // `Authorization: Bearer <token>`; unset ⇒ 503). Register only routes whose handlers exist
    // today — later tasks add their own route lines to this same auth-gated router.
    let api = Router::new()
        .route("/api/whoami", get(crate::auth::whoami_handler))
        .route("/api/capabilities", get(crate::auth::capabilities_handler))
        .route("/api/pools", get(crate::read_api::pools_handler))
        .route("/api/accounts", get(crate::read_api::accounts_handler))
        .route(
            "/api/accounts/{id}",
            patch(crate::write_api::patch_account_handler),
        )
        .route("/api/requests", get(crate::read_api::requests_handler))
        .route("/api/overview", get(crate::read_api::overview_handler))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::auth::require_admin,
        ));

    Router::new()
        .route("/responses", post(responses_handler))
        .route("/v1/messages", post(messages_handler))
        // Pooled variants: `/{pool}/…` narrows selection to accounts tagged with that pool slug
        // (see `filter_by_pool`). The bare paths above keep selecting over ALL accounts. The
        // `{pool}` segment is a param, so it never shadows the literal single-segment `/responses`
        // or the `/v1/*` / `/models` routes — matchit prefers a static segment over a param one.
        .route("/{pool}/responses", post(pooled_responses_handler))
        .route("/{pool}/v1/messages", post(pooled_messages_handler))
        // Model catalog (read-only GETs): real Codex models (bootstrap floor for now) merged with
        // PolyFlare's synthetic aliases. Routing is by method+path, so these never conflict with
        // the `/v1/*` POSTs above.
        .route("/models", get(crate::catalog::codex_models_handler))
        .route(
            "/backend-api/codex/models",
            get(crate::catalog::codex_models_handler),
        )
        .route("/v1/models", get(crate::catalog::v1_models_handler))
        // Auth-gated dashboard API (see `api` above): pools/accounts/requests/overview reads,
        // the account-settings patch, and `/api/whoami`.
        .merge(api)
        // Embedded dashboard UI (see `crate::dashboard`). `/dashboard` serves the SPA entrypoint;
        // `/dashboard/{*path}` serves its bundle assets (with SPA fallback to index.html).
        // Unauthenticated: it's a static asset bundle — the API calls it makes are what's gated.
        .route("/dashboard", get(crate::dashboard::dashboard_index))
        .route("/dashboard/", get(crate::dashboard::dashboard_index))
        .route("/dashboard/{*path}", get(crate::dashboard::dashboard_asset))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
        .with_state(state)
}
