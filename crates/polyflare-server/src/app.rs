//! Application state and router construction.

use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::DefaultBodyLimit;
use axum::routing::{any, get, patch, post};
use axum::Router;

use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::{CodexExecutor, CodexVersionCache, CodexWsExecutor};
use polyflare_core::{Continuity, ExecError, Executor, Provider, Selector};

use crate::account_cache::AccountCache;
use crate::runtime_settings::RuntimeSettings;
use polyflare_store::{Store, TokenCipher};

use crate::ingress::{
    messages_handler, pooled_messages_handler, pooled_responses_handler, responses_handler,
    websocket_fallback_handler,
};
use crate::refresh_locks::RefreshLocks;

/// Raised request-body limit: axum's `Json` extractor default (2 MB) 413s real
/// OpenAI-Responses requests. 100 MB is generous for real Codex turns while bounded.
pub(crate) const MAX_REQUEST_BODY_BYTES: usize = 100 * 1024 * 1024;

/// Shared server state: the per-provider executors, the account selector, the continuity engine,
/// the store + at-rest cipher, the OAuth refresher, and the per-provider upstream base URLs.
/// Wrapped in `Arc` by the caller.
pub struct AppState {
    pub codex_executor: Arc<dyn Executor>,
    pub anthropic_executor: Arc<dyn Executor>,
    /// Shared `reqwest::Client` for Codex control forwarding and ChatGPT backend passthrough.
    /// Production startup clones this same Arc-backed client into `CodexExecutor`, so all three
    /// paths share its connection pool and restricted Cloudflare affinity-cookie store.
    pub control_client: reqwest::Client,
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
    /// Optional token gating every `/api/*` dashboard route (see `crate::auth::require_admin`).
    /// `None` is opened only when the production builder resolves a loopback listener bind;
    /// otherwise the dashboard API remains disabled (503).
    pub admin_token: Option<String>,
    /// Live-editable Settings subsystem Task 4: the atomic holder for the 10 formerly-static
    /// `AppState` fields this task REMOVES from this struct — `max_account_attempts`,
    /// `starvation_wait_budget`, `starvation_heartbeat`, `wake_jitter_ms`, `stream_idle_timeout`,
    /// `inflight_penalty_pct`, `soft_drain_enabled`, `request_log_retention_days`,
    /// `usage_history_retention_days`, `live_logs`. Seeded once at startup from `ServeConfig`
    /// (`RuntimeSettings::new`), then overlaid with any persisted `settings` table rows
    /// (`crate::runtime_settings::overlay_persisted_settings`) before this `AppState` is built —
    /// see `main.rs::serve`. Every hot-path read that used to be `state.<field>` is now
    /// `state.runtime_settings.<field>()` (an `Ordering::Relaxed` atomic load, negligible cost —
    /// see `crate::runtime_settings`'s module doc). Live-mutable after startup via the (later)
    /// settings PATCH endpoint's `RuntimeSettings::set`, which re-validates through the SAME
    /// `clamp_<field>` fns the boot path uses.
    pub runtime_settings: Arc<RuntimeSettings>,
    /// Selects the client-facing WebSocket transport. Resolved once at startup from the persisted
    /// `client_websocket_enabled` setting (with an environment/default bootstrap). Read only in
    /// `build_app` below to shape the router: when `true`,
    /// the WS-handshake `GET /responses` (+ `/{pool}/responses`) routes to
    /// `crate::ws_relay::responses_ws_handler` (which ACCEPTS the upgrade); when explicitly
    /// disabled, it answers `426` via `crate::ingress::websocket_fallback_handler`. Production
    /// configuration defaults this field to `true`; focused test harnesses may still construct
    /// `false` directly when exercising the HTTP fallback.
    pub ws_downstream: bool,
    /// Between-turns relay idle policy (honest-liveness work, 2026-07-24; see
    /// `crate::ws_relay::WsRelayIdlePolicy`): keepalive ping cadence + idle budget for a parked
    /// upstream socket. Resolved once at startup from `websocket_idle_ping_secs` and
    /// `websocket_idle_budget_secs`; read only at
    /// pump start (`crate::ws_relay::pump::run_pump`), never per-frame.
    pub ws_relay_idle: crate::ws_relay::WsRelayIdlePolicy,
    /// Content-free live-log bus (see `crate::log_bus`): a broadcast channel + ring buffer fed
    /// from the `RequestLog` content-safety chokepoint (`crate::ingress`'s route wrappers). A
    /// later task exposes this over `/api/logs/stream`; present now so publishing starts
    /// immediately at the chokepoint instead of only once the SSE endpoint lands.
    pub log_bus: std::sync::Arc<crate::log_bus::LogBus>,
    /// B4/B5 Task 5: content-free counter of cross-account failover events (see
    /// `crate::observability::FailoverMetrics`) — incremented from
    /// `crate::ingress::run_failover_loop` at the same site that emits the
    /// `crate::observability::FailoverSignal` log/event.
    pub failover_metrics: std::sync::Arc<crate::observability::FailoverMetrics>,
    /// B8 Task 4: content-free counter of health-tier soft-drain TRANSITIONS (see
    /// `crate::observability::HealthTierMetrics`) — incremented at the same sites that emit the
    /// `crate::observability::HealthTierSignal`: `crate::ingress::record_failure` (pre-stream
    /// failures) and `crate::usage_refresh`'s poller loop (the codex usage-driven pass and, since
    /// the B8-review Finding 1 fix, the disjoint non-codex error-driven pass). Does NOT cover
    /// mid-stream watchdog-funnel transitions — see `HealthTierMetrics`'s doc for that known,
    /// accepted scope gap. In-memory only; resets on restart.
    pub health_tier_metrics: std::sync::Arc<crate::observability::HealthTierMetrics>,
    /// B5 Task 5: content-free counter of Layer 2 keepalive-wait terminal outcomes (see
    /// `crate::observability::StarvationMetrics`) — incremented from
    /// `crate::ingress::layer2_wait_stream` at the same site that emits the
    /// `crate::observability::StarvationSignal` log/event.
    pub starvation_metrics: std::sync::Arc<crate::observability::StarvationMetrics>,
    /// D18 Task 4: whether the proxy surface (`POST /responses`, `/v1/messages`,
    /// `/{pool}/responses`, `/{pool}/v1/messages`) requires a valid client API key
    /// (`crate::auth::require_client_key`, `route_layer`'d onto the extracted `proxy` sub-router
    /// in `build_app` below). Resolved ONCE at startup by the bind-address-aware posture
    /// (`crate::posture::resolve_proxy_enforcement`) — NEVER re-evaluated per request. `false` is
    /// the correct default for every existing test/dev harness that builds `AppState` directly
    /// without going through `resolve_proxy_enforcement` (a loopback-bind, no-keys posture).
    pub enforce_client_keys: bool,
    /// C9 Task 4: content-free counter of in-flight lease acquire/release events (see
    /// `crate::observability::LeaseMetrics`) — bumped from `crate::runtime_state::RuntimeStates::
    /// acquire_in_flight` (acquired) and `crate::runtime_state::InFlightGuard`'s `Drop` (released),
    /// via a handle threaded into `acquire_in_flight`'s call sites (`crate::ingress`) and stored on
    /// the returned guard so the release bump fires wherever/however the guard is dropped —
    /// disconnect, drain, error, timeout, panic, or failover reselect, mirroring the leak-proof
    /// release guarantee Task 1-2 already established for `in_flight` itself. In-memory only (like
    /// `FailoverMetrics`/`StarvationMetrics`/`HealthTierMetrics`); resets on restart.
    pub lease_metrics: std::sync::Arc<crate::observability::LeaseMetrics>,
    /// C11b Task 1: content-free counter of completed proxied requests, labeled by
    /// `(account_id, status)` (see `crate::observability::UpstreamRequestMetrics`). Bumped once
    /// per client request at each of the 3 request-completion wrapper sites (`control_route`/
    /// `responses_route`/`messages_route`). In-memory only; resets on restart.
    pub upstream_request_metrics: std::sync::Arc<crate::observability::UpstreamRequestMetrics>,
    /// C11b Task 1: content-free counter of 429 rate-limit writebacks, labeled by a fixed `type`
    /// string (`"upstream"`/`"backoff"`) (see `crate::observability::RateLimitMetrics`). Bumped
    /// inside `crate::runtime_state::RuntimeStates::record_rate_limit`, the single 429 chokepoint.
    /// In-memory only; resets on restart.
    pub rate_limit_metrics: std::sync::Arc<crate::observability::RateLimitMetrics>,
    /// WS-downstream relay Phase 2/3 Task 5: content-free counter of reconnect, move, anchor
    /// recovery, and terminal anchor-miss events (see `crate::observability::RelayMetrics`), labeled
    /// by exactly four fixed strings. Bumped from `crate::ws_relay::pump::run_pump`'s decision
    /// points via a handle threaded into the pump from `crate::ws_relay::relay`. In-memory only;
    /// resets on restart.
    pub relay_metrics: std::sync::Arc<crate::observability::RelayMetrics>,
    /// Live upstream Codex model catalogs (see `crate::model_catalog`), cached per account and
    /// projected into exact root/pool intersections with TTL + single-flight refresh. Startup and
    /// periodic warmers keep active scopes hot; stale exact-scope or static-floor fallback remains
    /// available on fetch failure. Known per-account support also feeds model eligibility.
    pub model_catalog: Arc<crate::model_catalog::ModelCatalogCache>,
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

/// Construct the Codex-provider executor for `AppState.codex_executor`.
///
/// `http_requests_use_upstream_websocket` describes this boundary precisely: it only changes the
/// upstream leg created for an HTTP `/responses` ingress request. It does not control the
/// client-facing WebSocket relay.
///
/// When enabled, this wraps a `CodexWsExecutor` around a fresh `CodexExecutor` as its
/// HTTP-SSE fallback. Per `SPEC-M5-WEBSOCKET.md` §6, HTTP-SSE "remains the fallback on every
/// path" — but that fallback is entirely internal to `CodexWsExecutor` (a 426 at handshake time,
/// scoped per-session/per-account-cooldown; see `polyflare_codex::ws::executor`'s module doc's
/// "Fallback scope" section). This function performs SELECTION only — which concrete `Executor`
/// impl backs the field — never new retry/failover machinery of its own (M5a plan's Global
/// Constraints: "No new retry/failover machinery. M5a swaps the transport under today's
/// behavior.").
///
/// `http_upstream_websocket_ping` selects that executor socket's active-turn keepalive policy. It
/// has no effect when `http_requests_use_upstream_websocket` is false and is unrelated to the
/// parked relay's idle ping setting.
pub fn build_codex_executor(
    http_requests_use_upstream_websocket: bool,
    http_upstream_websocket_ping: bool,
) -> Result<Arc<dyn Executor>, ExecError> {
    build_codex_executor_with_client(
        polyflare_codex::build_client()?,
        http_requests_use_upstream_websocket,
        http_upstream_websocket_ping,
    )
}

/// Construct the Codex executor around a caller-owned shared HTTP client.
///
/// `serve` uses this form so the HTTP-SSE executor and `AppState::control_client` are cheap clones
/// of one reqwest client rather than independently pooled clients.
pub fn build_codex_executor_with_client(
    client: reqwest::Client,
    http_requests_use_upstream_websocket: bool,
    http_upstream_websocket_ping: bool,
) -> Result<Arc<dyn Executor>, ExecError> {
    let http = CodexExecutor::from_client(client);
    if http_requests_use_upstream_websocket {
        Ok(Arc::new(CodexWsExecutor::new(
            Arc::new(http),
            http_upstream_websocket_ping,
        )))
    } else {
        Ok(Arc::new(http))
    }
}

pub fn build_app(state: Arc<AppState>) -> Router {
    // The dashboard API: every route sits behind `require_admin`. A configured token is required;
    // without one, only `build_app_for_bind` may install the startup-resolved loopback marker.
    // Register only routes whose handlers exist today.
    let api = Router::new()
        .route("/api/whoami", get(crate::auth::whoami_handler))
        .route("/api/capabilities", get(crate::auth::capabilities_handler))
        .route(
            "/api/pools",
            get(crate::read_api::pools_handler).post(crate::write_api::create_pool_handler),
        )
        .route(
            "/api/account-onboarding/codex",
            post(crate::account_onboarding::start_handler),
        )
        .route(
            "/api/account-onboarding/{id}",
            get(crate::account_onboarding::status_handler),
        )
        .route(
            "/api/account-onboarding/{id}/callback",
            post(crate::account_onboarding::callback_handler),
        )
        .route("/api/accounts", get(crate::read_api::accounts_handler))
        .route(
            "/api/providers",
            get(crate::provider_api::list).post(crate::provider_api::create),
        )
        .route(
            "/api/providers/{id}",
            patch(crate::provider_api::patch_provider).delete(crate::provider_api::delete_provider),
        )
        .route(
            "/api/providers/{id}/test",
            post(crate::provider_api::test_provider),
        )
        .route(
            "/api/providers/{id}/credentials",
            post(crate::provider_api::create_credential),
        )
        .route(
            "/api/provider-credentials/{id}",
            patch(crate::provider_api::patch_credential)
                .delete(crate::provider_api::delete_credential),
        )
        .route(
            "/api/providers/{id}/models",
            post(crate::provider_api::create_model),
        )
        .route(
            "/api/providers/{id}/models/sync",
            post(crate::provider_api::sync_models),
        )
        .route(
            "/api/provider-models/{id}",
            patch(crate::provider_api::patch_model).delete(crate::provider_api::delete_model),
        )
        .route(
            "/api/accounts/{id}",
            get(crate::read_api::account_detail_handler)
                .patch(crate::write_api::patch_account_handler)
                .delete(crate::write_api::delete_account_handler),
        )
        .route(
            "/api/accounts/{id}/trends",
            get(crate::read_api::account_trends_handler),
        )
        .route("/api/requests", get(crate::read_api::requests_handler))
        .route("/api/sessions", get(crate::read_api::sessions_handler))
        .route("/api/pace", get(crate::read_api::pace_handler))
        .route("/api/overview", get(crate::read_api::overview_handler))
        .route(
            "/api/overview/series",
            get(crate::read_api::overview_series_handler),
        )
        .route("/api/reports", get(crate::read_api::reports_handler))
        .route(
            "/api/settings",
            get(crate::read_api::settings_handler).patch(crate::write_api::patch_settings_handler),
        )
        .route(
            "/api/keys",
            get(crate::read_api::keys_handler).post(crate::write_api::create_key_handler),
        )
        .route("/api/keys/{id}", patch(crate::write_api::patch_key_handler))
        .route("/api/logs/stream", get(crate::sse::logs_stream_handler))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::auth::require_admin,
        ));

    // D18 Task 4: the caller-auth gate on the proxy surface. POST handlers ONLY — the GET-426
    // WS-fallback shim below is deliberately kept OUTSIDE this sub-router (see the comment on the
    // top-level `.route("/responses", get(...))` further down for why: a keyless WS-handshake
    // probe must always get 426, never 401, even when enforcement is on). `route_layer` is applied
    // only when `state.enforce_client_keys` is true, mirroring `api`'s `require_admin` composition
    // above exactly — this is the SAME `route_layer`-before-`.merge()` pattern, just conditional.
    // `/{pool}/…` narrows selection to accounts tagged with that pool slug (see `filter_by_pool`);
    // the bare paths keep selecting over ALL accounts. The `{pool}` segment is a param, so it never
    // shadows the literal single-segment `/responses` or the `/v1/*` / `/models` routes — matchit
    // prefers a static segment over a param one.
    let mut proxy = Router::new()
        .route("/responses", post(responses_handler))
        .route("/v1/messages", post(messages_handler))
        .route("/{pool}/responses", post(pooled_responses_handler))
        .route("/{pool}/v1/messages", post(pooled_messages_handler))
        // D17: the minimal codex CONTROL-endpoint surface (thin generic forwards — see
        // `crate::control`). UNPOOLED (no `/{pool}/…` variant exists today, matching the plan's
        // scope) and, like the routes above, gated by `require_client_key` when
        // `enforce_client_keys` — including the GETs (`jwks`, `thread/goal/get`), which is a
        // deliberate parity choice with codex-lb (not left open like `/models`). None of these
        // literal first-segments (`thread`, `agent-identities`, `wham`, `memories`) can ever be
        // captured by the `{pool}` param above: matchit prefers a static segment over a param one
        // at the SAME position, and none of these paths' second segment is `responses` or
        // `v1/messages` anyway, so there is no possible collision with `/{pool}/responses` /
        // `/{pool}/v1/messages` either.
        .route(
            "/thread/goal/set",
            post(crate::control::thread_goal_set_handler),
        )
        .route(
            "/thread/goal/clear",
            post(crate::control::thread_goal_clear_handler),
        )
        .route(
            "/thread/goal/get",
            get(crate::control::thread_goal_get_handler),
        )
        .route("/agent-identities/jwks", get(crate::control::jwks_handler))
        .route(
            "/wham/agent-identities/jwks",
            get(crate::control::wham_jwks_handler),
        )
        .route(
            "/memories/trace_summarize",
            post(crate::control::trace_summarize_handler),
        )
        .route(
            "/{pool}/memories/trace_summarize",
            post(crate::control::pooled_trace_summarize_handler),
        )
        // D14a: `POST /responses/compact` — a UNARY passthrough the real Codex CLI emits
        // (`codex-rs client.rs:159`), previously 404'd. Static seg 1 (`responses`) never collides
        // with `/{pool}/responses`'s param seg 1 (matchit prefers the static route), and seg 2
        // (`compact` vs `responses`) differs anyway — no possible shadowing either direction.
        .route("/responses/compact", post(crate::control::compact_handler))
        .route(
            "/{pool}/responses/compact",
            post(crate::control::pooled_compact_handler),
        )
        // Current codex-rs stable extension endpoints. These are unary JSON requests, but still
        // require the selected Codex account's bearer/account-id pairing and hard pool scope.
        .route(
            "/images/generations",
            post(crate::control::image_generations_handler),
        )
        .route(
            "/{pool}/images/generations",
            post(crate::control::pooled_image_generations_handler),
        )
        .route("/images/edits", post(crate::control::image_edits_handler))
        .route(
            "/{pool}/images/edits",
            post(crate::control::pooled_image_edits_handler),
        )
        // Standalone search is under-development/default-off in the vendored client. Routing the
        // exact endpoint keeps explicitly enabled clients compatible without enabling the feature.
        .route("/alpha/search", post(crate::control::alpha_search_handler))
        .route(
            "/{pool}/alpha/search",
            post(crate::control::pooled_alpha_search_handler),
        );
    if state.enforce_client_keys {
        proxy = proxy.route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::auth::require_client_key,
        ));
    }

    // C11 Task 2: the Prometheus scrape surface, top-level `/metrics` (Prometheus convention)
    // rather than folded into the `api` sub-router above (which would force `/api/metrics`) —
    // admin-gated by its own `route_layer` of the SAME `require_admin` middleware `api` uses, so
    // a scraper authenticates identically (`Authorization: Bearer <POLYFLARE_ADMIN_TOKEN>`; unset
    // token ⇒ 503, same "disabled, not silently open" posture). `/metrics` is a single static
    // literal segment, so matchit's static-over-param preference means it can never be shadowed
    // by (or shadow) the `/{pool}/...` proxy routes below.
    let metrics = Router::new()
        .route("/metrics", get(crate::metrics::metrics_handler))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::auth::require_admin,
        ));

    // `GET` on these two Codex-native proxy paths is a WebSocket-handshake attempt from a
    // WS-capable Codex client, never a real request — Codex only ever POSTs `/responses`.
    //
    // WS-downstream relay Task 2 (`POLYFLARE_WS_DOWNSTREAM` → `state.ws_downstream`): when ON, that
    // WS handshake is ACCEPTED — the GET routes to `crate::ws_relay::responses_ws_handler`, which
    // completes the upgrade (`101`) and relays to an upstream WS (Tasks 3-6). When OFF (the
    // explicit rollback), the GET answers `426 Upgrade Required` via
    // `crate::ingress::websocket_fallback_handler` — a correctness shim so a `supports_websockets`
    // client degrades to HTTP-SSE (codex-rs's SOLE WS→HTTP fallback trigger) instead of hard-failing
    // on axum's default 405. Both `get(_)` branches produce the SAME `MethodRouter<Arc<AppState>>`
    // type (the handler is type-erased inside), so a single flag-selected value serves both paths.
    // The off branch remains byte-identical to the pre-relay fallback. `/v1/messages` is the
    // Anthropic-format path — Codex never opens a WS there, so it's deliberately left without a GET.
    //
    // When WS is OFF, the 426 compatibility shim stays keyless so a WS-capable client can discover
    // HTTP fallback before presenting a proxy key. When WS is ON, however, this GET is a real
    // account-consuming proxy request and MUST share POST's client-key gate. Otherwise enabling
    // enforcement protects HTTP while leaving a keyless WebSocket bypass.
    let (responses_ws_get, pooled_responses_ws_get) = if state.ws_downstream {
        (
            get(crate::ws_relay::responses_ws_handler),
            get(crate::ws_relay::pooled_responses_ws_handler),
        )
    } else {
        (
            get(websocket_fallback_handler),
            get(websocket_fallback_handler),
        )
    };
    let mut ws_proxy = Router::new()
        .route("/responses", responses_ws_get)
        .route("/{pool}/responses", pooled_responses_ws_get);
    if state.ws_downstream && state.enforce_client_keys {
        ws_proxy = ws_proxy.route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::auth::require_client_key,
        ));
    }

    Router::new()
        .merge(ws_proxy)
        .merge(proxy)
        // Model catalog (read-only GETs): real Codex models (bootstrap floor for now) merged with
        // PolyFlare's synthetic aliases. Routing is by method+path, so these never conflict with
        // the `/v1/*` POSTs above.
        .route("/models", get(crate::catalog::codex_models_handler))
        .route(
            "/{pool}/models",
            get(crate::catalog::pooled_codex_models_handler),
        )
        .route(
            "/backend-api/codex/models",
            get(crate::catalog::codex_models_handler),
        )
        // Stock codex-rs reads account usage from the global `chatgpt_base_url`, independently of
        // its model provider. The exact usage routes synthesize PolyFlare's aggregate pool meter;
        // the catch-alls keep every unrelated ChatGPT backend feature on a direct, client-auth
        // passthrough while recording only normalized route telemetry.
        .route(
            "/backend-api/wham/usage",
            get(crate::chatgpt_backend::usage_handler),
        )
        .route(
            "/{pool}/backend-api/wham/usage",
            get(crate::chatgpt_backend::pooled_usage_handler),
        )
        .route(
            "/backend-api/{*path}",
            any(crate::chatgpt_backend::passthrough_handler),
        )
        .route(
            "/{pool}/backend-api/{*path}",
            any(crate::chatgpt_backend::pooled_passthrough_handler),
        )
        .route("/v1/models", get(crate::catalog::v1_models_handler))
        // Auth-gated dashboard API (see `api` above): pools/accounts/requests/overview reads,
        // the account-settings patch, and `/api/whoami`.
        .merge(api)
        // Auth-gated Prometheus scrape surface (see `metrics` above): `GET /metrics`.
        .merge(metrics)
        // Embedded dashboard UI (see `crate::dashboard`). `/dashboard` serves the SPA entrypoint;
        // `/dashboard/{*path}` serves its bundle assets (with SPA fallback to index.html).
        // Unauthenticated: it's a static asset bundle — the API calls it makes are what's gated.
        .route("/dashboard", get(crate::dashboard::dashboard_index))
        .route("/dashboard/", get(crate::dashboard::dashboard_index))
        .route("/dashboard/{*path}", get(crate::dashboard::dashboard_asset))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
        .with_state(state)
}

/// Build the production router with dashboard access resolved from the actual listener bind.
/// Existing test/embedding callers can keep using [`build_app`], whose tokenless behavior remains
/// fail-closed unless they explicitly supply a bind through this function.
pub fn build_app_for_bind(state: Arc<AppState>, bind_addr: &str) -> Router {
    let local_open = crate::auth::local_dashboard_access(state.admin_token.as_deref(), bind_addr);
    let app = build_app(state);
    if local_open {
        app.layer(axum::Extension(crate::auth::LocalDashboardAccess))
    } else {
        app
    }
}
