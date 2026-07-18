//! Application state and router construction.

use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post};
use axum::Router;

use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::{CodexExecutor, CodexVersionCache, CodexWsExecutor};
use polyflare_core::{Continuity, ExecError, Executor, Provider, Selector};

use crate::account_cache::AccountCache;
use polyflare_store::{Store, TokenCipher};

use crate::ingress::{
    messages_handler, pooled_messages_handler, pooled_responses_handler, responses_handler,
    websocket_fallback_handler,
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
    /// D17 Task 2: a shared `reqwest::Client` for the codex CONTROL-endpoint unary forward
    /// (`polyflare_codex::control_forward`), which takes `&reqwest::Client` as its first
    /// parameter rather than building its own. Built via `polyflare_codex::build_client()` — the
    /// SAME rustls/aws-lc-rs-pinned builder `CodexExecutor` uses for `/responses` — so this is a
    /// second `Client` *instance* sharing the identical TLS/fingerprint configuration, never an
    /// independently-configured (and therefore potentially divergent) one. `reqwest::Client` is
    /// `Arc`-backed internally, so holding a second instance here (rather than threading a
    /// concrete `CodexExecutor` accessor through the `Arc<dyn Executor>` trait object) is cheap
    /// and avoids a downcast. Control resolution itself (`crate::control::resolve_control_account`)
    /// does not use this field — it exists so Task 3's route handlers have a client to call
    /// `control_forward` with, without re-deriving the builder decision then.
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
    /// B4/B5 Task 5: the bounded cross-account failover loop's total upstream-attempt cap
    /// (`POLYFLARE_MAX_ACCOUNT_ATTEMPTS`, resolved ONCE at startup by
    /// `crate::config::max_account_attempts_from_env` — never read per-request). The production
    /// `/responses` entrypoint (`crate::ingress::responses_handler_impl`) reads this field; the
    /// `responses_handler_impl_for_test` seam still takes an explicit override for tests.
    pub max_account_attempts: u32,
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
    /// B5 Task 5: the Layer 2 keepalive recovery-wait's bounded wait budget
    /// (`POLYFLARE_STARVATION_WAIT_BUDGET_SECS`, resolved ONCE at startup by
    /// `crate::config::starvation_wait_budget_secs_from_env` — never read per-request). The
    /// production `/responses` entrypoint (`crate::ingress::responses_handler_impl`) reads this
    /// field instead of `crate::starvation::DEFAULT_WAIT_BUDGET`. `Duration::ZERO` ⇒ Layer 2 is
    /// DISABLED (the documented `=0` disable lever — see that config function's doc):
    /// `crate::ingress::try_layer2_recovery_wait` returns `None` immediately, falling straight
    /// through to today's pre-response fast 503/502.
    pub starvation_wait_budget: std::time::Duration,
    /// B5 Task 5: the Layer 2 keepalive tick interval (`POLYFLARE_STARVATION_HEARTBEAT_SECS`,
    /// resolved ONCE at startup by `crate::config::starvation_heartbeat_secs_from_env`, clamped
    /// against `starvation_wait_budget` above). The production entrypoint reads this field instead
    /// of `crate::starvation::DEFAULT_HEARTBEAT`.
    pub starvation_heartbeat: std::time::Duration,
    /// B10 Task 1 (THE CRUX): the per-waiter wake-jitter window
    /// (`POLYFLARE_STARVATION_WAKE_JITTER_MS`, resolved ONCE at startup by
    /// `crate::config::wake_jitter_ms_from_env` — never read per-request). Read directly by
    /// `crate::ingress::layer2_wait_stream` at wait entry to compute this waiter's own
    /// `jittered_wake_target_ms`, desynchronizing concurrent waiters on the same recovering
    /// account. `0` (the default) ⇒ zero offset ⇒ byte-for-byte today's pre-B10 behavior; it never
    /// touches `select.rs`, the account's stored `recover_at`/`cooldown_until`/`backoff_secs`, or
    /// which account is waited on.
    pub wake_jitter_ms: u64,
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
    /// Stream-idle-timeout plan Task 2: the per-response mid-stream idle deadline
    /// (`POLYFLARE_STREAM_IDLE_TIMEOUT_SECS`, resolved ONCE at startup by
    /// `crate::config::stream_idle_timeout_secs_from_env` — never read per-request). Threaded
    /// into every `execute_with_watchdog*`/`execute_recovery*` call site in `crate::ingress`,
    /// which passes it through to `crate::watchdog::wrap_stream` → `ObservingStream`'s
    /// `IdleDeadline` (Task 1's mechanism). `Duration::ZERO` ⇒ disabled (today's pre-fix
    /// behavior — the documented `=0` rollback lever).
    pub stream_idle_timeout: std::time::Duration,
    /// B8 Task 3: the codex-lb `soft_drain_enabled` disable lever
    /// (`POLYFLARE_SOFT_DRAIN_ENABLED`, resolved ONCE at startup by
    /// `crate::config::soft_drain_enabled_from_env` — never read per-request). Read by
    /// `crate::usage_refresh`'s poller loop (which owns the only usage-driven health-tier
    /// evaluation site — and, since the B8-review Finding 1 fix, the disjoint non-codex
    /// error-driven pass in the same loop) and threaded into `RuntimeStates::evaluate_with_usage`
    /// on every refresh cycle for BOTH the codex and non-codex passes. `false` forces every
    /// account's health tier to HEALTHY with cleared aux state (codex-lb's disable path,
    /// `load_balancer.py:2245-2249`) — the documented clean-rollback lever, matching today's exact
    /// pre-B8 behavior (`select.rs`'s `health_tier_pool` becomes a no-op single bucket) **in steady
    /// state**: the flag is honored only by the poller, so the per-request funnel (not flag-gated)
    /// can still transiently drive an account into DRAINING from errors between poller cycles
    /// (bounded to ≤600s, the poller's `REFRESH_INTERVAL`, before the next tick resets it — see
    /// `crate::config::soft_drain_enabled_from_env`'s doc for the same caveat). Defaults to `true`
    /// in every test/dev harness that builds `AppState` directly, matching
    /// `POLYFLARE_SOFT_DRAIN_ENABLED`'s unset-default (mirrors `enforce_client_keys`'s doc above for
    /// why test harnesses hardcode a value here).
    pub soft_drain_enabled: bool,
    /// C9 Task 3: the in-flight soft-penalty pct (`POLYFLARE_INFLIGHT_PENALTY_PCT`, resolved ONCE
    /// at startup by `crate::config::inflight_penalty_pct_from_env` — never read per-request).
    /// Copied onto each per-request `SelectionCtx.inflight_penalty_pct` at the selection sites in
    /// `crate::ingress`/`crate::control`, which `polyflare_core::select`'s `eligibility()` folds
    /// into `eff_used`/`eff_secondary_used` as `in_flight * inflight_penalty_pct`. `0.0` ⇒ the
    /// disable lever (in_flight still tracked, never folded into the weight).
    pub inflight_penalty_pct: f64,
    /// C9 Task 4: content-free counter of in-flight lease acquire/release events (see
    /// `crate::observability::LeaseMetrics`) — bumped from `crate::runtime_state::RuntimeStates::
    /// acquire_in_flight` (acquired) and `crate::runtime_state::InFlightGuard`'s `Drop` (released),
    /// via a handle threaded into `acquire_in_flight`'s call sites (`crate::ingress`) and stored on
    /// the returned guard so the release bump fires wherever/however the guard is dropped —
    /// disconnect, drain, error, timeout, panic, or failover reselect, mirroring the leak-proof
    /// release guarantee Task 1-2 already established for `in_flight` itself. In-memory only (like
    /// `FailoverMetrics`/`StarvationMetrics`/`HealthTierMetrics`); resets on restart.
    pub lease_metrics: std::sync::Arc<crate::observability::LeaseMetrics>,
    /// C12 Task 3: `request_log` age-retention, in days (`POLYFLARE_REQUEST_LOG_RETENTION_DAYS`,
    /// resolved ONCE at startup by `crate::config::request_log_retention_days_from_env` — never
    /// read per-request). Read only by `crate::retention::run_retention_pass`, once per tick.
    /// `0` (the default) ⇒ disabled — the pruner no-ops for this table, today's unbounded-growth
    /// behavior.
    pub request_log_retention_days: u32,
    /// C12 Task 3: `usage_history` age-retention, in days
    /// (`POLYFLARE_USAGE_HISTORY_RETENTION_DAYS`, resolved ONCE at startup by
    /// `crate::config::usage_history_retention_days_from_env`). Read only by
    /// `crate::retention::run_retention_pass`. `0` (the default) ⇒ disabled. Pruning always
    /// protects the latest row per `(account_id, window)` regardless of age (see
    /// `AccountRepo::prune_usage_history_older_than`) — this field only gates whether pruning
    /// happens at all.
    pub usage_history_retention_days: u32,
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
    /// D15 Task 3: the live upstream Codex model-catalog cache (see `crate::model_catalog`),
    /// merged onto the static bootstrap floor with a TTL + single-flight, falling back airtight to
    /// the floor on disable/no-accounts/fetch-failure. Read on the `/models` hot path via the sync
    /// `cached_or_fallback()` (`crate::catalog::build_catalog`'s callers); warmed by a background
    /// task in `serve` only when `POLYFLARE_MODEL_CATALOG_ENABLED` (default on). `/models` is
    /// advertised metadata only — this field is never consulted by routing/selection.
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

/// Construct the Codex-provider executor for `AppState.codex_executor`, selected by
/// `POLYFLARE_WS_UPSTREAM` (`ws_upstream`, see `crate::config::ServeConfig`).
///
/// **`ws_upstream == false` (the default): `CodexExecutor` exactly as before this flag existed —
/// zero behavior change.** This is the ONLY path every deployment and every existing test used
/// prior to M5a; nothing about it is touched by this function's `true` branch.
///
/// `ws_upstream == true`: wraps a `CodexWsExecutor` around a FRESH `CodexExecutor` as its
/// HTTP-SSE fallback. Per `SPEC-M5-WEBSOCKET.md` §6, HTTP-SSE "remains the fallback on every
/// path" — but that fallback is entirely internal to `CodexWsExecutor` (a 426 at handshake time,
/// scoped per-session/per-account-cooldown; see `polyflare_codex::ws::executor`'s module doc's
/// "Fallback scope" section). This function performs SELECTION only — which concrete `Executor`
/// impl backs the field — never new retry/failover machinery of its own (M5a plan's Global
/// Constraints: "No new retry/failover machinery. M5a swaps the transport under today's
/// behavior.").
pub fn build_codex_executor(ws_upstream: bool) -> Result<Arc<dyn Executor>, ExecError> {
    let http = CodexExecutor::new()?;
    if ws_upstream {
        Ok(Arc::new(CodexWsExecutor::new(Arc::new(http))))
    } else {
        Ok(Arc::new(http))
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
            get(crate::read_api::account_detail_handler)
                .patch(crate::write_api::patch_account_handler),
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

    Router::new()
        // `GET` on these two Codex-native proxy paths is a WebSocket-handshake attempt from a
        // WS-capable Codex client, never a real request — Codex only ever POSTs `/responses`.
        // Answering it with `426 Upgrade Required` (rather than falling through to axum's default
        // 405) is a temporary correctness shim so such a client degrades to HTTP-SSE instead of
        // hard-failing; see `crate::ingress::websocket_fallback_handler`'s doc for the full
        // rationale. `/v1/messages` is the Anthropic-format path — Codex never opens a WS there,
        // so it's deliberately left without this GET handler.
        //
        // D18 Task 4: this GET route is registered here, on the TOP-LEVEL (unlayered) router, NOT
        // inside `proxy` above — a keyless WS-handshake probe must always degrade to 426, never be
        // rejected with 401, regardless of whether client-key enforcement is on. `Router::merge`
        // combines a GET-only `MethodRouter` for a path with a POST-only one for the same path
        // (axum's `MethodRouter::merge_for_path` only panics on an actual method OVERLAP, e.g. two
        // GETs for the same path — disjoint methods merge cleanly), so splitting GET and POST
        // across the unlayered top-level router and the conditionally-layered `proxy` sub-router
        // below is safe and is exactly axum's supported pattern for "some methods on this path are
        // gated, others aren't."
        .route("/responses", get(websocket_fallback_handler))
        .route("/{pool}/responses", get(websocket_fallback_handler))
        .merge(proxy)
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
