//! Process configuration for `polyflare serve`, read from environment. Secrets are NOT here —
//! per-account bearer tokens live in the store; only shared base URLs + data paths are config.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Once;
use std::time::Duration;

use polyflare_core::RoutingStrategy;

// The three shared base URLs are the same for every account, so each has a production default and
// is overridable by its env var (for a mock/staging/self-hosted-proxy upstream) — none is required.
const DEFAULT_CODEX_UPSTREAM_URL: &str = "https://chatgpt.com/backend-api/codex";
const DEFAULT_ANTHROPIC_UPSTREAM_URL: &str = "https://api.anthropic.com";
const DEFAULT_AUTH_URL: &str = "https://auth.openai.com";

/// `serve` configuration. The upstream base URL is shared across accounts; per-account bearer
/// tokens are decrypted from the store per request.
pub struct ServeConfig {
    pub bind_addr: String,
    pub upstream_base_url: String,
    pub anthropic_upstream_base_url: String,
    pub auth_base_url: String,
    pub db_path: PathBuf,
    pub key_path: PathBuf,
    pub continuity_watchdog: Duration,
    /// M5 capture-fixture mechanism (content-safe, structural-only — see
    /// `crate::fingerprint_capture`): when set, every ingress request's HTTP fingerprint is
    /// appended to this path as JSON Lines. Unset ⇒ disabled, zero overhead.
    pub capture_fingerprint_path: Option<PathBuf>,
    /// Global default routing strategy (`POLYFLARE_ROUTING_STRATEGY`, default `capacity_weighted`).
    pub routing_strategy: RoutingStrategy,
    /// Per-pool routing-strategy overrides (`POLYFLARE_POOL_STRATEGY="slug=strategy,slug2=..."`).
    pub pool_strategies: HashMap<String, RoutingStrategy>,
    /// Admin token gating every `/api/*` dashboard route (`POLYFLARE_ADMIN_TOKEN`, via
    /// `Authorization: Bearer <token>`). Unset ⇒ the dashboard API is disabled (503), not open.
    pub admin_token: Option<String>,
    /// Enables the live log stream (`POLYFLARE_LIVE_LOGS=1|true`). Consumed by a later task
    /// (`/api/logs/stream`); wired through now so `AppState` construction doesn't churn again.
    pub live_logs: bool,
    /// M5a: selects the upstream WebSocket transport (`POLYFLARE_WS_UPSTREAM=1|true`) for the
    /// Codex executor instead of today's HTTP-SSE. **Default OFF** — off means `CodexExecutor`
    /// exactly as before this flag existed, zero behavior change (see `SPEC-M5-WEBSOCKET.md` §6:
    /// "HTTP-SSE remains the fallback on every path"). Consumed by
    /// `crate::app::build_codex_executor`.
    pub ws_upstream: bool,
    /// WS-downstream relay plan Task 2: selects the DOWNSTREAM (client-facing) WebSocket transport
    /// (`POLYFLARE_WS_DOWNSTREAM=1|true`). **Default OFF** — off means the WS-handshake `GET
    /// /responses` (and `/{pool}/responses`) still answers `426 Upgrade Required`
    /// (`crate::ingress::websocket_fallback_handler`), byte-identical to before this flag existed
    /// (see `docs/superpowers/specs/2026-07-20-ws-downstream-relay-design.md` §8: "Flag-gated
    /// (`POLYFLARE_WS_DOWNSTREAM`, default off); additive; HTTP-SSE and translation paths
    /// byte-unchanged"). Only an explicit `1`/`true` routes that GET to `crate::ws_relay`'s upgrade
    /// handler instead. Same fail-safe-default convention as `ws_upstream` above: any
    /// unset/empty/unrecognized value is treated as OFF, never a startup error. Consumed by
    /// `crate::app::build_app` (via `AppState::ws_downstream`) to shape the router.
    pub ws_downstream: bool,
    /// Opt-in client-initiated WS keepalive pings (`POLYFLARE_WS_CLIENT_PING=1|true`). **Default
    /// OFF = codex-rs-faithful**: real codex-rs NEVER initiates a client ping (it only auto-pongs
    /// inbound server pings and bounds reads with an idle timeout — `docs/WS-GROUND-TRUTH-CODEX.md`
    /// §7; verified in `codex-api` + `websocket-client`), so OFF makes PolyFlare's WS egress match it
    /// exactly. **ON = codex-lb-style keepalive pings** during a silent read, for deployments behind
    /// aggressive NAT/middleboxes that reap idle sockets; this is a DELIBERATE, documented
    /// fingerprint divergence from codex-rs. Consumed by `crate::app::build_codex_executor` →
    /// `polyflare_codex::ws::CodexWsExecutor` (no effect unless `ws_upstream` is also on).
    pub ws_client_ping: bool,
    /// B4/B5 Task 5: the bounded cross-account failover loop's total upstream-attempt cap
    /// (`POLYFLARE_MAX_ACCOUNT_ATTEMPTS`; see [`max_account_attempts_from_env`] for the
    /// malformed/zero handling decision). Resolved ONCE here at startup and threaded through
    /// `AppState` — never read per-request (see `crate::ingress::responses_handler_impl`, which
    /// used to hardcode this as a const before this task, and the TA6(b) T5 review that flagged
    /// per-request `env::var` as debt).
    pub max_account_attempts: u32,
    /// B5 Task 5: the Layer 2 keepalive recovery-wait's bounded wait budget
    /// (`POLYFLARE_STARVATION_WAIT_BUDGET_SECS`; see [`starvation_wait_budget_secs_from_env`] for
    /// the default/clamp/disable-lever decision). `Duration::ZERO` ⇒ Layer 2 is DISABLED (the
    /// documented `=0` lever). Resolved ONCE here at startup and threaded through `AppState` —
    /// never a per-request `env::var` (mirrors `max_account_attempts` above; this is the exact
    /// debt class the TA6(b) T5 review flagged, applied to B5's own two new knobs).
    pub starvation_wait_budget: Duration,
    /// B5 Task 5: the Layer 2 keepalive tick interval (`POLYFLARE_STARVATION_HEARTBEAT_SECS`; see
    /// [`starvation_heartbeat_secs_from_env`]). Resolved ONCE here, clamped against the ALREADY-
    /// resolved `starvation_wait_budget` above.
    pub starvation_heartbeat: Duration,
    /// D18 Task 4: `POLYFLARE_ALLOW_UNAUTHENTICATED_REMOTE=1` — the ONLY way to boot the proxy
    /// surface unauthenticated on a non-loopback bind with no client API key configured (see
    /// `crate::posture::resolve_proxy_enforcement`). Fail-safe-default: any value other than
    /// exactly `"1"` is treated as unset (mirrors `live_logs`/`ws_upstream` above) — a typo here
    /// must never accidentally grant the dangerous path.
    pub allow_unauthenticated_remote: bool,
    /// Stream-idle-timeout plan Task 2: the per-response mid-stream idle deadline
    /// (`POLYFLARE_STREAM_IDLE_TIMEOUT_SECS`; see [`stream_idle_timeout_secs_from_env`] for the
    /// default/clamp/disable-lever decision). Resolved ONCE here at startup and threaded through
    /// `AppState` — never a per-request `env::var` read (mirrors `max_account_attempts`/
    /// `starvation_wait_budget` above; same debt class the TA6(b) T5 review flagged).
    /// `Duration::ZERO` ⇒ Task 1's `IdleDeadline::Disabled` (no idle bound — today's pre-fix
    /// behavior, the documented `=0` rollback lever).
    pub stream_idle_timeout: Duration,
    /// B8 Task 3: the codex-lb `soft_drain_enabled` disable lever
    /// (`POLYFLARE_SOFT_DRAIN_ENABLED`; see [`soft_drain_enabled_from_env`] for the parse
    /// decision). Resolved ONCE here at startup and threaded through `AppState` — never a
    /// per-request `env::var` read (mirrors `max_account_attempts`/`starvation_wait_budget`/
    /// `stream_idle_timeout` above). `false` ⇒ `crate::usage_refresh`'s poller forces every
    /// account's health tier to HEALTHY with cleared aux state every cycle (codex-lb's disable
    /// path) — the documented clean-rollback lever. **Caveat:** this is a STEADY-STATE guarantee,
    /// not an instantaneous one — the flag is honored only by the poller; the per-request funnel is
    /// not flag-gated and can still transiently drive an account into DRAINING from errors between
    /// poller cycles (bounded to ≤ one `REFRESH_INTERVAL`, ≤600s, before the next poller tick resets
    /// it). Since the B8-review Finding 1 fix, the poller resets ALL providers (codex and
    /// non-codex), not just codex.
    pub soft_drain_enabled: bool,
    /// B10 Task 1 (THE CRUX): the per-waiter wake-jitter window
    /// (`POLYFLARE_STARVATION_WAKE_JITTER_MS`; see [`wake_jitter_ms_from_env`] for the
    /// default/clamp handling). Resolved ONCE here at startup and threaded through `AppState` —
    /// never a per-request `env::var` read (mirrors `starvation_wait_budget`/`starvation_heartbeat`
    /// above). `0` (the default) ⇒ zero offset ⇒ byte-for-byte today's pre-B10
    /// `crate::ingress::layer2_wait_stream` behavior; a positive value spreads concurrent waiters on
    /// the same recovering account across `[0, wake_jitter_ms]` of extra delay, never past the wait
    /// budget.
    pub wake_jitter_ms: u64,
    /// C9 Task 3: the in-flight soft-penalty pct (`POLYFLARE_INFLIGHT_PENALTY_PCT`; see
    /// [`inflight_penalty_pct_from_env`] for the default/clamp/disable-lever decision). Resolved
    /// ONCE here at startup and threaded through `AppState` → `SelectionCtx` — never a per-request
    /// `env::var` read (mirrors every other knob in this struct; `crate::select` reads it purely
    /// off `SelectionCtx`, never env, so `Selector::pick` stays pure-sync, M2-GATE1). `0.0` ⇒ the
    /// documented disable lever: `AccountSnapshot.in_flight` is still tracked (Tasks 1-2), it is
    /// simply folded in at zero weight, a byte-for-byte rollback to pre-C9 selection scoring.
    pub inflight_penalty_pct: f64,
    /// C12 Task 3: `request_log` age-retention, in days (`POLYFLARE_REQUEST_LOG_RETENTION_DAYS`;
    /// see [`request_log_retention_days_from_env`] for the default/clamp/disable-lever decision).
    /// Resolved ONCE here at startup and threaded through `AppState` — never a per-request
    /// `env::var` read (mirrors every other knob in this struct). `0` (the default) ⇒ disabled —
    /// `crate::retention::run_retention_pass` no-ops for `request_log` (today's unbounded-growth
    /// behavior, the documented clean-rollback lever).
    pub request_log_retention_days: u32,
    /// C12 Task 3: `usage_history` age-retention, in days
    /// (`POLYFLARE_USAGE_HISTORY_RETENTION_DAYS`; see
    /// [`usage_history_retention_days_from_env`]). Resolved ONCE here at startup and threaded
    /// through `AppState`. `0` (the default) ⇒ disabled. A non-zero value never risks the routing
    /// gate: `AccountRepo::prune_usage_history_older_than` always protects the latest row per
    /// `(account_id, window)` regardless of age.
    pub usage_history_retention_days: u32,
    /// D15 Task 3: `POLYFLARE_MODEL_CATALOG_TTL_SECS` — the live upstream model-catalog cache's
    /// refresh TTL (`crate::model_catalog::ModelCatalogCache`; see
    /// [`model_catalog_ttl_secs_from_env`] for the default/clamp decision). Resolved ONCE here at
    /// startup and threaded through `AppState`/the background-warm loop in `serve` — never a
    /// per-request `env::var` read (mirrors every other knob in this struct).
    pub model_catalog_ttl_secs: u64,
    /// D15 Task 3: `POLYFLARE_MODEL_CATALOG_ENABLED` — the live upstream model-catalog fetch's
    /// disable lever (see [`model_catalog_enabled_from_env`] for the default/parse decision).
    /// `false` ⇒ `serve` builds `AppState.model_catalog` with a floor-only source
    /// (`crate::model_catalog::floor_only_model_catalog`) and skips the background-warm task
    /// entirely — a clean rollback to today's static-only `/models` behavior, never a broken or
    /// empty catalog either way.
    pub model_catalog_enabled: bool,
}

/// TA6(b) Task 5: the one capability name resolved today. A pool tagged with this capability (via
/// `POLYFLARE_POOL_CAPABILITIES`) or a request carrying the matching `CAPABILITY_HEADER` value
/// requires `security_work_authorized` accounts — the selector's existing hard filter
/// (`select.rs:294,454`) already enforces it; this module only RESOLVES whether a given turn needs
/// it, proactively, without waiting for a `cyber_policy` rejection.
pub const SECURITY_WORK_CAPABILITY: &str = "security_work";

/// TA6(a)'s proactive-precedence-1 header: a request carrying
/// `X-PolyFlare-Capability: security_work` pre-filters to capability-holding accounts from turn 1.
/// `HeaderMap::get` is case-insensitive, so the exact casing here doesn't matter at the call site.
pub const CAPABILITY_HEADER: &str = "x-polyflare-capability";

/// Parse `POLYFLARE_POOL_CAPABILITIES` — a comma-separated list of `slug:capability` pairs (e.g.
/// `cyber:security_work`). Whitespace around each token is tolerated; empty segments are skipped.
/// A malformed pair, an empty slug, or an empty capability is a hard error — mirrors
/// `parse_pool_strategies`'s fail-fast tolerance. This is the pool-tagging mechanism TA6(b) calls
/// for (DESIGN-DECISIONS.md): the pool declares the requirement, the per-account
/// `security_work_authorized` flag declares who satisfies it — self-enforcing, since the selector
/// filters out any non-authorized account even if it's misplaced into a tagged pool.
fn parse_pool_capabilities(raw: &str) -> Result<HashMap<String, String>, String> {
    let mut map = HashMap::new();
    for pair in raw.split(',') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        let (slug, cap) = pair.split_once(':').ok_or_else(|| {
            format!("POLYFLARE_POOL_CAPABILITIES: expected 'slug:capability', got '{pair}'")
        })?;
        let slug = slug.trim();
        let cap = cap.trim();
        if slug.is_empty() {
            return Err(format!(
                "POLYFLARE_POOL_CAPABILITIES: empty pool slug in '{pair}'"
            ));
        }
        if cap.is_empty() {
            return Err(format!(
                "POLYFLARE_POOL_CAPABILITIES: empty capability in '{pair}'"
            ));
        }
        map.insert(slug.to_string(), cap.to_string());
    }
    Ok(map)
}

/// Resolves whether `pool` requires `capability`, per `POLYFLARE_POOL_CAPABILITIES` — read FRESH
/// on every call (no caching, no persistence): TA6(b) Task 5 is a per-turn resolution from
/// deployment config, not a stamp, so there is nothing to invalidate or race on. `pool: None` (the
/// bare, unpooled routes) never requires a capability from this source — only a named pool can be
/// tagged. A malformed env value fails OPEN on this signal only (treated as "no requirement"): it
/// never lowers the selector's hard capability filter, and Task 2's reactive move still catches a
/// genuine `cyber_policy` rejection regardless of this proactive signal, so a startup-time crash
/// isn't needed for correctness here the way it is for `POLYFLARE_ROUTING_STRATEGY`.
///
/// TA6(b) Task 5 review finding: fail-open used to be SILENT, giving an operator who typos the
/// value zero signal that their pool's capability tagging is disabled (every fresh session in that
/// pool then pays a wasted reactive round-trip). This function runs on every request, so the warn
/// is emitted at most ONCE per process via `POOL_CAPABILITIES_WARN_ONCE` — a per-request warning
/// would flood the logs, which is a worse bug than the original silence. The logged text is the
/// parse error from `parse_pool_capabilities` (pool slugs + capability tags, e.g. "expected
/// 'slug:capability', got 'cyber-security_work'") — never a secret, since this var is a pool→
/// capability map, not credentials.
pub fn pool_requires_capability(pool: Option<&str>, capability: &str) -> bool {
    let Some(pool) = pool.filter(|p| !p.is_empty()) else {
        return false;
    };
    let raw = match std::env::var("POLYFLARE_POOL_CAPABILITIES") {
        Ok(r) => r,
        Err(_) => return false,
    };
    match parse_pool_capabilities(&raw) {
        Ok(map) => map.get(pool).is_some_and(|c| c == capability),
        Err(e) => {
            static WARN_ONCE: Once = Once::new();
            WARN_ONCE.call_once(|| {
                tracing::warn!(
                    "POLYFLARE_POOL_CAPABILITIES is malformed ({e}); pool-capability tagging is \
                     disabled for the rest of this process's lifetime as a result (fails open — no \
                     pool is treated as requiring a capability from this source). The selector's \
                     hard capability filter and TA6(b) Task 2's reactive cyber_policy move remain in \
                     effect regardless, so this is a routing-efficiency signal, not a safety one. \
                     Fix the value and restart the process to re-enable pool tagging. (This warning \
                     is logged only once per process to avoid per-request spam.)"
                );
            });
            false
        }
    }
}

/// Parse `POLYFLARE_POOL_STRATEGY` — a comma-separated list of `slug=strategy` pairs. Whitespace
/// around each token is tolerated; empty segments are skipped. An unknown strategy name or a
/// malformed pair is a hard error (fail fast at startup rather than silently misroute a pool).
fn parse_pool_strategies(raw: &str) -> Result<HashMap<String, RoutingStrategy>, String> {
    let mut map = HashMap::new();
    for pair in raw.split(',') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        let (slug, strat) = pair.split_once('=').ok_or_else(|| {
            format!("POLYFLARE_POOL_STRATEGY: expected 'slug=strategy', got '{pair}'")
        })?;
        let slug = slug.trim();
        if slug.is_empty() {
            return Err(format!(
                "POLYFLARE_POOL_STRATEGY: empty pool slug in '{pair}'"
            ));
        }
        let strategy = RoutingStrategy::parse(strat.trim()).ok_or_else(|| {
            format!(
                "POLYFLARE_POOL_STRATEGY: unknown strategy '{}'",
                strat.trim()
            )
        })?;
        map.insert(slug.to_string(), strategy);
    }
    Ok(map)
}

/// B4/B5 Task 5: `POLYFLARE_MAX_ACCOUNT_ATTEMPTS` — total upstream attempts allowed per client
/// request for the bounded cross-account failover loop (`crate::ingress::run_failover_loop`).
/// Unset ⇒ `3` (the plan's `Global Constraints` bound, matching codex-lb's
/// `_STREAM_MAX_ACCOUNT_ATTEMPTS`); `1` ⇒ today's one-shot behavior (the clean-rollback lever).
///
/// **Malformed/zero handling — documented decision (this function IS the decision record):**
/// - A value that fails to parse as a `u32` (non-numeric, negative, out of range) resolves to the
///   SAME default as unset: `3`. We deliberately do NOT fall back to `1` (one-shot) on a parse
///   failure: `1` is a real, intentional configuration (the clean-rollback lever the plan
///   describes) that an operator must set EXPLICITLY and correctly. Silently collapsing a typo
///   (e.g. a stray trailing character, an accidentally-unset shell variable expansion) into the
///   MOST conservative behavior available — failover disabled — would quietly remove resilience
///   exactly when config has already drifted, which is a worse failure mode than falling back to
///   the field-proven default of 3. A malformed value therefore reads exactly like an absent one.
/// - An explicit, well-formed `0` is clamped UP to the floor of `1` — never down, and never
///   treated as "malformed → 3". `0` has no sane reading as "unset" (an operator who writes `0`
///   clearly intended a small, deliberate number, unlike a parse failure); it can only sanely mean
///   "as few attempts as possible", and the loop's own invariant (`ingress.rs`'s "Bookkeeping
///   order" doc) requires at least one attempt — a request must be tried, or the request path is
///   effectively disabled, indistinguishable from an outage. So `0` floors to `1`, not `3`.
///
/// Every malformed/absent case converges on `3`; only a well-formed `0` gets the special-cased
/// floor of `1`. This mirrors `ws_upstream`/`live_logs`'s "malformed ⇒ safe default, never a boot
/// crash" convention (`ServeConfig::from_env`'s doc), extended with one numeric clamp.
pub fn max_account_attempts_from_env() -> u32 {
    const DEFAULT: u32 = 3;
    match std::env::var("POLYFLARE_MAX_ACCOUNT_ATTEMPTS") {
        Ok(raw) => match raw.trim().parse::<u32>() {
            Ok(n) => clamp_max_account_attempts(n),
            Err(_) => DEFAULT,
        },
        Err(_) => DEFAULT,
    }
}

/// The pure bound [`max_account_attempts_from_env`] applies to an already-parsed value: an
/// explicit, well-formed `0` floors UP to `1` (a failover loop always needs at least one attempt —
/// see that function's doc); every other value passes through unchanged (no upper bound). Extracted
/// so the (later) settings PATCH handler validates a live update against the exact same bound the
/// boot path uses — one source of truth, not two copies that can drift.
pub fn clamp_max_account_attempts(raw: u32) -> u32 {
    if raw == 0 {
        1
    } else {
        raw
    }
}

/// B5 Task 5: `POLYFLARE_STARVATION_WAIT_BUDGET_SECS` — the Layer 2 keepalive recovery-wait's
/// bounded wait budget (`crate::ingress::layer2_wait_stream`). Unset ⇒ `60` (the plan's Task 4
/// default, matching codex-lb's `retry.py` DEFAULT); a WELL-FORMED, non-zero value is clamped to
/// `[1, 300]` (codex-lb's `MIN=1s`/`MAX=300s`); a MALFORMED value (non-numeric, negative, or out of
/// `u32` range) resolves to the SAME default as unset (`60`) — identical rationale to
/// [`max_account_attempts_from_env`]'s doc: a typo must never silently collapse to the most
/// conservative behavior available (here, that would be "wait disabled", which is a **safety**
/// regression — starvation protection silently turned off — not a conservative one).
///
/// **`0` is the documented DISABLE LEVER, not a clamp target — this is the load-bearing decision
/// this function encodes.** An explicit, well-formed `0` resolves to `Duration::ZERO`, which
/// `crate::ingress::try_layer2_recovery_wait` reads as "Layer 2 off": it returns `None`
/// immediately, before ever calling `soonest_recover` or committing an HTTP 200 — the caller falls
/// straight through to today's pre-response fast 503/502, with zero keepalives ever emitted. This
/// is DELIBERATELY different from `max_account_attempts_from_env`'s `0` handling (which floors UP
/// to `1`, since a failover loop always needs at least one upstream attempt to mean anything):
/// - `max_account_attempts = 0` has NO sane reading of its own (a request path that never attempts
///   a single upstream call is indistinguishable from an outage) — so it floors to the nearest
///   sane value, `1`.
/// - `starvation_wait_budget = 0` DOES have a sane, useful, and unambiguous reading of its own:
///   "never hold the client open waiting for a recovery — always fail fast." The plan's Task 5
///   explicitly names this as the intended operator lever (e.g., a deployment sitting behind a
///   proxy/load-balancer with a hard idle-connection timeout shorter than any sane wait budget,
///   where Layer 2's SSE keepalive strategy would itself get killed mid-wait by an intermediary
///   that PolyFlare has no visibility into — better to fail fast and let the CLIENT'S OWN retry
///   logic handle it than to commit a 200 that later gets silently truncated).
/// - A `1s`-floor treatment (mirroring `max_account_attempts`) would NOT satisfy that use case: it
///   would still commit an HTTP 200 and briefly delay the 503-equivalent outcome behind an in-band
///   SSE error frame instead of surfacing a real status code the client's own retry logic can act
///   on immediately.
pub fn starvation_wait_budget_secs_from_env() -> u32 {
    const DEFAULT: u32 = 60;
    match std::env::var("POLYFLARE_STARVATION_WAIT_BUDGET_SECS") {
        Ok(raw) => match raw.trim().parse::<u32>() {
            Ok(n) => clamp_starvation_wait_budget_secs(n),
            Err(_) => DEFAULT,
        },
        Err(_) => DEFAULT,
    }
}

/// The pure bound [`starvation_wait_budget_secs_from_env`] applies to an already-parsed value:
/// `0` is the documented DISABLE LEVER and passes through as-is (not a clamp target — see that
/// function's doc); any other value clamps to `[.., 300]` (codex-lb's `MAX=300s`; there is no
/// explicit floor here besides `0` itself, since `0` is a distinct, meaningful branch). Extracted
/// for the same one-source-of-truth reason as [`clamp_max_account_attempts`].
pub fn clamp_starvation_wait_budget_secs(raw: u32) -> u32 {
    const MAX: u32 = 300;
    match raw {
        0 => 0,
        n if n > MAX => MAX,
        n => n,
    }
}

/// B5 Task 5: `POLYFLARE_STARVATION_HEARTBEAT_SECS` — the keepalive tick interval during a Layer 2
/// wait. Unset ⇒ `10` (the plan's Task 4 default, matching codex-lb's `retry.py` HEARTBEAT); a
/// malformed value resolves to the same default as unset (`10`); EVERY value (default, well-formed,
/// or the malformed fallback) is then clamped to `[1, budget_secs.max(1)]` — the heartbeat can
/// never exceed the wait budget it ticks within (a heartbeat longer than the budget would mean the
/// client could see ZERO keepalives before the wait's own budget-exceeded outcome fires), and can
/// never be `0` (a `0`-second heartbeat has no sane reading of its own, unlike the wait budget's
/// `0` — see [`starvation_wait_budget_secs_from_env`]'s doc for why THAT `0` is different). Takes
/// the ALREADY-resolved `budget_secs` as a parameter — call this AFTER
/// [`starvation_wait_budget_secs_from_env`], never independently, so the clamp is against the real
/// resolved budget rather than an unresolved env read.
pub fn starvation_heartbeat_secs_from_env(budget_secs: u32) -> u32 {
    const DEFAULT: u32 = 10;
    let raw = match std::env::var("POLYFLARE_STARVATION_HEARTBEAT_SECS") {
        Ok(s) => s.trim().parse::<u32>().unwrap_or(DEFAULT),
        Err(_) => DEFAULT,
    };
    clamp_starvation_heartbeat_secs(raw, budget_secs)
}

/// The pure bound [`starvation_heartbeat_secs_from_env`] applies to an already-parsed value:
/// `[1, budget_secs.max(1)]` — the heartbeat can never be `0` (no sane reading of its own) and
/// can never exceed the (already-resolved) wait budget it ticks within. Cross-field: takes
/// `budget_secs` as a second argument rather than reading it from env, so callers (boot AND the
/// later settings PATCH handler) must resolve/validate the budget first, mirroring
/// [`starvation_heartbeat_secs_from_env`]'s own "call this AFTER the budget" contract.
pub fn clamp_starvation_heartbeat_secs(raw: u32, budget_secs: u32) -> u32 {
    raw.clamp(1, budget_secs.max(1))
}

/// Stream-idle-timeout plan (`docs/superpowers/plans/2026-07-18-stream-idle-timeout.md`) Task 2:
/// `POLYFLARE_STREAM_IDLE_TIMEOUT_SECS` — the per-response mid-stream idle deadline
/// (`crate::watchdog::ObservingStream`'s `IdleDeadline`; Task 1's mechanism). Unset ⇒
/// `crate::ingress::DEFAULT_STREAM_IDLE_TIMEOUT.as_secs()` (`300`, matching codex's own
/// `stream_idle_timeout` default — see that constant's doc for the single-source-of-truth
/// rationale).
///
/// **`0` is the documented DISABLE LEVER, not a clamp target** — mirrors
/// [`starvation_wait_budget_secs_from_env`]'s `0` semantics (a sane, useful, unambiguous reading
/// of its own: "never bound a stall — behave exactly as before this feature"), NOT
/// [`max_account_attempts_from_env`]'s `0` (which floors UP to `1` because a failover loop with
/// zero attempts has no sane reading). Here, `0` resolves to `Duration::ZERO` downstream, which
/// `crate::watchdog::IdleDeadline::new` reads as `Disabled` — Task 1's byte-for-byte pre-fix
/// behavior (bare `Poll::Pending` on inner silence, no deadline). This is the plan's Global
/// Constraints "Disable lever" bullet, verbatim.
///
/// A malformed value (non-numeric, negative, or out of `u64` range) resolves to the SAME default
/// as unset (`300`) — never silently collapsed to `0` (disabled), which would be a **safety**
/// regression (a typo would silently remove the proxy-wide stall bound). Identical rationale to
/// `starvation_wait_budget_secs_from_env`'s malformed handling.
///
/// A well-formed value above `MAX` (`3600`s = 1h — an absurd upper bound no genuine upstream
/// response should ever legitimately need to stay silent for) clamps DOWN to `MAX` — never
/// silently accepted as-is, never treated as malformed.
pub fn stream_idle_timeout_secs_from_env() -> u64 {
    let default = crate::ingress::DEFAULT_STREAM_IDLE_TIMEOUT.as_secs();
    match std::env::var("POLYFLARE_STREAM_IDLE_TIMEOUT_SECS") {
        Ok(raw) => match raw.trim().parse::<u64>() {
            Ok(n) => clamp_stream_idle_timeout_secs(n),
            Err(_) => default,
        },
        Err(_) => default,
    }
}

/// The pure bound [`stream_idle_timeout_secs_from_env`] applies to an already-parsed value: `0` is
/// the documented DISABLE LEVER and passes through as-is (not a clamp target — see that function's
/// doc); any other value clamps to `[.., 3600]` (1h — an absurd upper bound no genuine upstream
/// response should ever legitimately need to stay silent for). Extracted for the same
/// one-source-of-truth reason as [`clamp_max_account_attempts`].
pub fn clamp_stream_idle_timeout_secs(raw: u64) -> u64 {
    const MAX: u64 = 3600;
    match raw {
        0 => 0,
        n if n > MAX => MAX,
        n => n,
    }
}

/// B10 Task 1 (THE CRUX): `POLYFLARE_STARVATION_WAKE_JITTER_MS` — the per-waiter wake-jitter
/// window (`crate::ingress::wake_jitter_offset_ms`), which desynchronizes concurrent Layer 2
/// waiters on the same recovering account (see that function's doc + the B10 plan's Global
/// Constraints). Unset ⇒ `0` — **the default IS the disable lever** (unlike
/// [`starvation_wait_budget_secs_from_env`], where `0` is a distinct branch from a non-zero
/// default; here they're literally the same value, since `0` naturally means "no jitter", i.e.
/// today's exact pre-B10 behavior — there is no separate "sane default > 0" the plan calls for).
/// A well-formed, in-range value resolves to itself. A well-formed value ABOVE `MAX` (`30_000`ms —
/// an absolute ceiling so a hostile/huge value can't blow the B5 wait budget) clamps DOWN to
/// `MAX`, never silently accepted as-is. A MALFORMED value (non-numeric, negative, or out of `u64`
/// range) resolves to the SAME default as unset (`0`) — never a boot crash; identical rationale to
/// every other malformed-input decision in this module.
pub fn wake_jitter_ms_from_env() -> u64 {
    const DEFAULT: u64 = 0;
    match std::env::var("POLYFLARE_STARVATION_WAKE_JITTER_MS") {
        Ok(raw) => match raw.trim().parse::<u64>() {
            Ok(n) => clamp_wake_jitter_ms(n),
            Err(_) => DEFAULT,
        },
        Err(_) => DEFAULT,
    }
}

/// The pure bound [`wake_jitter_ms_from_env`] applies to an already-parsed value: clamps to
/// `[.., 30_000]`ms (an absolute ceiling so a hostile/huge value can't blow the B5 wait budget);
/// `0` has no special case here — unlike the other knobs' `0`, it's naturally the value that falls
/// out of "no jitter", not a distinct disable branch. Extracted for the same one-source-of-truth
/// reason as [`clamp_max_account_attempts`].
pub fn clamp_wake_jitter_ms(raw: u64) -> u64 {
    const MAX: u64 = 30_000;
    raw.min(MAX)
}

/// B8 Task 3: `POLYFLARE_SOFT_DRAIN_ENABLED` — the codex-lb `soft_drain_enabled` disable lever for
/// the usage-driven health-tier evaluation (`crate::runtime_state::RuntimeStates::evaluate_with_usage`,
/// called from `crate::usage_refresh::refresh_account`). Unlike every other bool flag in this
/// module (`live_logs`/`ws_upstream`/`allow_unauthenticated_remote`), this one defaults to **ON**
/// — codex-lb's own `soft_drain_enabled` defaults to `true` (plan Global Constraints: "Default on
/// (codex-lb `soft_drain_enabled`=true)"), and B8's whole point is that soft-drain preference is
/// active out of the box; an operator opts OUT, not in.
///
/// - Unset ⇒ `true` (the default — matches `max_account_attempts_from_env`'s "malformed/absent
///   converges on the safe default" convention, except here the *safe* default is ON since
///   soft-drain is a preference-only signal that can never override eligibility/security-floor/
///   continuity ownership; see the plan's Global Constraints).
/// - `"0"` / `"false"` / `"no"` / `"off"` (case-insensitive, trimmed) ⇒ `false` — the explicit
///   disable lever.
/// - Anything else (including a malformed/typo'd value) ⇒ `true` — a typo must never silently
///   disable a preference-only, safety-neutral signal; it just falls back to the default like
///   every other flag in this module treats a malformed value.
///
/// **Caveat (B8 review, not fixed here — see [`crate::app::AppState::soft_drain_enabled`]):** this
/// flag is honored ONLY by the usage-refresh poller (`crate::usage_refresh`). The per-request funnel
/// (`crate::runtime_state::RuntimeStates::record_transient_error`/`record_rate_limit`) does not read
/// it at all, so with the flag off an account can still transiently show DRAINING between poller
/// cycles (bounded to ≤600s) before the next tick resets it. "Clean rollback" therefore means
/// steady-state parity with pre-B8 behavior, not that soft-drain never fires while the flag is off.
pub fn soft_drain_enabled_from_env() -> bool {
    match std::env::var("POLYFLARE_SOFT_DRAIN_ENABLED") {
        Ok(raw) => !matches!(
            raw.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "no" | "off"
        ),
        Err(_) => true,
    }
}

/// C9 Task 3: `POLYFLARE_INFLIGHT_PENALTY_PCT` — the in-flight soft-penalty pct threaded onto
/// `SelectionCtx` and folded by `polyflare_core::select`'s `eligibility()` into
/// `eff_used`/`eff_secondary_used` as `in_flight * penalty_pct` (mirroring codex-lb
/// `load_balancer.py:2251-2263`'s `inflight_pressure_pct = in_flight * 2.5`). Unset ⇒ `2.5`
/// (codex-lb's own value — the default IS "on"). A well-formed, non-negative value is clamped to
/// `[0, MAX]` (`50.0` — an absolute ceiling so a hostile/huge value can't single-handedly clamp
/// every account's `eff_used` to 100 off a single in-flight request). A malformed value
/// (non-numeric, NaN, or out-of-`f64`-range) resolves to the SAME default as unset (`2.5`) —
/// never a boot crash, identical rationale to every other malformed-input decision in this module.
///
/// **`0` is the documented DISABLE LEVER, not a clamp target** (same shape as
/// [`starvation_wait_budget_secs_from_env`]'s `0`): an explicit, well-formed `0` resolves to
/// `0.0`, under which `in_flight * 0.0 == 0.0` for any `in_flight` — a clean, byte-for-byte
/// rollback to pre-C9 selection scoring (in_flight is still tracked, Tasks 1-2, it's just never
/// folded into the weight).
pub fn inflight_penalty_pct_from_env() -> f64 {
    const DEFAULT: f64 = 2.5;
    match std::env::var("POLYFLARE_INFLIGHT_PENALTY_PCT") {
        Ok(raw) => match raw.trim().parse::<f64>() {
            Ok(n) => clamp_inflight_penalty_pct(n),
            Err(_) => DEFAULT,
        },
        Err(_) => DEFAULT,
    }
}

/// The pure bound [`inflight_penalty_pct_from_env`] applies to an already-parsed value: `NaN` (a
/// value that parses successfully as a float — e.g. the literal string `"NaN"` — but is not a
/// number) maps to the SAME `2.5` default as a parse failure, since NaN comparisons are never
/// meaningfully "in range" (this is why the NaN check must live IN the clamp, not the caller's
/// `Err(_)` arm — NaN is an `Ok(n)`, not an `Err`); a negative value floors to `0.0`; a value above
/// `MAX` (`50.0` — an absolute ceiling so a hostile/huge value can't single-handedly clamp every
/// account's `eff_used` to 100 off a single in-flight request) clamps down to `MAX`; everything
/// else passes through unchanged, including the explicit `0.0` disable lever. Extracted for the
/// same one-source-of-truth reason as [`clamp_max_account_attempts`].
pub fn clamp_inflight_penalty_pct(raw: f64) -> f64 {
    const DEFAULT: f64 = 2.5;
    const MAX: f64 = 50.0;
    if raw.is_nan() {
        DEFAULT
    } else {
        // Safe to `.clamp()` here — the NaN case (the only input `clamp` itself would mishandle)
        // is already routed to `DEFAULT` above, and `0.0 <= MAX` is a fixed, valid range.
        raw.clamp(0.0, MAX)
    }
}

/// C12 Task 3: `POLYFLARE_REQUEST_LOG_RETENTION_DAYS` — how many days of `request_log` rows to
/// keep before `crate::retention::run_retention_pass` age-prunes them (batched, hourly; see
/// `crate::retention`). Unset ⇒ `0` — **disabled is the default**, unlike most numeric knobs in
/// this module: retention pruning is a destructive, irreversible background DELETE, so an operator
/// must opt IN explicitly rather than an absent env var accidentally enabling data loss on a fresh
/// deploy (mirrors the plan's Global Constraints: "Disabled by default; disable lever").
///
/// A well-formed value is clamped to `[0, 3650]` (10 years — an absolute ceiling; the field is
/// `u32` and days-since-epoch multiplied by 86400 must stay a sane `i64`, but the real reason for
/// the cap is that any larger number is not a meaningful "retention window" for an append-only log
/// table). A MALFORMED value (non-numeric, negative, or out of `u32` range) resolves to `0`
/// (disabled) — the SAME fail-safe rationale as every malformed-input decision in this module, but
/// inverted from the usual "malformed ⇒ safe non-zero default" shape: here `0` (disabled) IS the
/// safe value, so a typo can never silently turn ON an irreversible bulk-delete background task.
pub fn request_log_retention_days_from_env() -> u32 {
    retention_days_from_env("POLYFLARE_REQUEST_LOG_RETENTION_DAYS")
}

/// C12 Task 3: `POLYFLARE_USAGE_HISTORY_RETENTION_DAYS` — how many days of `usage_history` rows to
/// keep before `crate::retention::run_retention_pass` age-prunes them, ALWAYS protecting the latest
/// row per `(account_id, window)` regardless of age (`AccountRepo::prune_usage_history_older_than`
/// — the routing gate + dashboard depend on each account's last-known sample). Same
/// default/clamp/malformed semantics as [`request_log_retention_days_from_env`] — see that
/// function's doc for the full "disabled is the default, malformed ⇒ 0 not a safe non-zero
/// default" rationale, which applies here identically.
pub fn usage_history_retention_days_from_env() -> u32 {
    retention_days_from_env("POLYFLARE_USAGE_HISTORY_RETENTION_DAYS")
}

/// Shared parse/clamp for the two C12 retention-days knobs (see the two `pub fn`s above): unset or
/// malformed ⇒ `0` (disabled, fail-safe); a well-formed value clamps via [`clamp_retention_days`].
fn retention_days_from_env(var: &str) -> u32 {
    match std::env::var(var) {
        Ok(raw) => match raw.trim().parse::<u32>() {
            Ok(n) => clamp_retention_days(n),
            Err(_) => 0,
        },
        Err(_) => 0,
    }
}

/// The shared pure bound both C12 retention-days knobs apply to an already-parsed value: clamps to
/// `[0, 3650]` (10 years — an absolute ceiling; the field is `u32` and days-since-epoch multiplied
/// by 86400 must stay a sane `i64`, but the real reason for the cap is that any larger number is not
/// a meaningful "retention window" for an append-only log table). `0` has no special case — it's
/// already the floor, so it just passes through. Private: [`clamp_request_log_retention_days`]/
/// [`clamp_usage_history_retention_days`] below are the public, field-named entry points the (later)
/// settings PATCH handler validates against — same bound, two names, matching the two `*_from_env`
/// knobs above.
fn clamp_retention_days(raw: u32) -> u32 {
    const MAX: u32 = 3650;
    raw.min(MAX)
}

/// Field-named public entry point for [`request_log_retention_days_from_env`]'s bound — see
/// [`clamp_retention_days`] for the shared logic both retention-days knobs apply.
pub fn clamp_request_log_retention_days(raw: u32) -> u32 {
    clamp_retention_days(raw)
}

/// Field-named public entry point for [`usage_history_retention_days_from_env`]'s bound — see
/// [`clamp_retention_days`] for the shared logic both retention-days knobs apply.
pub fn clamp_usage_history_retention_days(raw: u32) -> u32 {
    clamp_retention_days(raw)
}

/// D15 Task 3: `POLYFLARE_MODEL_CATALOG_TTL_SECS` — the live upstream model-catalog cache's
/// refresh TTL (`crate::model_catalog::ModelCatalogCache::refresh_interval`, mirrors
/// `CodexVersionCache`'s TTL). Unset ⇒ `3600` (an hour — the model catalog changes rarely, so a
/// generous default keeps upstream traffic low without ever going stale for long). A well-formed
/// value is clamped to `[60, 86400]` (a floor so a hostile/typo'd tiny value can't hammer
/// upstream every request-adjacent tick; a ceiling of 24h so the cache can never go THAT stale). A
/// MALFORMED value (non-numeric, negative, or out of `u64` range) resolves to the SAME default as
/// unset (`3600`) — identical fail-safe rationale to every other malformed-input decision in this
/// module: a typo must never silently produce a surprising cadence.
pub fn model_catalog_ttl_secs_from_env() -> u64 {
    const DEFAULT: u64 = 3600;
    const MIN: u64 = 60;
    const MAX: u64 = 86400;
    match std::env::var("POLYFLARE_MODEL_CATALOG_TTL_SECS") {
        Ok(raw) => match raw.trim().parse::<u64>() {
            Ok(n) if n < MIN => MIN,
            Ok(n) if n > MAX => MAX,
            Ok(n) => n,
            Err(_) => DEFAULT,
        },
        Err(_) => DEFAULT,
    }
}

/// D15 Task 3: `POLYFLARE_MODEL_CATALOG_ENABLED` — the live upstream model-catalog fetch's disable
/// lever. Defaults to **ON** (mirrors [`soft_drain_enabled_from_env`]'s "default on, opt OUT not
/// IN" convention exactly, for the identical reason: the fallback ladder
/// (`crate::model_catalog::ModelCatalogCache`) is airtight — disable/no-accounts/fetch-failure all
/// degrade to today's static-floor `/models` behavior, so there is no safety reason to ship this
/// off by default).
///
/// - Unset ⇒ `true`.
/// - `"0"` / `"false"` / `"no"` / `"off"` (case-insensitive, trimmed) ⇒ `false` — the explicit
///   disable lever; `serve` then builds `AppState.model_catalog` via
///   `crate::model_catalog::floor_only_model_catalog()` and never spawns the background-warm task,
///   a clean, total rollback to pre-D15 static-only `/models`.
/// - Anything else (including a malformed/typo'd value) ⇒ `true` — a typo must never silently
///   disable a fetch whose every failure mode already degrades safely.
pub fn model_catalog_enabled_from_env() -> bool {
    match std::env::var("POLYFLARE_MODEL_CATALOG_ENABLED") {
        Ok(raw) => !matches!(
            raw.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "no" | "off"
        ),
        Err(_) => true,
    }
}

impl ServeConfig {
    pub fn from_env() -> Result<Self, String> {
        let bind_addr =
            std::env::var("POLYFLARE_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
        let upstream_base_url = std::env::var("POLYFLARE_UPSTREAM_URL")
            .unwrap_or_else(|_| DEFAULT_CODEX_UPSTREAM_URL.to_string());
        let anthropic_upstream_base_url = std::env::var("POLYFLARE_ANTHROPIC_UPSTREAM_URL")
            .unwrap_or_else(|_| DEFAULT_ANTHROPIC_UPSTREAM_URL.to_string());
        let auth_base_url =
            std::env::var("POLYFLARE_AUTH_URL").unwrap_or_else(|_| DEFAULT_AUTH_URL.to_string());
        let data_dir = data_dir_from_env();
        let continuity_watchdog = std::env::var("POLYFLARE_WATCHDOG_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(30));
        // M5 capture-fixture mechanism: unset (the default) ⇒ `None` ⇒ ingress never touches
        // `fingerprint_capture` at all. Mirrors `POLYFLARE_ANTHROPIC_UPSTREAM_URL` above, but
        // absence means "disabled" rather than falling back to a default value.
        let capture_fingerprint_path = std::env::var("POLYFLARE_CAPTURE_FINGERPRINT")
            .ok()
            .map(PathBuf::from);
        // Routing strategy: global default + optional per-pool overrides. Unknown names fail fast.
        let routing_strategy = match std::env::var("POLYFLARE_ROUTING_STRATEGY") {
            Ok(s) => RoutingStrategy::parse(&s)
                .ok_or_else(|| format!("POLYFLARE_ROUTING_STRATEGY: unknown strategy '{s}'"))?,
            Err(_) => RoutingStrategy::default(),
        };
        let pool_strategies = match std::env::var("POLYFLARE_POOL_STRATEGY") {
            Ok(raw) => parse_pool_strategies(&raw)?,
            Err(_) => HashMap::new(),
        };
        let admin_token = std::env::var("POLYFLARE_ADMIN_TOKEN").ok();
        let live_logs = matches!(
            std::env::var("POLYFLARE_LIVE_LOGS").as_deref(),
            Ok("1") | Ok("true")
        );
        // M5a: same fail-safe-default convention as `live_logs` above — any unset/empty/unrecognized
        // value is treated as OFF (never a startup error), so a malformed env var degrades to
        // today's HTTP-SSE behavior rather than failing to boot.
        let ws_upstream = matches!(
            std::env::var("POLYFLARE_WS_UPSTREAM").as_deref(),
            Ok("1") | Ok("true")
        );
        // WS-downstream relay plan Task 2: same fail-safe-default convention as `ws_upstream` above —
        // any unset/empty/unrecognized value is OFF (never a startup error), so a malformed env var
        // degrades to today's `426`-on-the-WS-GET behavior rather than failing to boot. Only an
        // explicit `1`/`true` engages the WS-relay accept path (`crate::ws_relay`).
        let ws_downstream = matches!(
            std::env::var("POLYFLARE_WS_DOWNSTREAM").as_deref(),
            Ok("1") | Ok("true")
        );
        // Opt-in client keepalive pings. Same fail-safe-default convention as `ws_upstream` above:
        // any unset/empty/unrecognized value is OFF, which is the codex-rs-faithful default (no
        // client-initiated ping). Only an explicit `1`/`true` opts into the codex-lb-style keepalive
        // (a deliberate, documented fingerprint divergence for aggressive-NAT/middlebox deployments).
        let ws_client_ping = matches!(
            std::env::var("POLYFLARE_WS_CLIENT_PING").as_deref(),
            Ok("1") | Ok("true")
        );
        // D18 Task 4: same fail-safe-default convention as `live_logs`/`ws_upstream` above — any
        // unset/empty/unrecognized value is treated as OFF (never a startup error on its own; the
        // consequence of leaving it off on a non-loopback bind with no keys is a REFUSE-TO-START
        // from `crate::posture::resolve_proxy_enforcement`, resolved further down in `serve()`).
        let allow_unauthenticated_remote = matches!(
            std::env::var("POLYFLARE_ALLOW_UNAUTHENTICATED_REMOTE").as_deref(),
            Ok("1")
        );
        let max_account_attempts = max_account_attempts_from_env();
        let starvation_wait_budget_secs = starvation_wait_budget_secs_from_env();
        let starvation_heartbeat_secs =
            starvation_heartbeat_secs_from_env(starvation_wait_budget_secs);
        let starvation_wait_budget = Duration::from_secs(starvation_wait_budget_secs as u64);
        let starvation_heartbeat = Duration::from_secs(starvation_heartbeat_secs as u64);
        let stream_idle_timeout = Duration::from_secs(stream_idle_timeout_secs_from_env());
        let soft_drain_enabled = soft_drain_enabled_from_env();
        let wake_jitter_ms = wake_jitter_ms_from_env();
        let inflight_penalty_pct = inflight_penalty_pct_from_env();
        let request_log_retention_days = request_log_retention_days_from_env();
        let usage_history_retention_days = usage_history_retention_days_from_env();
        let model_catalog_ttl_secs = model_catalog_ttl_secs_from_env();
        let model_catalog_enabled = model_catalog_enabled_from_env();
        Ok(ServeConfig {
            bind_addr,
            upstream_base_url,
            anthropic_upstream_base_url,
            auth_base_url,
            db_path: db_path(&data_dir),
            key_path: key_path(&data_dir),
            continuity_watchdog,
            capture_fingerprint_path,
            routing_strategy,
            pool_strategies,
            admin_token,
            live_logs,
            ws_upstream,
            ws_downstream,
            ws_client_ping,
            max_account_attempts,
            starvation_wait_budget,
            starvation_heartbeat,
            allow_unauthenticated_remote,
            stream_idle_timeout,
            soft_drain_enabled,
            wake_jitter_ms,
            inflight_penalty_pct,
            request_log_retention_days,
            usage_history_retention_days,
            model_catalog_ttl_secs,
            model_catalog_enabled,
        })
    }
}

/// The PolyFlare data directory: `$POLYFLARE_DATA_DIR`, else `$HOME/.polyflare`.
pub fn data_dir_from_env() -> PathBuf {
    if let Ok(dir) = std::env::var("POLYFLARE_DATA_DIR") {
        return PathBuf::from(dir);
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join(".polyflare")
}

/// The store DB path within a data directory.
pub fn db_path(data_dir: &Path) -> PathBuf {
    data_dir.join("store.db")
}

/// The at-rest key file path within a data directory (raw 32 bytes).
pub fn key_path(data_dir: &Path) -> PathBuf {
    data_dir.join("key")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_pool_strategies_reads_pairs_and_rejects_bad_input() {
        let m = parse_pool_strategies("a=fill_first, b = cache_affinity_tier ,").unwrap();
        assert_eq!(m.get("a"), Some(&RoutingStrategy::FillFirst));
        assert_eq!(m.get("b"), Some(&RoutingStrategy::CacheAffinityTier));
        assert_eq!(m.len(), 2, "trailing empty segment skipped");
        assert!(
            parse_pool_strategies("a=bogus").is_err(),
            "unknown strategy → err"
        );
        assert!(
            parse_pool_strategies("noequals").is_err(),
            "malformed pair → err"
        );
        assert!(
            parse_pool_strategies("=fill_first").is_err(),
            "empty slug → err"
        );
        assert!(parse_pool_strategies("").unwrap().is_empty());
    }

    #[test]
    fn parse_pool_capabilities_reads_pairs_and_rejects_bad_input() {
        let m = parse_pool_capabilities("cyber:security_work, other : some_cap ,").unwrap();
        assert_eq!(m.get("cyber"), Some(&"security_work".to_string()));
        assert_eq!(m.get("other"), Some(&"some_cap".to_string()));
        assert_eq!(m.len(), 2, "trailing empty segment skipped");
        assert!(
            parse_pool_capabilities("nocolon").is_err(),
            "malformed pair → err"
        );
        assert!(
            parse_pool_capabilities(":security_work").is_err(),
            "empty slug → err"
        );
        assert!(
            parse_pool_capabilities("cyber:").is_err(),
            "empty capability → err"
        );
        assert!(parse_pool_capabilities("").unwrap().is_empty());
    }

    /// Serializes tests in this module that mutate `POLYFLARE_POOL_CAPABILITIES` — env vars are
    /// process-global, and Rust's test harness runs `#[test]` fns on separate threads in the same
    /// process by default. Mirrors the guard pattern in
    /// `tests/cyber_pool_capability_resolution.rs::env_lock` (that file uses `tokio::sync::Mutex`
    /// because its guard is held across `.await`; these tests are synchronous, so a plain
    /// `std::sync::Mutex` suffices — and lock poisoning is recovered rather than propagated so one
    /// failing test's guard drop can't cascade-fail the others).
    fn pool_capabilities_env_lock() -> &'static std::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
    }

    /// TA6(b) Task 5 review finding: `pool_requires_capability` used to fail open *silently* on a
    /// malformed `POLYFLARE_POOL_CAPABILITIES`. The fix adds a warn-once `tracing::warn!`, but the
    /// FAIL-OPEN BEHAVIOR itself is unchanged — this test proves that: a malformed value still
    /// resolves to `false` (never panics, never spuriously requires the capability). The warn-once
    /// emission itself isn't asserted here: capturing/asserting on global `tracing` subscriber
    /// output from a unit test would need a custom subscriber wired in just for this, which is more
    /// machinery than this visibility-only, non-safety-relevant signal warrants (the doc comment on
    /// `pool_requires_capability` documents the log-once contract instead). Calling the function
    /// twice back-to-back exercises the `Once` guard without observing its output, confirming it
    /// doesn't alter the return value or panic on a second hit.
    #[test]
    fn pool_requires_capability_malformed_value_fails_open_without_panicking() {
        let _guard = pool_capabilities_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_POOL_CAPABILITIES", "not-a-valid-pair");
        }

        assert!(!pool_requires_capability(
            Some("cyber"),
            SECURITY_WORK_CAPABILITY
        ));
        // Second call: same malformed value, `Once` already fired — must still fail open cleanly.
        assert!(!pool_requires_capability(
            Some("cyber"),
            SECURITY_WORK_CAPABILITY
        ));

        unsafe {
            std::env::remove_var("POLYFLARE_POOL_CAPABILITIES");
        }
    }

    /// The well-formed path is unaffected by the warn-once addition: a valid
    /// `POLYFLARE_POOL_CAPABILITIES` still resolves exactly as before (tagged pool + matching
    /// capability ⇒ true; untagged pool, mismatched capability, or `pool: None` ⇒ false).
    #[test]
    fn pool_requires_capability_valid_value_resolves_correctly() {
        let _guard = pool_capabilities_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_POOL_CAPABILITIES", "cyber:security_work");
        }

        assert!(pool_requires_capability(
            Some("cyber"),
            SECURITY_WORK_CAPABILITY
        ));
        assert!(!pool_requires_capability(
            Some("general"),
            SECURITY_WORK_CAPABILITY
        ));
        assert!(!pool_requires_capability(
            Some("cyber"),
            "some_other_capability"
        ));
        assert!(!pool_requires_capability(None, SECURITY_WORK_CAPABILITY));

        unsafe {
            std::env::remove_var("POLYFLARE_POOL_CAPABILITIES");
        }
    }

    /// Serializes tests in this module that mutate `POLYFLARE_MAX_ACCOUNT_ATTEMPTS` — same
    /// rationale as `pool_capabilities_env_lock` (env vars are process-global, tests run
    /// concurrently on separate threads by default).
    fn max_account_attempts_env_lock() -> &'static std::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
    }

    /// B4/B5 Task 5: `POLYFLARE_MAX_ACCOUNT_ATTEMPTS=5` resolves to exactly `5`.
    #[test]
    fn max_account_attempts_reads_a_well_formed_value() {
        let _guard = max_account_attempts_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_MAX_ACCOUNT_ATTEMPTS", "5");
        }
        assert_eq!(max_account_attempts_from_env(), 5);
        unsafe {
            std::env::remove_var("POLYFLARE_MAX_ACCOUNT_ATTEMPTS");
        }
    }

    /// Unset ⇒ the documented default, 3.
    #[test]
    fn max_account_attempts_unset_defaults_to_three() {
        let _guard = max_account_attempts_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("POLYFLARE_MAX_ACCOUNT_ATTEMPTS");
        }
        assert_eq!(max_account_attempts_from_env(), 3);
    }

    /// A malformed value (non-numeric) ⇒ the SAME safe default as unset (3) — never a boot crash,
    /// and never silently collapsed to the one-shot value 1. See the function's doc for the full
    /// justification of this decision.
    #[test]
    fn max_account_attempts_malformed_defaults_to_three_not_one() {
        let _guard = max_account_attempts_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_MAX_ACCOUNT_ATTEMPTS", "not-a-number");
        }
        assert_eq!(max_account_attempts_from_env(), 3);
        unsafe {
            std::env::remove_var("POLYFLARE_MAX_ACCOUNT_ATTEMPTS");
        }
    }

    /// An explicit `0` is clamped UP to the floor of 1 (a request must attempt at least once) —
    /// never treated as malformed-hence-3, and never left at 0 (which would disable the request
    /// path entirely).
    #[test]
    fn max_account_attempts_zero_clamps_to_one_not_three() {
        let _guard = max_account_attempts_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_MAX_ACCOUNT_ATTEMPTS", "0");
        }
        assert_eq!(max_account_attempts_from_env(), 1);
        unsafe {
            std::env::remove_var("POLYFLARE_MAX_ACCOUNT_ATTEMPTS");
        }
    }

    /// Serializes tests in this module that mutate `POLYFLARE_STARVATION_WAIT_BUDGET_SECS`/
    /// `POLYFLARE_STARVATION_HEARTBEAT_SECS` — same rationale as the other env-lock helpers above.
    fn starvation_env_lock() -> &'static std::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
    }

    /// B5 Task 5: `POLYFLARE_STARVATION_WAIT_BUDGET_SECS=120` resolves to exactly `120` (within the
    /// `[1, 300]` clamp range — unchanged).
    #[test]
    fn starvation_wait_budget_reads_a_well_formed_in_range_value() {
        let _guard = starvation_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_STARVATION_WAIT_BUDGET_SECS", "120");
        }
        assert_eq!(starvation_wait_budget_secs_from_env(), 120);
        unsafe {
            std::env::remove_var("POLYFLARE_STARVATION_WAIT_BUDGET_SECS");
        }
    }

    /// Unset ⇒ the documented default, 60.
    #[test]
    fn starvation_wait_budget_unset_defaults_to_sixty() {
        let _guard = starvation_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("POLYFLARE_STARVATION_WAIT_BUDGET_SECS");
        }
        assert_eq!(starvation_wait_budget_secs_from_env(), 60);
    }

    /// `500` (above codex-lb's `MAX=300`) clamps DOWN to 300 — never silently accepted, never
    /// treated as malformed.
    #[test]
    fn starvation_wait_budget_above_max_clamps_to_three_hundred() {
        let _guard = starvation_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_STARVATION_WAIT_BUDGET_SECS", "500");
        }
        assert_eq!(starvation_wait_budget_secs_from_env(), 300);
        unsafe {
            std::env::remove_var("POLYFLARE_STARVATION_WAIT_BUDGET_SECS");
        }
    }

    /// `0` is the DISABLE LEVER — resolves to exactly `0`, distinct from BOTH the default (60) and
    /// the clamp floor (1). See the function's doc for the full justification of this decision.
    #[test]
    fn starvation_wait_budget_zero_is_the_disable_lever_not_a_clamp_to_one() {
        let _guard = starvation_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_STARVATION_WAIT_BUDGET_SECS", "0");
        }
        assert_eq!(starvation_wait_budget_secs_from_env(), 0);
        unsafe {
            std::env::remove_var("POLYFLARE_STARVATION_WAIT_BUDGET_SECS");
        }
    }

    /// A malformed value (non-numeric) ⇒ the SAME safe default as unset (60) — never a boot crash,
    /// and never silently collapsed to the disable lever (0), which would be a safety regression.
    #[test]
    fn starvation_wait_budget_malformed_defaults_to_sixty_not_zero() {
        let _guard = starvation_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_STARVATION_WAIT_BUDGET_SECS", "not-a-number");
        }
        assert_eq!(starvation_wait_budget_secs_from_env(), 60);
        unsafe {
            std::env::remove_var("POLYFLARE_STARVATION_WAIT_BUDGET_SECS");
        }
    }

    /// Unset heartbeat, against the default budget (60) ⇒ the documented default, 10 (well within
    /// the `[1, 60]` clamp range).
    #[test]
    fn starvation_heartbeat_unset_defaults_to_ten() {
        let _guard = starvation_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("POLYFLARE_STARVATION_HEARTBEAT_SECS");
        }
        assert_eq!(starvation_heartbeat_secs_from_env(60), 10);
    }

    /// A heartbeat larger than the (already-resolved) budget clamps DOWN to the budget — a
    /// heartbeat that never fires before the wait ends would defeat its purpose.
    #[test]
    fn starvation_heartbeat_larger_than_budget_clamps_down_to_budget() {
        let _guard = starvation_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_STARVATION_HEARTBEAT_SECS", "50");
        }
        assert_eq!(
            starvation_heartbeat_secs_from_env(5),
            5,
            "clamped down to the 5s budget, not left at 50"
        );
        unsafe {
            std::env::remove_var("POLYFLARE_STARVATION_HEARTBEAT_SECS");
        }
    }

    /// An explicit `0` heartbeat clamps UP to the floor of 1 — never `0` (unlike the wait budget's
    /// `0`, which has its own dedicated disable-lever meaning; a `0`-second heartbeat does not).
    #[test]
    fn starvation_heartbeat_zero_clamps_to_one_not_left_at_zero() {
        let _guard = starvation_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_STARVATION_HEARTBEAT_SECS", "0");
        }
        assert_eq!(starvation_heartbeat_secs_from_env(60), 1);
        unsafe {
            std::env::remove_var("POLYFLARE_STARVATION_HEARTBEAT_SECS");
        }
    }

    /// A malformed heartbeat ⇒ the same default as unset (10), then clamped identically to a
    /// well-formed one.
    #[test]
    fn starvation_heartbeat_malformed_defaults_to_ten_then_clamps() {
        let _guard = starvation_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_STARVATION_HEARTBEAT_SECS", "nope");
        }
        assert_eq!(starvation_heartbeat_secs_from_env(60), 10);
        assert_eq!(
            starvation_heartbeat_secs_from_env(5),
            5,
            "malformed default (10) still clamps down against a smaller budget"
        );
        unsafe {
            std::env::remove_var("POLYFLARE_STARVATION_HEARTBEAT_SECS");
        }
    }

    /// When the wait budget is disabled (`0`), the heartbeat clamp's upper bound floors to `1`
    /// (`budget_secs.max(1)`) rather than collapsing to an invalid `1..=0` range — irrelevant in
    /// practice (Layer 2 never runs when the budget is `0`), but must not panic or produce `0`.
    #[test]
    fn starvation_heartbeat_against_a_disabled_zero_budget_does_not_panic() {
        let _guard = starvation_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("POLYFLARE_STARVATION_HEARTBEAT_SECS");
        }
        assert_eq!(starvation_heartbeat_secs_from_env(0), 1);
    }

    /// Serializes tests in this module that mutate `POLYFLARE_STREAM_IDLE_TIMEOUT_SECS` — same
    /// rationale as the other env-lock helpers above (env vars are process-global, tests run
    /// concurrently on separate threads by default).
    fn stream_idle_timeout_env_lock() -> &'static std::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
    }

    /// Stream-idle-timeout plan Task 2: `POLYFLARE_STREAM_IDLE_TIMEOUT_SECS=30` resolves to
    /// exactly `30`.
    #[test]
    fn stream_idle_timeout_reads_a_well_formed_value() {
        let _guard = stream_idle_timeout_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_STREAM_IDLE_TIMEOUT_SECS", "30");
        }
        assert_eq!(stream_idle_timeout_secs_from_env(), 30);
        unsafe {
            std::env::remove_var("POLYFLARE_STREAM_IDLE_TIMEOUT_SECS");
        }
    }

    /// Unset ⇒ the documented default, `300` (matches codex's `stream_idle_timeout` default —
    /// `crate::ingress::DEFAULT_STREAM_IDLE_TIMEOUT`).
    #[test]
    fn stream_idle_timeout_unset_defaults_to_three_hundred() {
        let _guard = stream_idle_timeout_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("POLYFLARE_STREAM_IDLE_TIMEOUT_SECS");
        }
        assert_eq!(stream_idle_timeout_secs_from_env(), 300);
    }

    /// `0` is the DISABLE LEVER — resolves to exactly `0`, distinct from both the default (300)
    /// and any clamp floor. See the function's doc for the full justification (mirrors
    /// `starvation_wait_budget_secs_from_env`'s `0` decision, not `max_account_attempts_from_env`'s
    /// floor-to-1).
    #[test]
    fn stream_idle_timeout_zero_is_the_disable_lever() {
        let _guard = stream_idle_timeout_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_STREAM_IDLE_TIMEOUT_SECS", "0");
        }
        assert_eq!(stream_idle_timeout_secs_from_env(), 0);
        unsafe {
            std::env::remove_var("POLYFLARE_STREAM_IDLE_TIMEOUT_SECS");
        }
    }

    /// A malformed value (non-numeric) ⇒ the SAME safe default as unset (300) — never a boot
    /// crash, and never silently collapsed to the disable lever (0), which would be a safety
    /// regression (a typo would silently remove the proxy-wide stall bound).
    #[test]
    fn stream_idle_timeout_malformed_defaults_to_three_hundred_not_zero() {
        let _guard = stream_idle_timeout_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_STREAM_IDLE_TIMEOUT_SECS", "not-a-number");
        }
        assert_eq!(stream_idle_timeout_secs_from_env(), 300);
        unsafe {
            std::env::remove_var("POLYFLARE_STREAM_IDLE_TIMEOUT_SECS");
        }
    }

    /// An absurdly large value (above the `3600`s = 1h upper bound) clamps DOWN to `3600` — never
    /// silently accepted as-is, never treated as malformed (unlike the truly non-numeric case
    /// above, which resolves to the default instead).
    #[test]
    fn stream_idle_timeout_absurd_value_clamps_to_one_hour() {
        let _guard = stream_idle_timeout_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_STREAM_IDLE_TIMEOUT_SECS", "999999");
        }
        assert_eq!(stream_idle_timeout_secs_from_env(), 3600);
        unsafe {
            std::env::remove_var("POLYFLARE_STREAM_IDLE_TIMEOUT_SECS");
        }
    }

    /// Serializes tests in this module that mutate `POLYFLARE_SOFT_DRAIN_ENABLED` — same
    /// rationale as the other env-lock helpers above (env vars are process-global, tests run
    /// concurrently on separate threads by default).
    fn soft_drain_enabled_env_lock() -> &'static std::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
    }

    /// B8 Task 3 (e): unset ⇒ `true` — the documented default-ON behavior (soft-drain is an
    /// opt-out, not an opt-in preference signal).
    #[test]
    fn soft_drain_enabled_unset_defaults_to_true() {
        let _guard = soft_drain_enabled_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("POLYFLARE_SOFT_DRAIN_ENABLED");
        }
        assert!(soft_drain_enabled_from_env());
    }

    /// B8 Task 3 (e): `POLYFLARE_SOFT_DRAIN_ENABLED=0` ⇒ `false` — the explicit disable lever.
    #[test]
    fn soft_drain_enabled_zero_resolves_to_false() {
        let _guard = soft_drain_enabled_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_SOFT_DRAIN_ENABLED", "0");
        }
        assert!(!soft_drain_enabled_from_env());
        unsafe {
            std::env::remove_var("POLYFLARE_SOFT_DRAIN_ENABLED");
        }
    }

    /// B8 Task 3 (e): `POLYFLARE_SOFT_DRAIN_ENABLED=false` ⇒ `false` (mirrors the `0` case; both
    /// are documented disable spellings).
    #[test]
    fn soft_drain_enabled_false_string_resolves_to_false() {
        let _guard = soft_drain_enabled_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_SOFT_DRAIN_ENABLED", "false");
        }
        assert!(!soft_drain_enabled_from_env());
        unsafe {
            std::env::remove_var("POLYFLARE_SOFT_DRAIN_ENABLED");
        }
    }

    /// `no`/`off` are also accepted disable spellings, and matching is case-insensitive +
    /// whitespace-tolerant.
    #[test]
    fn soft_drain_enabled_no_and_off_resolve_to_false_case_insensitively() {
        let _guard = soft_drain_enabled_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        for v in ["no", "OFF", " False ", "NO"] {
            unsafe {
                std::env::set_var("POLYFLARE_SOFT_DRAIN_ENABLED", v);
            }
            assert!(!soft_drain_enabled_from_env(), "'{v}' should disable");
        }
        unsafe {
            std::env::remove_var("POLYFLARE_SOFT_DRAIN_ENABLED");
        }
    }

    /// A malformed/unrecognized value never silently disables a safety-neutral preference
    /// signal — it falls back to the default (`true`), matching every other flag in this module's
    /// "typo ⇒ safe default" convention.
    #[test]
    fn soft_drain_enabled_malformed_defaults_to_true() {
        let _guard = soft_drain_enabled_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_SOFT_DRAIN_ENABLED", "not-a-bool");
        }
        assert!(soft_drain_enabled_from_env());
        unsafe {
            std::env::remove_var("POLYFLARE_SOFT_DRAIN_ENABLED");
        }
    }

    /// Serializes tests in this module that mutate `POLYFLARE_STARVATION_WAKE_JITTER_MS` — same
    /// rationale as the other env-lock helpers above (env vars are process-global, tests run
    /// concurrently on separate threads by default).
    fn wake_jitter_env_lock() -> &'static std::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
    }

    /// B10 Task 1: unset ⇒ `0` — the documented disable lever AND the default (unlike
    /// `starvation_wait_budget_secs_from_env`, `0` here isn't a separate branch from the default;
    /// they're the same value, so "unset" and "explicit 0" resolve identically).
    #[test]
    fn wake_jitter_ms_unset_defaults_to_zero() {
        let _guard = wake_jitter_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("POLYFLARE_STARVATION_WAKE_JITTER_MS");
        }
        assert_eq!(wake_jitter_ms_from_env(), 0);
    }

    /// A well-formed, in-range value resolves to exactly itself.
    #[test]
    fn wake_jitter_ms_reads_a_well_formed_value() {
        let _guard = wake_jitter_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_STARVATION_WAKE_JITTER_MS", "250");
        }
        assert_eq!(wake_jitter_ms_from_env(), 250);
        unsafe {
            std::env::remove_var("POLYFLARE_STARVATION_WAKE_JITTER_MS");
        }
    }

    /// An absurdly large value clamps DOWN to the `30_000`ms ceiling — never silently accepted
    /// as-is (a hostile/huge value must not be able to blow the B5 wait budget).
    #[test]
    fn wake_jitter_ms_absurd_value_clamps_to_thirty_seconds() {
        let _guard = wake_jitter_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_STARVATION_WAKE_JITTER_MS", "999999");
        }
        assert_eq!(wake_jitter_ms_from_env(), 30_000);
        unsafe {
            std::env::remove_var("POLYFLARE_STARVATION_WAKE_JITTER_MS");
        }
    }

    /// A malformed value (non-numeric) ⇒ the SAME safe default as unset (`0`) — never a boot
    /// crash.
    #[test]
    fn wake_jitter_ms_malformed_defaults_to_zero() {
        let _guard = wake_jitter_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_STARVATION_WAKE_JITTER_MS", "not-a-number");
        }
        assert_eq!(wake_jitter_ms_from_env(), 0);
        unsafe {
            std::env::remove_var("POLYFLARE_STARVATION_WAKE_JITTER_MS");
        }
    }

    // ---- C9 Task 3 (d): `POLYFLARE_INFLIGHT_PENALTY_PCT` ----

    /// Serializes tests in this module that mutate `POLYFLARE_INFLIGHT_PENALTY_PCT` — same
    /// rationale as the other env-lock helpers above.
    fn inflight_penalty_pct_env_lock() -> &'static std::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
    }

    /// Unset ⇒ `2.5` (codex-lb's own value — the default is "on", unlike `wake_jitter_ms`'s
    /// unset-is-the-disable-lever shape).
    #[test]
    fn inflight_penalty_pct_unset_defaults_to_two_point_five() {
        let _guard = inflight_penalty_pct_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("POLYFLARE_INFLIGHT_PENALTY_PCT");
        }
        assert_eq!(inflight_penalty_pct_from_env(), 2.5);
    }

    /// `0` is the documented DISABLE LEVER, not a clamp target — an explicit, well-formed `0`
    /// resolves to exactly `0.0`, distinct from the `2.5` default.
    #[test]
    fn inflight_penalty_pct_explicit_zero_disables() {
        let _guard = inflight_penalty_pct_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_INFLIGHT_PENALTY_PCT", "0");
        }
        assert_eq!(inflight_penalty_pct_from_env(), 0.0);
        unsafe {
            std::env::remove_var("POLYFLARE_INFLIGHT_PENALTY_PCT");
        }
    }

    /// A well-formed, in-range value resolves to exactly itself.
    #[test]
    fn inflight_penalty_pct_reads_a_well_formed_value() {
        let _guard = inflight_penalty_pct_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_INFLIGHT_PENALTY_PCT", "5.5");
        }
        assert_eq!(inflight_penalty_pct_from_env(), 5.5);
        unsafe {
            std::env::remove_var("POLYFLARE_INFLIGHT_PENALTY_PCT");
        }
    }

    /// An absurdly large value clamps DOWN to the `50.0` ceiling — never silently accepted as-is
    /// (a hostile/huge value must not be able to single-handedly clamp every account's `eff_used`
    /// to 100 off a single in-flight request).
    #[test]
    fn inflight_penalty_pct_absurd_value_clamps_to_fifty() {
        let _guard = inflight_penalty_pct_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_INFLIGHT_PENALTY_PCT", "999999");
        }
        assert_eq!(inflight_penalty_pct_from_env(), 50.0);
        unsafe {
            std::env::remove_var("POLYFLARE_INFLIGHT_PENALTY_PCT");
        }
    }

    /// A negative value clamps UP to the `0.0` floor (never a negative penalty — that would
    /// invert the scoring direction).
    #[test]
    fn inflight_penalty_pct_negative_value_clamps_to_zero() {
        let _guard = inflight_penalty_pct_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_INFLIGHT_PENALTY_PCT", "-5");
        }
        assert_eq!(inflight_penalty_pct_from_env(), 0.0);
        unsafe {
            std::env::remove_var("POLYFLARE_INFLIGHT_PENALTY_PCT");
        }
    }

    /// A malformed value (non-numeric) ⇒ the SAME safe default as unset (`2.5`) — never a boot
    /// crash.
    #[test]
    fn inflight_penalty_pct_malformed_defaults_to_two_point_five() {
        let _guard = inflight_penalty_pct_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_INFLIGHT_PENALTY_PCT", "not-a-number");
        }
        assert_eq!(inflight_penalty_pct_from_env(), 2.5);
        unsafe {
            std::env::remove_var("POLYFLARE_INFLIGHT_PENALTY_PCT");
        }
    }

    // --- C12 Task 3: POLYFLARE_REQUEST_LOG_RETENTION_DAYS / POLYFLARE_USAGE_HISTORY_RETENTION_DAYS ---

    /// Serializes tests in this module that mutate `POLYFLARE_REQUEST_LOG_RETENTION_DAYS` /
    /// `POLYFLARE_USAGE_HISTORY_RETENTION_DAYS` — same rationale as the other env-lock helpers
    /// above (env vars are process-global, tests run concurrently on separate threads by default).
    fn retention_days_env_lock() -> &'static std::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
    }

    /// Unset ⇒ `0` — disabled is the DEFAULT for this knob (unlike most numeric knobs in this
    /// module), since retention pruning is a destructive background delete an operator must opt
    /// into explicitly.
    #[test]
    fn request_log_retention_days_unset_defaults_to_zero_disabled() {
        let _guard = retention_days_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("POLYFLARE_REQUEST_LOG_RETENTION_DAYS");
        }
        assert_eq!(request_log_retention_days_from_env(), 0);
    }

    /// A well-formed, in-range value resolves to exactly itself.
    #[test]
    fn request_log_retention_days_reads_a_well_formed_value() {
        let _guard = retention_days_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_REQUEST_LOG_RETENTION_DAYS", "30");
        }
        assert_eq!(request_log_retention_days_from_env(), 30);
        unsafe {
            std::env::remove_var("POLYFLARE_REQUEST_LOG_RETENTION_DAYS");
        }
    }

    /// An absurdly large value (`99999`, above the `3650`-day = 10-year ceiling) clamps DOWN to
    /// `3650` — never silently accepted as-is, never treated as malformed.
    #[test]
    fn request_log_retention_days_absurd_value_clamps_to_thirty_six_fifty() {
        let _guard = retention_days_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_REQUEST_LOG_RETENTION_DAYS", "99999");
        }
        assert_eq!(request_log_retention_days_from_env(), 3650);
        unsafe {
            std::env::remove_var("POLYFLARE_REQUEST_LOG_RETENTION_DAYS");
        }
    }

    /// A malformed value (non-numeric) ⇒ `0` (disabled) — the SAME fail-safe value as unset, never
    /// silently enabling an irreversible bulk-delete background task off a typo.
    #[test]
    fn request_log_retention_days_malformed_defaults_to_zero_disabled() {
        let _guard = retention_days_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_REQUEST_LOG_RETENTION_DAYS", "not-a-number");
        }
        assert_eq!(request_log_retention_days_from_env(), 0);
        unsafe {
            std::env::remove_var("POLYFLARE_REQUEST_LOG_RETENTION_DAYS");
        }
    }

    /// `usage_history`'s knob shares the exact same parse/clamp/malformed logic (via the shared
    /// `retention_days_from_env` helper) — one representative test proves the wiring reaches the
    /// right env var, distinct from `request_log`'s.
    #[test]
    fn usage_history_retention_days_reads_its_own_env_var() {
        let _guard = retention_days_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_USAGE_HISTORY_RETENTION_DAYS", "45");
            std::env::remove_var("POLYFLARE_REQUEST_LOG_RETENTION_DAYS");
        }
        assert_eq!(usage_history_retention_days_from_env(), 45);
        assert_eq!(
            request_log_retention_days_from_env(),
            0,
            "the two knobs are independent env vars"
        );
        unsafe {
            std::env::remove_var("POLYFLARE_USAGE_HISTORY_RETENTION_DAYS");
        }
    }

    /// Serializes tests in this module that mutate `POLYFLARE_MODEL_CATALOG_TTL_SECS` /
    /// `POLYFLARE_MODEL_CATALOG_ENABLED` — same rationale as the other env-lock helpers above.
    fn model_catalog_env_lock() -> &'static std::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
    }

    /// D15 Task 3 (c): unset ⇒ `3600` (an hour, the documented default).
    #[test]
    fn model_catalog_ttl_secs_unset_defaults_to_thirty_six_hundred() {
        let _guard = model_catalog_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("POLYFLARE_MODEL_CATALOG_TTL_SECS");
        }
        assert_eq!(model_catalog_ttl_secs_from_env(), 3600);
    }

    /// D15 Task 3 (c): a well-formed value BELOW the `60`s floor (`30`) clamps UP to `60` — never
    /// silently accepted as-is, never treated as malformed.
    #[test]
    fn model_catalog_ttl_secs_below_floor_clamps_to_sixty() {
        let _guard = model_catalog_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_MODEL_CATALOG_TTL_SECS", "30");
        }
        assert_eq!(model_catalog_ttl_secs_from_env(), 60);
        unsafe {
            std::env::remove_var("POLYFLARE_MODEL_CATALOG_TTL_SECS");
        }
    }

    /// A well-formed value ABOVE the `86400`s (24h) ceiling clamps DOWN to `86400`.
    #[test]
    fn model_catalog_ttl_secs_above_ceiling_clamps_to_one_day() {
        let _guard = model_catalog_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_MODEL_CATALOG_TTL_SECS", "999999");
        }
        assert_eq!(model_catalog_ttl_secs_from_env(), 86400);
        unsafe {
            std::env::remove_var("POLYFLARE_MODEL_CATALOG_TTL_SECS");
        }
    }

    /// A well-formed in-range value resolves to exactly itself.
    #[test]
    fn model_catalog_ttl_secs_reads_a_well_formed_in_range_value() {
        let _guard = model_catalog_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_MODEL_CATALOG_TTL_SECS", "7200");
        }
        assert_eq!(model_catalog_ttl_secs_from_env(), 7200);
        unsafe {
            std::env::remove_var("POLYFLARE_MODEL_CATALOG_TTL_SECS");
        }
    }

    /// D15 Task 3 (c): a malformed value (non-numeric) ⇒ the SAME safe default as unset (`3600`)
    /// — never a boot crash, never silently collapsed to the clamp floor.
    #[test]
    fn model_catalog_ttl_secs_malformed_defaults_to_thirty_six_hundred() {
        let _guard = model_catalog_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_MODEL_CATALOG_TTL_SECS", "not-a-number");
        }
        assert_eq!(model_catalog_ttl_secs_from_env(), 3600);
        unsafe {
            std::env::remove_var("POLYFLARE_MODEL_CATALOG_TTL_SECS");
        }
    }

    /// D15 Task 3 (c): unset ⇒ `true` — the documented default-ON behavior (the fallback ladder
    /// is airtight, so there's no safety reason to ship this off by default).
    #[test]
    fn model_catalog_enabled_unset_defaults_to_true() {
        let _guard = model_catalog_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("POLYFLARE_MODEL_CATALOG_ENABLED");
        }
        assert!(model_catalog_enabled_from_env());
    }

    /// D15 Task 3 (c): `"0"` is the explicit disable lever ⇒ `false`.
    #[test]
    fn model_catalog_enabled_zero_disables() {
        let _guard = model_catalog_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_MODEL_CATALOG_ENABLED", "0");
        }
        assert!(!model_catalog_enabled_from_env());
        unsafe {
            std::env::remove_var("POLYFLARE_MODEL_CATALOG_ENABLED");
        }
    }

    /// A malformed/typo'd value ⇒ the SAME safe default as unset (`true`) — never silently
    /// disables a fetch whose every failure mode already degrades safely.
    #[test]
    fn model_catalog_enabled_malformed_defaults_to_true() {
        let _guard = model_catalog_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_MODEL_CATALOG_ENABLED", "not-a-bool");
        }
        assert!(model_catalog_enabled_from_env());
        unsafe {
            std::env::remove_var("POLYFLARE_MODEL_CATALOG_ENABLED");
        }
    }

    // ---- Task 1 (live-editable settings): pure clamp_<field> fns ----
    // Each expected value here is copied VERBATIM from the corresponding *_from_env edge-case test
    // above (see that test for the bound's full rationale). These fns are pure — no env var, no
    // lock needed.

    #[test]
    fn clamp_max_account_attempts_bounds() {
        assert_eq!(clamp_max_account_attempts(0), 1, "0 floors to 1");
        assert_eq!(
            clamp_max_account_attempts(7),
            7,
            "well-formed passes through unchanged"
        );
        assert_eq!(clamp_max_account_attempts(1), 1);
    }

    #[test]
    fn clamp_starvation_wait_budget_secs_bounds() {
        assert_eq!(
            clamp_starvation_wait_budget_secs(0),
            0,
            "0 is the disable lever, not a clamp target"
        );
        assert_eq!(clamp_starvation_wait_budget_secs(120), 120);
        assert_eq!(
            clamp_starvation_wait_budget_secs(500),
            300,
            "clamps down to MAX=300"
        );
        assert_eq!(
            clamp_starvation_wait_budget_secs(300),
            300,
            "at the MAX boundary"
        );
    }

    #[test]
    fn clamp_starvation_heartbeat_secs_bounds() {
        assert_eq!(clamp_starvation_heartbeat_secs(10, 60), 10, "within range");
        assert_eq!(
            clamp_starvation_heartbeat_secs(50, 5),
            5,
            "clamps down to the budget"
        );
        assert_eq!(
            clamp_starvation_heartbeat_secs(0, 60),
            1,
            "0 floors to 1, unlike the wait budget's 0"
        );
        assert_eq!(
            clamp_starvation_heartbeat_secs(999, 60),
            60,
            "brief's representative edge"
        );
        assert_eq!(
            clamp_starvation_heartbeat_secs(10, 0),
            1,
            "budget_secs=0 → ceiling floors to budget_secs.max(1), never panics"
        );
    }

    #[test]
    fn clamp_stream_idle_timeout_secs_bounds() {
        assert_eq!(
            clamp_stream_idle_timeout_secs(0),
            0,
            "0 is the disable lever, not a clamp target"
        );
        assert_eq!(clamp_stream_idle_timeout_secs(30), 30);
        assert_eq!(
            clamp_stream_idle_timeout_secs(999_999),
            3600,
            "clamps down to MAX=3600 (1h)"
        );
        assert_eq!(
            clamp_stream_idle_timeout_secs(3600),
            3600,
            "at the MAX boundary"
        );
    }

    #[test]
    fn clamp_wake_jitter_ms_bounds() {
        assert_eq!(clamp_wake_jitter_ms(0), 0, "0 is both default and disable");
        assert_eq!(clamp_wake_jitter_ms(250), 250);
        assert_eq!(
            clamp_wake_jitter_ms(999_999),
            30_000,
            "clamps down to MAX=30_000ms"
        );
        assert_eq!(clamp_wake_jitter_ms(30_000), 30_000, "at the MAX boundary");
    }

    #[test]
    fn clamp_inflight_penalty_pct_bounds() {
        assert_eq!(
            clamp_inflight_penalty_pct(f64::NAN),
            2.5,
            "NaN → the 2.5 default, not passed through"
        );
        assert_eq!(
            clamp_inflight_penalty_pct(-5.0),
            0.0,
            "negative floors to 0.0"
        );
        assert_eq!(
            clamp_inflight_penalty_pct(999_999.0),
            50.0,
            "clamps down to MAX=50.0"
        );
        assert_eq!(
            clamp_inflight_penalty_pct(5.5),
            5.5,
            "well-formed passes through unchanged"
        );
        assert_eq!(
            clamp_inflight_penalty_pct(0.0),
            0.0,
            "explicit 0.0 disable lever passes through, distinct from NaN"
        );
        assert_eq!(
            clamp_inflight_penalty_pct(50.0),
            50.0,
            "at the MAX boundary"
        );
    }

    #[test]
    fn clamp_request_log_retention_days_bounds() {
        assert_eq!(clamp_request_log_retention_days(0), 0, "0 is disabled");
        assert_eq!(clamp_request_log_retention_days(30), 30);
        assert_eq!(
            clamp_request_log_retention_days(99_999),
            3650,
            "clamps down to MAX=3650 (10y)"
        );
        assert_eq!(
            clamp_request_log_retention_days(3650),
            3650,
            "at the MAX boundary"
        );
    }

    #[test]
    fn clamp_usage_history_retention_days_bounds() {
        assert_eq!(clamp_usage_history_retention_days(0), 0, "0 is disabled");
        assert_eq!(clamp_usage_history_retention_days(45), 45);
        assert_eq!(
            clamp_usage_history_retention_days(99_999),
            3650,
            "clamps down to MAX=3650 (10y)"
        );
        assert_eq!(
            clamp_usage_history_retention_days(3650),
            3650,
            "at the MAX boundary"
        );
    }
}
