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
            Ok(0) => 1,
            Ok(n) => n,
            Err(_) => DEFAULT,
        },
        Err(_) => DEFAULT,
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
    const MAX: u32 = 300;
    match std::env::var("POLYFLARE_STARVATION_WAIT_BUDGET_SECS") {
        Ok(raw) => match raw.trim().parse::<u32>() {
            Ok(0) => 0,
            Ok(n) if n > MAX => MAX,
            Ok(n) => n,
            Err(_) => DEFAULT,
        },
        Err(_) => DEFAULT,
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
    const MAX: u64 = 3600;
    match std::env::var("POLYFLARE_STREAM_IDLE_TIMEOUT_SECS") {
        Ok(raw) => match raw.trim().parse::<u64>() {
            Ok(0) => 0,
            Ok(n) if n > MAX => MAX,
            Ok(n) => n,
            Err(_) => default,
        },
        Err(_) => default,
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
            max_account_attempts,
            starvation_wait_budget,
            starvation_heartbeat,
            allow_unauthenticated_remote,
            stream_idle_timeout,
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

        assert!(!pool_requires_capability(Some("cyber"), SECURITY_WORK_CAPABILITY));
        // Second call: same malformed value, `Once` already fired — must still fail open cleanly.
        assert!(!pool_requires_capability(Some("cyber"), SECURITY_WORK_CAPABILITY));

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

        assert!(pool_requires_capability(Some("cyber"), SECURITY_WORK_CAPABILITY));
        assert!(!pool_requires_capability(Some("general"), SECURITY_WORK_CAPABILITY));
        assert!(!pool_requires_capability(Some("cyber"), "some_other_capability"));
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
        let _guard = starvation_env_lock().lock().unwrap_or_else(|e| e.into_inner());
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
        let _guard = starvation_env_lock().lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("POLYFLARE_STARVATION_WAIT_BUDGET_SECS");
        }
        assert_eq!(starvation_wait_budget_secs_from_env(), 60);
    }

    /// `500` (above codex-lb's `MAX=300`) clamps DOWN to 300 — never silently accepted, never
    /// treated as malformed.
    #[test]
    fn starvation_wait_budget_above_max_clamps_to_three_hundred() {
        let _guard = starvation_env_lock().lock().unwrap_or_else(|e| e.into_inner());
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
        let _guard = starvation_env_lock().lock().unwrap_or_else(|e| e.into_inner());
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
        let _guard = starvation_env_lock().lock().unwrap_or_else(|e| e.into_inner());
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
        let _guard = starvation_env_lock().lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("POLYFLARE_STARVATION_HEARTBEAT_SECS");
        }
        assert_eq!(starvation_heartbeat_secs_from_env(60), 10);
    }

    /// A heartbeat larger than the (already-resolved) budget clamps DOWN to the budget — a
    /// heartbeat that never fires before the wait ends would defeat its purpose.
    #[test]
    fn starvation_heartbeat_larger_than_budget_clamps_down_to_budget() {
        let _guard = starvation_env_lock().lock().unwrap_or_else(|e| e.into_inner());
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
        let _guard = starvation_env_lock().lock().unwrap_or_else(|e| e.into_inner());
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
        let _guard = starvation_env_lock().lock().unwrap_or_else(|e| e.into_inner());
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
        let _guard = starvation_env_lock().lock().unwrap_or_else(|e| e.into_inner());
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
}
