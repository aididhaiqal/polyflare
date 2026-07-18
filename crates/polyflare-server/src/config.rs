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
}
