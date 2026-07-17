//! Process configuration for `polyflare serve`, read from environment. Secrets are NOT here —
//! per-account bearer tokens live in the store; only shared base URLs + data paths are config.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
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
}
