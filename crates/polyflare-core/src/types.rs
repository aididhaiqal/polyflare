//! Core value types threaded through the request path.

use std::pin::Pin;

use bytes::Bytes;
use futures_core::Stream;

/// A request prepared for a specific backend. In M1 this is a thin wrapper over the
/// raw request JSON plus the target model; continuity/translation enrich it later.
#[derive(Debug, Clone)]
pub struct PreparedRequest {
    pub body: serde_json::Value,
    pub model: String,
}

/// Errors an executor can surface.
#[derive(Debug, thiserror::Error)]
pub enum ExecError {
    #[error("upstream request failed: {0}")]
    Upstream(String),
    #[error("stream error: {0}")]
    Stream(String),
}

/// A non-buffering streaming response body: pinned, boxed, `Send` stream of byte chunks.
pub type ResponseStream = Pin<Box<dyn Stream<Item = Result<Bytes, ExecError>> + Send>>;

/// A credential/endpoint an executor uses to reach an upstream. M1 = single account from config.
#[derive(Clone)]
pub struct Account {
    pub id: String,
    pub base_url: String,
    pub bearer_token: String,
}

// `bearer_token` is a secret and must never be printed in clear via `{:?}`.
impl std::fmt::Debug for Account {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Account")
            .field("id", &self.id)
            .field("base_url", &self.base_url)
            .field("bearer_token", &"***")
            .finish()
    }
}

/// Per-request context threaded through selection/continuity. Minimal in M1.
#[derive(Debug, Clone, Default)]
pub struct RequestCtx {
    pub session_id: Option<String>,
}

/// An owned account identifier — the `Selector`'s return type (M2-GATE1: owned, not a borrow).
/// `Hash`/`Ord` are additive to the seam so M2b-2 can key per-account maps + order deterministically.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AccountId(String);

impl AccountId {
    /// The id as a string slice (e.g. for store lookups).
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for AccountId {
    fn from(s: String) -> Self {
        AccountId(s)
    }
}

impl From<&str> for AccountId {
    fn from(s: &str) -> Self {
        AccountId(s.to_string())
    }
}

impl std::fmt::Display for AccountId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A per-account snapshot the `Selector` scores over. Durable fields come from the store
/// `Account`; window fields come from the latest `usage_history` rows; runtime fields
/// (`health_tier`, `error_count`, `cooldown_until`, `last_error_at`, `last_selected_at`,
/// `in_flight`) are live-tracked later and default to neutral values in M2b.
#[derive(Debug, Clone)]
pub struct AccountSnapshot {
    pub id: AccountId,
    /// active | rate_limited | quota_exceeded | paused | reauth_required | deactivated
    pub status: String,
    /// Primary-window used percent (0–100).
    pub used_percent: f64,
    /// Secondary-window used percent (0–100) — drives the capacity weight.
    pub secondary_used_percent: f64,
    /// Durable rate-limit/quota reset epoch (seconds); auto-recovery gate.
    pub reset_at: Option<i64>,
    /// Per-account capacity override (credits); `None` ⇒ derive from `plan_type`.
    pub capacity_credits: Option<f64>,
    /// normal | burn_first | preserve
    pub routing_policy: String,
    /// 0 healthy / 1 draining / 2 probing (defaulted 0 in M2b).
    pub health_tier: u8,
    pub error_count: u32,
    /// Generic "don't select until" epoch (seconds).
    pub cooldown_until: Option<i64>,
    /// Epoch (seconds) of the most recent error — drives error-backoff + drain recency.
    pub last_error_at: Option<i64>,
    /// Epoch (seconds) this account was last selected — a deterministic tiebreak key.
    pub last_selected_at: Option<i64>,
    /// free | plus | pro | prolite | team | business | enterprise | edu
    pub plan_type: String,
    /// TA6 hard-pre-filter capability flag.
    pub security_work_authorized: bool,
    /// In-flight request count (live-tracked later; 0 in M2b).
    pub in_flight: u32,
}

impl AccountSnapshot {
    /// A snapshot with neutral defaults (active, zero usage, healthy, no runtime state). The
    /// assembler overrides the durable/window fields it knows; runtime fields stay defaulted
    /// in M2b (live tracking is deferred).
    pub fn new(id: impl Into<AccountId>) -> Self {
        Self {
            id: id.into(),
            status: "active".to_string(),
            used_percent: 0.0,
            secondary_used_percent: 0.0,
            reset_at: None,
            capacity_credits: None,
            routing_policy: "normal".to_string(),
            health_tier: 0,
            error_count: 0,
            cooldown_until: None,
            last_error_at: None,
            last_selected_at: None,
            plan_type: "plus".to_string(),
            security_work_authorized: false,
            in_flight: 0,
        }
    }
}

/// Per-selection context (M2-GATE1). `now`/`rng_seed` keep the `Selector` pure + deterministic:
/// time and randomness are injected, never read inside the trait. `session_id` is the
/// session-affinity seam (unused by `capacity_weighted` scoring in M2b).
#[derive(Debug, Clone, Default)]
pub struct SelectionCtx {
    pub now: i64,
    pub require_security_work_authorized: bool,
    pub rng_seed: Option<u64>,
    pub session_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_debug_redacts_bearer_token() {
        let account = Account {
            id: "acct-1".into(),
            base_url: "https://example.test".into(),
            bearer_token: "super-secret-token-value".into(),
        };

        let debug_output = format!("{account:?}");

        assert!(
            !debug_output.contains("super-secret-token-value"),
            "Debug output must never contain the raw bearer token: {debug_output}"
        );
        assert!(
            debug_output.contains("***"),
            "Debug output must contain the redaction marker: {debug_output}"
        );
        assert!(
            debug_output.contains("acct-1"),
            "Debug output must still contain the id: {debug_output}"
        );
        assert!(
            debug_output.contains("https://example.test"),
            "Debug output must still contain the base_url: {debug_output}"
        );
    }
}
