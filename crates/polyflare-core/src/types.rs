//! Core value types threaded through the request path.

use std::pin::Pin;

use bytes::Bytes;
use futures_core::Stream;

use crate::provider::Provider;

/// A request prepared for a specific backend. In M1 this is a thin wrapper over the
/// raw request JSON plus the target model; continuity/translation enrich it later.
///
/// `forward_headers` carries the ordered codex-identity headers the ingress decided the executor
/// should send upstream: for a native `/responses` request, the client's own surviving inbound
/// headers (forwarded untouched); for a translated request (no real Codex client fingerprint
/// exists), a synthesized set (see `polyflare_codex::codex_headers`). The executor itself stays
/// dumb — it just sets these on the outbound request and overrides auth/accept.
#[derive(Clone)]
pub struct PreparedRequest {
    /// The materialized request body — present ONLY when there are no `raw_body` bytes to forward
    /// verbatim (a translated alias, an anchor-stripped full-resend recovery, or the Anthropic wire
    /// path). On the native `/responses` pass-through it is `None`: the wire bytes live in
    /// `raw_body`, and everything ingress needs from the body (model, tier, the continuity heuristic,
    /// the input count) is extracted once at parse time WITHOUT materializing the deep `input` tree.
    /// INVARIANT: `raw_body.is_none()` ⇒ `body.is_some()` — the executor always has something to send.
    pub body: Option<serde_json::Value>,
    pub model: String,
    pub forward_headers: Vec<(String, String)>,
    /// The client's ORIGINAL request bytes, when they can be forwarded upstream verbatim (the native
    /// `/responses` pass-through). `Some` ⇒ the executor sends these bytes as-is — no parse→
    /// re-serialize round-trip, and byte-identical to what the client sent (better fingerprint
    /// fidelity). `None` ⇒ the body was built or mutated (a translated alias, or an anchor-stripped
    /// full-resend recovery), so the executor serializes `body` (which is then `Some` per the
    /// invariant above).
    pub raw_body: Option<bytes::Bytes>,
}

// `body` carries the full user request/conversation content and must never be printed in clear
// via `{:?}` (mirrors `Account`'s `bearer_token` redaction below). `forward_headers` carries
// session/thread/conversation identity (real, forwarded ones for a native request) and must be
// redacted for the same content-safety reason, not just `body`.
impl std::fmt::Debug for PreparedRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedRequest")
            .field("model", &self.model)
            .field("body", &"<redacted>")
            .field("forward_headers", &"<redacted>")
            .finish()
    }
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
    /// The ChatGPT account id sent as the `chatgpt-account-id` companion header on Codex
    /// backend-api requests. The real Codex CLI always sends it paired with the Bearer, and it
    /// MUST correspond to `bearer_token`'s account — a load balancer that swaps the Bearer to a
    /// selected account but leaves a client's original account-id header would ship exactly the
    /// mismatched (token, account) pair the backend rejects. `None` for providers that don't use
    /// it (e.g. Anthropic).
    pub chatgpt_account_id: Option<String>,
}

// `bearer_token` is a secret and must never be printed in clear via `{:?}`.
impl std::fmt::Debug for Account {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Account")
            .field("id", &self.id)
            .field("base_url", &self.base_url)
            .field("bearer_token", &"***")
            .field("chatgpt_account_id", &self.chatgpt_account_id)
            .finish()
    }
}

/// Per-request context threaded through selection/continuity. `session_key`,
/// `client_previous_response_id`, and `is_full_resend` are derived at ingress from headers + body
/// BEFORE `prepare`.
#[derive(Debug, Clone, Default)]
pub struct RequestCtx {
    pub session_id: Option<String>,
    pub session_key: Option<SessionKey>,
    pub client_previous_response_id: Option<String>,
    pub is_full_resend: bool,
    /// Number of top-level `input` items, derived once at ingress (array length; a non-array present
    /// input counts as 1; absent as 0). Carried here so the watchdog's diagnostic input count no
    /// longer has to re-read the request body — which, on the native path, is never materialized.
    pub input_count: u32,
}

/// A derived conversation key + its strength (hard binds routing; soft is best-effort).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionKey {
    pub value: String,
    pub strength: KeyStrength,
}

/// How strongly a session key binds routing. `Hard` keys pin; `Soft` keys are best-effort.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyStrength {
    Hard,
    Soft,
}

/// Reasoning-typed output items from a completed turn. Sensitive user data: its `Debug` redacts
/// content. Populated only in R3 (M3-followup); `None` throughout M3-core.
#[derive(Clone)]
pub struct ReasoningItems(pub Vec<serde_json::Value>);

impl std::fmt::Debug for ReasoningItems {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReasoningItems([{} item(s) redacted])", self.0.len())
    }
}

/// Output of `prepare`: the (possibly-rewritten) request + how to route & guard it.
#[derive(Debug, Clone)]
pub struct Prepared {
    pub req: PreparedRequest,
    pub directive: ContinuityDirective,
}

/// How to route and guard a prepared request.
#[derive(Debug, Clone)]
pub struct ContinuityDirective {
    /// HARD routing pre-filter. `Some` ⇒ the request MUST route to this account (or Recover).
    pub pin_account: Option<AccountId>,
    /// Arm the silence watchdog — set ONLY on anchor-bearing requests.
    pub watchdog: WatchdogArm,
    /// What to do if the watchdog fires.
    pub recovery: RecoveryPlan,
    /// Threaded back to `observe` so it knows which session/turn this was.
    pub session_key: Option<SessionKey>,
}

/// Whether the silence watchdog is armed, and with what timeout.
#[derive(Debug, Clone, Copy)]
pub enum WatchdogArm {
    Disarmed,
    Armed { timeout: std::time::Duration },
}

/// What to do when the watchdog fires (or the owner is unavailable at prepare time).
#[derive(Clone)]
pub enum RecoveryPlan {
    /// The outgoing input is self-sufficient (a full-resend): on silence, re-execute this
    /// anchor-stripped request. Carries conversation content — redacted in `Debug`.
    ResendFull { anchorless_req: PreparedRequest },
    /// The outgoing input is a bare tail: on silence, surface `previous_response_not_found` so the
    /// client self-heals with a full resend.
    SignalClient,
    /// No anchor present ⇒ nothing to recover.
    None,
}

impl std::fmt::Debug for RecoveryPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecoveryPlan::ResendFull { .. } => {
                write!(f, "ResendFull {{ anchorless_req: <redacted> }}")
            }
            RecoveryPlan::SignalClient => write!(f, "SignalClient"),
            RecoveryPlan::None => write!(f, "None"),
        }
    }
}

/// What `observe` consumes — built by the watchdog wrapper as the stream resolves.
#[derive(Debug)]
pub enum TurnOutcome {
    /// Upstream produced its first event and we relayed it. `response_id` is sniffed from the
    /// streamed `response.created`/`response.completed`. `reasoning` is `None` until R3.
    Completed {
        session_key: Option<SessionKey>,
        account: AccountId,
        response_id: Option<String>,
        input_fingerprint: String,
        input_count: u32,
        reasoning: Option<ReasoningItems>,
    },
    /// Watchdog fired; we recovered (Strategy A) or signaled the client (Strategy B).
    Recovered {
        session_key: Option<SessionKey>,
        account: AccountId,
        new_response_id: Option<String>,
    },
    /// A hard upstream error (not silence).
    Failed { session_key: Option<SessionKey> },
}

/// Errors `Continuity` can surface. Generic `Display` — never leaks session content.
#[derive(Debug, thiserror::Error)]
pub enum ContinuityError {
    #[error("continuity store error")]
    Store(#[source] Box<dyn std::error::Error + Send + Sync>),
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
    /// Which backend pool this account belongs to — selects the executor + backend wire `Format`.
    pub provider: Provider,
    /// Named account-pool slug, or `None` (unpooled). The ingress narrows candidates to this via
    /// `filter_by_pool`: a named `/{pool}/...` path matches only accounts with the same slug; the
    /// bare paths match all accounts regardless of pool.
    pub pool: Option<String>,
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
            provider: Provider::Codex,
            pool: None,
        }
    }
}

/// Request cost/volume tier, derived at ingress from the model alias / reasoning effort (Claude
/// Code fans out `opus`→High orchestrator, `sonnet`→Medium subagent, `haiku`→Low searcher). Known
/// BEFORE routing, so a tier-aware strategy can pack cheap high-volume Low turns onto near-limit
/// accounts while steering expensive High turns to fresh/preserved capacity. `capacity_weighted`
/// ignores it; only `cache_affinity_tier` reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    High,
    Medium,
    Low,
}

/// Per-selection context (M2-GATE1). `now`/`rng_seed` keep the `Selector` pure + deterministic:
/// time and randomness are injected, never read inside the trait. `session_id` is the
/// session-affinity seam (unused by `capacity_weighted` scoring); `tier` is the subagent-tier
/// signal read only by `cache_affinity_tier`.
#[derive(Debug, Clone, Default)]
pub struct SelectionCtx {
    pub now: i64,
    pub require_security_work_authorized: bool,
    pub rng_seed: Option<u64>,
    pub session_id: Option<String>,
    pub tier: Option<Tier>,
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
            chatgpt_account_id: None,
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

    #[test]
    fn reasoning_items_debug_redacts_content() {
        let r = ReasoningItems(vec![
            serde_json::json!({"text": "super-secret-chain-of-thought"}),
        ]);
        let s = format!("{r:?}");
        assert!(
            !s.contains("super-secret-chain-of-thought"),
            "reasoning content must never appear in Debug: {s}"
        );
        assert!(
            s.contains("1 item"),
            "Debug should summarize count, not content: {s}"
        );
    }

    #[test]
    fn prepared_request_debug_redacts_body() {
        let req = PreparedRequest {
            body: Some(serde_json::json!({"input": "super-secret-user-conversation"})),
            model: "gpt-5.6-sol".to_string(),
            forward_headers: vec![],
            raw_body: None,
        };
        let s = format!("{req:?}");
        assert!(
            !s.contains("super-secret-user-conversation"),
            "PreparedRequest Debug must never leak the request body: {s}"
        );
        assert!(
            s.contains("<redacted>"),
            "Debug should mark the body redacted: {s}"
        );
        assert!(
            s.contains("gpt-5.6-sol"),
            "Debug should still contain the model: {s}"
        );
    }

    #[test]
    fn prepared_request_debug_redacts_forward_headers() {
        // forward_headers carries session/thread ids (real, forwarded ones on a native request) —
        // content-safety-sensitive the same way `body` is, so Debug must never print them either.
        let req = PreparedRequest {
            body: Some(serde_json::json!({})),
            model: "gpt-5.6-sol".to_string(),
            forward_headers: vec![
                (
                    "session-id".to_string(),
                    "super-secret-session-uuid".to_string(),
                ),
                (
                    "thread-id".to_string(),
                    "super-secret-thread-uuid".to_string(),
                ),
            ],
            raw_body: None,
        };
        let s = format!("{req:?}");
        assert!(
            !s.contains("super-secret-session-uuid"),
            "PreparedRequest Debug must never leak a forwarded header value: {s}"
        );
        assert!(
            !s.contains("super-secret-thread-uuid"),
            "PreparedRequest Debug must never leak a forwarded header value: {s}"
        );
        assert!(
            !s.contains("session-id") && !s.contains("thread-id"),
            "PreparedRequest Debug must never leak forwarded header names either: {s}"
        );
        assert!(
            s.contains("<redacted>"),
            "Debug should mark forward_headers redacted: {s}"
        );
    }

    #[test]
    fn new_snapshot_defaults_to_codex_provider() {
        let snap = AccountSnapshot::new("a");
        assert_eq!(snap.provider, Provider::Codex);
    }

    #[test]
    fn recovery_plan_debug_redacts_request_body() {
        let plan = RecoveryPlan::ResendFull {
            anchorless_req: PreparedRequest {
                body: Some(serde_json::json!({"input": "super-secret-conversation"})),
                model: "m".to_string(),
                forward_headers: vec![],
                raw_body: None,
            },
        };
        let s = format!("{plan:?}");
        assert!(
            !s.contains("super-secret-conversation"),
            "recovery must never leak the request body: {s}"
        );
        assert!(
            s.contains("redacted"),
            "Debug should mark the body redacted: {s}"
        );
    }
}
