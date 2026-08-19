//! Ingress: derive continuity ctx → prepare → ownership pre-filter → execute under the watchdog →
//! relay. Client-facing errors carry generic bodies (never a token, URL, or internal Display).

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::{Body, Bytes};
use axum::extract::{Json, Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use futures_util::StreamExt;

use polyflare_anthropic::{AnthropicToResponses, ResponsesToAnthropic};
use polyflare_codex::oauth::{
    classify_failure, is_fedramp_account, should_refresh, token_exp, OAuthError,
};
use polyflare_core::{
    Account, AccountId, AccountSnapshot, BackoffKind, Continuity, ContinuityDirective, ExecError,
    FailureSignal, NoopContinuity, Prepared, PreparedRequest, Provider, RecoveryPlan, RequestCtx,
    ResponseMetadata, ResponseStream, SelectionCtx, Selector, SessionKey, Tier, Translator,
    WatchdogArm,
};
use polyflare_store::{CustomProvider, PlainTokens, ProviderModel, RequestLogRecord, Store};

use crate::alias::{ModelAlias, TranslationTarget};
use crate::app::AppState;
use crate::collect_message::collect_anthropic_message;
use crate::config;
use crate::failover::{exclude_tried, failover_reason_code, failover_verdict, FailoverVerdict};
use crate::fingerprint_capture::{append_fingerprint_capture, capture_request_fingerprint};
use crate::observability::{FailoverSignal, RequestLog};
use crate::reactive_auth::{
    ReactiveAuth, ReactiveAuthError, PERSIST_MAX_ATTEMPTS, PERSIST_RETRY_BACKOFF,
};
use crate::session_key::parse_inbound_scoped;
use crate::snapshot::{
    filter_by_provider_and_pool, filter_by_traffic_eligibility, MessagesTraffic,
};
use crate::starvation;
use crate::translate_stream::wrap_translating_stream;
use crate::usage_capture;
use crate::watchdog::{
    apply_ownership, execute_recovery_tracked, execute_with_watchdog_tracked, signal_client_stream,
    CommitWitness, RouteDecision, WatchdogError,
};

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// The upstream `error.code` a failed attempt carried, if it carried one. Reads ONLY the bounded
/// code field the failover classifier already keys off — never `message`, never the body.
fn watchdog_error_code(error: &WatchdogError) -> Option<String> {
    match error {
        WatchdogError::Upstream(Some(signal)) => signal.error_code.clone(),
        WatchdogError::UpstreamHttp(response) => response.signal.error_code.clone(),
        _ => None,
    }
}

/// A copy of `prepared` whose `/responses` body carries no foreign reasoning envelopes, or `None`
/// when there was nothing to strip (so the caller never burns a pointless retry).
///
/// Handles both body representations: the native pass-through keeps the client's verbatim bytes in
/// `raw_body`, while translated/recovered requests carry a materialized `body`. Whichever holds the
/// request is the one rewritten — the other is left exactly as it was, preserving the executor's
/// existing "raw bytes win" contract.
fn prepared_with_stripped_reasoning(mut prepared: Prepared) -> Option<Prepared> {
    use crate::reasoning_transform::strip_unverifiable_reasoning_body;
    if let Some(raw) = prepared.req.raw_body.as_ref() {
        let stripped = strip_unverifiable_reasoning_body(raw)?;
        prepared.req.raw_body = Some(stripped.into());
        return Some(prepared);
    }
    let body = prepared.req.body.as_ref()?;
    let stripped = strip_unverifiable_reasoning_body(&serde_json::to_vec(body).ok()?)?;
    prepared.req.body = Some(serde_json::from_slice(&stripped).ok()?);
    Some(prepared)
}

/// Millisecond-resolution counterpart of [`unix_now`] — used ONLY by [`layer2_wait_stream`]'s
/// budget-deadline math (B5 Task 4 adversarial review, FIX 1). `unix_now()`'s whole-second
/// granularity is fine for durable `reset_at`/`cooldown_until` timestamps, but truncating a
/// sub-second wait *budget* to `.as_secs()` silently floors it to 0 — see that function's doc.
fn unix_now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Stream-idle-timeout plan (`docs/superpowers/plans/2026-07-18-stream-idle-timeout.md`) Task 2:
/// the DEFAULT source for the mid-stream idle deadline — matches codex's own `stream_idle_timeout`
/// default (`model-provider-info/src/lib.rs:26`, 300000ms). Every `execute_with_watchdog*`/
/// `execute_recovery*` call site below now threads `state.runtime_settings.stream_idle_timeout()`
/// (the real, config-resolved `Duration` seeded onto `AppState.runtime_settings` — see
/// `crate::config::stream_idle_timeout_secs_from_env`, `ServeConfig::from_env`, and
/// `crate::runtime_settings::RuntimeSettings`, resolved ONCE at startup and live-overridable
/// thereafter, never a per-request read of its own). This constant is no
/// longer read on the per-request path; it survives as `crate::config`'s single-source-of-truth
/// default (referenced directly by `stream_idle_timeout_secs_from_env`'s unset-env case) so the
/// "300s matches codex" fact lives in exactly one place.
pub(crate) const DEFAULT_STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(300);

/// Map a reasoning-effort string to a routing `Tier` (the subagent-tier signal the
/// `cache_affinity_tier` strategy reads). `minimal`/`low` → Low, `medium` → Medium, `high` → High.
fn tier_from_effort(effort: Option<&str>) -> Option<Tier> {
    match effort?.to_ascii_lowercase().as_str() {
        "high" => Some(Tier::High),
        "medium" => Some(Tier::Medium),
        "low" | "minimal" => Some(Tier::Low),
        _ => None,
    }
}

fn estimate_materialized_request_tokens(
    body: &serde_json::Value,
    input_field: &str,
    output_field: &str,
) -> u32 {
    let input_len = body
        .get(input_field)
        .and_then(|input| serde_json::to_vec(input).ok())
        .map_or(0, |bytes| bytes.len());
    let output_tokens = body
        .get(output_field)
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(4_096);
    crate::session_key::estimate_tokens_from_json_len(input_len, output_tokens)
}

/// The 503 an account-resolution failure surfaces.
///
/// `reason` is a FIXED label identifying which gate refused, warned once per occurrence. Added
/// 2026-07-26 after three separate investigations bottomed out at "a fast 503 with no explanation":
/// 58 client-visible refusals on one session with every admission counter at zero, and no way to
/// tell a non-active status from a failed token refresh. Content-free — a label and an account id,
/// never a token, body, or upstream message.
fn account_unavailable_because(reason: &'static str, account_id: &str) -> Response {
    tracing::warn!(
        reason,
        account_id,
        "account resolution refused the request (503)"
    );
    (StatusCode::SERVICE_UNAVAILABLE, "account unavailable").into_response()
}

/// The 503 a RESELECTED account's admission refusal surfaces.
///
/// `RuntimeStates::try_acquire_in_flight_weighted` is non-blocking: unlike the pinned-owner
/// acquire (which waits `wait_timeout`) it gives up instantly, and both recovery arms below turn
/// that into a client-visible 503. Until 2026-07-26 it shared `no_eligible`'s wording, so a
/// "capacity refused this account" 503 was indistinguishable in the logs from a "selector found
/// nothing at all" 503. The response BODY stays byte-identical to `no_eligible` — only the log line
/// differs, so no client sees a behaviour change. Content-free: a label, an account id, and the
/// integer pressure units the request asked for.
fn admission_refused_on_reselect(account_id: &str, pressure_units: u32) -> Response {
    tracing::warn!(
        reason = "admission_refused_on_reselect",
        account_id,
        pressure_units,
        "reselected account had no admission capacity (503)"
    );
    (StatusCode::SERVICE_UNAVAILABLE, "no eligible account").into_response()
}

/// Bench an account that a LOCAL gate just refused, so the selector stops handing it the next
/// request.
///
/// Every locally-generated, account-attributed 503 (account resolution refused, reselect admission
/// refused) used to leave routing health completely untouched: `record_failure` only writes on a
/// `WatchdogError::Upstream`/`UpstreamHttp`, and all of these paths `return` long before it. So the
/// account stayed exactly as preferred as it was a microsecond earlier, the very next request
/// picked it again, and the loop had no exit — 89 fast 503s on one account between 16:42 and 17:38
/// on 2026-07-26, none of which ever benched it. This is the missing exit: two refusals inside the
/// 60s window drop the account to DRAINING so the health-tier pool stops preferring it, and a third
/// trips the selector's `error_count >= 3` backoff gate — the pool moves on without a client retry.
///
/// Deliberately `record_transient_error` and not a cooldown or a status write: a local refusal is
/// not evidence about the upstream account, only about our ability to serve it right now. The
/// counter lives in memory, clears on the first success, and expires on its own (30s at three
/// strikes, capped at 300s) — so a transient DB blip or a momentary capacity squeeze costs one
/// backoff window, never a durable mark on the account.
fn bench_after_local_refusal(state: &AppState, id: &AccountId, now: i64) {
    let _ = state.runtime.record_transient_error(id, now);
}

/// `pub(crate)`: also reused by `crate::control::resolve_control_account`'s snapshot-read failure
/// path, for a byte-identical generic 500.
pub(crate) fn internal_error() -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
}

/// `pub(crate)`: also the D17 control-endpoint account resolution's (`crate::control`) no-eligible-
/// account response, so both paths return byte-identical 503s.
pub(crate) fn no_eligible() -> Response {
    tracing::warn!(
        reason = "no_eligible_account",
        "selection found no candidate (503)"
    );
    (StatusCode::SERVICE_UNAVAILABLE, "no eligible account").into_response()
}

/// TA6(b) Task 2 SECURITY FLOOR response: the capability-filtered reselect (triggered by a
/// `CapabilityRejection`) found no `security_work_authorized` account. A clean, DISTINCT 503 —
/// never the generic `BAD_GATEWAY` an ordinary upstream failure gets, and never a silent unfiltered
/// retry. See `reroute_cyber_rejection`'s doc for the invariant this protects.
fn no_authorized_account_for_security_work() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        "no authorized account available for security work",
    )
        .into_response()
}

/// Classify a failure signal and write the routing-health signal for the account `id` that
/// produced it, so the selector benches / cools it down on the NEXT request (via the runtime
/// overlay). A 429 ⇒ rate-limit cooldown (honoring `Retry-After`); a 5xx or a transport / mid-stream
/// drop ⇒ a transient error (the selector's error-backoff gate handles repeat offenders). Other 4xx
/// (a bad request, a 404) are a client/request problem, NOT an account-health signal, so they don't
/// bench the account. A `Continuity` error is not an account-health signal either.
///
/// A7: BEFORE any of the above, if the signal carries an upstream `error_code` that
/// `classify_failure` (the SAME code table the OAuth-refresh path at `resolve_core_account` uses —
/// reused, never copied, so the two paths can't drift) maps to a permanent class
/// (`ReauthRequired`/`Deactivated`, i.e. `.status()` is `Some`), this parks the account with that
/// durable terminal status instead: a terminal status supersedes health backoff, so `error_count` is
/// NOT also bumped, and `cooldown_until` is left untouched (null, absent a prior transient hit) —
/// only re-auth clears `reauth_required`, so a cooldown would wrongly auto-readmit a deauthed
/// account. Async because the durable write (`AccountRepo::update_status`) is; every call site
/// already awaits other work in the same `async fn`, so awaiting here is a plain, non-blocking
/// dependency, not a new sync/async boundary.
///
/// Capacity codes (`insufficient_quota` / `usage_not_included`) are distinct from account-health
/// errors: apply a short capacity cooldown without incrementing `error_count`, then request an
/// immediate authoritative `/wham/usage` refresh. Every other 429 also requests that refresh while
/// retaining the ordinary rate-limit cooldown and Retry-After handling.
///
/// WS-relay Phase 3 Task 4: this is the reusable CORE of `record_failure`'s policy, extracted
/// verbatim so the WS-downstream relay's exhaustion-move (`ws_relay`'s `on_upstream_error`) benches
/// an account with EXACTLY the same policy the HTTP path applies on a `WatchdogError::Upstream` —
/// one policy, two callers, never a second copy that can drift. `sig` is `Option<&FailureSignal>`
/// rather than `record_failure`'s `&WatchdogError`: the WS relay's `UpstreamSignal::Error`
/// classification already IS a `FailureSignal` (see `ws_relay::signal`), with no `WatchdogError` in
/// that path at all. `record_failure` below is now a thin caller: unwrap the `WatchdogError`, then
/// delegate here.
pub(crate) async fn bench_account_for_failure(
    state: &AppState,
    id: &AccountId,
    sig: Option<&FailureSignal>,
    now: i64,
) {
    if let Some(sig) = sig {
        if let Some(code) = &sig.error_code {
            if code.as_str() == "out_of_credits" {
                // Scoped billing rejection (overage credits depleted), not an account-wide limit:
                // fail over to another account WITHOUT benching, so this account keeps serving the
                // models/requests that don't draw on exhausted overage. `rate_limit::failure_signal`
                // only assigns this code when the response is NOT also a hard limit, so a genuine
                // plan-limit 429 still falls through to the cooldown arm below. Mirrors ccflare's
                // isAnthropicOutOfCredits fail-over-without-cooldown path.
                return;
            }
            if matches!(code.as_str(), "insufficient_quota" | "usage_not_included") {
                state.runtime.record_quota_exceeded(id, now);
                let _ = state
                    .store
                    .accounts()
                    .record_routing_cooldown(
                        id.as_str(),
                        now.saturating_add(crate::runtime_state::QUOTA_EXCEEDED_COOLDOWN_SECS),
                        "quota",
                        now,
                    )
                    .await;
                state.runtime.request_usage_refresh(id);
                return;
            }
            if let Some(status) = classify_failure(code).status() {
                let _ = state
                    .store
                    .accounts()
                    .update_status(id.as_str(), status)
                    .await;
                state.runtime.note_account_status(id.as_str(), status);
                return;
            }
        }
    }
    // An Anthropic subscription HARD limit (`blocked`/`queueing_hard`/`payment_required`, from the
    // `anthropic-ratelimit-unified-status` header) is a rate-limit even when it does not arrive as a
    // 429 — treat it exactly like one so the account is cooled until its reset rather than merely
    // accruing error count. Codex never sets these error codes, so this arm is Anthropic-only in
    // practice and cannot change Codex behavior.
    let is_anthropic_hard_limit = sig.is_some_and(|s| {
        matches!(
            s.error_code.as_deref(),
            Some("blocked" | "queueing_hard" | "payment_required")
        )
    });
    // A 529 is Anthropic's `overloaded_error` (Codex never emits it). Like better-ccflare, treat it
    // as a cooldown honoring whatever reset the response carried (via `rate_limit::failure_signal`'s
    // ladder), falling back to the floor cooldown when none is present — so an overloaded upstream
    // routes away briefly instead of merely accruing error count in the generic 5xx arm below.
    let transition = match sig {
        Some(sig) if sig.status == 429 || sig.status == 529 || is_anthropic_hard_limit => {
            state.runtime.request_usage_refresh(id);
            let delay = sig
                .retry_after
                .unwrap_or(crate::runtime_state::RATE_LIMITED_MIN_COOLDOWN_SECS)
                .clamp(
                    crate::runtime_state::RATE_LIMITED_MIN_COOLDOWN_SECS,
                    crate::runtime_state::MAX_COOLDOWN_SECS,
                );
            let reason = if sig.status == 529 { "overloaded" } else { "rate_limit" };
            let _ = state
                .store
                .accounts()
                .record_routing_cooldown(id.as_str(), now.saturating_add(delay), reason, now)
                .await;
            state
                .runtime
                .record_rate_limit(id, sig.retry_after, now, &state.rate_limit_metrics)
        }
        // 5xx (server error), 401/403 (bad credential / account-scoped auth), 408 (request timeout):
        // an ACCOUNT-health problem — bump the error count so a repeat offender hits the backoff gate.
        Some(sig) if (500..=599).contains(&sig.status) || matches!(sig.status, 401 | 403 | 408) => {
            state.runtime.record_transient_error(id, now)
        }
        Some(_) => None, // other 4xx (400/404/422/…): request-level, not account-health.
        // No HTTP status means origin connectivity (DNS/connect/TLS) or a stream transport loss,
        // neither of which is evidence that this account is unhealthy. Pre-response failures are
        // handled by the per-origin recovery circuit; post-response failures are never replayed.
        None => None,
    };
    // B8 Task 4: if that error just moved the account's soft-drain tier (an error-drain entering
    // DRAINING, or a probe-streak promotion), emit the content-free health-tier signal here — this
    // is one of the two edges that owns the log-bus/metrics handles (`&AppState`).
    if let Some(t) = transition {
        crate::observability::emit_health_tier_signal(
            &state.log_bus,
            &state.health_tier_metrics,
            id.as_str(),
            t.from,
            t.to,
            t.reason,
        );
    }
}

/// Thin `WatchdogError` unwrapper: a non-`Upstream` variant is not an account-health signal at all
/// (returns immediately), otherwise delegates the actual classification/bookkeeping to
/// [`bench_account_for_failure`] — the shared core the WS-downstream relay's exhaustion-move also
/// calls. Behavior-preserving extraction (WS-relay Phase 3 Task 4): identical outcome to the
/// pre-extraction body for every existing caller.
async fn record_failure(state: &AppState, id: &AccountId, err: &WatchdogError, now: i64) {
    let signal = match err {
        WatchdogError::Upstream(signal) => signal.as_ref(),
        WatchdogError::UpstreamHttp(response) => Some(&response.signal),
        _ => return,
    };
    bench_account_for_failure(state, id, signal, now).await;
}

/// M5 capture-fixture mechanism: if `state.capture_fingerprint_path` is set, append this
/// request's content-safe structural fingerprint (see `crate::fingerprint_capture`) to it. A
/// no-op (single `Option` check) when unset — the normal, always-disabled-by-default case. A
/// write failure (e.g. disk full) is logged content-safely and never fails the request itself.
fn maybe_capture_fingerprint(state: &AppState, method: &str, path: &str, headers: &HeaderMap) {
    if let Some(golden_path) = &state.capture_fingerprint_path {
        let record = capture_request_fingerprint(method, path, headers);
        if let Err(e) = append_fingerprint_capture(golden_path, &record) {
            eprintln!("polyflare: fingerprint capture write failed: {e}");
        }
    }
}

/// Inbound headers dropped before a native `/responses` request's surviving codex-identity headers
/// are captured into `PreparedRequest::forward_headers` (see that field's doc). `host` /
/// `content-length` / `connection` / `transfer-encoding` are hop-by-hop transport framing that must
/// never be replayed to a different upstream connection; `authorization` is dropped because the
/// executor always overrides it with the SELECTED account's own bearer token — forwarding the
/// client's own (irrelevant to upstream, and never to be logged/relayed) bearer would be at best
/// ignored and at worst a real secret leaking onto the wire under the wrong identity.
///
/// This is deliberately a small, conservative drop-list, not codex-lb's full native-vs-SDK
/// normalization (`_build_upstream_headers`/`_normalize_non_native_upstream_fingerprint` in
/// `codex-lb/app/core/clients/proxy.py`) — for now this just forwards what a native client sent;
/// full normalization is a follow-up.
const DROPPED_INBOUND_HEADERS: &[&str] = &[
    "host",
    "content-length",
    "connection",
    "transfer-encoding",
    "authorization",
    "chatgpt-account-id",
    "x-openai-fedramp",
];

/// Filters a native `/responses` request's inbound `HeaderMap` down to the surviving
/// codex-identity headers to forward upstream untouched (see `DROPPED_INBOUND_HEADERS`). A header
/// value that isn't valid visible-ASCII (`to_str()` fails) is silently skipped rather than
/// forwarded lossily.
///
/// D17 Task 3: promoted `pub(crate)` (from private) so `crate::control`'s handlers can reuse this
/// SAME hop-by-hop drop-list for the codex CONTROL-endpoint forward — the "dumb executor, smart
/// ingress" doctrine means control's forward headers should be filtered identically to
/// `/responses`'s, not a second, independently-maintained list.
pub(crate) fn forward_headers_from_inbound(headers: &HeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .filter(|(name, _)| {
            !DROPPED_INBOUND_HEADERS
                .iter()
                .any(|dropped| name.as_str().eq_ignore_ascii_case(dropped))
        })
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|v| (name.as_str().to_string(), v.to_string()))
        })
        .collect()
}

/// Synthesizes the codex-identity `forward_headers` for a TRANSLATED request (a Claude request
/// routed to the Codex pool): there is no real Codex client fingerprint to forward here, unlike the
/// native `/responses` path above, so this is where `polyflare_codex::codex_headers` (built from a
/// local `openai/codex` source read — see that module's doc) genuinely belongs.
fn synthesize_codex_forward_headers(
    body: &serde_json::Value,
    codex_version: &str,
) -> Vec<(String, String)> {
    use polyflare_codex::codex_headers::{
        codex_user_agent, conversation_key, originator, TurnIdentity,
    };

    let identity = TurnIdentity::derive(&conversation_key(body));
    vec![
        ("user-agent".to_string(), codex_user_agent(codex_version)),
        ("originator".to_string(), originator().to_string()),
        ("accept".to_string(), "text/event-stream".to_string()),
        ("session-id".to_string(), identity.session_id.clone()),
        ("thread-id".to_string(), identity.thread_id.clone()),
        (
            "x-client-request-id".to_string(),
            identity.thread_id.clone(),
        ),
        ("x-codex-window-id".to_string(), identity.window_id.clone()),
        (
            "x-codex-turn-metadata".to_string(),
            identity.turn_metadata_json(),
        ),
    ]
}

/// A stable, conversation-scoped `prompt_cache_key` for a translated (aliased) Codex body.
///
/// The alias path builds a fresh Codex body with no `prompt_cache_key`, so every turn of a
/// conversation cache-MISSED on OpenAI's prompt-prefix cache — re-prefilling the whole history each
/// turn. This derives a key from the request's STABLE prefix — the `instructions` (system prompt)
/// and the first `input` item — both identical across every turn of a conversation and distinct
/// between conversations, so the same conversation reuses the cache turn to turn. (This is ccflare's
/// conversation-mode key; we key on content rather than a session id because the translated
/// `/v1/messages` path carries no reliable Codex session id.) A content collision between two
/// unrelated conversations is harmless under `store:false` — the cache only helps up to the shared
/// prefix, which is exactly what matched.
///
/// Setting this BEFORE `synthesize_codex_forward_headers` also stabilizes the synthesized codex
/// identity headers, whose `conversation_key` prefers `prompt_cache_key` over the per-model fallback.
fn derive_alias_prompt_cache_key(body: &serde_json::Value) -> String {
    use sha2::{Digest, Sha256};
    let instructions = body
        .get("instructions")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let first_input = match body.get("input") {
        Some(serde_json::Value::Array(items)) => {
            items.first().map(|v| v.to_string()).unwrap_or_default()
        }
        Some(serde_json::Value::String(s)) => s.clone(),
        _ => String::new(),
    };
    let mut hasher = Sha256::new();
    hasher.update(instructions.as_bytes());
    hasher.update([0u8]); // domain separator so (instr, input) can't alias (instr∥input, "")
    hasher.update(first_input.as_bytes());
    hex::encode(&hasher.finalize()[..24]) // 48 hex chars, matching codex/ccflare key width
}

/// Content-free identifiers about a request's routing outcome, threaded back out of the deep
/// account-selection logic (`responses_handler_impl` / `messages_handler_native` /
/// `messages_handler_codex_aliased`) to the thin logging wrapper (`responses_route` /
/// `messages_route`) that builds the persisted/emitted `RequestLog` (see `crate::observability`).
/// Every field here is a routing-level scalar or a stable row id — never request/response content.
#[derive(Default)]
struct RouteOutcome {
    /// Whether ingress crossed wire protocols before contacting the selected target.
    aliased: bool,
    /// The account selected to serve (or attempted for) this request, when selection got that far.
    account_id: Option<String>,
    /// Actual upstream provider. Built-in paths leave this unset and use their fixed provider.
    provider_slug: Option<String>,
    provider_credential_id: Option<String>,
    upstream_model: Option<String>,
    upstream_transport: Option<String>,
    profile_revision: Option<String>,
    custom_pricing: Option<polyflare_core::pricing::CustomModelRates>,
    /// The tier asked of the UPSTREAM, after PolyFlare's own priority policy ran.
    requested_service_tier: Option<String>,
    /// The tier the upstream REPORTED, when the response carried one.
    actual_service_tier: Option<String>,
    /// The requested (native path) or resolved target (translated/aliased path) model string.
    model: Option<String>,
    /// `reasoning.effort` for this request, when known.
    reasoning_effort: Option<String>,
    /// Client-requested service tier on native Responses traffic, when explicitly present.
    service_tier: Option<String>,
    /// The codex sub-agent role label (`x-openai-subagent`, see `RequestCtx::subagent`), when
    /// known. Only the native `/responses` path can ever carry one (a real Codex client sends the
    /// header); the alias/translated `/v1/messages` paths are Claude→Codex requests with no codex
    /// sub-agent concept, so they always leave this `None`.
    subagent: Option<String>,
    /// One-way SHA-256 continuity/session key, when the ingress path derived one.
    session_key: Option<String>,
    /// Content-free pre-route token estimate used for weighted admission and terminal calibration.
    estimated_tokens: u32,
    /// Final capability boundary used to scope the downstream synthetic pool quota.
    require_security_work_authorized: bool,
}

struct ResponsesHandlerOptions {
    resolved_custom_route: Option<Vec<(CustomProvider, ProviderModel)>>,
    preapplied_priority_decision: Option<crate::priority_policy::PriorityDecision>,
    max_attempts: u32,
    starvation_budget: Duration,
    starvation_heartbeat: Duration,
}

fn stream_response(stream: ResponseStream) -> Response {
    let metadata = stream.metadata().clone();
    let status = StatusCode::from_u16(metadata.status).unwrap_or(StatusCode::BAD_GATEWAY);
    let has_content_type = metadata
        .headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("content-type"));
    let mut builder = Response::builder().status(status);
    for (name, value) in metadata.headers {
        if let (Ok(name), Ok(value)) = (
            axum::http::HeaderName::from_bytes(name.as_bytes()),
            axum::http::HeaderValue::from_str(&value),
        ) {
            builder = builder.header(name, value);
        }
    }
    if !has_content_type {
        builder = builder.header(header::CONTENT_TYPE, "text/event-stream");
    }
    builder
        .body(Body::from_stream(stream))
        .expect("valid response")
}

fn response_into_stream(response: Response) -> ResponseStream {
    let (parts, body) = response.into_parts();
    let metadata = ResponseMetadata {
        status: parts.status.as_u16(),
        headers: parts
            .headers
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.as_str().to_string(), value.to_string()))
            })
            .collect(),
    };
    ResponseStream::with_metadata(
        body.into_data_stream()
            .map(|chunk| chunk.map_err(|error| ExecError::Stream(error.to_string()))),
        metadata,
    )
}

async fn collect_responses_response(mut stream: ResponseStream) -> Result<serde_json::Value, ()> {
    let mut line_buffer = Vec::new();
    let mut terminal = None;
    while let Some(chunk) = stream.next().await {
        line_buffer.extend_from_slice(&chunk.map_err(|_| ())?);
        while let Some(position) = line_buffer.iter().position(|byte| *byte == b'\n') {
            let mut line = line_buffer.drain(..=position).collect::<Vec<_>>();
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            let text = String::from_utf8_lossy(&line);
            let Some(payload) = text.strip_prefix("data:") else {
                continue;
            };
            let Ok(event) = serde_json::from_str::<serde_json::Value>(payload.trim()) else {
                continue;
            };
            if matches!(
                event.get("type").and_then(|value| value.as_str()),
                Some("response.completed" | "response.incomplete")
            ) {
                terminal = event.get("response").cloned();
            } else if event.get("type").and_then(|value| value.as_str()) == Some("error") {
                return Err(());
            }
        }
    }
    terminal.ok_or(())
}

fn json_responses_response(value: serde_json::Value) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        Json(value),
    )
        .into_response()
}

async fn resolve_translation_custom_target(
    state: &AppState,
    provider_id: &str,
    model: &str,
    expected_wire_api: &str,
) -> Result<(CustomProvider, ProviderModel), Response> {
    let provider = state
        .store
        .providers()
        .get_provider(provider_id)
        .await
        .map_err(|_| internal_error())?
        .ok_or_else(|| {
            (
                StatusCode::BAD_GATEWAY,
                "translation target provider no longer exists",
            )
                .into_response()
        })?;
    if !provider.enabled {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "translation target provider is disabled",
        )
            .into_response());
    }
    if provider.wire_api != expected_wire_api {
        return Err((
            StatusCode::BAD_GATEWAY,
            "translation target provider protocol changed",
        )
            .into_response());
    }
    let models = state
        .store
        .providers()
        .list_models(provider_id)
        .await
        .map_err(|_| internal_error())?;
    let provider_model = models
        .into_iter()
        .find(|candidate| {
            candidate.enabled
                && (candidate.public_model == model || candidate.upstream_model == model)
        })
        .ok_or_else(|| {
            (
                StatusCode::BAD_GATEWAY,
                "translation target model is unavailable",
            )
                .into_response()
        })?;
    Ok((provider, provider_model))
}

fn upstream_http_error_response(error: &WatchdogError) -> Option<Response> {
    let WatchdogError::UpstreamHttp(response) = error else {
        return None;
    };
    let status = StatusCode::from_u16(response.signal.status).unwrap_or(StatusCode::BAD_GATEWAY);
    let mut builder = Response::builder().status(status);
    for (name, value) in &response.headers {
        if let (Ok(name), Ok(value)) = (
            axum::http::HeaderName::from_bytes(name.as_bytes()),
            axum::http::HeaderValue::from_str(value),
        ) {
            builder = builder.header(name, value);
        }
    }
    Some(
        builder
            .body(Body::from(response.body.clone()))
            .expect("valid upstream error response"),
    )
}

/// The local `429` returned when a client session exceeds the governor's enforce limit. Shaped as a
/// native Anthropic `rate_limit_error` so a Claude client handles it exactly as an upstream limit,
/// with a `Retry-After` and a marker header so a fronting proxy knows it is a deliberate policy
/// rejection (not an upstream flake to hold-and-retry).
fn session_reject_response(verdict: &crate::session_governor::SessionVerdict) -> Response {
    let body = serde_json::json!({
        "type": "error",
        "error": {
            "type": "rate_limit_error",
            "message": format!(
                "PolyFlare session budget exceeded: {} requests in the last hour (limit {}). \
                 This usually indicates runaway subagent fan-out.",
                verdict.count, verdict.enforce_limit
            )
        }
    });
    (
        StatusCode::TOO_MANY_REQUESTS,
        [
            ("x-polyflare-governor", "session-budget".to_string()),
            ("retry-after", verdict.retry_after_secs.to_string()),
        ],
        Json(body),
    )
        .into_response()
}

fn surface_watchdog_error(error: &WatchdogError) -> Response {
    if matches!(error, WatchdogError::AttemptBudgetExhausted) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": {
                    "code": "logical_turn_attempts_exhausted",
                    "message": "This logical turn exhausted its upstream attempt budget.",
                    "type": "invalid_request_error"
                }
            })),
        )
            .into_response();
    }
    upstream_http_error_response(error)
        .unwrap_or_else(|| (StatusCode::BAD_GATEWAY, "upstream error").into_response())
}

/// Outcome 3's non-streaming `/v1/messages` success response: a single buffered Anthropic
/// `Message`, `application/json` (not SSE) — the mirror of `stream_response` for a `stream:false`
/// client. `message` is already the fully-assembled `serde_json::Value` from
/// `collect_anthropic_message`; this just serializes and wraps it.
fn json_message_response(message: serde_json::Value) -> Response {
    let body = serde_json::to_vec(&message).unwrap_or_else(|_| b"{}".to_vec());
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .expect("valid response")
}

/// Dashboard-facing transport classification for HTTP ingress responses.
///
/// The wire request itself is always HTTP POST here, but a successful streaming response is
/// delivered as Server-Sent Events and should be distinguishable from buffered JSON in request
/// telemetry. WebSocket turns bypass these wrappers and are recorded separately by `ws_relay`.
fn response_transport(response: &Response) -> &'static str {
    let is_sse = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("text/event-stream"));

    if is_sse {
        "sse"
    } else {
        "http"
    }
}

/// Offer a request-outcome row to the Store's bounded FIFO writer, so a slow or failing DB write
/// never delays or fails the client's request and never creates an unbounded task per request.
/// The row is content-free by construction — it comes from `RequestLog::record`, the same audited
/// field set the tracing event carries (see `crate::observability`).
///
/// D17 Task 3: `pub(crate)` so `crate::control`'s handlers persist their content-free
/// control-endpoint log rows through this same queue rather than a parallel write path.
pub(crate) fn queue_persist_request_log(store: &Store, record: RequestLogRecord) {
    if let Err(e) = store.enqueue_request_log(record) {
        tracing::warn!(
            target: "polyflare_server::request",
            error = %e,
            "request_log queue failed"
        );
    }
}

/// Load + decrypt + refresh-if-stale the selected account, returning the core `Account` to execute
/// with plus its `Provider`, or a ready client-facing error `Response`.
///
/// `pub(crate)`: D17 Task 2's control-request account resolution (`crate::control::
/// resolve_control_account`) reuses this UNCHANGED — same decrypt/refresh/persist machinery, no
/// second implementation.
pub(crate) async fn resolve_core_account(
    state: &AppState,
    picked: &AccountId,
    now: i64,
) -> Result<(Account, Provider), Response> {
    let repo = state.store.accounts();
    // Resolve the account + tokens from the in-memory cache when possible (zero SQLite reads, zero
    // decrypt); on a miss, ONE `get_with_tokens` SELECT loads + populates it. Keyed to the TOKEN
    // generation (bumped by insert + update_tokens), so a rotated token is never served — but the
    // usage-refresh loop's periodic usage/status writes DON'T evict tokens (they bump only the
    // account/snapshot generation), keeping the token cache warm across refresh cycles.
    let store_gen = state.store.token_generation();
    let (account, mut tokens) = match state.token_cache.get(picked.as_str(), store_gen, now) {
        Some(pair) => pair,
        None => {
            let pair = match repo.get_with_tokens(picked.as_str(), &state.cipher).await {
                Ok(Some(p)) => p,
                Ok(None) | Err(_) => return Err(internal_error()),
            };
            state.token_cache.insert(
                picked.as_str(),
                pair.0.clone(),
                pair.1.clone(),
                store_gen,
                now,
            );
            pair
        }
    };
    let provider: Provider = match account.provider.parse() {
        Ok(p) => p,
        Err(_) => return Err(internal_error()),
    };
    // Refresh-on-stale is Codex-specific (the only OAuth client AppState holds today); Anthropic
    // subscription-OAuth refresh is Task 7 (VERIFY-gated — no confirmed endpoint/client_id yet).
    // An Anthropic account's stored access_token is used as-is until Task 7 lands.
    if provider == Provider::Codex
        && should_refresh(token_exp(&tokens.access_token), account.last_refresh, now)
    {
        // F2: serialize concurrent refreshes of the SAME account. OpenAI rotates the refresh token
        // on first use, so N parallel refreshes would leave the losers presenting a dead token and
        // wrongly mark the account `reauth_required`. Acquire the per-account lock, then double-check
        // staleness AGAINST THE STORED token — a peer may have already refreshed (and persisted) while
        // we waited for the lock, in which case the stored access token now has a far-future `exp`.
        let lock = state.refresh_locks.handle(picked);
        let _guard = lock.lock().await;
        let (fresh_account, fresh_tokens) =
            match repo.get_with_tokens(picked.as_str(), &state.cipher).await {
                Ok(Some(p)) => p,
                Ok(None) | Err(_) => return Err(internal_error()),
            };
        // F2 (failure-path single-mark): a peer that held this lock may have failed its refresh and
        // marked the account non-active; `last_refresh` is unchanged on failure, so bail here rather
        // than re-hitting OAuth with our own now-dead token (which would re-mark it once per waiter).
        if fresh_account.status != "active" {
            return Err(account_unavailable_because(
                "status_not_active_at_refresh",
                picked.as_str(),
            ));
        }
        if should_refresh(
            token_exp(&fresh_tokens.access_token),
            fresh_account.last_refresh,
            now,
        ) {
            // Still stale after the lock ⇒ we own the refresh. Use the FRESHLY-read refresh token (a
            // peer's rotation, if any, is already reflected here) rather than our pre-lock copy.
            match state.oauth.refresh(&fresh_tokens.refresh_token).await {
                Ok(refreshed) => {
                    let new = PlainTokens {
                        access_token: refreshed.tokens.access_token,
                        refresh_token: refreshed.tokens.refresh_token,
                        id_token: refreshed.tokens.id_token,
                    };
                    // Persist the rotated tokens — the ONE uniquely critical write on this path: the
                    // refresh already rotated the upstream refresh token, so LOSING this write leaves
                    // a dead refresh token in the DB and the account dies on its next refresh. The
                    // pool's `busy_timeout` (5s) already absorbs lock contention at the driver; these
                    // bounded retries add a backstop for a post-timeout busy or a transient IO blip.
                    // `update_tokens` is an idempotent UPDATE, so retrying is safe. On final failure
                    // fail closed: using the new access token while retaining the now-invalidated
                    // refresh token would create a one-request success followed by durable account
                    // death, and concurrent waiters would still read the old identity.
                    let mut persisted = false;
                    for attempt in 1..=PERSIST_MAX_ATTEMPTS {
                        match repo
                            .update_tokens(picked.as_str(), &new, &state.cipher, now)
                            .await
                        {
                            Ok(()) => {
                                persisted = true;
                                break;
                            }
                            Err(e) if attempt < PERSIST_MAX_ATTEMPTS => {
                                tracing::warn!(
                                    attempt,
                                    error = %e,
                                    "persist of refreshed tokens failed; retrying"
                                );
                                tokio::time::sleep(PERSIST_RETRY_BACKOFF).await;
                            }
                            Err(e) => {
                                tracing::error!(
                                    error = %e,
                                    "failed to persist refreshed tokens after \
                                     {PERSIST_MAX_ATTEMPTS} attempts"
                                );
                            }
                        }
                    }
                    if !persisted {
                        // The exchange already CONSUMED the stored refresh token, so the row now
                        // holds dead credentials regardless. Mark it now (best-effort — storage
                        // may still be down) instead of leaving an `active` account that dies as
                        // `refresh_token_reused` on its next refresh attempt.
                        let _ = repo.update_status(picked.as_str(), "reauth_required").await;
                        state
                            .runtime
                            .note_account_status(picked.as_str(), "reauth_required");
                        return Err(internal_error());
                    }
                    tokens = new;
                }
                Err(OAuthError::Endpoint {
                    code: Some(code), ..
                }) => {
                    if let Some(status) = classify_failure(&code).status() {
                        let _ = repo.update_status(picked.as_str(), status).await;
                        state.runtime.note_account_status(picked.as_str(), status);
                    }
                    return Err(account_unavailable_because(
                        "oauth_refresh_rejected",
                        picked.as_str(),
                    ));
                }
                Err(OAuthError::Endpoint { code: None, .. }) | Err(OAuthError::MalformedJwt(_)) => {
                    let _ = repo.update_status(picked.as_str(), "reauth_required").await;
                    state
                        .runtime
                        .note_account_status(picked.as_str(), "reauth_required");
                    return Err(account_unavailable_because(
                        "oauth_refresh_malformed",
                        picked.as_str(),
                    ));
                }
                Err(OAuthError::Transport(_)) => {}
            }
        } else {
            // Not stale after the lock — a peer refreshed while we waited (the fresh token we just
            // read IS theirs), or it simply isn't due yet. Adopt the stored token for this request
            // instead of calling refresh again with our pre-lock copy.
            tokens = fresh_tokens;
        }
    }
    Ok((
        Account {
            // The selected account's own ChatGPT id travels as the `chatgpt-account-id` header
            // paired with its Bearer (see `Account::chatgpt_account_id` / executor). Taken from the
            // stored row so it always matches the account whose token we're about to send.
            chatgpt_account_id: account.chatgpt_account_id,
            id: account.id,
            base_url: state.upstream_base_url_for(provider).to_string(),
            // Clone (not move) the token out: `PlainTokens` is `ZeroizeOnDrop`, so `tokens` can't be
            // partially moved from — and this way the original is wiped when `tokens` drops here.
            bearer_token: tokens.access_token.clone(),
            is_fedramp: provider == Provider::Codex && is_fedramp_account(&tokens.id_token),
        },
        provider,
    ))
}

/// One reactive OAuth recovery after an upstream 401, even when the rejected JWT still has a
/// future `exp`. The per-account lock plus rejected-token comparison makes concurrent 401s a
/// single-flight: the first waiter refreshes, later waiters adopt the newly stored token.
/// Whether a watchdog error is an upstream 401. Both the streaming (`Upstream`) and buffered
/// (`UpstreamHttp`) shapes carry the status; a rejected bearer surfaces as either.
pub(crate) fn is_unauthorized(error: &WatchdogError) -> bool {
    matches!(
        error,
        WatchdogError::Upstream(Some(signal)) if signal.status == 401
    ) || matches!(
        error,
        WatchdogError::UpstreamHttp(response) if response.signal.status == 401
    )
}

pub(crate) async fn force_refresh_after_unauthorized(
    state: &AppState,
    picked: &AccountId,
    rejected_access_token: &str,
    now: i64,
) -> Result<Option<Account>, Response> {
    ReactiveAuth::new(
        state.store.clone(),
        state.cipher.clone(),
        state.oauth.clone(),
        state.refresh_locks.clone(),
        state.upstream_base_url_for(Provider::Codex).to_string(),
        state.upstream_base_url_for(Provider::Anthropic).to_string(),
    )
    .refresh_after_unauthorized(picked, rejected_access_token, now)
    .await
    .map_err(|error| match error {
        ReactiveAuthError::Internal => internal_error(),
        ReactiveAuthError::AccountUnavailable => {
            account_unavailable_because("reactive_auth_unavailable", "-")
        }
    })
}

/// B5-anthropic Task 3: which SSE dialect the *client* of a Layer-1/Layer-2 recovery-wait speaks —
/// orthogonal to `pool_provider` (which accounts we wait ON: a Codex request always waits on Codex
/// accounts, an Anthropic request always waits on Anthropic accounts, regardless of which dialect
/// the ORIGINAL client speaks). Owns every dialect-specific frame the wait emits (keepalive, in-band
/// error) plus how the recovered upstream stream reaches the client (verbatim, or — T4 — Codex→
/// Anthropic translated). `Clone` is cheap: today's two variants are unit variants; T4's added
/// variant holds an `Arc`.
#[derive(Clone)]
enum WaitClient {
    /// Codex `/responses` client: `response.failed` error frames, `: keepalive` comments, recovered
    /// Codex stream forwarded verbatim. Byte-identical to pre-B5-anthropic behavior — every existing
    /// Codex call site passes this variant, and nothing about its output has changed.
    Codex,
    /// Native Anthropic `/v1/messages` client: `event: error` frames, `event: ping` keepalives,
    /// recovered Anthropic stream forwarded verbatim.
    Anthropic,
    /// Anthropic client served from a Codex pool (the `/v1/messages`→Codex alias path): `event:
    /// error`/`event: ping` frames like `Anthropic`, but the recovered CODEX stream is wrapped in a
    /// FRESH translator (response-side state is built from the stream, so a fresh instance is
    /// correct — see `AnthropicToResponses::translate_request`, which never touches
    /// `message_start_emitted`/`next_block_index`/`blocks`) and emitted as Anthropic SSE. The
    /// factory (rather than a moved instance) exists because Layer 1 and Layer 2 are
    /// mutually-exclusive call sites and the wait fn moves its state into an `async_stream`
    /// generator — a `Fn() -> Box<dyn Translator>` called ONLY at the actual serve site is cleaner
    /// than threading one pre-built instance across the try-layer1-then-layer2 sequence.
    AnthropicTranslated(std::sync::Arc<dyn Fn() -> Box<dyn Translator> + Send + Sync>),
}

impl WaitClient {
    /// The in-band SSE terminal-error frame for this dialect (Global Constraint: POST-200 COMMIT —
    /// see `in_band_error_frame`'s doc for why this is never a dropped/`Err` stream item).
    fn error_frame(&self, outcome: starvation::StarvationOutcome) -> Bytes {
        match self {
            WaitClient::Codex => starvation::in_band_error_frame(outcome),
            WaitClient::Anthropic | WaitClient::AnthropicTranslated(_) => {
                starvation::anthropic_in_band_error_frame(outcome)
            }
        }
    }

    /// A terminal, non-retryable in-band error for an attempt budget discovered after Layer 2
    /// already committed HTTP 200 with keepalives.
    fn attempt_budget_error_frame(&self) -> Bytes {
        match self {
            WaitClient::Codex => Bytes::from_static(
                b"event: response.failed\n\
                  data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\
                  \"invalid_prompt\",\"message\":\"This logical turn exhausted its upstream \
                  attempt budget.\"}}}\n\n",
            ),
            WaitClient::Anthropic | WaitClient::AnthropicTranslated(_) => Bytes::from_static(
                b"event: error\n\
                  data: {\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\
                  \"message\":\"This logical turn exhausted its upstream attempt budget.\"}}\n\n",
            ),
        }
    }

    /// The keepalive frame bytes for this dialect. Returns `Bytes` (not the stream's `Item` type
    /// directly) so callers wrap it as `Ok(..)` at the yield site themselves.
    fn keepalive_bytes(&self) -> Bytes {
        match self {
            // Byte-identical to `starvation::KEEPALIVE_FRAME` — this is NOT a new frame, just the
            // same constant reached through the new seam.
            WaitClient::Codex => Bytes::from_static(starvation::KEEPALIVE_FRAME),
            WaitClient::Anthropic | WaitClient::AnthropicTranslated(_) => {
                starvation::anthropic_ping_frame()
            }
        }
    }

    /// How the recovered upstream stream reaches the client. Verbatim for Codex + native Anthropic;
    /// `AnthropicTranslated` wraps the recovered CODEX stream in a FRESH `Translator` instance
    /// (built by the factory, called here — right at the serve site) so it reaches the client as
    /// Anthropic SSE. Only this recovered REAL stream is ever translated — the outer wait stream's
    /// own keepalive/error frames (already Anthropic-native bytes) are never routed through it, so
    /// they survive the `TranslatingStream` comment-drop untouched (`translate_stream.rs:60`).
    fn wrap_recovered(&self, stream: ResponseStream) -> ResponseStream {
        match self {
            WaitClient::Codex | WaitClient::Anthropic => stream,
            WaitClient::AnthropicTranslated(factory) => {
                crate::translate_stream::wrap_translating_stream(stream, factory())
            }
        }
    }
}

/// B5 Task 3 — Layer 1: serve the soonest `ErrorBackoff` account IMMEDIATELY (no wait) when the
/// caller's own selection just found the eligible pool empty. An error-backoff account is a
/// *probably-fine* soft signal (a short transient-upstream-error window) — better to try it than a
/// fast 503. Called at every empty-pool site in `responses_handler_impl`/`run_failover_loop`;
/// returns `None` when Layer 1 does not apply, and the CALLER must fall through to today's
/// behavior (a 503/502, unchanged) — this function never itself produces the empty-pool error.
///
/// # The GUARD (ported from codex-lb `logic.py:499-524`)
/// Only serves-now when there is MORE THAN ONE capability-filtered `ErrorBackoff` account, OR
/// EXACTLY ONE AND a capability-filtered `HardBlocked` account also exists
/// (`BackoffCensus::error_backoff_count`/`has_hardblocked`, `selector.backoff_census`). A LONE
/// error-backoff account with no hard-blocked peer is NOT served-now — this avoids hammering a
/// single flaky account on every request that happens to arrive while its pool is empty. A
/// `Cooldown`-kind `soonest_recover` result never applies either (it would 429 again; that's Layer
/// 2 / Task 4's wait, not implemented here).
///
/// # Security floor (inviolable)
/// `soonest_recover`/`backoff_census` both apply `standard_pool`'s capability pre-filter BEFORE
/// classifying (`select.rs`), so a cyber request can only ever resolve/count/serve a
/// `security_work_authorized` account here — structurally never a non-authorized one, regardless of
/// which account would otherwise recover soonest.
///
/// # Reuses the existing resolve+execute path — no new execution machinery
/// Exactly the same `resolve_core_account` + `execute_recovery` shape `RouteDecision::Recover`'s
/// `ResendFull` arm and `reroute_cyber_rejection` already use for "reselect after the pool didn't
/// hand back the original candidate, then relay as an anchorless resend" — Layer 1 is just a
/// different CANDIDATE SOURCE (`soonest_recover` instead of `selector.pick`) feeding the same
/// machinery, guarded by the census above.
#[allow(clippy::too_many_arguments)]
async fn try_layer1_serve_now(
    state: &AppState,
    snapshots: &[AccountSnapshot],
    selector: &dyn Selector,
    sel_ctx: &SelectionCtx,
    req: PreparedRequest,
    ctx: RequestCtx,
    session_key: Option<SessionKey>,
    now: i64,
    outcome: &mut RouteOutcome,
    // B5-anthropic Task 3: which SSE dialect the client speaks — see `WaitClient`'s doc. Threaded
    // through so this function's own served-stream site below can dialect-correctly wrap the
    // stream (identity for both variants today; T4 adds a translating one).
    client: &WaitClient,
) -> Option<Response> {
    let recovery = selector.soonest_recover(snapshots, sel_ctx)?;
    if recovery.kind != BackoffKind::ErrorBackoff {
        // Cooldown-kind ⇒ Layer 2 territory (the keepalive wait, Task 4) — not Layer 1.
        return None;
    }
    let census = selector.backoff_census(snapshots, sel_ctx);
    let guard_satisfied = census.error_backoff_count > 1
        || (census.error_backoff_count == 1 && census.has_hardblocked);
    if !guard_satisfied {
        return None;
    }

    let fresh = recovery.account_id;
    state.runtime.record_selected(&fresh, now);
    outcome.account_id = Some(fresh.as_str().to_string());
    let (account, provider) = match resolve_core_account(state, &fresh, now).await {
        Ok(a) => a,
        Err(r) => {
            bench_after_local_refusal(state, &fresh, now);
            return Some(r);
        }
    };
    let health_id = fresh.clone(); // `fresh` is moved into the executor below.
                                   // C9 Task 2: a real upstream attempt on `fresh` — same lease treatment as every other
                                   // streaming selection site (see `execute_recovery_tracked`'s call below, which is
                                   // `execute_recovery`'s exact behavior plus this one added, never-read `in_flight` capability —
                                   // see `execute_recovery`'s doc for why its own signature stays untouched).
                                   // The `?` here returns `None`, which the caller reads as "Layer 1 declined" and falls through
                                   // to Layer 2 / the 503 — an account-attributed local refusal like any other, so it benches too.
                                   // Without this, serve-soonest keeps nominating the same refused account on every request.
    let Some(in_flight) = state.runtime.try_acquire_in_flight_weighted(
        &fresh,
        now,
        &state.lease_metrics,
        sel_ctx.request_pressure_units,
    ) else {
        bench_after_local_refusal(state, &fresh, now);
        return None;
    };
    let response = match execute_recovery_tracked(
        state.executor_for(provider).as_ref(),
        state.continuity.clone(),
        req,
        &account,
        fresh,
        ctx,
        session_key,
        state.runtime.clone(),
        state.runtime_settings.stream_idle_timeout(),
        state.runtime_settings.max_account_attempts(),
        CommitWitness::new(),
        Some(in_flight),
    )
    .await
    {
        Ok(stream) => stream_response(client.wrap_recovered(stream)),
        Err(e) => {
            record_failure(state, &health_id, &e, unix_now()).await;
            surface_watchdog_error(&e)
        }
    };
    Some(response)
}

/// B5 Task 5: emits one [`crate::observability::StarvationSignal`] — the `tracing` event, the
/// `log_bus` event, and the `StarvationMetrics` bump — at a single call site, mirroring the exact
/// triple `run_failover_loop` already performs for [`FailoverSignal`] (`emit()` +
/// `log_bus.publish(..)` + `metrics.record()`, together, at the real transition). Called from every
/// terminal exit of [`layer2_wait_stream`]'s generator — `served` is `Some` ONLY at the genuine
/// splice-success site (see that function's doc, "B5 Task 5" section).
///
/// B10 Task 2: `wake_jitter_applied_ms` is this wait's own [`wake_jitter_offset_ms`] result — the
/// SAME value `layer2_wait_stream` already computed once at wait entry to build
/// `jittered_wake_target_ms` — passed through unchanged so the content-free signal lets an operator
/// see herd-damping is active (and roughly how spread out concurrent waiters are) without a new
/// signal type. `0` on every call site when `wake_jitter_ms` is unset/`0` (the disable lever).
#[allow(clippy::too_many_arguments)]
fn emit_starvation_signal(
    state: &AppState,
    wait_target: &AccountId,
    wait_started: Instant,
    reason: &'static str,
    served: Option<&str>,
    wake_jitter_applied_ms: u64,
) {
    let signal = crate::observability::StarvationSignal {
        reason,
        wait_target_account: wait_target.as_str(),
        served_account: served,
        waited_ms: wait_started.elapsed().as_millis() as u64,
        wake_jitter_applied_ms,
    };
    signal.emit();
    state.log_bus.publish(signal.to_log_event());
    state.starvation_metrics.record();
}

/// B5 Task 4 (THE CRUX) — Layer 2: for a `Cooldown`-kind `soonest_recover` result within budget,
/// commit HTTP 200 SSE IMMEDIATELY (by handing [`layer2_wait_stream`] to [`stream_response`]) and
/// move the wait + re-select + splice entirely INSIDE the stream body. Called at the SAME
/// empty-pool sites as [`try_layer1_serve_now`], immediately after it returns `None`.
///
/// # Why this is safe to call unconditionally after Layer 1 falls through
/// `soonest_recover` is a pure, cheap function over already-fetched snapshots — calling it again
/// here (Layer 1 already called it once, internally) costs nothing and keeps the two layers
/// fully decoupled: Layer 2 never needs Layer 1's internal `Recovery` value threaded across a
/// function boundary. Returns `None` in exactly two cases, both of which mean "Layer 2 does not
/// apply here; the caller must fall through to today's PRE-response fast 503/502":
/// - `soonest_recover` itself returns `None` — every capability-filtered account is either
///   `Eligible` (impossible; the caller only reaches here when selection failed) or `HardBlocked`.
///   Global Constraint: HARDBLOCKED IS NEVER A WAIT TARGET — no HTTP 200 is ever committed for an
///   all-HardBlocked pool, so the caller's ordinary 503/502 fires exactly as before B5.
/// - `recovery.kind == ErrorBackoff` — that account is Layer 1's territory (a lone backoff account
///   whose guard was rejected, per `try_layer1_serve_now`'s doc). Layer 2 must NOT wait on it:
///   waiting the full `error_backoff_secs` window for a single flaky account on every request that
///   happens to see an empty pool would be strictly worse than today's immediate 503, and would
///   silently change `lone_error_backoff_with_no_hardblocked_peer_does_not_serve_now`'s regression
///   contract (Task 3) from an immediate 503 to a slow one.
///
/// # Security floor (inviolable)
/// `soonest_recover` applies the SAME capability pre-filter `try_layer1_serve_now`/`standard_pool`
/// use, so a cyber request can only ever wait for a `security_work_authorized` account. `sel_ctx`
/// (carrying `require_security_work_authorized`) is cloned UNCHANGED into `layer2_wait_stream`,
/// which re-derives its post-wait `fresh_sel_ctx` from that same clone (only `now` is refreshed) —
/// see that function's doc for the re-select-side proof.
#[allow(clippy::too_many_arguments)]
fn try_layer2_recovery_wait(
    state: Arc<AppState>,
    snapshots: &[AccountSnapshot],
    pool: Option<String>,
    pool_provider: Provider,
    selector: Arc<dyn Selector>,
    sel_ctx: &SelectionCtx,
    req: PreparedRequest,
    ctx: RequestCtx,
    session_key: Option<SessionKey>,
    now: i64,
    budget: Duration,
    heartbeat: Duration,
    outcome: &mut RouteOutcome,
    // B5-anthropic Task 3: which SSE dialect the client speaks — see `WaitClient`'s doc. Forwarded
    // into `layer2_wait_stream`, which owns it for the generator's whole lifetime.
    client: WaitClient,
) -> Option<Response> {
    // B5 Task 5: the config-driven DISABLE LEVER — `POLYFLARE_STARVATION_WAIT_BUDGET_SECS=0`
    // resolves to `Duration::ZERO` (see `crate::config::starvation_wait_budget_secs_from_env`'s
    // doc), which turns Layer 2 off entirely: return `None` before even calling `soonest_recover`,
    // so the caller falls straight through to today's PRE-response fast 503/502 — no HTTP 200 is
    // ever committed and not a single keepalive is ever emitted, exactly like an all-HardBlocked
    // pool (Task 4's inviolable 5).
    if budget.is_zero() {
        return None;
    }
    let recovery = selector.soonest_recover(snapshots, sel_ctx)?;
    if recovery.kind != BackoffKind::Cooldown {
        return None;
    }
    // Best-effort observability id: the account this request is WAITING for at commit time — not
    // necessarily the one that ends up served (the post-wait re-select can land on a different,
    // also-recovered account, or none at all). Same content-safe id class every other
    // `outcome.account_id` assignment in this file uses.
    //
    // B5 Task 5: `RouteOutcome`/`RequestLog` are finalized SYNCHRONOUSLY, before
    // `layer2_wait_stream`'s generator body is ever polled (i.e. before the wait has even started)
    // — so this field can ONLY ever record the wait target, structurally, no matter what happens
    // inside the stream. This is the disclosed observability gap from Task 4's report. The fix
    // lives in `layer2_wait_stream`: `crate::observability::StarvationSignal`, emitted from INSIDE
    // the generator at the moment the real account is known, is the authoritative,
    // correctly-attributed record of who actually served a Layer-2 request — see that function's
    // doc and `crate::observability::StarvationSignal`'s doc for the full rationale.
    outcome.account_id = Some(recovery.account_id.as_str().to_string());
    let stream = layer2_wait_stream(
        state,
        pool,
        pool_provider,
        selector,
        sel_ctx.clone(),
        req,
        ctx,
        session_key,
        recovery.account_id,
        recovery.recover_at,
        now,
        heartbeat,
        budget,
        client,
    );
    Some(stream_response(stream))
}

/// B5 Task 4: the actual keepalive-wait-then-splice `ResponseStream`. Built with
/// `async_stream::stream!` — the bounded sleep/keepalive loop, the re-select, and the executor call
/// all run INSIDE the stream body, polled lazily by `Body::from_stream` (i.e. AFTER
/// `stream_response` has already returned its 200). Every `Arc`/owned value here is captured by the
/// generator and must outlive the call that constructed it — this is exactly why `state`/`selector`
/// arrive as owned `Arc`s (not borrows) and `req`/`ctx`/`session_key`/`pool`/`sel_ctx` arrive owned
/// (cloned by the caller, [`try_layer2_recovery_wait`]).
///
/// # Global Constraint — POST-200 COMMIT (the crux)
/// Every exit from this generator after the loop begins is a `yield Ok(..in_band_error_frame..)`
/// followed by `return`, NEVER an `Err` item (which would abort the chunked/HTTP-2 body
/// ungracefully — see `starvation::in_band_error_frame`'s doc) and NEVER anything that could
/// surface as a second HTTP status (impossible by construction: axum's `Body::from_stream` has
/// already committed the 200 by the time this generator is ever polled).
///
/// # Global Constraint — BOUNDED BUDGET
/// `target_ms = recover_at_ms.min(budget_deadline_ms)` caps the sleep loop itself; the explicit
/// `now_ms >= budget_deadline_ms` check after the loop additionally distinguishes "recovered in
/// time" from "budget exceeded" for accounts whose `recover_at` sits PAST the budget. Either way
/// the wait never runs past `wait_start + budget`.
///
/// # Precision note (B5 Task 4 adversarial review, FIX 1)
/// `budget` is honored to MILLISECOND resolution via [`unix_now_ms`], never truncated to whole
/// seconds. `wait_start`/`recover_at` stay `i64` UNIX-*seconds* (their natural granularity — they
/// come from durable `rate_limited`/`cooldown_until` timestamps that are already second-grained),
/// but the budget deadline itself is computed and compared in milliseconds. Doing
/// `wait_start.saturating_add(budget.as_secs() as i64)` — the pre-fix code — silently floors any
/// sub-second budget (e.g. 700ms) to 0, collapsing the entire wait to a same-instant no-op and
/// making the "emit keepalives → hit the budget ceiling" path structurally untestable. DO NOT
/// reintroduce a `.as_secs()` truncation here.
///
/// # Global Constraint — RE-SNAPSHOT AFTER THE WAIT (the load-bearing gotcha)
/// After the wait, this RE-FETCHES the account cache (`state.account_cache.snapshots`) AND
/// re-`overlay`s it with a FRESH `unix_now()` — `RuntimeStates::overlay` (`runtime_state.rs:88-97`)
/// deliberately DROPS an elapsed `cooldown_until`, so re-using the pre-wait `snapshots`/`now` here
/// would still see the stale (pre-recovery) cooldown and this would never serve. `fresh_sel_ctx` is
/// `sel_ctx.clone()` with ONLY `now` overwritten — `require_security_work_authorized`/`tier`/
/// `session_id` are carried over from the ORIGINAL ctx untouched, so the post-wait re-select
/// preserves the security floor exactly as strictly as the pre-wait one did.
///
/// # B5 Task 5 — the content-free starvation signal + the `outcome.account_id` fix
/// `wait_target` (new in Task 5) is the account `try_layer2_recovery_wait` was waiting for at
/// commit time — the SAME id `RouteOutcome.account_id` was already best-effort-set to before this
/// generator was ever polled. [`emit_starvation_signal`] fires at every terminal exit below, always
/// carrying `wait_target`, and carrying the SERVED account (`Some`) only at the genuine
/// splice-success site — this is the authoritative, correctly-attributed record of who actually
/// served the request, fixing the disclosed gap where `RouteOutcome`/`RequestLog` can only ever
/// record the wait target (see `crate::observability::StarvationSignal`'s doc for the full
/// rationale).
///
/// # Global Constraint — HERD DAMPING (B10 Task 1, THE CRUX)
/// Every waiter on the SAME account used to compute an IDENTICAL `target_ms` (below), so N
/// concurrent waiters woke within one heartbeat tick and re-selected in lockstep the instant the
/// account recovered — a self-inflicted thundering herd that can immediately re-429 it. This
/// generator now adds a small, bounded, PER-REQUEST jitter (`wake_jitter_offset_ms`) to its own
/// wake target ONLY — computed once, at wait entry, from this request's own session key
/// (`layer2_wait_request_key`) and the startup-resolved `AppState.wake_jitter_ms`
/// (`POLYFLARE_STARVATION_WAKE_JITTER_MS`, default `0`). It does NOT touch `select.rs` (`pick`
/// stays pure), does NOT touch the account's stored `recover_at`/`cooldown_until`/`backoff_secs`
/// (`soonest_recover`'s cross-account fairness ordering is unchanged — `wait_target`/`recover_at`
/// above are read-only inputs, never written here), and does NOT change WHICH account this waiter
/// is waiting on. `jittered_wake_target_ms` guarantees the jitter only ever DELAYS the wake beyond
/// `target_ms` (never before it) and never past `budget_deadline_ms` (never past the B5 budget
/// ceiling) — see that function's doc.
///
/// B10 Task 1 (THE CRUX): the per-waiter wake-jitter offset — a deterministic, bounded value in
/// `[0, wake_jitter_ms]`. PURE (no clock, no process-global `rand`): the SAME `request_key` always
/// yields the SAME offset (the plan's Global Constraints require a testable, "deterministic-per-
/// request" seam, not process-global `rand`), while DIFFERENT keys generally yield DIFFERENT
/// offsets — this is exactly what desynchronizes concurrent waiters on the same recovering account
/// (see `layer2_wait_stream`'s "Global Constraint — HERD DAMPING" doc). `wake_jitter_ms == 0` ⇒
/// ALWAYS `0` — the documented disable lever (`POLYFLARE_STARVATION_WAKE_JITTER_MS=0`,
/// `crate::config::wake_jitter_ms_from_env`'s default), byte-for-byte today's pre-B10 behavior.
///
/// Deliberately lives here, NOT in `polyflare-core::select` — `pick`/`eligibility`/
/// `soonest_recover` are pure over ACCOUNT snapshots only, with no clock/rand (B10's Global
/// Constraints, mirroring the M2-GATE1 purity contract). This helper is pure too, but over a
/// per-REQUEST key, and is never called from `select.rs`.
///
/// `DefaultHasher` (SipHash, fixed keys) is used rather than `RandomState`'s per-process-randomized
/// hasher — this is precisely why it's deterministic ACROSS PROCESS RUNS too, not merely within
/// one, which is what makes `same_key_is_deterministic` (below) a meaningful test rather than an
/// accident of one run.
pub fn wake_jitter_offset_ms(request_key: &str, wake_jitter_ms: u64) -> u64 {
    if wake_jitter_ms == 0 {
        return 0;
    }
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    request_key.hash(&mut hasher);
    hasher.finish() % wake_jitter_ms.saturating_add(1)
}

/// B10 Task 1: caps `target_ms + jitter_ms` at `budget_deadline_ms` — the "Bounded + never past
/// budget" / "Only spreads LATER, never earlier" Global Constraints, isolated as its own pure
/// function so both are testable without spinning up the generator. Never returns less than
/// `target_ms` (jitter only ever ADDS delay) and never more than `budget_deadline_ms` (jitter can
/// only spend room already inside the existing B5 budget ceiling — it can never extend the wait
/// past it).
pub(crate) fn jittered_wake_target_ms(
    target_ms: i64,
    jitter_ms: u64,
    budget_deadline_ms: i64,
) -> i64 {
    target_ms
        .saturating_add(jitter_ms as i64)
        .min(budget_deadline_ms)
}

/// B10 Task 1: the per-request identifier [`wake_jitter_offset_ms`] is seeded with. The native
/// `/responses` ingress path always derives a `SessionKey` (Hard, from
/// `x-codex-turn-state`/`session_id`; else Soft, from `x-request-id`/`prompt_cache_key`/a content
/// hash of `input` — see `crate::session_key::parse_inbound`), so `session_key` is `Some` in
/// practice: different concurrent waiters (different conversations / different clients) hash to
/// different keys, which is exactly the desync this task needs — and the SAME conversation retried
/// across turns hashes to the SAME key (deterministic), matching the plan's testability
/// requirement. The `None` branch is a defensive fallback for a hypothetical caller that carries no
/// session identity at all: a fresh CSPRNG nonce drawn ONCE here (never inside the sleep loop,
/// still flowing through the same deterministic hash helper above) — the plan's Global Constraints
/// explicitly allow this ("a single bounded rand draw at wait-entry is acceptable" when no stable
/// id is in scope).
fn layer2_wait_request_key(session_key: &Option<SessionKey>) -> String {
    match session_key {
        Some(sk) => sk.value.clone(),
        None => format!("{:x}", rand::random::<u64>()),
    }
}

#[allow(clippy::too_many_arguments)]
fn layer2_wait_stream(
    state: Arc<AppState>,
    pool: Option<String>,
    pool_provider: Provider,
    selector: Arc<dyn Selector>,
    sel_ctx: SelectionCtx,
    req: PreparedRequest,
    ctx: RequestCtx,
    session_key: Option<SessionKey>,
    wait_target: AccountId,
    recover_at: i64,
    wait_start: i64,
    heartbeat: Duration,
    budget: Duration,
    // B5-anthropic Task 3: which SSE dialect the client speaks — see `WaitClient`'s doc. Owned by
    // the generator for its whole lifetime (every yield site below reads it).
    client: WaitClient,
) -> ResponseStream {
    ResponseStream::new(async_stream::stream! {
        // B5 Task 5: wall-clock start of the wait, purely for the content-free
        // `StarvationSignal.waited_ms` field — independent of the `wait_start`/`recover_at`
        // UNIX-second math above (that math is unchanged from Task 4; this is additive).
        let wait_started = Instant::now();
        // See this function's "Precision note" doc: millisecond math, never `.as_secs()`.
        let budget_deadline_ms = wait_start
            .saturating_mul(1000)
            .saturating_add(budget.as_millis() as i64);
        let recover_at_ms = recover_at.saturating_mul(1000);
        // Never sleep past whichever comes first: the account's own recovery time, or the budget.
        let target_ms = recover_at_ms.min(budget_deadline_ms);

        // B10 Task 1 (THE CRUX): the per-waiter wake-jitter offset, computed ONCE here (never
        // re-drawn per heartbeat — see `wake_jitter_offset_ms`'s doc) from this request's own
        // session key + the startup-resolved `AppState.wake_jitter_ms`. `jittered_target_ms` only
        // ever DELAYS the wake beyond `target_ms` (never before it) and is capped at
        // `budget_deadline_ms` (never past the B5 budget) — see `jittered_wake_target_ms`'s doc and
        // this function's "Global Constraint — HERD DAMPING" doc above. `wake_jitter_ms == 0` (the
        // default) makes this byte-for-byte today's pre-B10 `target_ms`.
        let request_key = layer2_wait_request_key(&session_key);
        let jitter_ms = wake_jitter_offset_ms(&request_key, state.runtime_settings.wake_jitter_ms());
        let jittered_target_ms = jittered_wake_target_ms(target_ms, jitter_ms, budget_deadline_ms);

        loop {
            let t_ms = unix_now_ms();
            if t_ms >= jittered_target_ms {
                break;
            }
            let remaining_ms = (jittered_target_ms - t_ms).max(1) as u64;
            let tick = heartbeat.min(Duration::from_millis(remaining_ms));
            tokio::time::sleep(tick).await;
            // Only emit a keepalive if we're still genuinely waiting (avoids one trailing,
            // pointless keepalive emitted in the same instant selection is about to be retried).
            if unix_now_ms() < jittered_target_ms {
                yield Ok(client.keepalive_bytes());
            }
        }

        // BOUNDED BUDGET: the account's own recovery may sit PAST the budget (`target_ms` above
        // was capped at `budget_deadline_ms` in that case) — distinguish that from a genuine
        // recovery.
        if unix_now_ms() >= budget_deadline_ms && unix_now_ms() < recover_at_ms {
            emit_starvation_signal(
                &state,
                &wait_target,
                wait_started,
                starvation::StarvationOutcome::BudgetExceeded.code(),
                None,
                jitter_ms,
            );
            yield Ok(client.error_frame(starvation::StarvationOutcome::BudgetExceeded));
            return;
        }

        // RE-SNAPSHOT (see this function's doc): fresh fetch + fresh overlay + fresh `now`.
        let fresh_now = unix_now();
        let mut fresh_sel_ctx = sel_ctx.clone();
        fresh_sel_ctx.now = fresh_now; // every other field (notably
                                        // `require_security_work_authorized`) is carried over
                                        // from `sel_ctx` UNCHANGED — the security floor.
        let fresh_snapshots = match state.account_cache.snapshots(&state.store).await {
            Ok(s) => s,
            Err(_) => {
                emit_starvation_signal(
                    &state,
                    &wait_target,
                    wait_started,
                    starvation::StarvationOutcome::StillNothing.code(),
                    None,
                    jitter_ms,
                );
                yield Ok(client.error_frame(starvation::StarvationOutcome::StillNothing));
                return;
            }
        };
        let mut fresh_snapshots =
            filter_by_provider_and_pool(&fresh_snapshots, pool_provider, pool.as_deref());
        state
            .model_catalog
            .retain_accounts_supporting(&mut fresh_snapshots, &req.model);
        state.runtime.overlay(&mut fresh_snapshots, fresh_now);

        let fresh = match selector.pick(&fresh_snapshots, &fresh_sel_ctx) {
            Some(id) => id,
            None => {
                emit_starvation_signal(
                    &state,
                    &wait_target,
                    wait_started,
                    starvation::StarvationOutcome::StillNothing.code(),
                    None,
                    jitter_ms,
                );
                yield Ok(client.error_frame(starvation::StarvationOutcome::StillNothing));
                return;
            }
        };

        state.runtime.record_selected(&fresh, fresh_now);
        let (account, provider) = match resolve_core_account(&state, &fresh, fresh_now).await {
            Ok(a) => a,
            Err(_) => {
                bench_after_local_refusal(&state, &fresh, fresh_now);
                emit_starvation_signal(
                    &state,
                    &wait_target,
                    wait_started,
                    starvation::StarvationOutcome::ExecutorError.code(),
                    None,
                    jitter_ms,
                );
                yield Ok(client.error_frame(starvation::StarvationOutcome::ExecutorError));
                return;
            }
        };
        let health_id = fresh.clone(); // `fresh` is moved into the executor below.
        // C9 Task 2: the Layer-2 wait's actual served attempt is a real upstream request on
        // `fresh` — same lease treatment as every other streaming selection site.
        let Some(in_flight) =
            state
                .runtime
                .try_acquire_in_flight_weighted(
                    &fresh,
                    fresh_now,
                    &state.lease_metrics,
                    sel_ctx.request_pressure_units,
                )
        else {
            bench_after_local_refusal(&state, &fresh, fresh_now);
            emit_starvation_signal(
                &state,
                &wait_target,
                wait_started,
                starvation::StarvationOutcome::BudgetExceeded.code(),
                None,
                jitter_ms,
            );
            yield Ok(client.error_frame(starvation::StarvationOutcome::BudgetExceeded));
            return;
        };
        match execute_recovery_tracked(
            state.executor_for(provider).as_ref(),
            state.continuity.clone(),
            req,
            &account,
            fresh,
            ctx,
            session_key,
            state.runtime.clone(),
            state.runtime_settings.stream_idle_timeout(),
            state.runtime_settings.max_account_attempts(),
            CommitWitness::new(),
            Some(in_flight),
        )
        .await
        {
            Ok(real_stream) => {
                // SPLICE: the account actually serving this request is known NOW — this is the fix
                // for the disclosed `outcome.account_id` gap (see `try_layer2_recovery_wait`'s doc):
                // emit the AUTHORITATIVE served-account signal HERE, before forwarding the real
                // upstream stream verbatim (the client's actual answer, not a synthetic frame).
                emit_starvation_signal(
                    &state,
                    &wait_target,
                    wait_started,
                    starvation::STARVATION_RECOVERED_REASON,
                    Some(health_id.as_str()),
                    jitter_ms,
                );
                // B5-anthropic Task 3: dialect-wrap BEFORE forwarding — identity for `Codex`/
                // `Anthropic` today (T4's translating variant re-shapes the recovered Codex stream
                // into Anthropic SSE here).
                let mut real_stream = client.wrap_recovered(real_stream);
                while let Some(item) = real_stream.next().await {
                    yield item;
                }
            }
            Err(e) => {
                record_failure(&state, &health_id, &e, unix_now()).await;
                let attempt_budget_exhausted =
                    matches!(e, WatchdogError::AttemptBudgetExhausted);
                emit_starvation_signal(
                    &state,
                    &wait_target,
                    wait_started,
                    if attempt_budget_exhausted {
                        "logical_turn_attempts_exhausted"
                    } else {
                        starvation::StarvationOutcome::ExecutorError.code()
                    },
                    None,
                    jitter_ms,
                );
                yield Ok(if attempt_budget_exhausted {
                    client.attempt_budget_error_frame()
                } else {
                    client.error_frame(starvation::StarvationOutcome::ExecutorError)
                });
            }
        }
    })
}

/// TA6(b) Task 2: react to a `WatchdogError::CapabilityRejection` surfaced by Task 1's Armed-path
/// peek — the current owner cannot serve `cyber_policy`-gated (security) work. Reuses the EXACT
/// `ResendFull`/`execute_recovery` machinery `RouteDecision::Recover` already uses (see
/// `responses_handler_impl`'s `ingress.rs:~630` sibling branch): the caller passes the SAME
/// `anchorless_req` shape Task 1's rejecting attempt was armed with, this re-selects with
/// `SelectionCtx.require_security_work_authorized = true` (the selector's existing TA6 hard
/// pre-filter — `select.rs:294,454`), executes on the chosen capability-holding account, and
/// relays. `execute_recovery`'s `wrap_stream(..., OutcomeKind::Recovered, ...)` re-homes ownership
/// via `record_recovery` at stream completion — the same machinery `RouteDecision::Recover` uses,
/// so this function never calls `record_recovery` directly.
///
/// SECURITY FLOOR (inviolable): if the capability-filtered re-select yields no account, this
/// returns [`no_authorized_account_for_security_work`] — a clean, DISTINCT client error — and
/// NEVER falls back to an unfiltered pick or retries on a non-authorized account. `recovery` is
/// expected to be `RecoveryPlan::ResendFull` (the only shape an Armed watchdog that reached a real
/// upstream response can be armed with alongside a full-resend-shaped turn); any other shape (a
/// bare-tail `SignalClient` turn, which carries no self-sufficient resend body to safely reroute)
/// falls back to the ordinary generic-failure response, unchanged — content-safe, and still never
/// an unfiltered retry.
///
/// No double-relay: this is only ever reached when `CapabilityRejection` was returned as an `Err`
/// from `execute_with_watchdog` — which (per Task 1's peek-before-relay) means NO client byte was
/// ever written for this turn. This function's own relay is therefore the client's first and only
/// response, never a second one layered on top of content already sent.
#[allow(clippy::too_many_arguments)]
async fn reroute_cyber_rejection(
    state: &AppState,
    recovery: RecoveryPlan,
    snapshots: &[AccountSnapshot],
    selector: &dyn Selector,
    sel_ctx: &SelectionCtx,
    ctx: RequestCtx,
    session_key: Option<SessionKey>,
    now: i64,
    outcome: &mut RouteOutcome,
) -> Response {
    outcome.require_security_work_authorized = true;
    let anchorless_req = match recovery {
        RecoveryPlan::ResendFull { anchorless_req } => anchorless_req,
        _ => return (StatusCode::BAD_GATEWAY, "upstream error").into_response(),
    };

    // SECURITY FLOOR: filter to capability-holders BEFORE picking — never an unfiltered fallback.
    let mut cyber_ctx = sel_ctx.clone();
    cyber_ctx.require_security_work_authorized = true;
    let fresh = match selector.pick(snapshots, &cyber_ctx) {
        Some(id) => id,
        None => return no_authorized_account_for_security_work(),
    };

    state.runtime.record_selected(&fresh, now);
    outcome.account_id = Some(fresh.as_str().to_string());
    let (account, provider) = match resolve_core_account(state, &fresh, now).await {
        Ok(a) => a,
        Err(r) => {
            bench_after_local_refusal(state, &fresh, now);
            return r;
        }
    };
    let health_id = fresh.clone(); // `fresh` is moved into the executor below.
                                   // TA6(b) Task 3: captured BEFORE `session_key` moves into `execute_recovery` below — the stamp
                                   // (on success) is what makes the NEXT turn on this session pre-filter from the start instead
                                   // of paying the reject-and-move cost again.
    let session_key_for_stamp = session_key.clone();
    // C9 Task 2: the cyber-reroute's move onto the capability-holding account is a real upstream
    // attempt on `fresh` — same lease treatment as every other streaming selection site.
    let Some(in_flight) = state.runtime.try_acquire_in_flight_weighted(
        &fresh,
        now,
        &state.lease_metrics,
        sel_ctx.request_pressure_units,
    ) else {
        bench_after_local_refusal(state, &fresh, now);
        return admission_refused_on_reselect(fresh.as_str(), sel_ctx.request_pressure_units);
    };
    match execute_recovery_tracked(
        state.executor_for(provider).as_ref(),
        state.continuity.clone(),
        anchorless_req,
        &account,
        fresh,
        ctx,
        session_key,
        state.runtime.clone(),
        state.runtime_settings.stream_idle_timeout(),
        state.runtime_settings.max_account_attempts(),
        CommitWitness::new(),
        Some(in_flight),
    )
    .await
    {
        Ok(stream) => {
            // The move succeeded (upstream accepted the anchor-stripped resend on the
            // capability-holding account): stamp the session sticky-cyber NOW, so a LATER `prepare`
            // on this session pre-filters via `SelectionCtx.require_security_work_authorized`
            // instead of re-hitting a `cyber_policy` rejection — cost paid ONCE per session. Best-
            // effort: a stamp failure never fails the (already-successful) turn itself.
            if let Some(sk) = session_key_for_stamp {
                let _ = state
                    .continuity
                    .mark_required_capability(&sk, "security_work")
                    .await;
            }
            stream_response(stream)
        }
        Err(e) => {
            record_failure(state, &health_id, &e, unix_now()).await;
            surface_watchdog_error(&e)
        }
    }
}

/// B4 Task 4 (THE CRUX): the bounded cross-account failover loop. Generalizes
/// `reroute_cyber_rejection`'s single reselect→`execute_recovery`→relay step into a bounded loop,
/// composed from Tasks 1-3: [`failover_verdict`] (T1, the retryable-vs-terminal classifier),
/// [`exclude_tried`] (T2, the order-preserving tried-account pool filter), and [`CommitWitness`]
/// (T3, the commit-barrier signal).
///
/// Called ONLY for a request whose FIRST attempt (made by the caller, `responses_handler_impl_with_max_attempts`'s
/// `RouteDecision::Route` arm, via `execute_with_watchdog_tracked`) already failed with
/// `first_err`/`committed` AND was anchorless (`WatchdogArm::Disarmed` — see that call site's
/// CONTINUITY OWNERSHIP gate; a live-anchor turn never reaches this function at all). `resend_req`
/// is the ORIGINAL (already anchorless, hence self-sufficient) request body, reused unchanged on
/// every reselected account — mirroring `reroute_cyber_rejection`'s `anchorless_req` role, and the
/// established "reselect-after-failure ⇒ `execute_recovery`" idiom this codebase already uses for
/// both `reroute_cyber_rejection` and `RouteDecision::Recover`'s `ResendFull` arm (never
/// `execute_with_watchdog`, which is for a FIRST attempt only).
///
/// # Bookkeeping order (load-bearing — mirrors the plan's literal sequencing)
/// `tried` starts EMPTY. Each loop iteration evaluates `failover_verdict` for the account that
/// JUST failed using the `tried` set as it stood BEFORE that failure (i.e. `attempts_left` counts
/// "attempts already spent (`tried.len()`) + this one" against `max_attempts`); only on a
/// `FailoverNext` verdict is the failed account inserted into `tried` and excluded from the next
/// pick. This is what makes `max_attempts == 1` collapse to zero loop iterations (`0 + 1 < 1` is
/// false) — the one-shot regression proof — and what makes `max_attempts == 3` surface after
/// EXACTLY 3 total upstream attempts, not fewer or more.
///
/// # Security floor (inviolable — see the plan's Global Constraints)
/// `sel_ctx` is the SAME `SelectionCtx` the first attempt used, passed by shared reference and
/// never mutated: `require_security_work_authorized` is never reset to `false` here. Every reselect
/// (`exclude_tried` + `selector.pick`) re-applies that same flag via the selector's existing TA6
/// hard pre-filter (`select.rs`). If the filtered reselect ever returns `None` while the flag is
/// set, this returns [`no_authorized_account_for_security_work`] — the distinct security 503 —
/// NEVER an unfiltered retry (codex-lb's `retry.py:698-717` degrade is explicitly NOT ported here).
/// If the flag is unset, ordinary pool exhaustion returns [`no_eligible`] (today's 503), matching
/// `RouteDecision::Recover`'s existing exhaustion response for the same "selector picked nothing"
/// situation.
///
/// # Commit barrier (inviolable — see the plan's Global Constraints)
/// Every `Err` this function's own `execute_recovery_tracked` calls can produce is, BY
/// CONSTRUCTION, always pre-relay (see [`CommitWitness`]'s doc: these functions only ever return
/// `Err` before `wrap_stream` runs) — so `commit.is_committed()` reads `false` on every iteration of
/// THIS loop, same as the caller's own first-attempt `committed` this function is seeded with. This
/// is not a coincidence to special-case away: it is the structural reason a double-relay is
/// impossible here at all — once ANY attempt (the caller's first, or one of this loop's) returns
/// `Ok(stream)`, the function returns immediately and no further attempt is ever made. `committed`
/// is still threaded and checked explicitly (never hard-coded `false`) so `failover_verdict`'s
/// contract stays honest and any FUTURE change to the watchdog's `Err` shape can't silently
/// reintroduce a double-relay risk without this loop's own logic changing to match.
#[allow(clippy::too_many_arguments)]
async fn run_failover_loop(
    // B5 Task 4: widened from `&AppState` to `&Arc<AppState>` SOLELY so this function can hand an
    // owned `Arc<AppState>` (`state.clone()`) into `try_layer2_recovery_wait`'s 'static stream —
    // every pre-existing `state.field`/`resolve_core_account(state, ..)` use below is unchanged
    // (Rust's deref coercion resolves `&Arc<AppState>` to `&AppState` identically to before).
    state: &Arc<AppState>,
    first_failed_id: AccountId,
    first_err: WatchdogError,
    first_committed: bool,
    resend_req: PreparedRequest,
    snapshots: &[AccountSnapshot],
    selector: &dyn Selector,
    // B5 Task 4: an owned twin of `selector` (the caller's `Arc<dyn Selector>`), needed alongside
    // the borrowed `selector` above because `try_layer2_recovery_wait`'s stream must own it
    // ('static). Kept as a SEPARATE param (rather than widening `selector` itself, as `state`
    // was) to avoid touching this function's many pre-existing `selector.pick(..)` call sites.
    selector_arc: Arc<dyn Selector>,
    sel_ctx: &SelectionCtx,
    ctx: RequestCtx,
    session_key: Option<SessionKey>,
    now: i64,
    max_attempts: u32,
    // B5 Task 4: this site's own empty-pool candidate pool is narrowed by (provider=Codex, pool) —
    // `pool` wasn't previously threaded into this function at all; Layer 2's re-select needs it to
    // re-run the identical `filter_by_provider_and_pool` narrowing post-wait.
    pool: Option<String>,
    starvation_budget: Duration,
    starvation_heartbeat: Duration,
    outcome: &mut RouteOutcome,
) -> Response {
    let mut tried: HashSet<AccountId> = HashSet::new();
    let mut failed_id = first_failed_id;
    let mut err = first_err;
    let mut committed = first_committed;

    loop {
        // `tried.len()` does NOT yet include `failed_id` — see the doc's "Bookkeeping order".
        let attempts_left = (tried.len() as u32) + 1 < max_attempts;
        if failover_verdict(&err, attempts_left, committed) == FailoverVerdict::Surface {
            return surface_watchdog_error(&err);
        }
        // FailoverNext: this account is excluded from every future pick this request (T2). Clone
        // the id BEFORE it moves into `tried` — the observability signal below needs it as
        // `from_account` once `fresh` (the `to_account`) is known.
        let from_id = failed_id.clone();
        tried.insert(failed_id);

        let candidates = exclude_tried(snapshots, &tried);
        let fresh = match selector.pick(&candidates, sel_ctx) {
            Some(id) => id,
            None => {
                // B5 Task 3 — Layer 1: before surfacing the exhaustion error below, try the
                // guarded serve-soonest-error-backoff candidate over the SAME `candidates` (already
                // `exclude_tried`'d, so an account this request already tried is never re-served).
                // Cloned (not moved) so the ORIGINALS survive for Layer 2 below when Layer 1
                // doesn't apply — `try_layer1_serve_now`'s signature is untouched (Task 3 is
                // frozen), so the caller must clone instead.
                let layer1 = try_layer1_serve_now(
                    state,
                    &candidates,
                    selector,
                    sel_ctx,
                    resend_req.clone(),
                    ctx.clone(),
                    session_key.clone(),
                    now,
                    outcome,
                    &WaitClient::Codex,
                )
                .await;
                if let Some(resp) = layer1 {
                    return resp;
                }
                // B5 Task 4 — Layer 2: Cooldown-kind (or nothing at all / HardBlocked-only) is
                // Layer 1's fall-through territory. `state.clone()` is a cheap `Arc` clone (this
                // function's own `state` param is `&Arc<AppState>` — see its doc above).
                if let Some(resp) = try_layer2_recovery_wait(
                    state.clone(),
                    &candidates,
                    pool.clone(),
                    Provider::Codex,
                    selector_arc.clone(),
                    sel_ctx,
                    resend_req,
                    ctx,
                    session_key,
                    now,
                    starvation_budget,
                    starvation_heartbeat,
                    outcome,
                    WaitClient::Codex,
                ) {
                    return resp;
                }
                // SECURITY FLOOR: the flag is never reset — a filtered exhaustion is the distinct
                // security 503, never an unfiltered fallback. Otherwise, ordinary exhaustion
                // (e.g. a single-account pool whose only account just failed) surfaces exactly
                // like the immediate-Surface case: today's generic 502 — NOT `no_eligible()`'s
                // 503, which is reserved for "the selector found nothing BEFORE any attempt was
                // ever made" (`RouteDecision::NoEligibleAccount` / `RouteDecision::Recover`'s own
                // exhaustion). Regression-locked by the wedge suite `failure_routing.rs` (a
                // single-account pool's retryable failure has always surfaced as 502).
                return if sel_ctx.require_security_work_authorized {
                    no_authorized_account_for_security_work()
                } else {
                    surface_watchdog_error(&err)
                };
            }
        };
        // B4/B5 Task 5: the content-free failover signal — emitted exactly HERE, the actual
        // `FailoverNext` transition (a fresh account was just selected to replace `from_id`),
        // never merely at classification time. `attempt` is the 1-indexed upstream attempt this
        // request is now making (`tried.len()` already counts every account tried so far,
        // including `from_id`, per the "Bookkeeping order" doc above). Content-safety: `reason` is
        // a fixed bucket label (never the raw upstream code/message — see `failover_reason_code`),
        // and both ids are the same content-free row-id class `RequestLog::account_id` already
        // carries. NEVER a body/message/frame.
        let failover_signal = FailoverSignal {
            reason: failover_reason_code(&err),
            from_account: from_id.as_str(),
            to_account: fresh.as_str(),
            attempt: tried.len() as u32 + 1,
        };
        failover_signal.emit();
        state.log_bus.publish(failover_signal.to_log_event());
        state.failover_metrics.record();

        state.runtime.record_selected(&fresh, now);
        outcome.account_id = Some(fresh.as_str().to_string());
        let (account, provider) = match resolve_core_account(state, &fresh, now).await {
            Ok(a) => a,
            Err(r) => {
                bench_after_local_refusal(state, &fresh, now);
                return r;
            }
        };
        let health_id = fresh.clone(); // `fresh` is moved into the executor below.
        let commit = CommitWitness::new();
        // C9 Task 2 (THE CRUX — release A before B): a fresh lease for THIS iteration's account,
        // acquired right after selection. Moved into `execute_recovery_tracked` below: on `Ok`, it
        // rides inside the returned `ObservingStream` for the life of the client's response. On
        // `Err(e2)` it is released BY THE TIME `.await` resolves here — `execute_recovery_tracked`
        // only reaches `wrap_stream` (which is where the guard would move into a stream) on its own
        // success path, so a failed attempt drops the guard inside that function's own stack frame,
        // strictly before this match arm runs, and therefore strictly before the loop's next
        // `selector.pick` (at the top of the next iteration) can ever select account B. No explicit
        // `drop()` needed — this is Rust's ordinary move-then-scope-end semantics, not a special case.
        let Some(in_flight) = state.runtime.try_acquire_in_flight_weighted(
            &fresh,
            now,
            &state.lease_metrics,
            sel_ctx.request_pressure_units,
        ) else {
            // The loop's own `tried` set does not protect us here: this returns immediately rather
            // than continuing to the next candidate, so without the bench a deterministic selector
            // hands the NEXT request the same refused account and the failover loop reproduces the
            // very stall it exists to escape.
            bench_after_local_refusal(state, &fresh, now);
            return admission_refused_on_reselect(fresh.as_str(), sel_ctx.request_pressure_units);
        };
        match execute_recovery_tracked(
            state.executor_for(provider).as_ref(),
            state.continuity.clone(),
            resend_req.clone(),
            &account,
            fresh,
            ctx.clone(),
            session_key.clone(),
            state.runtime.clone(),
            state.runtime_settings.stream_idle_timeout(),
            max_attempts,
            commit.clone(),
            Some(in_flight),
        )
        .await
        {
            Ok(stream) => return stream_response(stream),
            Err(e2) => {
                record_failure(state, &health_id, &e2, unix_now()).await;
                failed_id = health_id;
                err = e2;
                committed = commit.is_committed();
            }
        }
    }
}

/// The bare `/responses` ingress entrypoint: selects over ALL Codex accounts (no pool filter).
/// Takes the RAW request bytes (not the `Json` extractor) so the native path can forward them
/// upstream verbatim — no parse→re-serialize round-trip (see `PreparedRequest::raw_body`).
pub async fn responses_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    responses_route(state, None, headers, body).await
}

/// The pooled `/{pool}/responses` ingress entrypoint: selects only over Codex accounts tagged with
/// the `{pool}` slug (see `filter_by_pool`).
pub async fn pooled_responses_handler(
    State(state): State<Arc<AppState>>,
    Path(pool): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    responses_route(state, Some(pool), headers, body).await
}

/// Answers a `GET` on the Codex-native `/responses` (and `/{pool}/responses`) path with
/// `426 Upgrade Required`, so a WebSocket-capable Codex client cleanly and permanently falls back
/// to HTTP-SSE for the rest of that session, instead of hard-failing.
///
/// WHY 426 specifically — do not "helpfully" change this to a 404 or 501: the real Codex CLI's
/// WS→HTTP fallback logic (`codex-rs/core/src/client.rs`, ~line 1596) checks for exactly
/// `StatusCode::UPGRADE_REQUIRED` at WS-handshake time. That is the SOLE trigger for
/// `WebsocketStreamOutcome::FallbackToHttp`, which flips `force_http_fallback` — a
/// session-lifetime, one-way switch, so the client never re-attempts WS again this session. Any
/// other status (404, 405, 500, …) is NOT recognized as a fallback signal by Codex; it surfaces as
/// a hard client error instead of a degrade.
///
/// PolyFlare has no WebSocket support at all today. Without this route, a client configured with
/// `supports_websockets = true` sends a `GET` upgrade request here, axum's default routing 405s
/// it (Method Not Allowed on a POST-only route), and the client hard-fails instead of degrading.
///
/// This is a deliberate, TEMPORARY correctness shim, not a permanent refusal: real WebSocket
/// support is a planned future milestone. When it lands, this handler should be replaced by an
/// actual upgrade handshake on these paths, not simply deleted — until then, 426 is the correct
/// steady-state answer to a WS attempt here.
///
/// Answers unconditionally on `GET` — it does not inspect `Upgrade`/`Connection` request headers
/// to distinguish a genuine WS handshake from a plain browser GET. That's the simpler option, and
/// it cannot mislead either way: `/responses` and `/{pool}/responses` are POST-only Codex-proxy
/// endpoints with no legitimate GET use, so a 426 with an explanatory body is an accurate answer
/// to any GET here, not just an upgrade attempt. Gating on headers would add parsing complexity
/// (and a header-shape assumption) for no correctness benefit.
pub async fn websocket_fallback_handler() -> Response {
    (
        StatusCode::UPGRADE_REQUIRED,
        "PolyFlare serves HTTP-SSE only on this endpoint; WebSocket upgrades are not supported.",
    )
        .into_response()
}

/// Persist independently observed stream timing and any captured usage, computing cost when the
/// required input/output counts are present. Content-free (numbers + model slug only). Missing
/// usage remains `None`; it is never fabricated as zero.
///
/// Non-blocking by construction: the stream wrapper calls this synchronously only to offer the
/// update to the Store's bounded writer. A slow or failing SQLite write therefore cannot delay or
/// fail the client's response, which has already been fully streamed by the time this runs.
fn apply_captured_usage(
    store: &Store,
    request_id: &str,
    model: Option<&str>,
    custom_pricing: Option<polyflare_core::pricing::CustomModelRates>,
    effective_service_tier: Option<&str>,
    captured: usage_capture::CapturedUsage,
) -> Option<tokio::sync::oneshot::Receiver<bool>> {
    let u = captured.usage.unwrap_or_default();
    // The served tier is RECORDED but deliberately NOT used for billing on the Codex path.
    //
    // Codex reports `service_tier: "default"` on `response.completed` even for turns it genuinely
    // serves at the priority tier — see openai/codex#30413 (open) and the Rho project, which
    // shipped a "fast mode was not applied" notice on this signal and then removed it as
    // unreliable (matthewyjiang/rho#675). Measuring this deployment's own traffic reproduces it:
    // priority-requested turns run ~1.3-1.9x the tokens/sec of standard ones on every model, and
    // reach first token several times sooner, while 100% of them report `default`.
    //
    // Billing therefore stays on the tier that was REQUESTED. Trusting the reported value here
    // would under-report real spend, which is the more dangerous error for a cost tracker: an
    // operator would believe they are paying standard rates for service being delivered — and
    // charged — at priority. `custom_pricing`'s tier still arrives via `effective_service_tier`,
    // which for a custom provider comes from that provider's own contract rather than this field.
    let served = captured.served_tier.map(|tier| tier.as_str());
    let cost = if let Some(rates) = custom_pricing {
        let (input_price, cached_price, output_price) = rates.rates_for(effective_service_tier);
        u.input_tokens.zip(u.output_tokens).map(|(input, output)| {
            let cached = u.cached_input_tokens.unwrap_or(0).clamp(0, input.max(0));
            let uncached = input.saturating_sub(cached);
            let orchestration_input = u.orchestration_input_tokens.unwrap_or(0).max(0);
            let orchestration_cached = u
                .orchestration_cached_input_tokens
                .unwrap_or(0)
                .clamp(0, orchestration_input);
            let orchestration_uncached = orchestration_input.saturating_sub(orchestration_cached);
            let orchestration_output = u.orchestration_output_tokens.unwrap_or(0).max(0);
            (uncached as f64 * input_price
                + cached as f64 * cached_price
                + output as f64 * output_price
                + orchestration_uncached as f64 * input_price
                + orchestration_cached as f64 * cached_price
                + orchestration_output as f64 * output_price)
                / 1_000_000.0
        })
    } else {
        model
            .and_then(polyflare_core::pricing::pricing_for_model)
            .zip(u.input_tokens)
            .zip(u.output_tokens)
            .map(|((pricing, input), output)| {
                polyflare_core::pricing::cost_usd(
                    pricing,
                    input,
                    output,
                    u.cached_input_tokens.unwrap_or(0),
                    effective_service_tier,
                )
            })
    };
    match store.enqueue_request_usage_with_receipt(polyflare_store::RequestUsageUpdate {
        request_id: request_id.to_string(),
        input_tokens: u.input_tokens,
        output_tokens: u.output_tokens,
        cached_input_tokens: u.cached_input_tokens,
        cache_write_input_tokens: u.cache_write_input_tokens,
        reasoning_tokens: u.reasoning_tokens,
        reported_total_tokens: u.reported_total_tokens,
        orchestration_input_tokens: u.orchestration_input_tokens,
        orchestration_output_tokens: u.orchestration_output_tokens,
        orchestration_cached_input_tokens: u.orchestration_cached_input_tokens,
        cost_usd: cost,
        actual_service_tier: served.map(str::to_string),
        latency_first_token_ms: captured.ttft_ms,
        duration_ms: captured.duration_ms,
        protocol_outcome: captured.protocol_outcome,
    }) {
        Ok(receipt) => Some(receipt),
        Err(e) => {
            tracing::warn!(
                target: "polyflare_server::request",
                error = %e,
                "request usage queue failed"
            );
            None
        }
    }
}

/// Shared `/responses` route: thin timing + content-safe logging wrapper around
/// [`responses_handler_impl`], parameterized by the optional account-pool slug. See
/// `crate::observability` for the content-safety constraint on what may be logged.
pub(crate) async fn responses_route(
    state: Arc<AppState>,
    pool: Option<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    responses_route_with_transport(state, pool, headers, body, None, None, None).await
}

pub(crate) async fn responses_custom_route_for_ws(
    state: Arc<AppState>,
    headers: HeaderMap,
    body: Bytes,
    targets: Vec<(CustomProvider, ProviderModel)>,
    priority_decision: crate::priority_policy::PriorityDecision,
) -> Response {
    responses_route_with_transport(
        state,
        None,
        headers,
        body,
        Some("websocket"),
        Some(targets),
        Some(priority_decision),
    )
    .await
}

pub(crate) async fn responses_translation_route_for_ws(
    state: Arc<AppState>,
    pool: Option<String>,
    headers: HeaderMap,
    body: Bytes,
    priority_decision: crate::priority_policy::PriorityDecision,
) -> Response {
    responses_route_with_transport(
        state,
        pool,
        headers,
        body,
        Some("websocket"),
        None,
        Some(priority_decision),
    )
    .await
}

async fn responses_route_with_transport(
    state: Arc<AppState>,
    pool: Option<String>,
    headers: HeaderMap,
    body: Bytes,
    downstream_transport: Option<&'static str>,
    resolved_custom_route: Option<Vec<(CustomProvider, ProviderModel)>>,
    preapplied_priority_decision: Option<crate::priority_policy::PriorityDecision>,
) -> Response {
    let start = Instant::now();
    maybe_capture_fingerprint(&state, "POST", "/responses", &headers);
    // Keep a Store clone BEFORE `state` moves into the impl; it owns the bounded writer handle.
    let log_store = state.store.clone();
    // A second Store handle moves into the stream wrapper to queue usage/cost on this same row
    // after completion. The single FIFO writer preserves insert-before-update ordering.
    let usage_store = state.store.clone();
    let pressure_runtime = state.runtime.clone();
    // Same reason: `state` moves into the impl below, so grab the log-bus handle first.
    let log_bus = state.log_bus.clone();
    // C11b Task 2: same reason — grab the content-free `upstream_requests` counter handle before
    // `state` moves into the impl below.
    let upstream_request_metrics = state.upstream_request_metrics.clone();
    let quota_state = state.clone();
    let catalog_etag = match pool.as_deref() {
        Some(pool) => crate::catalog::pooled_models_etag(&state, pool).await,
        None => crate::catalog::root_models_etag(&state).await,
    };
    let (mut response, outcome) = responses_handler_impl(
        state,
        pool.as_deref(),
        headers,
        body,
        resolved_custom_route,
        preapplied_priority_decision,
    )
    .await;
    apply_scope_models_etag(&mut response, catalog_etag);
    if response.status().is_success()
        && outcome.account_id.is_some()
        && outcome.provider_credential_id.is_none()
        && outcome
            .provider_slug
            .as_deref()
            .is_none_or(|slug| slug == "codex")
    {
        if let Ok(snapshots) = quota_state
            .account_cache
            .snapshots(&quota_state.store)
            .await
        {
            if let Some(quota) = crate::pool_quota::synthesize(
                &snapshots,
                Provider::Codex,
                pool.as_deref(),
                outcome.require_security_work_authorized,
            ) {
                crate::pool_quota::apply_http_headers(response.headers_mut(), &quota);
            }
        }
    }
    // Live-usage-cost-capture Task 6: clone the model slug for pricing BEFORE it's moved into
    // `RequestLog` below (`model: outcome.model` takes ownership of the original).
    let model_for_cost = outcome.model.clone();
    let custom_pricing = outcome.custom_pricing;
    // The tier the upstream actually served this turn at — cloned here for the same reason
    // `model_for_cost` is: `outcome` is moved into `RequestLog` below.
    let billed_service_tier = outcome.service_tier.clone();
    let estimated_tokens = outcome.estimated_tokens;
    let response_transport_kind = response_transport(&response);
    let eof_outcome = if response_transport_kind == "sse" {
        polyflare_store::RequestProtocolOutcome::TransportLost
    } else if response.status().is_success() {
        polyflare_store::RequestProtocolOutcome::Completed
    } else {
        polyflare_store::RequestProtocolOutcome::Failed
    };
    // Live-usage-cost-capture Task 4: a fresh per-request correlation id — content-free (128
    // random bits, never derived from request/response data) — so the (later) stream-wrapper task
    // can call `RequestLogRepo::update_usage` against the SAME row this request inserts.
    let request_id = format!("{:032x}", rand::random::<u128>());
    let log = RequestLog {
        method: "POST",
        path: "/responses".to_string(),
        // M4a: `/responses` may only ever route to a Codex-provider account — see this fn's
        // `filter_by_provider(&snapshots, Provider::Codex)` call below. The provider is
        // structurally fixed regardless of which branch produced the response (including the
        // early-exit error paths, which never resolve an account at all).
        provider: outcome
            .provider_slug
            .clone()
            .unwrap_or_else(|| Provider::Codex.to_string()),
        aliased: outcome.aliased,
        status: response.status(),
        duration_ms: start.elapsed().as_millis() as u64,
        account_id: outcome.account_id,
        target_kind: Some(
            if outcome.provider_credential_id.is_some() {
                "credential"
            } else {
                "account"
            }
            .to_string(),
        ),
        provider_credential_id: outcome.provider_credential_id,
        model: outcome.model,
        upstream_model: outcome.upstream_model,
        upstream_transport: outcome
            .upstream_transport
            .or_else(|| Some("http_sse".to_string())),
        profile_revision: outcome.profile_revision,
        reasoning_effort: outcome.reasoning_effort,
        // Not yet known at this chokepoint (SPEC-M4a has no per-account subscription-tier read
        // wired here today).
        service_tier: outcome.service_tier,
        requested_service_tier: outcome.requested_service_tier,
        actual_service_tier: outcome.actual_service_tier,
        transport: Some(
            downstream_transport
                .unwrap_or(response_transport_kind)
                .to_string(),
        ),
        // TODO(follow-up): populate ttft/tokens from the stream observer.
        ttft_ms: None,
        total_tokens: None,
        cached_tokens: None,
        subagent: outcome.subagent,
        request_id: Some(request_id.clone()),
        session_key: outcome.session_key,
    };
    log.emit();
    log_bus.publish(log.to_log_event());
    let mut finalized_event = log.to_log_event();
    finalized_event.kind = "request_finalized".to_string();
    finalized_event.message = "request telemetry finalized".to_string();
    let finalized_log_bus = log_bus.clone();
    // C11b Task 2: the content-free `upstream_requests` counter, keyed by the SAME
    // `(account_id, status)` pair `log` already carries — bumped exactly once per client request
    // (the final outcome only; per-attempt retries are `FailoverMetrics`, never double-counted
    // here).
    upstream_request_metrics.record_target(
        &log.provider,
        log.target_kind.as_deref().unwrap_or("account"),
        log.account_id
            .as_deref()
            .or(log.provider_credential_id.as_deref()),
        log.status.as_u16(),
    );
    queue_persist_request_log(&log_store, log.record(unix_now()));
    // Live-usage-cost-capture Task 6 (THE CRUX): wrap the final Response body in a
    // `UsageCapturingStream` — byte-for-byte passthrough (see that type's docs) that observes the
    // Codex SSE stream for a trailing `response.completed` frame's `usage` object + TTFT. This is
    // the SAME body `stream_response` built from `Body::from_stream(stream)` (or an error path's
    // non-streaming body — `into_data_stream` handles either uniformly), so re-wrapping it here
    // observes the identical bytes the client receives; nothing is altered, buffered, or delayed.
    // `on_done` only offers the update to the bounded queue; it never waits on SQLite.
    let (parts, body) = response.into_parts();
    let stream = body.into_data_stream(); // Stream<Item = Result<Bytes, axum::Error>>

    // live-row-tps-basis fix: pass the SAME route `start` captured above (it's `Instant`, hence
    // `Copy` — already used at `start.elapsed()` in the `RequestLog` build above, and reused here
    // by value) as the wrapper's clock origin, instead of letting the wrapper take its own
    // `Instant::now()`. This makes `ttft_ms` and the wrapper's `duration_ms` share an origin with
    // each other (and with the route's own timing), which is what `derive_tps` in `read_api.rs`
    // requires to compute a sane tokens/sec for live rows.
    let wrapped = usage_capture::UsageCapturingStream::new_with_eof_outcome(
        Box::pin(stream),
        start,
        eof_outcome,
        move |captured| {
            let store = usage_store;
            let request_id = request_id;
            let model = model_for_cost;
            if let Some(actual_tokens) = captured
                .usage
                .and_then(usage_capture::pressure_equivalent_tokens)
            {
                pressure_runtime.record_actual_pressure(estimated_tokens, actual_tokens);
            }
            let receipt = apply_captured_usage(
                &store,
                &request_id,
                model.as_deref(),
                custom_pricing,
                billed_service_tier.as_deref(),
                captured,
            );
            if let Some(receipt) = receipt {
                finalized_event.ts_ms = crate::log_bus::now_ms();
                finalized_event.latency_ms = captured.duration_ms;
                tokio::spawn(async move {
                    if matches!(receipt.await, Ok(true)) {
                        finalized_log_bus.publish(finalized_event);
                    }
                });
            }
        },
    );
    Response::from_parts(parts, Body::from_stream(wrapped))
}

/// Replace any account-native upstream catalog identity with the root/pool virtual identity.
/// `None` is meaningful: the exact fleet scope could not be authoritatively warmed, so exposing a
/// selected member's raw ETag would be actively wrong.
fn apply_scope_models_etag(response: &mut Response, catalog_etag: Option<String>) {
    response.headers_mut().remove("x-models-etag");
    if let Some(etag) = catalog_etag.and_then(|value| value.parse().ok()) {
        response.headers_mut().insert("x-models-etag", etag);
    }
}

/// B4/B5 Task 5: the production entrypoint reads the bounded failover loop's attempt cap from
/// `AppState.max_account_attempts` — resolved ONCE at startup by
/// `crate::config::max_account_attempts_from_env` and threaded through `AppState`/`ServeConfig`
/// (see that field's doc). Deliberately NOT a per-request `std::env::var` read — the TA6(b) T5
/// review flagged that pattern as debt; `max_attempts` is a plain `u32` copied out of `state`
/// before `state` (an `Arc`) moves into the impl below.
///
/// B5 Task 5: `AppState.starvation_wait_budget`/`starvation_heartbeat` are read the SAME way —
/// resolved ONCE at startup by `crate::config::starvation_wait_budget_secs_from_env`/
/// `starvation_heartbeat_secs_from_env` into `ServeConfig`/`AppState` (see those fields' docs), NOT
/// `starvation::DEFAULT_WAIT_BUDGET`/`DEFAULT_HEARTBEAT` (Task 4's placeholder consts — those now
/// serve ONLY the test seams below, which need a fixed, sleep-free default independent of any
/// `AppState` under test).
async fn responses_handler_impl(
    state: Arc<AppState>,
    pool: Option<&str>,
    headers: HeaderMap,
    raw: Bytes,
    resolved_custom_route: Option<Vec<(CustomProvider, ProviderModel)>>,
    preapplied_priority_decision: Option<crate::priority_policy::PriorityDecision>,
) -> (Response, RouteOutcome) {
    let max_attempts = state.runtime_settings.max_account_attempts();
    let starvation_wait_budget = state.runtime_settings.starvation_wait_budget();
    let starvation_heartbeat = state.runtime_settings.starvation_heartbeat();
    responses_handler_impl_with_max_attempts(
        state,
        pool,
        headers,
        raw,
        ResponsesHandlerOptions {
            resolved_custom_route,
            preapplied_priority_decision,
            max_attempts,
            starvation_budget: starvation_wait_budget,
            starvation_heartbeat,
        },
    )
    .await
}

/// B4 Task 4 test seam: drives the SAME real ingress logic `responses_handler_impl` does, but with
/// an explicit `max_attempts` for the bounded failover loop — the production HTTP entrypoint (via
/// `responses_handler_impl` above) uses `AppState.max_account_attempts` (Task 5's
/// `POLYFLARE_MAX_ACCOUNT_ATTEMPTS`, resolved once at startup) instead. This seam still exists so
/// integration tests can exercise a non-default bound (most importantly `max_attempts == 1`, the
/// "reproduces today's one-shot behavior EXACTLY" regression proof) directly, without needing to
/// thread an env var through process startup for a unit-scale test. Returns only the `Response` —
/// `RouteOutcome` is a private, logging-only type and can't cross the crate boundary in a `pub`
/// signature.
pub async fn responses_handler_impl_for_test(
    state: Arc<AppState>,
    pool: Option<String>,
    headers: HeaderMap,
    body: Bytes,
    max_attempts: u32,
) -> Response {
    responses_handler_impl_with_max_attempts(
        state,
        pool.as_deref(),
        headers,
        body,
        ResponsesHandlerOptions {
            resolved_custom_route: None,
            preapplied_priority_decision: None,
            max_attempts,
            starvation_budget: starvation::DEFAULT_WAIT_BUDGET,
            starvation_heartbeat: starvation::DEFAULT_HEARTBEAT,
        },
    )
    .await
    .0
}

/// B5 Task 4 test seam: identical to [`responses_handler_impl_for_test`], but ALSO overrides
/// Layer 2's wait budget + heartbeat. This is the ONLY way B5's test suite exercises a bounded,
/// fast keepalive wait without a real 10-60s sleep (the plan's own instruction: "Do NOT write a
/// test that really sleeps 10-60s"). Production (`responses_handler_impl`) always uses
/// `starvation::DEFAULT_WAIT_BUDGET`/`DEFAULT_HEARTBEAT`; Task 5 will replace both call sites' hard
/// consts with `AppState` fields resolved once at startup, at which point this seam gains the
/// equivalent override those fields would otherwise fix at process-start.
pub async fn responses_handler_impl_for_test_with_starvation_timing(
    state: Arc<AppState>,
    pool: Option<String>,
    headers: HeaderMap,
    body: Bytes,
    max_attempts: u32,
    starvation_budget: Duration,
    starvation_heartbeat: Duration,
) -> Response {
    responses_handler_impl_with_max_attempts(
        state,
        pool.as_deref(),
        headers,
        body,
        ResponsesHandlerOptions {
            resolved_custom_route: None,
            preapplied_priority_decision: None,
            max_attempts,
            starvation_budget,
            starvation_heartbeat,
        },
    )
    .await
    .0
}

async fn execute_custom_models(
    state: &AppState,
    headers: &HeaderMap,
    raw: &Bytes,
    targets: Vec<(CustomProvider, ProviderModel)>,
    wire_api: &str,
    affinity_identity_override: Option<String>,
    mut outcome: RouteOutcome,
) -> (Response, RouteOutcome) {
    let (response, custom) = crate::custom_provider::execute_targets(
        &state.store,
        &state.cipher,
        targets,
        wire_api,
        headers,
        raw,
        affinity_identity_override.as_deref(),
        state.runtime_settings.starvation_wait_budget(),
    )
    .await;
    outcome.provider_slug = Some(custom.provider_slug);
    outcome.provider_credential_id = custom.credential_id;
    outcome.upstream_model = Some(custom.upstream_model);
    outcome.upstream_transport = Some(custom.upstream_transport);
    if custom.effective_service_tier.is_some() {
        outcome.actual_service_tier = custom.effective_service_tier.clone();
        outcome.service_tier = custom.effective_service_tier;
    }
    outcome.profile_revision = custom.profile_revision;
    outcome.custom_pricing = match (
        custom.input_per_million,
        custom.cached_input_per_million,
        custom.output_per_million,
    ) {
        (Some(input), Some(cached), Some(output)) => {
            Some(polyflare_core::pricing::CustomModelRates {
                input_per_1m: input,
                cached_input_per_1m: cached,
                output_per_1m: output,
                priority_input_per_1m: custom.priority_input_per_million,
                priority_cached_input_per_1m: custom.priority_cached_input_per_million,
                priority_output_per_1m: custom.priority_output_per_million,
            })
        }
        _ => None,
    };
    (response, outcome)
}

async fn execute_anthropic_translation(
    state: Arc<AppState>,
    pool: Option<&str>,
    headers: &HeaderMap,
    raw: &Bytes,
    model_alias: ModelAlias,
) -> (Response, RouteOutcome) {
    let client_body = match serde_json::from_slice::<serde_json::Value>(raw) {
        Ok(serde_json::Value::Object(object)) => serde_json::Value::Object(object),
        _ => {
            return (
                (StatusCode::BAD_REQUEST, "invalid JSON body").into_response(),
                RouteOutcome::default(),
            )
        }
    };
    let client_wants_stream = client_body
        .get("stream")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    let mut translator = ResponsesToAnthropic::new();
    let mut translated = translator.translate_request(client_body);
    translated["model"] = serde_json::Value::String(model_alias.target_model.clone());
    // Both built-in and custom Anthropic executors must stream so the inverse adapter sees the
    // event lifecycle. A non-streaming Responses client is buffered after translation below.
    translated["stream"] = serde_json::Value::Bool(true);

    let (response, mut outcome) = match &model_alias.target {
        TranslationTarget::Builtin(Provider::Anthropic) => {
            let (response, mut outcome) = messages_handler_native(
                state.clone(),
                pool,
                translated,
                model_alias.target_model.clone(),
                // These bytes are PolyFlare's, not a first-party client's, so subscription-OAuth
                // accounts are not eligible to serve them.
                MessagesTraffic::Translated,
                // Nothing to forward verbatim: the body was synthesized by the translator.
                None,
            )
            .await;
            outcome.provider_slug = Some(Provider::Anthropic.to_string());
            (response, outcome)
        }
        TranslationTarget::Custom(provider_id) => {
            if pool.is_some() {
                return (
                    (
                        StatusCode::BAD_REQUEST,
                        "custom translation targets are root-scoped",
                    )
                        .into_response(),
                    RouteOutcome {
                        aliased: true,
                        model: Some(model_alias.target_model),
                        ..Default::default()
                    },
                );
            }
            let (provider, provider_model) = match resolve_translation_custom_target(
                &state,
                provider_id,
                &model_alias.target_model,
                "anthropic_messages",
            )
            .await
            {
                Ok(target) => target,
                Err(response) => {
                    return (
                        response,
                        RouteOutcome {
                            aliased: true,
                            model: Some(model_alias.target_model),
                            ..Default::default()
                        },
                    )
                }
            };
            let encoded = match serde_json::to_vec(&translated) {
                Ok(encoded) => Bytes::from(encoded),
                Err(_) => {
                    return (
                        internal_error(),
                        RouteOutcome {
                            aliased: true,
                            model: Some(model_alias.target_model),
                            ..Default::default()
                        },
                    )
                }
            };
            let (response, custom) = crate::custom_provider::execute(
                &state.store,
                &state.cipher,
                provider,
                provider_model,
                headers,
                &encoded,
                state.runtime_settings.starvation_wait_budget(),
            )
            .await;
            let outcome = RouteOutcome {
                provider_slug: Some(custom.provider_slug),
                provider_credential_id: custom.credential_id,
                upstream_model: Some(custom.upstream_model),
                upstream_transport: Some(custom.upstream_transport),
                profile_revision: custom.profile_revision,
                custom_pricing: match (
                    custom.input_per_million,
                    custom.cached_input_per_million,
                    custom.output_per_million,
                ) {
                    (Some(input), Some(cached), Some(output)) => {
                        Some(polyflare_core::pricing::CustomModelRates {
                            input_per_1m: input,
                            cached_input_per_1m: cached,
                            output_per_1m: output,
                            priority_input_per_1m: custom.priority_input_per_million,
                            priority_cached_input_per_1m: custom.priority_cached_input_per_million,
                            priority_output_per_1m: custom.priority_output_per_million,
                        })
                    }
                    _ => None,
                },
                model: Some(model_alias.target_model.clone()),
                ..Default::default()
            };
            (response, outcome)
        }
        _ => {
            return (
                (
                    StatusCode::BAD_GATEWAY,
                    "translation target protocol is invalid",
                )
                    .into_response(),
                RouteOutcome {
                    aliased: true,
                    model: Some(model_alias.target_model),
                    ..Default::default()
                },
            )
        }
    };
    outcome.aliased = true;
    outcome.model = Some(model_alias.target_model);
    if !response.status().is_success() {
        return (response, outcome);
    }
    let translated_stream = wrap_translating_stream(
        response_into_stream(response),
        Box::new(translator) as Box<dyn Translator>,
    );
    let response = if client_wants_stream {
        stream_response(translated_stream)
    } else {
        match collect_responses_response(translated_stream).await {
            Ok(response) => json_responses_response(response),
            Err(_) => (StatusCode::BAD_GATEWAY, "upstream error").into_response(),
        }
    };
    (response, outcome)
}

async fn responses_handler_impl_with_max_attempts(
    state: Arc<AppState>,
    pool: Option<&str>,
    headers: HeaderMap,
    raw: Bytes,
    options: ResponsesHandlerOptions,
) -> (Response, RouteOutcome) {
    let ResponsesHandlerOptions {
        resolved_custom_route,
        preapplied_priority_decision,
        max_attempts,
        starvation_budget,
        starvation_heartbeat,
    } = options;
    let (headers, raw) = match decode_responses_body(headers, raw).await {
        Ok(decoded) => decoded,
        Err(response) => return (response, RouteOutcome::default()),
    };
    // Parse ONCE — but only the scalars + the `input` SHAPE, NOT the deep conversation tree. The
    // wire bytes are forwarded verbatim (see `PreparedRequest::raw_body`), so `body` stays `None`
    // here; everything the request path needs (model, tier, continuity ctx, input count) comes off
    // this cheap parse. Only a MALFORMED body (invalid JSON, or a non-object root) 400s here;
    // semantic/schema checks (field types, numeric ranges, duplicate keys) are deferred to upstream,
    // the schema authority — a genuine pass-through, matching the old full-`Value` parse's tolerance.
    let mut facts = match parse_inbound_scoped(&headers, &raw, pool) {
        Some(f) => f,
        None => {
            return (
                (StatusCode::BAD_REQUEST, "invalid JSON body").into_response(),
                RouteOutcome::default(),
            )
        }
    };
    let now = unix_now();
    let policy_session_key = facts.ctx.session_key.as_ref().map(|key| key.value.clone());
    let is_subagent = facts
        .ctx
        .subagent
        .as_deref()
        .is_some_and(|subagent| !subagent.is_empty());
    let priority_decision = match preapplied_priority_decision {
        Some(decision) => decision,
        None => {
            state
                .runtime_settings
                .priority_policy
                .decide(
                    &state.store,
                    policy_session_key.as_deref(),
                    is_subagent,
                    now,
                )
                .await
        }
    };
    let raw = if preapplied_priority_decision.is_some() {
        raw
    } else {
        match crate::priority_policy::apply_decision(&raw, priority_decision) {
            Some(raw) => raw,
            None => {
                return (
                    (StatusCode::BAD_REQUEST, "invalid JSON body").into_response(),
                    RouteOutcome::default(),
                )
            }
        }
    };
    if preapplied_priority_decision.is_none()
        && priority_decision != crate::priority_policy::PriorityDecision::Passthrough
    {
        facts = match parse_inbound_scoped(&headers, &raw, pool) {
            Some(facts) => facts,
            None => {
                return (
                    (StatusCode::BAD_REQUEST, "invalid JSON body").into_response(),
                    RouteOutcome::default(),
                )
            }
        };
    }
    let model = facts.model;
    let model_for_selection = model.clone();
    let tier = tier_from_effort(facts.effort.as_deref());
    // Model + effort are known from the parse itself, regardless of what happens next; account_id
    // is filled in once (if ever) a `RouteDecision` actually selects one below. `subagent` comes
    // straight off `facts.ctx` (Task 1's `x-openai-subagent` extraction) — read here, BEFORE the
    // whole `facts.ctx` is moved into `ctx` below.
    let mut outcome = RouteOutcome {
        account_id: None,
        model: Some(model.clone()),
        reasoning_effort: facts.effort.clone(),
        service_tier: match priority_decision {
            crate::priority_policy::PriorityDecision::Standard => Some("standard".to_string()),
            _ => facts.service_tier.clone(),
        },
        // What we will actually ask the upstream for. A turn PolyFlare downgraded itself records
        // `standard` here, so its own policy never reads as an upstream refusal later.
        requested_service_tier: match priority_decision {
            crate::priority_policy::PriorityDecision::Standard => Some("standard".to_string()),
            _ => facts.service_tier.clone(),
        },
        subagent: facts.ctx.subagent.clone(),
        session_key: facts.ctx.session_key.as_ref().map(|key| key.value.clone()),
        estimated_tokens: facts.ctx.estimated_tokens,
        ..Default::default()
    };

    let translation_route = match state
        .store
        .translations()
        .resolve("openai_responses", &model)
        .await
    {
        Ok(route) => route,
        Err(_) => return (internal_error(), outcome),
    };
    if let Some(route) = translation_route {
        let target = match (
            route.target_kind.as_str(),
            route.target_provider_id.as_str(),
        ) {
            ("builtin_provider", "anthropic") => TranslationTarget::Builtin(Provider::Anthropic),
            ("custom_provider", provider_id) => TranslationTarget::Custom(provider_id.to_string()),
            _ => {
                return (
                    (
                        StatusCode::BAD_GATEWAY,
                        "translation target protocol is invalid",
                    )
                        .into_response(),
                    outcome,
                )
            }
        };
        let effective_service_tier = outcome.service_tier;
        let (response, mut translated_outcome) = execute_anthropic_translation(
            state,
            pool,
            &headers,
            &raw,
            ModelAlias {
                target,
                target_model: route.target_model,
                reasoning_effort: route.reasoning_effort,
            },
        )
        .await;
        translated_outcome.service_tier = effective_service_tier;
        return (response, translated_outcome);
    }

    if let Some(targets) = resolved_custom_route {
        return execute_custom_models(&state, &headers, &raw, targets, "responses", None, outcome)
            .await;
    }

    // Custom models are a root-catalog contract. Resolve them before any built-in account or
    // continuity selection so API-key providers never enter OAuth ownership machinery.
    if pool.is_none() && !crate::catalog::model_slug_is_reserved(&state, &model) {
        match state.store.providers().resolve_model_targets(&model).await {
            Ok(targets)
                if targets
                    .iter()
                    .any(|(provider, _)| provider.wire_api == "responses") =>
            {
                return execute_custom_models(
                    &state,
                    &headers,
                    &raw,
                    targets,
                    "responses",
                    None,
                    outcome,
                )
                .await;
            }
            Ok(targets) if !targets.is_empty() => {
                return (
                    (
                        StatusCode::BAD_REQUEST,
                        "this model requires a protocol translation route",
                    )
                        .into_response(),
                    outcome,
                )
            }
            Ok(_) => {}
            Err(_) => return (internal_error(), outcome),
        }
    }

    // C3: continuity ctx derived from headers + body at parse time.
    let ctx: RequestCtx = facts.ctx;
    // Native path: forward the REAL Codex client's own surviving inbound headers untouched (see
    // `forward_headers_from_inbound`) — this is a genuine Codex client, so its fingerprint is
    // already authentic; synthesizing here would only discard real conversation ids.
    let forward_headers = forward_headers_from_inbound(&headers);
    let req = PreparedRequest {
        // Native pass-through: the wire bytes ARE the body (below); no materialized `body` needed.
        body: None,
        model,
        forward_headers,
        // Forward the client's exact bytes upstream — no re-serialize, byte-identical fingerprint.
        raw_body: Some(raw),
    };

    // C4: prepare (resolve owner + arm + recovery plan).
    let prepared = match state.continuity.prepare(req, &ctx).await {
        Ok(p) => p,
        Err(_) => return (internal_error(), outcome),
    };

    let snapshots = match state.account_cache.snapshots(&state.store).await {
        Ok(s) => s,
        Err(_) => return (internal_error(), outcome),
    };
    // M4a has no cross-format translator (that's M4b): `/responses` may only ever pick a
    // Codex-provider account. One pass also narrows to the requested pool (`None` = all accounts).
    let mut snapshots = filter_by_provider_and_pool(&snapshots, Provider::Codex, pool);
    state
        .model_catalog
        .retain_accounts_supporting(&mut snapshots, &model_for_selection);
    // Overlay live per-account routing state (error_count/cooldown/last_error) onto the filtered
    // slice so the selector's eligibility gates see real failure signal, not neutral defaults.
    state.runtime.overlay(&mut snapshots, now);
    // The selector for this pool (its configured strategy override, else the global default).
    let selector = state.selector_for(pool);
    // B5 Task 4: an OWNED copy of the pool slug, needed wherever Layer 2's post-wait re-select
    // must re-run the identical `filter_by_provider_and_pool` narrowing inside a 'static stream.
    let pool_owned = pool.map(str::to_string);
    // TA6(b) Task 5: proactive resolution — OR two more independent true-sources onto Task 3's
    // directive value, NEVER overwrite it. A cyber-tagged pool (`POLYFLARE_POOL_CAPABILITIES`) or
    // the `X-PolyFlare-Capability: security_work` header requires the capability from turn 1, with
    // no rejection needed to discover it — but a session already sticky-cyber from a PRIOR move
    // must keep requiring it even when THIS turn routes through a non-cyber pool with no header.
    let pool_requires_cyber =
        config::pool_requires_capability(pool, config::SECURITY_WORK_CAPABILITY);
    let capability_header_present = headers
        .get(config::CAPABILITY_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim() == config::SECURITY_WORK_CAPABILITY)
        .unwrap_or(false);
    let sel_ctx = SelectionCtx {
        now,
        // The OR: Task 3's sticky-cyber directive, a cyber-tagged pool, or the capability header —
        // any ONE true-source is enough; none of the three can turn OFF another.
        require_security_work_authorized: prepared.directive.require_security_work_authorized
            || pool_requires_cyber
            || capability_header_present,
        rng_seed: None,
        session_id: ctx.session_id.clone(),
        tier,
        // C9 Task 3: startup-resolved (`AppState.inflight_penalty_pct`), never a per-request env
        // read — mirrors every other config-derived field on `sel_ctx`.
        inflight_penalty_pct: state.runtime_settings.inflight_penalty_pct(),
        request_pressure_units: state.runtime.request_pressure_units(ctx.estimated_tokens),
    };
    outcome.require_security_work_authorized = sel_ctx.require_security_work_authorized;
    let session_key = prepared.directive.session_key.clone();

    // C5: ownership pre-filter. New/unowned work selects and reserves under one runtime-state
    // critical section so a simultaneous main/subagent burst cannot all observe the same
    // pre-lease snapshot. A hard continuation owner is not a balancing choice and keeps the
    // ordinary acquire path below.
    let (route_decision, mut reserved_in_flight) = if prepared.directive.pin_account.is_none() {
        match state
            .runtime
            .select_and_acquire_wait(
                &mut snapshots,
                selector.as_ref(),
                &sel_ctx,
                now,
                &state.lease_metrics,
            )
            .await
        {
            Some((id, lease)) => (RouteDecision::Route(id), Some(lease)),
            None => (RouteDecision::NoEligibleAccount, None),
        }
    } else {
        (
            apply_ownership(&prepared.directive, &snapshots, selector.as_ref(), &sel_ctx),
            None,
        )
    };

    let response = match route_decision {
        RouteDecision::Route(id) => {
            outcome.account_id = Some(id.as_str().to_string());
            let (account, provider) = match resolve_core_account(&state, &id, now).await {
                Ok(a) => a,
                Err(r) => {
                    bench_after_local_refusal(&state, &id, now);
                    return (r, outcome);
                }
            };
            let health_id = id.clone(); // `id` is moved into the executor below.
                                        // C9 Task 2: the in-flight lease for this FIRST attempt on `id`. On success it
                                        // rides inside the returned stream; on any `Err` below (including the
                                        // `CapabilityRejection`/general-failure arms) it releases when
                                        // `execute_with_watchdog_tracked`'s own frame ends — strictly before
                                        // `reroute_cyber_rejection`/`run_failover_loop` (each of which acquires its OWN
                                        // fresh lease for whatever account it tries next) ever runs.
            let in_flight = match reserved_in_flight.take() {
                Some(lease) => lease,
                None => {
                    // A pinned continuation may wait briefly for its exact owner, including the
                    // reserved recovery slot. It must never spill to another account merely
                    // because that owner is busy: the previous response and turn state are scoped
                    // to the selected upstream identity.
                    let Some(lease) = state
                        .runtime
                        .acquire_pinned_in_flight_weighted(
                            &id,
                            now,
                            &state.lease_metrics,
                            sel_ctx.request_pressure_units,
                        )
                        .await
                    else {
                        return (no_eligible(), outcome);
                    };
                    lease
                }
            };
            // TA6(b) Task 2: capture the recovery plan + a `ctx` clone BEFORE `prepared`/`ctx`
            // move into the executor below, so a `CapabilityRejection` can trigger the cyber
            // reselect+resend (`reroute_cyber_rejection`) without re-preparing the request.
            let recovery_for_cyber = prepared.directive.recovery.clone();
            let ctx_for_cyber = ctx.clone();
            // B4 Task 4 — CONTINUITY OWNERSHIP gate (see the plan's Global Constraints): the
            // bounded cross-account failover loop (`run_failover_loop`) may only fan out an
            // ANCHORLESS attempt onto a NEW account. A live anchor (this turn is
            // `WatchdogArm::Armed`, i.e. it carries `previous_response_id`) must NEVER be
            // resent to a different account on a general (non-cyber) failure — that would
            // re-home the conversation's ownership off the back of an ordinary retryable
            // failure instead of the reviewed, capability-scoped `reroute_cyber_rejection`
            // path, and risks re-opening the wedge. So an Armed turn's failure here surfaces
            // exactly as before this task (today's 502) — see `tests/failover_loop.rs`'s `(e)`.
            // A Disarmed turn's own request body carries no anchor at all, so it is already a
            // self-sufficient resend for any account: clone it now, before `prepared` moves
            // into the executor below, in case a failure needs to fail over.
            let resend_req_for_loop = match prepared.directive.watchdog {
                WatchdogArm::Disarmed => Some(prepared.req.clone()),
                WatchdogArm::Armed { .. } => None,
            };
            let prepared_for_auth_retry = prepared.clone();
            let rejected_access_token = account.bearer_token.clone();
            let commit = CommitWitness::new();
            let mut execution = execute_with_watchdog_tracked(
                state.executor_for(provider).as_ref(),
                state.continuity.clone(),
                prepared,
                &account,
                id.clone(),
                ctx.clone(),
                state.runtime.clone(),
                state.runtime_settings.stream_idle_timeout(),
                max_attempts,
                commit.clone(),
                Some(in_flight),
            )
            .await;
            // Cross-provider poisoned history on the HTTP path (2026-07-25 live): the client fell
            // back to HTTP after a transport drop and the turn died with `invalid_encrypted_content`
            // — the relay's transform only ever saw the WS frame. Same one-shot recovery, same
            // account: rewrite the body (foreign reasoning envelopes out, plaintext summaries kept)
            // and retry once. Nothing was relayed yet (this is a pre-stream error), so a retry
            // cannot duplicate output. `None` from the transform means there is nothing to fix.
            //
            // Gated on the CODE SET, not the one code: 2026-07-29 the same poisoning surfaced as
            // `array_above_max_length` (a foreign `reasoning` item carrying a `content` array the
            // native validator requires to be empty) and this branch never ran, so the thread was
            // stuck resending an identical body to a deterministic validator. See
            // `reasoning_transform::is_unresendable_history_code`.
            if provider == Provider::Codex
                && execution
                    .as_ref()
                    .err()
                    .and_then(watchdog_error_code)
                    .as_deref()
                    .is_some_and(crate::reasoning_transform::is_unresendable_history_code)
            {
                if let Some(retry) =
                    prepared_with_stripped_reasoning(prepared_for_auth_retry.clone())
                {
                    state
                        .runtime
                        .refund_logical_turn_attempt(ctx.logical_turn_key.as_deref());
                    if let Some(retry_lease) = state
                        .runtime
                        .acquire_pinned_in_flight_weighted(
                            &id,
                            unix_now(),
                            &state.lease_metrics,
                            sel_ctx.request_pressure_units,
                        )
                        .await
                    {
                        state.relay_metrics.record("reasoning_transform_http_retry");
                        execution = execute_with_watchdog_tracked(
                            state.executor_for(provider).as_ref(),
                            state.continuity.clone(),
                            retry,
                            &account,
                            id.clone(),
                            ctx.clone(),
                            state.runtime.clone(),
                            state.runtime_settings.stream_idle_timeout(),
                            max_attempts,
                            commit.clone(),
                            Some(retry_lease),
                        )
                        .await;
                    }
                }
            }
            let unauthorized = matches!(
                &execution,
                Err(WatchdogError::Upstream(Some(signal))) if signal.status == 401
            ) || matches!(
                &execution,
                Err(WatchdogError::UpstreamHttp(response)) if response.signal.status == 401
            );
            if provider == Provider::Codex && unauthorized {
                // A rejected bearer cannot have started model sampling. Return this slot before
                // the synchronized same-account refresh so the authenticated retry, not the 401,
                // spends the logical turn's generation budget.
                state
                    .runtime
                    .refund_logical_turn_attempt(ctx.logical_turn_key.as_deref());
                match force_refresh_after_unauthorized(
                    &state,
                    &id,
                    &rejected_access_token,
                    unix_now(),
                )
                .await
                {
                    Ok(Some(refreshed_account)) => {
                        let Some(retry_lease) = state
                            .runtime
                            .acquire_pinned_in_flight_weighted(
                                &id,
                                unix_now(),
                                &state.lease_metrics,
                                sel_ctx.request_pressure_units,
                            )
                            .await
                        else {
                            return (no_eligible(), outcome);
                        };
                        execution = execute_with_watchdog_tracked(
                            state.executor_for(provider).as_ref(),
                            state.continuity.clone(),
                            prepared_for_auth_retry,
                            &refreshed_account,
                            id.clone(),
                            ctx.clone(),
                            state.runtime.clone(),
                            state.runtime_settings.stream_idle_timeout(),
                            max_attempts,
                            commit.clone(),
                            Some(retry_lease),
                        )
                        .await;
                    }
                    Ok(None) => {}
                    Err(response) => return (response, outcome),
                }
            }
            match execution {
                Ok(stream) => stream_response(stream),
                Err(WatchdogError::CapabilityRejection { .. }) => {
                    // NOT an account-health signal (see `record_failure`'s doc): a capability
                    // rejection says nothing about the owner's health, so no writeback here.
                    // Composes with (does NOT conflict with) B4's general failover loop: a
                    // `CapabilityRejection` always routes here, never into
                    // `run_failover_loop`, regardless of `resend_req_for_loop`.
                    reroute_cyber_rejection(
                        &state,
                        recovery_for_cyber,
                        &snapshots,
                        selector.as_ref(),
                        &sel_ctx,
                        ctx_for_cyber,
                        session_key.clone(),
                        now,
                        &mut outcome,
                    )
                    .await
                }
                Err(e) => {
                    record_failure(&state, &health_id, &e, unix_now()).await;
                    match resend_req_for_loop {
                        // Anchorless: eligible for the bounded cross-account failover loop.
                        Some(resend_req) => {
                            run_failover_loop(
                                &state,
                                health_id,
                                e,
                                commit.is_committed(),
                                resend_req,
                                &snapshots,
                                selector.as_ref(),
                                selector.clone(),
                                &sel_ctx,
                                ctx,
                                session_key.clone(),
                                now,
                                max_attempts,
                                pool_owned.clone(),
                                starvation_budget,
                                starvation_heartbeat,
                                &mut outcome,
                            )
                            .await
                        }
                        // A live-anchor pinned turn: surfaces exactly as before this task.
                        None => surface_watchdog_error(&e),
                    }
                }
            }
        }
        RouteDecision::Recover => {
            // Owner pinned but ineligible: recover on a freshly-selected account (full pool), or
            // signal the client if the input is a bare tail.
            match prepared.directive.recovery {
                RecoveryPlan::ResendFull { anchorless_req } => {
                    let fresh = match selector.pick(&snapshots, &sel_ctx) {
                        Some(id) => id,
                        None => {
                            // B5 Task 3 — Layer 1: guarded serve-soonest-error-backoff before
                            // the 503, over the SAME snapshots the pick above just exhausted.
                            // Cloned (not moved) so the ORIGINALS survive for Layer 2 below.
                            let layer1 = try_layer1_serve_now(
                                &state,
                                &snapshots,
                                selector.as_ref(),
                                &sel_ctx,
                                anchorless_req.clone(),
                                ctx.clone(),
                                session_key.clone(),
                                now,
                                &mut outcome,
                                &WaitClient::Codex,
                            )
                            .await;
                            let resp = layer1.or_else(|| {
                                try_layer2_recovery_wait(
                                    state.clone(),
                                    &snapshots,
                                    pool_owned.clone(),
                                    Provider::Codex,
                                    selector.clone(),
                                    &sel_ctx,
                                    anchorless_req,
                                    ctx,
                                    session_key,
                                    now,
                                    starvation_budget,
                                    starvation_heartbeat,
                                    &mut outcome,
                                    WaitClient::Codex,
                                )
                            });
                            return (resp.unwrap_or_else(no_eligible), outcome);
                        }
                    };
                    state.runtime.record_selected(&fresh, now);
                    outcome.account_id = Some(fresh.as_str().to_string());
                    let (account, provider) = match resolve_core_account(&state, &fresh, now).await
                    {
                        Ok(a) => a,
                        Err(r) => {
                            bench_after_local_refusal(&state, &fresh, now);
                            return (r, outcome);
                        }
                    };
                    let health_id = fresh.clone(); // `fresh` is moved into the executor below.
                                                   // C9 Task 2: the owner-ineligible recovery's reselected attempt on `fresh`
                                                   // is a real upstream request — same lease treatment as every other
                                                   // streaming selection site.
                    let Some(in_flight) = state.runtime.try_acquire_in_flight_weighted(
                        &fresh,
                        now,
                        &state.lease_metrics,
                        sel_ctx.request_pressure_units,
                    ) else {
                        bench_after_local_refusal(&state, &fresh, now);
                        return (
                            admission_refused_on_reselect(
                                fresh.as_str(),
                                sel_ctx.request_pressure_units,
                            ),
                            outcome,
                        );
                    };
                    match execute_recovery_tracked(
                        state.executor_for(provider).as_ref(),
                        state.continuity.clone(),
                        anchorless_req,
                        &account,
                        fresh,
                        ctx,
                        session_key,
                        state.runtime.clone(),
                        state.runtime_settings.stream_idle_timeout(),
                        max_attempts,
                        CommitWitness::new(),
                        Some(in_flight),
                    )
                    .await
                    {
                        Ok(stream) => stream_response(stream),
                        Err(e) => {
                            record_failure(&state, &health_id, &e, unix_now()).await;
                            surface_watchdog_error(&e)
                        }
                    }
                }
                RecoveryPlan::SignalClient => {
                    let owner = prepared
                        .directive
                        .pin_account
                        .clone()
                        .unwrap_or_else(|| AccountId::from("unknown"));
                    // No account is actually served here (the client is signaled, not relayed) —
                    // but `owner` is the pinned account this request was scoped to, so it's still
                    // a meaningful (and content-free) identifier to surface.
                    outcome.account_id = Some(owner.as_str().to_string());
                    let stream =
                        signal_client_stream(state.continuity.clone(), ctx, owner, session_key)
                            .await;
                    stream_response(stream)
                }
                RecoveryPlan::None => {
                    // No anchor ⇒ this request is self-sufficient (nothing to resume), so a
                    // pinned-but-ineligible owner (cooldown / rate-limited / reauth_required /
                    // a stale Soft session-row pin) is NOT fatal: fail over to any eligible
                    // account from the FULL candidate pool, ignoring the pin, and relay as a
                    // normal (Disarmed) request. `prepared.req` is still owned here — only
                    // `directive.recovery` was moved by the outer match.
                    match selector.pick(&snapshots, &sel_ctx) {
                        Some(fresh) => {
                            state.runtime.record_selected(&fresh, now);
                            outcome.account_id = Some(fresh.as_str().to_string());
                            let (account, provider) =
                                match resolve_core_account(&state, &fresh, now).await {
                                    Ok(a) => a,
                                    Err(r) => {
                                        bench_after_local_refusal(&state, &fresh, now);
                                        return (r, outcome);
                                    }
                                };
                            let fallback = Prepared {
                                req: prepared.req,
                                directive: ContinuityDirective {
                                    // The ORIGINAL pin, deliberately carried through even though
                                    // this attempt deliberately IGNORES it for routing (the
                                    // account was already chosen above — `fresh` — so nothing
                                    // downstream re-reads this field to route). It survives here
                                    // ONLY so `execute_with_watchdog_tracked` can stamp it onto
                                    // the turn as `expected_owner`: this spill must serve the turn
                                    // on `fresh` WITHOUT handing `fresh` the session's ownership.
                                    // Dropping it to `None` (as this did before) told the fence
                                    // "unpinned turn", which is exactly how the spilling account
                                    // used to steal the session and start an A/B ownership
                                    // oscillation. See `ContinuityRepo::record_completion`.
                                    pin_account: prepared.directive.pin_account.clone(),
                                    watchdog: prepared.directive.watchdog,
                                    recovery: RecoveryPlan::None,
                                    session_key: prepared.directive.session_key.clone(),
                                    require_security_work_authorized: prepared
                                        .directive
                                        .require_security_work_authorized,
                                },
                            };
                            let health_id = fresh.clone(); // moved into the executor below.
                                                           // C9 Task 2: the pin-ignoring fallback's attempt on `fresh` is a
                                                           // real upstream request — same lease treatment as every other
                                                           // streaming selection site.
                            let Some(in_flight) = state.runtime.try_acquire_in_flight_weighted(
                                &fresh,
                                now,
                                &state.lease_metrics,
                                sel_ctx.request_pressure_units,
                            ) else {
                                bench_after_local_refusal(&state, &fresh, now);
                                return (
                                    admission_refused_on_reselect(
                                        fresh.as_str(),
                                        sel_ctx.request_pressure_units,
                                    ),
                                    outcome,
                                );
                            };
                            match execute_with_watchdog_tracked(
                                state.executor_for(provider).as_ref(),
                                state.continuity.clone(),
                                fallback,
                                &account,
                                fresh,
                                ctx,
                                state.runtime.clone(),
                                state.runtime_settings.stream_idle_timeout(),
                                max_attempts,
                                CommitWitness::new(),
                                Some(in_flight),
                            )
                            .await
                            {
                                Ok(stream) => stream_response(stream),
                                Err(e) => {
                                    record_failure(&state, &health_id, &e, unix_now()).await;
                                    surface_watchdog_error(&e)
                                }
                            }
                        }
                        None => {
                            // B5 Task 3 — Layer 1: guarded serve-soonest-error-backoff before
                            // the 503. `prepared.req` is still owned here (see the comment
                            // above — only `directive.recovery` was moved by the outer match).
                            // Cloned (not moved) so the ORIGINAL survives for Layer 2 below.
                            let layer1 = try_layer1_serve_now(
                                &state,
                                &snapshots,
                                selector.as_ref(),
                                &sel_ctx,
                                prepared.req.clone(),
                                ctx.clone(),
                                session_key.clone(),
                                now,
                                &mut outcome,
                                &WaitClient::Codex,
                            )
                            .await;
                            layer1
                                .or_else(|| {
                                    try_layer2_recovery_wait(
                                        state.clone(),
                                        &snapshots,
                                        pool_owned.clone(),
                                        Provider::Codex,
                                        selector.clone(),
                                        &sel_ctx,
                                        prepared.req,
                                        ctx,
                                        session_key,
                                        now,
                                        starvation_budget,
                                        starvation_heartbeat,
                                        &mut outcome,
                                        WaitClient::Codex,
                                    )
                                })
                                .unwrap_or_else(no_eligible)
                        }
                    }
                }
            }
        }
        RouteDecision::NoEligibleAccount => {
            // B5 Task 3 — Layer 1: the unowned first-attempt pick found the eligible pool
            // empty; try the guarded serve-soonest-error-backoff candidate before the 503.
            // Cloned (not moved) so the ORIGINAL survives for Layer 2 below.
            let layer1 = try_layer1_serve_now(
                &state,
                &snapshots,
                selector.as_ref(),
                &sel_ctx,
                prepared.req.clone(),
                ctx.clone(),
                session_key.clone(),
                now,
                &mut outcome,
                &WaitClient::Codex,
            )
            .await;
            layer1
                .or_else(|| {
                    try_layer2_recovery_wait(
                        state.clone(),
                        &snapshots,
                        pool_owned.clone(),
                        Provider::Codex,
                        selector.clone(),
                        &sel_ctx,
                        prepared.req,
                        ctx,
                        session_key,
                        now,
                        starvation_budget,
                        starvation_heartbeat,
                        &mut outcome,
                        WaitClient::Codex,
                    )
                })
                .unwrap_or_else(no_eligible)
        }
    };
    (response, outcome)
}

async fn decode_responses_body(
    mut headers: HeaderMap,
    raw: Bytes,
) -> Result<(HeaderMap, Bytes), Response> {
    let encoding = headers
        .get(header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .map(str::trim);
    match encoding {
        None | Some("") | Some("identity") => Ok((headers, raw)),
        Some(encoding) if encoding.eq_ignore_ascii_case("zstd") => {
            let decoded = tokio::task::spawn_blocking(move || {
                use std::io::Read;

                let decoder =
                    zstd::stream::read::Decoder::new(std::io::Cursor::new(raw)).map_err(|_| ())?;
                let mut bounded = decoder.take(crate::app::MAX_REQUEST_BODY_BYTES as u64 + 1);
                let mut decoded = Vec::new();
                bounded.read_to_end(&mut decoded).map_err(|_| ())?;
                Ok::<Vec<u8>, ()>(decoded)
            })
            .await
            .map_err(|_| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "request decompression failed",
                )
                    .into_response()
            })?
            .map_err(|_| (StatusCode::BAD_REQUEST, "invalid zstd request body").into_response())?;
            if decoded.len() > crate::app::MAX_REQUEST_BODY_BYTES {
                return Err((
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "decompressed request body exceeds limit",
                )
                    .into_response());
            }
            headers.remove(header::CONTENT_ENCODING);
            headers.remove(header::CONTENT_LENGTH);
            Ok((headers, Bytes::from(decoded)))
        }
        Some(_) => Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported content encoding",
        )
            .into_response()),
    }
}

/// The `/v1/messages` ingress entrypoint. A client `model` string that a persisted translation
/// route maps to a Codex target takes the cross-provider translated
/// path; everything else (no alias, or an alias whose target is itself Anthropic) takes the native
/// same-format path, unchanged. Also a thin timing + content-safe logging wrapper (mirrors
/// `responses_handler` above) — see `crate::observability` for the content-safety constraint.
/// The bare `/v1/messages` ingress entrypoint: selects over ALL accounts of the resolved provider
/// (no pool filter).
pub async fn messages_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    messages_route(state, None, headers, body).await
}

/// The pooled `/{pool}/v1/messages` ingress entrypoint: selects only over the resolved provider's
/// accounts tagged with the `{pool}` slug (see `filter_by_pool`).
pub async fn pooled_messages_handler(
    State(state): State<Arc<AppState>>,
    Path(pool): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    messages_route(state, Some(pool), headers, body).await
}

async fn messages_route(
    state: Arc<AppState>,
    pool: Option<String>,
    headers: HeaderMap,
    raw_body: Bytes,
) -> Response {
    let start = Instant::now();
    // The raw bytes are retained alongside the parsed value so an admitted Claude request can be
    // forwarded verbatim (see `PreparedRequest::raw_body`); routing still needs the parsed model
    // and alias. Parsing here also preserves the 400 the `Json` extractor used to produce.
    let body = match serde_json::from_slice::<serde_json::Value>(&raw_body) {
        Ok(body) => body,
        Err(_) => return (StatusCode::BAD_REQUEST, "invalid JSON body").into_response(),
    };
    maybe_capture_fingerprint(&state, "POST", "/v1/messages", &headers);

    // Session-volume circuit breaker: contain a runaway client session (a subagent fan-out that
    // spirals) BEFORE account selection, so its excess costs no upstream quota. Keyed on the Claude
    // Code session id; anonymous traffic is ungoverned; off unless an operator sets an enforce
    // limit. Runs first so a rejected request never reaches an account.
    if let Some(session_key) = headers
        .get("x-claude-code-session-id")
        .and_then(|value| value.to_str().ok())
    {
        if let Some(verdict) = crate::session_governor::global().record(
            session_key,
            unix_now(),
            state.runtime_settings.session_warn_per_hour(),
            state.runtime_settings.session_enforce_per_hour(),
        ) {
            if verdict.should_warn {
                tracing::warn!(
                    count = verdict.count,
                    "a client session crossed the session-governor warn threshold \
                     (possible runaway subagent fan-out)"
                );
            }
            if verdict.rejected {
                return session_reject_response(&verdict);
            }
        }
    }

    // Keep the bounded background-writer handle before `state` moves into a sub-handler.
    let log_store = state.store.clone();
    // Observe terminal Anthropic usage on the exact client-facing body, then queue the update
    // behind the request-row insert through the same FIFO writer.
    let usage_store = state.store.clone();
    // Retained so the stream-tap's `on_done` can cool the account if the 200 stream smuggled a
    // rate-limit/overload error frame (see `stream_error_status`); full state is needed to bench.
    let state_for_stream_error = state.clone();
    let pressure_runtime = state.runtime.clone();
    // Same reason: `state` moves into a sub-handler below, so grab the log-bus handle first.
    let log_bus = state.log_bus.clone();
    // C11b Task 2: same reason — grab the content-free `upstream_requests` counter handle before
    // `state` moves into a sub-handler below.
    let upstream_request_metrics = state.upstream_request_metrics.clone();
    let model = body
        .get("model")
        .and_then(|m| m.as_str())
        .unwrap_or_default()
        .to_string();

    // Resolve once against the persisted operator rules. A read failure fails closed with 500:
    // silently taking the native Anthropic path could send a request to the wrong provider.
    let alias = match state
        .store
        .translations()
        .resolve("anthropic_messages", &model)
        .await
    {
        Ok(route) => route.and_then(|route| {
            let target = match (
                route.target_kind.as_str(),
                route.target_provider_id.as_str(),
            ) {
                ("builtin_provider", "codex") => Some(TranslationTarget::Builtin(Provider::Codex)),
                ("custom_provider", provider_id) => {
                    Some(TranslationTarget::Custom(provider_id.to_string()))
                }
                _ => None,
            }?;
            Some(ModelAlias {
                target,
                target_model: route.target_model,
                reasoning_effort: route.reasoning_effort,
            })
        }),
        Err(error) => {
            tracing::error!(
                target: "polyflare_server::request",
                %error,
                "translation route resolution failed"
            );
            let response = StatusCode::INTERNAL_SERVER_ERROR.into_response();
            let outcome = RouteOutcome {
                model: Some(model.clone()),
                ..Default::default()
            };
            let request_id = format!("{:032x}", rand::random::<u128>());
            let log = RequestLog {
                requested_service_tier: None,
                actual_service_tier: None,
                method: "POST",
                path: "/v1/messages".to_string(),
                provider: Provider::Anthropic.to_string(),
                aliased: false,
                status: response.status(),
                duration_ms: start.elapsed().as_millis() as u64,
                account_id: None,
                target_kind: Some("account".to_string()),
                provider_credential_id: None,
                model: outcome.model,
                upstream_model: None,
                upstream_transport: None,
                profile_revision: None,
                reasoning_effort: None,
                service_tier: None,
                transport: Some(response_transport(&response).to_string()),
                ttft_ms: None,
                total_tokens: None,
                cached_tokens: None,
                subagent: None,
                request_id: Some(request_id),
                session_key: None,
            };
            log.emit();
            log_bus.publish(log.to_log_event());
            queue_persist_request_log(&log_store, log.record(unix_now()));
            return response;
        }
    };
    let native_custom = if alias.is_none() && pool.is_none() {
        match state.store.providers().resolve_model_targets(&model).await {
            Ok(targets)
                if targets
                    .iter()
                    .any(|(provider, _)| provider.wire_api == "anthropic_messages") =>
            {
                Some(targets)
            }
            Ok(_) => None,
            Err(_) => return internal_error(),
        }
    } else {
        None
    };
    let aliased = alias.is_some();
    let builtin_provider = match alias.as_ref().map(|alias| &alias.target) {
        Some(TranslationTarget::Builtin(provider)) => *provider,
        _ => Provider::Anthropic,
    };

    let (response, outcome) = match (alias, native_custom) {
        (Some(model_alias), _)
            if model_alias.target == TranslationTarget::Builtin(Provider::Codex) =>
        {
            messages_handler_codex_aliased(state, pool.as_deref(), body, model_alias).await
        }
        (
            Some(
                model_alias @ ModelAlias {
                    target: TranslationTarget::Custom(_),
                    ..
                },
            ),
            _,
        ) => {
            if pool.is_some() {
                (
                    (
                        StatusCode::BAD_REQUEST,
                        "custom translation targets are root-scoped",
                    )
                        .into_response(),
                    RouteOutcome {
                        model: Some(model_alias.target_model),
                        ..Default::default()
                    },
                )
            } else {
                messages_handler_custom_responses(state, &headers, body, model_alias).await
            }
        }
        (None, Some(targets)) => {
            messages_handler_custom_native(state, &headers, body, targets).await
        }
        _ => {
            messages_handler_native(
                state,
                pool.as_deref(),
                body,
                model,
                MessagesTraffic::ClaudeNative,
                Some((&headers, &raw_body)),
            )
            .await
        }
    };
    let provider = outcome
        .provider_slug
        .clone()
        .unwrap_or_else(|| builtin_provider.to_string());
    // The account that served this turn — captured before `outcome` moves into `RequestLog` — so
    // the stream tap's `on_done` can cool the right account on a mid-stream error frame.
    let account_id_for_stream_error = outcome.account_id.clone();
    let model_for_cost = outcome.model.clone();
    let custom_pricing = outcome.custom_pricing;
    // The tier the upstream actually served this turn at — cloned here for the same reason
    // `model_for_cost` is: `outcome` is moved into `RequestLog` below.
    let billed_service_tier = outcome.service_tier.clone();
    let estimated_tokens = outcome.estimated_tokens;
    let response_transport_kind = response_transport(&response);
    let eof_outcome = if response_transport_kind == "sse" {
        polyflare_store::RequestProtocolOutcome::TransportLost
    } else if response.status().is_success() {
        polyflare_store::RequestProtocolOutcome::Completed
    } else {
        polyflare_store::RequestProtocolOutcome::Failed
    };

    // Live-usage-cost-capture Task 4: a fresh per-request correlation id — content-free (128
    // random bits, never derived from request/response data) — so the (later) stream-wrapper task
    // can call `RequestLogRepo::update_usage` against the SAME row this request inserts.
    let request_id = format!("{:032x}", rand::random::<u128>());
    let log = RequestLog {
        requested_service_tier: None,
        actual_service_tier: None,
        method: "POST",
        path: "/v1/messages".to_string(),
        provider,
        aliased,
        status: response.status(),
        duration_ms: start.elapsed().as_millis() as u64,
        account_id: outcome.account_id,
        target_kind: Some(
            if outcome.provider_credential_id.is_some() {
                "credential"
            } else {
                "account"
            }
            .to_string(),
        ),
        provider_credential_id: outcome.provider_credential_id,
        model: outcome.model,
        upstream_model: outcome.upstream_model,
        upstream_transport: outcome
            .upstream_transport
            .or_else(|| Some("http_sse".to_string())),
        profile_revision: outcome.profile_revision,
        reasoning_effort: outcome.reasoning_effort,
        // Not yet known at this chokepoint.
        service_tier: None,
        transport: Some(response_transport_kind.to_string()),
        ttft_ms: None,
        total_tokens: None,
        cached_tokens: None,
        subagent: outcome.subagent,
        request_id: Some(request_id.clone()),
        session_key: outcome.session_key,
    };
    log.emit();
    log_bus.publish(log.to_log_event());
    let mut finalized_event = log.to_log_event();
    finalized_event.kind = "request_finalized".to_string();
    finalized_event.message = "request telemetry finalized".to_string();
    let finalized_log_bus = log_bus.clone();
    // C11b Task 2: the content-free `upstream_requests` counter, keyed by the SAME
    // `(account_id, status)` pair `log` already carries — bumped exactly once per client request
    // (the final outcome only; per-attempt retries are `FailoverMetrics`, never double-counted
    // here).
    upstream_request_metrics.record_target(
        &log.provider,
        log.target_kind.as_deref().unwrap_or("account"),
        log.account_id
            .as_deref()
            .or(log.provider_credential_id.as_deref()),
        log.status.as_u16(),
    );
    queue_persist_request_log(&log_store, log.record(unix_now()));

    let (parts, body) = response.into_parts();
    let stream = body.into_data_stream();
    let wrapped = usage_capture::UsageCapturingStream::new_anthropic_with_eof_outcome(
        Box::pin(stream),
        start,
        eof_outcome,
        move |captured| {
            let store = usage_store;
            let request_id = request_id;
            let model = model_for_cost;
            // A status-200 stream that smuggled a rate_limit_error/overloaded_error frame cools the
            // serving account exactly as a real 429/529 would — otherwise the signal is invisible to
            // a status-code-only proxy and the account keeps being selected into the limit.
            if let (Some(status), Some(account_id)) =
                (captured.stream_error_status, account_id_for_stream_error)
            {
                let state = state_for_stream_error;
                tokio::spawn(async move {
                    let signal = polyflare_core::FailureSignal {
                        status,
                        retry_after: None,
                        error_code: None,
                    };
                    bench_account_for_failure(
                        &state,
                        &AccountId::from(account_id.as_str()),
                        Some(&signal),
                        unix_now(),
                    )
                    .await;
                });
            }
            if let Some(actual_tokens) = captured
                .usage
                .and_then(usage_capture::pressure_equivalent_tokens)
            {
                pressure_runtime.record_actual_pressure(estimated_tokens, actual_tokens);
            }
            let receipt = apply_captured_usage(
                &store,
                &request_id,
                model.as_deref(),
                custom_pricing,
                billed_service_tier.as_deref(),
                captured,
            );
            if let Some(receipt) = receipt {
                finalized_event.ts_ms = crate::log_bus::now_ms();
                finalized_event.latency_ms = captured.duration_ms;
                tokio::spawn(async move {
                    if matches!(receipt.await, Ok(true)) {
                        finalized_log_bus.publish(finalized_event);
                    }
                });
            }
        },
    );
    Response::from_parts(parts, Body::from_stream(wrapped))
}

/// Adapts axum's `HeaderMap` to `polyflare_anthropic`'s `HeaderSource`, so the admission and
/// allowlist rules stay in the provider crate and depend on no HTTP framework type.
///
/// A header whose value is not valid UTF-8 reads as absent. That is deliberate: such a value could
/// not be forwarded verbatim anyway, and treating it as missing makes admission fail closed rather
/// than admit a request on a header nobody could actually inspect.
struct ClientHeaders<'a>(&'a HeaderMap);

impl polyflare_anthropic::HeaderSource for ClientHeaders<'_> {
    fn get(&self, name: &str) -> Option<&str> {
        self.0.get(name).and_then(|value| value.to_str().ok())
    }

    fn names(&self) -> Vec<String> {
        self.0
            .keys()
            .map(|name| name.as_str().to_string())
            .collect()
    }
}

fn admitted_claude_affinity_identity(
    headers: &HeaderMap,
    body: &serde_json::Value,
) -> Option<String> {
    use sha2::{Digest, Sha256};

    let envelope = polyflare_anthropic::admit_native_request(&ClientHeaders(headers), body).ok()?;
    let mut hasher = Sha256::new();
    hasher.update(b"polyflare-custom-anthropic-affinity-v1");
    hasher.update([0]);
    hasher.update(envelope.session_id.as_bytes());
    Some(hex::encode(hasher.finalize()))
}

/// The native Anthropic-Messages ingress path: no alias applies, so this relays straight to an
/// Anthropic-provider account. Continuity is a no-op here (SPEC-M4 §3.7: the Anthropic backend has
/// no `previous_response_id`-style anchor), so every request is `Disarmed` and
/// `execute_with_watchdog`'s Disarmed branch just relays — the wedge machinery never arms.
async fn messages_handler_native(
    state: Arc<AppState>,
    pool: Option<&str>,
    body: serde_json::Value,
    model: String,
    traffic: MessagesTraffic,
    // The client's original headers and bytes, when this request came from a real client rather
    // than the translator. `None` on the translated path — there is nothing of the client's left
    // to forward once the body has been rebuilt in another protocol's shape.
    client_wire: Option<(&HeaderMap, &Bytes)>,
) -> (Response, RouteOutcome) {
    let now = unix_now();
    let estimated_tokens = estimate_materialized_request_tokens(&body, "messages", "max_tokens");
    // The client-requested model is known up front, regardless of what happens next; the native
    // Anthropic path carries no Codex-style reasoning-effort concept, so that field stays `None`.
    // No codex sub-agent concept either — this is a native Anthropic-Messages request.
    // Kept for selection (per-model cap filtering) — `model` itself moves into `PreparedRequest`.
    let requested_model = model.clone();
    let mut outcome = RouteOutcome {
        account_id: None,
        model: Some(model.clone()),
        reasoning_effort: None,
        service_tier: None,
        subagent: None,
        session_key: None,
        estimated_tokens,
        ..Default::default()
    };
    // Try to admit this as genuine Claude Code traffic. When it qualifies, the client's own bytes
    // and protocol envelope are forwarded verbatim and PolyFlare's only edit is the credential the
    // executor attaches — no parse/re-serialize round-trip can perturb the request.
    //
    // A rejection is NOT an error: an ordinary Anthropic SDK client is perfectly welcome on this
    // path, it simply gets the materialized body instead of byte pass-through, and (via the
    // eligibility filter above) can only be served by an API-key account.
    let admitted = client_wire.and_then(|(headers, raw)| {
        polyflare_anthropic::admit_native_request(&ClientHeaders(headers), &body)
            .ok()
            .map(|envelope| (envelope, headers, raw))
    });

    let req = match admitted {
        Some((envelope, headers, raw)) => {
            // Content-free compatibility observation: which client shape we admitted, never what
            // it asked. Outcome 7 turns this into persisted telemetry; for now it is a trace line.
            tracing::debug!(
                target: "polyflare_server::request",
                claude_client_shape = %envelope.shape_key(),
                "admitted native Claude request for byte pass-through"
            );
            PreparedRequest {
                body: None,
                model,
                forward_headers: polyflare_anthropic::forwarded_client_headers(&ClientHeaders(
                    headers,
                )),
                raw_body: Some(raw.clone()),
            }
        }
        None => PreparedRequest {
            // Not byte-forwardable ⇒ the materialized body is what's sent.
            body: Some(body),
            model,
            forward_headers: vec![],
            raw_body: None,
        },
    };
    let ctx = RequestCtx::default();

    let prepared = match NoopContinuity.prepare(req, &ctx).await {
        Ok(p) => p,
        Err(_) => return (internal_error(), outcome),
    };

    let snapshots = match state.account_cache.snapshots(&state.store).await {
        Ok(s) => s,
        Err(_) => return (internal_error(), outcome),
    };
    // M4a has no cross-format translator (that's M4b): `/v1/messages` may only ever pick an
    // Anthropic-provider account — the exact mirror of `/responses`'s Codex-only filter above.
    let snapshots = filter_by_provider_and_pool(&snapshots, Provider::Anthropic, pool);
    // A subscription-OAuth grant may serve only byte-faithful pass-through of a genuine client
    // request; translated traffic never becomes a candidate for one.
    let mut snapshots = filter_by_traffic_eligibility(&snapshots, traffic);
    // Per-model weekly caps: a seat whose cap for THIS model is exhausted (e.g. Fable at 100%)
    // cannot serve it until its reset, but serves every other model fine — so steer this request to
    // a seat with headroom instead of burning an attempt on a guaranteed 429. Fails open when every
    // seat is capped (see the helper), and is a no-op when nothing is capped.
    state
        .model_catalog
        .retain_accounts_without_capped_model(&mut snapshots, &requested_model);
    state.runtime.overlay(&mut snapshots, now);
    let selector = state.selector_for(pool);
    let sel_ctx = SelectionCtx {
        now,
        require_security_work_authorized: false,
        rng_seed: None,
        session_id: None,
        // Native Anthropic requests carry no Codex model-alias tier; tier steering is a
        // Codex-pool concern, so leave it unset here.
        tier: None,
        // C9 Task 3: startup-resolved, never a per-request env read.
        inflight_penalty_pct: state.runtime_settings.inflight_penalty_pct(),
        request_pressure_units: state.runtime.request_pressure_units(estimated_tokens),
    };
    let (picked, in_flight) = match state
        .runtime
        .select_and_acquire_wait(
            &mut snapshots,
            selector.as_ref(),
            &sel_ctx,
            now,
            &state.lease_metrics,
        )
        .await
    {
        Some(reservation) => reservation,
        None => {
            // B5-anthropic Task 3: the native `/v1/messages` mirror of `/responses`'s Layer 1 →
            // Layer 2 → `no_eligible` empty-pool fallthrough (see the `RouteDecision::
            // NoEligibleAccount` arm above) — same guarded serve-soonest-error-backoff (Layer 1),
            // then the keepalive recovery-wait (Layer 2), over the SAME Anthropic-only `snapshots`/
            // `sel_ctx` this handler already built above, but speaking the ANTHROPIC dialect
            // (`WaitClient::Anthropic`: `event: ping` keepalives, `event: error` terminal frames —
            // never Codex's `: keepalive`/`response.failed`). `session_key` is `None` throughout:
            // `NoopContinuity` never derives one on this path (SPEC-M4 §3.7 — no anchor concept for
            // the Anthropic backend), so there is nothing to key a waiter's wake-jitter beyond the
            // `layer2_wait_request_key` fallback's own per-wait random draw. Budget/heartbeat are
            // read straight off `state` (this handler has no test-seam override parameter, unlike
            // `responses_handler_impl`'s `_for_test_with_starvation_timing` — the REAL e2e harness
            // drives this through env-resolved `AppState` fields instead, mirroring production).
            let layer1 = try_layer1_serve_now(
                &state,
                &snapshots,
                selector.as_ref(),
                &sel_ctx,
                prepared.req.clone(),
                ctx.clone(),
                None,
                now,
                &mut outcome,
                &WaitClient::Anthropic,
            )
            .await;
            let resp = layer1.or_else(|| {
                try_layer2_recovery_wait(
                    state.clone(),
                    &snapshots,
                    pool.map(|p| p.to_string()),
                    Provider::Anthropic,
                    selector.clone(),
                    &sel_ctx,
                    prepared.req,
                    ctx,
                    None,
                    now,
                    state.runtime_settings.starvation_wait_budget(),
                    state.runtime_settings.starvation_heartbeat(),
                    &mut outcome,
                    WaitClient::Anthropic,
                )
            });
            return (resp.unwrap_or_else(no_eligible), outcome);
        }
    };
    outcome.account_id = Some(picked.as_str().to_string());
    let (account, provider) = match resolve_core_account(&state, &picked, now).await {
        Ok(a) => a,
        Err(r) => {
            bench_after_local_refusal(&state, &picked, now);
            return (r, outcome);
        }
    };

    let health_id = picked.clone(); // moved into the executor below.
                                    // Captured BEFORE the executor consumes its inputs, so a 401 can be retried on a freshly
                                    // refreshed token. Anthropic subscription-OAuth tokens are short-lived, and without this an
                                    // expired token 401s, the account is benched, and it never recovers until an operator
                                    // re-logs in — reactive refresh existed but was wired ONLY into the Codex `/responses` path.
    let rejected_access_token = account.bearer_token.clone();
    let prepared_for_auth_retry = prepared.clone();
    let ctx_for_auth_retry = ctx.clone();
    let picked_for_auth_retry = picked.clone();
    let max_attempts = state.runtime_settings.max_account_attempts();

    // C9 Task 2: the native `/v1/messages` streaming selection site — same lease treatment as
    // `/responses`'s Route arm.
    let mut execution = execute_with_watchdog_tracked(
        state.executor_for(provider).as_ref(),
        Arc::new(NoopContinuity) as Arc<dyn Continuity>,
        prepared,
        &account,
        picked,
        ctx,
        state.runtime.clone(),
        state.runtime_settings.stream_idle_timeout(),
        max_attempts,
        CommitWitness::new(),
        Some(in_flight),
    )
    .await;

    // Reactive refresh-and-retry on a 401, mirroring the Codex path. A rejected bearer cannot have
    // started sampling, so a single authenticated retry is safe and turns a silent bench into a
    // served request. `force_refresh_after_unauthorized` dispatches to the Anthropic refresh by the
    // account's stored `auth_mode`, so no provider branch is needed here.
    if is_unauthorized_execution(&execution) {
        match force_refresh_after_unauthorized(
            &state,
            &picked_for_auth_retry,
            &rejected_access_token,
            unix_now(),
        )
        .await
        {
            Ok(Some(refreshed_account)) => {
                if let Some(retry_lease) = state
                    .runtime
                    .acquire_pinned_in_flight_weighted(
                        &picked_for_auth_retry,
                        unix_now(),
                        &state.lease_metrics,
                        sel_ctx.request_pressure_units,
                    )
                    .await
                {
                    execution = execute_with_watchdog_tracked(
                        state.executor_for(provider).as_ref(),
                        Arc::new(NoopContinuity) as Arc<dyn Continuity>,
                        prepared_for_auth_retry,
                        &refreshed_account,
                        picked_for_auth_retry.clone(),
                        ctx_for_auth_retry,
                        state.runtime.clone(),
                        state.runtime_settings.stream_idle_timeout(),
                        max_attempts,
                        CommitWitness::new(),
                        Some(retry_lease),
                    )
                    .await;
                }
            }
            // Refresh failed (grant gone, or refresh endpoint rejected it): fall through with the
            // original 401 so it is recorded and surfaced honestly, exactly as before.
            Ok(None) => {}
            Err(refresh_response) => return (refresh_response, outcome),
        }
    }

    let response = match execution {
        Ok(stream) => stream_response(stream),
        Err(e) => {
            record_failure(&state, &health_id, &e, unix_now()).await;
            surface_watchdog_error(&e)
        }
    };
    (response, outcome)
}

/// A 401 on either watchdog-error shape, unwrapped from the executor's `Result`.
fn is_unauthorized_execution<T>(execution: &Result<T, WatchdogError>) -> bool {
    matches!(execution, Err(error) if is_unauthorized(error))
}

async fn messages_handler_custom_native(
    state: Arc<AppState>,
    headers: &HeaderMap,
    body: serde_json::Value,
    targets: Vec<(CustomProvider, ProviderModel)>,
) -> (Response, RouteOutcome) {
    let affinity_identity = admitted_claude_affinity_identity(headers, &body);
    let encoded = match serde_json::to_vec(&body) {
        Ok(encoded) => Bytes::from(encoded),
        Err(_) => return (internal_error(), RouteOutcome::default()),
    };
    execute_custom_models(
        &state,
        headers,
        &encoded,
        targets,
        "anthropic_messages",
        affinity_identity,
        RouteOutcome {
            model: body
                .get("model")
                .and_then(|value| value.as_str())
                .map(str::to_string),
            ..Default::default()
        },
    )
    .await
}

async fn messages_handler_custom_responses(
    state: Arc<AppState>,
    headers: &HeaderMap,
    body: serde_json::Value,
    model_alias: ModelAlias,
) -> (Response, RouteOutcome) {
    let TranslationTarget::Custom(provider_id) = &model_alias.target else {
        return (internal_error(), RouteOutcome::default());
    };
    let (provider, provider_model) = match resolve_translation_custom_target(
        &state,
        provider_id,
        &model_alias.target_model,
        "responses",
    )
    .await
    {
        Ok(target) => target,
        Err(response) => {
            return (
                response,
                RouteOutcome {
                    model: Some(model_alias.target_model),
                    reasoning_effort: model_alias.reasoning_effort,
                    ..Default::default()
                },
            )
        }
    };
    let client_wants_stream = body
        .get("stream")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    let mut translator = AnthropicToResponses::new_generic();
    let mut translated = translator.translate_request(body);
    translated["model"] = serde_json::Value::String(model_alias.target_model.clone());
    if let Some(effort) = &model_alias.reasoning_effort {
        translated["reasoning"] = serde_json::json!({"effort": effort});
    }
    if translated
        .get("prompt_cache_key")
        .and_then(serde_json::Value::as_str)
        .is_none()
    {
        translated["prompt_cache_key"] =
            serde_json::Value::String(derive_alias_prompt_cache_key(&translated));
    }
    let encoded = match serde_json::to_vec(&translated) {
        Ok(encoded) => Bytes::from(encoded),
        Err(_) => return (internal_error(), RouteOutcome::default()),
    };
    let (response, custom) = crate::custom_provider::execute(
        &state.store,
        &state.cipher,
        provider,
        provider_model,
        headers,
        &encoded,
        state.runtime_settings.starvation_wait_budget(),
    )
    .await;
    let outcome = RouteOutcome {
        provider_slug: Some(custom.provider_slug),
        provider_credential_id: custom.credential_id,
        upstream_model: Some(custom.upstream_model),
        upstream_transport: Some(custom.upstream_transport),
        profile_revision: custom.profile_revision,
        custom_pricing: match (
            custom.input_per_million,
            custom.cached_input_per_million,
            custom.output_per_million,
        ) {
            (Some(input), Some(cached), Some(output)) => {
                Some(polyflare_core::pricing::CustomModelRates {
                    input_per_1m: input,
                    cached_input_per_1m: cached,
                    output_per_1m: output,
                    priority_input_per_1m: custom.priority_input_per_million,
                    priority_cached_input_per_1m: custom.priority_cached_input_per_million,
                    priority_output_per_1m: custom.priority_output_per_million,
                })
            }
            _ => None,
        },
        model: Some(model_alias.target_model),
        reasoning_effort: model_alias.reasoning_effort,
        ..Default::default()
    };
    if !response.status().is_success() {
        return (response, outcome);
    }
    let translated_stream = wrap_translating_stream(
        response_into_stream(response),
        Box::new(translator) as Box<dyn Translator>,
    );
    let response = if client_wants_stream {
        stream_response(translated_stream)
    } else {
        match collect_anthropic_message(translated_stream).await {
            Ok(message) => json_message_response(message),
            Err(_) => (StatusCode::BAD_GATEWAY, "upstream error").into_response(),
        }
    };
    (response, outcome)
}

/// M4b-wiring: a client model string aliased to a Codex target (SPEC-M4 §3.6). Translates the
/// Anthropic-Messages request body into OpenAI-Responses via the per-turn stateful
/// `AnthropicToResponses` translator (`translate_request`), remaps `model` to the alias's target
/// and payload-overrides `reasoning.effort` when the alias specifies one, routes to the Codex pool
/// (the exact mirror of `/responses`'s partitioning), and — on success — wraps the raw
/// OpenAI-Responses response stream with the SAME translator instance
/// (`translate_stream::wrap_translating_stream`) so the client sees Anthropic-Messages SSE.
///
/// Continuity is a no-op here too: this translated turn never round-trips a Codex
/// `previous_response_id` back to an Anthropic client (SPEC-M4 §3.7's anchor-based
/// continuity/watchdog machinery is Codex-native-request-shaped only), so — like the native path
/// above — every request is `Disarmed` and the watchdog never arms.
async fn messages_handler_codex_aliased(
    state: Arc<AppState>,
    pool: Option<&str>,
    body: serde_json::Value,
    model_alias: ModelAlias,
) -> (Response, RouteOutcome) {
    let now = unix_now();
    // The resolved target model + effort are known up front from the alias itself, regardless of
    // what happens next. No codex sub-agent concept here either — this is a translated
    // Claude→Codex request, not a real Codex client turn carrying `x-openai-subagent`.
    let mut outcome = RouteOutcome {
        account_id: None,
        model: Some(model_alias.target_model.clone()),
        reasoning_effort: model_alias.reasoning_effort.clone(),
        service_tier: None,
        subagent: None,
        session_key: None,
        estimated_tokens: 0,
        ..Default::default()
    };
    // Outcome 3: the client's stream preference, read BEFORE `body` moves into
    // `translate_request` below. Anthropic's Messages API defaults `stream:false` — absent means
    // non-streaming, same as a real Anthropic client would treat it.
    let client_wants_stream = body
        .get("stream")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let mut translator = AnthropicToResponses::new();
    let mut translated_body = translator.translate_request(body);
    translated_body["model"] = serde_json::Value::String(model_alias.target_model.clone());
    if let Some(effort) = &model_alias.reasoning_effort {
        // U2/U4: confirm Codex effort payload shape — `{"reasoning":{"effort":...}}` is the
        // documented OpenAI-Responses request field; unverified end-to-end against a live Codex
        // backend.
        translated_body["reasoning"] = serde_json::json!({ "effort": effort });
    }

    // Give the fresh Codex body a stable, conversation-scoped `prompt_cache_key` so repeated turns
    // reuse OpenAI's prompt-prefix cache instead of cold-prefilling the whole history every turn.
    // Set only when absent (never clobber a client-supplied key) and BEFORE the header synthesis
    // below, which derives the codex identity from this key when present.
    if translated_body
        .get("prompt_cache_key")
        .and_then(|v| v.as_str())
        .is_none()
    {
        let key = derive_alias_prompt_cache_key(&translated_body);
        translated_body["prompt_cache_key"] = serde_json::Value::String(key);
    }
    let estimated_tokens =
        estimate_materialized_request_tokens(&translated_body, "input", "max_output_tokens");
    outcome.estimated_tokens = estimated_tokens;

    // Translated path: there is no real Codex client to forward, so SYNTHESIZE codex-rs's identity
    // headers (see `synthesize_codex_forward_headers`). Mirrors codex-lb's forward-native /
    // synthesize-non-native split. The User-Agent's codex version is resolved live (GitHub/npm,
    // cached) so it tracks the real fleet instead of a stale constant; `cached_or_fallback` is a
    // sync, zero-I/O read warmed out-of-band by the background refresh task.
    let forward_headers = synthesize_codex_forward_headers(
        &translated_body,
        &state.codex_version.cached_or_fallback(),
    );
    let model_for_selection = model_alias.target_model.clone();
    let req = PreparedRequest {
        // Translated alias body is built, not a raw pass-through ⇒ serialized by the executor.
        body: Some(translated_body),
        model: model_alias.target_model,
        forward_headers,
        raw_body: None,
    };
    let ctx = RequestCtx::default();

    let prepared = match NoopContinuity.prepare(req, &ctx).await {
        Ok(p) => p,
        Err(_) => return (internal_error(), outcome),
    };

    let snapshots = match state.account_cache.snapshots(&state.store).await {
        Ok(s) => s,
        Err(_) => return (internal_error(), outcome),
    };
    // The mirror of `/responses`'s Codex-only filter: an aliased-to-Codex turn may only ever pick
    // a Codex-provider account, regardless of what `/v1/messages` itself would otherwise select.
    let mut snapshots = filter_by_provider_and_pool(&snapshots, Provider::Codex, pool);
    state
        .model_catalog
        .retain_accounts_supporting(&mut snapshots, &model_for_selection);
    state.runtime.overlay(&mut snapshots, now);
    let selector = state.selector_for(pool);
    let sel_ctx = SelectionCtx {
        now,
        require_security_work_authorized: false,
        rng_seed: None,
        session_id: None,
        // The subagent tier IS the alias's reasoning effort (opus→high, sonnet→medium, haiku→low).
        tier: tier_from_effort(model_alias.reasoning_effort.as_deref()),
        // C9 Task 3: startup-resolved, never a per-request env read.
        inflight_penalty_pct: state.runtime_settings.inflight_penalty_pct(),
        request_pressure_units: state.runtime.request_pressure_units(estimated_tokens),
    };
    let (picked, in_flight) = match state
        .runtime
        .select_and_acquire_wait(
            &mut snapshots,
            selector.as_ref(),
            &sel_ctx,
            now,
            &state.lease_metrics,
        )
        .await
    {
        Some(reservation) => reservation,
        None => {
            // KNOWN LIMITATION (M4 non-streaming, 2026-07-22): this pool-STARVATION recovery-wait
            // fallback always returns SSE (via `try_layer1/2` → `stream_response`), even for a
            // `stream:false` client that the immediate-pick success path below would answer with a
            // buffered JSON `Message`. Buffering here would mean refactoring the shared
            // `try_layer1_serve_now`/`try_layer2_recovery_wait` helpers (also used by `/responses`)
            // to hand back the stream rather than a built Response — out of this slice's scope. A
            // non-streaming client that hits starvation therefore still receives Anthropic SSE
            // (rare edge; follow-up). The immediate-pick path (below) is fully non-streaming-aware.
            //
            // B5-anthropic Task 4: the Codex-aliased `/v1/messages` mirror of
            // `messages_handler_native`'s Layer 1 → Layer 2 → `no_eligible` empty-pool fallthrough
            // (T3), but waiting ON the Codex pool (`pool_provider = Provider::Codex`, the SAME
            // `snapshots`/`sel_ctx` this handler already built above) while speaking the
            // TRANSLATED Anthropic dialect to the client: `WaitClient::AnthropicTranslated`'s
            // `event: ping` keepalives (never Codex's `: keepalive`/`response.failed`), and — once
            // recovered — the real Codex stream translated into Anthropic SSE by a FRESH
            // `AnthropicToResponses` instance (built by the factory at the actual serve site, NOT
            // this handler's own `translator` above, which is reserved for the immediate-pick
            // success path below). `session_key` is `None`: `NoopContinuity` never derives one on
            // this path either (same as the native path's rationale).
            let client = WaitClient::AnthropicTranslated(std::sync::Arc::new(|| {
                Box::new(AnthropicToResponses::new()) as Box<dyn Translator>
            }));
            let layer1 = try_layer1_serve_now(
                &state,
                &snapshots,
                selector.as_ref(),
                &sel_ctx,
                prepared.req.clone(),
                ctx.clone(),
                None,
                now,
                &mut outcome,
                &client,
            )
            .await;
            let resp = layer1.or_else(|| {
                try_layer2_recovery_wait(
                    state.clone(),
                    &snapshots,
                    pool.map(|p| p.to_string()),
                    Provider::Codex,
                    selector.clone(),
                    &sel_ctx,
                    prepared.req,
                    ctx,
                    None,
                    now,
                    state.runtime_settings.starvation_wait_budget(),
                    state.runtime_settings.starvation_heartbeat(),
                    &mut outcome,
                    client,
                )
            });
            return (resp.unwrap_or_else(no_eligible), outcome);
        }
    };
    outcome.account_id = Some(picked.as_str().to_string());
    let (account, provider) = match resolve_core_account(&state, &picked, now).await {
        Ok(a) => a,
        Err(r) => {
            bench_after_local_refusal(&state, &picked, now);
            return (r, outcome);
        }
    };

    let health_id = picked.clone(); // moved into the executor below.
                                    // C9 Task 2: the Codex-aliased `/v1/messages` streaming selection site — same lease treatment
                                    // as `/responses`'s Route arm. `wrap_translating_stream` below just wraps the returned
                                    // `ResponseStream` (the `ObservingStream` carrying `_in_flight`) in another stream layer that
                                    // owns it by value — the lease's lifetime is unaffected by the translation wrapper.
    let response = match execute_with_watchdog_tracked(
        state.executor_for(provider).as_ref(),
        Arc::new(NoopContinuity) as Arc<dyn Continuity>,
        prepared,
        &account,
        picked,
        ctx,
        state.runtime.clone(),
        state.runtime_settings.stream_idle_timeout(),
        state.runtime_settings.max_account_attempts(),
        CommitWitness::new(),
        Some(in_flight),
    )
    .await
    {
        Ok(stream) => {
            let translated_stream =
                wrap_translating_stream(stream, Box::new(translator) as Box<dyn Translator>);
            if client_wants_stream {
                stream_response(translated_stream)
            } else {
                // Outcome 3: a non-streaming client gets one buffered `application/json` Message,
                // never SSE. `collect_anthropic_message` consumes the SAME already-translated
                // Anthropic frames the streaming branch would have relayed byte-for-byte — no
                // second translation. A mid-stream error propagates as the same generic,
                // content-safe response the streaming Err arm below uses; never a partial Message.
                match collect_anthropic_message(translated_stream).await {
                    Ok(message) => json_message_response(message),
                    Err(_) => (StatusCode::BAD_GATEWAY, "upstream error").into_response(),
                }
            }
        }
        Err(e) => {
            record_failure(&state, &health_id, &e, unix_now()).await;
            surface_watchdog_error(&e)
        }
    };
    (response, outcome)
}

#[cfg(test)]
mod tests {
    use super::{
        admitted_claude_affinity_identity, apply_scope_models_etag, derive_alias_prompt_cache_key,
        is_unauthorized, is_unauthorized_execution, response_transport, surface_watchdog_error,
        WaitClient,
    };
    use crate::watchdog::WatchdogError;
    use axum::body::{to_bytes, Body};
    use axum::http::{header, HeaderMap, HeaderValue, Response, StatusCode};
    use serde_json::json;

    fn signal(status: u16) -> polyflare_core::FailureSignal {
        polyflare_core::FailureSignal {
            status,
            retry_after: None,
            error_code: None,
        }
    }

    /// The 401 detection that gates the Anthropic (and Codex) reactive refresh-and-retry. It must
    /// fire on BOTH watchdog-error shapes and on nothing else — a false negative benches an account
    /// forever on an expired token; a false positive burns a refresh on an unrelated error.
    #[test]
    fn a_401_is_recognised_on_both_error_shapes_and_only_a_401() {
        assert!(is_unauthorized(&WatchdogError::Upstream(Some(signal(401)))));
        assert!(is_unauthorized(&WatchdogError::UpstreamHttp(
            polyflare_core::UpstreamHttpError {
                signal: signal(401),
                headers: Vec::new(),
                body: bytes::Bytes::new(),
            }
        )));
        // Not a 401 → no refresh.
        assert!(!is_unauthorized(&WatchdogError::Upstream(Some(signal(
            500
        )))));
        assert!(!is_unauthorized(&WatchdogError::Upstream(Some(signal(
            429
        )))));
        assert!(!is_unauthorized(&WatchdogError::AttemptBudgetExhausted));

        // The Result wrapper the executor returns: only an Err(401) qualifies.
        let ok: Result<(), WatchdogError> = Ok(());
        assert!(!is_unauthorized_execution(&ok));
        let err_401: Result<(), WatchdogError> = Err(WatchdogError::Upstream(Some(signal(401))));
        assert!(is_unauthorized_execution(&err_401));
        let err_500: Result<(), WatchdogError> = Err(WatchdogError::Upstream(Some(signal(500))));
        assert!(!is_unauthorized_execution(&err_500));
    }

    #[tokio::test]
    async fn logical_turn_budget_error_is_non_retryable_and_content_safe() {
        let response = surface_watchdog_error(&WatchdogError::AttemptBudgetExhausted);
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), 16 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["error"]["code"], "logical_turn_attempts_exhausted");
        assert_eq!(value["error"]["type"], "invalid_request_error");
    }

    #[test]
    fn post_commit_attempt_budget_frame_is_non_retryable_for_each_dialect() {
        let codex =
            String::from_utf8(WaitClient::Codex.attempt_budget_error_frame().to_vec()).unwrap();
        assert!(codex.contains("\"code\":\"invalid_prompt\""));
        assert!(codex.contains("logical turn exhausted"));

        let anthropic =
            String::from_utf8(WaitClient::Anthropic.attempt_budget_error_frame().to_vec()).unwrap();
        assert!(anthropic.contains("\"type\":\"invalid_request_error\""));
        assert!(anthropic.contains("logical turn exhausted"));
    }

    #[test]
    fn response_transport_distinguishes_sse_from_buffered_http() {
        let sse = Response::builder()
            .header(header::CONTENT_TYPE, "text/event-stream; charset=utf-8")
            .body(Body::empty())
            .unwrap();
        let json = Response::builder()
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::empty())
            .unwrap();

        assert_eq!(response_transport(&sse), "sse");
        assert_eq!(response_transport(&json), "http");
    }

    #[test]
    fn custom_anthropic_affinity_uses_only_an_admitted_hashed_claude_session() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "user-agent",
            HeaderValue::from_static("claude-cli/2.1.218 (external, sdk-ts)"),
        );
        headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
        headers.insert(
            "anthropic-beta",
            HeaderValue::from_static("claude-code-20250219,oauth-2025-04-20"),
        );
        headers.insert(
            "x-claude-code-session-id",
            HeaderValue::from_static("c38f98c8-7c2a-4e93-aa3d-a79df7a7015f"),
        );
        let first = json!({
            "model": "claude-balanced",
            "max_tokens": 100,
            "messages": [{"role": "user", "content": "first"}],
            "stream": true
        });
        let later = json!({
            "model": "claude-balanced",
            "max_tokens": 100,
            "messages": [{"role": "user", "content": "different content"}],
            "stream": true
        });
        let first_identity =
            admitted_claude_affinity_identity(&headers, &first).expect("admitted Claude identity");
        assert_eq!(
            first_identity,
            admitted_claude_affinity_identity(&headers, &later).unwrap(),
            "message content must not influence session affinity"
        );
        assert_eq!(first_identity.len(), 64);
        assert!(
            first_identity.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "the selector receives only an opaque hash"
        );

        let mut other_session = headers.clone();
        other_session.insert(
            "x-claude-code-session-id",
            HeaderValue::from_static("d49f98c8-7c2a-4e93-aa3d-a79df7a7015f"),
        );
        assert_ne!(
            first_identity,
            admitted_claude_affinity_identity(&other_session, &first).unwrap()
        );

        let mut missing_oauth_beta = headers.clone();
        missing_oauth_beta.insert(
            "anthropic-beta",
            HeaderValue::from_static("claude-code-20250219"),
        );
        assert!(
            admitted_claude_affinity_identity(&missing_oauth_beta, &first).is_none(),
            "an unadmitted caller cannot claim Claude session affinity"
        );

        let mut malformed_session = headers;
        malformed_session.insert(
            "x-claude-code-session-id",
            HeaderValue::from_static("../../not-a-session"),
        );
        assert!(admitted_claude_affinity_identity(&malformed_session, &first).is_none());
    }

    #[test]
    fn scoped_response_etag_never_leaks_selected_accounts_native_identity() {
        let mut cold = Response::builder()
            .header("x-models-etag", "account-native")
            .body(Body::empty())
            .unwrap();
        apply_scope_models_etag(&mut cold, None);
        assert!(
            cold.headers().get("x-models-etag").is_none(),
            "cold scope must remove the selected account's native ETag"
        );

        let mut warm = Response::builder()
            .header("x-models-etag", "account-native")
            .body(Body::empty())
            .unwrap();
        apply_scope_models_etag(&mut warm, Some("\"polyflare-scope\"".to_string()));
        assert_eq!(
            warm.headers()
                .get("x-models-etag")
                .and_then(|value| value.to_str().ok()),
            Some("\"polyflare-scope\"")
        );
    }

    /// The same conversation across turns (same system prompt + same first message, later turns
    /// append more input) must yield the SAME key — that is what makes the prompt cache hit.
    #[test]
    fn key_is_stable_across_turns_of_a_conversation() {
        let turn1 = json!({
            "instructions": "You are Claude Code.",
            "input": [{"role": "user", "content": [{"type": "input_text", "text": "hello"}]}],
        });
        let turn2 = json!({
            "instructions": "You are Claude Code.",
            "input": [
                {"role": "user", "content": [{"type": "input_text", "text": "hello"}]},
                {"role": "assistant", "content": [{"type": "output_text", "text": "hi"}]},
                {"role": "user", "content": [{"type": "input_text", "text": "next question"}]},
            ],
        });
        assert_eq!(
            derive_alias_prompt_cache_key(&turn1),
            derive_alias_prompt_cache_key(&turn2),
            "same instructions + same first input item ⇒ same conversation key"
        );
    }

    #[test]
    fn key_differs_across_conversations() {
        let base = json!({"instructions": "sys", "input": [{"text": "conv A"}]});
        let diff_first = json!({"instructions": "sys", "input": [{"text": "conv B"}]});
        let diff_instr = json!({"instructions": "other", "input": [{"text": "conv A"}]});
        let k = derive_alias_prompt_cache_key(&base);
        assert_ne!(
            k,
            derive_alias_prompt_cache_key(&diff_first),
            "different first message"
        );
        assert_ne!(
            k,
            derive_alias_prompt_cache_key(&diff_instr),
            "different system prompt"
        );
    }

    #[test]
    fn key_is_48_hex_chars_and_handles_missing_fields() {
        for body in [
            json!({}),
            json!({"input": []}),
            json!({"instructions": "x"}),
        ] {
            let k = derive_alias_prompt_cache_key(&body);
            assert_eq!(k.len(), 48, "48 hex chars for {body}");
            assert!(
                k.bytes().all(|b| b.is_ascii_hexdigit()),
                "hex only for {body}"
            );
        }
    }

    // B10 Task 1 (THE CRUX) — the per-waiter wake-jitter pure helpers. `select.rs`/
    // `runtime_state.rs`'s backoff are UNTOUCHED by this task; these helpers live entirely here,
    // over a per-REQUEST key, never over account snapshots.
    mod wake_jitter {
        use super::super::{jittered_wake_target_ms, wake_jitter_offset_ms};

        /// (a) Two DIFFERENT keys produce offsets bounded in `[0, wake_jitter_ms]`, and — the
        /// whole point — generally DIFFERENT values (desync). Not a proof for every possible pair,
        /// but at least one representative pair must differ, or every waiter would still wake in
        /// lockstep (today's B5 herd, byte-for-byte).
        #[test]
        fn offset_is_bounded_and_desyncs_different_keys() {
            let a = wake_jitter_offset_ms("waiter-a", 1000);
            let b = wake_jitter_offset_ms("waiter-b", 1000);
            assert!(a <= 1000, "offset must be in [0, wake_jitter_ms]: {a}");
            assert!(b <= 1000, "offset must be in [0, wake_jitter_ms]: {b}");
            assert_ne!(
                a, b,
                "different request keys must desync (else this is a no-op re-implementation of \
                 today's lockstep herd)"
            );
        }

        /// `wake_jitter_ms == 0` (the disable lever's resolved value) ⇒ ALWAYS `0`, for any key —
        /// byte-for-byte today's pre-B10 behavior.
        #[test]
        fn zero_jitter_window_is_always_zero() {
            assert_eq!(wake_jitter_offset_ms("any-key", 0), 0);
            assert_eq!(wake_jitter_offset_ms("another-key", 0), 0);
            assert_eq!(wake_jitter_offset_ms("", 0), 0);
        }

        /// Deterministic-per-request: the SAME key always yields the SAME offset — the testable
        /// seam the plan's Global Constraints require (not a process-global `rand` draw).
        #[test]
        fn same_key_is_deterministic() {
            assert_eq!(
                wake_jitter_offset_ms("stable-key", 5000),
                wake_jitter_offset_ms("stable-key", 5000),
                "same request key must always produce the same offset"
            );
            assert_eq!(
                wake_jitter_offset_ms("stable-key", 5000),
                wake_jitter_offset_ms("stable-key", 5000),
                "reproducible across repeated calls, not just adjacent ones"
            );
        }

        /// (b) The target math: `target_ms + jitter` capped at `budget_deadline_ms` — a jitter
        /// that would exceed the budget clamps DOWN to the budget, never past it.
        #[test]
        fn target_math_caps_at_the_budget_deadline() {
            let target_ms = 1_000_000_i64;
            let budget_deadline_ms = 1_000_500_i64; // only 500ms of budget room left
            assert_eq!(
                jittered_wake_target_ms(target_ms, 10_000, budget_deadline_ms),
                budget_deadline_ms,
                "a jitter that would exceed the budget clamps to the budget, never past it"
            );
        }

        /// Jitter only ever ADDS delay — the jittered target is never before `target_ms`, and
        /// zero jitter is a byte-for-byte no-op.
        #[test]
        fn target_math_never_wakes_before_target_ms() {
            let target_ms = 1_000_000_i64;
            let budget_deadline_ms = 1_100_000_i64; // plenty of budget room
            assert_eq!(
                jittered_wake_target_ms(target_ms, 0, budget_deadline_ms),
                target_ms,
                "zero jitter is a no-op — identical to today's target_ms"
            );
            let jittered = jittered_wake_target_ms(target_ms, 5_000, budget_deadline_ms);
            assert!(
                jittered >= target_ms,
                "jitter only ever adds delay, never wakes earlier than target_ms (got {jittered})"
            );
            assert_eq!(
                jittered, 1_005_000,
                "within budget room, the full jitter is applied"
            );
        }
    }

    // Live-usage-cost-capture Task 6 (THE CRUX) — `apply_captured_usage`'s persistence logic,
    // exercised at the helper level (an `insert` + a captured usage + a re-`list`), independent of
    // the stream-wrapper plumbing in `responses_route` itself.
    mod apply_captured_usage_tests {
        use super::super::apply_captured_usage;
        use crate::usage_capture::{CapturedUsage, ResponseUsage};
        use polyflare_store::{RequestLogRecord, Store};

        fn sample_record(request_id: &str) -> RequestLogRecord {
            RequestLogRecord {
                requested_service_tier: None,
                actual_service_tier: None,
                requested_at: 100,
                provider: "codex".into(),
                method: "POST".into(),
                path: "/responses".into(),
                aliased: false,
                status: 200,
                duration_ms: 1000,
                account_id: Some("acct-1".into()),
                target_kind: Some("account".into()),
                provider_credential_id: None,
                model: Some("gpt-5.6-sol".into()),
                upstream_model: None,
                upstream_transport: Some("http_sse".into()),
                profile_revision: None,
                reasoning_effort: None,
                service_tier: None,
                transport: Some("http".into()),
                ttft_ms: None,
                total_tokens: None,
                cached_tokens: None,
                subagent: None,
                request_id: Some(request_id.into()),
                session_key: None,
                input_tokens: None,
                output_tokens: None,
                cached_input_tokens: None,
                reasoning_tokens: None,
                orchestration_input_tokens: None,
                orchestration_output_tokens: None,
                orchestration_cached_input_tokens: None,
                cost_usd: None,
                latency_first_token_ms: None,
                protocol_outcome: None,
                error_code: None,
            }
        }

        /// The crux assertion: a known `CapturedUsage` (100k in / 10k out / 20k cached, gpt-5.6-sol
        /// default tier) fires an `update_usage` that lands `input_tokens`, a cost of ~0.71 USD (the
        /// SAME figure `polyflare_core::pricing`'s `cost_default_tier_gpt56_sol` test pins), and the
        /// TTFT — on the SAME row `insert` created, correlated purely by `request_id`.
        ///
        /// Live-row-tps-basis fix: ALSO asserts `duration_ms` is overwritten from the insert's
        /// baseline (1000, `sample_record`'s route+setup-only figure) to the captured end-to-end
        /// value (9000), and that the row's numbers now derive a SANE tokens/sec — mirroring
        /// `read_api::derive_tps`'s `output_tokens / ((duration_ms - ttft_ms) / 1000.0)` formula
        /// (that fn is private to `read_api.rs`, so this inlines the identical arithmetic) — rather
        /// than the pre-fix bug where `duration_ms` (route+setup only, e.g. ~50ms) could be SMALLER
        /// than a wrapper-origin `ttft_ms`, driving `derive_tps` to `None`/nonsense or an inflated
        /// value depending on which side of zero the subtraction landed.
        #[tokio::test]
        async fn fills_usage_cost_and_ttft_on_the_matching_row() {
            let dir = tempfile::tempdir().unwrap();
            let store = Store::open(&dir.path().join("s.db")).await.unwrap();
            let repo = store.request_log();
            repo.insert(&sample_record("rq")).await.unwrap();

            let captured = CapturedUsage {
                served_tier: None,
                usage: Some(ResponseUsage {
                    input_tokens: Some(100_000),
                    output_tokens: Some(10_000),
                    cached_input_tokens: Some(20_000),
                    cache_write_input_tokens: Some(1_000),
                    reasoning_tokens: Some(500),
                    reported_total_tokens: Some(110_000),
                    ..Default::default()
                }),
                ttft_ms: Some(1200),
                duration_ms: Some(9000),
                protocol_outcome: polyflare_store::RequestProtocolOutcome::Completed,
                stream_error_status: None,
            };
            apply_captured_usage(&store, "rq", Some("gpt-5.6-sol"), None, None, captured);
            store.flush_background_writes().await.unwrap();

            let row = repo
                .list(10, 0)
                .await
                .unwrap()
                .into_iter()
                .find(|r| r.request_id.as_deref() == Some("rq"))
                .unwrap();
            assert_eq!(row.input_tokens, Some(100_000));
            assert_eq!(row.output_tokens, Some(10_000));
            assert_eq!(row.cached_input_tokens, Some(20_000));
            assert_eq!(row.cache_write_input_tokens, Some(1_000));
            assert_eq!(row.reasoning_tokens, Some(500));
            assert_eq!(row.reported_total_tokens, Some(110_000));
            assert_eq!(row.usage_schema.as_deref(), Some("openai_responses_v1"));
            assert_eq!(row.usage_source.as_deref(), Some("upstream_response"));
            assert_eq!(row.usage_status.as_deref(), Some("final"));
            let cost = row
                .cost_usd
                .expect("cost must be computed for a known model");
            assert!(
                (cost - 0.71).abs() < 1e-9,
                "expected ~0.71 (80_000/1e6*5.0 + 20_000/1e6*0.5 + 10_000/1e6*30.0), got {cost}"
            );
            assert_eq!(row.latency_first_token_ms, Some(1200));
            assert_eq!(
                row.duration_ms, 9000,
                "duration_ms must be overwritten to the captured end-to-end value, not left at \
                 the insert's route+setup-only baseline (1000)"
            );

            // Same-origin sanity: generation throughput uses output tokens only, and
            // `derive_tps`'s formula on the now-consistent duration_ms/ttft_ms pair must be a
            // finite, positive number — not the inflated or nonsensical value the pre-fix
            // two-clock-origins bug produced.
            let output_tokens = 10_000_i64;
            let window_ms = row.duration_ms - row.latency_first_token_ms.unwrap();
            assert!(
                window_ms > 0,
                "post-TTFT generation window must be positive"
            );
            let tps = output_tokens as f64 / (window_ms as f64 / 1000.0);
            assert!(
                tps.is_finite() && tps > 0.0,
                "expected a sane finite tps, got {tps}"
            );
        }

        /// `captured.usage = None` (e.g. disconnect before `response.completed`) keeps token/cost
        /// evidence unknown while still persisting the independently observed TTFT and end-to-end
        /// duration.
        #[tokio::test]
        async fn no_usage_captured_still_persists_observed_stream_timings() {
            let dir = tempfile::tempdir().unwrap();
            let store = Store::open(&dir.path().join("s.db")).await.unwrap();
            let repo = store.request_log();
            repo.insert(&sample_record("rq2")).await.unwrap();

            apply_captured_usage(
                &store,
                "rq2",
                Some("gpt-5.6-sol"),
                None,
                None,
                CapturedUsage {
                    served_tier: None,
                    usage: None,
                    ttft_ms: Some(500),
                    duration_ms: Some(4000),
                    protocol_outcome: polyflare_store::RequestProtocolOutcome::Cancelled,
                stream_error_status: None,
                },
            );
            store.flush_background_writes().await.unwrap();

            let row = repo
                .list(10, 0)
                .await
                .unwrap()
                .into_iter()
                .find(|r| r.request_id.as_deref() == Some("rq2"))
                .unwrap();
            assert_eq!(row.input_tokens, None);
            assert_eq!(row.output_tokens, None);
            assert_eq!(row.cost_usd, None);
            assert_eq!(row.latency_first_token_ms, Some(500));
            assert_eq!(
                row.duration_ms, 4000,
                "observed end-to-end latency must not be discarded with missing final usage"
            );
        }

        #[tokio::test]
        async fn missing_output_usage_stays_unknown_instead_of_becoming_zero() {
            let dir = tempfile::tempdir().unwrap();
            let store = Store::open(&dir.path().join("s.db")).await.unwrap();
            let repo = store.request_log();
            repo.insert(&sample_record("rq3")).await.unwrap();

            apply_captured_usage(
                &store,
                "rq3",
                Some("gpt-5.6-sol"),
                None,
                None,
                CapturedUsage {
                    served_tier: None,
                    usage: Some(ResponseUsage {
                        input_tokens: Some(1_000),
                        output_tokens: None,
                        cached_input_tokens: None,
                        reasoning_tokens: None,
                        ..Default::default()
                    }),
                    ttft_ms: Some(250),
                    duration_ms: Some(2000),
                    protocol_outcome: polyflare_store::RequestProtocolOutcome::Completed,
                stream_error_status: None,
                },
            );
            store.flush_background_writes().await.unwrap();

            let row = repo
                .list(10, 0)
                .await
                .unwrap()
                .into_iter()
                .find(|r| r.request_id.as_deref() == Some("rq3"))
                .unwrap();
            assert_eq!(row.input_tokens, Some(1_000));
            assert_eq!(row.output_tokens, None);
            assert_eq!(row.cached_input_tokens, None);
            assert_eq!(row.cost_usd, None);
        }

        /// Billing must follow the tier the UPSTREAM REPORTED, not the tier requested. A custom
        /// provider that accepts a priority request and serves it as standard must be charged the
        /// standard rate — the case that motivated the priority rate columns.
        #[tokio::test]
        async fn custom_model_cost_uses_the_reported_tier_not_the_requested_one() {
            let dir = tempfile::tempdir().unwrap();
            let store = Store::open(&dir.path().join("store.db")).await.unwrap();
            let repo = store.request_log();
            // $2/1M input, $0.50 cached, $8 output; priority is double across the board.
            let rates = polyflare_core::pricing::CustomModelRates {
                input_per_1m: 2.0,
                cached_input_per_1m: 0.5,
                output_per_1m: 8.0,
                priority_input_per_1m: Some(4.0),
                priority_cached_input_per_1m: Some(1.0),
                priority_output_per_1m: Some(16.0),
            };
            let captured = || CapturedUsage {
                served_tier: None,
                usage: Some(ResponseUsage {
                    input_tokens: Some(1_000_000),
                    output_tokens: Some(1_000_000),
                    cached_input_tokens: Some(0),
                    ..Default::default()
                }),
                ttft_ms: None,
                duration_ms: Some(10),
                protocol_outcome: polyflare_store::RequestProtocolOutcome::Completed,
                stream_error_status: None,
            };

            for (request_id, tier, expected) in [
                ("standard-turn", Some("standard"), 10.0),
                // The premium applies only here.
                ("priority-turn", Some("priority"), 20.0),
                ("fast-turn", Some("fast"), 20.0),
                // Requested priority, served standard: bill standard.
                ("downgraded-turn", Some("standard"), 10.0),
                ("untiered-turn", None, 10.0),
            ] {
                repo.insert(&sample_record(request_id)).await.unwrap();
                apply_captured_usage(&store, request_id, None, Some(rates), tier, captured());
                store.flush_background_writes().await.unwrap();
                let row = repo
                    .list(50, 0)
                    .await
                    .unwrap()
                    .into_iter()
                    .find(|r| r.request_id.as_deref() == Some(request_id))
                    .unwrap();
                let cost = row.cost_usd.expect("custom pricing must produce a cost");
                assert!(
                    (cost - expected).abs() < 1e-9,
                    "{request_id} at tier {tier:?}: expected {expected}, got {cost}"
                );
            }
        }

        /// Billing must NOT follow the tier the terminal SSE frame reports.
        ///
        /// Codex reports `service_tier: "default"` for turns it genuinely serves at priority
        /// (openai/codex#30413). Costing those at the standard rate would under-report real spend
        /// — the more dangerous direction for a cost tracker — so the requested tier decides the
        /// rate while the reported tier is recorded for diagnostics only.
        ///
        /// gpt-5.6-sol is 5.0/30.0 standard and 10.0/60.0 priority per 1M; 100k in + 100k out
        /// stays under its 272k long-context threshold so this measures the TIER alone.
        #[tokio::test]
        async fn builtin_cost_follows_the_requested_tier_and_records_the_reported_one() {
            let dir = tempfile::tempdir().unwrap();
            let store = Store::open(&dir.path().join("s.db")).await.unwrap();
            let repo = store.request_log();

            let captured = |served: Option<crate::usage_capture::ServedTier>| CapturedUsage {
                served_tier: served,
                usage: Some(ResponseUsage {
                    input_tokens: Some(100_000),
                    output_tokens: Some(100_000),
                    cached_input_tokens: Some(0),
                    ..Default::default()
                }),
                ttft_ms: None,
                duration_ms: Some(10),
                protocol_outcome: polyflare_store::RequestProtocolOutcome::Completed,
                stream_error_status: None,
            };

            for (id, requested, served, expected) in [
                // The real-world case: asked priority, Codex claims default, bill priority anyway.
                (
                    "reported-default",
                    Some("priority"),
                    Some(crate::usage_capture::ServedTier::Default),
                    7.0,
                ),
                (
                    "reported-priority",
                    Some("priority"),
                    Some(crate::usage_capture::ServedTier::Priority),
                    7.0,
                ),
                // Never asked for priority: standard, whatever the label says.
                (
                    "never-asked",
                    None,
                    Some(crate::usage_capture::ServedTier::Default),
                    3.5,
                ),
            ] {
                repo.insert(&sample_record(id)).await.unwrap();
                apply_captured_usage(
                    &store,
                    id,
                    Some("gpt-5.6-sol"),
                    None,
                    requested,
                    captured(served),
                );
                store.flush_background_writes().await.unwrap();
                let row = repo
                    .list(50, 0)
                    .await
                    .unwrap()
                    .into_iter()
                    .find(|r| r.request_id.as_deref() == Some(id))
                    .unwrap();
                let cost = row.cost_usd.expect("known model must produce a cost");
                assert!(
                    (cost - expected).abs() < 1e-9,
                    "{id}: expected {expected}, got {cost}"
                );
                // The reported tier is still persisted — it is what made the analysis possible.
                assert_eq!(
                    row.actual_service_tier.as_deref(),
                    served.map(|tier| tier.as_str()),
                    "{id}: the reported tier must survive as a diagnostic"
                );
            }
        }

        /// A model with no configured priority rate is never charged a premium, even for a turn the
        /// upstream genuinely served as priority.
        #[tokio::test]
        async fn a_model_without_priority_rates_bills_standard_for_a_priority_turn() {
            let dir = tempfile::tempdir().unwrap();
            let store = Store::open(&dir.path().join("store.db")).await.unwrap();
            let repo = store.request_log();
            let rates = polyflare_core::pricing::CustomModelRates {
                input_per_1m: 2.0,
                cached_input_per_1m: 0.5,
                output_per_1m: 8.0,
                priority_input_per_1m: None,
                priority_cached_input_per_1m: None,
                priority_output_per_1m: None,
            };
            repo.insert(&sample_record("no-priority-rate"))
                .await
                .unwrap();
            apply_captured_usage(
                &store,
                "no-priority-rate",
                None,
                Some(rates),
                Some("priority"),
                CapturedUsage {
                    served_tier: None,
                    usage: Some(ResponseUsage {
                        input_tokens: Some(1_000_000),
                        output_tokens: Some(1_000_000),
                        cached_input_tokens: Some(0),
                        ..Default::default()
                    }),
                    ttft_ms: None,
                    duration_ms: Some(10),
                    protocol_outcome: polyflare_store::RequestProtocolOutcome::Completed,
                stream_error_status: None,
                },
            );
            store.flush_background_writes().await.unwrap();
            let row = repo
                .list(10, 0)
                .await
                .unwrap()
                .into_iter()
                .find(|r| r.request_id.as_deref() == Some("no-priority-rate"))
                .unwrap();
            let cost = row.cost_usd.unwrap();
            assert!(
                (cost - 10.0).abs() < 1e-9,
                "expected standard 10.0, got {cost}"
            );
        }
    }
}
