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

use polyflare_anthropic::AnthropicToResponses;
use polyflare_codex::oauth::{classify_failure, should_refresh, token_exp, OAuthError};
use polyflare_core::{
    Account, AccountId, AccountSnapshot, BackoffKind, Continuity, ContinuityDirective,
    NoopContinuity, Prepared, PreparedRequest, Provider, RecoveryPlan, RequestCtx, ResponseStream,
    SelectionCtx, Selector, SessionKey, Tier, Translator, WatchdogArm,
};
use polyflare_store::{PlainTokens, RequestLogRecord, RequestLogRepo};

use crate::alias::{self, ModelAlias};
use crate::app::AppState;
use crate::config;
use crate::failover::{exclude_tried, failover_reason_code, failover_verdict, FailoverVerdict};
use crate::fingerprint_capture::{append_fingerprint_capture, capture_request_fingerprint};
use crate::observability::{FailoverSignal, RequestLog};
use crate::session_key::parse_inbound;
use crate::snapshot::filter_by_provider_and_pool;
use crate::starvation;
use crate::translate_stream::wrap_translating_stream;
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

/// Bounded retries for the post-refresh token persist (the one write whose loss kills an account —
/// see the call site). `busy_timeout` is the first line of defense; this is the backstop.
const PERSIST_MAX_ATTEMPTS: u32 = 3;
/// Fixed backoff between persist retries (small — the write is on the hot lock).
const PERSIST_RETRY_BACKOFF: Duration = Duration::from_millis(100);

/// Stream-idle-timeout plan (`docs/superpowers/plans/2026-07-18-stream-idle-timeout.md`) Task 2:
/// the DEFAULT source for the mid-stream idle deadline — matches codex's own `stream_idle_timeout`
/// default (`model-provider-info/src/lib.rs:26`, 300000ms). Every `execute_with_watchdog*`/
/// `execute_recovery*` call site below now threads `state.stream_idle_timeout` (the real,
/// config-resolved `Duration` on `AppState` — see `crate::config::stream_idle_timeout_secs_from_env`
/// and `ServeConfig::from_env`, resolved ONCE at startup, never per-request). This constant is no
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

fn account_unavailable() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, "account unavailable").into_response()
}

/// `pub(crate)`: also reused by `crate::control::resolve_control_account`'s snapshot-read failure
/// path, for a byte-identical generic 500.
pub(crate) fn internal_error() -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
}

/// `pub(crate)`: also the D17 control-endpoint account resolution's (`crate::control`) no-eligible-
/// account response, so both paths return byte-identical 503s.
pub(crate) fn no_eligible() -> Response {
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

/// Classify a watchdog failure and write the routing-health signal for the account `id` that
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
/// A6 (deliberately NOT implemented here — a retirement, not a gap): there is no third branch
/// dispatching to `runtime_state::record_quota_exceeded`. See that function's doc comment for the
/// full evidence trail; in short, the real quota wire codes (`insufficient_quota`/
/// `usage_not_included`) never reach `sig.error_code` on this codebase's actual wire path (they
/// arrive inside a `response.failed` frame that's reframed as SSE and passed through to the client,
/// never becoming a `WatchdogError::Upstream(_)`), so a code-keyed quota branch here would be dead
/// code from day one. `usage_refresh.rs`'s poller is the sole, authoritative owner of the durable
/// `quota_exceeded` status. `failure_routing.rs` carries two regression tests proving a quota-shaped
/// code that DOES somehow reach `error_code` still falls through to the ordinary status-keyed
/// bucketing below (never to `record_quota_exceeded`).
async fn record_failure(state: &AppState, id: &AccountId, err: &WatchdogError, now: i64) {
    let WatchdogError::Upstream(signal) = err else {
        return;
    };
    if let Some(sig) = signal {
        if let Some(code) = &sig.error_code {
            if let Some(status) = classify_failure(code).status() {
                let _ = state
                    .store
                    .accounts()
                    .update_status(id.as_str(), status)
                    .await;
                return;
            }
        }
    }
    let transition = match signal {
        Some(sig) if sig.status == 429 => {
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
        None => state.runtime.record_transient_error(id, now), // transport error / mid-stream drop.
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
    /// The account selected to serve (or attempted for) this request, when selection got that far.
    account_id: Option<String>,
    /// The requested (native path) or resolved target (translated/aliased path) model string.
    model: Option<String>,
    /// `reasoning.effort` for this request, when known.
    reasoning_effort: Option<String>,
}

fn stream_response(stream: ResponseStream) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .body(Body::from_stream(stream))
        .expect("valid response")
}

/// Persist a request-outcome row off the response path: fire-and-forget (a detached task, mirroring
/// codex-lb's pattern), so a slow or failing DB write never delays or fails the client's request.
/// The row is content-free by construction — it comes from `RequestLog::record`, the same audited
/// field set the tracing event carries (see `crate::observability`). `repo` is taken by value (it
/// owns a cheap pool clone) so the caller can build it before `state` is consumed by the handler.
///
/// D17 Task 3: promoted `pub(crate)` (from private) so `crate::control`'s handlers persist their
/// content-free control-endpoint log rows through this SAME fire-and-forget funnel, rather than a
/// second, parallel `tokio::spawn` write path.
pub(crate) fn spawn_persist_request_log(repo: RequestLogRepo, record: RequestLogRecord) {
    tokio::spawn(async move {
        if let Err(e) = repo.insert(&record).await {
            tracing::warn!(
                target: "polyflare_server::request",
                error = %e,
                "request_log persist failed"
            );
        }
    });
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
            return Err(account_unavailable());
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
                    // `update_tokens` is an idempotent UPDATE, so retrying is safe. On FINAL failure
                    // the refresh still succeeded and `new` is valid in-memory for THIS request —
                    // serve it rather than 5xx — but the stored token is now stale (a later refresh
                    // will need re-auth); log loudly (content-safe: no token material).
                    for attempt in 1..=PERSIST_MAX_ATTEMPTS {
                        match repo
                            .update_tokens(picked.as_str(), &new, &state.cipher, now)
                            .await
                        {
                            Ok(()) => break, // success bumps the store generation, invalidating caches
                            Err(e) if attempt < PERSIST_MAX_ATTEMPTS => {
                                tracing::warn!(
                                    attempt,
                                    error = %e,
                                    "persist of refreshed tokens failed; retrying"
                                );
                                tokio::time::sleep(PERSIST_RETRY_BACKOFF).await;
                            }
                            Err(e) => tracing::error!(
                                error = %e,
                                "failed to persist refreshed tokens after {PERSIST_MAX_ATTEMPTS} \
                                 attempts; stored refresh token is now stale — account will need re-auth"
                            ),
                        }
                    }
                    tokens = new;
                }
                Err(OAuthError::Endpoint {
                    code: Some(code), ..
                }) => {
                    if let Some(status) = classify_failure(&code).status() {
                        let _ = repo.update_status(picked.as_str(), status).await;
                    }
                    return Err(account_unavailable());
                }
                Err(OAuthError::Endpoint { code: None, .. }) | Err(OAuthError::MalformedJwt(_)) => {
                    let _ = repo.update_status(picked.as_str(), "reauth_required").await;
                    return Err(account_unavailable());
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
        },
        provider,
    ))
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
        Err(r) => return Some(r),
    };
    let health_id = fresh.clone(); // `fresh` is moved into the executor below.
                                   // C9 Task 2: a real upstream attempt on `fresh` — same lease treatment as every other
                                   // streaming selection site (see `execute_recovery_tracked`'s call below, which is
                                   // `execute_recovery`'s exact behavior plus this one added, never-read `in_flight` capability —
                                   // see `execute_recovery`'s doc for why its own signature stays untouched).
    let in_flight = state
        .runtime
        .acquire_in_flight(&fresh, now, &state.lease_metrics);
    let response = match execute_recovery_tracked(
        state.executor_for(provider).as_ref(),
        state.continuity.clone(),
        req,
        &account,
        fresh,
        ctx,
        session_key,
        state.runtime.clone(),
        state.stream_idle_timeout,
        CommitWitness::new(),
        Some(in_flight),
    )
    .await
    {
        Ok(stream) => stream_response(stream),
        Err(e) => {
            record_failure(state, &health_id, &e, unix_now()).await;
            (StatusCode::BAD_GATEWAY, "upstream error").into_response()
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
) -> ResponseStream {
    Box::pin(async_stream::stream! {
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
        let jitter_ms = wake_jitter_offset_ms(&request_key, state.wake_jitter_ms);
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
                yield starvation::keepalive_item();
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
            yield Ok(starvation::in_band_error_frame(starvation::StarvationOutcome::BudgetExceeded));
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
                yield Ok(starvation::in_band_error_frame(starvation::StarvationOutcome::StillNothing));
                return;
            }
        };
        let mut fresh_snapshots =
            filter_by_provider_and_pool(&fresh_snapshots, pool_provider, pool.as_deref());
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
                yield Ok(starvation::in_band_error_frame(starvation::StarvationOutcome::StillNothing));
                return;
            }
        };

        state.runtime.record_selected(&fresh, fresh_now);
        let (account, provider) = match resolve_core_account(&state, &fresh, fresh_now).await {
            Ok(a) => a,
            Err(_) => {
                emit_starvation_signal(
                    &state,
                    &wait_target,
                    wait_started,
                    starvation::StarvationOutcome::ExecutorError.code(),
                    None,
                    jitter_ms,
                );
                yield Ok(starvation::in_band_error_frame(starvation::StarvationOutcome::ExecutorError));
                return;
            }
        };
        let health_id = fresh.clone(); // `fresh` is moved into the executor below.
        // C9 Task 2: the Layer-2 wait's actual served attempt is a real upstream request on
        // `fresh` — same lease treatment as every other streaming selection site.
        let in_flight = state.runtime.acquire_in_flight(&fresh, fresh_now, &state.lease_metrics);
        match execute_recovery_tracked(
            state.executor_for(provider).as_ref(),
            state.continuity.clone(),
            req,
            &account,
            fresh,
            ctx,
            session_key,
            state.runtime.clone(),
            state.stream_idle_timeout,
            CommitWitness::new(),
            Some(in_flight),
        )
        .await
        {
            Ok(mut real_stream) => {
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
                while let Some(item) = real_stream.next().await {
                    yield item;
                }
            }
            Err(e) => {
                record_failure(&state, &health_id, &e, unix_now()).await;
                emit_starvation_signal(
                    &state,
                    &wait_target,
                    wait_started,
                    starvation::StarvationOutcome::ExecutorError.code(),
                    None,
                    jitter_ms,
                );
                yield Ok(starvation::in_band_error_frame(starvation::StarvationOutcome::ExecutorError));
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
        Err(r) => return r,
    };
    let health_id = fresh.clone(); // `fresh` is moved into the executor below.
                                   // TA6(b) Task 3: captured BEFORE `session_key` moves into `execute_recovery` below — the stamp
                                   // (on success) is what makes the NEXT turn on this session pre-filter from the start instead
                                   // of paying the reject-and-move cost again.
    let session_key_for_stamp = session_key.clone();
    // C9 Task 2: the cyber-reroute's move onto the capability-holding account is a real upstream
    // attempt on `fresh` — same lease treatment as every other streaming selection site.
    let in_flight = state
        .runtime
        .acquire_in_flight(&fresh, now, &state.lease_metrics);
    match execute_recovery_tracked(
        state.executor_for(provider).as_ref(),
        state.continuity.clone(),
        anchorless_req,
        &account,
        fresh,
        ctx,
        session_key,
        state.runtime.clone(),
        state.stream_idle_timeout,
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
            (StatusCode::BAD_GATEWAY, "upstream error").into_response()
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
            return (StatusCode::BAD_GATEWAY, "upstream error").into_response();
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
                    (StatusCode::BAD_GATEWAY, "upstream error").into_response()
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
            Err(r) => return r,
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
        let in_flight = state
            .runtime
            .acquire_in_flight(&fresh, now, &state.lease_metrics);
        match execute_recovery_tracked(
            state.executor_for(provider).as_ref(),
            state.continuity.clone(),
            resend_req.clone(),
            &account,
            fresh,
            ctx.clone(),
            session_key.clone(),
            state.runtime.clone(),
            state.stream_idle_timeout,
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

/// Shared `/responses` route: thin timing + content-safe logging wrapper around
/// [`responses_handler_impl`], parameterized by the optional account-pool slug. See
/// `crate::observability` for the content-safety constraint on what may be logged.
async fn responses_route(
    state: Arc<AppState>,
    pool: Option<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let start = Instant::now();
    maybe_capture_fingerprint(&state, "POST", "/responses", &headers);
    // Build the log repo BEFORE `state` moves into the impl (it owns a cheap pool clone).
    let log_repo = state.store.request_log();
    // Same reason: `state` moves into the impl below, so grab the log-bus handle first.
    let log_bus = state.log_bus.clone();
    // C11b Task 2: same reason — grab the content-free `upstream_requests` counter handle before
    // `state` moves into the impl below.
    let upstream_request_metrics = state.upstream_request_metrics.clone();
    let (response, outcome) = responses_handler_impl(state, pool.as_deref(), headers, body).await;
    let log = RequestLog {
        method: "POST",
        path: "/responses",
        // M4a: `/responses` may only ever route to a Codex-provider account — see this fn's
        // `filter_by_provider(&snapshots, Provider::Codex)` call below. The provider is
        // structurally fixed regardless of which branch produced the response (including the
        // early-exit error paths, which never resolve an account at all).
        provider: Provider::Codex,
        aliased: false,
        status: response.status(),
        duration_ms: start.elapsed().as_millis() as u64,
        account_id: outcome.account_id,
        model: outcome.model,
        reasoning_effort: outcome.reasoning_effort,
        // Not yet known at this chokepoint (SPEC-M4a has no per-account subscription-tier read
        // wired here today).
        service_tier: None,
        transport: Some("http".to_string()),
        // TODO(follow-up): populate ttft/tokens from the stream observer.
        ttft_ms: None,
        total_tokens: None,
        cached_tokens: None,
    };
    log.emit();
    log_bus.publish(log.to_log_event());
    // C11b Task 2: the content-free `upstream_requests` counter, keyed by the SAME
    // `(account_id, status)` pair `log` already carries — bumped exactly once per client request
    // (the final outcome only; per-attempt retries are `FailoverMetrics`, never double-counted
    // here).
    upstream_request_metrics.record(log.account_id.as_deref(), log.status.as_u16());
    spawn_persist_request_log(log_repo, log.record(unix_now()));
    response
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
) -> (Response, RouteOutcome) {
    let max_attempts = state.max_account_attempts;
    let starvation_wait_budget = state.starvation_wait_budget;
    let starvation_heartbeat = state.starvation_heartbeat;
    responses_handler_impl_with_max_attempts(
        state,
        pool,
        headers,
        raw,
        max_attempts,
        starvation_wait_budget,
        starvation_heartbeat,
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
        max_attempts,
        starvation::DEFAULT_WAIT_BUDGET,
        starvation::DEFAULT_HEARTBEAT,
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
        max_attempts,
        starvation_budget,
        starvation_heartbeat,
    )
    .await
    .0
}

async fn responses_handler_impl_with_max_attempts(
    state: Arc<AppState>,
    pool: Option<&str>,
    headers: HeaderMap,
    raw: Bytes,
    max_attempts: u32,
    starvation_budget: Duration,
    starvation_heartbeat: Duration,
) -> (Response, RouteOutcome) {
    // Parse ONCE — but only the scalars + the `input` SHAPE, NOT the deep conversation tree. The
    // wire bytes are forwarded verbatim (see `PreparedRequest::raw_body`), so `body` stays `None`
    // here; everything the request path needs (model, tier, continuity ctx, input count) comes off
    // this cheap parse. Only a MALFORMED body (invalid JSON, or a non-object root) 400s here;
    // semantic/schema checks (field types, numeric ranges, duplicate keys) are deferred to upstream,
    // the schema authority — a genuine pass-through, matching the old full-`Value` parse's tolerance.
    let facts = match parse_inbound(&headers, &raw) {
        Some(f) => f,
        None => {
            return (
                (StatusCode::BAD_REQUEST, "invalid JSON body").into_response(),
                RouteOutcome::default(),
            )
        }
    };
    let model = facts.model;
    let now = unix_now();
    let tier = tier_from_effort(facts.effort.as_deref());
    // Model + effort are known from the parse itself, regardless of what happens next; account_id
    // is filled in once (if ever) a `RouteDecision` actually selects one below.
    let mut outcome = RouteOutcome {
        account_id: None,
        model: Some(model.clone()),
        reasoning_effort: facts.effort.clone(),
    };

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
        inflight_penalty_pct: state.inflight_penalty_pct,
    };
    let session_key = prepared.directive.session_key.clone();

    // C5: ownership pre-filter.
    let response =
        match apply_ownership(&prepared.directive, &snapshots, selector.as_ref(), &sel_ctx) {
            RouteDecision::Route(id) => {
                // Stamp last_selected_at NOW (not at completion) so concurrent picks in a burst see this
                // one — the round_robin + capacity_weighted tiebreaks read it.
                state.runtime.record_selected(&id, now);
                outcome.account_id = Some(id.as_str().to_string());
                let (account, provider) = match resolve_core_account(&state, &id, now).await {
                    Ok(a) => a,
                    Err(r) => return (r, outcome),
                };
                let health_id = id.clone(); // `id` is moved into the executor below.
                                            // C9 Task 2: the in-flight lease for this FIRST attempt on `id`. On success it
                                            // rides inside the returned stream; on any `Err` below (including the
                                            // `CapabilityRejection`/general-failure arms) it releases when
                                            // `execute_with_watchdog_tracked`'s own frame ends — strictly before
                                            // `reroute_cyber_rejection`/`run_failover_loop` (each of which acquires its OWN
                                            // fresh lease for whatever account it tries next) ever runs.
                let in_flight = state
                    .runtime
                    .acquire_in_flight(&id, now, &state.lease_metrics);
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
                let commit = CommitWitness::new();
                match execute_with_watchdog_tracked(
                    state.executor_for(provider).as_ref(),
                    state.continuity.clone(),
                    prepared,
                    &account,
                    id,
                    ctx.clone(),
                    state.runtime.clone(),
                    state.stream_idle_timeout,
                    commit.clone(),
                    Some(in_flight),
                )
                .await
                {
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
                            None => (StatusCode::BAD_GATEWAY, "upstream error").into_response(),
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
                                    )
                                });
                                return (resp.unwrap_or_else(no_eligible), outcome);
                            }
                        };
                        state.runtime.record_selected(&fresh, now);
                        outcome.account_id = Some(fresh.as_str().to_string());
                        let (account, provider) =
                            match resolve_core_account(&state, &fresh, now).await {
                                Ok(a) => a,
                                Err(r) => return (r, outcome),
                            };
                        let health_id = fresh.clone(); // `fresh` is moved into the executor below.
                                                       // C9 Task 2: the owner-ineligible recovery's reselected attempt on `fresh`
                                                       // is a real upstream request — same lease treatment as every other
                                                       // streaming selection site.
                        let in_flight =
                            state
                                .runtime
                                .acquire_in_flight(&fresh, now, &state.lease_metrics);
                        match execute_recovery_tracked(
                            state.executor_for(provider).as_ref(),
                            state.continuity.clone(),
                            anchorless_req,
                            &account,
                            fresh,
                            ctx,
                            session_key,
                            state.runtime.clone(),
                            state.stream_idle_timeout,
                            CommitWitness::new(),
                            Some(in_flight),
                        )
                        .await
                        {
                            Ok(stream) => stream_response(stream),
                            Err(e) => {
                                record_failure(&state, &health_id, &e, unix_now()).await;
                                (StatusCode::BAD_GATEWAY, "upstream error").into_response()
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
                                        Err(r) => return (r, outcome),
                                    };
                                let fallback = Prepared {
                                    req: prepared.req,
                                    directive: ContinuityDirective {
                                        pin_account: None,
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
                                let in_flight = state.runtime.acquire_in_flight(
                                    &fresh,
                                    now,
                                    &state.lease_metrics,
                                );
                                match execute_with_watchdog_tracked(
                                    state.executor_for(provider).as_ref(),
                                    state.continuity.clone(),
                                    fallback,
                                    &account,
                                    fresh,
                                    ctx,
                                    state.runtime.clone(),
                                    state.stream_idle_timeout,
                                    CommitWitness::new(),
                                    Some(in_flight),
                                )
                                .await
                                {
                                    Ok(stream) => stream_response(stream),
                                    Err(e) => {
                                        record_failure(&state, &health_id, &e, unix_now()).await;
                                        (StatusCode::BAD_GATEWAY, "upstream error").into_response()
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
                        )
                    })
                    .unwrap_or_else(no_eligible)
            }
        };
    (response, outcome)
}

/// The `/v1/messages` ingress entrypoint. A client `model` string that `alias::lookup_alias` maps
/// to a Codex target (SPEC-M4 §3.6 — the M4b headline feature) takes the cross-provider translated
/// path; everything else (no alias, or an alias whose target is itself Anthropic) takes the native
/// same-format path, unchanged. Also a thin timing + content-safe logging wrapper (mirrors
/// `responses_handler` above) — see `crate::observability` for the content-safety constraint.
/// The bare `/v1/messages` ingress entrypoint: selects over ALL accounts of the resolved provider
/// (no pool filter).
pub async fn messages_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response {
    messages_route(state, None, headers, body).await
}

/// The pooled `/{pool}/v1/messages` ingress entrypoint: selects only over the resolved provider's
/// accounts tagged with the `{pool}` slug (see `filter_by_pool`).
pub async fn pooled_messages_handler(
    State(state): State<Arc<AppState>>,
    Path(pool): Path<String>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response {
    messages_route(state, Some(pool), headers, body).await
}

async fn messages_route(
    state: Arc<AppState>,
    pool: Option<String>,
    headers: HeaderMap,
    body: serde_json::Value,
) -> Response {
    let start = Instant::now();
    maybe_capture_fingerprint(&state, "POST", "/v1/messages", &headers);
    // Build the log repo BEFORE `state` moves into a sub-handler (it owns a cheap pool clone).
    let log_repo = state.store.request_log();
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

    // Resolved once, up front: which provider this request structurally targets, and whether
    // that's via a model alias — both are decided entirely by `lookup_alias`, independent of
    // whether the downstream relay itself succeeds.
    let alias = alias::lookup_alias(&model);
    let aliased_to_codex = matches!(&alias, Some(a) if a.target_provider == Provider::Codex);
    let provider = if aliased_to_codex {
        Provider::Codex
    } else {
        Provider::Anthropic
    };

    let (response, outcome) = match alias {
        Some(model_alias) if model_alias.target_provider == Provider::Codex => {
            messages_handler_codex_aliased(state, pool.as_deref(), body, model_alias).await
        }
        _ => messages_handler_native(state, pool.as_deref(), body, model).await,
    };

    let log = RequestLog {
        method: "POST",
        path: "/v1/messages",
        provider,
        aliased: aliased_to_codex,
        status: response.status(),
        duration_ms: start.elapsed().as_millis() as u64,
        account_id: outcome.account_id,
        model: outcome.model,
        reasoning_effort: outcome.reasoning_effort,
        // Not yet known at this chokepoint.
        service_tier: None,
        transport: Some("http".to_string()),
        // TODO(follow-up): populate ttft/tokens from the stream observer.
        ttft_ms: None,
        total_tokens: None,
        cached_tokens: None,
    };
    log.emit();
    log_bus.publish(log.to_log_event());
    // C11b Task 2: the content-free `upstream_requests` counter, keyed by the SAME
    // `(account_id, status)` pair `log` already carries — bumped exactly once per client request
    // (the final outcome only; per-attempt retries are `FailoverMetrics`, never double-counted
    // here).
    upstream_request_metrics.record(log.account_id.as_deref(), log.status.as_u16());
    spawn_persist_request_log(log_repo, log.record(unix_now()));

    response
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
) -> (Response, RouteOutcome) {
    let now = unix_now();
    // The client-requested model is known up front, regardless of what happens next; the native
    // Anthropic path carries no Codex-style reasoning-effort concept, so that field stays `None`.
    let mut outcome = RouteOutcome {
        account_id: None,
        model: Some(model.clone()),
        reasoning_effort: None,
    };
    // Native Anthropic path: the AnthropicExecutor does not use `forward_headers` (that field is
    // the Codex egress identity set), so there is nothing to forward here.
    let req = PreparedRequest {
        // No raw pass-through on the Anthropic wire path ⇒ the materialized body is what's sent.
        body: Some(body),
        model,
        forward_headers: vec![],
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
    // M4a has no cross-format translator (that's M4b): `/v1/messages` may only ever pick an
    // Anthropic-provider account — the exact mirror of `/responses`'s Codex-only filter above.
    let mut snapshots = filter_by_provider_and_pool(&snapshots, Provider::Anthropic, pool);
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
        inflight_penalty_pct: state.inflight_penalty_pct,
    };
    let picked = match selector.pick(&snapshots, &sel_ctx) {
        Some(id) => id,
        None => return (no_eligible(), outcome),
    };
    state.runtime.record_selected(&picked, now);
    outcome.account_id = Some(picked.as_str().to_string());
    let (account, provider) = match resolve_core_account(&state, &picked, now).await {
        Ok(a) => a,
        Err(r) => return (r, outcome),
    };

    let health_id = picked.clone(); // moved into the executor below.
                                    // C9 Task 2: the native `/v1/messages` streaming selection site — same lease treatment as
                                    // `/responses`'s Route arm.
    let in_flight = state
        .runtime
        .acquire_in_flight(&picked, now, &state.lease_metrics);
    let response = match execute_with_watchdog_tracked(
        state.executor_for(provider).as_ref(),
        Arc::new(NoopContinuity) as Arc<dyn Continuity>,
        prepared,
        &account,
        picked,
        ctx,
        state.runtime.clone(),
        state.stream_idle_timeout,
        CommitWitness::new(),
        Some(in_flight),
    )
    .await
    {
        Ok(stream) => stream_response(stream),
        Err(e) => {
            record_failure(&state, &health_id, &e, unix_now()).await;
            (StatusCode::BAD_GATEWAY, "upstream error").into_response()
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
    // what happens next.
    let mut outcome = RouteOutcome {
        account_id: None,
        model: Some(model_alias.target_model.clone()),
        reasoning_effort: model_alias.reasoning_effort.clone(),
    };
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

    // Translated path: there is no real Codex client to forward, so SYNTHESIZE codex-rs's identity
    // headers (see `synthesize_codex_forward_headers`). Mirrors codex-lb's forward-native /
    // synthesize-non-native split. The User-Agent's codex version is resolved live (GitHub/npm,
    // cached) so it tracks the real fleet instead of a stale constant; `cached_or_fallback` is a
    // sync, zero-I/O read warmed out-of-band by the background refresh task.
    let forward_headers = synthesize_codex_forward_headers(
        &translated_body,
        &state.codex_version.cached_or_fallback(),
    );
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
        inflight_penalty_pct: state.inflight_penalty_pct,
    };
    let picked = match selector.pick(&snapshots, &sel_ctx) {
        Some(id) => id,
        None => return (no_eligible(), outcome),
    };
    state.runtime.record_selected(&picked, now);
    outcome.account_id = Some(picked.as_str().to_string());
    let (account, provider) = match resolve_core_account(&state, &picked, now).await {
        Ok(a) => a,
        Err(r) => return (r, outcome),
    };

    let health_id = picked.clone(); // moved into the executor below.
                                    // C9 Task 2: the Codex-aliased `/v1/messages` streaming selection site — same lease treatment
                                    // as `/responses`'s Route arm. `wrap_translating_stream` below just wraps the returned
                                    // `ResponseStream` (the `ObservingStream` carrying `_in_flight`) in another stream layer that
                                    // owns it by value — the lease's lifetime is unaffected by the translation wrapper.
    let in_flight = state
        .runtime
        .acquire_in_flight(&picked, now, &state.lease_metrics);
    let response = match execute_with_watchdog_tracked(
        state.executor_for(provider).as_ref(),
        Arc::new(NoopContinuity) as Arc<dyn Continuity>,
        prepared,
        &account,
        picked,
        ctx,
        state.runtime.clone(),
        state.stream_idle_timeout,
        CommitWitness::new(),
        Some(in_flight),
    )
    .await
    {
        Ok(stream) => {
            let translated_stream =
                wrap_translating_stream(stream, Box::new(translator) as Box<dyn Translator>);
            stream_response(translated_stream)
        }
        Err(e) => {
            record_failure(&state, &health_id, &e, unix_now()).await;
            (StatusCode::BAD_GATEWAY, "upstream error").into_response()
        }
    };
    (response, outcome)
}

#[cfg(test)]
mod tests {
    use super::derive_alias_prompt_cache_key;
    use serde_json::json;

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
}
