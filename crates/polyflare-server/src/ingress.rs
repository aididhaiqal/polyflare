//! Ingress: derive continuity ctx → prepare → ownership pre-filter → execute under the watchdog →
//! relay. Client-facing errors carry generic bodies (never a token, URL, or internal Display).

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::{Body, Bytes};
use axum::extract::{Json, Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use polyflare_anthropic::AnthropicToResponses;
use polyflare_codex::oauth::{classify_failure, should_refresh, token_exp, OAuthError};
use polyflare_core::{
    Account, AccountId, AccountSnapshot, Continuity, ContinuityDirective, NoopContinuity, Prepared,
    PreparedRequest, Provider, RecoveryPlan, RequestCtx, ResponseStream, SelectionCtx, Selector,
    SessionKey, Tier, Translator,
};
use polyflare_store::{PlainTokens, RequestLogRecord, RequestLogRepo};

use crate::alias::{self, ModelAlias};
use crate::app::AppState;
use crate::fingerprint_capture::{append_fingerprint_capture, capture_request_fingerprint};
use crate::observability::RequestLog;
use crate::session_key::parse_inbound;
use crate::snapshot::filter_by_provider_and_pool;
use crate::translate_stream::wrap_translating_stream;
use crate::watchdog::{
    apply_ownership, execute_recovery, execute_with_watchdog, signal_client_stream, RouteDecision,
    WatchdogError,
};

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Bounded retries for the post-refresh token persist (the one write whose loss kills an account —
/// see the call site). `busy_timeout` is the first line of defense; this is the backstop.
const PERSIST_MAX_ATTEMPTS: u32 = 3;
/// Fixed backoff between persist retries (small — the write is on the hot lock).
const PERSIST_RETRY_BACKOFF: Duration = Duration::from_millis(100);

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

fn internal_error() -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
}

fn no_eligible() -> Response {
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
                let _ = state.store.accounts().update_status(id.as_str(), status).await;
                return;
            }
        }
    }
    match signal {
        Some(sig) if sig.status == 429 => state.runtime.record_rate_limit(id, sig.retry_after, now),
        // 5xx (server error), 401/403 (bad credential / account-scoped auth), 408 (request timeout):
        // an ACCOUNT-health problem — bump the error count so a repeat offender hits the backoff gate.
        Some(sig) if (500..=599).contains(&sig.status) || matches!(sig.status, 401 | 403 | 408) => {
            state.runtime.record_transient_error(id, now)
        }
        Some(_) => {} // other 4xx (400/404/422/…): request-level, not account-health.
        None => state.runtime.record_transient_error(id, now), // transport error / mid-stream drop.
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
fn forward_headers_from_inbound(headers: &HeaderMap) -> Vec<(String, String)> {
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
fn spawn_persist_request_log(repo: RequestLogRepo, record: RequestLogRecord) {
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
async fn resolve_core_account(
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
    match execute_recovery(
        state.executor_for(provider).as_ref(),
        state.continuity.clone(),
        anchorless_req,
        &account,
        fresh,
        ctx,
        session_key,
        state.runtime.clone(),
    )
    .await
    {
        Ok(stream) => stream_response(stream),
        Err(e) => {
            record_failure(state, &health_id, &e, unix_now()).await;
            (StatusCode::BAD_GATEWAY, "upstream error").into_response()
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
    spawn_persist_request_log(log_repo, log.record(unix_now()));
    response
}

async fn responses_handler_impl(
    state: Arc<AppState>,
    pool: Option<&str>,
    headers: HeaderMap,
    raw: Bytes,
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
    let sel_ctx = SelectionCtx {
        now,
        require_security_work_authorized: false,
        rng_seed: None,
        session_id: ctx.session_id.clone(),
        tier,
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
                // TA6(b) Task 2: capture the recovery plan + a `ctx` clone BEFORE `prepared`/`ctx`
                // move into the executor below, so a `CapabilityRejection` can trigger the cyber
                // reselect+resend (`reroute_cyber_rejection`) without re-preparing the request.
                let recovery_for_cyber = prepared.directive.recovery.clone();
                let ctx_for_cyber = ctx.clone();
                match execute_with_watchdog(
                    state.executor_for(provider).as_ref(),
                    state.continuity.clone(),
                    prepared,
                    &account,
                    id,
                    ctx,
                    state.runtime.clone(),
                )
                .await
                {
                    Ok(stream) => stream_response(stream),
                    Err(WatchdogError::CapabilityRejection { .. }) => {
                        // NOT an account-health signal (see `record_failure`'s doc): a capability
                        // rejection says nothing about the owner's health, so no writeback here.
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
                        (StatusCode::BAD_GATEWAY, "upstream error").into_response()
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
                            None => return (no_eligible(), outcome),
                        };
                        state.runtime.record_selected(&fresh, now);
                        outcome.account_id = Some(fresh.as_str().to_string());
                        let (account, provider) =
                            match resolve_core_account(&state, &fresh, now).await {
                                Ok(a) => a,
                                Err(r) => return (r, outcome),
                            };
                        let health_id = fresh.clone(); // `fresh` is moved into the executor below.
                        match execute_recovery(
                            state.executor_for(provider).as_ref(),
                            state.continuity.clone(),
                            anchorless_req,
                            &account,
                            fresh,
                            ctx,
                            session_key,
                            state.runtime.clone(),
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
                                    },
                                };
                                let health_id = fresh.clone(); // moved into the executor below.
                                match execute_with_watchdog(
                                    state.executor_for(provider).as_ref(),
                                    state.continuity.clone(),
                                    fallback,
                                    &account,
                                    fresh,
                                    ctx,
                                    state.runtime.clone(),
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
                            None => no_eligible(),
                        }
                    }
                }
            }
            RouteDecision::NoEligibleAccount => no_eligible(),
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
    let response = match execute_with_watchdog(
        state.executor_for(provider).as_ref(),
        Arc::new(NoopContinuity) as Arc<dyn Continuity>,
        prepared,
        &account,
        picked,
        ctx,
        state.runtime.clone(),
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
    let response = match execute_with_watchdog(
        state.executor_for(provider).as_ref(),
        Arc::new(NoopContinuity) as Arc<dyn Continuity>,
        prepared,
        &account,
        picked,
        ctx,
        state.runtime.clone(),
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
}
