//! WS-downstream relay (client-facing WebSocket transport), gated by `POLYFLARE_WS_DOWNSTREAM`
//! (`crate::app::AppState::ws_downstream`, default off). When on, the codex CLI's WS-handshake
//! `GET /responses` (+ `/{pool}/responses`) routes here instead of to
//! `crate::ingress::websocket_fallback_handler`'s `426`, and PolyFlare ACCEPTS the upgrade.
//!
//! **Handshake boundary:** [`responses_ws_handler`] resolves the conversation's pinned owner
//! ([`resolve_owner`]) and completes that account's upstream Codex handshake
//! ([`dial_owner_upstream`]) BEFORE accepting the downstream upgrade. Initial `401` gets one
//! synchronized same-account refresh; non-upgrade failures remain actionable HTTP responses; and
//! the four Codex-consumed upstream upgrade headers are copied onto the downstream `101`.
//! [`relay`] then drives the bidirectional verbatim pump ([`pump::run_pump`]) for the connection's
//! life. Every `response.completed` frame's id is sniffed content-free
//! ([`sniff::sniff_completed_id`]) and fed into the SAME continuity engine the HTTP path uses
//! (`Continuity::observe` — see `polyflare_core::TurnOutcome::Completed`), so a later turn — HTTP or
//! WS, same or reconnected socket — resolves the same owner.
//!
//! **Phase 3 Task 4: the exhaustion-move.** On a durable upstream error (`pump`'s
//! `UpstreamSignal::Error`), the pump calls back into [`relay`]'s `on_upstream_error` closure, which
//! benches the failed account with the EXACT same policy the HTTP path uses
//! (`crate::ingress::bench_account_for_failure` — reused, not reinvented), re-resolves the owner
//! (`resolve_owner` overlays the fresh cooldown, so the just-benched account is skipped and the
//! selector falls through to the next eligible one), and re-dials whatever it picked
//! ([`redial::redial_upstream`]) — landing back on the SAME account (a retry) or a genuinely
//! DIFFERENT one (a move). The pump then continues on whatever it got back; a later
//! `response.completed` re-homes ownership naturally via the existing `on_completed_id` callback,
//! which the pump always calls with its CURRENT account (never the stale, pre-move one).
//!
//! **Relay-catalog-fixes Task 3: transient-429 retries in place.** Not every `UpstreamSignal::Error`
//! is durable exhaustion — a 429 whose `Retry-After` is short ([`TRANSIENT_RETRY_MAX_SECS`] or
//! under) is a transient throttle the SAME account will clear shortly. `on_upstream_error` checks
//! this FIRST: it waits out the retry-after and redials the SAME account (skipping
//! `bench_account_for_failure` and `resolve_owner` entirely), preserving the conversation's prompt
//! cache instead of discarding it on a bench+move. Only a durable/long/absent retry-after falls
//! through to the bench -> re-resolve -> move path described above, unchanged.
//!
//! **Phase 2/3 Task 5: reconnect/move/anchor-signal counters.** [`relay`] threads
//! `state.relay_metrics` (`crate::observability::RelayMetrics`) into [`pump::run_pump`], which bumps
//! it at its same-account-reconnect, cross-account-move, anchor-miss-client-resend, and terminal
//! same-account-anchor-miss decision points—content-free fixed labels only. See
//! `pump::run_pump`'s own doc comment for the exact bump sites.
//!
//! **Content-free (inviolable, `design §8`):** no frame body is ever logged or persisted anywhere in
//! this module or its submodules. Body inspection is restricted to bounded routing/error fields,
//! `response.id`, event discriminants, and numeric usage/timing in [`telemetry`]. Conversation
//! content is never returned from those observers or written to any sink.

use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::extract::ws::{WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use polyflare_codex::WsConn;
use polyflare_core::{Account, AccountId, FailureSignal, RequestCtx, SessionKey, TurnOutcome};

use crate::app::AppState;

mod owner;
mod pump;
mod redial;
mod session;
mod signal;
mod sniff;
mod telemetry;

pub(crate) use owner::{dial_owner_upstream, resolve_owner};
use redial::{redial_upstream_with_models_etag, RedialOutcome};
pub(crate) use session::ws_session_key;

/// Between-turns upstream idle policy for the relay pump (honest-liveness work, 2026-07-24).
///
/// Between turns the upstream socket is parked, not stalled — the mid-turn 290s read deadline
/// must not apply (poisoning a healthy idle socket kills its connection-scoped `store:false`
/// anchor and forces the next anchored delta into a client-visible resend round-trip). Instead the
/// pump waits up to `idle_budget`, keeping intermediaries convinced the socket is alive via
/// keepalive `Ping`s every `ping_interval` (empty payload, content-free; also a fast dead-peer
/// detector — a failed ping send surfaces immediately instead of at the next turn). When the
/// budget elapses, or the upstream drops while parked, the pump closes BOTH legs so codex's own
/// socket-liveness model (`is_closed()` ⇒ wipe anchor ledger ⇒ full resend) sees the truth and
/// reconnects silently — never a lied-alive downstream hiding a dead anchor.
///
/// Resolved ONCE at startup (`POLYFLARE_WS_IDLE_PING_SECS`, 0 = no pings;
/// `POLYFLARE_WS_IDLE_BUDGET_SECS`) — never read per-request.
#[derive(Clone, Copy, Debug)]
pub struct WsRelayIdlePolicy {
    /// Keepalive ping cadence while parked between turns; `None` sends no pings (the socket then
    /// usually dies to intermediary idle-reaping and the honest close fires on detection).
    pub ping_interval: Option<std::time::Duration>,
    /// How long a parked upstream is kept alive between turns before the relay deliberately lets
    /// the session go (honest close of both legs). Bounds "ping-pong forever": an abandoned TUI
    /// stops costing an upstream socket after this window; the user's return pays one native
    /// codex reconnect + full resend, exactly as if they had been connected directly.
    pub idle_budget: std::time::Duration,
}

impl Default for WsRelayIdlePolicy {
    fn default() -> Self {
        Self {
            ping_interval: Some(std::time::Duration::from_secs(30)),
            idle_budget: std::time::Duration::from_secs(1500),
        }
    }
}

/// The boundary (inclusive) a 429's `Retry-After` must fall at-or-under to be treated as
/// TRANSIENT — retried in place on the SAME account, waiting it out, rather than benched and
/// moved. Deliberately mirrors `runtime_state::RATE_LIMITED_MIN_COOLDOWN_SECS` (the floor
/// `bench_account_for_failure`'s cooldown clamps every rate-limit to): a `retry_after` at or
/// under that floor would clamp to the SAME effective cooldown as a move anyway, so treating it
/// as transient costs nothing extra durability-wise while avoiding the cache-losing move. A
/// longer/absent `retry_after` is durable and falls through to the existing bench -> resolve_owner
/// -> move path, unchanged.
///
/// Bound directly to the shared floor (not a duplicated literal) so the two can never drift.
const TRANSIENT_RETRY_MAX_SECS: i64 = crate::runtime_state::RATE_LIMITED_MIN_COOLDOWN_SECS;

/// Mirrors `control.rs`'s / `continuity.rs`'s own `unix_now` — a plain wall-clock read, no shared
/// helper exists at the crate root so each site that needs "now, in seconds" defines its own trivial
/// one rather than growing a needless cross-module dependency for a two-line function.
fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

async fn refresh_and_redial_unauthorized(
    state: &AppState,
    headers: &HeaderMap,
    account: &Account,
    relay_contract: &polyflare_codex::WsRelayContract,
    pool: Option<&str>,
) -> Option<(Account, WsConn)> {
    let account_id = AccountId::from(account.id.as_str());
    let refreshed = crate::ingress::force_refresh_after_unauthorized(
        state,
        &account_id,
        &account.bearer_token,
        unix_now(),
    )
    .await
    .ok()
    .flatten()?;
    match redial_for_scope(state, headers, &refreshed, relay_contract, pool).await {
        RedialOutcome::Connected(upstream) => Some((refreshed, *upstream)),
        RedialOutcome::Unauthorized | RedialOutcome::ContractDrift | RedialOutcome::Unavailable => {
            None
        }
    }
}

async fn redial_with_reactive_auth(
    state: &AppState,
    headers: &HeaderMap,
    account: Account,
    relay_contract: &polyflare_codex::WsRelayContract,
    pool: Option<&str>,
) -> Option<(Account, WsConn)> {
    match redial_for_scope(state, headers, &account, relay_contract, pool).await {
        RedialOutcome::Connected(upstream) => Some((account, *upstream)),
        RedialOutcome::Unauthorized => {
            refresh_and_redial_unauthorized(state, headers, &account, relay_contract, pool).await
        }
        RedialOutcome::ContractDrift | RedialOutcome::Unavailable => None,
    }
}

pub(crate) async fn redial_for_scope(
    state: &AppState,
    headers: &HeaderMap,
    account: &Account,
    relay_contract: &polyflare_codex::WsRelayContract,
    pool: Option<&str>,
) -> RedialOutcome {
    let current = match pool {
        Some(pool) => crate::catalog::pooled_models_etag(state, pool).await,
        None => crate::catalog::root_models_etag(state).await,
    };
    redial_upstream_with_models_etag(headers, account, relay_contract, Some(current)).await
}

/// Accepts the codex CLI's downstream WebSocket upgrade on `/responses` (routed here only when
/// `AppState::ws_downstream` is on — see `crate::app::build_app`). Returns `101 Switching
/// Protocols` only after the upstream socket is ready; otherwise returns the pre-upgrade failure.
pub async fn responses_ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    responses_ws_upgrade(ws, state, headers, None).await
}

pub async fn pooled_responses_ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
    Path(pool): Path<String>,
    headers: HeaderMap,
) -> Response {
    responses_ws_upgrade(ws, state, headers, Some(pool)).await
}

async fn responses_ws_upgrade(
    ws: WebSocketUpgrade,
    state: Arc<AppState>,
    headers: HeaderMap,
    pool: Option<String>,
) -> Response {
    // The conversation's content-free owner-lookup key is derivable from the handshake headers
    // ALONE (Phase-0: `session-id`/`thread-id`/`x-codex-window-id`) — computed here, before the
    // upgrade, so it moves into the post-upgrade future unchanged.
    let session_key = ws_session_key(&headers, pool.as_deref());
    let session_id = crate::session_key::session_id_from_headers(&headers);
    let catalog_etag = match pool.as_deref() {
        Some(pool) => crate::catalog::pooled_models_etag(&state, pool).await,
        None => crate::catalog::root_models_etag(&state).await,
    };
    let require_security_work_authorized = crate::config::pool_requires_capability(
        pool.as_deref(),
        crate::config::SECURITY_WORK_CAPABILITY,
    ) || headers
        .get(crate::config::CAPABILITY_HEADER)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.trim() == crate::config::SECURITY_WORK_CAPABILITY);
    let (account, ws_guard) = match resolve_owner(
        &state,
        &session_key,
        session_id.as_deref(),
        pool.as_deref(),
        require_security_work_authorized,
    )
    .await
    {
        Ok(account) => account,
        Err(error) => {
            if has_codex_visible_custom_models(&state).await {
                return crate::ingress::websocket_fallback_handler().await;
            }
            return relay_error_response(error);
        }
    };
    let rejected_access_token = account.bearer_token.clone();
    let first = dial_owner_upstream(&headers, &account).await;
    let (account, upstream) = match first {
        Ok(upstream) => (account, upstream),
        Err(error) if relay_error_status(&error) == Some(StatusCode::UNAUTHORIZED) => {
            let account_id = AccountId::from(account.id.as_str());
            match crate::ingress::force_refresh_after_unauthorized(
                &state,
                &account_id,
                &rejected_access_token,
                unix_now(),
            )
            .await
            {
                Ok(Some(refreshed_account)) => {
                    match dial_owner_upstream(&headers, &refreshed_account).await {
                        Ok(upstream) => (refreshed_account, upstream),
                        Err(error) => {
                            record_initial_dial_failure(&state, &refreshed_account, &error).await;
                            if has_codex_visible_custom_models(&state).await {
                                return crate::ingress::websocket_fallback_handler().await;
                            }
                            return relay_error_response(error);
                        }
                    }
                }
                // Preserve the actionable upstream 401 when OAuth itself had a transient failure.
                Ok(None) => {
                    record_initial_dial_failure(&state, &account, &error).await;
                    if has_codex_visible_custom_models(&state).await {
                        return crate::ingress::websocket_fallback_handler().await;
                    }
                    return relay_error_response(error);
                }
                Err(response) => return response,
            }
        }
        Err(error) => {
            record_initial_dial_failure(&state, &account, &error).await;
            if has_codex_visible_custom_models(&state).await {
                return crate::ingress::websocket_fallback_handler().await;
            }
            return relay_error_response(error);
        }
    };

    let upgrade_headers = upstream.upgrade_response_headers().to_vec();
    let relay_contract = upstream
        .relay_contract()
        .clone()
        .with_models_etag(catalog_etag.clone());
    let ws_pressure = Arc::new(Mutex::new(Some(ws_guard)));
    let routing_scope = RelayRoutingScope {
        pool,
        session_id,
        require_security_work_authorized,
    };
    let mut response = ws.on_upgrade(move |socket| {
        relay(
            socket,
            upstream,
            account,
            state,
            headers,
            session_key,
            routing_scope,
            ws_pressure,
            relay_contract,
        )
    });
    for (name, value) in upgrade_headers {
        if name.eq_ignore_ascii_case("x-models-etag") {
            continue;
        }
        if let (Ok(name), Ok(value)) = (
            axum::http::HeaderName::from_bytes(name.as_bytes()),
            axum::http::HeaderValue::from_str(&value),
        ) {
            response.headers_mut().insert(name, value);
        }
    }
    if let Some(etag) = catalog_etag.and_then(|value| value.parse().ok()) {
        response.headers_mut().insert("x-models-etag", etag);
    }
    response
}

async fn has_codex_visible_custom_models(state: &AppState) -> bool {
    state
        .store
        .providers()
        .list_enabled_models()
        .await
        .is_ok_and(|models| models.into_iter().any(|(_, model)| model.visible_in_codex))
}

fn relay_error_status(error: &owner::RelayError) -> Option<StatusCode> {
    match error {
        owner::RelayError::Upstream(polyflare_core::ExecError::UpstreamHttp(response)) => {
            StatusCode::from_u16(response.signal.status).ok()
        }
        _ => None,
    }
}

async fn record_initial_dial_failure(
    state: &AppState,
    account: &Account,
    error: &owner::RelayError,
) {
    let owner::RelayError::Upstream(error) = error else {
        return;
    };
    let signal = error.failure_signal();
    crate::ingress::bench_account_for_failure(
        state,
        &AccountId::from(account.id.as_str()),
        signal.as_ref(),
        unix_now(),
    )
    .await;
}

fn relay_error_response(error: owner::RelayError) -> Response {
    match error {
        owner::RelayError::NoEligibleAccount => {
            (StatusCode::SERVICE_UNAVAILABLE, "no eligible account").into_response()
        }
        owner::RelayError::Internal => {
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
        owner::RelayError::Upstream(polyflare_core::ExecError::UpstreamHttp(response)) => {
            let status =
                StatusCode::from_u16(response.signal.status).unwrap_or(StatusCode::BAD_GATEWAY);
            let mut builder = Response::builder().status(status);
            for (name, value) in response.headers {
                if let (Ok(name), Ok(value)) = (
                    axum::http::HeaderName::from_bytes(name.as_bytes()),
                    axum::http::HeaderValue::from_str(&value),
                ) {
                    builder = builder.header(name, value);
                }
            }
            builder
                .body(Body::from(response.body))
                .expect("valid upstream handshake error response")
        }
        owner::RelayError::Upstream(_) => {
            (StatusCode::BAD_GATEWAY, "upstream websocket unavailable").into_response()
        }
    }
}

struct RelayRoutingScope {
    pool: Option<String>,
    session_id: Option<String>,
    require_security_work_authorized: bool,
}

/// The post-upgrade relay future. Owner resolution and the initial upstream handshake have already
/// completed, so a downstream `101` always represents a usable upstream socket.
#[allow(clippy::too_many_arguments)]
async fn relay(
    socket: WebSocket,
    upstream: WsConn,
    account: Account,
    state: Arc<AppState>,
    headers: HeaderMap,
    session_key: SessionKey,
    routing_scope: RelayRoutingScope,
    ws_pressure: Arc<Mutex<Option<crate::runtime_state::WsSocketGuard>>>,
    relay_contract: polyflare_codex::WsRelayContract,
) {
    let pool = routing_scope.pool;
    let session_id = routing_scope.session_id;
    let require_security_work_authorized = routing_scope.require_security_work_authorized;
    // The ownership-recording callback the pump invokes on every sniffed `response.completed` id.
    // `Fn`, not `FnOnce`: one socket can carry many turns over its life, so each captured handle is
    // cloned fresh per call rather than consumed. Reuses the EXISTING continuity engine's `observe`
    // (wedge-sacred: `watchdog.rs`/`continuity.rs`/`ObservingStream` are never touched) — this one
    // call writes BOTH the session owner and the `response_id -> owner` anchor (it delegates to
    // `record_completion` internally). A failed write must not tear the relay down, hence `let _ =`.
    //
    // Phase 3 Task 4: the account id is now a PER-CALL parameter, not a captured value — the pump
    // passes its CURRENT account on every call (see `pump::run_pump`'s forward arm), so a turn
    // completed after a move re-homes ownership onto whoever ACTUALLY produced it, never onto the
    // stale account this closure was originally built against.
    let continuity = state.continuity.clone();
    let on_completed_id = {
        let continuity = continuity.clone();
        let session_key = session_key.clone();
        move |account_id: AccountId, response_id: String| {
            let continuity = continuity.clone();
            let session_key = session_key.clone();
            async move {
                let _ = continuity
                    .observe(
                        TurnOutcome::Completed {
                            session_key: Some(session_key),
                            account: account_id,
                            response_id: Some(response_id),
                            // The relay is content-free by construction — it never parses input
                            // bodies, so these stay empty/zero (see the task's decision log: the WS
                            // relay's wedge-avoidance is account-pinning, not fingerprint
                            // comparison).
                            input_fingerprint: String::new(),
                            input_count: 0,
                            reasoning: None,
                        },
                        &RequestCtx::default(),
                    )
                    .await;
            }
        }
    };

    // Phase 3 Task 4 (extended by the relay-catalog-fixes plan's Task 3): the exhaustion-move
    // engine. Called by the pump on a durable upstream error (`UpstreamSignal::Error`) with the
    // CURRENT account + the classified signal.
    //
    // A TRANSIENT 429 — `retry_after` present and at-or-under `TRANSIENT_RETRY_MAX_SECS` — is
    // retried IN PLACE on the SAME account: wait it out, then redial. This keeps the
    // conversation's prompt cache (a bench+move discards it) for exactly the case where the
    // upstream is asking for a short, bounded pause rather than signaling genuine exhaustion.
    // Never logged (content-free): no frame/error/account is inspected beyond the two fields
    // already on `sig`.
    //
    // Anything else (no `retry_after`, or a longer one) is DURABLE and falls through unchanged to
    // the existing bench -> re-resolve -> re-dial path: benches that account, re-resolves the
    // owner (skipping the just-benched one via the fresh cooldown overlay), and re-dials whatever
    // was picked. Returns `None` (teardown) only if no account is eligible at all, or the winning
    // candidate's upstream WS could not be reached even after `redial_upstream`'s bounded retries —
    // both clean, expected exhaustion outcomes, never logged.
    //
    // Reuse, not reinvention (wedge-sacred): `bench_account_for_failure` is the SAME policy
    // `record_failure` applies on the HTTP path; `resolve_owner` is the SAME owner-affine resolution
    // Task 3 already built; `redial_upstream` is Task 2's bounded same-account-or-new-account dial
    // helper (reused for BOTH the transient retry-in-place and the durable move). No new selection
    // or dial logic is written here.
    let on_upstream_error = {
        let state = state.clone();
        let headers = headers.clone();
        let session_key = session_key.clone();
        let pool = pool.clone();
        let session_id = session_id.clone();
        let relay_contract = relay_contract.clone();
        let ws_pressure = ws_pressure.clone();
        move |current: Account, sig: FailureSignal| {
            let state = state.clone();
            let headers = headers.clone();
            let session_key = session_key.clone();
            let pool = pool.clone();
            let session_id = session_id.clone();
            let relay_contract = relay_contract.clone();
            let ws_pressure = ws_pressure.clone();
            let require_security_work_authorized = require_security_work_authorized;
            async move {
                // Transient 429 (retry-after at/under the min-cooldown boundary): wait it out on
                // the SAME account and retry in place — keeps the conversation's prompt cache
                // instead of a cache-losing move. Only a durable/long/absent retry-after falls
                // through to bench -> re-select -> move.
                if sig.status == 429 {
                    if let Some(n) = sig.retry_after {
                        if (0..=TRANSIENT_RETRY_MAX_SECS).contains(&n) {
                            tokio::time::sleep(std::time::Duration::from_secs(n as u64)).await;
                            return redial_with_reactive_auth(
                                &state,
                                &headers,
                                current,
                                &relay_contract,
                                pool.as_deref(),
                            )
                            .await; // SAME account -> pump records reconnect_same_account, not a move
                        }
                    }
                }

                let now = unix_now();
                let current_id = AccountId::from(current.id.as_str());
                crate::ingress::bench_account_for_failure(&state, &current_id, Some(&sig), now)
                    .await;
                // The current upstream socket is no longer usable and this path is committed to a
                // durable reselect. Release its open-WS slot before reserving the replacement;
                // otherwise a global limit of one (or a saturated fleet) deadlocks the handoff on
                // its own stale guard until the admission timeout expires.
                drop(ws_pressure.lock().unwrap_or_else(|e| e.into_inner()).take());
                let (new_account, new_ws_guard) = resolve_owner(
                    &state,
                    &session_key,
                    session_id.as_deref(),
                    pool.as_deref(),
                    require_security_work_authorized,
                )
                .await
                .ok()?;
                let redial = redial_with_reactive_auth(
                    &state,
                    &headers,
                    new_account,
                    &relay_contract,
                    pool.as_deref(),
                )
                .await;
                if redial.is_some() {
                    *ws_pressure.lock().unwrap_or_else(|e| e.into_inner()) = Some(new_ws_guard);
                }
                redial
            }
        }
    };

    let on_pre_output_unauthorized = {
        let state = state.clone();
        let headers = headers.clone();
        let relay_contract = relay_contract.clone();
        let pool = pool.clone();
        move |current: Account| {
            let state = state.clone();
            let headers = headers.clone();
            let relay_contract = relay_contract.clone();
            let pool = pool.clone();
            async move {
                refresh_and_redial_unauthorized(
                    &state,
                    &headers,
                    &current,
                    &relay_contract,
                    pool.as_deref(),
                )
                .await
            }
        }
    };

    pump::run_pump(
        socket,
        upstream,
        headers,
        account,
        on_completed_id,
        on_upstream_error,
        on_pre_output_unauthorized,
        state,
        session_key,
        relay_contract,
        pool,
        require_security_work_authorized,
    )
    .await;
}
