//! The bidirectional verbatim relay pump (Task 6, extended by Phase 2/3 Task 3) — the WS-downstream
//! relay's core loop, once a downstream `WebSocket` has been upgraded and an upstream `WsConn`
//! dialed to the conversation's pinned owner account.
//!
//! **Two sockets, one plain forward — not codex-rs's mpsc/command-channel shape.** codex-rs's own
//! `responses_websocket.rs:62-125` fans a SINGLE socket's outbound side across multiple command
//! producers via an mpsc; that shape solves a different problem (many producers, one socket). The
//! relay has exactly two sockets and a plain bidirectional forward between them — the same shape as
//! codex-lb's `proxy_websocket` two-direction relay. A single `tokio::select!` loop owning both
//! sockets directly is YAGNI-correct here: each iteration creates a fresh `recv()`/`recv_text()`
//! future, so `select!` dropping the non-matched arm's future is harmless (nothing was consumed from
//! either socket without being observed), and forwarding to the OTHER socket inside the matched arm
//! has a free `&mut` with no `.split()` or channel required.
//!
//! **Phase 2: transparent same-account reconnect — MID-TURN ONLY (narrowed by the honest-liveness
//! work, 2026-07-24).** The upstream is `Option<WsConn>` — it goes `None` on ANY upstream drop (a
//! network blip, an idle server-side close, or the 60-minute `websocket_connection_limit_reached`
//! cap). A drop that catches a turn IN FLIGHT keeps the downstream open and re-dials the SAME
//! pinned `account` (eagerly, replaying the buffered frame — see below), as does the intercepted
//! cap signal. A replacement whose model, models ETag, or reasoning capability changed is an
//! exception: the downstream closes so Codex reconnects and receives the replacement socket's
//! fresh `101`. When the upstream is `None` mid-flow, the `select!` arm polling it is disabled via
//! the `if upstream.is_some()` guard and the next client `Text` frame re-dials via
//! `send_client_text`.
//!
//! **Honest liveness between turns (2026-07-24).** codex-rs's anchor ledger lives and dies with
//! its socket (`client.rs::websocket_connection`: `is_closed()` ⇒ wipe ledger ⇒ full resend), so a
//! relay that hides an upstream death behind a still-open downstream makes codex trust an anchor
//! that no longer exists — its next anchored delta is doomed to a client-visible
//! `previous_response_not_found` round-trip. Two changes restore the coupling:
//! - **Parked reads use `AppState::ws_relay_idle`, not the mid-turn stall deadline.** Between
//!   turns the upstream is read via `WsConn::recv_text_idle(idle_budget, ping_interval)`:
//!   keepalive `Ping`s (empty, content-free) keep intermediaries from reaping the healthy idle
//!   socket — previously the pump's own 290s `recv_text` deadline poisoned it (the 2026-07-24
//!   recurring "Reconnecting n/5": every failing turn had idled > 290s) — and a failed ping send
//!   surfaces a dead peer immediately. Mid-turn reads keep `recv_text`'s 290s stall bound.
//! - **A between-turns upstream end closes BOTH legs** (a genuine drop, a ping failure, or the
//!   idle budget deliberately expiring — labels `honest_close_upstream_drop` /
//!   `honest_close_idle_budget`). Codex sees its socket die, wipes its ledger, reconnects, and
//!   full-resends natively — silent, exactly its direct-connection behavior — instead of paying a
//!   failed anchored round-trip to discover what the relay already knew.
//!
//! - `UpstreamSignal::ConnectionLimit` (the 60-min cap) is INTERCEPTED — never forwarded to the
//!   client — and triggers an EAGER re-dial of the same account (rather than waiting for the next
//!   client frame), since the cap is a clean, expected, server-initiated boundary the client should
//!   never even see.
//! - `UpstreamSignal::Normal` is forwarded. An `AnchorMissing` on an anchored generating in-flight
//!   turn is answered by forging ONE retryable error envelope to the CLIENT (see
//!   [`client_resend_error_frame`]) so the client itself resends the full history — the relay never
//!   replays an anchored frame anchorless (its `input` is only the delta suffix; an anchorless
//!   replay silently restarts the conversation with just that suffix — the 2026-07-23 parrot
//!   incident). Anchorless/non-generating/repeat misses remain client-visible verbatim.
//!
//! **Task 4 (mid-turn replay): buffered in-flight replay on a mid-turn cap/drop.** `in_flight: Option<String>` holds
//! the raw client `response.create` frame of the CURRENT turn — set the moment it's forwarded
//! upstream and cleared on any client-visible terminal (`response.completed`, `response.failed`,
//! anchor-miss, or wrapped error). If the cap or a network drop fires WHILE a turn is in flight, the
//! client's own socket stays open and its
//! resend logic never fires (it's waiting on a reply, not re-sending) — so BOTH the `ConnectionLimit`
//! arm and the upstream-drop arm (`Ok(None) | Err(_)`) may, before any upstream event has crossed
//! downstream, re-dial and replay the buffered frame verbatim. Once any event is client-visible,
//! replay is forbidden and the downstream closes, avoiding duplicate/mixed output. Between turns
//! `in_flight` is `None`, so both arms are a no-op beyond the existing reconnect behavior. This is
//! same-account only at the moment of the move. If the client's later anchored retry misses on the
//! moved-to account, the anchor-miss arm signals the CLIENT to resend its full history (never an
//! anchorless replay of the delta). Held IN MEMORY ONLY, never logged.
//!
//! **Phase 3 Task 4 (this revision): exhaustion-move on `UpstreamSignal::Error`.** A durable upstream
//! error (anything the classifier didn't recognize as the cap or an anchor-miss) is forwarded to the
//! client VERBATIM FIRST — honest, the client reacts to it exactly as it would over HTTP-SSE — and
//! THEN `on_upstream_error` is awaited with the CURRENT account and the signal. The callback (built
//! in `mod.rs`) benches the account, re-selects, and re-dials — possibly landing back on the SAME
//! account (a retry) or a genuinely DIFFERENT one (a move); either way `account` and `upstream` are
//! updated to whatever it returns. `None` means no eligible account / every re-dial attempt failed —
//! the connection tears down.
//!
//! **Reactive authentication:** a wrapped 401 before any upstream frame from the current turn has
//! reached the client gets one synchronized same-account token refresh, redial, and verbatim replay.
//! A second 401, or any 401 after client-visible progress, follows the ordinary visible-error path;
//! replay after output is forbidden because it could duplicate streamed content.
//!
//! **Post-move ownership (arm C, correctness-critical):** the `on_completed_id` call site passes the
//! pump's CURRENT `account` — not whatever account the connection started on — so a `response.
//! completed` sniffed after a move is recorded against the account that ACTUALLY produced it. Wiring
//! this to the stale, original account would silently re-home ownership onto a benched/wrong account.
//!
//! **Consecutive-reconnect bound (Task-3 review follow-up).** `reconnects_since_progress` is bumped
//! on every re-dial (the eager cap re-dial, `send_client_text`'s internal re-dial, the exhaustion-move,
//! and each of this task's in-flight replays) and reset to 0 whenever a `response.
//! completed` is forwarded (real progress). Crossing [`MAX_RECONNECTS_WITHOUT_PROGRESS`] tears the
//! connection down — bounding a pathological upstream that re-caps/re-errors/re-drops on every fresh
//! dial without ever completing a turn, so a replay can never spin unboundedly.
//!
//! **Content-free:** no frame body is ever logged here. Inspection is limited to
//! [`super::sniff::sniff_completed_id`] (`type` + `response.id`),
//! [`super::signal::classify_upstream_signal`] (bounded error fields), and
//! [`super::telemetry`] (request metadata, event discriminants, numeric usage/timing). No
//! conversation content is returned or persisted, and there is no `tracing`/`log`/`println!`/
//! `eprintln!` in this module.

use std::future::Future;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::{Body, Bytes};
use axum::extract::ws::{Message, WebSocket};
use axum::http::{HeaderMap, StatusCode};
use futures_util::StreamExt;

use polyflare_codex::ws::WS_CONNECTION_LIMIT_CODE;
use polyflare_codex::{WsConn, WsRelayContract};
use polyflare_core::{Account, AccountId, FailureSignal, Provider, SessionKey};

use crate::app::AppState;

use super::redial::RedialOutcome;
use super::signal::{classify_upstream_signal, UpstreamSignal};
use super::sniff::sniff_completed_id;
use super::telemetry::{start_turn, WsRoutingOutcome, WsTurnTelemetry, WsTurnTerminal};

/// How many CONSECUTIVE re-dials (eager cap re-dial, client-send re-dial, an exhaustion-move, or
/// this task's mid-turn in-flight replay) are tolerated without a single forwarded `response.completed` in
/// between. Crossing this tears the connection down rather than spinning forever against a
/// pathological upstream that re-caps, re-errors, or re-drops on every fresh dial.
const MAX_RECONNECTS_WITHOUT_PROGRESS: u32 = 5;

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

async fn rewrite_rate_limit_frames(
    state: &AppState,
    pool: Option<&str>,
    require_security_work_authorized: bool,
    payload: &str,
) -> Option<(String, String)> {
    if !payload.contains("codex.rate_limits") {
        return None;
    }
    let snapshots = state.account_cache.snapshots(&state.store).await.ok()?;
    let quota = crate::pool_quota::synthesize(
        snapshots.as_ref(),
        Provider::Codex,
        pool,
        require_security_work_authorized,
    )?;
    crate::pool_quota::rewrite_ws_event(payload, &quota)
}

fn attempt_budget_exhausted_frame() -> String {
    serde_json::json!({
        "type": "error",
        "status": 400,
        "error": {
            "code": "logical_turn_attempts_exhausted",
            "message": "This logical turn exhausted its upstream attempt budget.",
            "type": "invalid_request_error"
        }
    })
    .to_string()
}

async fn surface_attempt_budget_exhausted(
    downstream: &mut WebSocket,
    telemetry: &mut Option<WsTurnTelemetry>,
    state: &AppState,
    account_id: &str,
) -> bool {
    let frame = attempt_budget_exhausted_frame();
    if downstream
        .send(Message::Text(frame.clone().into()))
        .await
        .is_err()
    {
        return false;
    }
    if let Some(mut turn) = telemetry.take() {
        if let Some(terminal) = turn.observe(&frame) {
            turn.finish(state, account_id, terminal).await;
        }
    }
    true
}

fn try_consume_active_turn_attempt(state: &AppState, telemetry: &Option<WsTurnTelemetry>) -> bool {
    state.runtime.try_consume_logical_turn_attempt(
        telemetry
            .as_ref()
            .and_then(WsTurnTelemetry::logical_turn_key),
        state.runtime_settings.max_account_attempts(),
        unix_now(),
    )
}

fn refund_active_turn_attempt(state: &AppState, telemetry: &Option<WsTurnTelemetry>) {
    state.runtime.refund_logical_turn_attempt(
        telemetry
            .as_ref()
            .and_then(WsTurnTelemetry::logical_turn_key),
    );
}

enum CustomFrameDisposition {
    Native,
    Relayed,
    Failed,
}

const MAX_CUSTOM_SSE_LINE_BYTES: usize = 1024 * 1024;

fn custom_ws_error_frame(status: StatusCode) -> String {
    serde_json::json!({
        "type": "error",
        "status": status.as_u16(),
        "error": {
            "code": "custom_provider_error",
            "message": "The custom model provider could not complete this request.",
            "type": "api_error"
        }
    })
    .to_string()
}

fn stateless_ws_event(line: &[u8]) -> Option<String> {
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    let data = line.strip_prefix(b"data:")?;
    let data = std::str::from_utf8(data).ok()?.trim();
    if data.is_empty() || data == "[DONE]" {
        return None;
    }
    let mut value: serde_json::Value = serde_json::from_str(data).ok()?;
    if let Some(response) = value
        .get_mut("response")
        .and_then(serde_json::Value::as_object_mut)
    {
        // A custom Responses provider is stateless from Codex's perspective. An empty terminal
        // id makes codex-rs send full history on the next WS request instead of a delta anchored
        // to an id the custom provider cannot resume through PolyFlare.
        if response.contains_key("id") {
            response.insert("id".into(), serde_json::Value::String(String::new()));
        }
    }
    Some(value.to_string())
}

async fn relay_custom_sse_body(downstream: &mut WebSocket, body: Body) -> bool {
    let mut stream = body.into_data_stream();
    let mut pending = Vec::new();
    loop {
        tokio::select! {
            downstream_message = downstream.recv() => {
                match downstream_message {
                    Some(Ok(Message::Ping(payload))) => {
                        if downstream.send(Message::Pong(payload)).await.is_err() {
                            return false;
                        }
                    }
                    Some(Ok(Message::Pong(_))) | Some(Ok(Message::Binary(_))) => {}
                    // Codex has one generating turn per socket. A second text request while the
                    // custom SSE stream is active would overlap two turns, so fail closed.
                    Some(Ok(Message::Text(_)))
                    | Some(Ok(Message::Close(_)))
                    | Some(Err(_))
                    | None => return false,
                }
            }
            chunk = stream.next() => {
                let Some(chunk) = chunk else {
                    break;
                };
                let Ok(chunk) = chunk else {
                    return false;
                };
                pending.extend_from_slice(&chunk);
                if pending.len() > MAX_CUSTOM_SSE_LINE_BYTES && !pending.contains(&b'\n') {
                    return false;
                }
                while let Some(newline) = pending.iter().position(|byte| *byte == b'\n') {
                    let line = pending.drain(..=newline).collect::<Vec<_>>();
                    if let Some(event) =
                        stateless_ws_event(&line[..line.len().saturating_sub(1)])
                    {
                        if downstream.send(Message::Text(event.into())).await.is_err() {
                            return false;
                        }
                    }
                }
            }
        }
    }
    if !pending.is_empty() {
        if let Some(event) = stateless_ws_event(&pending) {
            if downstream.send(Message::Text(event.into())).await.is_err() {
                return false;
            }
        }
    }
    true
}

async fn try_relay_custom_frame(
    downstream: &mut WebSocket,
    state: &std::sync::Arc<AppState>,
    headers: &HeaderMap,
    pool: Option<&str>,
    frame: &str,
) -> CustomFrameDisposition {
    let mut body: serde_json::Value = match serde_json::from_str(frame) {
        Ok(value) => value,
        Err(_) => return CustomFrameDisposition::Native,
    };
    let Some(object) = body.as_object_mut() else {
        return CustomFrameDisposition::Native;
    };
    if object.get("type").and_then(serde_json::Value::as_str) != Some("response.create") {
        return CustomFrameDisposition::Native;
    }
    let Some(model) = object.get("model").and_then(serde_json::Value::as_str) else {
        return CustomFrameDisposition::Native;
    };
    // Custom providers are a root-only route and native/translation slugs always win. Keep this
    // preflight identical to HTTP ingress so a pooled or reserved request stays on the native WS
    // path instead of being intercepted and silently changed to HTTP.
    if pool.is_some() || crate::catalog::model_slug_is_reserved(state, model) {
        return CustomFrameDisposition::Native;
    }
    let (provider, provider_model) = match state.store.providers().resolve_model(model).await {
        Ok(Some(route)) => route,
        Ok(None) => return CustomFrameDisposition::Native,
        Err(_) => {
            let _ = downstream
                .send(Message::Text(
                    custom_ws_error_frame(StatusCode::INTERNAL_SERVER_ERROR).into(),
                ))
                .await;
            return CustomFrameDisposition::Failed;
        }
    };

    if object.get("generate").and_then(serde_json::Value::as_bool) == Some(false) {
        let completed = serde_json::json!({
            "type": "response.completed",
            "response": {"id": ""}
        })
        .to_string();
        return if downstream
            .send(Message::Text(completed.into()))
            .await
            .is_ok()
        {
            CustomFrameDisposition::Relayed
        } else {
            CustomFrameDisposition::Failed
        };
    }

    object.remove("type");
    object.remove("generate");
    let encoded = match serde_json::to_vec(&body) {
        Ok(encoded) => encoded,
        Err(_) => return CustomFrameDisposition::Failed,
    };
    let response = crate::ingress::responses_custom_route_for_ws(
        state.clone(),
        headers.clone(),
        Bytes::from(encoded),
        provider,
        provider_model,
    )
    .await;
    let status = response.status();
    if !status.is_success() {
        let frame = custom_ws_error_frame(status);
        return if downstream.send(Message::Text(frame.into())).await.is_ok() {
            CustomFrameDisposition::Relayed
        } else {
            CustomFrameDisposition::Failed
        };
    }
    if relay_custom_sse_body(downstream, response.into_body()).await {
        CustomFrameDisposition::Relayed
    } else {
        let _ = downstream
            .send(Message::Text(
                custom_ws_error_frame(StatusCode::BAD_GATEWAY).into(),
            ))
            .await;
        CustomFrameDisposition::Failed
    }
}

/// Drive the relay for one WS-downstream conversation until the CLIENT goes away (a `Close`, a
/// closed socket, or a read error on the downstream leg) — an upstream drop alone never ends this.
///
/// - **client → backend:** a `Text` frame is sent to the upstream VERBATIM via
///   [`send_client_text`], which re-dials the SAME `account` first if the upstream is currently
///   dead (or after a send failure, once); on success the frame is also stashed into `in_flight`
///   (this task — this turn is now replayable until it completes); an inbound `Ping` is auto-ponged
///   back to the CLIENT inline (codex-rs fidelity: the relay never *initiates* a ping itself);
///   `Pong`/`Binary` are ignored (codex WS is text-only, content-free — never logged); a `Close`, a
///   closed socket (`None`), or a read error tears down both legs — this is the ONLY real teardown
///   path.
/// - **backend → client:** a `Text` frame is classified via [`classify_upstream_signal`] first.
///   `ConnectionLimit` is intercepted (never forwarded) and triggers an eager same-account re-dial,
///   THEN (this task) replays `in_flight` on the fresh socket if a turn was in flight;
///   `Normal` is forwarded VERBATIM. `AnchorMissing` on an anchored generating in-flight turn sends
///   the CLIENT one forged retryable envelope ([`client_resend_error_frame`]) so the client resends
///   its full history — never an anchorless replay of the delta suffix; anchorless/non-generating/
///   repeat misses are forwarded verbatim and clear `in_flight`. A `response.completed` id invokes
///   `on_completed_id` with the CURRENT account (the caller records ownership via
///   `Continuity::observe`) and resets the no-progress bound.
///   `Error(sig)` is also forwarded VERBATIM FIRST, then `on_upstream_error` is awaited with the
///   CURRENT account and the signal — on `Some((new_account, new_upstream))` the pump's account/
///   upstream become whatever it returns (same account = retry, different = a genuine move); `None`
///   tears the connection down. An error is terminal and never replayed — the client sends a fresh
///   request if it retries. A closed upstream (`Ok(None)`) or a read error (`Err(_)`) marks the
///   upstream dead; if a turn was in flight (this task) the pump EAGERLY re-dials the same account and
///   replays `in_flight` right here (the client is waiting, not going to resend on its own) —
///   otherwise (between turns) it simply stops polling the upstream until the client's next frame
///   re-dials via `send_client_text`.
///
/// The callbacks are `Fn`, not `FnOnce`, because a single socket can carry many turns (and possibly
/// many reconnects or moves) over its life — each call is fresh and independent.
///
/// **Task 5:** `relay_metrics` is a content-free counter handle (`crate::observability::
/// RelayMetrics`) bumped at fixed decision points — every same-account re-dial (the eager
/// `ConnectionLimit` re-dial, `send_client_text`'s internal re-dial, and the retry-in-place branch
/// of an `on_upstream_error` call), every cross-account move (the other branch of `on_upstream_
/// error`), every recovered same-account anchor, and every same-account `AnchorMissing` that cannot
/// be safely recovered. `account_changed_since_completed` tracks exactly that: set `true` the moment
/// `on_upstream_error` returns a DIFFERENT account, reset to `false` the moment a `response.
/// completed` is forwarded. Never carries anything beyond the four fixed label strings.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_pump<F, Fut, G, GFut, H, HFut>(
    mut downstream: WebSocket,
    upstream_conn: WsConn,
    headers: HeaderMap,
    account: Account,
    on_completed_id: F,
    on_upstream_error: G,
    on_pre_output_unauthorized: H,
    state: std::sync::Arc<AppState>,
    session_key: SessionKey,
    relay_contract: WsRelayContract,
    pool: Option<String>,
    require_security_work_authorized: bool,
) where
    F: Fn(AccountId, String) -> Fut,
    Fut: Future<Output = ()>,
    G: Fn(Account, FailureSignal) -> GFut,
    GFut: Future<Output = Option<(Account, WsConn)>>,
    H: Fn(Account) -> HFut,
    HFut: Future<Output = Option<(Account, WsConn)>>,
{
    let mut account = account;
    let relay_metrics = state.relay_metrics.clone();
    // Honest-liveness work (2026-07-24): the between-turns idle policy. A parked upstream is read
    // with `recv_text_idle(idle_budget, ping_interval)` instead of the mid-turn 290s stall
    // deadline — between turns silence is healthy, and poisoning the socket there killed its
    // connection-scoped anchor and forced the next anchored delta into a client-visible resend
    // round-trip (every failing turn in the 2026-07-24 incident had idled > 290s).
    let idle_policy = state.ws_relay_idle;
    let mut upstream: Option<WsConn> = Some(upstream_conn);
    let mut reconnects_since_progress: u32 = 0;
    // Anchored-resume observability: set when an ANCHORED generating frame is (re)sent on a
    // freshly re-dialed upstream — the one situation where the connection-scoped anchor may or
    // may not have survived. A later forwarded `response.completed` while set counts a resume win
    // (`anchor_resumed_after_redial`); the anchor-miss arm counts the loss via its existing
    // labels. Decides the lazy-redial-vs-honest-close trade with data instead of guesses.
    let mut anchored_redial_pending = false;
    // Task 4 (mid-turn replay): the raw client `response.create` frame of the CURRENT in-flight turn, held in memory
    // ONLY (never logged/persisted) so it can be REPLAYED on a same-account re-dial after a mid-turn
    // cap/drop — so the interrupted turn resumes without the client having to resend. Cleared on the
    // turn's `response.completed`. One in-flight turn per socket (codex's model).
    let mut in_flight: Option<String> = None;
    // Task 5: has the pinned account changed (a move) since the last forwarded
    // `response.completed`? Starts `false` — a fresh connection hasn't moved yet.
    let mut account_changed_since_completed = false;
    // Anchor-miss resend signal one-shot: set when the pump forges a client-resend envelope
    // (`client_resend_error_frame`), reset on the next forwarded `response.completed`. A client
    // honoring the signal resends FULL history (anchorless — it cannot miss again); a client that
    // instead repeats an anchored attempt gets the raw miss verbatim rather than a signal loop.
    let mut anchor_resend_pending = false;
    // One user-visible turn per socket at a time. Same-account reconnect/replay deliberately keeps
    // this alive so latency spans the interruption and the eventual completion still emits once.
    let mut turn_telemetry: Option<WsTurnTelemetry> = None;
    // The ACTIVE turn's aggregate-budget key, held at pump scope because `turn_telemetry` has
    // already been taken by `observe` when the `response.completed` sniff below needs it: a
    // forwarded completion clears the logical turn's spent attempts (codex reuses one turn id
    // across every tool-call round of a user turn — progress must reset the budget, or round
    // `max_account_attempts + 1` of a healthy loop is rejected as amplification).
    let mut active_turn_key: Option<String> = None;
    // Retry a mid-session 401 at most once, and only before any upstream frame from this turn has
    // crossed the downstream boundary.
    let mut reactive_auth_attempted = false;
    let mut client_visible_upstream_for_turn = false;
    // If the socket/pump disappears with a turn still active, preserve an explicit failed row
    // rather than silently losing the request. Client-side teardown defaults to nginx-style 499;
    // upstream/reconnect exhaustion sites below overwrite this with a server-side status.
    let mut unfinished_status = StatusCode::from_u16(499).expect("499 is a valid status");
    loop {
        // Which upstream-read regime applies THIS iteration: a turn awaiting output keeps the
        // mid-turn stall deadline (`recv_text`); a parked socket between turns gets the idle
        // policy (`recv_text_idle`) — no stall deadline, optional keepalive pings, honest budget.
        let turn_active = in_flight.is_some() || turn_telemetry.is_some();
        tokio::select! {
            down = downstream.recv() => {
                match down {
                    Some(Ok(Message::Text(t))) => {
                        let frame = t.to_string();
                        match try_relay_custom_frame(
                            &mut downstream,
                            &state,
                            &headers,
                            pool.as_deref(),
                            &frame,
                        )
                        .await
                        {
                            CustomFrameDisposition::Native => {}
                            CustomFrameDisposition::Relayed => continue,
                            CustomFrameDisposition::Failed => {
                                unfinished_status = StatusCode::BAD_GATEWAY;
                                break;
                            }
                        }
                        let mut logical_turn_key = None;
                        // Start before any lazy re-dial/send so TTFT and total latency cover the
                        // complete client-visible turn, matching the HTTP route clock origin.
                        if let Some(mut next_turn) =
                            start_turn(&headers, &frame, &session_key, pool.as_deref())
                        {
                            logical_turn_key =
                                next_turn.logical_turn_key().map(str::to_owned);
                            if !next_turn
                                .track_in_flight(
                                    &state,
                                    &AccountId::from(account.id.as_str()),
                                )
                                .await
                            {
                                let _ = downstream
                                    .send(Message::Text(client_resend_error_frame().into()))
                                    .await;
                                continue;
                            }
                            turn_telemetry = Some(next_turn);
                            active_turn_key = logical_turn_key.clone();
                            reactive_auth_attempted = false;
                            client_visible_upstream_for_turn = false;
                        }
                        let redialed = send_client_text(
                            &mut upstream,
                            &headers,
                            &account,
                            &relay_contract,
                            &state,
                            pool.as_deref(),
                            frame.clone(),
                            logical_turn_key.as_deref(),
                        )
                        .await;
                        match redialed {
                            SendClientOutcome::Failed => {
                                unfinished_status = StatusCode::BAD_GATEWAY;
                                break; // upstream could not be (re-)established at all.
                            }
                            SendClientOutcome::Unauthorized => {
                                if reactive_auth_attempted
                                    || client_visible_upstream_for_turn
                                {
                                    unfinished_status = StatusCode::BAD_GATEWAY;
                                    break;
                                }
                                reactive_auth_attempted = true;
                                let Some((refreshed_account, mut refreshed_upstream)) =
                                    on_pre_output_unauthorized(account.clone()).await
                                else {
                                    unfinished_status = StatusCode::BAD_GATEWAY;
                                    break;
                                };
                                if !state.runtime.try_consume_logical_turn_attempt(
                                    logical_turn_key.as_deref(),
                                    state.runtime_settings.max_account_attempts(),
                                    unix_now(),
                                ) {
                                    account = refreshed_account;
                                    upstream = Some(refreshed_upstream);
                                    in_flight = None;
                                    if !surface_attempt_budget_exhausted(
                                        &mut downstream,
                                        &mut turn_telemetry,
                                        &state,
                                        &account.id,
                                    )
                                    .await
                                    {
                                        break;
                                    }
                                    continue;
                                }
                                if refreshed_upstream.send_text(frame.clone()).await.is_err() {
                                    unfinished_status = StatusCode::BAD_GATEWAY;
                                    break;
                                }
                                if is_anchored_generating_frame(&frame) {
                                    relay_metrics.record("anchored_send_after_redial");
                                    anchored_redial_pending = true;
                                }
                                account = refreshed_account;
                                upstream = Some(refreshed_upstream);
                                in_flight = Some(frame);
                                relay_metrics.record("reconnect_same_account");
                                reconnects_since_progress += 1;
                                if reconnects_since_progress > MAX_RECONNECTS_WITHOUT_PROGRESS {
                                    unfinished_status = StatusCode::BAD_GATEWAY;
                                    break;
                                }
                            }
                            SendClientOutcome::AttemptBudgetExhausted => {
                                in_flight = None;
                                if !surface_attempt_budget_exhausted(
                                    &mut downstream,
                                    &mut turn_telemetry,
                                    &state,
                                    &account.id,
                                )
                                .await
                                {
                                    break;
                                }
                            }
                            SendClientOutcome::Sent { redialed } => {
                                if redialed && is_anchored_generating_frame(&frame) {
                                    // An anchored delta went out on a FRESH socket — the anchor
                                    // may or may not have survived; a later completed counts the
                                    // win, the anchor-miss arm counts the loss.
                                    relay_metrics.record("anchored_send_after_redial");
                                    anchored_redial_pending = true;
                                }
                                // This task: this turn is now in flight — replayable until it completes
                                // (a same-account cap/drop mid-turn replays THIS frame, not a client
                                // resend).
                                in_flight = Some(frame);
                                if redialed {
                                    relay_metrics.record("reconnect_same_account");
                                    reconnects_since_progress += 1;
                                    if reconnects_since_progress > MAX_RECONNECTS_WITHOUT_PROGRESS {
                                        unfinished_status = StatusCode::BAD_GATEWAY;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        if downstream.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    // codex WS is text-only; ignore silently — content-free, never logged.
                    Some(Ok(Message::Pong(_))) | Some(Ok(Message::Binary(_))) => {}
                    // The CLIENT went away — real teardown.
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                }
            }
            // Only poll the upstream when one is live. When `upstream` is `None` this arm is
            // disabled by the `if` guard, so the loop waits purely on the client — whose next Text
            // frame re-dials via `send_client_text` above. Mid-turn keeps the 290s stall deadline;
            // between turns the idle policy applies (see `turn_active` above).
            up = async {
                let conn = upstream.as_mut().unwrap();
                if turn_active {
                    conn.recv_text().await
                } else {
                    conn.recv_text_idle(idle_policy.idle_budget, idle_policy.ping_interval).await
                }
            }, if upstream.is_some() => {
                match up {
                    Ok(Some(text)) => {
                        match classify_upstream_signal(&text) {
                            UpstreamSignal::ConnectionLimit => {
                                // The 60-min server cap: INTERCEPT (never forward) and eagerly
                                // re-dial the SAME account so the client never sees this boundary.
                                if client_visible_upstream_for_turn {
                                    // Replaying after any forwarded event can duplicate output the
                                    // client has already consumed. End the downstream socket and
                                    // let Codex recover with a fresh request instead.
                                    unfinished_status = StatusCode::BAD_GATEWAY;
                                    break;
                                }
                                let redial = super::redial_for_scope(
                                    &state,
                                    &headers,
                                    &account,
                                    &relay_contract,
                                    pool.as_deref(),
                                )
                                .await;
                                let next = match redial {
                                    RedialOutcome::Connected(conn) => {
                                        Some((account.clone(), *conn))
                                    }
                                    RedialOutcome::Unauthorized if !reactive_auth_attempted => {
                                        reactive_auth_attempted = true;
                                        on_pre_output_unauthorized(account.clone()).await
                                    }
                                    RedialOutcome::Unauthorized
                                    | RedialOutcome::ContractDrift
                                    | RedialOutcome::Unavailable => None,
                                };
                                let Some((next_account, next_upstream)) = next else {
                                    unfinished_status = StatusCode::BAD_GATEWAY;
                                    break;
                                };
                                account = next_account;
                                upstream = Some(next_upstream);
                                // Only a successful (possibly reactively refreshed) redial reaches
                                // this point, so the reconnect counter cannot count teardowns.
                                // This task: if a turn was in flight when the cap hit, replay it on the
                                // fresh socket so the interrupted turn resumes (same account -> the
                                // anchor resumes). No-op between turns.
                                if let Some(frame) = in_flight.clone() {
                                    if !try_consume_active_turn_attempt(
                                        &state,
                                        &turn_telemetry,
                                    ) {
                                        in_flight = None;
                                        if !surface_attempt_budget_exhausted(
                                            &mut downstream,
                                            &mut turn_telemetry,
                                            &state,
                                            &account.id,
                                        )
                                        .await
                                        {
                                            break;
                                        }
                                        continue;
                                    }
                                    let anchored = is_anchored_generating_frame(&frame);
                                    if upstream.as_mut().unwrap().send_text(frame).await.is_err() {
                                        unfinished_status = StatusCode::BAD_GATEWAY;
                                        break;
                                    }
                                    if anchored {
                                        relay_metrics.record("anchored_send_after_redial");
                                        anchored_redial_pending = true;
                                    }
                                }
                                relay_metrics.record("reconnect_same_account");
                                reconnects_since_progress += 1;
                                if reconnects_since_progress > MAX_RECONNECTS_WITHOUT_PROGRESS {
                                    unfinished_status = StatusCode::BAD_GATEWAY;
                                    break;
                                }
                            }
                            // Task 4: a durable error. Forward VERBATIM FIRST (honest — the client
                            // reacts to it exactly as it would over HTTP-SSE), THEN bench + re-select
                            // + re-dial via the caller-provided move engine.
                            UpstreamSignal::Error(sig) => {
                                // This upstream has emitted a terminal protocol error and cannot
                                // serve another turn. Retire it before any same-account refresh or
                                // cross-account move dials a replacement, so the open-WS hard cap
                                // measures real sockets rather than briefly allowing old + new.
                                drop(upstream.take());
                                if sig.status == 401
                                    && !reactive_auth_attempted
                                    && !client_visible_upstream_for_turn
                                    && in_flight.is_some()
                                {
                                    refund_active_turn_attempt(&state, &turn_telemetry);
                                    reactive_auth_attempted = true;
                                    if let Some((refreshed_account, mut refreshed_upstream)) =
                                        on_pre_output_unauthorized(account.clone()).await
                                    {
                                        let frame = in_flight
                                            .clone()
                                            .expect("401 replay eligibility checked in_flight");
                                        if !try_consume_active_turn_attempt(
                                            &state,
                                            &turn_telemetry,
                                        ) {
                                            account = refreshed_account;
                                            upstream = Some(refreshed_upstream);
                                            in_flight = None;
                                            if !surface_attempt_budget_exhausted(
                                                &mut downstream,
                                                &mut turn_telemetry,
                                                &state,
                                                &account.id,
                                            )
                                            .await
                                            {
                                                break;
                                            }
                                            continue;
                                        }
                                        let anchored = is_anchored_generating_frame(&frame);
                                        if refreshed_upstream.send_text(frame).await.is_err() {
                                            unfinished_status = StatusCode::BAD_GATEWAY;
                                            break;
                                        }
                                        if anchored {
                                            relay_metrics.record("anchored_send_after_redial");
                                            anchored_redial_pending = true;
                                        }
                                        account = refreshed_account;
                                        upstream = Some(refreshed_upstream);
                                        relay_metrics.record("reconnect_same_account");
                                        reconnects_since_progress += 1;
                                        if reconnects_since_progress
                                            > MAX_RECONNECTS_WITHOUT_PROGRESS
                                        {
                                            unfinished_status = StatusCode::BAD_GATEWAY;
                                            break;
                                        }
                                        continue;
                                    }
                                }
                                if downstream.send(Message::Text(text.clone().into())).await.is_err() {
                                    break;
                                }
                                client_visible_upstream_for_turn = true;
                                if let Some(mut turn) = turn_telemetry.take() {
                                    if let Some(terminal) = turn.observe(&text) {
                                        turn.finish(&state, &account.id, terminal).await;
                                    } else {
                                        turn_telemetry = Some(turn);
                                    }
                                }
                                // The client saw a terminal error and will issue a fresh
                                // response.create if it retries. Never replay this failed frame if
                                // the newly dialed socket drops before that resend arrives.
                                in_flight = None;
                                let current_id = account.id.clone();
                                match on_upstream_error(account.clone(), sig).await {
                                    Some((new_account, new_upstream)) => {
                                        // Task 5: same account id back => a retry-in-place
                                        // (reconnect); a DIFFERENT id => a genuine cross-account
                                        // move. Compared BEFORE `account` is overwritten below.
                                        if new_account.id == current_id {
                                            relay_metrics.record("reconnect_same_account");
                                        } else {
                                            relay_metrics.record("move_cross_account");
                                            account_changed_since_completed = true;
                                        }
                                        account = new_account; // same account (retry) or a NEW one (moved).
                                        upstream = Some(new_upstream);
                                        reconnects_since_progress += 1;
                                        if reconnects_since_progress > MAX_RECONNECTS_WITHOUT_PROGRESS {
                                            unfinished_status = StatusCode::BAD_GATEWAY;
                                            break;
                                        }
                                    }
                                    // No eligible account, or every re-dial attempt failed -> teardown.
                                    None => {
                                        unfinished_status = StatusCode::SERVICE_UNAVAILABLE;
                                        break;
                                    }
                                }
                            }
                            // An ephemeral `store:false` anchor can disappear on the same account
                            // as well as after a move. An anchored frame is a CLIENT-PLANNED DELTA
                            // (codex-rs `prepare_websocket_request` sets `previous_response_id`
                            // exactly when `input` holds only the new suffix), so the relay can
                            // NEVER recover it server-side: replaying it anchorless would silently
                            // restart the conversation with just that suffix — the 2026-07-23
                            // parrot incident (a ~240k-token session reborn as a 39-token one,
                            // 200 OK, no error anywhere). The only party holding the full history
                            // is the client — make IT resend.
                            UpstreamSignal::AnchorMissing => {
                                // If an anchored-after-redial send was pending, this IS its
                                // outcome: the anchor did not survive the fresh socket. The loss
                                // is counted by this arm's existing labels; just clear the flag.
                                anchored_redial_pending = false;
                                // Forge the ONE error shape codex-rs classifies as retryable
                                // (see `client_resend_error_frame`): its failed attempt leaves the
                                // client's per-connection delta ledger unresolved, so the bounded
                                // in-place retry arrives as a FULL anchorless resend. Forwarding
                                // the raw miss instead would map to codex-rs's non-retryable
                                // `InvalidRequest` and wedge the task.
                                let resend_eligible = !anchor_resend_pending
                                    && in_flight
                                        .as_deref()
                                        .is_some_and(is_anchored_generating_frame);
                                if resend_eligible {
                                    let forged = client_resend_error_frame();
                                    if downstream
                                        .send(Message::Text(forged.clone().into()))
                                        .await
                                        .is_err()
                                    {
                                        break;
                                    }
                                    client_visible_upstream_for_turn = true;
                                    if let Some(mut turn) = turn_telemetry.take() {
                                        if let Some(terminal) = turn.observe(&forged) {
                                            turn.finish(&state, &account.id, terminal).await;
                                        } else {
                                            turn_telemetry = Some(turn);
                                        }
                                    }
                                    // The client's turn is over; its retry is a fresh
                                    // `response.create` (full history, no anchor — it cannot miss
                                    // again). Never replay this dead frame.
                                    in_flight = None;
                                    anchor_resend_pending = true;
                                    relay_metrics.record("anchor_miss_client_resend");
                                    continue;
                                }
                                // Anchorless/non-generating frames gain nothing from a resend
                                // signal (codex only anchors deltas), and a second miss while a
                                // signal is already pending means the client is not honoring the
                                // resend contract — surface the truth verbatim.
                                if downstream.send(Message::Text(text.clone().into())).await.is_err() {
                                    break;
                                }
                                client_visible_upstream_for_turn = true;
                                if let Some(mut turn) = turn_telemetry.take() {
                                    if let Some(terminal) = turn.observe(&text) {
                                        turn.finish(&state, &account.id, terminal).await;
                                    } else {
                                        turn_telemetry = Some(turn);
                                    }
                                }
                                in_flight = None;
                                if !account_changed_since_completed {
                                    relay_metrics.record("same_account_anchor_miss");
                                }
                            }
                            // Normal: forward VERBATIM, then sniff for ownership.
                            UpstreamSignal::Normal => {
                                if let Some((selected, aggregate)) = rewrite_rate_limit_frames(
                                    &state,
                                    pool.as_deref(),
                                    require_security_work_authorized,
                                    &text,
                                )
                                .await
                                {
                                    if downstream
                                        .send(Message::Text(selected.into()))
                                        .await
                                        .is_err()
                                        || downstream
                                            .send(Message::Text(aggregate.into()))
                                            .await
                                            .is_err()
                                    {
                                        break;
                                    }
                                } else if downstream
                                    .send(Message::Text(text.clone().into()))
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                                client_visible_upstream_for_turn = true;
                                let mut terminal_seen = false;
                                if let Some(mut turn) = turn_telemetry.take() {
                                    if let Some(terminal) = turn.observe(&text) {
                                        terminal_seen = true;
                                        turn.finish(&state, &account.id, terminal).await;
                                    } else {
                                        turn_telemetry = Some(turn);
                                    }
                                }
                                // `response.failed` is a terminal Normal-class frame but carries
                                // no completion id, so the ownership sniff below cannot clear it.
                                if terminal_seen {
                                    in_flight = None;
                                }
                                if let Some(id) = sniff_completed_id(&text) {
                                    // The CURRENT account, not whatever the connection started on —
                                    // a completed turn after a move must re-home to the account that
                                    // actually produced it, never the stale/benched original.
                                    on_completed_id(AccountId::from(account.id.as_str()), id).await;
                                    // A completed turn is real progress: reset the no-progress bound
                                    // and the move flag — a completed turn on the current account
                                    // means any LATER anchor-miss is once again same-account — and
                                    // re-arm the one-shot anchor-miss resend signal.
                                    reconnects_since_progress = 0;
                                    account_changed_since_completed = false;
                                    anchor_resend_pending = false;
                                    in_flight = None; // This task: the turn finished — nothing to replay.
                                    if anchored_redial_pending {
                                        // The anchored delta sent on a fresh socket COMPLETED —
                                        // the connection-scoped anchor genuinely resumed.
                                        relay_metrics.record("anchor_resumed_after_redial");
                                        anchored_redial_pending = false;
                                    }
                                    // Progress also resets the aggregate turn budget: the next
                                    // tool-call round under this SAME codex turn id is new work,
                                    // not a retry of a failing turn.
                                    state
                                        .runtime
                                        .clear_logical_turn_attempts(active_turn_key.as_deref());
                                    active_turn_key = None;
                                }
                            }
                        }
                    }
                    // The upstream dropped (network blip / idle close / a mid-stream close), or —
                    // between turns only — the idle budget elapsed and the relay is deliberately
                    // letting the session go.
                    end @ (Ok(None) | Err(_)) => {
                        let idle_budget_expired = !turn_active
                            && matches!(&end, Err(e) if polyflare_codex::ws::is_read_idle_error(e));
                        // This task: a mid-turn drop (a turn is in flight) — the client is waiting and
                        // won't resend on its own. Eagerly re-dial the SAME account and replay the
                        // buffered frame so the turn resumes. Between turns (in_flight None) keep the
                        // lazy behavior: the next client frame re-dials via `send_client_text`.
                        if in_flight.is_some() {
                            if client_visible_upstream_for_turn {
                                unfinished_status = StatusCode::BAD_GATEWAY;
                                break;
                            }
                            let redial = super::redial_for_scope(
                                &state,
                                &headers,
                                &account,
                                &relay_contract,
                                pool.as_deref(),
                            )
                            .await;
                            let next = match redial {
                                RedialOutcome::Connected(conn) => Some((account.clone(), *conn)),
                                RedialOutcome::Unauthorized if !reactive_auth_attempted => {
                                    reactive_auth_attempted = true;
                                    on_pre_output_unauthorized(account.clone()).await
                                }
                                RedialOutcome::Unauthorized
                                | RedialOutcome::ContractDrift
                                | RedialOutcome::Unavailable => None,
                            };
                            let Some((next_account, next_upstream)) = next else {
                                unfinished_status = StatusCode::BAD_GATEWAY;
                                break;
                            };
                            account = next_account;
                            upstream = Some(next_upstream);
                            if let Some(frame) = in_flight.clone() {
                                if !try_consume_active_turn_attempt(&state, &turn_telemetry) {
                                    in_flight = None;
                                    if !surface_attempt_budget_exhausted(
                                        &mut downstream,
                                        &mut turn_telemetry,
                                        &state,
                                        &account.id,
                                    )
                                    .await
                                    {
                                        break;
                                    }
                                    continue;
                                }
                                let anchored = is_anchored_generating_frame(&frame);
                                if upstream.as_mut().unwrap().send_text(frame).await.is_err() {
                                    unfinished_status = StatusCode::BAD_GATEWAY;
                                    break;
                                }
                                if anchored {
                                    relay_metrics.record("anchored_send_after_redial");
                                    anchored_redial_pending = true;
                                }
                            }
                            relay_metrics.record("reconnect_same_account");
                            reconnects_since_progress += 1;
                            if reconnects_since_progress > MAX_RECONNECTS_WITHOUT_PROGRESS {
                                unfinished_status = StatusCode::BAD_GATEWAY;
                                break;
                            }
                        } else {
                            // HONEST MIRROR (2026-07-24): between turns, the connection-scoped
                            // anchor died with this socket. Keeping the downstream open would let
                            // codex's one-bit liveness model (`is_closed()` ⇒ keep the anchor
                            // ledger) believe an anchor that no longer exists, guaranteeing its
                            // next anchored delta a client-visible `previous_response_not_found`
                            // round-trip. Close BOTH legs instead: codex sees its socket die,
                            // wipes its ledger (`client.rs::websocket_connection`), reconnects,
                            // and full-resends natively — silent, no failed attempt, exactly the
                            // direct-connection behavior. The counter label distinguishes the
                            // deliberate idle-budget let-go from a genuine upstream drop.
                            relay_metrics.record(if idle_budget_expired {
                                "honest_close_idle_budget"
                            } else {
                                "honest_close_upstream_drop"
                            });
                            if turn_active {
                                // A live turn with nothing replayable (its telemetry outlived its
                                // buffered frame) — record the teardown as a transport loss, not a
                                // client cancel.
                                unfinished_status = StatusCode::BAD_GATEWAY;
                            }
                            let _ = downstream.send(Message::Close(None)).await;
                            break;
                        }
                    }
                }
            }
        }
    }
    if let Some(turn) = turn_telemetry {
        let routing = if unfinished_status.as_u16() == 499 {
            // The downstream client disappeared (or could no longer receive frames). That is not
            // evidence that the selected account is unhealthy; only release the turn lease.
            WsRoutingOutcome::TerminalNoWriteback
        } else {
            WsRoutingOutcome::TransportLoss
        };
        turn.finish(
            &state,
            &account.id,
            WsTurnTerminal {
                status: unfinished_status,
                usage: None,
                routing,
                protocol_outcome: if unfinished_status.as_u16() == 499 {
                    polyflare_store::RequestProtocolOutcome::Cancelled
                } else {
                    polyflare_store::RequestProtocolOutcome::TransportLost
                },
            },
        )
        .await;
    }
}

/// True when `frame` is a generating `response.create` carrying a top-level
/// `previous_response_id` — a CLIENT-PLANNED DELTA. codex-rs sets that field exactly when `input`
/// holds only the new suffix (`client.rs::prepare_websocket_request`), so an anchored frame is
/// never a full resend: it must never be replayed anchorless, and its anchor-miss can only be
/// resolved by the client resending. The frame stays in memory only; this reads two bounded
/// fields and the presence of a third, never content.
fn is_anchored_generating_frame(frame: &str) -> bool {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(frame) else {
        return false;
    };
    let Some(object) = value.as_object() else {
        return false;
    };
    object.get("type").and_then(serde_json::Value::as_str) == Some("response.create")
        && object.get("generate").and_then(serde_json::Value::as_bool) != Some(false)
        && object.contains_key("previous_response_id")
}

/// The forged downstream error envelope that makes codex-rs retry the turn as a FULL resend.
///
/// Shape-matched to the genuine 60-minute cap envelope: codex-rs's
/// `parse_wrapped_websocket_error_event` / `map_wrapped_websocket_error_event`
/// (`codex-api/src/endpoint/responses_websocket.rs`) map `error.code ==
/// "websocket_connection_limit_reached"` to `ApiError::Retryable` -> `CodexErr::Stream` ->
/// `is_retryable() == true`, and the failed attempt leaves the client's per-connection delta
/// ledger unresolved (`last_response_rx` never sees a completed id), so the bounded in-place
/// retry resends the full history with no anchor (`client.rs::prepare_websocket_request`). The
/// code constant is reused from `polyflare_codex::ws` — the same one the pump's own classifier
/// matches upstream — so the two can never drift.
fn client_resend_error_frame() -> String {
    serde_json::json!({
        "type": "error",
        "status": 409,
        "error": {
            "code": WS_CONNECTION_LIMIT_CODE,
            "message": "Conversation anchor is no longer available on this connection; \
                        retrying resends the full conversation.",
        },
    })
    .to_string()
}

/// Send a client `Text` frame upstream VERBATIM, re-dialing the SAME `account` first if the
/// upstream is currently dead. One transparent re-dial + resend on a send failure.
///
/// Returns a typed outcome so a handshake 401 can enter the synchronized refresh path instead of
/// being confused with transport exhaustion. Successful sends report whether a redial occurred so
/// the caller can maintain [`MAX_RECONNECTS_WITHOUT_PROGRESS`].
enum SendClientOutcome {
    Sent { redialed: bool },
    Unauthorized,
    AttemptBudgetExhausted,
    Failed,
}

#[allow(clippy::too_many_arguments)]
async fn send_client_text(
    upstream: &mut Option<WsConn>,
    headers: &HeaderMap,
    account: &Account,
    relay_contract: &WsRelayContract,
    state: &AppState,
    pool: Option<&str>,
    text: String,
    logical_turn_key: Option<&str>,
) -> SendClientOutcome {
    let mut redialed = false;
    for _ in 0..2 {
        if upstream.is_none() {
            match super::redial_for_scope(state, headers, account, relay_contract, pool).await {
                RedialOutcome::Connected(conn) => *upstream = Some(*conn),
                RedialOutcome::Unauthorized => return SendClientOutcome::Unauthorized,
                RedialOutcome::ContractDrift | RedialOutcome::Unavailable => {
                    return SendClientOutcome::Failed;
                }
            }
            redialed = true;
        }
        let conn = upstream.as_mut().unwrap();
        if !state.runtime.try_consume_logical_turn_attempt(
            logical_turn_key,
            state.runtime_settings.max_account_attempts(),
            unix_now(),
        ) {
            return SendClientOutcome::AttemptBudgetExhausted;
        }
        // Clone so a failed send can be retried verbatim against the re-dialed connection.
        if conn.send_text(text.clone()).await.is_ok() {
            return SendClientOutcome::Sent { redialed };
        }
        *upstream = None; // dead — the loop re-dials once more and re-sends.
    }
    SendClientOutcome::Failed
}

#[cfg(test)]
mod tests {
    use super::{
        classify_upstream_signal, client_resend_error_frame, is_anchored_generating_frame,
        UpstreamSignal,
    };

    #[test]
    fn anchored_generating_delta_is_resend_eligible() {
        let frame = r#"{"type":"response.create","previous_response_id":"resp_1","input":[{"role":"user","content":"the delta suffix"}]}"#;
        assert!(is_anchored_generating_frame(frame));
    }

    #[test]
    fn prewarm_anchorless_and_malformed_frames_are_not_resend_eligible() {
        // A `generate:false` prewarm is not a user turn — codex's prewarm error handling is not
        // the turn-retry path, so it must fall through to the verbatim forward.
        assert!(!is_anchored_generating_frame(
            r#"{"type":"response.create","generate":false,"previous_response_id":"resp_1","input":[]}"#
        ));
        // An anchorless frame IS the full history — it cannot anchor-miss, and a resend signal
        // would gain nothing.
        assert!(!is_anchored_generating_frame(
            r#"{"type":"response.create","input":[{"role":"user","content":"full"}]}"#
        ));
        // Only a top-level anchor counts — nested evidence inside `input` must not qualify.
        assert!(!is_anchored_generating_frame(
            r#"{"type":"response.create","input":[{"previous_response_id":"nested","content":"x"}]}"#
        ));
        assert!(!is_anchored_generating_frame(
            r#"{"type":"other","previous_response_id":"resp_1"}"#
        ));
        assert!(!is_anchored_generating_frame("not json"));
        assert!(!is_anchored_generating_frame(r#""a json string""#));
    }

    #[test]
    fn forged_resend_frame_matches_the_codex_retryable_cap_shape() {
        let frame = client_resend_error_frame();
        let value: serde_json::Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(value["type"], "error");
        assert_eq!(
            value["error"]["code"],
            polyflare_codex::ws::WS_CONNECTION_LIMIT_CODE,
            "must be the ONE code codex-rs maps to ApiError::Retryable (in-place retry with an \
             unresolved delta ledger => a FULL anchorless resend)"
        );
        // Pin against drift with our own classifier: the forged frame must be exactly the shape
        // the pump itself recognizes as the retryable cap when it arrives FROM upstream.
        assert!(matches!(
            classify_upstream_signal(&frame),
            UpstreamSignal::ConnectionLimit
        ));
    }
}
