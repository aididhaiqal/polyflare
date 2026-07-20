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
//! **Phase 2: transparent same-account reconnect.** The upstream is now `Option<WsConn>` rather
//! than a bare `WsConn` — it goes `None` on ANY upstream drop (a network blip, an idle server-side
//! close, or the 60-minute `websocket_connection_limit_reached` cap) and is re-dialed against the
//! SAME pinned `account` on the next client `Text` frame (or eagerly, for the cap signal and for a
//! mid-turn drop — see below). The client's downstream socket is NEVER closed by an upstream drop;
//! only the client itself closing (`Close`/`None`/`Err` on `downstream.recv()`) is real teardown.
//! When the upstream is `None`, the `select!` arm polling it is disabled via the `if upstream.
//! is_some()` guard, so the loop parks purely on the client — the next client `Text` frame is what
//! re-dials and repopulates `Some(upstream)`, UNLESS this task's eager mid-turn re-dial (below) has
//! already done so first.
//!
//! - `UpstreamSignal::ConnectionLimit` (the 60-min cap) is INTERCEPTED — never forwarded to the
//!   client — and triggers an EAGER re-dial of the same account (rather than waiting for the next
//!   client frame), since the cap is a clean, expected, server-initiated boundary the client should
//!   never even see.
//! - `UpstreamSignal::Normal` / `AnchorMissing` are forward-only: the client resolves an anchor-miss
//!   itself (a stripped-anchor resend), exactly as it does over HTTP-SSE.
//!
//! **Task 4 (mid-turn replay): buffered in-flight replay on a mid-turn cap/drop.** `in_flight: Option<String>` holds
//! the raw client `response.create` frame of the CURRENT turn — set the moment it's forwarded
//! upstream, cleared the moment `sniff_completed_id` sees that turn's `response.completed`. If the
//! cap or a network drop fires WHILE a turn is in flight, the client's own socket stays open and its
//! resend logic never fires (it's waiting on a reply, not re-sending) — so BOTH the `ConnectionLimit`
//! arm and the upstream-drop arm (`Ok(None) | Err(_)`) now, after a successful same-account re-dial,
//! replay the buffered frame verbatim on the fresh socket via `WsConn::send_text` (no reparse) so the
//! interrupted turn resumes invisibly instead of stalling on the client's ~290s read-idle. Between
//! turns `in_flight` is `None`, so both arms are a no-op beyond the existing reconnect behavior. This
//! is same-account only: a cross-account move (`UpstreamSignal::Error`) never replays — the client
//! full-resends there by design (unchanged). Held IN MEMORY ONLY, never logged.
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
//! **Content-free:** no frame body is ever logged here. The ONLY body inspection is
//! [`super::sniff::sniff_completed_id`] (reads `type` + `response.id`) and
//! [`super::signal::classify_upstream_signal`] (reads `type`, `error.code`, `status`,
//! `headers["retry-after"]`) — never anything else, never printed. No `tracing`/`log`/`println!`/
//! `eprintln!` anywhere in this module.

use std::future::Future;
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use axum::http::HeaderMap;

use polyflare_codex::WsConn;
use polyflare_core::{Account, AccountId, FailureSignal};

use crate::observability::RelayMetrics;

use super::redial::redial_upstream;
use super::signal::{classify_upstream_signal, UpstreamSignal};
use super::sniff::sniff_completed_id;

/// How many CONSECUTIVE re-dials (eager cap re-dial, client-send re-dial, an exhaustion-move, or
/// this task's mid-turn in-flight replay) are tolerated without a single forwarded `response.completed` in
/// between. Crossing this tears the connection down rather than spinning forever against a
/// pathological upstream that re-caps, re-errors, or re-drops on every fresh dial.
const MAX_RECONNECTS_WITHOUT_PROGRESS: u32 = 5;

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
///   `Normal`/`AnchorMissing` are forwarded VERBATIM then sniffed for a `response.completed` id — if
///   present, `on_completed_id` is awaited with the CURRENT account + the id (the caller records
///   ownership via `Continuity::observe`), the reconnect-without-progress bound resets to 0, and
///   `in_flight` is cleared (this task — the turn finished, nothing left to replay).
///   `Error(sig)` is also forwarded VERBATIM FIRST, then `on_upstream_error` is awaited with the
///   CURRENT account and the signal — on `Some((new_account, new_upstream))` the pump's account/
///   upstream become whatever it returns (same account = retry, different = a genuine move); `None`
///   tears the connection down. A cross-account move never replays `in_flight` — the client full-
///   resends there by design. A closed upstream (`Ok(None)`) or a read error (`Err(_)`) marks the
///   upstream dead; if a turn was in flight (this task) the pump EAGERLY re-dials the same account and
///   replays `in_flight` right here (the client is waiting, not going to resend on its own) —
///   otherwise (between turns) it simply stops polling the upstream until the client's next frame
///   re-dials via `send_client_text`.
///
/// `on_completed_id` and `on_upstream_error` are both `Fn`, not `FnOnce`, because a single socket can
/// carry many turns (and possibly many moves) over its life — each call is fresh and independent.
///
/// **Task 5:** `relay_metrics` is a content-free counter handle (`crate::observability::
/// RelayMetrics`) bumped at exactly three decision points — every same-account re-dial (the eager
/// `ConnectionLimit` re-dial, `send_client_text`'s internal re-dial, and the retry-in-place branch
/// of an `on_upstream_error` call), every cross-account move (the other branch of `on_upstream_
/// error`), and every `AnchorMissing` frame that arrives while the pinned account has NOT changed
/// since the last forwarded `response.completed` (the residual same-account non-resumption Design
/// Note 4 calls out). `account_changed_since_completed` tracks exactly that: set `true` the moment
/// `on_upstream_error` returns a DIFFERENT account, reset to `false` the moment a `response.
/// completed` is forwarded. Never carries anything beyond the three fixed label strings.
pub(crate) async fn run_pump<F, Fut, G, GFut>(
    mut downstream: WebSocket,
    upstream_conn: WsConn,
    headers: HeaderMap,
    account: Account,
    on_completed_id: F,
    on_upstream_error: G,
    relay_metrics: Arc<RelayMetrics>,
) where
    F: Fn(AccountId, String) -> Fut,
    Fut: Future<Output = ()>,
    G: Fn(Account, FailureSignal) -> GFut,
    GFut: Future<Output = Option<(Account, WsConn)>>,
{
    let mut account = account;
    let mut upstream: Option<WsConn> = Some(upstream_conn);
    let mut reconnects_since_progress: u32 = 0;
    // Task 4 (mid-turn replay): the raw client `response.create` frame of the CURRENT in-flight turn, held in memory
    // ONLY (never logged/persisted) so it can be REPLAYED on a same-account re-dial after a mid-turn
    // cap/drop — so the interrupted turn resumes without the client having to resend. Cleared on the
    // turn's `response.completed`. One in-flight turn per socket (codex's model).
    let mut in_flight: Option<String> = None;
    // Task 5: has the pinned account changed (a move) since the last forwarded
    // `response.completed`? Starts `false` — a fresh connection hasn't moved yet.
    let mut account_changed_since_completed = false;
    loop {
        tokio::select! {
            down = downstream.recv() => {
                match down {
                    Some(Ok(Message::Text(t))) => {
                        let frame = t.to_string();
                        let redialed = send_client_text(&mut upstream, &headers, &account, frame.clone()).await;
                        match redialed {
                            None => break, // upstream could not be (re-)established at all.
                            Some(redial) => {
                                // This task: this turn is now in flight — replayable until it completes
                                // (a same-account cap/drop mid-turn replays THIS frame, not a client
                                // resend).
                                in_flight = Some(frame);
                                if redial {
                                    relay_metrics.record("reconnect_same_account");
                                    reconnects_since_progress += 1;
                                    if reconnects_since_progress > MAX_RECONNECTS_WITHOUT_PROGRESS {
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
            // frame re-dials via `send_client_text` above.
            up = async { upstream.as_mut().unwrap().recv_text().await }, if upstream.is_some() => {
                match up {
                    Ok(Some(text)) => {
                        match classify_upstream_signal(&text) {
                            UpstreamSignal::ConnectionLimit => {
                                // The 60-min server cap: INTERCEPT (never forward) and eagerly
                                // re-dial the SAME account so the client never sees this boundary.
                                upstream = redial_upstream(&headers, &account).await;
                                // Only a SUCCESSFUL re-dial is a reconnect — a failed one tears the
                                // connection down and records nothing, matching the send-path
                                // (`send_client_text` → `None` → `break`) so the counter can't
                                // over-count teardowns.
                                if upstream.is_none() {
                                    break;
                                }
                                // This task: if a turn was in flight when the cap hit, replay it on the
                                // fresh socket so the interrupted turn resumes (same account -> the
                                // anchor resumes). No-op between turns.
                                if let Some(frame) = in_flight.clone() {
                                    if upstream.as_mut().unwrap().send_text(frame).await.is_err() {
                                        break;
                                    }
                                }
                                relay_metrics.record("reconnect_same_account");
                                reconnects_since_progress += 1;
                                if reconnects_since_progress > MAX_RECONNECTS_WITHOUT_PROGRESS {
                                    break;
                                }
                            }
                            // Task 4: a durable error. Forward VERBATIM FIRST (honest — the client
                            // reacts to it exactly as it would over HTTP-SSE), THEN bench + re-select
                            // + re-dial via the caller-provided move engine.
                            UpstreamSignal::Error(sig) => {
                                if downstream.send(Message::Text(text.clone().into())).await.is_err() {
                                    break;
                                }
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
                                            // The in-flight turn belonged to the OLD (now benched)
                                            // account; the client resolves the cross-account
                                            // anchor-miss by full-resending, which re-populates
                                            // `in_flight`. Clear it so a drop on the NEW upstream
                                            // (before that resend) can't replay the stale frame.
                                            in_flight = None;
                                        }
                                        account = new_account; // same account (retry) or a NEW one (moved).
                                        upstream = Some(new_upstream);
                                        reconnects_since_progress += 1;
                                        if reconnects_since_progress > MAX_RECONNECTS_WITHOUT_PROGRESS {
                                            break;
                                        }
                                    }
                                    // No eligible account, or every re-dial attempt failed -> teardown.
                                    None => break,
                                }
                            }
                            // Task 5: an anchor-miss forwarded verbatim (unchanged from Task 3) —
                            // but if the pinned account has NOT changed since the last completed
                            // turn, this is the residual same-account non-resumption the counter
                            // exists to measure. A miss right after a move (the flag still `true`)
                            // is the EXPECTED cross-account case and is deliberately NOT counted.
                            UpstreamSignal::AnchorMissing => {
                                if downstream.send(Message::Text(text.clone().into())).await.is_err() {
                                    break;
                                }
                                if !account_changed_since_completed {
                                    relay_metrics.record("same_account_anchor_miss");
                                }
                            }
                            // Normal: forward VERBATIM, then sniff for ownership.
                            UpstreamSignal::Normal => {
                                if downstream.send(Message::Text(text.clone().into())).await.is_err() {
                                    break;
                                }
                                if let Some(id) = sniff_completed_id(&text) {
                                    // The CURRENT account, not whatever the connection started on —
                                    // a completed turn after a move must re-home to the account that
                                    // actually produced it, never the stale/benched original.
                                    on_completed_id(AccountId::from(account.id.as_str()), id).await;
                                    // A completed turn is real progress: reset the no-progress bound
                                    // and the move flag — a completed turn on the current account
                                    // means any LATER anchor-miss is once again same-account.
                                    reconnects_since_progress = 0;
                                    account_changed_since_completed = false;
                                    in_flight = None; // This task: the turn finished — nothing to replay.
                                }
                            }
                        }
                    }
                    // The upstream dropped (network blip / idle close / a mid-stream close). Keep
                    // the downstream OPEN; mark the upstream dead; the next client frame re-dials
                    // the same account via `send_client_text`.
                    Ok(None) | Err(_) => {
                        upstream = None;
                        // This task: a mid-turn drop (a turn is in flight) — the client is waiting and
                        // won't resend on its own. Eagerly re-dial the SAME account and replay the
                        // buffered frame so the turn resumes. Between turns (in_flight None) keep the
                        // lazy behavior: the next client frame re-dials via `send_client_text`.
                        if in_flight.is_some() {
                            upstream = redial_upstream(&headers, &account).await;
                            if upstream.is_none() {
                                break;
                            }
                            if let Some(frame) = in_flight.clone() {
                                if upstream.as_mut().unwrap().send_text(frame).await.is_err() {
                                    break;
                                }
                            }
                            relay_metrics.record("reconnect_same_account");
                            reconnects_since_progress += 1;
                            if reconnects_since_progress > MAX_RECONNECTS_WITHOUT_PROGRESS {
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Send a client `Text` frame upstream VERBATIM, re-dialing the SAME `account` first if the
/// upstream is currently dead. One transparent re-dial + resend on a send failure.
///
/// Returns `None` when the upstream cannot be (re-)established at all — the caller tears the
/// connection down in that case. Otherwise returns `Some(redialed)`, where `redialed` is whether a
/// re-dial happened along the way (`true`) or the already-live upstream was used as-is (`false`) —
/// the caller feeds this into [`MAX_RECONNECTS_WITHOUT_PROGRESS`]'s bound (Task-4 E: every re-dial
/// site counts, not just the exhaustion-move's).
async fn send_client_text(
    upstream: &mut Option<WsConn>,
    headers: &HeaderMap,
    account: &Account,
    text: String,
) -> Option<bool> {
    let mut redialed = false;
    for _ in 0..2 {
        if upstream.is_none() {
            *upstream = redial_upstream(headers, account).await;
            redialed = true;
            if upstream.is_none() {
                return None;
            }
        }
        let conn = upstream.as_mut().unwrap();
        // Clone so a failed send can be retried verbatim against the re-dialed connection.
        if conn.send_text(text.clone()).await.is_ok() {
            return Some(redialed);
        }
        *upstream = None; // dead — the loop re-dials once more and re-sends.
    }
    None
}
