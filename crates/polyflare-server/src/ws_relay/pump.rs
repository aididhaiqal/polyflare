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
//! SAME pinned `account` on the next client `Text` frame (or eagerly, for the cap signal — see
//! below). The client's downstream socket is NEVER closed by an upstream drop; only the client
//! itself closing (`Close`/`None`/`Err` on `downstream.recv()`) is real teardown. When the upstream
//! is `None`, the `select!` arm polling it is disabled via the `if upstream.is_some()` guard, so the
//! loop parks purely on the client — the next client `Text` frame is what re-dials and repopulates
//! `Some(upstream)`. This is the buffer-free flush point: nothing is queued while the upstream is
//! down, the client simply doesn't hear back until it sends again.
//!
//! - `UpstreamSignal::ConnectionLimit` (the 60-min cap) is INTERCEPTED — never forwarded to the
//!   client — and triggers an EAGER re-dial of the same account (rather than waiting for the next
//!   client frame), since the cap is a clean, expected, server-initiated boundary the client should
//!   never even see.
//! - `UpstreamSignal::Normal` / `AnchorMissing` are forward-only: the client resolves an anchor-miss
//!   itself (a stripped-anchor resend), exactly as it does over HTTP-SSE.
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
//! on every re-dial (the eager cap re-dial, `send_client_text`'s internal re-dial, and the Task-4
//! move) and reset to 0 whenever a `response.completed` is forwarded (real progress). Crossing
//! [`MAX_RECONNECTS_WITHOUT_PROGRESS`] tears the connection down — bounding a pathological upstream
//! that re-caps/re-errors on every fresh dial without ever completing a turn.
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

/// How many CONSECUTIVE re-dials (eager cap re-dial, client-send re-dial, or a Task-4 move) are
/// tolerated without a single forwarded `response.completed` in between. Crossing this tears the
/// connection down rather than spinning forever against a pathological upstream that re-caps or
/// re-errors on every fresh dial.
const MAX_RECONNECTS_WITHOUT_PROGRESS: u32 = 5;

/// Drive the relay for one WS-downstream conversation until the CLIENT goes away (a `Close`, a
/// closed socket, or a read error on the downstream leg) — an upstream drop alone never ends this.
///
/// - **client → backend:** a `Text` frame is sent to the upstream VERBATIM via
///   [`send_client_text`], which re-dials the SAME `account` first if the upstream is currently
///   dead (or after a send failure, once); an inbound `Ping` is auto-ponged back to the CLIENT
///   inline (codex-rs fidelity: the relay never *initiates* a ping itself); `Pong`/`Binary` are
///   ignored (codex WS is text-only, content-free — never logged); a `Close`, a closed socket
///   (`None`), or a read error tears down both legs — this is the ONLY real teardown path.
/// - **backend → client:** a `Text` frame is classified via [`classify_upstream_signal`] first.
///   `ConnectionLimit` is intercepted (never forwarded) and triggers an eager same-account re-dial;
///   `Normal`/`AnchorMissing` are forwarded VERBATIM then sniffed for a `response.completed` id — if
///   present, `on_completed_id` is awaited with the CURRENT account + the id (the caller records
///   ownership via `Continuity::observe`), and the reconnect-without-progress bound resets to 0.
///   `Error(sig)` is also forwarded VERBATIM FIRST, then `on_upstream_error` is awaited with the
///   CURRENT account and the signal — on `Some((new_account, new_upstream))` the pump's account/
///   upstream become whatever it returns (same account = retry, different = a genuine move); `None`
///   tears the connection down. A closed upstream (`Ok(None)`) or a read error (`Err(_)`) marks the
///   upstream dead WITHOUT tearing down the client — the loop simply stops polling it until the
///   client's next frame re-dials.
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
    // Task 5: has the pinned account changed (a move) since the last forwarded
    // `response.completed`? Starts `false` — a fresh connection hasn't moved yet.
    let mut account_changed_since_completed = false;
    loop {
        tokio::select! {
            down = downstream.recv() => {
                match down {
                    Some(Ok(Message::Text(t))) => {
                        let redialed = send_client_text(&mut upstream, &headers, &account, t.to_string()).await;
                        match redialed {
                            None => break, // upstream could not be (re-)established at all.
                            Some(true) => {
                                relay_metrics.record("reconnect_same_account");
                                reconnects_since_progress += 1;
                                if reconnects_since_progress > MAX_RECONNECTS_WITHOUT_PROGRESS {
                                    break;
                                }
                            }
                            Some(false) => {}
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
                                }
                            }
                        }
                    }
                    // The upstream dropped (network blip / idle close / a mid-stream close). Keep
                    // the downstream OPEN; mark the upstream dead; the next client frame re-dials
                    // the same account via `send_client_text`.
                    Ok(None) | Err(_) => {
                        upstream = None;
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
