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
//! **Phase 2 (this revision): transparent same-account reconnect.** The upstream is now
//! `Option<WsConn>` rather than a bare `WsConn` — it goes `None` on ANY upstream drop (a network
//! blip, an idle server-side close, or the 60-minute `websocket_connection_limit_reached` cap) and
//! is re-dialed against the SAME pinned `account` on the next client `Text` frame (or eagerly, for
//! the cap signal — see below). The client's downstream socket is NEVER closed by an upstream drop;
//! only the client itself closing (`Close`/`None`/`Err` on `downstream.recv()`) is real teardown.
//! When the upstream is `None`, the `select!` arm polling it is disabled via the `if upstream.is_some()`
//! guard, so the loop parks purely on the client — the next client `Text` frame is what re-dials and
//! repopulates `Some(upstream)`. This is the buffer-free flush point: nothing is queued while the
//! upstream is down, the client simply doesn't hear back until it sends again.
//!
//! - `UpstreamSignal::ConnectionLimit` (the 60-min cap) is INTERCEPTED — never forwarded to the
//!   client — and triggers an EAGER re-dial of the same account (rather than waiting for the next
//!   client frame), since the cap is a clean, expected, server-initiated boundary the client should
//!   never even see.
//! - `UpstreamSignal::Normal` / `AnchorMissing` / `Error(_)` are all forward-only in this phase
//!   (Phase-3's Task 4 adds the bench-and-move behavior on `Error`; here it is treated exactly like
//!   `Normal`).
//!
//! **Content-free:** no frame body is ever logged here. The ONLY body inspection is
//! [`super::sniff::sniff_completed_id`] (reads `type` + `response.id`) and
//! [`super::signal::classify_upstream_signal`] (reads `type`, `error.code`, `status`,
//! `headers["retry-after"]`) — never anything else, never printed. No `tracing`/`log`/`println!`/
//! `eprintln!` anywhere in this module.

use axum::extract::ws::{Message, WebSocket};
use axum::http::HeaderMap;

use polyflare_codex::WsConn;
use polyflare_core::Account;

use super::redial::redial_upstream;
use super::signal::{classify_upstream_signal, UpstreamSignal};
use super::sniff::sniff_completed_id;

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
///   any other classification is forwarded to the client VERBATIM FIRST (the client is never held
///   up on the ownership write below), then sniffed for a `response.completed` id, and if present
///   `on_completed_id` is awaited with it (the caller records ownership via `Continuity::observe`).
///   A closed upstream (`Ok(None)`) or a read error (`Err(_)`) marks the upstream dead WITHOUT
///   tearing down the client — the loop simply stops polling it until the client's next frame
///   re-dials.
///
/// `on_completed_id` is `Fn`, not `FnOnce`, because a single socket can carry many turns over its
/// life — each sniffed id is a fresh, independent call.
pub(crate) async fn run_pump<F, Fut>(
    mut downstream: WebSocket,
    upstream_conn: WsConn,
    headers: HeaderMap,
    account: Account,
    on_completed_id: F,
) where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mut upstream: Option<WsConn> = Some(upstream_conn);
    loop {
        tokio::select! {
            down = downstream.recv() => {
                match down {
                    Some(Ok(Message::Text(t))) => {
                        if !send_client_text(&mut upstream, &headers, &account, t.to_string()).await {
                            break;
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
                                if upstream.is_none() {
                                    break;
                                }
                            }
                            // Normal / AnchorMissing / Error: forward VERBATIM first, then sniff.
                            // (Phase-3's Task 4 adds the bench+move behavior on Error; here it is
                            // forward-only, exactly like Normal.)
                            _ => {
                                if downstream.send(Message::Text(text.clone().into())).await.is_err() {
                                    break;
                                }
                                if let Some(id) = sniff_completed_id(&text) {
                                    on_completed_id(id).await;
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
/// upstream is currently dead. One transparent re-dial + resend on a send failure. Returns `false`
/// when the upstream cannot be (re-)established at all — the caller tears the connection down in
/// that case; any other outcome returns `true` and the frame has been sent.
async fn send_client_text(
    upstream: &mut Option<WsConn>,
    headers: &HeaderMap,
    account: &Account,
    text: String,
) -> bool {
    for _ in 0..2 {
        if upstream.is_none() {
            *upstream = redial_upstream(headers, account).await;
            if upstream.is_none() {
                return false;
            }
        }
        let conn = upstream.as_mut().unwrap();
        // Clone so a failed send can be retried verbatim against the re-dialed connection.
        if conn.send_text(text.clone()).await.is_ok() {
            return true;
        }
        *upstream = None; // dead — the loop re-dials once more and re-sends.
    }
    false
}
