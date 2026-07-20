//! The bidirectional verbatim relay pump (Task 6) — the WS-downstream relay's core loop, once a
//! downstream `WebSocket` has been upgraded and an upstream `WsConn` dialed to the conversation's
//! pinned owner account.
//!
//! **Two sockets, one plain forward — not codex-rs's mpsc/command-channel shape.** codex-rs's own
//! `responses_websocket.rs:62-125` fans a SINGLE socket's outbound side across multiple command
//! producers via an mpsc; that shape solves a different problem (many producers, one socket). The
//! relay has exactly two sockets and a plain bidirectional forward between them — the same shape as
//! codex-lb's `proxy_websocket` two-direction relay. A single `tokio::select!` loop owning both
//! sockets directly is YAGNI-correct for Phase-1 (no reconnect/buffering yet — Phase 2's job): each
//! iteration creates a fresh `recv()`/`recv_text()` future, so `select!` dropping the non-matched
//! arm's future is harmless (nothing was consumed from either socket without being observed), and
//! forwarding to the OTHER socket inside the matched arm has a free `&mut` with no `.split()` or
//! channel required.
//!
//! **Content-free:** no frame body is ever logged here. The ONLY body inspection is
//! [`super::sniff::sniff_completed_id`], reading just `type` + `response.id` — never anything else,
//! never printed.

use axum::extract::ws::{Message, WebSocket};

use polyflare_codex::WsConn;

use super::sniff::sniff_completed_id;

/// Drive the relay for one WS-downstream conversation until either leg closes or errors.
///
/// - **client → backend:** a `Text` frame is forwarded to `upstream` VERBATIM (`to_string()`, never
///   reparsed); an inbound `Ping` is auto-ponged back to the CLIENT inline (codex-rs fidelity: the
///   relay never *initiates* a ping itself); `Pong`/`Binary` are ignored (codex WS is text-only,
///   content-free — never logged); a `Close`, a closed socket (`None`), or a read error tears down
///   both legs.
/// - **backend → client:** a `Text` frame is forwarded to the downstream client VERBATIM FIRST, so
///   the client is never held up on the ownership write; only THEN is it sniffed for a
///   `response.completed` id, and if present `on_completed_id` is awaited with it (the caller
///   records ownership via `Continuity::observe`). A closed socket (`Ok(None)`) or a read error
///   tears down both legs.
///
/// `on_completed_id` is `Fn`, not `FnOnce`, because a single socket can carry many turns over its
/// life — each sniffed id is a fresh, independent call.
pub(crate) async fn run_pump<F, Fut>(
    mut downstream: WebSocket,
    mut upstream: WsConn,
    on_completed_id: F,
) where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    loop {
        tokio::select! {
            down = downstream.recv() => {
                match down {
                    Some(Ok(Message::Text(t))) => {
                        if upstream.send_text(t.to_string()).await.is_err() {
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
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                }
            }
            up = upstream.recv_text() => {
                match up {
                    Ok(Some(text)) => {
                        // Forward FIRST — the client must never wait on the ownership write below.
                        if downstream.send(Message::Text(text.clone().into())).await.is_err() {
                            break;
                        }
                        if let Some(id) = sniff_completed_id(&text) {
                            on_completed_id(id).await;
                        }
                    }
                    Ok(None) | Err(_) => break,
                }
            }
        }
    }
}
