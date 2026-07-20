//! WS-downstream relay (client-facing WebSocket transport), gated by `POLYFLARE_WS_DOWNSTREAM`
//! (`crate::app::AppState::ws_downstream`, default off). When on, the codex CLI's WS-handshake
//! `GET /responses` (+ `/{pool}/responses`) routes here instead of to
//! `crate::ingress::websocket_fallback_handler`'s `426`, and PolyFlare ACCEPTS the upgrade.
//!
//! **Task 2 scope (this module today): the accept SEAM only.** [`responses_ws_handler`] performs the
//! WebSocket upgrade and then immediately drops the socket — closing it cleanly. The real
//! bidirectional relay pump (account-pinning, upstream WS dial via `polyflare_codex::ws::WsConn`,
//! verbatim frame forwarding, the content-free `response.id` ownership sniff, and transparent
//! same-account reconnect) is Tasks 3-6. The handler's SIGNATURE is deliberately stable now — it
//! already takes the handshake `HeaderMap` (Tasks 3-5 derive the conversation `session_key` from it,
//! per `crate::session_key`) and the shared [`AppState`] (selection/ownership/dial seams) — so later
//! tasks fill in the pump without re-plumbing the route.
//!
//! **Content-free (inviolable, `design §8`):** the stub reads no frame; even the real relay only ever
//! touches the content-free response-id sniff + handshake-header normalization. No frame body is ever
//! logged or persisted (PolyFlare's permanent limit).

use std::sync::Arc;

use axum::extract::ws::{WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::Response;

use crate::app::AppState;

mod owner;
mod session;

#[allow(unused_imports)]
// wired into the relay pump by Tasks 4-6 (dial + reconnect + observe).
pub(crate) use owner::{resolve_owner, RelayError};
pub(crate) use session::ws_session_key;

/// Accepts the codex CLI's downstream WebSocket upgrade on `/responses` (routed here only when
/// `AppState::ws_downstream` is on — see `crate::app::build_app`). Returns the `101 Switching
/// Protocols` upgrade response; the post-upgrade future runs [`relay_stub`].
///
/// `state` and `headers` are bound now (not `_`-ignored) because Tasks 3-5 need them: `headers`
/// carries the handshake identity the conversation `session_key` is derived from, and `state` is the
/// entry to the selection/ownership engine and the upstream WS dial. Wiring them into the signature
/// today keeps it stable for those tasks.
pub async fn responses_ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    // Seams held for Tasks 3-6. The conversation's content-free owner-lookup key is derivable from
    // the handshake headers ALONE (Phase-0: `session-id`/`thread-id`/`x-codex-window-id`), so it is
    // computed here — later tasks resolve/pin the owning account on it. The stub drops it.
    let _session_key = ws_session_key(&headers);
    let _ = &state;
    ws.on_upgrade(relay_stub)
}

/// Task 2 stub post-upgrade future: accept then immediately drop the socket, which closes it
/// cleanly. Reads nothing off the wire — content-free by construction. Tasks 3-6 replace this with
/// the real bidirectional pump.
async fn relay_stub(socket: WebSocket) {
    let _ = socket;
}
