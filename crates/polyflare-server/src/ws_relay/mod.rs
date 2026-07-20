//! WS-downstream relay (client-facing WebSocket transport), gated by `POLYFLARE_WS_DOWNSTREAM`
//! (`crate::app::AppState::ws_downstream`, default off). When on, the codex CLI's WS-handshake
//! `GET /responses` (+ `/{pool}/responses`) routes here instead of to
//! `crate::ingress::websocket_fallback_handler`'s `426`, and PolyFlare ACCEPTS the upgrade.
//!
//! **Task 6 (this module now): the real relay.** [`responses_ws_handler`] accepts the upgrade; its
//! post-upgrade future ([`relay`]) resolves the conversation's pinned owner account
//! ([`resolve_owner`], Task 3), dials that account's upstream Codex WS ([`dial_owner_upstream`],
//! Task 4), and drives the bidirectional verbatim pump ([`pump::run_pump`], Task 6) for the
//! connection's life. Every `response.completed` frame's id is sniffed content-free
//! ([`sniff::sniff_completed_id`]) and fed into the SAME continuity engine the HTTP path uses
//! (`Continuity::observe` — see `polyflare_core::TurnOutcome::Completed`), so a later turn — HTTP or
//! WS, same or reconnected socket — resolves the same owner. On either resolve/dial failure the
//! downstream socket is simply dropped (a clean close); MVP has no re-select loop (a later phase's
//! job).
//!
//! **Content-free (inviolable, `design §8`):** no frame body is ever logged or persisted anywhere in
//! this module or its submodules; the ONLY body inspection at all is [`sniff::sniff_completed_id`]
//! reading `type` + `response.id`. This is PolyFlare's permanent limit.

use std::sync::Arc;

use axum::extract::ws::{WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::Response;

use polyflare_core::{AccountId, RequestCtx, SessionKey, TurnOutcome};

use crate::app::AppState;

mod owner;
mod pump;
mod session;
mod signal;
mod sniff;

pub(crate) use owner::{dial_owner_upstream, resolve_owner};
pub(crate) use session::ws_session_key;

/// Accepts the codex CLI's downstream WebSocket upgrade on `/responses` (routed here only when
/// `AppState::ws_downstream` is on — see `crate::app::build_app`). Returns the `101 Switching
/// Protocols` upgrade response; the post-upgrade future runs [`relay`].
pub async fn responses_ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    // The conversation's content-free owner-lookup key is derivable from the handshake headers
    // ALONE (Phase-0: `session-id`/`thread-id`/`x-codex-window-id`) — computed here, before the
    // upgrade, so it moves into the post-upgrade future unchanged.
    let session_key = ws_session_key(&headers);
    ws.on_upgrade(move |socket| relay(socket, state, headers, session_key))
}

/// The real post-upgrade relay future (Task 6): resolve the conversation's owner, dial its upstream
/// WS, then pump frames both ways for the connection's life.
///
/// Any resolve/dial failure closes the downstream socket cleanly (by simply dropping it — Rust's
/// ordinary drop semantics close the connection, exactly as Task 2's original stub relied on) and
/// returns without ever reading a frame. Never logs the failure: `RelayError`'s variants are
/// generic-by-design (see `owner.rs`), but even so this path deliberately doesn't surface them
/// anywhere — content-free.
async fn relay(
    socket: WebSocket,
    state: Arc<AppState>,
    headers: HeaderMap,
    session_key: SessionKey,
) {
    let Ok(account) = resolve_owner(&state, &session_key).await else {
        return;
    };
    let Ok(upstream) = dial_owner_upstream(&headers, &account).await else {
        return;
    };

    // The ownership-recording callback the pump invokes on every sniffed `response.completed` id.
    // `Fn`, not `FnOnce`: one socket can carry many turns over its life, so each captured handle is
    // cloned fresh per call rather than consumed. Reuses the EXISTING continuity engine's `observe`
    // (wedge-sacred: `watchdog.rs`/`continuity.rs`/`ObservingStream` are never touched) — this one
    // call writes BOTH the session owner and the `response_id -> owner` anchor (it delegates to
    // `record_completion` internally). A failed write must not tear the relay down, hence `let _ =`.
    let continuity = state.continuity.clone();
    let account_id = AccountId::from(account.id.as_str());
    let on_completed_id = move |response_id: String| {
        let continuity = continuity.clone();
        let session_key = session_key.clone();
        let account_id = account_id.clone();
        async move {
            let _ = continuity
                .observe(
                    TurnOutcome::Completed {
                        session_key: Some(session_key),
                        account: account_id,
                        response_id: Some(response_id),
                        // The relay is content-free by construction — it never parses input bodies,
                        // so these stay empty/zero (see the task's decision log: the WS relay's
                        // wedge-avoidance is account-pinning, not fingerprint comparison).
                        input_fingerprint: String::new(),
                        input_count: 0,
                        reasoning: None,
                    },
                    &RequestCtx::default(),
                )
                .await;
        }
    };

    pump::run_pump(socket, upstream, on_completed_id).await;
}
