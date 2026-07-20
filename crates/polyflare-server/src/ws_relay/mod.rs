//! WS-downstream relay (client-facing WebSocket transport), gated by `POLYFLARE_WS_DOWNSTREAM`
//! (`crate::app::AppState::ws_downstream`, default off). When on, the codex CLI's WS-handshake
//! `GET /responses` (+ `/{pool}/responses`) routes here instead of to
//! `crate::ingress::websocket_fallback_handler`'s `426`, and PolyFlare ACCEPTS the upgrade.
//!
//! **Task 6: the real relay.** [`responses_ws_handler`] accepts the upgrade; its post-upgrade future
//! ([`relay`]) resolves the conversation's pinned owner account ([`resolve_owner`], Task 3), dials
//! that account's upstream Codex WS ([`dial_owner_upstream`], Task 4), and drives the bidirectional
//! verbatim pump ([`pump::run_pump`], Task 6) for the connection's life. Every `response.completed`
//! frame's id is sniffed content-free ([`sniff::sniff_completed_id`]) and fed into the SAME
//! continuity engine the HTTP path uses (`Continuity::observe` — see
//! `polyflare_core::TurnOutcome::Completed`), so a later turn — HTTP or WS, same or reconnected
//! socket — resolves the same owner. On either resolve/dial failure the downstream socket is simply
//! dropped (a clean close).
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
//! **Phase 2/3 Task 5: reconnect/move/residual-anchor-miss counters.** [`relay`] threads
//! `state.relay_metrics` (`crate::observability::RelayMetrics`) into [`pump::run_pump`], which bumps
//! it at exactly its three same-account-reconnect, cross-account-move, and same-account-anchor-miss
//! decision points — a content-free (three fixed labels only) signal for the deferred watchdog
//! decision. See `pump::run_pump`'s own doc comment for the exact bump sites.
//!
//! **Content-free (inviolable, `design §8`):** no frame body is ever logged or persisted anywhere in
//! this module or its submodules; the ONLY body inspection at all is [`sniff::sniff_completed_id`]
//! reading `type` + `response.id`. This is PolyFlare's permanent limit.

use std::sync::Arc;

use axum::extract::ws::{WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::Response;

use polyflare_core::{Account, AccountId, FailureSignal, RequestCtx, SessionKey, TurnOutcome};

use crate::app::AppState;

mod owner;
mod pump;
mod redial;
mod session;
mod signal;
mod sniff;

pub(crate) use owner::{dial_owner_upstream, resolve_owner};
use redial::redial_upstream;
pub(crate) use session::ws_session_key;

/// Mirrors `control.rs`'s / `continuity.rs`'s own `unix_now` — a plain wall-clock read, no shared
/// helper exists at the crate root so each site that needs "now, in seconds" defines its own trivial
/// one rather than growing a needless cross-module dependency for a two-line function.
fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

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

    // Phase 3 Task 4: the exhaustion-move engine. Called by the pump on a durable upstream error
    // (`UpstreamSignal::Error`) with the CURRENT account + the classified signal; benches that
    // account, re-resolves the owner (skipping the just-benched one via the fresh cooldown overlay),
    // and re-dials whatever was picked. Returns `None` (teardown) only if no account is eligible at
    // all, or the winning candidate's upstream WS could not be reached even after
    // `redial_upstream`'s bounded retries — both clean, expected exhaustion outcomes, never logged.
    //
    // Reuse, not reinvention (wedge-sacred): `bench_account_for_failure` is the SAME policy
    // `record_failure` applies on the HTTP path; `resolve_owner` is the SAME owner-affine resolution
    // Task 3 already built; `redial_upstream` is Task 2's bounded same-account-or-new-account dial
    // helper. No new selection or dial logic is written here.
    let on_upstream_error = {
        let state = state.clone();
        let headers = headers.clone();
        let session_key = session_key.clone();
        move |current: Account, sig: FailureSignal| {
            let state = state.clone();
            let headers = headers.clone();
            let session_key = session_key.clone();
            async move {
                let now = unix_now();
                crate::ingress::bench_account_for_failure(
                    &state,
                    &AccountId::from(current.id.as_str()),
                    Some(&sig),
                    now,
                )
                .await;
                let new_account = resolve_owner(&state, &session_key).await.ok()?;
                let new_upstream = redial_upstream(&headers, &new_account).await?;
                Some((new_account, new_upstream))
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
        state.relay_metrics.clone(),
    )
    .await;
}
