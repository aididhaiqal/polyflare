//! Transient listener on the fixed OAuth loopback redirect (`localhost:1455/auth/callback`).
//!
//! OpenAI's app registration pins the redirect URI to `http://localhost:1455/auth/callback`, which
//! resolves on the machine running the BROWSER. When the dashboard is opened on the same machine
//! as the server (the common local deployment), binding that port while a browser-method flow is
//! pending lets the server catch the redirect itself and complete the flow hands-free — the
//! dialog's paste-the-URL path stays as the universal fallback (remote browser, port taken by a
//! concurrent `codex login`, …).
//!
//! Lifecycle: [`ensure_listener`] is called on every browser-flow start; one listener task binds
//! `127.0.0.1:1455` and shuts itself down once no pending browser Codex flow remains (checked on
//! a short cadence), releasing the port for other tools. A failed bind is logged at info and
//! otherwise ignored.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::{Query, State};
use axum::response::Html;
use axum::routing::get;
use axum::Router;
use serde::Deserialize;

use crate::account_onboarding::{claim_and_complete, CompleteError};
use crate::app::AppState;

/// Whether the process-wide listener task is alive. Process-wide (not `AppState`) because the
/// bound PORT is process-wide: two `AppState`s (tests) must not fight over it.
static LISTENER_RUNNING: AtomicBool = AtomicBool::new(false);

/// How often the listener re-checks whether any pending browser flow still needs it.
const IDLE_CHECK_INTERVAL: Duration = Duration::from_secs(15);

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Ensure the loopback listener is running while browser flows are pending. Idempotent and
/// non-blocking; bind failure downgrades to the manual paste path.
pub(crate) fn ensure_listener(state: Arc<AppState>) {
    if LISTENER_RUNNING.swap(true, Ordering::SeqCst) {
        return;
    }
    tokio::spawn(async move {
        let listener = match tokio::net::TcpListener::bind(("127.0.0.1", 1455)).await {
            Ok(listener) => listener,
            Err(error) => {
                tracing::info!(
                    %error,
                    "oauth loopback port 1455 unavailable; sign-in falls back to pasting the \
                     redirect URL"
                );
                LISTENER_RUNNING.store(false, Ordering::SeqCst);
                return;
            }
        };
        tracing::info!("oauth loopback listener bound on 127.0.0.1:1455");
        serve(listener, state).await;
        LISTENER_RUNNING.store(false, Ordering::SeqCst);
        tracing::info!("oauth loopback listener released 127.0.0.1:1455");
    });
}

/// Serve callbacks until no pending browser Codex flow remains. Public so integration tests can
/// run it on an ephemeral port.
pub async fn serve(listener: tokio::net::TcpListener, state: Arc<AppState>) {
    let idle_state = state.clone();
    let app = Router::new()
        .route("/auth/callback", get(callback_handler))
        .with_state(state);
    let shutdown = async move {
        loop {
            tokio::time::sleep(IDLE_CHECK_INTERVAL).await;
            match idle_state
                .store
                .onboarding()
                .has_pending_browser_codex_flow(unix_now())
                .await
            {
                Ok(true) => {}
                // No pending flow — release the port. A storage error also releases: holding a
                // fixed port on a broken store helps nobody, and the next flow start re-binds.
                Ok(false) | Err(_) => return,
            }
        }
    };
    if let Err(error) = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
    {
        tracing::warn!(%error, "oauth loopback listener terminated with an error");
    }
}

#[derive(Deserialize)]
struct CallbackParams {
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    error: Option<String>,
}

async fn callback_handler(
    State(app): State<Arc<AppState>>,
    Query(params): Query<CallbackParams>,
) -> Html<String> {
    let Some(state_param) = params.state.filter(|value| !value.is_empty()) else {
        return page(
            "Sign-in not recognized",
            "This callback carried no state token.",
            false,
        );
    };
    let flow = match app
        .store
        .onboarding()
        .get_pending_by_state(&state_param, unix_now())
        .await
    {
        Ok(Some(flow)) => flow,
        Ok(None) => {
            return page(
                "Sign-in link no longer valid",
                "This sign-in was already completed, expired, or belongs to another server. \
                 Start a new sign-in from the PolyFlare dashboard.",
                false,
            )
        }
        Err(_) => {
            return page(
                "Something went wrong",
                "PolyFlare could not look up this sign-in. Paste the redirect URL into the \
                 dashboard dialog instead.",
                false,
            )
        }
    };
    if let Some(error) = params.error.filter(|value| !value.is_empty()) {
        if app
            .store
            .onboarding()
            .claim(&flow.id, unix_now())
            .await
            .ok()
            .flatten()
            .is_some()
        {
            let _ = app
                .store
                .onboarding()
                .fail(&flow.id, "authorize_denied", unix_now())
                .await;
        }
        tracing::warn!(flow_id = %flow.id, error, "oauth loopback callback carried an error");
        return page(
            "Sign-in was not completed",
            "The authorization was cancelled or refused. You can close this tab and start over \
             from the PolyFlare dashboard.",
            false,
        );
    }
    let Some(code) = params.code.filter(|value| !value.is_empty()) else {
        return page(
            "Sign-in not recognized",
            "This callback carried no authorization code. Start a new sign-in from the \
             PolyFlare dashboard.",
            false,
        );
    };

    match claim_and_complete(&app, &flow.id, &code).await {
        Ok(_) => page(
            "Account connected",
            "You can close this tab — the PolyFlare dashboard will update on its own.",
            true,
        ),
        Err(CompleteError::AlreadyUsed) => page(
            "Sign-in already completed",
            "This sign-in was already processed. Check the PolyFlare dashboard.",
            false,
        ),
        Err(CompleteError::Failed("seat_mismatch")) => page(
            "Wrong ChatGPT account",
            "You signed in with a different ChatGPT account than the one being repaired. \
             Nothing was changed — start over from the dashboard and use the matching account.",
            false,
        ),
        Err(CompleteError::Failed(_)) => page(
            "Sign-in could not be completed",
            "The token exchange failed. Start a new sign-in from the PolyFlare dashboard.",
            false,
        ),
    }
}

/// A minimal, dependency-free result page for the browser tab the redirect lands in.
fn page(title: &str, body: &str, ok: bool) -> Html<String> {
    let tone = if ok { "#16a34a" } else { "#b45309" };
    Html(format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>{title}</title></head>\
         <body style=\"font-family:-apple-system,system-ui,sans-serif;display:flex;\
         align-items:center;justify-content:center;min-height:90vh;background:#f8f7f4\">\
         <div style=\"max-width:26rem;padding:2rem;border:1px solid #e2ded6;border-radius:12px;\
         background:#fff\"><h1 style=\"font-size:1.05rem;margin:0;color:{tone}\">{title}</h1>\
         <p style=\"font-size:.85rem;color:#57534e;line-height:1.5\">{body}</p></div></body></html>"
    ))
}
