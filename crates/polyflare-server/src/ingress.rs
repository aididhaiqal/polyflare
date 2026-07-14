//! Ingress: assemble candidate snapshots → select an account → refresh its token if stale →
//! decrypt → relay the executor's stream. Client-facing errors carry generic bodies (never a
//! token, an upstream URL, or an internal error `Display`).

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::{Json, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};

use polyflare_codex::oauth::{classify_failure, should_refresh, OAuthError};
use polyflare_core::{Account, PreparedRequest, SelectionCtx};
use polyflare_store::PlainTokens;

use crate::app::AppState;
use crate::snapshot::assemble_snapshots;

/// Current unix time in seconds (0 on the impossible pre-epoch error).
fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Generic client-facing response for a request whose selected account cannot be served (its
/// token could not be refreshed and the account was marked, or the token endpoint failed). The
/// body is generic — never a token, a URL, or an internal error `Display`.
fn account_unavailable() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, "account unavailable").into_response()
}

pub async fn responses_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let model = body
        .get("model")
        .and_then(|m| m.as_str())
        .unwrap_or_default()
        .to_string();
    let now = unix_now();

    // 1. Assemble candidate snapshots from the store.
    let snapshots = match assemble_snapshots(&state.store).await {
        Ok(s) => s,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    };

    // 2. Select an account. No eligible account → 503.
    let ctx = SelectionCtx {
        now,
        require_security_work_authorized: false,
        rng_seed: None,
        session_id: None,
    };
    let picked = match state.selector.pick(&snapshots, &ctx) {
        Some(id) => id,
        None => return (StatusCode::SERVICE_UNAVAILABLE, "no eligible account").into_response(),
    };

    // 3. Load the selected account.
    let repo = state.store.accounts();
    let account = match repo.get(picked.as_str()).await {
        Ok(Some(a)) => a,
        Ok(None) | Err(_) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
    };

    // 4. Decrypt tokens; refresh if the stored token is stale (>8 days).
    let mut tokens = match repo.decrypt_tokens(picked.as_str(), &state.cipher).await {
        Ok(Some(t)) => t,
        Ok(None) | Err(_) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
    };
    if should_refresh(account.last_refresh, now) {
        // A refresh failure MUST NOT loop: M2b has no live error_count/cooldown tracking, so an
        // account left `active` after a failed refresh stays eligible, gets re-selected, is still
        // stale, and would refresh+fail forever. Every non-transport failure therefore marks the
        // account (making it ineligible next time) and stops THIS request — only a genuine network
        // blip (`Transport`) is a no-op that proceeds with the current token.
        match state.oauth.refresh(&tokens.refresh_token).await {
            Ok(refreshed) => {
                let new = PlainTokens {
                    access_token: refreshed.tokens.access_token,
                    refresh_token: refreshed.tokens.refresh_token,
                    id_token: refreshed.tokens.id_token,
                };
                // Persist best-effort. If the write fails, `last_refresh` stays stale, so the next
                // request simply re-refreshes (harmless duplicate refresh) — an acceptable
                // trade-off vs. dropping a request whose refresh actually succeeded.
                let _ = repo
                    .update_tokens(picked.as_str(), &new, &state.cipher, now)
                    .await;
                tokens = new;
            }
            // Token-endpoint error with a parseable code → classify it. Permanent codes mark the
            // account (reauth_required / deactivated); a Transient-classified code (e.g.
            // server_error) leaves the account eligible for a later retry. Either way the refresh
            // did not complete, so this request cannot be served with the 8-day-stale token.
            Err(OAuthError::Endpoint {
                code: Some(code), ..
            }) => {
                if let Some(status) = classify_failure(&code).status() {
                    let _ = repo.update_status(picked.as_str(), status).await;
                }
                return account_unavailable();
            }
            // HTTP failure with no parseable code, or a broken/rotated id_token that won't decode:
            // unclassifiable, so mark reauth_required to break the loop and stop.
            Err(OAuthError::Endpoint { code: None, .. }) | Err(OAuthError::MalformedJwt(_)) => {
                let _ = repo.update_status(picked.as_str(), "reauth_required").await;
                return account_unavailable();
            }
            // Genuine network blip — the ONLY no-op: leave the account eligible and proceed with
            // the current token (which may still be valid).
            Err(OAuthError::Transport(_)) => {}
        }
    }

    // 5. Build the core Account (shared upstream base URL + per-account bearer) and execute.
    let core_account = Account {
        id: account.id,
        base_url: state.upstream_base_url.clone(),
        bearer_token: tokens.access_token,
    };
    let req = PreparedRequest { body, model };
    match state.executor.execute(req, &core_account).await {
        Ok(stream) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/event-stream")
            .body(Body::from_stream(stream))
            .expect("valid response"),
        // Generic 502 — never forward the upstream error Display (may carry the URL).
        Err(_) => (StatusCode::BAD_GATEWAY, "upstream error").into_response(),
    }
}
