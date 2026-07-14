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
        match state.oauth.refresh(&tokens.refresh_token).await {
            Ok(refreshed) => {
                let new = PlainTokens {
                    access_token: refreshed.tokens.access_token,
                    refresh_token: refreshed.tokens.refresh_token,
                    id_token: refreshed.tokens.id_token,
                };
                // Persist best-effort; a write failure must not drop the request.
                let _ = repo
                    .update_tokens(picked.as_str(), &new, &state.cipher, now)
                    .await;
                tokens = new;
            }
            Err(OAuthError::Endpoint {
                code: Some(code), ..
            }) => {
                // Mark the account per the classified failure; proceed with the current token
                // (re-selection / retry orchestration is M3).
                if let Some(status) = classify_failure(&code).status() {
                    let _ = repo.update_status(picked.as_str(), status).await;
                }
            }
            Err(_) => {} // transient / network → proceed with the current token
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
