//! Ingress: derive continuity ctx → prepare → ownership pre-filter → execute under the watchdog →
//! relay. Client-facing errors carry generic bodies (never a token, URL, or internal Display).

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::{Json, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use polyflare_codex::oauth::{classify_failure, should_refresh, OAuthError};
use polyflare_core::{
    Account, AccountId, PreparedRequest, RecoveryPlan, RequestCtx, ResponseStream, SelectionCtx,
};
use polyflare_store::PlainTokens;

use crate::app::AppState;
use crate::session_key::derive_request_ctx;
use crate::snapshot::assemble_snapshots;
use crate::watchdog::{
    apply_ownership, execute_recovery, execute_with_watchdog, signal_client_stream, RouteDecision,
};

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn account_unavailable() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, "account unavailable").into_response()
}

fn internal_error() -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
}

fn no_eligible() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, "no eligible account").into_response()
}

fn stream_response(stream: ResponseStream) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .body(Body::from_stream(stream))
        .expect("valid response")
}

/// Load + decrypt + refresh-if-stale the selected account, returning the core `Account` to execute
/// with, or a ready client-facing error `Response`.
async fn resolve_core_account(
    state: &AppState,
    picked: &AccountId,
    now: i64,
) -> Result<Account, Response> {
    let repo = state.store.accounts();
    let account = match repo.get(picked.as_str()).await {
        Ok(Some(a)) => a,
        Ok(None) | Err(_) => return Err(internal_error()),
    };
    let mut tokens = match repo.decrypt_tokens(picked.as_str(), &state.cipher).await {
        Ok(Some(t)) => t,
        Ok(None) | Err(_) => return Err(internal_error()),
    };
    if should_refresh(account.last_refresh, now) {
        match state.oauth.refresh(&tokens.refresh_token).await {
            Ok(refreshed) => {
                let new = PlainTokens {
                    access_token: refreshed.tokens.access_token,
                    refresh_token: refreshed.tokens.refresh_token,
                    id_token: refreshed.tokens.id_token,
                };
                let _ = repo
                    .update_tokens(picked.as_str(), &new, &state.cipher, now)
                    .await;
                tokens = new;
            }
            Err(OAuthError::Endpoint {
                code: Some(code), ..
            }) => {
                if let Some(status) = classify_failure(&code).status() {
                    let _ = repo.update_status(picked.as_str(), status).await;
                }
                return Err(account_unavailable());
            }
            Err(OAuthError::Endpoint { code: None, .. }) | Err(OAuthError::MalformedJwt(_)) => {
                let _ = repo.update_status(picked.as_str(), "reauth_required").await;
                return Err(account_unavailable());
            }
            Err(OAuthError::Transport(_)) => {}
        }
    }
    Ok(Account {
        id: account.id,
        base_url: state.upstream_base_url.clone(),
        bearer_token: tokens.access_token,
    })
}

pub async fn responses_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let model = body
        .get("model")
        .and_then(|m| m.as_str())
        .unwrap_or_default()
        .to_string();
    let now = unix_now();

    // C3: derive continuity ctx from headers + body.
    let ctx: RequestCtx = derive_request_ctx(&headers, &body);
    let req = PreparedRequest { body, model };

    // C4: prepare (resolve owner + arm + recovery plan).
    let prepared = match state.continuity.prepare(req, &ctx).await {
        Ok(p) => p,
        Err(_) => return internal_error(),
    };

    let snapshots = match assemble_snapshots(&state.store).await {
        Ok(s) => s,
        Err(_) => return internal_error(),
    };
    let sel_ctx = SelectionCtx {
        now,
        require_security_work_authorized: false,
        rng_seed: None,
        session_id: ctx.session_id.clone(),
    };
    let session_key = prepared.directive.session_key.clone();

    // C5: ownership pre-filter.
    match apply_ownership(
        &prepared.directive,
        &snapshots,
        state.selector.as_ref(),
        &sel_ctx,
    ) {
        RouteDecision::Route(id) => {
            let account = match resolve_core_account(&state, &id, now).await {
                Ok(a) => a,
                Err(r) => return r,
            };
            match execute_with_watchdog(
                state.executor.as_ref(),
                state.continuity.clone(),
                prepared,
                &account,
                id,
                ctx,
            )
            .await
            {
                Ok(stream) => stream_response(stream),
                Err(_) => (StatusCode::BAD_GATEWAY, "upstream error").into_response(),
            }
        }
        RouteDecision::Recover => {
            // Owner pinned but ineligible: recover on a freshly-selected account (full pool), or
            // signal the client if the input is a bare tail.
            match prepared.directive.recovery {
                RecoveryPlan::ResendFull { anchorless_req } => {
                    let fresh = match state.selector.pick(&snapshots, &sel_ctx) {
                        Some(id) => id,
                        None => return no_eligible(),
                    };
                    let account = match resolve_core_account(&state, &fresh, now).await {
                        Ok(a) => a,
                        Err(r) => return r,
                    };
                    match execute_recovery(
                        state.executor.as_ref(),
                        state.continuity.clone(),
                        anchorless_req,
                        &account,
                        fresh,
                        ctx,
                        session_key,
                    )
                    .await
                    {
                        Ok(stream) => stream_response(stream),
                        Err(_) => (StatusCode::BAD_GATEWAY, "upstream error").into_response(),
                    }
                }
                RecoveryPlan::SignalClient => {
                    let owner = prepared
                        .directive
                        .pin_account
                        .clone()
                        .unwrap_or_else(|| AccountId::from("unknown"));
                    let stream =
                        signal_client_stream(state.continuity.clone(), ctx, owner, session_key)
                            .await;
                    stream_response(stream)
                }
                RecoveryPlan::None => internal_error(),
            }
        }
        RouteDecision::NoEligibleAccount => no_eligible(),
    }
}
