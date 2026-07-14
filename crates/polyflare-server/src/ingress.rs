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
    Account, AccountId, ContinuityDirective, Prepared, PreparedRequest, Provider, RecoveryPlan,
    RequestCtx, ResponseStream, SelectionCtx,
};
use polyflare_store::PlainTokens;

use crate::app::AppState;
use crate::session_key::derive_request_ctx;
use crate::snapshot::{assemble_snapshots, filter_by_provider};
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
/// with plus its `Provider`, or a ready client-facing error `Response`.
async fn resolve_core_account(
    state: &AppState,
    picked: &AccountId,
    now: i64,
) -> Result<(Account, Provider), Response> {
    let repo = state.store.accounts();
    let account = match repo.get(picked.as_str()).await {
        Ok(Some(a)) => a,
        Ok(None) | Err(_) => return Err(internal_error()),
    };
    let provider: Provider = match account.provider.parse() {
        Ok(p) => p,
        Err(_) => return Err(internal_error()),
    };
    let mut tokens = match repo.decrypt_tokens(picked.as_str(), &state.cipher).await {
        Ok(Some(t)) => t,
        Ok(None) | Err(_) => return Err(internal_error()),
    };
    // Refresh-on-stale is Codex-specific (the only OAuth client AppState holds today); Anthropic
    // subscription-OAuth refresh is Task 7 (VERIFY-gated — no confirmed endpoint/client_id yet).
    // An Anthropic account's stored access_token is used as-is until Task 7 lands.
    if provider == Provider::Codex && should_refresh(account.last_refresh, now) {
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
    Ok((
        Account {
            id: account.id,
            base_url: state.upstream_base_url_for(provider).to_string(),
            bearer_token: tokens.access_token,
        },
        provider,
    ))
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
    // M4a has no cross-format translator (that's M4b): `/responses` may only ever pick a
    // Codex-provider account.
    let snapshots = filter_by_provider(&snapshots, Provider::Codex);
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
            let (account, provider) = match resolve_core_account(&state, &id, now).await {
                Ok(a) => a,
                Err(r) => return r,
            };
            match execute_with_watchdog(
                state.executor_for(provider).as_ref(),
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
                    let (account, provider) = match resolve_core_account(&state, &fresh, now).await
                    {
                        Ok(a) => a,
                        Err(r) => return r,
                    };
                    match execute_recovery(
                        state.executor_for(provider).as_ref(),
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
                RecoveryPlan::None => {
                    // No anchor ⇒ this request is self-sufficient (nothing to resume), so a
                    // pinned-but-ineligible owner (cooldown / rate-limited / reauth_required /
                    // a stale Soft session-row pin) is NOT fatal: fail over to any eligible
                    // account from the FULL candidate pool, ignoring the pin, and relay as a
                    // normal (Disarmed) request. `prepared.req` is still owned here — only
                    // `directive.recovery` was moved by the outer match.
                    match state.selector.pick(&snapshots, &sel_ctx) {
                        Some(fresh) => {
                            let (account, provider) =
                                match resolve_core_account(&state, &fresh, now).await {
                                    Ok(a) => a,
                                    Err(r) => return r,
                                };
                            let fallback = Prepared {
                                req: prepared.req,
                                directive: ContinuityDirective {
                                    pin_account: None,
                                    watchdog: prepared.directive.watchdog,
                                    recovery: RecoveryPlan::None,
                                    session_key: prepared.directive.session_key.clone(),
                                },
                            };
                            match execute_with_watchdog(
                                state.executor_for(provider).as_ref(),
                                state.continuity.clone(),
                                fallback,
                                &account,
                                fresh,
                                ctx,
                            )
                            .await
                            {
                                Ok(stream) => stream_response(stream),
                                Err(_) => {
                                    (StatusCode::BAD_GATEWAY, "upstream error").into_response()
                                }
                            }
                        }
                        None => no_eligible(),
                    }
                }
            }
        }
        RouteDecision::NoEligibleAccount => no_eligible(),
    }
}
