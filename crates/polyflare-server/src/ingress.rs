//! Ingress: derive continuity ctx → prepare → ownership pre-filter → execute under the watchdog →
//! relay. Client-facing errors carry generic bodies (never a token, URL, or internal Display).

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::{Json, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use polyflare_anthropic::AnthropicToResponses;
use polyflare_codex::oauth::{classify_failure, should_refresh, OAuthError};
use polyflare_core::{
    Account, AccountId, Continuity, ContinuityDirective, NoopContinuity, Prepared, PreparedRequest,
    Provider, RecoveryPlan, RequestCtx, ResponseStream, SelectionCtx, Translator,
};
use polyflare_store::PlainTokens;

use crate::alias::{self, ModelAlias};
use crate::app::AppState;
use crate::session_key::derive_request_ctx;
use crate::snapshot::{assemble_snapshots, filter_by_provider};
use crate::translate_stream::wrap_translating_stream;
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
        // F2: serialize concurrent refreshes of the SAME account. OpenAI rotates the refresh token
        // on first use, so N parallel refreshes would leave the losers presenting a dead token and
        // wrongly mark the account `reauth_required`. Acquire the per-account lock, then double-check
        // staleness — a peer may have already refreshed (and persisted) while we waited for the lock.
        let lock = state.refresh_locks.handle(picked);
        let _guard = lock.lock().await;
        let fresh_account = match repo.get(picked.as_str()).await {
            Ok(Some(a)) => a,
            Ok(None) | Err(_) => return Err(internal_error()),
        };
        // F2 (failure-path single-mark): a peer that held this lock may have failed its refresh and
        // marked the account non-active; `last_refresh` is unchanged on failure, so bail here rather
        // than re-hitting OAuth with our own now-dead token (which would re-mark it once per waiter).
        if fresh_account.status != "active" {
            return Err(account_unavailable());
        }
        if should_refresh(fresh_account.last_refresh, now) {
            match state.oauth.refresh(&tokens.refresh_token).await {
                Ok(refreshed) => {
                    let new = PlainTokens {
                        access_token: refreshed.tokens.access_token,
                        refresh_token: refreshed.tokens.refresh_token,
                        id_token: refreshed.tokens.id_token,
                    };
                    if let Err(e) = repo
                        .update_tokens(picked.as_str(), &new, &state.cipher, now)
                        .await
                    {
                        // Refresh succeeded and `new` is valid in-memory for THIS request; don't fail
                        // over a persist error — surface it (content-safe). Observability is M5.
                        eprintln!("polyflare: failed to persist refreshed tokens: {e}");
                    }
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
        } else {
            // A peer already refreshed (and persisted) while we waited for the lock: pick up their
            // fresh tokens instead of calling refresh again with our now-stale copy.
            match repo.decrypt_tokens(picked.as_str(), &state.cipher).await {
                Ok(Some(t)) => tokens = t,
                Ok(None) | Err(_) => return Err(internal_error()),
            }
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

/// The `/v1/messages` ingress entrypoint. A client `model` string that `alias::lookup_alias` maps
/// to a Codex target (SPEC-M4 §3.6 — the M4b headline feature) takes the cross-provider translated
/// path; everything else (no alias, or an alias whose target is itself Anthropic) takes the native
/// same-format path, unchanged.
pub async fn messages_handler(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let model = body
        .get("model")
        .and_then(|m| m.as_str())
        .unwrap_or_default()
        .to_string();

    match alias::lookup_alias(&model) {
        Some(model_alias) if model_alias.target_provider == Provider::Codex => {
            messages_handler_codex_aliased(state, body, model_alias).await
        }
        _ => messages_handler_native(state, body, model).await,
    }
}

/// The native Anthropic-Messages ingress path: no alias applies, so this relays straight to an
/// Anthropic-provider account. Continuity is a no-op here (SPEC-M4 §3.7: the Anthropic backend has
/// no `previous_response_id`-style anchor), so every request is `Disarmed` and
/// `execute_with_watchdog`'s Disarmed branch just relays — the wedge machinery never arms.
async fn messages_handler_native(
    state: Arc<AppState>,
    body: serde_json::Value,
    model: String,
) -> Response {
    let now = unix_now();
    let req = PreparedRequest { body, model };
    let ctx = RequestCtx::default();

    let prepared = match NoopContinuity.prepare(req, &ctx).await {
        Ok(p) => p,
        Err(_) => return internal_error(),
    };

    let snapshots = match assemble_snapshots(&state.store).await {
        Ok(s) => s,
        Err(_) => return internal_error(),
    };
    // M4a has no cross-format translator (that's M4b): `/v1/messages` may only ever pick an
    // Anthropic-provider account — the exact mirror of `/responses`'s Codex-only filter above.
    let snapshots = filter_by_provider(&snapshots, Provider::Anthropic);
    let sel_ctx = SelectionCtx {
        now,
        require_security_work_authorized: false,
        rng_seed: None,
        session_id: None,
    };
    let picked = match state.selector.pick(&snapshots, &sel_ctx) {
        Some(id) => id,
        None => return no_eligible(),
    };
    let (account, provider) = match resolve_core_account(&state, &picked, now).await {
        Ok(a) => a,
        Err(r) => return r,
    };

    match execute_with_watchdog(
        state.executor_for(provider).as_ref(),
        Arc::new(NoopContinuity) as Arc<dyn Continuity>,
        prepared,
        &account,
        picked,
        ctx,
    )
    .await
    {
        Ok(stream) => stream_response(stream),
        Err(_) => (StatusCode::BAD_GATEWAY, "upstream error").into_response(),
    }
}

/// M4b-wiring: a client model string aliased to a Codex target (SPEC-M4 §3.6). Translates the
/// Anthropic-Messages request body into OpenAI-Responses via the per-turn stateful
/// `AnthropicToResponses` translator (`translate_request`), remaps `model` to the alias's target
/// and payload-overrides `reasoning.effort` when the alias specifies one, routes to the Codex pool
/// (the exact mirror of `/responses`'s partitioning), and — on success — wraps the raw
/// OpenAI-Responses response stream with the SAME translator instance
/// (`translate_stream::wrap_translating_stream`) so the client sees Anthropic-Messages SSE.
///
/// Continuity is a no-op here too: this translated turn never round-trips a Codex
/// `previous_response_id` back to an Anthropic client (SPEC-M4 §3.7's anchor-based
/// continuity/watchdog machinery is Codex-native-request-shaped only), so — like the native path
/// above — every request is `Disarmed` and the watchdog never arms.
async fn messages_handler_codex_aliased(
    state: Arc<AppState>,
    body: serde_json::Value,
    model_alias: ModelAlias,
) -> Response {
    let now = unix_now();
    let mut translator = AnthropicToResponses::new();
    let mut translated_body = translator.translate_request(body);
    translated_body["model"] = serde_json::Value::String(model_alias.target_model.clone());
    if let Some(effort) = &model_alias.reasoning_effort {
        // U2/U4: confirm Codex effort payload shape — `{"reasoning":{"effort":...}}` is the
        // documented OpenAI-Responses request field; unverified end-to-end against a live Codex
        // backend.
        translated_body["reasoning"] = serde_json::json!({ "effort": effort });
    }

    let req = PreparedRequest {
        body: translated_body,
        model: model_alias.target_model,
    };
    let ctx = RequestCtx::default();

    let prepared = match NoopContinuity.prepare(req, &ctx).await {
        Ok(p) => p,
        Err(_) => return internal_error(),
    };

    let snapshots = match assemble_snapshots(&state.store).await {
        Ok(s) => s,
        Err(_) => return internal_error(),
    };
    // The mirror of `/responses`'s Codex-only filter: an aliased-to-Codex turn may only ever pick
    // a Codex-provider account, regardless of what `/v1/messages` itself would otherwise select.
    let snapshots = filter_by_provider(&snapshots, Provider::Codex);
    let sel_ctx = SelectionCtx {
        now,
        require_security_work_authorized: false,
        rng_seed: None,
        session_id: None,
    };
    let picked = match state.selector.pick(&snapshots, &sel_ctx) {
        Some(id) => id,
        None => return no_eligible(),
    };
    let (account, provider) = match resolve_core_account(&state, &picked, now).await {
        Ok(a) => a,
        Err(r) => return r,
    };

    match execute_with_watchdog(
        state.executor_for(provider).as_ref(),
        Arc::new(NoopContinuity) as Arc<dyn Continuity>,
        prepared,
        &account,
        picked,
        ctx,
    )
    .await
    {
        Ok(stream) => {
            let translated_stream =
                wrap_translating_stream(stream, Box::new(translator) as Box<dyn Translator>);
            stream_response(translated_stream)
        }
        Err(_) => (StatusCode::BAD_GATEWAY, "upstream error").into_response(),
    }
}
