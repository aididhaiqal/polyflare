//! Ingress: derive continuity ctx → prepare → ownership pre-filter → execute under the watchdog →
//! relay. Client-facing errors carry generic bodies (never a token, URL, or internal Display).

use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

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
use polyflare_store::{PlainTokens, RequestLogRecord, RequestLogRepo};

use crate::alias::{self, ModelAlias};
use crate::app::AppState;
use crate::fingerprint_capture::{append_fingerprint_capture, capture_request_fingerprint};
use crate::observability::RequestLog;
use crate::session_key::derive_request_ctx;
use crate::snapshot::filter_by_provider;
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

/// M5 capture-fixture mechanism: if `state.capture_fingerprint_path` is set, append this
/// request's content-safe structural fingerprint (see `crate::fingerprint_capture`) to it. A
/// no-op (single `Option` check) when unset — the normal, always-disabled-by-default case. A
/// write failure (e.g. disk full) is logged content-safely and never fails the request itself.
fn maybe_capture_fingerprint(state: &AppState, method: &str, path: &str, headers: &HeaderMap) {
    if let Some(golden_path) = &state.capture_fingerprint_path {
        let record = capture_request_fingerprint(method, path, headers);
        if let Err(e) = append_fingerprint_capture(golden_path, &record) {
            eprintln!("polyflare: fingerprint capture write failed: {e}");
        }
    }
}

/// Inbound headers dropped before a native `/responses` request's surviving codex-identity headers
/// are captured into `PreparedRequest::forward_headers` (see that field's doc). `host` /
/// `content-length` / `connection` / `transfer-encoding` are hop-by-hop transport framing that must
/// never be replayed to a different upstream connection; `authorization` is dropped because the
/// executor always overrides it with the SELECTED account's own bearer token — forwarding the
/// client's own (irrelevant to upstream, and never to be logged/relayed) bearer would be at best
/// ignored and at worst a real secret leaking onto the wire under the wrong identity.
///
/// This is deliberately a small, conservative drop-list, not codex-lb's full native-vs-SDK
/// normalization (`_build_upstream_headers`/`_normalize_non_native_upstream_fingerprint` in
/// `codex-lb/app/core/clients/proxy.py`) — for now this just forwards what a native client sent;
/// full normalization is a follow-up.
const DROPPED_INBOUND_HEADERS: &[&str] = &[
    "host",
    "content-length",
    "connection",
    "transfer-encoding",
    "authorization",
];

/// Filters a native `/responses` request's inbound `HeaderMap` down to the surviving
/// codex-identity headers to forward upstream untouched (see `DROPPED_INBOUND_HEADERS`). A header
/// value that isn't valid visible-ASCII (`to_str()` fails) is silently skipped rather than
/// forwarded lossily.
fn forward_headers_from_inbound(headers: &HeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .filter(|(name, _)| {
            !DROPPED_INBOUND_HEADERS
                .iter()
                .any(|dropped| name.as_str().eq_ignore_ascii_case(dropped))
        })
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|v| (name.as_str().to_string(), v.to_string()))
        })
        .collect()
}

/// Synthesizes the codex-identity `forward_headers` for a TRANSLATED request (a Claude request
/// routed to the Codex pool): there is no real Codex client fingerprint to forward here, unlike the
/// native `/responses` path above, so this is where `polyflare_codex::codex_headers` (built from a
/// local `openai/codex` source read — see that module's doc) genuinely belongs.
fn synthesize_codex_forward_headers(
    body: &serde_json::Value,
    codex_version: &str,
) -> Vec<(String, String)> {
    use polyflare_codex::codex_headers::{
        codex_user_agent, conversation_key, originator, TurnIdentity,
    };

    let identity = TurnIdentity::derive(&conversation_key(body));
    vec![
        ("user-agent".to_string(), codex_user_agent(codex_version)),
        ("originator".to_string(), originator().to_string()),
        ("accept".to_string(), "text/event-stream".to_string()),
        ("session-id".to_string(), identity.session_id.clone()),
        ("thread-id".to_string(), identity.thread_id.clone()),
        (
            "x-client-request-id".to_string(),
            identity.thread_id.clone(),
        ),
        ("x-codex-window-id".to_string(), identity.window_id.clone()),
        (
            "x-codex-turn-metadata".to_string(),
            identity.turn_metadata_json(),
        ),
    ]
}

fn stream_response(stream: ResponseStream) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .body(Body::from_stream(stream))
        .expect("valid response")
}

/// Persist a request-outcome row off the response path: fire-and-forget (a detached task, mirroring
/// codex-lb's pattern), so a slow or failing DB write never delays or fails the client's request.
/// The row is content-free by construction — it comes from `RequestLog::record`, the same audited
/// field set the tracing event carries (see `crate::observability`). `repo` is taken by value (it
/// owns a cheap pool clone) so the caller can build it before `state` is consumed by the handler.
fn spawn_persist_request_log(repo: RequestLogRepo, record: RequestLogRecord) {
    tokio::spawn(async move {
        if let Err(e) = repo.insert(&record).await {
            tracing::warn!(
                target: "polyflare_server::request",
                error = %e,
                "request_log persist failed"
            );
        }
    });
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
                        // (A successful write bumps the store generation, auto-invalidating the
                        // account cache — no explicit invalidation needed here.)
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
            // The selected account's own ChatGPT id travels as the `chatgpt-account-id` header
            // paired with its Bearer (see `Account::chatgpt_account_id` / executor). Taken from the
            // stored row so it always matches the account whose token we're about to send.
            chatgpt_account_id: account.chatgpt_account_id,
            id: account.id,
            base_url: state.upstream_base_url_for(provider).to_string(),
            bearer_token: tokens.access_token,
        },
        provider,
    ))
}

/// The `/responses` ingress entrypoint. Thin timing + content-safe logging wrapper around
/// [`responses_handler_impl`] — see `crate::observability` for the content-safety constraint on
/// what may be logged.
pub async fn responses_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let start = Instant::now();
    maybe_capture_fingerprint(&state, "POST", "/responses", &headers);
    // Build the log repo BEFORE `state` moves into the impl (it owns a cheap pool clone).
    let log_repo = state.store.request_log();
    let response = responses_handler_impl(state, headers, body).await;
    let log = RequestLog {
        method: "POST",
        path: "/responses",
        // M4a: `/responses` may only ever route to a Codex-provider account — see this fn's
        // `filter_by_provider(&snapshots, Provider::Codex)` call below. The provider is
        // structurally fixed regardless of which branch produced the response (including the
        // early-exit error paths, which never resolve an account at all).
        provider: Provider::Codex,
        aliased: false,
        status: response.status(),
        duration_ms: start.elapsed().as_millis() as u64,
    };
    log.emit();
    spawn_persist_request_log(log_repo, log.record(unix_now()));
    response
}

async fn responses_handler_impl(
    state: Arc<AppState>,
    headers: HeaderMap,
    body: serde_json::Value,
) -> Response {
    let model = body
        .get("model")
        .and_then(|m| m.as_str())
        .unwrap_or_default()
        .to_string();
    let now = unix_now();

    // C3: derive continuity ctx from headers + body.
    let ctx: RequestCtx = derive_request_ctx(&headers, &body);
    // Native path: forward the REAL Codex client's own surviving inbound headers untouched (see
    // `forward_headers_from_inbound`) — this is a genuine Codex client, so its fingerprint is
    // already authentic; synthesizing here would only discard real conversation ids.
    let forward_headers = forward_headers_from_inbound(&headers);
    let req = PreparedRequest {
        body,
        model,
        forward_headers,
    };

    // C4: prepare (resolve owner + arm + recovery plan).
    let prepared = match state.continuity.prepare(req, &ctx).await {
        Ok(p) => p,
        Err(_) => return internal_error(),
    };

    let snapshots = match state.account_cache.snapshots(&state.store).await {
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
/// same-format path, unchanged. Also a thin timing + content-safe logging wrapper (mirrors
/// `responses_handler` above) — see `crate::observability` for the content-safety constraint.
pub async fn messages_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let start = Instant::now();
    maybe_capture_fingerprint(&state, "POST", "/v1/messages", &headers);
    // Build the log repo BEFORE `state` moves into a sub-handler (it owns a cheap pool clone).
    let log_repo = state.store.request_log();
    let model = body
        .get("model")
        .and_then(|m| m.as_str())
        .unwrap_or_default()
        .to_string();

    // Resolved once, up front: which provider this request structurally targets, and whether
    // that's via a model alias — both are decided entirely by `lookup_alias`, independent of
    // whether the downstream relay itself succeeds.
    let alias = alias::lookup_alias(&model);
    let aliased_to_codex = matches!(&alias, Some(a) if a.target_provider == Provider::Codex);
    let provider = if aliased_to_codex {
        Provider::Codex
    } else {
        Provider::Anthropic
    };

    let response = match alias {
        Some(model_alias) if model_alias.target_provider == Provider::Codex => {
            messages_handler_codex_aliased(state, body, model_alias).await
        }
        _ => messages_handler_native(state, body, model).await,
    };

    let log = RequestLog {
        method: "POST",
        path: "/v1/messages",
        provider,
        aliased: aliased_to_codex,
        status: response.status(),
        duration_ms: start.elapsed().as_millis() as u64,
    };
    log.emit();
    spawn_persist_request_log(log_repo, log.record(unix_now()));

    response
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
    // Native Anthropic path: the AnthropicExecutor does not use `forward_headers` (that field is
    // the Codex egress identity set), so there is nothing to forward here.
    let req = PreparedRequest {
        body,
        model,
        forward_headers: vec![],
    };
    let ctx = RequestCtx::default();

    let prepared = match NoopContinuity.prepare(req, &ctx).await {
        Ok(p) => p,
        Err(_) => return internal_error(),
    };

    let snapshots = match state.account_cache.snapshots(&state.store).await {
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

    // Translated path: there is no real Codex client to forward, so SYNTHESIZE codex-rs's identity
    // headers (see `synthesize_codex_forward_headers`). Mirrors codex-lb's forward-native /
    // synthesize-non-native split. The User-Agent's codex version is resolved live (GitHub/npm,
    // cached) so it tracks the real fleet instead of a stale constant; `cached_or_fallback` is a
    // sync, zero-I/O read warmed out-of-band by the background refresh task.
    let forward_headers = synthesize_codex_forward_headers(
        &translated_body,
        &state.codex_version.cached_or_fallback(),
    );
    let req = PreparedRequest {
        body: translated_body,
        model: model_alias.target_model,
        forward_headers,
    };
    let ctx = RequestCtx::default();

    let prepared = match NoopContinuity.prepare(req, &ctx).await {
        Ok(p) => p,
        Err(_) => return internal_error(),
    };

    let snapshots = match state.account_cache.snapshots(&state.store).await {
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
