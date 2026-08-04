//! Authenticated dashboard Codex OAuth onboarding. The browser receives only an authorize URL and
//! a random flow id; PKCE verifier and OAuth tokens remain server-side throughout.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use polyflare_codex::oauth::{generate_pkce, generate_state, REDIRECT_URI};
use polyflare_codex::Refreshed;
use polyflare_store::{Account, OnboardingFlow, PlainTokens};
use serde::{Deserialize, Serialize};

use crate::app::AppState;
use crate::write_api::valid_pool_slug;

const FLOW_TTL_SECONDS: i64 = 10 * 60;

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn safe_error(status: StatusCode, code: &'static str) -> Response {
    (status, Json(serde_json::json!({ "error": code }))).into_response()
}

#[derive(Debug, Deserialize)]
pub struct StartRequest {
    #[serde(default)]
    initial_pool: Option<String>,
    /// When present, this flow re-authenticates exactly this existing account: completion refuses
    /// a callback whose seat belongs to any other account instead of silently updating it.
    #[serde(default)]
    account_id: Option<String>,
    /// `codex` (the default, for compatibility with every existing caller) or `anthropic`.
    #[serde(default)]
    provider: Option<String>,
}

/// Which upstream a flow is onboarding against.
///
/// The two differ in more than a URL. Codex redirects to a fixed loopback port this server can
/// listen on, so its flow can complete hands-free; Anthropic redirects to a page on its own domain
/// that displays a code for the operator to paste back. Modelling that as one enum keeps the
/// difference explicit at every branch instead of scattered string comparisons.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowProvider {
    Codex,
    Anthropic,
}

impl FlowProvider {
    fn parse(raw: Option<&str>) -> Option<Self> {
        match raw.map(str::trim).filter(|v| !v.is_empty()) {
            None | Some("codex") => Some(Self::Codex),
            Some("anthropic") => Some(Self::Anthropic),
            Some(_) => None,
        }
    }

    /// The `provider` value stored on accounts and flows.
    fn wire(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Anthropic => "anthropic",
        }
    }
}

#[derive(Debug, Serialize)]
pub struct StartResponse {
    flow_id: String,
    authorize_url: String,
    expires_at: i64,
}

/// Shared start-request validation: a valid pool slug, and — for a targeted re-auth — a real
/// Codex account. Failing at start beats sending the operator through a whole login only to
/// bounce at completion.
async fn validate_start(
    state: &AppState,
    body: StartRequest,
) -> Result<(Option<String>, Option<String>, FlowProvider), Response> {
    let Some(provider) = FlowProvider::parse(body.provider.as_deref()) else {
        return Err(safe_error(StatusCode::BAD_REQUEST, "unsupported_provider"));
    };
    let initial_pool = body.initial_pool.map(|v| v.trim().to_string());
    if initial_pool
        .as_deref()
        .is_some_and(|slug| !valid_pool_slug(slug))
    {
        return Err(safe_error(StatusCode::BAD_REQUEST, "invalid_pool_slug"));
    }
    let intended_account_id = match body.account_id.map(|v| v.trim().to_string()) {
        // A targeted re-auth must name an account of the SAME provider: signing into Claude
        // cannot repair a Codex seat, and letting the mismatch through would only surface as a
        // confusing seat check after the operator had already logged in.
        Some(id) if !id.is_empty() => match state.store.accounts().get(&id).await {
            Ok(Some(account)) if account.provider == provider.wire() => Some(id),
            Ok(Some(_)) | Ok(None) => {
                return Err(safe_error(StatusCode::NOT_FOUND, "account_not_found"))
            }
            Err(_) => {
                return Err(safe_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "storage_error",
                ))
            }
        },
        _ => None,
    };
    Ok((initial_pool, intended_account_id, provider))
}

pub async fn start_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<StartRequest>,
) -> Response {
    let (initial_pool, intended_account_id, provider) = match validate_start(&state, body).await {
        Ok(validated) => validated,
        Err(response) => return response,
    };
    if provider == FlowProvider::Anthropic {
        return anthropic_start(state, initial_pool, intended_account_id).await;
    }

    let flow_id = format!("oauth_{}", generate_state());
    let oauth_state = generate_state();
    let (verifier, challenge) = generate_pkce();
    let verifier_enc = match state.cipher.encrypt(&verifier) {
        Ok(value) => value,
        Err(_) => return safe_error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error"),
    };
    let now = unix_now();
    if state
        .store
        .onboarding()
        .expire_and_prune(now)
        .await
        .is_err()
    {
        return safe_error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error");
    }
    let expires_at = now + FLOW_TTL_SECONDS;
    let flow = OnboardingFlow {
        id: flow_id.clone(),
        provider: "codex".into(),
        oauth_state: oauth_state.clone(),
        verifier_enc,
        initial_pool,
        status: "pending".into(),
        created_at: now,
        expires_at,
        finished_at: None,
        account_id: None,
        error_code: None,
        // Codex uses the fixed registered loopback redirect, so there is nothing per-flow to record.
        redirect_uri: None,
        intended_account_id,
        method: "browser".into(),
        device_auth_id: None,
        user_code: None,
        interval_seconds: None,
    };
    if state.store.onboarding().create(&flow).await.is_err() {
        return safe_error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error");
    }
    // Same-machine convenience: while this flow is pending, a transient listener on the fixed
    // OAuth redirect port catches the browser's callback and completes the flow hands-free. Bind
    // failures (a concurrent `codex login` owns 1455, or the dashboard runs remotely) degrade
    // silently to the dialog's paste-the-URL path.
    crate::oauth_loopback::ensure_listener(state.clone());

    Json(StartResponse {
        flow_id,
        authorize_url: state.oauth.build_authorize_url(&oauth_state, &challenge),
        expires_at,
    })
    .into_response()
}

/// Begin an Anthropic subscription login.
///
/// # Why this is a separate path rather than a parameter on the Codex one
/// Anthropic has no loopback redirect to listen on. Its reviewed contract redirects to
/// `platform.claude.com/oauth/code/callback`, a page that DISPLAYS a code for the operator to
/// bring back — so the flow is inherently manual, there is no transient listener to start, and the
/// completion step takes a pasted code rather than a captured URL.
///
/// # The client registration is not verified in this repository
/// `oauth_contract` marks `client_id` [`Provenance::UnverifiedInRepo`]: a Messages capture proves
/// the request shape, never the OAuth registration. Its docs say a caller should refuse to start a
/// real login "rather than silently attempting one with a guessed registration".
///
/// An operator who has explicitly asked to add a Claude account is not the silent case that
/// warning is about, so this starts the flow — and reports the provenance in the response, so the
/// dialog can say plainly that a rejection at Anthropic's end may mean the registration is wrong
/// rather than the operator's credentials. Attempting it IS how that value gets verified: no probe
/// from here can do it, because Anthropic's edge answers automated requests with a bot-protection
/// page before OAuth validation ever runs.
async fn anthropic_start(
    state: Arc<AppState>,
    initial_pool: Option<String>,
    intended_account_id: Option<String>,
) -> Response {
    let client = match polyflare_anthropic::oauth::AnthropicOAuthClient::new() {
        Ok(client) => client,
        Err(_) => {
            return safe_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "oauth_client_unavailable",
            )
        }
    };
    let flow_id = format!("oauth_{}", generate_state());
    let oauth_state = polyflare_anthropic::oauth::generate_state();
    let pkce = polyflare_anthropic::oauth::generate_pkce();
    let verifier_enc = match state.cipher.encrypt(&pkce.verifier) {
        Ok(value) => value,
        Err(_) => return safe_error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error"),
    };
    let now = unix_now();
    if state
        .store
        .onboarding()
        .expire_and_prune(now)
        .await
        .is_err()
    {
        return safe_error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error");
    }
    let redirect_uri = client.contract().manual_redirect_uri.to_string();
    let expires_at = now + FLOW_TTL_SECONDS;
    let flow = OnboardingFlow {
        id: flow_id.clone(),
        provider: FlowProvider::Anthropic.wire().into(),
        oauth_state: oauth_state.clone(),
        verifier_enc,
        initial_pool,
        status: "pending".into(),
        created_at: now,
        expires_at,
        finished_at: None,
        account_id: None,
        error_code: None,
        // Unlike Codex's fixed loopback port, the redirect is part of the token-exchange request
        // and must be replayed EXACTLY at completion, so it is recorded per flow.
        redirect_uri: Some(redirect_uri.clone()),
        intended_account_id,
        method: "manual".into(),
        device_auth_id: None,
        user_code: None,
        interval_seconds: None,
    };
    if state.store.onboarding().create(&flow).await.is_err() {
        return safe_error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error");
    }
    Json(serde_json::json!({
        "flow_id": flow_id,
        "authorize_url": client.build_authorize_url(&oauth_state, &pkce, &redirect_uri),
        "expires_at": expires_at,
        "provider": "anthropic",
        // The operator brings a code back by hand; there is no listener to wait on.
        "completion": "paste_code",
        "client_id_verified": client.contract().verified_for_production(),
    }))
    .into_response()
}

/// Finish an Anthropic login from the code the operator pasted.
///
/// Accepts what Anthropic's callback page actually gives them — `code#state`, a bare code, or the
/// full callback URL — because insisting on one shape only invites a paste that is *correct* and
/// still rejected.
async fn anthropic_complete(state: &AppState, flow_id: &str, pasted: &str) -> Response {
    let trimmed = pasted.trim();
    if trimmed.is_empty() {
        return safe_error(StatusCode::BAD_REQUEST, "authorization_code_missing");
    }
    let (code, returned_state) = match reqwest::Url::parse(trimmed) {
        Ok(url) if url.scheme() == "https" => {
            let code = url
                .query_pairs()
                .find(|(k, _)| k == "code")
                .map(|(_, v)| v.into_owned());
            let st = url
                .query_pairs()
                .find(|(k, _)| k == "state")
                .map(|(_, v)| v.into_owned());
            match code {
                Some(code) => (code, st),
                None => return safe_error(StatusCode::BAD_REQUEST, "authorization_code_missing"),
            }
        }
        // `code#state` is the form Anthropic's page presents.
        _ => match trimmed.split_once('#') {
            Some((code, st)) => (code.to_string(), Some(st.to_string())),
            None => (trimmed.to_string(), None),
        },
    };

    let before_claim = match state.store.onboarding().get(flow_id).await {
        Ok(Some(flow)) => flow,
        Ok(None) => return safe_error(StatusCode::NOT_FOUND, "flow_not_found"),
        Err(_) => return safe_error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error"),
    };
    if before_claim.expires_at <= unix_now() {
        return safe_error(StatusCode::GONE, "flow_expired");
    }
    // A state value is only checked when one came back. Anthropic's page shows `code#state`, but a
    // bare code is a legitimate paste, and rejecting it would fail a login that is otherwise
    // sound. When state IS present it must match — that is the CSRF check, and it is not optional.
    if let Some(returned) = returned_state.as_deref().filter(|v| !v.is_empty()) {
        if polyflare_anthropic::oauth::verify_state(&before_claim.oauth_state, returned).is_err() {
            return safe_error(StatusCode::BAD_REQUEST, "state_mismatch");
        }
    }

    match anthropic_claim_and_complete(state, flow_id, &code).await {
        Ok(account_id) => {
            Json(serde_json::json!({ "status": "completed", "account_id": account_id }))
                .into_response()
        }
        Err(CompleteError::AlreadyUsed) => safe_error(StatusCode::CONFLICT, "flow_already_used"),
        Err(CompleteError::Failed(code)) => {
            let status = match code {
                "seat_mismatch" | "intended_account_missing" => StatusCode::CONFLICT,
                "exchange_rejected_code" | "exchange_rejected_request" => StatusCode::BAD_REQUEST,
                "exchange_failed" | "exchange_rejected_client" => StatusCode::BAD_GATEWAY,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            safe_error(status, code)
        }
    }
}

pub(crate) async fn anthropic_claim_and_complete(
    state: &AppState,
    flow_id: &str,
    code: &str,
) -> Result<String, CompleteError> {
    let flow = match state.store.onboarding().claim(flow_id, unix_now()).await {
        Ok(Some(flow)) => flow,
        Ok(None) => return Err(CompleteError::AlreadyUsed),
        Err(_) => return Err(CompleteError::Failed("storage_error")),
    };
    let fail = |code: &'static str| async move {
        let _ = state
            .store
            .onboarding()
            .fail(flow_id, code, unix_now())
            .await;
        CompleteError::Failed(code)
    };
    let verifier = match state.cipher.decrypt(&flow.verifier_enc) {
        Ok(value) => value,
        Err(_) => return Err(fail("storage_error").await),
    };
    let client = match polyflare_anthropic::oauth::AnthropicOAuthClient::new() {
        Ok(client) => client,
        Err(_) => return Err(fail("oauth_client_unavailable").await),
    };
    // The redirect recorded at start, replayed verbatim — the token endpoint compares it to the
    // one the authorization was issued against.
    let redirect_uri = flow
        .redirect_uri
        .clone()
        .unwrap_or_else(|| client.contract().manual_redirect_uri.to_string());
    let now = unix_now();
    let tokens = match client
        .exchange_code(code, &verifier, &redirect_uri, now)
        .await
    {
        Ok(tokens) => tokens,
        // A bare `exchange_failed` is a dead end: it cannot distinguish "the pasted code was
        // already used" from "our client registration is wrong", and those need opposite
        // responses from the operator. Anthropic states which in the token response, so it is
        // recorded on the flow and logged. Status and error CODE only — never the body, which
        // would risk carrying credential material into a log.
        Err(error) => {
            let specific = match &error {
                polyflare_anthropic::oauth::OAuthError::Endpoint { status, code } => {
                    tracing::warn!(
                        status = *status,
                        oauth_error = code.as_deref().unwrap_or("-"),
                        "anthropic onboarding token exchange rejected"
                    );
                    match code.as_deref() {
                        // The authorization code itself: expired, already spent, or issued
                        // against a different redirect. A fresh sign-in fixes it.
                        Some("invalid_grant") => "exchange_rejected_code",
                        // The client registration this build ships is not accepted. No operator
                        // action fixes that, and saying so beats sending them round again.
                        Some("invalid_client") | Some("unauthorized_client") => {
                            "exchange_rejected_client"
                        }
                        Some("invalid_request") => "exchange_rejected_request",
                        _ => "exchange_failed",
                    }
                }
                other => {
                    tracing::warn!(error = %other, "anthropic onboarding token exchange failed");
                    "exchange_failed"
                }
            };
            return Err(fail(specific).await);
        }
    };
    match persist_anthropic(
        state,
        tokens,
        flow.initial_pool,
        flow.intended_account_id.as_deref(),
        flow_id,
    )
    .await
    {
        Ok(account_id) => Ok(account_id),
        Err(code) => Err(fail(code).await),
    }
}

/// Persist a completed Anthropic login.
///
/// Identity comes from the token response's own account fields — the login never requests
/// `user:profile`, so there is no profile call to make and no broader grant to ask for.
async fn persist_anthropic(
    state: &AppState,
    tokens: polyflare_anthropic::oauth::OAuthTokens,
    initial_pool: Option<String>,
    intended_account_id: Option<&str>,
    flow_id: &str,
) -> Result<String, &'static str> {
    let identity = tokens.identity.clone().ok_or("identity_missing")?;
    if identity.upstream_identity.trim().is_empty() {
        return Err("identity_missing");
    }
    let id = format!("anthropic_{}", identity.upstream_identity);

    if let Some(intended_id) = intended_account_id {
        let intended = state
            .store
            .accounts()
            .get(intended_id)
            .await
            .map_err(|_| "storage_error")?
            .ok_or("intended_account_missing")?;
        // The upstream identity IS the account id here, so a targeted re-auth matches when the
        // login lands on the same row it was started to repair.
        if intended.id != id {
            return Err("seat_mismatch");
        }
    }

    let now = unix_now();
    let account = Account {
        id: id.clone(),
        // Anthropic issues no ChatGPT identifiers; leaving them empty is truthful, and the
        // dashboard already renders an account without them.
        chatgpt_account_id: None,
        chatgpt_user_id: None,
        email: identity.email.clone().unwrap_or_default(),
        alias: None,
        workspace_id: None,
        workspace_label: None,
        seat_type: None,
        // The subscription tier is not in the token response; the usage endpoint reports it later.
        plan_type: "unknown".into(),
        routing_policy: "normal".into(),
        last_refresh: now,
        created_at: now,
        status: "active".into(),
        deactivation_reason: None,
        reset_at: None,
        blocked_at: None,
        security_work_authorized: false,
        usage_cap_percent: None,
        usage_cap_override: false,
        provider: FlowProvider::Anthropic.wire().into(),
        pool: initial_pool,
    };
    let plain = PlainTokens {
        access_token: tokens.access_token,
        refresh_token: tokens.refresh_token,
        // Anthropic returns no id token; the field exists for Codex's OIDC response.
        id_token: String::new(),
    };
    state
        .store
        .accounts()
        .upsert_oauth_and_complete_flow(
            &account,
            &plain,
            &state.cipher,
            flow_id,
            intended_account_id,
        )
        .await
        .map_err(|error| match error {
            polyflare_store::StoreError::InvalidState(message)
                if message.contains("re-auth account") =>
            {
                "intended_account_missing"
            }
            _ => "storage_error",
        })?;
    Ok(id)
}

#[derive(Debug, Serialize)]
pub struct DeviceStartResponse {
    flow_id: String,
    user_code: String,
    verification_url: String,
    expires_at: i64,
    interval_seconds: i64,
}

/// Start a DEVICE-code flow: the operator enters `user_code` at the verification page from any
/// browser on any machine, and a background task here polls the auth server until approval. This
/// is the method that works when the dashboard is opened remotely — the browser flow's registered
/// redirect is pinned to the SERVER-unreachable `localhost:1455` of the viewer's machine.
pub async fn device_start_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<StartRequest>,
) -> Response {
    let (initial_pool, intended_account_id, provider) = match validate_start(&state, body).await {
        Ok(validated) => validated,
        Err(response) => return response,
    };
    // Device-code onboarding is a Codex capability. Anthropic's reviewed contract has no device
    // endpoint, so accepting the request and quietly running a browser flow would be a lie about
    // what the operator asked for.
    if provider != FlowProvider::Codex {
        return safe_error(
            StatusCode::BAD_REQUEST,
            "device_flow_unsupported_for_provider",
        );
    }
    let now = unix_now();
    if state
        .store
        .onboarding()
        .expire_and_prune(now)
        .await
        .is_err()
    {
        return safe_error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error");
    }

    let device = match state.oauth.request_device_code().await {
        Ok(device) => device,
        Err(polyflare_codex::oauth::OAuthError::Endpoint {
            code: Some(code), ..
        }) if code == "device_auth_unavailable" => {
            return safe_error(StatusCode::BAD_GATEWAY, "device_auth_unavailable")
        }
        Err(_) => return safe_error(StatusCode::BAD_GATEWAY, "device_auth_failed"),
    };

    let flow_id = format!("oauth_{}", generate_state());
    let expires_at = now + device.expires_in_seconds.clamp(60, 3600);
    let flow = OnboardingFlow {
        id: flow_id.clone(),
        provider: "codex".into(),
        // Unused by the device flow but UNIQUE NOT NULL in the schema; a random value keeps it so.
        oauth_state: generate_state(),
        verifier_enc: Vec::new(),
        initial_pool,
        status: "pending".into(),
        created_at: now,
        expires_at,
        finished_at: None,
        account_id: None,
        error_code: None,
        redirect_uri: None,
        intended_account_id,
        method: "device".into(),
        device_auth_id: Some(device.device_auth_id.clone()),
        user_code: Some(device.user_code.clone()),
        interval_seconds: Some(device.interval_seconds),
    };
    if state.store.onboarding().create(&flow).await.is_err() {
        return safe_error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error");
    }

    tokio::spawn(poll_device_flow(state.clone(), flow));

    Json(DeviceStartResponse {
        flow_id,
        user_code: device.user_code,
        verification_url: device.verification_url,
        expires_at,
        interval_seconds: device.interval_seconds,
    })
    .into_response()
}

/// Poll spacing when the auth server suggests none (it often sends `interval: 0`, which codex-lb
/// polls hot on — 5s is the RFC 8628 default and keeps a ~15-minute flow at ≤180 polls). A
/// positive server-suggested interval is respected as-is.
const DEVICE_POLL_DEFAULT_INTERVAL_SECS: i64 = 5;

/// The device flow's server-side poller: wait for the operator to approve at the verification
/// page, then claim the flow and persist through the SAME completion path as the browser flow
/// (including the targeted-re-auth seat check). Restart loses the poller — the flow then simply
/// expires and the operator starts a new one; acceptable for a single-node deployment.
async fn poll_device_flow(state: Arc<AppState>, flow: OnboardingFlow) {
    let (Some(device_auth_id), Some(user_code)) =
        (flow.device_auth_id.clone(), flow.user_code.clone())
    else {
        return;
    };
    let interval = match flow.interval_seconds {
        Some(secs) if secs > 0 => secs as u64,
        _ => DEVICE_POLL_DEFAULT_INTERVAL_SECS as u64,
    };
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
        let now = unix_now();
        if now >= flow.expires_at {
            // expire_and_prune (run by any onboarding API call) marks the row failed/expired;
            // the poller just stops.
            return;
        }
        match state
            .oauth
            .poll_device_token(&device_auth_id, &user_code)
            .await
        {
            // Not approved yet.
            Ok(None) => continue,
            // Transient transport problem — keep polling until the flow's TTL.
            Err(polyflare_codex::oauth::OAuthError::Transport(_)) => continue,
            Err(polyflare_codex::oauth::OAuthError::Endpoint { code, .. }) => {
                if state
                    .store
                    .onboarding()
                    .claim(&flow.id, now)
                    .await
                    .ok()
                    .flatten()
                    .is_some()
                {
                    let _ = state
                        .store
                        .onboarding()
                        .fail(
                            &flow.id,
                            if code.is_some() {
                                "device_auth_denied"
                            } else {
                                "device_auth_failed"
                            },
                            unix_now(),
                        )
                        .await;
                }
                return;
            }
            Err(polyflare_codex::oauth::OAuthError::MalformedJwt(_)) => {
                if state
                    .store
                    .onboarding()
                    .claim(&flow.id, now)
                    .await
                    .ok()
                    .flatten()
                    .is_some()
                {
                    let _ = state
                        .store
                        .onboarding()
                        .fail(&flow.id, "device_auth_failed", unix_now())
                        .await;
                }
                return;
            }
            Ok(Some(refreshed)) => {
                // Claim first: completion requires the exchanging state, and a claim that fails
                // means the flow expired or finished elsewhere — drop the result.
                if state
                    .store
                    .onboarding()
                    .claim(&flow.id, now)
                    .await
                    .ok()
                    .flatten()
                    .is_none()
                {
                    return;
                }
                match persist_refreshed(
                    &state,
                    refreshed,
                    flow.initial_pool.clone(),
                    flow.intended_account_id.as_deref(),
                    &flow.id,
                )
                .await
                {
                    Ok(account_id) => {
                        tracing::info!(flow_id = %flow.id, account_id, "device flow completed");
                    }
                    Err(code) => {
                        let _ = state
                            .store
                            .onboarding()
                            .fail(&flow.id, code, unix_now())
                            .await;
                        tracing::warn!(flow_id = %flow.id, code, "device flow failed to persist");
                    }
                }
                return;
            }
        }
    }
}

#[derive(Debug, Serialize)]
pub struct FlowStatusResponse {
    status: String,
    expires_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    account_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_code: Option<String>,
}

pub async fn status_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    if state
        .store
        .onboarding()
        .expire_and_prune(unix_now())
        .await
        .is_err()
    {
        return safe_error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error");
    }
    let flow = match state.store.onboarding().get(&id).await {
        Ok(Some(flow)) => flow,
        Ok(None) => return safe_error(StatusCode::NOT_FOUND, "flow_not_found"),
        Err(_) => return safe_error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error"),
    };
    Json(FlowStatusResponse {
        status: if flow.error_code.as_deref() == Some("flow_expired") {
            "expired".into()
        } else {
            flow.status
        },
        expires_at: flow.expires_at,
        account_id: flow.account_id,
        error_code: flow.error_code,
    })
    .into_response()
}

#[derive(Debug, Deserialize)]
pub struct CallbackRequest {
    callback_url: String,
}

pub async fn callback_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<CallbackRequest>,
) -> Response {
    if state
        .store
        .onboarding()
        .expire_and_prune(unix_now())
        .await
        .is_err()
    {
        return safe_error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error");
    }
    // An Anthropic flow completes from a pasted code, not a captured loopback URL, so it is
    // dispatched before the Codex-shaped URL validation below would reject it.
    match state.store.onboarding().get(&id).await {
        Ok(Some(flow)) if flow.provider == FlowProvider::Anthropic.wire() => {
            return anthropic_complete(&state, &id, &body.callback_url).await;
        }
        Ok(Some(_)) => {}
        Ok(None) => return safe_error(StatusCode::NOT_FOUND, "flow_not_found"),
        Err(_) => return safe_error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error"),
    }

    let parsed = match reqwest::Url::parse(body.callback_url.trim()) {
        Ok(url)
            if url.scheme() == "http"
                && url.host_str() == Some("localhost")
                && url.port_or_known_default() == Some(1455)
                && url.path() == "/auth/callback" =>
        {
            url
        }
        _ => return safe_error(StatusCode::BAD_REQUEST, "invalid_callback_url"),
    };
    let callback_state = parsed
        .query_pairs()
        .find(|(key, _)| key == "state")
        .map(|(_, value)| value.into_owned());
    let code = parsed
        .query_pairs()
        .find(|(key, _)| key == "code")
        .map(|(_, value)| value.into_owned());
    let before_claim = match state.store.onboarding().get(&id).await {
        Ok(Some(flow)) => flow,
        Ok(None) => return safe_error(StatusCode::NOT_FOUND, "flow_not_found"),
        Err(_) => return safe_error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error"),
    };
    if before_claim.expires_at <= unix_now() {
        return safe_error(StatusCode::GONE, "flow_expired");
    }
    if callback_state.as_deref() != Some(before_claim.oauth_state.as_str()) {
        return safe_error(StatusCode::BAD_REQUEST, "state_mismatch");
    }
    let Some(code) = code.filter(|value| !value.is_empty()) else {
        return safe_error(StatusCode::BAD_REQUEST, "authorization_code_missing");
    };

    match claim_and_complete(&state, &id, &code).await {
        Ok(account_id) => {
            Json(serde_json::json!({ "status": "completed", "account_id": account_id }))
                .into_response()
        }
        Err(CompleteError::AlreadyUsed) => safe_error(StatusCode::CONFLICT, "flow_already_used"),
        Err(CompleteError::Failed(code)) => {
            // A refused targeted re-auth is the operator signing into the wrong seat (or the
            // target vanishing mid-flow) — a conflict with the flow's stated intent, not a
            // server fault. No account row was touched.
            let status = match code {
                "seat_mismatch" | "intended_account_missing" => StatusCode::CONFLICT,
                "exchange_failed" => StatusCode::BAD_GATEWAY,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            safe_error(status, code)
        }
    }
}

/// How [`claim_and_complete`] failed — the flow was already consumed, or it failed with the
/// error code now recorded on the flow row.
pub(crate) enum CompleteError {
    AlreadyUsed,
    Failed(&'static str),
}

/// Claim the flow and finish it with an authorization `code`: decrypt the PKCE verifier, exchange
/// at the token endpoint, and persist (including the targeted-re-auth seat check). Shared by the
/// manual paste-the-URL endpoint and the transient loopback listener, so both paths enforce
/// exactly the same rules. On any post-claim error the flow is marked failed with the code.
pub(crate) async fn claim_and_complete(
    state: &AppState,
    flow_id: &str,
    code: &str,
) -> Result<String, CompleteError> {
    let flow = match state.store.onboarding().claim(flow_id, unix_now()).await {
        Ok(Some(flow)) => flow,
        Ok(None) => return Err(CompleteError::AlreadyUsed),
        Err(_) => return Err(CompleteError::Failed("storage_error")),
    };
    let fail = |code: &'static str| async move {
        let _ = state
            .store
            .onboarding()
            .fail(flow_id, code, unix_now())
            .await;
        CompleteError::Failed(code)
    };
    let verifier = match state.cipher.decrypt(&flow.verifier_enc) {
        Ok(value) => value,
        Err(_) => return Err(fail("storage_error").await),
    };
    let refreshed = match state
        .oauth
        .exchange_code(code, &verifier, REDIRECT_URI)
        .await
    {
        Ok(value) => value,
        Err(_) => return Err(fail("exchange_failed").await),
    };
    match persist_refreshed(
        state,
        refreshed,
        flow.initial_pool,
        flow.intended_account_id.as_deref(),
        flow_id,
    )
    .await
    {
        Ok(account_id) => Ok(account_id),
        Err(code) => Err(fail(code).await),
    }
}

async fn persist_refreshed(
    state: &AppState,
    refreshed: Refreshed,
    initial_pool: Option<String>,
    intended_account_id: Option<&str>,
    flow_id: &str,
) -> Result<String, &'static str> {
    let claims = refreshed.claims.ok_or("identity_missing")?;
    // Targeted re-auth: the exchanged seat must belong to the account this flow was started to
    // repair. Refusing here (before any write) is what keeps a wrong-seat login from silently
    // onboarding/updating a DIFFERENT account while the broken one stays reauth_required.
    if let Some(intended_id) = intended_account_id {
        let intended = state
            .store
            .accounts()
            .get(intended_id)
            .await
            .map_err(|_| "storage_error")?
            .ok_or("intended_account_missing")?;
        let seat_matches = match intended.chatgpt_account_id.as_deref() {
            Some(stored) => claims.chatgpt_account_id.as_deref() == Some(stored),
            // An account without a stored ChatGPT id (possible for imported rows) can only be
            // matched by email — case-insensitive, and only when both sides actually have one.
            None => match (intended.email.as_str(), claims.email.as_deref()) {
                ("", _) | (_, None) => false,
                (stored, Some(returned)) => stored.eq_ignore_ascii_case(returned),
            },
        };
        if !seat_matches {
            return Err("seat_mismatch");
        }
    }
    let tokens = PlainTokens {
        access_token: refreshed.tokens.access_token,
        refresh_token: refreshed.tokens.refresh_token,
        id_token: refreshed.tokens.id_token,
    };
    let now = unix_now();
    let identity = claims
        .chatgpt_account_id
        .clone()
        .or_else(|| claims.sub.clone())
        .ok_or("identity_missing")?;
    let id = format!("codex_{identity}");
    let account = Account {
        id: id.clone(),
        chatgpt_account_id: claims.chatgpt_account_id,
        chatgpt_user_id: claims.chatgpt_user_id,
        email: claims.email.unwrap_or_default(),
        alias: None,
        workspace_id: claims.workspace_id,
        workspace_label: claims.workspace_label,
        seat_type: claims.seat_type,
        plan_type: claims.chatgpt_plan_type.unwrap_or_else(|| "unknown".into()),
        routing_policy: "normal".into(),
        last_refresh: now,
        created_at: now,
        status: "active".into(),
        deactivation_reason: None,
        reset_at: None,
        blocked_at: None,
        security_work_authorized: false,
        // A newly onboarded account starts uncapped; the ceiling is set deliberately later.
        usage_cap_percent: None,
        usage_cap_override: false,
        provider: "codex".into(),
        pool: initial_pool,
    };
    state
        .store
        .accounts()
        .upsert_oauth_and_complete_flow(
            &account,
            &tokens,
            &state.cipher,
            flow_id,
            intended_account_id,
        )
        .await
        .map_err(|error| match error {
            // The verified target vanished between the seat check and the write. (InvalidState's
            // other cause — a flow no longer in `exchanging` — cannot reach here: this call runs
            // immediately after the atomic claim.)
            polyflare_store::StoreError::InvalidState(message)
                if message.contains("re-auth account") =>
            {
                "intended_account_missing"
            }
            _ => "storage_error",
        })
}
