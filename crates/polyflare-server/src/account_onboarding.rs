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
) -> Result<(Option<String>, Option<String>), Response> {
    let initial_pool = body.initial_pool.map(|v| v.trim().to_string());
    if initial_pool
        .as_deref()
        .is_some_and(|slug| !valid_pool_slug(slug))
    {
        return Err(safe_error(StatusCode::BAD_REQUEST, "invalid_pool_slug"));
    }
    let intended_account_id = match body.account_id.map(|v| v.trim().to_string()) {
        Some(id) if !id.is_empty() => match state.store.accounts().get(&id).await {
            Ok(Some(account)) if account.provider == "codex" => Some(id),
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
    Ok((initial_pool, intended_account_id))
}

pub async fn start_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<StartRequest>,
) -> Response {
    let (initial_pool, intended_account_id) = match validate_start(&state, body).await {
        Ok(validated) => validated,
        Err(response) => return response,
    };

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
    let (initial_pool, intended_account_id) = match validate_start(&state, body).await {
        Ok(validated) => validated,
        Err(response) => return response,
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
