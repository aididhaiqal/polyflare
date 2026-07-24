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
}

#[derive(Debug, Serialize)]
pub struct StartResponse {
    flow_id: String,
    authorize_url: String,
    expires_at: i64,
}

pub async fn start_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<StartRequest>,
) -> Response {
    let initial_pool = body.initial_pool.map(|v| v.trim().to_string());
    if initial_pool
        .as_deref()
        .is_some_and(|slug| !valid_pool_slug(slug))
    {
        return safe_error(StatusCode::BAD_REQUEST, "invalid_pool_slug");
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
    };
    if state.store.onboarding().create(&flow).await.is_err() {
        return safe_error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error");
    }

    Json(StartResponse {
        flow_id,
        authorize_url: state.oauth.build_authorize_url(&oauth_state, &challenge),
        expires_at,
    })
    .into_response()
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

    let flow = match state.store.onboarding().claim(&id, unix_now()).await {
        Ok(Some(flow)) => flow,
        Ok(None) => return safe_error(StatusCode::CONFLICT, "flow_already_used"),
        Err(_) => return safe_error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error"),
    };
    let verifier = match state.cipher.decrypt(&flow.verifier_enc) {
        Ok(value) => value,
        Err(_) => {
            let _ = state
                .store
                .onboarding()
                .fail(&id, "storage_error", unix_now())
                .await;
            return safe_error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error");
        }
    };
    let refreshed = match state
        .oauth
        .exchange_code(&code, &verifier, REDIRECT_URI)
        .await
    {
        Ok(value) => value,
        Err(_) => {
            let _ = state
                .store
                .onboarding()
                .fail(&id, "exchange_failed", unix_now())
                .await;
            return safe_error(StatusCode::BAD_GATEWAY, "exchange_failed");
        }
    };
    let account_id = match persist_refreshed(&state, refreshed, flow.initial_pool, &id).await {
        Ok(value) => value,
        Err(code) => {
            let _ = state.store.onboarding().fail(&id, code, unix_now()).await;
            return safe_error(StatusCode::INTERNAL_SERVER_ERROR, code);
        }
    };
    Json(serde_json::json!({ "status": "completed", "account_id": account_id })).into_response()
}

async fn persist_refreshed(
    state: &AppState,
    refreshed: Refreshed,
    initial_pool: Option<String>,
    flow_id: &str,
) -> Result<String, &'static str> {
    let claims = refreshed.claims.ok_or("identity_missing")?;
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
        provider: "codex".into(),
        pool: initial_pool,
    };
    state
        .store
        .accounts()
        .upsert_oauth_and_complete_flow(&account, &tokens, &state.cipher, flow_id)
        .await
        .map_err(|_| "storage_error")
}
