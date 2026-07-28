//! Authenticated admin API for the content-free priority-tier policy.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::app::AppState;
use crate::priority_policy::{
    valid_session_key, PriorityPolicyConfig, SessionMode, GLOBAL_SETTING_KEY,
    SESSION_SETTING_PREFIX,
};

#[derive(Serialize)]
pub struct PriorityPolicyView {
    pub config: PriorityPolicyConfig,
}

pub async fn get(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(PriorityPolicyView {
        config: state.runtime_settings.priority_policy.config(),
    })
}

pub async fn patch(
    State(state): State<Arc<AppState>>,
    Json(config): Json<PriorityPolicyConfig>,
) -> Response {
    if !config.validate() {
        return (StatusCode::BAD_REQUEST, "invalid priority policy").into_response();
    }
    let encoded = match serde_json::to_string(&config) {
        Ok(encoded) => encoded,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    if state
        .store
        .settings()
        .set(GLOBAL_SETTING_KEY, &encoded, unix_now())
        .await
        .is_err()
    {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    if state
        .runtime_settings
        .priority_policy
        .set_config(config)
        .is_err()
    {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    Json(serde_json::json!({ "ok": true })).into_response()
}

#[derive(Deserialize)]
pub struct SessionPriorityBody {
    mode: String,
}

pub async fn set_session(
    State(state): State<Arc<AppState>>,
    Path(session_key): Path<String>,
    Json(body): Json<SessionPriorityBody>,
) -> Response {
    if !valid_session_key(&session_key) {
        return (StatusCode::BAD_REQUEST, "invalid session key").into_response();
    }
    let mode = match body.mode.as_str() {
        "inherit" => None,
        "priority" => Some(SessionMode::Priority),
        "standard" => Some(SessionMode::Standard),
        _ => return (StatusCode::BAD_REQUEST, "invalid priority mode").into_response(),
    };
    let setting_key = format!("{SESSION_SETTING_PREFIX}{session_key}");
    let persisted = match mode {
        Some(mode) => {
            let encoded = match serde_json::to_string(&mode) {
                Ok(encoded) => encoded,
                Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            };
            state
                .store
                .settings()
                .set(&setting_key, &encoded, unix_now())
                .await
        }
        None => state.store.settings().delete(&setting_key).await,
    };
    if persisted.is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    state
        .runtime_settings
        .priority_policy
        .set_session_override(session_key, mode);
    Json(serde_json::json!({ "ok": true })).into_response()
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}
