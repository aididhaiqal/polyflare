//! Dashboard API for per-account model support — the operator surface behind the CLI's
//! `polyflare models` and the dashboard override control.
//!
//! A write updates the store AND the in-memory overlay in the same call, so a declaration takes
//! effect on the very next request rather than waiting for the 30s reloader (which exists only for
//! cross-process changes, i.e. the CLI).

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;

use crate::app::AppState;

fn unix_now() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Reload the whole declared-support overlay from the store into the catalog. Called after every
/// write so the change is live immediately; the table is tiny so a full reload is trivial.
async fn refresh_overlay(state: &AppState) {
    if let Ok(rows) = state.store.account_model_support().get_all().await {
        state.model_catalog.set_declared_support(
            rows.into_iter()
                .map(|row| (row.account_id, row.model, row.supported)),
        );
    }
}

/// `GET /api/model-support` — every per-account model declaration.
pub async fn get_handler(State(state): State<Arc<AppState>>) -> Response {
    let rows = match state.store.account_model_support().get_all().await {
        Ok(rows) => rows,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "storage_error").into_response(),
    };
    let items: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|row| {
            serde_json::json!({
                "account_id": row.account_id,
                "model": row.model,
                "supported": row.supported,
                "source": match row.source {
                    polyflare_store::SupportSource::Operator => "operator",
                    polyflare_store::SupportSource::Probe => "probe",
                },
                "updated_at": row.updated_at,
            })
        })
        .collect();
    Json(serde_json::json!({ "declarations": items })).into_response()
}

#[derive(Debug, Deserialize)]
pub struct SetRequest {
    account_id: String,
    model: String,
    supported: bool,
}

/// `PUT /api/model-support` — declare (operator override) whether an account supports a model.
/// Always recorded as `operator`, which outranks any probe row.
pub async fn put_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<SetRequest>,
) -> Response {
    let model = body.model.trim();
    let account_id = body.account_id.trim();
    if model.is_empty() || account_id.is_empty() {
        return (StatusCode::BAD_REQUEST, "account_id and model are required").into_response();
    }
    match state.store.accounts().get(account_id).await {
        Ok(Some(_)) => {}
        Ok(None) => return (StatusCode::NOT_FOUND, "account_not_found").into_response(),
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "storage_error").into_response(),
    }
    if state
        .store
        .account_model_support()
        .set(
            account_id,
            model,
            body.supported,
            polyflare_store::SupportSource::Operator,
            unix_now(),
        )
        .await
        .is_err()
    {
        return (StatusCode::INTERNAL_SERVER_ERROR, "storage_error").into_response();
    }
    refresh_overlay(&state).await;
    Json(serde_json::json!({ "ok": true })).into_response()
}

#[derive(Debug, Deserialize)]
pub struct ClearRequest {
    account_id: String,
    model: String,
}

/// `DELETE /api/model-support` — remove a declaration, reverting to "unknown".
pub async fn delete_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ClearRequest>,
) -> Response {
    if state
        .store
        .account_model_support()
        .delete(body.account_id.trim(), body.model.trim())
        .await
        .is_err()
    {
        return (StatusCode::INTERNAL_SERVER_ERROR, "storage_error").into_response();
    }
    refresh_overlay(&state).await;
    Json(serde_json::json!({ "ok": true })).into_response()
}
