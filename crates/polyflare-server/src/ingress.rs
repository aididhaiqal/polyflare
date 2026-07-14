//! Ingress: decode an OpenAI-Responses request and relay the executor's stream to the client.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Json, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};

use polyflare_core::PreparedRequest;

use crate::app::AppState;

pub async fn responses_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let model = body
        .get("model")
        .and_then(|m| m.as_str())
        .unwrap_or_default()
        .to_string();
    let req = PreparedRequest { body, model };

    // M1: single account from config; pool selection arrives in M2.
    match state.executor.execute(req, &state.account).await {
        Ok(stream) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/event-stream")
            .body(Body::from_stream(stream))
            .expect("valid response"),
        Err(e) => (StatusCode::BAD_GATEWAY, format!("upstream error: {e}")).into_response(),
    }
}
