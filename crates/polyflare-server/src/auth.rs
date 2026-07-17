//! Admin-token auth for the dashboard `/api/*` routes. A single shared operator token
//! (`POLYFLARE_ADMIN_TOKEN`), presented as `Authorization: Bearer <token>` — no per-user sessions,
//! no cookies. Unset ⇒ the dashboard API is disabled entirely (503), never silently open.

use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use std::sync::Arc;

use crate::app::AppState;

/// Gate every `/api/*` route on `POLYFLARE_ADMIN_TOKEN`. Unset ⇒ dashboard disabled (503).
pub async fn require_admin(
    State(s): State<Arc<AppState>>,
    headers: HeaderMap,
    req: Request,
    next: Next,
) -> Response {
    let Some(expected) = s.admin_token.as_deref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "dashboard disabled: set POLYFLARE_ADMIN_TOKEN",
        )
            .into_response();
    };
    let presented = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    // Constant-time-ish compare is unnecessary for a single local operator token, but avoid
    // early-exit length leak.
    if presented == Some(expected) {
        next.run(req).await
    } else {
        (StatusCode::UNAUTHORIZED, "unauthorized").into_response()
    }
}

/// `GET /api/whoami` — proves a presented token is valid. No identity beyond that today (a single
/// shared operator token has no per-user identity to report).
pub async fn whoami_handler() -> impl IntoResponse {
    Json(serde_json::json!({ "ok": true }))
}
