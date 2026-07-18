//! Write API: dashboard-driven account configuration. Mutates only non-secret account SETTINGS —
//! pool, routing policy, pause/resume (status), and the `security_work_authorized` capability
//! flag (TA6) — never a token or any secret. Gated on
//! `POLYFLARE_ADMIN_TOKEN` (`crate::auth::require_admin`) like every other `/api/*` route — the
//! proxy surface (`/responses`, `/v1/messages`) remains unauthenticated network-boundary trust; see
//! PORTING-CODEXLB.md D18. Every write bumps the store generation, so
//! the running server's account cache picks the change up on the next selection without a restart.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Deserializer};

use crate::app::AppState;

/// Deserialize a field so absent, `null`, and a value are three DISTINCT outcomes: absent →
/// `None` (via `#[serde(default)]`, this fn isn't called), `null` → `Some(None)`, value →
/// `Some(Some(v))`. Plain `Option<Option<T>>` collapses absent and null both to `None`; this keeps
/// them apart so `null` can mean "clear the pool" while absent means "leave it unchanged".
fn double_option<'de, D, T>(de: D) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Deserialize::deserialize(de).map(Some)
}

const ROUTING_POLICIES: &[&str] = &["normal", "burn_first", "preserve"];
/// Only pause/resume are settable from the dashboard. The usage-refresh loop owns the rate-limit
/// statuses (`active`/`rate_limited`/`quota_exceeded`) and `reauth_required`/`deactivated` are
/// lifecycle states — the UI must not stomp them. `paused` is a manual hold the refresh loop leaves
/// alone (it only moves accounts already in a usage-controlled status).
const SETTABLE_STATUSES: &[&str] = &["active", "paused"];

/// A partial account-settings update. Each field is optional: absent means "leave unchanged". For
/// `pool`, an explicit `null` means "clear (unpool)" and a string means "assign/create" — hence the
/// double option (absent vs null vs value are three distinct intents).
#[derive(Deserialize)]
pub struct AccountPatch {
    #[serde(default, deserialize_with = "double_option")]
    pool: Option<Option<String>>,
    #[serde(default)]
    routing_policy: Option<String>,
    #[serde(default)]
    status: Option<String>,
    /// The cyber-work capability flag (TA6). Absent means "leave unchanged"; `Some(bool)` sets it.
    /// Never a token/secret field — this is the only capability toggle the operator surface exposes.
    #[serde(default)]
    security_work_authorized: Option<bool>,
}

fn bad_request(msg: &'static str) -> Response {
    (StatusCode::BAD_REQUEST, msg).into_response()
}

fn internal_error() -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
}

/// `PATCH /api/accounts/{id}` — update an account's pool / routing policy / paused state. Validates
/// every field BEFORE applying any, so a bad value never leaves a half-applied update.
pub async fn patch_account_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(patch): Json<AccountPatch>,
) -> Response {
    let repo = state.store.accounts();
    match repo.get(&id).await {
        Ok(Some(_)) => {}
        Ok(None) => return (StatusCode::NOT_FOUND, "no such account").into_response(),
        Err(_) => return internal_error(),
    }

    // Validate first (fail closed, all-or-nothing).
    if let Some(rp) = &patch.routing_policy {
        if !ROUTING_POLICIES.contains(&rp.as_str()) {
            return bad_request("routing_policy must be one of normal|burn_first|preserve");
        }
    }
    if let Some(st) = &patch.status {
        if !SETTABLE_STATUSES.contains(&st.as_str()) {
            return bad_request("status may only be set to active or paused");
        }
    }

    // Apply. Each helper bumps the store generation, so the account cache re-reads on next selection.
    if let Some(pool) = &patch.pool {
        if repo.update_pool(&id, pool.as_deref()).await.is_err() {
            return internal_error();
        }
    }
    if let Some(rp) = &patch.routing_policy {
        if repo.update_routing_policy(&id, rp).await.is_err() {
            return internal_error();
        }
    }
    if let Some(st) = &patch.status {
        if repo.update_status(&id, st).await.is_err() {
            return internal_error();
        }
    }
    if let Some(authorized) = patch.security_work_authorized {
        if repo
            .update_security_work_authorized(&id, authorized)
            .await
            .is_err()
        {
            return internal_error();
        }
    }

    (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response()
}
