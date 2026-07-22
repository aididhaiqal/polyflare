//! Write API: dashboard-driven account configuration. Mutates only non-secret account SETTINGS —
//! pool, routing policy, pause/resume (status), and the `security_work_authorized` capability
//! flag (TA6) — never a token or any secret. Gated on
//! `POLYFLARE_ADMIN_TOKEN` (`crate::auth::require_admin`) like every other `/api/*` route — the
//! proxy surface (`/responses`, `/v1/messages`) remains unauthenticated network-boundary trust; see
//! PORTING-CODEXLB.md D18. Every write bumps the store generation, so
//! the running server's account cache picks the change up on the next selection without a restart.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Deserializer};

use crate::app::AppState;
use crate::read_api::{live_field_kind, FieldKind, LIVE_KEYS_ORDER};
use crate::runtime_settings::SettingValue;

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

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
    /// The account's human-readable alias. Absent means "leave unchanged"; a non-empty trimmed
    /// value (<=64 chars) sets it; `null` or an empty/whitespace value clears it — mirrors `pool`'s
    /// double-Option shape.
    #[serde(default, deserialize_with = "double_option")]
    alias: Option<Option<String>>,
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
    // `alias`: present means set/clear. A non-empty trimmed value must be 1..=64 chars; an
    // empty/whitespace value clears (normalized to None below).
    if let Some(Some(a)) = &patch.alias {
        let t = a.trim();
        if !t.is_empty() && t.chars().count() > 64 {
            return bad_request("alias must be 1..=64 characters");
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
    if let Some(alias) = &patch.alias {
        // present: set a trimmed non-empty value, else clear (empty/whitespace/null -> None).
        let normalized = alias.as_deref().map(str::trim).filter(|t| !t.is_empty());
        if repo.update_alias(&id, normalized).await.is_err() {
            return internal_error();
        }
    }

    (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response()
}

#[derive(Deserialize)]
pub struct DeleteQuery {
    #[serde(default)]
    delete_history: bool,
}

/// `DELETE /api/accounts/{id}` — remove an account. `?delete_history=true` purges its `request_log`
/// rows; otherwise those rows are detached (`account_id` set NULL) and kept for reporting. Either way
/// the account's `usage_history` FK-cascades away and its `continuity` sessions FK-detach (see
/// `AccountRepo::delete`).
pub async fn delete_account_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    axum::extract::Query(q): axum::extract::Query<DeleteQuery>,
) -> Response {
    match state.store.accounts().delete(&id, q.delete_history).await {
        Ok(true) => (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response(),
        Ok(false) => (StatusCode::NOT_FOUND, "no such account").into_response(),
        Err(_) => internal_error(),
    }
}

/// `PATCH /api/settings` — live-edit one or more of the 10 live-editable `RuntimeSettings` fields
/// (Settings subsystem Task 5). Body is a bare JSON object `{ <key>: <value> }`; each `key` must be
/// one of `crate::read_api::LIVE_KEYS_ORDER` (else `400`, and no key in the body is applied — same
/// validate-BEFORE-apply, fail-closed posture as `patch_account_handler` above), and each `value`
/// must coerce to that field's kind per `crate::read_api::live_field_kind` (a JSON number for a
/// `U64`/`F64` field, a JSON bool for a `Bool` field — wrong JSON type is also a `400`, never a
/// silent coercion).
///
/// Applied in `LIVE_KEYS_ORDER` — a FIXED order with `starvation_wait_budget` before
/// `starvation_heartbeat` — never the JSON object's own (arbitrary/insertion) key order, so a
/// single PATCH containing both clamps the heartbeat against the INCOMING budget (see
/// `crate::runtime_settings`'s module doc's Ordering note). Each applied key persists the CLAMPED
/// CANONICAL value `RuntimeSettings::set` returns (never the raw request value — a clamped
/// `50.0` round-trips through `f64::to_string`/`str::parse` on reboot, an unclamped `99.0` would
/// not re-clamp until the next live write) to `store.settings()`, so a later restart's startup
/// overlay picks up exactly what is live right now.
///
/// Content-free: only the 10 known config keys are ever read from the body; no key/value here is
/// ever a token/secret (the settings PATCH surface has no access to any).
pub async fn patch_settings_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let Some(obj) = body.as_object() else {
        return bad_request("request body must be a JSON object");
    };

    // Validate first (fail closed, all-or-nothing): every key present must be one of the 10 live
    // keys, and every value must coerce to that field's kind. Collected in the FIXED canonical
    // order (budget before heartbeat) — see this fn's doc.
    let mut to_apply: Vec<(&'static str, SettingValue)> = Vec::new();
    for key in LIVE_KEYS_ORDER {
        let Some(raw) = obj.get(*key) else {
            continue;
        };
        let kind = match live_field_kind(key) {
            Some(k) => k,
            None => return bad_request("unknown or non-live setting key"),
        };
        let value = match kind {
            FieldKind::U64 => match raw.as_u64() {
                Some(n) => SettingValue::U64(n),
                None => return bad_request("value must be a non-negative integer"),
            },
            FieldKind::F64 => match raw.as_f64() {
                Some(n) => SettingValue::F64(n),
                None => return bad_request("value must be a number"),
            },
            FieldKind::Bool => match raw.as_bool() {
                Some(b) => SettingValue::Bool(b),
                None => return bad_request("value must be a boolean"),
            },
        };
        to_apply.push((*key, value));
    }
    // Any key in the body that isn't one of the 10 live keys never reaches `set` — reject the
    // whole PATCH instead of silently ignoring it.
    if obj.len() != to_apply.len() {
        return bad_request("unknown or non-live setting key");
    }

    // Apply + persist, in the same canonical order just validated.
    let now = unix_now();
    for (key, value) in to_apply {
        let stored = match state.runtime_settings.set(key, value) {
            Ok(s) => s,
            Err(_) => return bad_request("invalid setting value"),
        };
        if state.store.settings().set(key, &stored, now).await.is_err() {
            return internal_error();
        }
    }

    (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response()
}

// --- Dashboard API-keys subsystem Outcome 1: `POST /api/keys` (create-show-once) + `PATCH
// /api/keys/{id}` (enable/disable). `GET /api/keys` is in `crate::read_api`. ---

/// `POST /api/keys` body: an optional human-readable label (e.g. "laptop", "ci"). Absent/`null` ⇒
/// no label — mirrors `crate::keys::create_key`'s own `Option<&str>` parameter.
#[derive(Deserialize)]
pub struct CreateKeyRequest {
    #[serde(default)]
    label: Option<String>,
}

/// `POST /api/keys` — mint a new client proxy API key. Delegates to `crate::keys::create_key`
/// (the ONLY place a raw key is ever produced — see its doc), which stores the key's hash +
/// prefix and hands back the plaintext for this ONE response only.
///
/// **Content-safety (inviolable):** `created.raw` is placed directly into the JSON response body
/// below and NOWHERE else — this function contains no `tracing::`/`println!`/`eprintln!` call of
/// any kind, and this route is a dashboard `/api/*` handler, not the proxied-request path, so no
/// `RequestLog` row is ever built from it. The raw key is unrecoverable after this response: only
/// `created.key_hash` (never returned to any caller) persists.
pub async fn create_key_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateKeyRequest>,
) -> Response {
    let now = unix_now();
    let created = match crate::keys::create_key(&state.store, body.label.as_deref(), now).await {
        Ok(c) => c,
        Err(_) => return internal_error(),
    };
    (
        StatusCode::CREATED,
        Json(serde_json::json!({
            "id": created.id,
            "key_prefix": created.key_prefix,
            "key": created.raw,
        })),
    )
        .into_response()
}

/// `PATCH /api/keys/{id}` body: enable or disable the key. Unlike `AccountPatch`'s optional
/// fields, this patch only ever does one thing today, so `enabled` is required.
#[derive(Deserialize)]
pub struct PatchKeyRequest {
    enabled: bool,
}

/// `PATCH /api/keys/{id}` — enable/disable a client proxy API key (`ApiKeyRepo::set_enabled`).
/// Unknown `id` → `404`. `set_enabled` itself has no rows-affected signal to distinguish "updated"
/// from "no such row" (unlike `AccountRepo::delete`'s `bool` return), so existence is checked via
/// a `list()` scan first — cheap at this table's expected size, and the same
/// check-before-apply shape `patch_account_handler` already uses above.
pub async fn patch_key_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(patch): Json<PatchKeyRequest>,
) -> Response {
    let repo = state.store.api_keys();
    let rows = match repo.list().await {
        Ok(r) => r,
        Err(_) => return internal_error(),
    };
    if !rows.iter().any(|r| r.id == id) {
        return (StatusCode::NOT_FOUND, "no such key").into_response();
    }
    if repo.set_enabled(&id, patch.enabled).await.is_err() {
        return internal_error();
    }

    (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response()
}
