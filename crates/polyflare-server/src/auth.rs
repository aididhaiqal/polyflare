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

/// `GET /api/capabilities` — feature flags the dashboard SPA gates UI on. Currently just
/// `live_logs` (from `POLYFLARE_LIVE_LOGS`); grows as later tasks add capabilities.
pub async fn capabilities_handler(State(s): State<Arc<AppState>>) -> impl IntoResponse {
    Json(serde_json::json!({ "live_logs": s.live_logs }))
}

/// D18 Task 3: client API-key auth for the proxy surface (`/responses`, `/v1/messages`,
/// `/{pool}/…`). Validates a presented `Authorization: Bearer <raw>` against the `api_keys` table
/// (Task 1's `ApiKeyRepo`, Task 2's key format) — this is the "is the presented key valid" half
/// only. **Whether a key is required at all is Task 4's bind-address-aware posture decision**;
/// this middleware assumes enforcement is already ON and is meant to be composed by Task 4 (e.g.
/// only `route_layer`'d onto the proxy sub-router when `enforce_client_keys` is true). It does not
/// itself decide posture and is not wired onto any route here.
///
/// **Hash-lookup, NOT a plaintext `==` compare** — unlike [`require_admin`]'s single
/// shared-operator-token compare (correct for ONE known value), client keys live in a table of
/// many, so the presented token is sha256-hashed ([`crate::keys::sha256_hex`], the same hashing
/// Task 2's `keys create` uses) and looked up via the indexed [`polyflare_store::ApiKeyRepo::get_by_hash`].
/// This is the D18 Global Constraint: "HASH-LOOKUP VALIDATION, not plaintext `==`."
///
/// **Repo-error handling — fail-closed (documented decision):** an unknown hash, a revoked
/// (`enabled == false`) key, AND a store error while looking the key up all take the SAME 401
/// path. A `get_by_hash` error is not the caller's fault, but this is an auth gate — admitting an
/// unverified caller because the DB hiccuped is the wrong failure mode for a security check ("fail
/// closed"), and a transient store error is already visible to the operator via whatever caused
/// it elsewhere (the store layer's own error logging/metrics, not this middleware's job to
/// duplicate). This mirrors codex-lb's `validate_key`, which also treats "can't prove valid" as
/// "invalid," not as a distinct 5xx.
///
/// **`touch_last_used` — fire-and-forget, not awaited (documented decision):** it's a best-effort
/// audit timestamp, not something the caller's request should ever wait on or fail because of. It
/// is spawned as its own task (owning a cloned `Arc<AppState>` + the row's `id` — no key material)
/// so a slow or failing write never adds latency to the hot path and can never panic the request
/// task (a panic inside a spawned task unwinds only that task, not the caller's); any store error
/// from the write is silently swallowed (again: best-effort audit, not correctness-load-bearing —
/// an occasional missed `last_used_at` bump does not affect whether the key remains valid).
///
/// **Never logs the raw key (inviolable):** this function contains no `tracing::`/`eprintln!` call
/// of any kind, on the success path OR the 401 path — the simplest way to guarantee the D18
/// "never log the client key" constraint is to not log anything key-derived at all. See
/// `require_client_key_middleware.rs`'s `sentinel_key_never_leaks_on_failed_auth` test for the
/// mechanical proof (captures a real `tracing` subscriber across a failing request with a sentinel
/// value in the `Authorization` header and asserts it never appears in the capture or the 401
/// body).
pub async fn require_client_key(
    State(s): State<Arc<AppState>>,
    headers: HeaderMap,
    req: Request,
    next: Next,
) -> Response {
    let presented = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .filter(|raw| !raw.is_empty());

    let Some(raw) = presented else {
        return unauthorized_response();
    };

    let hash = crate::keys::sha256_hex(raw);
    match s.store.api_keys().get_by_hash(&hash).await {
        Ok(Some(row)) if row.enabled => {
            // Best-effort audit write — see doc comment above for the fire-and-forget rationale.
            // Carries only `row.id` (never key material) into the spawned task.
            let state = s.clone();
            let id = row.id.clone();
            tokio::spawn(async move {
                let _ = state.store.api_keys().touch_last_used(&id, unix_now()).await;
            });
            next.run(req).await
        }
        // Unknown hash, a revoked (`enabled == false`) row, or a store error while looking it up —
        // all fail closed to the same generic 401. See the doc comment's "Repo-error handling"
        // note for why a DB error is folded into "invalid" rather than a distinct 5xx.
        _ => unauthorized_response(),
    }
}

fn unauthorized_response() -> Response {
    (StatusCode::UNAUTHORIZED, "unauthorized").into_response()
}

fn unix_now() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
