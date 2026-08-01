//! Admin authentication for dashboard `/api/*` routes. Three credentials are accepted, in
//! descending order of preference:
//!
//! 1. **A passkey session** — minted by `crate::passkey_auth` after a WebAuthn assertion, presented
//!    as `Authorization: Bearer <session token>` and validated by hash lookup.
//! 2. **`POLYFLARE_ADMIN_TOKEN`** — a shared operator token, and the break-glass path if every
//!    passkey is lost.
//! 3. **[`LocalDashboardAccess`]** — the tokenless loopback bypass, available ONLY while no passkey
//!    is registered. Registering one withdraws it permanently, which is how the dashboard stops
//!    trusting "any process that reached loopback" (including anything that could hit the
//!    credential-export route).
//!
//! Non-loopback deployments with no credential configured stay disabled rather than silently
//! opening the management surface.

use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use std::sync::Arc;

use crate::app::AppState;

/// Request-extension marker installed once at startup when the dashboard has no configured token
/// and the listener is bound to a loopback address. It is derived from the server bind, never from
/// caller-controlled forwarding headers.
#[derive(Debug, Clone, Copy)]
pub struct LocalDashboardAccess;

/// Resolve the zero-config local dashboard posture. Parse the complete socket address and fail
/// closed for hostnames, unspecified addresses, and malformed input.
pub fn local_dashboard_access(admin_token: Option<&str>, bind_addr: &str) -> bool {
    admin_token.is_none()
        && bind_addr
            .parse::<std::net::SocketAddr>()
            .map(|addr| addr.ip().is_loopback())
            .unwrap_or(false)
}

/// The presented `Authorization: Bearer <token>`, if any.
pub(crate) fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .filter(|token| !token.is_empty())
}

/// Whether this request carries proof of authorization: a live passkey session, or the configured
/// admin token. Deliberately excludes the loopback bypass — callers that also honour the bypass
/// check it separately, so the two are never conflated.
pub(crate) async fn presented_credential_is_valid(s: &AppState, headers: &HeaderMap) -> bool {
    let Some(presented) = bearer_token(headers) else {
        return false;
    };
    if let Some(expected) = s.admin_token.as_deref() {
        if constant_time_eq(presented.as_bytes(), expected.as_bytes()) {
            return true;
        }
    }
    // A passkey session token. Hash-lookup, never a plaintext compare — the same rule `api_keys`
    // follows, so a database reader cannot replay a live session.
    s.store
        .passkeys()
        .session_is_valid(&crate::keys::sha256_hex(presented), unix_now())
        .await
        .unwrap_or(false)
}

/// Whether the tokenless loopback bypass may still admit this request. It is withdrawn the moment
/// a passkey exists: "anything that reached loopback is the operator" is only an acceptable
/// posture while there is no way for the operator to actually prove who they are. A store error
/// fails CLOSED (treated as "a passkey exists"), because admitting an unauthenticated caller
/// because the database hiccuped is the wrong failure mode for an auth gate.
async fn local_bypass_available(s: &AppState, marker_present: bool) -> bool {
    marker_present && !s.store.passkeys().any_registered().await.unwrap_or(true)
}

/// Whether a request would be admitted, without consuming it. Powers `GET /api/auth/status`, whose
/// whole job is telling the login screen if it already has access.
pub(crate) async fn request_is_authenticated(
    s: &AppState,
    headers: &HeaderMap,
    marker_present: bool,
) -> bool {
    presented_credential_is_valid(s, headers).await
        || local_bypass_available(s, marker_present).await
}

/// Gate every `/api/*` route on a passkey session, `POLYFLARE_ADMIN_TOKEN`, or — only while no
/// passkey is registered — the startup-resolved loopback marker. With none of the three available,
/// a non-loopback deployment reports the dashboard disabled (503) rather than opening it.
pub async fn require_admin(
    State(s): State<Arc<AppState>>,
    headers: HeaderMap,
    req: Request,
    next: Next,
) -> Response {
    if presented_credential_is_valid(&s, &headers).await {
        return next.run(req).await;
    }
    let marker_present = req.extensions().get::<LocalDashboardAccess>().is_some();
    if local_bypass_available(&s, marker_present).await {
        return next.run(req).await;
    }
    // A registered passkey means sign-in is possible, so an unauthenticated caller gets 401 (go
    // sign in) rather than 503 (nothing is configured).
    let signin_possible =
        s.admin_token.is_some() || s.store.passkeys().any_registered().await.unwrap_or(false);
    if signin_possible {
        (StatusCode::UNAUTHORIZED, "unauthorized").into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "dashboard disabled: register a passkey from a local session or set \
             POLYFLARE_ADMIN_TOKEN",
        )
            .into_response()
    }
}

fn constant_time_eq(presented: &[u8], expected: &[u8]) -> bool {
    let compared_len = presented.len().max(expected.len());
    let mut difference = presented.len() ^ expected.len();
    for index in 0..compared_len {
        let left = presented.get(index).copied().unwrap_or(0);
        let right = expected.get(index).copied().unwrap_or(0);
        difference |= usize::from(left ^ right);
    }
    difference == 0
}

/// `GET /api/whoami` — proves a presented token is valid. No identity beyond that today (a single
/// shared operator token has no per-user identity to report).
pub async fn whoami_handler() -> impl IntoResponse {
    Json(serde_json::json!({ "ok": true }))
}

/// `GET /api/capabilities` — feature flags the dashboard SPA gates UI on. Currently just
/// `live_logs` (from `POLYFLARE_LIVE_LOGS`); grows as later tasks add capabilities.
pub async fn capabilities_handler(State(s): State<Arc<AppState>>) -> impl IntoResponse {
    Json(serde_json::json!({ "live_logs": s.runtime_settings.live_logs() }))
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
/// **`touch_last_used` — bounded and not awaited:** it's a best-effort audit timestamp, not
/// something the caller's request should wait on or fail because of. The generated row id (no key
/// material) is offered to the Store's bounded FIFO writer. A full/closed queue drops this audit
/// update; an occasional missed timestamp does not affect whether the key remains valid.
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
    if authenticate_client_key(&s, &headers).await {
        next.run(req).await
    } else {
        unauthorized_response()
    }
}

/// Validate and audit one client key without coupling the caller to Axum middleware composition.
///
/// Synthetic ChatGPT usage is intentionally not part of the model proxy router, but remote
/// deployments still need the same key boundary before exposing aggregate fleet capacity.
pub(crate) async fn authenticate_client_key(s: &AppState, headers: &HeaderMap) -> bool {
    let presented = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .filter(|raw| !raw.is_empty());

    let Some(raw) = presented else {
        return false;
    };

    let hash = crate::keys::sha256_hex(raw);
    match s.store.api_keys().get_by_hash(&hash).await {
        Ok(Some(row)) if row.enabled => {
            // Best-effort bounded audit write. Carries only the generated row id, never the raw
            // presented key or its hash, and never creates a task per request.
            let _ = s.store.enqueue_api_key_touch(row.id, unix_now());
            true
        }
        // Unknown hash, a revoked (`enabled == false`) row, or a store error while looking it up —
        // all fail closed to the same generic 401. See the doc comment's "Repo-error handling"
        // note for why a DB error is folded into "invalid" rather than a distinct 5xx.
        _ => false,
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

#[cfg(test)]
mod tests {
    use super::{constant_time_eq, local_dashboard_access};

    #[test]
    fn tokenless_dashboard_opens_only_on_parsed_loopback_binds() {
        assert!(local_dashboard_access(None, "127.0.0.1:8080"));
        assert!(local_dashboard_access(None, "127.8.9.10:8080"));
        assert!(local_dashboard_access(None, "[::1]:8080"));
        assert!(!local_dashboard_access(None, "0.0.0.0:8080"));
        assert!(!local_dashboard_access(None, "[::]:8080"));
        assert!(!local_dashboard_access(None, "localhost:8080"));
    }

    #[test]
    fn configured_token_always_disables_local_bypass() {
        assert!(!local_dashboard_access(Some("secret"), "127.0.0.1:8080"));
    }

    #[test]
    fn admin_token_comparison_requires_exact_bytes_and_length() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secrex", b"secret"));
        assert!(!constant_time_eq(b"secret-extra", b"secret"));
        assert!(!constant_time_eq(b"", b"secret"));
    }
}
