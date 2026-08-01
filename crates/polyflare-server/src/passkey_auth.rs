//! Passkey (WebAuthn) sign-in for the dashboard.
//!
//! # Why this exists
//! Without it, an unconfigured dashboard has exactly two postures: a shared bearer token typed by
//! hand, or — on a loopback bind with no token — *no authentication at all*, which means any local
//! process can reach every `/api` route, credential export included. A passkey gives the operator
//! real authentication with less effort than the token, which is the only way "authenticated by
//! default" actually sticks.
//!
//! # The bootstrap, and why it cannot lock you out
//! The first passkey is registered from the session that is already trusted (the open local
//! dashboard, or a valid admin token). From the moment ONE passkey exists,
//! [`crate::auth::require_admin`] stops honouring the tokenless local bypass — that transition is
//! the whole security point. Two independent break-glass paths remain: `POLYFLARE_ADMIN_TOKEN`
//! always authenticates, and `polyflare passkeys remove` operates directly on the database.
//!
//! # Origin binding (the constraint that shapes the UX)
//! WebAuthn forbids an IP address as a relying-party id, so passkeys work at
//! `http://localhost:8080` but NOT `http://127.0.0.1:8080` — browsers treat `localhost` as a
//! secure context, so plain HTTP is fine there.
//!
//! A credential is scoped to its relying-party id and usable only from that host or a subdomain of
//! it. `POLYFLARE_PASSKEY_ORIGIN` therefore takes a comma-separated LIST, and
//! `POLYFLARE_PASSKEY_RP_ID` sets the id those origins share — pointing it at a parent domain is
//! what lets one passkey cover every machine on a tailnet. Hosts with no common parent
//! (`localhost` vs a `ts.net` name) cannot share a credential under any configuration; see
//! [`build_webauthn`].
//!
//! # Challenge state
//! In-flight registration/authentication challenges live in memory with a short TTL, never in the
//! database: they are single-use, worthless after a minute, and a restart mid-ceremony should
//! simply fail the ceremony rather than leave replayable state behind.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use webauthn_rs::prelude::{
    CreationChallengeResponse, Passkey, PasskeyAuthentication, PasskeyRegistration,
    PublicKeyCredential, RegisterPublicKeyCredential, RequestChallengeResponse, Url, Uuid,
    Webauthn, WebauthnBuilder,
};

use crate::app::AppState;

/// How long an in-flight ceremony challenge stays usable.
const CHALLENGE_TTL_SECS: i64 = 120;
/// How long a passkey session lasts before the operator must touch the authenticator again.
const SESSION_TTL_SECS: i64 = 14 * 86_400;

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn safe_error(status: StatusCode, code: &'static str) -> Response {
    (status, Json(serde_json::json!({ "error": code }))).into_response()
}

/// In-flight ceremony state, keyed by an opaque handle the browser echoes back.
#[derive(Default)]
pub struct CeremonyStore {
    registrations: Mutex<HashMap<String, (PasskeyRegistration, i64)>>,
    authentications: Mutex<HashMap<String, (PasskeyAuthentication, i64)>>,
}

impl CeremonyStore {
    fn put_registration(&self, handle: String, state: PasskeyRegistration, now: i64) {
        let mut guard = self.registrations.lock().unwrap();
        guard.retain(|_, (_, expires)| *expires > now);
        guard.insert(handle, (state, now + CHALLENGE_TTL_SECS));
    }

    fn take_registration(&self, handle: &str, now: i64) -> Option<PasskeyRegistration> {
        let mut guard = self.registrations.lock().unwrap();
        // Single-use: removed on read, so a replayed handle finds nothing.
        guard
            .remove(handle)
            .filter(|(_, expires)| *expires > now)
            .map(|(state, _)| state)
    }

    fn put_authentication(&self, handle: String, state: PasskeyAuthentication, now: i64) {
        let mut guard = self.authentications.lock().unwrap();
        guard.retain(|_, (_, expires)| *expires > now);
        guard.insert(handle, (state, now + CHALLENGE_TTL_SECS));
    }

    fn take_authentication(&self, handle: &str, now: i64) -> Option<PasskeyAuthentication> {
        let mut guard = self.authentications.lock().unwrap();
        guard
            .remove(handle)
            .filter(|(_, expires)| *expires > now)
            .map(|(state, _)| state)
    }
}

/// Build the relying-party config.
///
/// `origins` is one or more origins the dashboard is browsed at (comma-separated in
/// `POLYFLARE_PASSKEY_ORIGIN`); `rp_id` optionally overrides the relying-party id, which is what
/// makes ONE passkey usable from several origins.
///
/// # Why the relying-party id is the whole story
/// A credential is bound to its RP id, and a browser will only use it from an origin whose host is
/// that id or a subdomain of it. So:
///
/// - Default (`rp_id` unset) — the id is the first origin's host, and the passkey works at exactly
///   that host.
/// - Set `rp_id` to a PARENT domain (`tailnet-name.ts.net`) while serving from
///   `mac.tailnet-name.ts.net`, and subdomain matching is enabled: one passkey then works from
///   every machine on that tailnet.
/// - Hosts that share no parent — `localhost` and `mac.tailnet.ts.net` — cannot be covered by a
///   single RP id at all. That is a WebAuthn rule, not a limitation here: browse both at the same
///   hostname, or register one passkey per origin.
///
/// `None` (passkeys disabled, server still starts) when no usable origin is configured.
pub fn build_webauthn(origins: &str, rp_id: Option<&str>) -> Option<Arc<Webauthn>> {
    let parsed = origins
        .split(',')
        .map(str::trim)
        .filter(|origin| !origin.is_empty())
        .filter_map(|origin| Url::parse(origin).ok())
        .collect::<Vec<_>>();
    let first = parsed.first()?;
    let first_host = first.host_str()?;

    // WebAuthn requires the relying-party id to be a domain; an IP literal is not permitted.
    let rp_id = rp_id.map(str::trim).filter(|id| !id.is_empty());
    let effective_rp_id = rp_id.unwrap_or(first_host);
    if effective_rp_id.parse::<std::net::IpAddr>().is_ok() {
        tracing::warn!(
            rp_id = effective_rp_id,
            "passkey sign-in disabled: WebAuthn does not allow an IP address as a relying-party \
             id — browse the dashboard at http://localhost:<port> instead"
        );
        return None;
    }
    // Subdomain matching is only meaningful when the id is a PARENT of the origin host; enabling
    // it unconditionally would widen the credential's scope for no reason.
    let needs_subdomains = parsed
        .iter()
        .filter_map(|url| url.host_str())
        .any(|host| host != effective_rp_id && host.ends_with(&format!(".{effective_rp_id}")));

    let mut builder = match WebauthnBuilder::new(effective_rp_id, first) {
        Ok(builder) => builder.rp_name("PolyFlare"),
        Err(error) => {
            tracing::warn!(
                rp_id = effective_rp_id,
                %error,
                "passkey sign-in disabled: invalid relying-party config"
            );
            return None;
        }
    };
    if needs_subdomains {
        builder = builder.allow_subdomains(true);
    }
    for extra in parsed.iter().skip(1) {
        builder = builder.append_allowed_origin(extra);
    }
    match builder.build() {
        Ok(webauthn) => Some(Arc::new(webauthn)),
        Err(error) => {
            tracing::warn!(
                rp_id = effective_rp_id,
                %error,
                "passkey sign-in disabled: invalid relying-party config"
            );
            None
        }
    }
}

/// The single relying-party user. The dashboard has one operator identity — there are no per-user
/// accounts — so the handle is derived deterministically (a SHA-256 of a fixed label, truncated to
/// 16 bytes) rather than stored: every registration on this server must agree on it, and a random
/// per-ceremony handle would make each passkey look like a different user to the authenticator.
fn operator_handle() -> Uuid {
    let digest = crate::keys::sha256_hex("polyflare-dashboard-operator");
    let mut bytes = [0u8; 16];
    for (index, slot) in bytes.iter_mut().enumerate() {
        *slot = u8::from_str_radix(&digest[index * 2..index * 2 + 2], 16).unwrap_or(0);
    }
    Uuid::from_bytes(bytes)
}

async fn registered_passkeys(state: &AppState) -> Result<Vec<(String, Passkey)>, Response> {
    let rows = state
        .store
        .passkeys()
        .list()
        .await
        .map_err(|_| safe_error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error"))?;
    Ok(rows
        .into_iter()
        .filter_map(|row| {
            serde_json::from_str::<Passkey>(&row.credential_json)
                .ok()
                .map(|passkey| (row.id, passkey))
        })
        .collect())
}

#[derive(Debug, Serialize)]
pub struct AuthStatusResponse {
    /// Whether this build/origin can do passkeys at all (see [`build_webauthn`]).
    passkey_supported: bool,
    /// Whether at least one passkey is registered. Once true, the tokenless local bypass is off.
    passkey_registered: bool,
    /// Whether this request is currently authenticated.
    authenticated: bool,
}

/// `GET /api/auth/status` — PUBLIC (never behind `require_admin`): the login screen needs to know
/// which sign-in methods exist before it can authenticate. Reports only booleans about the
/// server's posture, never anything about a specific credential.
pub async fn status_handler(
    State(state): State<Arc<AppState>>,
    local: Option<axum::Extension<crate::auth::LocalDashboardAccess>>,
    headers: HeaderMap,
) -> Response {
    let passkey_registered = state
        .store
        .passkeys()
        .any_registered()
        .await
        .unwrap_or(false);
    let authenticated =
        crate::auth::request_is_authenticated(&state, &headers, local.is_some()).await;
    Json(AuthStatusResponse {
        passkey_supported: state.webauthn.is_some(),
        passkey_registered,
        authenticated,
    })
    .into_response()
}

#[derive(Debug, Serialize)]
pub struct RegisterStartResponse {
    handle: String,
    options: CreationChallengeResponse,
}

#[derive(Debug, Deserialize)]
pub struct RegisterFinishRequest {
    handle: String,
    label: Option<String>,
    credential: RegisterPublicKeyCredential,
}

/// `POST /api/auth/passkey/register/start` — ADMIN-GATED. Registration must be reachable only from
/// an already-trusted session; an open registration endpoint would let anyone who can reach the
/// port enrol their own authenticator and own the dashboard.
pub async fn register_start_handler(State(state): State<Arc<AppState>>) -> Response {
    let Some(webauthn) = state.webauthn.clone() else {
        return safe_error(StatusCode::BAD_REQUEST, "passkey_unsupported_origin");
    };
    let existing = match registered_passkeys(&state).await {
        Ok(existing) => existing,
        Err(response) => return response,
    };
    // Excluding registered credentials stops the same authenticator enrolling twice.
    let exclude = existing
        .iter()
        .map(|(_, passkey)| passkey.cred_id().clone())
        .collect::<Vec<_>>();
    let (options, registration) = match webauthn.start_passkey_registration(
        operator_handle(),
        "polyflare",
        "PolyFlare operator",
        Some(exclude),
    ) {
        Ok(pair) => pair,
        Err(error) => {
            tracing::warn!(%error, "could not start passkey registration");
            return safe_error(StatusCode::INTERNAL_SERVER_ERROR, "registration_failed");
        }
    };
    let handle = crate::keys::random_token();
    state
        .passkey_ceremonies
        .put_registration(handle.clone(), registration, unix_now());
    Json(RegisterStartResponse { handle, options }).into_response()
}

/// `POST /api/auth/passkey/register/finish` — ADMIN-GATED. Verifies the attestation and stores the
/// resulting public credential.
pub async fn register_finish_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<RegisterFinishRequest>,
) -> Response {
    let actor = crate::identity::actor_label(&headers, state.trust_forwarded_identity);
    let Some(webauthn) = state.webauthn.clone() else {
        return safe_error(StatusCode::BAD_REQUEST, "passkey_unsupported_origin");
    };
    let Some(registration) = state
        .passkey_ceremonies
        .take_registration(&body.handle, unix_now())
    else {
        return safe_error(StatusCode::BAD_REQUEST, "challenge_expired");
    };
    let passkey = match webauthn.finish_passkey_registration(&body.credential, &registration) {
        Ok(passkey) => passkey,
        Err(error) => {
            tracing::warn!(%error, "passkey registration verification failed");
            return safe_error(StatusCode::BAD_REQUEST, "registration_rejected");
        }
    };
    let credential_id = base64url(passkey.cred_id().as_ref());
    let Ok(credential_json) = serde_json::to_string(&passkey) else {
        return safe_error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error");
    };
    let label = body
        .label
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty() && l.len() <= 64)
        .unwrap_or_else(|| "Passkey".to_string());
    let id = format!("pk_{}", crate::keys::random_token());
    if state
        .store
        .passkeys()
        .insert(&id, &credential_id, &credential_json, &label, unix_now())
        .await
        .is_err()
    {
        return safe_error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error");
    }
    tracing::warn!(passkey_id = %id, label, actor, "dashboard passkey registered");
    state.log_bus.publish(crate::log_bus::LogEvent::new(
        crate::log_bus::LogLevel::Warn,
        "passkey_registered",
        format!("passkey '{label}' registered by {actor}"),
    ));
    Json(serde_json::json!({ "id": id, "label": label })).into_response()
}

#[derive(Debug, Serialize)]
pub struct LoginStartResponse {
    handle: String,
    options: RequestChallengeResponse,
}

#[derive(Debug, Deserialize)]
pub struct LoginFinishRequest {
    handle: String,
    credential: PublicKeyCredential,
}

/// `POST /api/auth/passkey/login/start` — PUBLIC by necessity (it is the sign-in path). Safe to
/// expose: it returns only a random challenge plus the credential ids allowed to answer it, and
/// answering requires the private key held in the authenticator.
pub async fn login_start_handler(State(state): State<Arc<AppState>>) -> Response {
    let Some(webauthn) = state.webauthn.clone() else {
        return safe_error(StatusCode::BAD_REQUEST, "passkey_unsupported_origin");
    };
    let existing = match registered_passkeys(&state).await {
        Ok(existing) => existing,
        Err(response) => return response,
    };
    if existing.is_empty() {
        return safe_error(StatusCode::BAD_REQUEST, "no_passkey_registered");
    }
    let credentials = existing
        .iter()
        .map(|(_, passkey)| passkey.clone())
        .collect::<Vec<_>>();
    let (options, authentication) = match webauthn.start_passkey_authentication(&credentials) {
        Ok(pair) => pair,
        Err(error) => {
            tracing::warn!(%error, "could not start passkey authentication");
            return safe_error(StatusCode::INTERNAL_SERVER_ERROR, "authentication_failed");
        }
    };
    let handle = crate::keys::random_token();
    state
        .passkey_ceremonies
        .put_authentication(handle.clone(), authentication, unix_now());
    Json(LoginStartResponse { handle, options }).into_response()
}

/// `POST /api/auth/passkey/login/finish` — PUBLIC. Verifies the assertion and, only then, mints a
/// session token. The raw token is returned once and stored only as a SHA-256 hash.
pub async fn login_finish_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<LoginFinishRequest>,
) -> Response {
    let Some(webauthn) = state.webauthn.clone() else {
        return safe_error(StatusCode::BAD_REQUEST, "passkey_unsupported_origin");
    };
    let now = unix_now();
    let Some(authentication) = state
        .passkey_ceremonies
        .take_authentication(&body.handle, now)
    else {
        return safe_error(StatusCode::UNAUTHORIZED, "challenge_expired");
    };
    let result = match webauthn.finish_passkey_authentication(&body.credential, &authentication) {
        Ok(result) => result,
        Err(error) => {
            // Deliberately coarse: a failed assertion tells the caller nothing beyond "no".
            tracing::warn!(%error, "passkey assertion rejected");
            return safe_error(StatusCode::UNAUTHORIZED, "assertion_rejected");
        }
    };

    let existing = match registered_passkeys(&state).await {
        Ok(existing) => existing,
        Err(response) => return response,
    };
    let Some((id, mut passkey)) = existing
        .into_iter()
        .find(|(_, passkey)| passkey.cred_id() == result.cred_id())
    else {
        return safe_error(StatusCode::UNAUTHORIZED, "assertion_rejected");
    };
    // Persist the counter/backup-state the authenticator just reported: WebAuthn's cloned-device
    // detection is inert unless the stored credential advances with each assertion.
    if passkey.update_credential(&result).is_some() {
        if let Ok(updated) = serde_json::to_string(&passkey) {
            let _ = state
                .store
                .passkeys()
                .update_after_use(&id, &updated, now)
                .await;
        }
    }

    let token = crate::keys::random_token();
    let repo = state.store.passkeys();
    let _ = repo.prune_sessions(now).await;
    if repo
        .create_session(
            &crate::keys::sha256_hex(&token),
            &id,
            now,
            now + SESSION_TTL_SECS,
        )
        .await
        .is_err()
    {
        return safe_error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error");
    }
    tracing::info!(passkey_id = %id, "dashboard passkey sign-in");
    Json(serde_json::json!({
        "token": token,
        "expires_at": now + SESSION_TTL_SECS
    }))
    .into_response()
}

/// `POST /api/auth/logout` — ends the presented session. Idempotent.
pub async fn logout_handler(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let Some(token) = crate::auth::bearer_token(&headers) {
        let _ = state
            .store
            .passkeys()
            .delete_session(&crate::keys::sha256_hex(token))
            .await;
    }
    Json(serde_json::json!({ "ok": true })).into_response()
}

#[derive(Debug, Serialize)]
pub struct PasskeyView {
    id: String,
    label: String,
    created_at: i64,
    last_used_at: Option<i64>,
}

/// `GET /api/auth/passkeys` — ADMIN-GATED list for the settings UI.
pub async fn list_handler(State(state): State<Arc<AppState>>) -> Response {
    match state.store.passkeys().list().await {
        Ok(rows) => Json(
            rows.into_iter()
                .map(|row| PasskeyView {
                    id: row.id,
                    label: row.label,
                    created_at: row.created_at,
                    last_used_at: row.last_used_at,
                })
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(_) => safe_error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error"),
    }
}

/// `DELETE /api/auth/passkeys/{id}` — ADMIN-GATED. Removing the LAST passkey reopens the tokenless
/// local bypass on a loopback bind, so it is refused unless an admin token exists to authenticate
/// with afterwards; otherwise a stray click would silently downgrade the dashboard's posture.
pub async fn delete_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let actor = crate::identity::actor_label(&headers, state.trust_forwarded_identity);
    let repo = state.store.passkeys();
    let remaining = match repo.list().await {
        Ok(rows) => rows.len(),
        Err(_) => return safe_error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error"),
    };
    if remaining <= 1 && state.admin_token.is_none() {
        return safe_error(StatusCode::CONFLICT, "last_passkey_needs_admin_token");
    }
    match repo.delete(&id).await {
        Ok(true) => {
            tracing::warn!(passkey_id = %id, actor, "dashboard passkey removed");
            state.log_bus.publish(crate::log_bus::LogEvent::new(
                crate::log_bus::LogLevel::Warn,
                "passkey_removed",
                format!("passkey removed by {actor}"),
            ));
            Json(serde_json::json!({ "ok": true })).into_response()
        }
        Ok(false) => safe_error(StatusCode::NOT_FOUND, "not_found"),
        Err(_) => safe_error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error"),
    }
}

fn base64url(bytes: &[u8]) -> String {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine as _;
    URL_SAFE_NO_PAD.encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn webauthn_is_built_for_localhost_but_refused_for_ip_origins() {
        assert!(
            build_webauthn("http://localhost:8080", None).is_some(),
            "localhost is a valid relying-party id and a secure context"
        );
        // WebAuthn forbids IP-literal relying-party ids — this must fail closed, not silently
        // produce a config browsers will reject at ceremony time.
        assert!(build_webauthn("http://127.0.0.1:8080", None).is_none());
        assert!(build_webauthn("http://[::1]:8080", None).is_none());
        assert!(build_webauthn("not a url", None).is_none());
        assert!(build_webauthn("", None).is_none());
        assert!(build_webauthn("https://mac.tail1234.ts.net", None).is_some());
    }

    #[test]
    fn a_parent_rp_id_covers_every_origin_beneath_it() {
        // The multi-origin fix: one credential scoped to the tailnet domain, usable from every
        // machine on it. Both origins must be accepted by the SAME relying party.
        let webauthn = build_webauthn(
            "https://mac.tail1234.ts.net, https://laptop.tail1234.ts.net",
            Some("tail1234.ts.net"),
        )
        .expect("a parent rp id spanning two subdomains is valid");
        let origins = webauthn
            .get_allowed_origins()
            .iter()
            .map(|o| o.to_string())
            .collect::<Vec<_>>();
        assert!(origins.iter().any(|o| o.contains("mac.tail1234.ts.net")));
        assert!(origins.iter().any(|o| o.contains("laptop.tail1234.ts.net")));
    }

    #[test]
    fn an_ip_relying_party_id_is_refused_however_it_arrives() {
        // Explicitly, not just as a derived default — an operator could set it directly.
        assert!(build_webauthn("https://example.test", Some("10.0.0.1")).is_none());
    }

    #[test]
    fn unrelated_hosts_cannot_share_one_relying_party() {
        // localhost and a tailnet name share no parent, so no rp id covers both. The build still
        // succeeds (scoped to the id given) — the SECOND origin is simply not reachable by that
        // credential, which is why the docs tell operators to register one passkey per origin.
        let webauthn = build_webauthn("http://localhost:8080, https://mac.tail1234.ts.net", None)
            .expect("the first origin remains usable");
        assert!(
            webauthn
                .get_allowed_origins()
                .iter()
                .any(|o| o.to_string().contains("localhost")),
            "the relying party stays anchored to the first origin"
        );
    }

    #[test]
    fn ceremony_state_is_single_use_and_expires() {
        let store = CeremonyStore::default();
        let webauthn = build_webauthn("http://localhost:8080", None).unwrap();
        let (_, registration) = webauthn
            .start_passkey_registration(Uuid::new_v4(), "u", "u", None)
            .unwrap();
        store.put_registration("h1".into(), registration, 1_000);

        // Expired challenges are refused even before the sweep runs.
        assert!(store
            .take_registration("h1", 1_000 + CHALLENGE_TTL_SECS)
            .is_none());

        let (_, registration) = webauthn
            .start_passkey_registration(Uuid::new_v4(), "u", "u", None)
            .unwrap();
        store.put_registration("h2".into(), registration, 1_000);
        assert!(store.take_registration("h2", 1_010).is_some());
        // Single-use: a replayed handle finds nothing.
        assert!(store.take_registration("h2", 1_010).is_none());
    }
}
