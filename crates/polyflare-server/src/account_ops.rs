//! Two admin account operations the dashboard's Lifecycle panel exposes: an on-demand health
//! probe, and a credential export.
//!
//! # Force probe
//! Rotates the access token if it is stale, then fetches live `/wham/usage` and reports what came
//! back. Deliberately does NOT send an inference request: a probe must never spend the quota it is
//! reporting on. Both halves reuse the machinery the background sweep already runs
//! ([`crate::reactive_auth::ReactiveAuth::refresh_stale_codex_token`] +
//! [`crate::usage_refresh::refresh_account_now`]), so a probe and a sweep cycle cannot disagree.
//!
//! # Credential export
//! Returns the account's decrypted tokens in the codex CLI `auth.json` shape (codex-lb's
//! `export_auth` equivalent). This is the ONE endpoint that deliberately hands plaintext
//! credentials out of the server, so it is deliberately narrow:
//!
//! - `POST`, never `GET` — a URL carrying no secret still ends up in browser history and proxy
//!   logs, and a GET is reachable by cross-origin form/img navigation in a way a JSON POST is not.
//! - `Cache-Control: no-store` on the response, so neither the browser nor any intermediary keeps
//!   a copy on disk.
//! - Every export publishes an audit event to the live-log bus (account id + operation only — the
//!   bus is content-free and MUST NOT carry the tokens themselves).
//! - Admin-gated like every `/api/*` route (`crate::auth::require_admin`).
//!
//! Request/response BODIES are never logged anywhere (see `crate::observability`'s content-safety
//! constraint), which is what keeps the exported tokens out of the request log.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use polyflare_codex::oauth::token_exp;
use polyflare_core::AccountId;
use serde::Serialize;

use crate::app::AppState;
use crate::log_bus::LogEvent;

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[derive(Debug, Serialize)]
pub struct ProbeResponse {
    /// Whether live usage was fetched successfully. `false` means the upstream probe did not
    /// return trustworthy usage (bad credential, upstream error) — the caller should treat the
    /// account as unhealthy rather than assume the cached numbers still hold.
    usage_refreshed: bool,
    /// Whether the probe rotated a stale access token before probing.
    token_rotated: bool,
    /// `valid` | `expired` | `missing`, derived post-probe from the access token's own `exp`.
    token_state: &'static str,
    token_expires_at: Option<i64>,
    /// The account's status after the probe (a probe can move it between usage-controlled
    /// statuses, e.g. clear a `quota_exceeded` whose window has since reset).
    status: String,
    probed_at: i64,
}

/// `POST /api/accounts/{id}/probe` — refresh this account's credential + live usage on demand.
pub async fn probe_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    headers: axum::http::HeaderMap,
) -> Response {
    let actor = crate::identity::actor_label(&headers, state.trust_forwarded_identity);
    let repo = state.store.accounts();
    let account = match repo.get(&id).await {
        Ok(Some(account)) => account,
        Ok(None) => return (StatusCode::NOT_FOUND, "no such account").into_response(),
        Err(_) => return crate::ingress::internal_error(),
    };
    // Usage probing is Codex-specific: `/wham/usage` has no Anthropic equivalent, so a probe there
    // would report nothing while looking like it succeeded.
    if account.provider != "codex" {
        return (
            StatusCode::BAD_REQUEST,
            "probe is only available for codex accounts",
        )
            .into_response();
    }

    // Rotate first so the usage fetch below presents a live bearer. A rotation failure is not
    // fatal to the probe — the usage fetch reports the resulting health either way.
    let token_rotated = crate::reactive_auth::ReactiveAuth::new(
        state.store.clone(),
        state.cipher.clone(),
        state.oauth.clone(),
        state.refresh_locks.clone(),
        state
            .upstream_base_url_for(polyflare_core::Provider::Codex)
            .to_string(),
        state
            .upstream_base_url_for(polyflare_core::Provider::Anthropic)
            .to_string(),
    )
    .refresh_stale_codex_token(&AccountId::from(id.as_str()), unix_now())
    .await
    .unwrap_or(false);

    let usage_refreshed = crate::usage_refresh::refresh_account_now(&state, &id)
        .await
        .unwrap_or(false);

    // Re-read post-probe: the rotation and the usage write may both have changed this row.
    let (status, token_state, token_expires_at) =
        match repo.get_with_tokens(&id, &state.cipher).await {
            Ok(Some((account, tokens))) => {
                let now = unix_now();
                let (state_label, exp) = match token_exp(&tokens.access_token) {
                    Some(exp) if exp < now => ("expired", Some(exp)),
                    Some(exp) => ("valid", Some(exp)),
                    None => ("missing", None),
                };
                (account.status, state_label, exp)
            }
            Ok(None) | Err(_) => return crate::ingress::internal_error(),
        };

    state.log_bus.publish(LogEvent::info(
        "account_probe",
        format!(
            "probe by {actor}: usage_refreshed={usage_refreshed} token_rotated={token_rotated} \
             token={token_state}"
        ),
    ));

    Json(ProbeResponse {
        usage_refreshed,
        token_rotated,
        token_state,
        token_expires_at,
        status,
        probed_at: unix_now(),
    })
    .into_response()
}

/// The codex CLI `auth.json` token block.
#[derive(Debug, Serialize)]
pub struct ExportedTokens {
    id_token: String,
    access_token: String,
    refresh_token: String,
    account_id: Option<String>,
}

/// The codex CLI `auth.json` document. `OPENAI_API_KEY` is always null for an OAuth account —
/// codex reads it as "this file authenticates by tokens, not an API key".
#[derive(Debug, Serialize)]
pub struct ExportAuthResponse {
    #[serde(rename = "OPENAI_API_KEY")]
    openai_api_key: Option<String>,
    tokens: ExportedTokens,
    /// RFC 3339, matching what the codex CLI writes.
    last_refresh: String,
}

/// Format a unix timestamp as the RFC 3339 UTC string the codex CLI writes into `auth.json`.
/// Hand-rolled (civil-from-days) rather than pulling a date crate in for one field.
fn rfc3339_utc(unix_seconds: i64) -> String {
    let days = unix_seconds.div_euclid(86_400);
    let secs_of_day = unix_seconds.rem_euclid(86_400);
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        y,
        m,
        d,
        secs_of_day / 3_600,
        (secs_of_day % 3_600) / 60,
        secs_of_day % 60
    )
}

/// `POST /api/accounts/{id}/export-auth` — the account's credentials as a codex CLI `auth.json`.
///
/// See the module docs for why this is POST-only, `no-store`, and audited. Codex accounts only:
/// an Anthropic or static-bearer account has no `auth.json` shape to export into, and emitting a
/// half-filled document would produce a file that silently fails in the CLI.
pub async fn export_auth_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    headers: axum::http::HeaderMap,
) -> Response {
    let actor = crate::identity::actor_label(&headers, state.trust_forwarded_identity);
    let repo = state.store.accounts();
    let (account, tokens) = match repo.get_with_tokens(&id, &state.cipher).await {
        Ok(Some(pair)) => pair,
        Ok(None) => return (StatusCode::NOT_FOUND, "no such account").into_response(),
        Err(_) => return crate::ingress::internal_error(),
    };
    if account.provider != "codex" {
        return (
            StatusCode::BAD_REQUEST,
            "auth export is only available for codex accounts",
        )
            .into_response();
    }
    if tokens.refresh_token.is_empty() {
        return (
            StatusCode::CONFLICT,
            "this account has no refresh token to export",
        )
            .into_response();
    }

    // Audit BEFORE returning: an export that reaches the client must already be recorded, and the
    // bus carries the account id + operation only — never the tokens.
    state.log_bus.publish(LogEvent::new(
        crate::log_bus::LogLevel::Warn,
        "account_auth_export",
        format!("credentials exported as codex auth.json by {actor}"),
    ));
    tracing::warn!(
        account_id = %account.id,
        actor,
        "account credentials exported via the dashboard auth-export endpoint"
    );

    let body = ExportAuthResponse {
        openai_api_key: None,
        tokens: ExportedTokens {
            id_token: tokens.id_token.clone(),
            access_token: tokens.access_token.clone(),
            refresh_token: tokens.refresh_token.clone(),
            account_id: account.chatgpt_account_id.clone(),
        },
        last_refresh: rfc3339_utc(account.last_refresh),
    };
    (
        // Keep the secret out of every cache between here and the operator's browser.
        [
            (header::CACHE_CONTROL, "no-store, max-age=0"),
            (header::PRAGMA, "no-cache"),
        ],
        Json(body),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_utc_matches_known_timestamps() {
        assert_eq!(rfc3339_utc(0), "1970-01-01T00:00:00Z");
        assert_eq!(rfc3339_utc(1_700_000_000), "2023-11-14T22:13:20Z");
        // A leap day, and the last second before a year rolls over.
        assert_eq!(rfc3339_utc(1_709_208_000), "2024-02-29T12:00:00Z");
        assert_eq!(rfc3339_utc(1_735_689_599), "2024-12-31T23:59:59Z");
        assert_eq!(rfc3339_utc(1_735_689_600), "2025-01-01T00:00:00Z");
    }
}
