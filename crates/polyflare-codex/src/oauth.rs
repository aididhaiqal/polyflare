//! OpenAI OAuth for the Codex backend: decode-only JWT claims, an 8-day refresh gate,
//! `POST /oauth/token` refresh (Task 4), and permanent-failure classification. Tokens are never
//! logged (see the redacting `Debug` on `RefreshedTokens`). See
//! docs/reference/codex-lb-port-reference.md §OAuth.

use std::time::Duration;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use serde::Deserialize;
use serde_json::Value;

/// Refresh when the stored token is older than 8 days (`token_refresh_interval_days`).
const TOKEN_REFRESH_INTERVAL_DAYS: i64 = 8;
/// The nested OpenAI auth claim carrying auth-scoped identity fields.
const AUTH_CLAIM: &str = "https://api.openai.com/auth";

/// Codex-relevant identity claims decoded (NOT signature-verified) from an id_token.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Claims {
    pub email: Option<String>,
    pub sub: Option<String>,
    pub chatgpt_account_id: Option<String>,
    pub chatgpt_user_id: Option<String>,
    pub chatgpt_plan_type: Option<String>,
    pub workspace_id: Option<String>,
    pub workspace_label: Option<String>,
    pub seat_type: Option<String>,
    pub exp: Option<i64>,
}

/// How a refresh failure should transition the account's `status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureClass {
    /// Token/session invalidated — the user can re-authenticate.
    ReauthRequired,
    /// Account-level termination — deactivate.
    Deactivated,
    /// Not a permanent failure (network / 5xx / unknown) — retry later.
    Transient,
}

impl FailureClass {
    /// The store `status` string this class maps to (`None` for `Transient` — status unchanged).
    pub fn status(self) -> Option<&'static str> {
        match self {
            FailureClass::ReauthRequired => Some("reauth_required"),
            FailureClass::Deactivated => Some("deactivated"),
            FailureClass::Transient => None,
        }
    }
}

/// Errors from OAuth decode / refresh.
#[derive(Debug, thiserror::Error)]
pub enum OAuthError {
    #[error("malformed jwt: {0}")]
    MalformedJwt(String),
    #[error("oauth transport error: {0}")]
    Transport(String),
    #[error("oauth endpoint returned status {status} (error code: {code:?})")]
    Endpoint { status: u16, code: Option<String> },
}

/// `true` when the token was last refreshed more than 8 days before `now` (epoch seconds).
pub fn should_refresh(last_refresh: i64, now: i64) -> bool {
    now - last_refresh > TOKEN_REFRESH_INTERVAL_DAYS * 86_400
}

/// Classify a token-endpoint error code into a status transition, verified against the codex-lb
/// source of truth `app/core/balancer/logic.py`: `PERMANENT_FAILURE_CODES` (12) is the full
/// permanent set, and `account_status_for_permanent_failure` returns `REAUTH_REQUIRED` when the
/// code is in `REAUTH_REQUIRED_FAILURE_CODES` (the 9 token/session codes below) else `DEACTIVATED`
/// — that `else` reaches only the 3 `account_*` account-terminal codes, since the function is only
/// called for codes already in the permanent set. Any code outside the permanent set ⇒ Transient.
pub fn classify_failure(code: &str) -> FailureClass {
    match code {
        // account-terminal (in PERMANENT_FAILURE_CODES, NOT in REAUTH_REQUIRED_FAILURE_CODES)
        "account_deactivated" | "account_suspended" | "account_deleted" => {
            FailureClass::Deactivated
        }
        // REAUTH_REQUIRED_FAILURE_CODES (codex-lb logic.py:36-48)
        "refresh_token_expired"
        | "refresh_token_reused"
        | "refresh_token_invalidated"
        | "invalid_grant"
        | "token_invalidated"
        | "token_expired"
        | "app_session_terminated"
        | "account_session_expired"
        | "account_auth_invalidated" => FailureClass::ReauthRequired,
        _ => FailureClass::Transient,
    }
}

/// Decode (WITHOUT verifying) the identity claims from a JWT id_token.
pub fn decode_claims(id_token: &str) -> Result<Claims, OAuthError> {
    let payload_b64 = id_token
        .split('.')
        .nth(1)
        .ok_or_else(|| OAuthError::MalformedJwt("missing payload segment".to_string()))?;
    let bytes = URL_SAFE_NO_PAD
        .decode(payload_b64)
        .map_err(|e| OAuthError::MalformedJwt(format!("base64url: {e}")))?;
    let v: Value = serde_json::from_slice(&bytes)
        .map_err(|e| OAuthError::MalformedJwt(format!("json: {e}")))?;

    let auth = v.get(AUTH_CLAIM);
    // Prefer the nested auth claim, then the top-level claim.
    let pick = |key: &str| -> Option<String> {
        auth.and_then(|a| a.get(key))
            .or_else(|| v.get(key))
            .and_then(Value::as_str)
            .map(str::to_string)
    };
    // chatgpt_user_id precedence: auth claim > top-level > sub.
    let chatgpt_user_id = auth
        .and_then(|a| a.get("chatgpt_user_id"))
        .or_else(|| v.get("chatgpt_user_id"))
        .or_else(|| v.get("sub"))
        .and_then(Value::as_str)
        .map(str::to_string);

    Ok(Claims {
        email: pick("email"),
        sub: v.get("sub").and_then(Value::as_str).map(str::to_string),
        chatgpt_account_id: pick("chatgpt_account_id"),
        chatgpt_user_id,
        chatgpt_plan_type: pick("chatgpt_plan_type"),
        workspace_id: pick("workspace_id"),
        workspace_label: pick("workspace_label"),
        seat_type: pick("seat_type"),
        exp: v.get("exp").and_then(Value::as_i64),
    })
}

/// The OAuth client id used by the Codex CLI (a public, non-secret protocol constant).
const CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
/// The refresh scope.
const SCOPE: &str = "openid profile email";

/// Three OAuth tokens returned by a refresh. Never logged: `Debug` redacts every field.
#[derive(Clone)]
pub struct RefreshedTokens {
    pub access_token: String,
    pub refresh_token: String,
    pub id_token: String,
}

impl std::fmt::Debug for RefreshedTokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RefreshedTokens")
            .field("access_token", &"***")
            .field("refresh_token", &"***")
            .field("id_token", &"***")
            .finish()
    }
}

/// A completed refresh: the new tokens plus the identity claims decoded from the new id_token.
#[derive(Debug, Clone)]
pub struct Refreshed {
    pub tokens: RefreshedTokens,
    pub claims: Claims,
}

/// The token-endpoint success body. `refresh_token` may be omitted (no rotation) → keep the old.
#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    refresh_token: Option<String>,
    id_token: String,
}

/// OAuth client for the Codex backend. Holds a `reqwest::Client` + the auth base URL
/// (default `https://auth.openai.com`; overridable so tests point at `MockOAuth`).
pub struct OAuthClient {
    http: reqwest::Client,
    auth_base_url: String,
}

impl OAuthClient {
    pub fn new(auth_base_url: impl Into<String>) -> Result<Self, OAuthError> {
        let http = reqwest::Client::builder()
            .build()
            .map_err(|e| OAuthError::Transport(e.to_string()))?;
        Ok(Self {
            http,
            auth_base_url: auth_base_url.into(),
        })
    }

    /// Exchange a refresh token for fresh tokens via `POST {auth_base_url}/oauth/token`. On a
    /// non-2xx response, the endpoint's `error` code (if present) is surfaced for classification.
    pub async fn refresh(&self, refresh_token: &str) -> Result<Refreshed, OAuthError> {
        let url = format!("{}/oauth/token", self.auth_base_url.trim_end_matches('/'));
        let body = serde_json::json!({
            "grant_type": "refresh_token",
            "client_id": CLIENT_ID,
            "refresh_token": refresh_token,
            "scope": SCOPE,
        });
        let resp = self
            .http
            .post(url)
            .json(&body)
            .timeout(Duration::from_secs(8))
            .send()
            .await
            .map_err(|e| OAuthError::Transport(e.to_string()))?;

        let status = resp.status();
        if !status.is_success() {
            let code = resp
                .json::<Value>()
                .await
                .ok()
                .and_then(|v| v.get("error").and_then(Value::as_str).map(str::to_string));
            return Err(OAuthError::Endpoint {
                status: status.as_u16(),
                code,
            });
        }

        let token: TokenResponse = resp
            .json()
            .await
            .map_err(|e| OAuthError::Transport(e.to_string()))?;
        let claims = decode_claims(&token.id_token)?;
        Ok(Refreshed {
            tokens: RefreshedTokens {
                access_token: token.access_token,
                // OpenAI may omit a rotated refresh token → keep the caller's existing one.
                refresh_token: token
                    .refresh_token
                    .unwrap_or_else(|| refresh_token.to_string()),
                id_token: token.id_token,
            },
            claims,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;

    /// Build a JWT with an unsigned base64url-no-pad payload from a JSON value.
    fn make_jwt(payload: &serde_json::Value) -> String {
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"none"}"#);
        let body = URL_SAFE_NO_PAD.encode(serde_json::to_vec(payload).unwrap());
        format!("{header}.{body}.sig")
    }

    #[test]
    fn should_refresh_at_8_day_boundary() {
        let day = 86_400;
        assert!(!should_refresh(0, 8 * day), "exactly 8 days ⇒ not yet");
        assert!(should_refresh(0, 8 * day + 1), "just over 8 days ⇒ refresh");
        assert!(!should_refresh(1000, 1000), "fresh");
    }

    #[test]
    fn decode_claims_reads_top_level_and_nested_auth() {
        let payload = serde_json::json!({
            "email": "user@example.test",
            "sub": "sub-123",
            "exp": 1_800_000_000i64,
            "https://api.openai.com/auth": {
                "chatgpt_account_id": "acct-xyz",
                "chatgpt_user_id": "user-auth",
                "chatgpt_plan_type": "pro"
            }
        });
        let claims = decode_claims(&make_jwt(&payload)).unwrap();
        assert_eq!(claims.email.as_deref(), Some("user@example.test"));
        assert_eq!(claims.chatgpt_account_id.as_deref(), Some("acct-xyz"));
        assert_eq!(claims.chatgpt_user_id.as_deref(), Some("user-auth")); // auth-claim wins
        assert_eq!(claims.chatgpt_plan_type.as_deref(), Some("pro"));
        assert_eq!(claims.exp, Some(1_800_000_000));
    }

    #[test]
    fn chatgpt_user_id_falls_back_to_sub() {
        let claims = decode_claims(&make_jwt(&serde_json::json!({ "sub": "sub-only" }))).unwrap();
        assert_eq!(claims.chatgpt_user_id.as_deref(), Some("sub-only"));
    }

    #[test]
    fn chatgpt_user_id_top_level_wins_over_sub_when_no_auth_claim() {
        // Middle precedence tier: no nested auth claim, but a top-level `chatgpt_user_id`
        // that differs from `sub` ⇒ the top-level value wins over `sub`.
        let claims = decode_claims(&make_jwt(&serde_json::json!({
            "sub": "sub-fallback",
            "chatgpt_user_id": "top-level-user"
        })))
        .unwrap();
        assert_eq!(claims.chatgpt_user_id.as_deref(), Some("top-level-user"));
        assert_eq!(claims.sub.as_deref(), Some("sub-fallback"));
    }

    #[test]
    fn malformed_jwt_missing_payload_errors() {
        assert!(matches!(
            decode_claims("only-one-segment"),
            Err(OAuthError::MalformedJwt(_))
        ));
    }

    #[test]
    fn classify_failure_splits_reauth_vs_deactivate_vs_transient() {
        assert_eq!(
            classify_failure("invalid_grant"),
            FailureClass::ReauthRequired
        );
        assert_eq!(
            classify_failure("refresh_token_expired"),
            FailureClass::ReauthRequired
        );
        assert_eq!(
            classify_failure("account_deleted"),
            FailureClass::Deactivated
        );
        assert_eq!(
            classify_failure("account_suspended"),
            FailureClass::Deactivated
        );
        assert_eq!(
            classify_failure("temporarily_unavailable"),
            FailureClass::Transient
        );
        assert_eq!(
            FailureClass::ReauthRequired.status(),
            Some("reauth_required")
        );
        assert_eq!(FailureClass::Deactivated.status(), Some("deactivated"));
        assert_eq!(FailureClass::Transient.status(), None);
    }

    /// Pin EVERY permanent-failure code to the status codex-lb assigns it, so the split can't
    /// silently drift. Table verified against codex-lb `app/core/balancer/logic.py`:
    /// `PERMANENT_FAILURE_CODES` (the 12 keys) + `REAUTH_REQUIRED_FAILURE_CODES` (the 9-code
    /// frozenset) driving `account_status_for_permanent_failure`.
    #[test]
    fn classify_failure_pins_every_permanent_code() {
        let cases: &[(&str, FailureClass)] = &[
            // REAUTH_REQUIRED_FAILURE_CODES (9)
            ("refresh_token_expired", FailureClass::ReauthRequired),
            ("refresh_token_reused", FailureClass::ReauthRequired),
            ("refresh_token_invalidated", FailureClass::ReauthRequired),
            ("invalid_grant", FailureClass::ReauthRequired),
            ("token_invalidated", FailureClass::ReauthRequired),
            ("token_expired", FailureClass::ReauthRequired),
            ("app_session_terminated", FailureClass::ReauthRequired),
            ("account_session_expired", FailureClass::ReauthRequired),
            ("account_auth_invalidated", FailureClass::ReauthRequired),
            // PERMANENT but NOT reauth ⇒ account-terminal ⇒ Deactivated (3)
            ("account_deactivated", FailureClass::Deactivated),
            ("account_suspended", FailureClass::Deactivated),
            ("account_deleted", FailureClass::Deactivated),
        ];
        for (code, expected) in cases {
            assert_eq!(
                classify_failure(code),
                *expected,
                "permanent code {code} must map to {expected:?}"
            );
        }
        // Anything outside PERMANENT_FAILURE_CODES is transient (not a permanent failure).
        for code in [
            "",
            "temporarily_unavailable",
            "server_error",
            "rate_limited",
            "unknown",
        ] {
            assert_eq!(
                classify_failure(code),
                FailureClass::Transient,
                "non-permanent code {code:?} must be Transient"
            );
        }
    }

    #[test]
    fn refreshed_tokens_debug_redacts_secrets() {
        let t = RefreshedTokens {
            access_token: "secret-access-xyz".to_string(),
            refresh_token: "secret-refresh-xyz".to_string(),
            id_token: "secret-id-xyz".to_string(),
        };
        let s = format!("{t:?}");
        assert!(
            !s.contains("secret-access-xyz"),
            "must not leak access token"
        );
        assert!(
            !s.contains("secret-refresh-xyz"),
            "must not leak refresh token"
        );
        assert!(!s.contains("secret-id-xyz"), "must not leak id token");
        assert!(s.contains("***"), "must redact with ***");
    }
}
