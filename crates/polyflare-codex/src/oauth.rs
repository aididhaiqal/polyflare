//! OpenAI OAuth for the Codex backend: decode-only JWT claims, an 8-day refresh gate,
//! `POST /oauth/token` refresh (Task 4), and permanent-failure classification. Tokens are never
//! logged (see the redacting `Debug` on `RefreshedTokens`). See
//! docs/reference/codex-lb-port-reference.md §OAuth.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
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

/// Classify a token-endpoint error code into a status transition (port reference §permanent
/// failure codes). Account-terminal codes ⇒ Deactivated; token/session codes ⇒ ReauthRequired;
/// anything else ⇒ Transient.
pub fn classify_failure(code: &str) -> FailureClass {
    match code {
        "account_deactivated" | "account_suspended" | "account_deleted" => {
            FailureClass::Deactivated
        }
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
}
