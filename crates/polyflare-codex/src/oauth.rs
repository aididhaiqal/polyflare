//! OpenAI OAuth for the Codex backend: decode-only JWT claims, an expiry-driven refresh gate
//! (`should_refresh`/`token_exp` — refresh within a margin of the access token's own `exp`, with an
//! age-gate fallback), `POST /oauth/token` refresh (Task 4), and permanent-failure classification.
//! Tokens are never logged (see the redacting `Debug` on `RefreshedTokens`). See
//! docs/reference/codex-lb-port-reference.md §OAuth.

use std::time::Duration;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use serde::Deserialize;
use serde_json::Value;

/// Refresh once the access token is within this window of its own `exp` — a GENEROUS margin (2
/// days) so a transient refresh failure still has ~2 days of request-opportunities to retry before
/// the token actually dies. The real Codex access-token TTL is ~10 days, so this refreshes ~2 days
/// early: it adapts to whatever TTL the auth server issues (unlike a hardcoded age gate, it can't
/// silently start refreshing AFTER expiry if OpenAI shortens the TTL) while keeping the early-refresh
/// resilience of the prior fixed 8-day gate.
const REFRESH_MARGIN_SECS: i64 = 2 * 86_400;
/// Fallback age gate, used ONLY when the access token's `exp` can't be decoded (malformed / missing):
/// refresh when the stored token is older than this. Ensures we still refresh eventually rather than
/// never if we somehow can't read an expiry.
const TOKEN_REFRESH_FALLBACK_DAYS: i64 = 8;
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

/// Decode (WITHOUT verifying) a JWT's `exp` claim (unix seconds) — used to time refresh against the
/// access token's ACTUAL expiry. `None` for a malformed / exp-less token (caller falls back to the
/// age gate). The access token is a JWT just like the id_token; this reads only its `exp`.
pub fn token_exp(jwt: &str) -> Option<i64> {
    let payload_b64 = jwt.split('.').nth(1)?;
    let bytes = URL_SAFE_NO_PAD.decode(payload_b64).ok()?;
    let v: Value = serde_json::from_slice(&bytes).ok()?;
    v.get("exp").and_then(Value::as_i64)
}

/// Whether the account's access token should be refreshed now (epoch seconds). Primary signal is the
/// token's own `exp`: refresh once within [`REFRESH_MARGIN_SECS`] of expiry, so timing adapts to the
/// server-issued TTL. If `exp` can't be decoded (`access_exp` is `None`), fall back to the fixed
/// age gate on `last_refresh` so we still refresh eventually.
pub fn should_refresh(access_exp: Option<i64>, last_refresh: i64, now: i64) -> bool {
    match access_exp {
        // `saturating_sub`: `exp` comes from an UNVERIFIED token, so a pathological value near
        // `i64::MIN` must not underflow (debug/CI panic; release wrap-to-huge ⇒ never refresh a dead
        // token). Saturating keeps the intended meaning — a garbage/very-past `exp` ⇒ refresh.
        Some(exp) => now >= exp.saturating_sub(REFRESH_MARGIN_SECS),
        None => now - last_refresh > TOKEN_REFRESH_FALLBACK_DAYS * 86_400,
    }
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
/// The LOGIN (authorization_code) scope. MUST include `offline_access` or the exchange returns no
/// refresh token; the extra `api.connectors.*` scopes match the real Codex CLI's authorize request.
/// Deliberately distinct from the refresh-only [`SCOPE`].
const AUTHORIZE_SCOPE: &str =
    "openid profile email offline_access api.connectors.read api.connectors.invoke";
/// The Codex CLI's own `originator` on the authorize URL (fingerprint-faithful — matches
/// `codex_headers::originator()` and codex-rs `login/src/server.rs`).
const LOGIN_ORIGINATOR: &str = "codex_cli_rs";
/// The fixed loopback redirect the OpenAI app registration expects — host/port are NOT changeable
/// ("OpenAI dislikes port changes"). Both codex-lb and CLIProxyAPI hardcode this exact value.
pub const REDIRECT_URI: &str = "http://localhost:1455/auth/callback";
/// The loopback callback port, matching [`REDIRECT_URI`].
pub const CALLBACK_PORT: u16 = 1455;

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
///
/// `claims` is **best-effort**: a refresh returns fresh access/refresh tokens even when the new
/// `id_token` can't be decoded (`None`). The refresh path only needs the tokens; discarding a
/// perfectly good token pair because the cosmetic `id_token` was malformed would wrongly deactivate
/// an otherwise-healthy account. (Identity claims are populated authoritatively at import, not here.)
#[derive(Debug, Clone)]
pub struct Refreshed {
    pub tokens: RefreshedTokens,
    pub claims: Option<Claims>,
}

/// The token-endpoint success body. `refresh_token` may be omitted (no rotation) → keep the old.
#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    refresh_token: Option<String>,
    id_token: String,
}

/// Generate a PKCE `(verifier, S256 challenge)` pair (RFC 7636): a 64-random-byte URL-safe-base64
/// (no-pad) verifier, and `challenge = base64url(SHA256(verifier_ascii))`.
pub fn generate_pkce() -> (String, String) {
    use rand::RngCore;
    use sha2::{Digest, Sha256};
    let mut bytes = [0u8; 64];
    rand::rng().fill_bytes(&mut bytes);
    let verifier = URL_SAFE_NO_PAD.encode(bytes);
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    (verifier, challenge)
}

/// Generate a random `state` value (the CSRF / flow-correlation token echoed on the callback).
pub fn generate_state() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
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
        // Best-effort: a malformed id_token must NOT discard the valid access/refresh tokens.
        let claims = decode_claims(&token.id_token).ok();
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

    /// The Codex CLI's authorize URL for the authorization_code + PKCE login flow. Parameters and
    /// order match codex-rs `login/src/server.rs::build_authorize_url` (fingerprint-faithful).
    pub fn build_authorize_url(&self, state: &str, code_challenge: &str) -> String {
        let base = format!(
            "{}/oauth/authorize",
            self.auth_base_url.trim_end_matches('/')
        );
        reqwest::Url::parse_with_params(
            &base,
            &[
                ("response_type", "code"),
                ("client_id", CLIENT_ID),
                ("redirect_uri", REDIRECT_URI),
                ("scope", AUTHORIZE_SCOPE),
                ("code_challenge", code_challenge),
                ("code_challenge_method", "S256"),
                ("id_token_add_organizations", "true"),
                ("codex_cli_simplified_flow", "true"),
                ("state", state),
                ("originator", LOGIN_ORIGINATOR),
            ],
        )
        .expect("authorize URL is always valid")
        .to_string()
    }

    /// Exchange an authorization `code` (+ the PKCE `code_verifier`) for tokens. Unlike [`refresh`]
    /// (JSON), the authorization_code grant is sent **form-urlencoded** — matching both codex-lb and
    /// CLIProxyAPI. Returns the same [`Refreshed`] shape (tokens + decoded identity claims).
    ///
    /// [`refresh`]: Self::refresh
    pub async fn exchange_code(
        &self,
        code: &str,
        code_verifier: &str,
        redirect_uri: &str,
    ) -> Result<Refreshed, OAuthError> {
        let url = format!("{}/oauth/token", self.auth_base_url.trim_end_matches('/'));
        let resp = self
            .http
            .post(url)
            .form(&[
                ("grant_type", "authorization_code"),
                ("client_id", CLIENT_ID),
                ("code", code),
                ("code_verifier", code_verifier),
                ("redirect_uri", redirect_uri),
            ])
            .timeout(Duration::from_secs(15))
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
        let claims = decode_claims(&token.id_token).ok();
        Ok(Refreshed {
            tokens: RefreshedTokens {
                access_token: token.access_token,
                // The authorization_code grant (with `offline_access`) always returns a refresh
                // token; default defensively rather than panic if a mock/edge case omits it.
                refresh_token: token.refresh_token.unwrap_or_default(),
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
    fn should_refresh_is_driven_by_token_exp() {
        let day = 86_400;
        let now = 1_000_000_000;
        // Token expires in 3 days ⇒ outside the 2-day margin ⇒ don't refresh yet.
        assert!(
            !should_refresh(Some(now + 3 * day), 0, now),
            "3d to exp ⇒ wait"
        );
        // Exactly at the 2-day margin ⇒ refresh.
        assert!(
            should_refresh(Some(now + 2 * day), 0, now),
            "at margin ⇒ refresh"
        );
        // Within 2 days of expiry ⇒ refresh.
        assert!(
            should_refresh(Some(now + day), 0, now),
            "1d to exp ⇒ refresh"
        );
        // Already expired ⇒ refresh.
        assert!(should_refresh(Some(now - day), 0, now), "expired ⇒ refresh");
        // A fresh 10-day token (the real Codex TTL) is NOT refreshed — regardless of last_refresh.
        assert!(
            !should_refresh(Some(now + 10 * day), now - 9 * day, now),
            "fresh 10d token ⇒ no refresh even if last_refresh is old"
        );
    }

    #[test]
    fn should_refresh_falls_back_to_age_gate_without_exp() {
        let day = 86_400;
        // No decodable exp ⇒ the 8-day age gate on last_refresh applies.
        assert!(
            !should_refresh(None, 0, 8 * day),
            "None + exactly 8d ⇒ not yet"
        );
        assert!(
            should_refresh(None, 0, 8 * day + 1),
            "None + over 8d ⇒ refresh"
        );
        assert!(
            !should_refresh(None, 1000, 1000),
            "None + fresh ⇒ no refresh"
        );
    }

    #[test]
    fn should_refresh_saturates_on_pathological_exp() {
        // A corrupt/hostile token's `exp` near i64::MIN must NOT underflow the margin subtraction
        // (debug panic / release wrap). Saturating ⇒ a garbage very-past exp is treated as "refresh".
        let now = 1_000_000_000;
        assert!(
            should_refresh(Some(i64::MIN), 0, now),
            "i64::MIN exp ⇒ refresh, no panic"
        );
        assert!(should_refresh(Some(i64::MIN + 1), 0, now));
        // The far-future end can't underflow and must NOT trigger a refresh.
        assert!(
            !should_refresh(Some(i64::MAX), 0, now),
            "i64::MAX exp ⇒ no refresh"
        );
    }

    #[test]
    fn token_exp_decodes_exp_or_none() {
        let jwt = make_jwt(&serde_json::json!({ "exp": 1_800_000_000i64, "sub": "s" }));
        assert_eq!(token_exp(&jwt), Some(1_800_000_000));
        // No exp claim ⇒ None (caller falls back to the age gate).
        assert_eq!(
            token_exp(&make_jwt(&serde_json::json!({ "sub": "s" }))),
            None
        );
        // Malformed JWT ⇒ None.
        assert_eq!(token_exp("not-a-jwt"), None);
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

    #[test]
    fn generate_pkce_challenge_is_s256_of_verifier() {
        use sha2::{Digest, Sha256};
        let (verifier, challenge) = generate_pkce();
        // RFC 7636: challenge = base64url-no-pad(SHA256(ASCII(verifier))).
        let expected = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        assert_eq!(challenge, expected);
        // Verifier is a non-trivial URL-safe string; two calls differ (randomness).
        assert!(verifier.len() >= 43);
        assert!(!verifier.contains(['+', '/', '=']));
        assert_ne!(generate_pkce().0, verifier);
    }

    #[test]
    fn build_authorize_url_carries_the_codex_login_params() {
        let client = OAuthClient::new("https://auth.example.test").unwrap();
        let url = client.build_authorize_url("STATE123", "CHALLENGE456");
        let parsed = reqwest::Url::parse(&url).unwrap();
        assert_eq!(parsed.path(), "/oauth/authorize");
        let q: std::collections::HashMap<_, _> = parsed.query_pairs().into_owned().collect();
        assert_eq!(q["response_type"], "code");
        assert_eq!(q["client_id"], "app_EMoamEEZ73f0CkXaXp7hrann");
        assert_eq!(q["redirect_uri"], "http://localhost:1455/auth/callback");
        assert_eq!(q["code_challenge"], "CHALLENGE456");
        assert_eq!(q["code_challenge_method"], "S256");
        assert_eq!(q["state"], "STATE123");
        assert_eq!(q["originator"], "codex_cli_rs");
        // MUST request offline_access or no refresh token comes back.
        assert!(
            q["scope"].contains("offline_access"),
            "scope: {}",
            q["scope"]
        );
    }
}
