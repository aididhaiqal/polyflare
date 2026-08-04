//! Anthropic subscription-OAuth: authorization-code + PKCE login, token exchange, and refresh.
//!
//! Mirrors `polyflare_codex::oauth`'s shape, with three deliberate differences forced by the
//! provider:
//!
//! 1. **Opaque access tokens.** A Codex access token is a JWT whose `exp` can be decoded, so its
//!    refresh gate reads the token itself. An Anthropic access token carries no readable expiry, so
//!    the only truthful deadline is the `expires_in` the token endpoint returned — computed here
//!    and persisted alongside the token (`accounts.access_token_expires_at`).
//! 2. **Dynamic redirect URI.** The loopback callback binds an OS-assigned port, so the redirect
//!    URI is only known at authorize time and must be replayed verbatim on exchange.
//! 3. **Granted scopes are persisted and replayed.** A refresh sends back exactly what was granted;
//!    it never widens the grant, even if this binary's default scope set later grows.
//!
//! Tokens, authorization codes, and PKCE verifiers are never logged: every type holding one has a
//! redacting `Debug`.

use std::time::Duration;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use serde::Deserialize;
use serde_json::Value;

use crate::oauth_contract::{self, OAuthContract, CONTRACT};

/// Refresh this long before the stored expiry. Anthropic access tokens are short-lived compared to
/// Codex's ~10 days, so the margin is minutes, not days — but it must still comfortably exceed a
/// slow request plus a retry, so a token cannot die mid-turn.
pub const REFRESH_MARGIN_SECS: i64 = 300;

/// Errors from an Anthropic OAuth operation. Deliberately carries no token, code, or verifier.
#[derive(Debug, thiserror::Error)]
pub enum OAuthError {
    #[error("oauth transport error: {0}")]
    Transport(String),
    #[error("oauth endpoint returned status {status} (error code: {code:?})")]
    Endpoint { status: u16, code: Option<String> },
    #[error("oauth response was malformed: {0}")]
    MalformedResponse(String),
    #[error("oauth endpoint {0} is not an allowlisted Anthropic host")]
    DisallowedEndpoint(String),
    #[error("oauth state mismatch")]
    StateMismatch,
    #[error("oauth contract {0} is not verified for production use")]
    UnverifiedContract(&'static str),
}

/// How an OAuth failure should transition the account's status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureClass {
    /// The grant is gone; only a fresh operator login can recover it.
    ReauthRequired,
    /// Network, 5xx, or an unrecognized code — retry later, do not touch stored credentials.
    Transient,
}

impl FailureClass {
    /// The store `status` string this class maps to (`None` leaves the status unchanged).
    pub fn status(self) -> Option<&'static str> {
        match self {
            FailureClass::ReauthRequired => Some("reauth_required"),
            FailureClass::Transient => None,
        }
    }
}

/// Classify a token-endpoint failure.
///
/// Only errors that RFC 6749 defines as terminal for the grant are permanent. Everything else —
/// including any code this binary does not recognize — is transient, because wrongly classifying a
/// transient blip as permanent would force an operator through an avoidable browser re-login.
pub fn classify_failure(status: u16, code: Option<&str>) -> FailureClass {
    match code {
        Some("invalid_grant") | Some("invalid_client") | Some("unauthorized_client") => {
            FailureClass::ReauthRequired
        }
        // A 400 with no machine-readable code is ambiguous; only an explicit terminal code above
        // justifies discarding a working refresh token.
        _ => {
            if status == 401 && code.is_none() {
                FailureClass::ReauthRequired
            } else {
                FailureClass::Transient
            }
        }
    }
}

/// A PKCE verifier/challenge pair. The verifier is a secret until it is exchanged.
#[derive(Clone)]
pub struct Pkce {
    pub verifier: String,
    pub challenge: String,
}

impl std::fmt::Debug for Pkce {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pkce")
            .field("verifier", &"***")
            .field("challenge", &self.challenge)
            .finish()
    }
}

/// Generate a PKCE `(verifier, S256 challenge)` pair (RFC 7636).
pub fn generate_pkce() -> Pkce {
    use rand::RngCore;
    use sha2::{Digest, Sha256};
    let mut bytes = [0u8; 64];
    rand::rng().fill_bytes(&mut bytes);
    let verifier = URL_SAFE_NO_PAD.encode(bytes);
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    Pkce {
        verifier,
        challenge,
    }
}

/// Generate the `state` value (CSRF / flow-correlation token echoed on the callback).
pub fn generate_state() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

/// Constant-time-ish comparison of the returned `state` against the one this process generated.
///
/// A mismatch means the callback did not originate from the authorization request we started, so
/// the code must not be exchanged.
pub fn verify_state(expected: &str, returned: &str) -> Result<(), OAuthError> {
    if expected.len() == returned.len()
        && expected
            .bytes()
            .zip(returned.bytes())
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0
    {
        Ok(())
    } else {
        Err(OAuthError::StateMismatch)
    }
}

/// The non-secret identity of the account that authorized a grant.
///
/// `upstream_identity` is what the store keys a re-login on. It is an opaque account identifier,
/// not a credential — but it is still never printed anywhere an operator-facing string is built,
/// beyond the redacted form the CLI shows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamIdentity {
    pub upstream_identity: String,
    pub email: Option<String>,
}

impl UpstreamIdentity {
    /// A short, non-reversible display form for CLI/dashboard output.
    pub fn redacted(&self) -> String {
        let head: String = self.upstream_identity.chars().take(8).collect();
        match self.email.as_deref() {
            Some(email) => format!("{}… <{}>", head, redact_email(email)),
            None => format!("{head}…"),
        }
    }
}

/// `alice@example.test` -> `a…e@example.test`. Enough for an operator to recognize which of their
/// own accounts this is, without writing a full address into logs or screenshots.
fn redact_email(email: &str) -> String {
    match email.split_once('@') {
        Some((local, domain)) if local.len() > 2 => {
            let first = local.chars().next().unwrap_or('*');
            let last = local.chars().last().unwrap_or('*');
            format!("{first}…{last}@{domain}")
        }
        Some((_, domain)) => format!("…@{domain}"),
        None => "…".to_string(),
    }
}

/// A completed exchange or refresh: credentials plus the facts needed to persist them truthfully.
#[derive(Clone)]
pub struct OAuthTokens {
    pub access_token: String,
    pub refresh_token: String,
    /// Absolute expiry (unix seconds), computed from the response's `expires_in`. `None` only when
    /// the endpoint reported no lifetime at all.
    pub access_token_expires_at: Option<i64>,
    /// Scopes the server actually granted. May be narrower than requested.
    pub granted_scopes: String,
    /// Present on an initial exchange; a refresh response usually omits account identity.
    pub identity: Option<UpstreamIdentity>,
}

impl std::fmt::Debug for OAuthTokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthTokens")
            .field("access_token", &"***")
            .field("refresh_token", &"***")
            .field("access_token_expires_at", &self.access_token_expires_at)
            .field("granted_scopes", &self.granted_scopes)
            .field("identity", &self.identity)
            .finish()
    }
}

/// Whether a stored access token should be refreshed now.
///
/// A missing expiry refreshes eagerly: an OAuth account whose deadline we cannot read is one we
/// cannot prove is still valid, and a needless refresh is far cheaper than serving a dead token.
pub fn should_refresh(access_token_expires_at: Option<i64>, now: i64) -> bool {
    match access_token_expires_at {
        Some(expires_at) => now >= expires_at.saturating_sub(REFRESH_MARGIN_SECS),
        None => true,
    }
}

/// The token-endpoint success body.
#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    /// Absent on a refresh that does not rotate — the caller keeps its existing refresh token.
    refresh_token: Option<String>,
    expires_in: Option<i64>,
    scope: Option<String>,
    account: Option<AccountClaim>,
    #[serde(rename = "account_uuid")]
    account_uuid: Option<String>,
}

#[derive(Deserialize)]
struct AccountClaim {
    uuid: Option<String>,
    email_address: Option<String>,
    email: Option<String>,
}

/// OAuth client for Anthropic subscription accounts.
#[derive(Clone)]
pub struct AnthropicOAuthClient {
    http: reqwest::Client,
    contract: OAuthContract,
    /// Overridden only by `#[cfg(test)]` constructors; production always uses `contract`'s URLs.
    authorize_url: String,
    token_url: String,
}

impl AnthropicOAuthClient {
    /// A client bound to the reviewed production contract.
    pub fn new() -> Result<Self, OAuthError> {
        Self::with_contract(CONTRACT)
    }

    pub fn with_contract(contract: OAuthContract) -> Result<Self, OAuthError> {
        if !oauth_contract::is_allowed_endpoint(contract.authorize_url) {
            return Err(OAuthError::DisallowedEndpoint(
                contract.authorize_url.to_string(),
            ));
        }
        if !oauth_contract::is_allowed_endpoint(contract.token_url) {
            return Err(OAuthError::DisallowedEndpoint(
                contract.token_url.to_string(),
            ));
        }
        let http = reqwest::Client::builder()
            .build()
            .map_err(|e| OAuthError::Transport(e.to_string()))?;
        Ok(Self {
            http,
            contract,
            authorize_url: contract.authorize_url.to_string(),
            token_url: contract.token_url.to_string(),
        })
    }

    /// Point the client at a test server. Compiled only under `cfg(test)` or the `test-endpoints`
    /// feature, so no production configuration path — environment variable, config file, or API
    /// field — can redirect an authorization code or refresh token away from the allowlisted
    /// Anthropic hosts.
    #[cfg(any(test, feature = "test-endpoints"))]
    pub fn with_endpoints(authorize_url: String, token_url: String) -> Self {
        Self {
            http: reqwest::Client::new(),
            contract: CONTRACT,
            authorize_url,
            token_url,
        }
    }

    pub fn contract(&self) -> &OAuthContract {
        &self.contract
    }

    /// Build the authorize URL for a login.
    ///
    /// `redirect_uri` is supplied by the caller because the automatic flow binds an OS-assigned
    /// loopback port; the manual flow passes [`OAuthContract::manual_redirect_uri`]. Whichever is
    /// used, the SAME value must be replayed on [`Self::exchange_code`].
    pub fn build_authorize_url(&self, state: &str, pkce: &Pkce, redirect_uri: &str) -> String {
        reqwest::Url::parse_with_params(
            &self.authorize_url,
            &[
                ("code", "true"),
                ("client_id", self.contract.client_id),
                ("response_type", "code"),
                ("redirect_uri", redirect_uri),
                ("scope", self.contract.default_scopes),
                ("code_challenge", &pkce.challenge),
                ("code_challenge_method", "S256"),
                ("state", state),
            ],
        )
        .expect("authorize URL is built from a validated https base")
        .to_string()
    }

    /// Exchange an authorization code for tokens.
    ///
    /// `now` (unix seconds) is supplied by the caller so the absolute expiry is computed here,
    /// once, rather than left for a caller to remember — an access token persisted without its
    /// deadline would be refreshed either never or constantly.
    /// Exchange an authorization code for tokens.
    ///
    /// `state` is sent in the token request, not merely checked locally. Anthropic's callback page
    /// presents the two values CONCATENATED as `code#state`, which is only necessary if the client
    /// has to forward both — a state used purely for local CSRF would come back as an ordinary
    /// query parameter, the way every other provider returns it. Omitting it was answered with a
    /// bare `HTTP 400` carrying no `error` field, which is what a rejected request SHAPE looks
    /// like rather than a rejected grant.
    pub async fn exchange_code(
        &self,
        code: &str,
        pkce_verifier: &str,
        redirect_uri: &str,
        state: &str,
        now: i64,
    ) -> Result<OAuthTokens, OAuthError> {
        let mut body = serde_json::json!({
            "grant_type": "authorization_code",
            "client_id": self.contract.client_id,
            "code": code,
            "code_verifier": pkce_verifier,
            "redirect_uri": redirect_uri,
        });
        if !state.is_empty() {
            body["state"] = Value::String(state.to_string());
        }
        let response = self.post_token(&body, Duration::from_secs(15)).await?;
        Ok(self.to_tokens(response, None, self.contract.default_scopes, now))
    }

    /// Exchange a refresh token for a fresh access token.
    ///
    /// `granted_scopes` is what the account actually holds; it is replayed verbatim so a refresh
    /// can never widen a grant beyond what the operator originally authorized.
    pub async fn refresh(
        &self,
        refresh_token: &str,
        granted_scopes: &str,
        now: i64,
    ) -> Result<OAuthTokens, OAuthError> {
        let mut body = serde_json::json!({
            "grant_type": "refresh_token",
            "client_id": self.contract.client_id,
            "refresh_token": refresh_token,
        });
        if !granted_scopes.trim().is_empty() {
            body["scope"] = Value::String(granted_scopes.to_string());
        }
        let response = self.post_token(&body, Duration::from_secs(8)).await?;
        // A response that omits `refresh_token` means "keep using the one you have" — discarding it
        // would strand the account with no way to refresh again.
        Ok(self.to_tokens(response, Some(refresh_token), granted_scopes, now))
    }

    async fn post_token(
        &self,
        body: &Value,
        timeout: Duration,
    ) -> Result<TokenResponse, OAuthError> {
        let response = self
            .http
            .post(&self.token_url)
            .json(body)
            .timeout(timeout)
            .send()
            .await
            .map_err(|e| OAuthError::Transport(e.to_string()))?;

        let status = response.status();
        if !status.is_success() {
            let code = response
                .json::<Value>()
                .await
                .ok()
                .and_then(|v| v.get("error").and_then(Value::as_str).map(str::to_string));
            return Err(OAuthError::Endpoint {
                status: status.as_u16(),
                code,
            });
        }
        response
            .json::<TokenResponse>()
            .await
            .map_err(|e| OAuthError::MalformedResponse(e.to_string()))
    }

    fn to_tokens(
        &self,
        response: TokenResponse,
        fallback_refresh_token: Option<&str>,
        requested_scopes: &str,
        now: i64,
    ) -> OAuthTokens {
        let identity = extract_identity(&response);
        OAuthTokens {
            access_token: response.access_token,
            refresh_token: response
                .refresh_token
                .or_else(|| fallback_refresh_token.map(str::to_string))
                .unwrap_or_default(),
            access_token_expires_at: absolute_expiry(response.expires_in, now),
            // An omitted `scope` means the server granted exactly what was asked for (RFC 6749 §5.1).
            granted_scopes: response
                .scope
                .unwrap_or_else(|| requested_scopes.to_string()),
            identity,
        }
    }
}

/// Recover the account identity from a token response.
///
/// Tolerates absence entirely: the plan requests `user:profile` only when profile-derived identity
/// is genuinely required, so an ordinary `user:inference` login must work from whatever the token
/// response itself carries.
fn extract_identity(response: &TokenResponse) -> Option<UpstreamIdentity> {
    let uuid = response
        .account
        .as_ref()
        .and_then(|a| a.uuid.clone())
        .or_else(|| response.account_uuid.clone())?;
    let email = response
        .account
        .as_ref()
        .and_then(|a| a.email_address.clone().or_else(|| a.email.clone()));
    Some(UpstreamIdentity {
        upstream_identity: uuid,
        email,
    })
}

/// Turn a response's relative `expires_in` into the absolute deadline the store persists.
///
/// Separate from the client so the caller supplies `now` (no hidden clock read), which keeps the
/// expiry deterministic in tests and makes the single source of "when does this token die" obvious.
pub fn absolute_expiry(expires_in: Option<i64>, now: i64) -> Option<i64> {
    expires_in
        .filter(|secs| *secs > 0)
        .map(|secs| now.saturating_add(secs))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pkce_challenge_is_the_s256_of_the_verifier() {
        use sha2::{Digest, Sha256};
        let pkce = generate_pkce();
        assert_eq!(
            pkce.challenge,
            URL_SAFE_NO_PAD.encode(Sha256::digest(pkce.verifier.as_bytes()))
        );
        // Two logins must never share a verifier.
        assert_ne!(pkce.verifier, generate_pkce().verifier);
        // The verifier must not leak through Debug.
        assert!(!format!("{pkce:?}").contains(&pkce.verifier));
    }

    #[test]
    fn state_must_match_exactly() {
        let state = generate_state();
        assert!(verify_state(&state, &state).is_ok());
        assert!(matches!(
            verify_state(&state, "other"),
            Err(OAuthError::StateMismatch)
        ));
        let mut tampered = state.clone();
        tampered.pop();
        tampered.push(if state.ends_with('A') { 'B' } else { 'A' });
        assert!(matches!(
            verify_state(&state, &tampered),
            Err(OAuthError::StateMismatch)
        ));
    }

    #[test]
    fn refresh_gate_fires_within_the_margin_and_when_expiry_is_unknown() {
        let expires_at = 1_800_000_000;
        assert!(!should_refresh(Some(expires_at), expires_at - 600));
        assert!(should_refresh(
            Some(expires_at),
            expires_at - REFRESH_MARGIN_SECS
        ));
        assert!(should_refresh(Some(expires_at), expires_at + 1));
        // Unknown expiry refreshes rather than gambling on a token we cannot vouch for.
        assert!(should_refresh(None, 0));
        // A pathological stored value must not underflow into "never refresh".
        assert!(should_refresh(Some(i64::MIN), 0));
    }

    #[test]
    fn absolute_expiry_ignores_nonpositive_lifetimes() {
        assert_eq!(absolute_expiry(Some(3600), 1_000), Some(4_600));
        assert_eq!(absolute_expiry(None, 1_000), None);
        assert_eq!(absolute_expiry(Some(0), 1_000), None);
        assert_eq!(absolute_expiry(Some(-5), 1_000), None);
    }

    #[test]
    fn only_terminal_grant_errors_force_reauth() {
        assert_eq!(
            classify_failure(400, Some("invalid_grant")),
            FailureClass::ReauthRequired
        );
        assert_eq!(
            classify_failure(401, Some("invalid_client")),
            FailureClass::ReauthRequired
        );
        assert_eq!(classify_failure(401, None), FailureClass::ReauthRequired);
        // Ambiguous / transient conditions must never discard a working refresh token.
        assert_eq!(classify_failure(500, None), FailureClass::Transient);
        assert_eq!(classify_failure(429, None), FailureClass::Transient);
        assert_eq!(classify_failure(400, None), FailureClass::Transient);
        assert_eq!(
            classify_failure(400, Some("temporarily_unavailable")),
            FailureClass::Transient
        );
        assert_eq!(FailureClass::Transient.status(), None);
        assert_eq!(
            FailureClass::ReauthRequired.status(),
            Some("reauth_required")
        );
    }

    #[test]
    fn identity_is_redacted_for_display() {
        let identity = UpstreamIdentity {
            upstream_identity: "0123456789abcdef-uuid".into(),
            email: Some("alice@example.test".into()),
        };
        let shown = identity.redacted();
        assert_eq!(shown, "01234567… <a…e@example.test>");
        assert!(!shown.contains("0123456789abcdef-uuid"));
        assert!(!shown.contains("alice@"));

        let short_local = UpstreamIdentity {
            upstream_identity: "abc".into(),
            email: Some("ab@example.test".into()),
        };
        assert_eq!(short_local.redacted(), "abc… <…@example.test>");
    }

    #[test]
    fn authorize_url_carries_pkce_and_the_callers_redirect() {
        let client = AnthropicOAuthClient::new().unwrap();
        let pkce = generate_pkce();
        let url = client.build_authorize_url("state-xyz", &pkce, "http://127.0.0.1:49152/callback");
        let parsed = reqwest::Url::parse(&url).unwrap();
        let params: std::collections::HashMap<_, _> = parsed.query_pairs().into_owned().collect();
        assert_eq!(parsed.host_str(), Some("claude.com"));
        assert_eq!(params["client_id"], CONTRACT.client_id);
        assert_eq!(params["code_challenge"], pkce.challenge);
        assert_eq!(params["code_challenge_method"], "S256");
        assert_eq!(params["state"], "state-xyz");
        assert_eq!(params["redirect_uri"], "http://127.0.0.1:49152/callback");
        // Only the inference scope — never Claude Code's broader permissions.
        assert_eq!(params["scope"], "user:inference");
        // The verifier is a secret and must never appear in a URL the browser sees.
        assert!(!url.contains(&pkce.verifier));
    }

    /// A client pointed at a spawned mock token endpoint.
    async fn mock_client(mock: polyflare_testkit::MockAnthropicOAuth) -> AnthropicOAuthClient {
        let token_url = mock.spawn().await;
        AnthropicOAuthClient::with_endpoints(CONTRACT.authorize_url.to_string(), token_url)
    }

    #[tokio::test]
    async fn exchange_records_expiry_scopes_and_identity_and_replays_the_redirect() {
        let mock = polyflare_testkit::MockAnthropicOAuth::ok("access-1", "refresh-1");
        let client = mock_client(mock.clone()).await;

        let tokens = client
            .exchange_code(
                "auth-code",
                "verifier-xyz",
                "http://127.0.0.1:49152/callback",
                "state-xyz",
                1_000,
            )
            .await
            .unwrap();

        assert_eq!(tokens.access_token, "access-1");
        assert_eq!(tokens.refresh_token, "refresh-1");
        // 1_000 + expires_in(3600). Without this the refresh gate has no deadline to read.
        assert_eq!(tokens.access_token_expires_at, Some(4_600));
        assert_eq!(tokens.granted_scopes, "user:inference");
        assert_eq!(
            tokens.identity,
            Some(UpstreamIdentity {
                upstream_identity: "acct-uuid-1".into(),
                email: Some("operator@example.test".into()),
            })
        );

        let body = mock.last_body().unwrap();
        assert_eq!(body["grant_type"], "authorization_code");
        assert_eq!(body["code_verifier"], "verifier-xyz");
        // The token endpoint compares the redirect byte-for-byte against the authorize request.
        assert_eq!(body["redirect_uri"], "http://127.0.0.1:49152/callback");
        assert_eq!(body["client_id"], CONTRACT.client_id);
        // `state` travels IN the exchange, not merely checked locally. Anthropic presents the
        // callback as `code#state` — concatenated — which is only necessary if the client has to
        // forward both. Omitting it drew a bare HTTP 400 with no `error` field: a rejected request
        // shape, not a rejected grant.
        assert_eq!(body["state"], "state-xyz");
    }

    /// A caller with no state to send must not put an empty one on the wire.
    #[tokio::test]
    async fn an_absent_state_is_omitted_rather_than_sent_empty() {
        let mock = polyflare_testkit::MockAnthropicOAuth::ok("access-1", "refresh-1");
        let client = mock_client(mock.clone()).await;
        client
            .exchange_code(
                "auth-code",
                "verifier-xyz",
                "http://127.0.0.1:1/cb",
                "",
                1_000,
            )
            .await
            .unwrap();
        let body = mock.last_body().unwrap();
        assert!(
            body.get("state").is_none(),
            "an empty state is worse than none: it asserts a value the client does not have"
        );
    }

    #[tokio::test]
    async fn refresh_replays_the_granted_scope_and_never_widens_it() {
        let mock = polyflare_testkit::MockAnthropicOAuth::ok("access-2", "refresh-2");
        let client = mock_client(mock.clone()).await;

        let tokens = client
            .refresh("refresh-1", "user:inference", 5_000)
            .await
            .unwrap();

        assert_eq!(tokens.access_token, "access-2");
        assert_eq!(tokens.refresh_token, "refresh-2", "rotation is adopted");
        assert_eq!(tokens.access_token_expires_at, Some(8_600));

        let body = mock.last_body().unwrap();
        assert_eq!(body["grant_type"], "refresh_token");
        assert_eq!(body["refresh_token"], "refresh-1");
        // Exactly the persisted grant is replayed — a refresh must not request more than the
        // operator originally authorized, even if this binary's defaults later grow.
        assert_eq!(body["scope"], "user:inference");
    }

    #[tokio::test]
    async fn a_refresh_that_does_not_rotate_keeps_the_existing_refresh_token() {
        let mock = polyflare_testkit::MockAnthropicOAuth::from_response(
            polyflare_testkit::AnthropicOAuthResponse::Ok {
                access_token: "access-3".into(),
                // The server omits `refresh_token` entirely.
                refresh_token: None,
                expires_in: Some(60),
                scope: None,
                account_uuid: None,
                email: None,
            },
        );
        let client = mock_client(mock).await;

        let tokens = client
            .refresh("refresh-keepme", "user:inference", 100)
            .await
            .unwrap();

        // Dropping the old token here would strand the account with no way to ever refresh again.
        assert_eq!(tokens.refresh_token, "refresh-keepme");
        assert_eq!(tokens.access_token, "access-3");
        assert_eq!(tokens.access_token_expires_at, Some(160));
        // An omitted `scope` means the grant is unchanged, not empty.
        assert_eq!(tokens.granted_scopes, "user:inference");
        assert_eq!(tokens.identity, None, "a refresh need not restate identity");
    }

    #[tokio::test]
    async fn a_narrowed_grant_is_recorded_as_what_the_server_actually_returned() {
        let mock = polyflare_testkit::MockAnthropicOAuth::from_response(
            polyflare_testkit::AnthropicOAuthResponse::Ok {
                access_token: "access-4".into(),
                refresh_token: Some("refresh-4".into()),
                expires_in: Some(3600),
                // The server grants LESS than was requested.
                scope: Some("user:inference".into()),
                account_uuid: Some("acct-uuid-2".into()),
                email: None,
            },
        );
        let client = mock_client(mock).await;

        let tokens = client
            .exchange_code("code", "verifier", CONTRACT.manual_redirect_uri, "state", 0)
            .await
            .unwrap();
        assert_eq!(tokens.granted_scopes, "user:inference");
        assert_eq!(
            tokens.identity.unwrap().email,
            None,
            "identity works without a profile scope"
        );
    }

    #[tokio::test]
    async fn terminal_and_transient_endpoint_failures_are_distinguished() {
        let client = mock_client(polyflare_testkit::MockAnthropicOAuth::error(
            400,
            "invalid_grant",
        ))
        .await;
        let err = client
            .refresh("dead-token", "user:inference", 0)
            .await
            .unwrap_err();
        let OAuthError::Endpoint { status, code } = err else {
            panic!("expected an endpoint error, got {err:?}");
        };
        assert_eq!(
            classify_failure(status, code.as_deref()),
            FailureClass::ReauthRequired
        );

        let client = mock_client(polyflare_testkit::MockAnthropicOAuth::error_no_code(503)).await;
        let err = client
            .refresh("live-token", "user:inference", 0)
            .await
            .unwrap_err();
        let OAuthError::Endpoint { status, code } = err else {
            panic!("expected an endpoint error, got {err:?}");
        };
        assert_eq!(
            classify_failure(status, code.as_deref()),
            FailureClass::Transient,
            "a 5xx must not discard a working refresh token"
        );
    }

    #[tokio::test]
    async fn oauth_errors_never_carry_token_material() {
        let client = mock_client(polyflare_testkit::MockAnthropicOAuth::error(
            400,
            "invalid_grant",
        ))
        .await;
        let err = client
            .exchange_code(
                "SECRET-CODE",
                "SECRET-VERIFIER",
                "http://127.0.0.1:1/cb",
                "SECRET-STATE",
                0,
            )
            .await
            .unwrap_err();
        let rendered = format!("{err} {err:?}");
        assert!(!rendered.contains("SECRET-CODE"));
        assert!(!rendered.contains("SECRET-VERIFIER"));
    }

    #[test]
    fn a_contract_pointing_off_the_allowlist_is_refused() {
        let rogue = OAuthContract {
            token_url: "https://evil.test/v1/oauth/token",
            ..CONTRACT
        };
        assert!(matches!(
            AnthropicOAuthClient::with_contract(rogue),
            Err(OAuthError::DisallowedEndpoint(_))
        ));
    }
}
