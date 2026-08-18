//! Shared same-account OAuth recovery for upstream `401` responses.
//!
//! Responses, control endpoints, WebSocket handshakes/redials, and model discovery must all
//! serialize refresh-token rotation through the same per-account lock. Keeping the mechanism here
//! prevents a control-plane catalog request from racing a data-plane request with the same refresh
//! token.

use std::time::Duration;

use polyflare_anthropic::oauth::{
    classify_failure as classify_anthropic_failure, AnthropicOAuthClient,
    OAuthError as AnthropicOAuthError,
};
use polyflare_codex::oauth::{
    classify_failure, is_fedramp_account, should_refresh, token_exp, OAuthClient, OAuthError,
};
use polyflare_core::{Account, AccountId};
use polyflare_store::{AuthMode, PlainTokens, Store, TokenCipher};

use crate::refresh_locks::RefreshLocks;

/// Bounded retries for the post-refresh token persist. Losing this write after upstream rotates
/// the refresh token makes the stored credentials unusable on the next refresh.
pub(crate) const PERSIST_MAX_ATTEMPTS: u32 = 3;
pub(crate) const PERSIST_RETRY_BACKOFF: Duration = Duration::from_millis(100);

#[derive(Clone)]
pub struct ReactiveAuth {
    store: Store,
    cipher: TokenCipher,
    oauth: OAuthClient,
    refresh_locks: RefreshLocks,
    codex_base_url: String,
    anthropic_base_url: String,
    /// Built from the reviewed Anthropic contract at construction. `None` if that construction
    /// failed (an endpoint outside the allowlist), in which case an Anthropic OAuth account simply
    /// cannot be refreshed — the request fails rather than falling back to another provider's
    /// auth server.
    anthropic_oauth: Option<AnthropicOAuthClient>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReactiveAuthError {
    Internal,
    AccountUnavailable,
}

impl ReactiveAuth {
    pub fn new(
        store: Store,
        cipher: TokenCipher,
        oauth: OAuthClient,
        refresh_locks: RefreshLocks,
        codex_base_url: String,
        anthropic_base_url: String,
    ) -> Self {
        Self {
            store,
            cipher,
            oauth,
            refresh_locks,
            codex_base_url,
            anthropic_base_url,
            anthropic_oauth: AnthropicOAuthClient::new().ok(),
        }
    }

    /// Point Anthropic refreshes at a mock token endpoint. Test-only (see the crate's
    /// `test-endpoints` dev-dependency feature); production always uses the reviewed contract.
    #[cfg(test)]
    pub(crate) fn with_anthropic_token_url(mut self, token_url: String) -> Self {
        self.anthropic_oauth = Some(AnthropicOAuthClient::with_endpoints(
            polyflare_anthropic::CONTRACT.authorize_url.to_string(),
            token_url,
        ));
        self
    }

    /// Refresh once after `rejected_access_token` receives a 401. Concurrent callers for the same
    /// account single-flight; waiters adopt the already-rotated stored token rather than refreshing
    /// again. `Ok(None)` represents a transient OAuth failure for which the original 401 should
    /// remain visible.
    pub(crate) async fn refresh_after_unauthorized(
        &self,
        picked: &AccountId,
        rejected_access_token: &str,
        now: i64,
    ) -> Result<Option<Account>, ReactiveAuthError> {
        let repo = self.store.accounts();
        let lock = self.refresh_locks.handle(picked);
        let _guard = lock.lock().await;
        let (stored_account, stored_tokens, auth) = repo
            .get_with_tokens_and_auth(picked.as_str(), &self.cipher)
            .await
            .map_err(|_| ReactiveAuthError::Internal)?
            .ok_or(ReactiveAuthError::Internal)?;
        if stored_account.status != "active" {
            return Err(ReactiveAuthError::AccountUnavailable);
        }

        // Which auth server (if any) may be asked to rotate THIS account's credentials. Dispatching
        // on the stored mode rather than the ingress path is what stops an Anthropic account being
        // refreshed against OpenAI's token endpoint, or a static API key being "refreshed" at all.
        let mode = auth.mode();
        let base_url = match mode {
            AuthMode::CodexOauth => self.codex_base_url.clone(),
            AuthMode::AnthropicOauth => self.anthropic_base_url.clone(),
            // A static bearer has no refresh token and no auth server. A 401 on one is a real
            // credential problem the operator must fix, so surface it instead of retrying.
            AuthMode::StaticBearer | AuthMode::Unknown => {
                return Err(ReactiveAuthError::AccountUnavailable)
            }
        };

        // A peer already rotated this account's token while we waited on the lock: adopt theirs
        // rather than burning a second refresh (and, with a rotating server, invalidating theirs).
        if stored_tokens.access_token != rejected_access_token {
            return Ok(Some(Account {
                id: stored_account.id,
                base_url,
                bearer_token: stored_tokens.access_token.clone(),
                chatgpt_account_id: stored_account.chatgpt_account_id,
                is_fedramp: mode == AuthMode::CodexOauth
                    && is_fedramp_account(&stored_tokens.id_token),
            }));
        }

        if mode == AuthMode::AnthropicOauth {
            return self
                .refresh_anthropic(
                    picked,
                    &stored_account,
                    &stored_tokens,
                    &auth,
                    base_url,
                    now,
                )
                .await;
        }

        let new = match self
            .rotate_codex_tokens(picked, &stored_tokens.refresh_token, now)
            .await?
        {
            Some(new) => new,
            None => return Ok(None),
        };

        Ok(Some(Account {
            id: stored_account.id,
            base_url: self.codex_base_url.clone(),
            bearer_token: new.access_token.clone(),
            chatgpt_account_id: stored_account.chatgpt_account_id,
            is_fedramp: is_fedramp_account(&new.id_token),
        }))
    }

    /// Proactively rotate a STALE Codex access token off the request path. The usage sweep calls
    /// this each cycle so an idle account's token is kept warm instead of refreshing only when
    /// traffic next lands on it — without this, an account idle past its refresh token's lifetime
    /// dies straight into `reauth_required`.
    ///
    /// Runs for every usage-controlled status (`active`/`rate_limited`/`quota_exceeded`): a benched
    /// account is exactly the idle account whose token would otherwise go stale. Never touches
    /// `paused`/`reauth_required`/`deactivated`. Returns whether a rotation was persisted.
    pub(crate) async fn refresh_stale_codex_token(
        &self,
        picked: &AccountId,
        now: i64,
    ) -> Result<bool, ReactiveAuthError> {
        let repo = self.store.accounts();
        let lock = self.refresh_locks.handle(picked);
        let _guard = lock.lock().await;
        // Fresh read under the lock: a request-path refresh may have rotated while we waited.
        let (stored_account, stored_tokens, auth) = repo
            .get_with_tokens_and_auth(picked.as_str(), &self.cipher)
            .await
            .map_err(|_| ReactiveAuthError::Internal)?
            .ok_or(ReactiveAuthError::Internal)?;
        if !matches!(
            stored_account.status.as_str(),
            "active" | "rate_limited" | "quota_exceeded"
        ) {
            return Ok(false);
        }
        if auth.mode() != AuthMode::CodexOauth {
            return Ok(false);
        }
        if !should_refresh(
            token_exp(&stored_tokens.access_token),
            stored_account.last_refresh,
            now,
        ) {
            return Ok(false);
        }
        Ok(self
            .rotate_codex_tokens(picked, &stored_tokens.refresh_token, now)
            .await?
            .is_some())
    }

    /// Proactively refresh an idle Anthropic account's opaque access token before it expires — the
    /// Anthropic analogue of [`Self::refresh_stale_codex_token`]. Without it, an Anthropic account
    /// with no request traffic relies solely on the on-request 401→refresh path, so one left idle
    /// past its access-token lifetime can strand into `reauth_required`. Expiry comes from the
    /// stored column (the token is opaque, with no JWT `exp`); a `None` column refreshes eagerly.
    pub(crate) async fn refresh_stale_anthropic_token(
        &self,
        picked: &AccountId,
        now: i64,
    ) -> Result<bool, ReactiveAuthError> {
        let repo = self.store.accounts();
        let lock = self.refresh_locks.handle(picked);
        let _guard = lock.lock().await;
        // Fresh read under the lock: a request-path refresh may have rotated while we waited.
        let (stored_account, stored_tokens, auth) = repo
            .get_with_tokens_and_auth(picked.as_str(), &self.cipher)
            .await
            .map_err(|_| ReactiveAuthError::Internal)?
            .ok_or(ReactiveAuthError::Internal)?;
        if !matches!(
            stored_account.status.as_str(),
            "active" | "rate_limited" | "quota_exceeded"
        ) {
            return Ok(false);
        }
        if auth.mode() != AuthMode::AnthropicOauth {
            return Ok(false);
        }
        if !polyflare_anthropic::oauth::should_refresh(auth.access_token_expires_at, now) {
            return Ok(false);
        }
        Ok(self
            .refresh_anthropic(
                picked,
                &stored_account,
                &stored_tokens,
                &auth,
                self.anthropic_base_url.clone(),
                now,
            )
            .await?
            .is_some())
    }

    /// The shared Codex rotate-and-persist step. The caller MUST hold this account's refresh lock
    /// and have re-read `stored_refresh_token` under it. `Ok(None)` is a transient OAuth failure —
    /// stored credentials untouched, the caller's original error (if any) stays visible.
    async fn rotate_codex_tokens(
        &self,
        picked: &AccountId,
        stored_refresh_token: &str,
        now: i64,
    ) -> Result<Option<PlainTokens>, ReactiveAuthError> {
        let repo = self.store.accounts();
        let refreshed = match self.oauth.refresh(stored_refresh_token).await {
            Ok(refreshed) => refreshed,
            Err(OAuthError::Endpoint {
                code: Some(code), ..
            }) => {
                if let Some(status) = classify_failure(&code).status() {
                    let _ = repo.update_status(picked.as_str(), status).await;
                    return Err(ReactiveAuthError::AccountUnavailable);
                }
                return Ok(None);
            }
            Err(OAuthError::Endpoint { code: None, .. }) | Err(OAuthError::MalformedJwt(_)) => {
                let _ = repo.update_status(picked.as_str(), "reauth_required").await;
                return Err(ReactiveAuthError::AccountUnavailable);
            }
            Err(OAuthError::Transport(_)) => return Ok(None),
        };

        let new = PlainTokens {
            access_token: refreshed.tokens.access_token,
            refresh_token: refreshed.tokens.refresh_token,
            id_token: refreshed.tokens.id_token,
        };
        let mut persisted = false;
        for attempt in 1..=PERSIST_MAX_ATTEMPTS {
            match repo
                .update_tokens(picked.as_str(), &new, &self.cipher, now)
                .await
            {
                Ok(()) => {
                    persisted = true;
                    break;
                }
                Err(error) if attempt < PERSIST_MAX_ATTEMPTS => {
                    tracing::warn!(
                        attempt,
                        error = %error,
                        "persist of reactively refreshed tokens failed; retrying"
                    );
                    tokio::time::sleep(PERSIST_RETRY_BACKOFF).await;
                }
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        "failed to persist reactively refreshed tokens after \
                         {PERSIST_MAX_ATTEMPTS} attempts"
                    );
                }
            }
        }
        if !persisted {
            // Fail closed, immediately: the upstream exchange already CONSUMED the stored refresh
            // token, so the row now holds dead credentials whatever we do. Marking here surfaces
            // the account as needing re-auth at the moment it became true, instead of `active`
            // limping along until the next refresh burns as `refresh_token_reused`. Best-effort:
            // if storage is down this write fails too, and the next refresh attempt re-marks.
            let _ = repo.update_status(picked.as_str(), "reauth_required").await;
            return Err(ReactiveAuthError::Internal);
        }
        Ok(Some(new))
    }

    /// The Anthropic half of [`Self::refresh_after_unauthorized`]. Called with the per-account lock
    /// already held.
    ///
    /// Differs from the Codex path in three ways the provider forces: the access token is opaque so
    /// its expiry comes from the response and must be persisted with it; the persisted grant is
    /// replayed so a refresh can never widen it; and an omitted `refresh_token` means "keep yours"
    /// rather than "you now have none".
    async fn refresh_anthropic(
        &self,
        picked: &AccountId,
        stored_account: &polyflare_store::Account,
        stored_tokens: &PlainTokens,
        auth: &polyflare_store::UpstreamAuth,
        base_url: String,
        now: i64,
    ) -> Result<Option<Account>, ReactiveAuthError> {
        let repo = self.store.accounts();
        // No usable client means no refresh. Falling back to any other auth server would send this
        // account's refresh token somewhere it does not belong.
        let Some(client) = self.anthropic_oauth.as_ref() else {
            return Err(ReactiveAuthError::AccountUnavailable);
        };

        let granted = auth.granted_scopes.clone().unwrap_or_default();
        let refreshed = match client
            .refresh(&stored_tokens.refresh_token, &granted, now)
            .await
        {
            Ok(refreshed) => refreshed,
            Err(AnthropicOAuthError::Endpoint { status, code }) => {
                match classify_anthropic_failure(status, code.as_deref()).status() {
                    Some(status) => {
                        let _ = repo.update_status(picked.as_str(), status).await;
                        return Err(ReactiveAuthError::AccountUnavailable);
                    }
                    // Transient: leave the stored credentials untouched and let the original 401
                    // surface. Discarding a working refresh token here would force an avoidable
                    // browser re-login for what may be a momentary upstream blip.
                    None => return Ok(None),
                }
            }
            Err(_) => return Ok(None),
        };

        let new = PlainTokens {
            access_token: refreshed.access_token,
            refresh_token: refreshed.refresh_token,
            // Anthropic issues no id_token; the column keeps its empty placeholder.
            id_token: String::new(),
        };
        let mut persisted = false;
        for attempt in 1..=PERSIST_MAX_ATTEMPTS {
            match repo
                .update_oauth_tokens(
                    picked.as_str(),
                    &new,
                    &self.cipher,
                    now,
                    refreshed.access_token_expires_at,
                )
                .await
            {
                Ok(()) => {
                    persisted = true;
                    break;
                }
                Err(error) if attempt < PERSIST_MAX_ATTEMPTS => {
                    tracing::warn!(
                        attempt,
                        error = %error,
                        "persist of reactively refreshed Anthropic tokens failed; retrying"
                    );
                    tokio::time::sleep(PERSIST_RETRY_BACKOFF).await;
                }
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        "failed to persist reactively refreshed Anthropic tokens after \
                         {PERSIST_MAX_ATTEMPTS} attempts"
                    );
                }
            }
        }
        // Fail closed: upstream may have rotated the refresh token, so a token we could not store
        // is one that will not survive a restart. Using it now would hide an account we have
        // already lost the ability to refresh. Mark it immediately (best-effort — storage may
        // still be down) rather than leaving `active` credentials that die on the next refresh.
        if !persisted {
            let _ = repo.update_status(picked.as_str(), "reauth_required").await;
            return Err(ReactiveAuthError::Internal);
        }

        Ok(Some(Account {
            id: stored_account.id.clone(),
            base_url,
            bearer_token: new.access_token.clone(),
            // Anthropic accounts carry no ChatGPT companion identity and are never FedRAMP-routed.
            chatgpt_account_id: None,
            is_fedramp: false,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polyflare_store::Account as StoredAccount;
    use polyflare_testkit::MockOAuth;

    #[tokio::test]
    async fn concurrent_clients_share_one_refresh_token_rotation() {
        let fedramp_id_token = concat!(
            "eyJhbGciOiJub25lIn0.",
            "eyJodHRwczovL2FwaS5vcGVuYWkuY29tL2F1dGgiOnsiY2hhdGdwdF9hY2NvdW50X2lz",
            "X2ZlZHJhbXAiOnRydWV9fQ.sig"
        );
        let oauth = MockOAuth::ok("new-access", "new-refresh", fedramp_id_token);
        let oauth_handle = oauth.clone();
        let oauth_url = oauth.spawn().await;
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let cipher = TokenCipher::from_key_bytes(&[59u8; 32]).unwrap();
        store
            .accounts()
            .insert(
                &StoredAccount {
                    id: "acct-a".to_string(),
                    chatgpt_account_id: Some("chatgpt-a".to_string()),
                    chatgpt_user_id: None,
                    email: "a@example.test".to_string(),
                    alias: None,
                    workspace_id: None,
                    workspace_label: None,
                    seat_type: None,
                    plan_type: "plus".to_string(),
                    routing_policy: "eligible".to_string(),
                    last_refresh: 1,
                    created_at: 1,
                    status: "active".to_string(),
                    deactivation_reason: None,
                    reset_at: None,
                    blocked_at: None,
                    security_work_authorized: false,
                    usage_cap_percent: None,
                    usage_cap_override: false,
                    provider: "codex".to_string(),
                    pool: None,
                },
                &PlainTokens {
                    access_token: "old-access".to_string(),
                    refresh_token: "old-refresh".to_string(),
                    id_token: "old-id".to_string(),
                },
                &cipher,
            )
            .await
            .unwrap();

        let auth = ReactiveAuth::new(
            store.clone(),
            cipher.clone(),
            OAuthClient::new(oauth_url).unwrap(),
            RefreshLocks::default(),
            "https://example.test/backend-api/codex".to_string(),
            "https://example.test/anthropic".to_string(),
        );
        let catalog_plane = auth.clone();
        let data_plane = auth.clone();
        let account = AccountId::from("acct-a");
        let catalog_account = account.clone();
        let data_account = account.clone();
        let (catalog_result, data_result) = tokio::join!(
            catalog_plane.refresh_after_unauthorized(&catalog_account, "old-access", 10),
            data_plane.refresh_after_unauthorized(&data_account, "old-access", 10),
        );

        for result in [catalog_result, data_result] {
            let refreshed = result.unwrap().expect("both callers adopt a valid bearer");
            assert_eq!(refreshed.id, account.as_str());
            assert_eq!(refreshed.bearer_token, "new-access");
            assert!(
                refreshed.is_fedramp,
                "both the refresher and lock waiter must adopt the refreshed account identity"
            );
        }
        assert_eq!(
            oauth_handle.hit_count(),
            1,
            "only the lock winner may rotate the shared refresh token"
        );
        let (_, stored) = store
            .accounts()
            .get_with_tokens("acct-a", &cipher)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.access_token, "new-access");
        assert_eq!(stored.refresh_token, "new-refresh");
        assert_eq!(stored.id_token, fedramp_id_token);
    }

    #[tokio::test]
    async fn refreshed_identity_is_not_returned_when_rotated_tokens_cannot_persist() {
        let oauth = MockOAuth::ok("new-access", "new-refresh", "eyJhbGciOiJub25lIn0.e30.sig");
        let oauth_url = oauth.spawn().await;
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let cipher = TokenCipher::from_key_bytes(&[60u8; 32]).unwrap();
        store
            .accounts()
            .insert(
                &StoredAccount {
                    id: "acct-persist-fails".to_string(),
                    chatgpt_account_id: Some("chatgpt-old".to_string()),
                    chatgpt_user_id: None,
                    email: "persist@example.test".to_string(),
                    alias: None,
                    workspace_id: None,
                    workspace_label: None,
                    seat_type: None,
                    plan_type: "plus".to_string(),
                    routing_policy: "eligible".to_string(),
                    last_refresh: 1,
                    created_at: 1,
                    status: "active".to_string(),
                    deactivation_reason: None,
                    reset_at: None,
                    blocked_at: None,
                    security_work_authorized: false,
                    usage_cap_percent: None,
                    usage_cap_override: false,
                    provider: "codex".to_string(),
                    pool: None,
                },
                &PlainTokens {
                    access_token: "old-access".to_string(),
                    refresh_token: "old-refresh".to_string(),
                    id_token: "old-id".to_string(),
                },
                &cipher,
            )
            .await
            .unwrap();
        sqlx::query(
            "CREATE TRIGGER reject_token_rotation \
             BEFORE UPDATE OF access_token_enc, refresh_token_enc, id_token_enc ON accounts \
             BEGIN SELECT RAISE(FAIL, 'injected token persistence failure'); END",
        )
        .execute(store.pool())
        .await
        .unwrap();
        let auth = ReactiveAuth::new(
            store.clone(),
            cipher.clone(),
            OAuthClient::new(oauth_url).unwrap(),
            RefreshLocks::default(),
            "https://example.test/backend-api/codex".to_string(),
            "https://example.test/anthropic".to_string(),
        );

        let result = auth
            .refresh_after_unauthorized(&AccountId::from("acct-persist-fails"), "old-access", 10)
            .await;

        assert!(matches!(result, Err(ReactiveAuthError::Internal)));
        let (account, stored) = store
            .accounts()
            .get_with_tokens("acct-persist-fails", &cipher)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.access_token, "old-access");
        assert_eq!(stored.refresh_token, "old-refresh");
        // The upstream exchange already consumed the stored refresh token, so the row now holds
        // dead credentials: the account must be parked immediately, not left `active` to die as
        // `refresh_token_reused` on its next refresh.
        assert_eq!(account.status, "reauth_required");
    }

    fn codex_account(id: &str, status: &str, last_refresh: i64) -> StoredAccount {
        StoredAccount {
            id: id.to_string(),
            chatgpt_account_id: Some(format!("chatgpt-{id}")),
            chatgpt_user_id: None,
            email: "sweep@example.test".to_string(),
            alias: None,
            workspace_id: None,
            workspace_label: None,
            seat_type: None,
            plan_type: "plus".to_string(),
            routing_policy: "eligible".to_string(),
            last_refresh,
            created_at: 1,
            status: status.to_string(),
            deactivation_reason: None,
            reset_at: None,
            blocked_at: None,
            security_work_authorized: false,
            usage_cap_percent: None,
            usage_cap_override: false,
            provider: "codex".to_string(),
            pool: None,
        }
    }

    async fn sweep_fixture(
        dir: &tempfile::TempDir,
        oauth: MockOAuth,
        status: &str,
        last_refresh: i64,
    ) -> (Store, TokenCipher, ReactiveAuth) {
        let oauth_url = oauth.spawn().await;
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let cipher = TokenCipher::from_key_bytes(&[61u8; 32]).unwrap();
        store
            .accounts()
            .insert(
                &codex_account("acct-sweep", status, last_refresh),
                &PlainTokens {
                    // Not a JWT, so staleness runs on the age gate against `last_refresh`.
                    access_token: "old-access".to_string(),
                    refresh_token: "old-refresh".to_string(),
                    id_token: "old-id".to_string(),
                },
                &cipher,
            )
            .await
            .unwrap();
        let auth = ReactiveAuth::new(
            store.clone(),
            cipher.clone(),
            OAuthClient::new(oauth_url).unwrap(),
            RefreshLocks::default(),
            "https://example.test/backend-api/codex".to_string(),
            "https://example.test/anthropic".to_string(),
        );
        (store, cipher, auth)
    }

    /// Ten days after `last_refresh = 1` — comfortably past the 8-day no-exp age gate.
    const STALE_NOW: i64 = 10 * 86_400;

    #[tokio::test]
    async fn sweep_rotates_a_stale_token_and_persists_it() {
        let oauth = MockOAuth::ok("new-access", "new-refresh", "eyJhbGciOiJub25lIn0.e30.sig");
        let oauth_handle = oauth.clone();
        let dir = tempfile::tempdir().unwrap();
        let (store, cipher, auth) = sweep_fixture(&dir, oauth, "active", 1).await;

        let rotated = auth
            .refresh_stale_codex_token(&AccountId::from("acct-sweep"), STALE_NOW)
            .await
            .unwrap();

        assert!(rotated);
        assert_eq!(oauth_handle.hit_count(), 1);
        let (_, stored) = store
            .accounts()
            .get_with_tokens("acct-sweep", &cipher)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.access_token, "new-access");
        assert_eq!(stored.refresh_token, "new-refresh");
    }

    #[tokio::test]
    async fn sweep_keeps_a_benched_accounts_token_warm_but_never_a_parked_one() {
        // A quota-benched account is exactly the idle account whose token would otherwise go
        // stale over a week-long bench.
        let oauth = MockOAuth::ok("new-access", "new-refresh", "eyJhbGciOiJub25lIn0.e30.sig");
        let oauth_handle = oauth.clone();
        let dir = tempfile::tempdir().unwrap();
        let (_, _, auth) = sweep_fixture(&dir, oauth, "quota_exceeded", 1).await;
        assert!(auth
            .refresh_stale_codex_token(&AccountId::from("acct-sweep"), STALE_NOW)
            .await
            .unwrap());
        assert_eq!(oauth_handle.hit_count(), 1);

        // Parked statuses are untouchable: only an operator re-auth may revive them.
        for status in ["reauth_required", "paused", "deactivated"] {
            let oauth = MockOAuth::ok("new-access", "new-refresh", "eyJhbGciOiJub25lIn0.e30.sig");
            let oauth_handle = oauth.clone();
            let dir = tempfile::tempdir().unwrap();
            let (_, _, auth) = sweep_fixture(&dir, oauth, status, 1).await;
            assert!(
                !auth
                    .refresh_stale_codex_token(&AccountId::from("acct-sweep"), STALE_NOW)
                    .await
                    .unwrap(),
                "{status} must not be refreshed"
            );
            assert_eq!(oauth_handle.hit_count(), 0, "{status} must not hit OAuth");
        }
    }

    #[tokio::test]
    async fn sweep_leaves_a_fresh_token_alone() {
        let oauth = MockOAuth::ok("new-access", "new-refresh", "eyJhbGciOiJub25lIn0.e30.sig");
        let oauth_handle = oauth.clone();
        let dir = tempfile::tempdir().unwrap();
        // Refreshed "now": the age gate is nowhere near due.
        let (store, cipher, auth) = sweep_fixture(&dir, oauth, "active", STALE_NOW).await;

        let rotated = auth
            .refresh_stale_codex_token(&AccountId::from("acct-sweep"), STALE_NOW)
            .await
            .unwrap();

        assert!(!rotated);
        assert_eq!(oauth_handle.hit_count(), 0);
        let (_, stored) = store
            .accounts()
            .get_with_tokens("acct-sweep", &cipher)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.access_token, "old-access");
    }
}

#[cfg(test)]
mod anthropic_refresh_tests {
    use super::*;
    use polyflare_store::{Account as StoreAccount, NewUpstreamAuth};
    use polyflare_testkit::{AnthropicOAuthResponse, MockAnthropicOAuth};

    fn anthropic_account(id: &str) -> StoreAccount {
        StoreAccount {
            id: id.to_string(),
            chatgpt_account_id: None,
            chatgpt_user_id: None,
            email: "operator@example.test".to_string(),
            alias: None,
            workspace_id: None,
            workspace_label: None,
            seat_type: None,
            plan_type: "max".to_string(),
            routing_policy: "normal".to_string(),
            last_refresh: 1,
            created_at: 1,
            status: "active".to_string(),
            deactivation_reason: None,
            reset_at: None,
            blocked_at: None,
            security_work_authorized: false,
            usage_cap_percent: None,
            usage_cap_override: false,
            provider: "anthropic".to_string(),
            pool: None,
        }
    }

    /// A store holding one Anthropic subscription-OAuth account, plus a `ReactiveAuth` whose
    /// Anthropic refreshes go to `mock`.
    async fn fixture(
        dir: &tempfile::TempDir,
        mock: MockAnthropicOAuth,
    ) -> (Store, TokenCipher, ReactiveAuth) {
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let cipher = TokenCipher::from_key_bytes(&[3u8; 32]).unwrap();
        store
            .accounts()
            .upsert_anthropic_oauth(
                &anthropic_account("acct-anthropic"),
                &PlainTokens {
                    access_token: "old-access".to_string(),
                    refresh_token: "old-refresh".to_string(),
                    id_token: String::new(),
                },
                &cipher,
                &NewUpstreamAuth {
                    upstream_identity: "seat-1".to_string(),
                    access_token_expires_at: Some(100),
                    oauth_contract_version: "anthropic-oauth-2026-07".to_string(),
                    granted_scopes: "user:inference".to_string(),
                },
            )
            .await
            .unwrap();
        let token_url = mock.spawn().await;
        let auth = ReactiveAuth::new(
            store.clone(),
            cipher.clone(),
            polyflare_codex::oauth::OAuthClient::new("https://unused.test").unwrap(),
            RefreshLocks::default(),
            "https://example.test/backend-api/codex".to_string(),
            "https://api.anthropic.test".to_string(),
        )
        .with_anthropic_token_url(token_url);
        (store, cipher, auth)
    }

    #[tokio::test]
    async fn anthropic_401_refreshes_against_anthropic_and_persists_the_new_expiry() {
        let dir = tempfile::tempdir().unwrap();
        let mock = MockAnthropicOAuth::ok("new-access", "new-refresh");
        let (store, cipher, auth) = fixture(&dir, mock.clone()).await;

        let refreshed = auth
            .refresh_after_unauthorized(&AccountId::from("acct-anthropic"), "old-access", 1_000)
            .await
            .unwrap()
            .expect("a successful refresh yields a usable account");

        assert_eq!(refreshed.bearer_token, "new-access");
        // The Anthropic base URL, never the Codex one.
        assert_eq!(refreshed.base_url, "https://api.anthropic.test");
        assert_eq!(refreshed.chatgpt_account_id, None);
        assert!(!refreshed.is_fedramp);

        // The persisted grant was replayed verbatim — a refresh never widens what was authorized.
        assert_eq!(mock.last_body().unwrap()["scope"], "user:inference");
        assert_eq!(mock.last_body().unwrap()["grant_type"], "refresh_token");

        let (_, tokens, stored) = store
            .accounts()
            .get_with_tokens_and_auth("acct-anthropic", &cipher)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(tokens.access_token, "new-access");
        assert_eq!(tokens.refresh_token, "new-refresh");
        // 1_000 + expires_in(3600). A rotation that left the OLD expiry would make the refresh
        // gate read a stale deadline for a brand-new token.
        assert_eq!(stored.access_token_expires_at, Some(4_600));
    }

    #[tokio::test]
    async fn concurrent_401s_for_one_anthropic_account_refresh_exactly_once() {
        let dir = tempfile::tempdir().unwrap();
        let mock = MockAnthropicOAuth::ok("new-access", "new-refresh");
        let (_store, _cipher, auth) = fixture(&dir, mock.clone()).await;

        let first_plane = auth.clone();
        let second_plane = auth.clone();
        let account = AccountId::from("acct-anthropic");
        let (first_account, second_account) = (account.clone(), account.clone());
        let (first, second) = tokio::join!(
            first_plane.refresh_after_unauthorized(&first_account, "old-access", 1_000),
            second_plane.refresh_after_unauthorized(&second_account, "old-access", 1_000),
        );

        for result in [first, second] {
            assert_eq!(
                result
                    .unwrap()
                    .expect("both callers get a bearer")
                    .bearer_token,
                "new-access"
            );
        }
        // The waiter adopts the rotated token instead of refreshing again — a second refresh would
        // invalidate the first one's freshly issued credentials on a rotating server.
        assert_eq!(mock.hit_count(), 1);
    }

    #[tokio::test]
    async fn a_terminal_grant_error_marks_the_account_for_reauth() {
        let dir = tempfile::tempdir().unwrap();
        let (store, _cipher, auth) =
            fixture(&dir, MockAnthropicOAuth::error(400, "invalid_grant")).await;

        let result = auth
            .refresh_after_unauthorized(&AccountId::from("acct-anthropic"), "old-access", 1_000)
            .await;
        assert!(matches!(result, Err(ReactiveAuthError::AccountUnavailable)));
        assert_eq!(
            store
                .accounts()
                .get("acct-anthropic")
                .await
                .unwrap()
                .unwrap()
                .status,
            "reauth_required"
        );
    }

    #[tokio::test]
    async fn a_transient_failure_leaves_the_stored_credentials_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let mock = MockAnthropicOAuth::from_response(AnthropicOAuthResponse::Error {
            status: 503,
            code: None,
        });
        let (store, cipher, auth) = fixture(&dir, mock).await;

        let result = auth
            .refresh_after_unauthorized(&AccountId::from("acct-anthropic"), "old-access", 1_000)
            .await;
        // `Ok(None)`: the original 401 stays visible; the account is NOT condemned.
        assert!(result.unwrap().is_none());

        let account = store
            .accounts()
            .get("acct-anthropic")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(account.status, "active", "a 5xx must not force a re-login");
        let tokens = store
            .accounts()
            .decrypt_tokens("acct-anthropic", &cipher)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(tokens.refresh_token, "old-refresh", "the grant is intact");
    }

    #[tokio::test]
    async fn a_static_bearer_account_is_never_refreshed() {
        let dir = tempfile::tempdir().unwrap();
        let mock = MockAnthropicOAuth::ok("must-not-be-used", "must-not-be-used");
        let (store, _cipher, auth) = fixture(&dir, mock.clone()).await;
        // Demote the account to a static API key, as a legacy Anthropic row would be.
        sqlx::query("UPDATE accounts SET auth_mode = 'static_bearer' WHERE id = 'acct-anthropic'")
            .execute(store.pool())
            .await
            .unwrap();

        let result = auth
            .refresh_after_unauthorized(&AccountId::from("acct-anthropic"), "old-access", 1_000)
            .await;

        assert!(matches!(result, Err(ReactiveAuthError::AccountUnavailable)));
        // An API key has no refresh token; asking any auth server to rotate one is meaningless.
        assert_eq!(mock.hit_count(), 0, "no token endpoint may be contacted");
    }
}
