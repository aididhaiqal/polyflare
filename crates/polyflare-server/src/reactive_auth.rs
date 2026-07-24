//! Shared same-account OAuth recovery for upstream `401` responses.
//!
//! Responses, control endpoints, WebSocket handshakes/redials, and model discovery must all
//! serialize refresh-token rotation through the same per-account lock. Keeping the mechanism here
//! prevents a control-plane catalog request from racing a data-plane request with the same refresh
//! token.

use std::time::Duration;

use polyflare_codex::oauth::{classify_failure, is_fedramp_account, OAuthClient, OAuthError};
use polyflare_core::{Account, AccountId};
use polyflare_store::{PlainTokens, Store, TokenCipher};

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
    ) -> Self {
        Self {
            store,
            cipher,
            oauth,
            refresh_locks,
            codex_base_url,
        }
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
        let (stored_account, stored_tokens) = repo
            .get_with_tokens(picked.as_str(), &self.cipher)
            .await
            .map_err(|_| ReactiveAuthError::Internal)?
            .ok_or(ReactiveAuthError::Internal)?;
        if stored_account.status != "active" {
            return Err(ReactiveAuthError::AccountUnavailable);
        }

        if stored_tokens.access_token != rejected_access_token {
            return Ok(Some(Account {
                id: stored_account.id,
                base_url: self.codex_base_url.clone(),
                bearer_token: stored_tokens.access_token.clone(),
                chatgpt_account_id: stored_account.chatgpt_account_id,
                is_fedramp: is_fedramp_account(&stored_tokens.id_token),
            }));
        }

        let refreshed = match self.oauth.refresh(&stored_tokens.refresh_token).await {
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
            return Err(ReactiveAuthError::Internal);
        }

        Ok(Some(Account {
            id: stored_account.id,
            base_url: self.codex_base_url.clone(),
            bearer_token: new.access_token.clone(),
            chatgpt_account_id: stored_account.chatgpt_account_id,
            is_fedramp: is_fedramp_account(&new.id_token),
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
        );

        let result = auth
            .refresh_after_unauthorized(&AccountId::from("acct-persist-fails"), "old-access", 10)
            .await;

        assert!(matches!(result, Err(ReactiveAuthError::Internal)));
        let (_, stored) = store
            .accounts()
            .get_with_tokens("acct-persist-fails", &cipher)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.access_token, "old-access");
        assert_eq!(stored.refresh_token, "old-refresh");
    }
}
