//! Durable, single-use state for dashboard OAuth onboarding. PKCE verifiers are stored only as
//! encrypted blobs and are returned solely to the server after an atomic pending -> exchanging
//! claim.

use sqlx::sqlite::SqlitePool;

use crate::StoreError;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct OnboardingFlow {
    pub id: String,
    pub provider: String,
    pub oauth_state: String,
    pub verifier_enc: Vec<u8>,
    pub initial_pool: Option<String>,
    pub status: String,
    pub created_at: i64,
    pub expires_at: i64,
    pub finished_at: Option<i64>,
    pub account_id: Option<String>,
    pub error_code: Option<String>,
}

#[derive(Clone)]
pub struct OnboardingRepo {
    pool: SqlitePool,
}

impl OnboardingRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn create(&self, flow: &OnboardingFlow) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO account_onboarding_flows (id, provider, oauth_state, verifier_enc, \
             initial_pool, status, created_at, expires_at) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)",
        )
        .bind(&flow.id)
        .bind(&flow.provider)
        .bind(&flow.oauth_state)
        .bind(&flow.verifier_enc)
        .bind(flow.initial_pool.as_deref())
        .bind(flow.created_at)
        .bind(flow.expires_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get(&self, id: &str) -> Result<Option<OnboardingFlow>, StoreError> {
        Ok(sqlx::query_as::<_, OnboardingFlow>(
            "SELECT id, provider, oauth_state, verifier_enc, initial_pool, status, created_at, \
             expires_at, finished_at, account_id, error_code \
             FROM account_onboarding_flows WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?)
    }

    /// Atomically consumes a pending, unexpired flow. A callback can claim a flow only once,
    /// including when the subsequent upstream exchange fails.
    pub async fn claim(&self, id: &str, now: i64) -> Result<Option<OnboardingFlow>, StoreError> {
        let mut tx = self.pool.begin().await?;
        let changed = sqlx::query(
            "UPDATE account_onboarding_flows SET status = 'exchanging' \
             WHERE id = ? AND status = 'pending' AND expires_at > ?",
        )
        .bind(id)
        .bind(now)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        let flow = if changed == 1 {
            sqlx::query_as::<_, OnboardingFlow>(
                "SELECT id, provider, oauth_state, verifier_enc, initial_pool, status, created_at, \
                 expires_at, finished_at, account_id, error_code \
                 FROM account_onboarding_flows WHERE id = ?",
            )
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?
        } else {
            None
        };
        tx.commit().await?;
        Ok(flow)
    }

    pub async fn complete(&self, id: &str, account_id: &str, now: i64) -> Result<(), StoreError> {
        sqlx::query(
            "UPDATE account_onboarding_flows SET status = 'completed', account_id = ?, \
             finished_at = ?, error_code = NULL, verifier_enc = X'' \
             WHERE id = ? AND status = 'exchanging'",
        )
        .bind(account_id)
        .bind(now)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn fail(&self, id: &str, error_code: &str, now: i64) -> Result<(), StoreError> {
        sqlx::query(
            "UPDATE account_onboarding_flows SET status = 'failed', error_code = ?, \
             finished_at = ?, verifier_enc = X'' WHERE id = ? AND status = 'exchanging'",
        )
        .bind(error_code)
        .bind(now)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Scrub expired pending verifiers and prune old terminal rows. Called opportunistically by
    /// onboarding API traffic so an abandoned browser flow retains no verifier indefinitely.
    pub async fn expire_and_prune(&self, now: i64) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "UPDATE account_onboarding_flows SET status = 'failed', error_code = 'flow_expired', \
             finished_at = ?, verifier_enc = X'' \
             WHERE status = 'pending' AND expires_at <= ?",
        )
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "DELETE FROM account_onboarding_flows WHERE status IN ('completed', 'failed') \
             AND finished_at < ?",
        )
        .bind(now - 86_400)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{Account, OnboardingFlow, PlainTokens, Store, TokenCipher};

    fn account(id: &str) -> Account {
        Account {
            id: id.into(),
            chatgpt_account_id: Some(id.into()),
            chatgpt_user_id: None,
            email: "user@example.test".into(),
            alias: None,
            workspace_id: None,
            workspace_label: None,
            seat_type: None,
            plan_type: "pro".into(),
            routing_policy: "normal".into(),
            last_refresh: 20,
            created_at: 20,
            status: "active".into(),
            deactivation_reason: None,
            reset_at: None,
            blocked_at: None,
            security_work_authorized: false,
            provider: "codex".into(),
            pool: None,
        }
    }

    #[tokio::test]
    async fn verifier_is_encrypted_and_flow_is_claimed_once() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let cipher = TokenCipher::from_key_bytes(&[5u8; 32]).unwrap();
        let verifier = "plain-verifier-marker";
        let flow = OnboardingFlow {
            id: "flow-1".into(),
            provider: "codex".into(),
            oauth_state: "state-1".into(),
            verifier_enc: cipher.encrypt(verifier).unwrap(),
            initial_pool: Some("team-a".into()),
            status: "pending".into(),
            created_at: 10,
            expires_at: 100,
            finished_at: None,
            account_id: None,
            error_code: None,
        };
        store.onboarding().create(&flow).await.unwrap();
        let raw: Vec<u8> = sqlx::query_scalar(
            "SELECT verifier_enc FROM account_onboarding_flows WHERE id = 'flow-1'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert!(!raw
            .windows(verifier.len())
            .any(|w| w == verifier.as_bytes()));
        let claimed = store
            .onboarding()
            .claim("flow-1", 20)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cipher.decrypt(&claimed.verifier_enc).unwrap(), verifier);
        assert!(store
            .onboarding()
            .claim("flow-1", 20)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn expired_flow_cannot_be_claimed() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let flow = OnboardingFlow {
            id: "flow-2".into(),
            provider: "codex".into(),
            oauth_state: "state-2".into(),
            verifier_enc: vec![1],
            initial_pool: None,
            status: "pending".into(),
            created_at: 1,
            expires_at: 2,
            finished_at: None,
            account_id: None,
            error_code: None,
        };
        store.onboarding().create(&flow).await.unwrap();
        assert!(store
            .onboarding()
            .claim("flow-2", 2)
            .await
            .unwrap()
            .is_none());
        store.onboarding().expire_and_prune(2).await.unwrap();
        let expired = store.onboarding().get("flow-2").await.unwrap().unwrap();
        assert_eq!(expired.error_code.as_deref(), Some("flow_expired"));
        assert!(expired.verifier_enc.is_empty());
    }

    #[tokio::test]
    async fn account_write_rolls_back_when_flow_cannot_complete() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let cipher = TokenCipher::from_key_bytes(&[5u8; 32]).unwrap();
        let flow = OnboardingFlow {
            id: "flow-pending".into(),
            provider: "codex".into(),
            oauth_state: "state".into(),
            verifier_enc: cipher.encrypt("verifier").unwrap(),
            initial_pool: None,
            status: "pending".into(),
            created_at: 1,
            expires_at: 100,
            finished_at: None,
            account_id: None,
            error_code: None,
        };
        store.onboarding().create(&flow).await.unwrap();
        let result = store
            .accounts()
            .upsert_oauth_and_complete_flow(
                &account("chatgpt-atomic"),
                &PlainTokens {
                    access_token: "a".into(),
                    refresh_token: "r".into(),
                    id_token: "i".into(),
                },
                &cipher,
                "flow-pending",
            )
            .await;
        assert!(result.is_err());
        assert!(store
            .accounts()
            .get("chatgpt-atomic")
            .await
            .unwrap()
            .is_none());
    }
}
