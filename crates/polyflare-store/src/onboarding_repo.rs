//! Durable, single-use state for dashboard OAuth onboarding. PKCE verifiers are stored only as
//! encrypted blobs and are returned solely to the server after an atomic pending -> exchanging
//! claim.

use sqlx::sqlite::SqlitePool;

use crate::StoreError;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct OnboardingFlow {
    pub id: String,
    /// `codex` | `anthropic`. Stored in the `flow_provider` column (migration 0025 retired the
    /// original `provider` column, whose CHECK constraint pinned every flow to Codex) and read back
    /// under this name by the repository's SELECTs.
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
    /// The exact `redirect_uri` sent on the authorize request. The token endpoint compares it
    /// byte-for-byte on exchange, and an Anthropic loopback flow binds an OS-assigned port that is
    /// only known once the callback listener is bound — so it cannot be reconstructed later.
    /// `None` for Codex flows, which use a fixed registered redirect.
    pub redirect_uri: Option<String>,
    /// When this flow is a targeted re-authentication, the id of the account it must repair.
    /// Completion refuses a callback whose seat does not belong to this account, so signing into
    /// the wrong ChatGPT account fails loudly instead of silently updating a different row.
    /// `None` is an ordinary untargeted "Add account" flow.
    pub intended_account_id: Option<String>,
    /// `browser` (authorization_code + loopback redirect) | `device` (user enters a short code at
    /// the auth server's verification page and the SERVER polls for approval — the only method a
    /// REMOTE browser can complete, since the registered redirect is pinned to localhost:1455).
    pub method: String,
    /// Device flow only: the auth server's handle for the pending device authorization.
    pub device_auth_id: Option<String>,
    /// Device flow only: the short code the operator enters at the verification page.
    pub user_code: Option<String>,
    /// Device flow only: the auth server's requested poll interval (seconds).
    pub interval_seconds: Option<i64>,
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
            "INSERT INTO account_onboarding_flows (id, flow_provider, oauth_state, verifier_enc, \
             initial_pool, status, created_at, expires_at, redirect_uri, intended_account_id, \
             method, device_auth_id, user_code, interval_seconds) \
             VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&flow.id)
        .bind(&flow.provider)
        .bind(&flow.oauth_state)
        .bind(&flow.verifier_enc)
        .bind(flow.initial_pool.as_deref())
        .bind(flow.created_at)
        .bind(flow.expires_at)
        .bind(flow.redirect_uri.as_deref())
        .bind(flow.intended_account_id.as_deref())
        .bind(&flow.method)
        .bind(flow.device_auth_id.as_deref())
        .bind(flow.user_code.as_deref())
        .bind(flow.interval_seconds)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    const SELECT_COLUMNS: &'static str =
        "SELECT id, flow_provider AS provider, oauth_state, verifier_enc, initial_pool, status, \
         created_at, expires_at, finished_at, account_id, error_code, redirect_uri, \
         intended_account_id, method, device_auth_id, user_code, interval_seconds \
         FROM account_onboarding_flows";

    pub async fn get(&self, id: &str) -> Result<Option<OnboardingFlow>, StoreError> {
        Ok(
            sqlx::query_as::<_, OnboardingFlow>(&format!("{} WHERE id = ?", Self::SELECT_COLUMNS))
                .bind(id)
                .fetch_optional(&self.pool)
                .await?,
        )
    }

    /// Look up a PENDING, unexpired flow by its `oauth_state` — how the transient loopback
    /// listener maps an incoming `/auth/callback?state=...` redirect back to its flow. The state
    /// column is UNIQUE, and restricting to pending/unexpired keeps a replayed redirect from
    /// resurrecting a finished flow.
    pub async fn get_pending_by_state(
        &self,
        oauth_state: &str,
        now: i64,
    ) -> Result<Option<OnboardingFlow>, StoreError> {
        Ok(sqlx::query_as::<_, OnboardingFlow>(&format!(
            "{} WHERE oauth_state = ? AND status = 'pending' AND expires_at > ?",
            Self::SELECT_COLUMNS
        ))
        .bind(oauth_state)
        .bind(now)
        .fetch_optional(&self.pool)
        .await?)
    }

    /// Whether any pending, unexpired BROWSER-method Codex flow exists — the loopback listener's
    /// stay-alive condition.
    pub async fn has_pending_browser_codex_flow(&self, now: i64) -> Result<bool, StoreError> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM account_onboarding_flows \
             WHERE flow_provider = 'codex' AND method = 'browser' AND status = 'pending' \
             AND expires_at > ?",
        )
        .bind(now)
        .fetch_one(&self.pool)
        .await?
            > 0)
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
            sqlx::query_as::<_, OnboardingFlow>(&format!("{} WHERE id = ?", Self::SELECT_COLUMNS))
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
            usage_cap_percent: None,
            usage_cap_override: false,
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
            redirect_uri: None,
            intended_account_id: None,
            method: "browser".into(),
            device_auth_id: None,
            user_code: None,
            interval_seconds: None,
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
            redirect_uri: None,
            intended_account_id: None,
            method: "browser".into(),
            device_auth_id: None,
            user_code: None,
            interval_seconds: None,
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

    /// A ChatGPT TEAM workspace shares one `chatgpt_account_id` across every member. Matching on
    /// it alone made the SECOND member's login land on the FIRST member's row — tokens replaced,
    /// identity still showing the previous member, and no new account anywhere (2026-09-15).
    /// Each member must get its own seat.
    #[tokio::test]
    async fn two_members_of_one_team_workspace_get_their_own_seats() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let cipher = TokenCipher::from_key_bytes(&[5u8; 32]).unwrap();

        let mut first = account("codex_team-ws");
        first.chatgpt_account_id = Some("team-ws".into());
        first.chatgpt_user_id = Some("user-AAAA1111zzzz".into());
        first.email = "first@team.test".into();
        let first_id = complete_login(&store, &cipher, "flow-a", &first).await;

        let mut second = account("codex_team-ws");
        second.chatgpt_account_id = Some("team-ws".into());
        second.chatgpt_user_id = Some("user-BBBB2222zzzz".into());
        second.email = "second@team.test".into();
        let second_id = complete_login(&store, &cipher, "flow-b", &second).await;

        assert_eq!(first_id, "codex_team-ws");
        assert_eq!(
            second_id, "codex_team-ws_BBBB2222",
            "the newcomer is scoped to its user instead of taking the first member's row"
        );
        let kept = store.accounts().get(&first_id).await.unwrap().unwrap();
        assert_eq!(
            kept.email, "first@team.test",
            "the first member's seat is untouched"
        );
        assert_eq!(kept.chatgpt_user_id.as_deref(), Some("user-AAAA1111zzzz"));
        let added = store.accounts().get(&second_id).await.unwrap().unwrap();
        assert_eq!(added.email, "second@team.test");
        assert_eq!(added.chatgpt_user_id.as_deref(), Some("user-BBBB2222zzzz"));
    }

    /// The same member logging in again still updates ONE row, and the row takes on the identity
    /// the login just proved rather than keeping a stale email/plan.
    #[tokio::test]
    async fn the_same_seat_re_logging_in_updates_in_place_and_refreshes_its_identity() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let cipher = TokenCipher::from_key_bytes(&[5u8; 32]).unwrap();

        let mut seat = account("codex_solo");
        seat.chatgpt_account_id = Some("solo".into());
        seat.chatgpt_user_id = Some("user-SOLO0001".into());
        seat.email = "old@solo.test".into();
        seat.plan_type = "pro".into();
        let id = complete_login(&store, &cipher, "flow-a", &seat).await;

        let mut again = seat.clone();
        again.email = "new@solo.test".into();
        again.plan_type = "unknown".into(); // a login that cannot see the plan must not erase it
        let same = complete_login(&store, &cipher, "flow-b", &again).await;

        assert_eq!(same, id, "one seat, one row");
        let row = store.accounts().get(&id).await.unwrap().unwrap();
        assert_eq!(row.email, "new@solo.test", "identity follows the login");
        assert_eq!(
            row.plan_type, "pro",
            "an unknown plan never erases a known one"
        );
    }

    /// Rows that predate user-id capture are still repairable by a re-login, but only by the seat
    /// that owns them: a different member of the same workspace gets its own row.
    #[tokio::test]
    async fn a_legacy_row_without_a_user_id_is_repaired_only_by_its_own_seat() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let cipher = TokenCipher::from_key_bytes(&[5u8; 32]).unwrap();

        let mut legacy = account("legacy-row");
        legacy.chatgpt_account_id = Some("team-ws".into());
        legacy.chatgpt_user_id = None;
        legacy.email = "owner@team.test".into();
        store
            .accounts()
            .insert(&legacy, &plain_tokens(), &cipher)
            .await
            .unwrap();

        let mut stranger = account("codex_team-ws");
        stranger.chatgpt_account_id = Some("team-ws".into());
        stranger.chatgpt_user_id = Some("user-STRANGER".into());
        stranger.email = "stranger@team.test".into();
        let stranger_id = complete_login(&store, &cipher, "flow-a", &stranger).await;
        assert_ne!(
            stranger_id, "legacy-row",
            "a different member never adopts it"
        );
        let untouched = store.accounts().get("legacy-row").await.unwrap().unwrap();
        assert_eq!(untouched.email, "owner@team.test");
        assert!(untouched.chatgpt_user_id.is_none());

        let mut owner = account("codex_team-ws");
        owner.chatgpt_account_id = Some("team-ws".into());
        owner.chatgpt_user_id = Some("user-OWNER01".into());
        owner.email = "Owner@Team.Test".into(); // same seat, different case
        let owner_id = complete_login(&store, &cipher, "flow-b", &owner).await;
        assert_eq!(
            owner_id, "legacy-row",
            "its own seat repairs the row instead of duplicating it"
        );
        let repaired = store.accounts().get("legacy-row").await.unwrap().unwrap();
        assert_eq!(
            repaired.chatgpt_user_id.as_deref(),
            Some("user-OWNER01"),
            "the missing user id is backfilled by the repair"
        );
    }

    fn plain_tokens() -> PlainTokens {
        PlainTokens {
            access_token: "a".into(),
            refresh_token: "r".into(),
            id_token: "i".into(),
        }
    }

    /// Run one onboarding flow to completion for `candidate`, returning the account id it wrote.
    async fn complete_login(
        store: &Store,
        cipher: &TokenCipher,
        flow_id: &str,
        candidate: &Account,
    ) -> String {
        let flow = OnboardingFlow {
            id: flow_id.into(),
            provider: "codex".into(),
            oauth_state: format!("state-{flow_id}"),
            verifier_enc: cipher.encrypt("verifier").unwrap(),
            initial_pool: None,
            status: "pending".into(),
            created_at: 1,
            expires_at: 1_000_000,
            finished_at: None,
            account_id: None,
            error_code: None,
            redirect_uri: None,
            intended_account_id: None,
            method: "browser".into(),
            device_auth_id: None,
            user_code: None,
            interval_seconds: None,
        };
        store.onboarding().create(&flow).await.unwrap();
        store.onboarding().claim(flow_id, 2).await.unwrap().unwrap();
        store
            .accounts()
            .upsert_oauth_and_complete_flow(candidate, &plain_tokens(), cipher, flow_id, None)
            .await
            .unwrap()
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
            redirect_uri: None,
            intended_account_id: None,
            method: "browser".into(),
            device_auth_id: None,
            user_code: None,
            interval_seconds: None,
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
                None,
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
