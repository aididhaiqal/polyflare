//! Account model + repository. Durable metadata lives in `Account`; the three OAuth tokens are
//! stored ONLY as XChaCha20-Poly1305 ciphertext and decrypted on demand.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use sqlx::sqlite::SqlitePool;

use crate::crypto::TokenCipher;
use crate::StoreError;

/// Durable, non-secret account columns. The three token columns are intentionally absent —
/// they never leave the store as plaintext except through [`AccountRepo::decrypt_tokens`].
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Account {
    pub id: String,
    pub chatgpt_account_id: Option<String>,
    pub chatgpt_user_id: Option<String>,
    pub email: String,
    pub alias: Option<String>,
    pub workspace_id: Option<String>,
    pub workspace_label: Option<String>,
    pub seat_type: Option<String>,
    pub plan_type: String,
    pub routing_policy: String,
    pub last_refresh: i64,
    pub created_at: i64,
    pub status: String,
    pub deactivation_reason: Option<String>,
    pub reset_at: Option<i64>,
    pub blocked_at: Option<i64>,
    pub security_work_authorized: bool,
    /// 'codex' | 'anthropic' — which backend pool this account belongs to.
    pub provider: String,
    /// Named account pool slug, or `None` (unpooled). Unpooled accounts are reachable only via the
    /// bare ingress paths; a non-null slug also makes the account reachable via `/{pool}/...`.
    pub pool: Option<String>,
}

/// The three OAuth tokens in plaintext. Used as insert/update input and as decrypt output.
/// Never logged: its `Debug` redacts every field.
#[derive(Clone)]
pub struct PlainTokens {
    pub access_token: String,
    pub refresh_token: String,
    pub id_token: String,
}

impl std::fmt::Debug for PlainTokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlainTokens")
            .field("access_token", &"***")
            .field("refresh_token", &"***")
            .field("id_token", &"***")
            .finish()
    }
}

/// The three token columns as stored: XChaCha20-Poly1305 ciphertext (24-byte nonce prepended).
/// This is the "encrypted token record" the importer produces and the repository persists.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct EncryptedTokens {
    pub access_token_enc: Vec<u8>,
    pub refresh_token_enc: Vec<u8>,
    pub id_token_enc: Vec<u8>,
}

impl EncryptedTokens {
    /// Encrypt a [`PlainTokens`] triple under `cipher`.
    pub fn encrypt(tokens: &PlainTokens, cipher: &TokenCipher) -> Result<Self, StoreError> {
        Ok(Self {
            access_token_enc: cipher.encrypt(&tokens.access_token)?,
            refresh_token_enc: cipher.encrypt(&tokens.refresh_token)?,
            id_token_enc: cipher.encrypt(&tokens.id_token)?,
        })
    }
}

/// The latest usage percentage + reset for one window of an account. `window_minutes` is the
/// window's DURATION (so a consumer can tell a 5h window from a weekly one regardless of which
/// slot it was stored in), and `recorded_at` is when this row was written (so a consumer can tell
/// live data from a window upstream stopped refreshing).
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct WindowUsage {
    pub used_percent: f64,
    pub reset_at: Option<i64>,
    pub window_minutes: Option<i64>,
    pub recorded_at: i64,
}

/// The latest usage per window ("primary"/"secondary") for an account. Missing windows are
/// `None` (the snapshot assembler treats them as zero usage).
#[derive(Debug, Clone, Default)]
pub struct UsageSnapshot {
    pub primary: Option<WindowUsage>,
    pub secondary: Option<WindowUsage>,
}

/// Full column list for `SELECT`ing an `Account` (must match the `FromRow` field order/names).
const SELECT_ACCOUNT_BY_ID: &str = "SELECT id, chatgpt_account_id, chatgpt_user_id, email, \
    alias, workspace_id, workspace_label, seat_type, plan_type, routing_policy, last_refresh, \
    created_at, status, deactivation_reason, reset_at, blocked_at, security_work_authorized, \
    provider, pool FROM accounts WHERE id = ?";

const SELECT_ALL_ACCOUNTS: &str = "SELECT id, chatgpt_account_id, chatgpt_user_id, email, \
    alias, workspace_id, workspace_label, seat_type, plan_type, routing_policy, last_refresh, \
    created_at, status, deactivation_reason, reset_at, blocked_at, security_work_authorized, \
    provider, pool FROM accounts ORDER BY id";

const SELECT_ACCOUNT_BY_CHATGPT_ID: &str =
    "SELECT id, chatgpt_account_id, chatgpt_user_id, email, \
    alias, workspace_id, workspace_label, seat_type, plan_type, routing_policy, last_refresh, \
    created_at, status, deactivation_reason, reset_at, blocked_at, security_work_authorized, \
    provider, pool FROM accounts WHERE chatgpt_account_id = ?";

/// CRUD over the `accounts` table. Cheap to construct (clones the pool handle + generation Arc).
pub struct AccountRepo {
    pool: SqlitePool,
    /// Bumped on every write so the server's `AccountCache` auto-invalidates (see `Store`).
    generation: Arc<AtomicU64>,
}

impl AccountRepo {
    pub fn new(pool: SqlitePool, generation: Arc<AtomicU64>) -> Self {
        Self { pool, generation }
    }

    /// Advance the account-write generation. Called after every successful mutation so a cached
    /// account pool is invalidated by the WRITE itself, not by each caller remembering to.
    fn bump_generation(&self) {
        self.generation.fetch_add(1, Ordering::Release);
    }

    /// Insert an account, encrypting its tokens on the way in.
    pub async fn insert(
        &self,
        account: &Account,
        tokens: &PlainTokens,
        cipher: &TokenCipher,
    ) -> Result<(), StoreError> {
        let enc = EncryptedTokens::encrypt(tokens, cipher)?;
        self.insert_encrypted(account, &enc).await
    }

    /// Insert an account whose tokens are already XChaCha-encrypted (used by the importer).
    pub async fn insert_encrypted(
        &self,
        account: &Account,
        enc: &EncryptedTokens,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO accounts (\
                id, chatgpt_account_id, chatgpt_user_id, email, alias, \
                workspace_id, workspace_label, seat_type, plan_type, routing_policy, \
                access_token_enc, refresh_token_enc, id_token_enc, \
                last_refresh, created_at, status, deactivation_reason, \
                reset_at, blocked_at, security_work_authorized, provider, pool\
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(account.id.as_str())
        .bind(account.chatgpt_account_id.as_deref())
        .bind(account.chatgpt_user_id.as_deref())
        .bind(account.email.as_str())
        .bind(account.alias.as_deref())
        .bind(account.workspace_id.as_deref())
        .bind(account.workspace_label.as_deref())
        .bind(account.seat_type.as_deref())
        .bind(account.plan_type.as_str())
        .bind(account.routing_policy.as_str())
        .bind(enc.access_token_enc.as_slice())
        .bind(enc.refresh_token_enc.as_slice())
        .bind(enc.id_token_enc.as_slice())
        .bind(account.last_refresh)
        .bind(account.created_at)
        .bind(account.status.as_str())
        .bind(account.deactivation_reason.as_deref())
        .bind(account.reset_at)
        .bind(account.blocked_at)
        .bind(account.security_work_authorized)
        .bind(account.provider.as_str())
        .bind(account.pool.as_deref())
        .execute(&self.pool)
        .await?;
        self.bump_generation();
        Ok(())
    }

    /// Fetch one account's metadata by id.
    pub async fn get(&self, id: &str) -> Result<Option<Account>, StoreError> {
        let account = sqlx::query_as::<_, Account>(SELECT_ACCOUNT_BY_ID)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(account)
    }

    /// List all accounts' metadata, ordered by id.
    pub async fn list(&self) -> Result<Vec<Account>, StoreError> {
        let accounts = sqlx::query_as::<_, Account>(SELECT_ALL_ACCOUNTS)
            .fetch_all(&self.pool)
            .await?;
        Ok(accounts)
    }

    /// Find an account by its ChatGPT account id — used by `polyflare login` to decide onboard
    /// (insert) vs re-auth (update the existing seat's tokens) instead of creating a duplicate row.
    pub async fn find_by_chatgpt_account_id(
        &self,
        chatgpt_account_id: &str,
    ) -> Result<Option<Account>, StoreError> {
        let account = sqlx::query_as::<_, Account>(SELECT_ACCOUNT_BY_CHATGPT_ID)
            .bind(chatgpt_account_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(account)
    }

    /// Update an account's status string.
    pub async fn update_status(&self, id: &str, status: &str) -> Result<(), StoreError> {
        sqlx::query("UPDATE accounts SET status = ? WHERE id = ?")
            .bind(status)
            .bind(id)
            .execute(&self.pool)
            .await?;
        self.bump_generation();
        Ok(())
    }

    /// Assign (`Some(slug)`) or clear (`None`) an account's pool. Bumps the store generation so the
    /// server's account cache re-reads on the next selection.
    pub async fn update_pool(&self, id: &str, pool: Option<&str>) -> Result<(), StoreError> {
        sqlx::query("UPDATE accounts SET pool = ? WHERE id = ?")
            .bind(pool)
            .bind(id)
            .execute(&self.pool)
            .await?;
        self.bump_generation();
        Ok(())
    }

    /// Set an account's routing policy (`normal` | `burn_first` | `preserve`). Bumps the generation.
    pub async fn update_routing_policy(
        &self,
        id: &str,
        routing_policy: &str,
    ) -> Result<(), StoreError> {
        sqlx::query("UPDATE accounts SET routing_policy = ? WHERE id = ?")
            .bind(routing_policy)
            .bind(id)
            .execute(&self.pool)
            .await?;
        self.bump_generation();
        Ok(())
    }

    /// Update an account's status AND its `reset_at` routing gate together — the usage-refresh
    /// quota mapping writes both (e.g. `quota_exceeded` + the weekly window's reset time). Bumps
    /// the generation so the account cache re-reads fresh usage.
    pub async fn update_status_and_reset(
        &self,
        id: &str,
        status: &str,
        reset_at: Option<i64>,
    ) -> Result<(), StoreError> {
        sqlx::query("UPDATE accounts SET status = ?, reset_at = ? WHERE id = ?")
            .bind(status)
            .bind(reset_at)
            .bind(id)
            .execute(&self.pool)
            .await?;
        self.bump_generation();
        Ok(())
    }

    /// Insert one `usage_history` window row (from a runtime usage refresh). `window` is
    /// `"primary"` (5h) or `"secondary"` (weekly). Append-only, exactly the shape the codex-lb
    /// importer writes, so `latest_usage` reads it back unchanged.
    pub async fn insert_usage_window(
        &self,
        account_id: &str,
        window: &str,
        used_percent: f64,
        reset_at: Option<i64>,
        window_minutes: Option<i64>,
        recorded_at: i64,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO usage_history (account_id, recorded_at, \"window\", used_percent, \
             reset_at, window_minutes) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(account_id)
        .bind(recorded_at)
        .bind(window)
        .bind(used_percent)
        .bind(reset_at)
        .bind(window_minutes)
        .execute(&self.pool)
        .await?;
        self.bump_generation();
        Ok(())
    }

    /// Replace an account's tokens (re-encrypting) and stamp `last_refresh`.
    pub async fn update_tokens(
        &self,
        id: &str,
        tokens: &PlainTokens,
        cipher: &TokenCipher,
        last_refresh: i64,
    ) -> Result<(), StoreError> {
        let enc = EncryptedTokens::encrypt(tokens, cipher)?;
        sqlx::query(
            "UPDATE accounts SET access_token_enc = ?, refresh_token_enc = ?, \
             id_token_enc = ?, last_refresh = ? WHERE id = ?",
        )
        .bind(enc.access_token_enc.as_slice())
        .bind(enc.refresh_token_enc.as_slice())
        .bind(enc.id_token_enc.as_slice())
        .bind(last_refresh)
        .bind(id)
        .execute(&self.pool)
        .await?;
        self.bump_generation();
        Ok(())
    }

    /// Decrypt and return an account's three tokens, or `None` if the account is absent.
    pub async fn decrypt_tokens(
        &self,
        id: &str,
        cipher: &TokenCipher,
    ) -> Result<Option<PlainTokens>, StoreError> {
        let enc = sqlx::query_as::<_, EncryptedTokens>(
            "SELECT access_token_enc, refresh_token_enc, id_token_enc FROM accounts WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        match enc {
            Some(enc) => Ok(Some(PlainTokens {
                access_token: cipher.decrypt(&enc.access_token_enc)?,
                refresh_token: cipher.decrypt(&enc.refresh_token_enc)?,
                id_token: cipher.decrypt(&enc.id_token_enc)?,
            })),
            None => Ok(None),
        }
    }

    /// The most-recent `usage_history` row for each window ("primary"/"secondary") of an account.
    pub async fn latest_usage(&self, account_id: &str) -> Result<UsageSnapshot, StoreError> {
        Ok(UsageSnapshot {
            primary: self.latest_window_usage(account_id, "primary").await?,
            secondary: self.latest_window_usage(account_id, "secondary").await?,
        })
    }

    /// The most-recent usage row for a single window, or `None` if the account has none.
    async fn latest_window_usage(
        &self,
        account_id: &str,
        window: &str,
    ) -> Result<Option<WindowUsage>, StoreError> {
        let row = sqlx::query_as::<_, WindowUsage>(
            "SELECT used_percent, reset_at, window_minutes, recorded_at FROM usage_history \
             WHERE account_id = ? AND \"window\" = ? ORDER BY recorded_at DESC LIMIT 1",
        )
        .bind(account_id)
        .bind(window)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_tokens_debug_redacts_secret_values() {
        let tokens = PlainTokens {
            access_token: "super-secret-access-xyz".to_string(),
            refresh_token: "super-secret-refresh-xyz".to_string(),
            id_token: "super-secret-id-xyz".to_string(),
        };
        let s = format!("{tokens:?}");
        assert!(
            !s.contains("super-secret-access-xyz"),
            "Debug must not leak the access token"
        );
        assert!(
            !s.contains("super-secret-refresh-xyz"),
            "Debug must not leak the refresh token"
        );
        assert!(
            !s.contains("super-secret-id-xyz"),
            "Debug must not leak the id token"
        );
        assert!(s.contains("***"), "Debug must redact with `***`");
    }
}
