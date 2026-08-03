//! Repository over the singleton `admin_token` table — the shared operator token for the dashboard
//! management API, stored so it can be configured without an environment variable.
//!
//! Store-layer only: this crate never sees the plaintext token. Callers hand in a `token_hash`
//! (sha256 hex, computed by the server crate's `admin_token` module) and this repo persists and
//! returns it for comparison. The plaintext is generated, shown to the operator exactly once, and
//! never written anywhere — the same reveal-once discipline `api_keys` follows.

use sqlx::sqlite::SqlitePool;

use crate::StoreError;

/// The installed admin token, as much of it as is safe to keep.
///
/// Carries `token_hash` because verification is a comparison against a single known value rather
/// than an indexed lookup over many rows — unlike [`crate::ApiKeyRow`], which can omit the hash
/// precisely because callers search *by* it. The hash is not secret (it cannot be reversed into a
/// 256-bit random token), but it is still the one field a caller must not log.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct AdminTokenRow {
    pub token_hash: String,
    /// First few characters of the plaintext — identifies WHICH token is installed, far too short
    /// to be usable as one.
    pub token_prefix: String,
    pub created_at: i64,
}

/// Read/replace/remove the single admin-token row. Cheap to construct (clones the pool handle).
pub struct AdminTokenRepo {
    pool: SqlitePool,
}

impl AdminTokenRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// The installed token, or `None` when none is stored.
    pub async fn get(&self) -> Result<Option<AdminTokenRow>, StoreError> {
        let row = sqlx::query_as::<_, AdminTokenRow>(
            "SELECT token_hash, token_prefix, created_at FROM admin_token WHERE id = 1",
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    /// Install a token, replacing any current one.
    ///
    /// Rotation is deliberately a replace rather than an insert: the previous token stops working
    /// the instant this returns, which is the entire point of rotating one. Keeping the old row
    /// around "for audit" would leave a second live credential behind.
    pub async fn set(
        &self,
        token_hash: &str,
        token_prefix: &str,
        now: i64,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO admin_token (id, token_hash, token_prefix, created_at) \
             VALUES (1, ?, ?, ?) \
             ON CONFLICT(id) DO UPDATE SET token_hash = excluded.token_hash, \
             token_prefix = excluded.token_prefix, created_at = excluded.created_at",
        )
        .bind(token_hash)
        .bind(token_prefix)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Remove the stored token. Returns whether there was one to remove, so a caller can tell an
    /// operator "cleared" apart from "there was nothing configured".
    pub async fn clear(&self) -> Result<bool, StoreError> {
        let result = sqlx::query("DELETE FROM admin_token WHERE id = 1")
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }
}

#[cfg(test)]
mod tests {
    use crate::store::Store;

    async fn store() -> Store {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(&dir.path().join("s.db")).await.unwrap();
        std::mem::forget(dir);
        s
    }

    #[tokio::test]
    async fn a_fresh_store_has_no_admin_token() {
        let s = store().await;
        assert!(s.admin_token().get().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn set_then_get_round_trips_the_hash_and_prefix() {
        let s = store().await;
        let repo = s.admin_token();
        repo.set("hash-a", "pfa_abc123", 100).await.unwrap();

        let row = repo.get().await.unwrap().expect("a token is installed");
        assert_eq!(row.token_hash, "hash-a");
        assert_eq!(row.token_prefix, "pfa_abc123");
        assert_eq!(row.created_at, 100);
    }

    /// Rotation must leave exactly ONE live token. A second row would mean the token an operator
    /// believes they revoked still authenticates.
    #[tokio::test]
    async fn setting_a_second_token_replaces_the_first_rather_than_adding_one() {
        let s = store().await;
        let repo = s.admin_token();
        repo.set("hash-a", "pfa_aaa", 100).await.unwrap();
        repo.set("hash-b", "pfa_bbb", 200).await.unwrap();

        let row = repo.get().await.unwrap().expect("a token is installed");
        assert_eq!(row.token_hash, "hash-b", "the new token is the live one");
        assert_eq!(row.created_at, 200);

        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM admin_token")
            .fetch_one(s.pool())
            .await
            .unwrap();
        assert_eq!(count.0, 1, "the old token must not survive as a second row");
    }

    #[tokio::test]
    async fn clear_removes_the_token_and_reports_whether_there_was_one() {
        let s = store().await;
        let repo = s.admin_token();
        repo.set("hash-a", "pfa_aaa", 100).await.unwrap();

        assert!(repo.clear().await.unwrap(), "a stored token was removed");
        assert!(repo.get().await.unwrap().is_none());
        assert!(
            !repo.clear().await.unwrap(),
            "clearing again reports there was nothing to clear"
        );
    }
}
