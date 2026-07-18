//! D18 Task 1: repository over the `api_keys` table. Store-layer only — this crate never sees a
//! plaintext client key. Callers hand in a `key_hash` (sha256 hex of the raw key, computed by
//! Task 2's key-gen helper / Task 3's middleware) and this repo persists/looks it up by hash. The
//! `ApiKeyRow` returned to callers carries no raw-key field and no way to reconstruct one — only
//! the hash (used for the indexed lookup) and a short display prefix ever exist in this table.

use sqlx::sqlite::SqlitePool;

use crate::StoreError;

/// One `api_keys` row. Deliberately carries `key_prefix` (safe to display) and NOT `key_hash` —
/// callers look keys up BY hash via [`ApiKeyRepo::get_by_hash`], so the row returned to a caller
/// (e.g. the middleware, the `keys list` CLI) has no reason to re-expose the hash, and every
/// consumer of this row is content-safe by construction: there is no field here from which a
/// caller could reconstruct or compare against a raw key.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ApiKeyRow {
    pub id: String,
    pub key_prefix: String,
    pub label: Option<String>,
    pub enabled: bool,
    pub created_at: i64,
    pub last_used_at: Option<i64>,
}

const SELECT_COLUMNS: &str = "id, key_prefix, label, enabled, created_at, last_used_at";

const SELECT_BY_HASH: &str =
    "SELECT id, key_prefix, label, enabled, created_at, last_used_at FROM api_keys WHERE key_hash = ?";

/// CRUD over the `api_keys` table. Cheap to construct (clones the pool handle).
///
/// No write-generation bump: unlike `AccountRepo`, this repo has no in-process cache to
/// invalidate. The middleware (Task 3) validates a presented key via [`Self::get_by_hash`] on
/// every request — a fresh indexed lookup, not a cached snapshot — so there is nothing here for a
/// generation counter to protect. If a future task adds an in-process key cache (a Task 3/4
/// concern per the plan), that cache would need its own invalidation signal at that point; Task 1
/// deliberately does not pre-build one.
pub struct ApiKeyRepo {
    pool: SqlitePool,
}

impl ApiKeyRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Insert a new key row. `key_hash` must be the sha256 hex of the plaintext key (computed by
    /// the caller — this repo never sees the plaintext). Fails (does not silently overwrite) if
    /// `key_hash` already exists, via the `UNIQUE` constraint on that column.
    pub async fn create(
        &self,
        id: &str,
        key_hash: &str,
        key_prefix: &str,
        label: Option<&str>,
        now: i64,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO api_keys (id, key_hash, key_prefix, label, enabled, created_at) \
             VALUES (?, ?, ?, ?, 1, ?)",
        )
        .bind(id)
        .bind(key_hash)
        .bind(key_prefix)
        .bind(label)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// The hot validation path: look up a row by its hash (indexed via `idx_api_keys_key_hash`).
    /// Returns the row regardless of `enabled` — enforcement of `enabled` is the middleware's job
    /// (Task 3), not this repo's; a caller that needs "is this key usable" must check
    /// `row.enabled` itself.
    pub async fn get_by_hash(&self, key_hash: &str) -> Result<Option<ApiKeyRow>, StoreError> {
        let row = sqlx::query_as::<_, ApiKeyRow>(SELECT_BY_HASH)
            .bind(key_hash)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row)
    }

    /// All keys, newest first. Never carries a raw key or the hash — display-safe by construction
    /// (see [`ApiKeyRow`]).
    pub async fn list(&self) -> Result<Vec<ApiKeyRow>, StoreError> {
        let rows = sqlx::query_as::<_, ApiKeyRow>(&format!(
            "SELECT {SELECT_COLUMNS} FROM api_keys ORDER BY created_at DESC"
        ))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Enable/disable a key. Revoke = `set_enabled(id, false)`. The repo does not filter on this
    /// flag itself (see [`Self::get_by_hash`]) — it only persists the flag for the middleware to
    /// enforce.
    pub async fn set_enabled(&self, id: &str, enabled: bool) -> Result<(), StoreError> {
        sqlx::query("UPDATE api_keys SET enabled = ? WHERE id = ?")
            .bind(enabled)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Stamp `last_used_at` on a successful validation. Called from the request hot path (Task
    /// 3) — a single indexed `UPDATE`, no read-modify-write.
    pub async fn touch_last_used(&self, id: &str, now: i64) -> Result<(), StoreError> {
        sqlx::query("UPDATE api_keys SET last_used_at = ? WHERE id = ?")
            .bind(now)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
#[allow(unused_imports)] // `super::*` types (ApiKeyRow, StoreError) are only used via inference
mod tests {
    use super::*;
    use crate::store::Store;

    async fn store() -> Store {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(&dir.path().join("s.db")).await.unwrap();
        std::mem::forget(dir);
        s
    }

    #[tokio::test]
    async fn create_then_get_by_hash_round_trips() {
        let s = store().await;
        let repo = s.api_keys();
        repo.create("id1", "hash1", "sk-pf-abc123456", Some("laptop"), 100)
            .await
            .unwrap();

        let row = repo.get_by_hash("hash1").await.unwrap().unwrap();
        assert_eq!(row.id, "id1");
        assert_eq!(row.key_prefix, "sk-pf-abc123456");
        assert_eq!(row.label.as_deref(), Some("laptop"));
        assert!(row.enabled);
        assert_eq!(row.created_at, 100);
        assert_eq!(row.last_used_at, None);
    }

    #[tokio::test]
    async fn get_by_hash_wrong_hash_is_none() {
        let s = store().await;
        let repo = s.api_keys();
        repo.create("id1", "hash1", "sk-pf-abc123456", None, 100)
            .await
            .unwrap();

        assert!(repo.get_by_hash("not-the-hash").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn set_enabled_false_still_returns_the_row_but_disabled() {
        let s = store().await;
        let repo = s.api_keys();
        repo.create("id1", "hash1", "sk-pf-abc123456", None, 100)
            .await
            .unwrap();

        repo.set_enabled("id1", false).await.unwrap();

        let row = repo.get_by_hash("hash1").await.unwrap().unwrap();
        assert_eq!(row.id, "id1", "repo still returns the row");
        assert!(
            !row.enabled,
            "enabled reflects the revoke; enforcement is the middleware's job, not the repo's"
        );
    }

    #[tokio::test]
    async fn list_contains_prefix_and_label_but_no_raw_key_field() {
        let s = store().await;
        let repo = s.api_keys();
        repo.create("id1", "hash1", "sk-pf-abc123456", Some("laptop"), 100)
            .await
            .unwrap();
        repo.create("id2", "hash2", "sk-pf-def987654", None, 200)
            .await
            .unwrap();

        let rows = repo.list().await.unwrap();
        assert_eq!(rows.len(), 2);
        let laptop = rows.iter().find(|r| r.id == "id1").unwrap();
        assert_eq!(laptop.key_prefix, "sk-pf-abc123456");
        assert_eq!(laptop.label.as_deref(), Some("laptop"));
        // Compile-time content-safety: `ApiKeyRow` has no field a raw key could live in beyond
        // the ones asserted above (id/key_prefix/label/enabled/created_at/last_used_at) — there
        // is no `.raw_key`/`.key`/`.key_hash` accessor to even attempt calling here.
    }

    #[tokio::test]
    async fn touch_last_used_updates_the_timestamp() {
        let s = store().await;
        let repo = s.api_keys();
        repo.create("id1", "hash1", "sk-pf-abc123456", None, 100)
            .await
            .unwrap();

        repo.touch_last_used("id1", 555).await.unwrap();

        let row = repo.get_by_hash("hash1").await.unwrap().unwrap();
        assert_eq!(row.last_used_at, Some(555));
    }

    #[tokio::test]
    async fn duplicate_key_hash_is_an_error_not_a_silent_overwrite() {
        let s = store().await;
        let repo = s.api_keys();
        repo.create("id1", "dup-hash", "sk-pf-abc123456", None, 100)
            .await
            .unwrap();

        let result = repo
            .create("id2", "dup-hash", "sk-pf-zzz999999", None, 200)
            .await;
        assert!(result.is_err(), "UNIQUE(key_hash) must reject the insert");

        // The original row is untouched (no silent overwrite).
        let row = repo.get_by_hash("dup-hash").await.unwrap().unwrap();
        assert_eq!(row.id, "id1");
    }
}
