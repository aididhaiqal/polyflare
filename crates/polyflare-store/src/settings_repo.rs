//! Task 3 of the live-editable Settings subsystem: a repository over the `settings` table, which
//! persists config overrides as plain key/value strings. Content-free by construction — this
//! table (and this repo) never stores a token or secret, only config keys like `"live_logs"`. A
//! later task uses [`SettingsRepo::get_all`] to build a startup overlay (DB overrides layered on
//! top of file/env config) and [`SettingsRepo::set`] to persist a PATCH to that config.

use std::collections::HashMap;

use sqlx::sqlite::SqlitePool;

use crate::StoreError;

/// CRUD over the `settings` table. Cheap to construct (clones the pool handle) — mirrors
/// `ApiKeyRepo`'s shape.
pub struct SettingsRepo {
    pool: SqlitePool,
}

impl SettingsRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// All settings as a key→value map. Empty on a fresh store (no rows written yet). Callers
    /// (the startup overlay) treat every value as an opaque string — this repo does no
    /// type-coercion; that is the overlay's job.
    pub async fn get_all(&self) -> Result<HashMap<String, String>, StoreError> {
        let rows: Vec<(String, String)> = sqlx::query_as("SELECT key, value FROM settings")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().collect())
    }

    /// Upsert one setting. Inserts a new row, or — if `key` already exists — overwrites its
    /// `value`/`updated_at` in place (no duplicate row for the same key).
    pub async fn set(&self, key: &str, value: &str, now: i64) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
        )
        .bind(key)
        .bind(value)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Read-modify-write ONE setting atomically, returning the value the row now holds.
    ///
    /// `get_all` + [`Self::set`] is a lost-update race whenever a value is a collection edited in
    /// place: two operators adding a different entry concurrently both read the old string, and
    /// whichever writes second silently discards the other's addition. The transaction opens with
    /// `BEGIN IMMEDIATE`, which takes SQLite's write lock BEFORE the read rather than trying to
    /// upgrade a read lock at commit time, so the read and the write are serialized against any
    /// other writer as one unit. (A plain deferred `BEGIN` would not lose the update under WAL, but
    /// it would fail the second committer with `SQLITE_BUSY_SNAPSHOT` instead of serializing it.)
    ///
    /// `edit` receives the current raw value (`None` when the row does not exist yet) and returns
    /// the replacement, or `None` to leave the row untouched — which is how a caller rejects its own
    /// change (a validation failure) without writing. Callers carry the REASON for that rejection
    /// out-of-band, so this signature stays free of caller-specific error types.
    pub async fn mutate(
        &self,
        key: &str,
        now: i64,
        edit: impl FnOnce(Option<&str>) -> Option<String>,
    ) -> Result<String, StoreError> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let current: Option<(String,)> = sqlx::query_as("SELECT value FROM settings WHERE key = ?")
            .bind(key)
            .fetch_optional(&mut *tx)
            .await?;
        let current = current.map(|row| row.0);
        let Some(next) = edit(current.as_deref()) else {
            tx.rollback().await?;
            return Ok(current.unwrap_or_default());
        };
        sqlx::query(
            "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
        )
        .bind(key)
        .bind(&next)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(next)
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

    /// Two concurrent additions to the same collection-valued setting must BOTH survive. The
    /// read-modify-write this replaced lost whichever addition committed first.
    #[tokio::test]
    async fn concurrent_mutations_do_not_lose_an_entry() {
        let s = store().await;
        let repo = s.settings();
        repo.set("pins", "a", 1).await.unwrap();

        let (left, right) = tokio::join!(
            repo.mutate("pins", 2, |current| {
                Some(format!("{}\nb", current.unwrap_or_default()))
            }),
            repo.mutate("pins", 3, |current| {
                Some(format!("{}\nc", current.unwrap_or_default()))
            }),
        );
        left.unwrap();
        right.unwrap();

        let stored = repo.get_all().await.unwrap();
        let value = stored.get("pins").expect("row exists");
        let entries: Vec<&str> = value.lines().collect();
        assert!(entries.contains(&"a"), "the pre-existing entry survives");
        assert!(entries.contains(&"b"), "the first addition survives");
        assert!(
            entries.contains(&"c"),
            "the second addition survives — a lost update would drop one of b/c"
        );
    }

    /// Returning `None` from the edit closure leaves the row exactly as it was.
    #[tokio::test]
    async fn a_rejected_mutation_writes_nothing() {
        let s = store().await;
        let repo = s.settings();
        repo.set("pins", "a", 1).await.unwrap();
        let unchanged = repo.mutate("pins", 2, |_| None).await.unwrap();
        assert_eq!(unchanged, "a");
        assert_eq!(repo.get_all().await.unwrap().get("pins").unwrap(), "a");
    }

    #[tokio::test]
    async fn get_all_on_fresh_store_is_empty() {
        let s = store().await;
        let repo = s.settings();
        assert!(repo.get_all().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn set_then_get_all_round_trips() {
        let s = store().await;
        let repo = s.settings();
        repo.set("live_logs", "true", 100).await.unwrap();

        let all = repo.get_all().await.unwrap();
        assert_eq!(all.get("live_logs").map(String::as_str), Some("true"));
    }

    #[tokio::test]
    async fn set_overwrites_existing_key_not_a_second_row() {
        let s = store().await;
        let repo = s.settings();
        repo.set("live_logs", "true", 100).await.unwrap();
        repo.set("live_logs", "false", 200).await.unwrap();

        let all = repo.get_all().await.unwrap();
        assert_eq!(
            all.get("live_logs").map(String::as_str),
            Some("false"),
            "second set() overwrites the value"
        );
        assert_eq!(
            all.len(),
            1,
            "overwrite must not leave a duplicate row for the same key"
        );
    }
}
