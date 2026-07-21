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
