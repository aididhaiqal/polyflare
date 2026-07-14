//! Repository over the continuity state machine tables. Runtime-checked sqlx; no conversation
//! content is ever written here.

use sqlx::sqlite::SqlitePool;

use crate::StoreError;

/// One `continuity_sessions` row.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct SessionRow {
    pub session_key: String,
    pub key_strength: String,
    pub owning_account_id: Option<String>,
    pub anchor_response_id: Option<String>,
    pub last_input_fingerprint: Option<String>,
    pub last_input_count: Option<i64>,
    pub reasoning_cache_ref: Option<String>,
    pub state: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub last_activity_at: i64,
}

const SELECT_SESSION: &str = "SELECT session_key, key_strength, owning_account_id, \
    anchor_response_id, last_input_fingerprint, last_input_count, reasoning_cache_ref, state, \
    created_at, updated_at, last_activity_at FROM continuity_sessions WHERE session_key = ?";

/// CRUD over the continuity state machine. Cheap to construct (clones the pool handle).
pub struct ContinuityRepo {
    pool: SqlitePool,
}

impl ContinuityRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Fetch a session row by key.
    pub async fn get_session(&self, session_key: &str) -> Result<Option<SessionRow>, StoreError> {
        let row = sqlx::query_as::<_, SessionRow>(SELECT_SESSION)
            .bind(session_key)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row)
    }

    /// Resolve a `response_id` to its owning account id, if known.
    pub async fn get_anchor_owner(&self, response_id: &str) -> Result<Option<String>, StoreError> {
        let owner: Option<(String,)> = sqlx::query_as::<_, (String,)>(
            "SELECT owning_account_id FROM continuity_anchors WHERE response_id = ?",
        )
        .bind(response_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(owner.map(|(o,)| o))
    }

    /// Create the session row `state='fresh'` if it does not already exist (idempotent).
    pub async fn ensure_session(
        &self,
        session_key: &str,
        key_strength: &str,
        now: i64,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT OR IGNORE INTO continuity_sessions \
             (session_key, key_strength, state, created_at, updated_at, last_activity_at) \
             VALUES (?, ?, 'fresh', ?, ?, ?)",
        )
        .bind(session_key)
        .bind(key_strength)
        .bind(now)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Set the session state (e.g. `'reattaching'`) + bump activity timestamps.
    pub async fn set_state(
        &self,
        session_key: &str,
        state: &str,
        now: i64,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "UPDATE continuity_sessions SET state = ?, updated_at = ?, last_activity_at = ? \
             WHERE session_key = ?",
        )
        .bind(state)
        .bind(now)
        .bind(now)
        .bind(session_key)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Record a completed turn: pin owner + anchor + `state='anchored'`, and map the response id
    /// to its owner. Atomic (single transaction). The session row must already exist (prepare
    /// calls `ensure_session`); `INSERT OR IGNORE` guards a race.
    #[allow(clippy::too_many_arguments)]
    pub async fn record_completion(
        &self,
        session_key: &str,
        key_strength: &str,
        owning_account: &str,
        anchor_response_id: &str,
        input_fingerprint: &str,
        input_count: i64,
        now: i64,
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "INSERT OR IGNORE INTO continuity_sessions \
             (session_key, key_strength, state, created_at, updated_at, last_activity_at) \
             VALUES (?, ?, 'fresh', ?, ?, ?)",
        )
        .bind(session_key)
        .bind(key_strength)
        .bind(now)
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE continuity_sessions SET owning_account_id = ?, anchor_response_id = ?, \
             last_input_fingerprint = ?, last_input_count = ?, state = 'anchored', \
             updated_at = ?, last_activity_at = ? WHERE session_key = ?",
        )
        .bind(owning_account)
        .bind(anchor_response_id)
        .bind(input_fingerprint)
        .bind(input_count)
        .bind(now)
        .bind(now)
        .bind(session_key)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT OR REPLACE INTO continuity_anchors \
             (response_id, session_key, owning_account_id, created_at) VALUES (?, ?, ?, ?)",
        )
        .bind(anchor_response_id)
        .bind(session_key)
        .bind(owning_account)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Record a recovery. If a new anchor id was produced, re-home the owner + anchor + map it;
    /// otherwise just mark the session `anchored` again (Strategy B produced no new id).
    pub async fn record_recovery(
        &self,
        session_key: &str,
        owning_account: &str,
        new_response_id: Option<&str>,
        now: i64,
    ) -> Result<(), StoreError> {
        match new_response_id {
            Some(rid) => {
                let mut tx = self.pool.begin().await?;
                sqlx::query(
                    "UPDATE continuity_sessions SET owning_account_id = ?, anchor_response_id = ?, \
                     state = 'anchored', updated_at = ?, last_activity_at = ? WHERE session_key = ?",
                )
                .bind(owning_account)
                .bind(rid)
                .bind(now)
                .bind(now)
                .bind(session_key)
                .execute(&mut *tx)
                .await?;
                sqlx::query(
                    "INSERT OR REPLACE INTO continuity_anchors \
                     (response_id, session_key, owning_account_id, created_at) VALUES (?, ?, ?, ?)",
                )
                .bind(rid)
                .bind(session_key)
                .bind(owning_account)
                .bind(now)
                .execute(&mut *tx)
                .await?;
                tx.commit().await?;
            }
            None => {
                sqlx::query(
                    "UPDATE continuity_sessions SET state = 'anchored', updated_at = ?, \
                     last_activity_at = ? WHERE session_key = ?",
                )
                .bind(now)
                .bind(now)
                .bind(session_key)
                .execute(&self.pool)
                .await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(unused_imports)] // `super::*` types (SessionRow, StoreError) are only used via inference
mod tests {
    use super::*;
    use crate::store::Store;

    async fn store() -> Store {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(&dir.path().join("s.db")).await.unwrap();
        std::mem::forget(dir);
        s
    }

    async fn seed_account(s: &Store, id: &str) {
        // A real account row so the owning_account FK is satisfiable.
        sqlx::query(
            "INSERT INTO accounts (id, email, access_token_enc, refresh_token_enc, id_token_enc, created_at) \
             VALUES (?, 'e@x', X'00', X'00', X'00', 0)",
        )
        .bind(id)
        .execute(s.pool())
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn completion_records_owner_anchor_and_map() {
        let s = store().await;
        seed_account(&s, "A").await;
        let repo = s.continuity();

        repo.ensure_session("sk1", "soft", 100).await.unwrap();
        repo.record_completion("sk1", "soft", "A", "resp_1", "fp", 3, 200)
            .await
            .unwrap();

        let row = repo.get_session("sk1").await.unwrap().unwrap();
        assert_eq!(row.owning_account_id.as_deref(), Some("A"));
        assert_eq!(row.anchor_response_id.as_deref(), Some("resp_1"));
        assert_eq!(row.state, "anchored");
        assert_eq!(
            repo.get_anchor_owner("resp_1").await.unwrap().as_deref(),
            Some("A")
        );
    }

    #[tokio::test]
    async fn ensure_session_is_idempotent_and_fresh() {
        let s = store().await;
        let repo = s.continuity();
        repo.ensure_session("sk2", "hard", 1).await.unwrap();
        repo.ensure_session("sk2", "hard", 2).await.unwrap(); // no-op, no error
        let row = repo.get_session("sk2").await.unwrap().unwrap();
        assert_eq!(row.state, "fresh");
        assert_eq!(row.key_strength, "hard");
    }

    #[tokio::test]
    async fn recovery_rehomes_owner_and_new_anchor() {
        let s = store().await;
        seed_account(&s, "A").await;
        seed_account(&s, "B").await;
        let repo = s.continuity();
        repo.ensure_session("sk3", "soft", 1).await.unwrap();
        repo.record_completion("sk3", "soft", "A", "resp_1", "fp", 2, 2)
            .await
            .unwrap();
        repo.record_recovery("sk3", "B", Some("resp_2"), 3)
            .await
            .unwrap();
        let row = repo.get_session("sk3").await.unwrap().unwrap();
        assert_eq!(
            row.owning_account_id.as_deref(),
            Some("B"),
            "recovery re-homes owner"
        );
        assert_eq!(row.anchor_response_id.as_deref(), Some("resp_2"));
        assert_eq!(
            repo.get_anchor_owner("resp_2").await.unwrap().as_deref(),
            Some("B")
        );
    }

    #[tokio::test]
    async fn get_anchor_owner_is_none_when_absent() {
        let s = store().await;
        let repo = s.continuity();
        assert!(repo.get_anchor_owner("nope").await.unwrap().is_none());
    }
}
