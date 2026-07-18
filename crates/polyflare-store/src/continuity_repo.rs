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
    /// TA6(b) Task 3 (`migrations/0008`): a comma-separated capability-tag SET stamped by
    /// `set_required_capability` once a cyber-rejected turn is successfully rerouted onto a
    /// capability-holding account. `NULL`/empty ⇒ no sticky requirement (the common case).
    /// Content-free — a capability tag, never conversation content.
    pub required_capabilities: Option<String>,
}

impl SessionRow {
    /// Whether `capability` is present in this session's sticky capability set.
    pub fn has_capability(&self, capability: &str) -> bool {
        self.required_capabilities
            .as_deref()
            .map(|set| set.split(',').any(|tag| tag == capability))
            .unwrap_or(false)
    }
}

const SELECT_SESSION: &str = "SELECT session_key, key_strength, owning_account_id, \
    anchor_response_id, last_input_fingerprint, last_input_count, reasoning_cache_ref, state, \
    created_at, updated_at, last_activity_at, required_capabilities \
    FROM continuity_sessions WHERE session_key = ?";

/// One `continuity_sessions` row joined to its owning account's email (TA6(c): operator
/// session->account affinity visibility). Content-free: no `anchor_response_id`,
/// `last_input_fingerprint`, `last_input_count`, or `reasoning_cache_ref` — those stay internal
/// to `SessionRow`, out of scope for this read-only surface.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct SessionWithOwner {
    pub session_key: String,
    pub key_strength: String,
    pub owning_account_id: Option<String>,
    pub owner_email: Option<String>,
    pub state: String,
    pub required_capabilities: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub last_activity_at: i64,
}

const SELECT_SESSIONS_WITH_OWNER: &str = "SELECT s.session_key, s.key_strength, \
    s.owning_account_id, a.email AS owner_email, s.state, s.required_capabilities, \
    s.created_at, s.updated_at, s.last_activity_at \
    FROM continuity_sessions s \
    LEFT JOIN accounts a ON a.id = s.owning_account_id \
    ORDER BY s.last_activity_at DESC \
    LIMIT ? OFFSET ?";

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

    /// List `continuity_sessions` rows LEFT JOINed to their owning account's email (a session
    /// with `owning_account_id IS NULL` — never completed a turn, or its owner was deleted — MUST
    /// still be returned, with `owner_email = None`; an INNER JOIN would silently drop it).
    /// Ordered by `last_activity_at DESC` (backed by `idx_continuity_sessions_activity`).
    pub async fn list_sessions_with_owner(
        &self,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<SessionWithOwner>, sqlx::Error> {
        sqlx::query_as::<_, SessionWithOwner>(SELECT_SESSIONS_WITH_OWNER)
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await
    }

    /// Total count of `continuity_sessions` rows (for the `{total, rows}` pagination envelope).
    pub async fn count_sessions(&self) -> Result<i64, sqlx::Error> {
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM continuity_sessions")
            .fetch_one(&self.pool)
            .await?;
        Ok(count)
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
    /// Anchored-path fast path: ensure the session row exists AND mark it `reattaching` in ONE
    /// UPSERT — behavior-equivalent to `ensure_session` + `set_state("reattaching")` but a single
    /// write/commit per anchored request (one fewer fsync on the hot path). A missing row is
    /// inserted directly in `reattaching`; an existing row is moved to `reattaching` with its
    /// `updated_at`/`last_activity_at` bumped (its `created_at` and `key_strength` preserved,
    /// exactly as the two-call sequence left them).
    pub async fn ensure_session_reattaching(
        &self,
        session_key: &str,
        key_strength: &str,
        now: i64,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO continuity_sessions \
             (session_key, key_strength, state, created_at, updated_at, last_activity_at) \
             VALUES (?, ?, 'reattaching', ?, ?, ?) \
             ON CONFLICT(session_key) DO UPDATE SET \
             state = 'reattaching', updated_at = ?, last_activity_at = ?",
        )
        .bind(session_key)
        .bind(key_strength)
        .bind(now) // created_at (insert path only)
        .bind(now) // updated_at (insert path)
        .bind(now) // last_activity_at (insert path)
        .bind(now) // updated_at (conflict update)
        .bind(now) // last_activity_at (conflict update)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

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

    /// TA6(b) Task 3: stamp `capability` into the session's sticky capability SET (union, not
    /// overwrite — a no-op if already present). Called once, right when a cyber-rejected turn is
    /// successfully rerouted onto a capability-holding account
    /// (`ingress.rs::reroute_cyber_rejection`), so a LATER `prepare` on this session pre-filters
    /// via `SelectionCtx.require_security_work_authorized` instead of re-hitting the rejection —
    /// the reject-and-move cost is paid ONCE per session, not once per turn. Content-free:
    /// `capability` is a fixed capability tag, never conversation content. A no-op (no rows
    /// touched) if the session row doesn't exist yet — the caller only ever reaches this after a
    /// turn on that session already completed `prepare` (which `ensure_session`s the row).
    pub async fn set_required_capability(
        &self,
        session_key: &str,
        capability: &str,
        now: i64,
    ) -> Result<(), StoreError> {
        let existing: Option<(Option<String>,)> = sqlx::query_as(
            "SELECT required_capabilities FROM continuity_sessions WHERE session_key = ?",
        )
        .bind(session_key)
        .fetch_optional(&self.pool)
        .await?;
        let current = existing.and_then(|(c,)| c).unwrap_or_default();
        let mut tags: Vec<&str> = current.split(',').filter(|t| !t.is_empty()).collect();
        if !tags.contains(&capability) {
            tags.push(capability);
        }
        let updated = tags.join(",");
        sqlx::query(
            "UPDATE continuity_sessions SET required_capabilities = ?, updated_at = ? \
             WHERE session_key = ?",
        )
        .bind(updated)
        .bind(now)
        .bind(session_key)
        .execute(&self.pool)
        .await?;
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

    /// Like `seed_account` but with a caller-chosen email, so the list-with-owner tests can prove
    /// the joined `owner_email` tracks the RIGHT account per row (not just any non-null value).
    async fn seed_account_with_email(s: &Store, id: &str, email: &str) {
        sqlx::query(
            "INSERT INTO accounts (id, email, access_token_enc, refresh_token_enc, id_token_enc, created_at) \
             VALUES (?, ?, X'00', X'00', X'00', 0)",
        )
        .bind(id)
        .bind(email)
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

    // ---- TA6(b) Task 3: sticky-cyber capability -----------------------------------------------

    #[tokio::test]
    async fn set_required_capability_stamps_the_session_row() {
        let s = store().await;
        let repo = s.continuity();
        repo.ensure_session("skCap", "soft", 1).await.unwrap();

        // Before the stamp: no capability requirement (the common, non-cyber case).
        let before = repo.get_session("skCap").await.unwrap().unwrap();
        assert!(!before.has_capability("security_work"));

        repo.set_required_capability("skCap", "security_work", 2)
            .await
            .unwrap();

        let after = repo.get_session("skCap").await.unwrap().unwrap();
        assert!(
            after.has_capability("security_work"),
            "the session row carries the sticky-cyber flag after the stamp"
        );
    }

    #[tokio::test]
    async fn set_required_capability_is_idempotent_and_content_free() {
        let s = store().await;
        let repo = s.continuity();
        repo.ensure_session("skCap2", "soft", 1).await.unwrap();
        repo.set_required_capability("skCap2", "security_work", 2)
            .await
            .unwrap();
        // Stamping the SAME capability again must not duplicate it in the set.
        repo.set_required_capability("skCap2", "security_work", 3)
            .await
            .unwrap();
        let row = repo.get_session("skCap2").await.unwrap().unwrap();
        assert_eq!(
            row.required_capabilities.as_deref(),
            Some("security_work"),
            "the tag set stays a single entry, not duplicated"
        );
    }

    #[tokio::test]
    async fn a_non_cyber_session_never_carries_the_capability_flag() {
        // Regression: an ordinary session that never went through a cyber move must never report
        // `has_capability` true — the column defaults to NULL/absent.
        let s = store().await;
        let repo = s.continuity();
        repo.ensure_session("skPlain", "soft", 1).await.unwrap();
        let row = repo.get_session("skPlain").await.unwrap().unwrap();
        assert!(!row.has_capability("security_work"));
        assert_eq!(row.required_capabilities, None);
    }

    // ---- TA6(c) Task 1: list sessions with owner email (LEFT JOIN accounts) -------------------

    #[tokio::test]
    async fn list_sessions_with_owner_left_joins_email_and_orders_by_activity_desc() {
        let s = store().await;
        seed_account_with_email(&s, "A", "a@example.com").await;
        seed_account_with_email(&s, "B", "b@example.com").await;
        let repo = s.continuity();

        // skA: owned by A, last_activity_at = 300 (most recent).
        repo.ensure_session("skA", "soft", 1).await.unwrap();
        repo.record_completion("skA", "soft", "A", "respA", "fp", 1, 300)
            .await
            .unwrap();
        // skB: owned by B, last_activity_at = 200 (oldest).
        repo.ensure_session("skB", "soft", 1).await.unwrap();
        repo.record_completion("skB", "soft", "B", "respB", "fp", 1, 200)
            .await
            .unwrap();
        // skNone: NEVER completed a turn -> owning_account_id stays NULL (a fresh session), with
        // last_activity_at = 250 (middle) so ordering + the LEFT JOIN survival are both proved.
        repo.ensure_session("skNone", "soft", 250).await.unwrap();

        // (d) count_sessions() == 3.
        assert_eq!(repo.count_sessions().await.unwrap(), 3);

        // (a) list_sessions_with_owner(10, 0) returns all 3, ordered by last_activity_at DESC.
        let rows = repo.list_sessions_with_owner(10, 0).await.unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].session_key, "skA");
        assert_eq!(rows[1].session_key, "skNone");
        assert_eq!(rows[2].session_key, "skB");

        // (b) each owned row's owner_email matches ITS OWN account, not just any non-null value.
        assert_eq!(rows[0].owning_account_id.as_deref(), Some("A"));
        assert_eq!(rows[0].owner_email.as_deref(), Some("a@example.com"));
        assert_eq!(rows[2].owning_account_id.as_deref(), Some("B"));
        assert_eq!(rows[2].owner_email.as_deref(), Some("b@example.com"));

        // (c) the NO-owner row survives the LEFT JOIN: owner_email == None, owning_account_id ==
        // None (an INNER JOIN would silently drop this row instead).
        assert_eq!(rows[1].owning_account_id, None);
        assert_eq!(rows[1].owner_email, None);
        assert_eq!(rows[1].state, "fresh");
    }

    #[tokio::test]
    async fn list_sessions_with_owner_paginates_via_limit_and_offset() {
        let s = store().await;
        let repo = s.continuity();
        repo.ensure_session("p1", "soft", 100).await.unwrap();
        repo.ensure_session("p2", "soft", 200).await.unwrap();
        repo.ensure_session("p3", "soft", 300).await.unwrap();

        // (e) LIMIT/OFFSET paginate: limit 2 -> first 2 (by last_activity_at DESC: p3, p2); offset
        // 2 -> the 3rd (p1).
        let page1 = repo.list_sessions_with_owner(2, 0).await.unwrap();
        assert_eq!(page1.len(), 2);
        assert_eq!(page1[0].session_key, "p3");
        assert_eq!(page1[1].session_key, "p2");

        let page2 = repo.list_sessions_with_owner(2, 2).await.unwrap();
        assert_eq!(page2.len(), 1);
        assert_eq!(page2[0].session_key, "p1");
    }
}
