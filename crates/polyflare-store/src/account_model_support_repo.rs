//! Per-account support for models the upstream `/models` endpoint does not enumerate.
//!
//! The model catalog is built entirely from each account's `/models` list, so it cannot know about
//! a hidden preview model (one usable on a seat but absent from that seat's enumeration). This repo
//! is the store side of recording that missing fact: for a given account and model string, is the
//! model supported, and was that established by a probe or declared by an operator.
//!
//! An operator row outranks a probe row (see `set`) — the probe signal is imperfect, because an
//! account can accept a request for a model and silently serve a fallback, so a human's explicit
//! declaration must win.

use sqlx::sqlite::SqlitePool;

use crate::StoreError;

/// How a support fact was established. An operator declaration is authoritative over a probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupportSource {
    Probe,
    Operator,
}

impl SupportSource {
    fn as_str(self) -> &'static str {
        match self {
            SupportSource::Probe => "probe",
            SupportSource::Operator => "operator",
        }
    }

    fn parse(raw: &str) -> Self {
        match raw {
            "operator" => SupportSource::Operator,
            _ => SupportSource::Probe,
        }
    }
}

/// One `account_model_support` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountModelSupportRow {
    pub account_id: String,
    pub model: String,
    pub supported: bool,
    pub source: SupportSource,
    pub updated_at: i64,
}

/// CRUD over `account_model_support`. Cheap to construct (clones the pool handle).
pub struct AccountModelSupportRepo {
    pool: SqlitePool,
}

impl AccountModelSupportRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Every support row. Small by nature (a handful of hidden models × a few accounts), so the
    /// server loads the whole set into memory for the selection hot path.
    pub async fn get_all(&self) -> Result<Vec<AccountModelSupportRow>, StoreError> {
        let rows: Vec<(String, String, i64, String, i64)> = sqlx::query_as(
            "SELECT account_id, model, supported, source, updated_at FROM account_model_support",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(
                |(account_id, model, supported, source, updated_at)| AccountModelSupportRow {
                    account_id,
                    model,
                    supported: supported != 0,
                    source: SupportSource::parse(&source),
                    updated_at,
                },
            )
            .collect())
    }

    /// Record support for one (account, model).
    ///
    /// A `Probe` write must NOT clobber an existing `operator` row — the operator's declaration is
    /// authoritative, and a background probe running later should never silently undo it. The
    /// `WHERE` on the conflict path enforces exactly that: an operator write always applies; a probe
    /// write applies only when it is not overwriting an operator row.
    pub async fn set(
        &self,
        account_id: &str,
        model: &str,
        supported: bool,
        source: SupportSource,
        now: i64,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO account_model_support (account_id, model, supported, source, updated_at) \
             VALUES (?, ?, ?, ?, ?) \
             ON CONFLICT(account_id, model) DO UPDATE SET \
               supported = excluded.supported, \
               source = excluded.source, \
               updated_at = excluded.updated_at \
             WHERE excluded.source = 'operator' OR account_model_support.source <> 'operator'",
        )
        .bind(account_id)
        .bind(model)
        .bind(i64::from(supported))
        .bind(source.as_str())
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Remove a support row, reverting the (account, model) to "unknown" — selection then falls
    /// back to the live `/models` cache for it.
    pub async fn delete(&self, account_id: &str, model: &str) -> Result<bool, StoreError> {
        let result =
            sqlx::query("DELETE FROM account_model_support WHERE account_id = ? AND model = ?")
                .bind(account_id)
                .bind(model)
                .execute(&self.pool)
                .await?;
        Ok(result.rows_affected() > 0)
    }
}

#[cfg(test)]
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
    async fn a_fresh_store_has_no_support_rows() {
        let s = store().await;
        assert!(s
            .account_model_support()
            .get_all()
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn set_then_get_round_trips() {
        let s = store().await;
        let repo = s.account_model_support();
        repo.set(
            "acct-1",
            "gpt-daybreak-blue-latest",
            true,
            SupportSource::Operator,
            100,
        )
        .await
        .unwrap();
        let rows = repo.get_all().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].account_id, "acct-1");
        assert_eq!(rows[0].model, "gpt-daybreak-blue-latest");
        assert!(rows[0].supported);
        assert_eq!(rows[0].source, SupportSource::Operator);
    }

    /// The precedence rule: a probe must not overwrite an operator's declaration.
    #[tokio::test]
    async fn a_probe_cannot_clobber_an_operator_declaration() {
        let s = store().await;
        let repo = s.account_model_support();
        // Operator says NOT supported.
        repo.set("acct-1", "m", false, SupportSource::Operator, 100)
            .await
            .unwrap();
        // A later probe claims supported — must be ignored.
        repo.set("acct-1", "m", true, SupportSource::Probe, 200)
            .await
            .unwrap();
        let rows = repo.get_all().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert!(
            !rows[0].supported,
            "operator's false must survive the probe"
        );
        assert_eq!(rows[0].source, SupportSource::Operator);
    }

    /// An operator override always applies, even over an existing probe row.
    #[tokio::test]
    async fn an_operator_override_replaces_a_probe_row() {
        let s = store().await;
        let repo = s.account_model_support();
        repo.set("acct-1", "m", true, SupportSource::Probe, 100)
            .await
            .unwrap();
        repo.set("acct-1", "m", false, SupportSource::Operator, 200)
            .await
            .unwrap();
        let rows = repo.get_all().await.unwrap();
        assert!(!rows[0].supported);
        assert_eq!(rows[0].source, SupportSource::Operator);
    }

    /// A probe may update another probe row (re-probing refreshes the observation).
    #[tokio::test]
    async fn a_probe_updates_a_previous_probe() {
        let s = store().await;
        let repo = s.account_model_support();
        repo.set("acct-1", "m", true, SupportSource::Probe, 100)
            .await
            .unwrap();
        repo.set("acct-1", "m", false, SupportSource::Probe, 200)
            .await
            .unwrap();
        let rows = repo.get_all().await.unwrap();
        assert!(!rows[0].supported, "a fresh probe supersedes a stale probe");
    }

    #[tokio::test]
    async fn delete_reverts_to_unknown() {
        let s = store().await;
        let repo = s.account_model_support();
        repo.set("acct-1", "m", true, SupportSource::Operator, 100)
            .await
            .unwrap();
        assert!(repo.delete("acct-1", "m").await.unwrap());
        assert!(repo.get_all().await.unwrap().is_empty());
        assert!(
            !repo.delete("acct-1", "m").await.unwrap(),
            "second delete is a no-op"
        );
    }
}
