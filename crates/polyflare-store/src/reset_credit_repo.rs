use sqlx::sqlite::SqlitePool;
use sqlx::FromRow;

use crate::StoreError;

#[derive(Debug, Clone, FromRow)]
pub struct ResetCredit {
    pub account_id: String,
    pub credit_id: String,
    pub reset_type: Option<String>,
    pub status: Option<String>,
    pub granted_at: Option<i64>,
    pub expires_at: Option<i64>,
    pub title: Option<String>,
    pub description: Option<String>,
    pub redeem_started_at: Option<i64>,
    pub redeemed_at: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct ResetCreditSnapshot {
    pub account_id: String,
    pub available_count: i64,
    pub fetched_at: i64,
    pub credits: Vec<ResetCredit>,
}

#[derive(Debug, Clone, FromRow)]
pub struct ResetCreditRedeemRequest {
    pub account_id: String,
    pub redeem_request_id: String,
    pub credit_id: String,
    pub created_at: i64,
    pub result_code: Option<String>,
    pub windows_reset: Option<i64>,
    pub redeemed_at: Option<i64>,
    pub completed_at: Option<i64>,
}

#[derive(Debug, Clone, FromRow)]
pub struct ResetCreditNativeRequest {
    pub redeem_request_id: String,
    pub account_id: Option<String>,
    pub requested_credit_id: Option<String>,
    pub pool_scope: Option<String>,
    pub created_at: i64,
    pub result_code: Option<String>,
    pub windows_reset: Option<i64>,
    pub completed_at: Option<i64>,
}

#[derive(Debug, Clone, FromRow)]
pub struct ResetCreditRedeemResult {
    pub account_id: String,
    pub redeem_request_id: String,
    pub result_code: String,
    pub windows_reset: i64,
    pub redeemed_at: Option<i64>,
    pub completed_at: i64,
}

#[derive(Clone)]
pub struct ResetCreditRepo {
    pool: SqlitePool,
}

impl ResetCreditRepo {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn replace_snapshot(
        &self,
        account_id: &str,
        available_count: i64,
        fetched_at: i64,
        credits: &[ResetCredit],
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM reset_credits WHERE account_id = ?")
            .bind(account_id)
            .execute(&mut *tx)
            .await?;
        for credit in credits {
            sqlx::query(
                "INSERT INTO reset_credits (
                    account_id, credit_id, reset_type, status, granted_at, expires_at, title,
                    description, redeem_started_at, redeemed_at
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(account_id)
            .bind(&credit.credit_id)
            .bind(&credit.reset_type)
            .bind(&credit.status)
            .bind(credit.granted_at)
            .bind(credit.expires_at)
            .bind(&credit.title)
            .bind(&credit.description)
            .bind(credit.redeem_started_at)
            .bind(credit.redeemed_at)
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query(
            "INSERT INTO reset_credit_snapshots (account_id, available_count, fetched_at)
             VALUES (?, ?, ?)
             ON CONFLICT(account_id) DO UPDATE SET
                available_count = excluded.available_count,
                fetched_at = excluded.fetched_at",
        )
        .bind(account_id)
        .bind(available_count.max(0))
        .bind(fetched_at)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn get_snapshot(
        &self,
        account_id: &str,
    ) -> Result<Option<ResetCreditSnapshot>, StoreError> {
        let header = sqlx::query_as::<_, (i64, i64)>(
            "SELECT available_count, fetched_at FROM reset_credit_snapshots WHERE account_id = ?",
        )
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?;
        let Some((available_count, fetched_at)) = header else {
            return Ok(None);
        };
        let credits = sqlx::query_as::<_, ResetCredit>(
            "SELECT account_id, credit_id, reset_type, status, granted_at, expires_at, title,
                    description, redeem_started_at, redeemed_at
             FROM reset_credits WHERE account_id = ?
             ORDER BY CASE WHEN expires_at IS NULL THEN 1 ELSE 0 END, expires_at, credit_id",
        )
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(Some(ResetCreditSnapshot {
            account_id: account_id.to_string(),
            available_count,
            fetched_at,
            credits,
        }))
    }

    pub async fn list_snapshots(&self) -> Result<Vec<ResetCreditSnapshot>, StoreError> {
        let account_ids = sqlx::query_scalar::<_, String>(
            "SELECT account_id FROM reset_credit_snapshots ORDER BY account_id",
        )
        .fetch_all(&self.pool)
        .await?;
        let mut snapshots = Vec::with_capacity(account_ids.len());
        for account_id in account_ids {
            if let Some(snapshot) = self.get_snapshot(&account_id).await? {
                snapshots.push(snapshot);
            }
        }
        Ok(snapshots)
    }

    pub async fn invalidate(&self, account_id: &str) -> Result<(), StoreError> {
        sqlx::query("DELETE FROM reset_credit_snapshots WHERE account_id = ?")
            .bind(account_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn try_acquire_claim(
        &self,
        account_id: &str,
        holder_id: &str,
        now: i64,
        expires_at: i64,
    ) -> Result<bool, StoreError> {
        let result = sqlx::query(
            "INSERT INTO reset_credit_redeem_claims (account_id, holder_id, expires_at)
             VALUES (?, ?, ?)
             ON CONFLICT(account_id) DO UPDATE SET
                holder_id = excluded.holder_id,
                expires_at = excluded.expires_at
             WHERE reset_credit_redeem_claims.expires_at < ?",
        )
        .bind(account_id)
        .bind(holder_id)
        .bind(expires_at)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn renew_claim(
        &self,
        account_id: &str,
        holder_id: &str,
        expires_at: i64,
    ) -> Result<bool, StoreError> {
        let result = sqlx::query(
            "UPDATE reset_credit_redeem_claims SET expires_at = ?
             WHERE account_id = ? AND holder_id = ?",
        )
        .bind(expires_at)
        .bind(account_id)
        .bind(holder_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn release_claim(&self, account_id: &str, holder_id: &str) -> Result<(), StoreError> {
        sqlx::query(
            "DELETE FROM reset_credit_redeem_claims WHERE account_id = ? AND holder_id = ?",
        )
        .bind(account_id)
        .bind(holder_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn pin_request(
        &self,
        account_id: &str,
        redeem_request_id: &str,
        credit_id: &str,
        now: i64,
        ttl_seconds: i64,
    ) -> Result<String, StoreError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM reset_credit_redeem_requests WHERE created_at < ?")
            .bind(now.saturating_sub(ttl_seconds))
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "INSERT OR IGNORE INTO reset_credit_redeem_requests (
                account_id, redeem_request_id, credit_id, created_at
             ) VALUES (?, ?, ?, ?)",
        )
        .bind(account_id)
        .bind(redeem_request_id)
        .bind(credit_id)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        let pinned = sqlx::query_scalar::<_, String>(
            "SELECT credit_id FROM reset_credit_redeem_requests
             WHERE account_id = ? AND redeem_request_id = ?",
        )
        .bind(account_id)
        .bind(redeem_request_id)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(pinned)
    }

    pub async fn get_native_request(
        &self,
        redeem_request_id: &str,
        now: i64,
        ttl_seconds: i64,
    ) -> Result<Option<ResetCreditNativeRequest>, StoreError> {
        Ok(sqlx::query_as::<_, ResetCreditNativeRequest>(
            "SELECT redeem_request_id, account_id, requested_credit_id, pool_scope, created_at,
                    result_code, windows_reset, completed_at
             FROM reset_credit_native_requests
             WHERE redeem_request_id = ? AND created_at >= ?",
        )
        .bind(redeem_request_id)
        .bind(now.saturating_sub(ttl_seconds))
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn pin_native_request(
        &self,
        redeem_request_id: &str,
        account_id: &str,
        requested_credit_id: Option<&str>,
        pool_scope: &str,
        now: i64,
        ttl_seconds: i64,
    ) -> Result<ResetCreditNativeRequest, StoreError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM reset_credit_native_requests WHERE created_at < ?")
            .bind(now.saturating_sub(ttl_seconds))
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "INSERT OR IGNORE INTO reset_credit_native_requests (
                redeem_request_id, account_id, requested_credit_id, pool_scope, created_at
             ) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(redeem_request_id)
        .bind(account_id)
        .bind(requested_credit_id)
        .bind(pool_scope)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        let pinned = sqlx::query_as::<_, ResetCreditNativeRequest>(
            "SELECT redeem_request_id, account_id, requested_credit_id, pool_scope, created_at,
                    result_code, windows_reset, completed_at
             FROM reset_credit_native_requests WHERE redeem_request_id = ?",
        )
        .bind(redeem_request_id)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(pinned)
    }

    pub async fn complete_native_no_credit(
        &self,
        redeem_request_id: &str,
        pool_scope: &str,
        now: i64,
        ttl_seconds: i64,
    ) -> Result<ResetCreditNativeRequest, StoreError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM reset_credit_native_requests WHERE created_at < ?")
            .bind(now.saturating_sub(ttl_seconds))
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "INSERT OR IGNORE INTO reset_credit_native_requests (
                redeem_request_id, account_id, requested_credit_id, pool_scope, created_at,
                result_code, windows_reset, completed_at
             ) VALUES (?, NULL, NULL, ?, ?, 'no_credit', 0, ?)",
        )
        .bind(redeem_request_id)
        .bind(pool_scope)
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        let pinned = sqlx::query_as::<_, ResetCreditNativeRequest>(
            "SELECT redeem_request_id, account_id, requested_credit_id, pool_scope, created_at,
                    result_code, windows_reset, completed_at
             FROM reset_credit_native_requests WHERE redeem_request_id = ?",
        )
        .bind(redeem_request_id)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(pinned)
    }

    pub async fn complete_native_account_no_credit(
        &self,
        redeem_request_id: &str,
        account_id: &str,
        completed_at: i64,
    ) -> Result<bool, StoreError> {
        let result = sqlx::query(
            "UPDATE reset_credit_native_requests SET
                result_code = 'no_credit', windows_reset = 0, completed_at = ?
             WHERE redeem_request_id = ? AND account_id = ? AND result_code IS NULL",
        )
        .bind(completed_at)
        .bind(redeem_request_id)
        .bind(account_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn get_request(
        &self,
        account_id: &str,
        redeem_request_id: &str,
        now: i64,
        ttl_seconds: i64,
    ) -> Result<Option<ResetCreditRedeemRequest>, StoreError> {
        Ok(sqlx::query_as::<_, ResetCreditRedeemRequest>(
            "SELECT account_id, redeem_request_id, credit_id, created_at, result_code,
                    windows_reset, redeemed_at, completed_at
             FROM reset_credit_redeem_requests
             WHERE account_id = ? AND redeem_request_id = ? AND created_at >= ?",
        )
        .bind(account_id)
        .bind(redeem_request_id)
        .bind(now.saturating_sub(ttl_seconds))
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn get_result(
        &self,
        account_id: &str,
        redeem_request_id: &str,
        now: i64,
        ttl_seconds: i64,
    ) -> Result<Option<ResetCreditRedeemResult>, StoreError> {
        Ok(sqlx::query_as::<_, ResetCreditRedeemResult>(
            "SELECT account_id, redeem_request_id, result_code, windows_reset,
                    redeemed_at, completed_at
             FROM reset_credit_redeem_results
             WHERE account_id = ? AND redeem_request_id = ? AND completed_at >= ?",
        )
        .bind(account_id)
        .bind(redeem_request_id)
        .bind(now.saturating_sub(ttl_seconds))
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn pin_fleet_request(
        &self,
        redeem_request_id: &str,
        account_ids_json: &str,
        now: i64,
        ttl_seconds: i64,
    ) -> Result<String, StoreError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM reset_credit_fleet_requests WHERE created_at < ?")
            .bind(now.saturating_sub(ttl_seconds))
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "INSERT OR IGNORE INTO reset_credit_fleet_requests (
                redeem_request_id, account_ids_json, created_at
             ) VALUES (?, ?, ?)",
        )
        .bind(redeem_request_id)
        .bind(account_ids_json)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        let pinned = sqlx::query_scalar::<_, String>(
            "SELECT account_ids_json FROM reset_credit_fleet_requests
             WHERE redeem_request_id = ?",
        )
        .bind(redeem_request_id)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(pinned)
    }

    pub async fn complete_request(
        &self,
        account_id: &str,
        redeem_request_id: &str,
        code: &str,
        windows_reset: i64,
        redeemed_at: Option<i64>,
        completed_at: i64,
    ) -> Result<ResetCreditRedeemResult, StoreError> {
        self.complete_request_with_native(
            account_id,
            redeem_request_id,
            code,
            windows_reset,
            redeemed_at,
            completed_at,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn complete_request_with_native(
        &self,
        account_id: &str,
        redeem_request_id: &str,
        code: &str,
        windows_reset: i64,
        redeemed_at: Option<i64>,
        completed_at: i64,
        native_request_id: Option<&str>,
    ) -> Result<ResetCreditRedeemResult, StoreError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM reset_credit_redeem_results WHERE completed_at < ?")
            .bind(completed_at.saturating_sub(24 * 3_600))
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "INSERT OR IGNORE INTO reset_credit_redeem_results (
                account_id, redeem_request_id, result_code, windows_reset, redeemed_at, completed_at
             ) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(account_id)
        .bind(redeem_request_id)
        .bind(code)
        .bind(windows_reset)
        .bind(redeemed_at)
        .bind(completed_at)
        .execute(&mut *tx)
        .await?;
        let terminal = sqlx::query_as::<_, ResetCreditRedeemResult>(
            "SELECT account_id, redeem_request_id, result_code, windows_reset,
                    redeemed_at, completed_at
             FROM reset_credit_redeem_results
             WHERE account_id = ? AND redeem_request_id = ?",
        )
        .bind(account_id)
        .bind(redeem_request_id)
        .fetch_one(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE reset_credit_redeem_requests SET
                result_code = ?, windows_reset = ?, redeemed_at = ?, completed_at = ?
             WHERE account_id = ? AND redeem_request_id = ?",
        )
        .bind(&terminal.result_code)
        .bind(terminal.windows_reset)
        .bind(terminal.redeemed_at)
        .bind(terminal.completed_at)
        .bind(account_id)
        .bind(redeem_request_id)
        .execute(&mut *tx)
        .await?;
        if let Some(native_request_id) = native_request_id {
            sqlx::query(
                "UPDATE reset_credit_native_requests SET
                    result_code = ?, windows_reset = ?, completed_at = ?
                 WHERE redeem_request_id = ? AND account_id = ? AND result_code IS NULL",
            )
            .bind(&terminal.result_code)
            .bind(terminal.windows_reset)
            .bind(terminal.completed_at)
            .bind(native_request_id)
            .bind(account_id)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(terminal)
    }
}
