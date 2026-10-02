//! Per-message Claude Code usage imported from local transcripts (`claude_local_usage`): the
//! ingest side writes rows idempotently, the analytics side reads range aggregates. Content-free
//! by construction — model names, token counts, timestamps and a project path, never message
//! text.

use std::collections::HashMap;

use sqlx::{FromRow, SqlitePool};

use crate::StoreError;

/// One imported message, as the push script reports it. `cost_usd` is computed by the server at
/// ingest from the API list price of `model`; `None` when the model is unpriced.
#[derive(Debug, Clone, PartialEq)]
pub struct ClaudeLocalUsageRow {
    pub machine: String,
    pub request_id: String,
    pub message_id: String,
    pub session_id: Option<String>,
    pub project: Option<String>,
    pub model: String,
    pub ts: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_write_5m_tokens: i64,
    pub cache_write_1h_tokens: i64,
    pub cache_read_tokens: i64,
    pub service_tier: Option<String>,
    pub speed: Option<String>,
    pub cost_usd: Option<f64>,
}

/// One aggregate line (per model, day, project or machine).
#[derive(Debug, Clone, PartialEq, FromRow)]
pub struct ClaudeLocalAgg {
    pub key: String,
    pub messages: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_write_tokens: i64,
    pub cache_read_tokens: i64,
    /// Sum over priced rows only; `priced_messages` says how many rows contributed.
    pub cost_usd: f64,
    pub priced_messages: i64,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ClaudeLocalSummary {
    pub totals: Option<ClaudeLocalAgg>,
    pub by_model: Vec<ClaudeLocalAgg>,
    /// `key` is the UTC day `YYYY-MM-DD`, ascending.
    pub by_day: Vec<ClaudeLocalAgg>,
    pub by_project: Vec<ClaudeLocalAgg>,
    pub by_machine: Vec<ClaudeLocalAgg>,
    /// Newest imported message in the whole table (not just the range), for "data up to".
    pub latest_ts: Option<i64>,
}

/// Claude transcript usage in the shape the Reports API merges into its own metrics. Token
/// fields follow the Responses contract the dashboard assumes: `input_tokens` is the WHOLE
/// prompt (fresh + cache writes + cache reads), `cached_tokens` the cache-read subset.
#[derive(Debug, Clone, PartialEq, Default, FromRow)]
pub struct ClaudeReportAgg {
    pub key: String,
    pub bucket_ts: i64,
    pub messages: i64,
    pub input_tokens: i64,
    pub cached_tokens: i64,
    pub cache_write_tokens: i64,
    pub output_tokens: i64,
    pub cost_usd: f64,
}

const REPORT_SELECT: &str = "COUNT(*) AS messages,      COALESCE(SUM(input_tokens + cache_write_5m_tokens + cache_write_1h_tokens + cache_read_tokens),0) AS input_tokens,      COALESCE(SUM(cache_read_tokens),0) AS cached_tokens,      COALESCE(SUM(cache_write_5m_tokens + cache_write_1h_tokens),0) AS cache_write_tokens,      COALESCE(SUM(output_tokens),0) AS output_tokens,      COALESCE(SUM(cost_usd),0.0) AS cost_usd";

#[derive(Clone)]
pub struct ClaudeLocalUsageRepo {
    pool: SqlitePool,
}

const AGG_SELECT: &str = "COUNT(*) AS messages, \
     COALESCE(SUM(input_tokens),0) AS input_tokens, COALESCE(SUM(output_tokens),0) AS output_tokens, \
     COALESCE(SUM(cache_write_5m_tokens + cache_write_1h_tokens),0) AS cache_write_tokens, \
     COALESCE(SUM(cache_read_tokens),0) AS cache_read_tokens, \
     COALESCE(SUM(cost_usd),0.0) AS cost_usd, COUNT(cost_usd) AS priced_messages";

impl ClaudeLocalUsageRepo {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Insert every row that is not already present; returns how many were new. One
    /// transaction per batch so a 500-row push is one fsync, not five hundred.
    pub async fn insert_batch(
        &self,
        rows: &[ClaudeLocalUsageRow],
        imported_at: i64,
    ) -> Result<u64, StoreError> {
        let mut tx = self.pool.begin().await?;
        let mut inserted = 0u64;
        for row in rows {
            let result = sqlx::query(
                "INSERT OR IGNORE INTO claude_local_usage (
                    machine, request_id, message_id, session_id, project, model, ts,
                    input_tokens, output_tokens, cache_write_5m_tokens, cache_write_1h_tokens,
                    cache_read_tokens, service_tier, speed, cost_usd, imported_at
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&row.machine)
            .bind(&row.request_id)
            .bind(&row.message_id)
            .bind(&row.session_id)
            .bind(&row.project)
            .bind(&row.model)
            .bind(row.ts)
            .bind(row.input_tokens)
            .bind(row.output_tokens)
            .bind(row.cache_write_5m_tokens)
            .bind(row.cache_write_1h_tokens)
            .bind(row.cache_read_tokens)
            .bind(&row.service_tier)
            .bind(&row.speed)
            .bind(row.cost_usd)
            .bind(imported_at)
            .execute(&mut *tx)
            .await?;
            inserted += result.rows_affected();
        }
        tx.commit().await?;
        Ok(inserted)
    }

    async fn agg_by(
        &self,
        key_expr: &str,
        since_ts: i64,
        order: &str,
        limit: i64,
    ) -> Result<Vec<ClaudeLocalAgg>, StoreError> {
        let sql = format!(
            "SELECT {key_expr} AS key, {AGG_SELECT} FROM claude_local_usage WHERE ts >= ? GROUP BY 1 ORDER BY {order} LIMIT ?"
        );
        Ok(sqlx::query_as::<_, ClaudeLocalAgg>(&sql)
            .bind(since_ts)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?)
    }

    pub async fn summary(&self, since_ts: i64) -> Result<ClaudeLocalSummary, StoreError> {
        let totals = sqlx::query_as::<_, ClaudeLocalAgg>(&format!(
            "SELECT 'all' AS key, {AGG_SELECT} FROM claude_local_usage WHERE ts >= ?"
        ))
        .bind(since_ts)
        .fetch_optional(&self.pool)
        .await?
        .filter(|t| t.messages > 0);
        let by_model = self
            .agg_by("model", since_ts, "cost_usd DESC, messages DESC", 50)
            .await?;
        let by_day = self
            .agg_by(
                "strftime('%Y-%m-%d', ts, 'unixepoch')",
                since_ts,
                "key ASC",
                400,
            )
            .await?;
        let by_project = self
            .agg_by(
                "COALESCE(project, '(unknown)')",
                since_ts,
                "cost_usd DESC, messages DESC",
                25,
            )
            .await?;
        let by_machine = self
            .agg_by("machine", since_ts, "cost_usd DESC", 25)
            .await?;
        let latest_ts: Option<i64> = sqlx::query_scalar("SELECT MAX(ts) FROM claude_local_usage")
            .fetch_one(&self.pool)
            .await?;
        Ok(ClaudeLocalSummary {
            totals,
            by_model,
            by_day,
            by_project,
            by_machine,
            latest_ts,
        })
    }

    /// Per time bucket (`(ts / bucket_secs) * bucket_secs`) for the Reports time series.
    pub async fn report_series(
        &self,
        since_ts: i64,
        bucket_secs: i64,
    ) -> Result<Vec<ClaudeReportAgg>, StoreError> {
        let sql = format!(
            "SELECT 'bucket' AS key, (ts / ?) * ? AS bucket_ts, {REPORT_SELECT}              FROM claude_local_usage WHERE ts >= ? GROUP BY bucket_ts ORDER BY bucket_ts"
        );
        Ok(sqlx::query_as::<_, ClaudeReportAgg>(&sql)
            .bind(bucket_secs)
            .bind(bucket_secs)
            .bind(since_ts)
            .fetch_all(&self.pool)
            .await?)
    }

    /// Per Reports dimension: `model` is the transcript's model slug; `provider` and
    /// `operation` collapse to `claude_code`; `account` is `claude-code:<machine>`.
    pub async fn report_breakdown(
        &self,
        since_ts: i64,
        dimension: &str,
    ) -> Result<Vec<ClaudeReportAgg>, StoreError> {
        let key_expr = match dimension {
            "model" => "model",
            "account" => "'claude-code:' || machine",
            _ => "'claude_code'",
        };
        let sql = format!(
            "SELECT {key_expr} AS key, 0 AS bucket_ts, {REPORT_SELECT}              FROM claude_local_usage WHERE ts >= ? GROUP BY 1 ORDER BY cost_usd DESC"
        );
        Ok(sqlx::query_as::<_, ClaudeReportAgg>(&sql)
            .bind(since_ts)
            .fetch_all(&self.pool)
            .await?)
    }

    pub async fn report_totals(&self, since_ts: i64) -> Result<ClaudeReportAgg, StoreError> {
        let sql = format!(
            "SELECT 'all' AS key, 0 AS bucket_ts, {REPORT_SELECT} FROM claude_local_usage WHERE ts >= ?"
        );
        Ok(sqlx::query_as::<_, ClaudeReportAgg>(&sql)
            .bind(since_ts)
            .fetch_one(&self.pool)
            .await?)
    }

    /// Newest imported `ts` per machine — what each push script has delivered so far.
    pub async fn latest_by_machine(&self) -> Result<HashMap<String, i64>, StoreError> {
        let rows: Vec<(String, i64)> =
            sqlx::query_as("SELECT machine, MAX(ts) FROM claude_local_usage GROUP BY machine")
                .fetch_all(&self.pool)
                .await?;
        Ok(rows.into_iter().collect())
    }
}
