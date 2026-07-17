//! Request-log model + repository: one content-free row per request OUTCOME — the persisted
//! backend for the (post-MVP) dashboard's request-history view.
//!
//! # Content safety
//! Mirrors `polyflare_server::observability`'s constraint: NO conversation content and NO free-form
//! request-derived strings. Every field here is a PolyFlare-generated bounded value
//! (provider/method/path/status), a number, or a unix-epoch timestamp — the same audited field set
//! `RequestLog::emit()` logs. New columns grow this table into codex-lb's full request_log set one
//! feature at a time (see `migrations/0004_request_log.sql`); any request-derived string (raw model,
//! client IP, User-Agent, upstream error text) is gated on an explicit content-safety decision.

use sqlx::sqlite::SqlitePool;

use crate::StoreError;

/// A content-free request-outcome record to persist. Every field is a bounded enum-like string, a
/// number, or an epoch timestamp — never request content. This is the insert input; the persisted
/// row (with its surrogate id) is [`RequestLogRow`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestLogRecord {
    /// Unix-epoch seconds at request completion.
    pub requested_at: i64,
    /// Backend pool that served (or would have served) the request: `"codex"` | `"anthropic"`.
    pub provider: String,
    /// HTTP method, e.g. `"POST"`.
    pub method: String,
    /// Ingress path: `"/responses"` | `"/v1/messages"`.
    pub path: String,
    /// Whether the client model string was alias-mapped to a different target.
    pub aliased: bool,
    /// Client-facing HTTP status code.
    pub status: u16,
    /// Total request duration in milliseconds.
    pub duration_ms: i64,
    /// The serving account's stable id (never a token or session id) — content-free identifier.
    pub account_id: Option<String>,
    /// The resolved backend model name (e.g. `"gpt-5.6-sol"`) — a bounded identifier, never
    /// request/response content.
    pub model: Option<String>,
    /// The requested reasoning effort (e.g. `"high"`), if applicable to the provider/model.
    pub reasoning_effort: Option<String>,
    /// The resolved service tier (e.g. `"priority"`), if applicable.
    pub service_tier: Option<String>,
    /// The transport used to serve the request (e.g. `"http"`, `"sse"`).
    pub transport: Option<String>,
    /// Time-to-first-token in milliseconds, for streaming requests.
    pub ttft_ms: Option<i64>,
    /// Total tokens consumed by the request (prompt + completion), a content-free count.
    pub total_tokens: Option<i64>,
    /// Cached tokens counted toward `total_tokens`, a content-free count.
    pub cached_tokens: Option<i64>,
}

/// A persisted request-log row: a [`RequestLogRecord`] plus its surrogate primary key. `status` is
/// widened to `i64` because that is how SQLite stores it (the insert narrows from `u16`).
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct RequestLogRow {
    pub id: i64,
    pub requested_at: i64,
    pub provider: String,
    pub method: String,
    pub path: String,
    pub aliased: bool,
    pub status: i64,
    pub duration_ms: i64,
    pub account_id: Option<String>,
    pub model: Option<String>,
    pub reasoning_effort: Option<String>,
    pub service_tier: Option<String>,
    pub transport: Option<String>,
    pub ttft_ms: Option<i64>,
    pub total_tokens: Option<i64>,
    pub cached_tokens: Option<i64>,
}

/// Repository over the `request_log` table.
pub struct RequestLogRepo {
    pool: SqlitePool,
}

impl RequestLogRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Insert one request-outcome row. Called fire-and-forget off the response path (see the
    /// ingress completion wrappers), so its failure never affects the client's request.
    pub async fn insert(&self, record: &RequestLogRecord) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO request_log \
             (requested_at, provider, method, path, aliased, status, duration_ms, \
              account_id, model, reasoning_effort, service_tier, transport, ttft_ms, \
              total_tokens, cached_tokens) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(record.requested_at)
        .bind(&record.provider)
        .bind(&record.method)
        .bind(&record.path)
        .bind(record.aliased)
        .bind(i64::from(record.status))
        .bind(record.duration_ms)
        .bind(&record.account_id)
        .bind(&record.model)
        .bind(&record.reasoning_effort)
        .bind(&record.service_tier)
        .bind(&record.transport)
        .bind(record.ttft_ms)
        .bind(record.total_tokens)
        .bind(record.cached_tokens)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// The newest `limit` rows, `offset`-paginated, newest-first — the dashboard list query.
    pub async fn list(&self, limit: i64, offset: i64) -> Result<Vec<RequestLogRow>, StoreError> {
        let rows = sqlx::query_as::<_, RequestLogRow>(
            "SELECT id, requested_at, provider, method, path, aliased, status, duration_ms, \
             account_id, model, reasoning_effort, service_tier, transport, ttft_ms, \
             total_tokens, cached_tokens \
             FROM request_log \
             ORDER BY requested_at DESC, id DESC \
             LIMIT ? OFFSET ?",
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Total row count (for pagination).
    pub async fn count(&self) -> Result<i64, StoreError> {
        let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM request_log")
            .fetch_one(&self.pool)
            .await?;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Store;

    /// Insert a row carrying every content-free metric field and read it back via the repo's
    /// existing `list` query, asserting the new columns round-trip. `repo.page` (paginated
    /// filtering) lands in a later task; `list` is the repo's current read path and is exactly
    /// what `read_api.rs::requests_handler` already calls.
    #[tokio::test]
    async fn insert_and_read_back_content_free_metrics() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let repo = store.request_log();

        let rec = RequestLogRecord {
            requested_at: 100,
            provider: "codex".into(),
            method: "POST".into(),
            path: "/responses".into(),
            aliased: false,
            status: 200,
            duration_ms: 1800,
            account_id: Some("acct-1".into()),
            model: Some("gpt-5.6-sol".into()),
            reasoning_effort: Some("high".into()),
            service_tier: Some("priority".into()),
            transport: Some("http".into()),
            ttft_ms: Some(700),
            total_tokens: Some(3204),
            cached_tokens: Some(1100),
        };
        repo.insert(&rec).await.unwrap();

        let rows = repo.list(10, 0).await.unwrap();
        assert_eq!(repo.count().await.unwrap(), 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].account_id.as_deref(), Some("acct-1"));
        assert_eq!(rows[0].model.as_deref(), Some("gpt-5.6-sol"));
        assert_eq!(rows[0].reasoning_effort.as_deref(), Some("high"));
        assert_eq!(rows[0].service_tier.as_deref(), Some("priority"));
        assert_eq!(rows[0].transport.as_deref(), Some("http"));
        assert_eq!(rows[0].ttft_ms, Some(700));
        assert_eq!(rows[0].total_tokens, Some(3204));
        assert_eq!(rows[0].cached_tokens, Some(1100));
    }
}
