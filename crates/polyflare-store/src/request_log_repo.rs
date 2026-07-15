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
             (requested_at, provider, method, path, aliased, status, duration_ms) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(record.requested_at)
        .bind(&record.provider)
        .bind(&record.method)
        .bind(&record.path)
        .bind(record.aliased)
        .bind(i64::from(record.status))
        .bind(record.duration_ms)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// The newest `limit` rows, `offset`-paginated, newest-first — the dashboard list query.
    pub async fn list(&self, limit: i64, offset: i64) -> Result<Vec<RequestLogRow>, StoreError> {
        let rows = sqlx::query_as::<_, RequestLogRow>(
            "SELECT id, requested_at, provider, method, path, aliased, status, duration_ms \
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
