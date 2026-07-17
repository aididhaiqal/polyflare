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
use sqlx::{QueryBuilder, Sqlite};

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

/// Dashboard filter set for [`RequestLogRepo::page`]. Every field is optional; an unset field
/// applies no filter. `status_class` is not a raw column value — `"success"` maps to `status < 300`,
/// `"error"` maps to `status >= 400`, and any other value (including `"all"` or unset) applies no
/// status filter.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RequestsFilter {
    pub account: Option<String>,
    pub provider: Option<String>,
    pub status_class: Option<String>,
    pub model: Option<String>,
    pub transport: Option<String>,
    pub since_ts: Option<i64>,
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

    /// The dashboard's filtered, paginated request-log query: a `limit`-sized page (newest first)
    /// matching `filter`, plus the FILTERED total (not the whole table's row count) so the client's
    /// page count matches what's actually being shown. The `WHERE` is built dynamically — only the
    /// filters actually present in `filter` are bound.
    pub async fn page(
        &self,
        filter: &RequestsFilter,
        limit: i64,
        offset: i64,
    ) -> Result<(Vec<RequestLogRow>, u64), StoreError> {
        let mut select = QueryBuilder::<Sqlite>::new(
            "SELECT id, requested_at, provider, method, path, aliased, status, duration_ms, \
             account_id, model, reasoning_effort, service_tier, transport, ttft_ms, \
             total_tokens, cached_tokens FROM request_log",
        );
        Self::push_where(&mut select, filter);
        select.push(" ORDER BY requested_at DESC, id DESC LIMIT ");
        select.push_bind(limit);
        select.push(" OFFSET ");
        select.push_bind(offset);
        let rows = select
            .build_query_as::<RequestLogRow>()
            .fetch_all(&self.pool)
            .await?;

        let mut count = QueryBuilder::<Sqlite>::new("SELECT COUNT(*) FROM request_log");
        Self::push_where(&mut count, filter);
        let total: i64 = count.build_query_scalar().fetch_one(&self.pool).await?;

        Ok((rows, total.max(0) as u64))
    }

    /// Append a `WHERE ...` clause (or nothing, if `filter` is empty) binding only the filters that
    /// are present. Shared between the row query and the matching count query in [`Self::page`] so
    /// the total always reflects the same filter as the page.
    fn push_where(qb: &mut QueryBuilder<'_, Sqlite>, filter: &RequestsFilter) {
        let mut first = true;

        fn sep(qb: &mut QueryBuilder<'_, Sqlite>, first: &mut bool) {
            if *first {
                qb.push(" WHERE ");
                *first = false;
            } else {
                qb.push(" AND ");
            }
        }

        if let Some(v) = &filter.account {
            sep(qb, &mut first);
            qb.push("account_id = ");
            qb.push_bind(v.clone());
        }
        if let Some(v) = &filter.provider {
            sep(qb, &mut first);
            qb.push("provider = ");
            qb.push_bind(v.clone());
        }
        match filter.status_class.as_deref() {
            Some("success") => {
                sep(qb, &mut first);
                qb.push("status < 300");
            }
            Some("error") => {
                sep(qb, &mut first);
                qb.push("status >= 400");
            }
            _ => {}
        }
        if let Some(v) = &filter.model {
            sep(qb, &mut first);
            qb.push("model = ");
            qb.push_bind(v.clone());
        }
        if let Some(v) = &filter.transport {
            sep(qb, &mut first);
            qb.push("transport = ");
            qb.push_bind(v.clone());
        }
        if let Some(v) = filter.since_ts {
            sep(qb, &mut first);
            qb.push("requested_at >= ");
            qb.push_bind(v);
        }
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

    fn rec(provider: &str, path: &str, status: u16, model: Option<&str>) -> RequestLogRecord {
        RequestLogRecord {
            requested_at: 100,
            provider: provider.into(),
            method: "POST".into(),
            path: path.into(),
            aliased: false,
            status,
            duration_ms: 1000,
            account_id: Some("acct-1".into()),
            model: model.map(String::from),
            reasoning_effort: None,
            service_tier: None,
            transport: Some("http".into()),
            ttft_ms: Some(200),
            total_tokens: Some(500),
            cached_tokens: None,
        }
    }

    /// `page` applies the filters (provider, status_class) and returns a `(rows, total)` pair where
    /// `total` reflects the FILTERED count, not the whole table — the dashboard's "how many pages"
    /// number must match what's actually being shown.
    #[tokio::test]
    async fn page_filters_by_provider_and_status_class_with_matching_total() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let repo = store.request_log();

        repo.insert(&rec("codex", "/responses", 200, Some("gpt-5.6-sol")))
            .await
            .unwrap();
        repo.insert(&rec("codex", "/responses", 500, Some("gpt-5.6-sol")))
            .await
            .unwrap();
        repo.insert(&rec("anthropic", "/v1/messages", 200, Some("claude")))
            .await
            .unwrap();

        // provider=codex → 2 rows (one success, one error), total==2.
        let filter = RequestsFilter {
            provider: Some("codex".into()),
            ..Default::default()
        };
        let (rows, total) = repo.page(&filter, 10, 0).await.unwrap();
        assert_eq!(total, 2);
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| r.provider == "codex"));

        // status_class=error → only the 500.
        let filter = RequestsFilter {
            status_class: Some("error".into()),
            ..Default::default()
        };
        let (rows, total) = repo.page(&filter, 10, 0).await.unwrap();
        assert_eq!(total, 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].status, 500);

        // status_class=success → the two 200s.
        let filter = RequestsFilter {
            status_class: Some("success".into()),
            ..Default::default()
        };
        let (rows, total) = repo.page(&filter, 10, 0).await.unwrap();
        assert_eq!(total, 2);
        assert_eq!(rows.len(), 2);

        // No filter → all 3, and limit/offset still page within the filtered set.
        let (rows, total) = repo.page(&RequestsFilter::default(), 1, 1).await.unwrap();
        assert_eq!(total, 3);
        assert_eq!(rows.len(), 1);
    }
}
