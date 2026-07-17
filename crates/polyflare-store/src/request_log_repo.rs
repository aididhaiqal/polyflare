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

/// Defines the HTTP status code threshold that classifies a request as an error across all read
/// surfaces: [`RequestLogRepo::aggregate_since`], [`RequestLogRepo::series_since`],
/// [`RequestLogRepo::recent_errors`], and [`RequestLogRepo::page`] filtering. Errors are
/// requests with status >= this value; success are status < 300; 3xx redirects count toward
/// total but not toward success or error.
///
/// **IMPORTANT:** This constant defines error classification EVERYWHERE in this module. If this
/// value changes, all SQL queries and filters that reference it MUST be updated together to
/// maintain consistency across the dashboard's request metrics. See the four usage sites:
/// `aggregate_since`, `series_since`, `recent_errors`, and `push_where`.
const ERROR_STATUS_MIN: i64 = 400;

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

/// The `GET /api/overview` KPI rollup produced by [`RequestLogRepo::aggregate_since`]: content-free
/// counts/metrics over a time window, never rows.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RequestAggregate {
    pub total: i64,
    pub success: i64,
    pub error: i64,
    pub avg_latency_ms: f64,
    pub total_tokens: i64,
}

/// One time bucket of [`RequestLogRepo::series_since`]: a content-free request-volume rollup for a
/// single `bucket_secs`-wide window — the dashboard overview's request-volume chart. Same metric
/// set as [`RequestAggregate`] minus `success` (the chart only plots total/error volume + latency +
/// tokens per bucket; `success` is derivable as `requests - errors` if a future consumer needs it).
#[derive(Debug, Clone, PartialEq)]
pub struct RequestBucket {
    /// Bucket start, unix-epoch seconds (i.e. `(requested_at / bucket_secs) * bucket_secs`).
    pub ts: i64,
    pub requests: i64,
    pub errors: i64,
    pub avg_latency_ms: f64,
    pub total_tokens: i64,
}

/// One row of [`RequestLogRepo::recent_errors`]: content-free error identification, never a body or
/// upstream error message.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct RecentErrorRow {
    pub status: i64,
    pub account_id: Option<String>,
    pub error_code: Option<String>,
    pub requested_at: i64,
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

    /// One-query content-free rollup over `request_log` rows at/after `since_ts` — the dashboard
    /// overview's KPI tile (`GET /api/overview`). `total`/`success`/`error` classify by the same
    /// status-class rule as [`RequestsFilter::status_class`] (`success` = `status < 300`, `error` =
    /// `status >= 400`; the gap between them, e.g. 3xx redirects, counts toward `total` only).
    /// `avg_latency_ms`/`total_tokens` default to `0.0`/`0` (via `COALESCE`) when no row matches,
    /// so an empty window renders as zeroed KPIs rather than nulls.
    pub async fn aggregate_since(&self, since_ts: i64) -> Result<RequestAggregate, StoreError> {
        let row: (i64, i64, i64, f64, i64) = sqlx::query_as(&format!(
            "SELECT COUNT(*), \
                    COALESCE(SUM(CASE WHEN status < 300 THEN 1 ELSE 0 END), 0), \
                    COALESCE(SUM(CASE WHEN status >= {} THEN 1 ELSE 0 END), 0), \
                    COALESCE(AVG(duration_ms), 0.0), \
                    COALESCE(SUM(total_tokens), 0) \
             FROM request_log WHERE requested_at >= ?",
            ERROR_STATUS_MIN
        ))
        .bind(since_ts)
        .fetch_one(&self.pool)
        .await?;
        Ok(RequestAggregate {
            total: row.0,
            success: row.1,
            error: row.2,
            avg_latency_ms: row.3,
            total_tokens: row.4,
        })
    }

    /// Content-free request-volume time series for the dashboard overview's chart
    /// (`GET /api/overview/series`): one row per `bucket_secs`-wide bucket at/after `since_ts`,
    /// ascending by bucket start (`ts`). Bucketing is done in SQL via integer-division grouping
    /// (`(requested_at / bucket_secs) * bucket_secs`), not by fetching rows into Rust. `errors`
    /// classifies by the SAME rule [`Self::aggregate_since`] uses (`status >= 400`); `avg_latency_ms`
    /// / `total_tokens` are per-bucket `AVG`/`SUM`, never null (a bucket only exists here because it
    /// has >= 1 row).
    ///
    /// # `bucket_secs` guard
    /// Clamped to a minimum of 1 so the SQL integer division can never divide by zero. There is no
    /// legitimate caller today (a hardcoded hourly bucket — see `read_api.rs`'s
    /// `OVERVIEW_SERIES_BUCKET_SECS`) that would pass `<= 0`; clamping (rather than returning a new
    /// `StoreError` variant) keeps this infallible-by-construction like `aggregate_since`.
    ///
    /// # Gaps are NOT filled in here
    /// SQL only emits buckets that have at least one row — a window with no traffic for an hour
    /// produces NO row for that hour, not a zeroed one. Zero-filling the full `[since_ts, now]` grid
    /// is the caller's job (`read_api.rs::overview_series_handler`), done exactly once there, so a
    /// chart consumer never has to distinguish "no data" from "gap in the array" itself.
    pub async fn series_since(
        &self,
        since_ts: i64,
        bucket_secs: i64,
    ) -> Result<Vec<RequestBucket>, StoreError> {
        let bucket_secs = bucket_secs.max(1);
        let rows: Vec<(i64, i64, i64, f64, i64)> = sqlx::query_as(&format!(
            "SELECT (requested_at / ?) * ? AS bucket_ts, \
                    COUNT(*), \
                    COALESCE(SUM(CASE WHEN status >= {} THEN 1 ELSE 0 END), 0), \
                    COALESCE(AVG(duration_ms), 0.0), \
                    COALESCE(SUM(total_tokens), 0) \
             FROM request_log WHERE requested_at >= ? \
             GROUP BY bucket_ts ORDER BY bucket_ts ASC",
            ERROR_STATUS_MIN
        ))
        .bind(bucket_secs)
        .bind(bucket_secs)
        .bind(since_ts)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(ts, requests, errors, avg_latency_ms, total_tokens)| RequestBucket {
                ts,
                requests,
                errors,
                avg_latency_ms,
                total_tokens,
            })
            .collect())
    }

    /// The newest `limit` content-free error rows (`status >= 400`), newest first — the dashboard
    /// overview's `recent_errors` tile. Deliberately a narrow, dedicated projection (not `page`):
    /// the overview only ever wants these four fields, and `error_code` (migration 0005) isn't on
    /// [`RequestLogRow`] — wiring it there is a separate, larger change (every other
    /// `RequestLogRecord`/`RequestLogRow` call site would need updating) that this dashboard-read
    /// feature doesn't need.
    pub async fn recent_errors(&self, limit: i64) -> Result<Vec<RecentErrorRow>, StoreError> {
        let rows = sqlx::query_as::<_, RecentErrorRow>(&format!(
            "SELECT status, account_id, error_code, requested_at FROM request_log \
             WHERE status >= {} ORDER BY requested_at DESC, id DESC LIMIT ?",
            ERROR_STATUS_MIN
        ))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
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
                qb.push("status >= ");
                qb.push_bind(ERROR_STATUS_MIN);
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

    fn row_at(
        requested_at: i64,
        status: u16,
        total_tokens: Option<i64>,
        duration_ms: i64,
    ) -> RequestLogRecord {
        RequestLogRecord {
            requested_at,
            provider: "codex".into(),
            method: "POST".into(),
            path: "/responses".into(),
            aliased: false,
            status,
            duration_ms,
            account_id: None,
            model: None,
            reasoning_effort: None,
            service_tier: None,
            transport: None,
            ttft_ms: None,
            total_tokens,
            cached_tokens: None,
        }
    }

    /// `aggregate_since` rolls up ONLY rows at/after `since_ts`, classifying by the same
    /// status-class rule `page`'s `status_class` filter uses, and defaults an empty window's
    /// latency/tokens to zero (not null).
    #[tokio::test]
    async fn aggregate_since_rolls_up_counts_latency_and_tokens_within_the_window() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let repo = store.request_log();

        repo.insert(&row_at(50, 200, Some(999), 999)).await.unwrap(); // before the window — must not count
        repo.insert(&row_at(200, 200, Some(1000), 100))
            .await
            .unwrap();
        repo.insert(&row_at(210, 500, Some(2000), 300))
            .await
            .unwrap();

        let agg = repo.aggregate_since(200).await.unwrap();
        assert_eq!(agg.total, 2, "the ts=50 row is outside the window");
        assert_eq!(agg.success, 1);
        assert_eq!(agg.error, 1);
        assert_eq!(agg.total_tokens, 3000);
        assert_eq!(agg.avg_latency_ms, 200.0, "(100 + 300) / 2");

        let empty = repo.aggregate_since(1_000_000).await.unwrap();
        assert_eq!(empty.total, 0);
        assert_eq!(
            empty.avg_latency_ms, 0.0,
            "an empty window rolls up to zero, not null"
        );
        assert_eq!(empty.total_tokens, 0);
    }

    /// `series_since` groups rows into `bucket_secs`-wide buckets (via SQL integer-division), each
    /// bucket carrying its own `requests`/`errors`/`avg_latency_ms`/`total_tokens` rollup — mirroring
    /// `aggregate_since`'s per-window math, just repeated per bucket instead of over the whole
    /// window.
    #[tokio::test]
    async fn series_since_buckets_rows_and_rolls_up_metrics_per_bucket() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let repo = store.request_log();

        // bucket_secs = 100 → buckets start at multiples of 100.
        // Bucket [0, 100): one row at ts=50.
        repo.insert(&row_at(50, 200, Some(500), 100))
            .await
            .unwrap();
        // Bucket [100, 200): two rows, one success one error, at ts=120 and ts=150.
        repo.insert(&row_at(120, 200, Some(1000), 100))
            .await
            .unwrap();
        repo.insert(&row_at(150, 500, Some(2000), 300))
            .await
            .unwrap();

        let buckets = repo.series_since(0, 100).await.unwrap();
        assert_eq!(buckets.len(), 2, "two distinct buckets have rows");

        assert_eq!(buckets[0].ts, 0);
        assert_eq!(buckets[0].requests, 1);
        assert_eq!(buckets[0].errors, 0);
        assert_eq!(buckets[0].total_tokens, 500);
        assert_eq!(buckets[0].avg_latency_ms, 100.0);

        assert_eq!(buckets[1].ts, 100);
        assert_eq!(buckets[1].requests, 2);
        assert_eq!(buckets[1].errors, 1);
        assert_eq!(buckets[1].total_tokens, 3000);
        assert_eq!(buckets[1].avg_latency_ms, 200.0, "(100 + 300) / 2");
    }

    /// Buckets come back ascending by `ts` regardless of insertion order.
    #[tokio::test]
    async fn series_since_orders_buckets_ascending_by_ts() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let repo = store.request_log();

        // Insert the LATER bucket's row first, to prove ordering isn't just insertion order.
        repo.insert(&row_at(950, 200, Some(1), 50)).await.unwrap();
        repo.insert(&row_at(50, 200, Some(1), 50)).await.unwrap();
        repo.insert(&row_at(450, 200, Some(1), 50)).await.unwrap();

        let buckets = repo.series_since(0, 100).await.unwrap();
        let tss: Vec<i64> = buckets.iter().map(|b| b.ts).collect();
        assert_eq!(tss, vec![0, 400, 900]);
    }

    /// Rows strictly before `since_ts` are excluded from every bucket, same as `aggregate_since`'s
    /// window boundary.
    #[tokio::test]
    async fn series_since_excludes_rows_before_the_window() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let repo = store.request_log();

        repo.insert(&row_at(50, 200, Some(1), 50)).await.unwrap(); // before the window
        repo.insert(&row_at(200, 200, Some(7), 50)).await.unwrap(); // in the window

        let buckets = repo.series_since(200, 100).await.unwrap();
        assert_eq!(buckets.len(), 1, "the ts=50 row's bucket must not appear");
        assert_eq!(buckets[0].ts, 200);
        assert_eq!(buckets[0].total_tokens, 7);
    }

    /// An empty range (no rows at/after `since_ts`) returns an empty `Vec`, not a panic or an error.
    #[tokio::test]
    async fn series_since_returns_empty_vec_for_a_window_with_no_rows() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let repo = store.request_log();

        repo.insert(&row_at(50, 200, Some(1), 50)).await.unwrap();

        let buckets = repo.series_since(1_000_000, 100).await.unwrap();
        assert!(buckets.is_empty());
    }

    /// Gaps between buckets that DO have rows are not filled in by the store — only buckets with at
    /// least one row are emitted. This is the documented split of responsibility: `series_since`
    /// stays a pure SQL rollup, and zero-filling the full grid is the handler's job
    /// (`read_api.rs::overview_series_handler`).
    #[tokio::test]
    async fn series_since_does_not_zero_fill_gaps_between_populated_buckets() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let repo = store.request_log();

        repo.insert(&row_at(0, 200, Some(1), 50)).await.unwrap(); // bucket 0
        // Buckets 100 and 200 have no rows at all.
        repo.insert(&row_at(300, 200, Some(1), 50)).await.unwrap(); // bucket 300

        let buckets = repo.series_since(0, 100).await.unwrap();
        assert_eq!(
            buckets.len(),
            2,
            "only the two populated buckets are returned, not the empty ones in between"
        );
        assert_eq!(buckets[0].ts, 0);
        assert_eq!(buckets[1].ts, 300);
    }

    /// `bucket_secs <= 0` is clamped to `1` rather than panicking on a divide-by-zero in SQL — every
    /// row lands in its own one-second bucket instead.
    #[tokio::test]
    async fn series_since_clamps_non_positive_bucket_secs_to_avoid_divide_by_zero() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let repo = store.request_log();

        repo.insert(&row_at(10, 200, Some(1), 50)).await.unwrap();
        repo.insert(&row_at(20, 200, Some(1), 50)).await.unwrap();

        let buckets = repo.series_since(0, 0).await.unwrap();
        assert_eq!(
            buckets.len(),
            2,
            "clamped to bucket_secs=1 → each row gets its own bucket"
        );
        assert_eq!(buckets[0].ts, 10);
        assert_eq!(buckets[1].ts, 20);
    }

    /// `recent_errors` returns only `status >= 400` rows, newest first, and honors `limit`.
    /// `error_code` isn't yet written by `insert()` (only the codex-lb importer populates it via
    /// raw SQL today — see `import.rs`), so it round-trips as `NULL` until a later task wires a
    /// native write path for it; `account_id` already round-trips since `insert()` binds it today.
    #[tokio::test]
    async fn recent_errors_returns_newest_error_rows_first_and_omits_success() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let repo = store.request_log();

        repo.insert(&rec("codex", "/responses", 200, None))
            .await
            .unwrap(); // success — excluded
        repo.insert(&rec("codex", "/responses", 500, None))
            .await
            .unwrap();
        repo.insert(&rec("codex", "/responses", 429, None))
            .await
            .unwrap();

        let errors = repo.recent_errors(10).await.unwrap();
        assert_eq!(errors.len(), 2, "the 200 row is excluded");
        assert_eq!(
            errors[0].status, 429,
            "id DESC tiebreak => most recently inserted first"
        );
        assert_eq!(errors[0].account_id.as_deref(), Some("acct-1"));
        assert!(errors[0].error_code.is_none());
        assert_eq!(errors[1].status, 500);

        let limited = repo.recent_errors(1).await.unwrap();
        assert_eq!(limited.len(), 1);
        assert_eq!(limited[0].status, 429);
    }
}
