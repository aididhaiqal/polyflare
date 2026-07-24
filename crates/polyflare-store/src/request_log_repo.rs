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

use std::collections::HashMap;

use sqlx::sqlite::SqlitePool;
use sqlx::{QueryBuilder, Sqlite};

use crate::StoreError;

/// Native streaming rows with a terminal `protocol_outcome` classify by that outcome, preserving
/// their initial HTTP status only as transport metadata. Legacy native rows classify by HTTP
/// status. Imported codex-lb rows use `status = 0` as an explicit "HTTP status unavailable"
/// sentinel and classify by their bounded `outcome` instead. Unknown status-0 rows and 3xx
/// redirects count toward total but not toward success or error.
///
/// **IMPORTANT:** These expressions define classification EVERYWHERE in this module. Keep
/// aggregates, report series, recent errors, and page filters on the same pair.
const SUCCESS_SQL: &str = "(protocol_outcome = 'completed' OR \
    (protocol_outcome IS NULL AND ((status >= 100 AND status < 300) OR \
    (status = 0 AND outcome = 'success'))))";
const ERROR_SQL: &str = "(protocol_outcome IN \
    ('failed', 'incomplete', 'cancelled', 'transport_lost') OR \
    (protocol_outcome IS NULL AND (status >= 400 OR \
    (status = 0 AND outcome = 'error'))))";

/// Logical backend traffic includes current rows with the dedicated provider plus historical
/// gateway rows that predate that provider identity but already carry a normalized backend path.
/// Provider filters use this expression so selecting Codex never leaks legacy backend operations.
const BACKEND_REQUEST_SQL: &str = "(provider = 'chatgpt_backend' OR \
    path GLOB 'chatgpt_backend_synthetic_*' OR \
    path GLOB 'chatgpt_backend_passthrough_*')";

/// Upstream Responses total when observed, then the legacy compatibility total, then a complete
/// input+output pair. Cached input and reasoning output are subsets and are never added again.
const API_TOTAL_SQL: &str = "CASE \
    WHEN reported_total_tokens >= 0 THEN reported_total_tokens \
    WHEN total_tokens >= 0 THEN total_tokens \
    WHEN input_tokens IS NOT NULL AND output_tokens IS NOT NULL \
        THEN MAX(input_tokens, 0) + MAX(output_tokens, 0) \
    ELSE 0 END";

/// Codex's primary blended/effective usage: non-cached input plus all output. This is distinct
/// from upstream API total, monetary cost, and PolyFlare's weighted routing-pressure estimate.
const EFFECTIVE_TOKENS_SQL: &str = "CASE \
    WHEN input_tokens IS NOT NULL AND output_tokens IS NOT NULL THEN \
        MAX(MAX(input_tokens, 0) - \
            MIN(MAX(COALESCE(cached_input_tokens, cached_tokens, 0), 0), MAX(input_tokens, 0)), 0) \
        + MAX(output_tokens, 0) \
    ELSE 0 END";

type RequestBucketSqlRow = (i64, i64, i64, f64, i64, i64, i64, i64, i64);
type ReportMetricsSqlRow = (
    i64,
    i64,
    f64,
    i64,
    i64,
    i64,
    i64,
    i64,
    i64,
    i64,
    i64,
    f64,
    f64,
    i64,
);
type ReportBucketSqlRow = (
    i64,
    i64,
    i64,
    f64,
    i64,
    i64,
    i64,
    i64,
    i64,
    i64,
    i64,
    i64,
    f64,
    f64,
    i64,
);
type ReportBreakdownSqlRow = (
    String,
    i64,
    i64,
    f64,
    i64,
    i64,
    i64,
    i64,
    i64,
    i64,
    i64,
    i64,
    f64,
    f64,
    i64,
);

/// A bounded, content-free terminal result observed inside a native Codex response stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestProtocolOutcome {
    Completed,
    Failed,
    Incomplete,
    Cancelled,
    TransportLost,
}

impl RequestProtocolOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Incomplete => "incomplete",
            Self::Cancelled => "cancelled",
            Self::TransportLost => "transport_lost",
        }
    }
}

/// A content-free request-outcome record to persist. Every field is a bounded enum-like string, a
/// number, or an epoch timestamp — never request content. This is the insert input; the persisted
/// row (with its surrogate id) is [`RequestLogRow`].
///
/// Not `Eq` (only `PartialEq`): `cost_usd: Option<f64>` (migration 0005) has no `Eq` impl, since
/// `f64` doesn't implement it.
#[derive(Debug, Clone, PartialEq)]
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
    /// Which target namespace served the request: `account` for built-in OAuth pools or
    /// `credential` for a custom-provider API key. `None` only on legacy/imported rows.
    pub target_kind: Option<String>,
    /// Stable id of the selected custom-provider credential. Never the secret or a secret-derived
    /// value; mutually exclusive with `account_id` on native rows.
    pub provider_credential_id: Option<String>,
    /// The resolved backend model name (e.g. `"gpt-5.6-sol"`) — a bounded identifier, never
    /// request/response content.
    pub model: Option<String>,
    /// Model slug actually sent upstream. Distinct from the public `model` when a provider maps a
    /// stable public alias such as `fugu-ultra` to a versioned upstream slug.
    pub upstream_model: Option<String>,
    /// Upstream transport used behind PolyFlare (`codex_ws`, `http_sse`, ...), distinct from the
    /// downstream/client-facing `transport`.
    pub upstream_transport: Option<String>,
    /// Content-free SHA-256 prefix of the normalized custom model profile configuration.
    pub profile_revision: Option<String>,
    /// The requested reasoning effort (e.g. `"high"`), if applicable to the provider/model.
    pub reasoning_effort: Option<String>,
    /// The resolved service tier (e.g. `"priority"`), if applicable.
    pub service_tier: Option<String>,
    /// The transport used to serve the request (e.g. `"http"`, `"sse"`).
    pub transport: Option<String>,
    /// Time-to-first-token in milliseconds, for streaming requests.
    pub ttft_ms: Option<i64>,
    /// Legacy compatibility total. New canonical capture keeps the upstream value separately in
    /// [`RequestLogRow::reported_total_tokens`].
    pub total_tokens: Option<i64>,
    /// Legacy compatibility copy of cached input.
    pub cached_tokens: Option<i64>,
    /// The codex sub-agent role label (`x-openai-subagent`: `review`/`compact`/
    /// `memory_consolidation`/`collab_spawn`), or `None` for the main agent — a bounded role slug,
    /// never conversation content, same content-safety class as `model`/`transport`.
    pub subagent: Option<String>,
    /// PolyFlare's generated request-correlation id (migration 0005) — a content-free identifier,
    /// never conversation content. Populated at insert time and used by
    /// [`RequestLogRepo::update_usage`] to correlate the stream wrapper's post-completion usage
    /// backfill to this row.
    pub request_id: Option<String>,
    /// PolyFlare's one-way SHA-256 continuity/session key. This is the same content-free identifier
    /// shown by the Sessions dashboard, never a raw session/thread/window header.
    pub session_key: Option<String>,
    /// Prompt/input token count (migration 0005), a content-free count. `None` until the stream
    /// wrapper backfills it via [`RequestLogRepo::update_usage`].
    pub input_tokens: Option<i64>,
    /// Completion/output token count (migration 0005), a content-free count.
    pub output_tokens: Option<i64>,
    /// Cached tokens counted toward `input_tokens` (migration 0005), a content-free count.
    pub cached_input_tokens: Option<i64>,
    /// Reasoning token count (migration 0005), a content-free count.
    pub reasoning_tokens: Option<i64>,
    /// Provider-reported orchestration work billed outside ordinary visible input/output.
    pub orchestration_input_tokens: Option<i64>,
    pub orchestration_output_tokens: Option<i64>,
    pub orchestration_cached_input_tokens: Option<i64>,
    /// Computed request cost in USD (migration 0005).
    pub cost_usd: Option<f64>,
    /// Time-to-first-token in milliseconds (migration 0005). Distinct from `ttft_ms` (migration
    /// 0007): that field is populated today by the observability path; this one is codex-lb's
    /// import-shaped near-neighbor of the same concept, backfilled by the stream wrapper alongside
    /// the other usage/cost columns (see 0005's migration comment).
    pub latency_first_token_ms: Option<i64>,
    /// Bounded native stream terminal result. HTTP-SSE normally fills this through
    /// [`RequestLogRepo::update_usage`] after the response body drains; transports such as the
    /// downstream WebSocket relay that already know the terminal at insert time set it directly.
    pub protocol_outcome: Option<RequestProtocolOutcome>,
}

/// A persisted request-log row: a [`RequestLogRecord`] plus its surrogate primary key. `status` is
/// widened to `i64` because that is how SQLite stores it (the insert narrows from `u16`). Not `Eq`
/// for the same reason as [`RequestLogRecord`] (`cost_usd: Option<f64>`).
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
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
    pub target_kind: Option<String>,
    pub provider_credential_id: Option<String>,
    pub model: Option<String>,
    pub upstream_model: Option<String>,
    pub upstream_transport: Option<String>,
    pub profile_revision: Option<String>,
    pub reasoning_effort: Option<String>,
    pub service_tier: Option<String>,
    pub transport: Option<String>,
    pub ttft_ms: Option<i64>,
    pub total_tokens: Option<i64>,
    pub cached_tokens: Option<i64>,
    pub subagent: Option<String>,
    pub request_id: Option<String>,
    pub session_key: Option<String>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cached_input_tokens: Option<i64>,
    /// Input tokens written into a provider cache. This is independent evidence from cache reads.
    pub cache_write_input_tokens: Option<i64>,
    pub reasoning_tokens: Option<i64>,
    /// The upstream `usage.total_tokens` value. Unlike the older `total_tokens` compatibility
    /// column, this is never synthesized by PolyFlare.
    pub reported_total_tokens: Option<i64>,
    /// Usage payload contract, provenance, and evidentiary status. New terminal Responses usage is
    /// `openai_responses_v1` / `upstream_response` / `final`; migrated rows are explicitly legacy.
    pub usage_schema: Option<String>,
    pub usage_source: Option<String>,
    pub usage_status: Option<String>,
    pub orchestration_input_tokens: Option<i64>,
    pub orchestration_output_tokens: Option<i64>,
    pub orchestration_cached_input_tokens: Option<i64>,
    pub cost_usd: Option<f64>,
    pub latency_first_token_ms: Option<i64>,
    /// Bounded native stream terminal result. `None` on legacy/imported rows.
    pub protocol_outcome: Option<String>,
    /// Legacy codex-lb's bounded text outcome (`success` | `error`). Native PolyFlare rows use a
    /// real HTTP `status` and leave this null.
    pub outcome: Option<String>,
    /// Legacy codex-lb's bounded machine error code, when its `outcome` is `error`.
    pub error_code: Option<String>,
}

impl RequestLogRow {
    /// API token total with explicit evidence precedence. `None` means neither an authoritative
    /// total nor a complete input/output pair exists.
    pub fn api_total_tokens(&self) -> Option<i64> {
        self.reported_total_tokens
            .filter(|value| *value >= 0)
            .or(self.total_tokens)
            .filter(|value| *value >= 0)
            .or_else(|| {
                self.input_tokens
                    .zip(self.output_tokens)
                    .map(|(input, output)| input.max(0).saturating_add(output.max(0)))
            })
    }

    pub fn uncached_input_tokens(&self) -> Option<i64> {
        self.input_tokens.map(|input| {
            let input = input.max(0);
            let cached = self
                .cached_input_tokens
                .or(self.cached_tokens)
                .unwrap_or(0)
                .clamp(0, input);
            input.saturating_sub(cached)
        })
    }

    pub fn visible_output_tokens(&self) -> Option<i64> {
        self.output_tokens.map(|output| {
            output
                .max(0)
                .saturating_sub(self.reasoning_tokens.unwrap_or(0).clamp(0, output.max(0)))
        })
    }

    pub fn effective_tokens(&self) -> Option<i64> {
        self.uncached_input_tokens()
            .zip(self.output_tokens)
            .map(|(input, output)| input.saturating_add(output.max(0)))
    }
}

/// Dashboard filter set for [`RequestLogRepo::page`]. Every field is optional; an unset field
/// applies no filter. `status_class` is not a raw column value: native rows classify by HTTP
/// status, imported `status = 0` rows by their bounded `outcome`, and any other value (including
/// `"all"` or unset) applies no status filter. `provider` accepts comma-separated values; `model`
/// selects every logical non-backend provider, and backend classification includes historical
/// normalized backend paths whose stored provider predates `chatgpt_backend`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RequestsFilter {
    pub request_id: Option<String>,
    pub session_key: Option<String>,
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
    pub effective_tokens: i64,
    pub cache_write_input_tokens: i64,
    pub orchestration_tokens: i64,
    pub orchestration_cached_tokens: i64,
}

/// Lifetime content-free request totals for one built-in account.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AccountRequestTotals {
    pub request_count: i64,
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
    pub effective_tokens: i64,
    pub cache_write_input_tokens: i64,
    pub orchestration_tokens: i64,
    pub orchestration_cached_tokens: i64,
}

/// The shared metric set for the Reports/Analytics endpoints (`GET /api/reports*`), computed by
/// [`RequestLogRepo::reports_totals`], [`RequestLogRepo::reports_series`], and
/// [`RequestLogRepo::reports_breakdown`] via the SAME underlying SELECT list (see
/// `RequestLogRepo::reports_metric_select_list`) — only the WHERE/GROUP BY differ between the
/// three. A superset of [`RequestAggregate`]/[`RequestBucket`]'s fields (adds
/// cost/cached/reasoning-token/TTFT metrics on top of the overview's request/error/latency/token
/// set); content-free like every other read surface in this module: numbers only, never row
/// content.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ReportMetrics {
    pub requests: i64,
    pub errors: i64,
    pub cost_usd: f64,
    pub tokens: i64,
    pub input_tokens: i64,
    pub cached_tokens: i64,
    pub cache_write_tokens: i64,
    pub reasoning_tokens: i64,
    pub effective_tokens: i64,
    pub orchestration_tokens: i64,
    pub orchestration_cached_tokens: i64,
    pub avg_duration_ms: f64,
    pub avg_ttft_ms: f64,
    /// `COUNT` of rows with a non-NULL `latency_first_token_ms` — the denominator `AVG` silently
    /// uses to compute `avg_ttft_ms` (SQL `AVG` skips NULLs rather than treating them as zero).
    /// Exposed so a caller/chart can distinguish "no TTFT data in this window" (`0`) from "the
    /// average really is 0ms".
    pub ttft_sample_count: i64,
}

/// One time bucket of [`RequestLogRepo::reports_series`]: [`ReportMetrics`] scoped to a single
/// `bucket_secs`-wide window. Same "SQL emits only non-empty buckets" contract as
/// [`RequestBucket`]/[`RequestLogRepo::series_since`] — zero-filling the full `[since_ts, now]`
/// grid is the caller's job, not this repo's.
#[derive(Debug, Clone, PartialEq)]
pub struct ReportBucket {
    /// Bucket start, unix-epoch seconds (`(requested_at / bucket_secs) * bucket_secs`).
    pub ts: i64,
    pub metrics: ReportMetrics,
}

/// One row of [`RequestLogRepo::reports_breakdown`]: [`ReportMetrics`] scoped to one value of the
/// requested dimension (account/model/provider/operation). `key` is the dimension's raw value,
/// or `""` for NULL — `account_id`/`model` are nullable columns (see [`RequestLogRecord`]);
/// provider and the content-safe derived operation label are never NULL.
#[derive(Debug, Clone, PartialEq)]
pub struct ReportBreakdownRow {
    pub key: String,
    pub metrics: ReportMetrics,
}

/// One row of [`RequestLogRepo::recent_errors`]: content-free error identification, never a body or
/// upstream error message.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct RecentErrorRow {
    pub status: i64,
    pub provider: String,
    pub account_id: Option<String>,
    pub target_kind: Option<String>,
    pub provider_credential_id: Option<String>,
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
              account_id, target_kind, provider_credential_id, model, upstream_model, \
              upstream_transport, profile_revision, reasoning_effort, service_tier, transport, ttft_ms, \
              total_tokens, cached_tokens, subagent, request_id, session_key, input_tokens, \
              output_tokens, cached_input_tokens, reasoning_tokens, orchestration_input_tokens, \
              orchestration_output_tokens, orchestration_cached_input_tokens, cost_usd, \
              latency_first_token_ms, protocol_outcome) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \
                     ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(record.requested_at)
        .bind(&record.provider)
        .bind(&record.method)
        .bind(&record.path)
        .bind(record.aliased)
        .bind(i64::from(record.status))
        .bind(record.duration_ms)
        .bind(&record.account_id)
        .bind(&record.target_kind)
        .bind(&record.provider_credential_id)
        .bind(&record.model)
        .bind(&record.upstream_model)
        .bind(&record.upstream_transport)
        .bind(&record.profile_revision)
        .bind(&record.reasoning_effort)
        .bind(&record.service_tier)
        .bind(&record.transport)
        .bind(record.ttft_ms)
        .bind(record.total_tokens)
        .bind(record.cached_tokens)
        .bind(&record.subagent)
        .bind(&record.request_id)
        .bind(&record.session_key)
        .bind(record.input_tokens)
        .bind(record.output_tokens)
        .bind(record.cached_input_tokens)
        .bind(record.reasoning_tokens)
        .bind(record.orchestration_input_tokens)
        .bind(record.orchestration_output_tokens)
        .bind(record.orchestration_cached_input_tokens)
        .bind(record.cost_usd)
        .bind(record.latency_first_token_ms)
        .bind(record.protocol_outcome.map(RequestProtocolOutcome::as_str))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Fill in terminal usage, cost, and timing on an already-inserted row, correlated by
    /// `request_id` — the stream wrapper's post-completion usage backfill (streaming responses
    /// only know final token/cost counts once the stream ends, well after [`Self::insert`] already
    /// wrote the row). Does NOT bump any store generation: `request_log` is a history table, not
    /// the account cache the server re-reads on generation bumps. A no-op (still `Ok`) if no row
    /// matches `request_id` — this is called fire-and-forget off the response path, same as
    /// `insert`, so a miss must never surface as an error.
    ///
    /// `duration_ms` (live-row-tps-basis fix): the stream wrapper's true end-to-end duration,
    /// measured from the SAME clock origin as `latency_first_token_ms` (the route's own
    /// `Instant`). This OVERWRITES the row's `duration_ms` (set at insert time to the much
    /// smaller route+setup-only duration, measured before the body streamed) with the true
    /// total — the two must share an origin for `derive_tps` in `read_api.rs` to be meaningful.
    /// `COALESCE`'d against the existing value: `None` leaves the insert's original `duration_ms`
    /// untouched. Timing can still be written when final token usage is unavailable.
    #[allow(clippy::too_many_arguments)]
    pub async fn update_usage(
        &self,
        request_id: &str,
        input_tokens: Option<i64>,
        output_tokens: Option<i64>,
        cached_input_tokens: Option<i64>,
        cache_write_input_tokens: Option<i64>,
        reasoning_tokens: Option<i64>,
        reported_total_tokens: Option<i64>,
        orchestration_input_tokens: Option<i64>,
        orchestration_output_tokens: Option<i64>,
        orchestration_cached_input_tokens: Option<i64>,
        cost_usd: Option<f64>,
        latency_first_token_ms: Option<i64>,
        duration_ms: Option<i64>,
        protocol_outcome: Option<RequestProtocolOutcome>,
    ) -> Result<(), StoreError> {
        let has_usage = input_tokens.is_some()
            || output_tokens.is_some()
            || cached_input_tokens.is_some()
            || cache_write_input_tokens.is_some()
            || reasoning_tokens.is_some()
            || reported_total_tokens.is_some()
            || orchestration_input_tokens.is_some()
            || orchestration_output_tokens.is_some()
            || orchestration_cached_input_tokens.is_some();
        sqlx::query(
            "UPDATE request_log SET input_tokens=?, output_tokens=?, cached_input_tokens=?, \
             cache_write_input_tokens=?, reasoning_tokens=?, reported_total_tokens=?, \
             usage_schema = CASE WHEN ? THEN 'openai_responses_v1' ELSE usage_schema END, \
             usage_source = CASE WHEN ? THEN 'upstream_response' ELSE usage_source END, \
             usage_status = CASE WHEN ? THEN 'final' ELSE usage_status END, \
             orchestration_input_tokens=?, orchestration_output_tokens=?, \
             orchestration_cached_input_tokens=?, cost_usd=?, latency_first_token_ms=?, \
             duration_ms = COALESCE(?, duration_ms), \
             protocol_outcome = COALESCE(?, protocol_outcome) WHERE request_id=?",
        )
        .bind(input_tokens)
        .bind(output_tokens)
        .bind(cached_input_tokens)
        .bind(cache_write_input_tokens)
        .bind(reasoning_tokens)
        .bind(reported_total_tokens)
        .bind(has_usage)
        .bind(has_usage)
        .bind(has_usage)
        .bind(orchestration_input_tokens)
        .bind(orchestration_output_tokens)
        .bind(orchestration_cached_input_tokens)
        .bind(cost_usd)
        .bind(latency_first_token_ms)
        .bind(duration_ms)
        .bind(protocol_outcome.map(RequestProtocolOutcome::as_str))
        .bind(request_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// The newest `limit` rows, `offset`-paginated, newest-first — the dashboard list query.
    pub async fn list(&self, limit: i64, offset: i64) -> Result<Vec<RequestLogRow>, StoreError> {
        let rows = sqlx::query_as::<_, RequestLogRow>(
            "SELECT id, requested_at, provider, method, path, aliased, status, duration_ms, \
             account_id, target_kind, provider_credential_id, model, upstream_model, \
             upstream_transport, profile_revision, reasoning_effort, service_tier, transport, ttft_ms, \
             total_tokens, cached_tokens, subagent, request_id, session_key, input_tokens, output_tokens, \
             cached_input_tokens, cache_write_input_tokens, reasoning_tokens, \
             reported_total_tokens, usage_schema, usage_source, usage_status, orchestration_input_tokens, \
             orchestration_output_tokens, orchestration_cached_input_tokens, cost_usd, latency_first_token_ms, \
             outcome, error_code, protocol_outcome \
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

    /// Aggregate one account's lifetime request count and API-token total entirely in SQLite.
    /// This avoids materializing an unbounded request history for the account-detail dashboard.
    pub async fn account_totals(
        &self,
        account_id: &str,
    ) -> Result<AccountRequestTotals, StoreError> {
        let row: (i64, i64) = sqlx::query_as(&format!(
            "SELECT COUNT(*), COALESCE(SUM({API_TOTAL_SQL}), 0) \
             FROM request_log WHERE account_id = ?"
        ))
        .bind(account_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(AccountRequestTotals {
            request_count: row.0,
            total_tokens: row.1,
        })
    }

    /// Request counts per account at/after `since_ts`, returned in one grouped query.
    pub async fn account_counts_since(
        &self,
        since_ts: i64,
    ) -> Result<HashMap<String, i64>, StoreError> {
        let rows = sqlx::query_as::<_, (String, i64)>(
            "SELECT account_id, COUNT(*) \
             FROM request_log \
             WHERE account_id IS NOT NULL AND requested_at >= ? \
             GROUP BY account_id",
        )
        .bind(since_ts)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().collect())
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
             account_id, target_kind, provider_credential_id, model, upstream_model, \
             upstream_transport, profile_revision, reasoning_effort, service_tier, transport, ttft_ms, \
             total_tokens, cached_tokens, subagent, request_id, session_key, input_tokens, output_tokens, \
             cached_input_tokens, cache_write_input_tokens, reasoning_tokens, \
             reported_total_tokens, usage_schema, usage_source, usage_status, orchestration_input_tokens, \
             orchestration_output_tokens, orchestration_cached_input_tokens, cost_usd, latency_first_token_ms, \
             outcome, error_code, protocol_outcome \
             FROM request_log",
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
    /// status-class rule as [`RequestsFilter::status_class`] (native HTTP plus imported outcome;
    /// 3xx and unknown status-0 rows count toward `total` only).
    /// `avg_latency_ms`/`total_tokens` default to `0.0`/`0` when no row matches. API total prefers
    /// upstream `reported_total_tokens`, then the legacy compatibility total, then a complete
    /// input/output pair. Effective usage is the separate Codex measure: uncached input + output.
    pub async fn aggregate_since(&self, since_ts: i64) -> Result<RequestAggregate, StoreError> {
        let row: (i64, i64, i64, f64, i64, i64, i64, i64, i64) =
            sqlx::query_as(&format!(
            "SELECT COUNT(*), \
                    COALESCE(SUM(CASE WHEN {success} THEN 1 ELSE 0 END), 0), \
                    COALESCE(SUM(CASE WHEN {error} THEN 1 ELSE 0 END), 0), \
                    COALESCE(AVG(duration_ms), 0.0), \
                    COALESCE(SUM({api_total}), 0), \
                    COALESCE(SUM({effective}), 0), \
                    COALESCE(SUM(MAX(COALESCE(cache_write_input_tokens, 0), 0)), 0), \
                    COALESCE(SUM(COALESCE(orchestration_input_tokens, 0) + COALESCE(orchestration_output_tokens, 0)), 0), \
                    COALESCE(SUM(COALESCE(orchestration_cached_input_tokens, 0)), 0) \
             FROM request_log WHERE requested_at >= ?",
            success = SUCCESS_SQL,
            error = ERROR_SQL,
            api_total = API_TOTAL_SQL,
            effective = EFFECTIVE_TOKENS_SQL,
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
            effective_tokens: row.5,
            cache_write_input_tokens: row.6,
            orchestration_tokens: row.7,
            orchestration_cached_tokens: row.8,
        })
    }

    /// Content-free request-volume time series for the dashboard overview's chart
    /// (`GET /api/overview/series`): one row per `bucket_secs`-wide bucket at/after `since_ts`,
    /// ascending by bucket start (`ts`). Bucketing is done in SQL via integer-division grouping
    /// (`(requested_at / bucket_secs) * bucket_secs`), not by fetching rows into Rust. `errors`
    /// classifies by the SAME native-HTTP/imported-outcome rule [`Self::aggregate_since`] uses;
    /// `avg_latency_ms`
    /// / `total_tokens` are per-bucket `AVG`/`SUM`, never null (a bucket only exists here because it
    /// has >= 1 row). `total_tokens` falls back per row the same way [`Self::aggregate_since`]'s does
    /// (a non-negative reported total, then a non-negative compatibility total, then a complete
    /// `input_tokens + output_tokens` pair; never `reasoning_tokens`).
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
        let rows: Vec<RequestBucketSqlRow> = sqlx::query_as(&format!(
            "SELECT (requested_at / ?) * ? AS bucket_ts, \
                    COUNT(*), \
                    COALESCE(SUM(CASE WHEN {error} THEN 1 ELSE 0 END), 0), \
                    COALESCE(AVG(duration_ms), 0.0), \
                    COALESCE(SUM({api_total}), 0), \
                    COALESCE(SUM({effective}), 0), \
                    COALESCE(SUM(MAX(COALESCE(cache_write_input_tokens, 0), 0)), 0), \
                    COALESCE(SUM(COALESCE(orchestration_input_tokens, 0) + COALESCE(orchestration_output_tokens, 0)), 0), \
                    COALESCE(SUM(COALESCE(orchestration_cached_input_tokens, 0)), 0) \
             FROM request_log WHERE requested_at >= ? \
             GROUP BY bucket_ts ORDER BY bucket_ts ASC",
            error = ERROR_SQL,
            api_total = API_TOTAL_SQL,
            effective = EFFECTIVE_TOKENS_SQL,
        ))
        .bind(bucket_secs)
        .bind(bucket_secs)
        .bind(since_ts)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    ts,
                    requests,
                    errors,
                    avg_latency_ms,
                    total_tokens,
                    effective_tokens,
                    cache_write_input_tokens,
                    orchestration_tokens,
                    orchestration_cached_tokens,
                )| RequestBucket {
                    ts,
                    requests,
                    errors,
                    avg_latency_ms,
                    total_tokens,
                    effective_tokens,
                    cache_write_input_tokens,
                    orchestration_tokens,
                    orchestration_cached_tokens,
                },
            )
            .collect())
    }

    /// The canonical metric SELECT list shared by [`Self::reports_totals`]/
    /// [`Self::reports_series`]/[`Self::reports_breakdown`] — errors classified by the SAME
    /// native-HTTP/imported-outcome rule as `aggregate_since`/`series_since`, and
    /// `tokens` uses the same authoritative/compatibility precedence as overview totals. Cached
    /// input and reasoning output remain subset dimensions and are never added to API total.
    /// Effective tokens are uncached input + output. Column
    /// order here MUST match the tuple type each caller destructures and
    /// [`Self::report_metrics_from_row`].
    fn reports_metric_select_list() -> String {
        format!(
            "COUNT(*), \
             COALESCE(SUM(CASE WHEN {error} THEN 1 ELSE 0 END), 0), \
             COALESCE(SUM(cost_usd), 0.0), \
             COALESCE(SUM({api_total}), 0), \
             COALESCE(SUM(MAX(COALESCE(input_tokens, 0), 0)), 0), \
             COALESCE(SUM(COALESCE(cached_tokens, cached_input_tokens, 0)), 0), \
             COALESCE(SUM(MAX(COALESCE(cache_write_input_tokens, 0), 0)), 0), \
             COALESCE(SUM(COALESCE(reasoning_tokens, 0)), 0), \
             COALESCE(SUM({effective}), 0), \
             COALESCE(SUM(COALESCE(orchestration_input_tokens, 0) + COALESCE(orchestration_output_tokens, 0)), 0), \
             COALESCE(SUM(COALESCE(orchestration_cached_input_tokens, 0)), 0), \
             COALESCE(AVG(duration_ms), 0.0), \
             COALESCE(AVG(latency_first_token_ms), 0.0), \
             COUNT(latency_first_token_ms)",
            error = ERROR_SQL,
            api_total = API_TOTAL_SQL,
            effective = EFFECTIVE_TOKENS_SQL,
        )
    }

    /// Map one row of [`Self::reports_metric_select_list`] (in that exact order) to
    /// [`ReportMetrics`] — the single place all three `reports_*` methods share this conversion, so
    /// the column order only has to be kept in sync in one place.
    fn report_metrics_from_row(row: ReportMetricsSqlRow) -> ReportMetrics {
        ReportMetrics {
            requests: row.0,
            errors: row.1,
            cost_usd: row.2,
            tokens: row.3,
            input_tokens: row.4,
            cached_tokens: row.5,
            cache_write_tokens: row.6,
            reasoning_tokens: row.7,
            effective_tokens: row.8,
            orchestration_tokens: row.9,
            orchestration_cached_tokens: row.10,
            avg_duration_ms: row.11,
            avg_ttft_ms: row.12,
            ttft_sample_count: row.13,
        }
    }

    /// Content-free [`ReportMetrics`] rollup over `request_log` rows at/after `since_ts`,
    /// optionally narrowed to one `provider` — the Reports/Analytics `GET /api/reports` composite
    /// endpoint's top-line KPI tile. Same shared metric SELECT list as [`Self::reports_series`]/
    /// [`Self::reports_breakdown`] (see [`Self::reports_metric_select_list`]); only the WHERE
    /// differs. An empty window/filter still returns a zeroed [`ReportMetrics`] (every metric is
    /// `COALESCE`'d), never an error — same "empty rolls up to zero, not null" contract as
    /// [`Self::aggregate_since`].
    pub async fn reports_totals(
        &self,
        since_ts: i64,
        provider: Option<&str>,
    ) -> Result<ReportMetrics, StoreError> {
        let sql = format!(
            "SELECT {} FROM request_log WHERE requested_at >= ?{}",
            Self::reports_metric_select_list(),
            if provider.is_some() {
                " AND provider = ?"
            } else {
                ""
            }
        );
        let mut query = sqlx::query_as::<_, ReportMetricsSqlRow>(&sql).bind(since_ts);
        if let Some(p) = provider {
            query = query.bind(p);
        }
        let row = query.fetch_one(&self.pool).await?;
        Ok(Self::report_metrics_from_row(row))
    }

    /// Content-free [`ReportBucket`] time series over `request_log` rows at/after `since_ts`,
    /// bucketed by `bucket_secs` (same integer-division grouping as [`Self::series_since`];
    /// clamped to a minimum of 1 to avoid a SQL divide-by-zero), optionally narrowed to one
    /// `provider` — the Reports/Analytics endpoint's cost/token/latency-over-time chart. SQL only
    /// emits buckets that have >= 1 matching row; zero-filling the `[since_ts, now]` grid is the
    /// caller's job, same contract as [`Self::series_since`].
    pub async fn reports_series(
        &self,
        since_ts: i64,
        bucket_secs: i64,
        provider: Option<&str>,
    ) -> Result<Vec<ReportBucket>, StoreError> {
        let bucket_secs = bucket_secs.max(1);
        let sql = format!(
            "SELECT (requested_at / ?) * ? AS bucket_ts, {} \
             FROM request_log WHERE requested_at >= ?{} \
             GROUP BY bucket_ts ORDER BY bucket_ts ASC",
            Self::reports_metric_select_list(),
            if provider.is_some() {
                " AND provider = ?"
            } else {
                ""
            }
        );
        let mut query = sqlx::query_as::<_, ReportBucketSqlRow>(&sql)
            .bind(bucket_secs)
            .bind(bucket_secs)
            .bind(since_ts);
        if let Some(p) = provider {
            query = query.bind(p);
        }
        let rows = query.fetch_all(&self.pool).await?;
        Ok(rows
            .into_iter()
            .map(|row| ReportBucket {
                ts: row.0,
                metrics: Self::report_metrics_from_row((
                    row.1, row.2, row.3, row.4, row.5, row.6, row.7, row.8, row.9, row.10, row.11,
                    row.12, row.13, row.14,
                )),
            })
            .collect())
    }

    /// Content-free [`ReportBreakdownRow`] rollup grouped by one dimension (`account`/`model`/
    /// `provider`/`operation`, mapping to the account-or-credential target/model/provider or a
    /// bounded label derived from the normalized route) over `request_log` rows
    /// at/after `since_ts`, optionally narrowed to one `provider` — the Reports/Analytics
    /// endpoint's per-account/per-model/per-provider cost breakdown table. Rows are ordered by
    /// summed `cost_usd` descending (biggest spenders first). `dimension` values other than
    /// `"account"`/`"model"`/`"provider"`/`"operation"` default to `"model"`; the handler is
    /// expected to validate `dimension` before calling this, so this fallback is defense-in-depth,
    /// not the primary validation. NULL dimension values (`account_id`/`model` are nullable — see
    /// [`RequestLogRecord`]) collapse into a single `""`-keyed row via `COALESCE(col, '')`.
    pub async fn reports_breakdown(
        &self,
        since_ts: i64,
        dimension: &str,
        provider: Option<&str>,
    ) -> Result<Vec<ReportBreakdownRow>, StoreError> {
        let expression = match dimension {
            "account" => "COALESCE(provider_credential_id, account_id, '')",
            "provider" => "provider",
            "operation" => {
                "CASE \
                    WHEN path LIKE 'chatgpt_backend_synthetic_%' THEN 'Synthetic usage' \
                    WHEN path LIKE 'chatgpt_backend_passthrough_%' THEN 'Backend passthrough' \
                    ELSE 'Model response' \
                 END"
            }
            _ => "COALESCE(model, '')",
        };
        let sql = format!(
            "SELECT {expression} AS dim_key, {metrics} \
             FROM request_log WHERE requested_at >= ?{provider_clause} \
             GROUP BY {expression} ORDER BY COALESCE(SUM(cost_usd), 0.0) DESC",
            expression = expression,
            metrics = Self::reports_metric_select_list(),
            provider_clause = if provider.is_some() {
                " AND provider = ?"
            } else {
                ""
            }
        );
        let mut query = sqlx::query_as::<_, ReportBreakdownSqlRow>(&sql).bind(since_ts);
        if let Some(p) = provider {
            query = query.bind(p);
        }
        let rows = query.fetch_all(&self.pool).await?;
        Ok(rows
            .into_iter()
            .map(|row| ReportBreakdownRow {
                key: row.0,
                metrics: Self::report_metrics_from_row((
                    row.1, row.2, row.3, row.4, row.5, row.6, row.7, row.8, row.9, row.10, row.11,
                    row.12, row.13, row.14,
                )),
            })
            .collect())
    }

    /// The newest `limit` content-free error rows, using the same native-HTTP/imported-outcome
    /// classification as every aggregate and filter above.
    pub async fn recent_errors(&self, limit: i64) -> Result<Vec<RecentErrorRow>, StoreError> {
        let rows = sqlx::query_as::<_, RecentErrorRow>(&format!(
            "SELECT status, provider, account_id, target_kind, provider_credential_id, \
                    error_code, requested_at FROM request_log \
             WHERE {error} ORDER BY requested_at DESC, id DESC LIMIT ?",
            error = ERROR_SQL,
        ))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Delete `request_log` rows with `requested_at < cutoff`, in batches of `batch_size`, looping
    /// until a batch deletes fewer than `batch_size` rows (or zero). Returns the total number of
    /// rows deleted across all batches. Content-free: only a count crosses this boundary, never row
    /// contents, and nothing is logged here.
    ///
    /// # Batching approach
    /// Each batch runs `DELETE FROM request_log WHERE rowid IN (SELECT rowid FROM request_log WHERE
    /// requested_at < ?1 LIMIT ?2)` as its own statement (own implicit transaction), rather than one
    /// unbounded `DELETE ... WHERE requested_at < ?`, so a large prune never holds SQLite's single
    /// writer lock for longer than one batch. The `rowid IN (SELECT ... LIMIT)` form is used instead
    /// of `DELETE ... LIMIT` directly because the latter requires SQLite's
    /// `SQLITE_ENABLE_UPDATE_DELETE_LIMIT` compile flag, which sqlx's bundled SQLite build may lack;
    /// the subselect form is portable standard SQL.
    ///
    /// # `batch_size <= 0` guard
    /// Treated as a no-op (returns `0` immediately, deletes nothing) rather than looping. This is
    /// deliberately defensive: today's only caller always passes a positive batch size (e.g.
    /// `10_000`), but binding a non-positive value into SQLite's `LIMIT` means "no limit" (SQLite
    /// treats `LIMIT <= -1` as unbounded, and `LIMIT 0` matches zero rows some engines but SQLite's
    /// own `LIMIT 0` is well-defined as "zero rows" — the ambiguity across a negative batch size is
    /// what's dangerous), and would either turn one "batch" into an unbounded delete or, combined
    /// with the "loop until a batch affects `< batch_size` rows" termination check, could underflow /
    /// never terminate. Rejecting non-positive `batch_size` up front avoids relying on that SQLite
    /// edge-case behavior entirely.
    pub async fn prune_older_than(&self, cutoff: i64, batch_size: i64) -> Result<u64, StoreError> {
        if batch_size <= 0 {
            return Ok(0);
        }

        let mut total: u64 = 0;
        loop {
            let result = sqlx::query(
                "DELETE FROM request_log WHERE rowid IN \
                 (SELECT rowid FROM request_log WHERE requested_at < ?1 LIMIT ?2)",
            )
            .bind(cutoff)
            .bind(batch_size)
            .execute(&self.pool)
            .await?;

            let affected = result.rows_affected();
            total += affected;

            if affected < batch_size as u64 {
                break;
            }
        }

        Ok(total)
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

        if let Some(v) = &filter.request_id {
            sep(qb, &mut first);
            qb.push("request_id = ");
            qb.push_bind(v.clone());
        }
        if let Some(v) = &filter.session_key {
            sep(qb, &mut first);
            qb.push("session_key = ");
            qb.push_bind(v.clone());
        }
        if let Some(v) = &filter.account {
            sep(qb, &mut first);
            qb.push("(account_id = ");
            qb.push_bind(v.clone());
            qb.push(" OR provider_credential_id = ");
            qb.push_bind(v.clone());
            qb.push(")");
        }
        if let Some(v) = &filter.provider {
            let providers: Vec<&str> = v
                .split(',')
                .map(str::trim)
                .filter(|provider| !provider.is_empty())
                .collect();
            let includes_all = providers.contains(&"all")
                || (providers.contains(&"model") && providers.contains(&"chatgpt_backend"));
            if !providers.is_empty() && !includes_all {
                sep(qb, &mut first);
                qb.push("(");
                for (index, provider) in providers.iter().enumerate() {
                    if index > 0 {
                        qb.push(" OR ");
                    }
                    match *provider {
                        "model" => {
                            qb.push("NOT ");
                            qb.push(BACKEND_REQUEST_SQL);
                        }
                        "chatgpt_backend" | "backend" => {
                            qb.push(BACKEND_REQUEST_SQL);
                        }
                        "none" => {
                            qb.push("0 = 1");
                        }
                        provider => {
                            qb.push("(provider = ");
                            qb.push_bind(if provider == "claude" {
                                "anthropic".to_string()
                            } else {
                                provider.to_string()
                            });
                            qb.push(" AND NOT ");
                            qb.push(BACKEND_REQUEST_SQL);
                            qb.push(")");
                        }
                    }
                }
                qb.push(")");
            }
        }
        match filter.status_class.as_deref() {
            Some("success") => {
                sep(qb, &mut first);
                qb.push(SUCCESS_SQL);
            }
            Some("error") => {
                sep(qb, &mut first);
                qb.push(ERROR_SQL);
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
            target_kind: Some("account".into()),
            provider_credential_id: None,
            model: Some("gpt-5.6-sol".into()),
            upstream_model: None,
            upstream_transport: None,
            profile_revision: None,
            reasoning_effort: Some("high".into()),
            service_tier: Some("priority".into()),
            transport: Some("http".into()),
            ttft_ms: Some(700),
            total_tokens: Some(3204),
            cached_tokens: Some(1100),
            subagent: Some("review".into()),
            request_id: None,
            session_key: None,
            input_tokens: None,
            output_tokens: None,
            cached_input_tokens: None,
            reasoning_tokens: None,
            orchestration_input_tokens: None,
            orchestration_output_tokens: None,
            orchestration_cached_input_tokens: None,
            cost_usd: None,
            latency_first_token_ms: None,
            protocol_outcome: None,
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
        assert_eq!(rows[0].subagent.as_deref(), Some("review"));
    }

    /// `page` (the dashboard's filtered/paginated query) round-trips `subagent` too — a SEPARATE
    /// `QueryBuilder` SELECT from `list`'s, so this guards against the two SELECT column lists
    /// drifting out of sync (a `FromRow` column-count mismatch panics at runtime, not compile time).
    #[tokio::test]
    async fn page_round_trips_subagent_and_session_key() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let repo = store.request_log();

        let mut rec = rec("codex", "/responses", 200, Some("gpt-5.6-sol"));
        rec.subagent = Some("compact".into());
        rec.session_key = Some("hashed-session-a".into());
        repo.insert(&rec).await.unwrap();

        let (rows, total) = repo
            .page(
                &RequestsFilter {
                    session_key: Some("hashed-session-a".into()),
                    ..Default::default()
                },
                10,
                0,
            )
            .await
            .unwrap();
        assert_eq!(total, 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].subagent.as_deref(), Some("compact"));
        assert_eq!(rows[0].session_key.as_deref(), Some("hashed-session-a"));
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
            target_kind: Some("account".into()),
            provider_credential_id: None,
            model: model.map(String::from),
            upstream_model: None,
            upstream_transport: None,
            profile_revision: None,
            reasoning_effort: None,
            service_tier: None,
            transport: Some("http".into()),
            ttft_ms: Some(200),
            total_tokens: Some(500),
            cached_tokens: None,
            subagent: None,
            request_id: None,
            session_key: None,
            input_tokens: None,
            output_tokens: None,
            cached_input_tokens: None,
            reasoning_tokens: None,
            orchestration_input_tokens: None,
            orchestration_output_tokens: None,
            orchestration_cached_input_tokens: None,
            cost_usd: None,
            latency_first_token_ms: None,
            protocol_outcome: None,
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

    #[tokio::test]
    async fn page_provider_filter_is_multi_select_and_classifies_legacy_backend_paths() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let repo = store.request_log();

        repo.insert(&rec("codex", "/responses", 200, Some("gpt-5.6-sol")))
            .await
            .unwrap();
        repo.insert(&rec("anthropic", "/v1/messages", 200, Some("claude")))
            .await
            .unwrap();
        let mut legacy_backend = rec("codex", "chatgpt_backend_synthetic_wham/usage", 200, None);
        legacy_backend.account_id = None;
        repo.insert(&legacy_backend).await.unwrap();
        let mut current_backend = rec(
            "chatgpt_backend",
            "chatgpt_backend_passthrough_wham/remote/control/server",
            101,
            None,
        );
        current_backend.account_id = None;
        repo.insert(&current_backend).await.unwrap();

        let (model_rows, model_total) = repo
            .page(
                &RequestsFilter {
                    provider: Some("model".into()),
                    ..Default::default()
                },
                10,
                0,
            )
            .await
            .unwrap();
        assert_eq!(model_total, 2);
        assert!(model_rows
            .iter()
            .all(|row| !row.path.starts_with("chatgpt_backend_")));

        let (codex_rows, codex_total) = repo
            .page(
                &RequestsFilter {
                    provider: Some("codex".into()),
                    ..Default::default()
                },
                10,
                0,
            )
            .await
            .unwrap();
        assert_eq!(codex_total, 1);
        assert_eq!(codex_rows[0].path, "/responses");

        let (backend_rows, backend_total) = repo
            .page(
                &RequestsFilter {
                    provider: Some("chatgpt_backend".into()),
                    ..Default::default()
                },
                10,
                0,
            )
            .await
            .unwrap();
        assert_eq!(backend_total, 2);
        assert!(backend_rows
            .iter()
            .all(|row| row.path.starts_with("chatgpt_backend_")));

        let (multi_rows, multi_total) = repo
            .page(
                &RequestsFilter {
                    provider: Some("codex,anthropic".into()),
                    ..Default::default()
                },
                10,
                0,
            )
            .await
            .unwrap();
        assert_eq!(multi_total, 2);
        assert_eq!(multi_rows.len(), 2);
    }

    #[tokio::test]
    async fn page_filters_by_exact_request_correlation_id() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let repo = store.request_log();

        let mut first = rec("codex", "/responses", 200, Some("gpt-5.6-sol"));
        first.request_id = Some("request-debug-a".into());
        repo.insert(&first).await.unwrap();

        let mut second = rec("codex", "/responses", 200, Some("gpt-5.6-sol"));
        second.request_id = Some("request-debug-b".into());
        repo.insert(&second).await.unwrap();

        let filter = RequestsFilter {
            request_id: Some("request-debug-b".into()),
            ..Default::default()
        };
        let (rows, total) = repo.page(&filter, 10, 0).await.unwrap();

        assert_eq!(total, 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].request_id.as_deref(), Some("request-debug-b"));
    }

    #[tokio::test]
    async fn account_filter_matches_built_in_accounts_and_custom_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let repo = store.request_log();

        let mut built_in = rec("codex", "/responses", 200, Some("gpt-5.6-sol"));
        built_in.account_id = Some("account-a".into());
        built_in.target_kind = Some("account".into());
        repo.insert(&built_in).await.unwrap();

        let mut custom = rec("sakana", "/responses", 200, Some("fugu-ultra"));
        custom.target_kind = Some("credential".into());
        custom.provider_credential_id = Some("credential-fugu".into());
        repo.insert(&custom).await.unwrap();

        let (account_rows, account_total) = repo
            .page(
                &RequestsFilter {
                    account: Some("account-a".into()),
                    ..Default::default()
                },
                10,
                0,
            )
            .await
            .unwrap();
        assert_eq!(account_total, 1);
        assert_eq!(account_rows[0].provider, "codex");

        let (credential_rows, credential_total) = repo
            .page(
                &RequestsFilter {
                    account: Some("credential-fugu".into()),
                    ..Default::default()
                },
                10,
                0,
            )
            .await
            .unwrap();
        assert_eq!(credential_total, 1);
        assert_eq!(credential_rows[0].provider, "sakana");

        let breakdown = repo.reports_breakdown(0, "account", None).await.unwrap();
        assert!(breakdown.iter().any(|row| row.key == "account-a"));
        assert!(
            breakdown.iter().any(|row| row.key == "credential-fugu"),
            "the target breakdown must not collapse custom credentials into an empty account row"
        );
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
            target_kind: None,
            provider_credential_id: None,
            model: None,
            upstream_model: None,
            upstream_transport: None,
            profile_revision: None,
            reasoning_effort: None,
            service_tier: None,
            transport: None,
            ttft_ms: None,
            total_tokens,
            cached_tokens: None,
            subagent: None,
            request_id: None,
            session_key: None,
            input_tokens: None,
            output_tokens: None,
            cached_input_tokens: None,
            reasoning_tokens: None,
            orchestration_input_tokens: None,
            orchestration_output_tokens: None,
            orchestration_cached_input_tokens: None,
            cost_usd: None,
            latency_first_token_ms: None,
            protocol_outcome: None,
        }
    }

    /// Like [`row_at`], but shaped like an imported/backfilled row: `total_tokens` is `None` and
    /// only the 0005 `input_tokens`/`output_tokens` columns are populated — the case
    /// `aggregate_since`/`series_since` must fall back on (Task 7). `reasoning_tokens` is also set
    /// (to a value that would double-count the total if wrongly added) so a wrong implementation
    /// that adds it in shows up as a wrong sum, not a coincidentally-passing test.
    fn row_at_split_tokens(
        requested_at: i64,
        status: u16,
        duration_ms: i64,
        input_tokens: i64,
        output_tokens: i64,
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
            target_kind: None,
            provider_credential_id: None,
            model: None,
            upstream_model: None,
            upstream_transport: None,
            profile_revision: None,
            reasoning_effort: None,
            service_tier: None,
            transport: None,
            ttft_ms: None,
            total_tokens: None,
            cached_tokens: None,
            subagent: None,
            request_id: None,
            session_key: None,
            input_tokens: Some(input_tokens),
            output_tokens: Some(output_tokens),
            cached_input_tokens: None,
            reasoning_tokens: Some(50), // subset of output_tokens — must NOT be added to the sum
            orchestration_input_tokens: None,
            orchestration_output_tokens: None,
            orchestration_cached_input_tokens: None,
            cost_usd: None,
            latency_first_token_ms: None,
            protocol_outcome: None,
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
        // Task 7: an imported-shaped row (total_tokens=None, input/output set) within the window —
        // duration_ms=200 keeps avg_latency_ms's numerator/denominator arithmetic unchanged below.
        repo.insert(&row_at_split_tokens(205, 200, 200, 400, 100))
            .await
            .unwrap();

        let agg = repo.aggregate_since(200).await.unwrap();
        assert_eq!(agg.total, 3, "the ts=50 row is outside the window");
        assert_eq!(agg.success, 2);
        assert_eq!(agg.error, 1);
        assert_eq!(
            agg.total_tokens, 3500,
            "1000 + 2000 + (400 input + 100 output), NOT + the row's reasoning_tokens"
        );
        assert_eq!(agg.avg_latency_ms, 200.0, "(100 + 300 + 200) / 3");

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
        repo.insert(&row_at(50, 200, Some(500), 100)).await.unwrap();
        // Bucket [100, 200): two rows, one success one error, at ts=120 and ts=150, plus a third
        // (Task 7) imported-shaped row at ts=180: total_tokens=None, input/output set —
        // duration_ms=200 keeps avg_latency_ms's arithmetic unchanged below.
        repo.insert(&row_at(120, 200, Some(1000), 100))
            .await
            .unwrap();
        repo.insert(&row_at(150, 500, Some(2000), 300))
            .await
            .unwrap();
        repo.insert(&row_at_split_tokens(180, 200, 200, 700, 300))
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
        assert_eq!(buckets[1].requests, 3);
        assert_eq!(buckets[1].errors, 1);
        assert_eq!(
            buckets[1].total_tokens, 4000,
            "1000 + 2000 + (700 input + 300 output), NOT + the row's reasoning_tokens"
        );
        assert_eq!(buckets[1].avg_latency_ms, 200.0, "(100 + 300 + 200) / 3");
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

    /// A full `RequestLogRecord` shaped for the reports-aggregation tests: imported-shaped
    /// (`total_tokens`/`cached_tokens` are always `None`, so `reports_totals`/`reports_series`/
    /// `reports_breakdown` must fall back to `input_tokens+output_tokens` /
    /// `cached_input_tokens`), with `cost_usd`/`latency_first_token_ms` exposed as parameters so
    /// individual seed rows can leave them `None` per the brief ("some cost_usd, some
    /// latency_first_token_ms NULL").
    #[allow(clippy::too_many_arguments)]
    fn report_row(
        requested_at: i64,
        provider: &str,
        model: &str,
        account_id: Option<&str>,
        status: u16,
        duration_ms: i64,
        input_tokens: i64,
        output_tokens: i64,
        cached_input_tokens: i64,
        reasoning_tokens: i64,
        cost_usd: Option<f64>,
        latency_first_token_ms: Option<i64>,
    ) -> RequestLogRecord {
        RequestLogRecord {
            requested_at,
            provider: provider.into(),
            method: "POST".into(),
            path: "/responses".into(),
            aliased: false,
            status,
            duration_ms,
            account_id: account_id.map(String::from),
            target_kind: account_id.map(|_| "account".into()),
            provider_credential_id: None,
            model: Some(model.into()),
            upstream_model: None,
            upstream_transport: None,
            profile_revision: None,
            reasoning_effort: None,
            service_tier: None,
            transport: Some("http".into()),
            ttft_ms: None,
            total_tokens: None,
            cached_tokens: None,
            subagent: None,
            request_id: None,
            session_key: None,
            input_tokens: Some(input_tokens),
            output_tokens: Some(output_tokens),
            cached_input_tokens: Some(cached_input_tokens),
            reasoning_tokens: Some(reasoning_tokens),
            orchestration_input_tokens: None,
            orchestration_output_tokens: None,
            orchestration_cached_input_tokens: None,
            cost_usd,
            latency_first_token_ms,
            protocol_outcome: None,
        }
    }

    /// Seeds a fresh store with 4 rows spanning 2 buckets (`[0,100)` / `[100,200)`), 2 models
    /// (`model-a`/`model-b`), and 2 providers (`codex`/`anthropic`), orthogonally: each bucket,
    /// model, and provider has exactly 2 rows, so grouping/filtering by any one dimension is a
    /// meaningful (non-degenerate) test. Row 3 has `account_id: None` (exercises the NULL -> `""`
    /// breakdown key), and row 2 has `latency_first_token_ms: None` + row 3 has `cost_usd: None`
    /// (exercises the "AVG/SUM skip NULLs, COALESCE only fills the all-NULL/no-rows case" rule).
    /// Returns the `TempDir` alongside the repo so the backing SQLite file isn't dropped early.
    async fn seed_reports_fixture() -> (tempfile::TempDir, RequestLogRepo) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let repo = store.request_log();

        // Row 1: bucket [0,100), codex, model-a, acct-1, success.
        repo.insert(&report_row(
            10,
            "codex",
            "model-a",
            Some("acct-1"),
            200,
            100,
            100,
            50,
            20,
            10,
            Some(1.0),
            Some(40),
        ))
        .await
        .unwrap();
        // Row 2: bucket [0,100), anthropic, model-b, acct-2, error (500), NULL ttft.
        repo.insert(&report_row(
            50,
            "anthropic",
            "model-b",
            Some("acct-2"),
            500,
            300,
            200,
            100,
            30,
            20,
            Some(2.0),
            None,
        ))
        .await
        .unwrap();
        // Row 3: bucket [100,200), codex, model-b, NULL account_id, success, NULL cost_usd.
        repo.insert(&report_row(
            110,
            "codex",
            "model-b",
            None,
            200,
            200,
            300,
            150,
            40,
            30,
            None,
            Some(60),
        ))
        .await
        .unwrap();
        // Row 4: bucket [100,200), anthropic, model-a, acct-1, error (404).
        repo.insert(&report_row(
            150,
            "anthropic",
            "model-a",
            Some("acct-1"),
            404,
            400,
            400,
            200,
            50,
            40,
            Some(4.0),
            Some(80),
        ))
        .await
        .unwrap();

        (dir, repo)
    }

    /// `reports_totals` sums cost (`NULL` -> 0 via `COALESCE`), falls back tokens to
    /// `input_tokens+output_tokens` (never `+reasoning_tokens`), classifies errors by `status >=
    /// 400`, and computes `avg_ttft_ms`/`ttft_sample_count` over only the non-NULL
    /// `latency_first_token_ms` rows (row 2's NULL must be excluded from both, not treated as 0).
    /// Also asserts an empty window rolls up to a zeroed `ReportMetrics`, not null/error.
    #[tokio::test]
    async fn reports_totals_computes_cost_token_fallback_errors_and_ttft_over_the_window() {
        let (_dir, repo) = seed_reports_fixture().await;

        let totals = repo.reports_totals(0, None).await.unwrap();
        assert_eq!(totals.requests, 4);
        assert_eq!(totals.errors, 2, "row2 (500) and row4 (404) are >= 400");
        assert_eq!(totals.cost_usd, 7.0, "1.0 + 2.0 + 0.0 (row3's NULL) + 4.0");
        assert_eq!(
            totals.tokens, 1500,
            "each row's total_tokens is NULL -> input+output fallback: 150+300+450+600, \
             NOT + reasoning_tokens"
        );
        assert_eq!(
            totals.cached_tokens, 140,
            "cached_tokens is NULL on every row -> cached_input_tokens fallback: 20+30+40+50"
        );
        assert_eq!(totals.reasoning_tokens, 100, "10+20+30+40");
        assert_eq!(totals.avg_duration_ms, 250.0, "(100+300+200+400)/4");
        assert_eq!(
            totals.ttft_sample_count, 3,
            "row2's NULL latency_first_token_ms must not count as a sample"
        );
        assert_eq!(
            totals.avg_ttft_ms, 60.0,
            "(40+60+80)/3 -- row2's NULL excluded from both the sum and the count"
        );

        let empty = repo.reports_totals(1_000_000, None).await.unwrap();
        assert_eq!(
            empty,
            ReportMetrics::default(),
            "an empty window rolls up to zero, not null"
        );
    }

    /// The `provider` filter narrows `reports_totals` to only that provider's rows (row1+row3 for
    /// `codex`), and the token/cost/ttft math still applies the same fallback/NULL-skip rules
    /// within the narrowed set.
    #[tokio::test]
    async fn reports_totals_provider_filter_narrows_to_matching_rows_only() {
        let (_dir, repo) = seed_reports_fixture().await;

        let totals = repo.reports_totals(0, Some("codex")).await.unwrap();
        assert_eq!(totals.requests, 2, "only row1 and row3 are provider=codex");
        assert_eq!(totals.errors, 0);
        assert_eq!(totals.cost_usd, 1.0, "row1's 1.0 + row3's NULL-as-0");
        assert_eq!(totals.tokens, 600, "150 (row1) + 450 (row3)");
        assert_eq!(totals.ttft_sample_count, 2);
        assert_eq!(totals.avg_ttft_ms, 50.0, "(40+60)/2");
    }

    /// `reports_breakdown("model")` groups by `model`, with each group's `ReportMetrics` matching
    /// what a manual sum over that group's rows would produce, ordered by summed `cost_usd`
    /// descending (model-a's 5.0 > model-b's 2.0).
    #[tokio::test]
    async fn reports_breakdown_by_model_groups_metrics_and_orders_by_cost_desc() {
        let (_dir, repo) = seed_reports_fixture().await;

        let rows = repo.reports_breakdown(0, "model", None).await.unwrap();
        assert_eq!(rows.len(), 2);

        assert_eq!(
            rows[0].key, "model-a",
            "model-a's cost (1.0+4.0=5.0) > model-b's (2.0+0=2.0)"
        );
        assert_eq!(rows[0].metrics.requests, 2);
        assert_eq!(rows[0].metrics.errors, 1, "row4 (404)");
        assert_eq!(rows[0].metrics.cost_usd, 5.0);
        assert_eq!(rows[0].metrics.tokens, 750, "150 (row1) + 600 (row4)");
        assert_eq!(rows[0].metrics.avg_ttft_ms, 60.0, "(40+80)/2");
        assert_eq!(rows[0].metrics.ttft_sample_count, 2);

        assert_eq!(rows[1].key, "model-b");
        assert_eq!(rows[1].metrics.requests, 2);
        assert_eq!(rows[1].metrics.errors, 1, "row2 (500)");
        assert_eq!(rows[1].metrics.cost_usd, 2.0);
        assert_eq!(rows[1].metrics.tokens, 750, "300 (row2) + 450 (row3)");
        assert_eq!(
            rows[1].metrics.avg_ttft_ms, 60.0,
            "row2's NULL ttft excluded -- only row3's 60 counts"
        );
        assert_eq!(rows[1].metrics.ttft_sample_count, 1);
    }

    /// `reports_breakdown("account")` collapses row3's `NULL` `account_id` into a single
    /// `""`-keyed row via `COALESCE(account_id, '')`, alongside the two real account ids.
    #[tokio::test]
    async fn reports_breakdown_by_account_maps_null_account_id_to_empty_string_key() {
        let (_dir, repo) = seed_reports_fixture().await;

        let rows = repo.reports_breakdown(0, "account", None).await.unwrap();
        let keys: Vec<&str> = rows.iter().map(|r| r.key.as_str()).collect();
        assert!(keys.contains(&"acct-1"), "rows 1 and 4");
        assert!(keys.contains(&"acct-2"), "row 2");
        assert!(
            keys.contains(&""),
            "row3's NULL account_id must collapse to the empty-string key, not be dropped"
        );

        let empty_key_row = rows.iter().find(|r| r.key.is_empty()).unwrap();
        assert_eq!(
            empty_key_row.metrics.requests, 1,
            "only row3 has a NULL account_id"
        );
        assert_eq!(empty_key_row.metrics.cost_usd, 0.0);
    }

    #[tokio::test]
    async fn reports_breakdown_by_operation_separates_backend_traffic_from_model_responses() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let repo = store.request_log();

        let mut synthetic = report_row(
            10,
            "chatgpt_backend",
            "unused",
            None,
            200,
            10,
            0,
            0,
            0,
            0,
            None,
            None,
        );
        synthetic.path = "chatgpt_backend_synthetic_wham/usage".into();
        synthetic.model = None;
        repo.insert(&synthetic).await.unwrap();

        let mut passthrough = report_row(
            20,
            "chatgpt_backend",
            "unused",
            None,
            101,
            20,
            0,
            0,
            0,
            0,
            None,
            None,
        );
        passthrough.path = "chatgpt_backend_passthrough_wham/remote/control/server".into();
        passthrough.model = None;
        repo.insert(&passthrough).await.unwrap();

        repo.insert(&report_row(
            30,
            "codex",
            "gpt-5.6",
            Some("acct"),
            200,
            30,
            0,
            0,
            0,
            0,
            None,
            None,
        ))
        .await
        .unwrap();

        let rows = repo.reports_breakdown(0, "operation", None).await.unwrap();
        for expected in ["Synthetic usage", "Backend passthrough", "Model response"] {
            let row = rows
                .iter()
                .find(|row| row.key == expected)
                .unwrap_or_else(|| panic!("missing operation bucket {expected}: {rows:?}"));
            assert_eq!(row.metrics.requests, 1);
        }
    }

    /// An unrecognized `dimension` value defaults to grouping by `model` (defense-in-depth; the
    /// handler is expected to validate `dimension` before calling this).
    #[tokio::test]
    async fn reports_breakdown_unknown_dimension_defaults_to_model() {
        let (_dir, repo) = seed_reports_fixture().await;

        let by_model = repo.reports_breakdown(0, "model", None).await.unwrap();
        let by_bogus = repo.reports_breakdown(0, "nonsense", None).await.unwrap();
        assert_eq!(
            by_bogus, by_model,
            "an unrecognized dimension must default to grouping by model"
        );
    }

    /// `reports_series` buckets rows the same way `series_since` does (integer-division grouping,
    /// ascending by `ts`), but with the full `ReportMetrics` set per bucket instead of just
    /// requests/errors/latency/tokens.
    #[tokio::test]
    async fn reports_series_buckets_ascending_with_per_bucket_metrics() {
        let (_dir, repo) = seed_reports_fixture().await;

        let buckets = repo.reports_series(0, 100, None).await.unwrap();
        assert_eq!(buckets.len(), 2);

        assert_eq!(buckets[0].ts, 0);
        assert_eq!(buckets[0].metrics.requests, 2);
        assert_eq!(buckets[0].metrics.errors, 1, "row2 (500)");
        assert_eq!(buckets[0].metrics.cost_usd, 3.0, "row1's 1.0 + row2's 2.0");
        assert_eq!(buckets[0].metrics.tokens, 450, "150 (row1) + 300 (row2)");
        assert_eq!(
            buckets[0].metrics.ttft_sample_count, 1,
            "row2's NULL ttft excluded"
        );
        assert_eq!(buckets[0].metrics.avg_ttft_ms, 40.0);

        assert_eq!(buckets[1].ts, 100);
        assert_eq!(buckets[1].metrics.requests, 2);
        assert_eq!(buckets[1].metrics.errors, 1, "row4 (404)");
        assert_eq!(
            buckets[1].metrics.cost_usd, 4.0,
            "row3's NULL-as-0 + row4's 4.0"
        );
        assert_eq!(buckets[1].metrics.tokens, 1050, "450 (row3) + 600 (row4)");
        assert_eq!(buckets[1].metrics.ttft_sample_count, 2);
        assert_eq!(buckets[1].metrics.avg_ttft_ms, 70.0, "(60+80)/2");
    }

    /// The `provider` filter narrows `reports_series` per-bucket, same as it narrows
    /// `reports_totals`: with `provider=codex`, each bucket keeps only its `codex` row (row1 in
    /// bucket 0, row3 in bucket 100).
    #[tokio::test]
    async fn reports_series_provider_filter_narrows_bucket_contents() {
        let (_dir, repo) = seed_reports_fixture().await;

        let buckets = repo.reports_series(0, 100, Some("codex")).await.unwrap();
        assert_eq!(buckets.len(), 2, "codex has exactly one row in each bucket");
        assert_eq!(buckets[0].metrics.requests, 1);
        assert_eq!(buckets[0].metrics.tokens, 150, "row1 only");
        assert_eq!(buckets[1].metrics.requests, 1);
        assert_eq!(buckets[1].metrics.tokens, 450, "row3 only");
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

    /// `prune_older_than` deletes ONLY rows with `requested_at < cutoff`, leaving rows at/after the
    /// cutoff intact, and returns the exact number of rows deleted.
    #[tokio::test]
    async fn prune_older_than_deletes_only_rows_strictly_before_cutoff() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let repo = store.request_log();

        repo.insert(&row_at(50, 200, Some(1), 50)).await.unwrap(); // older — pruned
        repo.insert(&row_at(99, 200, Some(1), 50)).await.unwrap(); // older — pruned
        repo.insert(&row_at(100, 200, Some(1), 50)).await.unwrap(); // == cutoff — kept
        repo.insert(&row_at(150, 200, Some(1), 50)).await.unwrap(); // newer — kept

        let deleted = repo.prune_older_than(100, 100).await.unwrap();
        assert_eq!(deleted, 2, "only the two rows before ts=100 are pruned");
        assert_eq!(repo.count().await.unwrap(), 2);

        let remaining = repo.list(10, 0).await.unwrap();
        assert!(remaining.iter().all(|r| r.requested_at >= 100));
    }

    /// Batching: when more than `batch_size` rows are eligible, `prune_older_than` loops
    /// internally across multiple batches until all eligible rows are gone, returning the TOTAL
    /// deleted across all batches (not just the last one).
    #[tokio::test]
    async fn prune_older_than_deletes_all_eligible_rows_across_multiple_batches() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let repo = store.request_log();

        // 5 old rows (well before cutoff), batch_size=2 forces 3 internal batches (2+2+1).
        for ts in [10, 20, 30, 40, 50] {
            repo.insert(&row_at(ts, 200, Some(1), 50)).await.unwrap();
        }
        // 1 row at/after cutoff — must survive.
        repo.insert(&row_at(1000, 200, Some(1), 50)).await.unwrap();

        let deleted = repo.prune_older_than(1000, 2).await.unwrap();
        assert_eq!(deleted, 5, "all 5 old rows deleted across batches of 2");
        assert_eq!(repo.count().await.unwrap(), 1);
        let remaining = repo.list(10, 0).await.unwrap();
        assert_eq!(remaining[0].requested_at, 1000);
    }

    /// `update_usage` fills the six 0005 usage/cost columns on an already-inserted row, correlated
    /// by `request_id` — the stream wrapper's post-completion usage backfill. A no-op on an unknown
    /// `request_id` still returns `Ok` (fire-and-forget from the response path).
    ///
    /// Live-row-tps-basis fix: also asserts `duration_ms` is OVERWRITTEN by `update_usage`'s
    /// trailing param — the insert's original `duration_ms` (1000, from `sample_record`) must be
    /// replaced by the stream wrapper's true end-to-end value (9000), not left at the smaller
    /// route+setup-only figure.
    #[tokio::test]
    async fn update_usage_fills_row_by_request_id() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::Store::open(&dir.path().join("s.db")).await.unwrap();
        let repo = store.request_log();
        let mut rec = sample_record();
        rec.request_id = Some("req-xyz".into());
        assert_eq!(rec.duration_ms, 1000, "insert-time baseline duration_ms");
        repo.insert(&rec).await.unwrap();
        repo.update_usage(
            "req-xyz",
            Some(8380),
            Some(120),
            Some(6912),
            Some(256),
            Some(40),
            Some(8500),
            Some(11),
            Some(7),
            Some(3),
            Some(0.089),
            Some(3510),
            Some(9000),
            Some(RequestProtocolOutcome::Completed),
        )
        .await
        .unwrap();
        let row = repo
            .list(10, 0)
            .await
            .unwrap()
            .into_iter()
            .find(|r| r.request_id.as_deref() == Some("req-xyz"))
            .unwrap();
        assert_eq!(row.input_tokens, Some(8380));
        assert_eq!(row.output_tokens, Some(120));
        assert_eq!(row.cached_input_tokens, Some(6912));
        assert_eq!(row.cache_write_input_tokens, Some(256));
        assert_eq!(row.reasoning_tokens, Some(40));
        assert_eq!(row.reported_total_tokens, Some(8500));
        assert_eq!(row.usage_schema.as_deref(), Some("openai_responses_v1"));
        assert_eq!(row.usage_source.as_deref(), Some("upstream_response"));
        assert_eq!(row.usage_status.as_deref(), Some("final"));
        assert_eq!(row.orchestration_input_tokens, Some(11));
        assert_eq!(row.orchestration_output_tokens, Some(7));
        assert_eq!(row.orchestration_cached_input_tokens, Some(3));
        assert_eq!(row.cost_usd, Some(0.089));
        assert_eq!(row.latency_first_token_ms, Some(3510));
        assert_eq!(row.request_id.as_deref(), Some("req-xyz"));
        assert_eq!(row.protocol_outcome.as_deref(), Some("completed"));
        assert_eq!(
            row.duration_ms, 9000,
            "duration_ms must be overwritten to the stream wrapper's true end-to-end value"
        );

        // no-op on unknown id returns Ok
        repo.update_usage(
            "nope",
            Some(1),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(RequestProtocolOutcome::Completed),
        )
        .await
        .unwrap();

        // duration_ms: None must leave the existing value untouched (COALESCE), not null it out.
        repo.update_usage(
            "req-xyz", None, None, None, None, None, None, None, None, None, None, None, None, None,
        )
        .await
        .unwrap();
        let row = repo
            .list(10, 0)
            .await
            .unwrap()
            .into_iter()
            .find(|r| r.request_id.as_deref() == Some("req-xyz"))
            .unwrap();
        assert_eq!(
            row.duration_ms, 9000,
            "duration_ms: None must COALESCE to the existing value, not overwrite it"
        );
        assert_eq!(
            row.protocol_outcome.as_deref(),
            Some("completed"),
            "a metrics-only update must not erase the terminal protocol outcome"
        );
    }

    #[tokio::test]
    async fn canonical_usage_preserves_upstream_total_and_derives_codex_metrics_without_double_counting(
    ) {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::Store::open(&dir.path().join("s.db")).await.unwrap();
        let repo = store.request_log();
        let mut rec = sample_record();
        rec.request_id = Some("canonical-usage".into());
        rec.total_tokens = None;
        rec.cached_tokens = None;
        repo.insert(&rec).await.unwrap();

        repo.update_usage(
            "canonical-usage",
            Some(100),
            Some(25),
            Some(80),
            Some(12),
            Some(5),
            Some(999),
            None,
            None,
            None,
            None,
            None,
            None,
            Some(RequestProtocolOutcome::Completed),
        )
        .await
        .unwrap();

        let row = repo.list(1, 0).await.unwrap().remove(0);
        assert_eq!(row.api_total_tokens(), Some(999));
        assert_eq!(row.uncached_input_tokens(), Some(20));
        assert_eq!(row.visible_output_tokens(), Some(20));
        assert_eq!(row.effective_tokens(), Some(45));

        let aggregate = repo.aggregate_since(0).await.unwrap();
        assert_eq!(aggregate.total_tokens, 999);
        assert_eq!(aggregate.effective_tokens, 45);
        assert_eq!(aggregate.cache_write_input_tokens, 12);

        let report = repo.reports_totals(0, None).await.unwrap();
        assert_eq!(report.tokens, 999);
        assert_eq!(report.input_tokens, 100);
        assert_eq!(report.cached_tokens, 80);
        assert_eq!(report.cache_write_tokens, 12);
        assert_eq!(report.reasoning_tokens, 5);
        assert_eq!(report.effective_tokens, 45);
    }

    #[tokio::test]
    async fn invalid_legacy_total_does_not_hide_a_complete_input_output_pair() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::Store::open(&dir.path().join("s.db")).await.unwrap();
        let repo = store.request_log();
        let mut rec = sample_record();
        rec.request_id = Some("invalid-legacy-total".into());
        rec.total_tokens = Some(-1);
        rec.input_tokens = Some(100);
        rec.output_tokens = Some(25);
        repo.insert(&rec).await.unwrap();

        let row = repo.list(1, 0).await.unwrap().remove(0);
        assert_eq!(row.api_total_tokens(), Some(125));

        let aggregate = repo.aggregate_since(0).await.unwrap();
        assert_eq!(aggregate.total_tokens, 125);
    }

    #[tokio::test]
    async fn orchestration_only_terminal_usage_is_still_classified_as_final() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::Store::open(&dir.path().join("s.db")).await.unwrap();
        let repo = store.request_log();
        let mut rec = sample_record();
        rec.request_id = Some("orchestration-only".into());
        repo.insert(&rec).await.unwrap();

        repo.update_usage(
            "orchestration-only",
            None,
            None,
            None,
            None,
            None,
            None,
            Some(11),
            Some(7),
            Some(3),
            None,
            None,
            None,
            Some(RequestProtocolOutcome::Completed),
        )
        .await
        .unwrap();

        let row = repo.list(1, 0).await.unwrap().remove(0);
        assert_eq!(row.usage_schema.as_deref(), Some("openai_responses_v1"));
        assert_eq!(row.usage_source.as_deref(), Some("upstream_response"));
        assert_eq!(row.usage_status.as_deref(), Some("final"));
    }

    #[tokio::test]
    async fn terminal_protocol_outcome_overrides_initial_http_status_in_all_classification() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::Store::open(&dir.path().join("s.db")).await.unwrap();
        let repo = store.request_log();

        let outcomes = [
            ("completed", RequestProtocolOutcome::Completed),
            ("failed", RequestProtocolOutcome::Failed),
            ("incomplete", RequestProtocolOutcome::Incomplete),
            ("cancelled", RequestProtocolOutcome::Cancelled),
            ("transport-lost", RequestProtocolOutcome::TransportLost),
        ];
        for (request_id, outcome) in outcomes {
            let mut rec = sample_record();
            rec.request_id = Some(request_id.into());
            rec.status = 200;
            repo.insert(&rec).await.unwrap();
            repo.update_usage(
                request_id,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(outcome),
            )
            .await
            .unwrap();
        }

        // Legacy rows without a protocol outcome retain their existing HTTP classification.
        let mut legacy_success = sample_record();
        legacy_success.request_id = Some("legacy-success".into());
        legacy_success.status = 200;
        repo.insert(&legacy_success).await.unwrap();
        let mut legacy_error = sample_record();
        legacy_error.request_id = Some("legacy-error".into());
        legacy_error.status = 503;
        repo.insert(&legacy_error).await.unwrap();

        let aggregate = repo.aggregate_since(0).await.unwrap();
        assert_eq!(aggregate.total, 7);
        assert_eq!(
            aggregate.success, 2,
            "only completed and legacy HTTP 200 count as successful"
        );
        assert_eq!(
            aggregate.error, 5,
            "failed, incomplete, cancelled, transport_lost, and legacy HTTP 503 count as errors"
        );

        let errors = repo.recent_errors(10).await.unwrap();
        assert_eq!(errors.len(), 5);

        let (success_rows, success_total) = repo
            .page(
                &RequestsFilter {
                    status_class: Some("success".into()),
                    ..RequestsFilter::default()
                },
                20,
                0,
            )
            .await
            .unwrap();
        assert_eq!(success_total, 2);
        assert_eq!(success_rows.len(), 2);

        let (error_rows, error_total) = repo
            .page(
                &RequestsFilter {
                    status_class: Some("error".into()),
                    ..RequestsFilter::default()
                },
                20,
                0,
            )
            .await
            .unwrap();
        assert_eq!(error_total, 5);
        assert_eq!(error_rows.len(), 5);
    }

    /// A full `RequestLogRecord` with every existing field populated and every new (usage) field
    /// `None` — the shared base for tests that only care about a couple of fields on top.
    fn sample_record() -> RequestLogRecord {
        RequestLogRecord {
            requested_at: 100,
            provider: "codex".into(),
            method: "POST".into(),
            path: "/responses".into(),
            aliased: false,
            status: 200,
            duration_ms: 1000,
            account_id: Some("acct-1".into()),
            target_kind: Some("account".into()),
            provider_credential_id: None,
            model: Some("gpt-5.6-sol".into()),
            upstream_model: None,
            upstream_transport: None,
            profile_revision: None,
            reasoning_effort: None,
            service_tier: None,
            transport: Some("http".into()),
            ttft_ms: Some(200),
            total_tokens: Some(500),
            cached_tokens: None,
            subagent: None,
            request_id: None,
            session_key: None,
            input_tokens: None,
            output_tokens: None,
            cached_input_tokens: None,
            reasoning_tokens: None,
            orchestration_input_tokens: None,
            orchestration_output_tokens: None,
            orchestration_cached_input_tokens: None,
            cost_usd: None,
            latency_first_token_ms: None,
            protocol_outcome: None,
        }
    }

    /// A cutoff in the future deletes every row; a cutoff before every row's timestamp deletes
    /// nothing and returns 0.
    #[tokio::test]
    async fn prune_older_than_future_cutoff_deletes_all_past_cutoff_deletes_none() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let repo = store.request_log();

        repo.insert(&row_at(100, 200, Some(1), 50)).await.unwrap();
        repo.insert(&row_at(200, 200, Some(1), 50)).await.unwrap();

        // Cutoff before all rows → deletes none.
        let deleted = repo.prune_older_than(50, 100).await.unwrap();
        assert_eq!(deleted, 0);
        assert_eq!(repo.count().await.unwrap(), 2);

        // Cutoff far in the future → deletes all.
        let deleted = repo.prune_older_than(1_000_000, 100).await.unwrap();
        assert_eq!(deleted, 2);
        assert_eq!(repo.count().await.unwrap(), 0);
    }

    /// `batch_size <= 0` is treated as a no-op (returns 0, deletes nothing) rather than looping
    /// forever or being misinterpreted by SQLite's `LIMIT` semantics (a non-positive `LIMIT` binds
    /// to "no limit" in SQLite, which would turn one batch into an unbounded delete).
    #[tokio::test]
    async fn prune_older_than_non_positive_batch_size_is_a_no_op() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let repo = store.request_log();

        repo.insert(&row_at(10, 200, Some(1), 50)).await.unwrap();

        assert_eq!(repo.prune_older_than(1_000_000, 0).await.unwrap(), 0);
        assert_eq!(repo.prune_older_than(1_000_000, -5).await.unwrap(), 0);
        assert_eq!(
            repo.count().await.unwrap(),
            1,
            "no-op guard must not delete anything"
        );
    }
}
