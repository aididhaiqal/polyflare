//! Claude Code transcript usage: ingest (`POST /api/claude/local-usage`, fed by
//! `scripts/claude-usage-push` from each machine's `~/.claude/projects/**/*.jsonl`) and the
//! Analytics summary (`GET /api/claude/local-usage?range=`). Both sit behind `require_admin`.
//! Content-free: a row is a model name, token counts, a timestamp, ids and a project path —
//! never message text. Cost is the API list-price estimate from `polyflare_core::pricing`.

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};

use polyflare_store::{ClaudeLocalAgg, ClaudeLocalUsageRow};

use crate::app::AppState;

/// Hard cap per push so a runaway script cannot pin the writer; the script batches at 500.
const MAX_ROWS_PER_PUSH: usize = 5_000;
const MAX_IDENT_LEN: usize = 128;
const MAX_PROJECT_LEN: usize = 512;

#[derive(Debug, Deserialize)]
pub struct IngestRow {
    request_id: String,
    message_id: String,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    project: Option<String>,
    model: String,
    /// Unix seconds.
    ts: i64,
    #[serde(default)]
    input_tokens: i64,
    #[serde(default)]
    output_tokens: i64,
    #[serde(default)]
    cache_write_5m_tokens: i64,
    #[serde(default)]
    cache_write_1h_tokens: i64,
    #[serde(default)]
    cache_read_tokens: i64,
    #[serde(default)]
    service_tier: Option<String>,
    #[serde(default)]
    speed: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct IngestRequest {
    machine: String,
    rows: Vec<IngestRow>,
}

#[derive(Serialize)]
struct IngestResponse {
    received: usize,
    inserted: u64,
    /// Models with no price row — their rows are stored with `cost_usd: null`.
    unpriced_models: Vec<String>,
}

fn bounded(value: &str, max: usize) -> String {
    value.trim().chars().take(max).collect()
}

pub async fn ingest_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<IngestRequest>,
) -> Response {
    let machine = bounded(&body.machine, MAX_IDENT_LEN);
    if machine.is_empty() {
        return (StatusCode::BAD_REQUEST, "machine is required").into_response();
    }
    if body.rows.len() > MAX_ROWS_PER_PUSH {
        return (StatusCode::PAYLOAD_TOO_LARGE, "at most 5000 rows per push").into_response();
    }
    let mut unpriced: Vec<String> = Vec::new();
    let mut rows = Vec::with_capacity(body.rows.len());
    for r in body.rows {
        let request_id = bounded(&r.request_id, MAX_IDENT_LEN);
        let message_id = bounded(&r.message_id, MAX_IDENT_LEN);
        let model = bounded(&r.model, MAX_IDENT_LEN);
        if request_id.is_empty() || message_id.is_empty() || model.is_empty() || r.ts <= 0 {
            continue;
        }
        let cost_usd = polyflare_core::pricing::pricing_for_model(&model).map(|price| {
            polyflare_core::pricing::claude_message_cost_usd(
                price,
                r.input_tokens,
                r.cache_write_5m_tokens,
                r.cache_write_1h_tokens,
                r.cache_read_tokens,
                r.output_tokens,
            )
        });
        if cost_usd.is_none() && !unpriced.contains(&model) {
            unpriced.push(model.clone());
        }
        rows.push(ClaudeLocalUsageRow {
            machine: machine.clone(),
            request_id,
            message_id,
            session_id: r.session_id.as_deref().map(|s| bounded(s, MAX_IDENT_LEN)),
            project: r.project.as_deref().map(|s| bounded(s, MAX_PROJECT_LEN)),
            model,
            ts: r.ts,
            input_tokens: r.input_tokens.max(0),
            output_tokens: r.output_tokens.max(0),
            cache_write_5m_tokens: r.cache_write_5m_tokens.max(0),
            cache_write_1h_tokens: r.cache_write_1h_tokens.max(0),
            cache_read_tokens: r.cache_read_tokens.max(0),
            service_tier: r.service_tier.as_deref().map(|s| bounded(s, 32)),
            speed: r.speed.as_deref().map(|s| bounded(s, 32)),
            cost_usd,
        });
    }
    let received = rows.len();
    match state
        .store
        .claude_local_usage()
        .insert_batch(&rows, crate::usage_refresh::unix_now())
        .await
    {
        Ok(inserted) => Json(IngestResponse {
            received,
            inserted,
            unpriced_models: unpriced,
        })
        .into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "storage_error").into_response(),
    }
}

#[derive(Debug, Deserialize)]
pub struct SummaryQuery {
    range: Option<String>,
}

#[derive(Serialize)]
struct AggView {
    key: String,
    messages: i64,
    input_tokens: i64,
    output_tokens: i64,
    cache_write_tokens: i64,
    cache_read_tokens: i64,
    cost_usd: f64,
    priced_messages: i64,
}

impl From<ClaudeLocalAgg> for AggView {
    fn from(a: ClaudeLocalAgg) -> Self {
        Self {
            key: a.key,
            messages: a.messages,
            input_tokens: a.input_tokens,
            output_tokens: a.output_tokens,
            cache_write_tokens: a.cache_write_tokens,
            cache_read_tokens: a.cache_read_tokens,
            cost_usd: a.cost_usd,
            priced_messages: a.priced_messages,
        }
    }
}

#[derive(Serialize)]
struct SummaryView {
    range: String,
    since_ts: i64,
    latest_ts: Option<i64>,
    totals: Option<AggView>,
    by_model: Vec<AggView>,
    by_day: Vec<AggView>,
    by_project: Vec<AggView>,
    by_machine: Vec<AggView>,
}

/// `24h` | `7d` (default) | `30d` | `90d`, mirroring the Analytics page's range switch.
fn range_secs(range: &str) -> Option<i64> {
    match range {
        "24h" => Some(24 * 3600),
        "7d" => Some(7 * 86_400),
        "30d" => Some(30 * 86_400),
        "90d" => Some(90 * 86_400),
        _ => None,
    }
}

pub async fn summary_handler(
    State(state): State<Arc<AppState>>,
    Query(q): Query<SummaryQuery>,
) -> Response {
    let range = q.range.as_deref().unwrap_or("7d").to_string();
    let Some(secs) = range_secs(&range) else {
        return (StatusCode::BAD_REQUEST, "range must be 24h, 7d, 30d or 90d").into_response();
    };
    let since_ts = crate::usage_refresh::unix_now() - secs;
    match state.store.claude_local_usage().summary(since_ts).await {
        Ok(s) => Json(SummaryView {
            range,
            since_ts,
            latest_ts: s.latest_ts,
            totals: s.totals.map(Into::into),
            by_model: s.by_model.into_iter().map(Into::into).collect(),
            by_day: s.by_day.into_iter().map(Into::into).collect(),
            by_project: s.by_project.into_iter().map(Into::into).collect(),
            by_machine: s.by_machine.into_iter().map(Into::into).collect(),
        })
        .into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "storage_error").into_response(),
    }
}
