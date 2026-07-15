//! Read-only JSON API backing the dashboard: pools, accounts (with their live rate-limit windows +
//! reset times), and recent request-log rows. This surface is admin-facing and returns non-secret
//! account METADATA only — id, email, pool, provider, status, plan, usage percentages and reset
//! epochs. It NEVER returns a token, refresh token, or id_token (those never leave the store as
//! plaintext except through the executor's own Bearer use). No conversation content is stored, so
//! none can be exposed here. Like the rest of the MVP endpoints these are unauthenticated and rely
//! on the network boundary; admin auth is a follow-up.

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::response::IntoResponse;
use axum::Json;
use serde::{Deserialize, Serialize};

use polyflare_store::WindowUsage;

use crate::app::AppState;

/// One rate-limit window as the dashboard consumes it: how full it is and when it resets.
#[derive(Serialize)]
struct WindowView {
    used_percent: f64,
    /// Absolute unix-epoch seconds when the window resets, or null if upstream didn't report one.
    reset_at: Option<i64>,
}

impl From<WindowUsage> for WindowView {
    fn from(w: WindowUsage) -> Self {
        WindowView {
            used_percent: w.used_percent,
            reset_at: w.reset_at,
        }
    }
}

/// One account row for the dashboard. `primary` is the 5h window (often absent — upstream stopped
/// emitting it for current plans; a null primary means "not reported", NOT blocked). `secondary`
/// is the weekly window. `reset_at` is the durable routing-gate reset stamped by the usage refresh.
#[derive(Serialize)]
struct AccountView {
    id: String,
    email: String,
    pool: Option<String>,
    provider: String,
    status: String,
    plan_type: String,
    reset_at: Option<i64>,
    /// 5h window (may be null).
    primary: Option<WindowView>,
    /// Weekly window (may be null).
    secondary: Option<WindowView>,
}

/// `GET /api/accounts` — every account with its latest usage windows + reset times. This is where
/// the "see the reset time (5h + weekly)" goal is surfaced.
pub async fn accounts_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let repo = state.store.accounts();
    let accounts = match repo.list().await {
        Ok(a) => a,
        Err(_) => return Response::error(),
    };
    let mut views = Vec::with_capacity(accounts.len());
    for account in accounts {
        // Per-account latest usage (small N; a dashboard read, never the hot path).
        let usage = repo.latest_usage(&account.id).await.unwrap_or_default();
        views.push(AccountView {
            id: account.id,
            email: account.email,
            pool: account.pool,
            provider: account.provider,
            status: account.status,
            plan_type: account.plan_type,
            reset_at: account.reset_at,
            primary: usage.primary.map(Into::into),
            secondary: usage.secondary.map(Into::into),
        });
    }
    Response::ok(views)
}

/// One pool as the dashboard lists it. `pool = null` is the unpooled group (accounts reachable only
/// via the bare ingress paths). `active` counts accounts whose status is `active`.
#[derive(Serialize)]
struct PoolView {
    pool: Option<String>,
    accounts: usize,
    active: usize,
}

/// `GET /api/pools` — the configured pools with account + active counts, aggregated from the
/// account list. Sorted with the unpooled group last, named pools alphabetically before it.
pub async fn pools_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let accounts = match state.store.accounts().list().await {
        Ok(a) => a,
        Err(_) => return Response::error(),
    };
    // (accounts, active) per pool key. `None` collects the unpooled accounts.
    let mut by_pool: std::collections::BTreeMap<Option<String>, (usize, usize)> =
        std::collections::BTreeMap::new();
    for account in &accounts {
        let entry = by_pool.entry(account.pool.clone()).or_insert((0, 0));
        entry.0 += 1;
        if account.status == "active" {
            entry.1 += 1;
        }
    }
    // BTreeMap orders `None` before `Some(..)`; the dashboard wants named pools first, unpooled
    // last, so pull the unpooled group out and append it.
    let unpooled = by_pool.remove(&None);
    let mut views: Vec<PoolView> = by_pool
        .into_iter()
        .map(|(pool, (accounts, active))| PoolView {
            pool,
            accounts,
            active,
        })
        .collect();
    if let Some((accounts, active)) = unpooled {
        views.push(PoolView {
            pool: None,
            accounts,
            active,
        });
    }
    Response::ok(views)
}

/// One request-log row for the dashboard: content-free by construction (the same audited field set
/// the tracing event carries — method/path/provider/status/latency, never a body or identity).
#[derive(Serialize)]
struct RequestRowView {
    id: i64,
    requested_at: i64,
    provider: String,
    method: String,
    path: String,
    aliased: bool,
    status: i64,
    duration_ms: i64,
}

/// `GET /api/requests` pagination. `limit` is clamped to [1, MAX_LIMIT]; `offset` defaults to 0.
#[derive(Deserialize)]
pub struct RequestsQuery {
    limit: Option<i64>,
    offset: Option<i64>,
}

const DEFAULT_LIMIT: i64 = 100;
const MAX_LIMIT: i64 = 1000;

#[derive(Serialize)]
struct RequestsView {
    /// Total rows in the log (for the client to paginate); the returned `rows` are one page.
    total: i64,
    rows: Vec<RequestRowView>,
}

/// `GET /api/requests?limit=&offset=` — recent request-log rows (newest first, per the repo's
/// ordering) plus the total count.
pub async fn requests_handler(
    State(state): State<Arc<AppState>>,
    Query(q): Query<RequestsQuery>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let offset = q.offset.unwrap_or(0).max(0);
    let repo = state.store.request_log();
    let rows = match repo.list(limit, offset).await {
        Ok(r) => r,
        Err(_) => return Response::error(),
    };
    let total = repo.count().await.unwrap_or(0);
    let rows = rows
        .into_iter()
        .map(|r| RequestRowView {
            id: r.id,
            requested_at: r.requested_at,
            provider: r.provider,
            method: r.method,
            path: r.path,
            aliased: r.aliased,
            status: r.status,
            duration_ms: r.duration_ms,
        })
        .collect();
    Response::ok(RequestsView { total, rows })
}

/// A tiny JSON responder: `Ok(200, body)` or a content-safe `500` (the store error's own text is
/// never surfaced — a read failure returns a generic body, like the ingress error paths).
enum Response<T: Serialize> {
    Ok(T),
    Error,
}

impl<T: Serialize> Response<T> {
    fn ok(body: T) -> Self {
        Response::Ok(body)
    }
    fn error() -> Self {
        Response::Error
    }
}

impl<T: Serialize> axum::response::IntoResponse for Response<T> {
    fn into_response(self) -> axum::response::Response {
        use axum::http::StatusCode;
        match self {
            Response::Ok(body) => (StatusCode::OK, Json(body)).into_response(),
            Response::Error => {
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
            }
        }
    }
}
