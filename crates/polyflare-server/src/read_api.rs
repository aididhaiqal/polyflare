//! Read-only JSON API backing the dashboard: pools, accounts (with their live rate-limit windows +
//! reset times), and recent request-log rows. This surface is admin-facing and returns non-secret
//! account METADATA only — id, email, pool, provider, status, plan, usage percentages and reset
//! epochs. It NEVER returns a token, refresh token, or id_token (those never leave the store as
//! plaintext except through the executor's own Bearer use). No conversation content is stored, so
//! none can be exposed here. Like the rest of the MVP endpoints these are unauthenticated and rely
//! on the network boundary; admin auth is a follow-up.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Query, State};
use axum::response::IntoResponse;
use axum::Json;
use serde::{Deserialize, Serialize};

use polyflare_codex::oauth::token_exp;
use polyflare_store::RequestsFilter;

use crate::app::AppState;
use crate::usage_windows::{resolve, ResolvedWindow};

/// Rolling lookback for `AccountView::request_count_24h`: how many requests this account served in
/// the last 24h.
const ACCOUNT_REQUEST_COUNT_WINDOW_SECS: i64 = 24 * 3600;

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// One rate-limit window as the dashboard consumes it: how full it is, when it resets, and whether
/// the data is stale (upstream stopped refreshing it — see `crate::usage_windows`). A stale window
/// is still returned so the last-known value is visible, but flagged so it never reads as live.
#[derive(Serialize)]
struct WindowView {
    used_percent: f64,
    /// Absolute unix-epoch seconds when the window resets, or null if upstream didn't report one.
    reset_at: Option<i64>,
    stale: bool,
}

impl From<ResolvedWindow> for WindowView {
    fn from(w: ResolvedWindow) -> Self {
        WindowView {
            used_percent: w.used_percent,
            reset_at: w.reset_at,
            stale: w.stale,
        }
    }
}

/// One entry of `AccountView::usage`: a named rate-limit window (`"five_hour"` | `"weekly"`), how
/// full it is, and when it resets. The array is ADAPTIVE — a window the provider/account doesn't
/// report at all (e.g. `five_hour` during the current no-5h-limit promo) is omitted entirely, never
/// emitted as a zeroed placeholder.
#[derive(Serialize)]
struct UsageWindowView {
    window: &'static str,
    used_percent: f64,
    reset_at: Option<i64>,
}

/// The account's stored access-token health, derived from the token's OWN unverified JWT `exp`
/// claim — never the token itself (see module docs: this surface never returns a token).
/// `access_state` is `"missing"` (no token / undecryptable / `exp` unreadable), `"expired"`
/// (`exp < now`), or `"valid"`. `access_expires_at` is the raw expiry unix-epoch-seconds (content-
/// safe on its own — it identifies a moment in time, not a credential) or `null` when unknown.
#[derive(Serialize)]
struct TokenHealthView {
    access_state: &'static str,
    access_expires_at: Option<i64>,
}

/// One account row for the dashboard. Windows are resolved by DURATION, not storage slot (see
/// `crate::usage_windows`): `five_hour` is the 5h limit (null when upstream isn't reporting one —
/// e.g. the current no-5h-limit promo — which means "not reported", NOT blocked); `weekly` is the
/// weekly limit. `reset_at` is the durable routing-gate reset stamped by the usage refresh.
#[derive(Serialize)]
struct AccountView {
    id: String,
    email: String,
    pool: Option<String>,
    provider: String,
    status: String,
    plan_type: String,
    routing_policy: String,
    reset_at: Option<i64>,
    /// 5h window (may be null).
    five_hour: Option<WindowView>,
    /// Weekly window (may be null).
    weekly: Option<WindowView>,
    /// Adaptive per-window usage, `{window, used_percent, reset_at}[]` — the same resolved windows
    /// as `five_hour`/`weekly` above, restated as a list for dashboard consumers that want to
    /// iterate rather than address two fixed fields.
    usage: Vec<UsageWindowView>,
    /// Access-token health derived from the stored token's JWT `exp` — never the token.
    token_health: TokenHealthView,
    /// Requests this account served in the last 24h (from `request_log`).
    request_count_24h: i64,
}

/// `GET /api/accounts` — every account with its latest usage windows + reset times. This is where
/// the "see the reset time (5h + weekly)" goal is surfaced.
pub async fn accounts_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let repo = state.store.accounts();
    let accounts = match repo.list().await {
        Ok(a) => a,
        Err(_) => return Response::error(),
    };
    let now = unix_now();
    let mut views = Vec::with_capacity(accounts.len());
    for account in accounts {
        // Per-account latest usage (small N; a dashboard read, never the hot path), resolved by
        // duration + freshness so the right window shows under the right heading.
        let usage = repo.latest_usage(&account.id).await.unwrap_or_default();
        let resolved = resolve(&usage, now);
        let mut usage_windows = Vec::with_capacity(2);
        if let Some(w) = &resolved.five_hour {
            usage_windows.push(UsageWindowView {
                window: "five_hour",
                used_percent: w.used_percent,
                reset_at: w.reset_at,
            });
        }
        if let Some(w) = &resolved.weekly {
            usage_windows.push(UsageWindowView {
                window: "weekly",
                used_percent: w.used_percent,
                reset_at: w.reset_at,
            });
        }

        // Token health: derived ONLY from the access token's own unverified JWT `exp` — the token
        // itself never leaves `get_with_tokens`'s scope here. A decrypt failure or missing account
        // (shouldn't happen — we just listed it) collapses to "missing", same as no token at all.
        let token_health = match repo.get_with_tokens(&account.id, &state.cipher).await {
            Ok(Some((_, tokens))) => match token_exp(&tokens.access_token) {
                Some(exp) if exp < now => TokenHealthView {
                    access_state: "expired",
                    access_expires_at: Some(exp),
                },
                Some(exp) => TokenHealthView {
                    access_state: "valid",
                    access_expires_at: Some(exp),
                },
                None => TokenHealthView {
                    access_state: "missing",
                    access_expires_at: None,
                },
            },
            _ => TokenHealthView {
                access_state: "missing",
                access_expires_at: None,
            },
        };

        let request_count_24h = state
            .store
            .request_log()
            .page(
                &RequestsFilter {
                    account: Some(account.id.clone()),
                    since_ts: Some(now - ACCOUNT_REQUEST_COUNT_WINDOW_SECS),
                    ..Default::default()
                },
                1,
                0,
            )
            .await
            .map(|(_, total)| total as i64)
            .unwrap_or(0);

        views.push(AccountView {
            id: account.id,
            email: account.email,
            pool: account.pool,
            provider: account.provider,
            status: account.status,
            plan_type: account.plan_type,
            routing_policy: account.routing_policy,
            reset_at: account.reset_at,
            five_hour: resolved.five_hour.map(Into::into),
            weekly: resolved.weekly.map(Into::into),
            usage: usage_windows,
            token_health,
            request_count_24h,
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
/// the tracing event carries — method/path/provider/status/latency plus the content-free per-request
/// metrics — never a body or identity). `tps` is derived, not stored: `total_tokens` over the
/// generation window (`duration_ms - ttft_ms`, seconds), present only when both source fields are
/// present and the window is positive.
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
    account_id: Option<String>,
    model: Option<String>,
    reasoning_effort: Option<String>,
    service_tier: Option<String>,
    transport: Option<String>,
    ttft_ms: Option<i64>,
    total_tokens: Option<i64>,
    cached_tokens: Option<i64>,
    tps: Option<f64>,
}

/// Tokens/sec over the post-first-token generation window, when derivable: needs both
/// `total_tokens` and `ttft_ms`, and a positive `duration_ms - ttft_ms` window (else the divisor
/// would be zero or negative and the ratio is meaningless).
fn derive_tps(duration_ms: i64, ttft_ms: Option<i64>, total_tokens: Option<i64>) -> Option<f64> {
    let ttft_ms = ttft_ms?;
    let total_tokens = total_tokens?;
    if duration_ms <= ttft_ms {
        return None;
    }
    Some(total_tokens as f64 / ((duration_ms - ttft_ms) as f64 / 1000.0))
}

/// `GET /api/requests` filters + pagination. `limit` is clamped to [1, MAX_LIMIT]; `offset`
/// defaults to 0. `status_class` is `"success"` (status < 300), `"error"` (status >= 400), or
/// anything else / unset (no status filter). All filters are content-free identifiers, never
/// request/response text.
#[derive(Deserialize)]
pub struct RequestsQuery {
    limit: Option<i64>,
    offset: Option<i64>,
    account: Option<String>,
    provider: Option<String>,
    status_class: Option<String>,
    model: Option<String>,
    transport: Option<String>,
    since_ts: Option<i64>,
}

const DEFAULT_LIMIT: i64 = 100;
const MAX_LIMIT: i64 = 1000;

#[derive(Serialize)]
struct RequestsView {
    /// Total rows in the log (for the client to paginate); the returned `rows` are one page.
    total: i64,
    rows: Vec<RequestRowView>,
}

/// `GET /api/requests?limit=&offset=&account=&provider=&status_class=&model=&transport=&since_ts=`
/// — filtered, paginated request-log rows (newest first, per the repo's ordering) plus the total
/// count MATCHING the filters (not the whole table).
pub async fn requests_handler(
    State(state): State<Arc<AppState>>,
    Query(q): Query<RequestsQuery>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let offset = q.offset.unwrap_or(0).max(0);
    let filter = RequestsFilter {
        account: q.account,
        provider: q.provider,
        status_class: q.status_class,
        model: q.model,
        transport: q.transport,
        since_ts: q.since_ts,
    };
    let repo = state.store.request_log();
    let (rows, total) = match repo.page(&filter, limit, offset).await {
        Ok(r) => r,
        Err(_) => return Response::error(),
    };
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
            tps: derive_tps(r.duration_ms, r.ttft_ms, r.total_tokens),
            account_id: r.account_id,
            model: r.model,
            reasoning_effort: r.reasoning_effort,
            service_tier: r.service_tier,
            transport: r.transport,
            ttft_ms: r.ttft_ms,
            total_tokens: r.total_tokens,
            cached_tokens: r.cached_tokens,
        })
        .collect();
    Response::ok(RequestsView {
        total: total as i64,
        rows,
    })
}

/// Default lookback window for the overview KPI tile: rolling 24h. Not yet client-configurable
/// (no query param) — the brief's `Consumes` list is just the store/cache/runtime, and a
/// dashboard-side "last 24h" default is the common landing-page convention. A `since_ts` override
/// can be added later the same way `/api/requests` added its own filter set.
const OVERVIEW_KPI_WINDOW_SECS: i64 = 24 * 3600;

/// How many `recent_errors` rows the overview surfaces.
const RECENT_ERRORS_LIMIT: i64 = 10;

/// The overview KPI tile: request volume/outcome/latency/token rollup over the last
/// [`OVERVIEW_KPI_WINDOW_SECS`], straight off [`polyflare_store::RequestAggregate`].
#[derive(Serialize)]
struct KpisView {
    requests: i64,
    success: i64,
    errors: i64,
    /// `success / requests`, or `0.0` when `requests == 0` (avoids a NaN from a 0/0 divide).
    success_rate: f64,
    avg_latency_ms: f64,
    total_tokens: i64,
}

impl From<polyflare_store::RequestAggregate> for KpisView {
    fn from(a: polyflare_store::RequestAggregate) -> Self {
        let success_rate = if a.total > 0 {
            a.success as f64 / a.total as f64
        } else {
            0.0
        };
        KpisView {
            requests: a.total,
            success: a.success,
            errors: a.error,
            success_rate,
            avg_latency_ms: a.avg_latency_ms,
            total_tokens: a.total_tokens,
        }
    }
}

/// One provider's quota tile. `five_hour`/`weekly` are remaining-percent (`100 - used_percent`),
/// the WORST CASE (minimum remaining) across that provider's accounts — the number that matters
/// for "are we about to run out of capacity", not an average that would hide one exhausted account
/// behind many fresh ones.
///
/// # Known simplification
/// These come from `AccountCache::snapshots()`'s `used_percent`/`secondary_used_percent`, which
/// `assemble_snapshots` defaults to `0.0` when upstream isn't reporting a window at all (see
/// `crate::snapshot`) — i.e. this layer cannot distinguish "genuinely 0% used" from "no window
/// reported". `/api/accounts` gets that distinction from a richer per-account query
/// (`usage_windows::resolve` over `repo.latest_usage`); re-deriving that here per provider is
/// deferred until a real need for it shows up. Both windows are therefore always present for any
/// provider with at least one account.
#[derive(Serialize)]
struct ProviderQuotaView {
    provider: String,
    five_hour: f64,
    weekly: f64,
}

/// One pool's account/availability counts for the overview (computed inline from
/// `account_cache.snapshots()` — NOT sourced from `/api/pools`, which is extended separately).
/// `available` uses the same eligibility rule as the top-level `accounts_available` (see
/// [`is_available`]), scoped to this pool.
#[derive(Serialize)]
struct PoolOverviewView {
    pool: Option<String>,
    accounts: usize,
    available: usize,
}

/// One `recent_errors` row: content-free error identification, never a body or upstream message.
#[derive(Serialize)]
struct RecentErrorView {
    status: i64,
    account_id: Option<String>,
    error_code: Option<String>,
    requested_at: i64,
}

impl From<polyflare_store::RecentErrorRow> for RecentErrorView {
    fn from(r: polyflare_store::RecentErrorRow) -> Self {
        RecentErrorView {
            status: r.status,
            account_id: r.account_id,
            error_code: r.error_code,
            requested_at: r.requested_at,
        }
    }
}

#[derive(Serialize)]
struct OverviewView {
    kpis: KpisView,
    /// One entry per provider PRESENT in the account pool (a provider with zero accounts is
    /// omitted entirely — never a zeroed placeholder entry).
    quota: Vec<ProviderQuotaView>,
    pools: Vec<PoolOverviewView>,
    /// Count of accounts eligible for routing right now (see [`is_available`]), across ALL pools.
    accounts_available: usize,
    recent_errors: Vec<RecentErrorView>,
}

/// An account counts as "available" for the dashboard's headline number when its durable status is
/// `active` AND it isn't currently benched by the live runtime overlay (`cooldown_until` absent or
/// already elapsed). This mirrors (a coarse approximation of) the selector's own eligibility gate
/// without re-deriving its full state-machine (rate_limited/quota_exceeded reset logic, error
/// backoff) here — good enough for a dashboard headline, not a routing decision.
fn is_available(snap: &polyflare_core::AccountSnapshot, now: i64) -> bool {
    snap.status == "active" && snap.cooldown_until.is_none_or(|cd| cd < now)
}

/// `GET /api/overview` — the dashboard landing-page aggregates: request KPIs (rolling 24h),
/// per-provider quota headroom, per-pool account/availability counts, the global available-account
/// count, and the most recent errors. Every field is a content-free aggregate/metric/identifier —
/// never a request body, token, or conversation content.
pub async fn overview_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let now = unix_now();

    let kpis = match state
        .store
        .request_log()
        .aggregate_since(now - OVERVIEW_KPI_WINDOW_SECS)
        .await
    {
        Ok(a) => KpisView::from(a),
        Err(_) => return Response::error(),
    };

    let recent_errors = match state
        .store
        .request_log()
        .recent_errors(RECENT_ERRORS_LIMIT)
        .await
    {
        Ok(rows) => rows.into_iter().map(RecentErrorView::from).collect(),
        Err(_) => return Response::error(),
    };

    let snapshots = match state.account_cache.snapshots(&state.store).await {
        Ok(s) => s,
        Err(_) => return Response::error(),
    };
    // The cache's `Arc<Vec<..>>` is shared; the runtime overlay mutates in place, so clone the
    // slice into an owned `Vec` first (same pattern the ingress path uses before `Selector::pick`).
    let mut snapshots = (*snapshots).clone();
    state.runtime.overlay(&mut snapshots, now);

    let accounts_available = snapshots.iter().filter(|s| is_available(s, now)).count();

    // Pools: group by `pool` (None = unpooled), counting total + available per group. Named pools
    // alphabetically first, unpooled group last — same convention as `/api/pools`.
    let mut by_pool: std::collections::BTreeMap<Option<String>, (usize, usize)> =
        std::collections::BTreeMap::new();
    for snap in &snapshots {
        let entry = by_pool.entry(snap.pool.clone()).or_insert((0, 0));
        entry.0 += 1;
        if is_available(snap, now) {
            entry.1 += 1;
        }
    }
    let unpooled = by_pool.remove(&None);
    let mut pools: Vec<PoolOverviewView> = by_pool
        .into_iter()
        .map(|(pool, (accounts, available))| PoolOverviewView {
            pool,
            accounts,
            available,
        })
        .collect();
    if let Some((accounts, available)) = unpooled {
        pools.push(PoolOverviewView {
            pool: None,
            accounts,
            available,
        });
    }

    // Quota: group by provider, taking the MINIMUM remaining-percent (i.e. the account closest to
    // exhausted) across each provider's accounts per window.
    let mut by_provider: std::collections::BTreeMap<String, (f64, f64)> =
        std::collections::BTreeMap::new();
    for snap in &snapshots {
        let remaining_5h = 100.0 - snap.used_percent;
        let remaining_weekly = 100.0 - snap.secondary_used_percent;
        by_provider
            .entry(snap.provider.to_string())
            .and_modify(|(five, weekly)| {
                *five = five.min(remaining_5h);
                *weekly = weekly.min(remaining_weekly);
            })
            .or_insert((remaining_5h, remaining_weekly));
    }
    let quota = by_provider
        .into_iter()
        .map(|(provider, (five_hour, weekly))| ProviderQuotaView {
            provider,
            five_hour,
            weekly,
        })
        .collect();

    Response::ok(OverviewView {
        kpis,
        quota,
        pools,
        accounts_available,
        recent_errors,
    })
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
