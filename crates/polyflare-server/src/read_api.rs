//! Read-only JSON API backing the dashboard: pools, accounts (with their live rate-limit windows +
//! reset times), recent request-log rows, and continuity sessions (session→account affinity). This
//! surface is admin-facing and returns non-secret account METADATA only — id, email, pool,
//! provider, status, plan, usage percentages and reset epochs. It NEVER returns a token, refresh
//! token, or id_token (those never leave the store as plaintext except through the executor's own
//! Bearer use). No conversation content is stored, so none can be exposed here. `/api/sessions`'s
//! `session_key` is a sha256 hash of session-identifying input (see `crate::session_key`)
//! — a one-way digest, content-free, never raw header/content — so it is safe to surface as-is.
//! Like the rest of the MVP endpoints these are unauthenticated and rely on the network boundary;
//! admin auth is a follow-up.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
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
    alias: Option<String>,
    pool: Option<String>,
    provider: String,
    status: String,
    plan_type: String,
    routing_policy: String,
    security_work_authorized: bool,
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
            alias: account.alias,
            pool: account.pool,
            provider: account.provider,
            status: account.status,
            plan_type: account.plan_type,
            routing_policy: account.routing_policy,
            security_work_authorized: account.security_work_authorized,
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

/// `AccountDetailView::identity`: the account's non-secret identity/workspace metadata. A subset of
/// `Account`'s own fields — never a token, never `chatgpt_account_id`/`chatgpt_user_id` (those are
/// upstream-facing identifiers, not dashboard-facing).
#[derive(Serialize)]
struct AccountIdentityView {
    id: String,
    email: String,
    alias: Option<String>,
    workspace_id: Option<String>,
    workspace_label: Option<String>,
    seat_type: Option<String>,
    plan_type: String,
    provider: String,
    pool: Option<String>,
}

/// `AccountDetailView::request_totals`: how many requests this account has served (all-time) and
/// their summed token count. Derived from `RequestLogRepo::page` filtered to this account: `total`
/// is the filtered row count (accurate regardless of page size); `total_tokens` sums each returned
/// row's `total_tokens`, falling back per row to `input_tokens + output_tokens` (Task 7: `0005`
/// import/backfill rows never populate `total_tokens` itself; a missing sub-count coalesces to 0,
/// and `reasoning_tokens` is never added — it's a subset of `output_tokens`, not a third count).
/// The query fetches with an effectively unbounded limit rather than the dashboard's page-sized
/// default (see `account_detail_handler`) — this is a per-account detail read, not the hot path,
/// same tradeoff `accounts_handler` already makes for `request_count_24h`.
#[derive(Serialize)]
struct RequestTotalsView {
    request_count: i64,
    total_tokens: i64,
}

/// `GET /api/accounts/{id}` response: the dashboard's per-account detail page. Every field is
/// content-free/secret-free (see module docs) — `token_status` in particular carries only the
/// derived JWT-`exp` state, never the token (identical derivation to `AccountView::token_health`).
#[derive(Serialize)]
struct AccountDetailView {
    identity: AccountIdentityView,
    status: String,
    /// Adaptive per-window usage, same shape/derivation as `AccountView::usage`.
    quota_windows: Vec<UsageWindowView>,
    token_status: TokenHealthView,
    routing_policy: String,
    security_work_authorized: bool,
    request_totals: RequestTotalsView,
}

/// Upper bound on rows fetched from `request_log` when summing an account's lifetime
/// `total_tokens` (see `RequestTotalsView`). Large enough that no real account's history is
/// truncated at MVP scale; revisit with a dedicated per-account aggregate query if that changes.
const ACCOUNT_REQUEST_TOTALS_LIMIT: i64 = i64::MAX;

/// `GET /api/accounts/{id}` — the dashboard's per-account detail view: identity, status, adaptive
/// quota-window usage, secret-safe token status, routing policy, `security_work_authorized`, and
/// lifetime request totals. `404` when `id` doesn't name an existing account.
pub async fn account_detail_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> axum::response::Response {
    let repo = state.store.accounts();
    let account = match repo.get(&id).await {
        Ok(Some(a)) => a,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    };

    let now = unix_now();

    // Quota windows: identical derivation to `accounts_handler`'s `usage` field.
    let usage = repo.latest_usage(&id).await.unwrap_or_default();
    let resolved = resolve(&usage, now);
    let mut quota_windows = Vec::with_capacity(2);
    if let Some(w) = &resolved.five_hour {
        quota_windows.push(UsageWindowView {
            window: "five_hour",
            used_percent: w.used_percent,
            reset_at: w.reset_at,
        });
    }
    if let Some(w) = &resolved.weekly {
        quota_windows.push(UsageWindowView {
            window: "weekly",
            used_percent: w.used_percent,
            reset_at: w.reset_at,
        });
    }

    // Token status: derived ONLY from the access token's own unverified JWT `exp` — the token
    // itself never leaves `get_with_tokens`'s scope here (identical pattern to `accounts_handler`).
    let token_status = match repo.get_with_tokens(&id, &state.cipher).await {
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

    // Lifetime request totals for this account (see `RequestTotalsView`'s docs on the tradeoff).
    let (rows, total) = state
        .store
        .request_log()
        .page(
            &RequestsFilter {
                account: Some(id.clone()),
                ..Default::default()
            },
            ACCOUNT_REQUEST_TOTALS_LIMIT,
            0,
        )
        .await
        .unwrap_or_default();
    let request_totals = RequestTotalsView {
        request_count: total as i64,
        total_tokens: rows
            .iter()
            .map(|r| {
                r.total_tokens
                    .unwrap_or_else(|| r.input_tokens.unwrap_or(0) + r.output_tokens.unwrap_or(0))
            })
            .sum(),
    };

    Json(AccountDetailView {
        identity: AccountIdentityView {
            id: account.id,
            email: account.email,
            alias: account.alias,
            workspace_id: account.workspace_id,
            workspace_label: account.workspace_label,
            seat_type: account.seat_type,
            plan_type: account.plan_type,
            provider: account.provider,
            pool: account.pool,
        },
        status: account.status,
        quota_windows,
        token_status,
        routing_policy: account.routing_policy,
        security_work_authorized: account.security_work_authorized,
        request_totals,
    })
    .into_response()
}

/// One point of an [`TrendsView`] series: when (`recorded_at`, unix seconds) and how full the
/// window was (`used_percent`) at that moment. Content-free by construction — no request/response
/// data, only a timestamp + percentage.
#[derive(Serialize)]
struct Point {
    t: i64,
    v: f64,
}

/// `GET /api/accounts/{id}/trends` response: the account's 7-day usage history split by window,
/// ordered oldest-first. An account with no `usage_history` rows in range gets empty arrays (still
/// `200`, not `404` — the account may simply be quiet, not missing). The `secondaryScheduled` plan
/// line is out of scope here (a later phase). `forecast` (D16 T5) is the secondary-window EWMA
/// depletion forecast rebuilt from the full history — `None` when there are fewer than 2 samples,
/// the rate never establishes, or the window has already reset. Content-free: numeric fields + a
/// `RiskLevel` enum only (see `polyflare_core::depletion::DepletionForecast`).
#[derive(Serialize)]
struct TrendsView {
    account_id: String,
    primary: Vec<Point>,
    secondary: Vec<Point>,
    forecast: Option<polyflare_core::depletion::DepletionForecast>,
}

/// Lookback for `/api/accounts/{id}/trends`: 7 days.
const TRENDS_LOOKBACK_SECS: i64 = 7 * 24 * 3600;

/// `GET /api/accounts/{id}/trends` — the dashboard's per-account 7-day usage trend: `primary`/
/// `secondary` point series (`{t, v}`) straight off `usage_history`, ordered oldest-first. Always
/// `200`, even for an account with no history (empty series) — this endpoint doesn't validate that
/// `id` names an existing account, since an empty trend is a valid answer either way.
pub async fn account_trends_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let now = unix_now();
    let rows = match state
        .store
        .accounts()
        .usage_history_since(&id, now - TRENDS_LOOKBACK_SECS)
        .await
    {
        Ok(rows) => rows,
        Err(_) => return Response::error(),
    };

    let mut primary = Vec::new();
    let mut secondary = Vec::new();
    for (recorded_at, window, used_percent) in rows {
        let point = Point {
            t: recorded_at,
            v: used_percent,
        };
        match window.as_str() {
            "primary" => primary.push(point),
            "secondary" => secondary.push(point),
            _ => {}
        }
    }

    // Build the per-account secondary-window depletion forecast (content-free) from the FULL
    // history — `usage_history_since` above drops `reset_at`/`window_minutes`, which the EWMA
    // assembler needs, so this re-queries via `usage_history_full_since` (same lookback).
    let full_rows = state
        .store
        .accounts()
        .usage_history_full_since(&id, now - TRENDS_LOOKBACK_SECS)
        .await
        .unwrap_or_default();
    let samples: Vec<polyflare_core::depletion::UsageSample> = full_rows
        .iter()
        .filter(|(w, _)| w == "secondary")
        .map(|(_, u)| polyflare_core::depletion::UsageSample {
            used_percent: u.used_percent,
            reset_at: u.reset_at,
            window_minutes: u.window_minutes,
            recorded_at: u.recorded_at,
        })
        .collect();
    let forecast = polyflare_core::depletion::compute_depletion_for_account(&samples, now);

    Response::ok(TrendsView {
        account_id: id,
        primary,
        secondary,
        forecast,
    })
}

/// One pool as the dashboard lists it. `pool = null` is the unpooled group (accounts reachable only
/// via the bare ingress paths). `active` counts accounts whose DURABLE status is `active`;
/// `available` narrows that further to accounts also not currently benched by the live runtime
/// overlay (see [`is_available`]) — the same eligibility rule `/api/overview` uses, scoped to this
/// pool. `usage_percent` is the mean primary-window `used_percent` across the pool's accounts (0.0
/// for an empty pool). `strategy` is the pool's configured routing-selector name (`AppState::
/// selector_for`) — the global default when the pool has no override.
#[derive(Serialize)]
struct PoolView {
    pool: Option<String>,
    accounts: usize,
    active: usize,
    available: usize,
    usage_percent: f64,
    strategy: String,
}

/// `GET /api/pools` — the configured pools with account/active/available counts, mean usage, and
/// routing strategy, aggregated from the live account-cache snapshots (overlaid with runtime
/// cooldown state, same source `/api/overview` reads). Sorted with the unpooled group last, named
/// pools alphabetically before it.
pub async fn pools_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let now = unix_now();
    let snapshots = match state.account_cache.snapshots(&state.store).await {
        Ok(s) => s,
        Err(_) => return Response::error(),
    };
    // The cache's `Arc<Vec<..>>` is shared; the runtime overlay mutates in place, so clone the
    // slice into an owned `Vec` first (same pattern `overview_handler` uses).
    let mut snapshots = (*snapshots).clone();
    state.runtime.overlay(&mut snapshots, now);

    // (accounts, active, available, used_percent sum) per pool key. `None` collects the unpooled
    // accounts.
    let mut by_pool: std::collections::BTreeMap<Option<String>, (usize, usize, usize, f64)> =
        std::collections::BTreeMap::new();
    for snap in &snapshots {
        let entry = by_pool.entry(snap.pool.clone()).or_insert((0, 0, 0, 0.0));
        entry.0 += 1;
        if snap.status == "active" {
            entry.1 += 1;
        }
        if is_available(snap, now) {
            entry.2 += 1;
        }
        entry.3 += snap.used_percent;
    }
    // BTreeMap orders `None` before `Some(..)`; the dashboard wants named pools first, unpooled
    // last, so pull the unpooled group out and append it.
    let unpooled = by_pool.remove(&None);
    let to_view =
        |pool: Option<String>,
         (accounts, active, available, used_sum): (usize, usize, usize, f64)| {
            let usage_percent = if accounts > 0 {
                used_sum / accounts as f64
            } else {
                0.0
            };
            let strategy = state.selector_for(pool.as_deref()).name().to_string();
            PoolView {
                pool,
                accounts,
                active,
                available,
                usage_percent,
                strategy,
            }
        };
    let mut views: Vec<PoolView> = by_pool
        .into_iter()
        .map(|(pool, counts)| to_view(pool, counts))
        .collect();
    if let Some(counts) = unpooled {
        views.push(to_view(None, counts));
    }
    Response::ok(views)
}

/// `GET /api/pace` — pool-wide WeeklyCreditPace forecast (admin-gated). `{ "pace": null }` when
/// there is no eligible, fresh, positive-capacity account. Content-free: credits/percentages/hours/
/// counts + status/confidence enums only — NEVER any email or conversation content.
pub async fn pace_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let now = unix_now();
    let snapshots = match state.account_cache.snapshots(&state.store).await {
        Ok(s) => s,
        Err(_) => return Response::error(),
    };
    let mut snapshots = (*snapshots).clone();
    state.runtime.overlay(&mut snapshots, now);

    let mut inputs: Vec<polyflare_core::weekly_pace::PaceAccountInput> = Vec::new();
    for snap in &snapshots {
        // secondary-window capacity: per-account override else plan-derived (same rule select.rs uses).
        let full_credits = snap
            .capacity_credits
            .unwrap_or_else(|| polyflare_core::select::plan_capacity_secondary(&snap.plan_type));
        // latest secondary usage for reset_at + window_minutes; full history for burn/smoothing.
        // One extra SELECT per account — acceptable on this admin-gated, human-triggered read (not
        // the hot proxy path); do NOT add caching here (see task notes).
        let full_rows = state
            .store
            .accounts()
            .usage_history_full_since(
                snap.id.as_str(),
                now - polyflare_core::weekly_pace::RECENT_BURN_WINDOW_SECS,
            )
            .await
            .unwrap_or_default();
        let secondary_history: Vec<polyflare_core::depletion::UsageSample> = full_rows
            .iter()
            .filter(|(w, _)| w == "secondary")
            .map(|(_, u)| polyflare_core::depletion::UsageSample {
                used_percent: u.used_percent,
                reset_at: u.reset_at,
                window_minutes: u.window_minutes,
                recorded_at: u.recorded_at,
            })
            .collect();
        let latest = state
            .store
            .accounts()
            .latest_usage(snap.id.as_str())
            .await
            .ok()
            .and_then(|u| u.secondary);
        let (reset_at, window_minutes) = match latest {
            Some(w) => (w.reset_at, w.window_minutes),
            None => (None, None),
        };
        inputs.push(polyflare_core::weekly_pace::PaceAccountInput {
            account_id: snap.id.as_str().to_string(),
            status_eligible: matches!(
                snap.status.as_str(),
                "active" | "rate_limited" | "quota_exceeded"
            ),
            full_credits,
            used_percent: snap.secondary_used_percent,
            reset_at,
            window_minutes,
            secondary_history,
        });
    }

    // 600s = the usage poller's REFRESH_INTERVAL; 30 = codex-lb's default smoothing window.
    let report = polyflare_core::weekly_pace::build_weekly_credit_pace(&inputs, now, 600, 30);
    Response::ok(PaceView { pace: report })
}

#[derive(Serialize)]
struct PaceView {
    pace: Option<polyflare_core::weekly_pace::WeeklyCreditPaceReport>,
}

/// One request-log row for the dashboard: content-free by construction (the same audited field set
/// the tracing event carries — method/path/provider/status/latency plus the content-free per-request
/// metrics — never a body or identity). `total_tokens` falls back to `input_tokens + output_tokens`
/// when `total_tokens` itself is null (Task 7: `0005` import/backfill rows never populate it; a
/// missing sub-count coalesces to 0, and `reasoning_tokens` is never added — it's a subset of
/// `output_tokens`). `tps` is derived, not stored: that (fallback) `total_tokens` over the
/// generation window (`duration_ms - ttft_ms`, seconds — `ttft_ms` itself falling back to
/// `latency_first_token_ms` for the same import/backfill rows), present only when both source
/// fields are present and the window is positive. The row's own serialized `ttft_ms` field is
/// NOT backfilled this way — only used internally to derive `tps`.
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
    subagent: Option<String>,
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
        .map(|r| {
            // Task 7: fall back to input+output when total_tokens itself is null (never add
            // reasoning_tokens — it's a subset of output_tokens, not a third count), and to
            // latency_first_token_ms when ttft_ms is null, purely for the tps derivation below.
            let total_tokens = r
                .total_tokens
                .or_else(|| match (r.input_tokens, r.output_tokens) {
                    (None, None) => None,
                    (i, o) => Some(i.unwrap_or(0) + o.unwrap_or(0)),
                });
            let tps_ttft_ms = r.ttft_ms.or(r.latency_first_token_ms);
            RequestRowView {
                id: r.id,
                requested_at: r.requested_at,
                provider: r.provider,
                method: r.method,
                path: r.path,
                aliased: r.aliased,
                status: r.status,
                duration_ms: r.duration_ms,
                tps: derive_tps(r.duration_ms, tps_ttft_ms, total_tokens),
                account_id: r.account_id,
                model: r.model,
                reasoning_effort: r.reasoning_effort,
                service_tier: r.service_tier,
                transport: r.transport,
                ttft_ms: r.ttft_ms,
                total_tokens,
                cached_tokens: r.cached_tokens,
                subagent: r.subagent,
            }
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

/// Bucket width for `GET /api/overview/series`: hourly, matching the 24h landing-page convention
/// [`OVERVIEW_KPI_WINDOW_SECS`] already sets. Not client-configurable today — same YAGNI rationale
/// as `OVERVIEW_KPI_WINDOW_SECS`: the brief calls for a fixed 24h/1h default, not a query-param
/// surface with no real consumer yet.
const OVERVIEW_SERIES_BUCKET_SECS: i64 = 3600;

/// One bucket of `OverviewSeriesView.buckets`. Mirrors `polyflare_store::RequestBucket`
/// field-for-field — every value is a count, a timestamp, or an averaged metric, never content.
#[derive(Serialize)]
struct SeriesBucketView {
    ts: i64,
    requests: i64,
    errors: i64,
    avg_latency_ms: f64,
    total_tokens: i64,
}

impl From<polyflare_store::RequestBucket> for SeriesBucketView {
    fn from(b: polyflare_store::RequestBucket) -> Self {
        SeriesBucketView {
            ts: b.ts,
            requests: b.requests,
            errors: b.errors,
            avg_latency_ms: b.avg_latency_ms,
            total_tokens: b.total_tokens,
        }
    }
}

#[derive(Serialize)]
struct OverviewSeriesView {
    bucket_secs: i64,
    /// Ascending by `ts`, one entry per bucket across the WHOLE `[since_ts, now]` grid — zero-filled
    /// (see [`overview_series_handler`]), so this never has a hole for the chart to special-case.
    buckets: Vec<SeriesBucketView>,
}

/// `GET /api/overview/series` — the dashboard overview's request-volume chart: a rolling
/// [`OVERVIEW_KPI_WINDOW_SECS`] window bucketed into [`OVERVIEW_SERIES_BUCKET_SECS`]-wide buckets,
/// oldest first. `polyflare_store::RequestLogRepo::series_since` only emits buckets that have rows;
/// this handler zero-fills every other bucket in the aligned `[since_ts, now]` grid so the response
/// is always a complete, gap-free series — the ONLY place that zero-fill happens.
pub async fn overview_series_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let now = unix_now();
    let bucket_secs = OVERVIEW_SERIES_BUCKET_SECS;
    let since_ts = now - OVERVIEW_KPI_WINDOW_SECS;

    let rows = match state
        .store
        .request_log()
        .series_since(since_ts, bucket_secs)
        .await
    {
        Ok(r) => r,
        Err(_) => return Response::error(),
    };
    let mut by_ts: std::collections::BTreeMap<i64, polyflare_store::RequestBucket> =
        rows.into_iter().map(|b| (b.ts, b)).collect();

    // Zero-fill the full grid from the aligned window start through the aligned "now" bucket, so a
    // window with sparse (or zero) traffic still renders as a continuous series.
    let aligned_start = (since_ts / bucket_secs) * bucket_secs;
    let aligned_now = (now / bucket_secs) * bucket_secs;
    let mut buckets = Vec::new();
    let mut ts = aligned_start;
    while ts <= aligned_now {
        let bucket = by_ts.remove(&ts).unwrap_or(polyflare_store::RequestBucket {
            ts,
            requests: 0,
            errors: 0,
            avg_latency_ms: 0.0,
            total_tokens: 0,
        });
        buckets.push(SeriesBucketView::from(bucket));
        ts += bucket_secs;
    }

    Response::ok(OverviewSeriesView {
        bucket_secs,
        buckets,
    })
}

/// One `continuity_sessions` row for the dashboard, LEFT JOINed to its owning account's email
/// (TA6(c): session→account affinity visibility). `session_key` is a sha256 hash — opaque,
/// content-free (see module docs). `owning_account_id`/`owner_email` are `null` for a session that
/// never completed a turn (`state == "fresh"`) or whose owning account was deleted (`ON DELETE
/// SET NULL`) — that is NOT dropped, it's a valid row. `required_capabilities` is the TA6(b)
/// sticky-cyber capability tag set (comma-separated), or `null` when none is stamped.
#[derive(Serialize)]
struct SessionRowView {
    session_key: String,
    key_strength: String,
    owning_account_id: Option<String>,
    owner_email: Option<String>,
    state: String,
    required_capabilities: Option<String>,
    created_at: i64,
    updated_at: i64,
    last_activity_at: i64,
}

impl From<polyflare_store::continuity_repo::SessionWithOwner> for SessionRowView {
    fn from(s: polyflare_store::continuity_repo::SessionWithOwner) -> Self {
        SessionRowView {
            session_key: s.session_key,
            key_strength: s.key_strength,
            owning_account_id: s.owning_account_id,
            owner_email: s.owner_email,
            state: s.state,
            required_capabilities: s.required_capabilities,
            created_at: s.created_at,
            updated_at: s.updated_at,
            last_activity_at: s.last_activity_at,
        }
    }
}

/// `GET /api/sessions` pagination. Mirrors `RequestsQuery` exactly: `limit` clamps to
/// `[1, MAX_LIMIT]` (default `DEFAULT_LIMIT`), `offset` defaults to 0 and is floored at 0.
#[derive(Deserialize)]
pub struct SessionsQuery {
    limit: Option<i64>,
    offset: Option<i64>,
}

#[derive(Serialize)]
struct SessionsView {
    /// Total rows in `continuity_sessions` (for the client to paginate); `rows` is one page.
    total: i64,
    rows: Vec<SessionRowView>,
}

/// `GET /api/sessions?limit=&offset=` — paginated `continuity_sessions` rows (most-recently-active
/// first) LEFT JOINed to their owning account's email, plus the total row count. Content-free by
/// construction (see module docs): no anchor id, input fingerprint, or reasoning-cache reference is
/// ever surfaced. `ContinuityRepo::list_sessions_with_owner`/`count_sessions` return `sqlx::Error`
/// (not the crate's usual `StoreError`) — a query failure maps to the same generic content-safe
/// `500` every other handler here uses, never the store error's own text.
pub async fn sessions_handler(
    State(state): State<Arc<AppState>>,
    Query(q): Query<SessionsQuery>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let offset = q.offset.unwrap_or(0).max(0);
    let repo = state.store.continuity();
    let rows = match repo.list_sessions_with_owner(limit, offset).await {
        Ok(rows) => rows,
        Err(_) => return Response::error(),
    };
    let total = match repo.count_sessions().await {
        Ok(total) => total,
        Err(_) => return Response::error(),
    };
    Response::ok(SessionsView {
        total,
        rows: rows.into_iter().map(SessionRowView::from).collect(),
    })
}

/// `GET /api/reports`'s `range` query param resolves to a `(since_ts lookback, bucket_secs)` pair.
/// `24h` buckets hourly (matching [`OVERVIEW_SERIES_BUCKET_SECS`]'s convention); `7d`/`30d` bucket
/// daily — an hourly grid over 30 days would be 720 points, finer than a report chart needs.
const REPORTS_RANGE_24H_LOOKBACK_SECS: i64 = 24 * 3600;
const REPORTS_RANGE_7D_LOOKBACK_SECS: i64 = 7 * 24 * 3600;
const REPORTS_RANGE_30D_LOOKBACK_SECS: i64 = 30 * 24 * 3600;
const REPORTS_BUCKET_HOURLY_SECS: i64 = 3600;
const REPORTS_BUCKET_DAILY_SECS: i64 = 24 * 3600;

/// `GET /api/reports` query params. `range` ∈ {`24h`,`7d`,`30d`}: ABSENT defaults to `7d`; an
/// explicit value outside that set is a `400` (see [`reports_handler`]), never silently
/// defaulted. `dimension` ∈ {`account`,`model`,`provider`} follows the identical absent-defaults/
/// explicit-invalid-400 rule, defaulting to `model`. `provider` (unvalidated — any string) narrows
/// all three store calls to that provider, same as `/api/requests`' own `provider` filter.
#[derive(Deserialize)]
pub struct ReportsQuery {
    range: Option<String>,
    dimension: Option<String>,
    provider: Option<String>,
}

/// One bucket of `ReportsView::time_series`. Mirrors [`SeriesBucketView`]'s flat-field style:
/// every [`polyflare_store::ReportMetrics`] field is serialized directly on the bucket, never
/// nested under a `metrics` key.
#[derive(Serialize)]
struct ReportBucketView {
    ts: i64,
    requests: i64,
    errors: i64,
    cost_usd: f64,
    tokens: i64,
    cached_tokens: i64,
    reasoning_tokens: i64,
    avg_duration_ms: f64,
    avg_ttft_ms: f64,
    ttft_sample_count: i64,
}

impl From<polyflare_store::ReportBucket> for ReportBucketView {
    fn from(b: polyflare_store::ReportBucket) -> Self {
        ReportBucketView {
            ts: b.ts,
            requests: b.metrics.requests,
            errors: b.metrics.errors,
            cost_usd: b.metrics.cost_usd,
            tokens: b.metrics.tokens,
            cached_tokens: b.metrics.cached_tokens,
            reasoning_tokens: b.metrics.reasoning_tokens,
            avg_duration_ms: b.metrics.avg_duration_ms,
            avg_ttft_ms: b.metrics.avg_ttft_ms,
            ttft_sample_count: b.metrics.ttft_sample_count,
        }
    }
}

/// One row of `ReportsView::breakdown`: [`polyflare_store::ReportMetrics`] scoped to one value of
/// the requested `dimension` (flat-field style, same as [`ReportBucketView`]).
#[derive(Serialize)]
struct ReportBreakdownView {
    key: String,
    requests: i64,
    errors: i64,
    cost_usd: f64,
    tokens: i64,
    cached_tokens: i64,
    reasoning_tokens: i64,
    avg_duration_ms: f64,
    avg_ttft_ms: f64,
    ttft_sample_count: i64,
}

impl From<polyflare_store::ReportBreakdownRow> for ReportBreakdownView {
    fn from(r: polyflare_store::ReportBreakdownRow) -> Self {
        ReportBreakdownView {
            key: r.key,
            requests: r.metrics.requests,
            errors: r.metrics.errors,
            cost_usd: r.metrics.cost_usd,
            tokens: r.metrics.tokens,
            cached_tokens: r.metrics.cached_tokens,
            reasoning_tokens: r.metrics.reasoning_tokens,
            avg_duration_ms: r.metrics.avg_duration_ms,
            avg_ttft_ms: r.metrics.avg_ttft_ms,
            ttft_sample_count: r.metrics.ttft_sample_count,
        }
    }
}

/// `ReportsView::totals`: the same flat [`polyflare_store::ReportMetrics`] fields as
/// [`ReportBucketView`]/[`ReportBreakdownView`], plus two derived ratios the dashboard's KPI tiles
/// want directly rather than re-deriving client-side: `error_rate` (`errors / requests`, `0.0`
/// when `requests == 0`) and `cache_hit_rate` (`cached_tokens / tokens`, `0.0` when `tokens == 0`)
/// — both guarded against a 0/0 divide the same way `KpisView::success_rate` already is.
#[derive(Serialize)]
struct ReportTotalsView {
    requests: i64,
    errors: i64,
    cost_usd: f64,
    tokens: i64,
    cached_tokens: i64,
    reasoning_tokens: i64,
    avg_duration_ms: f64,
    avg_ttft_ms: f64,
    ttft_sample_count: i64,
    error_rate: f64,
    cache_hit_rate: f64,
}

impl From<polyflare_store::ReportMetrics> for ReportTotalsView {
    fn from(m: polyflare_store::ReportMetrics) -> Self {
        let error_rate = if m.requests > 0 {
            m.errors as f64 / m.requests as f64
        } else {
            0.0
        };
        let cache_hit_rate = if m.tokens > 0 {
            m.cached_tokens as f64 / m.tokens as f64
        } else {
            0.0
        };
        ReportTotalsView {
            requests: m.requests,
            errors: m.errors,
            cost_usd: m.cost_usd,
            tokens: m.tokens,
            cached_tokens: m.cached_tokens,
            reasoning_tokens: m.reasoning_tokens,
            avg_duration_ms: m.avg_duration_ms,
            avg_ttft_ms: m.avg_ttft_ms,
            ttft_sample_count: m.ttft_sample_count,
            error_rate,
            cache_hit_rate,
        }
    }
}

/// `GET /api/reports` response: the dashboard Reports page's composite payload — a zero-filled
/// time series, a per-dimension breakdown, and top-line totals, all sourced from the SAME
/// `since_ts`/`provider` window (see [`reports_handler`]).
#[derive(Serialize)]
struct ReportsView {
    time_series: Vec<ReportBucketView>,
    breakdown: Vec<ReportBreakdownView>,
    totals: ReportTotalsView,
}

/// `GET /api/reports?range=&dimension=&provider=` — the dashboard Reports page's composite
/// analytics endpoint: assembles [`polyflare_store::RequestLogRepo::reports_totals`]/
/// `reports_series`/`reports_breakdown` into one payload over a shared `(since_ts, bucket_secs)`
/// window resolved from `range`.
///
/// `range` ∈ {`24h`,`7d`,`30d`} (absent → `7d`) and `dimension` ∈ {`account`,`model`,`provider`}
/// (absent → `model`); an EXPLICIT value outside those sets is a `400`, never silently defaulted
/// (only absence defaults) — the same "unknown-but-present is a client error" posture as
/// `/api/requests`' `status_class` would if it validated (it currently doesn't, but this endpoint
/// does, per its own brief). `provider` is passed through unvalidated, same as elsewhere in this
/// module.
///
/// `time_series` is ZERO-FILLED across the aligned `[since_ts, now]` grid at `bucket_secs` — the
/// store's `reports_series` only emits buckets with >= 1 row, same contract `series_since` has;
/// this handler fills the gaps, mirroring [`overview_series_handler`]'s zero-fill exactly (same
/// aligned-grid-walk, same "remove-from-map-or-default" pattern).
pub async fn reports_handler(
    State(state): State<Arc<AppState>>,
    Query(q): Query<ReportsQuery>,
) -> axum::response::Response {
    let now = unix_now();

    let range = q.range.as_deref().unwrap_or("7d");
    let (since_ts, bucket_secs) = match range {
        "24h" => (
            now - REPORTS_RANGE_24H_LOOKBACK_SECS,
            REPORTS_BUCKET_HOURLY_SECS,
        ),
        "7d" => (
            now - REPORTS_RANGE_7D_LOOKBACK_SECS,
            REPORTS_BUCKET_DAILY_SECS,
        ),
        "30d" => (
            now - REPORTS_RANGE_30D_LOOKBACK_SECS,
            REPORTS_BUCKET_DAILY_SECS,
        ),
        _ => return (StatusCode::BAD_REQUEST, "invalid range").into_response(),
    };

    let dimension = q.dimension.as_deref().unwrap_or("model");
    if !matches!(dimension, "account" | "model" | "provider") {
        return (StatusCode::BAD_REQUEST, "invalid dimension").into_response();
    }

    let provider = q.provider.as_deref();
    let repo = state.store.request_log();

    let totals = match repo.reports_totals(since_ts, provider).await {
        Ok(t) => t,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    };
    let series_rows = match repo.reports_series(since_ts, bucket_secs, provider).await {
        Ok(r) => r,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    };
    let breakdown_rows = match repo.reports_breakdown(since_ts, dimension, provider).await {
        Ok(r) => r,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    };

    // Zero-fill the full grid from the aligned window start through the aligned "now" bucket —
    // byte-for-byte the same walk `overview_series_handler` does, just over `ReportBucket`/
    // `ReportMetrics` instead of `RequestBucket`.
    let mut by_ts: std::collections::BTreeMap<i64, polyflare_store::ReportBucket> =
        series_rows.into_iter().map(|b| (b.ts, b)).collect();
    let aligned_start = (since_ts / bucket_secs) * bucket_secs;
    let aligned_now = (now / bucket_secs) * bucket_secs;
    let mut time_series = Vec::new();
    let mut ts = aligned_start;
    while ts <= aligned_now {
        let bucket = by_ts.remove(&ts).unwrap_or(polyflare_store::ReportBucket {
            ts,
            metrics: polyflare_store::ReportMetrics::default(),
        });
        time_series.push(ReportBucketView::from(bucket));
        ts += bucket_secs;
    }

    let breakdown = breakdown_rows
        .into_iter()
        .map(ReportBreakdownView::from)
        .collect();

    Json(ReportsView {
        time_series,
        breakdown,
        totals: ReportTotalsView::from(totals),
    })
    .into_response()
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
