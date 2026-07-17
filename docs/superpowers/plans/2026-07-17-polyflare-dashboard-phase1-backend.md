# PolyFlare Dashboard — Phase 1 Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Phase-1 backend for the PolyFlare dashboard — admin-token auth, the content-free request-log extension, the observability/aggregate read endpoints, and the SSE live-log stream — so the React SPA (a separate plan) has a complete, tested API to consume.

**Architecture:** Extend the existing axum server (`crates/polyflare-server`). Add an auth middleware layer over `/api/*`, one content-free SQLite migration extending `request_log`, new/extended read handlers in `read_api.rs`, and a live-log bus (a `tokio::sync::broadcast` channel + ring buffer) published from the existing `observability::RequestLog` content-safety chokepoint and drained by an SSE handler. No conversation content is ever stored, streamed, or returned.

**Tech Stack:** Rust, axum 0.8, tokio, sqlx (SQLite), the existing `polyflare-store` / `polyflare-server` crates, and the existing test harness (`MockUpstream` + temp `Store`, as in `crates/polyflare-server/tests/failure_routing.rs`).

## Global Constraints

- **Content-safety invariant (SPEC §8):** No request/response bodies or conversation content may be stored, streamed, or returned. Every new column, `LogEvent` field, and API field carries only outcomes, timings, counts, and routing metadata. `observability::RequestLog` stays the single chokepoint.
- **Auth:** every `/api/*` route (existing and new) is gated by `POLYFLARE_ADMIN_TOKEN` via `Authorization: Bearer <token>`. When the token env var is unset, `/api/*` returns `503` (dashboard disabled).
- **Live-logs flag:** `POLYFLARE_LIVE_LOGS` (values `1`/`true` = on) gates `GET /api/logs/stream`; when off it returns `404` and `/api/capabilities` reports `live_logs=false`.
- **Config pattern:** all env reads go through `crates/polyflare-server/src/config.rs` `Config::from_env` (existing `POLYFLARE_*` convention); do not scatter `std::env::var` in handlers.
- **Zero warnings:** `cargo clippy --all-targets` must be clean; run `cargo fmt` before every commit.
- **Test harness:** integration tests build a real `AppState` + `build_app` + `TcpListener` exactly as `tests/failure_routing.rs::spawn` does; reuse that shape (copy the `account()` + `spawn()` helpers into a shared `tests/support/mod.rs` in Task 1 and import it thereafter).
- **Provider values:** `provider` is `"codex"` | `"anthropic"` in the store; the dashboard labels anthropic as "claude" in the FRONTEND only — the API returns the raw store value.

---

## File Structure

- `crates/polyflare-store/migrations/0007_request_log_metrics.sql` — **new**: content-free columns on `request_log`.
- `crates/polyflare-store/src/request_log_repo.rs` — **modify**: extend `RequestLogRecord` + insert + add the filtered/paginated query used by `/api/requests`.
- `crates/polyflare-server/src/observability.rs` — **modify**: `RequestLog` carries + records the new content-free metrics and publishes a `LogEvent`.
- `crates/polyflare-server/src/config.rs` — **modify**: parse `POLYFLARE_ADMIN_TOKEN` + `POLYFLARE_LIVE_LOGS`.
- `crates/polyflare-server/src/auth.rs` — **new**: the admin-token middleware + `whoami` + `capabilities` handlers.
- `crates/polyflare-server/src/log_bus.rs` — **new**: `LogEvent`, the broadcast channel, and the bounded ring buffer.
- `crates/polyflare-server/src/sse.rs` — **new**: `GET /api/logs/stream` (flag-gated SSE).
- `crates/polyflare-server/src/read_api.rs` — **modify**: extend `AccountView`, `pools_handler`, `requests_handler`; add `overview_handler`, `account_detail_handler`, `account_trends_handler`.
- `crates/polyflare-server/src/app.rs` — **modify**: register new routes; wrap `/api/*` in the auth layer; add the log bus to `AppState`.
- `crates/polyflare-server/tests/support/mod.rs` — **new**: shared `spawn`/`account` test helpers.
- `crates/polyflare-server/tests/dashboard_api.rs` — **new**: integration tests for all endpoints.

---

## Task 1: request_log content-free metrics migration + record extension

**Files:**
- Create: `crates/polyflare-store/migrations/0007_request_log_metrics.sql`
- Modify: `crates/polyflare-store/src/request_log_repo.rs` (the `RequestLogRecord` struct ~line 20, its `insert` SQL, and `RequestLogRow` FromRow ~line 40)
- Test: `crates/polyflare-store/src/request_log_repo.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: existing `RequestLogRepo`, `RequestLogRecord { requested_at, provider, method, path, aliased, status, duration_ms }`.
- Produces: `RequestLogRecord` gains content-free `Option` fields `account_id: Option<String>`, `model: Option<String>`, `reasoning_effort: Option<String>`, `service_tier: Option<String>`, `transport: Option<String>`, `ttft_ms: Option<i64>`, `total_tokens: Option<i64>`, `cached_tokens: Option<i64>`. `RequestLogRow` gains the same. `RequestLogRepo::insert` persists them.

- [ ] **Step 1: Write the migration**

```sql
-- 0007_request_log_metrics.sql
-- Content-free per-request metrics for the dashboard's Requests view + Overview KPIs.
-- Every column here is an outcome/metric/identifier — NEVER conversation content.
ALTER TABLE request_log ADD COLUMN account_id       TEXT;
ALTER TABLE request_log ADD COLUMN model            TEXT;
ALTER TABLE request_log ADD COLUMN reasoning_effort TEXT;
ALTER TABLE request_log ADD COLUMN service_tier     TEXT;
ALTER TABLE request_log ADD COLUMN transport        TEXT;
ALTER TABLE request_log ADD COLUMN ttft_ms          INTEGER;
ALTER TABLE request_log ADD COLUMN total_tokens     INTEGER;
ALTER TABLE request_log ADD COLUMN cached_tokens    INTEGER;
```

- [ ] **Step 2: Write the failing test** (append to `request_log_repo.rs` tests)

```rust
#[tokio::test]
async fn insert_and_read_back_content_free_metrics() {
    let store = crate::Store::open_in_memory().await.unwrap(); // or the temp-file open helper used elsewhere
    let repo = store.request_log();
    let rec = RequestLogRecord {
        requested_at: 100, provider: "codex".into(), method: "POST".into(),
        path: "/responses".into(), aliased: false, status: 200, duration_ms: 1800,
        account_id: Some("acct-1".into()), model: Some("gpt-5.6-sol".into()),
        reasoning_effort: Some("high".into()), service_tier: Some("priority".into()),
        transport: Some("http".into()), ttft_ms: Some(700),
        total_tokens: Some(3204), cached_tokens: Some(1100),
    };
    repo.insert(&rec).await.unwrap();
    let (rows, total) = repo.page(Default::default(), 10, 0).await.unwrap();
    assert_eq!(total, 1);
    assert_eq!(rows[0].model.as_deref(), Some("gpt-5.6-sol"));
    assert_eq!(rows[0].total_tokens, Some(3204));
    assert_eq!(rows[0].account_id.as_deref(), Some("acct-1"));
}
```

*(If `Store::open_in_memory` does not exist, use the same `tempfile` + `Store::open` pattern as `tests/failure_routing.rs`. `repo.page` is added in Task 11; for this task assert via the existing `recent`/list method the repo already exposes and add the `page` assertion when Task 11 lands — or stub `page` returning the recent rows now.)*

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test -p polyflare-store insert_and_read_back_content_free_metrics`
Expected: FAIL — `RequestLogRecord` has no field `account_id`.

- [ ] **Step 4: Extend `RequestLogRecord` + `RequestLogRow` + `insert`**

Add the eight `Option` fields (documented as content-free) to both structs. Update the `INSERT INTO request_log (...)` column list + bind list to include them (bind `Option` directly — sqlx maps `None`→`NULL`). Update `SELECT` column lists to include the new columns so `FromRow` populates them.

- [ ] **Step 5: Run migration + test to verify pass**

Run: `cargo test -p polyflare-store insert_and_read_back_content_free_metrics`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cargo fmt && git add crates/polyflare-store
git commit -m "feat(store): content-free request_log metrics columns (0007)"
```

---

## Task 2: Admin-token auth middleware + whoami

**Files:**
- Modify: `crates/polyflare-server/src/config.rs` (`Config` struct + `from_env`)
- Create: `crates/polyflare-server/src/auth.rs`
- Modify: `crates/polyflare-server/src/app.rs` (add `admin_token` to `AppState`; wrap `/api` routes in the middleware; add `/api/whoami`)
- Create: `crates/polyflare-server/tests/support/mod.rs` (shared `spawn`/`account`, copied from `failure_routing.rs`)
- Test: `crates/polyflare-server/tests/dashboard_api.rs`

**Interfaces:**
- Consumes: `AppState`, `build_app`.
- Produces: `AppState.admin_token: Option<String>`; `auth::require_admin` (an axum `from_fn_with_state` middleware); `auth::whoami_handler`. Every `/api/*` route sits behind `require_admin`.

- [ ] **Step 1: Copy the shared test harness** into `tests/support/mod.rs` — the `account(id)` builder and `spawn(upstream_url) -> (String, Arc<AppState>)` from `tests/failure_routing.rs`, adding `admin_token: Some("secret".into())` to the `AppState` literal. Add `mod support;` to `tests/dashboard_api.rs`.

- [ ] **Step 2: Write the failing test**

```rust
// tests/dashboard_api.rs
mod support;
use support::spawn;

#[tokio::test]
async fn whoami_requires_admin_token() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, _state) = spawn(up).await; // admin_token = Some("secret")
    let c = reqwest::Client::new();

    let no_tok = c.get(format!("{pf}/api/whoami")).send().await.unwrap();
    assert_eq!(no_tok.status(), 401);

    let ok = c.get(format!("{pf}/api/whoami"))
        .header("authorization", "Bearer secret").send().await.unwrap();
    assert_eq!(ok.status(), 200);
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test -p polyflare-server --test dashboard_api whoami_requires_admin_token`
Expected: FAIL — `/api/whoami` route does not exist (404, not 401).

- [ ] **Step 4: Implement `auth.rs`**

```rust
use axum::{extract::State, http::{HeaderMap, StatusCode}, middleware::Next,
           response::{IntoResponse, Response}, extract::Request, Json};
use std::sync::Arc;
use crate::app::AppState;

/// Gate every `/api/*` route on `POLYFLARE_ADMIN_TOKEN`. Unset ⇒ dashboard disabled (503).
pub async fn require_admin(State(s): State<Arc<AppState>>, headers: HeaderMap,
                           req: Request, next: Next) -> Response {
    let Some(expected) = s.admin_token.as_deref() else {
        return (StatusCode::SERVICE_UNAVAILABLE, "dashboard disabled: set POLYFLARE_ADMIN_TOKEN").into_response();
    };
    let presented = headers.get("authorization").and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    // Constant-time-ish compare is unnecessary for a single local operator token, but avoid early-exit length leak.
    if presented == Some(expected) { next.run(req).await }
    else { (StatusCode::UNAUTHORIZED, "unauthorized").into_response() }
}

pub async fn whoami_handler() -> impl IntoResponse {
    Json(serde_json::json!({ "ok": true }))
}
```

Add `admin_token: Option<String>` to `AppState`. In `config.rs`, add `pub admin_token: Option<String>` to `Config` and `admin_token: std::env::var("POLYFLARE_ADMIN_TOKEN").ok()` in `from_env`. In `serve` (main.rs), thread `config.admin_token` into the `AppState` literal.

- [ ] **Step 5: Wire the middleware in `app.rs`** — split the `/api/*` routes into their own `Router` and apply the layer:

```rust
let api = Router::new()
    .route("/api/whoami", get(crate::auth::whoami_handler))
    .route("/api/capabilities", get(crate::auth::capabilities_handler)) // Task 3
    .route("/api/pools", get(crate::read_api::pools_handler))
    .route("/api/accounts", get(crate::read_api::accounts_handler))
    .route("/api/accounts/{id}", get(crate::read_api::account_detail_handler)   // Task 8
                                 .patch(crate::write_api::patch_account_handler))
    .route("/api/accounts/{id}/trends", get(crate::read_api::account_trends_handler)) // Task 9
    .route("/api/overview", get(crate::read_api::overview_handler))             // Task 6
    .route("/api/requests", get(crate::read_api::requests_handler))
    .route("/api/logs/stream", get(crate::sse::logs_stream_handler))            // Task 5
    .route_layer(axum::middleware::from_fn_with_state(state.clone(), crate::auth::require_admin));

Router::new()
    .merge(api)
    // …existing proxy + /dashboard routes unchanged…
    .with_state(state)
```

*(Register only the routes that exist as you land each task; the final route set is shown here for reference. `route_layer` applies the auth only to these `/api` routes, not the proxy paths.)*

- [ ] **Step 6: Run test to verify pass**

Run: `cargo test -p polyflare-server --test dashboard_api whoami_requires_admin_token`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
cargo fmt && git add crates/polyflare-server
git commit -m "feat(server): admin-token auth middleware + /api/whoami"
```

---

## Task 3: /api/capabilities (feature flags)

**Files:**
- Modify: `crates/polyflare-server/src/config.rs` (add `live_logs: bool`)
- Modify: `crates/polyflare-server/src/auth.rs` (`capabilities_handler`)
- Modify: `crates/polyflare-server/src/app.rs` (`AppState.live_logs: bool`)
- Test: `tests/dashboard_api.rs`

**Interfaces:**
- Consumes: `AppState`, `require_admin`.
- Produces: `AppState.live_logs: bool`; `auth::capabilities_handler` → `{ "live_logs": bool }`.

- [ ] **Step 1: Failing test**

```rust
#[tokio::test]
async fn capabilities_reports_live_logs_flag() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, _s) = spawn(up).await; // spawn sets live_logs = true for tests
    let r = reqwest::Client::new().get(format!("{pf}/api/capabilities"))
        .header("authorization","Bearer secret").send().await.unwrap();
    assert_eq!(r.status(), 200);
    let v: serde_json::Value = r.json().await.unwrap();
    assert_eq!(v["live_logs"], true);
}
```

- [ ] **Step 2: Run — expect FAIL** (`cargo test -p polyflare-server --test dashboard_api capabilities_reports_live_logs_flag`; route missing).

- [ ] **Step 3: Implement**

```rust
// auth.rs
pub async fn capabilities_handler(State(s): State<Arc<AppState>>) -> impl IntoResponse {
    Json(serde_json::json!({ "live_logs": s.live_logs }))
}
```

`config.rs`: `live_logs: matches!(std::env::var("POLYFLARE_LIVE_LOGS").as_deref(), Ok("1") | Ok("true"))`. Add `live_logs: bool` to `AppState`; set `live_logs: true` in the test `spawn` helper.

- [ ] **Step 4: Run — expect PASS.**
- [ ] **Step 5: Commit** `feat(server): /api/capabilities exposes live_logs flag`.

---

## Task 4: Log bus — LogEvent, broadcast channel, ring buffer, publish from RequestLog

**Files:**
- Create: `crates/polyflare-server/src/log_bus.rs`
- Modify: `crates/polyflare-server/src/app.rs` (`AppState.log_bus: Arc<LogBus>`)
- Modify: `crates/polyflare-server/src/observability.rs` (`RequestLog::record`/emit path publishes a `LogEvent`)
- Test: `crates/polyflare-server/src/log_bus.rs` unit test

**Interfaces:**
- Produces: `LogBus::new(capacity) -> Arc<LogBus>`; `LogBus::publish(&self, LogEvent)`; `LogBus::subscribe(&self) -> (Vec<LogEvent> /*backfill*/, broadcast::Receiver<LogEvent>)`; `LogEvent { ts_ms: i64, level: LogLevel, provider: Option<String>, account: Option<String>, model: Option<String>, status: Option<u16>, latency_ms: Option<i64>, kind: String, message: String }` (all content-free). `LogLevel { Info, Warn, Error, Debug }` (serde lowercase).

- [ ] **Step 1: Failing test**

```rust
#[tokio::test]
async fn publish_delivers_to_subscriber_and_backfills() {
    let bus = LogBus::new(16);
    bus.publish(LogEvent::info("test", "warmup line")); // pre-subscribe → ring buffer
    let (backfill, mut rx) = bus.subscribe();
    assert_eq!(backfill.len(), 1);
    bus.publish(LogEvent::info("test", "live line"));
    let got = rx.recv().await.unwrap();
    assert_eq!(got.message, "live line");
}
```

- [ ] **Step 2: Run — expect FAIL** (`cargo test -p polyflare-server --lib log_bus`; module missing).

- [ ] **Step 3: Implement `log_bus.rs`**

```rust
use std::sync::Mutex;
use tokio::sync::broadcast;

#[derive(Clone, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel { Info, Warn, Error, Debug }

#[derive(Clone, serde::Serialize)]
pub struct LogEvent {
    pub ts_ms: i64, pub level: LogLevel,
    #[serde(skip_serializing_if="Option::is_none")] pub provider: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")] pub account: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")] pub model: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")] pub status: Option<u16>,
    #[serde(skip_serializing_if="Option::is_none")] pub latency_ms: Option<i64>,
    pub kind: String, pub message: String,
}
impl LogEvent {
    pub fn info(kind: &str, message: impl Into<String>) -> Self { Self::new(LogLevel::Info, kind, message) }
    pub fn new(level: LogLevel, kind: &str, message: impl Into<String>) -> Self {
        Self { ts_ms: now_ms(), level, provider: None, account: None, model: None,
               status: None, latency_ms: None, kind: kind.to_string(), message: message.into() }
    }
}
fn now_ms() -> i64 { std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
    .map(|d| d.as_millis() as i64).unwrap_or(0) }

pub struct LogBus { tx: broadcast::Sender<LogEvent>, ring: Mutex<std::collections::VecDeque<LogEvent>>, cap: usize }
impl LogBus {
    pub fn new(cap: usize) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self { tx: broadcast::channel(1024).0, ring: Mutex::new(Default::default()), cap })
    }
    pub fn publish(&self, ev: LogEvent) {
        { let mut r = self.ring.lock().unwrap_or_else(|e| e.into_inner());
          if r.len() == self.cap { r.pop_front(); } r.push_back(ev.clone()); }
        let _ = self.tx.send(ev); // Err only when no subscribers — fine
    }
    pub fn subscribe(&self) -> (Vec<LogEvent>, broadcast::Receiver<LogEvent>) {
        let rx = self.tx.subscribe();
        let backfill = self.ring.lock().unwrap_or_else(|e| e.into_inner()).iter().cloned().collect();
        (backfill, rx)
    }
}
```

Add `pub log_bus: std::sync::Arc<crate::log_bus::LogBus>` to `AppState`; construct `LogBus::new(1000)` in `serve` + the test `spawn`.

- [ ] **Step 4: Publish from the chokepoint** — in `observability.rs`, where `RequestLog` records the outcome, also build and publish a `LogEvent` (level from status: 2xx→Info, 429/5xx→Warn, 4xx→Error) carrying provider/account/model/status/latency + `kind="request"` + a content-free message like `format!("req {status} · {provider} {model} · {latency}ms")`. Thread the `log_bus` handle into the `RequestLog` recording path (it already runs off the response path). Do NOT include any body text.

- [ ] **Step 5: Run — expect PASS** (`cargo test -p polyflare-server --lib log_bus`).
- [ ] **Step 6: Commit** `feat(server): content-free log bus (broadcast + ring buffer) published from RequestLog`.

---

## Task 5: /api/logs/stream — flag-gated SSE

**Files:**
- Create: `crates/polyflare-server/src/sse.rs`
- Modify: `app.rs` (route already added in Task 2's api router)
- Test: `tests/dashboard_api.rs`

**Interfaces:**
- Consumes: `AppState.live_logs`, `AppState.log_bus`.
- Produces: `sse::logs_stream_handler` — `404` when `!live_logs`; else `text/event-stream` of `data: <LogEvent json>\n\n` frames (backfill first, then live), with a 15s heartbeat comment.

- [ ] **Step 1: Failing test**

```rust
#[tokio::test]
async fn logs_stream_404_when_flag_off_else_streams() {
    // flag OFF variant: build a state with live_logs=false (spawn_with(live_logs:false))
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, state) = spawn(up).await; // live_logs = true
    // publish one event, then connect and read the first frame
    state.log_bus.publish(crate::_test_log_event()); // or state.log_bus.publish(LogEvent::info("t","hello"))
    let r = reqwest::Client::new().get(format!("{pf}/api/logs/stream"))
        .header("authorization","Bearer secret").send().await.unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()["content-type"], "text/event-stream");
    let chunk = r.bytes_stream().next().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&chunk).contains("hello"));
}
```

Add a sibling test that builds a `live_logs=false` state (add a `spawn_flag(live_logs: bool)` variant to `support`) and asserts `GET /api/logs/stream` → `404`.

- [ ] **Step 2: Run — expect FAIL** (handler missing).

- [ ] **Step 3: Implement `sse.rs`**

```rust
use axum::{extract::State, http::StatusCode, response::{sse::{Event, Sse}, IntoResponse, Response}};
use std::{sync::Arc, time::Duration};
use futures_util::stream::{self, Stream, StreamExt};
use crate::app::AppState;

pub async fn logs_stream_handler(State(s): State<Arc<AppState>>) -> Response {
    if !s.live_logs { return (StatusCode::NOT_FOUND, "live logs disabled").into_response(); }
    let (backfill, rx) = s.log_bus.subscribe();
    let backfill = stream::iter(backfill.into_iter().map(sse_ok));
    let live = tokio_stream::wrappers::BroadcastStream::new(rx)
        .filter_map(|r| async move { r.ok().map(sse_ok) });
    let body = backfill.chain(live);
    Sse::new(body).keep_alive(axum::response::sse::KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}
fn sse_ok(ev: crate::log_bus::LogEvent) -> Result<Event, std::convert::Infallible> {
    Ok(Event::default().data(serde_json::to_string(&ev).unwrap_or_default()))
}
```

Add the `tokio-stream` dependency (features `sync`) to `polyflare-server/Cargo.toml` if not present.

- [ ] **Step 4: Run — expect PASS** (both the 200-stream and 404-when-off tests).
- [ ] **Step 5: Commit** `feat(server): flag-gated SSE /api/logs/stream`.

---

## Task 6: /api/overview aggregates

**Files:**
- Modify: `read_api.rs` (`overview_handler` + `OverviewView`)
- Modify: `request_log_repo.rs` (add `aggregate_since(since_ts) -> RequestAggregate` — counts by status class, sum tokens, avg latency)
- Test: `tests/dashboard_api.rs`

**Interfaces:**
- Consumes: `store.request_log()`, `state.account_cache.snapshots()`, `state.runtime`.
- Produces: `overview_handler` → `OverviewView { kpis, quota, pools, accounts_available, recent_errors }`. `RequestLogRepo::aggregate_since(i64) -> RequestAggregate { total, success, error, avg_latency_ms, total_tokens }`.

- [ ] **Step 1: Failing test** — seed 3 request_log rows (2×200, 1×429) via the repo, then `GET /api/overview` and assert `kpis.requests == 3`, `kpis.success_rate` ≈ 0.667, `recent_errors` non-empty.

```rust
#[tokio::test]
async fn overview_reports_kpis_from_request_log() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, state) = spawn(up).await;
    let repo = state.store.request_log();
    for (st, tok) in [(200,1000),(200,2000),(429,0)] {
        repo.insert(&support::req_row(st, tok)).await.unwrap();
    }
    let v: serde_json::Value = reqwest::Client::new().get(format!("{pf}/api/overview"))
        .header("authorization","Bearer secret").send().await.unwrap().json().await.unwrap();
    assert_eq!(v["kpis"]["requests"], 3);
    assert_eq!(v["kpis"]["errors"], 1);
    assert!(v["recent_errors"].as_array().unwrap().len() >= 1);
}
```

Add `support::req_row(status, tokens)` returning a `RequestLogRecord` (content-free).

- [ ] **Step 2: Run — expect FAIL.**
- [ ] **Step 3: Implement** `aggregate_since` (one `SELECT count(*), sum(status<300), sum(status>=400), avg(duration_ms), sum(total_tokens) FROM request_log WHERE requested_at >= ?`) and `overview_handler` assembling `kpis` (requests/success_rate/error count/avg_latency/tokens), `quota` (per-provider windows from snapshots — group `account_cache.snapshots()` by provider, min remaining per window), `pools` (reuse Task 10's pool summary), `accounts_available` (count of eligible accounts via `runtime.overlay` + eligibility), and `recent_errors` (last N `request_log` rows with `status>=400`, grouped by `(status, account_id)`).
- [ ] **Step 4: Run — expect PASS.**
- [ ] **Step 5: Commit** `feat(server): /api/overview aggregates (kpis, quota, pools, errors)`.

---

## Task 7: Extend /api/accounts

**Files:** Modify `read_api.rs` (`AccountView` ~line 53 + `accounts_handler`). Test: `tests/dashboard_api.rs`.

**Interfaces:** `AccountView` gains `provider`, `pool`, per-window `usage` (`{window, used_percent, reset_at}[]`), `token_health` (`{access_state, access_expires_at}` derived from stored token + JWT `exp`), `request_count_24h`. Existing fields (id/email/status/plan/reset) unchanged.

- [ ] **Step 1: Failing test** — insert an account (with a pool) + a usage window, `GET /api/accounts`, assert the row carries `provider`, `pool`, a `usage` array, and `token_health`.
- [ ] **Step 2: Run — expect FAIL.**
- [ ] **Step 3: Implement** — extend `AccountView` + populate from `assemble_snapshots` / `store.accounts().latest_usage(id)` (already used elsewhere) + `runtime`. `token_health.access_state` = derive from decrypted token JWT `exp` via the existing `polyflare_codex::oauth::token_exp` (do NOT expose the token; only the state + expiry ts). `request_count_24h` from `request_log` grouped by `account_id`.
- [ ] **Step 4: Run — expect PASS.**
- [ ] **Step 5: Commit** `feat(server): /api/accounts carries provider/pool/usage/token-health`.

---

## Task 8: GET /api/accounts/{id} detail

**Files:** Modify `read_api.rs` (`account_detail_handler` + `AccountDetailView`). Test: `tests/dashboard_api.rs`.

**Interfaces:** `account_detail_handler(Path(id), State)` → `AccountDetailView { identity, status, quota_windows, token_status, routing_policy, security_work_authorized, request_totals }` or `404` for an unknown id. Content-free.

- [ ] **Step 1: Failing test** — insert account `acct-1`; `GET /api/accounts/acct-1` → 200 with `routing_policy`, `security_work_authorized`, `quota_windows`; `GET /api/accounts/nope` → 404.
- [ ] **Step 2: Run — expect FAIL.**
- [ ] **Step 3: Implement** using `store.accounts().get(id)` + `latest_usage(id)` + token state; return `404` on `None`.
- [ ] **Step 4: Run — expect PASS.**
- [ ] **Step 5: Commit** `feat(server): GET /api/accounts/{id} detail`.

---

## Task 9: GET /api/accounts/{id}/trends

**Files:** Modify `read_api.rs` (`account_trends_handler`); Modify `request_log_repo.rs`/`account.rs` if a `usage_history` time-series query is needed. Test: `tests/dashboard_api.rs`.

**Interfaces:** `account_trends_handler(Path(id), State)` → `{ account_id, primary: [{t, v}], secondary: [{t, v}] }` — 7-day per-window percent series derived from `usage_history` rows (ordered by timestamp). The `secondaryScheduled` plan line is **out of scope** (Phase 2) — return only `primary`/`secondary`.

- [ ] **Step 1: Failing test** — seed 3 `usage_history` rows for `acct-1` across timestamps; `GET /api/accounts/acct-1/trends` → `primary` array length 3, values are percents in `0..=100`.
- [ ] **Step 2: Run — expect FAIL.**
- [ ] **Step 3: Implement** a `usage_history`-ordered query (last 7 days) mapped to `{t: iso, v: percent}`; empty array when no history.
- [ ] **Step 4: Run — expect PASS.**
- [ ] **Step 5: Commit** `feat(server): GET /api/accounts/{id}/trends (7-day usage series)`.

---

## Task 10: Extend /api/pools

**Files:** Modify `read_api.rs` (`pools_handler` ~line 110 + `PoolView`). Test: `tests/dashboard_api.rs`.

**Interfaces:** `PoolView` gains `available` (eligible count via `runtime` overlay), `usage_percent` (aggregate/mean across the pool's accounts), `strategy` (from `state.selector_for(Some(slug))` name). Existing `{name, count, active}` kept.

- [ ] **Step 1: Failing test** — 2 accounts in pool `default` (1 active, 1 cooled-down via `state.runtime.record_rate_limit`), `GET /api/pools`, assert the `default` pool row has `count==2`, `available==1`, a numeric `usage_percent`, and a `strategy` string.
- [ ] **Step 2: Run — expect FAIL.**
- [ ] **Step 3: Implement** — extend `PoolView`; compute `available` from `runtime.overlay(snapshots)` + eligibility; `strategy` from the selector's name.
- [ ] **Step 4: Run — expect PASS.**
- [ ] **Step 5: Commit** `feat(server): /api/pools carries available/usage/strategy`.

---

## Task 11: Extend /api/requests (filters + pagination + metrics)

**Files:** Modify `read_api.rs` (`RequestsQuery` ~line 162 + `requests_handler` + `RequestRowView` ~line 149); Modify `request_log_repo.rs` (`page(filter, limit, offset) -> (Vec<RequestLogRow>, u64 total)`). Test: `tests/dashboard_api.rs`.

**Interfaces:** `RequestsQuery` gains `account`, `provider`, `status_class` (`success`|`error`|`all`), `model`, `transport`, `since_ts`, plus existing `limit`/`offset`. `RequestRowView` gains `account_id`, `model`, `reasoning_effort`, `service_tier`, `transport`, `ttft_ms`, `total_tokens`, `cached_tokens`, `tps` (derived: `total_tokens / (duration_ms - ttft_ms)` when both present, else null). `RequestLogRepo::page` applies the filters + returns the page and the total count.

- [ ] **Step 1: Failing test** — insert 3 rows (2 codex/200, 1 anthropic/200); `GET /api/requests?provider=codex&limit=10` returns `total==2` and rows carry `model`/`transport`/`total_tokens`; `GET /api/requests?status_class=error` returns only the error rows.
- [ ] **Step 2: Run — expect FAIL.**
- [ ] **Step 3: Implement** `page` with a dynamically-built `WHERE` (bind each present filter; `status_class` maps to `status<300` / `status>=400`), `ORDER BY requested_at DESC LIMIT ? OFFSET ?`, plus a `SELECT count(*)` with the same `WHERE`. Extend `RequestRowView` mapping + `tps` derivation.
- [ ] **Step 4: Run — expect PASS.**
- [ ] **Step 5: Commit** `feat(server): /api/requests filters + pagination + content-free metrics`.

---

## Task 12: Populate request metrics at the ingress chokepoint

**Files:** Modify `crates/polyflare-server/src/ingress.rs` (the `persist_outcome`/`RequestLog` construction sites) + `observability.rs` (`RequestLog` gains the metric fields). Test: `tests/dashboard_api.rs` (end-to-end).

**Interfaces:** `RequestLog` carries `account_id`, `model`, `reasoning_effort`, `service_tier`, `transport`, `ttft_ms`, `total_tokens`, `cached_tokens`; `RequestLog::record` writes them into `RequestLogRecord`. Ingress fills them from data it already has (selected account id, model string, effort/tier from the parsed request facts, `transport="http"` for now, timing + token counts from the observed stream where available — else `None`).

- [ ] **Step 1: Failing e2e test** — drive one `POST /responses` through the app against a `MockUpstream` that returns a `response.completed`, then `GET /api/requests` and assert the row's `account_id` is the served account and `model` is the requested model (content-free).
- [ ] **Step 2: Run — expect FAIL** (fields are `None`).
- [ ] **Step 3: Implement** — thread the already-known values into the `RequestLog` at the ingress persist sites; `transport` is the literal `"http"` (ws lands with the WS milestone); token counts from the stream observer when present, else `None`. Never read the body.
- [ ] **Step 4: Run — expect PASS.**
- [ ] **Step 5: Commit** `feat(server): populate content-free request metrics at ingress`.

---

## Self-Review

**Spec coverage:** §3.3 auth → Task 2; §3.4 SSE/flag → Tasks 4–5; §5 migration → Task 1 + Task 12 (populate); §6 endpoints — whoami/capabilities (2,3), overview (6), accounts (7), account detail (8), trends (9), pools (10), requests (11), logs/stream (5) → all covered; §8 content-safety → enforced per-task (only content-free fields) + Global Constraints. Deferred `⚑` items (per-model quotas, reset-credit inventory, plan trend line, warm-up flag, proxy binding, WS transport) are correctly **absent**. Frontend (§4, §7 rendering) is the separate Frontend plan.

**Placeholder scan:** No "TBD"/"handle edge cases". Two explicit forward-references are called out (`repo.page` used in Task 6's test exists as of Task 11; sequence Task 11 before Task 6 if executing strictly by test-compile, or land `page` as a thin wrapper in Task 1). Recommend execution order: 1 → 11 → 6 to keep every test compiling, then 2–5, 7–10, 12. **Fix applied:** reorder note added here.

**Type consistency:** `RequestLogRecord`/`RequestLogRow` field names match across Tasks 1/11/12; `LogEvent` fields match across Tasks 4/5; `AppState` additions (`admin_token`, `live_logs`, `log_bus`) are introduced once (Tasks 2/3/4) and consumed by name thereafter; handler names (`overview_handler`, `account_detail_handler`, `account_trends_handler`, `capabilities_handler`, `logs_stream_handler`, `whoami_handler`) match the `app.rs` route registrations in Task 2.

---

## Execution Handoff

After this backend plan is executed and green, write the **Frontend plan** (`docs/superpowers/plans/2026-07-17-polyflare-dashboard-phase1-frontend.md`) against these live endpoints: the SPA stack (React Router + TanStack Query + Recharts + Tailwind/Radix + lucide), the ccflare-skin design system + 12-col grid, and the pages (Login, Overview, Accounts cards/list + detail + master-detail switch, Pools, Requests, Live Logs).
