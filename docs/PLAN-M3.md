# PolyFlare M3-core — Continuity Engine (the "wedge fix") Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn `store:false` conversation continuity into an explicit, persisted, watchdog-guarded state machine so a dead ephemeral anchor is always *detected and recovered* (bounded, seconds) instead of silently hanging forever (the "wedge").

**Architecture:** Ownership routing narrows the candidate pool to the anchor's owning account *before* `Selector::pick` (no `Selector` trait change). Every anchor-bearing request is wrapped by a first-byte watchdog: it races the first upstream chunk against a configurable `N`; on silence it drops the dead upstream stream (cancel-safe) and recovers — either re-executing the client's full input with the anchor stripped (`ResendFull`, only when the request already carries full history) or emitting a `previous_response_not_found` signal so the client self-heals (`SignalClient`). PolyFlare persists **no conversation content** — only a session-key row, a `response_id → owning_account` map, and state. The Codex executor stays a stateless SSE pass-through; the watchdog wraps it in the server.

**Tech Stack:** Rust 2021, tokio (`tokio::time::timeout`), axum 0.8, reqwest 0.12 (streaming), futures 0.3 (`StreamExt`, `stream::once`/`chain`, hand-written `Stream`), sqlx 0.8 (runtime-checked, SQLite), sha2/hex (session-key + input fingerprint), thiserror, async-trait.

## Global Constraints

*Every task's requirements implicitly include this section. Values are copied verbatim from SPEC-M3 + the controller resolutions.*

- **Rust edition 2021**, workspace resolver `2`. New code compiles under `cargo clippy --workspace --all-targets -- -D warnings`.
- **sqlx is runtime-checked only**: `sqlx::query` / `sqlx::query_as::<_, T>` / `#[derive(sqlx::FromRow)]`. NO compile-time macros, NO `DATABASE_URL`. Quote `"window"`-style SQLite keywords. Migrations are **forward-only** (`0002_continuity.sql`, never edit `0001`).
- **The watchdog arms ONLY on anchor-bearing requests** — a request carrying a client `previous_response_id`. A no-anchor request is `WatchdogArm::Disarmed` and must never be subject to anchor-recovery or false-triggering.
- **Recovery is signal-client by default; PolyFlare persists NO conversation content.** `SignalClient` (emit `previous_response_not_found`) is used for a bare-tail dead anchor; `ResendFull` (strip anchor, re-proxy) is used **only** when the outgoing input already carries full history (`is_full_resend`). No conversation bodies are ever written to the store.
- **Never log tokens, bearer tokens, or reasoning content.** New sensitive-bearing types (`ReasoningItems`, `RecoveryPlan`) get a **redacting `Debug` + a redaction test**. `ContinuityError` has a generic `Display` that carries no session content.
- **Streaming stays non-buffering + peek-before-relay.** Never write a client byte until the first upstream chunk arrives (this is what makes restart safe). The response-id sniffer forwards every byte unchanged and never accumulates the full body (bounded ≤ 64 KiB, then stops sniffing).
- **Watchdog `N` ≈ 30s, configurable** via `ServeConfig::continuity_watchdog` (default `Duration::from_secs(30)`, biased high for Sol's slow first token). Tests inject a tiny `N` (`Duration::from_millis(150)`).
- **Detection = first-bytes** (race the first stream item; silence is invisible at `send()`/200 headers). **Recovery = signal-client** unless full input is present.
- **VERIFY-at-implementation items (do not guess — capture, then finalize):** (1) the exact `previous_response_not_found` wire shape the real Codex CLI / Claude Code self-heals from (from codex-lb masking behavior or a live capture); (2) the exact session-key header names (`x-codex-turn-state`, session / `prompt_cache_key`) against the live Codex CLI (SPEC-M3 risk 4). Tests assert on the `previous_response_not_found` **code substring**, never the exact envelope, so they survive the finalized shape.
- **Client-facing errors carry generic bodies** — never a token, URL, or internal `Display`.
- **Gates before EVERY commit:** `cargo fmt --all -- --check` → `cargo clippy --workspace --all-targets -- -D warnings` → `cargo test --workspace`, all green.

---

## File Structure

**New files**

- `crates/polyflare-store/migrations/0002_continuity.sql` — `continuity_sessions` + `continuity_anchors` (no conversation content). (C2)
- `crates/polyflare-store/src/continuity_repo.rs` — `ContinuityRepo` + `SessionRow` over the pool. (C2)
- `crates/polyflare-core/src/continuity.rs` — `NoopContinuity` (Anthropic-path uniformity) + a `box_store_err` helper is NOT here (it needs `StoreError`; lives server-side). (C1)
- `crates/polyflare-server/src/session_key.rs` — session-key derivation, `is_full_resend`, `client_previous_response_id` extraction, `sha256_hex`. (C3)
- `crates/polyflare-server/src/continuity.rs` — `CodexContinuity` (holds a `ContinuityRepo`) implementing the async `Continuity` trait: `prepare` (C4) + `observe` (C6).
- `crates/polyflare-server/src/watchdog.rs` — `apply_ownership`, `execute_with_watchdog`, `execute_recovery`, `signal_client_stream`, `ObservingStream`, `ResponseIdSniffer`, `WatchdogError`. (C5)
- `crates/polyflare-server/tests/wedge_regression.rs` — the RED-until-C7 acceptance test + R1 assertion. (C0/C8)
- `crates/polyflare-server/tests/ownership.rs` — 2nd turn returns to the same account. (C8)
- `crates/polyflare-server/tests/signal_client.rs` — Strategy-B bare-tail dead anchor. (C8)
- `crates/polyflare-server/tests/watchdog_race.rs` — unit-level first-byte race / silence / hard-error / cancel-safety. (C5/C8)

**Modified files**

- `crates/polyflare-core/src/types.rs` — add `SessionKey`, `KeyStrength`, `Prepared`, `ContinuityDirective`, `WatchdogArm`, `RecoveryPlan`, `TurnOutcome`, `ContinuityError`, `ReasoningItems`; enrich `RequestCtx`. (C1)
- `crates/polyflare-core/src/traits.rs` — reshape `Continuity` to async `prepare -> Result<Prepared, _>` + `observe`. (C1)
- `crates/polyflare-core/src/lib.rs` — export the new types + `NoopContinuity`. (C1)
- `crates/polyflare-store/src/lib.rs` + `src/store.rs` — export `ContinuityRepo`/`SessionRow`, add `Store::continuity()`. (C2)
- `crates/polyflare-server/src/lib.rs` — `pub mod session_key; pub mod continuity; pub mod watchdog;`. (C3–C5)
- `crates/polyflare-server/src/ingress.rs` — rewrite the handler around `prepare → apply_ownership → execute_with_watchdog`, factor `resolve_core_account`. (C7)
- `crates/polyflare-server/src/app.rs` — add `continuity: Arc<dyn Continuity>` to `AppState`. (C7)
- `crates/polyflare-server/src/config.rs` — add `continuity_watchdog: Duration`. (C7)
- `crates/polyflare-server/src/main.rs` — build `CodexContinuity`, pass `continuity` + `continuity_watchdog`. (C7)
- `crates/polyflare-server/tests/{e2e_passthrough,ingress_relays,pool_selection,refresh_path,large_body}.rs` — add the `continuity` field to their `AppState`. (C7)
- `crates/polyflare-testkit/src/lib.rs` — add `MockUpstream` id-emitting + silent-on-anchor modes, record all bodies. (C0)
- `crates/polyflare-testkit/Cargo.toml` — add `bytes`. (C0)
- `crates/polyflare-server/Cargo.toml` — add `futures-util`, `futures-core`, `bytes`, `async-trait`, `sha2`, `hex`; dev-dep already has testkit/reqwest/futures-util. (C1–C5)
- `Cargo.toml` (workspace) — add `sha2 = "0.10"`, `hex = "0.4"`. (C1)

---

## Placement note (deliberate deviation from SPEC-M3 §3.2)

SPEC-M3 §3.2 sketches `polyflare-core/src/continuity.rs` as holding "the Continuity trait + Codex state machine". This plan keeps **`polyflare-core` free of `sqlx`**: the trait + neutral types + `NoopContinuity` live in core; the store-backed `CodexContinuity` + the watchdog wrapper + `apply_ownership` live in **`polyflare-server`** (which already depends on core + store + codex, and is where the watchdog "wraps the executor" per §3.2/§E4). This respects the existing dependency graph (core is pure, no DB) and M2-GATE1's "reshape a seam only where its milestone builds on it".

---

## Task C0: RED wedge-regression harness (mock silent-on-anchor mode + failing e2e)

**Files:**
- Modify: `crates/polyflare-testkit/src/lib.rs`
- Modify: `crates/polyflare-testkit/Cargo.toml`
- Create: `crates/polyflare-server/tests/wedge_regression.rs`

**Interfaces:**
- Produces (testkit): `MockUpstream::new(events: Vec<String>) -> Self` (unchanged Scripted mode); `MockUpstream::with_ids(events: Vec<String>) -> Self` (always responds, emits `response.created`/`response.completed` with generated `resp_N` ids); `MockUpstream::silent_on_anchor(events: Vec<String>) -> Self` (anchor ⇒ 200 + never-yielding body, no keep-alive; no-anchor ⇒ same as `with_ids`); `MockUpstream::spawn(self) -> String`; `MockUpstream::last_body() -> Option<Value>`; `MockUpstream::bodies() -> Vec<Value>`; `MockUpstream::request_count() -> usize`; `MockUpstream::last_authorization() -> Option<String>`; `MockUpstream::emitted_response_ids() -> Vec<String>`.
- Consumes (test): the existing `polyflare_server::app::{build_app, AppState}` **current** shape (no `continuity` field yet — added in C7).

- [ ] **Step 1: Add `bytes` to the testkit crate**

Edit `crates/polyflare-testkit/Cargo.toml`, under `[dependencies]` add:

```toml
bytes = { workspace = true }
```

- [ ] **Step 2: Rewrite `MockUpstream` with modes + full recording**

Replace the `MockUpstream` struct, its `impl`, and `handler` in `crates/polyflare-testkit/src/lib.rs` (keep `MockOAuth` and the imports of `MockOAuth` unchanged). New content for the `MockUpstream` region:

```rust
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Json, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use bytes::Bytes;
use futures_util::stream::{self, Stream};
use tokio::net::TcpListener;

/// The response behavior of a `MockUpstream`.
#[derive(Clone)]
enum MockMode {
    /// Legacy: always stream the fixed `events` as SSE `data:` frames (with keep-alive). Never
    /// injects a `response.id`. Used by the M1/M2 pass-through tests unchanged.
    Scripted,
    /// Emit `response.created`(resp_N) + `events` + `response.completed`(resp_N), generating a
    /// fresh `resp_N` id per response. If `silent_on_anchor` and the body carries
    /// `previous_response_id`, instead return 200 headers then a never-yielding body (no
    /// keep-alive) — the wedge (silence-after-accept).
    WithIds { silent_on_anchor: bool },
}

/// A scriptable mock upstream: serves `POST /responses`, records every request body + the last
/// `Authorization` header, and streams SSE per its [`MockMode`].
#[derive(Clone)]
pub struct MockUpstream {
    events: Arc<Vec<String>>,
    mode: MockMode,
    bodies: Arc<Mutex<Vec<serde_json::Value>>>,
    last_authorization: Arc<Mutex<Option<String>>>,
    emitted_ids: Arc<Mutex<Vec<String>>>,
    counter: Arc<AtomicU32>,
}

impl MockUpstream {
    fn build(events: Vec<String>, mode: MockMode) -> Self {
        Self {
            events: Arc::new(events),
            mode,
            bodies: Arc::new(Mutex::new(Vec::new())),
            last_authorization: Arc::new(Mutex::new(None)),
            emitted_ids: Arc::new(Mutex::new(Vec::new())),
            counter: Arc::new(AtomicU32::new(0)),
        }
    }

    /// Legacy scripted mode: stream `events` verbatim (no id injection).
    pub fn new(events: Vec<String>) -> Self {
        Self::build(events, MockMode::Scripted)
    }

    /// Always respond, injecting `response.created`/`response.completed` with a generated id.
    pub fn with_ids(events: Vec<String>) -> Self {
        Self::build(events, MockMode::WithIds { silent_on_anchor: false })
    }

    /// Respond with ids for anchorless requests; go silent (200 + no body) when the request
    /// carries `previous_response_id` — the wedge.
    pub fn silent_on_anchor(events: Vec<String>) -> Self {
        Self::build(events, MockMode::WithIds { silent_on_anchor: true })
    }

    /// The most recent request body, if any.
    pub fn last_body(&self) -> Option<serde_json::Value> {
        self.bodies.lock().unwrap().last().cloned()
    }

    /// Every recorded request body, in order.
    pub fn bodies(&self) -> Vec<serde_json::Value> {
        self.bodies.lock().unwrap().clone()
    }

    /// How many requests the mock has received.
    pub fn request_count(&self) -> usize {
        self.bodies.lock().unwrap().len()
    }

    /// The `Authorization` header of the most recent request (e.g. `"Bearer <token>"`).
    pub fn last_authorization(&self) -> Option<String> {
        self.last_authorization.lock().unwrap().clone()
    }

    /// The `response.id`s the mock has emitted, in order.
    pub fn emitted_response_ids(&self) -> Vec<String> {
        self.emitted_ids.lock().unwrap().clone()
    }

    /// Bind an ephemeral port, serve in a background task, and return the base URL.
    pub async fn spawn(self) -> String {
        let app = Router::new()
            .route("/responses", post(handler))
            .layer(DefaultBodyLimit::max(100 * 1024 * 1024))
            .with_state(self);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }
}

fn sse_frame(payload: &str) -> Bytes {
    Bytes::from(format!("data: {payload}\n\n"))
}

async fn handler(
    State(mock): State<MockUpstream>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let has_anchor = body.get("previous_response_id").is_some();
    mock.bodies.lock().unwrap().push(body);
    *mock.last_authorization.lock().unwrap() = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    match mock.mode {
        MockMode::Scripted => {
            let events = (*mock.events).clone();
            let s = stream::iter(events.into_iter().map(|d| Ok::<Event, Infallible>(Event::default().data(d))));
            Sse::new(s).keep_alive(KeepAlive::default()).into_response()
        }
        MockMode::WithIds { silent_on_anchor } => {
            if silent_on_anchor && has_anchor {
                // The wedge: 200 headers, then a body that never yields a byte (no keep-alive).
                let pending = stream::pending::<Result<Bytes, std::io::Error>>();
                return Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "text/event-stream")
                    .body(Body::from_stream(pending))
                    .unwrap();
            }
            let n = mock.counter.fetch_add(1, Ordering::SeqCst) + 1;
            let id = format!("resp_{n}");
            mock.emitted_ids.lock().unwrap().push(id.clone());
            let mut frames: Vec<Bytes> = Vec::new();
            frames.push(sse_frame(&format!(
                r#"{{"type":"response.created","response":{{"id":"{id}"}}}}"#
            )));
            for e in mock.events.iter() {
                frames.push(sse_frame(e));
            }
            frames.push(sse_frame(&format!(
                r#"{{"type":"response.completed","response":{{"id":"{id}"}}}}"#
            )));
            let s = stream::iter(frames.into_iter().map(Ok::<Bytes, std::io::Error>));
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "text/event-stream")
                .body(Body::from_stream(s))
                .unwrap()
        }
    }
}
```

Then update the existing testkit unit test `mock_emits_events_and_records_body` (it still uses `new` + `last_body` — no change needed to its body; confirm it references `last_body().unwrap()["model"]`, which still works).

- [ ] **Step 3: Confirm testkit + existing suites still compile & pass**

Run: `cargo test -p polyflare-testkit && cargo test -p polyflare-server`
Expected: PASS (Scripted mode is byte-compatible with the M1/M2 tests).

- [ ] **Step 4: Write the RED wedge test (C0-compatible form, ignored until C7)**

Create `crates/polyflare-server/tests/wedge_regression.rs`. This form references only the **current** `AppState` (no `continuity` field). It is `#[ignore]`d so the workspace gate stays green through C1–C6; it is confirmed RED by running it explicitly.

```rust
//! Wedge regression: an anchor-bearing request routed to a silent-on-anchor upstream must NOT
//! hang. RED until C7 wires the watchdog into ingress; then it goes GREEN.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::CapacityWeighted;
use polyflare_server::app::{build_app, AppState};
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};
use polyflare_testkit::MockUpstream;

fn now() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
}

fn store_account(id: &str, token: &str) -> Account {
    Account {
        id: id.to_string(),
        chatgpt_account_id: None,
        chatgpt_user_id: None,
        email: "u@example.test".to_string(),
        alias: None,
        workspace_id: None,
        workspace_label: None,
        seat_type: None,
        plan_type: "pro".to_string(),
        routing_policy: "normal".to_string(),
        last_refresh: now(),
        created_at: now(),
        status: "active".to_string(),
        deactivation_reason: None,
        reset_at: None,
        blocked_at: None,
        security_work_authorized: false,
    }
}

async fn spawn_polyflare(upstream: String) -> String {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[9u8; 32]).unwrap();
    store
        .accounts()
        .insert(
            &store_account("e2e", "tokE"),
            &PlainTokens {
                access_token: "tokE".to_string(),
                refresh_token: "r".to_string(),
                id_token: "i".to_string(),
            },
            &cipher,
        )
        .await
        .unwrap();
    std::mem::forget(dir);

    let state = Arc::new(AppState {
        executor: Arc::new(CodexExecutor::new().unwrap()),
        selector: Arc::new(CapacityWeighted),
        store,
        cipher,
        oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(),
        upstream_base_url: upstream,
    });
    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

#[tokio::test]
#[ignore = "RED until C7 wires the watchdog; un-ignore in C7"]
async fn anchor_bearing_request_to_silent_upstream_does_not_wedge() {
    let mock = MockUpstream::silent_on_anchor(vec![
        r#"{"type":"response.output_text.delta","delta":"ok"}"#.to_string(),
    ]);
    let handle = mock.clone();
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(upstream).await;

    let client = reqwest::Client::new();
    // Full multi-item history + a dead anchor => the classic wedge input.
    let request = client
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({
            "model": "gpt-5.6-sol",
            "previous_response_id": "resp_dead",
            "input": [
                {"role": "user", "content": "turn one"},
                {"role": "assistant", "content": "reply one"},
                {"role": "user", "content": "turn two"}
            ]
        }))
        .send();

    // Bounded wall-clock: at C0 (no watchdog) the client hangs on the silent body and this elapses
    // (RED). At C7 the watchdog recovers within N and the stream completes (GREEN).
    let outcome = tokio::time::timeout(Duration::from_secs(5), async {
        let resp = request.await.unwrap();
        assert_eq!(resp.status(), 200);
        let mut body = String::new();
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            body.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
        }
        body
    })
    .await;

    let body = outcome.expect("request must complete within 5s (no wedge)");
    assert!(body.contains("response.completed"), "client must see a completed stream");
    assert_eq!(handle.request_count(), 2, "one silent attempt + one recovery");
}
```

- [ ] **Step 5: Confirm the wedge test fails RED (documents the wedge)**

Run: `cargo test -p polyflare-server --test wedge_regression -- --ignored`
Expected: FAIL — `request must complete within 5s (no wedge)` (the 5s timeout elapses; current ingress has no watchdog and relays the silent body).

- [ ] **Step 6: Commit**

```bash
git add crates/polyflare-testkit/ crates/polyflare-server/tests/wedge_regression.rs
git commit -m "test(m3): add silent-on-anchor mock mode + RED wedge-regression e2e (C0)"
```

---

## Task C1: Reshape the `Continuity` trait + continuity types

**Files:**
- Modify: `crates/polyflare-core/src/types.rs`
- Modify: `crates/polyflare-core/src/traits.rs`
- Create: `crates/polyflare-core/src/continuity.rs`
- Modify: `crates/polyflare-core/src/lib.rs`
- Modify: `Cargo.toml` (workspace), `crates/polyflare-server/Cargo.toml`

**Interfaces:**
- Consumes: existing `PreparedRequest`, `AccountId`, `RequestCtx` (enriched here).
- Produces: `SessionKey`, `KeyStrength`, `ReasoningItems`, `Prepared`, `ContinuityDirective`, `WatchdogArm`, `RecoveryPlan`, `TurnOutcome`, `ContinuityError`; `trait Continuity { async fn prepare(&self, req: PreparedRequest, ctx: &RequestCtx) -> Result<Prepared, ContinuityError>; async fn observe(&self, outcome: TurnOutcome, ctx: &RequestCtx) -> Result<(), ContinuityError>; }`; `struct NoopContinuity`.

- [ ] **Step 1: Add the redaction test for `ReasoningItems` (write first)**

Append to `crates/polyflare-core/src/types.rs` `mod tests`:

```rust
    #[test]
    fn reasoning_items_debug_redacts_content() {
        let r = ReasoningItems(vec![serde_json::json!({"text": "super-secret-chain-of-thought"})]);
        let s = format!("{r:?}");
        assert!(!s.contains("super-secret-chain-of-thought"), "reasoning content must never appear in Debug: {s}");
        assert!(s.contains("1 item"), "Debug should summarize count, not content: {s}");
    }

    #[test]
    fn recovery_plan_debug_redacts_request_body() {
        let plan = RecoveryPlan::ResendFull {
            anchorless_req: PreparedRequest {
                body: serde_json::json!({"input": "super-secret-conversation"}),
                model: "m".to_string(),
            },
        };
        let s = format!("{plan:?}");
        assert!(!s.contains("super-secret-conversation"), "recovery must never leak the request body: {s}");
        assert!(s.contains("redacted"), "Debug should mark the body redacted: {s}");
    }
```

- [ ] **Step 2: Run the tests to verify they fail (types don't exist)**

Run: `cargo test -p polyflare-core reasoning_items_debug_redacts_content`
Expected: FAIL — `cannot find type ReasoningItems`.

- [ ] **Step 3: Add the continuity types to `types.rs`**

Add `use serde_json;` is already available via the crate. Append this block to `crates/polyflare-core/src/types.rs` (before `mod tests`):

```rust
/// A derived conversation key + its strength (hard binds routing; soft is best-effort).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionKey {
    pub value: String,
    pub strength: KeyStrength,
}

/// How strongly a session key binds routing. `Hard` keys pin; `Soft` keys are best-effort.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyStrength {
    Hard,
    Soft,
}

/// Reasoning-typed output items from a completed turn. Sensitive user data: its `Debug` redacts
/// content. Populated only in R3 (M3-followup); `None` throughout M3-core.
#[derive(Clone)]
pub struct ReasoningItems(pub Vec<serde_json::Value>);

impl std::fmt::Debug for ReasoningItems {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReasoningItems([{} item(s) redacted])", self.0.len())
    }
}

/// Output of `prepare`: the (possibly-rewritten) request + how to route & guard it.
#[derive(Debug, Clone)]
pub struct Prepared {
    pub req: PreparedRequest,
    pub directive: ContinuityDirective,
}

/// How to route and guard a prepared request.
#[derive(Debug, Clone)]
pub struct ContinuityDirective {
    /// HARD routing pre-filter. `Some` ⇒ the request MUST route to this account (or Recover).
    pub pin_account: Option<AccountId>,
    /// Arm the silence watchdog — set ONLY on anchor-bearing requests.
    pub watchdog: WatchdogArm,
    /// What to do if the watchdog fires.
    pub recovery: RecoveryPlan,
    /// Threaded back to `observe` so it knows which session/turn this was.
    pub session_key: Option<SessionKey>,
}

/// Whether the silence watchdog is armed, and with what timeout.
#[derive(Debug, Clone, Copy)]
pub enum WatchdogArm {
    Disarmed,
    Armed { timeout: std::time::Duration },
}

/// What to do when the watchdog fires (or the owner is unavailable at prepare time).
#[derive(Clone)]
pub enum RecoveryPlan {
    /// The outgoing input is self-sufficient (a full-resend): on silence, re-execute this
    /// anchor-stripped request. Carries conversation content — redacted in `Debug`.
    ResendFull { anchorless_req: PreparedRequest },
    /// The outgoing input is a bare tail: on silence, surface `previous_response_not_found` so the
    /// client self-heals with a full resend.
    SignalClient,
    /// No anchor present ⇒ nothing to recover.
    None,
}

impl std::fmt::Debug for RecoveryPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecoveryPlan::ResendFull { .. } => {
                write!(f, "ResendFull {{ anchorless_req: <redacted> }}")
            }
            RecoveryPlan::SignalClient => write!(f, "SignalClient"),
            RecoveryPlan::None => write!(f, "None"),
        }
    }
}

/// What `observe` consumes — built by the watchdog wrapper as the stream resolves.
#[derive(Debug)]
pub enum TurnOutcome {
    /// Upstream produced its first event and we relayed it. `response_id` is sniffed from the
    /// streamed `response.created`/`response.completed`. `reasoning` is `None` until R3.
    Completed {
        session_key: Option<SessionKey>,
        account: AccountId,
        response_id: Option<String>,
        input_fingerprint: String,
        input_count: u32,
        reasoning: Option<ReasoningItems>,
    },
    /// Watchdog fired; we recovered (Strategy A) or signaled the client (Strategy B).
    Recovered {
        session_key: Option<SessionKey>,
        account: AccountId,
        new_response_id: Option<String>,
    },
    /// A hard upstream error (not silence).
    Failed { session_key: Option<SessionKey> },
}

/// Errors `Continuity` can surface. Generic `Display` — never leaks session content.
#[derive(Debug, thiserror::Error)]
pub enum ContinuityError {
    #[error("continuity store error")]
    Store(#[source] Box<dyn std::error::Error + Send + Sync>),
}
```

Then enrich `RequestCtx` — replace the existing struct:

```rust
/// Per-request context threaded through selection/continuity. `session_key`,
/// `client_previous_response_id`, and `is_full_resend` are derived at ingress from headers + body
/// BEFORE `prepare`.
#[derive(Debug, Clone, Default)]
pub struct RequestCtx {
    pub session_id: Option<String>,
    pub session_key: Option<SessionKey>,
    pub client_previous_response_id: Option<String>,
    pub is_full_resend: bool,
}
```

- [ ] **Step 4: Run the redaction tests to verify they pass**

Run: `cargo test -p polyflare-core reasoning_items_debug_redacts_content recovery_plan_debug_redacts_request_body`
Expected: PASS.

- [ ] **Step 5: Reshape the `Continuity` trait**

In `crates/polyflare-core/src/traits.rs`, update the imports and replace the `Continuity` trait:

```rust
use crate::types::{
    Account, AccountId, AccountSnapshot, ContinuityError, ExecError, Prepared, PreparedRequest,
    RequestCtx, ResponseStream, SelectionCtx, TurnOutcome,
};
```

```rust
/// The continuity state machine seam (M3). `prepare` resolves session + ownership and decides
/// routing + watchdog; `observe` advances the machine from how the turn resolved. Both read/write
/// persisted session state and may fail.
#[async_trait]
pub trait Continuity: Send + Sync {
    async fn prepare(
        &self,
        req: PreparedRequest,
        ctx: &RequestCtx,
    ) -> Result<Prepared, ContinuityError>;

    async fn observe(&self, outcome: TurnOutcome, ctx: &RequestCtx) -> Result<(), ContinuityError>;
}
```

(Leave `Executor`, `Selector`, `Coordinator` unchanged. `PreparedRequest` stays imported for the trait signature.)

- [ ] **Step 6: Add `NoopContinuity` (Anthropic-path uniformity)**

Create `crates/polyflare-core/src/continuity.rs`:

```rust
//! Continuity implementations that live in the neutral core. `NoopContinuity` keeps a non-Codex
//! backend's ingress path uniform: it never pins, never arms the watchdog, and observes nothing.

use async_trait::async_trait;

use crate::traits::Continuity;
use crate::types::{
    ContinuityDirective, ContinuityError, Prepared, PreparedRequest, RecoveryPlan, RequestCtx,
    TurnOutcome, WatchdogArm,
};

/// A `Continuity` that does nothing — for backends without continuity (e.g. Anthropic in M3).
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopContinuity;

#[async_trait]
impl Continuity for NoopContinuity {
    async fn prepare(
        &self,
        req: PreparedRequest,
        ctx: &RequestCtx,
    ) -> Result<Prepared, ContinuityError> {
        Ok(Prepared {
            req,
            directive: ContinuityDirective {
                pin_account: None,
                watchdog: WatchdogArm::Disarmed,
                recovery: RecoveryPlan::None,
                session_key: ctx.session_key.clone(),
            },
        })
    }

    async fn observe(&self, _outcome: TurnOutcome, _ctx: &RequestCtx) -> Result<(), ContinuityError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn noop_prepare_disarms_and_never_pins() {
        let noop = NoopContinuity;
        let req = PreparedRequest { body: serde_json::json!({}), model: "m".to_string() };
        let prepared = noop.prepare(req, &RequestCtx::default()).await.unwrap();
        assert!(prepared.directive.pin_account.is_none());
        assert!(matches!(prepared.directive.watchdog, WatchdogArm::Disarmed));
        assert!(matches!(prepared.directive.recovery, RecoveryPlan::None));
    }
}
```

`NoopContinuity`'s test uses `tokio` — add `tokio` as a **dev-dependency** to `crates/polyflare-core/Cargo.toml`:

```toml
[dev-dependencies]
tokio = { workspace = true }
```

- [ ] **Step 7: Export the new symbols**

In `crates/polyflare-core/src/lib.rs`, add `pub mod continuity;`, re-export `NoopContinuity`, and extend the `types` re-export:

```rust
pub mod continuity;
```
```rust
pub use continuity::NoopContinuity;
```
```rust
pub use types::{
    Account, AccountId, AccountSnapshot, ContinuityDirective, ContinuityError, ExecError,
    KeyStrength, Prepared, PreparedRequest, ReasoningItems, RecoveryPlan, RequestCtx,
    ResponseStream, SelectionCtx, SessionKey, TurnOutcome, WatchdogArm,
};
```

- [ ] **Step 8: Add workspace + server deps used by later tasks**

In the workspace `Cargo.toml` `[workspace.dependencies]` add:

```toml
sha2 = "0.10"
hex = "0.4"
```

In `crates/polyflare-server/Cargo.toml` `[dependencies]` add:

```toml
futures-core = { workspace = true }
futures-util = { workspace = true }
bytes = { workspace = true }
async-trait = { workspace = true }
sha2 = { workspace = true }
hex = { workspace = true }
```

- [ ] **Step 9: Verify the whole workspace still compiles + passes**

Run: `cargo test -p polyflare-core && cargo build --workspace`
Expected: PASS (nothing implements the reshaped trait yet except `NoopContinuity`; the server does not consume it until C7).

- [ ] **Step 10: fmt + clippy + commit**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings
git add Cargo.toml crates/polyflare-core/ crates/polyflare-server/Cargo.toml
git commit -m "feat(m3): reshape Continuity trait (async prepare/observe) + continuity types (C1)"
```

---

## Task C2: Persistence — `0002_continuity.sql` + `ContinuityRepo`

**Files:**
- Create: `crates/polyflare-store/migrations/0002_continuity.sql`
- Create: `crates/polyflare-store/src/continuity_repo.rs`
- Modify: `crates/polyflare-store/src/store.rs`, `crates/polyflare-store/src/lib.rs`

**Interfaces:**
- Produces: `SessionRow { session_key, key_strength, owning_account_id: Option<String>, anchor_response_id: Option<String>, last_input_fingerprint: Option<String>, last_input_count: Option<i64>, reasoning_cache_ref: Option<String>, state, created_at, updated_at, last_activity_at }`; `ContinuityRepo::{new, get_session, get_anchor_owner, ensure_session, set_state, record_completion, record_recovery}`; `Store::continuity() -> ContinuityRepo`.

- [ ] **Step 1: Write the migration**

Create `crates/polyflare-store/migrations/0002_continuity.sql`:

```sql
-- PolyFlare continuity state machine (M3). Forward-only. NO conversation content is stored here:
-- only per-session state, the last-observed anchor id, and a response_id -> owning-account map.
-- Timestamps are INTEGER unix-epoch seconds.

CREATE TABLE IF NOT EXISTS continuity_sessions (
    session_key            TEXT    PRIMARY KEY,
    key_strength           TEXT    NOT NULL,              -- 'hard' | 'soft'
    owning_account_id      TEXT        REFERENCES accounts(id) ON DELETE SET NULL,
    anchor_response_id     TEXT,                          -- last response.id we saw complete
    last_input_fingerprint TEXT,                          -- diagnostic sha256 of the input array
    last_input_count       INTEGER,                       -- diagnostic item count of the input array
    reasoning_cache_ref    TEXT,                          -- R3 (M3-followup); NULL in M3-core
    state                  TEXT    NOT NULL,              -- 'fresh'|'anchored'|'reattaching'|'recover'
    created_at             INTEGER NOT NULL,
    updated_at             INTEGER NOT NULL,
    last_activity_at       INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_continuity_sessions_activity
    ON continuity_sessions (last_activity_at);

-- response_id -> owner map: a CLIENT-supplied previous_response_id resolves to its account even
-- when the derived session_key differs (or is soft/absent). The ownership backbone.
CREATE TABLE IF NOT EXISTS continuity_anchors (
    response_id       TEXT    PRIMARY KEY,
    session_key       TEXT    NOT NULL REFERENCES continuity_sessions(session_key) ON DELETE CASCADE,
    owning_account_id TEXT    NOT NULL,
    created_at        INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_continuity_anchors_session
    ON continuity_anchors (session_key);
```

- [ ] **Step 2: Write the repo test first**

Create `crates/polyflare-store/src/continuity_repo.rs` with just the test module, to drive the API:

```rust
//! Repository over the continuity state machine tables. Runtime-checked sqlx; no conversation
//! content is ever written here.

use sqlx::sqlite::SqlitePool;

use crate::StoreError;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;

    async fn store() -> Store {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(&dir.path().join("s.db")).await.unwrap();
        std::mem::forget(dir);
        s
    }

    async fn seed_account(s: &Store, id: &str) {
        // A real account row so the owning_account FK is satisfiable.
        sqlx::query(
            "INSERT INTO accounts (id, email, access_token_enc, refresh_token_enc, id_token_enc, created_at) \
             VALUES (?, 'e@x', X'00', X'00', X'00', 0)",
        )
        .bind(id)
        .execute(s.pool())
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn completion_records_owner_anchor_and_map() {
        let s = store().await;
        seed_account(&s, "A").await;
        let repo = s.continuity();

        repo.ensure_session("sk1", "soft", 100).await.unwrap();
        repo.record_completion("sk1", "soft", "A", "resp_1", "fp", 3, 200).await.unwrap();

        let row = repo.get_session("sk1").await.unwrap().unwrap();
        assert_eq!(row.owning_account_id.as_deref(), Some("A"));
        assert_eq!(row.anchor_response_id.as_deref(), Some("resp_1"));
        assert_eq!(row.state, "anchored");
        assert_eq!(repo.get_anchor_owner("resp_1").await.unwrap().as_deref(), Some("A"));
    }

    #[tokio::test]
    async fn ensure_session_is_idempotent_and_fresh() {
        let s = store().await;
        let repo = s.continuity();
        repo.ensure_session("sk2", "hard", 1).await.unwrap();
        repo.ensure_session("sk2", "hard", 2).await.unwrap(); // no-op, no error
        let row = repo.get_session("sk2").await.unwrap().unwrap();
        assert_eq!(row.state, "fresh");
        assert_eq!(row.key_strength, "hard");
    }

    #[tokio::test]
    async fn recovery_rehomes_owner_and_new_anchor() {
        let s = store().await;
        seed_account(&s, "A").await;
        seed_account(&s, "B").await;
        let repo = s.continuity();
        repo.ensure_session("sk3", "soft", 1).await.unwrap();
        repo.record_completion("sk3", "soft", "A", "resp_1", "fp", 2, 2).await.unwrap();
        repo.record_recovery("sk3", "B", Some("resp_2"), 3).await.unwrap();
        let row = repo.get_session("sk3").await.unwrap().unwrap();
        assert_eq!(row.owning_account_id.as_deref(), Some("B"), "recovery re-homes owner");
        assert_eq!(row.anchor_response_id.as_deref(), Some("resp_2"));
        assert_eq!(repo.get_anchor_owner("resp_2").await.unwrap().as_deref(), Some("B"));
    }

    #[tokio::test]
    async fn get_anchor_owner_is_none_when_absent() {
        let s = store().await;
        let repo = s.continuity();
        assert!(repo.get_anchor_owner("nope").await.unwrap().is_none());
    }
}
```

- [ ] **Step 3: Run to verify it fails (repo not implemented)**

Run: `cargo test -p polyflare-store completion_records_owner_anchor_and_map`
Expected: FAIL — `no method named continuity` / `ContinuityRepo` unresolved.

- [ ] **Step 4: Implement `SessionRow` + `ContinuityRepo`**

Insert above the `#[cfg(test)]` module in `crates/polyflare-store/src/continuity_repo.rs`:

```rust
/// One `continuity_sessions` row.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct SessionRow {
    pub session_key: String,
    pub key_strength: String,
    pub owning_account_id: Option<String>,
    pub anchor_response_id: Option<String>,
    pub last_input_fingerprint: Option<String>,
    pub last_input_count: Option<i64>,
    pub reasoning_cache_ref: Option<String>,
    pub state: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub last_activity_at: i64,
}

const SELECT_SESSION: &str = "SELECT session_key, key_strength, owning_account_id, \
    anchor_response_id, last_input_fingerprint, last_input_count, reasoning_cache_ref, state, \
    created_at, updated_at, last_activity_at FROM continuity_sessions WHERE session_key = ?";

/// CRUD over the continuity state machine. Cheap to construct (clones the pool handle).
pub struct ContinuityRepo {
    pool: SqlitePool,
}

impl ContinuityRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Fetch a session row by key.
    pub async fn get_session(&self, session_key: &str) -> Result<Option<SessionRow>, StoreError> {
        let row = sqlx::query_as::<_, SessionRow>(SELECT_SESSION)
            .bind(session_key)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row)
    }

    /// Resolve a `response_id` to its owning account id, if known.
    pub async fn get_anchor_owner(&self, response_id: &str) -> Result<Option<String>, StoreError> {
        let owner: Option<(String,)> = sqlx::query_as::<_, (String,)>(
            "SELECT owning_account_id FROM continuity_anchors WHERE response_id = ?",
        )
        .bind(response_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(owner.map(|(o,)| o))
    }

    /// Create the session row `state='fresh'` if it does not already exist (idempotent).
    pub async fn ensure_session(
        &self,
        session_key: &str,
        key_strength: &str,
        now: i64,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT OR IGNORE INTO continuity_sessions \
             (session_key, key_strength, state, created_at, updated_at, last_activity_at) \
             VALUES (?, ?, 'fresh', ?, ?, ?)",
        )
        .bind(session_key)
        .bind(key_strength)
        .bind(now)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Set the session state (e.g. `'reattaching'`) + bump activity timestamps.
    pub async fn set_state(&self, session_key: &str, state: &str, now: i64) -> Result<(), StoreError> {
        sqlx::query(
            "UPDATE continuity_sessions SET state = ?, updated_at = ?, last_activity_at = ? \
             WHERE session_key = ?",
        )
        .bind(state)
        .bind(now)
        .bind(now)
        .bind(session_key)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Record a completed turn: pin owner + anchor + `state='anchored'`, and map the response id
    /// to its owner. Atomic (single transaction). The session row must already exist (prepare
    /// calls `ensure_session`); `INSERT OR IGNORE` guards a race.
    #[allow(clippy::too_many_arguments)]
    pub async fn record_completion(
        &self,
        session_key: &str,
        key_strength: &str,
        owning_account: &str,
        anchor_response_id: &str,
        input_fingerprint: &str,
        input_count: i64,
        now: i64,
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "INSERT OR IGNORE INTO continuity_sessions \
             (session_key, key_strength, state, created_at, updated_at, last_activity_at) \
             VALUES (?, ?, 'fresh', ?, ?, ?)",
        )
        .bind(session_key)
        .bind(key_strength)
        .bind(now)
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE continuity_sessions SET owning_account_id = ?, anchor_response_id = ?, \
             last_input_fingerprint = ?, last_input_count = ?, state = 'anchored', \
             updated_at = ?, last_activity_at = ? WHERE session_key = ?",
        )
        .bind(owning_account)
        .bind(anchor_response_id)
        .bind(input_fingerprint)
        .bind(input_count)
        .bind(now)
        .bind(now)
        .bind(session_key)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT OR REPLACE INTO continuity_anchors \
             (response_id, session_key, owning_account_id, created_at) VALUES (?, ?, ?, ?)",
        )
        .bind(anchor_response_id)
        .bind(session_key)
        .bind(owning_account)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Record a recovery. If a new anchor id was produced, re-home the owner + anchor + map it;
    /// otherwise just mark the session `anchored` again (Strategy B produced no new id).
    pub async fn record_recovery(
        &self,
        session_key: &str,
        owning_account: &str,
        new_response_id: Option<&str>,
        now: i64,
    ) -> Result<(), StoreError> {
        match new_response_id {
            Some(rid) => {
                let mut tx = self.pool.begin().await?;
                sqlx::query(
                    "UPDATE continuity_sessions SET owning_account_id = ?, anchor_response_id = ?, \
                     state = 'anchored', updated_at = ?, last_activity_at = ? WHERE session_key = ?",
                )
                .bind(owning_account)
                .bind(rid)
                .bind(now)
                .bind(now)
                .bind(session_key)
                .execute(&mut *tx)
                .await?;
                sqlx::query(
                    "INSERT OR REPLACE INTO continuity_anchors \
                     (response_id, session_key, owning_account_id, created_at) VALUES (?, ?, ?, ?)",
                )
                .bind(rid)
                .bind(session_key)
                .bind(owning_account)
                .bind(now)
                .execute(&mut *tx)
                .await?;
                tx.commit().await?;
            }
            None => {
                sqlx::query(
                    "UPDATE continuity_sessions SET state = 'anchored', updated_at = ?, \
                     last_activity_at = ? WHERE session_key = ?",
                )
                .bind(now)
                .bind(now)
                .bind(session_key)
                .execute(&self.pool)
                .await?;
            }
        }
        Ok(())
    }
}
```

- [ ] **Step 5: Add `Store::continuity()` + module wiring**

In `crates/polyflare-store/src/store.rs`, add the import + method:

```rust
use crate::continuity_repo::ContinuityRepo;
```
```rust
    /// The continuity repository over this store's pool.
    pub fn continuity(&self) -> ContinuityRepo {
        ContinuityRepo::new(self.pool.clone())
    }
```

In `crates/polyflare-store/src/lib.rs`, register + export:

```rust
pub mod continuity_repo;
```
```rust
pub use continuity_repo::{ContinuityRepo, SessionRow};
```

- [ ] **Step 6: Run the repo tests to verify they pass**

Run: `cargo test -p polyflare-store continuity`
Expected: PASS (4 tests).

- [ ] **Step 7: fmt + clippy + commit**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings
git add crates/polyflare-store/
git commit -m "feat(m3): 0002_continuity migration + ContinuityRepo (sessions + anchors) (C2)"
```

---

## Task C3: Session-key derivation + `is_full_resend` + anchor extraction

**Files:**
- Create: `crates/polyflare-server/src/session_key.rs`
- Modify: `crates/polyflare-server/src/lib.rs`

**Interfaces:**
- Produces: `pub fn sha256_hex(bytes: &[u8]) -> String`; `pub fn derive_request_ctx(headers: &HeaderMap, body: &Value) -> RequestCtx`; internal `derive_session_key`, `is_full_resend`.
- Consumes: `polyflare_core::{RequestCtx, SessionKey, KeyStrength}`.

- [ ] **Step 1: Write the derivation tests first**

Create `crates/polyflare-server/src/session_key.rs`:

```rust
//! Ingress-time derivation of the continuity RequestCtx from headers + body: the session key + its
//! strength, whether the input is a full-resend, and any client-supplied previous_response_id.
//!
//! VERIFY-at-implementation (SPEC-M3 risk 4): the exact Codex CLI header names
//! (`x-codex-turn-state`, session / `prompt_cache_key`) must be re-verified against the live CLI —
//! a wrong key silently weakens ownership. The rules below mirror codex-lb `helpers.py:988-1064`
//! (session key) and `helpers.py:849-861` (full-resend heuristic).

use axum::http::HeaderMap;
use polyflare_core::{KeyStrength, RequestCtx, SessionKey};
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Lowercase hex sha256 of `bytes`. Used for stable, content-free session keys + input fingerprints.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    hex::encode(h.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hdr(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                v.parse().unwrap(),
            );
        }
        h
    }

    #[test]
    fn turn_state_header_yields_hard_key() {
        let ctx = derive_request_ctx(&hdr(&[("x-codex-turn-state", "ts-abc")]), &serde_json::json!({}));
        let sk = ctx.session_key.unwrap();
        assert_eq!(sk.strength, KeyStrength::Hard);
    }

    #[test]
    fn session_header_yields_hard_key() {
        let ctx = derive_request_ctx(&hdr(&[("session_id", "sess-1")]), &serde_json::json!({}));
        assert_eq!(ctx.session_key.unwrap().strength, KeyStrength::Hard);
    }

    #[test]
    fn no_session_headers_yields_soft_key() {
        let ctx = derive_request_ctx(&hdr(&[]), &serde_json::json!({"input": "hi"}));
        assert_eq!(ctx.session_key.unwrap().strength, KeyStrength::Soft);
    }

    #[test]
    fn multi_item_input_is_full_resend() {
        let ctx = derive_request_ctx(
            &hdr(&[]),
            &serde_json::json!({"input": [{"a": 1}, {"b": 2}]}),
        );
        assert!(ctx.is_full_resend);
    }

    #[test]
    fn single_small_item_is_not_full_resend() {
        let ctx = derive_request_ctx(&hdr(&[]), &serde_json::json!({"input": [{"role": "user"}]}));
        assert!(!ctx.is_full_resend);
    }

    #[test]
    fn long_string_input_is_full_resend() {
        let big = "x".repeat(4096);
        let ctx = derive_request_ctx(&hdr(&[]), &serde_json::json!({"input": big}));
        assert!(ctx.is_full_resend);
    }

    #[test]
    fn previous_response_id_is_extracted() {
        let ctx = derive_request_ctx(
            &hdr(&[]),
            &serde_json::json!({"previous_response_id": "resp_9", "input": "hi"}),
        );
        assert_eq!(ctx.client_previous_response_id.as_deref(), Some("resp_9"));
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p polyflare-server --lib session_key`
Expected: FAIL — `cannot find function derive_request_ctx`.

- [ ] **Step 3: Implement the derivation**

Insert above the `#[cfg(test)]` module in `session_key.rs`:

```rust
fn header_str(headers: &HeaderMap, name: &str) -> Option<String> {
    headers.get(name).and_then(|v| v.to_str().ok()).map(str::to_string)
}

/// Multi-item input array, or a string ≥ 4096 chars, or a single item serializing ≥ 4096 chars.
/// Faithful to codex-lb `helpers.py:849-861` (VERIFY against `../codex-lb` at implementation).
fn is_full_resend(input: Option<&Value>) -> bool {
    match input {
        Some(Value::String(s)) => s.len() >= 4096,
        Some(Value::Array(items)) => {
            if items.len() >= 2 {
                true
            } else if items.len() == 1 {
                serde_json::to_string(&items[0]).map(|s| s.len() >= 4096).unwrap_or(false)
            } else {
                false
            }
        }
        _ => false,
    }
}

/// Derive the session key: `x-codex-turn-state` ⇒ Hard; else a session header (+ `prompt_cache_key`
/// isolating threads) ⇒ Hard; else a soft key from `x-request-id` / `prompt_cache_key` / content
/// hash. Values are hashed so no raw header/content is stored.
fn derive_session_key(headers: &HeaderMap, body: &Value) -> SessionKey {
    if let Some(ts) = header_str(headers, "x-codex-turn-state") {
        return SessionKey { value: sha256_hex(format!("turn:{ts}").as_bytes()), strength: KeyStrength::Hard };
    }
    if let Some(sess) = header_str(headers, "session_id").or_else(|| header_str(headers, "x-session-id")) {
        let mut raw = sess;
        if let Some(pck) = body.get("prompt_cache_key").and_then(|v| v.as_str()) {
            raw = format!("{raw}:{pck}");
        }
        return SessionKey { value: sha256_hex(format!("session:{raw}").as_bytes()), strength: KeyStrength::Hard };
    }
    let soft = header_str(headers, "x-request-id")
        .or_else(|| body.get("prompt_cache_key").and_then(|v| v.as_str()).map(str::to_string))
        .unwrap_or_else(|| body.get("input").map(|i| i.to_string()).unwrap_or_default());
    SessionKey { value: sha256_hex(format!("soft:{soft}").as_bytes()), strength: KeyStrength::Soft }
}

/// Build the continuity `RequestCtx` from headers + body BEFORE `prepare`.
pub fn derive_request_ctx(headers: &HeaderMap, body: &Value) -> RequestCtx {
    let session_key = derive_session_key(headers, body);
    let client_previous_response_id = body
        .get("previous_response_id")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let is_full_resend = is_full_resend(body.get("input"));
    let session_id = header_str(headers, "session_id").or_else(|| header_str(headers, "x-session-id"));
    RequestCtx { session_id, session_key: Some(session_key), client_previous_response_id, is_full_resend }
}
```

- [ ] **Step 4: Register the module**

In `crates/polyflare-server/src/lib.rs` add:

```rust
pub mod session_key;
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p polyflare-server --lib session_key`
Expected: PASS (7 tests).

- [ ] **Step 6: fmt + clippy + commit**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings
git add crates/polyflare-server/src/session_key.rs crates/polyflare-server/src/lib.rs
git commit -m "feat(m3): ingress session-key derivation + is_full_resend + anchor extraction (C3)"
```

---

## Task C4: `CodexContinuity::prepare` — owner resolution, watchdog arm, recovery plan

**Files:**
- Create: `crates/polyflare-server/src/continuity.rs`
- Modify: `crates/polyflare-server/src/lib.rs`

**Interfaces:**
- Consumes: `polyflare_store::ContinuityRepo`; `polyflare_core::{Continuity, Prepared, PreparedRequest, RequestCtx, ContinuityDirective, WatchdogArm, RecoveryPlan, AccountId, KeyStrength}`.
- Produces: `CodexContinuity::new(repo: ContinuityRepo, watchdog_timeout: Duration) -> Self` implementing `Continuity`. `prepare` resolves `pin_account` (anchor map first, then session row), arms the watchdog only when an anchor is present, and builds `ResendFull { anchorless_req }` (full-resend) or `SignalClient` (bare tail).

- [ ] **Step 1: Write `prepare` tests first**

Create `crates/polyflare-server/src/continuity.rs`:

```rust
//! The Codex continuity state machine: a store-backed `Continuity` impl. Holds a `ContinuityRepo`;
//! persists NO conversation content — only session state + a response_id -> owner map.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use polyflare_core::{
    AccountId, Continuity, ContinuityDirective, ContinuityError, KeyStrength, Prepared,
    PreparedRequest, RecoveryPlan, RequestCtx, TurnOutcome, WatchdogArm,
};
use polyflare_store::{ContinuityRepo, StoreError};

fn now_secs() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
}

fn box_store_err(e: StoreError) -> ContinuityError {
    ContinuityError::Store(Box::new(e))
}

fn strength_str(s: KeyStrength) -> &'static str {
    match s {
        KeyStrength::Hard => "hard",
        KeyStrength::Soft => "soft",
    }
}

/// Codex continuity backed by a `ContinuityRepo`. `watchdog_timeout` (N) is stamped into the
/// directive on every anchor-bearing request.
pub struct CodexContinuity {
    repo: ContinuityRepo,
    watchdog_timeout: Duration,
}

impl CodexContinuity {
    pub fn new(repo: ContinuityRepo, watchdog_timeout: Duration) -> Self {
        Self { repo, watchdog_timeout }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polyflare_store::Store;

    async fn make() -> (Store, CodexContinuity) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("s.db")).await.unwrap();
        std::mem::forget(dir);
        let cont = CodexContinuity::new(store.continuity(), Duration::from_millis(150));
        (store, cont)
    }

    fn req(body: serde_json::Value) -> PreparedRequest {
        PreparedRequest { body, model: "gpt-5.6-sol".to_string() }
    }

    #[tokio::test]
    async fn no_anchor_disarms_and_does_not_pin() {
        let (_s, cont) = make().await;
        let ctx = RequestCtx {
            session_key: Some(polyflare_core::SessionKey { value: "sk".into(), strength: KeyStrength::Soft }),
            ..Default::default()
        };
        let p = cont.prepare(req(serde_json::json!({"input": "hi"})), &ctx).await.unwrap();
        assert!(p.directive.pin_account.is_none());
        assert!(matches!(p.directive.watchdog, WatchdogArm::Disarmed));
        assert!(matches!(p.directive.recovery, RecoveryPlan::None));
    }

    #[tokio::test]
    async fn anchor_full_resend_arms_with_resendfull_stripped() {
        let (_s, cont) = make().await;
        let ctx = RequestCtx {
            session_key: Some(polyflare_core::SessionKey { value: "sk".into(), strength: KeyStrength::Soft }),
            client_previous_response_id: Some("resp_dead".into()),
            is_full_resend: true,
            ..Default::default()
        };
        let body = serde_json::json!({"previous_response_id": "resp_dead", "input": [{"a":1},{"b":2}]});
        let p = cont.prepare(req(body), &ctx).await.unwrap();
        assert!(matches!(p.directive.watchdog, WatchdogArm::Armed { .. }));
        match p.directive.recovery {
            RecoveryPlan::ResendFull { anchorless_req } => {
                assert!(anchorless_req.body.get("previous_response_id").is_none(), "anchor stripped");
                assert!(anchorless_req.body.get("input").is_some(), "full input preserved");
            }
            other => panic!("expected ResendFull, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn anchor_bare_tail_arms_with_signalclient() {
        let (_s, cont) = make().await;
        let ctx = RequestCtx {
            session_key: Some(polyflare_core::SessionKey { value: "sk".into(), strength: KeyStrength::Soft }),
            client_previous_response_id: Some("resp_x".into()),
            is_full_resend: false,
            ..Default::default()
        };
        let body = serde_json::json!({"previous_response_id": "resp_x", "input": "tail"});
        let p = cont.prepare(req(body), &ctx).await.unwrap();
        assert!(matches!(p.directive.watchdog, WatchdogArm::Armed { .. }));
        assert!(matches!(p.directive.recovery, RecoveryPlan::SignalClient));
    }

    #[tokio::test]
    async fn anchor_map_resolves_owner_for_pin() {
        let (store, cont) = make().await;
        // Seed an account + a completed turn so the anchor map knows resp_1 -> A.
        sqlx::query(
            "INSERT INTO accounts (id, email, access_token_enc, refresh_token_enc, id_token_enc, created_at) \
             VALUES ('A', 'e@x', X'00', X'00', X'00', 0)",
        )
        .execute(store.pool())
        .await
        .unwrap();
        store.continuity().ensure_session("skA", "soft", 1).await.unwrap();
        store.continuity().record_completion("skA", "soft", "A", "resp_1", "fp", 2, 1).await.unwrap();

        let ctx = RequestCtx {
            session_key: Some(polyflare_core::SessionKey { value: "skZ".into(), strength: KeyStrength::Soft }),
            client_previous_response_id: Some("resp_1".into()),
            is_full_resend: true,
            ..Default::default()
        };
        let body = serde_json::json!({"previous_response_id": "resp_1", "input": [{"a":1},{"b":2}]});
        let p = cont.prepare(req(body), &ctx).await.unwrap();
        assert_eq!(p.directive.pin_account, Some(AccountId::from("A")), "anchor map pins to owner");
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p polyflare-server --lib continuity::tests::no_anchor_disarms_and_does_not_pin`
Expected: FAIL — `Continuity` not implemented for `CodexContinuity`.

- [ ] **Step 3: Implement `prepare` (+ a stub `observe` for now)**

Insert the trait impl between the `impl CodexContinuity` block and the `#[cfg(test)]` module in `continuity.rs`:

```rust
#[async_trait]
impl Continuity for CodexContinuity {
    async fn prepare(
        &self,
        req: PreparedRequest,
        ctx: &RequestCtx,
    ) -> Result<Prepared, ContinuityError> {
        let now = now_secs();
        let session_key = ctx.session_key.clone();
        let anchor = ctx.client_previous_response_id.clone();

        // Resolve the owner: the client-supplied anchor map is authoritative; else the session row.
        let mut owner: Option<AccountId> = None;
        if let Some(rid) = anchor.as_deref() {
            if let Some(acc) = self.repo.get_anchor_owner(rid).await.map_err(box_store_err)? {
                owner = Some(AccountId::from(acc));
            }
        }
        if owner.is_none() {
            if let Some(sk) = session_key.as_ref() {
                if let Some(row) = self.repo.get_session(&sk.value).await.map_err(box_store_err)? {
                    owner = row.owning_account_id.map(AccountId::from);
                }
            }
        }

        // Ensure a session row exists (Fresh on miss); mark reattaching when an anchor is in flight.
        if let Some(sk) = session_key.as_ref() {
            self.repo
                .ensure_session(&sk.value, strength_str(sk.strength), now)
                .await
                .map_err(box_store_err)?;
            if anchor.is_some() {
                self.repo.set_state(&sk.value, "reattaching", now).await.map_err(box_store_err)?;
            }
        }

        // Arm the watchdog ONLY on anchor-bearing requests; pick the recovery strategy.
        let (watchdog, recovery) = if anchor.is_some() {
            let arm = WatchdogArm::Armed { timeout: self.watchdog_timeout };
            if ctx.is_full_resend {
                let mut stripped = req.body.clone();
                if let Some(obj) = stripped.as_object_mut() {
                    obj.remove("previous_response_id");
                }
                let anchorless_req = PreparedRequest { body: stripped, model: req.model.clone() };
                (arm, RecoveryPlan::ResendFull { anchorless_req })
            } else {
                (arm, RecoveryPlan::SignalClient)
            }
        } else {
            (WatchdogArm::Disarmed, RecoveryPlan::None)
        };

        Ok(Prepared {
            req,
            directive: ContinuityDirective { pin_account: owner, watchdog, recovery, session_key },
        })
    }

    async fn observe(&self, _outcome: TurnOutcome, _ctx: &RequestCtx) -> Result<(), ContinuityError> {
        // Implemented in C6.
        Ok(())
    }
}
```

- [ ] **Step 4: Register the module**

In `crates/polyflare-server/src/lib.rs` add:

```rust
pub mod continuity;
```

- [ ] **Step 5: Run the `prepare` tests to verify they pass**

Run: `cargo test -p polyflare-server --lib continuity`
Expected: PASS (4 tests).

- [ ] **Step 6: fmt + clippy + commit**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings
git add crates/polyflare-server/src/continuity.rs crates/polyflare-server/src/lib.rs
git commit -m "feat(m3): CodexContinuity::prepare — owner resolution + watchdog arm + recovery plan (C4)"
```

---

## Task C5: The watchdog — `apply_ownership` + `execute_with_watchdog` + recovery + observing stream

*This is the riskiest task. The core is the first-byte race (`tokio::time::timeout(N, stream.next())`), the cancel-safe drop of the dead stream on silence, the peek-before-relay reconstruction (`stream::once(first).chain(rest)`), and the observe-on-stream-end adapter.*

**Files:**
- Create: `crates/polyflare-server/src/watchdog.rs`
- Create: `crates/polyflare-server/tests/watchdog_race.rs`
- Modify: `crates/polyflare-server/src/lib.rs`

**Interfaces:**
- Consumes: `polyflare_core::{Executor, Continuity, Account, AccountId, AccountSnapshot, Selector, SelectionCtx, PreparedRequest, Prepared, ContinuityDirective, WatchdogArm, RecoveryPlan, TurnOutcome, SessionKey, ResponseStream, ExecError}`; `crate::session_key::sha256_hex`.
- Produces:
  - `pub enum RouteDecision { Route(AccountId), Recover, NoEligibleAccount }`
  - `pub fn apply_ownership(directive: &ContinuityDirective, candidates: &[AccountSnapshot], selector: &dyn Selector, ctx: &SelectionCtx) -> RouteDecision`
  - `pub async fn execute_with_watchdog(executor: &dyn Executor, continuity: Arc<dyn Continuity>, prepared: Prepared, account: &Account, account_id: AccountId, ctx: RequestCtx) -> Result<ResponseStream, WatchdogError>`
  - `pub async fn execute_recovery(executor: &dyn Executor, continuity: Arc<dyn Continuity>, anchorless_req: PreparedRequest, account: &Account, account_id: AccountId, ctx: RequestCtx, session_key: Option<SessionKey>) -> Result<ResponseStream, WatchdogError>`
  - `pub async fn signal_client_stream(continuity: Arc<dyn Continuity>, ctx: RequestCtx, account_id: AccountId, session_key: Option<SessionKey>) -> ResponseStream`
  - `pub enum WatchdogError { Upstream, Continuity }`

- [ ] **Step 1: Write the watchdog integration tests first**

Create `crates/polyflare-server/tests/watchdog_race.rs`:

```rust
//! Unit/integration tests for the watchdog first-byte race, driving `execute_with_watchdog`
//! directly against a MockUpstream + CodexExecutor with a tiny N.

use std::sync::Arc;

use futures_util::StreamExt;
use polyflare_codex::CodexExecutor;
use polyflare_core::{
    Account, AccountId, Continuity, ContinuityDirective, NoopContinuity, Prepared, PreparedRequest,
    RecoveryPlan, RequestCtx, WatchdogArm,
};
use polyflare_server::watchdog::{execute_with_watchdog, WatchdogError};
use polyflare_testkit::MockUpstream;
use std::time::Duration;

fn core_account(base_url: String) -> Account {
    Account { id: "acct".into(), base_url, bearer_token: "tok".into() }
}

async fn drain(stream: polyflare_core::ResponseStream) -> String {
    let mut body = String::new();
    let mut s = stream;
    while let Some(chunk) = s.next().await {
        body.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
    }
    body
}

fn armed_full_resend(body: serde_json::Value) -> Prepared {
    // Anchor present + full-resend => Armed + ResendFull(anchor stripped).
    let mut stripped = body.clone();
    stripped.as_object_mut().unwrap().remove("previous_response_id");
    Prepared {
        req: PreparedRequest { body, model: "m".into() },
        directive: ContinuityDirective {
            pin_account: None,
            watchdog: WatchdogArm::Armed { timeout: Duration::from_millis(150) },
            recovery: RecoveryPlan::ResendFull {
                anchorless_req: PreparedRequest { body: stripped, model: "m".into() },
            },
            session_key: None,
        },
    }
}

#[tokio::test]
async fn relays_when_first_byte_arrives_before_timeout() {
    let mock = MockUpstream::with_ids(vec![
        r#"{"type":"response.output_text.delta","delta":"hi"}"#.to_string(),
    ]);
    let handle = mock.clone();
    let base = mock.spawn().await;
    let exec = CodexExecutor::new().unwrap();
    let cont: Arc<dyn Continuity> = Arc::new(NoopContinuity);

    // Anchor present but the mock (with_ids, not silent) responds promptly => alive => relay.
    let prepared = armed_full_resend(serde_json::json!({"previous_response_id": "resp_a", "input": [{"a":1},{"b":2}]}));
    let stream = execute_with_watchdog(&exec, cont, prepared, &core_account(base), AccountId::from("acct"), RequestCtx::default())
        .await
        .unwrap();
    let body = drain(stream).await;
    assert!(body.contains("response.completed"));
    assert_eq!(handle.request_count(), 1, "no recovery needed");
}

#[tokio::test]
async fn recovers_on_silence_via_resend_full() {
    let mock = MockUpstream::silent_on_anchor(vec![
        r#"{"type":"response.output_text.delta","delta":"recovered"}"#.to_string(),
    ]);
    let handle = mock.clone();
    let base = mock.spawn().await;
    let exec = CodexExecutor::new().unwrap();
    let cont: Arc<dyn Continuity> = Arc::new(NoopContinuity);

    let orig = serde_json::json!({"previous_response_id": "resp_dead", "input": [{"a":1},{"b":2}]});
    let prepared = armed_full_resend(orig.clone());
    let stream = execute_with_watchdog(&exec, cont, prepared, &core_account(base), AccountId::from("acct"), RequestCtx::default())
        .await
        .unwrap();

    let done = tokio::time::timeout(Duration::from_secs(3), drain(stream)).await.expect("bounded");
    assert!(done.contains("response.completed"), "recovery stream completed");
    assert_eq!(handle.request_count(), 2, "silent attempt + recovery");
    let bodies = handle.bodies();
    assert!(bodies[0].get("previous_response_id").is_some(), "1st carried the dead anchor");
    assert!(bodies[1].get("previous_response_id").is_none(), "recovery stripped the anchor");
    // R1: the recovery's input equals the client's input (never trimmed).
    assert_eq!(bodies[1]["input"], orig["input"], "full-resend not trimmed");
}

#[tokio::test]
async fn hard_upstream_error_is_watchdog_upstream() {
    let exec = CodexExecutor::new().unwrap();
    let cont: Arc<dyn Continuity> = Arc::new(NoopContinuity);
    // Unreachable upstream => execute() errors before any stream.
    let prepared = armed_full_resend(serde_json::json!({"previous_response_id": "resp_a", "input": [{"a":1},{"b":2}]}));
    let res = execute_with_watchdog(&exec, cont, prepared, &core_account("http://127.0.0.1:1".into()), AccountId::from("acct"), RequestCtx::default()).await;
    assert!(matches!(res, Err(WatchdogError::Upstream)));
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p polyflare-server --test watchdog_race`
Expected: FAIL — `unresolved import polyflare_server::watchdog`.

- [ ] **Step 3: Implement `watchdog.rs`**

Create `crates/polyflare-server/src/watchdog.rs`:

```rust
//! The silence watchdog + ownership pre-filter. `execute_with_watchdog` races the FIRST upstream
//! chunk against N; on silence it drops the dead stream (cancel-safe) and recovers. Peek-before-
//! relay: no client byte is written until the first upstream chunk arrives, so a restart is always
//! safe. The Codex executor is untouched — this wraps it in the server.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_core::Stream;
use futures_util::stream::{self, StreamExt};
use polyflare_core::{
    Account, AccountId, AccountSnapshot, Continuity, ContinuityDirective, ExecError, Executor,
    Prepared, PreparedRequest, RecoveryPlan, RequestCtx, ResponseStream, Selector, SelectionCtx,
    SessionKey, TurnOutcome, WatchdogArm,
};

use crate::session_key::sha256_hex;

/// VERIFY-at-implementation (SPEC-M3 risk 7): capture the exact `previous_response_not_found` shape
/// the real Codex CLI / Claude Code self-heals from (codex-lb masking behavior or a live capture)
/// and finalize this payload. Tests assert on the code substring only, so they survive the change.
const SIGNAL_SSE: &[u8] = concat!(
    "event: response.failed\n",
    "data: {\"type\":\"response.failed\",\"response\":{\"error\":",
    "{\"code\":\"previous_response_not_found\",\"message\":\"anchor not resumable; resend full history\"}}}\n\n",
).as_bytes();

/// Where a request should go after ownership resolution.
pub enum RouteDecision {
    /// Execute normally on this account (owner eligible, or unowned pick).
    Route(AccountId),
    /// Owner is pinned but ineligible ⇒ recover (never a hard fail).
    Recover,
    /// Unowned and the pool is empty ⇒ 503.
    NoEligibleAccount,
}

/// Errors the watchdog surfaces. Generic — never leaks a token, URL, or internal `Display`.
#[derive(Debug, thiserror::Error)]
pub enum WatchdogError {
    #[error("upstream error")]
    Upstream,
    #[error("continuity recovery unavailable")]
    Continuity,
}

/// HARD ownership pre-filter: narrow candidates to the pinned owner BEFORE `Selector::pick` (no
/// Selector-trait change). Owner ineligible ⇒ `Recover`; unowned + empty ⇒ `NoEligibleAccount`.
pub fn apply_ownership(
    directive: &ContinuityDirective,
    candidates: &[AccountSnapshot],
    selector: &dyn Selector,
    ctx: &SelectionCtx,
) -> RouteDecision {
    match directive.pin_account.as_ref() {
        Some(owner) => {
            let narrowed: Vec<AccountSnapshot> =
                candidates.iter().filter(|s| &s.id == owner).cloned().collect();
            match selector.pick(&narrowed, ctx) {
                Some(id) => RouteDecision::Route(id),
                None => RouteDecision::Recover,
            }
        }
        None => match selector.pick(candidates, ctx) {
            Some(id) => RouteDecision::Route(id),
            None => RouteDecision::NoEligibleAccount,
        },
    }
}

/// Diagnostic input fingerprint (sha256 hex of the `input` JSON) + item count. Not used to gate a
/// trim in M3-core (we never trim) — recorded by `observe` for diagnostics only.
fn input_fingerprint_and_count(body: &serde_json::Value) -> (String, u32) {
    let input = body.get("input");
    let count = match input {
        Some(serde_json::Value::Array(a)) => a.len() as u32,
        Some(_) => 1,
        None => 0,
    };
    let canon = input.map(|v| v.to_string()).unwrap_or_default();
    (sha256_hex(canon.as_bytes()), count)
}

/// Execute a prepared request under the watchdog. Disarmed (no anchor) ⇒ relay + sniff + observe
/// Completed. Armed ⇒ race the first byte: alive ⇒ rebuild + relay; hard error ⇒ observe Failed +
/// `Upstream`; silence/empty ⇒ drop the dead stream and recover per the directive.
pub async fn execute_with_watchdog(
    executor: &dyn Executor,
    continuity: Arc<dyn Continuity>,
    prepared: Prepared,
    account: &Account,
    account_id: AccountId,
    ctx: RequestCtx,
) -> Result<ResponseStream, WatchdogError> {
    let Prepared { req, directive } = prepared;
    let session_key = directive.session_key.clone();
    let (fp, count) = input_fingerprint_and_count(&req.body);

    match directive.watchdog {
        WatchdogArm::Disarmed => {
            // No anchor ⇒ cannot be silent. Relay + sniff + observe(Completed).
            let stream = executor.execute(req, account).await.map_err(|_| WatchdogError::Upstream)?;
            Ok(wrap_stream(stream, continuity, ctx, account_id, session_key, OutcomeKind::Completed { fp, count }))
        }
        WatchdogArm::Armed { timeout } => {
            let mut stream = executor.execute(req, account).await.map_err(|_| WatchdogError::Upstream)?;
            match tokio::time::timeout(timeout, stream.next()).await {
                Ok(Some(Ok(first))) => {
                    // ALIVE: rebuild the full stream (peek-before-relay) + sniff + observe(Completed).
                    let rebuilt: ResponseStream =
                        Box::pin(stream::once(async move { Ok::<Bytes, ExecError>(first) }).chain(stream));
                    Ok(wrap_stream(rebuilt, continuity, ctx, account_id, session_key, OutcomeKind::Completed { fp, count }))
                }
                Ok(Some(Err(_))) => {
                    // Hard upstream error before any client byte ⇒ observe(Failed) + 502.
                    let _ = continuity
                        .observe(TurnOutcome::Failed { session_key: session_key.clone() }, &ctx)
                        .await;
                    Err(WatchdogError::Upstream)
                }
                Ok(None) | Err(_) => {
                    // Ok(None): upstream closed with zero events on an anchored req == dead anchor.
                    // Err(_): the N timeout elapsed == the wedge. Both ⇒ RECOVER. Drop = cancel.
                    drop(stream);
                    match directive.recovery {
                        RecoveryPlan::ResendFull { anchorless_req } => {
                            execute_recovery(executor, continuity, anchorless_req, account, account_id, ctx, session_key).await
                        }
                        RecoveryPlan::SignalClient => {
                            Ok(signal_client_stream(continuity, ctx, account_id, session_key).await)
                        }
                        RecoveryPlan::None => Err(WatchdogError::Continuity),
                    }
                }
            }
        }
    }
}

/// Re-execute an anchor-stripped request (Strategy A). Anchorless ⇒ cannot be silent, so no second
/// watchdog. Sniffs the new id and observes `Recovered`.
pub async fn execute_recovery(
    executor: &dyn Executor,
    continuity: Arc<dyn Continuity>,
    anchorless_req: PreparedRequest,
    account: &Account,
    account_id: AccountId,
    ctx: RequestCtx,
    session_key: Option<SessionKey>,
) -> Result<ResponseStream, WatchdogError> {
    let stream = executor.execute(anchorless_req, account).await.map_err(|_| WatchdogError::Upstream)?;
    Ok(wrap_stream(stream, continuity, ctx, account_id, session_key, OutcomeKind::Recovered))
}

/// Emit a synthetic `previous_response_not_found` (Strategy B) so the client self-heals with a full
/// resend. Observes `Recovered` (no new id) and returns a one-shot stream. No upstream call.
pub async fn signal_client_stream(
    continuity: Arc<dyn Continuity>,
    ctx: RequestCtx,
    account_id: AccountId,
    session_key: Option<SessionKey>,
) -> ResponseStream {
    let _ = continuity
        .observe(TurnOutcome::Recovered { session_key, account: account_id, new_response_id: None }, &ctx)
        .await;
    Box::pin(stream::once(async move { Ok::<Bytes, ExecError>(Bytes::from_static(SIGNAL_SSE)) }))
}

// ---- observe-on-stream-end + response-id sniffing ------------------------------------------------

#[derive(Clone)]
enum OutcomeKind {
    Completed { fp: String, count: u32 },
    Recovered,
}

fn build_outcome(
    kind: OutcomeKind,
    session_key: Option<SessionKey>,
    account: AccountId,
    id: Option<String>,
) -> TurnOutcome {
    match kind {
        OutcomeKind::Completed { fp, count } => TurnOutcome::Completed {
            session_key,
            account,
            response_id: id,
            input_fingerprint: fp,
            input_count: count,
            reasoning: None,
        },
        OutcomeKind::Recovered => TurnOutcome::Recovered { session_key, account, new_response_id: id },
    }
}

/// Bounded, non-buffering sniffer for the streamed `response.id`. Accumulates ≤ 64 KiB until it can
/// parse a `response.created`/`response.completed` id, then stops accumulating and forwards bytes.
struct ResponseIdSniffer {
    buf: Vec<u8>,
    id: Option<String>,
    done: bool,
}

impl ResponseIdSniffer {
    fn new() -> Self {
        Self { buf: Vec::new(), id: None, done: false }
    }

    fn feed(&mut self, bytes: &Bytes) {
        if self.done {
            return;
        }
        self.buf.extend_from_slice(bytes);
        if let Some(id) = extract_response_id(&self.buf) {
            self.id = Some(id);
            self.done = true;
            self.buf = Vec::new();
        } else if self.buf.len() > 64 * 1024 {
            self.done = true; // give up sniffing; stay non-buffering
            self.buf = Vec::new();
        }
    }

    fn take_id(&mut self) -> Option<String> {
        self.id.take()
    }
}

fn extract_response_id(buf: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(buf);
    for line in text.lines() {
        let Some(payload) = line.strip_prefix("data:").map(str::trim) else { continue };
        if payload == "[DONE]" {
            continue;
        }
        let Ok(v) = serde_json::from_str::<serde_json::Value>(payload) else { continue };
        let ty = v.get("type").and_then(|t| t.as_str()).unwrap_or_default();
        if ty == "response.created" || ty == "response.completed" {
            if let Some(id) = v.get("response").and_then(|r| r.get("id")).and_then(|i| i.as_str()) {
                return Some(id.to_string());
            }
        }
    }
    None
}

enum ObserveState {
    Streaming,
    Observing(Pin<Box<dyn Future<Output = ()> + Send>>),
    Done,
}

/// Wraps a byte stream: forwards every chunk unchanged while sniffing the `response.id`, then — on
/// stream end — awaits `continuity.observe(...)` INLINE before yielding the terminal `None`. This
/// makes ownership deterministic (turn N's state is persisted before the client sees end-of-stream).
struct ObservingStream {
    inner: ResponseStream,
    sniffer: ResponseIdSniffer,
    continuity: Arc<dyn Continuity>,
    ctx: RequestCtx,
    account: AccountId,
    session_key: Option<SessionKey>,
    kind: OutcomeKind,
    state: ObserveState,
}

impl Stream for ObservingStream {
    type Item = Result<Bytes, ExecError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut(); // ObservingStream is Unpin (all fields Unpin)
        loop {
            match &mut this.state {
                ObserveState::Streaming => match this.inner.as_mut().poll_next(cx) {
                    Poll::Ready(Some(Ok(bytes))) => {
                        this.sniffer.feed(&bytes);
                        return Poll::Ready(Some(Ok(bytes)));
                    }
                    Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                    Poll::Ready(None) => {
                        let outcome = build_outcome(
                            this.kind.clone(),
                            this.session_key.clone(),
                            this.account.clone(),
                            this.sniffer.take_id(),
                        );
                        let continuity = this.continuity.clone();
                        let ctx = this.ctx.clone();
                        let fut = Box::pin(async move {
                            let _ = continuity.observe(outcome, &ctx).await;
                        });
                        this.state = ObserveState::Observing(fut);
                        // loop: poll the observe future this wakeup
                    }
                    Poll::Pending => return Poll::Pending,
                },
                ObserveState::Observing(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(()) => {
                        this.state = ObserveState::Done;
                        return Poll::Ready(None);
                    }
                    Poll::Pending => return Poll::Pending,
                },
                ObserveState::Done => return Poll::Ready(None),
            }
        }
    }
}

fn wrap_stream(
    inner: ResponseStream,
    continuity: Arc<dyn Continuity>,
    ctx: RequestCtx,
    account: AccountId,
    session_key: Option<SessionKey>,
    kind: OutcomeKind,
) -> ResponseStream {
    Box::pin(ObservingStream {
        inner,
        sniffer: ResponseIdSniffer::new(),
        continuity,
        ctx,
        account,
        session_key,
        kind,
        state: ObserveState::Streaming,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_id_from_response_created() {
        let sse = b"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_42\"}}\n\n";
        assert_eq!(extract_response_id(sse).as_deref(), Some("resp_42"));
    }

    #[test]
    fn sniffer_is_bounded_and_stops_after_found() {
        let mut s = ResponseIdSniffer::new();
        s.feed(&Bytes::from_static(b"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\"}}\n\n"));
        assert_eq!(s.take_id().as_deref(), Some("resp_1"));
        assert!(s.done);
    }
}
```

- [ ] **Step 4: Register the module**

In `crates/polyflare-server/src/lib.rs` add:

```rust
pub mod watchdog;
```

- [ ] **Step 5: Run the watchdog tests to verify they pass**

Run: `cargo test -p polyflare-server --test watchdog_race && cargo test -p polyflare-server --lib watchdog`
Expected: PASS (3 integration + 2 unit).

- [ ] **Step 6: fmt + clippy + commit**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings
git add crates/polyflare-server/src/watchdog.rs crates/polyflare-server/src/lib.rs crates/polyflare-server/tests/watchdog_race.rs
git commit -m "feat(m3): silence watchdog — first-byte race + cancel-safe recover + observing stream (C5)"
```

---

## Task C6: `CodexContinuity::observe` — advance the state machine

**Files:**
- Modify: `crates/polyflare-server/src/continuity.rs`

**Interfaces:**
- Consumes: `ContinuityRepo::{record_completion, record_recovery}` (C2); `TurnOutcome` (C1).
- Produces: `CodexContinuity::observe` records the owning account + new anchor + advances state.

- [ ] **Step 1: Write the observe tests first**

Append to `crates/polyflare-server/src/continuity.rs` `mod tests`:

```rust
    #[tokio::test]
    async fn observe_completed_records_owner_and_anchor() {
        let (store, cont) = make().await;
        sqlx::query(
            "INSERT INTO accounts (id, email, access_token_enc, refresh_token_enc, id_token_enc, created_at) \
             VALUES ('A', 'e@x', X'00', X'00', X'00', 0)",
        )
        .execute(store.pool())
        .await
        .unwrap();
        let sk = polyflare_core::SessionKey { value: "skC".into(), strength: KeyStrength::Soft };
        cont.repo.ensure_session("skC", "soft", 1).await.unwrap();
        cont.observe(
            TurnOutcome::Completed {
                session_key: Some(sk),
                account: AccountId::from("A"),
                response_id: Some("resp_7".into()),
                input_fingerprint: "fp".into(),
                input_count: 2,
                reasoning: None,
            },
            &RequestCtx::default(),
        )
        .await
        .unwrap();
        let owner = store.continuity().get_anchor_owner("resp_7").await.unwrap();
        assert_eq!(owner.as_deref(), Some("A"));
        let row = store.continuity().get_session("skC").await.unwrap().unwrap();
        assert_eq!(row.state, "anchored");
    }

    #[tokio::test]
    async fn observe_recovered_rehomes_owner() {
        let (store, cont) = make().await;
        for id in ["A", "B"] {
            sqlx::query(
                "INSERT INTO accounts (id, email, access_token_enc, refresh_token_enc, id_token_enc, created_at) \
                 VALUES (?, 'e@x', X'00', X'00', X'00', 0)",
            )
            .bind(id)
            .execute(store.pool())
            .await
            .unwrap();
        }
        cont.repo.ensure_session("skR", "soft", 1).await.unwrap();
        cont.repo.record_completion("skR", "soft", "A", "resp_1", "fp", 2, 1).await.unwrap();
        let sk = polyflare_core::SessionKey { value: "skR".into(), strength: KeyStrength::Soft };
        cont.observe(
            TurnOutcome::Recovered { session_key: Some(sk), account: AccountId::from("B"), new_response_id: Some("resp_2".into()) },
            &RequestCtx::default(),
        )
        .await
        .unwrap();
        assert_eq!(store.continuity().get_anchor_owner("resp_2").await.unwrap().as_deref(), Some("B"));
    }
```

*(Note: the tests read `cont.repo` — keep the `repo` field crate-visible. It is a private field of a struct in the same module, so the tests can access it.)*

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p polyflare-server --lib continuity::tests::observe_completed_records_owner_and_anchor`
Expected: FAIL — assertion fails (`observe` is the C4 stub returning `Ok(())` without writing).

- [ ] **Step 3: Implement `observe`**

Replace the stub `observe` in the `impl Continuity for CodexContinuity` block:

```rust
    async fn observe(&self, outcome: TurnOutcome, _ctx: &RequestCtx) -> Result<(), ContinuityError> {
        let now = now_secs();
        match outcome {
            TurnOutcome::Completed { session_key, account, response_id, input_fingerprint, input_count, .. } => {
                if let (Some(sk), Some(rid)) = (session_key, response_id) {
                    self.repo
                        .record_completion(
                            &sk.value,
                            strength_str(sk.strength),
                            account.as_str(),
                            &rid,
                            &input_fingerprint,
                            input_count as i64,
                            now,
                        )
                        .await
                        .map_err(box_store_err)?;
                }
                Ok(())
            }
            TurnOutcome::Recovered { session_key, account, new_response_id } => {
                if let Some(sk) = session_key {
                    self.repo
                        .record_recovery(&sk.value, account.as_str(), new_response_id.as_deref(), now)
                        .await
                        .map_err(box_store_err)?;
                }
                Ok(())
            }
            TurnOutcome::Failed { .. } => Ok(()),
        }
    }
```

- [ ] **Step 4: Run the observe tests to verify they pass**

Run: `cargo test -p polyflare-server --lib continuity`
Expected: PASS (6 tests total).

- [ ] **Step 5: fmt + clippy + commit**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings
git add crates/polyflare-server/src/continuity.rs
git commit -m "feat(m3): CodexContinuity::observe — record owner/anchor + advance state (C6)"
```

---

## Task C7: Wire into ingress + AppState + config + main; make the wedge test GREEN

**Files:**
- Modify: `crates/polyflare-server/src/app.rs`, `src/config.rs`, `src/main.rs`, `src/ingress.rs`
- Modify: `crates/polyflare-server/tests/{e2e_passthrough,ingress_relays,pool_selection,refresh_path,large_body,wedge_regression}.rs`

**Interfaces:**
- Consumes: `derive_request_ctx` (C3), `CodexContinuity` (C4/C6), `apply_ownership`/`execute_with_watchdog`/`execute_recovery`/`signal_client_stream`/`RouteDecision` (C5).
- Produces: `AppState { .., continuity: Arc<dyn Continuity> }`; the full continuity-aware ingress flow.

- [ ] **Step 1: Add `continuity` to `AppState`**

In `crates/polyflare-server/src/app.rs`, extend imports + struct:

```rust
use polyflare_core::{Continuity, Executor, Selector};
```
```rust
pub struct AppState {
    pub executor: Arc<dyn Executor>,
    pub selector: Arc<dyn Selector>,
    pub continuity: Arc<dyn Continuity>,
    pub store: Store,
    pub cipher: TokenCipher,
    pub oauth: OAuthClient,
    pub upstream_base_url: String,
}
```

- [ ] **Step 2: Add the configurable watchdog N**

In `crates/polyflare-server/src/config.rs`, add `use std::time::Duration;`, a struct field, and its default:

```rust
    pub continuity_watchdog: Duration,
```
In `from_env`, before `Ok(ServeConfig { .. })`, add:

```rust
        let continuity_watchdog = std::env::var("POLYFLARE_WATCHDOG_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(30));
```
and include `continuity_watchdog,` in the returned struct.

- [ ] **Step 3: Rewrite the ingress handler**

Replace the body of `crates/polyflare-server/src/ingress.rs` (keep `unix_now` + `account_unavailable`; add the new flow). Full new file:

```rust
//! Ingress: derive continuity ctx → prepare → ownership pre-filter → execute under the watchdog →
//! relay. Client-facing errors carry generic bodies (never a token, URL, or internal Display).

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::{Json, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use polyflare_codex::oauth::{classify_failure, should_refresh, OAuthError};
use polyflare_core::{Account, AccountId, PreparedRequest, RecoveryPlan, RequestCtx, ResponseStream, SelectionCtx};
use polyflare_store::PlainTokens;

use crate::app::AppState;
use crate::session_key::derive_request_ctx;
use crate::snapshot::assemble_snapshots;
use crate::watchdog::{apply_ownership, execute_recovery, execute_with_watchdog, signal_client_stream, RouteDecision};

fn unix_now() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
}

fn account_unavailable() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, "account unavailable").into_response()
}

fn internal_error() -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
}

fn no_eligible() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, "no eligible account").into_response()
}

fn stream_response(stream: ResponseStream) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .body(Body::from_stream(stream))
        .expect("valid response")
}

/// Load + decrypt + refresh-if-stale the selected account, returning the core `Account` to execute
/// with, or a ready client-facing error `Response`.
async fn resolve_core_account(state: &AppState, picked: &AccountId, now: i64) -> Result<Account, Response> {
    let repo = state.store.accounts();
    let account = match repo.get(picked.as_str()).await {
        Ok(Some(a)) => a,
        Ok(None) | Err(_) => return Err(internal_error()),
    };
    let mut tokens = match repo.decrypt_tokens(picked.as_str(), &state.cipher).await {
        Ok(Some(t)) => t,
        Ok(None) | Err(_) => return Err(internal_error()),
    };
    if should_refresh(account.last_refresh, now) {
        match state.oauth.refresh(&tokens.refresh_token).await {
            Ok(refreshed) => {
                let new = PlainTokens {
                    access_token: refreshed.tokens.access_token,
                    refresh_token: refreshed.tokens.refresh_token,
                    id_token: refreshed.tokens.id_token,
                };
                let _ = repo.update_tokens(picked.as_str(), &new, &state.cipher, now).await;
                tokens = new;
            }
            Err(OAuthError::Endpoint { code: Some(code), .. }) => {
                if let Some(status) = classify_failure(&code).status() {
                    let _ = repo.update_status(picked.as_str(), status).await;
                }
                return Err(account_unavailable());
            }
            Err(OAuthError::Endpoint { code: None, .. }) | Err(OAuthError::MalformedJwt(_)) => {
                let _ = repo.update_status(picked.as_str(), "reauth_required").await;
                return Err(account_unavailable());
            }
            Err(OAuthError::Transport(_)) => {}
        }
    }
    Ok(Account { id: account.id, base_url: state.upstream_base_url.clone(), bearer_token: tokens.access_token })
}

pub async fn responses_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let model = body.get("model").and_then(|m| m.as_str()).unwrap_or_default().to_string();
    let now = unix_now();

    // C3: derive continuity ctx from headers + body.
    let ctx: RequestCtx = derive_request_ctx(&headers, &body);
    let req = PreparedRequest { body, model };

    // C4: prepare (resolve owner + arm + recovery plan).
    let prepared = match state.continuity.prepare(req, &ctx).await {
        Ok(p) => p,
        Err(_) => return internal_error(),
    };

    let snapshots = match assemble_snapshots(&state.store).await {
        Ok(s) => s,
        Err(_) => return internal_error(),
    };
    let sel_ctx = SelectionCtx {
        now,
        require_security_work_authorized: false,
        rng_seed: None,
        session_id: ctx.session_id.clone(),
    };
    let session_key = prepared.directive.session_key.clone();

    // C5: ownership pre-filter.
    match apply_ownership(&prepared.directive, &snapshots, state.selector.as_ref(), &sel_ctx) {
        RouteDecision::Route(id) => {
            let account = match resolve_core_account(&state, &id, now).await {
                Ok(a) => a,
                Err(r) => return r,
            };
            match execute_with_watchdog(state.executor.as_ref(), state.continuity.clone(), prepared, &account, id, ctx).await {
                Ok(stream) => stream_response(stream),
                Err(_) => (StatusCode::BAD_GATEWAY, "upstream error").into_response(),
            }
        }
        RouteDecision::Recover => {
            // Owner pinned but ineligible: recover on a freshly-selected account (full pool), or
            // signal the client if the input is a bare tail.
            match prepared.directive.recovery {
                RecoveryPlan::ResendFull { anchorless_req } => {
                    let fresh = match state.selector.pick(&snapshots, &sel_ctx) {
                        Some(id) => id,
                        None => return no_eligible(),
                    };
                    let account = match resolve_core_account(&state, &fresh, now).await {
                        Ok(a) => a,
                        Err(r) => return r,
                    };
                    match execute_recovery(state.executor.as_ref(), state.continuity.clone(), anchorless_req, &account, fresh, ctx, session_key).await {
                        Ok(stream) => stream_response(stream),
                        Err(_) => (StatusCode::BAD_GATEWAY, "upstream error").into_response(),
                    }
                }
                RecoveryPlan::SignalClient => {
                    let owner = prepared.directive.pin_account.clone().unwrap_or_else(|| AccountId::from("unknown"));
                    let stream = signal_client_stream(state.continuity.clone(), ctx, owner, session_key).await;
                    stream_response(stream)
                }
                RecoveryPlan::None => internal_error(),
            }
        }
        RouteDecision::NoEligibleAccount => no_eligible(),
    }
}
```

- [ ] **Step 4: Wire `CodexContinuity` into the binary**

In `crates/polyflare-server/src/main.rs`, update `serve()`: build the continuity from the store's pool BEFORE moving the store into `AppState`.

```rust
use std::sync::Arc;
use polyflare_core::{CapacityWeighted, Continuity, Selector};
use polyflare_server::continuity::CodexContinuity;
```
Inside `serve()`, after `let store = Store::open(...)`:

```rust
    let continuity: Arc<dyn Continuity> =
        Arc::new(CodexContinuity::new(store.continuity(), config.continuity_watchdog));
```
and add `continuity,` to the `AppState { .. }` literal.

- [ ] **Step 5: Update the 5 existing test `AppState` constructions**

In each of `e2e_passthrough.rs`, `ingress_relays.rs`, `pool_selection.rs`, `refresh_path.rs`, `large_body.rs`: add the continuity import and field. Pattern (apply to each `AppState { .. }` literal — build continuity from the store handle BEFORE the store is moved):

```rust
use polyflare_core::{Continuity};
use polyflare_server::continuity::CodexContinuity;
use std::time::Duration;
```
```rust
    let continuity: std::sync::Arc<dyn Continuity> =
        std::sync::Arc::new(CodexContinuity::new(store.continuity(), Duration::from_secs(30)));
    let state = Arc::new(AppState {
        executor: /* unchanged */,
        selector: /* unchanged */,
        continuity,
        store,
        /* unchanged fields */
    });
```
*(These tests use no anchors → `Disarmed` → the watchdog never fires; behavior is unchanged except a `fresh` session row is written.)*

- [ ] **Step 6: Make the wedge test GREEN**

Rewrite `crates/polyflare-server/tests/wedge_regression.rs` to its final form: add the `continuity` field, drop `#[ignore]`, and strengthen the assertions.

```rust
//! Wedge regression (GREEN from C7): an anchor-bearing full-resend routed to a silent-on-anchor
//! upstream is detected within N, recovered by stripping the anchor and re-sending the FULL input,
//! and completes — no hang. Also asserts R1 (full-resend never trimmed).

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{CapacityWeighted, Continuity};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};
use polyflare_testkit::MockUpstream;

fn now() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
}

fn store_account(id: &str, token: &str) -> Account {
    Account {
        id: id.to_string(),
        chatgpt_account_id: None,
        chatgpt_user_id: None,
        email: "u@example.test".to_string(),
        alias: None,
        workspace_id: None,
        workspace_label: None,
        seat_type: None,
        plan_type: "pro".to_string(),
        routing_policy: "normal".to_string(),
        last_refresh: now(),
        created_at: now(),
        status: "active".to_string(),
        deactivation_reason: None,
        reset_at: None,
        blocked_at: None,
        security_work_authorized: false,
    }
    // token bound below via PlainTokens
    ; let _ = token; unreachable!()
}

async fn spawn_polyflare(upstream: String) -> String {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[9u8; 32]).unwrap();
    let mut acct = store_account_ok("e2e");
    acct.plan_type = "pro".to_string();
    store
        .accounts()
        .insert(
            &acct,
            &PlainTokens { access_token: "tokE".to_string(), refresh_token: "r".to_string(), id_token: "i".to_string() },
            &cipher,
        )
        .await
        .unwrap();
    let continuity: Arc<dyn Continuity> =
        Arc::new(CodexContinuity::new(store.continuity(), Duration::from_millis(150)));
    std::mem::forget(dir);

    let state = Arc::new(AppState {
        executor: Arc::new(CodexExecutor::new().unwrap()),
        selector: Arc::new(CapacityWeighted),
        continuity,
        store,
        cipher,
        oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(),
        upstream_base_url: upstream,
    });
    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

fn store_account_ok(id: &str) -> Account {
    Account {
        id: id.to_string(),
        chatgpt_account_id: None,
        chatgpt_user_id: None,
        email: "u@example.test".to_string(),
        alias: None,
        workspace_id: None,
        workspace_label: None,
        seat_type: None,
        plan_type: "pro".to_string(),
        routing_policy: "normal".to_string(),
        last_refresh: now(),
        created_at: now(),
        status: "active".to_string(),
        deactivation_reason: None,
        reset_at: None,
        blocked_at: None,
        security_work_authorized: false,
    }
}

#[tokio::test]
async fn anchor_bearing_request_to_silent_upstream_does_not_wedge() {
    let mock = MockUpstream::silent_on_anchor(vec![
        r#"{"type":"response.output_text.delta","delta":"ok"}"#.to_string(),
    ]);
    let handle = mock.clone();
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(upstream).await;

    let client = reqwest::Client::new();
    let input = serde_json::json!([
        {"role": "user", "content": "turn one"},
        {"role": "assistant", "content": "reply one"},
        {"role": "user", "content": "turn two"}
    ]);
    let request = client
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "previous_response_id": "resp_dead", "input": input}))
        .send();

    let body = tokio::time::timeout(Duration::from_secs(5), async {
        let resp = request.await.unwrap();
        assert_eq!(resp.status(), 200);
        let mut body = String::new();
        let mut s = resp.bytes_stream();
        while let Some(chunk) = s.next().await {
            body.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
        }
        body
    })
    .await
    .expect("must complete within 5s (no wedge)");

    assert!(body.contains("response.completed"), "client saw a completed stream");
    assert_eq!(handle.request_count(), 2, "silent attempt + recovery");
    let bodies = handle.bodies();
    assert!(bodies[0].get("previous_response_id").is_some(), "1st carried the dead anchor");
    assert!(bodies[1].get("previous_response_id").is_none(), "recovery stripped the anchor");
    assert_eq!(bodies[1]["input"], input, "R1: full-resend not trimmed");
}
```

*(Remove the unused `store_account`/`token` scaffolding above — keep only `store_account_ok`, `spawn_polyflare`, and the test. The dead `store_account` with `unreachable!()` is shown to flag: DELETE it; the executor should keep just `store_account_ok`.)*

- [ ] **Step 7: Run the full server suite**

Run: `cargo test -p polyflare-server`
Expected: PASS — including `anchor_bearing_request_to_silent_upstream_does_not_wedge` (now GREEN) and all existing pass-through tests.

- [ ] **Step 8: fmt + clippy + full workspace test + commit**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
git add crates/polyflare-server/
git commit -m "feat(m3): wire continuity+watchdog into ingress; wedge regression GREEN (C7)"
```

---

## Task C8: Round out the acceptance tests (ownership, Strategy B, cancel-safety)

**Files:**
- Create: `crates/polyflare-server/tests/ownership.rs`
- Create: `crates/polyflare-server/tests/signal_client.rs`
- Modify: `crates/polyflare-server/tests/wedge_regression.rs` (add the cancel-safety test)

**Interfaces:**
- Consumes: `MockUpstream::{with_ids, silent_on_anchor}`, `AppState { continuity, .. }`, a custom test `Selector` (`PreferB`).

- [ ] **Step 1: Ownership test — 2nd turn returns to the same account**

Create `crates/polyflare-server/tests/ownership.rs`:

```rust
//! Continuity ownership: turn 1 (no anchor) lands on account A and records resp_1 -> A; turn 2
//! carries `previous_response_id: resp_1` and MUST route back to A — the pin overrides a selector
//! that otherwise prefers B.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{AccountId, AccountSnapshot, Continuity, Selector, SelectionCtx};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};
use polyflare_testkit::MockUpstream;

fn now() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
}

/// A selector that prefers "B" when present (so an unpinned turn goes to B), else the first.
struct PreferB;
impl Selector for PreferB {
    fn pick(&self, candidates: &[AccountSnapshot], _ctx: &SelectionCtx) -> Option<AccountId> {
        if let Some(b) = candidates.iter().find(|s| s.id.as_str() == "B") {
            return Some(b.id.clone());
        }
        candidates.first().map(|s| s.id.clone())
    }
}

fn account(id: &str) -> Account {
    Account {
        id: id.to_string(),
        chatgpt_account_id: None,
        chatgpt_user_id: None,
        email: "u@example.test".to_string(),
        alias: None,
        workspace_id: None,
        workspace_label: None,
        seat_type: None,
        plan_type: "pro".to_string(),
        routing_policy: "normal".to_string(),
        last_refresh: now(),
        created_at: now(),
        status: "active".to_string(),
        deactivation_reason: None,
        reset_at: None,
        blocked_at: None,
        security_work_authorized: false,
    }
}

async fn drain(resp: reqwest::Response) -> String {
    let mut body = String::new();
    let mut s = resp.bytes_stream();
    while let Some(chunk) = s.next().await {
        body.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
    }
    body
}

#[tokio::test]
async fn second_turn_pins_back_to_owning_account() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[7u8; 32]).unwrap();
    // Turn 1: only account A exists (token "tokA"), so it lands on A.
    store
        .accounts()
        .insert(&account("A"), &PlainTokens { access_token: "tokA".into(), refresh_token: "r".into(), id_token: "i".into() }, &cipher)
        .await
        .unwrap();
    let continuity: Arc<dyn Continuity> =
        Arc::new(CodexContinuity::new(store.continuity(), Duration::from_secs(30)));

    let mock = MockUpstream::with_ids(vec![r#"{"type":"response.output_text.delta","delta":"x"}"#.to_string()]);
    let handle = mock.clone();
    let upstream = mock.spawn().await;

    let state = Arc::new(AppState {
        executor: Arc::new(CodexExecutor::new().unwrap()),
        selector: Arc::new(PreferB),
        continuity,
        store,
        cipher: TokenCipher::from_key_bytes(&[7u8; 32]).unwrap(),
        oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(),
        upstream_base_url: upstream,
    });
    let app = build_app(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let pf = format!("http://{addr}");

    let client = reqwest::Client::new();
    // Turn 1: no anchor. Lands on A; mock emits resp_1; observe records resp_1 -> A.
    let r1 = client.post(format!("{pf}/responses")).json(&serde_json::json!({"model":"m","input":"hi"})).send().await.unwrap();
    let b1 = drain(r1).await;
    assert!(b1.contains("resp_1"));

    // Insert account B (token "tokB"); PreferB would now pick B when unpinned.
    state
        .store
        .accounts()
        .insert(&account("B"), &PlainTokens { access_token: "tokB".into(), refresh_token: "r".into(), id_token: "i".into() }, &state.cipher)
        .await
        .unwrap();

    // Turn 2: carries the anchor resp_1 -> ownership pins to A despite PreferB.
    let r2 = client
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model":"m","previous_response_id":"resp_1","input":"more"}))
        .send()
        .await
        .unwrap();
    let _ = drain(r2).await;
    assert_eq!(handle.last_authorization().as_deref(), Some("Bearer tokA"), "turn 2 pinned back to A");
    std::mem::forget(dir);
}
```

- [ ] **Step 2: Run the ownership test**

Run: `cargo test -p polyflare-server --test ownership`
Expected: PASS.

- [ ] **Step 3: Strategy-B test — bare-tail dead anchor signals the client**

Create `crates/polyflare-server/tests/signal_client.rs`:

```rust
//! Strategy B: a bare-tail request carrying a dead anchor to a silent-on-anchor upstream must
//! surface `previous_response_not_found` within N (bounded) so the client self-heals — not a hang.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{CapacityWeighted, Continuity};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};
use polyflare_testkit::MockUpstream;

fn now() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
}

fn account(id: &str) -> Account {
    Account {
        id: id.to_string(),
        chatgpt_account_id: None,
        chatgpt_user_id: None,
        email: "u@example.test".to_string(),
        alias: None,
        workspace_id: None,
        workspace_label: None,
        seat_type: None,
        plan_type: "pro".to_string(),
        routing_policy: "normal".to_string(),
        last_refresh: now(),
        created_at: now(),
        status: "active".to_string(),
        deactivation_reason: None,
        reset_at: None,
        blocked_at: None,
        security_work_authorized: false,
    }
}

#[tokio::test]
async fn bare_tail_dead_anchor_signals_previous_response_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[4u8; 32]).unwrap();
    store
        .accounts()
        .insert(&account("A"), &PlainTokens { access_token: "tokA".into(), refresh_token: "r".into(), id_token: "i".into() }, &cipher)
        .await
        .unwrap();
    let continuity: Arc<dyn Continuity> =
        Arc::new(CodexContinuity::new(store.continuity(), Duration::from_millis(150)));
    std::mem::forget(dir);

    let mock = MockUpstream::silent_on_anchor(vec![]);
    let upstream = mock.spawn().await;
    let state = Arc::new(AppState {
        executor: Arc::new(CodexExecutor::new().unwrap()),
        selector: Arc::new(CapacityWeighted),
        continuity,
        store,
        cipher,
        oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(),
        upstream_base_url: upstream,
    });
    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let pf = format!("http://{addr}");

    let client = reqwest::Client::new();
    let body = tokio::time::timeout(Duration::from_secs(3), async {
        // Bare tail (short string) + dead anchor => is_full_resend=false => SignalClient.
        let resp = client
            .post(format!("{pf}/responses"))
            .json(&serde_json::json!({"model":"m","previous_response_id":"resp_dead","input":"tail"}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let mut body = String::new();
        let mut s = resp.bytes_stream();
        while let Some(chunk) = s.next().await {
            body.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
        }
        body
    })
    .await
    .expect("signal must arrive within 3s (no hang)");

    // Assert on the CODE substring only (the exact envelope is a verify-at-impl item).
    assert!(body.contains("previous_response_not_found"), "client received the self-heal signal: {body}");
}
```

- [ ] **Step 4: Run the signal test**

Run: `cargo test -p polyflare-server --test signal_client`
Expected: PASS.

- [ ] **Step 5: Cancel-safety smoke test (client disconnect mid-race)**

Append to `crates/polyflare-server/tests/wedge_regression.rs`:

```rust
#[tokio::test]
async fn client_disconnect_mid_race_is_clean() {
    let mock = MockUpstream::silent_on_anchor(vec![
        r#"{"type":"response.output_text.delta","delta":"ok"}"#.to_string(),
    ]);
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(upstream).await;

    // Client uses a 60ms read budget — shorter than N (150ms) — so it disconnects mid-race.
    let client = reqwest::Client::builder().timeout(std::time::Duration::from_millis(60)).build().unwrap();
    let input = serde_json::json!([{"role":"user","content":"a"},{"role":"user","content":"b"}]);
    let res = client
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model":"m","previous_response_id":"resp_dead","input":input}))
        .send()
        .await;
    // The client errors/aborts; the assertion that matters is the server survives (no panic/leak):
    // a subsequent normal request to the same server must still succeed.
    let _ = res;

    let ok_client = reqwest::Client::new();
    let resp = ok_client
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model":"m","input":"fresh"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "server healthy after a mid-race client disconnect");
}
```

- [ ] **Step 6: No-secret-logging guard (redaction sanity across new types)**

Add a focused unit test to `crates/polyflare-server/src/watchdog.rs` `mod tests`:

```rust
    #[test]
    fn watchdog_error_display_is_generic() {
        assert_eq!(WatchdogError::Upstream.to_string(), "upstream error");
        assert_eq!(WatchdogError::Continuity.to_string(), "continuity recovery unavailable");
    }
```

- [ ] **Step 7: Run the whole workspace green**

Run: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace`
Expected: PASS (all M3-core tests + all prior M1/M2 tests).

- [ ] **Step 8: Commit**

```bash
git add crates/polyflare-server/
git commit -m "test(m3): ownership pin + Strategy-B signal + cancel-safety + error-display guards (C8)"
```

---

## Self-Review

**1. Spec coverage (SPEC-M3 §Q8 C0–C8 + the resolutions):**

| Item | Task | Notes |
|---|---|---|
| C0 silent-on-anchor mock + RED wedge e2e | C0 | `MockUpstream::silent_on_anchor`; test `#[ignore]`d until C7, confirmed RED via `--ignored`. |
| C1 reshape `Continuity` (async `prepare`→`Prepared`/`observe`) + `ContinuityDirective`/`RecoveryPlan`/`TurnOutcome`/`ContinuityError`/`SessionKey`/`ReasoningItems`; enrich `RequestCtx`; `NoopContinuity` | C1 | Exact §3.3 shapes; redacting `Debug` + tests on `ReasoningItems` + `RecoveryPlan`. |
| C2 `0002_continuity.sql` (sessions + anchors) + `ContinuityRepo` | C2 | Runtime-checked sqlx; forward-only; **no conversation content**. |
| C3 session-key derivation (turn-state hard / session hard / soft) + `is_full_resend` + anchor extraction | C3 | Header names + heuristic flagged VERIFY-at-impl (risk 4). |
| C4 `CodexContinuity::prepare` — owner resolve (anchor map → session), arm watchdog **only on anchors**, build recovery plan, write `fresh`/`reattaching` | C4 | Resolution baked: watchdog arms only on anchor-bearing requests. |
| C5 `apply_ownership` (narrow → `pick`; `None`→Recover) — **no Selector-trait change** | C5 | Placement per §Q1/S3. |
| C5 `execute_with_watchdog` — first-byte race, cancel-on-silence, ResendFull / SignalClient, configurable N | C5 | Detection=first-bytes, recovery=signal-client/ResendFull; peek-before-relay by construction. |
| C6/C7 non-buffering response sniff → `response.id`; `observe` records anchor+owner, advances state | C5 (sniffer, `ObservingStream`) + C6 (`observe`) | Bounded ≤ 64 KiB sniffer; observe awaited inline on stream end. |
| C7 wire into ingress; wedge test GREEN | C7 | `prepare` → `apply_ownership` → watchdog; N via `ServeConfig`. |
| C8 wedge + ownership + R1 + Strategy-B tests green; no-token/no-content logging | C8 | Ownership pin, Strategy-B signal, cancel-safety, R1 (`bodies[1]["input"] == input`), generic `Display`. |
| Resolution: PolyFlare persists NO conversation content | C2 schema + C6 observe | Only session state + response_id→owner map. |
| Resolution: SignalClient wire shape = verify-at-impl | C5 `SIGNAL_SSE` const comment + C8 asserts code substring only | Not a guess baked into a brittle test. |
| Resolution: M3 adds NO `store:false`-forcing validator | (n/a — absent by construction) | Orthogonal; M4 concern. |
| Followups F1–F5 excluded | — | Reasoning cache (`reasoning_cache` table, `ReasoningItems` population), singleflight, error/cooldown tracking, O(N) collapse, id_token decouple are a separate later plan. |

**2. Placeholder scan:** No `TBD`/`implement later`/"add error handling". Every code step is complete, compilable Rust. Two explicit, intentional VERIFY-at-implementation markers (SignalClient wire shape; session-key header names) are called out with a capture step, not left as silent guesses — the tests are written to survive whatever the capture yields (assert on the `previous_response_not_found` code substring; assert on key *strength*, not exact value). One deliberate DELETE note: C7 Step 6 shows a dead `store_account`/`unreachable!()` scaffold explicitly flagged for removal so the executor keeps only `store_account_ok`.

**3. Type consistency:** `Continuity::{prepare,observe}` signatures match across `traits.rs` (C1), `NoopContinuity` (C1), and `CodexContinuity` (C4/C6). `Prepared{req,directive}`, `ContinuityDirective{pin_account,watchdog,recovery,session_key}`, `WatchdogArm::{Disarmed,Armed{timeout}}`, `RecoveryPlan::{ResendFull{anchorless_req},SignalClient,None}`, `TurnOutcome::{Completed{..},Recovered{..},Failed{..}}` are used identically in C1 (def), C4/C6 (producer/consumer), and C5 (watchdog). `ContinuityRepo::{get_session,get_anchor_owner,ensure_session,set_state,record_completion,record_recovery}` names match between C2 (def) and C4/C6 (callers). `apply_ownership`/`execute_with_watchdog`/`execute_recovery`/`signal_client_stream`/`RouteDecision`/`WatchdogError` names match between C5 (def) and C7 (ingress). `Store::continuity()` (C2) is used by C4 tests, C7 main, and the five updated test files. `AppState.continuity: Arc<dyn Continuity>` (C7) is populated identically in `main.rs` and all test constructions. `sha256_hex` (C3) is reused by C5's `input_fingerprint_and_count`. `MockUpstream::{with_ids,silent_on_anchor,bodies,request_count,last_authorization}` (C0) are used with matching signatures in C5/C7/C8 tests.
