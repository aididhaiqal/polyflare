//! D18 Task 5 (final) — the never-log CONTENT-SAFETY proof over the WHOLE request path, plus the
//! full valid/invalid-key e2e, both through the REAL `crate::app::build_app` stack (real `Store`,
//! real router, real HTTP over a loopback `TcpListener` — never the `responses_handler_impl`
//! test seams).
//!
//! T3's `require_client_key_middleware.rs::sentinel_key_never_leaks_on_failed_auth` already proved
//! the MIDDLEWARE never logs the raw key, on the 401/failure path, against a bare `/probe` route
//! (not the real proxy stack). What that test structurally CANNOT prove is what happens to a key
//! that PASSES the middleware and reaches the real handler — specifically, the
//! `crate::observability::RequestLog` chokepoint that:
//!   1. `tracing::info!`s a "request completed" event (`RequestLog::emit`),
//!   2. publishes a `LogEvent` onto `crate::log_bus::LogBus` (backfill + the `/api/logs/stream`
//!      SSE feed `crate::sse::logs_stream_handler` drains),
//!   3. persists a `polyflare_store::RequestLogRecord` row to the `request_log` table
//!      (fire-and-forget via `tokio::spawn` — see `ingress.rs::spawn_persist_request_log`).
//!
//! `RequestLog`/`RequestLogRecord`/`LogEvent` are all structurally content-free (no header/body
//! field exists on any of them to leak into) — this suite's job is to prove that INVARIANT holds
//! all the way through a REAL successful proxied request carrying a real, valid, enforced client
//! key, not just to trust the type definitions.

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{CapacityWeighted, Continuity, Executor};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_server::keys::sha256_hex;
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};
use polyflare_testkit::MockUpstream;

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn codex_account(id: &str) -> Account {
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
        provider: "codex".to_string(),
        pool: None,
    }
}

fn tokens() -> PlainTokens {
    PlainTokens {
        access_token: "tok".to_string(),
        refresh_token: "r".to_string(),
        id_token: "i".to_string(),
    }
}

/// A recognizable, content-distinguishable clean upstream stream — so a passing test can assert
/// the CORRECT stream was served (not just any 200), and a failing content-safety test would show
/// the sentinel sitting right next to this in whatever body/log it leaked into.
fn ok_events() -> Vec<String> {
    vec![
        r#"{"type":"response.output_text.delta","delta":"hi"}"#.to_string(),
        r#"{"type":"response.completed"}"#.to_string(),
    ]
}

/// Builds a real `AppState` (real `Store`, real `build_app`) with client-key enforcement ON, the
/// admin token set (so `/api/logs/stream` — itself `require_admin`-gated — is reachable), and
/// `live_logs: true` (so the SSE live-log path is exercisable, not just the in-memory backfill).
async fn spawn(enforce_client_keys: bool) -> (String, Arc<AppState>) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    std::mem::forget(dir);
    let cipher = TokenCipher::from_key_bytes(&[9u8; 32]).unwrap();
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_secs(30),
    ));

    let mock = MockUpstream::new(ok_events());
    let upstream = mock.spawn().await;

    store
        .accounts()
        .insert(&codex_account("codex-1"), &tokens(), &cipher)
        .await
        .unwrap();

    let state = Arc::new(AppState {
        codex_executor: Arc::new(CodexExecutor::new().unwrap()) as Arc<dyn Executor>,
        control_client: polyflare_codex::build_client().expect("build control_client"),
        anthropic_executor: Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap())
            as Arc<dyn Executor>,
        selector: Arc::new(CapacityWeighted),
        pool_selectors: Default::default(),
        continuity,
        store,
        cipher,
        oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(),
        upstream_base_url: upstream,
        anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),
        refresh_locks: Default::default(),
        capture_fingerprint_path: None,
        codex_version: Arc::new(polyflare_codex::CodexVersionCache::new().unwrap()),
        account_cache: Arc::new(polyflare_server::account_cache::AccountCache::new()),
        token_cache: Default::default(),
        runtime: Default::default(),
        admin_token: Some("admin-secret".to_string()),
        live_logs: true,
        log_bus: polyflare_server::log_bus::LogBus::new(1000),
        max_account_attempts: 3,
        failover_metrics: polyflare_server::observability::FailoverMetrics::new(),
        health_tier_metrics: polyflare_server::observability::HealthTierMetrics::new(),
        starvation_wait_budget: Duration::from_secs(60),
        starvation_heartbeat: Duration::from_secs(10),
        wake_jitter_ms: 0,        inflight_penalty_pct: 2.5,

        starvation_metrics: polyflare_server::observability::StarvationMetrics::new(),
        stream_idle_timeout: std::time::Duration::from_secs(300),
        soft_drain_enabled: true,
        enforce_client_keys,
    });

    let app = build_app(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), state)
}

fn responses_body() -> serde_json::Value {
    serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"})
}

/// Directly inserts a valid, enabled `api_keys` row for a CHOSEN raw key (rather than
/// `crate::keys::create_key`'s randomly generated one) — this suite needs to control the raw key
/// value so it can embed a grep-able sentinel and assert that exact sentinel never appears
/// anywhere it shouldn't. Mirrors exactly what `keys create`/`create_key` do under the hood
/// (`sha256_hex` the raw value, store only the hash + a display prefix), just with the raw value
/// supplied by the test instead of `crate::keys::generate_key`'s CSPRNG.
async fn insert_key_for_raw(store: &Store, raw: &str, label: &str) {
    let hash = sha256_hex(raw);
    let prefix: String = raw.chars().take(15).collect();
    store
        .api_keys()
        .create(&format!("key_{label}"), &hash, &prefix, Some(label), now())
        .await
        .unwrap();
}

async fn drain(resp: reqwest::Response) -> String {
    let mut body = String::new();
    let mut s = resp.bytes_stream();
    while let Some(chunk) = s.next().await {
        match chunk {
            Ok(bytes) => body.push_str(&String::from_utf8_lossy(&bytes)),
            Err(_) => break,
        }
    }
    body
}

/// A `tracing` sink that captures everything written to it, for content-safety assertions —
/// identical pattern to `require_client_key_middleware.rs::sentinel_key_never_leaks_on_failed_auth`.
#[derive(Clone, Default)]
struct BufWriter(Arc<Mutex<Vec<u8>>>);
impl std::io::Write for BufWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BufWriter {
    type Writer = BufWriter;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

// ---------------------------------------------------------------------------------------------
// THE HEADLINE: the SENTINEL client key never leaks into the request_log row, the SSE live-log
// feed, or any tracing output — driven through a REAL, SUCCESSFUL, key-enforced request (the key
// PASSES the middleware and reaches the handler's post-request logging chokepoint).
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn never_logs_the_client_key_end_to_end() {
    const SENTINEL: &str = "SENTINELVALUE12345";
    let raw = format!("sk-pf-{SENTINEL}");

    let (base, state) = spawn(true).await;
    insert_key_for_raw(&state.store, &raw, "sentinel").await;

    // Capture EVERY tracing event for the lifetime of this request — middleware AND handler AND
    // the fire-and-forget request_log persist task all run on this same current-thread runtime's
    // single OS thread (see the comment on `require_client_key_middleware.rs`'s analogous test),
    // so a thread-local `set_default` here sees all of it.
    let buf = BufWriter::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(buf.clone())
        .with_max_level(tracing::Level::TRACE)
        .finish();
    let guard = tracing::subscriber::set_default(subscriber);

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/responses"))
        .header("authorization", format!("Bearer {raw}"))
        .json(&responses_body())
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "the sentinel key is a VALID enabled key — it must reach the real handler, not 401"
    );
    let body = drain(resp).await;
    assert!(
        body.contains("response.completed"),
        "sanity: the real clean upstream stream was served: {body}"
    );
    assert!(
        !body.contains(SENTINEL),
        "the client-facing response body must never echo the presented key"
    );

    // The request_log persist is fire-and-forget (`tokio::spawn` — see
    // `ingress.rs::spawn_persist_request_log`); poll with a bounded, generous timeout instead of
    // assuming it landed the instant the HTTP response finished draining.
    let mut rows = Vec::new();
    for _ in 0..50 {
        rows = state.store.request_log().list(10, 0).await.unwrap();
        if !rows.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    drop(guard);

    assert_eq!(rows.len(), 1, "exactly one request_log row for this request: {rows:?}");
    let row = &rows[0];
    // `RequestLogRow` is structurally content-free (no header/body field exists on the type at
    // all — see `polyflare_store::request_log_repo`'s module doc) — this Debug-format scan is
    // belt-and-suspenders proof on top of that structural guarantee, covering every field
    // (including any added later) in one assertion rather than enumerating them by hand.
    let row_debug = format!("{row:?}");
    assert!(
        !row_debug.contains(SENTINEL) && !row_debug.contains(&raw),
        "the persisted request_log row must never contain the client key, got: {row_debug}"
    );
    assert_eq!(row.status, 200);
    assert_eq!(row.path, "/responses");

    // The SSE live-log feed: the SAME `LogEvent` `crate::sse::logs_stream_handler` drains from
    // `LogBus` (backfill first, then live) — subscribing to the bus directly here observes
    // exactly what that endpoint would emit, without the extra ceremony of parsing SSE frames off
    // the wire for content that's already proven identical by `sse.rs`'s own `sse_ok` (a direct,
    // lossless `serde_json::to_string` of the same `LogEvent`).
    let (backfill, _rx) = state.log_bus.subscribe();
    let request_events: Vec<_> = backfill.iter().filter(|e| e.kind == "request").collect();
    assert_eq!(request_events.len(), 1, "got: {backfill:?}");
    let ev_debug = format!("{:?}", request_events[0]);
    assert!(
        !ev_debug.contains(SENTINEL) && !ev_debug.contains(&raw),
        "the live-log-bus event must never contain the client key, got: {ev_debug}"
    );
    // Also prove it over the actual wire format the SSE endpoint serializes.
    let ev_json = serde_json::to_string(request_events[0]).unwrap();
    assert!(!ev_json.contains(SENTINEL) && !ev_json.contains(&raw));

    // Also exercise the REAL `/api/logs/stream` endpoint (live_logs: true, admin-gated) over an
    // actual HTTP connection — reads a bounded prefix of the SSE body (backfill is served first,
    // so this request's event is guaranteed to already be in it) rather than draining forever
    // (the endpoint never closes the stream — it holds it open for live events).
    let sse_resp = client
        .get(format!("{base}/api/logs/stream"))
        .header("authorization", "Bearer admin-secret")
        .send()
        .await
        .unwrap();
    assert_eq!(sse_resp.status(), 200, "live_logs:true ⇒ the SSE endpoint is reachable");
    let mut sse_stream = sse_resp.bytes_stream();
    let mut sse_body = String::new();
    // Bounded read: enough bytes to definitely cover the backfilled request event, never the
    // whole (open-ended) stream.
    while sse_body.len() < 4096 {
        match tokio::time::timeout(Duration::from_millis(500), sse_stream.next()).await {
            Ok(Some(Ok(chunk))) => sse_body.push_str(&String::from_utf8_lossy(&chunk)),
            _ => break,
        }
    }
    assert!(
        sse_body.contains("\"kind\":\"request\""),
        "sanity: the backfilled request event is present in the SSE stream: {sse_body:?}"
    );
    assert!(
        !sse_body.contains(SENTINEL) && !sse_body.contains(&raw),
        "the /api/logs/stream SSE wire body must never contain the client key, got: {sse_body:?}"
    );

    // Finally, the tracing capture spanning the entire request (middleware validation, the
    // handler's `RequestLog::emit()`, and the background persist task).
    let captured = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
    assert!(
        !captured.contains(SENTINEL) && !captured.contains(&raw),
        "no tracing sink may ever see the raw key across the whole request path, got: {captured:?}"
    );
    assert!(
        !captured.contains(&sha256_hex(&raw)),
        "the key hash must not appear in tracing output either (audit by key_id/prefix only)"
    );
}

// ---------------------------------------------------------------------------------------------
// FULL E2E: a key-enforced pool ⇒ a VALID key gets a clean proxied response, an INVALID key (a
// well-formed but unknown token, not just an absent header) gets 401, an absent header gets 401 —
// all through the real `build_app` stack.
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn valid_key_gets_a_clean_proxied_response() {
    let (base, state) = spawn(true).await;
    let raw = "sk-pf-valid-e2e-key-0001";
    insert_key_for_raw(&state.store, raw, "valid").await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/responses"))
        .header("authorization", format!("Bearer {raw}"))
        .json(&responses_body())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = drain(resp).await;
    assert!(
        body.contains("response.completed") && body.contains("response.output_text.delta"),
        "the client gets the real clean upstream stream, unmodified: {body}"
    );
}

#[tokio::test]
async fn invalid_well_formed_key_gets_401() {
    let (base, state) = spawn(true).await;
    // Enforcement is on (a key exists) but the PRESENTED key was never created — well-formed
    // (matches the `sk-pf-` shape) but unknown, distinct from Task 4's "absent header" case.
    insert_key_for_raw(&state.store, "sk-pf-some-other-key", "other").await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/responses"))
        .header("authorization", "Bearer sk-pf-this-key-was-never-created-abcdef")
        .json(&responses_body())
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        401,
        "a well-formed but unknown key must be rejected exactly like an absent one"
    );
}

#[tokio::test]
async fn absent_key_gets_401_when_enforced() {
    let (base, state) = spawn(true).await;
    insert_key_for_raw(&state.store, "sk-pf-some-key", "other").await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/responses"))
        .json(&responses_body())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn revoked_key_gets_401() {
    let (base, state) = spawn(true).await;
    let raw = "sk-pf-revoked-e2e-key-0001";
    let hash = sha256_hex(raw);
    let prefix: String = raw.chars().take(15).collect();
    state
        .store
        .api_keys()
        .create("key_revoked", &hash, &prefix, Some("revoked"), now())
        .await
        .unwrap();
    state
        .store
        .api_keys()
        .set_enabled("key_revoked", false)
        .await
        .unwrap();

    let resp = reqwest::Client::new()
        .post(format!("{base}/responses"))
        .header("authorization", format!("Bearer {raw}"))
        .json(&responses_body())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401, "a revoked (disabled) key must be rejected");
}
