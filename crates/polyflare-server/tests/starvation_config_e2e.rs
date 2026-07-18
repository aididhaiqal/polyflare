//! B5 Task 5 (final) — the e2e through the REAL `build_app`/HTTP stack (not the
//! `responses_handler_impl_for_test_with_starvation_timing` seam `starvation_layer2.rs`/
//! `starvation_observability.rs` use): `POLYFLARE_STARVATION_WAIT_BUDGET_SECS`/
//! `POLYFLARE_STARVATION_HEARTBEAT_SECS`, resolved through the REAL `config` functions into
//! `AppState` exactly as `ServeConfig`/`main.rs` do at startup, drive Layer 2's actual behavior over
//! a genuine HTTP round trip. Mirrors `tests/config_driven_failover.rs`'s shape (B4/B5 Task 5's own
//! precedent for "config-driven, not the test seam").
//!
//! Two things proven here:
//! 1. A pool where the only account is briefly cooled down then recovers ⇒ the client gets
//!    keepalives THEN a clean stream, budget/heartbeat are honored as resolved from env (not
//!    hardcoded test constants), and the content-free starvation signal fires.
//! 2. `POLYFLARE_STARVATION_WAIT_BUDGET_SECS=0` (resolved via the real
//!    `config::starvation_wait_budget_secs_from_env`) ⇒ Layer 2 is disabled: a cooldown-only empty
//!    pool gets a FAST 503 (pre-response, no HTTP 200, no keepalive) — the regression/disable-lever
//!    proof.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use polyflare_codex::oauth::OAuthClient;
use polyflare_core::{CapacityWeighted, Continuity};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::config;
use polyflare_server::continuity::CodexContinuity;
use polyflare_store::{PlainTokens, Store, TokenCipher};

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn account(id: &str, status: &str) -> polyflare_store::Account {
    polyflare_store::Account {
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
        last_refresh: i64::MAX / 2,
        created_at: 1,
        status: status.to_string(),
        deactivation_reason: None,
        reset_at: None,
        blocked_at: None,
        security_work_authorized: false,
        provider: "codex".to_string(),
        pool: None,
    }
}

fn tokens(access_token: &str) -> PlainTokens {
    PlainTokens {
        access_token: access_token.to_string(),
        refresh_token: "r".into(),
        id_token: "i".into(),
    }
}

async fn spawn_store() -> (Store, TokenCipher, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[55u8; 32]).unwrap();
    (store, cipher, dir)
}

/// Builds a real `AppState` against a real upstream (`polyflare_codex::CodexExecutor`, pointed at
/// `upstream_url`), with `starvation_wait_budget`/`starvation_heartbeat` resolved through the REAL
/// `config` functions (never hardcoded), mirroring `tests/config_driven_failover.rs::build_state`'s
/// "config is resolved ONCE, before `AppState` construction" contract.
fn build_state(
    store: Store,
    cipher: TokenCipher,
    upstream_url: String,
    starvation_wait_budget: Duration,
    starvation_heartbeat: Duration,
) -> Arc<AppState> {
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_secs(30),
    ));
    Arc::new(AppState {
        enforce_client_keys: false,
        codex_executor: Arc::new(polyflare_codex::CodexExecutor::new().unwrap()),
        control_client: polyflare_codex::build_client().expect("build control_client"),
        anthropic_executor: Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap()),
        selector: Arc::new(CapacityWeighted),
        pool_selectors: Default::default(),
        continuity,
        store,
        cipher,
        oauth: OAuthClient::new("http://127.0.0.1:9".to_string()).unwrap(),
        upstream_base_url: upstream_url,
        anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),
        refresh_locks: Default::default(),
        capture_fingerprint_path: None,
        codex_version: Arc::new(polyflare_codex::CodexVersionCache::new().unwrap()),
        account_cache: Arc::new(polyflare_server::account_cache::AccountCache::new()),
        token_cache: Default::default(),
        admin_token: None,
        live_logs: false,
        log_bus: polyflare_server::log_bus::LogBus::new(1000),
        max_account_attempts: 3,
        failover_metrics: polyflare_server::observability::FailoverMetrics::new(),
        health_tier_metrics: polyflare_server::observability::HealthTierMetrics::new(),
        starvation_wait_budget,
        starvation_heartbeat,
        wake_jitter_ms: 0,
        inflight_penalty_pct: 2.5,
        lease_metrics: polyflare_server::observability::LeaseMetrics::new(),
        upstream_request_metrics: polyflare_server::observability::UpstreamRequestMetrics::new(),
        rate_limit_metrics: polyflare_server::observability::RateLimitMetrics::new(),
        model_catalog: polyflare_server::model_catalog::floor_only_model_catalog(),

        starvation_metrics: polyflare_server::observability::StarvationMetrics::new(),
        stream_idle_timeout: std::time::Duration::from_secs(300),
        soft_drain_enabled: true,
        request_log_retention_days: 0,
        usage_history_retention_days: 0,
        runtime: Default::default(),
    })
}

async fn spawn_app(state: Arc<AppState>) -> String {
    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

/// B5-anthropic Task 3: like `build_state`, but wires the `anthropic_upstream_base_url` to a real
/// mock instead of the `http://127.0.0.1:9` placeholder — for the native-Anthropic Layer-2 e2e
/// below, which never touches the Codex upstream at all.
fn build_state_native_anthropic(
    store: Store,
    cipher: TokenCipher,
    anthropic_upstream_url: String,
    starvation_wait_budget: Duration,
    starvation_heartbeat: Duration,
) -> Arc<AppState> {
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_secs(30),
    ));
    Arc::new(AppState {
        enforce_client_keys: false,
        codex_executor: Arc::new(polyflare_codex::CodexExecutor::new().unwrap()),
        control_client: polyflare_codex::build_client().expect("build control_client"),
        anthropic_executor: Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap()),
        selector: Arc::new(CapacityWeighted),
        pool_selectors: Default::default(),
        continuity,
        store,
        cipher,
        oauth: OAuthClient::new("http://127.0.0.1:9".to_string()).unwrap(),
        upstream_base_url: "http://127.0.0.1:9".to_string(),
        anthropic_upstream_base_url: anthropic_upstream_url,
        refresh_locks: Default::default(),
        capture_fingerprint_path: None,
        codex_version: Arc::new(polyflare_codex::CodexVersionCache::new().unwrap()),
        account_cache: Arc::new(polyflare_server::account_cache::AccountCache::new()),
        token_cache: Default::default(),
        admin_token: None,
        live_logs: false,
        log_bus: polyflare_server::log_bus::LogBus::new(1000),
        max_account_attempts: 3,
        failover_metrics: polyflare_server::observability::FailoverMetrics::new(),
        health_tier_metrics: polyflare_server::observability::HealthTierMetrics::new(),
        starvation_wait_budget,
        starvation_heartbeat,
        wake_jitter_ms: 0,
        inflight_penalty_pct: 2.5,
        lease_metrics: polyflare_server::observability::LeaseMetrics::new(),
        upstream_request_metrics: polyflare_server::observability::UpstreamRequestMetrics::new(),
        rate_limit_metrics: polyflare_server::observability::RateLimitMetrics::new(),
        model_catalog: polyflare_server::model_catalog::floor_only_model_catalog(),

        starvation_metrics: polyflare_server::observability::StarvationMetrics::new(),
        stream_idle_timeout: std::time::Duration::from_secs(300),
        soft_drain_enabled: true,
        request_log_retention_days: 0,
        usage_history_retention_days: 0,
        runtime: Default::default(),
    })
}

/// A `provider="anthropic"` account row (mirrors `account()` above, which is hardcoded to
/// `"codex"`), carrying a seeded sentinel email so the content-safety assertions below have
/// something concrete to check never leaks.
fn anthropic_account(id: &str, status: &str, sentinel_email: &str) -> polyflare_store::Account {
    polyflare_store::Account {
        id: id.to_string(),
        chatgpt_account_id: None,
        chatgpt_user_id: None,
        email: sentinel_email.to_string(),
        alias: None,
        workspace_id: None,
        workspace_label: None,
        seat_type: None,
        plan_type: "pro".to_string(),
        routing_policy: "normal".to_string(),
        last_refresh: i64::MAX / 2,
        created_at: 1,
        status: status.to_string(),
        deactivation_reason: None,
        reset_at: None,
        blocked_at: None,
        security_work_authorized: false,
        provider: "anthropic".to_string(),
        pool: None,
    }
}

/// A minimal upstream that always returns a clean `response.completed` SSE stream — real-enough for
/// the recovered-account leg of the e2e (the request never actually reaches it in the budget=0
/// disable-lever test, since the account never recovers within the process's lifetime there).
async fn spawn_stub_upstream() -> String {
    use axum::routing::post;
    use axum::Router;
    async fn respond() -> axum::response::Response {
        let body = "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\"}}\n\n\
                     data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\"}}\n\n";
        axum::response::Response::builder()
            .status(200)
            .header("content-type", "text/event-stream")
            .body(axum::body::Body::from(body))
            .unwrap()
    }
    let app = Router::new().route("/responses", post(respond));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

/// A minimal upstream serving genuine Anthropic-Messages SSE (`event: message_start` /
/// `event: content_block_delta` / `event: message_stop`) — the recovered-account leg of the
/// native-Anthropic Layer-2 e2e below. Embeds `sentinel_text` in the `content_block_delta` so the
/// success-path test can assert the REAL recovered stream (not a synthetic keepalive/error frame)
/// was spliced through verbatim. Also returns a shared hit counter so the budget-exceeded test can
/// assert upstream was NEVER actually reached (the wait gave up before any re-select/execute ran).
async fn spawn_anthropic_stub_upstream(
    sentinel_text: &str,
) -> (String, Arc<std::sync::atomic::AtomicUsize>) {
    use axum::extract::State;
    use axum::routing::post;
    use axum::Router;

    #[derive(Clone)]
    struct MockState {
        text: String,
        hits: Arc<std::sync::atomic::AtomicUsize>,
    }

    async fn respond(State(s): State<MockState>) -> axum::response::Response {
        s.hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let body = format!(
            "event: message_start\ndata: {{\"type\":\"message_start\",\"message\":{{\"id\":\"msg_1\",\"role\":\"assistant\"}}}}\n\n\
             event: content_block_delta\ndata: {{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{{\"type\":\"text_delta\",\"text\":\"{}\"}}}}\n\n\
             event: message_stop\ndata: {{\"type\":\"message_stop\"}}\n\n",
            s.text
        );
        axum::response::Response::builder()
            .status(200)
            .header("content-type", "text/event-stream")
            .body(axum::body::Body::from(body))
            .unwrap()
    }

    let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mock_state = MockState {
        text: sentinel_text.to_string(),
        hits: hits.clone(),
    };
    let app = Router::new()
        .route("/v1/messages", post(respond))
        .with_state(mock_state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), hits)
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

/// Serializes the tests in this file that mutate `POLYFLARE_STARVATION_WAIT_BUDGET_SECS`/
/// `POLYFLARE_STARVATION_HEARTBEAT_SECS` — env vars are process-global.
fn starvation_env_lock() -> &'static std::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
}

/// (1) THE e2e: a single-account pool briefly cooled down (durable `rate_limited`/`reset_at`,
/// recovering ~2 real seconds after the request starts) ⇒ through the REAL `build_app`, the client
/// gets keepalives THEN the clean upstream stream, with budget/heartbeat resolved via the REAL
/// `config::starvation_wait_budget_secs_from_env`/`starvation_heartbeat_secs_from_env` (not a
/// hardcoded test constant), and a content-free starvation signal fires.
#[tokio::test]
async fn all_accounts_recover_during_the_wait_client_gets_keepalives_then_clean_stream_config_driven(
) {
    // Scoped so the (synchronous) `MutexGuard` is dropped BEFORE any `.await` below —
    // `clippy::await_holding_lock` (mirrors `config_driven_failover.rs`'s own scoping).
    let (budget_secs, heartbeat_secs) = {
        let _guard = starvation_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_STARVATION_WAIT_BUDGET_SECS", "6");
            std::env::set_var("POLYFLARE_STARVATION_HEARTBEAT_SECS", "1");
        }
        let budget_secs = config::starvation_wait_budget_secs_from_env();
        let heartbeat_secs = config::starvation_heartbeat_secs_from_env(budget_secs);
        unsafe {
            std::env::remove_var("POLYFLARE_STARVATION_WAIT_BUDGET_SECS");
            std::env::remove_var("POLYFLARE_STARVATION_HEARTBEAT_SECS");
        }
        (budget_secs, heartbeat_secs)
    };
    assert_eq!(
        budget_secs, 6,
        "the env var round-trips through the real config parser"
    );
    assert_eq!(heartbeat_secs, 1);

    let (store, cipher, _dir) = spawn_store().await;
    let upstream_url = spawn_stub_upstream().await;
    // `reset_at` starts as a far-future placeholder — see the re-anchoring right before the
    // request fires below for why the REAL margin isn't set here.
    let mut a = account("A", "rate_limited");
    a.reset_at = Some(now() + 3600);
    store
        .accounts()
        .insert(&a, &tokens("tokA"), &cipher)
        .await
        .unwrap();
    let state = build_state(
        store,
        cipher,
        upstream_url,
        Duration::from_secs(budget_secs as u64),
        Duration::from_secs(heartbeat_secs as u64),
    );
    let pf = spawn_app(state.clone()).await;

    // Anti-flake fix (this test used to set `reset_at = now() + 2` BEFORE the SQLite insert +
    // `spawn_app` (TCP bind + `tokio::spawn`'ing the axum server) above. That setup is real I/O
    // and, under the CPU contention of the full workspace test suite running in parallel, can
    // itself eat a non-trivial fraction of a short margin — occasionally the whole 2s of it,
    // leaving the account already "recovered" (or the keepalive window already exhausted) by the
    // time the request ever reached the wait loop, so zero keepalives fired and the assertion
    // below flaked. Re-anchoring `reset_at` to a wall-clock timestamp captured HERE — immediately
    // before the request fires, after all setup I/O is done — means the only latency the margin
    // has to absorb is the local HTTP round trip + axum routing (sub-millisecond in practice), not
    // store/server startup. The margin is also widened from 2s to 4s for extra slack against
    // scheduler jitter, giving room for multiple heartbeat ticks (heartbeat=1s) rather than
    // requiring exactly one to land in a narrow window. `update_status_and_reset` bumps the
    // account_cache generation so the fresh `reset_at` is what the request actually observes.
    let fire_at = now();
    state
        .store
        .accounts()
        .update_status_and_reset("A", "rate_limited", Some(fire_at + 4))
        .await
        .unwrap();

    let resp = reqwest::Client::new()
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "m", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "the wait committed HTTP 200 immediately"
    );
    let body = drain(resp).await;
    assert!(
        body.contains(": keepalive"),
        "at least one keepalive must have been emitted during the ~4s wait: {body}"
    );
    assert!(
        body.contains("response.completed"),
        "the client gets the clean upstream stream once the account recovers: {body}"
    );

    assert_eq!(
        state.starvation_metrics.total(),
        1,
        "exactly one Layer 2 wait terminal outcome recorded"
    );
    let (backfill, _rx) = state.log_bus.subscribe();
    let starvation_events: Vec<_> = backfill.iter().filter(|e| e.kind == "starvation").collect();
    assert_eq!(starvation_events.len(), 1, "got: {backfill:?}");
    let ev = starvation_events[0];
    assert_eq!(ev.account.as_deref(), Some("A"));
    assert!(ev.latency_ms.is_some_and(|ms| ms > 0));

    // CONTENT-SAFETY: no body/token ever leaks into the signal.
    let msg_lc = ev.message.to_lowercase();
    for forbidden in ["\"hi\"", "response.completed", "toka", "bearer"] {
        assert!(
            !msg_lc.contains(forbidden),
            "forbidden content `{forbidden}` leaked into the starvation signal: {}",
            ev.message
        );
    }
}

/// (2) THE DISABLE LEVER / regression: `POLYFLARE_STARVATION_WAIT_BUDGET_SECS=0`, resolved through
/// the REAL config function into `Duration::ZERO`, disables Layer 2 entirely — a cooldown-only
/// empty pool (the account is never going to recover within the test's lifetime, but that must not
/// even matter: Layer 2 never enters ANY wait at all) gets a FAST, pre-response 503. No HTTP 200 is
/// ever committed, no keepalive is ever emitted, and no starvation signal fires.
#[tokio::test]
async fn budget_zero_disables_layer_2_fast_503_config_driven() {
    // Scoped so the (synchronous) `MutexGuard` is dropped BEFORE any `.await` below.
    let (budget_secs, heartbeat_secs) = {
        let _guard = starvation_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_STARVATION_WAIT_BUDGET_SECS", "0");
        }
        let budget_secs = config::starvation_wait_budget_secs_from_env();
        let heartbeat_secs = config::starvation_heartbeat_secs_from_env(budget_secs);
        unsafe {
            std::env::remove_var("POLYFLARE_STARVATION_WAIT_BUDGET_SECS");
        }
        (budget_secs, heartbeat_secs)
    };
    assert_eq!(
        budget_secs, 0,
        "the disable lever round-trips through the real config parser"
    );

    let (store, cipher, _dir) = spawn_store().await;
    let mut a = account("A", "rate_limited");
    a.reset_at = Some(now() + 3600); // would never recover in any sane test budget anyway
    store
        .accounts()
        .insert(&a, &tokens("tokA"), &cipher)
        .await
        .unwrap();
    let upstream_url = spawn_stub_upstream().await;
    let state = build_state(
        store,
        cipher,
        upstream_url,
        Duration::from_secs(budget_secs as u64), // Duration::ZERO
        Duration::from_secs(heartbeat_secs as u64),
    );
    let pf = spawn_app(state.clone()).await;

    let start = Instant::now();
    let resp = reqwest::Client::new()
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "m", "input": "hi"}))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let elapsed = start.elapsed();

    assert_eq!(
        status, 503,
        "budget=0 ⇒ Layer 2 disabled ⇒ today's fast 503"
    );
    assert!(
        // Widened from 2s to 5s for slack against scheduler jitter under the full workspace test
        // suite's CPU contention — this only guards against a REGRESSION where Layer 2 accidentally
        // enters its real wait loop (which ticks on 1s heartbeats and would blow well past 5s), not
        // against ordinary request-handling latency.
        elapsed < Duration::from_secs(5),
        "must be a FAST pre-response 503 — no wait loop of any kind ran (elapsed={elapsed:?})"
    );

    assert_eq!(
        state.starvation_metrics.total(),
        0,
        "no Layer 2 wait ever started — the metric must not bump"
    );
    let (backfill, _rx) = state.log_bus.subscribe();
    assert!(
        backfill.iter().all(|e| e.kind != "starvation"),
        "no starvation signal should fire when Layer 2 is disabled: {backfill:?}"
    );
}

/// B5-anthropic Task 3 (3): the native Anthropic `/v1/messages` empty-pool analogue of test (1)
/// above. A single `provider="anthropic"` account is briefly cooled down (durable
/// `rate_limited`/`reset_at`); the native handler's empty-pool branch must now fall through
/// Layer 1 → Layer 2 exactly like `/responses` does, but speaking the ANTHROPIC dialect: `event:
/// ping` keepalives (never Codex's `: keepalive` comment), and the REAL recovered Anthropic SSE
/// stream spliced through verbatim once the account recovers (never a translated/synthetic shape).
#[tokio::test]
async fn native_anthropic_recovery_wait_serves_ping_keepalives_then_clean_anthropic_stream() {
    let (budget_secs, heartbeat_secs) = {
        let _guard = starvation_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_STARVATION_WAIT_BUDGET_SECS", "6");
            std::env::set_var("POLYFLARE_STARVATION_HEARTBEAT_SECS", "1");
        }
        let budget_secs = config::starvation_wait_budget_secs_from_env();
        let heartbeat_secs = config::starvation_heartbeat_secs_from_env(budget_secs);
        unsafe {
            std::env::remove_var("POLYFLARE_STARVATION_WAIT_BUDGET_SECS");
            std::env::remove_var("POLYFLARE_STARVATION_HEARTBEAT_SECS");
        }
        (budget_secs, heartbeat_secs)
    };
    assert_eq!(budget_secs, 6);
    assert_eq!(heartbeat_secs, 1);

    let (store, cipher, _dir) = spawn_store().await;
    let sentinel_email = "sentinel-anthropic-user@example.test".to_string();
    let sentinel_token = "sentinel-tok-9F3Q".to_string();
    let sentinel_text = "SENTINEL-RECOVERED-CONTENT-9F3Q";
    let (anthropic_upstream, upstream_hits) = spawn_anthropic_stub_upstream(sentinel_text).await;

    let mut a = anthropic_account("anthropic-a", "rate_limited", &sentinel_email);
    a.reset_at = Some(now() + 3600); // far-future placeholder — re-anchored right before the request.
    store
        .accounts()
        .insert(&a, &tokens(&sentinel_token), &cipher)
        .await
        .unwrap();

    let state = build_state_native_anthropic(
        store,
        cipher,
        anthropic_upstream,
        Duration::from_secs(budget_secs as u64),
        Duration::from_secs(heartbeat_secs as u64),
    );
    let pf = spawn_app(state.clone()).await;

    // Same anti-flake re-anchoring as test (1): capture `fire_at` immediately before the request
    // fires, after all setup I/O is done.
    let fire_at = now();
    state
        .store
        .accounts()
        .update_status_and_reset("anthropic-a", "rate_limited", Some(fire_at + 4))
        .await
        .unwrap();

    let resp = reqwest::Client::new()
        .post(format!("{pf}/v1/messages"))
        .json(&serde_json::json!({
            "model": "claude-3-5-legacy-model",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "the wait committed HTTP 200 immediately"
    );
    let body = drain(resp).await;

    assert!(
        body.contains("event: ping"),
        "at least one Anthropic `event: ping` keepalive must fire during the ~4s wait: {body}"
    );
    assert!(
        body.contains("data: {\"type\":\"ping\"}"),
        "the ping frame must carry the fixed Anthropic ping payload: {body}"
    );
    assert!(
        !body.contains(": keepalive"),
        "the native Anthropic path must never emit Codex's `: keepalive` comment frame: {body}"
    );
    assert!(
        body.contains("event: message_start") && body.contains("event: message_stop"),
        "the REAL recovered Anthropic stream's event shape must be spliced through verbatim: {body}"
    );
    assert!(
        body.contains(sentinel_text),
        "the REAL recovered upstream stream (not a synthetic frame) must be spliced through: {body}"
    );
    assert!(
        !body.contains("event: response.failed"),
        "a successful recovery must never emit the Codex-dialect in-band error frame: {body}"
    );
    assert_eq!(
        upstream_hits.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "exactly one real upstream attempt after recovery"
    );

    assert_eq!(
        state.starvation_metrics.total(),
        1,
        "exactly one Layer 2 wait terminal outcome recorded"
    );
    let (backfill, _rx) = state.log_bus.subscribe();
    let starvation_events: Vec<_> = backfill.iter().filter(|e| e.kind == "starvation").collect();
    assert_eq!(starvation_events.len(), 1, "got: {backfill:?}");
    let ev = starvation_events[0];
    assert_eq!(ev.account.as_deref(), Some("anthropic-a"));
    assert!(ev.latency_ms.is_some_and(|ms| ms > 0));

    // CONTENT-SAFETY: the seeded sentinel email/token never leak into the starvation signal.
    let msg_lc = ev.message.to_lowercase();
    for forbidden in [
        sentinel_email.to_lowercase(),
        sentinel_token.to_lowercase(),
        "bearer".to_string(),
    ] {
        assert!(
            !msg_lc.contains(&forbidden),
            "forbidden content `{forbidden}` leaked into the starvation signal: {}",
            ev.message
        );
    }
}

/// B5-anthropic Task 3 (4): forces the BOUNDED-BUDGET terminal (never a genuine recovery — the
/// account's `reset_at` sits far past the tiny budget) on the native Anthropic path, and asserts the
/// client-facing terminal is the Anthropic `event: error` frame (never Codex's `response.failed`
/// shape), carrying only the fixed content-free reason code — and that the seeded account
/// email/token never reach the client, because the upstream is never actually called.
#[tokio::test]
async fn native_anthropic_recovery_wait_budget_exceeded_emits_anthropic_error_frame() {
    let (budget_secs, heartbeat_secs) = {
        let _guard = starvation_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_STARVATION_WAIT_BUDGET_SECS", "2");
            std::env::set_var("POLYFLARE_STARVATION_HEARTBEAT_SECS", "1");
        }
        let budget_secs = config::starvation_wait_budget_secs_from_env();
        let heartbeat_secs = config::starvation_heartbeat_secs_from_env(budget_secs);
        unsafe {
            std::env::remove_var("POLYFLARE_STARVATION_WAIT_BUDGET_SECS");
            std::env::remove_var("POLYFLARE_STARVATION_HEARTBEAT_SECS");
        }
        (budget_secs, heartbeat_secs)
    };
    assert_eq!(budget_secs, 2);
    assert_eq!(heartbeat_secs, 1);

    let (store, cipher, _dir) = spawn_store().await;
    let sentinel_email = "sentinel-budget-user@example.test".to_string();
    let sentinel_token = "sentinel-tok-BUDGETXX".to_string();
    let sentinel_text = "SENTINEL-SHOULD-NEVER-BE-SENT";
    let (anthropic_upstream, upstream_hits) = spawn_anthropic_stub_upstream(sentinel_text).await;

    let mut a = anthropic_account("anthropic-b", "rate_limited", &sentinel_email);
    // Recovers 3600s from now — well past the 2s budget, so this MUST end in BudgetExceeded, never
    // a genuine recovery, no matter how long the test process itself takes to run.
    a.reset_at = Some(now() + 3600);
    store
        .accounts()
        .insert(&a, &tokens(&sentinel_token), &cipher)
        .await
        .unwrap();

    let state = build_state_native_anthropic(
        store,
        cipher,
        anthropic_upstream,
        Duration::from_secs(budget_secs as u64),
        Duration::from_secs(heartbeat_secs as u64),
    );
    let pf = spawn_app(state.clone()).await;

    let resp = reqwest::Client::new()
        .post(format!("{pf}/v1/messages"))
        .json(&serde_json::json!({
            "model": "claude-3-5-legacy-model",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "the wait committed HTTP 200 immediately, even though it ends in an in-band error"
    );
    let body = drain(resp).await;

    assert!(
        body.contains("event: ping"),
        "at least one keepalive must fire during the bounded 2s wait: {body}"
    );
    assert!(
        body.contains("event: error"),
        "the budget-exceeded terminal must be an Anthropic `event: error` frame: {body}"
    );
    assert!(
        body.contains("\"type\":\"overloaded_error\""),
        "the fixed Anthropic error type: {body}"
    );
    assert!(
        body.contains("starvation_wait_budget_exceeded"),
        "the fixed, content-free reason code: {body}"
    );
    assert!(
        !body.contains("event: response.failed"),
        "must never emit the Codex-dialect frame on the Anthropic path: {body}"
    );

    // Upstream must NEVER have been reached — the wait gave up before any re-select/execute ran.
    assert_eq!(
        upstream_hits.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "budget exceeded ⇒ zero real upstream attempts"
    );
    assert!(
        !body.contains(sentinel_text),
        "no real upstream content can leak — upstream was never called: {body}"
    );
    let body_lc = body.to_lowercase();
    assert!(
        !body_lc.contains(&sentinel_email.to_lowercase()),
        "the seeded account email must never leak into the client-facing frame: {body}"
    );
    assert!(
        !body_lc.contains(&sentinel_token.to_lowercase()),
        "the seeded account token must never leak into the client-facing frame: {body}"
    );
    assert!(
        !body_lc.contains("bearer"),
        "no bearer token material in the client-facing frame: {body}"
    );

    assert_eq!(
        state.starvation_metrics.total(),
        1,
        "exactly one Layer 2 wait terminal outcome recorded"
    );
    let (backfill, _rx) = state.log_bus.subscribe();
    let starvation_events: Vec<_> = backfill.iter().filter(|e| e.kind == "starvation").collect();
    assert_eq!(starvation_events.len(), 1, "got: {backfill:?}");
    assert_eq!(starvation_events[0].account.as_deref(), Some("anthropic-b"));
}
