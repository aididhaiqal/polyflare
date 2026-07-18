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

        starvation_metrics: polyflare_server::observability::StarvationMetrics::new(),
        stream_idle_timeout: std::time::Duration::from_secs(300),
        soft_drain_enabled: true,
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
