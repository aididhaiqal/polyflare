//! Codex CLI's ONLY WebSocket→HTTP fallback trigger is a `426 Upgrade Required` returned at
//! WS-handshake time (`codex-rs/core/src/client.rs` ~line 1596: `StatusCode::UPGRADE_REQUIRED` →
//! `WebsocketStreamOutcome::FallbackToHttp` → `force_http_fallback`, a session-lifetime one-way
//! switch). PolyFlare has no WebSocket support at all, so a client configured with
//! `supports_websockets = true` sending a `GET` upgrade request to `/responses` must get exactly
//! 426 — axum's default 405 (Method Not Allowed) is NOT a recognized fallback trigger and would
//! hard-fail the client instead of degrading it to HTTP-SSE. See `crate::ingress::
//! websocket_fallback_handler` for the full rationale.
//!
//! Also proves the new GET route doesn't shadow or break the existing POST `/responses` /
//! `/{pool}/responses` handlers — a plain regression net alongside the new coverage.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{CapacityWeighted, Continuity, Executor};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};
use polyflare_testkit::MockUpstream;

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn codex_account(id: &str, pool: Option<&str>) -> Account {
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
        pool: pool.map(str::to_string),
    }
}

fn tokens() -> PlainTokens {
    PlainTokens {
        access_token: "tok".to_string(),
        refresh_token: "r".to_string(),
        id_token: "i".to_string(),
    }
}

async fn spawn_polyflare(store: Store, upstream: String) -> String {
    let cipher = TokenCipher::from_key_bytes(&[13u8; 32]).unwrap();
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_secs(30),
    ));
    let codex_executor: Arc<dyn Executor> = Arc::new(CodexExecutor::new().unwrap());
    let anthropic_executor: Arc<dyn Executor> =
        Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap());

    let state = Arc::new(AppState {
        codex_executor,
        anthropic_executor,
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
        codex_version: std::sync::Arc::new(polyflare_codex::CodexVersionCache::new().unwrap()),
        account_cache: std::sync::Arc::new(polyflare_server::account_cache::AccountCache::new()),
        token_cache: Default::default(),
        admin_token: None,
        live_logs: false,
        log_bus: polyflare_server::log_bus::LogBus::new(1000),
        max_account_attempts: 3,
        failover_metrics: polyflare_server::observability::FailoverMetrics::new(),

        runtime: Default::default(),
    });
    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

/// An empty store is enough for the GET-fallback tests: the fallback route answers before any
/// account selection happens.
async fn empty_store() -> Store {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    std::mem::forget(dir);
    store
}

fn ok_events() -> Vec<String> {
    vec![
        r#"{"type":"response.output_text.delta","delta":"hi"}"#.to_string(),
        r#"{"type":"response.completed"}"#.to_string(),
    ]
}

#[tokio::test]
async fn get_bare_responses_returns_426_upgrade_required() {
    let store = empty_store().await;
    let mock = MockUpstream::new(ok_events());
    let handle = mock.clone();
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(store, upstream).await;
    let client = reqwest::Client::new();

    let resp = client.get(format!("{pf}/responses")).send().await.unwrap();

    assert_eq!(
        resp.status(),
        426,
        "GET /responses (a WS-handshake attempt) must get exactly 426 — Codex's sole \
         fallback-to-HTTP trigger — not axum's default 405"
    );
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("HTTP-SSE"),
        "426 body should explain PolyFlare serves HTTP-SSE only, got: {body}"
    );
    assert_eq!(
        handle.request_count(),
        0,
        "the fallback response must not reach the upstream at all"
    );
}

#[tokio::test]
async fn get_pooled_responses_returns_426_upgrade_required() {
    let store = empty_store().await;
    let mock = MockUpstream::new(ok_events());
    let handle = mock.clone();
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(store, upstream).await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{pf}/pool-a/responses"))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        426,
        "GET /{{pool}}/responses must also get 426, same as the bare path"
    );
    assert_eq!(handle.request_count(), 0);
}

#[tokio::test]
async fn post_responses_still_routes_to_the_real_handler() {
    // Regression net: the new GET route must not shadow or break the existing POST handler.
    let store = empty_store().await;
    store
        .accounts()
        .insert(
            &codex_account("codex-1", None),
            &tokens(),
            &TokenCipher::from_key_bytes(&[13u8; 32]).unwrap(),
        )
        .await
        .unwrap();
    let mock = MockUpstream::new(ok_events());
    let handle = mock.clone();
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(store, upstream).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"}))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        200,
        "POST /responses must still route to the real handler unchanged"
    );
    assert_eq!(handle.request_count(), 1, "exactly one upstream call");
}

#[tokio::test]
async fn post_pooled_responses_still_routes_to_the_real_handler() {
    let store = empty_store().await;
    store
        .accounts()
        .insert(
            &codex_account("codex-a", Some("pool-a")),
            &tokens(),
            &TokenCipher::from_key_bytes(&[13u8; 32]).unwrap(),
        )
        .await
        .unwrap();
    let mock = MockUpstream::new(ok_events());
    let handle = mock.clone();
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(store, upstream).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{pf}/pool-a/responses"))
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"}))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        200,
        "POST /{{pool}}/responses must still route to the real handler unchanged"
    );
    assert_eq!(handle.request_count(), 1);
}
