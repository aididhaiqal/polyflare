//! Multi-pool routing: `/{pool}/responses` must select ONLY accounts tagged with that pool slug,
//! while the bare `/responses` path keeps selecting over ALL accounts. Proven by giving each
//! account a distinct bearer token and asserting which one reached the (shared) upstream via the
//! mock's `last_authorization()`.

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

fn account(id: &str, pool: Option<&str>) -> Account {
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

/// One account's distinct bearer token — its fingerprint at the upstream, so `last_authorization()`
/// reveals which account served a given request.
fn tokens(access: &str) -> PlainTokens {
    PlainTokens {
        access_token: access.to_string(),
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
        enforce_client_keys: false,
        codex_executor,
        control_client: polyflare_codex::build_client().expect("build control_client"),
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
        health_tier_metrics: polyflare_server::observability::HealthTierMetrics::new(),
        starvation_wait_budget: std::time::Duration::from_secs(60),
        starvation_heartbeat: std::time::Duration::from_secs(10),
        wake_jitter_ms: 0,
        starvation_metrics: polyflare_server::observability::StarvationMetrics::new(),
        stream_idle_timeout: std::time::Duration::from_secs(300),
        soft_drain_enabled: true,
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

/// A two-pool store: `codex-a` in `pool-a` (token `tok-a`), `codex-b` in `pool-b` (token `tok-b`).
async fn two_pool_store() -> Store {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[13u8; 32]).unwrap();
    store
        .accounts()
        .insert(
            &account("codex-a", Some("pool-a")),
            &tokens("tok-a"),
            &cipher,
        )
        .await
        .unwrap();
    store
        .accounts()
        .insert(
            &account("codex-b", Some("pool-b")),
            &tokens("tok-b"),
            &cipher,
        )
        .await
        .unwrap();
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
async fn pooled_path_routes_only_to_that_pools_account() {
    let store = two_pool_store().await;
    let mock = MockUpstream::new(ok_events());
    let handle = mock.clone();
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(store, upstream).await;
    let client = reqwest::Client::new();

    // /pool-a/responses → only codex-a is eligible → its token reaches upstream.
    let resp = client
        .post(format!("{pf}/pool-a/responses"))
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        handle.last_authorization().as_deref(),
        Some("Bearer tok-a"),
        "pool-a path must route to the pool-a account"
    );

    // /pool-b/responses → only codex-b.
    let resp = client
        .post(format!("{pf}/pool-b/responses"))
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        handle.last_authorization().as_deref(),
        Some("Bearer tok-b"),
        "pool-b path must route to the pool-b account"
    );
}

#[tokio::test]
async fn unknown_pool_slug_has_no_eligible_account() {
    let store = two_pool_store().await;
    let mock = MockUpstream::new(ok_events());
    let handle = mock.clone();
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(store, upstream).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{pf}/pool-zzz/responses"))
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        503,
        "a slug matching no account must yield no-eligible-account"
    );
    assert_eq!(handle.request_count(), 0, "upstream must never be called");
}

#[tokio::test]
async fn bare_path_selects_across_all_pools() {
    let store = two_pool_store().await;
    let mock = MockUpstream::new(ok_events());
    let handle = mock.clone();
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(store, upstream).await;
    let client = reqwest::Client::new();

    // Bare /responses ignores pool tagging entirely: with only pooled accounts present, it still
    // finds an eligible one (backward compatibility — pooled accounts remain reachable bare).
    let resp = client
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let auth = handle.last_authorization();
    assert!(
        matches!(auth.as_deref(), Some("Bearer tok-a") | Some("Bearer tok-b")),
        "bare path routes to some pooled account, got {auth:?}"
    );
}
