//! Per-pool routing strategy: a pool configured with a different `Selector` routes differently from
//! the global default over the SAME accounts. Proven with two deterministic strategies that pick
//! OPPOSITE accounts — the global default `fill_first` (warmest) picks the higher-usage account,
//! while the pool's `usage_weighted` (least-used) picks the lower-usage one — asserted via each
//! account's distinct bearer token reaching the mock upstream.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{Continuity, Executor, FillFirst, Selector, UsageWeighted};
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
        provider: "codex".to_string(),
        pool: Some("p1".to_string()),
    }
}

fn tokens(access: &str) -> PlainTokens {
    PlainTokens {
        access_token: access.to_string(),
        refresh_token: "r".to_string(),
        id_token: "i".to_string(),
    }
}

async fn seed() -> Store {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[13u8; 32]).unwrap();
    let repo = store.accounts();
    // Both accounts in pool "p1". `warm` has higher weekly usage than `lean`.
    repo.insert(&account("warm"), &tokens("tok-warm"), &cipher)
        .await
        .unwrap();
    repo.insert(&account("lean"), &tokens("tok-lean"), &cipher)
        .await
        .unwrap();
    repo.insert_usage_window(
        "warm",
        "secondary",
        40.0,
        Some(now() + 604800),
        Some(10080),
        now(),
    )
    .await
    .unwrap();
    repo.insert_usage_window(
        "lean",
        "secondary",
        10.0,
        Some(now() + 604800),
        Some(10080),
        now(),
    )
    .await
    .unwrap();
    std::mem::forget(dir);
    store
}

async fn spawn(store: Store, upstream: String) -> String {
    let cipher = TokenCipher::from_key_bytes(&[13u8; 32]).unwrap();
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_secs(30),
    ));
    // Global default = fill_first (warmest wins); pool "p1" overridden to usage_weighted (least-used).
    let mut pool_selectors: HashMap<String, Arc<dyn Selector>> = HashMap::new();
    pool_selectors.insert("p1".to_string(), Arc::new(UsageWeighted));

    let state = Arc::new(AppState {
        codex_executor: Arc::new(CodexExecutor::new().unwrap()) as Arc<dyn Executor>,
        anthropic_executor: Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap())
            as Arc<dyn Executor>,
        selector: Arc::new(FillFirst),
        pool_selectors,
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
        admin_token: None,
        live_logs: false,
        log_bus: polyflare_server::log_bus::LogBus::new(1000),

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

fn ok_events() -> Vec<String> {
    vec![r#"{"type":"response.completed"}"#.to_string()]
}

#[tokio::test]
async fn per_pool_strategy_overrides_the_global_default() {
    let mock = MockUpstream::new(ok_events());
    let handle = mock.clone();
    let upstream = mock.spawn().await;
    let pf = spawn(seed().await, upstream).await;

    let client = reqwest::Client::new();

    // Bare /responses → global default fill_first → the WARMER account (warm, 40%).
    let resp = client
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        handle.last_authorization().as_deref(),
        Some("Bearer tok-warm"),
        "bare path uses the global fill_first → warmest account"
    );

    // /p1/responses → pool override usage_weighted → the LEAST-used account (lean, 10%).
    let resp = client
        .post(format!("{pf}/p1/responses"))
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        handle.last_authorization().as_deref(),
        Some("Bearer tok-lean"),
        "pool p1 uses usage_weighted → least-used account, opposite of the default"
    );
}
