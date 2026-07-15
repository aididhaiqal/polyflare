//! Dashboard read API: `/api/accounts` surfaces per-account usage windows + reset times (the
//! "see the reset time" goal), `/api/pools` aggregates accounts by pool, `/api/requests` pages the
//! request log. Asserts shape + that NO secret (token) is present in any response body.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{CapacityWeighted, Continuity, Executor};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_store::{Account, PlainTokens, RequestLogRecord, Store, TokenCipher};

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn account(id: &str, email: &str, pool: Option<&str>) -> Account {
    Account {
        id: id.to_string(),
        chatgpt_account_id: None,
        chatgpt_user_id: None,
        email: email.to_string(),
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
        reset_at: Some(1_783_900_000),
        blocked_at: None,
        security_work_authorized: false,
        provider: "codex".to_string(),
        pool: pool.map(str::to_string),
    }
}

fn tokens() -> PlainTokens {
    PlainTokens {
        access_token: "SECRET-ACCESS-TOKEN".to_string(),
        refresh_token: "SECRET-REFRESH".to_string(),
        id_token: "SECRET-ID".to_string(),
    }
}

async fn seed_store() -> Store {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[13u8; 32]).unwrap();
    let repo = store.accounts();
    repo.insert(
        &account("codex-a", "a@example.test", Some("team-a")),
        &tokens(),
        &cipher,
    )
    .await
    .unwrap();
    repo.insert(
        &account("codex-b", "b@example.test", None),
        &tokens(),
        &cipher,
    )
    .await
    .unwrap();
    // Only codex-a gets a weekly usage window (5h/primary absent, as upstream currently behaves).
    repo.insert_usage_window(
        "codex-a",
        "secondary",
        73.5,
        Some(1_783_900_000),
        Some(10080),
        now(),
    )
    .await
    .unwrap();
    // One request-log row so /api/requests has something to page.
    store
        .request_log()
        .insert(&RequestLogRecord {
            requested_at: now(),
            provider: "codex".to_string(),
            method: "POST".to_string(),
            path: "/responses".to_string(),
            aliased: false,
            status: 200,
            duration_ms: 12,
        })
        .await
        .unwrap();
    std::mem::forget(dir);
    store
}

async fn spawn(store: Store) -> String {
    let cipher = TokenCipher::from_key_bytes(&[13u8; 32]).unwrap();
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_secs(30),
    ));
    let state = Arc::new(AppState {
        codex_executor: Arc::new(CodexExecutor::new().unwrap()) as Arc<dyn Executor>,
        anthropic_executor: Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap())
            as Arc<dyn Executor>,
        selector: Arc::new(CapacityWeighted),
        continuity,
        store,
        cipher,
        oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(),
        upstream_base_url: "http://127.0.0.1:9".to_string(),
        anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),
        refresh_locks: Default::default(),
        capture_fingerprint_path: None,
        codex_version: Arc::new(polyflare_codex::CodexVersionCache::new().unwrap()),
        account_cache: Arc::new(polyflare_server::account_cache::AccountCache::new()),
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
async fn accounts_endpoint_surfaces_usage_windows_and_reset_times() {
    let pf = spawn(seed_store().await).await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{pf}/api/accounts"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 2);

    // Accounts are listed by id, so codex-a is first.
    let a = &arr[0];
    assert_eq!(a["id"], "codex-a");
    assert_eq!(a["pool"], "team-a");
    assert_eq!(a["reset_at"], 1_783_900_000_i64);
    // Weekly (secondary) window present with its own reset; 5h (primary) absent → null.
    assert_eq!(a["secondary"]["used_percent"], 73.5);
    assert_eq!(a["secondary"]["reset_at"], 1_783_900_000_i64);
    assert!(
        a["primary"].is_null(),
        "5h window not reported → null, not blocked"
    );

    // codex-b is unpooled and has no usage window yet.
    let b = &arr[1];
    assert_eq!(b["id"], "codex-b");
    assert!(b["pool"].is_null());
    assert!(b["secondary"].is_null());
}

#[tokio::test]
async fn no_secret_token_is_ever_present_in_a_read_response() {
    let pf = spawn(seed_store().await).await;
    let client = reqwest::Client::new();
    for path in ["/api/accounts", "/api/pools", "/api/requests"] {
        let text = client
            .get(format!("{pf}{path}"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(
            !text.contains("SECRET"),
            "{path} response leaked a token: {text}"
        );
    }
}

#[tokio::test]
async fn pools_endpoint_aggregates_named_and_unpooled_groups() {
    let pf = spawn(seed_store().await).await;
    let client = reqwest::Client::new();
    let body: serde_json::Value = client
        .get(format!("{pf}/api/pools"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let arr = body.as_array().unwrap();
    // Named pool "team-a" first, unpooled (null) group last.
    assert_eq!(arr[0]["pool"], "team-a");
    assert_eq!(arr[0]["accounts"], 1);
    assert_eq!(arr[0]["active"], 1);
    assert!(arr[1]["pool"].is_null(), "unpooled group last");
    assert_eq!(arr[1]["accounts"], 1);
}

#[tokio::test]
async fn requests_endpoint_pages_the_log() {
    let pf = spawn(seed_store().await).await;
    let client = reqwest::Client::new();
    let body: serde_json::Value = client
        .get(format!("{pf}/api/requests?limit=10"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["total"], 1);
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["path"], "/responses");
    assert_eq!(rows[0]["status"], 200);
    assert_eq!(rows[0]["duration_ms"], 12);
}
