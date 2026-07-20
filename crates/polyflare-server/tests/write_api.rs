//! Write API: `PATCH /api/accounts/{id}` updates pool / routing policy / paused state. Asserts the
//! change round-trips (readable via `/api/accounts` + the store), that validation fails closed
//! (bad routing_policy/status → 400, unknown id → 404), and that a partial patch leaves other
//! fields untouched.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{CapacityWeighted, Continuity, Executor};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};

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

fn tokens() -> PlainTokens {
    PlainTokens {
        access_token: "a".to_string(),
        refresh_token: "r".to_string(),
        id_token: "i".to_string(),
    }
}

async fn spawn_with(store: Store) -> String {
    let cipher = TokenCipher::from_key_bytes(&[13u8; 32]).unwrap();
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_secs(30),
    ));
    let state = Arc::new(AppState {
        enforce_client_keys: false,
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
        upstream_base_url: "http://127.0.0.1:9".to_string(),
        anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),
        refresh_locks: Default::default(),
        capture_fingerprint_path: None,
        codex_version: Arc::new(polyflare_codex::CodexVersionCache::new().unwrap()),
        account_cache: Arc::new(polyflare_server::account_cache::AccountCache::new()),
        token_cache: Default::default(),
        admin_token: Some("secret".to_string()),
        live_logs: true,
        ws_downstream: false,
        log_bus: polyflare_server::log_bus::LogBus::new(1000),
        max_account_attempts: 3,
        failover_metrics: polyflare_server::observability::FailoverMetrics::new(),
        health_tier_metrics: polyflare_server::observability::HealthTierMetrics::new(),
        starvation_wait_budget: std::time::Duration::from_secs(60),
        starvation_heartbeat: std::time::Duration::from_secs(10),
        wake_jitter_ms: 0,
        inflight_penalty_pct: 2.5,
        lease_metrics: polyflare_server::observability::LeaseMetrics::new(),
        upstream_request_metrics: polyflare_server::observability::UpstreamRequestMetrics::new(),
        rate_limit_metrics: polyflare_server::observability::RateLimitMetrics::new(),
        relay_metrics: polyflare_server::observability::RelayMetrics::new(),
        model_catalog: polyflare_server::model_catalog::floor_only_model_catalog(),

        starvation_metrics: polyflare_server::observability::StarvationMetrics::new(),
        stream_idle_timeout: std::time::Duration::from_secs(300),
        soft_drain_enabled: true,
        request_log_retention_days: 0,
        usage_history_retention_days: 0,
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

async fn store_with_one() -> Store {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[13u8; 32]).unwrap();
    store
        .accounts()
        .insert(&account("acct-1", None), &tokens(), &cipher)
        .await
        .unwrap();
    std::mem::forget(dir);
    store
}

async fn fetch_account(pf: &str, id: &str) -> serde_json::Value {
    let body: serde_json::Value = reqwest::Client::new()
        .get(format!("{pf}/api/accounts"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    body.as_array()
        .unwrap()
        .iter()
        .find(|a| a["id"] == id)
        .cloned()
        .unwrap()
}

#[tokio::test]
async fn patch_assigns_pool_and_pauses_then_clears() {
    let store = store_with_one().await;
    let repo = store.accounts();
    let pf = spawn_with(store).await;
    let client = reqwest::Client::new();

    // Assign a pool + pause + set routing policy in one patch.
    let resp = client
        .patch(format!("{pf}/api/accounts/acct-1"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "pool": "team-a", "status": "paused", "routing_policy": "burn_first" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let a = fetch_account(&pf, "acct-1").await;
    assert_eq!(a["pool"], "team-a");
    assert_eq!(a["status"], "paused");
    // routing_policy isn't in the read view, so verify it via the store.
    assert_eq!(
        repo.get("acct-1").await.unwrap().unwrap().routing_policy,
        "burn_first"
    );

    // A partial patch (status only) must leave pool + routing_policy untouched.
    let resp = client
        .patch(format!("{pf}/api/accounts/acct-1"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "status": "active" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let a = fetch_account(&pf, "acct-1").await;
    assert_eq!(a["status"], "active");
    assert_eq!(a["pool"], "team-a", "pool untouched by a status-only patch");

    // Explicit null clears the pool (unpool).
    let resp = client
        .patch(format!("{pf}/api/accounts/acct-1"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "pool": null }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert!(fetch_account(&pf, "acct-1").await["pool"].is_null());
}

#[tokio::test]
async fn patch_toggles_security_work_authorized_and_leaves_it_alone_when_absent() {
    // TA6 Task 4: the operator write path for the cyber-capability flag. `account()` seeds
    // `security_work_authorized: false`.
    let store = store_with_one().await;
    let repo = store.accounts();
    let pf = spawn_with(store).await;
    let client = reqwest::Client::new();

    // A patch WITH the field flips it.
    let resp = client
        .patch(format!("{pf}/api/accounts/acct-1"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "security_work_authorized": true }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    // security_work_authorized isn't in the /api/accounts LIST view (only the per-account detail
    // view), so verify it via the store — same pattern as routing_policy above.
    assert!(
        repo.get("acct-1")
            .await
            .unwrap()
            .unwrap()
            .security_work_authorized
    );

    // A patch WITHOUT the field (Option semantics regression) must leave it unchanged.
    let resp = client
        .patch(format!("{pf}/api/accounts/acct-1"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "status": "paused" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert!(
        repo.get("acct-1")
            .await
            .unwrap()
            .unwrap()
            .security_work_authorized,
        "a patch omitting the field must not clear it"
    );

    // Flip it back off explicitly.
    let resp = client
        .patch(format!("{pf}/api/accounts/acct-1"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "security_work_authorized": false }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert!(
        !repo
            .get("acct-1")
            .await
            .unwrap()
            .unwrap()
            .security_work_authorized
    );
}

#[tokio::test]
async fn patch_sets_clears_and_validates_alias() {
    // Task 2: `alias` mirrors `pool`'s double-Option shape (absent = unchanged, `null`/whitespace =
    // clear, non-empty <=64 chars = set). Verified via the store (not the `/api/accounts` list
    // view), same pattern as routing_policy/security_work_authorized above — `alias` only appears
    // in the per-account detail view, not the list view.
    let store = store_with_one().await;
    let repo = store.accounts();
    let pf = spawn_with(store).await;
    let client = reqwest::Client::new();

    // Set a value.
    let resp = client
        .patch(format!("{pf}/api/accounts/acct-1"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "alias": "prod-1" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        repo.get("acct-1").await.unwrap().unwrap().alias.as_deref(),
        Some("prod-1")
    );

    // Explicit null clears it.
    let resp = client
        .patch(format!("{pf}/api/accounts/acct-1"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "alias": null }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(repo.get("acct-1").await.unwrap().unwrap().alias, None);

    // Whitespace-only also clears (normalized).
    repo.update_alias("acct-1", Some("prod-1")).await.unwrap();
    let resp = client
        .patch(format!("{pf}/api/accounts/acct-1"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "alias": "   " }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(repo.get("acct-1").await.unwrap().unwrap().alias, None);

    // A 65-char alias is rejected, and the existing value is left untouched (validate-before-apply).
    repo.update_alias("acct-1", Some("prod-1")).await.unwrap();
    let too_long = "a".repeat(65);
    let resp = client
        .patch(format!("{pf}/api/accounts/acct-1"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "alias": too_long }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    assert_eq!(
        repo.get("acct-1").await.unwrap().unwrap().alias.as_deref(),
        Some("prod-1"),
        "a rejected patch must not apply the alias change"
    );

    // Absent alias leaves it unchanged.
    let resp = client
        .patch(format!("{pf}/api/accounts/acct-1"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "status": "paused" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        repo.get("acct-1").await.unwrap().unwrap().alias.as_deref(),
        Some("prod-1"),
        "a patch omitting alias must not clear it"
    );
}

#[tokio::test]
async fn patch_validation_fails_closed() {
    let pf = spawn_with(store_with_one().await).await;
    let client = reqwest::Client::new();

    // Unknown account → 404.
    let resp = client
        .patch(format!("{pf}/api/accounts/nope"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "status": "paused" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    // Invalid routing policy → 400, and nothing applied.
    let resp = client
        .patch(format!("{pf}/api/accounts/acct-1"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "routing_policy": "aggressive", "pool": "x" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    assert!(
        fetch_account(&pf, "acct-1").await["pool"].is_null(),
        "a rejected patch must not partially apply the pool change"
    );

    // A status the UI may not set (e.g. deactivated) → 400.
    let resp = client
        .patch(format!("{pf}/api/accounts/acct-1"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "status": "deactivated" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}
