//! D16 Task 5: `GET /api/pace` (pool-wide WeeklyCreditPace) + the per-account `forecast` field on
//! `GET /api/accounts/{id}/trends`. Content-safety is the crux here: both endpoints must return a
//! REAL, non-null report/forecast (seeded with fresh secondary usage rows) AND never leak the
//! seeded account's email or token — proving the assertion has teeth, not a vacuous null-check.
//!
//! Harness copied from `tests/read_api.rs`'s self-contained `account`/`spawn_with_state` pattern
//! (not `tests/support/mod.rs`, whose `account()` hardcodes the email — this test needs a sentinel
//! email it controls).

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{Continuity, Executor};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_server::runtime_settings::{RuntimeSettings, RuntimeSettingsFields};
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};

/// Sentinel content that must NEVER appear in either endpoint's response body.
const SENTINEL_EMAIL: &str = "sentinel-d16@example.test";
const SENTINEL_TOKEN: &str = "sk-SENTINEL-D16-TOKEN";

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn account(id: &str, email: &str) -> Account {
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
        reset_at: None,
        blocked_at: None,
        security_work_authorized: false,
        provider: "codex".to_string(),
        pool: None,
    }
}

fn sentinel_tokens() -> PlainTokens {
    PlainTokens {
        access_token: SENTINEL_TOKEN.to_string(),
        refresh_token: "sk-SENTINEL-D16-REFRESH".to_string(),
        id_token: "sk-SENTINEL-D16-ID".to_string(),
    }
}

/// Seeds one account (`acct-1`, sentinel email + sentinel token) with 2 FRESH secondary usage rows
/// so both `/api/pace` and the trends `forecast` have real (non-null) data to report:
/// `recorded_at` = now-600 and now, `used_percent` 40 -> 50, `reset_at` = now + 6 days,
/// `window_minutes` = 10080 (weekly).
async fn seed_store() -> Store {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[13u8; 32]).unwrap();
    let repo = store.accounts();
    repo.insert(
        &account("acct-1", SENTINEL_EMAIL),
        &sentinel_tokens(),
        &cipher,
    )
    .await
    .unwrap();

    let reset_at = now() + 6 * 24 * 3600;
    repo.insert_usage_window(
        "acct-1",
        "secondary",
        40.0,
        Some(reset_at),
        Some(10_080),
        now() - 600,
    )
    .await
    .unwrap();
    repo.insert_usage_window(
        "acct-1",
        "secondary",
        50.0,
        Some(reset_at),
        Some(10_080),
        now(),
    )
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
        enforce_client_keys: false,
        codex_executor: Arc::new(CodexExecutor::new().unwrap()) as Arc<dyn Executor>,
        control_client: polyflare_codex::build_client().expect("build control_client"),
        anthropic_executor: Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap())
            as Arc<dyn Executor>,
        selector: Arc::new(polyflare_core::CapacityWeighted),
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
        runtime_settings: Arc::new(RuntimeSettings::new_from_fields(RuntimeSettingsFields {
            max_account_attempts: 3,
            starvation_wait_budget: std::time::Duration::from_secs(60),
            starvation_heartbeat: std::time::Duration::from_secs(10),
            wake_jitter_ms: 0,
            stream_idle_timeout: std::time::Duration::from_secs(300),
            inflight_penalty_pct: 2.5,
            soft_drain_enabled: true,
            request_log_retention_days: 0,
            usage_history_retention_days: 0,
            live_logs: true,
        })),
        ws_downstream: false,
        log_bus: polyflare_server::log_bus::LogBus::new(1000),
        failover_metrics: polyflare_server::observability::FailoverMetrics::new(),
        health_tier_metrics: polyflare_server::observability::HealthTierMetrics::new(),
        lease_metrics: polyflare_server::observability::LeaseMetrics::new(),
        upstream_request_metrics: polyflare_server::observability::UpstreamRequestMetrics::new(),
        rate_limit_metrics: polyflare_server::observability::RateLimitMetrics::new(),
        relay_metrics: polyflare_server::observability::RelayMetrics::new(),
        model_catalog: polyflare_server::model_catalog::floor_only_model_catalog(),
        starvation_metrics: polyflare_server::observability::StarvationMetrics::new(),
        runtime: Default::default(),
    });
    let app = build_app(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn pace_requires_admin_and_is_content_safe() {
    let pf = spawn(seed_store().await).await;
    let client = reqwest::Client::new();

    // (a) /api/pace requires admin: keyless -> 401.
    let no_tok = client.get(format!("{pf}/api/pace")).send().await.unwrap();
    assert_eq!(no_tok.status(), 401, "keyless /api/pace must be rejected");

    // (b) With the admin bearer -> 200, and a REAL (non-null) report: `pace` must be an object
    // (not null) carrying a numeric `total_full_credits` and a `status` — this is the "green but
    // vacuous" guard: if the seeded usage didn't make the report eligible, `pace` would be null
    // and the content-safety assertion below would prove nothing.
    let resp = client
        .get(format!("{pf}/api/pace"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let text = resp.text().await.unwrap();
    let body: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert!(
        !body["pace"].is_null(),
        "pace must be a REAL report (non-null) for this assertion to have teeth: {body}"
    );
    assert!(
        body["pace"]["total_full_credits"].is_number(),
        "pace.total_full_credits must be numeric: {body}"
    );
    assert!(
        body["pace"]["status"].is_string(),
        "pace.status must be a string enum: {body}"
    );

    // (c) content-safety: the sentinel email/token must never appear in /api/pace's body.
    assert!(
        !text.contains(SENTINEL_EMAIL),
        "/api/pace leaked the account email: {text}"
    );
    assert!(
        !text.contains(SENTINEL_TOKEN),
        "/api/pace leaked the account token: {text}"
    );

    // (d) the trends endpoint's new `forecast` field: present, non-null (same "has teeth"
    // requirement), and content-safe.
    let trends_resp = client
        .get(format!("{pf}/api/accounts/acct-1/trends"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(trends_resp.status(), 200);
    let trends_text = trends_resp.text().await.unwrap();
    let trends_body: serde_json::Value = serde_json::from_str(&trends_text).unwrap();
    assert!(
        trends_body.get("forecast").is_some(),
        "trends body must carry a forecast field: {trends_body}"
    );
    assert!(
        !trends_body["forecast"].is_null(),
        "forecast must be a REAL forecast (non-null) for this assertion to have teeth: {trends_body}"
    );
    assert!(
        !trends_text.contains(SENTINEL_EMAIL),
        "/api/accounts/{{id}}/trends leaked the account email: {trends_text}"
    );
    assert!(
        !trends_text.contains(SENTINEL_TOKEN),
        "/api/accounts/{{id}}/trends leaked the account token: {trends_text}"
    );
}
