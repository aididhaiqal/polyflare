//! Regression: the raised 100 MB body limit holds through the store-backed serve path.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{CapacityWeighted, Continuity};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_server::runtime_settings::{RuntimeSettings, RuntimeSettingsFields};
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};
use polyflare_testkit::MockUpstream;
use std::time::Duration;

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn store_account(id: &str) -> Account {
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

#[tokio::test]
async fn large_request_body_is_not_rejected_with_413() {
    let mock = MockUpstream::new(vec![r#"{"type":"response.completed"}"#.to_string()])
        .with_response_header("x-request-id", "upstream-large-body")
        .with_response_header("x-codex-turn-state", "turn-large-body");
    let handle = mock.clone();
    let upstream = mock.spawn().await;

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[6u8; 32]).unwrap();
    store
        .accounts()
        .insert(
            &store_account("large-body"),
            &PlainTokens {
                access_token: "tok".to_string(),
                refresh_token: "r".to_string(),
                id_token: "i".to_string(),
            },
            &cipher,
        )
        .await
        .unwrap();
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_secs(30),
    ));
    std::mem::forget(dir);

    let state = Arc::new(AppState {
        enforce_client_keys: false,
        codex_executor: Arc::new(CodexExecutor::new().unwrap()),
        control_client: polyflare_codex::build_client().expect("build control_client"),
        anthropic_executor: Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap()),
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
            live_logs: false,
        })),
        ws_downstream: false,
        ws_relay_idle: polyflare_server::ws_relay::WsRelayIdlePolicy::default(),
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
    let app = build_app(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let payload = serde_json::json!({
        "model": "gpt-5.6-sol",
        "input": "x".repeat(2_500_000),
    });
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/responses"))
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "large body must not be rejected with 413"
    );
    assert_eq!(
        resp.headers()
            .get("x-request-id")
            .and_then(|value| value.to_str().ok()),
        Some("upstream-large-body")
    );
    assert_eq!(
        resp.headers()
            .get("x-codex-turn-state")
            .and_then(|value| value.to_str().ok()),
        Some("turn-large-body")
    );

    let last_body = handle.last_body().unwrap();
    assert_eq!(last_body["model"], "gpt-5.6-sol");
    assert_eq!(last_body["input"].as_str().unwrap().len(), 2_500_000);

    // Current Codex enables request compression by default and ChatGPT-authenticated Responses
    // traffic uses zstd. PolyFlare must parse the decompressed request for routing, then forward
    // valid JSON bytes upstream without replaying the stale content-encoding/content-length.
    let compressed_payload = serde_json::json!({
        "model": "gpt-5.6-sol",
        "input": [{"role": "user", "content": "compressed"}],
    });
    let wire = serde_json::to_vec(&compressed_payload).unwrap();
    let compressed = zstd::stream::encode_all(std::io::Cursor::new(wire), 1).unwrap();
    let resp = client
        .post(format!("http://{addr}/responses"))
        .header("content-type", "application/json")
        .header("content-encoding", "zstd")
        .body(compressed)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "zstd Codex request must be accepted");
    assert_eq!(
        handle.last_body().unwrap(),
        compressed_payload,
        "the upstream receives the decoded JSON request"
    );
}
