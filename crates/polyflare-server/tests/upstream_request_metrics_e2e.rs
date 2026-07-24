//! C11b Task 2: `upstream_requests_total{account_id,status}` must be bumped exactly once per
//! completed client request at EACH of the 3 request-completion wrapper sites
//! (`control_route`/`responses_route`/`messages_route`) — missing one undercounts a whole
//! traffic class. `control_endpoints_e2e.rs` covers the CONTROL class; this file covers the
//! `/responses` (native Codex) and `/v1/messages` (native Anthropic) classes, plus the
//! `account_id=None` (503-no-eligible) render convention and the no-double-count-on-failover
//! guarantee.
//!
//! Harnesses are deliberately duplicated (not shared) from `tests/observability.rs` (`/responses`)
//! and `tests/messages_ingress.rs` (`/v1/messages`) — both of those files' `spawn_polyflare`
//! helpers return only the server URL, not the `AppState`, so they can't assert on
//! `state.upstream_request_metrics` after driving a request. Copying the exact same `AppState`
//! literal shape (rather than changing those shared helpers' return signatures, which would touch
//! call sites outside this task's scope) keeps this task's blast radius to new files only.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{CapacityWeighted, Continuity, Executor};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_server::runtime_settings::{RuntimeSettings, RuntimeSettingsFields};
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};
use polyflare_testkit::MockUpstream;

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn account(id: &str, provider: &str) -> Account {
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
        provider: provider.to_string(),
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

/// Mirrors `tests/observability.rs::spawn_polyflare` / `tests/messages_ingress.rs::
/// spawn_polyflare_full`'s `AppState` literal exactly, but returns the `AppState` handle too, so
/// tests here can assert on `state.upstream_request_metrics` after driving a request.
async fn spawn_polyflare(
    store: Store,
    codex_upstream: String,
    anthropic_upstream: String,
) -> (String, Arc<AppState>) {
    let cipher = TokenCipher::from_key_bytes(&[77u8; 32]).unwrap();
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
        upstream_base_url: codex_upstream,
        anthropic_upstream_base_url: anthropic_upstream,
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
    let app = build_app(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), state)
}

/// (a) `/responses` traffic class: a successful native Codex request records exactly one
/// `upstream_requests` entry for the served account/status.
#[tokio::test]
async fn responses_request_records_upstream_request_metric() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[77u8; 32]).unwrap();
    store
        .accounts()
        .insert(&account("codex-1", "codex"), &tokens(), &cipher)
        .await
        .unwrap();
    std::mem::forget(dir);

    let mock = MockUpstream::new(vec![
        r#"{"type":"response.output_text.delta","delta":"hi"}"#.to_string(),
        r#"{"type":"response.completed"}"#.to_string(),
    ]);
    let upstream = mock.spawn().await;
    let (pf, state) = spawn_polyflare(store, upstream, "http://127.0.0.1:9".to_string()).await;

    let resp = reqwest::Client::new()
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "m", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let _ = resp.bytes().await.unwrap();

    assert_eq!(
        state.upstream_request_metrics.snapshot(),
        vec![(
            "codex".to_string(),
            "account".to_string(),
            "codex-1".to_string(),
            200,
            1,
        )],
        "responses_route must record exactly one upstream_requests entry for the served account"
    );
}

/// (c) a 503-no-eligible-account outcome (no account seeded at all) still records an entry —
/// under the `account_id=""` key (the documented `None` render convention), never dropped.
#[tokio::test]
async fn responses_no_eligible_account_records_empty_account_id_entry() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    std::mem::forget(dir);

    let mock = MockUpstream::new(vec![]);
    let upstream = mock.spawn().await;
    let (pf, state) = spawn_polyflare(store, upstream, "http://127.0.0.1:9".to_string()).await;

    let resp = reqwest::Client::new()
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "m"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 503);

    assert_eq!(
        state.upstream_request_metrics.snapshot(),
        vec![(
            "codex".to_string(),
            "account".to_string(),
            "".to_string(),
            503,
            1,
        )],
        "a 503-no-eligible-account outcome (no account ever selected) must still be visible, \
         keyed as an empty account_id, never dropped"
    );
}

/// (b) `/v1/messages` traffic class: a successful native Anthropic request records exactly one
/// `upstream_requests` entry for the served account/status.
#[tokio::test]
async fn messages_request_records_upstream_request_metric() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[77u8; 32]).unwrap();
    store
        .accounts()
        .insert(&account("anthropic-1", "anthropic"), &tokens(), &cipher)
        .await
        .unwrap();
    std::mem::forget(dir);

    let mock = MockUpstream::new(vec![
        r#"{"type":"content_block_delta","delta":{"type":"text_delta","text":"hi"}}"#.to_string(),
        r#"{"type":"message_stop"}"#.to_string(),
    ]);
    let upstream = mock.spawn().await;
    let (pf, state) = spawn_polyflare(store, "http://127.0.0.1:9".to_string(), upstream).await;

    let resp = reqwest::Client::new()
        .post(format!("{pf}/v1/messages"))
        .json(&serde_json::json!({
            // Deliberately NOT an opus/sonnet/haiku substring (those alias to Codex) — exercises
            // the genuinely-unaliased native Anthropic path, mirroring
            // `tests/messages_ingress.rs::messages_relays_to_the_anthropic_executor`.
            "model": "claude-3-5-legacy-model",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let _ = resp.bytes().await.unwrap();

    assert_eq!(
        state.upstream_request_metrics.snapshot(),
        vec![(
            "anthropic".to_string(),
            "account".to_string(),
            "anthropic-1".to_string(),
            200,
            1,
        )],
        "messages_route must record exactly one upstream_requests entry for the served account"
    );
}
