//! WS-downstream relay Task 2: `POLYFLARE_WS_DOWNSTREAM` (threaded as `AppState::ws_downstream`)
//! routes the codex CLI's WS-handshake `GET /responses` to the new `ws_relay` accept handler when
//! ON, and keeps today's `426 Upgrade Required` fallback (`ingress::websocket_fallback_handler`)
//! when OFF (the default). See `docs/superpowers/specs/2026-07-20-ws-downstream-relay-design.md` §8.
//!
//! WHY a REAL server + REAL WS client (not `oneshot`): axum's `WebSocketUpgrade` extractor pulls the
//! `hyper::upgrade::OnUpgrade` value out of the request extensions, which is only present on a live,
//! upgradable connection. A tower `oneshot` call has no such connection, so the extractor rejects
//! with `ConnectionNotUpgradable` → `426` — indistinguishable from the flag-OFF fallback. Driving a
//! real `tokio_tungstenite` client against a real `axum::serve` listener (the same harness the WS
//! examples use) is the only way to observe the `101` accept distinctly from the `426` fallback.

use std::sync::Arc;
use std::time::Duration;

use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{CapacityWeighted, Continuity, Executor};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_store::{Store, TokenCipher};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Error as WsError;

/// Spawn a PolyFlare instance with `ws_downstream` set as given, returning its `ws://addr` base.
/// The store is empty — both tests answer (accept or 426) at the WS-handshake before any account
/// selection, so no seeded account is needed.
async fn spawn(ws_downstream: bool) -> String {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    std::mem::forget(dir);

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
        upstream_base_url: "http://127.0.0.1:9".to_string(),
        anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),
        refresh_locks: Default::default(),
        capture_fingerprint_path: None,
        codex_version: std::sync::Arc::new(polyflare_codex::CodexVersionCache::new().unwrap()),
        account_cache: std::sync::Arc::new(polyflare_server::account_cache::AccountCache::new()),
        token_cache: Default::default(),
        admin_token: None,
        live_logs: false,
        ws_downstream,
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
    format!("ws://{addr}")
}

/// Attempt a WS handshake at `<base>/responses` and return the resulting HTTP status code: the
/// handshake's `101` on accept, or the non-101 status (e.g. `426`) the server answered with.
async fn ws_handshake_status(base: &str) -> u16 {
    match connect_async(format!("{base}/responses")).await {
        // Accept: `WebSocketUpgrade` completed the handshake with `101 Switching Protocols`. The
        // stub relay then immediately drops the socket, but the handshake status is already 101.
        Ok((_ws, resp)) => resp.status().as_u16(),
        // Non-101: tungstenite surfaces the server's HTTP response verbatim (this is how a `426`
        // fallback arrives at the client — the sole trigger codex-rs recognizes for WS→HTTP).
        Err(WsError::Http(resp)) => resp.status().as_u16(),
        Err(other) => panic!("unexpected WS handshake error (not an HTTP status): {other}"),
    }
}

/// Default OFF: the WS-handshake `GET /responses` still answers exactly `426`, byte-identical to
/// before this flag existed — codex-rs's sole WS→HTTP-SSE fallback trigger.
#[tokio::test]
async fn ws_get_responses_returns_426_when_downstream_flag_off() {
    let base = spawn(false).await;
    let status = ws_handshake_status(&base).await;
    assert_eq!(
        status, 426,
        "with POLYFLARE_WS_DOWNSTREAM off, a WS handshake on /responses must still get 426 \
         (the unchanged fallback), never an accepted upgrade"
    );
}

/// Flag ON: the same WS handshake is ACCEPTED — it routes to `ws_relay::responses_ws_handler`, which
/// completes the upgrade (`101 Switching Protocols`), NOT the `426` fallback. (The stub relay closes
/// immediately for now; Tasks 3-6 add the real pump.)
#[tokio::test]
async fn ws_get_responses_accepts_upgrade_when_flag_on() {
    let base = spawn(true).await;
    let status = ws_handshake_status(&base).await;
    assert_ne!(
        status, 426,
        "with POLYFLARE_WS_DOWNSTREAM on, the WS handshake must be routed to the relay accept \
         handler, not the 426 fallback"
    );
    assert_eq!(
        status, 101,
        "the relay handler must ACCEPT the WS upgrade with 101 Switching Protocols"
    );
}
