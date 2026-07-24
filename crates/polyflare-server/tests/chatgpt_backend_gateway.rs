use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::IntoResponse;
use axum::routing::any;
use axum::Router;
use polyflare_codex::oauth::OAuthClient;
use polyflare_core::{CapacityWeighted, Continuity};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_server::runtime_settings::{RuntimeSettings, RuntimeSettingsFields};
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

#[derive(Clone, Debug)]
struct CapturedRequest {
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
}

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Option<CapturedRequest>>>);

async fn upstream_handler(
    State(capture): State<Capture>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    *capture.0.lock().unwrap() = Some(CapturedRequest {
        method,
        uri,
        headers,
        body,
    });
    (
        StatusCode::MULTI_STATUS,
        [
            ("content-type", "application/octet-stream"),
            ("etag", "gateway-etag"),
            ("set-cookie", "cf_clearance=next; Secure; HttpOnly"),
            ("connection", "close"),
        ],
        "upstream-body",
    )
}

async fn spawn_upstream() -> (String, Capture) {
    let capture = Capture::default();
    let app = Router::new()
        .route("/backend-api/{*path}", any(upstream_handler))
        .with_state(capture.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), capture)
}

fn account(id: &str, plan_type: &str) -> Account {
    Account {
        id: id.to_string(),
        chatgpt_account_id: None,
        chatgpt_user_id: None,
        email: format!("{id}@example.test"),
        alias: None,
        workspace_id: None,
        workspace_label: None,
        seat_type: None,
        plan_type: plan_type.to_string(),
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

async fn spawn_polyflare(upstream_root: &str) -> (String, Arc<AppState>) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    std::mem::forget(dir);
    let cipher = TokenCipher::from_key_bytes(&[17u8; 32]).unwrap();
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_secs(30),
    ));
    let state = Arc::new(AppState {
        codex_executor: Arc::new(polyflare_codex::CodexExecutor::new().unwrap()),
        control_client: polyflare_codex::build_client().unwrap(),
        anthropic_executor: Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap()),
        selector: Arc::new(CapacityWeighted),
        pool_selectors: Default::default(),
        continuity,
        store,
        cipher,
        oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(),
        upstream_base_url: format!("{upstream_root}/backend-api/codex"),
        anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),
        refresh_locks: Default::default(),
        capture_fingerprint_path: None,
        codex_version: Arc::new(polyflare_codex::CodexVersionCache::new().unwrap()),
        account_cache: Arc::new(polyflare_server::account_cache::AccountCache::new()),
        token_cache: Default::default(),
        runtime: Default::default(),
        admin_token: None,
        runtime_settings: Arc::new(RuntimeSettings::new_from_fields(RuntimeSettingsFields {
            max_account_attempts: 3,
            starvation_wait_budget: Duration::from_secs(60),
            starvation_heartbeat: Duration::from_secs(10),
            wake_jitter_ms: 0,
            stream_idle_timeout: Duration::from_secs(300),
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
        starvation_metrics: polyflare_server::observability::StarvationMetrics::new(),
        lease_metrics: polyflare_server::observability::LeaseMetrics::new(),
        upstream_request_metrics: polyflare_server::observability::UpstreamRequestMetrics::new(),
        rate_limit_metrics: polyflare_server::observability::RateLimitMetrics::new(),
        relay_metrics: polyflare_server::observability::RelayMetrics::new(),
        model_catalog: polyflare_server::model_catalog::floor_only_model_catalog(),
        enforce_client_keys: false,
    });
    let app = build_app(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), state)
}

async fn seed_quota(state: &AppState, id: &str, plan_type: &str, used_percent: f64, reset_at: i64) {
    state
        .store
        .accounts()
        .insert(
            &account(id, plan_type),
            &PlainTokens {
                access_token: "unused-access".to_string(),
                refresh_token: "unused-refresh".to_string(),
                id_token: "unused-id".to_string(),
            },
            &state.cipher,
        )
        .await
        .unwrap();
    state
        .store
        .accounts()
        .insert_usage_window(
            id,
            "secondary",
            used_percent,
            Some(reset_at),
            Some(10_080),
            now(),
        )
        .await
        .unwrap();
}

async fn request_rows(state: &AppState, expected: usize) -> Vec<polyflare_store::RequestLogRow> {
    for _ in 0..50 {
        let rows = state.store.request_log().list(20, 0).await.unwrap();
        if rows.len() >= expected {
            return rows;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    state.store.request_log().list(20, 0).await.unwrap()
}

#[tokio::test]
async fn wham_usage_returns_capacity_weighted_pool_as_canonical_codex_limit() {
    let (upstream, capture) = spawn_upstream().await;
    let (base, state) = spawn_polyflare(&upstream).await;
    let reset_at = now() + 86_400;
    seed_quota(&state, "plus", "plus", 50.0, reset_at).await;
    seed_quota(&state, "pro", "pro", 10.0, reset_at).await;

    let response = reqwest::Client::new()
        .get(format!("{base}/backend-api/wham/usage"))
        .header("authorization", "Bearer local-codex-auth")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.unwrap();
    assert_eq!(payload["plan_type"], "pro");
    assert_eq!(payload["rate_limit"]["allowed"], true);
    assert_eq!(payload["rate_limit"]["limit_reached"], false);
    assert!(payload["rate_limit"].get("primary_window").is_none());
    assert_eq!(
        payload["rate_limit"]["secondary_window"]["used_percent"],
        15
    );
    assert_eq!(
        payload["rate_limit"]["secondary_window"]["limit_window_seconds"],
        604_800
    );
    assert_eq!(
        payload["rate_limit"]["secondary_window"]["reset_at"],
        reset_at
    );
    assert_eq!(payload["rate_limit_reset_credits"]["available_count"], 0);
    assert!(
        capture.0.lock().unwrap().is_none(),
        "synthetic usage must not contact the passthrough upstream"
    );

    let rows = request_rows(&state, 1).await;
    assert_eq!(rows[0].path, "chatgpt_backend_synthetic_wham/usage");
    assert_eq!(rows[0].status, 200);
}

#[tokio::test]
async fn wham_usage_requires_client_auth_and_fails_closed_without_fresh_pool_evidence() {
    let (upstream, capture) = spawn_upstream().await;
    let (base, state) = spawn_polyflare(&upstream).await;
    let client = reqwest::Client::new();

    let unauthenticated = client
        .get(format!("{base}/backend-api/wham/usage"))
        .send()
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);

    let unavailable = client
        .get(format!("{base}/backend-api/wham/usage"))
        .header("authorization", "Bearer local-codex-auth")
        .send()
        .await
        .unwrap();
    assert_eq!(unavailable.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        capture.0.lock().unwrap().is_none(),
        "usage failures must not silently fall through to one upstream account"
    );

    let rows = request_rows(&state, 2).await;
    assert_eq!(rows.len(), 2);
    assert!(rows
        .iter()
        .all(|row| row.path == "chatgpt_backend_synthetic_wham/usage"));
}

#[tokio::test]
async fn pool_scoped_usage_includes_only_members_of_the_named_pool() {
    let (upstream, _capture) = spawn_upstream().await;
    let (base, state) = spawn_polyflare(&upstream).await;
    let reset_at = now() + 86_400;
    seed_quota(&state, "work-account", "pro", 20.0, reset_at).await;
    seed_quota(&state, "other-account", "pro", 80.0, reset_at).await;
    state
        .store
        .accounts()
        .replace_pools("work-account", &["work".to_string()])
        .await
        .unwrap();
    state
        .store
        .accounts()
        .replace_pools("other-account", &["other".to_string()])
        .await
        .unwrap();

    let response = reqwest::Client::new()
        .get(format!("{base}/work/backend-api/wham/usage"))
        .header("authorization", "Bearer local-codex-auth")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.unwrap();
    assert_eq!(
        payload["rate_limit"]["secondary_window"]["used_percent"],
        20
    );
}

#[tokio::test]
async fn unmodified_backend_route_is_transparently_forwarded_and_safely_observed() {
    const AUTH_SECRET: &str = "Bearer auth-secret-must-not-be-logged";
    const BODY_SECRET: &str = "body-secret-must-not-be-logged";
    const QUERY_SECRET: &str = "query-secret-must-not-be-logged";

    let (upstream, capture) = spawn_upstream().await;
    let (base, state) = spawn_polyflare(&upstream).await;
    let response = reqwest::Client::new()
        .patch(format!(
            "{base}/backend-api/wham/settings/user?mode=fast&opaque={QUERY_SECRET}"
        ))
        .header("authorization", AUTH_SECRET)
        .header("chatgpt-account-id", "acct-client")
        .header("x-custom-client-header", "preserved")
        .body(BODY_SECRET)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::MULTI_STATUS);
    assert_eq!(response.headers()["etag"], "gateway-etag");
    assert_eq!(
        response.headers()["set-cookie"],
        "cf_clearance=next; Secure; HttpOnly"
    );
    assert!(
        response.headers().get("connection").is_none(),
        "hop-by-hop response headers must not be relayed"
    );
    assert_eq!(response.bytes().await.unwrap(), "upstream-body");

    let request = capture.0.lock().unwrap().clone().expect("upstream request");
    let expected_query = format!("mode=fast&opaque={QUERY_SECRET}");
    assert_eq!(request.method, Method::PATCH);
    assert_eq!(request.uri.path(), "/backend-api/wham/settings/user");
    assert_eq!(request.uri.query(), Some(expected_query.as_str()));
    assert_eq!(request.headers["authorization"], AUTH_SECRET);
    assert_eq!(request.headers["chatgpt-account-id"], "acct-client");
    assert_eq!(request.headers["x-custom-client-header"], "preserved");
    assert_eq!(request.body, BODY_SECRET);

    let rows = request_rows(&state, 1).await;
    assert_eq!(
        rows[0].path,
        "chatgpt_backend_passthrough_wham/settings/user"
    );
    assert_eq!(rows[0].status, 207);
    assert_eq!(rows[0].account_id, None);
    let debug = format!("{rows:?}");
    for secret in [AUTH_SECRET, BODY_SECRET, QUERY_SECRET] {
        assert!(
            !debug.contains(secret),
            "request telemetry leaked a secret: {debug}"
        );
    }
}
