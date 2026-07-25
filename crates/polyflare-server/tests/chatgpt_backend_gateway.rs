use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::ws::{Message as AxumMessage, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use polyflare_codex::oauth::OAuthClient;
use polyflare_core::{CapacityWeighted, Continuity};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_server::runtime_settings::{RuntimeSettings, RuntimeSettingsFields, SettingValue};
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
) -> Response {
    let path = uri.path().to_string();
    *capture.0.lock().unwrap() = Some(CapturedRequest {
        method,
        uri,
        headers,
        body,
    });
    if path == "/backend-api/wham/rate-limit-reset-credits" {
        return axum::Json(serde_json::json!({
            "available_count": 1,
            "credits": [{
                "id": "credit-upstream",
                "reset_type": "rate_limit_reset",
                "status": "available",
                "granted_at": "2026-07-01T00:00:00Z",
                "expires_at": "2026-08-01T00:00:00Z",
                "title": "Full reset",
                "description": "Reset active rate-limit windows"
            }]
        }))
        .into_response();
    }
    if path == "/backend-api/wham/rate-limit-reset-credits/consume" {
        return axum::Json(serde_json::json!({
            "code": "reset",
            "windows_reset": 2,
            "credit": {
                "id": "credit-upstream",
                "status": "redeemed",
                "redeemed_at": "2026-07-25T02:00:00Z"
            }
        }))
        .into_response();
    }
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
        .into_response()
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

#[derive(Clone, Default)]
struct WsCapture(Arc<Mutex<Vec<(HeaderMap, Uri)>>>);

async fn remote_control_ws_handler(
    State(capture): State<WsCapture>,
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    uri: Uri,
) -> impl IntoResponse {
    capture.0.lock().unwrap().push((headers, uri));
    ws.protocols(["codex-remote-v2"])
        .on_upgrade(remote_control_ws_echo)
}

async fn remote_control_ws_echo(mut socket: WebSocket) {
    if let Some(Ok(AxumMessage::Text(text))) = socket.recv().await {
        let _ = socket
            .send(AxumMessage::Text(format!("upstream:{text}").into()))
            .await;
    }
}

async fn spawn_ws_upstream() -> (String, WsCapture) {
    let capture = WsCapture::default();
    let app = Router::new()
        .route(
            "/backend-api/wham/remote/control/server",
            get(remote_control_ws_handler),
        )
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
    spawn_polyflare_with_enforcement(upstream_root, false).await
}

async fn spawn_polyflare_with_enforcement(
    upstream_root: &str,
    enforce_client_keys: bool,
) -> (String, Arc<AppState>) {
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
        enforce_client_keys,
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
    assert!(
        payload.get("additional_rate_limits").is_none(),
        "main-limit replacement must avoid presenting the same pool as both Codex and PolyFlare usage"
    );
    assert_eq!(payload["rate_limit_reset_credits"]["available_count"], 0);
    assert!(
        capture.0.lock().unwrap().is_none(),
        "synthetic usage must not contact the passthrough upstream"
    );

    let rows = request_rows(&state, 1).await;
    assert_eq!(rows[0].path, "chatgpt_backend_synthetic_wham/usage");
    assert_eq!(rows[0].provider, "chatgpt_backend");
    assert_eq!(rows[0].status, 200);
}

#[tokio::test]
async fn codex_api_path_style_reads_the_same_aggregate_usage() {
    let (upstream, capture) = spawn_upstream().await;
    let (base, state) = spawn_polyflare(&upstream).await;
    let reset_at = now() + 86_400;
    seed_quota(&state, "pro", "pro", 25.0, reset_at).await;

    let response = reqwest::Client::new()
        .get(format!("{base}/api/codex/usage"))
        .header("authorization", "Bearer local-codex-auth")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.unwrap();
    assert_eq!(payload["plan_type"], "pro");
    assert_eq!(
        payload["rate_limit"]["secondary_window"]["used_percent"],
        25
    );
    assert_eq!(
        payload["rate_limit"]["secondary_window"]["reset_at"],
        reset_at
    );
    assert!(
        capture.0.lock().unwrap().is_none(),
        "the Codex API path style must use the synthetic fleet meter"
    );
}

#[tokio::test]
async fn native_codex_can_list_and_consume_an_aggregated_reset_credit() {
    let (upstream, _capture) = spawn_upstream().await;
    let (base, state) = spawn_polyflare(&upstream).await;
    let reset_at = now() + 5 * 24 * 3_600;
    seed_quota(&state, "reset-account", "plus", 90.0, reset_at).await;
    sqlx::query("UPDATE accounts SET chatgpt_account_id = ? WHERE id = ?")
        .bind("workspace-reset")
        .bind("reset-account")
        .execute(state.store.pool())
        .await
        .unwrap();
    state
        .store
        .reset_credits()
        .replace_snapshot(
            "reset-account",
            1,
            now(),
            &[polyflare_store::ResetCredit {
                account_id: "reset-account".to_string(),
                credit_id: "credit-upstream".to_string(),
                reset_type: Some("rate_limit_reset".to_string()),
                status: Some("available".to_string()),
                granted_at: Some(now() - 100),
                expires_at: Some(now() + 7 * 24 * 3_600),
                title: Some("Full reset".to_string()),
                description: None,
                redeem_started_at: None,
                redeemed_at: None,
            }],
        )
        .await
        .unwrap();

    let client = reqwest::Client::new();
    let listed = client
        .get(format!("{base}/backend-api/wham/rate-limit-reset-credits"))
        .header("authorization", "Bearer local-client")
        .send()
        .await
        .unwrap();
    assert_eq!(listed.status(), 200);
    let listed: serde_json::Value = listed.json().await.unwrap();
    assert_eq!(listed["available_count"], 1);
    let opaque_id = listed["credits"][0]["id"].as_str().unwrap().to_string();
    assert_ne!(opaque_id, "credit-upstream");

    let usage = client
        .get(format!("{base}/backend-api/wham/usage"))
        .header("authorization", "Bearer local-client")
        .send()
        .await
        .unwrap();
    assert_eq!(usage.status(), 200);
    let usage: serde_json::Value = usage.json().await.unwrap();
    assert_eq!(usage["rate_limit_reset_credits"]["available_count"], 1);

    let consumed = client
        .post(format!(
            "{base}/backend-api/wham/rate-limit-reset-credits/consume"
        ))
        .header("authorization", "Bearer local-client")
        .json(&serde_json::json!({
            "redeem_request_id": "codex-request-1",
            "credit_id": opaque_id
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(consumed.status(), 200);
    assert_eq!(
        consumed.json::<serde_json::Value>().await.unwrap(),
        serde_json::json!({"code": "reset", "windows_reset": 2})
    );

    let ledger = state
        .store
        .reset_credits()
        .get_request("reset-account", "codex-request-1", now(), 86_400)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ledger.credit_id, "credit-upstream");
    assert_eq!(ledger.result_code.as_deref(), Some("reset"));
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
async fn remotely_enforced_wham_usage_requires_a_valid_polyflare_client_key() {
    let (upstream, _capture) = spawn_upstream().await;
    let (base, state) = spawn_polyflare_with_enforcement(&upstream, true).await;
    let reset_at = now() + 86_400;
    seed_quota(&state, "pro", "pro", 10.0, reset_at).await;

    let unknown = reqwest::Client::new()
        .get(format!("{base}/backend-api/wham/usage"))
        .header("authorization", "Bearer merely-non-empty")
        .send()
        .await
        .unwrap();
    assert_eq!(
        unknown.status(),
        StatusCode::UNAUTHORIZED,
        "remote posture must not expose aggregate capacity to an arbitrary bearer"
    );

    let key = polyflare_server::keys::create_key(&state.store, Some("usage-test"), now())
        .await
        .unwrap();
    let valid = reqwest::Client::new()
        .get(format!("{base}/backend-api/wham/usage"))
        .header("authorization", format!("Bearer {}", key.raw))
        .send()
        .await
        .unwrap();
    assert_eq!(valid.status(), StatusCode::OK);
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
    state
        .runtime_settings
        .set("wham_usage_replace_main_limit", SettingValue::Bool(false))
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
    assert_eq!(
        payload["additional_rate_limits"][0]["limit_name"],
        "PolyFlare work pool"
    );
    assert_eq!(
        payload["additional_rate_limits"][0]["metered_feature"],
        "polyflare_pool_work"
    );
    assert_eq!(
        payload["additional_rate_limits"][0]["rate_limit"],
        payload["rate_limit"]
    );
}

#[tokio::test]
async fn explicitly_disabled_backend_passthrough_does_not_contact_upstream() {
    let (upstream, capture) = spawn_upstream().await;
    let (base, state) = spawn_polyflare(&upstream).await;
    state
        .runtime_settings
        .set(
            "chatgpt_backend_passthrough_enabled",
            SettingValue::Bool(false),
        )
        .unwrap();
    let response = reqwest::Client::new()
        .patch(format!("{base}/backend-api/wham/settings/user"))
        .header("authorization", "Bearer must-not-leave-polyflare")
        .body("body-must-not-leave-polyflare")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let payload: serde_json::Value = response.json().await.unwrap();
    assert_eq!(payload["error"]["code"], "backend_passthrough_disabled");
    assert!(
        capture.0.lock().unwrap().is_none(),
        "disabled passthrough must not contact the upstream"
    );

    let rows = request_rows(&state, 1).await;
    assert_eq!(
        rows[0].path,
        "chatgpt_backend_passthrough_wham/settings/user"
    );
    assert_eq!(rows[0].provider, "chatgpt_backend");
    assert_eq!(rows[0].status, 404);
}

#[tokio::test]
async fn enabled_backend_route_is_transparently_forwarded_and_safely_observed() {
    const AUTH_SECRET: &str = "Bearer auth-secret-must-not-be-logged";
    const BODY_SECRET: &str = "body-secret-must-not-be-logged";
    const QUERY_SECRET: &str = "query-secret-must-not-be-logged";

    let (upstream, capture) = spawn_upstream().await;
    let (base, state) = spawn_polyflare(&upstream).await;
    state
        .runtime_settings
        .set(
            "chatgpt_backend_passthrough_enabled",
            SettingValue::Bool(true),
        )
        .unwrap();
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
    assert_eq!(rows[0].provider, "chatgpt_backend");
    assert_eq!(rows[0].account_id, None);
    let debug = format!("{rows:?}");
    for secret in [AUTH_SECRET, BODY_SECRET, QUERY_SECRET] {
        assert!(
            !debug.contains(secret),
            "request telemetry leaked a secret: {debug}"
        );
    }
}

#[tokio::test]
async fn remote_control_websocket_upgrades_relays_and_logs_as_backend_traffic() {
    const FRAME_SECRET: &str = "remote-frame-must-not-be-logged";
    const AUTH_SECRET: &str = "Bearer remote-token-must-not-be-logged";
    const QUERY_SECRET: &str = "remote-query-must-not-be-logged";

    let (upstream, capture) = spawn_ws_upstream().await;
    let (base, state) = spawn_polyflare(&upstream).await;
    for downstream_path in [
        "/backend-api/wham/remote/control/server",
        "/work/backend-api/wham/remote/control/server",
    ] {
        let ws_url = format!(
            "{}{downstream_path}?installation_id={QUERY_SECRET}",
            base.replacen("http://", "ws://", 1)
        );
        let mut request =
            tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request(ws_url)
                .unwrap();
        request
            .headers_mut()
            .insert("authorization", AUTH_SECRET.parse().unwrap());
        request
            .headers_mut()
            .insert("x-codex-protocol-version", "2".parse().unwrap());
        request
            .headers_mut()
            .insert("sec-websocket-protocol", "codex-remote-v2".parse().unwrap());

        let (mut socket, response) = tokio_tungstenite::connect_async(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
        assert_eq!(
            response.headers()["sec-websocket-protocol"],
            "codex-remote-v2"
        );
        socket
            .send(tokio_tungstenite::tungstenite::Message::Text(
                FRAME_SECRET.into(),
            ))
            .await
            .unwrap();
        let reply = socket.next().await.unwrap().unwrap();
        assert_eq!(
            reply.into_text().unwrap(),
            format!("upstream:{FRAME_SECRET}")
        );
        socket.close(None).await.unwrap();
    }

    let captures = capture.0.lock().unwrap().clone();
    assert_eq!(captures.len(), 2);
    let expected_query = format!("installation_id={QUERY_SECRET}");
    for (headers, uri) in captures {
        assert_eq!(headers["authorization"], AUTH_SECRET);
        assert_eq!(headers["x-codex-protocol-version"], "2");
        assert_eq!(headers["sec-websocket-protocol"], "codex-remote-v2");
        assert_eq!(uri.query(), Some(expected_query.as_str()));
    }

    let rows = request_rows(&state, 2).await;
    assert_eq!(rows.len(), 2);
    for row in &rows {
        assert_eq!(
            row.path,
            "chatgpt_backend_passthrough_wham/remote/control/server"
        );
        assert_eq!(row.provider, "chatgpt_backend");
        assert_eq!(row.status, 101);
        assert_eq!(row.transport.as_deref(), Some("ws"));
        assert_eq!(row.upstream_transport.as_deref(), Some("ws"));
    }
    let debug = format!("{rows:?}");
    assert!(!debug.contains(FRAME_SECRET));
    assert!(!debug.contains(AUTH_SECRET));
    assert!(!debug.contains(QUERY_SECRET));
}
