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
use polyflare_server::runtime_settings::{RuntimeSettings, RuntimeSettingsFields};
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
        ws_downstream,
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

/// Flag ON no longer means "blindly accept": the relay must resolve and dial upstream before it
/// commits the downstream protocol switch. With no eligible account, the client receives the
/// actionable capacity status instead of a misleading `101` followed by an unexplained close.
#[tokio::test]
async fn ws_get_responses_does_not_upgrade_before_upstream_is_ready() {
    let base = spawn(true).await;
    let status = ws_handshake_status(&base).await;
    assert_eq!(
        status, 503,
        "an empty fleet must fail before downstream upgrade, not return 101 and silently close"
    );
}

/// Task 6: the real bidirectional pump + content-free ownership sniff, exercised end to end through
/// a REAL downstream WS client and a REAL (mocked) upstream WS — no invented downstream-trait
/// abstraction, exactly as the task brief calls for.
mod relay_through {
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use axum::http::{HeaderName, HeaderValue};
    use futures_util::{SinkExt, StreamExt};
    use polyflare_codex::oauth::OAuthClient;
    use polyflare_codex::CodexExecutor;
    use polyflare_core::{Continuity, RoundRobin};
    use polyflare_server::app::{build_app, AppState};
    use polyflare_server::continuity::CodexContinuity;
    use polyflare_server::runtime_settings::{
        RuntimeSettings, RuntimeSettingsFields, SettingValue,
    };
    use polyflare_store::{
        Account as StoreAccount, NewCustomProvider, NewProviderModel, PlainTokens, Store,
        TokenCipher,
    };
    use polyflare_testkit::{MockOAuth, MockWsUpstream, ScriptedTurn};
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::Message as TMessage;

    fn now() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    /// Spawn a real PolyFlare instance with `ws_downstream` on, exactly ONE Codex account seeded
    /// (`id`), and `upstream_base_url` pointed at the mock upstream — so `resolve_owner`'s
    /// unpinned-selection path (Task 3) trivially picks this one account (`RoundRobin` over a
    /// single candidate), and `dial_owner_upstream` (Task 4) connects it to `mock_base`. Returns the
    /// downstream `ws://` base AND the `Arc<AppState>` (kept alive so the test can read
    /// `state.store.continuity()` afterward — `Store` isn't `Clone`, so the state handle is the only
    /// way back to it once `build_app` has consumed a clone of the `Arc`).
    async fn spawn_with_pinned_account(id: &str, mock_base: &str) -> (String, Arc<AppState>) {
        spawn_with_pinned_account_and_oauth(id, mock_base, "http://127.0.0.1:9").await
    }

    async fn spawn_with_pinned_account_and_oauth(
        id: &str,
        mock_base: &str,
        oauth_url: &str,
    ) -> (String, Arc<AppState>) {
        spawn_with_pinned_account_full(
            id,
            mock_base,
            oauth_url,
            polyflare_server::ws_relay::WsRelayIdlePolicy::default(),
        )
        .await
    }

    /// [`spawn_with_pinned_account`] with an explicit between-turns idle policy, for the
    /// honest-liveness tests (short ping cadences / budgets instead of the production 30s/1500s).
    async fn spawn_with_pinned_account_and_idle(
        id: &str,
        mock_base: &str,
        idle: polyflare_server::ws_relay::WsRelayIdlePolicy,
    ) -> (String, Arc<AppState>) {
        spawn_with_pinned_account_full(id, mock_base, "http://127.0.0.1:9", idle).await
    }

    async fn spawn_with_pinned_account_full(
        id: &str,
        mock_base: &str,
        oauth_url: &str,
        ws_relay_idle: polyflare_server::ws_relay::WsRelayIdlePolicy,
    ) -> (String, Arc<AppState>) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        std::mem::forget(dir);

        let cipher = TokenCipher::from_key_bytes(&[7u8; 32]).unwrap();
        store
            .accounts()
            .insert(
                &StoreAccount {
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
                    last_refresh: now(), // fresh: never triggers a live OAuth refresh in this test
                    created_at: now(),
                    status: "active".to_string(),
                    deactivation_reason: None,
                    reset_at: None,
                    blocked_at: None,
                    security_work_authorized: false,
                    provider: "codex".to_string(),
                    pool: None,
                },
                &PlainTokens {
                    access_token: "tok".into(),
                    refresh_token: "r".into(),
                    id_token: "i".into(),
                },
                &cipher,
            )
            .await
            .unwrap();
        store
            .accounts()
            .insert_usage_window(
                id,
                "secondary",
                40.0,
                Some(now() + 86_400),
                Some(10_080),
                now(),
            )
            .await
            .unwrap();

        let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
            store.continuity(),
            Duration::from_secs(30),
        ));
        let codex_executor: Arc<dyn polyflare_core::Executor> =
            Arc::new(CodexExecutor::new().unwrap());
        let anthropic_executor: Arc<dyn polyflare_core::Executor> =
            Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap());

        let state = Arc::new(AppState {
            enforce_client_keys: false,
            codex_executor,
            control_client: polyflare_codex::build_client().expect("build control_client"),
            anthropic_executor,
            // A single seeded account: RoundRobin's tiebreak is moot, it's the only candidate.
            selector: Arc::new(RoundRobin),
            pool_selectors: Default::default(),
            continuity,
            store,
            cipher,
            oauth: OAuthClient::new(oauth_url).unwrap(),
            // THE crux: dial_owner_upstream builds the upstream WS URL from this field for the
            // Codex provider — pointing it at the mock is what makes the relay dial the mock.
            upstream_base_url: mock_base.to_string(),
            anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),
            refresh_locks: Default::default(),
            capture_fingerprint_path: None,
            codex_version: Arc::new(polyflare_codex::CodexVersionCache::new().unwrap()),
            account_cache: Arc::new(polyflare_server::account_cache::AccountCache::new()),
            token_cache: Default::default(),
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
            ws_downstream: true,
            ws_relay_idle,
            log_bus: polyflare_server::log_bus::LogBus::new(1000),
            failover_metrics: polyflare_server::observability::FailoverMetrics::new(),
            health_tier_metrics: polyflare_server::observability::HealthTierMetrics::new(),
            starvation_metrics: polyflare_server::observability::StarvationMetrics::new(),
            runtime: Default::default(),
            lease_metrics: polyflare_server::observability::LeaseMetrics::new(),
            upstream_request_metrics: polyflare_server::observability::UpstreamRequestMetrics::new(
            ),
            rate_limit_metrics: polyflare_server::observability::RateLimitMetrics::new(),
            relay_metrics: polyflare_server::observability::RelayMetrics::new(),
            model_catalog: polyflare_server::model_catalog::floor_only_model_catalog(),
        });

        let app = build_app(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("ws://{addr}"), state)
    }

    async fn spawn_handshake_upstream(
        accepted_bearer: &'static str,
    ) -> (String, Arc<std::sync::Mutex<Vec<String>>>) {
        use axum::extract::{State, WebSocketUpgrade};
        use axum::http::{HeaderMap, StatusCode};
        use axum::response::{IntoResponse, Response};
        use axum::routing::get;
        use axum::Router;

        async fn handshake(
            State((accepted_bearer, seen)): State<(
                &'static str,
                Arc<std::sync::Mutex<Vec<String>>>,
            )>,
            headers: HeaderMap,
            ws: WebSocketUpgrade,
        ) -> Response {
            let bearer = headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_string();
            seen.lock().unwrap().push(bearer.clone());
            if bearer != format!("Bearer {accepted_bearer}") {
                return (StatusCode::UNAUTHORIZED, "stale bearer").into_response();
            }

            let mut response =
                ws.on_upgrade(|mut socket| async move { while socket.recv().await.is_some() {} });
            for (name, value) in [
                ("x-codex-turn-state", "turn-state-1"),
                ("x-models-etag", "models-etag-1"),
                ("x-reasoning-included", "true"),
                ("openai-model", "gpt-5.6-sol"),
            ] {
                response.headers_mut().insert(
                    HeaderName::from_bytes(name.as_bytes()).unwrap(),
                    HeaderValue::from_static(value),
                );
            }
            response
        }

        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let app = Router::new()
            .route("/responses", get(handshake))
            .with_state((accepted_bearer, seen.clone()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("ws://{addr}"), seen)
    }

    async fn spawn_rejecting_ws_upstream(status: axum::http::StatusCode) -> String {
        let app = axum::Router::new().route(
            "/responses",
            axum::routing::get(move || async move { status }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("ws://{addr}")
    }

    #[tokio::test]
    async fn rate_limit_event_becomes_selected_and_aggregate_pool_meters() {
        let rate_limits = serde_json::json!({
            "type": "codex.rate_limits",
            "plan_type": "pro",
            "rate_limits": {
                "primary": {
                    "used_percent": 70.0,
                    "window_minutes": 300,
                    "reset_at": now() + 3_600
                },
                "secondary": {
                    "used_percent": 80.0,
                    "window_minutes": 10080,
                    "reset_at": now() + 86_400
                }
            }
        })
        .to_string();
        let mock = MockWsUpstream::new(ScriptedTurn::normal(vec![rate_limits]));
        let upstream = mock.spawn().await;
        let (base, _state) = spawn_with_pinned_account("quota-account", &upstream).await;

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect("downstream WS handshake");
        ws.send(TMessage::Text(
            serde_json::json!({
                "type": "response.create",
                "model": "gpt-5.6-sol",
                "input": [{"role": "user", "content": "hello"}]
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();

        let mut meters = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let TMessage::Text(text) = ws.next().await.unwrap().unwrap() else {
                    continue;
                };
                let value: serde_json::Value = serde_json::from_str(&text).unwrap();
                if value["type"] == "codex.rate_limits" {
                    meters.push(value);
                } else if value["type"] == "response.completed" {
                    break;
                }
            }
        })
        .await
        .expect("completed turn");

        assert_eq!(meters.len(), 2);
        assert_eq!(meters[0]["metered_limit_name"], "polyflare_selected");
        assert_eq!(meters[0]["rate_limits"]["secondary"]["used_percent"], 80.0);
        assert_eq!(meters[1]["metered_limit_name"], "codex");
        assert_eq!(meters[1]["rate_limits"]["secondary"]["used_percent"], 40.0);
        assert!(
            meters[1]["rate_limits"].get("primary").is_none(),
            "aggregate 5h remains absent without fresh fleet-wide evidence"
        );
    }

    #[tokio::test]
    async fn custom_model_ws_frame_bridges_to_http_sse_provider() {
        use axum::body::Bytes;
        use axum::extract::State;
        use axum::routing::post;
        use axum::Router;
        use std::sync::Mutex;

        async fn custom_response(
            State(seen): State<Arc<Mutex<Vec<serde_json::Value>>>>,
            body: Bytes,
        ) -> (
            axum::http::StatusCode,
            [(&'static str, &'static str); 1],
            &'static str,
        ) {
            seen.lock()
                .unwrap()
                .push(serde_json::from_slice(&body).unwrap());
            (
                axum::http::StatusCode::OK,
                [("content-type", "text/event-stream")],
                concat!(
                    "event: response.completed\n",
                    "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"provider-id\",",
                    "\"usage\":{\"input_tokens\":3,\"output_tokens\":1}}}\n\n"
                ),
            )
        }

        let seen = Arc::new(Mutex::new(Vec::new()));
        let custom_app = Router::new()
            .route("/v1/responses", post(custom_response))
            .with_state(seen.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let custom_addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, custom_app).await.unwrap();
        });

        let codex_upstream = MockWsUpstream::new(ScriptedTurn::normal(Vec::new()));
        let codex_base = codex_upstream.spawn().await;
        let (base, state) = spawn_with_pinned_account("acct-custom-bridge", &codex_base).await;
        let timestamp = now();
        state
            .store
            .providers()
            .create_provider(&NewCustomProvider {
                id: "provider-custom-ws".into(),
                slug: "custom-ws".into(),
                display_name: "Custom WS bridge".into(),
                base_url: format!("http://{custom_addr}/v1"),
                wire_api: "responses".into(),
                enabled: true,
                stateless_responses: true,
                allow_private_hosts: true,
                connect_timeout_ms: 1_000,
                stream_idle_timeout_ms: 10_000,
                request_max_retries: 0,
                max_concurrency: Some(2),
                created_at: timestamp,
            })
            .await
            .unwrap();
        state
            .store
            .providers()
            .create_credential(
                "credential-custom-ws",
                "provider-custom-ws",
                "primary",
                "secret",
                1.0,
                Some(2),
                timestamp,
                &state.cipher,
            )
            .await
            .unwrap();
        state
            .store
            .providers()
            .create_model(&NewProviderModel {
                id: "model-custom-ws".into(),
                provider_id: "provider-custom-ws".into(),
                public_model: "fugu-ultra".into(),
                upstream_model: "fugu-ultra-v1".into(),
                display_name: "Fugu Ultra".into(),
                context_window: Some(1_000_000),
                max_output_tokens: None,
                supports_tools: true,
                supports_vision: true,
                supports_parallel_tool_calls: true,
                supports_web_search: true,
                supports_reasoning_summaries: true,
                reasoning_levels_json: r#"["high"]"#.into(),
                model_info_json: None,
                instruction_mode: "none".into(),
                instruction_text: String::new(),
                request_overrides_json: "{}".into(),
                input_per_million: Some(1.0),
                cached_input_per_million: Some(0.5),
                output_per_million: Some(4.0),
                visible_in_codex: true,
                visible_in_openai: true,
                enabled: true,
                created_at: timestamp,
            })
            .await
            .unwrap();

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect("downstream WS handshake");
        ws.send(TMessage::Text(
            serde_json::json!({
                "type": "response.create",
                "model": "fugu-ultra",
                "input": [{"role": "user", "content": "hello"}],
                "previous_response_id": "stale-custom-anchor"
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();

        let terminal = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let TMessage::Text(text) = ws.next().await.unwrap().unwrap() else {
                    continue;
                };
                let value: serde_json::Value = serde_json::from_str(&text).unwrap();
                if value["type"] == "response.completed" {
                    break value;
                }
            }
        })
        .await
        .expect("custom provider must produce a WS terminal");
        assert_eq!(
            terminal["response"]["id"], "",
            "stateless provider must force the next Codex WS request to carry full history"
        );
        {
            let captured = seen.lock().unwrap();
            assert_eq!(captured.len(), 1);
            assert_eq!(captured[0]["model"], "fugu-ultra-v1");
            assert!(captured[0].get("type").is_none());
            assert!(captured[0].get("previous_response_id").is_none());
        }
        state.store.flush_background_writes().await.unwrap();
        let row = state
            .store
            .request_log()
            .list(10, 0)
            .await
            .unwrap()
            .into_iter()
            .find(|row| row.provider == "custom-ws")
            .expect("custom WS request log row");
        assert_eq!(row.target_kind.as_deref(), Some("credential"));
        assert_eq!(
            row.provider_credential_id.as_deref(),
            Some("credential-custom-ws")
        );
        assert_eq!(row.transport.as_deref(), Some("websocket"));
        assert_eq!(row.upstream_transport.as_deref(), Some("http_sse"));
    }

    #[tokio::test]
    async fn custom_model_lookup_failure_never_forwards_frame_to_codex_ws() {
        let codex_upstream = MockWsUpstream::new(ScriptedTurn::normal(Vec::new()));
        let codex_base = codex_upstream.clone().spawn().await;
        let (base, state) =
            spawn_with_pinned_account("acct-custom-lookup-failure", &codex_base).await;

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect("downstream WS handshake");
        sqlx::query("DROP TABLE provider_models")
            .execute(state.store.pool())
            .await
            .unwrap();

        ws.send(TMessage::Text(
            serde_json::json!({
                "type": "response.create",
                "model": "fugu-ultra",
                "input": []
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();

        let reply = tokio::time::timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("lookup failure must answer instead of reaching Codex")
            .expect("an error frame")
            .expect("no websocket error");
        let TMessage::Text(reply) = reply else {
            panic!("expected a text error frame");
        };
        let value: serde_json::Value = serde_json::from_str(&reply).unwrap();
        assert_eq!(value["type"], "error");
        assert_eq!(value["status"], 500);
        assert_eq!(value["error"]["code"], "custom_provider_error");
        assert!(
            codex_upstream.frames().is_empty(),
            "a provider-catalog failure must fail closed, never forward a possible custom slug to Codex"
        );
    }

    #[tokio::test]
    async fn cold_root_ws_strips_account_native_etag_but_forwards_other_upgrade_metadata() {
        let (upstream, _) = spawn_handshake_upstream("tok").await;
        let (base, _) = spawn_with_pinned_account("acct-metadata", &upstream).await;

        let (_ws, response) = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect("downstream WS handshake");
        for (name, expected) in [
            ("x-codex-turn-state", "turn-state-1"),
            ("x-reasoning-included", "true"),
            ("openai-model", "gpt-5.6-sol"),
        ] {
            assert_eq!(
                response.headers().get(name).and_then(|v| v.to_str().ok()),
                Some(expected),
                "{name} must survive the upstream-to-downstream upgrade boundary"
            );
        }
        assert!(
            response.headers().get("x-models-etag").is_none(),
            "a cold root fleet must never expose the selected account's native models ETag"
        );
    }

    #[tokio::test]
    async fn upstream_426_reaches_codex_as_the_http_fallback_signal() {
        let upstream = MockWsUpstream::rejecting_handshake().spawn().await;
        let (base, _) = spawn_with_pinned_account("acct-fallback", &upstream).await;

        let error = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect_err("upstream 426 must reject the downstream upgrade");
        let tokio_tungstenite::tungstenite::Error::Http(response) = error else {
            panic!("expected an HTTP handshake rejection");
        };
        assert_eq!(
            response.status().as_u16(),
            426,
            "426 is Codex's only WebSocket-to-HTTP fallback trigger and must survive the relay"
        );
    }

    #[tokio::test]
    async fn initial_upstream_failure_updates_account_health_before_rejecting_upgrade() {
        let upstream =
            spawn_rejecting_ws_upstream(axum::http::StatusCode::SERVICE_UNAVAILABLE).await;
        let (base, state) = spawn_with_pinned_account("acct-initial-failure", &upstream).await;

        let error = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect_err("upstream 503 must reject the downstream upgrade");
        let tokio_tungstenite::tungstenite::Error::Http(response) = error else {
            panic!("expected an HTTP handshake rejection");
        };
        assert_eq!(response.status().as_u16(), 503);

        let mut snapshots = vec![polyflare_core::AccountSnapshot::new("acct-initial-failure")];
        state.runtime.overlay(&mut snapshots, now());
        assert_eq!(
            snapshots[0].error_count, 1,
            "an initial upstream handshake failure must feed the shared routing-health state"
        );
    }

    #[tokio::test]
    async fn initial_ws_401_refreshes_once_and_upgrades_with_the_same_account() {
        const VALID_JWT: &str = "eyJhbGciOiJub25lIn0.e30.sig";
        let (upstream, seen) = spawn_handshake_upstream(VALID_JWT).await;
        let oauth = MockOAuth::ok(VALID_JWT, "rotated-refresh", VALID_JWT);
        let oauth_handle = oauth.clone();
        let oauth_url = oauth.spawn().await;
        let (base, state) =
            spawn_with_pinned_account_and_oauth("acct-ws-auth", &upstream, &oauth_url).await;

        let (_ws, response) = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect("the refreshed same-account handshake must upgrade");
        assert_eq!(response.status().as_u16(), 101);
        assert_eq!(oauth_handle.hit_count(), 1, "exactly one reactive refresh");
        assert_eq!(
            *seen.lock().unwrap(),
            vec!["Bearer tok".to_string(), format!("Bearer {VALID_JWT}")],
            "the retry must use the refreshed bearer on the same selected account"
        );
        let (_, tokens) = state
            .store
            .accounts()
            .get_with_tokens("acct-ws-auth", &state.cipher)
            .await
            .unwrap()
            .expect("stored account");
        assert_eq!(tokens.access_token, VALID_JWT);
    }

    #[tokio::test]
    async fn concurrent_initial_ws_401s_collapse_to_one_refresh() {
        const VALID_JWT: &str = "eyJhbGciOiJub25lIn0.e30.sig";
        let (upstream, seen) = spawn_handshake_upstream(VALID_JWT).await;
        let oauth = MockOAuth::ok(VALID_JWT, "rotated-refresh", VALID_JWT);
        let oauth_handle = oauth.clone();
        let oauth_url = oauth.spawn().await;
        let (base, _) =
            spawn_with_pinned_account_and_oauth("acct-ws-auth-race", &upstream, &oauth_url).await;

        let first = tokio_tungstenite::connect_async(format!("{base}/responses"));
        let second = tokio_tungstenite::connect_async(format!("{base}/responses"));
        let (first, second) = tokio::join!(first, second);
        let (_first_ws, first_response) = first.expect("first refreshed handshake");
        let (_second_ws, second_response) = second.expect("second refreshed handshake");

        assert_eq!(first_response.status().as_u16(), 101);
        assert_eq!(second_response.status().as_u16(), 101);
        assert_eq!(
            oauth_handle.hit_count(),
            1,
            "the per-account refresh lock must collapse concurrent WS 401 recovery"
        );
        let seen = seen.lock().unwrap().clone();
        assert_eq!(
            seen.iter()
                .filter(|authorization| authorization.as_str() == "Bearer tok")
                .count(),
            2,
            "both concurrent handshakes must first observe the stale bearer"
        );
        assert_eq!(
            seen.iter()
                .filter(|authorization| { authorization.as_str() == format!("Bearer {VALID_JWT}") })
                .count(),
            2,
            "both retries must adopt the one refreshed bearer"
        );
    }

    #[tokio::test]
    async fn established_ws_401_before_output_refreshes_and_replays_same_account_once() {
        const VALID_JWT: &str = "eyJhbGciOiJub25lIn0.e30.sig";
        let mock = MockWsUpstream::scripted(vec![
            ScriptedTurn::ErrorEnvelope {
                status: 401,
                code: "unauthorized".to_string(),
                message: "expired".to_string(),
                error_extra: Vec::new(),
                headers: Vec::new(),
            },
            ScriptedTurn::normal(Vec::new()),
        ])
        .capturing_raw_frames();
        let upstream = mock.clone().spawn().await;
        let oauth = MockOAuth::ok(VALID_JWT, "rotated-refresh", VALID_JWT);
        let oauth_handle = oauth.clone();
        let oauth_url = oauth.spawn().await;
        let (base, state) =
            spawn_with_pinned_account_and_oauth("acct-ws-turn-auth", &upstream, &oauth_url).await;
        state
            .runtime_settings
            .set("max_account_attempts", SettingValue::U64(1))
            .unwrap();

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect("downstream WS handshake");
        let frame = r#"{"type":"response.create","input":[],"client_metadata":{"turn_id":"turn-auth-retry"}}"#.to_string();
        ws.send(TMessage::Text(frame.clone().into())).await.unwrap();

        let TMessage::Text(reply) = ws.next().await.expect("completion").expect("no WS error")
        else {
            panic!("expected a completed response after reactive auth");
        };
        let reply: serde_json::Value = serde_json::from_str(&reply).unwrap();
        assert_eq!(
            reply["type"], "response.completed",
            "the pre-output 401 must remain hidden when same-account refresh succeeds"
        );
        assert_eq!(oauth_handle.hit_count(), 1);
        assert_eq!(mock.raw_frames(), vec![frame.clone(), frame]);
        assert_eq!(
            mock.handshake_authorizations(),
            vec![
                Some("Bearer tok".to_string()),
                Some(format!("Bearer {VALID_JWT}"))
            ]
        );
    }

    #[tokio::test]
    async fn established_ws_401_after_visible_output_is_not_retried() {
        const VALID_JWT: &str = "eyJhbGciOiJub25lIn0.e30.sig";
        let visible =
            serde_json::json!({"type":"response.output_text.delta","delta":"visible"}).to_string();
        let mock = MockWsUpstream::new(ScriptedTurn::ErrorAfterEvents {
            events: vec![visible],
            status: 401,
            code: "unauthorized".to_string(),
            message: "expired".to_string(),
        })
        .capturing_raw_frames();
        let upstream = mock.clone().spawn().await;
        let oauth = MockOAuth::ok(VALID_JWT, "rotated-refresh", VALID_JWT);
        let oauth_handle = oauth.clone();
        let oauth_url = oauth.spawn().await;
        let (base, _) =
            spawn_with_pinned_account_and_oauth("acct-ws-visible-auth", &upstream, &oauth_url)
                .await;

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect("downstream WS handshake");
        ws.send(TMessage::Text(
            r#"{"type":"response.create","input":[]}"#.into(),
        ))
        .await
        .unwrap();

        let first = ws
            .next()
            .await
            .expect("visible event")
            .expect("no WS error");
        assert!(matches!(first, TMessage::Text(_)));
        let TMessage::Text(error) = ws.next().await.expect("401 error").expect("no WS error")
        else {
            panic!("expected the upstream 401 after visible output");
        };
        let error: serde_json::Value = serde_json::from_str(&error).unwrap();
        assert_eq!(error["status"], 401);
        assert_eq!(
            oauth_handle.hit_count(),
            0,
            "retry is forbidden after any client-visible upstream event"
        );
        assert_eq!(mock.raw_frames().len(), 1, "the turn must not be replayed");
        assert_eq!(mock.handshake_count(), 1);
    }

    /// Phase 3, Task 4 variant of [`spawn_with_pinned_account`]: seeds TWO active Codex accounts
    /// (`id_a`, `id_b`), both pointing at the SAME mock upstream (so a re-dial to either succeeds
    /// identically) and deliberately does NOT pre-pin either as an owner — the first turn's
    /// selection runs `resolve_owner`'s unpinned path (`RoundRobin` over the two, tying to the
    /// lexicographically smaller id), so the exhaustion-move test can observe a REAL re-select
    /// rather than asserting on a hardcoded pin.
    async fn spawn_with_two_accounts(
        id_a: &str,
        id_b: &str,
        mock_base: &str,
    ) -> (String, Arc<AppState>) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        std::mem::forget(dir);

        let cipher = TokenCipher::from_key_bytes(&[11u8; 32]).unwrap();
        for id in [id_a, id_b] {
            store
                .accounts()
                .insert(
                    &StoreAccount {
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
                        last_refresh: now(), // fresh: never triggers a live OAuth refresh
                        created_at: now(),
                        status: "active".to_string(),
                        deactivation_reason: None,
                        reset_at: None,
                        blocked_at: None,
                        security_work_authorized: false,
                        provider: "codex".to_string(),
                        pool: None,
                    },
                    &PlainTokens {
                        access_token: format!("tok-{id}"),
                        refresh_token: "r".into(),
                        id_token: "i".into(),
                    },
                    &cipher,
                )
                .await
                .unwrap();
        }

        let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
            store.continuity(),
            Duration::from_secs(30),
        ));
        let codex_executor: Arc<dyn polyflare_core::Executor> =
            Arc::new(CodexExecutor::new().unwrap());
        let anthropic_executor: Arc<dyn polyflare_core::Executor> =
            Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap());

        let state = Arc::new(AppState {
            enforce_client_keys: false,
            codex_executor,
            control_client: polyflare_codex::build_client().expect("build control_client"),
            anthropic_executor,
            // Two candidates: RoundRobin ties to the lexicographically smaller id — the test relies
            // on this to know which account gets picked (and thus benched) first.
            selector: Arc::new(RoundRobin),
            pool_selectors: Default::default(),
            continuity,
            store,
            cipher,
            oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(),
            // Both accounts' upstream dial lands on the SAME mock — the move is proven by WHICH
            // account gets benched + who owns the completed turn, not by which URL was dialed.
            upstream_base_url: mock_base.to_string(),
            anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),
            refresh_locks: Default::default(),
            capture_fingerprint_path: None,
            codex_version: Arc::new(polyflare_codex::CodexVersionCache::new().unwrap()),
            account_cache: Arc::new(polyflare_server::account_cache::AccountCache::new()),
            token_cache: Default::default(),
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
            ws_downstream: true,
            ws_relay_idle: polyflare_server::ws_relay::WsRelayIdlePolicy::default(),
            log_bus: polyflare_server::log_bus::LogBus::new(1000),
            failover_metrics: polyflare_server::observability::FailoverMetrics::new(),
            health_tier_metrics: polyflare_server::observability::HealthTierMetrics::new(),
            starvation_metrics: polyflare_server::observability::StarvationMetrics::new(),
            runtime: Default::default(),
            lease_metrics: polyflare_server::observability::LeaseMetrics::new(),
            upstream_request_metrics: polyflare_server::observability::UpstreamRequestMetrics::new(
            ),
            rate_limit_metrics: polyflare_server::observability::RateLimitMetrics::new(),
            relay_metrics: polyflare_server::observability::RelayMetrics::new(),
            model_catalog: polyflare_server::model_catalog::floor_only_model_catalog(),
        });

        let app = build_app(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("ws://{addr}"), state)
    }

    /// **The real proof.** A REAL `tokio-tungstenite` client connects to the downstream `/responses`
    /// WS; a `response.create` text frame reaches the `MockWsUpstream` BYTE-VERBATIM (unsorted keys +
    /// doubled interior whitespace survive — a serde reparse would destroy both); the mock's
    /// scripted `response.completed` reply reaches the client verbatim; and the sniffed completed id
    /// is recorded as owned by the account the relay actually dialed
    /// (`store.continuity().get_anchor_owner`) — proving the pump's `on_completed_id` callback is
    /// wired all the way to `Continuity::observe`, not just invoked in isolation.
    #[tokio::test]
    async fn forwards_verbatim_and_records_ownership_on_completion() {
        // The scripted turn streams ONE deliberately non-canonical event frame (keys out of
        // alphabetical order + doubled interior whitespace) before its auto `response.completed`.
        // A parse-then-reserialize on the backend->client leg would sort the keys and collapse the
        // whitespace, so a byte-exact match at the CLIENT proves that leg forwards verbatim too
        // (the client->backend leg is proven separately below via `mock.raw_frames()`).
        let weird_event =
            r#"{"type":"response.output_text.delta",  "z_field":1,  "a_field":2}"#.to_string();
        let mock = MockWsUpstream::new(ScriptedTurn::normal(vec![weird_event.clone()]))
            .capturing_raw_frames();
        let mock_base = mock.clone().spawn().await;

        let (base, state) = spawn_with_pinned_account("acct-relay", &mock_base).await;

        // A realistic downstream handshake: the three content-free identity headers the relay's
        // `session_key` derivation reads (Task 5) — not load-bearing for THIS test's assertions
        // (an unpinned conversation selects the sole account regardless), but present for realism.
        let mut request = format!("{base}/responses").into_client_request().unwrap();
        for (k, v) in [
            ("session-id", "s-relay"),
            ("thread-id", "t-relay"),
            ("x-codex-window-id", "w-relay:0"),
        ] {
            request.headers_mut().insert(
                HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        let (mut ws, _resp) = tokio_tungstenite::connect_async(request)
            .await
            .expect("downstream WS handshake must succeed");

        // Deliberately unsorted keys + doubled interior whitespace: a parse-then-reserialize forward
        // would sort keys and collapse the spaces, so an exact-bytes match at the mock proves the
        // relay's client->backend leg never reparsed the frame.
        let raw =
            r#"{"type":"response.create",  "z_before_a":1,  "a_after_z":2,"input":[]}"#.to_string();
        ws.send(TMessage::Text(raw.clone().into())).await.unwrap();

        // The mock records on its own server task; poll briefly for the frame to land.
        for _ in 0..50 {
            if !mock.raw_frames().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            mock.raw_frames(),
            vec![raw],
            "the relay must forward the client's frame to the upstream mock BYTE-VERBATIM"
        );

        // First frame back is the scripted non-canonical event, which the relay must forward to the
        // client BYTE-VERBATIM (a reparse would sort `a_field` before `z_field` and collapse the
        // doubled whitespace, changing these exact bytes) — the backend->client verbatim proof.
        let TMessage::Text(event) = ws.next().await.expect("an event").expect("no ws error") else {
            panic!("expected a text frame back from the relay");
        };
        assert_eq!(
            event.as_str(),
            weird_event,
            "the relay must forward the backend's frame to the client BYTE-VERBATIM"
        );

        // Then `ScriptedTurn::normal` auto-replies `response.completed` (id `resp_1`); the client
        // must receive it verbatim, and its id is what the ownership sniff records below.
        let TMessage::Text(reply) = ws.next().await.expect("a reply").expect("no ws error") else {
            panic!("expected a text frame back from the relay");
        };
        let v: serde_json::Value = serde_json::from_str(&reply).unwrap();
        assert_eq!(v["type"], "response.completed");
        assert_eq!(v["response"]["id"], "resp_1");

        // Ownership: the sniffed id must be anchored to the account the relay actually dialed. The
        // write happens async (`on_completed_id` awaited AFTER the client-facing send), so poll
        // briefly rather than asserting immediately.
        let mut owner = None;
        for _ in 0..50 {
            owner = state
                .store
                .continuity()
                .get_anchor_owner("resp_1")
                .await
                .unwrap();
            if owner.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            owner.as_deref(),
            Some("acct-relay"),
            "the completed response's ownership must be pinned to the account the relay dialed, \
             proving the pump's sniff -> Continuity::observe wiring actually ran"
        );
    }

    #[tokio::test]
    async fn completed_ws_turn_clears_account_errors_and_releases_its_lease() {
        let mock = MockWsUpstream::new(ScriptedTurn::normal(Vec::new()));
        let mock_base = mock.spawn().await;
        let (base, state) = spawn_with_pinned_account("acct-ws-success", &mock_base).await;
        let account_id = polyflare_core::AccountId::from("acct-ws-success");

        state.runtime.record_transient_error(&account_id, now());

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect("downstream WS handshake");
        ws.send(TMessage::Text(
            r#"{"type":"response.create","input":[]}"#.into(),
        ))
        .await
        .unwrap();

        while let Some(frame) = ws.next().await {
            let TMessage::Text(text) = frame.expect("no WS error") else {
                continue;
            };
            let value: serde_json::Value = serde_json::from_str(&text).unwrap();
            if value["type"] == "response.completed" {
                break;
            }
        }

        let mut snapshots = vec![polyflare_core::AccountSnapshot::new("acct-ws-success")];
        state.runtime.overlay(&mut snapshots, now());
        assert_eq!(
            snapshots[0].error_count, 0,
            "a protocol-completed WS turn must clear stale account errors just like HTTP"
        );
        assert_eq!(
            snapshots[0].in_flight, 0,
            "the per-turn WS lease must release at the terminal frame"
        );
        assert_eq!(state.lease_metrics.acquired(), 1);
        assert_eq!(state.lease_metrics.released(), 1);
    }

    #[tokio::test]
    async fn incomplete_ws_turn_is_terminal_neutral_and_releases_its_lease() {
        let mock = MockWsUpstream::new(ScriptedTurn::incomplete("max_output_tokens"));
        let mock_base = mock.spawn().await;
        let (base, state) = spawn_with_pinned_account("acct-ws-incomplete", &mock_base).await;
        let account_id = polyflare_core::AccountId::from("acct-ws-incomplete");
        state.runtime.record_transient_error(&account_id, now());

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect("downstream WS handshake");
        ws.send(TMessage::Text(
            r#"{"type":"response.create","input":[]}"#.into(),
        ))
        .await
        .unwrap();

        let TMessage::Text(text) = ws
            .next()
            .await
            .expect("incomplete terminal")
            .expect("no WS error")
        else {
            panic!("expected a text terminal");
        };
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["type"], "response.incomplete");

        let mut snapshots = vec![polyflare_core::AccountSnapshot::new("acct-ws-incomplete")];
        state.runtime.overlay(&mut snapshots, now());
        assert_eq!(
            snapshots[0].in_flight, 0,
            "response.incomplete must release the per-turn lease while the socket stays open"
        );
        assert_eq!(
            snapshots[0].error_count, 1,
            "an incomplete response is terminal but not evidence of account sickness or success"
        );
        assert_eq!(state.lease_metrics.acquired(), 1);
        assert_eq!(state.lease_metrics.released(), 1);

        state.store.flush_background_writes().await.unwrap();
        let rows = state.store.request_log().list(10, 0).await.unwrap();
        assert_eq!(
            rows.len(),
            1,
            "the incomplete turn must emit one request row"
        );
        assert_eq!(rows[0].status, 502);
        assert_eq!(
            rows[0].protocol_outcome.as_deref(),
            Some("incomplete"),
            "an HTTP-looking 502 must retain the more precise WS terminal classification"
        );
    }

    #[tokio::test]
    async fn active_ws_turn_holds_one_lease_until_client_teardown() {
        let mock = MockWsUpstream::new(ScriptedTurn::stall());
        let mock_base = mock.clone().spawn().await;
        let (base, state) = spawn_with_pinned_account("acct-ws-active", &mock_base).await;

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect("downstream WS handshake");
        let mut idle = vec![polyflare_core::AccountSnapshot::new("acct-ws-active")];
        state.runtime.overlay(&mut idle, now());
        assert!(
            idle[0].last_selected_at.is_some(),
            "the WS owner must be stamped at handshake selection, before any turn starts"
        );
        assert_eq!(
            idle[0].in_flight, 0,
            "an idle long-lived socket is not itself an in-flight request"
        );
        ws.send(TMessage::Text(
            r#"{"type":"response.create","input":[]}"#.into(),
        ))
        .await
        .unwrap();

        for _ in 0..50 {
            if !mock.frames().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let mut active = vec![polyflare_core::AccountSnapshot::new("acct-ws-active")];
        state.runtime.overlay(&mut active, now());
        assert_eq!(
            active[0].in_flight, 1,
            "one active generating WS turn must contribute one unit of selection pressure"
        );

        ws.close(None).await.unwrap();
        for _ in 0..50 {
            let mut released = vec![polyflare_core::AccountSnapshot::new("acct-ws-active")];
            state.runtime.overlay(&mut released, now());
            if released[0].in_flight == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let mut released = vec![polyflare_core::AccountSnapshot::new("acct-ws-active")];
        state.runtime.overlay(&mut released, now());
        assert_eq!(released[0].in_flight, 0);
        assert_eq!(
            released[0].error_count, 0,
            "a downstream client cancellation must release pressure without blaming the account"
        );
        assert_eq!(state.lease_metrics.acquired(), 1);
        assert_eq!(state.lease_metrics.released(), 1);
        state.store.flush_background_writes().await.unwrap();
        let rows = state.store.request_log().list(10, 0).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].protocol_outcome.as_deref(),
            Some("cancelled"),
            "downstream teardown must be classified as client cancellation, not provider failure"
        );
    }

    #[tokio::test]
    async fn capability_header_filters_initial_ws_owner() {
        let mock = MockWsUpstream::new(ScriptedTurn::normal(Vec::new()));
        let mock_base = mock.spawn().await;
        let (base, state) =
            spawn_with_two_accounts("acct-a-unauthorized", "acct-b-authorized", &mock_base).await;
        state
            .store
            .accounts()
            .update_security_work_authorized("acct-b-authorized", true)
            .await
            .unwrap();

        let mut request = format!("{base}/responses").into_client_request().unwrap();
        request.headers_mut().insert(
            HeaderName::from_static("x-polyflare-capability"),
            HeaderValue::from_static("security_work"),
        );
        request.headers_mut().insert(
            HeaderName::from_static("session-id"),
            HeaderValue::from_static("ws-capability-session"),
        );
        let (mut ws, _) = tokio_tungstenite::connect_async(request)
            .await
            .expect("an authorized account is available");
        ws.send(TMessage::Text(
            r#"{"type":"response.create","input":[]}"#.into(),
        ))
        .await
        .unwrap();

        while let Some(frame) = ws.next().await {
            let TMessage::Text(text) = frame.expect("no WS error") else {
                continue;
            };
            let value: serde_json::Value = serde_json::from_str(&text).unwrap();
            if value["type"] == "response.completed" {
                break;
            }
        }

        let mut owner = None;
        for _ in 0..50 {
            owner = state
                .store
                .continuity()
                .get_anchor_owner("resp_1")
                .await
                .unwrap();
            if owner.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            owner.as_deref(),
            Some("acct-b-authorized"),
            "WS must apply the same hard capability pre-filter as HTTP"
        );
    }

    #[tokio::test]
    async fn pooled_ws_path_cannot_select_an_account_outside_the_pool() {
        let mock = MockWsUpstream::new(ScriptedTurn::normal(Vec::new()));
        let mock_base = mock.spawn().await;
        let (base, state) =
            spawn_with_two_accounts("acct-a-outside", "acct-b-inside", &mock_base).await;

        let accounts = state.store.accounts();
        accounts
            .replace_pools("acct-a-outside", &["pool-a".to_string()])
            .await
            .unwrap();
        accounts
            .replace_pools("acct-b-inside", &["pool-b".to_string()])
            .await
            .unwrap();

        // Without propagating the path pool into owner resolution, RoundRobin would select the
        // lexicographically first account (`acct-a-outside`). The pool-b route must instead make
        // `acct-b-inside` the only eligible owner.
        let mut request = format!("{base}/pool-b/responses")
            .into_client_request()
            .unwrap();
        for (k, v) in [
            ("session-id", "s-pool-boundary"),
            ("thread-id", "t-pool-boundary"),
        ] {
            request.headers_mut().insert(
                HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        let (mut ws, _resp) = tokio_tungstenite::connect_async(request)
            .await
            .expect("pooled downstream WS handshake must succeed");
        ws.send(TMessage::Text(
            r#"{"type":"response.create","input":[]}"#.into(),
        ))
        .await
        .unwrap();

        let TMessage::Text(reply) = ws.next().await.expect("a reply").expect("no ws error") else {
            panic!("expected a text response from the relay");
        };
        let completed: serde_json::Value = serde_json::from_str(&reply).unwrap();
        assert_eq!(completed["type"], "response.completed");
        assert_eq!(completed["response"]["id"], "resp_1");

        let mut owner = None;
        for _ in 0..50 {
            owner = state
                .store
                .continuity()
                .get_anchor_owner("resp_1")
                .await
                .unwrap();
            if owner.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            owner.as_deref(),
            Some("acct-b-inside"),
            "a pooled WS route must never escape to an account outside that pool"
        );
    }

    /// A completed WS turn must populate the same durable telemetry surface as HTTP-SSE. This is
    /// intentionally an end-to-end assertion through a real downstream socket and mocked upstream:
    /// parsing helpers alone would not prove the pump actually emits one row at the turn boundary.
    #[tokio::test]
    async fn completed_turn_records_full_request_telemetry() {
        let delta = r#"{"type":"response.output_text.delta","delta":"synthetic"}"#.to_string();
        let mock = MockWsUpstream::new(ScriptedTurn::normal_with_usage(
            vec![delta],
            100_000,
            10_000,
            20_000,
            1_000,
            2_000,
        ));
        let mock_base = mock.spawn().await;
        let (base, state) = spawn_with_pinned_account("acct-telemetry", &mock_base).await;

        let mut request = format!("{base}/responses").into_client_request().unwrap();
        request.headers_mut().insert(
            HeaderName::from_static("x-openai-subagent"),
            HeaderValue::from_static("review"),
        );
        for (name, value) in [
            ("session-id", "session-telemetry"),
            ("thread-id", "thread-telemetry"),
            ("x-codex-window-id", "window-telemetry:0"),
        ] {
            request.headers_mut().insert(
                HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );
        }
        let (mut ws, _) = tokio_tungstenite::connect_async(request)
            .await
            .expect("downstream WS handshake");

        let create = serde_json::json!({
            "type": "response.create",
            "model": "gpt-5.6-sol",
            "reasoning": {"effort": "high"},
            "service_tier": "priority",
            "input": []
        })
        .to_string();
        ws.send(TMessage::Text(create.into())).await.unwrap();

        while let Some(frame) = ws.next().await {
            let TMessage::Text(text) = frame.expect("no WS error") else {
                continue;
            };
            let value: serde_json::Value = serde_json::from_str(&text).unwrap();
            if value["type"] == "response.completed" {
                break;
            }
        }

        state.store.flush_background_writes().await.unwrap();
        let rows = state.store.request_log().list(10, 0).await.unwrap();
        assert_eq!(rows.len(), 1, "one completed WS turn must produce one row");
        let row = &rows[0];
        assert_eq!(row.account_id.as_deref(), Some("acct-telemetry"));
        assert_eq!(row.model.as_deref(), Some("gpt-5.6-sol"));
        assert_eq!(row.reasoning_effort.as_deref(), Some("high"));
        assert_eq!(row.service_tier.as_deref(), Some("priority"));
        assert_eq!(row.transport.as_deref(), Some("ws"));
        assert_eq!(row.subagent.as_deref(), Some("review"));
        assert_eq!(
            row.protocol_outcome.as_deref(),
            Some("completed"),
            "the durable WS row must classify by its terminal protocol event, not merely the 101/200 transport status"
        );
        let session_key = row
            .session_key
            .as_deref()
            .expect("WS telemetry must carry its continuity session hash");
        // The completion is forwarded to the client before the content-free ownership write is
        // awaited. Poll that independently durable side effect instead of racing it.
        let mut session = None;
        for _ in 0..50 {
            session = state
                .store
                .continuity()
                .find_session_with_owner(session_key)
                .await
                .unwrap();
            if session.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let session =
            session.expect("request session hash must resolve to an exact continuity row");
        assert_eq!(session.owning_account_id.as_deref(), Some("acct-telemetry"));
        assert_eq!(row.status, 200);
        assert_eq!(row.input_tokens, Some(100_000));
        assert_eq!(row.output_tokens, Some(10_000));
        assert_eq!(row.cached_input_tokens, Some(20_000));
        assert_eq!(row.cache_write_input_tokens, Some(1_000));
        assert_eq!(row.reasoning_tokens, Some(2_000));
        assert_eq!(row.reported_total_tokens, Some(110_000));
        assert_eq!(row.usage_schema.as_deref(), Some("openai_responses_v1"));
        assert_eq!(row.usage_source.as_deref(), Some("upstream_response"));
        assert_eq!(row.usage_status.as_deref(), Some("final"));
        assert_eq!(row.total_tokens, Some(110_000));
        assert_eq!(row.cached_tokens, Some(20_000));
        assert!(row.ttft_ms.is_some());
        assert_eq!(row.ttft_ms, row.latency_first_token_ms);
        assert!(
            row.duration_ms >= row.ttft_ms.unwrap(),
            "duration and TTFT must share the same start"
        );
        assert_eq!(row.cost_usd, Some(1.42), "priority rates must be applied");
    }

    /// Phase 2, Task 3 (updated by Task 4, relay-catalog-fixes plan): a
    /// `websocket_connection_limit_reached` reply (the 60-minute server cap) must be INTERCEPTED by
    /// the pump — never forwarded to the client — and the pump must eagerly re-dial the SAME account.
    /// Since Task 4, the pump also REPLAYS the buffered in-flight frame on that fresh connection, so
    /// turn 1 completes AUTOMATICALLY (no explicit client resend needed — that stale assumption is
    /// exactly what `mid_turn_cap_replays_inflight_frame` exists to pin down in detail). This test's
    /// remaining job: prove the reconnect stays on the SAME account across BOTH turn 1 (the replay)
    /// and a genuine follow-up turn 2 sent by the client afterward — and that turn 2 does NOT trigger
    /// yet another reconnect (the redialed socket is already live and reused).
    #[tokio::test]
    async fn reconnect_on_connection_limit_stays_same_account() {
        let mock = MockWsUpstream::scripted(vec![
            ScriptedTurn::connection_limit_reached(409),
            ScriptedTurn::normal(vec![]),
        ])
        .with_upgrade_response_headers(vec![
            vec![
                ("x-codex-turn-state".to_string(), "turn-state-1".to_string()),
                (
                    "x-models-etag".to_string(),
                    "models-etag-stable".to_string(),
                ),
                ("x-reasoning-included".to_string(), "true".to_string()),
                ("openai-model".to_string(), "gpt-5.6-sol".to_string()),
            ],
            vec![
                ("x-codex-turn-state".to_string(), "turn-state-2".to_string()),
                (
                    "x-models-etag".to_string(),
                    "models-etag-stable".to_string(),
                ),
                ("x-reasoning-included".to_string(), "false".to_string()),
                ("openai-model".to_string(), "gpt-5.6-sol".to_string()),
            ],
        ]);
        let mock_base = mock.clone().spawn().await;

        let (base, state) = spawn_with_pinned_account("acct-cap", &mock_base).await;

        let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect("downstream WS handshake must succeed");

        // Turn 1: the mock replies with the wrapped cap-error envelope; the pump intercepts it,
        // eagerly re-dials, and (Task 4) replays this SAME frame on the fresh socket — so the
        // client's very next inbound frame is turn 1's own completion, not the cap error.
        let frame1 = r#"{"type":"response.create","input":[]}"#.to_string();
        ws.send(TMessage::Text(frame1.into())).await.unwrap();

        let TMessage::Text(reply1) = ws.next().await.expect("a reply").expect("no ws error") else {
            panic!("expected a text frame back from the relay");
        };
        let v1: serde_json::Value = serde_json::from_str(&reply1).unwrap();
        assert_eq!(
            v1["type"], "response.completed",
            "the cap frame must never reach the client — the buffered turn must complete \
             automatically via the Task 4 replay, got: {v1:?}"
        );
        let resp_id_1 = v1["response"]["id"].as_str().unwrap().to_string();

        // The pump must have eagerly re-dialed the SAME account: a second handshake at the mock.
        let mut handshakes = mock.handshake_count();
        for _ in 0..50 {
            handshakes = mock.handshake_count();
            if handshakes >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            handshakes >= 2,
            "the pump must eagerly re-dial after an intercepted cap frame (handshakes: {handshakes})"
        );

        // Ownership stays pinned to the SAME account across the reconnect.
        let mut owner = None;
        for _ in 0..50 {
            owner = state
                .store
                .continuity()
                .get_anchor_owner(&resp_id_1)
                .await
                .unwrap();
            if owner.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            owner.as_deref(),
            Some("acct-cap"),
            "the reconnect must stay on the SAME pinned account"
        );

        // Task 5: the eager cap re-dial must have bumped the content-free
        // `reconnect_same_account` counter (and never `move_cross_account`, since no move
        // happened here).
        let snapshot = state.relay_metrics.snapshot();
        let reconnects = snapshot
            .iter()
            .find(|(k, _)| k == "reconnect_same_account")
            .map(|(_, v)| *v)
            .unwrap_or(0);
        assert!(
            reconnects >= 1,
            "expected reconnect_same_account >= 1 after the cap re-dial, got snapshot: {snapshot:?}"
        );
        assert!(
            !snapshot.iter().any(|(k, _)| k == "move_cross_account"),
            "a same-account cap reconnect must never bump move_cross_account, got: {snapshot:?}"
        );

        // Turn 2, a genuine follow-up from the client on the ALREADY-reused connection: answered
        // `response.completed` with a NEW id, and the reused socket means no THIRD handshake.
        let frame2 = format!(
            r#"{{"type":"response.create","input":[],"previous_response_id":"{resp_id_1}"}}"#
        );
        ws.send(TMessage::Text(frame2.into())).await.unwrap();

        let TMessage::Text(reply2) = ws.next().await.expect("a reply").expect("no ws error") else {
            panic!("expected a text frame back from the relay");
        };
        let v2: serde_json::Value = serde_json::from_str(&reply2).unwrap();
        assert_eq!(v2["type"], "response.completed");
        let resp_id_2 = v2["response"]["id"].as_str().unwrap().to_string();
        assert_ne!(
            resp_id_2, resp_id_1,
            "turn 2 must be a genuinely new completion"
        );

        assert_eq!(
            mock.handshake_count(),
            handshakes,
            "turn 2 must reuse the already-redialed socket, not trigger a THIRD handshake"
        );

        let mut owner2 = None;
        for _ in 0..50 {
            owner2 = state
                .store
                .continuity()
                .get_anchor_owner(&resp_id_2)
                .await
                .unwrap();
            if owner2.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            owner2.as_deref(),
            Some("acct-cap"),
            "turn 2 must still be owned by the SAME pinned account"
        );
    }

    /// A transparent upstream reconnect cannot update the HTTP 101 metadata already observed by
    /// the downstream Codex client. If the server-selected model, model-catalog ETag, or reasoning
    /// capability changes, close the downstream socket so Codex performs its own fresh handshake.
    #[tokio::test]
    async fn reconnect_closes_downstream_when_upgrade_contract_drifts() {
        let mock = MockWsUpstream::scripted(vec![
            ScriptedTurn::connection_limit_reached(409),
            ScriptedTurn::normal(vec![]),
        ])
        .with_upgrade_response_headers(vec![
            vec![
                ("x-models-etag".to_string(), "models-etag-1".to_string()),
                ("x-reasoning-included".to_string(), "true".to_string()),
                ("openai-model".to_string(), "gpt-5.6-sol".to_string()),
            ],
            vec![
                ("x-models-etag".to_string(), "models-etag-2".to_string()),
                ("x-reasoning-included".to_string(), "false".to_string()),
                ("openai-model".to_string(), "gpt-5.6-terra".to_string()),
            ],
        ])
        .capturing_raw_frames();
        let mock_base = mock.clone().spawn().await;
        let (base, _) = spawn_with_pinned_account("acct-contract-drift", &mock_base).await;

        let (mut ws, response) = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect("initial downstream handshake");
        assert_eq!(
            response
                .headers()
                .get("openai-model")
                .and_then(|value| value.to_str().ok()),
            Some("gpt-5.6-sol")
        );

        ws.send(TMessage::Text(
            r#"{"type":"response.create","input":[]}"#.into(),
        ))
        .await
        .unwrap();

        let next = tokio::time::timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("contract drift must terminate promptly");
        assert!(
            !matches!(next, Some(Ok(TMessage::Text(_)))),
            "the relay must not replay onto a socket whose hidden 101 contract changed"
        );
        assert_eq!(
            mock.handshake_count(),
            2,
            "the drift decision must happen after the replacement upstream handshake"
        );
        assert_eq!(
            mock.raw_frames().len(),
            1,
            "the request must not be replayed after the replacement handshake contract drifts"
        );
    }

    /// Phase 2, Task 3 (updated by Task 4, relay-catalog-fixes plan): an upstream drop mid-stream
    /// (network blip / idle close / anything short of the client closing) must NOT tear down the
    /// client's downstream socket. Since Task 4, a drop that catches a turn IN FLIGHT is no longer
    /// silent: the pump eagerly re-dials the same account AND replays the buffered frame, so turn 1
    /// completes automatically. This test's remaining job: the same-account reconnect proof, PLUS a
    /// genuine follow-up turn 2 reusing that same redialed socket (no further handshake).
    #[tokio::test]
    async fn reconnect_on_upstream_drop_keeps_downstream_open() {
        let mock = MockWsUpstream::scripted(vec![
            ScriptedTurn::close_mid_stream(vec![]),
            ScriptedTurn::normal(vec![]),
        ]);
        let mock_base = mock.clone().spawn().await;

        let (base, state) = spawn_with_pinned_account("acct-drop", &mock_base).await;

        let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect("downstream WS handshake must succeed");

        // Turn 1: the mock accepts the frame, sends nothing, then closes its own socket mid-turn.
        // Task 4: the pump eagerly re-dials AND replays this frame on the fresh socket, so turn 1
        // completes automatically — no explicit client resend needed.
        let frame1 = r#"{"type":"response.create","input":[]}"#.to_string();
        ws.send(TMessage::Text(frame1.into())).await.unwrap();

        let TMessage::Text(reply1) = ws.next().await.expect("a reply").expect("no ws error") else {
            panic!("expected a text frame back from the relay");
        };
        let v1: serde_json::Value = serde_json::from_str(&reply1).unwrap();
        assert_eq!(
            v1["type"], "response.completed",
            "the buffered turn must complete automatically via the Task 4 eager redial + replay \
             after a mid-turn upstream drop, got: {v1:?}"
        );
        let resp_id_1 = v1["response"]["id"].as_str().unwrap().to_string();

        let mut handshakes = mock.handshake_count();
        for _ in 0..50 {
            handshakes = mock.handshake_count();
            if handshakes >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            handshakes >= 2,
            "the relay must have re-dialed a second connection after the upstream drop"
        );

        let mut owner = None;
        for _ in 0..50 {
            owner = state
                .store
                .continuity()
                .get_anchor_owner(&resp_id_1)
                .await
                .unwrap();
            if owner.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            owner.as_deref(),
            Some("acct-drop"),
            "the reconnect after an upstream drop must stay on the SAME pinned account"
        );

        // Turn 2, a genuine follow-up from the client on the ALREADY-reused connection.
        let frame2 = format!(
            r#"{{"type":"response.create","input":[],"previous_response_id":"{resp_id_1}"}}"#
        );
        ws.send(TMessage::Text(frame2.into())).await.unwrap();

        let TMessage::Text(reply2) = ws.next().await.expect("a reply").expect("no ws error") else {
            panic!("expected a text frame back from the relay");
        };
        let v2: serde_json::Value = serde_json::from_str(&reply2).unwrap();
        assert_eq!(v2["type"], "response.completed");
        let resp_id_2 = v2["response"]["id"].as_str().unwrap().to_string();
        assert_ne!(
            resp_id_2, resp_id_1,
            "turn 2 must be a genuinely new completion"
        );

        assert_eq!(
            mock.handshake_count(),
            handshakes,
            "turn 2 must reuse the already-redialed socket, not trigger a THIRD handshake"
        );

        let mut owner2 = None;
        for _ in 0..50 {
            owner2 = state
                .store
                .continuity()
                .get_anchor_owner(&resp_id_2)
                .await
                .unwrap();
            if owner2.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            owner2.as_deref(),
            Some("acct-drop"),
            "turn 2 must still be owned by the SAME pinned account"
        );
    }

    #[tokio::test]
    async fn upstream_drop_after_visible_output_closes_without_replay() {
        let visible =
            serde_json::json!({"type":"response.output_text.delta","delta":"visible"}).to_string();
        let mock = MockWsUpstream::new(ScriptedTurn::close_mid_stream(vec![visible.clone()]))
            .capturing_raw_frames();
        let mock_base = mock.clone().spawn().await;
        let (base, state) = spawn_with_pinned_account("acct-visible-drop", &mock_base).await;

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect("downstream WS handshake");
        ws.send(TMessage::Text(
            r#"{"type":"response.create","input":[]}"#.into(),
        ))
        .await
        .unwrap();

        let TMessage::Text(first) = ws
            .next()
            .await
            .expect("visible delta")
            .expect("no WS error")
        else {
            panic!("expected the visible delta");
        };
        assert_eq!(first.as_str(), visible);
        let next = tokio::time::timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("post-output drop must terminate promptly");
        assert!(!matches!(next, Some(Ok(TMessage::Text(_)))));
        assert_eq!(mock.handshake_count(), 1, "must not redial after output");
        assert_eq!(mock.raw_frames().len(), 1, "must not replay after output");
        let mut snapshots = vec![polyflare_core::AccountSnapshot::new("acct-visible-drop")];
        state.runtime.overlay(&mut snapshots, now());
        assert_eq!(
            snapshots[0].error_count, 1,
            "a WS transport loss after visible output must feed routing health exactly once"
        );
        state.store.flush_background_writes().await.unwrap();
        let rows = state.store.request_log().list(10, 0).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].protocol_outcome.as_deref(),
            Some("transport_lost"),
            "a terminal-less upstream loss must not remain a legacy status-only row"
        );
    }

    #[tokio::test]
    async fn connection_limit_after_visible_output_closes_without_replay() {
        let visible =
            serde_json::json!({"type":"response.output_text.delta","delta":"visible"}).to_string();
        let mock = MockWsUpstream::new(ScriptedTurn::ErrorAfterEvents {
            events: vec![visible.clone()],
            status: 409,
            code: "websocket_connection_limit_reached".to_string(),
            message: "connection limit".to_string(),
        })
        .capturing_raw_frames();
        let mock_base = mock.clone().spawn().await;
        let (base, _) = spawn_with_pinned_account("acct-visible-cap", &mock_base).await;

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect("downstream WS handshake");
        ws.send(TMessage::Text(
            r#"{"type":"response.create","input":[]}"#.into(),
        ))
        .await
        .unwrap();

        let TMessage::Text(first) = ws
            .next()
            .await
            .expect("visible delta")
            .expect("no WS error")
        else {
            panic!("expected the visible delta");
        };
        assert_eq!(first.as_str(), visible);
        let next = tokio::time::timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("post-output cap must terminate promptly");
        assert!(!matches!(next, Some(Ok(TMessage::Text(_)))));
        assert_eq!(mock.handshake_count(), 1, "must not redial after output");
        assert_eq!(mock.raw_frames().len(), 1, "must not replay after output");
    }

    #[tokio::test]
    async fn hidden_redial_401_refreshes_before_replaying_the_turn() {
        const VALID_JWT: &str = "eyJhbGciOiJub25lIn0.e30.sig";
        let mock = MockWsUpstream::scripted(vec![
            ScriptedTurn::connection_limit_reached(409),
            ScriptedTurn::normal(Vec::new()),
        ])
        .with_handshake_authorizations(vec![
            None,
            Some(format!("Bearer {VALID_JWT}")),
            Some(format!("Bearer {VALID_JWT}")),
        ])
        .capturing_raw_frames();
        let mock_base = mock.clone().spawn().await;
        let oauth = MockOAuth::ok(VALID_JWT, "rotated-refresh", VALID_JWT);
        let oauth_handle = oauth.clone();
        let oauth_url = oauth.spawn().await;
        let (base, _) =
            spawn_with_pinned_account_and_oauth("acct-hidden-redial-auth", &mock_base, &oauth_url)
                .await;

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect("initial downstream handshake");
        let frame = r#"{"type":"response.create","input":[]}"#.to_string();
        ws.send(TMessage::Text(frame.clone().into())).await.unwrap();

        let TMessage::Text(reply) = tokio::time::timeout(Duration::from_secs(3), ws.next())
            .await
            .expect("hidden redial auth recovery must finish promptly")
            .expect("completion")
            .expect("no WS error")
        else {
            panic!("expected completion after refreshed hidden redial");
        };
        let reply: serde_json::Value = serde_json::from_str(&reply).unwrap();
        assert_eq!(reply["type"], "response.completed");
        assert_eq!(oauth_handle.hit_count(), 1);
        assert_eq!(mock.raw_frames(), vec![frame.clone(), frame]);
        assert_eq!(
            mock.handshake_authorizations(),
            vec![
                Some("Bearer tok".to_string()),
                Some("Bearer tok".to_string()),
                Some(format!("Bearer {VALID_JWT}")),
            ],
            "the first hidden redial 401 must refresh instead of retrying the stale bearer"
        );
        assert_eq!(mock.handshake_count(), 2);
    }

    /// Phase 3, Task 4: a durable upstream error (anything other than the connection-limit / anchor-
    /// missing special cases — here a wrapped 429) must MOVE the conversation to a DIFFERENT account:
    /// bench the account that just errored, re-select (falling through to the other eligible
    /// account), and re-dial it — rather than retrying the same exhausted account forever.
    ///
    /// Two accounts are seeded, deliberately UNPINNED — the selector (`RoundRobin`, ties to the
    /// lexicographically smaller id) picks one for turn 1. Both accounts' upstream dial lands on the
    /// SAME mock, scripted `[rate_limited_429(300), previous_response_not_found("whatever"),
    /// normal(vec![])]`: turn 1 gets the 429 (which must reach the client verbatim — Design Note 3,
    /// the error is forwarded first, THEN the move happens). The client's anchored retry reaches
    /// the moved-to account and misses there; that anchored frame is a client-planned DELTA, so
    /// PolyFlare must NOT replay it anchorless (the parrot amputation) — it answers with the
    /// forged retryable resend signal, and the client's FULL resend completes on the moved-to
    /// account.
    #[tokio::test]
    async fn durable_error_moves_to_a_second_account() {
        let mock = MockWsUpstream::scripted(vec![
            ScriptedTurn::rate_limited_429(300),
            ScriptedTurn::previous_response_not_found("whatever"),
            ScriptedTurn::normal(vec![]),
        ]);
        let mock_base = mock.clone().spawn().await;

        let (base, state) = spawn_with_two_accounts("acct-move-a", "acct-move-b", &mock_base).await;

        let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect("downstream WS handshake must succeed");

        // Turn 1: the selector picks one account (RoundRobin ties to "acct-move-a"); the mock
        // replies with the wrapped 429 error envelope.
        let frame1 = r#"{"type":"response.create","input":[]}"#.to_string();
        ws.send(TMessage::Text(frame1.into())).await.unwrap();

        // The client MUST receive the 429 error frame verbatim — forwarded, never swallowed.
        let TMessage::Text(err_frame) = ws
            .next()
            .await
            .expect("an error frame")
            .expect("no ws error")
        else {
            panic!("expected a text frame back from the relay");
        };
        let ev: serde_json::Value = serde_json::from_str(&err_frame).unwrap();
        assert_eq!(ev["type"], "error");
        assert_eq!(ev["error"]["code"], "rate_limit_exceeded");

        // The relay must have benched the errored account and re-dialed — a second handshake at the
        // shared mock (whether the SAME or the OTHER account re-dials, the mock sees a new socket).
        let mut handshakes = mock.handshake_count();
        for _ in 0..50 {
            handshakes = mock.handshake_count();
            if handshakes >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            handshakes >= 2,
            "the relay must re-dial after a durable error (handshakes: {handshakes})"
        );

        // Exactly ONE of {A, B} now carries a live cooldown in the runtime overlay — the benched one.
        let mut snaps = vec![
            polyflare_core::AccountSnapshot::new("acct-move-a"),
            polyflare_core::AccountSnapshot::new("acct-move-b"),
        ];
        state.runtime.overlay(&mut snaps, now());
        let benched: Vec<String> = snaps
            .iter()
            .filter(|s| s.cooldown_until.is_some())
            .map(|s| s.id.as_str().to_string())
            .collect();
        assert_eq!(
            benched.len(),
            1,
            "exactly one account must carry a live cooldown after the move, got: {benched:?}"
        );

        // Turn 2, on the re-dialed (moved-to) connection: the anchored attempt misses there. That
        // frame is a client-planned DELTA (its input is only the suffix), so the relay must NOT
        // "recover" it anchorless — it answers with the forged retryable resend signal instead.
        let frame2 =
            r#"{"type":"response.create","input":[{"role":"user","content":"retry"}],"previous_response_id":"whatever"}"#
                .to_string();
        ws.send(TMessage::Text(frame2.into())).await.unwrap();

        let TMessage::Text(signal) = ws.next().await.expect("a reply").expect("no ws error") else {
            panic!("expected a text frame back from the relay");
        };
        let sv: serde_json::Value = serde_json::from_str(&signal).unwrap();
        assert_eq!(sv["type"], "error", "got: {sv:?}");
        assert_eq!(
            sv["error"]["code"], "websocket_connection_limit_reached",
            "the post-move anchor miss must become the client-resend signal, got: {sv:?}"
        );

        // The client honors the signal with a FULL anchorless resend (its delta ledger was never
        // resolved), which completes normally on the moved-to account.
        let frame3 =
            r#"{"type":"response.create","input":[{"role":"user","content":"turn 1"},{"role":"user","content":"retry"}]}"#
                .to_string();
        ws.send(TMessage::Text(frame3.into())).await.unwrap();

        let TMessage::Text(reply) = ws.next().await.expect("a reply").expect("no ws error") else {
            panic!("expected a text frame back from the relay");
        };
        let v: serde_json::Value = serde_json::from_str(&reply).unwrap();
        assert_eq!(
            v["type"], "response.completed",
            "the client's full resend must complete on the moved-to account, got: {v:?}"
        );
        let resp_id = v["response"]["id"].as_str().unwrap().to_string();

        let frames = mock.frames();
        assert_eq!(
            frames.len(),
            3,
            "initial failure, anchored post-move attempt, and the CLIENT's full resend — never \
             an internal anchorless replay"
        );
        assert_eq!(
            frames[1].previous_response_id.as_deref(),
            Some("whatever"),
            "the client retry carries its old account-owned anchor verbatim"
        );
        assert_eq!(
            frames[2].previous_response_id, None,
            "the client's resend is anchorless by construction"
        );
        assert_eq!(
            frames[2].input_len, 2,
            "the client's resend carries the FULL history, not the delta suffix"
        );

        // Ownership: the completed turn's owner must be the account NOT in the benched set — the
        // moved-to account — proving the pump records the CURRENT account, not the original one.
        let mut owner = None;
        for _ in 0..50 {
            owner = state
                .store
                .continuity()
                .get_anchor_owner(&resp_id)
                .await
                .unwrap();
            if owner.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let owner = owner.expect("the completed response must be owned by some account");
        assert_ne!(
            owner, benched[0],
            "the completed turn's owner must NOT be the benched (errored) account"
        );
        assert!(
            owner == "acct-move-a" || owner == "acct-move-b",
            "owner must be one of the two seeded accounts, got {owner}"
        );

        state.store.flush_background_writes().await.unwrap();
        let rows = state.store.request_log().list(10, 0).await.unwrap();
        assert_eq!(
            rows.len(),
            3,
            "the forwarded 429, the signaled (409) turn, and the successful resend are three \
             client-visible WS outcomes"
        );
        let failed = rows
            .iter()
            .find(|row| row.status == 429)
            .expect("the durable upstream error must be logged");
        rows.iter()
            .find(|row| row.status == 409)
            .expect("the resend-signaled turn must be logged, never silently absorbed");
        let succeeded = rows
            .iter()
            .find(|row| row.status == 200)
            .expect("the post-move completion must be logged");
        assert_eq!(
            failed.account_id.as_deref(),
            Some(benched[0].as_str()),
            "the failure row belongs to the account that actually returned the 429"
        );
        assert_eq!(
            succeeded.account_id.as_deref(),
            Some(owner.as_str()),
            "the success row belongs to the moved-to account that completed the turn"
        );
        assert!(rows
            .iter()
            .all(|row| row.transport.as_deref() == Some("ws")));

        // Task 5: the move must have bumped the content-free `move_cross_account` counter (and
        // never `same_account_anchor_miss`, which is a different signal entirely).
        let snapshot = state.relay_metrics.snapshot();
        let moves = snapshot
            .iter()
            .find(|(k, _)| k == "move_cross_account")
            .map(|(_, v)| *v)
            .unwrap_or(0);
        assert!(
            moves >= 1,
            "expected move_cross_account >= 1 after the durable-error move, got snapshot: {snapshot:?}"
        );
        assert!(
            !snapshot
                .iter()
                .any(|(k, _)| k == "same_account_anchor_miss"),
            "a durable-error move must never bump same_account_anchor_miss, got: {snapshot:?}"
        );
    }

    #[tokio::test]
    async fn durable_ws_reselect_preserves_capability_floor() {
        let mock = MockWsUpstream::scripted(vec![
            ScriptedTurn::rate_limited_429(300),
            ScriptedTurn::normal(vec![]),
        ]);
        let mock_base = mock.clone().spawn().await;
        let (base, state) =
            spawn_with_two_accounts("acct-capmove-a", "acct-capmove-b-unauthorized", &mock_base)
                .await;
        state
            .store
            .accounts()
            .update_security_work_authorized("acct-capmove-a", true)
            .await
            .unwrap();
        state
            .store
            .accounts()
            .insert(
                &StoreAccount {
                    id: "acct-capmove-c".to_string(),
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
                    security_work_authorized: true,
                    provider: "codex".to_string(),
                    pool: None,
                },
                &PlainTokens {
                    access_token: "tok-acct-capmove-c".into(),
                    refresh_token: "r".into(),
                    id_token: "i".into(),
                },
                &state.cipher,
            )
            .await
            .unwrap();

        let mut request = format!("{base}/responses").into_client_request().unwrap();
        request.headers_mut().insert(
            HeaderName::from_static("x-polyflare-capability"),
            HeaderValue::from_static("security_work"),
        );
        let (mut ws, _) = tokio_tungstenite::connect_async(request)
            .await
            .expect("initial authorized owner");
        ws.send(TMessage::Text(
            r#"{"type":"response.create","input":[]}"#.into(),
        ))
        .await
        .unwrap();
        let _ = ws
            .next()
            .await
            .expect("durable error")
            .expect("no WS error");

        for _ in 0..50 {
            if mock.handshake_count() >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let authorizations = mock.handshake_authorizations();
        assert_eq!(
            authorizations.first().and_then(Option::as_deref),
            Some("Bearer tok-acct-capmove-a")
        );
        assert_eq!(
            authorizations.get(1).and_then(Option::as_deref),
            Some("Bearer tok-acct-capmove-c"),
            "durable reselect must skip the alphabetically earlier unauthorized account"
        );

        ws.send(TMessage::Text(
            r#"{"type":"response.create","input":[]}"#.into(),
        ))
        .await
        .unwrap();
        let TMessage::Text(reply) = ws.next().await.expect("completion").expect("no WS error")
        else {
            panic!("expected completion text");
        };
        let value: serde_json::Value = serde_json::from_str(&reply).unwrap();
        assert_eq!(value["type"], "response.completed");
    }

    /// THE 2026-07-23 parrot regression. An anchored generating frame is a CLIENT-PLANNED delta —
    /// codex-rs sets `previous_response_id` exactly when `input` holds only the new suffix — so an
    /// anchor miss must NEVER be "recovered" by replaying the frame anchorless: that silently
    /// restarts the conversation with just the suffix (a ~240k-token session reborn upstream as a
    /// 39-token one, 200 OK, no error anywhere). The pump must instead forge the one error
    /// envelope codex-rs classifies as RETRYABLE (`websocket_connection_limit_reached` →
    /// `ApiError::Retryable` → `CodexErr::Stream`), whose failed attempt leaves the client's
    /// per-connection delta ledger unresolved — so the client's bounded retry arrives as a FULL
    /// anchorless resend, and no context is ever lost.
    #[tokio::test]
    async fn anchor_miss_asks_the_client_to_resend_never_replays_anchorless() {
        let mock = MockWsUpstream::scripted(vec![
            ScriptedTurn::normal(vec![]),
            ScriptedTurn::previous_response_not_found("resp_1"),
            ScriptedTurn::normal(vec![]),
        ])
        .capturing_raw_frames();
        let mock_base = mock.clone().spawn().await;

        let (base, state) = spawn_with_pinned_account("acct-anchor-resend", &mock_base).await;

        let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect("downstream WS handshake must succeed");

        // Turn 1: a normal completion — seeds the conversation and the anchor chain.
        let frame1 = r#"{"type":"response.create","input":[{"role":"user","content":"turn 1"}]}"#
            .to_string();
        ws.send(TMessage::Text(frame1.into())).await.unwrap();

        let TMessage::Text(reply1) = ws.next().await.expect("a reply").expect("no ws error") else {
            panic!("expected a text frame back from the relay");
        };
        let v1: serde_json::Value = serde_json::from_str(&reply1).unwrap();
        assert_eq!(v1["type"], "response.completed");

        // Turn 2: a client-planned DELTA (suffix + anchor) whose anchor the upstream rejects.
        let frame2 =
            r#"{"type":"response.create","input":[{"role":"user","content":"the delta suffix"}],"previous_response_id":"resp_1"}"#
                .to_string();
        ws.send(TMessage::Text(frame2.into())).await.unwrap();

        // The client must receive the forged RETRYABLE envelope — not a silent completion of an
        // amputated conversation, and not the raw 400 (codex-rs maps that to the non-retryable
        // `InvalidRequest` and wedges the task).
        let TMessage::Text(reply2) = ws
            .next()
            .await
            .expect("the resend signal")
            .expect("no ws error")
        else {
            panic!("expected a text frame back from the relay");
        };
        let v2: serde_json::Value = serde_json::from_str(&reply2).unwrap();
        assert_eq!(v2["type"], "error", "got: {v2:?}");
        assert_eq!(
            v2["error"]["code"], "websocket_connection_limit_reached",
            "must be the ONE shape codex-rs retries in place, got: {v2:?}"
        );

        // THE regression assertion: exactly turn 1 + the anchored attempt reached the upstream.
        // No anchorless replay of the delta ever went out.
        let frames = mock.raw_frames();
        assert_eq!(
            frames.len(),
            2,
            "no internal anchorless replay may exist, got: {frames:?}"
        );
        let attempt: serde_json::Value = serde_json::from_str(&frames[1]).unwrap();
        assert_eq!(
            attempt["previous_response_id"], "resp_1",
            "the client's anchored attempt is forwarded verbatim"
        );

        // Simulate codex-rs's native reaction to the retryable error: a FULL anchorless resend
        // (its delta ledger was never resolved by a completed response).
        let frame3 =
            r#"{"type":"response.create","input":[{"role":"user","content":"turn 1"},{"role":"assistant","content":"reply 1"},{"role":"user","content":"the delta suffix"}]}"#
                .to_string();
        ws.send(TMessage::Text(frame3.into())).await.unwrap();

        let TMessage::Text(reply3) = ws
            .next()
            .await
            .expect("the resent turn's completion")
            .expect("no ws error")
        else {
            panic!("expected a text frame back from the relay");
        };
        let v3: serde_json::Value = serde_json::from_str(&reply3).unwrap();
        assert_eq!(v3["type"], "response.completed");

        let frames = mock.raw_frames();
        assert_eq!(frames.len(), 3, "turn 1, anchored attempt, client resend");
        let resend: serde_json::Value = serde_json::from_str(&frames[2]).unwrap();
        assert!(
            resend.get("previous_response_id").is_none(),
            "the resend is anchorless by construction"
        );
        assert_eq!(
            resend["input"].as_array().map(Vec::len),
            Some(3),
            "the resend carries the FULL history, not a suffix"
        );

        // Three rows: turn 1 (200), the signaled turn (409 — visible, honest), the resend (200).
        let mut rows = Vec::new();
        for _ in 0..50 {
            state.store.flush_background_writes().await.unwrap();
            rows = state.store.request_log().list(10, 0).await.unwrap();
            if rows.len() == 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let mut statuses: Vec<i64> = rows.iter().map(|row| row.status).collect();
        statuses.sort_unstable();
        assert_eq!(
            statuses,
            vec![200, 200, 409],
            "the aborted turn must be visible as its own row, never silently absorbed"
        );

        // Metrics: the resend signal has its own label; nothing is falsely counted as recovered,
        // as a terminal miss, or as a move.
        let snapshot = state.relay_metrics.snapshot();
        assert!(
            snapshot
                .iter()
                .any(|(k, v)| k == "anchor_miss_client_resend" && *v == 1),
            "expected anchor_miss_client_resend == 1, got snapshot: {snapshot:?}"
        );
        assert!(
            !snapshot
                .iter()
                .any(|(k, _)| k == "same_account_anchor_miss"),
            "a signaled resend is not a terminal miss, got: {snapshot:?}"
        );
        assert!(
            !snapshot.iter().any(|(k, _)| k == "move_cross_account"),
            "a same-account anchor-miss must never bump move_cross_account, got: {snapshot:?}"
        );
    }

    /// The resend signal is one-shot per completed turn. A client honoring it resends FULL
    /// history (anchorless — it cannot miss again); a client that instead repeats an anchored
    /// attempt gets the raw miss forwarded verbatim rather than an endless signal loop.
    #[tokio::test]
    async fn repeated_anchor_miss_after_resend_signal_surfaces_verbatim() {
        let mock = MockWsUpstream::scripted(vec![
            ScriptedTurn::normal(vec![]),
            ScriptedTurn::previous_response_not_found("resp_1"),
            ScriptedTurn::previous_response_not_found("resp_1"),
        ])
        .capturing_raw_frames();
        let mock_base = mock.clone().spawn().await;

        let (base, state) = spawn_with_pinned_account("acct-anchor-loop", &mock_base).await;
        let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect("downstream WS handshake must succeed");

        ws.send(TMessage::Text(
            r#"{"type":"response.create","input":[]}"#.into(),
        ))
        .await
        .unwrap();
        let _ = ws.next().await.expect("turn 1").expect("no ws error");

        // Anchored attempt #1: answered with the forged retryable resend signal.
        ws.send(TMessage::Text(
            r#"{"type":"response.create","input":[{"role":"user","content":"retry"}],"previous_response_id":"resp_1"}"#
                .into(),
        ))
        .await
        .unwrap();
        let TMessage::Text(signal) = ws
            .next()
            .await
            .expect("the resend signal")
            .expect("no ws error")
        else {
            panic!("expected a text error frame");
        };
        let signal: serde_json::Value = serde_json::from_str(&signal).unwrap();
        assert_eq!(
            signal["error"]["code"],
            "websocket_connection_limit_reached"
        );

        // Anchored attempt #2 — the client is NOT honoring the resend contract. The raw miss
        // must surface verbatim now; a second signal would loop forever against such a client.
        ws.send(TMessage::Text(
            r#"{"type":"response.create","input":[{"role":"user","content":"retry"}],"previous_response_id":"resp_1"}"#
                .into(),
        ))
        .await
        .unwrap();
        let TMessage::Text(reply) = ws
            .next()
            .await
            .expect("terminal miss")
            .expect("no ws error")
        else {
            panic!("expected a text error frame");
        };
        let value: serde_json::Value = serde_json::from_str(&reply).unwrap();
        assert_eq!(value["error"]["code"], "previous_response_not_found");
        assert_eq!(
            mock.raw_frames().len(),
            3,
            "turn 1 plus two client-sent anchored attempts — never an internal replay"
        );

        let snapshot = state.relay_metrics.snapshot();
        assert!(snapshot
            .iter()
            .any(|(k, v)| k == "anchor_miss_client_resend" && *v == 1));
        assert!(snapshot
            .iter()
            .any(|(k, v)| k == "same_account_anchor_miss" && *v == 1));
    }

    /// A prewarm (`generate:false`) is not a user-generating turn and cannot be replayed safely.
    #[tokio::test]
    async fn same_account_anchor_miss_does_not_replay_non_generating_frame() {
        let mock = MockWsUpstream::new(ScriptedTurn::previous_response_not_found("resp_1"))
            .capturing_raw_frames();
        let mock_base = mock.clone().spawn().await;

        let (base, state) = spawn_with_pinned_account("acct-anchor-prewarm", &mock_base).await;
        let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect("downstream WS handshake must succeed");

        ws.send(TMessage::Text(
            r#"{"type":"response.create","generate":false,"input":[],"previous_response_id":"resp_1"}"#
                .into(),
        ))
        .await
        .unwrap();
        let TMessage::Text(reply) = ws
            .next()
            .await
            .expect("terminal miss")
            .expect("no ws error")
        else {
            panic!("expected a text error frame");
        };
        let value: serde_json::Value = serde_json::from_str(&reply).unwrap();
        assert_eq!(value["error"]["code"], "previous_response_not_found");
        assert_eq!(
            mock.raw_frames().len(),
            1,
            "the non-generating frame must not be replayed"
        );

        let snapshot = state.relay_metrics.snapshot();
        assert!(!snapshot
            .iter()
            .any(|(k, _)| k == "same_account_anchor_recovered"));
        assert!(snapshot
            .iter()
            .any(|(k, v)| k == "same_account_anchor_miss" && *v == 1));
    }

    /// Task 4 (relay-catalog-fixes plan): a mid-turn cap — the `websocket_connection_limit_reached`
    /// envelope arriving BEFORE any `response.completed` for the turn — must not just intercept +
    /// eagerly re-dial (Phase 2, Task 3 already does that) but REPLAY the client's buffered
    /// in-flight frame on the fresh socket, so the interrupted turn resumes WITHOUT the client
    /// resending. One account, scripted `[connection_limit_reached(409), normal(vec![])]`, with
    /// `.capturing_raw_frames()` so `raw_frames()` can prove the SAME frame bytes were relayed
    /// TWICE (once to socket 1, which caps; once REPLAYED to socket 2, which completes) — never a
    /// second client send.
    #[tokio::test]
    async fn mid_turn_cap_replays_inflight_frame() {
        let mock = MockWsUpstream::scripted(vec![
            ScriptedTurn::connection_limit_reached(409),
            ScriptedTurn::normal(vec![]),
        ])
        .capturing_raw_frames();
        let mock_base = mock.clone().spawn().await;

        let (base, state) = spawn_with_pinned_account("acct-mid-turn-cap", &mock_base).await;

        let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect("downstream WS handshake must succeed");

        // ONE client frame — the in-flight turn. Socket 1 caps it; the pump must eagerly re-dial
        // AND replay this exact frame on socket 2, with NO further send from the client.
        let frame = r#"{"type":"response.create","input":[]}"#.to_string();
        ws.send(TMessage::Text(frame.clone().into())).await.unwrap();

        // The client must receive `response.completed` directly — the cap frame was intercepted,
        // never forwarded, so the FIRST (and only) thing the client sees is the completion.
        // Bounded by a generous timeout: without the replay fix, nothing is ever sent to the
        // re-dialed socket, so the client would otherwise hang forever waiting for a reply.
        let reply = tokio::time::timeout(Duration::from_secs(10), ws.next())
            .await
            .expect("the replayed turn must complete within the timeout, not hang")
            .expect("a reply")
            .expect("no ws error");
        let TMessage::Text(reply) = reply else {
            panic!("expected a text frame back from the relay");
        };
        let v: serde_json::Value = serde_json::from_str(&reply).unwrap();
        assert_eq!(
            v["type"], "response.completed",
            "the client must never see the intercepted cap/error frame — the buffered frame must \
             be replayed on the re-dialed socket so the turn completes transparently, got: {v:?}"
        );
        let resp_id = v["response"]["id"].as_str().unwrap().to_string();

        // The eager re-dial happened: a second handshake at the mock.
        assert!(
            mock.handshake_count() >= 2,
            "the pump must eagerly re-dial after the intercepted cap frame (handshakes: {})",
            mock.handshake_count()
        );

        // The proof the REPLAY (not a client resend) is what reached socket 2: the mock's
        // raw-frame log shows the client's ORIGINAL frame bytes TWICE — once on socket 1 (capped),
        // once replayed verbatim on socket 2 (completed) — even though the client only ever sent
        // it ONCE.
        let mut raw = mock.raw_frames();
        for _ in 0..50 {
            raw = mock.raw_frames();
            if raw.len() >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            raw,
            vec![frame.clone(), frame.clone()],
            "the buffered in-flight frame must be replayed BYTE-VERBATIM on the re-dialed socket, \
             proving a buffer replay rather than a client resend"
        );

        // Ownership: the completed turn is recorded against the same pinned account.
        let mut owner = None;
        for _ in 0..50 {
            owner = state
                .store
                .continuity()
                .get_anchor_owner(&resp_id)
                .await
                .unwrap();
            if owner.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            owner.as_deref(),
            Some("acct-mid-turn-cap"),
            "the replayed turn must complete on the SAME pinned account"
        );

        let snapshot = state.relay_metrics.snapshot();
        let reconnects = snapshot
            .iter()
            .find(|(k, _)| k == "reconnect_same_account")
            .map(|(_, v)| *v)
            .unwrap_or(0);
        assert!(
            reconnects >= 1,
            "expected reconnect_same_account >= 1 after the mid-turn cap replay, got snapshot: \
             {snapshot:?}"
        );

        state.store.flush_background_writes().await.unwrap();
        let rows = state.store.request_log().list(10, 0).await.unwrap();
        assert_eq!(
            rows.len(),
            1,
            "transparent replay must preserve one logical turn and never double-count telemetry"
        );
        assert_eq!(rows[0].status, 200);
        assert_eq!(rows[0].account_id.as_deref(), Some("acct-mid-turn-cap"));
        assert_eq!(rows[0].transport.as_deref(), Some("ws"));
    }

    #[tokio::test]
    async fn logical_turn_budget_stops_transparent_ws_replay_with_a_terminal_error() {
        let mock = MockWsUpstream::scripted(vec![
            ScriptedTurn::connection_limit_reached(409),
            ScriptedTurn::normal(vec![]),
        ])
        .capturing_raw_frames();
        let mock_base = mock.clone().spawn().await;
        let (base, state) = spawn_with_pinned_account("acct-turn-budget", &mock_base).await;
        state
            .runtime_settings
            .set("max_account_attempts", SettingValue::U64(1))
            .unwrap();

        let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect("downstream WS handshake must succeed");
        let frame = serde_json::json!({
            "type": "response.create",
            "input": [],
            "client_metadata": {"turn_id": "turn-budget-raw-secret"}
        })
        .to_string();
        ws.send(TMessage::Text(frame.clone().into())).await.unwrap();

        let reply = tokio::time::timeout(Duration::from_secs(10), ws.next())
            .await
            .expect("budget exhaustion must answer instead of hanging")
            .expect("a terminal reply")
            .expect("no websocket error");
        let TMessage::Text(reply) = reply else {
            panic!("expected a text frame");
        };
        let value: serde_json::Value = serde_json::from_str(&reply).unwrap();
        assert_eq!(value["type"], "error");
        assert_eq!(value["status"], 400);
        assert_eq!(value["error"]["code"], "logical_turn_attempts_exhausted");
        assert!(
            !reply.contains("turn-budget-raw-secret"),
            "the raw turn id must never appear in a client error"
        );

        let mut frames = mock.raw_frames();
        for _ in 0..50 {
            frames = mock.raw_frames();
            if !frames.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            frames,
            vec![frame],
            "the exhausted aggregate budget must stop the transparent replay"
        );
    }

    /// The 2026-07-24 live regression (request `7a72c42f…`): codex stamps ONE `turn_id` on every
    /// tool-call round of a user turn (the submission id in codex-rs `session/turn_context.rs`),
    /// so the aggregate budget — charged on every upstream send — rejected round
    /// `max_account_attempts + 1` of a perfectly healthy tool loop with an instant
    /// `logical_turn_attempts_exhausted` 400 (live signature: 3 completed rounds, then a 0 ms
    /// failure). A forwarded `response.completed` is progress, not amplification: it must reset
    /// the turn's budget so every later round under the SAME turn id completes. The limit is
    /// pinned to 1 here, so WITHOUT the reset every round after the first would be rejected.
    #[tokio::test]
    async fn healthy_tool_loop_under_one_turn_id_outlives_the_attempt_budget() {
        const ROUNDS: usize = 4;
        let script = (0..ROUNDS)
            .map(|_| ScriptedTurn::normal(vec![]))
            .collect::<Vec<_>>();
        let mock_base = MockWsUpstream::scripted(script).spawn().await;
        let (base, state) = spawn_with_pinned_account("acct-tool-loop", &mock_base).await;
        state
            .runtime_settings
            .set("max_account_attempts", SettingValue::U64(1))
            .unwrap();

        let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect("downstream WS handshake must succeed");
        for round in 0..ROUNDS {
            let frame = serde_json::json!({
                "type": "response.create",
                "input": [],
                "client_metadata": {"turn_id": "one-turn-many-rounds"}
            })
            .to_string();
            ws.send(TMessage::Text(frame.into())).await.unwrap();

            let reply = tokio::time::timeout(Duration::from_secs(10), ws.next())
                .await
                .unwrap_or_else(|_| panic!("round {round} must answer instead of hanging"))
                .expect("a terminal reply")
                .expect("no websocket error");
            let TMessage::Text(reply) = reply else {
                panic!("expected a text frame in round {round}");
            };
            let value: serde_json::Value = serde_json::from_str(&reply).unwrap();
            assert_eq!(
                value["type"], "response.completed",
                "round {round} of a healthy tool loop must complete, not hit the attempt \
                 budget: {reply}"
            );
        }
    }

    /// Honest-liveness (2026-07-24): when the upstream dies BETWEEN turns, the relay must close
    /// the downstream too — codex's anchor ledger lives and dies with its socket
    /// (`client.rs::websocket_connection`), so a still-open downstream would make it trust a dead
    /// anchor and pay a client-visible `previous_response_not_found` round-trip on its next delta.
    /// The old behavior (park and lazily re-dial on the next frame) is exactly what produced the
    /// recurring "Reconnecting n/5" incident.
    #[tokio::test]
    async fn between_turns_upstream_death_closes_the_downstream_honestly() {
        let mock = MockWsUpstream::scripted(vec![ScriptedTurn::normal_then_close(vec![])]);
        let mock_base = mock.clone().spawn().await;
        let (base, state) = spawn_with_pinned_account("acct-honest-close", &mock_base).await;

        let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect("downstream WS handshake must succeed");
        ws.send(TMessage::Text(
            r#"{"type":"response.create","input":[]}"#.to_string().into(),
        ))
        .await
        .unwrap();

        let TMessage::Text(reply) = ws.next().await.expect("a reply").expect("no ws error") else {
            panic!("expected a text frame back from the relay");
        };
        let v: serde_json::Value = serde_json::from_str(&reply).unwrap();
        assert_eq!(v["type"], "response.completed", "turn 1 completes normally");

        // The mock closed its socket right after the completed frame. WITHOUT any further client
        // activity, the relay must mirror that death downstream — a Close (or clean end), never a
        // silent park that hides the dead anchor.
        let next = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("the downstream must close after a between-turns upstream death, not park");
        match next {
            None | Some(Ok(TMessage::Close(_))) => {}
            other => panic!("expected the downstream to close, got: {other:?}"),
        }

        assert_eq!(
            mock.handshake_count(),
            1,
            "a between-turns death must NOT trigger a lazy same-account re-dial"
        );
        let snapshot = state.relay_metrics.snapshot();
        let honest = snapshot
            .iter()
            .find(|(k, _)| k == "honest_close_upstream_drop")
            .map(|(_, v)| *v)
            .unwrap_or(0);
        assert!(
            honest >= 1,
            "expected honest_close_upstream_drop >= 1, got snapshot: {snapshot:?}"
        );
    }

    /// Honest-liveness (2026-07-24): a parked upstream (between turns) is kept warm with
    /// keepalive `Ping`s at the configured cadence — previously the pump's own mid-turn 290s read
    /// deadline poisoned the healthy idle socket (every failing turn in the incident had idled
    /// > 290s), and no pings ever reached the upstream (the relay answers client pings locally).
    /// The parked socket must then serve the next turn WITHOUT a re-dial.
    #[tokio::test]
    async fn parked_upstream_is_kept_alive_by_keepalive_pings() {
        let mock = MockWsUpstream::scripted(vec![
            ScriptedTurn::normal(vec![]),
            ScriptedTurn::normal(vec![]),
        ]);
        let mock_base = mock.clone().spawn().await;
        let idle = polyflare_server::ws_relay::WsRelayIdlePolicy {
            ping_interval: Some(Duration::from_millis(100)),
            idle_budget: Duration::from_secs(30),
        };
        let (base, _state) =
            spawn_with_pinned_account_and_idle("acct-keepalive", &mock_base, idle).await;

        let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect("downstream WS handshake must succeed");
        ws.send(TMessage::Text(
            r#"{"type":"response.create","input":[]}"#.to_string().into(),
        ))
        .await
        .unwrap();
        let TMessage::Text(reply) = ws.next().await.expect("a reply").expect("no ws error") else {
            panic!("expected a text frame back from the relay");
        };
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&reply).unwrap()["type"],
            "response.completed"
        );

        // Park between turns: the relay must ping the UPSTREAM on the configured cadence.
        let mut pings = 0;
        for _ in 0..100 {
            pings = mock.ping_count();
            if pings >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            pings >= 2,
            "expected >= 2 keepalive pings on the parked upstream, got {pings}"
        );

        // The kept-alive socket serves turn 2 with NO second handshake.
        ws.send(TMessage::Text(
            r#"{"type":"response.create","input":[]}"#.to_string().into(),
        ))
        .await
        .unwrap();
        let TMessage::Text(reply2) = ws.next().await.expect("a reply").expect("no ws error") else {
            panic!("expected a text frame back from the relay");
        };
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&reply2).unwrap()["type"],
            "response.completed"
        );
        assert_eq!(
            mock.handshake_count(),
            1,
            "the pinged, parked socket must serve turn 2 without a re-dial"
        );
    }

    /// Honest-liveness (2026-07-24): the keepalive must not run forever. When the between-turns
    /// idle budget elapses with no client activity, the relay deliberately lets the session go —
    /// closing BOTH legs (label `honest_close_idle_budget`) so codex reconnects natively on the
    /// user's return instead of trusting an anchor the relay stopped keeping alive.
    #[tokio::test]
    async fn idle_budget_expiry_closes_both_legs_honestly() {
        let mock = MockWsUpstream::scripted(vec![ScriptedTurn::normal(vec![])]);
        let mock_base = mock.clone().spawn().await;
        let idle = polyflare_server::ws_relay::WsRelayIdlePolicy {
            ping_interval: Some(Duration::from_millis(50)),
            idle_budget: Duration::from_millis(300),
        };
        let (base, state) =
            spawn_with_pinned_account_and_idle("acct-idle-budget", &mock_base, idle).await;

        let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect("downstream WS handshake must succeed");
        ws.send(TMessage::Text(
            r#"{"type":"response.create","input":[]}"#.to_string().into(),
        ))
        .await
        .unwrap();
        let TMessage::Text(reply) = ws.next().await.expect("a reply").expect("no ws error") else {
            panic!("expected a text frame back from the relay");
        };
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&reply).unwrap()["type"],
            "response.completed"
        );

        // No further client activity: after ~300ms the budget expires and the downstream closes.
        let next = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("the downstream must close once the idle budget expires, not park forever");
        match next {
            None | Some(Ok(TMessage::Close(_))) => {}
            other => panic!("expected the downstream to close, got: {other:?}"),
        }

        let snapshot = state.relay_metrics.snapshot();
        let expired = snapshot
            .iter()
            .find(|(k, _)| k == "honest_close_idle_budget")
            .map(|(_, v)| *v)
            .unwrap_or(0);
        assert!(
            expired >= 1,
            "expected honest_close_idle_budget >= 1, got snapshot: {snapshot:?}"
        );
    }

    /// Task 3 (relay-catalog-fixes plan): a TRANSIENT 429 — `retry_after` at/under
    /// `TRANSIENT_RETRY_MAX_SECS` (30s) — must retry the SAME account (wait it out, redial in
    /// place) rather than bench + move, so the conversation's prompt cache is preserved. Mirrors
    /// `durable_error_moves_to_a_second_account` above but scripts a SHORT retry-after (5s, well
    /// under the 30s boundary) instead of a durable one (300s), and asserts the OPPOSITE outcome:
    /// no bench, no move, same owner.
    ///
    /// Two accounts are seeded, deliberately UNPINNED (`RoundRobin` ties to the lexicographically
    /// smaller id, "acct-transient-a", for turn 1 — same tiebreak precedent as
    /// `spawn_with_two_accounts`'s other callers). Both accounts' upstream dial lands on the SAME
    /// mock, scripted `[rate_limited_429(5), normal(vec![])]`.
    #[tokio::test]
    async fn transient_429_retries_same_account_no_move() {
        let mock = MockWsUpstream::scripted(vec![
            ScriptedTurn::rate_limited_429(5),
            ScriptedTurn::normal(vec![]),
        ]);
        let mock_base = mock.clone().spawn().await;

        let (base, state) =
            spawn_with_two_accounts("acct-transient-a", "acct-transient-b", &mock_base).await;

        let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect("downstream WS handshake must succeed");

        // Turn 1: the selector picks one account (RoundRobin ties to "acct-transient-a"); the mock
        // replies with the wrapped 429 error envelope (retry_after 5s, well under the 30s boundary).
        let frame1 = r#"{"type":"response.create","input":[]}"#.to_string();
        ws.send(TMessage::Text(frame1.into())).await.unwrap();

        // The client MUST still receive the 429 error frame verbatim — forwarded first, exactly as
        // the durable path does (Design Note 3 is unchanged by this task).
        let TMessage::Text(err_frame) = ws
            .next()
            .await
            .expect("an error frame")
            .expect("no ws error")
        else {
            panic!("expected a text frame back from the relay");
        };
        let ev: serde_json::Value = serde_json::from_str(&err_frame).unwrap();
        assert_eq!(ev["type"], "error");
        assert_eq!(ev["error"]["code"], "rate_limit_exceeded");

        // Neither account may be benched: a transient 429 skips `bench_account_for_failure`
        // entirely — the opposite of the durable test, which asserts exactly one benched account.
        let mut snaps = vec![
            polyflare_core::AccountSnapshot::new("acct-transient-a"),
            polyflare_core::AccountSnapshot::new("acct-transient-b"),
        ];
        state.runtime.overlay(&mut snaps, now());
        let benched: Vec<String> = snaps
            .iter()
            .filter(|s| s.cooldown_until.is_some())
            .map(|s| s.id.as_str().to_string())
            .collect();
        assert!(
            benched.is_empty(),
            "a transient 429 must NOT bench either account, got: {benched:?}"
        );

        // The relay waits out the ~5s retry-after in place, then redials the SAME account. Drive
        // the follow-up turn now; a generous timeout means a regression (falling through to the
        // durable bench->move path, which redials immediately) still passes quickly, while a hang
        // (no redial at all) fails loudly instead of stalling the suite.
        let frame2 = r#"{"type":"response.create","input":[],"previous_response_id":"whatever"}"#
            .to_string();
        ws.send(TMessage::Text(frame2.into())).await.unwrap();

        let reply = tokio::time::timeout(Duration::from_secs(10), ws.next())
            .await
            .expect("the retry-in-place redial + reply must land within the timeout")
            .expect("a reply")
            .expect("no ws error");
        let TMessage::Text(reply) = reply else {
            panic!("expected a text frame back from the relay");
        };
        let v: serde_json::Value = serde_json::from_str(&reply).unwrap();
        assert_eq!(v["type"], "response.completed");
        let resp_id = v["response"]["id"].as_str().unwrap().to_string();

        // Ownership: the completed turn's owner must be the SAME account the conversation started
        // on — "acct-transient-a" (RoundRobin's tiebreak, per this test's doc comment) — no move.
        let mut owner = None;
        for _ in 0..50 {
            owner = state
                .store
                .continuity()
                .get_anchor_owner(&resp_id)
                .await
                .unwrap();
            if owner.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            owner.as_deref(),
            Some("acct-transient-a"),
            "a transient 429 must retry the SAME account, never move to the other one"
        );

        // The retry-in-place must bump `reconnect_same_account` and never `move_cross_account`.
        let snapshot = state.relay_metrics.snapshot();
        let reconnects = snapshot
            .iter()
            .find(|(k, _)| k == "reconnect_same_account")
            .map(|(_, v)| *v)
            .unwrap_or(0);
        assert!(
            reconnects >= 1,
            "expected reconnect_same_account >= 1 after the transient-429 retry, got snapshot: \
             {snapshot:?}"
        );
        assert!(
            !snapshot.iter().any(|(k, _)| k == "move_cross_account"),
            "a transient-429 retry-in-place must never bump move_cross_account, got: {snapshot:?}"
        );
    }
}
