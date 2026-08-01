//! The native Anthropic-Messages ingress path: `/v1/messages` selects only Anthropic-provider
//! accounts and relays through `AnthropicExecutor`; continuity is a no-op (SPEC-M4 §3.7 — no
//! `previous_response_id`-style anchor exists for this backend, so the watchdog never arms).

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::http::StatusCode;
use axum::routing::post;
use axum::Router;
use futures_util::StreamExt;
use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{CapacityWeighted, Continuity, Executor};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_server::runtime_settings::{RuntimeSettings, RuntimeSettingsFields};
use polyflare_store::{
    Account, NewCustomProvider, NewProviderModel, NewTranslationRoute, PlainTokens, Store,
    TokenCipher, TranslationRouteUpdate,
};
use polyflare_testkit::MockUpstream;

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn anthropic_account(id: &str) -> Account {
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
        usage_cap_percent: None,
        usage_cap_override: false,
        provider: "anthropic".to_string(),
        pool: None,
    }
}

fn codex_account(id: &str) -> Account {
    Account {
        provider: "codex".to_string(),
        ..anthropic_account(id)
    }
}

fn tokens() -> PlainTokens {
    PlainTokens {
        access_token: "tok".to_string(),
        refresh_token: "r".to_string(),
        id_token: "i".to_string(),
    }
}

async fn spawn_polyflare(store: Store, anthropic_upstream: String) -> String {
    spawn_polyflare_full(store, "http://127.0.0.1:9".to_string(), anthropic_upstream).await
}

/// Like `spawn_polyflare`, but also lets a test point the Codex upstream at a live mock (needed to
/// exercise the M4b-wiring cross-provider `/v1/messages` -> Codex path — `spawn_polyflare`'s
/// hardcoded dummy Codex address is fine for the native-only tests above, which never route there).
async fn spawn_polyflare_full(
    store: Store,
    codex_upstream: String,
    anthropic_upstream: String,
) -> String {
    let cipher = TokenCipher::from_key_bytes(&[21u8; 32]).unwrap();
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
        webauthn: None,
        passkey_ceremonies: std::sync::Arc::new(Default::default()),
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
    format!("http://{addr}")
}

#[tokio::test]
async fn messages_relays_to_the_anthropic_executor() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    // Must match the cipher `spawn_polyflare` builds `AppState` with ([21u8; 32]) — otherwise
    // `resolve_core_account`'s `decrypt_tokens` fails and this 500s instead of routing (same
    // fix already applied to `provider_dispatch.rs`'s analogous fixture).
    let cipher = TokenCipher::from_key_bytes(&[21u8; 32]).unwrap();
    store
        .accounts()
        .insert(&anthropic_account("anthropic-1"), &tokens(), &cipher)
        .await
        .unwrap();
    std::mem::forget(dir);

    let mock = MockUpstream::new(vec![
        r#"{"type":"content_block_delta","delta":{"type":"text_delta","text":"hi"}}"#.to_string(),
        r#"{"type":"message_stop"}"#.to_string(),
    ]);
    let handle = mock.clone();
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(store, upstream).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{pf}/v1/messages"))
        .json(&serde_json::json!({
            // Deliberately NOT an opus/sonnet/haiku substring (M4b-wiring: those now alias to
            // Codex — see `messages_aliases_opus_to_codex_and_relays_translated_anthropic_sse`
            // below) so this exercises the genuinely-unaliased native Anthropic path.
            "model": "claude-3-5-legacy-model",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let mut body = String::new();
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        body.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
    }
    assert!(body.contains("content_block_delta"));
    assert!(body.contains("message_stop"));
    assert_eq!(
        handle.last_body().unwrap()["model"],
        "claude-3-5-legacy-model"
    );
}

#[tokio::test]
async fn responses_route_can_translate_to_anthropic_and_back() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[21u8; 32]).unwrap();
    store
        .accounts()
        .insert(&anthropic_account("anthropic-1"), &tokens(), &cipher)
        .await
        .unwrap();
    store
        .translations()
        .create(&NewTranslationRoute {
            id: "responses-to-anthropic".into(),
            name: "Responses to Anthropic".into(),
            enabled: true,
            source_protocol: "openai_responses".into(),
            match_kind: "exact".into(),
            model_pattern: "claude-test".into(),
            target_kind: "builtin_provider".into(),
            target_provider_id: "anthropic".into(),
            target_model: "claude-test-upstream".into(),
            reasoning_effort: None,
            priority: 1,
            created_at: now(),
        })
        .await
        .unwrap();
    std::mem::forget(dir);

    let mock = MockUpstream::new(vec![
        r#"{"type":"message_start","message":{"model":"claude-test-upstream","usage":{"input_tokens":5}}}"#.into(),
        r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#.into(),
        r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hello"}}"#.into(),
        r#"{"type":"content_block_stop","index":0}"#.into(),
        r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}"#.into(),
        r#"{"type":"message_stop"}"#.into(),
    ]);
    let handle = mock.clone();
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(store, upstream).await;

    let response = reqwest::Client::new()
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({
            "model": "claude-test",
            "instructions": "Be concise",
            "input": [{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}],
            "max_output_tokens": 321,
            "stream": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body = response.text().await.unwrap();
    assert!(body.contains("response.output_text.delta"));
    assert!(body.contains("response.completed"));
    assert!(body.contains("\"total_tokens\":7"));

    let forwarded = handle.last_body().unwrap();
    assert_eq!(forwarded["model"], "claude-test-upstream");
    assert_eq!(forwarded["system"], "Be concise");
    assert_eq!(forwarded["max_tokens"], 321);
    assert_eq!(forwarded["stream"], true);

    let buffered = reqwest::Client::new()
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({
            "model": "claude-test",
            "input": "hi again",
            "stream": false
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(buffered.status(), 200);
    assert_eq!(
        buffered
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("application/json")
    );
    let buffered: serde_json::Value = buffered.json().await.unwrap();
    assert_eq!(buffered["status"], "completed");
    assert_eq!(buffered["output"][0]["content"][0]["text"], "hello");
}

#[tokio::test]
async fn messages_route_can_translate_to_a_custom_responses_provider() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[21u8; 32]).unwrap();
    let mock = MockUpstream::new(vec![
        r#"{"type":"response.created","response":{"id":"resp_1","model":"custom-upstream"}}"#.into(),
        r#"{"type":"response.output_item.added","item":{"id":"item_1","type":"message","role":"assistant"}}"#.into(),
        r#"{"type":"response.content_part.added","item_id":"item_1","part":{"type":"output_text","text":""}}"#.into(),
        r#"{"type":"response.output_text.delta","item_id":"item_1","delta":"custom hello"}"#.into(),
        r#"{"type":"response.output_text.done","item_id":"item_1","text":"custom hello"}"#.into(),
        r#"{"type":"response.completed","response":{"id":"resp_1","model":"custom-upstream","status":"completed","usage":{"input_tokens":4,"output_tokens":2}}}"#.into(),
    ]);
    let handle = mock.clone();
    let upstream = mock.spawn().await;
    store
        .providers()
        .create_provider(&NewCustomProvider {
            id: "provider-custom-responses".into(),
            slug: "custom-responses".into(),
            display_name: "Custom Responses".into(),
            base_url: upstream,
            wire_api: "responses".into(),
            enabled: true,
            stateless_responses: true,
            allow_private_hosts: true,
            connect_timeout_ms: 1_000,
            stream_idle_timeout_ms: 10_000,
            request_max_retries: 0,
            max_concurrency: None,
            created_at: now(),
        })
        .await
        .unwrap();
    store
        .providers()
        .create_credential(
            "credential-custom-responses",
            "provider-custom-responses",
            "primary",
            "custom-secret",
            1.0,
            None,
            now(),
            &cipher,
        )
        .await
        .unwrap();
    store
        .providers()
        .create_model(&NewProviderModel {
            id: "model-custom-responses".into(),
            provider_id: "provider-custom-responses".into(),
            public_model: "custom-public".into(),
            upstream_model: "custom-upstream".into(),
            display_name: "Custom".into(),
            context_window: None,
            max_output_tokens: Some(4096),
            supports_tools: true,
            supports_vision: true,
            supports_parallel_tool_calls: true,
            supports_web_search: false,
            supports_reasoning_summaries: false,
            reasoning_levels_json: "[]".into(),
            model_info_json: None,
            instruction_mode: "none".into(),
            instruction_text: String::new(),
            request_overrides_json: "{}".into(),
            input_per_million: None,
            cached_input_per_million: None,
            output_per_million: None,
            visible_in_codex: true,
            visible_in_openai: true,
            enabled: true,
            created_at: now(),
        })
        .await
        .unwrap();
    store
        .translations()
        .create(&NewTranslationRoute {
            id: "messages-to-custom".into(),
            name: "Messages to custom".into(),
            enabled: true,
            source_protocol: "anthropic_messages".into(),
            match_kind: "exact".into(),
            model_pattern: "custom-claude-alias".into(),
            target_kind: "custom_provider".into(),
            target_provider_id: "provider-custom-responses".into(),
            target_model: "custom-public".into(),
            reasoning_effort: None,
            priority: 1,
            created_at: now(),
        })
        .await
        .unwrap();
    std::mem::forget(dir);
    let pf = spawn_polyflare(store, "http://127.0.0.1:9".into()).await;

    let response = reqwest::Client::new()
        .post(format!("{pf}/v1/messages"))
        .json(&serde_json::json!({
            "model": "custom-claude-alias",
            "messages": [{"role":"user","content":"hello"}],
            "max_tokens": 777,
            "stream": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body = response.text().await.unwrap();
    assert!(body.contains("custom hello"));
    assert_eq!(
        handle.last_authorization().as_deref(),
        Some("Bearer custom-secret")
    );
    let forwarded = handle.last_body().unwrap();
    assert_eq!(forwarded["model"], "custom-upstream");
    assert_eq!(forwarded["max_output_tokens"], 777);
    assert_eq!(
        forwarded["prompt_cache_key"].as_str().unwrap().len(),
        48,
        "translated custom Responses turns receive a stable cache-affinity key"
    );
    assert!(forwarded.get("store").is_none());
}

#[tokio::test]
async fn native_custom_messages_failover_attributes_usage_and_cost_to_the_final_target() {
    let failed_upstream = Router::new().route(
        "/v1/messages",
        post(|| async { (StatusCode::TOO_MANY_REQUESTS, "provider exhausted") }),
    );
    let failed_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let failed_addr = failed_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(failed_listener, failed_upstream).await.unwrap();
    });

    let successful_mock = MockUpstream::new(vec![
        r#"{"type":"message_start","message":{"id":"msg_final","model":"anthropic-final","usage":{"input_tokens":80,"cache_creation_input_tokens":10,"cache_read_input_tokens":20,"output_tokens":0}}}"#.into(),
        r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#.into(),
        r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"done"}}"#.into(),
        r#"{"type":"content_block_stop","index":0}"#.into(),
        r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":25}}"#.into(),
        r#"{"type":"message_stop"}"#.into(),
    ]);
    let successful_upstream = successful_mock.spawn().await;

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let inspect_store = store.clone();
    let cipher = TokenCipher::from_key_bytes(&[21u8; 32]).unwrap();
    let created_at = now();
    for (
        provider_id,
        slug,
        base_url,
        credential_id,
        upstream_model,
        input_price,
        cached_price,
        output_price,
    ) in [
        (
            "provider-a-anthropic-failed",
            "anthropic-failed",
            format!("http://{failed_addr}/v1"),
            "credential-a-anthropic-failed",
            "anthropic-failed",
            100.0,
            100.0,
            100.0,
        ),
        (
            "provider-b-anthropic-final",
            "anthropic-final",
            format!("{successful_upstream}/v1"),
            "credential-b-anthropic-final",
            "anthropic-final",
            2.0,
            0.5,
            8.0,
        ),
    ] {
        store
            .providers()
            .create_provider(&NewCustomProvider {
                id: provider_id.into(),
                slug: slug.into(),
                display_name: slug.into(),
                base_url,
                wire_api: "anthropic_messages".into(),
                enabled: true,
                stateless_responses: false,
                allow_private_hosts: true,
                connect_timeout_ms: 1_000,
                stream_idle_timeout_ms: 10_000,
                request_max_retries: 0,
                max_concurrency: None,
                created_at,
            })
            .await
            .unwrap();
        store
            .providers()
            .create_credential(
                credential_id,
                provider_id,
                "primary",
                &format!("secret-{slug}"),
                1.0,
                None,
                created_at,
                &cipher,
            )
            .await
            .unwrap();
        store
            .providers()
            .create_model(&NewProviderModel {
                id: format!("model-{slug}"),
                provider_id: provider_id.into(),
                public_model: "anthropic-balanced".into(),
                upstream_model: upstream_model.into(),
                display_name: "Anthropic Balanced".into(),
                context_window: None,
                max_output_tokens: Some(4096),
                supports_tools: true,
                supports_vision: true,
                supports_parallel_tool_calls: true,
                supports_web_search: false,
                supports_reasoning_summaries: false,
                reasoning_levels_json: "[]".into(),
                model_info_json: None,
                instruction_mode: "none".into(),
                instruction_text: String::new(),
                request_overrides_json: "{}".into(),
                input_per_million: Some(input_price),
                cached_input_per_million: Some(cached_price),
                output_per_million: Some(output_price),
                visible_in_codex: true,
                visible_in_openai: true,
                enabled: true,
                created_at,
            })
            .await
            .unwrap();
    }
    std::mem::forget(dir);
    let pf = spawn_polyflare(store, "http://127.0.0.1:9".into()).await;

    let response = reqwest::Client::new()
        .post(format!("{pf}/v1/messages"))
        .json(&serde_json::json!({
            "model": "anthropic-balanced",
            "messages": [{"role":"user","content":"hello"}],
            "max_tokens": 100,
            "stream": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.text().await.unwrap().contains("message_stop"));

    inspect_store.flush_background_writes().await.unwrap();
    let row = inspect_store
        .request_log()
        .list(10, 0)
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.model.as_deref() == Some("anthropic-balanced"))
        .expect("native custom Messages request row");
    assert_eq!(row.provider, "anthropic-final");
    assert_eq!(
        row.provider_credential_id.as_deref(),
        Some("credential-b-anthropic-final")
    );
    assert_eq!(row.input_tokens, Some(110));
    assert_eq!(row.cached_input_tokens, Some(20));
    assert_eq!(row.cache_write_input_tokens, Some(10));
    assert_eq!(row.output_tokens, Some(25));
    assert_eq!(row.usage_status.as_deref(), Some("final"));
    let cost = row
        .cost_usd
        .expect("final Anthropic target pricing must be applied");
    assert!(
        (cost - 0.000_39).abs() < 1e-12,
        "cost must use the successful target's prices; got {cost}"
    );
}

#[tokio::test]
async fn admitted_claude_session_stays_on_its_completed_custom_target() {
    let upstream_a_mock = MockUpstream::new(vec![r#"{"type":"message_stop"}"#.to_string()]);
    let upstream_a_handle = upstream_a_mock.clone();
    let upstream_a = upstream_a_mock.spawn().await;
    let upstream_b_mock = MockUpstream::new(vec![r#"{"type":"message_stop"}"#.to_string()]);
    let upstream_b_handle = upstream_b_mock.clone();
    let upstream_b = upstream_b_mock.spawn().await;

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let control_store = store.clone();
    let cipher = TokenCipher::from_key_bytes(&[21u8; 32]).unwrap();
    let created_at = now();
    for (provider_id, slug, base_url, credential_id) in [
        (
            "provider-a-claude-affinity",
            "a-claude-affinity",
            format!("{upstream_a}/v1"),
            "credential-a-claude-affinity",
        ),
        (
            "provider-b-claude-affinity",
            "b-claude-affinity",
            format!("{upstream_b}/v1"),
            "credential-b-claude-affinity",
        ),
    ] {
        store
            .providers()
            .create_provider(&NewCustomProvider {
                id: provider_id.into(),
                slug: slug.into(),
                display_name: slug.into(),
                base_url,
                wire_api: "anthropic_messages".into(),
                enabled: true,
                stateless_responses: false,
                allow_private_hosts: true,
                connect_timeout_ms: 1_000,
                stream_idle_timeout_ms: 10_000,
                request_max_retries: 0,
                max_concurrency: None,
                created_at,
            })
            .await
            .unwrap();
        store
            .providers()
            .create_credential(
                credential_id,
                provider_id,
                "primary",
                &format!("secret-{slug}"),
                1.0,
                None,
                created_at,
                &cipher,
            )
            .await
            .unwrap();
        store
            .providers()
            .create_model(&NewProviderModel {
                id: format!("model-{slug}"),
                provider_id: provider_id.into(),
                public_model: "claude-affinity-model".into(),
                upstream_model: format!("upstream-{slug}"),
                display_name: "Claude Affinity Model".into(),
                context_window: None,
                max_output_tokens: Some(4_096),
                supports_tools: true,
                supports_vision: true,
                supports_parallel_tool_calls: true,
                supports_web_search: false,
                supports_reasoning_summaries: false,
                reasoning_levels_json: "[]".into(),
                model_info_json: None,
                instruction_mode: "none".into(),
                instruction_text: String::new(),
                request_overrides_json: "{}".into(),
                input_per_million: None,
                cached_input_per_million: None,
                output_per_million: None,
                visible_in_codex: true,
                visible_in_openai: true,
                enabled: true,
                created_at,
            })
            .await
            .unwrap();
    }
    control_store
        .providers()
        .set_credential_health(
            "credential-a-claude-affinity",
            "cooldown",
            Some(i64::MAX),
            created_at,
        )
        .await
        .unwrap();
    std::mem::forget(dir);
    let pf = spawn_polyflare(store, "http://127.0.0.1:9".into()).await;

    let client = reqwest::Client::new();
    let send_turn = || {
        client
            .post(format!("{pf}/v1/messages"))
            .header("anthropic-version", "2023-06-01")
            .header("anthropic-beta", "claude-code-20250219,oauth-2025-04-20")
            .header("user-agent", "claude-cli/2.1.218 (external, sdk-ts)")
            .header(
                "x-claude-code-session-id",
                "c38f98c8-7c2a-4e93-aa3d-a79df7a7015f",
            )
            .json(&serde_json::json!({
                "model": "claude-affinity-model",
                "max_tokens": 100,
                "messages": [{"role":"user","content":"hello"}],
                "stream": true
            }))
    };
    let first = send_turn().send().await.unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    assert!(first.text().await.unwrap().contains("message_stop"));
    assert_eq!(upstream_b_handle.request_count(), 1);
    assert_eq!(upstream_a_handle.request_count(), 0);

    control_store
        .providers()
        .set_credential_health(
            "credential-a-claude-affinity",
            "healthy",
            None,
            created_at + 1,
        )
        .await
        .unwrap();
    let repeated = send_turn().send().await.unwrap();
    assert_eq!(repeated.status(), StatusCode::OK);
    assert!(repeated.text().await.unwrap().contains("message_stop"));
    assert_eq!(
        upstream_b_handle.request_count(),
        2,
        "the admitted Claude session must keep its warm target after the other target recovers"
    );
    assert_eq!(
        upstream_a_handle.request_count(),
        0,
        "routing fairness must not displace a healthy admitted-session affinity"
    );
}

#[tokio::test]
async fn native_custom_messages_image_uses_only_a_vision_capable_target() {
    let non_vision_mock = MockUpstream::error_status(400, "vision is unsupported");
    let non_vision_handle = non_vision_mock.clone();
    let non_vision_upstream = non_vision_mock.spawn().await;
    let vision_mock = MockUpstream::new(vec![r#"{"type":"message_stop"}"#.to_string()]);
    let vision_handle = vision_mock.clone();
    let vision_upstream = vision_mock.spawn().await;

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[21u8; 32]).unwrap();
    let created_at = now();
    for (provider_id, slug, base_url, credential_id, supports_vision) in [
        (
            "provider-a-no-vision",
            "a-no-vision",
            format!("{non_vision_upstream}/v1"),
            "credential-a-no-vision",
            false,
        ),
        (
            "provider-b-with-vision",
            "b-with-vision",
            format!("{vision_upstream}/v1"),
            "credential-b-with-vision",
            true,
        ),
    ] {
        store
            .providers()
            .create_provider(&NewCustomProvider {
                id: provider_id.into(),
                slug: slug.into(),
                display_name: slug.into(),
                base_url,
                wire_api: "anthropic_messages".into(),
                enabled: true,
                stateless_responses: false,
                allow_private_hosts: true,
                connect_timeout_ms: 1_000,
                stream_idle_timeout_ms: 10_000,
                request_max_retries: 0,
                max_concurrency: None,
                created_at,
            })
            .await
            .unwrap();
        store
            .providers()
            .create_credential(
                credential_id,
                provider_id,
                "primary",
                &format!("secret-{slug}"),
                1.0,
                None,
                created_at,
                &cipher,
            )
            .await
            .unwrap();
        store
            .providers()
            .create_model(&NewProviderModel {
                id: format!("model-{slug}"),
                provider_id: provider_id.into(),
                public_model: "claude-vision-balanced".into(),
                upstream_model: format!("upstream-{slug}"),
                display_name: "Claude Vision Balanced".into(),
                context_window: None,
                max_output_tokens: Some(4_096),
                supports_tools: true,
                supports_vision,
                supports_parallel_tool_calls: true,
                supports_web_search: false,
                supports_reasoning_summaries: false,
                reasoning_levels_json: "[]".into(),
                model_info_json: None,
                instruction_mode: "none".into(),
                instruction_text: String::new(),
                request_overrides_json: "{}".into(),
                input_per_million: None,
                cached_input_per_million: None,
                output_per_million: None,
                visible_in_codex: true,
                visible_in_openai: true,
                enabled: true,
                created_at,
            })
            .await
            .unwrap();
    }
    std::mem::forget(dir);
    let pf = spawn_polyflare(store, "http://127.0.0.1:9".into()).await;

    let response = reqwest::Client::new()
        .post(format!("{pf}/v1/messages"))
        .json(&serde_json::json!({
            "model": "claude-vision-balanced",
            "max_tokens": 100,
            "messages": [{
                "role": "user",
                "content": [{
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": "image/png",
                        "data": "opaque"
                    }
                }]
            }],
            "stream": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.text().await.unwrap().contains("message_stop"));
    assert_eq!(
        non_vision_handle.request_count(),
        0,
        "an Anthropic image request must not be sent to a target lacking vision"
    );
    assert_eq!(vision_handle.request_count(), 1);
}

#[tokio::test]
async fn responses_route_can_translate_to_a_custom_anthropic_provider() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[21u8; 32]).unwrap();
    let mock = MockUpstream::new(vec![
        r#"{"type":"message_start","message":{"model":"anthropic-upstream","usage":{"input_tokens":3}}}"#.into(),
        r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#.into(),
        r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"anthropic custom"}}"#.into(),
        r#"{"type":"content_block_stop","index":0}"#.into(),
        r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}"#.into(),
        r#"{"type":"message_stop"}"#.into(),
    ]);
    let handle = mock.clone();
    let upstream = mock.spawn().await;
    store
        .providers()
        .create_provider(&NewCustomProvider {
            id: "provider-custom-anthropic".into(),
            slug: "custom-anthropic".into(),
            display_name: "Custom Anthropic".into(),
            base_url: format!("{upstream}/v1"),
            wire_api: "anthropic_messages".into(),
            enabled: true,
            stateless_responses: false,
            allow_private_hosts: true,
            connect_timeout_ms: 1_000,
            stream_idle_timeout_ms: 10_000,
            request_max_retries: 0,
            max_concurrency: None,
            created_at: now(),
        })
        .await
        .unwrap();
    store
        .providers()
        .create_credential(
            "credential-custom-anthropic",
            "provider-custom-anthropic",
            "primary",
            "anthropic-secret",
            1.0,
            None,
            now(),
            &cipher,
        )
        .await
        .unwrap();
    store
        .providers()
        .create_model(&NewProviderModel {
            id: "model-custom-anthropic".into(),
            provider_id: "provider-custom-anthropic".into(),
            public_model: "anthropic-public".into(),
            upstream_model: "anthropic-upstream".into(),
            display_name: "Anthropic Custom".into(),
            context_window: None,
            max_output_tokens: Some(4096),
            supports_tools: true,
            supports_vision: true,
            supports_parallel_tool_calls: true,
            supports_web_search: false,
            supports_reasoning_summaries: false,
            reasoning_levels_json: "[]".into(),
            model_info_json: None,
            instruction_mode: "none".into(),
            instruction_text: String::new(),
            request_overrides_json: "{}".into(),
            input_per_million: None,
            cached_input_per_million: None,
            output_per_million: None,
            visible_in_codex: true,
            visible_in_openai: true,
            enabled: true,
            created_at: now(),
        })
        .await
        .unwrap();
    store
        .translations()
        .create(&NewTranslationRoute {
            id: "responses-to-custom-anthropic".into(),
            name: "Responses to custom Anthropic".into(),
            enabled: true,
            source_protocol: "openai_responses".into(),
            match_kind: "exact".into(),
            model_pattern: "anthropic-alias".into(),
            target_kind: "custom_provider".into(),
            target_provider_id: "provider-custom-anthropic".into(),
            target_model: "anthropic-public".into(),
            reasoning_effort: None,
            priority: 1,
            created_at: now(),
        })
        .await
        .unwrap();
    std::mem::forget(dir);
    let pf = spawn_polyflare(store, "http://127.0.0.1:9".into()).await;

    let response = reqwest::Client::new()
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({
            "model": "anthropic-alias",
            "input": "hello",
            "max_output_tokens": 222,
            "stream": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body = response.text().await.unwrap();
    assert!(body.contains("anthropic custom"));
    let headers = handle.last_headers().unwrap();
    assert_eq!(
        headers
            .get("x-api-key")
            .and_then(|value| value.to_str().ok()),
        Some("anthropic-secret")
    );
    assert_eq!(
        headers
            .get("anthropic-version")
            .and_then(|value| value.to_str().ok()),
        Some("2023-06-01")
    );
    assert!(headers.get("authorization").is_none());
    let forwarded = handle.last_body().unwrap();
    assert_eq!(forwarded["model"], "anthropic-upstream");
    assert_eq!(forwarded["max_tokens"], 222);
}

#[tokio::test]
async fn disabling_a_seeded_translation_route_restores_native_anthropic_routing() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[21u8; 32]).unwrap();
    store
        .accounts()
        .insert(&anthropic_account("anthropic-1"), &tokens(), &cipher)
        .await
        .unwrap();
    let route = store
        .translations()
        .get("default-anthropic-opus")
        .await
        .unwrap()
        .unwrap();
    store
        .translations()
        .update(
            &route.id,
            &TranslationRouteUpdate {
                name: route.name,
                enabled: false,
                source_protocol: route.source_protocol,
                match_kind: route.match_kind,
                model_pattern: route.model_pattern,
                target_kind: route.target_kind,
                target_provider_id: route.target_provider_id,
                target_model: route.target_model,
                reasoning_effort: route.reasoning_effort,
                priority: route.priority,
                updated_at: now(),
            },
        )
        .await
        .unwrap();
    std::mem::forget(dir);

    let mock = MockUpstream::new(vec![
        r#"{"type":"content_block_delta","delta":{"type":"text_delta","text":"native"}}"#
            .to_string(),
        r#"{"type":"message_stop"}"#.to_string(),
    ]);
    let handle = mock.clone();
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(store, upstream).await;
    let response = reqwest::Client::new()
        .post(format!("{pf}/v1/messages"))
        .json(&serde_json::json!({
            "model": "claude-opus-4-1-20250805",
            "messages": [{"role": "user", "content": "hi"}],
            "stream": true
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    assert_eq!(
        handle.last_body().unwrap()["model"],
        "claude-opus-4-1-20250805"
    );
}

#[tokio::test]
async fn messages_returns_503_when_pool_has_no_anthropic_account() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    std::mem::forget(dir);

    let pf = spawn_polyflare(store, "http://127.0.0.1:9".to_string()).await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{pf}/v1/messages"))
        // Deliberately NOT an opus/sonnet/haiku substring — see the model-choice comment in
        // `messages_relays_to_the_anthropic_executor` above.
        .json(&serde_json::json!({"model": "claude-3-5-legacy-model", "messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 503);
}

/// M4b-wiring, the headline cross-provider path: a `claude-opus-...` model string aliases to
/// Codex's `gpt-5.6-sol` @ high effort (`polyflare_server::alias::lookup_alias`). This asserts (a)
/// the upstream Codex mock received the remapped `model` + injected `reasoning.effort`, and (b)
/// the client-facing body is genuine Anthropic-Messages SSE (`message_start`/`content_block_*`/
/// `message_stop`), not the raw OpenAI-Responses shape the mock actually emitted.
///
/// M4 Outcome 3: this client explicitly sends `"stream": true`, so the streaming path must stay
/// byte-identical to before Outcome 3 existed — see
/// `messages_aliased_to_codex_buffers_a_json_message_when_client_does_not_stream` below for the
/// (now-default) non-streaming buffered-Message path.
#[tokio::test]
async fn messages_aliases_opus_to_codex_and_relays_translated_anthropic_sse() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[21u8; 32]).unwrap();
    store
        .accounts()
        .insert(&codex_account("codex-1"), &tokens(), &cipher)
        .await
        .unwrap();
    std::mem::forget(dir);

    // A scripted OpenAI-Responses turn: one text block, "hi".
    let mock = MockUpstream::new(vec![
        r#"{"type":"response.created","response":{"id":"resp_1","status":"in_progress","model":"gpt-5.6-sol","usage":null}}"#.to_string(),
        r#"{"type":"response.output_item.added","item":{"id":"item_1","type":"message","role":"assistant","content":[]}}"#.to_string(),
        r#"{"type":"response.content_part.added","item_id":"item_1","part":{"type":"output_text","text":"","annotations":[]}}"#.to_string(),
        r#"{"type":"response.output_text.delta","item_id":"item_1","delta":"hi"}"#.to_string(),
        r#"{"type":"response.output_text.done","item_id":"item_1","text":"hi"}"#.to_string(),
        r#"{"type":"response.completed","response":{"id":"resp_1","status":"completed","model":"gpt-5.6-sol","usage":{"output_tokens":1}}}"#.to_string(),
    ]);
    let handle = mock.clone();
    let codex_upstream = mock.spawn().await;
    let pf = spawn_polyflare_full(store, codex_upstream, "http://127.0.0.1:9".to_string()).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{pf}/v1/messages"))
        .json(&serde_json::json!({
            "model": "claude-opus-4-1-20250805",
            "messages": [{"role": "user", "content": "hi"}],
            "stream": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get(reqwest::header::CONTENT_TYPE).unwrap(),
        "text/event-stream"
    );

    // (a) the Codex upstream received the remapped model + injected reasoning effort.
    let sent = handle.last_body().unwrap();
    assert_eq!(sent["model"], "gpt-5.6-sol");
    assert_eq!(sent["reasoning"]["effort"], "high");

    // (b) the client sees Anthropic-Messages SSE, not the raw OpenAI-Responses shape.
    let mut body = String::new();
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        body.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
    }
    assert!(body.contains("message_start"), "body: {body}");
    assert!(body.contains("content_block_delta"), "body: {body}");
    assert!(body.contains("message_stop"), "body: {body}");
    assert!(
        !body.contains("response.output_text.delta"),
        "client must never see the raw OpenAI-Responses event shape: {body}"
    );
}

/// M4 Outcome 3: Anthropic's Messages API defaults `stream:false` — a client that omits `stream`
/// entirely (as here) must get back exactly ONE buffered `application/json` Anthropic `Message`,
/// not SSE. Same scripted Codex turn as the `stream:true` test above, folded into a Message
/// instead of relayed as frames.
#[tokio::test]
async fn messages_aliased_to_codex_buffers_a_json_message_when_client_does_not_stream() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[21u8; 32]).unwrap();
    store
        .accounts()
        .insert(&codex_account("codex-2"), &tokens(), &cipher)
        .await
        .unwrap();
    std::mem::forget(dir);

    let mock = MockUpstream::new(vec![
        r#"{"type":"response.created","response":{"id":"resp_2","status":"in_progress","model":"gpt-5.6-sol","usage":null}}"#.to_string(),
        r#"{"type":"response.output_item.added","item":{"id":"item_1","type":"message","role":"assistant","content":[]}}"#.to_string(),
        r#"{"type":"response.content_part.added","item_id":"item_1","part":{"type":"output_text","text":"","annotations":[]}}"#.to_string(),
        r#"{"type":"response.output_text.delta","item_id":"item_1","delta":"hi"}"#.to_string(),
        r#"{"type":"response.output_text.done","item_id":"item_1","text":"hi"}"#.to_string(),
        r#"{"type":"response.completed","response":{"id":"resp_2","status":"completed","model":"gpt-5.6-sol","usage":{"output_tokens":1}}}"#.to_string(),
    ]);
    let codex_upstream = mock.spawn().await;
    let pf = spawn_polyflare_full(store, codex_upstream, "http://127.0.0.1:9".to_string()).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{pf}/v1/messages"))
        .json(&serde_json::json!({
            "model": "claude-opus-4-1-20250805",
            "messages": [{"role": "user", "content": "hi"}]
            // No "stream" field -- Anthropic's own default (stream:false).
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get(reqwest::header::CONTENT_TYPE).unwrap(),
        "application/json"
    );

    let message: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(message["type"], "message");
    assert_eq!(message["role"], "assistant");
    assert_eq!(message["stop_reason"], "end_turn");
    assert_eq!(
        message["content"],
        serde_json::json!([{"type": "text", "text": "hi"}])
    );
}

/// An aliased-to-Codex request with no Codex account in the pool: `filter_by_provider(Codex)`
/// leaves no candidates, so this 503s exactly like the native path's empty-pool case above — it
/// must NOT silently fall back to the (present) Anthropic account, since an aliased turn's
/// translated body is Codex-shaped and would be meaningless sent to an Anthropic backend.
#[tokio::test]
async fn messages_aliased_to_codex_returns_503_when_no_codex_account_is_seeded() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[21u8; 32]).unwrap();
    store
        .accounts()
        .insert(&anthropic_account("anthropic-1"), &tokens(), &cipher)
        .await
        .unwrap();
    std::mem::forget(dir);

    let pf = spawn_polyflare(store, "http://127.0.0.1:9".to_string()).await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{pf}/v1/messages"))
        .json(&serde_json::json!({
            "model": "claude-opus-4-1-20250805",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 503);
}

/// A real Claude Code request must reach upstream byte-identically, with only the credential
/// swapped. This is the end-to-end proof of the pass-through path: the model name here is
/// deliberately unaliased so the native path (not a translation) handles it.
#[tokio::test]
async fn a_real_claude_client_request_is_forwarded_byte_for_byte() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[21u8; 32]).unwrap();
    store
        .accounts()
        .insert(&anthropic_account("anthropic-1"), &tokens(), &cipher)
        .await
        .unwrap();
    std::mem::forget(dir);

    let mock = MockUpstream::new(vec![r#"{"type":"message_stop"}"#.to_string()]);
    let handle = mock.clone();
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(store, upstream).await;

    // Key order and spacing are deliberately NOT what serde_json would emit on a round-trip, so a
    // parse/re-serialize anywhere in the path would visibly change these bytes.
    let raw = concat!(
        r#"{"model":"claude-3-5-legacy-model","max_tokens":4096,"#,
        r#""messages":[{"role":"user","content":"hi"}],"stream":true}"#
    );

    let resp = reqwest::Client::new()
        .post(format!("{pf}/v1/messages"))
        .header("content-type", "application/json")
        .header("anthropic-version", "2023-06-01")
        .header("anthropic-beta", "claude-code-20250219,oauth-2025-04-20")
        .header("user-agent", "claude-cli/2.1.218 (external, sdk-ts)")
        .header("x-app", "cli")
        .header(
            "x-claude-code-session-id",
            "c38f98c8-7c2a-4e93-aa3d-a79df7a7015f",
        )
        .header("x-stainless-package-version", "0.94.0")
        .header("authorization", "Bearer CALLER-SECRET")
        .header("cookie", "session=should-not-travel")
        .body(raw)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let _ = resp.bytes().await.unwrap();

    let upstream_headers = handle.last_headers().unwrap();
    // The caller's credential was replaced by the selected account's, not forwarded.
    assert_eq!(upstream_headers.get("authorization").unwrap(), "Bearer tok");
    assert!(upstream_headers.get("cookie").is_none());
    // The client's own protocol envelope survived, including beta order and session id.
    assert_eq!(
        upstream_headers.get("anthropic-beta").unwrap(),
        "claude-code-20250219,oauth-2025-04-20"
    );
    assert_eq!(
        upstream_headers.get("x-claude-code-session-id").unwrap(),
        "c38f98c8-7c2a-4e93-aa3d-a79df7a7015f"
    );
    assert_eq!(
        upstream_headers.get("user-agent").unwrap(),
        "claude-cli/2.1.218 (external, sdk-ts)"
    );
    assert_eq!(upstream_headers.get_all("content-type").iter().count(), 1);
    assert_eq!(
        handle.last_body().unwrap()["model"],
        "claude-3-5-legacy-model"
    );
}

/// The same request WITHOUT Claude Code's markers is still served — it simply takes the
/// materialized path instead of byte pass-through, and none of the client's headers travel.
#[tokio::test]
async fn a_generic_sdk_request_is_served_without_borrowing_the_claude_envelope() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[21u8; 32]).unwrap();
    store
        .accounts()
        .insert(&anthropic_account("anthropic-1"), &tokens(), &cipher)
        .await
        .unwrap();
    std::mem::forget(dir);

    let mock = MockUpstream::new(vec![r#"{"type":"message_stop"}"#.to_string()]);
    let handle = mock.clone();
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(store, upstream).await;

    let resp = reqwest::Client::new()
        .post(format!("{pf}/v1/messages"))
        .header("user-agent", "anthropic-sdk-python/0.40.0")
        .header("x-stainless-package-version", "0.40.0")
        .json(&serde_json::json!({
            "model": "claude-3-5-legacy-model",
            "max_tokens": 4096,
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let _ = resp.bytes().await.unwrap();

    let upstream_headers = handle.last_headers().unwrap();
    assert_eq!(upstream_headers.get("authorization").unwrap(), "Bearer tok");
    // A non-Claude client's identity is NOT forwarded: PolyFlare must not dress arbitrary traffic
    // up in a first-party envelope, and it does not invent one either.
    assert!(upstream_headers.get("x-claude-code-session-id").is_none());
    assert_eq!(
        upstream_headers
            .get("user-agent")
            .map(|v| v.to_str().unwrap()),
        None,
        "the generic client's own user-agent is not relayed"
    );
    assert_eq!(
        handle.last_body().unwrap()["model"],
        "claude-3-5-legacy-model"
    );
}
