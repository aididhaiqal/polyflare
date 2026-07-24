mod support;

use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::Router;
use polyflare_store::{NewCustomProvider, NewProviderModel};

#[derive(Clone, Debug)]
struct Seen {
    authorization: Option<String>,
    body: serde_json::Value,
}

async fn upstream_handler(
    State(seen): State<Arc<Mutex<Vec<Seen>>>>,
    headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, [(&'static str, &'static str); 1], String) {
    seen.lock().unwrap().push(Seen {
        authorization: headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
        body: serde_json::from_slice(&body).unwrap(),
    });
    (
        StatusCode::OK,
        [("content-type", "text/event-stream")],
        concat!(
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_custom\",",
            "\"usage\":{\"input_tokens\":100,\"output_tokens\":25,\"total_tokens\":125,",
            "\"input_tokens_details\":{\"cached_tokens\":20,\"cache_write_tokens\":9,\"orchestration_input_tokens\":10,",
            "\"orchestration_input_cached_tokens\":3},",
            "\"output_tokens_details\":{\"reasoning_tokens\":5,\"orchestration_output_tokens\":4}}}}\n\n"
        )
        .to_string(),
    )
}

async fn retryable_failure_handler(
    State(seen): State<Arc<Mutex<Vec<Seen>>>>,
    headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, &'static str) {
    seen.lock().unwrap().push(Seen {
        authorization: headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
        body: serde_json::from_slice(&body).unwrap(),
    });
    (StatusCode::TOO_MANY_REQUESTS, "provider exhausted")
}

#[tokio::test]
async fn custom_model_routes_statelessly_and_is_provider_aware_everywhere() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let upstream = Router::new()
        .route("/v1/responses", post(upstream_handler))
        .with_state(seen.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, upstream).await.unwrap();
    });

    let (base, state) = support::spawn("http://127.0.0.1:9".into()).await;
    let now = 100;
    state
        .store
        .providers()
        .create_provider(&NewCustomProvider {
            id: "provider-sakana".into(),
            slug: "sakana".into(),
            display_name: "Sakana".into(),
            base_url: format!("http://{upstream_addr}/v1"),
            wire_api: "responses".into(),
            enabled: true,
            stateless_responses: true,
            allow_private_hosts: true,
            connect_timeout_ms: 1_000,
            stream_idle_timeout_ms: 10_000,
            request_max_retries: 1,
            max_concurrency: Some(4),
            created_at: now,
        })
        .await
        .unwrap();
    state
        .store
        .providers()
        .create_credential(
            "credential-sakana",
            "provider-sakana",
            "primary",
            "secret-fish",
            1.0,
            Some(2),
            now,
            &state.cipher,
        )
        .await
        .unwrap();
    state
        .store
        .providers()
        .create_model(&NewProviderModel {
            id: "model-fugu".into(),
            provider_id: "provider-sakana".into(),
            public_model: "fugu-ultra".into(),
            upstream_model: "fugu-ultra-v1.1".into(),
            display_name: "Fugu Ultra".into(),
            context_window: Some(1_000_000),
            max_output_tokens: None,
            supports_tools: true,
            supports_vision: true,
            supports_parallel_tool_calls: true,
            supports_web_search: true,
            supports_reasoning_summaries: true,
            reasoning_levels_json: r#"["high","xhigh","max"]"#.into(),
            model_info_json: None,
            input_per_million: Some(1.0),
            cached_input_per_million: Some(0.5),
            output_per_million: Some(4.0),
            visible_in_codex: true,
            visible_in_openai: true,
            enabled: true,
            created_at: now,
        })
        .await
        .unwrap();

    let catalog: serde_json::Value = reqwest::Client::new()
        .get(format!("{base}/models"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(catalog["models"]
        .as_array()
        .unwrap()
        .iter()
        .any(|model| model["slug"] == "fugu-ultra"));

    let policy = reqwest::Client::new()
        .patch(format!("{base}/api/provider-models/model-fugu"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "visible_in_codex": false }))
        .send()
        .await
        .unwrap();
    assert_eq!(policy.status(), StatusCode::OK);

    let codex_catalog: serde_json::Value = reqwest::get(format!("{base}/models"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        !codex_catalog["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|model| model["id"] == "fugu-ultra"),
        "Codex discovery policy must hide the model without disabling its route"
    );
    let openai_catalog: serde_json::Value = reqwest::get(format!("{base}/v1/models"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        openai_catalog["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|model| model["id"] == "fugu-ultra"),
        "surface policies are independent"
    );

    let response = reqwest::Client::new()
        .post(format!("{base}/responses"))
        .json(&serde_json::json!({
            "model": "fugu-ultra",
            "stream": true,
            "previous_response_id": "resp_old_owner",
            "input": [{"role": "user", "content": "hello"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let text = response.text().await.unwrap();
    assert!(text.contains("response.completed"));

    {
        let captured = seen.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert_eq!(
            captured[0].authorization.as_deref(),
            Some("Bearer secret-fish")
        );
        assert_eq!(captured[0].body["model"], "fugu-ultra-v1.1");
        assert!(captured[0].body.get("previous_response_id").is_none());
    }

    state.store.flush_background_writes().await.unwrap();
    let rows = state.store.request_log().list(10, 0).await.unwrap();
    let row = rows
        .iter()
        .find(|row| row.provider == "sakana")
        .expect("custom-provider request row");
    assert_eq!(row.target_kind.as_deref(), Some("credential"));
    assert_eq!(
        row.provider_credential_id.as_deref(),
        Some("credential-sakana")
    );
    assert_eq!(row.model.as_deref(), Some("fugu-ultra"));
    assert_eq!(row.upstream_model.as_deref(), Some("fugu-ultra-v1.1"));
    assert_eq!(row.cache_write_input_tokens, Some(9));
    assert_eq!(row.reasoning_tokens, Some(5));
    assert_eq!(row.reported_total_tokens, Some(125));
    assert_eq!(row.usage_schema.as_deref(), Some("openai_responses_v1"));
    assert_eq!(row.usage_source.as_deref(), Some("upstream_response"));
    assert_eq!(row.usage_status.as_deref(), Some("final"));
    assert_eq!(row.orchestration_input_tokens, Some(10));
    assert_eq!(row.orchestration_output_tokens, Some(4));
    assert_eq!(row.orchestration_cached_input_tokens, Some(3));
    let cost = row.cost_usd.expect("custom-provider usage must be priced");
    assert!(
        (cost - 0.000_214_5).abs() < 1e-12,
        "ordinary 190 microdollars plus orchestration 24.5 microdollars, got {cost}"
    );

    let test_result: serde_json::Value = reqwest::Client::new()
        .post(format!("{base}/api/providers/provider-sakana/test"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(test_result["ok"], true);
    assert_eq!(test_result["provider"], "sakana");
    assert_eq!(test_result["model"], "fugu-ultra");
    assert_eq!(test_result["credential_id"], "credential-sakana");

    let policy = reqwest::Client::new()
        .patch(format!("{base}/api/provider-models/model-fugu"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "visible_in_openai": false }))
        .send()
        .await
        .unwrap();
    assert_eq!(policy.status(), StatusCode::OK);
    let hidden_catalog: serde_json::Value = reqwest::get(format!("{base}/v1/models"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        !hidden_catalog["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|model| model["id"] == "fugu-ultra"),
        "a route-only model must disappear from every disabled discovery surface"
    );

    let route_only_response = reqwest::Client::new()
        .post(format!("{base}/responses"))
        .json(&serde_json::json!({
            "model": "fugu-ultra",
            "stream": true,
            "input": [{ "role": "user", "content": "route-only request" }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        route_only_response.status(),
        StatusCode::OK,
        "catalog visibility must not disable an explicitly addressed custom-model route"
    );
    assert!(route_only_response
        .text()
        .await
        .unwrap()
        .contains("response.completed"));
    let captured = seen.lock().unwrap();
    assert_eq!(
        captured.last().unwrap().body["model"],
        "fugu-ultra-v1.1",
        "a route-only request must still use the configured provider model"
    );
}

#[tokio::test]
async fn exhausted_retryable_status_attributes_final_credential_in_logs_and_metrics() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let upstream = Router::new()
        .route("/v1/responses", post(retryable_failure_handler))
        .with_state(seen.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, upstream).await.unwrap();
    });

    let (base, state) = support::spawn("http://127.0.0.1:9".into()).await;
    let now = 100;
    state
        .store
        .providers()
        .create_provider(&NewCustomProvider {
            id: "provider-retry".into(),
            slug: "retry-provider".into(),
            display_name: "Retry Provider".into(),
            base_url: format!("http://{upstream_addr}/v1"),
            wire_api: "responses".into(),
            enabled: true,
            stateless_responses: true,
            allow_private_hosts: true,
            connect_timeout_ms: 1_000,
            stream_idle_timeout_ms: 10_000,
            request_max_retries: 1,
            max_concurrency: Some(4),
            created_at: now,
        })
        .await
        .unwrap();
    for (id, label, secret) in [
        ("credential-retry-a", "first", "secret-retry-a"),
        ("credential-retry-b", "final", "secret-retry-b"),
    ] {
        state
            .store
            .providers()
            .create_credential(
                id,
                "provider-retry",
                label,
                secret,
                1.0,
                Some(2),
                now,
                &state.cipher,
            )
            .await
            .unwrap();
    }
    state
        .store
        .providers()
        .create_model(&NewProviderModel {
            id: "model-retry".into(),
            provider_id: "provider-retry".into(),
            public_model: "retry-model".into(),
            upstream_model: "retry-model-upstream".into(),
            display_name: "Retry Model".into(),
            context_window: None,
            max_output_tokens: None,
            supports_tools: true,
            supports_vision: false,
            supports_parallel_tool_calls: true,
            supports_web_search: false,
            supports_reasoning_summaries: false,
            reasoning_levels_json: "[]".into(),
            model_info_json: None,
            input_per_million: None,
            cached_input_per_million: None,
            output_per_million: None,
            visible_in_codex: true,
            visible_in_openai: true,
            enabled: true,
            created_at: now,
        })
        .await
        .unwrap();

    let response = reqwest::Client::new()
        .post(format!("{base}/responses"))
        .json(&serde_json::json!({
            "model": "retry-model",
            "stream": true,
            "input": "hello"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);

    let attempts = seen.lock().unwrap().clone();
    assert_eq!(attempts.len(), 2);
    assert_eq!(
        attempts[1].authorization.as_deref(),
        Some("Bearer secret-retry-b"),
        "the second credential must be the final attempted target"
    );

    state.store.flush_background_writes().await.unwrap();
    let rows = state.store.request_log().list(10, 0).await.unwrap();
    let row = rows
        .iter()
        .find(|row| row.provider == "retry-provider")
        .expect("custom-provider failure row");
    assert_eq!(row.status, 429);
    assert_eq!(row.target_kind.as_deref(), Some("credential"));
    assert_eq!(
        row.provider_credential_id.as_deref(),
        Some("credential-retry-b")
    );

    let metrics = reqwest::Client::new()
        .get(format!("{base}/metrics"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics.contains(
        "polyflare_upstream_requests_total{provider=\"retry-provider\",target_kind=\"credential\",target_id=\"credential-retry-b\",status=\"429\"} 1"
    ));
    assert!(!metrics.contains("secret-retry-a"));
    assert!(!metrics.contains("secret-retry-b"));
}

#[tokio::test]
async fn exhausted_transport_errors_attribute_final_credential_in_logs_and_metrics() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let unavailable_addr = listener.local_addr().unwrap();
    drop(listener);

    let (base, state) = support::spawn("http://127.0.0.1:9".into()).await;
    let now = 100;
    state
        .store
        .providers()
        .create_provider(&NewCustomProvider {
            id: "provider-transport".into(),
            slug: "transport-provider".into(),
            display_name: "Transport Provider".into(),
            base_url: format!("http://{unavailable_addr}/v1"),
            wire_api: "responses".into(),
            enabled: true,
            stateless_responses: true,
            allow_private_hosts: true,
            connect_timeout_ms: 100,
            stream_idle_timeout_ms: 10_000,
            request_max_retries: 1,
            max_concurrency: Some(4),
            created_at: now,
        })
        .await
        .unwrap();
    for (id, label, secret) in [
        ("credential-transport-a", "first", "secret-transport-a"),
        ("credential-transport-b", "final", "secret-transport-b"),
    ] {
        state
            .store
            .providers()
            .create_credential(
                id,
                "provider-transport",
                label,
                secret,
                1.0,
                Some(2),
                now,
                &state.cipher,
            )
            .await
            .unwrap();
    }
    state
        .store
        .providers()
        .create_model(&NewProviderModel {
            id: "model-transport".into(),
            provider_id: "provider-transport".into(),
            public_model: "transport-model".into(),
            upstream_model: "transport-model-upstream".into(),
            display_name: "Transport Model".into(),
            context_window: None,
            max_output_tokens: None,
            supports_tools: true,
            supports_vision: false,
            supports_parallel_tool_calls: true,
            supports_web_search: false,
            supports_reasoning_summaries: false,
            reasoning_levels_json: "[]".into(),
            model_info_json: None,
            input_per_million: None,
            cached_input_per_million: None,
            output_per_million: None,
            visible_in_codex: true,
            visible_in_openai: true,
            enabled: true,
            created_at: now,
        })
        .await
        .unwrap();

    let response = reqwest::Client::new()
        .post(format!("{base}/responses"))
        .json(&serde_json::json!({
            "model": "transport-model",
            "stream": true,
            "input": "hello"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    state.store.flush_background_writes().await.unwrap();
    let rows = state.store.request_log().list(10, 0).await.unwrap();
    let row = rows
        .iter()
        .find(|row| row.provider == "transport-provider")
        .expect("custom-provider transport failure row");
    assert_eq!(row.status, 503);
    assert_eq!(row.target_kind.as_deref(), Some("credential"));
    assert_eq!(
        row.provider_credential_id.as_deref(),
        Some("credential-transport-b")
    );

    let metrics = reqwest::Client::new()
        .get(format!("{base}/metrics"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics.contains(
        "polyflare_upstream_requests_total{provider=\"transport-provider\",target_kind=\"credential\",target_id=\"credential-transport-b\",status=\"503\"} 1"
    ));
    assert!(!metrics.contains("secret-transport-a"));
    assert!(!metrics.contains("secret-transport-b"));
}

#[tokio::test]
async fn provider_management_never_returns_api_key() {
    let (base, _state) = support::spawn("http://127.0.0.1:9".into()).await;
    let client = reqwest::Client::new();
    let provider: serde_json::Value = client
        .post(format!("{base}/api/providers"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({
            "slug": "sakana",
            "display_name": "Sakana",
            "base_url": "https://api.sakana.ai/v1",
            "stateless_responses": true,
            "stream_idle_timeout_ms": 7200000,
            "request_max_retries": 4
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let provider_id = provider["id"].as_str().unwrap();
    let response = client
        .post(format!("{base}/api/providers/{provider_id}/credentials"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({
            "label": "primary",
            "api_key": "never-return-this-key",
            "routing_weight": 1
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = response.text().await.unwrap();
    assert!(!body.contains("never-return-this-key"));
    assert!(!body.contains("api_key"));
    let credential: serde_json::Value = serde_json::from_str(&body).unwrap();
    let credential_id = credential["id"].as_str().unwrap();

    let patched: serde_json::Value = client
        .patch(format!("{base}/api/provider-credentials/{credential_id}"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "enabled": false }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .expect("management mutations return the dashboard's JSON envelope");
    assert_eq!(patched, serde_json::json!({ "ok": true }));
}

#[tokio::test]
async fn provider_management_rejects_partial_codex_model_info() {
    let (base, _state) = support::spawn("http://127.0.0.1:9".into()).await;
    let client = reqwest::Client::new();
    let provider: serde_json::Value = client
        .post(format!("{base}/api/providers"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({
            "slug": "partial-metadata-provider",
            "display_name": "Partial Metadata Provider",
            "base_url": "https://example.com/v1"
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let provider_id = provider["id"].as_str().unwrap();

    let response = client
        .post(format!("{base}/api/providers/{provider_id}/models"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({
            "public_model": "partial-model-info",
            "upstream_model": "partial-model-info-upstream",
            "display_name": "Partial Model Info",
            "model_info": {}
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response.text().await.unwrap(), "invalid or reserved model");

    let structural_override = client
        .post(format!("{base}/api/providers/{provider_id}/models"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({
            "public_model": "structural-model-info",
            "upstream_model": "structural-model-info-upstream",
            "display_name": "Structural Model Info",
            "model_info": {"truncation_policy": {}}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(structural_override.status(), StatusCode::BAD_REQUEST);

    let generated = client
        .post(format!("{base}/api/providers/{provider_id}/models"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({
            "public_model": "generated-model-info",
            "upstream_model": "generated-model-info-upstream",
            "display_name": "Generated Model Info"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        generated.status(),
        StatusCode::CREATED,
        "omitting the advanced override must retain the generated complete template"
    );

    let extended = client
        .post(format!("{base}/api/providers/{provider_id}/models"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({
            "public_model": "extended-model-info",
            "upstream_model": "extended-model-info-upstream",
            "display_name": "Extended Model Info",
            "model_info": {
                "description": "Operator supplied description",
                "priority": 25
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(extended.status(), StatusCode::CREATED);

    let catalog: serde_json::Value = reqwest::get(format!("{base}/models"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        !catalog["models"]
            .as_array()
            .unwrap()
            .iter()
            .any(|model| model["slug"] == "partial-model-info"),
        "invalid partial metadata must never enter the rich Codex catalog"
    );
    let generated = catalog["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|model| model["slug"] == "generated-model-info")
        .expect("generated default custom model must be present in the rich catalog");
    assert!(generated["supported_reasoning_levels"].is_array());
    assert_eq!(generated["truncation_policy"]["mode"], "tokens");
    assert!(generated["supports_parallel_tool_calls"].is_boolean());
    let extended = catalog["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|model| model["slug"] == "extended-model-info")
        .expect("safe metadata extension must retain a complete generated catalog entry");
    assert_eq!(extended["description"], "Operator supplied description");
    assert_eq!(extended["priority"], 25);
    assert_eq!(extended["truncation_policy"]["mode"], "tokens");
}
