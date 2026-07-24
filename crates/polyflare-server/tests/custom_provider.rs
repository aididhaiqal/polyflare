mod support;

use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
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

async fn models_handler(headers: HeaderMap) -> (StatusCode, axum::Json<serde_json::Value>) {
    assert_eq!(
        headers
            .get("authorization")
            .and_then(|value| value.to_str().ok()),
        Some("Bearer secret-fish")
    );
    (
        StatusCode::OK,
        axum::Json(serde_json::json!({
            "object": "list",
            "data": [
                {"id": "fugu", "object": "model", "owned_by": "sakana"},
                {"id": "fugu-ultra", "object": "model", "owned_by": "sakana"},
                {"id": "fugu-cyber", "object": "model", "owned_by": "sakana"},
                {"id": "not valid/slug", "object": "model"}
            ]
        })),
    )
}

async fn openrouter_models_handler(
    headers: HeaderMap,
) -> (StatusCode, axum::Json<serde_json::Value>) {
    assert_eq!(
        headers
            .get("authorization")
            .and_then(|value| value.to_str().ok()),
        Some("Bearer secret-router")
    );
    (
        StatusCode::OK,
        axum::Json(serde_json::json!({
            "data": [
                {
                    "id": "anthropic/claude-sonnet-5",
                    "name": "Anthropic: Claude Sonnet 5",
                    "context_length": 1_000_000,
                    "architecture": {
                        "input_modalities": ["text", "image"],
                        "output_modalities": ["text"]
                    },
                    "top_provider": {
                        "max_completion_tokens": 128_000
                    },
                    "supported_parameters": [
                        "reasoning", "reasoning_effort", "tool_choice", "tools"
                    ],
                    "reasoning": {
                        "supported_efforts": ["max", "xhigh", "high", "medium", "low"],
                        "default_effort": "high"
                    },
                    "pricing": {
                        "prompt": "0.000002",
                        "completion": "0.00001",
                        "input_cache_read": "0.0000002"
                    }
                },
                {
                    "id": "deepseek/deepseek-r1:free",
                    "name": "DeepSeek R1 (free)",
                    "context_length": 163_840,
                    "architecture": {
                        "input_modalities": ["text"],
                        "output_modalities": ["text"]
                    },
                    "supported_parameters": ["reasoning"]
                }
            ]
        })),
    )
}

#[tokio::test]
async fn model_sync_previews_then_imports_only_selected_ids_without_overwriting_manual_models() {
    let upstream = Router::new().route("/v1/models", get(models_handler));
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
            id: "provider-discovery".into(),
            slug: "sakana-discovery".into(),
            display_name: "Sakana Discovery".into(),
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
            "credential-discovery",
            "provider-discovery",
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
            id: "model-manual-ultra".into(),
            provider_id: "provider-discovery".into(),
            public_model: "fugu-ultra".into(),
            upstream_model: "fugu-ultra-v1.1".into(),
            display_name: "My Fugu Ultra".into(),
            context_window: Some(1_000_000),
            max_output_tokens: None,
            supports_tools: true,
            supports_vision: true,
            supports_parallel_tool_calls: true,
            supports_web_search: true,
            supports_reasoning_summaries: true,
            reasoning_levels_json: r#"["high","xhigh","max"]"#.into(),
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
            created_at: now,
        })
        .await
        .unwrap();

    let preview = reqwest::Client::new()
        .post(format!(
            "{base}/api/providers/provider-discovery/models/discover"
        ))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(preview.status(), StatusCode::OK);
    let preview: serde_json::Value = preview.json().await.unwrap();
    assert_eq!(preview["discovered"], 3);
    assert_eq!(preview["models"].as_array().unwrap().len(), 3);
    assert_eq!(
        preview["models"]
            .as_array()
            .unwrap()
            .iter()
            .find(|model| model["upstream_model"] == "fugu-ultra")
            .unwrap()["state"],
        "configured"
    );
    assert_eq!(
        state
            .store
            .providers()
            .list_models("provider-discovery")
            .await
            .unwrap()
            .len(),
        1,
        "preview must not persist discovered models"
    );

    let empty = reqwest::Client::new()
        .post(format!(
            "{base}/api/providers/provider-discovery/models/sync"
        ))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({"model_ids": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(empty.status(), StatusCode::BAD_REQUEST);

    let oversized = reqwest::Client::new()
        .post(format!(
            "{base}/api/providers/provider-discovery/models/sync"
        ))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({"model_ids": vec!["fugu"; 1_001]}))
        .send()
        .await
        .unwrap();
    assert_eq!(oversized.status(), StatusCode::BAD_REQUEST);

    let response = reqwest::Client::new()
        .post(format!(
            "{base}/api/providers/provider-discovery/models/sync"
        ))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({"model_ids": ["fugu", "fugu-ultra"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let result: serde_json::Value = response.json().await.unwrap();
    assert_eq!(result["discovered"], 3);
    assert_eq!(result["selected"], 2);
    assert_eq!(result["imported"], 1);
    assert_eq!(result["skipped_existing"], 1);

    let models = state
        .store
        .providers()
        .list_models("provider-discovery")
        .await
        .unwrap();
    assert_eq!(models.len(), 2);
    let manual = models
        .iter()
        .find(|model| model.public_model == "fugu-ultra")
        .unwrap();
    assert_eq!(manual.upstream_model, "fugu-ultra-v1.1");
    assert_eq!(manual.display_name, "My Fugu Ultra");
    assert_eq!(manual.reasoning_levels_json, r#"["high","xhigh","max"]"#);
    assert!(models.iter().any(|model| model.public_model == "fugu"));
    assert!(
        models
            .iter()
            .all(|model| model.upstream_model != "fugu-cyber"),
        "an unselected discovery candidate must not be imported"
    );

    let fugu = models
        .iter()
        .find(|model| model.public_model == "fugu")
        .unwrap();
    let response = reqwest::Client::new()
        .patch(format!("{base}/api/provider-models/{}", fugu.id))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({
            "display_name": "Fugu",
            "supports_reasoning_summaries": false,
            "reasoning_levels": ["high", "xhigh"]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let duplicate_efforts = reqwest::Client::new()
        .patch(format!("{base}/api/provider-models/{}", fugu.id))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({
            "reasoning_levels": ["high", "high"]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(duplicate_efforts.status(), StatusCode::BAD_REQUEST);

    let catalog: serde_json::Value = reqwest::Client::new()
        .get(format!("{base}/models"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let fugu = catalog["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|model| model["slug"] == "fugu")
        .expect("edited imported model should be in the rich catalog");
    assert_eq!(fugu["default_reasoning_level"], "high");
    assert_eq!(
        fugu["supported_reasoning_levels"]
            .as_array()
            .unwrap()
            .iter()
            .map(|level| level["effort"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["high", "xhigh"]
    );
}

#[tokio::test]
async fn openrouter_discovery_preserves_ids_and_normalizes_metadata_before_selected_import() {
    let seen = Arc::new(Mutex::new(Vec::<Seen>::new()));
    let upstream = Router::new()
        .route("/api/v1/models", get(openrouter_models_handler))
        .route("/api/v1/responses", post(upstream_handler))
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
            id: "provider-openrouter".into(),
            slug: "openrouter".into(),
            display_name: "OpenRouter".into(),
            base_url: format!("http://{upstream_addr}/api/v1"),
            wire_api: "responses".into(),
            enabled: true,
            stateless_responses: true,
            allow_private_hosts: true,
            connect_timeout_ms: 1_000,
            stream_idle_timeout_ms: 10_000,
            request_max_retries: 0,
            max_concurrency: Some(4),
            created_at: now,
        })
        .await
        .unwrap();
    state
        .store
        .providers()
        .create_credential(
            "credential-openrouter",
            "provider-openrouter",
            "primary",
            "secret-router",
            1.0,
            Some(2),
            now,
            &state.cipher,
        )
        .await
        .unwrap();

    let preview = reqwest::Client::new()
        .post(format!(
            "{base}/api/providers/provider-openrouter/models/discover"
        ))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(preview.status(), StatusCode::OK);
    let preview: serde_json::Value = preview.json().await.unwrap();
    let sonnet = preview["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|model| model["upstream_model"] == "anthropic/claude-sonnet-5")
        .unwrap();
    assert_eq!(
        sonnet["suggested_public_model"],
        "openrouter/anthropic/claude-sonnet-5"
    );
    assert_eq!(sonnet["display_name"], "Anthropic: Claude Sonnet 5");
    assert_eq!(sonnet["context_window"], 1_000_000);
    assert_eq!(sonnet["max_output_tokens"], 128_000);
    assert_eq!(sonnet["supports_tools"], true);
    assert_eq!(sonnet["supports_vision"], true);
    assert_eq!(sonnet["supports_reasoning"], true);
    assert_eq!(
        sonnet["reasoning_levels"],
        serde_json::json!(["max", "xhigh", "high", "medium", "low"])
    );
    assert_eq!(sonnet["input_per_million"], 2.0);
    assert_eq!(sonnet["cached_input_per_million"], 0.2);
    assert_eq!(sonnet["output_per_million"], 10.0);
    let deepseek = preview["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|model| model["upstream_model"] == "deepseek/deepseek-r1:free")
        .unwrap();
    assert_eq!(deepseek["supports_reasoning"], true);
    assert_eq!(deepseek["reasoning_levels"], serde_json::json!([]));

    let imported = reqwest::Client::new()
        .post(format!(
            "{base}/api/providers/provider-openrouter/models/sync"
        ))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({
            "model_ids": ["anthropic/claude-sonnet-5"]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(imported.status(), StatusCode::OK);
    let result: serde_json::Value = imported.json().await.unwrap();
    assert_eq!(result["selected"], 1);
    assert_eq!(result["imported"], 1);

    let models = state
        .store
        .providers()
        .list_models("provider-openrouter")
        .await
        .unwrap();
    assert_eq!(models.len(), 1);
    assert_eq!(
        models[0].public_model,
        "openrouter/anthropic/claude-sonnet-5"
    );
    assert_eq!(models[0].upstream_model, "anthropic/claude-sonnet-5");

    let response = reqwest::Client::new()
        .post(format!("{base}/responses"))
        .json(&serde_json::json!({
            "model": "openrouter/anthropic/claude-sonnet-5",
            "input": [{"role": "user", "content": "hello"}],
            "stream": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    {
        let observed = seen.lock().unwrap();
        assert_eq!(observed.len(), 1);
        assert_eq!(
            observed[0].authorization.as_deref(),
            Some("Bearer secret-router")
        );
        assert_eq!(
            observed[0].body["model"], "anthropic/claude-sonnet-5",
            "the provider-qualified public ID must never replace OpenRouter's exact upstream ID"
        );
    }

    let profile = reqwest::Client::new()
        .post(format!("{base}/api/providers/provider-openrouter/models"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({
            "public_model": "openrouter/anthropic/claude-sonnet-5~reviewer",
            "upstream_model": "anthropic/claude-sonnet-5",
            "display_name": "Claude Sonnet 5 · Reviewer",
            "instruction_mode": "append",
            "instruction_text": "Review the proposed change."
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(profile.status(), StatusCode::CREATED);
    assert!(state
        .store
        .providers()
        .delete_model(&models[0].id)
        .await
        .unwrap());

    let preview = reqwest::Client::new()
        .post(format!(
            "{base}/api/providers/provider-openrouter/models/discover"
        ))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(preview.status(), StatusCode::OK);
    let preview: serde_json::Value = preview.json().await.unwrap();
    let sonnet = preview["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|model| model["upstream_model"] == "anthropic/claude-sonnet-5")
        .unwrap();
    assert_eq!(
        sonnet["state"], "available",
        "a profile sharing the upstream model must not block restoring the base public alias"
    );
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
            instruction_mode: "none".into(),
            instruction_text: String::new(),
            request_overrides_json: "{}".into(),
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
async fn custom_model_profiles_share_upstream_transform_requests_and_log_only_revision() {
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
            id: "provider-profiles".into(),
            slug: "profiles".into(),
            display_name: "Profiles".into(),
            base_url: format!("http://{upstream_addr}/v1"),
            wire_api: "responses".into(),
            enabled: true,
            stateless_responses: true,
            allow_private_hosts: true,
            connect_timeout_ms: 1_000,
            stream_idle_timeout_ms: 10_000,
            request_max_retries: 0,
            max_concurrency: Some(4),
            created_at: now,
        })
        .await
        .unwrap();
    state
        .store
        .providers()
        .create_credential(
            "credential-profiles",
            "provider-profiles",
            "primary",
            "secret-profiles",
            1.0,
            Some(4),
            now,
            &state.cipher,
        )
        .await
        .unwrap();

    let client = reqwest::Client::new();
    for invalid_model in [
        serde_json::json!({
            "public_model": "invalid-none-text",
            "upstream_model": "vendor/shared-upstream",
            "display_name": "Invalid",
            "instruction_mode": "none",
            "instruction_text": "must not be accepted"
        }),
        serde_json::json!({
            "public_model": "invalid-empty-append",
            "upstream_model": "vendor/shared-upstream",
            "display_name": "Invalid",
            "instruction_mode": "append",
            "instruction_text": " "
        }),
        serde_json::json!({
            "public_model": "invalid-output-override",
            "upstream_model": "vendor/shared-upstream",
            "display_name": "Invalid",
            "max_output_tokens": 100,
            "request_overrides": {"max_output_tokens": 101}
        }),
    ] {
        let response = client
            .post(format!("{base}/api/providers/provider-profiles/models"))
            .header("authorization", "Bearer secret")
            .json(&invalid_model)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    for model in [
        serde_json::json!({
            "public_model": "shared-model",
            "upstream_model": "vendor/shared-upstream",
            "display_name": "Shared model",
            "max_output_tokens": 1024
        }),
        serde_json::json!({
            "public_model": "shared-model~reviewer",
            "upstream_model": "vendor/shared-upstream",
            "display_name": "Shared model · Reviewer",
            "max_output_tokens": 1024,
            "instruction_mode": "append",
            "instruction_text": "Review the change and report concrete defects only.",
            "request_overrides": {
                "reasoning_effort": "high",
                "max_output_tokens": 256
            }
        }),
        serde_json::json!({
            "public_model": "shared-model~specialist",
            "upstream_model": "vendor/shared-upstream",
            "display_name": "Shared model · Specialist",
            "max_output_tokens": 1024,
            "instruction_mode": "replace",
            "instruction_text": "Use the specialist operating instructions."
        }),
    ] {
        let response = client
            .post(format!("{base}/api/providers/provider-profiles/models"))
            .header("authorization", "Bearer secret")
            .json(&model)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    for (model, instructions) in [
        ("shared-model", "ORIGINAL NONE"),
        ("shared-model~reviewer", "ORIGINAL APPEND"),
        ("shared-model~specialist", "ORIGINAL REPLACE"),
    ] {
        let response = client
            .post(format!("{base}/responses"))
            .json(&serde_json::json!({
                "model": model,
                "stream": true,
                "instructions": instructions,
                "reasoning": {"effort": "low"},
                "max_output_tokens": 900,
                "input": "hello"
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response
            .text()
            .await
            .unwrap()
            .contains("response.completed"));
    }

    {
        let captured = seen.lock().unwrap();
        assert_eq!(captured.len(), 3);
        assert_eq!(captured[0].body["model"], "vendor/shared-upstream");
        assert_eq!(captured[0].body["instructions"], "ORIGINAL NONE");
        assert_eq!(captured[0].body["reasoning"]["effort"], "low");
        assert_eq!(captured[0].body["max_output_tokens"], 900);

        assert_eq!(captured[1].body["model"], "vendor/shared-upstream");
        assert_eq!(
            captured[1].body["instructions"],
            concat!(
                "ORIGINAL APPEND\n\n",
                "--- PolyFlare model profile ---\n",
                "Review the change and report concrete defects only."
            )
        );
        assert_eq!(captured[1].body["reasoning"]["effort"], "high");
        assert_eq!(captured[1].body["max_output_tokens"], 256);

        assert_eq!(
            captured[2].body["instructions"],
            "Use the specialist operating instructions."
        );
    }

    for model in ["shared-model~reviewer", "shared-model~specialist"] {
        let invalid = client
            .post(format!("{base}/responses"))
            .json(&serde_json::json!({
                "model": model,
                "stream": true,
                "instructions": {"unexpected": true},
                "input": "hello"
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    }
    assert_eq!(
        seen.lock().unwrap().len(),
        3,
        "invalid profile input must fail before the upstream request"
    );

    state.store.flush_background_writes().await.unwrap();
    let rows = state.store.request_log().list(20, 0).await.unwrap();
    let plain = rows
        .iter()
        .find(|row| row.model.as_deref() == Some("shared-model"))
        .unwrap();
    assert_eq!(plain.profile_revision, None);
    for model in ["shared-model~reviewer", "shared-model~specialist"] {
        let row = rows
            .iter()
            .find(|row| row.model.as_deref() == Some(model))
            .unwrap();
        let revision = row.profile_revision.as_deref().unwrap();
        assert_eq!(revision.len(), 16);
        assert!(revision.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }
    let serialized_rows = serde_json::to_string(
        &client
            .get(format!("{base}/api/requests?limit=20"))
            .header("authorization", "Bearer secret")
            .send()
            .await
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(!serialized_rows.contains("Review the change"));
    assert!(!serialized_rows.contains("specialist operating instructions"));
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
            instruction_mode: "none".into(),
            instruction_text: String::new(),
            request_overrides_json: "{}".into(),
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
            instruction_mode: "none".into(),
            instruction_text: String::new(),
            request_overrides_json: "{}".into(),
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
