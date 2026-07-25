mod support;

use support::spawn;

#[tokio::test]
async fn translation_routes_support_authenticated_crud_and_server_side_matching() {
    let upstream = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, _) = spawn(upstream).await;
    let client = reqwest::Client::new();

    let unauthenticated = client
        .get(format!("{pf}/api/translations"))
        .send()
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), 401);

    let initial: serde_json::Value = client
        .get(format!("{pf}/api/translations"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(initial["routes"].as_array().unwrap().len(), 3);
    assert_eq!(initial["recent_requests"], serde_json::json!([]));

    let created_response = client
        .post(format!("{pf}/api/translations"))
        .bearer_auth("secret")
        .json(&serde_json::json!({
            "name": "Exact test route",
            "enabled": true,
            "source_protocol": "anthropic_messages",
            "match_kind": "exact",
            "model_pattern": "CLAUDE-TEST",
            "target_kind": "builtin_provider",
            "target_provider_id": "codex",
            "target_model": "gpt-test",
            "reasoning_effort": "xhigh",
            "priority": 5
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(created_response.status(), 201);
    let created: serde_json::Value = created_response.json().await.unwrap();
    let id = created["id"].as_str().unwrap();

    let matched: serde_json::Value = client
        .post(format!("{pf}/api/translations/test"))
        .bearer_auth("secret")
        .json(&serde_json::json!({
            "source_protocol": "anthropic_messages",
            "model": "claude-test"
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(matched["matched"], true);
    assert_eq!(matched["route"]["id"], id);

    let updated = client
        .patch(format!("{pf}/api/translations/{id}"))
        .bearer_auth("secret")
        .json(&serde_json::json!({
            "name": "Disabled route",
            "enabled": false,
            "source_protocol": "anthropic_messages",
            "match_kind": "prefix",
            "model_pattern": "claude-test",
            "target_kind": "builtin_provider",
            "target_provider_id": "codex",
            "target_model": "gpt-test-v2",
            "reasoning_effort": null,
            "priority": 6
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(updated.status(), 200);

    let unmatched: serde_json::Value = client
        .post(format!("{pf}/api/translations/test"))
        .bearer_auth("secret")
        .json(&serde_json::json!({
            "source_protocol": "anthropic_messages",
            "model": "claude-test-v2"
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(unmatched["matched"], false);
    assert!(unmatched["route"].is_null());

    let deleted = client
        .delete(format!("{pf}/api/translations/{id}"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), 200);
}

#[tokio::test]
async fn translation_route_validation_rejects_unsupported_protocols_and_targets() {
    let upstream = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, _) = spawn(upstream).await;
    let response = reqwest::Client::new()
        .post(format!("{pf}/api/translations"))
        .bearer_auth("secret")
        .json(&serde_json::json!({
            "name": "Invalid",
            "enabled": true,
            "source_protocol": "openai_responses",
            "match_kind": "contains",
            "model_pattern": "x",
            "target_kind": "builtin_provider",
            "target_provider_id": "codex",
            "target_model": "y",
            "reasoning_effort": null,
            "priority": 1
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);

    let reverse = reqwest::Client::new()
        .post(format!("{pf}/api/translations"))
        .bearer_auth("secret")
        .json(&serde_json::json!({
            "name": "Responses to Anthropic",
            "enabled": true,
            "source_protocol": "openai_responses",
            "match_kind": "exact",
            "model_pattern": "claude-test",
            "target_kind": "builtin_provider",
            "target_provider_id": "anthropic",
            "target_model": "claude-test",
            "reasoning_effort": null,
            "priority": 1
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(reverse.status(), 201);
}
