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

/// A route can be enabled, well-formed and matching, yet have nothing able to serve it: a
/// subscription-OAuth grant authorizes one first-party client shape, so selection excludes those
/// accounts from translated traffic. The API reports that per target so the operator sees the trap
/// in the route list instead of discovering it as a runtime "no eligible account".
#[tokio::test]
async fn translation_targets_report_who_can_actually_serve_them() {
    let upstream = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, state) = spawn(upstream).await;
    let client = reqwest::Client::new();

    // The harness seeds one Codex account. Codex OAuth serves translated traffic — that is the
    // existing product — so the seeded routes are servable.
    let listed: serde_json::Value = client
        .get(format!("{pf}/api/translations"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let codex = &listed["target_capacity"]["builtin_provider:codex"];
    assert_eq!(codex["eligible"], 1);
    assert_eq!(codex["barred_subscription"], 0);

    // Onboard an Anthropic subscription-OAuth account and point a route at it.
    state
        .store
        .accounts()
        .upsert_anthropic_oauth(
            &polyflare_store::Account {
                id: "acct-anthropic".into(),
                chatgpt_account_id: None,
                chatgpt_user_id: None,
                email: "operator@example.test".into(),
                alias: None,
                workspace_id: None,
                workspace_label: None,
                seat_type: None,
                plan_type: "max".into(),
                routing_policy: "normal".into(),
                last_refresh: 0,
                created_at: 0,
                status: "active".into(),
                deactivation_reason: None,
                reset_at: None,
                blocked_at: None,
                security_work_authorized: false,
                usage_cap_percent: None,
                usage_cap_override: false,
                provider: "anthropic".into(),
                pool: None,
            },
            &polyflare_store::PlainTokens {
                access_token: "a".into(),
                refresh_token: "r".into(),
                id_token: String::new(),
            },
            &state.cipher,
            &polyflare_store::NewUpstreamAuth {
                upstream_identity: "seat-1".into(),
                access_token_expires_at: Some(1_800_000_000),
                oauth_contract_version: "anthropic-oauth-2026-07".into(),
                granted_scopes: "user:inference".into(),
            },
        )
        .await
        .unwrap();

    let created = client
        .post(format!("{pf}/api/translations"))
        .bearer_auth("secret")
        .json(&serde_json::json!({
            "name": "Responses clients to Claude",
            "enabled": true,
            "source_protocol": "openai_responses",
            "match_kind": "contains",
            "model_pattern": "sonnet-via-responses",
            "target_kind": "builtin_provider",
            "target_provider_id": "anthropic",
            "target_model": "claude-sonnet-4-6",
            "priority": 500,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);

    let listed: serde_json::Value = client
        .get(format!("{pf}/api/translations"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let anthropic = &listed["target_capacity"]["builtin_provider:anthropic"];
    assert_eq!(
        anthropic["eligible"], 0,
        "a subscription grant cannot serve translated traffic"
    );
    assert_eq!(
        anthropic["barred_subscription"], 1,
        "and the operator must be told WHY it is zero, not just that it is"
    );

    // test_match carries the same verdict for the route it resolved.
    let tested: serde_json::Value = client
        .post(format!("{pf}/api/translations/test"))
        .bearer_auth("secret")
        .json(&serde_json::json!({
            "source_protocol": "openai_responses",
            "model": "sonnet-via-responses",
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(tested["matched"], true);
    assert_eq!(tested["target_capacity"]["eligible"], 0);
    assert_eq!(tested["target_capacity"]["barred_subscription"], 1);

    // No match is NOT a failure: the request is never translated, and a real Claude client is
    // forwarded byte-for-byte instead. Capacity is absent because no target was chosen.
    let unmatched: serde_json::Value = client
        .post(format!("{pf}/api/translations/test"))
        .bearer_auth("secret")
        .json(&serde_json::json!({
            "source_protocol": "anthropic_messages",
            "model": "claude-model-with-no-route",
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(unmatched["matched"], false);
    assert!(unmatched["target_capacity"].is_null());
}
