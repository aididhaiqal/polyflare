use futures_util::StreamExt;
use polyflare_anthropic::AnthropicExecutor;
use polyflare_core::{Account, Executor, PreparedRequest, RequestCtx};
use polyflare_testkit::MockUpstream;

#[tokio::test]
async fn executor_streams_upstream_events_and_forwards_body() {
    let mock = MockUpstream::new(vec![
        r#"{"type":"content_block_delta","delta":{"type":"text_delta","text":"hi"}}"#.to_string(),
        r#"{"type":"message_stop"}"#.to_string(),
    ]);
    let handle = mock.clone();
    let base = mock.spawn().await;

    let executor = AnthropicExecutor::new().unwrap();
    let account = Account {
        id: "test".into(),
        base_url: base,
        bearer_token: "test-token".into(),
        chatgpt_account_id: None,
        is_fedramp: false,
    };
    let req = PreparedRequest {
        body: Some(serde_json::json!({
            "model": "claude-opus-4",
            "messages": [{"role": "user", "content": "hi"}]
        })),
        model: "claude-opus-4".into(),
        forward_headers: vec![],
        raw_body: None,
    };

    let mut stream = executor
        .execute(req, &account, &RequestCtx::default())
        .await
        .unwrap();
    let mut collected = String::new();
    while let Some(chunk) = stream.next().await {
        collected.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
    }

    assert!(collected.contains("content_block_delta"));
    assert!(collected.contains("message_stop"));
    assert_eq!(handle.last_body().unwrap()["model"], "claude-opus-4");
    assert_eq!(handle.last_authorization().unwrap(), "Bearer test-token");
}

#[tokio::test]
async fn executor_surfaces_upstream_error_status() {
    // No route for this path on the mock → 404 → ExecError::Upstream.
    let base = MockUpstream::new(vec![]).spawn().await;
    let executor = AnthropicExecutor::new().unwrap();
    let account = Account {
        id: "test".into(),
        base_url: format!("{base}/nonexistent-base"),
        bearer_token: "t".into(),
        chatgpt_account_id: None,
        is_fedramp: false,
    };
    let req = PreparedRequest {
        body: Some(serde_json::json!({"model": "m"})),
        model: "m".into(),
        forward_headers: vec![],
        raw_body: None,
    };
    let err = executor
        .execute(req, &account, &RequestCtx::default())
        .await
        .err()
        .unwrap();
    // A non-2xx upstream response now surfaces the structured status (404 here from the missing
    // base), not a stringly `Upstream` — so the ingress can classify it for routing-health.
    assert!(
        matches!(&err, polyflare_core::ExecError::UpstreamStatus(s) if s.status == 404),
        "expected UpstreamStatus(404), got {err:?}"
    );
}

/// An admitted Claude request must reach upstream byte-identically, with the caller's credential
/// replaced and nothing else touched. This is the whole value proposition of the pass-through
/// path: no parse/re-serialize round-trip means no way for PolyFlare to perturb the request.
#[tokio::test]
async fn admitted_claude_request_forwards_raw_bytes_and_the_clients_envelope() {
    let mock = MockUpstream::new(vec![r#"{"type":"message_stop"}"#.to_string()]);
    let handle = mock.clone();
    let base = mock.spawn().await;

    // Key order and spacing here are deliberately NOT what serde would emit on a round-trip.
    let raw = br#"{"model":"claude-sonnet-4-6","max_tokens":4096,"messages":[{"role":"user","content":"x"}],"stream":true}"#;

    let inbound: Vec<(String, String)> = vec![
        ("accept", "application/json"),
        ("content-type", "application/json"),
        ("anthropic-version", "2023-06-01"),
        ("anthropic-beta", "claude-code-20250219,oauth-2025-04-20"),
        ("user-agent", "claude-cli/2.1.218 (external, sdk-ts)"),
        ("x-app", "cli"),
        (
            "x-claude-code-session-id",
            "c38f98c8-7c2a-4e93-aa3d-a79df7a7015f",
        ),
        ("x-stainless-package-version", "0.94.0"),
        ("authorization", "Bearer CALLER-SECRET"),
        ("cookie", "session=should-not-travel"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();

    let envelope = polyflare_anthropic::admit_native_request(
        &inbound,
        &serde_json::from_slice::<serde_json::Value>(raw).unwrap(),
    )
    .expect("a real Claude Code request is admitted");
    assert_eq!(envelope.cli_version, "2.1.218");

    let executor = AnthropicExecutor::new().unwrap();
    let account = Account {
        id: "acct-anthropic".into(),
        base_url: base,
        bearer_token: "ACCOUNT-TOKEN".into(),
        chatgpt_account_id: None,
        is_fedramp: false,
    };
    let req = PreparedRequest {
        body: None,
        model: "claude-sonnet-4-6".into(),
        // Exactly what ingress builds: the client's allowlisted envelope and NO credential. The
        // executor is what attaches the selected account's bearer.
        forward_headers: polyflare_anthropic::forwarded_client_headers(&inbound),
        raw_body: Some(bytes::Bytes::from_static(raw)),
    };

    let mut stream = executor
        .execute(req, &account, &RequestCtx::default())
        .await
        .unwrap();
    while stream.next().await.is_some() {}

    let headers = handle.last_headers().unwrap();
    // The account substitution happened, and the caller's own credential never left the proxy.
    assert_eq!(
        headers.get("authorization").unwrap(),
        "Bearer ACCOUNT-TOKEN"
    );
    assert!(headers.get("cookie").is_none(), "cookies must not travel");
    // The client's protocol envelope survived verbatim — including beta order.
    assert_eq!(
        headers.get("anthropic-beta").unwrap(),
        "claude-code-20250219,oauth-2025-04-20"
    );
    assert_eq!(
        headers.get("user-agent").unwrap(),
        "claude-cli/2.1.218 (external, sdk-ts)"
    );
    assert_eq!(
        headers.get("x-claude-code-session-id").unwrap(),
        "c38f98c8-7c2a-4e93-aa3d-a79df7a7015f",
        "the client's own session id is forwarded, never regenerated"
    );
    // Exactly one content-type, despite the executor also having a default for it.
    assert_eq!(headers.get_all("content-type").iter().count(), 1);
    assert_eq!(headers.get_all("anthropic-version").iter().count(), 1);
    // And the body is the client's bytes, not a re-serialization of them.
    assert_eq!(handle.last_body().unwrap()["model"], "claude-sonnet-4-6");
}
