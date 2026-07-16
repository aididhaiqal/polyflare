use futures_util::StreamExt;
use polyflare_anthropic::AnthropicExecutor;
use polyflare_core::{Account, Executor, PreparedRequest};
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
    };
    let req = PreparedRequest {
        body: serde_json::json!({
            "model": "claude-opus-4",
            "messages": [{"role": "user", "content": "hi"}]
        }),
        model: "claude-opus-4".into(),
        forward_headers: vec![],
        raw_body: None,
    };

    let mut stream = executor.execute(req, &account).await.unwrap();
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
    };
    let req = PreparedRequest {
        body: serde_json::json!({"model": "m"}),
        model: "m".into(),
        forward_headers: vec![],
        raw_body: None,
    };
    let err = executor.execute(req, &account).await.err().unwrap();
    assert!(matches!(err, polyflare_core::ExecError::Upstream(_)));
}
