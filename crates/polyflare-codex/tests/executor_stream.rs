use futures_util::StreamExt;
use polyflare_codex::executor::CodexExecutor;
use polyflare_core::{Account, Executor, PreparedRequest};
use polyflare_testkit::MockUpstream;

#[tokio::test]
async fn executor_streams_upstream_events_and_forwards_body() {
    let mock = MockUpstream::new(vec![
        r#"{"type":"response.output_text.delta","delta":"hi"}"#.to_string(),
        r#"{"type":"response.completed"}"#.to_string(),
    ]);
    let handle = mock.clone();
    let base = mock.spawn().await;

    let executor = CodexExecutor::new().unwrap();
    let account = Account {
        id: "test".into(),
        base_url: base,
        bearer_token: "test-token".into(),
    };
    let req = PreparedRequest {
        body: serde_json::json!({"model": "gpt-5.6-sol", "input": "hello"}),
        model: "gpt-5.6-sol".into(),
        forward_headers: vec![],
    };

    let mut stream = executor.execute(req, &account).await.unwrap();
    let mut collected = String::new();
    while let Some(chunk) = stream.next().await {
        collected.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
    }

    assert!(collected.contains("response.output_text.delta"));
    assert!(collected.contains("response.completed"));
    assert_eq!(handle.last_body().unwrap()["model"], "gpt-5.6-sol");
}

#[tokio::test]
async fn executor_surfaces_upstream_error_status() {
    // No route for this path on the mock → 404 → ExecError::Upstream.
    let base = MockUpstream::new(vec![]).spawn().await;
    let executor = CodexExecutor::new().unwrap();
    let account = Account {
        id: "test".into(),
        base_url: format!("{base}/nonexistent-base"),
        bearer_token: "t".into(),
    };
    let req = PreparedRequest {
        body: serde_json::json!({"model": "m"}),
        model: "m".into(),
        forward_headers: vec![],
    };
    let err = executor.execute(req, &account).await.err().unwrap();
    assert!(matches!(err, polyflare_core::ExecError::Upstream(_)));
}
