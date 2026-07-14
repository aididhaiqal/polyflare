//! End-to-end: client → polyflare server → executor → mock upstream, streaming the whole way.

use std::sync::Arc;

use futures_util::StreamExt;
use polyflare_codex::CodexExecutor;
use polyflare_core::Account;
use polyflare_server::app::{build_app, AppState};
use polyflare_testkit::MockUpstream;

async fn spawn_polyflare(upstream: String) -> String {
    let state = Arc::new(AppState {
        executor: Arc::new(CodexExecutor::new().unwrap()),
        account: Account {
            id: "e2e".into(),
            base_url: upstream,
            bearer_token: "tok".into(),
        },
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
async fn end_to_end_streaming_passthrough() {
    let mock = MockUpstream::new(vec![
        r#"{"type":"response.output_text.delta","delta":"a"}"#.to_string(),
        r#"{"type":"response.output_text.delta","delta":"b"}"#.to_string(),
        r#"{"type":"response.completed"}"#.to_string(),
    ]);
    let handle = mock.clone();
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(upstream).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let mut body = String::new();
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        body.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
    }

    // All three upstream events relayed, in order, with the model forwarded upstream.
    let first = body.find("delta\":\"a").unwrap();
    let second = body.find("delta\":\"b").unwrap();
    let done = body.find("response.completed").unwrap();
    assert!(first < second && second < done);
    assert_eq!(handle.last_body().unwrap()["model"], "gpt-5.6-sol");
}
