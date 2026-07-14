use std::sync::Arc;

use futures_util::StreamExt;
use polyflare_codex::CodexExecutor;
use polyflare_core::Account;
use polyflare_server::app::{build_app, AppState};
use polyflare_testkit::MockUpstream;

#[tokio::test]
async fn server_relays_upstream_stream_to_client() {
    let mock = MockUpstream::new(vec![
        r#"{"type":"response.output_text.delta","delta":"yo"}"#.to_string(),
        r#"{"type":"response.completed"}"#.to_string(),
    ]);
    let handle = mock.clone();
    let upstream = mock.spawn().await;

    let state = Arc::new(AppState {
        executor: Arc::new(CodexExecutor::new().unwrap()),
        account: Account { id: "a".into(), base_url: upstream, bearer_token: "tok".into() },
    });
    let app = build_app(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/responses"))
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "text/event-stream"
    );

    let mut body = String::new();
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        body.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
    }
    assert!(body.contains("response.output_text.delta"));
    assert!(body.contains("response.completed"));
    assert_eq!(handle.last_body().unwrap()["model"], "gpt-5.6-sol");
}
