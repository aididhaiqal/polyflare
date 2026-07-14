//! Regression test: axum's `Json` extractor defaults to a 2 MB body limit, which would
//! 413 real Codex requests (long conversations / file reads). `build_app` raises this to
//! 100 MB via `DefaultBodyLimit::max`. This proves the raised limit holds end-to-end:
//! client -> polyflare server -> executor -> mock upstream, for a body well over 2 MB.

use std::sync::Arc;

use polyflare_codex::CodexExecutor;
use polyflare_core::Account;
use polyflare_server::app::{build_app, AppState};
use polyflare_testkit::MockUpstream;

#[tokio::test]
async fn large_request_body_is_not_rejected_with_413() {
    let mock = MockUpstream::new(vec![r#"{"type":"response.completed"}"#.to_string()]);
    let handle = mock.clone();
    let upstream = mock.spawn().await;

    let state = Arc::new(AppState {
        executor: Arc::new(CodexExecutor::new().unwrap()),
        account: Account {
            id: "large-body".into(),
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

    // Well over axum's default 2 MB Json limit.
    let payload = serde_json::json!({
        "model": "gpt-5.6-sol",
        "input": "x".repeat(2_500_000),
    });

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/responses"))
        .json(&payload)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "large body must not be rejected with 413");

    let last_body = handle.last_body().unwrap();
    assert_eq!(last_body["model"], "gpt-5.6-sol");
    assert_eq!(
        last_body["input"].as_str().unwrap().len(),
        2_500_000,
        "mock upstream must have received the full large body"
    );
}
