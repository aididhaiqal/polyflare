//! Wedge regression: an anchor-bearing request routed to a silent-on-anchor upstream must NOT
//! hang. RED until C7 wires the watchdog into ingress; then it goes GREEN.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::CapacityWeighted;
use polyflare_server::app::{build_app, AppState};
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};
use polyflare_testkit::MockUpstream;

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn store_account(id: &str, _token: &str) -> Account {
    Account {
        id: id.to_string(),
        chatgpt_account_id: None,
        chatgpt_user_id: None,
        email: "u@example.test".to_string(),
        alias: None,
        workspace_id: None,
        workspace_label: None,
        seat_type: None,
        plan_type: "pro".to_string(),
        routing_policy: "normal".to_string(),
        last_refresh: now(),
        created_at: now(),
        status: "active".to_string(),
        deactivation_reason: None,
        reset_at: None,
        blocked_at: None,
        security_work_authorized: false,
    }
}

async fn spawn_polyflare(upstream: String) -> String {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[9u8; 32]).unwrap();
    store
        .accounts()
        .insert(
            &store_account("e2e", "tokE"),
            &PlainTokens {
                access_token: "tokE".to_string(),
                refresh_token: "r".to_string(),
                id_token: "i".to_string(),
            },
            &cipher,
        )
        .await
        .unwrap();
    std::mem::forget(dir);

    let state = Arc::new(AppState {
        executor: Arc::new(CodexExecutor::new().unwrap()),
        selector: Arc::new(CapacityWeighted),
        store,
        cipher,
        oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(),
        upstream_base_url: upstream,
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
#[ignore = "RED until C7 wires the watchdog; un-ignore in C7"]
async fn anchor_bearing_request_to_silent_upstream_does_not_wedge() {
    let mock = MockUpstream::silent_on_anchor(vec![
        r#"{"type":"response.output_text.delta","delta":"ok"}"#.to_string(),
    ]);
    let handle = mock.clone();
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(upstream).await;

    let client = reqwest::Client::new();
    // Full multi-item history + a dead anchor => the classic wedge input.
    let request = client
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({
            "model": "gpt-5.6-sol",
            "previous_response_id": "resp_dead",
            "input": [
                {"role": "user", "content": "turn one"},
                {"role": "assistant", "content": "reply one"},
                {"role": "user", "content": "turn two"}
            ]
        }))
        .send();

    // Bounded wall-clock: at C0 (no watchdog) the client hangs on the silent body and this elapses
    // (RED). At C7 the watchdog recovers within N and the stream completes (GREEN).
    let outcome = tokio::time::timeout(Duration::from_secs(5), async {
        let resp = request.await.unwrap();
        assert_eq!(resp.status(), 200);
        let mut body = String::new();
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            body.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
        }
        body
    })
    .await;

    let body = outcome.expect("request must complete within 5s (no wedge)");
    assert!(
        body.contains("response.completed"),
        "client must see a completed stream"
    );
    assert_eq!(
        handle.request_count(),
        2,
        "one silent attempt + one recovery"
    );
}
