//! Wedge regression (GREEN from C7): an anchor-bearing full-resend routed to a silent-on-anchor
//! upstream is detected within N, recovered by stripping the anchor and re-sending the FULL input,
//! and completes — no hang. Also asserts R1 (full-resend never trimmed).

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{CapacityWeighted, Continuity};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};
use polyflare_testkit::MockUpstream;

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

async fn spawn_polyflare(upstream: String) -> String {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[9u8; 32]).unwrap();
    let mut acct = store_account_ok("e2e");
    acct.plan_type = "pro".to_string();
    store
        .accounts()
        .insert(
            &acct,
            &PlainTokens {
                access_token: "tokE".to_string(),
                refresh_token: "r".to_string(),
                id_token: "i".to_string(),
            },
            &cipher,
        )
        .await
        .unwrap();
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_millis(150),
    ));
    std::mem::forget(dir);

    let state = Arc::new(AppState {
        executor: Arc::new(CodexExecutor::new().unwrap()),
        selector: Arc::new(CapacityWeighted),
        continuity,
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

fn store_account_ok(id: &str) -> Account {
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

#[tokio::test]
async fn anchor_bearing_request_to_silent_upstream_does_not_wedge() {
    let mock = MockUpstream::silent_on_anchor(vec![
        r#"{"type":"response.output_text.delta","delta":"ok"}"#.to_string(),
    ]);
    let handle = mock.clone();
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(upstream).await;

    let client = reqwest::Client::new();
    let input = serde_json::json!([
        {"role": "user", "content": "turn one"},
        {"role": "assistant", "content": "reply one"},
        {"role": "user", "content": "turn two"}
    ]);
    let request = client
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "previous_response_id": "resp_dead", "input": input}))
        .send();

    let body = tokio::time::timeout(Duration::from_secs(5), async {
        let resp = request.await.unwrap();
        assert_eq!(resp.status(), 200);
        let mut body = String::new();
        let mut s = resp.bytes_stream();
        while let Some(chunk) = s.next().await {
            body.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
        }
        body
    })
    .await
    .expect("must complete within 5s (no wedge)");

    assert!(
        body.contains("response.completed"),
        "client saw a completed stream"
    );
    assert_eq!(handle.request_count(), 2, "silent attempt + recovery");
    let bodies = handle.bodies();
    assert!(
        bodies[0].get("previous_response_id").is_some(),
        "1st carried the dead anchor"
    );
    assert!(
        bodies[1].get("previous_response_id").is_none(),
        "recovery stripped the anchor"
    );
    assert_eq!(bodies[1]["input"], input, "R1: full-resend not trimmed");
}
