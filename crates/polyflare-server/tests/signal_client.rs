//! Strategy B: a bare-tail request carrying a dead anchor to a silent-on-anchor upstream must
//! surface `previous_response_not_found` within N (bounded) so the client self-heals — not a hang.

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

fn account(id: &str) -> Account {
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
        provider: "codex".to_string(),
    }
}

#[tokio::test]
async fn bare_tail_dead_anchor_signals_previous_response_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[4u8; 32]).unwrap();
    store
        .accounts()
        .insert(
            &account("A"),
            &PlainTokens {
                access_token: "tokA".into(),
                refresh_token: "r".into(),
                id_token: "i".into(),
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

    let mock = MockUpstream::silent_on_anchor(vec![]);
    let upstream = mock.spawn().await;
    let state = Arc::new(AppState {
        codex_executor: Arc::new(CodexExecutor::new().unwrap()),
        anthropic_executor: Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap()),
        selector: Arc::new(CapacityWeighted),
        continuity,
        store,
        cipher,
        oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(),
        upstream_base_url: upstream,
        anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),
        refresh_locks: Default::default(),
        capture_fingerprint_path: None,
    });
    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let pf = format!("http://{addr}");

    let client = reqwest::Client::new();
    let body = tokio::time::timeout(Duration::from_secs(3), async {
        // Bare tail (short string) + dead anchor => is_full_resend=false => SignalClient.
        let resp = client
            .post(format!("{pf}/responses"))
            .json(
                &serde_json::json!({"model":"m","previous_response_id":"resp_dead","input":"tail"}),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let mut body = String::new();
        let mut s = resp.bytes_stream();
        while let Some(chunk) = s.next().await {
            body.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
        }
        body
    })
    .await
    .expect("signal must arrive within 3s (no hang)");

    // Assert on the CODE substring only (the exact envelope is a verify-at-impl item).
    assert!(
        body.contains("previous_response_not_found"),
        "client received the self-heal signal: {body}"
    );
}
