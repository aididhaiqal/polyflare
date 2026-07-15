//! The native Anthropic-Messages ingress path: `/v1/messages` selects only Anthropic-provider
//! accounts and relays through `AnthropicExecutor`; continuity is a no-op (SPEC-M4 §3.7 — no
//! `previous_response_id`-style anchor exists for this backend, so the watchdog never arms).

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{CapacityWeighted, Continuity, Executor};
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

fn anthropic_account(id: &str) -> Account {
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
        provider: "anthropic".to_string(),
    }
}

fn tokens() -> PlainTokens {
    PlainTokens {
        access_token: "tok".to_string(),
        refresh_token: "r".to_string(),
        id_token: "i".to_string(),
    }
}

async fn spawn_polyflare(store: Store, anthropic_upstream: String) -> String {
    let cipher = TokenCipher::from_key_bytes(&[21u8; 32]).unwrap();
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_secs(30),
    ));
    let codex_executor: Arc<dyn Executor> = Arc::new(CodexExecutor::new().unwrap());
    let anthropic_executor: Arc<dyn Executor> =
        Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap());

    let state = Arc::new(AppState {
        codex_executor,
        anthropic_executor,
        selector: Arc::new(CapacityWeighted),
        continuity,
        store,
        cipher,
        oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(),
        upstream_base_url: "http://127.0.0.1:9".to_string(),
        anthropic_upstream_base_url: anthropic_upstream,
        refresh_locks: Default::default(),
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
async fn messages_relays_to_the_anthropic_executor() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    // Must match the cipher `spawn_polyflare` builds `AppState` with ([21u8; 32]) — otherwise
    // `resolve_core_account`'s `decrypt_tokens` fails and this 500s instead of routing (same
    // fix already applied to `provider_dispatch.rs`'s analogous fixture).
    let cipher = TokenCipher::from_key_bytes(&[21u8; 32]).unwrap();
    store
        .accounts()
        .insert(&anthropic_account("anthropic-1"), &tokens(), &cipher)
        .await
        .unwrap();
    std::mem::forget(dir);

    let mock = MockUpstream::new(vec![
        r#"{"type":"content_block_delta","delta":{"type":"text_delta","text":"hi"}}"#.to_string(),
        r#"{"type":"message_stop"}"#.to_string(),
    ]);
    let handle = mock.clone();
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(store, upstream).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{pf}/v1/messages"))
        .json(&serde_json::json!({
            "model": "claude-opus-4",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let mut body = String::new();
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        body.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
    }
    assert!(body.contains("content_block_delta"));
    assert!(body.contains("message_stop"));
    assert_eq!(handle.last_body().unwrap()["model"], "claude-opus-4");
}

#[tokio::test]
async fn messages_returns_503_when_pool_has_no_anthropic_account() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    std::mem::forget(dir);

    let pf = spawn_polyflare(store, "http://127.0.0.1:9".to_string()).await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{pf}/v1/messages"))
        .json(&serde_json::json!({"model": "claude-opus-4", "messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 503);
}
