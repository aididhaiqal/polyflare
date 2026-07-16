//! Continuity ownership: turn 1 (no anchor) lands on account A and records resp_1 -> A; turn 2
//! carries `previous_response_id: resp_1` and MUST route back to A — the pin overrides a selector
//! that otherwise prefers B.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{AccountId, AccountSnapshot, Continuity, SelectionCtx, Selector};
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

/// A selector that prefers "B" when present (so an unpinned turn goes to B), else the first.
struct PreferB;
impl Selector for PreferB {
    fn pick(&self, candidates: &[AccountSnapshot], _ctx: &SelectionCtx) -> Option<AccountId> {
        if let Some(b) = candidates.iter().find(|s| s.id.as_str() == "B") {
            return Some(b.id.clone());
        }
        candidates.first().map(|s| s.id.clone())
    }
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
        pool: None,
    }
}

async fn drain(resp: reqwest::Response) -> String {
    let mut body = String::new();
    let mut s = resp.bytes_stream();
    while let Some(chunk) = s.next().await {
        body.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
    }
    body
}

#[tokio::test]
async fn second_turn_pins_back_to_owning_account() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[7u8; 32]).unwrap();
    // Turn 1: only account A exists (token "tokA"), so it lands on A.
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
        Duration::from_secs(30),
    ));

    let mock = MockUpstream::with_ids(vec![
        r#"{"type":"response.output_text.delta","delta":"x"}"#.to_string()
    ]);
    let handle = mock.clone();
    let upstream = mock.spawn().await;

    let state = Arc::new(AppState {
        codex_executor: Arc::new(CodexExecutor::new().unwrap()),
        anthropic_executor: Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap()),
        selector: Arc::new(PreferB),
        pool_selectors: Default::default(),
        continuity,
        store,
        cipher: TokenCipher::from_key_bytes(&[7u8; 32]).unwrap(),
        oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(),
        upstream_base_url: upstream,
        anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),
        refresh_locks: Default::default(),
        capture_fingerprint_path: None,
        codex_version: std::sync::Arc::new(polyflare_codex::CodexVersionCache::new().unwrap()),
        account_cache: std::sync::Arc::new(polyflare_server::account_cache::AccountCache::new()),
        token_cache: Default::default(),
        runtime: Default::default(),
    });
    let app = build_app(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let pf = format!("http://{addr}");

    let client = reqwest::Client::new();
    // Turn 1: no anchor. Lands on A; mock emits resp_1; observe records resp_1 -> A.
    let r1 = client
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model":"m","input":"hi"}))
        .send()
        .await
        .unwrap();
    let b1 = drain(r1).await;
    assert!(b1.contains("resp_1"));

    // Insert account B (token "tokB"); PreferB would now pick B when unpinned.
    state
        .store
        .accounts()
        .insert(
            &account("B"),
            &PlainTokens {
                access_token: "tokB".into(),
                refresh_token: "r".into(),
                id_token: "i".into(),
            },
            &state.cipher,
        )
        .await
        .unwrap();

    // Turn 2: carries the anchor resp_1 -> ownership pins to A despite PreferB.
    let r2 = client
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model":"m","previous_response_id":"resp_1","input":"more"}))
        .send()
        .await
        .unwrap();
    let _ = drain(r2).await;
    assert_eq!(
        handle.last_authorization().as_deref(),
        Some("Bearer tokA"),
        "turn 2 pinned back to A"
    );
    std::mem::forget(dir);
}
