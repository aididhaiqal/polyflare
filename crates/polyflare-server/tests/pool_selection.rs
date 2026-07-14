//! Pool selection edge case: an empty pool (no accounts) → the handler returns 503.

use std::sync::Arc;

use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{CapacityWeighted, Continuity};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_store::{Store, TokenCipher};
use std::time::Duration;

#[tokio::test]
async fn no_eligible_account_returns_503() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[8u8; 32]).unwrap();
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_secs(30),
    ));
    std::mem::forget(dir);

    let state = Arc::new(AppState {
        codex_executor: Arc::new(CodexExecutor::new().unwrap()),
        anthropic_executor: Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap()),
        selector: Arc::new(CapacityWeighted),
        continuity,
        store, // no accounts inserted → empty snapshot pool
        cipher,
        oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(),
        upstream_base_url: "http://127.0.0.1:9".to_string(),
        anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),
    });
    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/responses"))
        .json(&serde_json::json!({"model": "gpt-5.6-sol"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 503, "empty pool must yield 503");
}
