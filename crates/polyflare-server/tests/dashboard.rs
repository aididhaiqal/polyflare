//! Embedded dashboard: `/dashboard` serves the built SPA `index.html`, `/dashboard/{*path}` serves
//! its hashed bundle assets with correct content types, and an unknown sub-path falls back to the
//! SPA entrypoint. Proves the rust-embed wiring end-to-end against a running server.

use std::sync::Arc;
use std::time::Duration;

use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{CapacityWeighted, Continuity, Executor};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_store::{Store, TokenCipher};

async fn spawn() -> String {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    std::mem::forget(dir);
    let cipher = TokenCipher::from_key_bytes(&[13u8; 32]).unwrap();
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_secs(30),
    ));
    let state = Arc::new(AppState {
        codex_executor: Arc::new(CodexExecutor::new().unwrap()) as Arc<dyn Executor>,
        anthropic_executor: Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap())
            as Arc<dyn Executor>,
        selector: Arc::new(CapacityWeighted),
        pool_selectors: Default::default(),
        continuity,
        store,
        cipher,
        oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(),
        upstream_base_url: "http://127.0.0.1:9".to_string(),
        anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),
        refresh_locks: Default::default(),
        capture_fingerprint_path: None,
        codex_version: Arc::new(polyflare_codex::CodexVersionCache::new().unwrap()),
        account_cache: Arc::new(polyflare_server::account_cache::AccountCache::new()),
        token_cache: Default::default(),
        admin_token: None,
        live_logs: false,
        log_bus: polyflare_server::log_bus::LogBus::new(1000),

        runtime: Default::default(),
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
async fn dashboard_index_and_assets_and_spa_fallback() {
    let pf = spawn().await;
    let client = reqwest::Client::new();

    // /dashboard → the SPA entrypoint.
    let resp = client.get(format!("{pf}/dashboard")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let ctype = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(
        ctype.contains("text/html"),
        "index served as html, got {ctype}"
    );
    let html = resp.text().await.unwrap();
    assert!(html.contains(r#"id="root""#), "SPA mount point present");
    // Its bundle assets are referenced under the /dashboard/ base.
    assert!(
        html.contains("/dashboard/assets/"),
        "base-prefixed asset URL present"
    );

    // Pull the JS bundle URL out of the HTML and fetch it — it must serve as JS.
    let js_url = html
        .split('"')
        .find(|s| s.starts_with("/dashboard/assets/") && s.ends_with(".js"))
        .expect("a hashed JS bundle URL in index.html");
    let js = client.get(format!("{pf}{js_url}")).send().await.unwrap();
    assert_eq!(js.status(), 200);
    let js_ctype = js
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(
        js_ctype.contains("javascript"),
        "JS bundle typed as javascript, got {js_ctype}"
    );

    // Unknown sub-path → SPA fallback to index.html (200 + html, not 404).
    let spa = client
        .get(format!("{pf}/dashboard/some/client/route"))
        .send()
        .await
        .unwrap();
    assert_eq!(spa.status(), 200);
    assert!(spa.text().await.unwrap().contains(r#"id="root""#));
}
