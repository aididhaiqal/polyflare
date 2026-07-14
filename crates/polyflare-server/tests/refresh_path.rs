//! Refresh-path coverage: a STALE stored account (old `last_refresh` ⇒ `should_refresh` true)
//! forces the OAuth refresh branch that the other server tests skip. Wires `OAuthClient` at a
//! `MockOAuth` and asserts: (a) a successful refresh persists + relays with the new bearer;
//! (b) a classified permanent failure marks the account `reauth_required` and excludes it; and
//! (c) an HTTP failure with no parseable code also marks it (guarding the no-loop fix). A final
//! test asserts a failing upstream yields a generic 502.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::http::StatusCode;
use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::CapacityWeighted;
use polyflare_server::app::{build_app, AppState};
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};
use polyflare_testkit::{MockOAuth, MockUpstream};

/// A decode-able id_token (payload `{}`) so a `MockOAuth::ok` refresh returns `Ok` rather than
/// `Err(MalformedJwt)` when the client decodes the returned id_token.
const VALID_JWT: &str = "eyJhbGciOiJub25lIn0.e30.sig";
/// A `last_refresh` far enough in the past that `should_refresh` is always true.
const STALE: i64 = 1;

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn account(id: &str, last_refresh: i64) -> Account {
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
        last_refresh,
        created_at: 1,
        status: "active".to_string(),
        deactivation_reason: None,
        reset_at: None,
        blocked_at: None,
        security_work_authorized: false,
    }
}

/// Spawn a store-backed polyflare server with one account (tokens `old-*`). Returns the base URL
/// and the shared `AppState` so the test can inspect the store after a request.
async fn spawn(
    oauth_url: String,
    upstream_url: String,
    last_refresh: i64,
) -> (String, Arc<AppState>) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[7u8; 32]).unwrap();
    store
        .accounts()
        .insert(
            &account("acct-1", last_refresh),
            &PlainTokens {
                access_token: "old-access".to_string(),
                refresh_token: "old-refresh".to_string(),
                id_token: "old-id".to_string(),
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
        oauth: OAuthClient::new(oauth_url).unwrap(),
        upstream_base_url: upstream_url,
    });
    let app = build_app(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), state)
}

/// An inline upstream that always fails (`500`) at `POST /responses`.
async fn spawn_failing_upstream() -> String {
    async fn fail() -> StatusCode {
        StatusCode::INTERNAL_SERVER_ERROR
    }
    let app = axum::Router::new().route("/responses", axum::routing::post(fail));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn stale_account_refreshes_persists_and_relays_with_new_token() {
    let upstream = MockUpstream::new(vec![r#"{"type":"response.completed"}"#.to_string()]);
    let up_handle = upstream.clone();
    let upstream_url = upstream.spawn().await;

    let oauth = MockOAuth::ok("new-access", "new-refresh", VALID_JWT);
    let oauth_handle = oauth.clone();
    let oauth_url = oauth.spawn().await;

    let (pf, state) = spawn(oauth_url, upstream_url, STALE).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.contains("response.completed"));

    // The refresh was performed with the account's stored (old) refresh token.
    assert_eq!(
        oauth_handle.last_body().unwrap()["refresh_token"],
        "old-refresh"
    );
    // The refreshed tokens are persisted (re-encrypted) in the store.
    let toks = state
        .store
        .accounts()
        .decrypt_tokens("acct-1", &state.cipher)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(toks.access_token, "new-access");
    assert_eq!(toks.refresh_token, "new-refresh");
    // The NEW access token is what reached the upstream as the bearer.
    assert_eq!(
        up_handle.last_authorization().unwrap(),
        "Bearer new-access",
        "the refreshed access token must be used for the upstream call"
    );
}

#[tokio::test]
async fn permanent_refresh_failure_marks_reauth_and_excludes_account() {
    let upstream = MockUpstream::new(vec![r#"{"type":"response.completed"}"#.to_string()]);
    let upstream_url = upstream.spawn().await;
    let oauth = MockOAuth::error(400, "invalid_grant"); // classified reauth_required
    let oauth_url = oauth.spawn().await;
    let (pf, state) = spawn(oauth_url, upstream_url, STALE).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "gpt-5.6-sol"}))
        .send()
        .await
        .unwrap();
    // The token is dead ⇒ this request cannot be served (generic, no detail).
    assert_eq!(resp.status(), 503);

    let acct = state.store.accounts().get("acct-1").await.unwrap().unwrap();
    assert_eq!(acct.status, "reauth_required");

    // A follow-up request: the marked account is now ineligible ⇒ empty pool ⇒ 503.
    let resp2 = client
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "gpt-5.6-sol"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp2.status(),
        503,
        "a reauth_required account must be excluded from selection"
    );
}

#[tokio::test]
async fn refresh_http_error_without_code_marks_reauth() {
    let upstream = MockUpstream::new(vec![r#"{"type":"response.completed"}"#.to_string()]);
    let upstream_url = upstream.spawn().await;
    let oauth = MockOAuth::error_no_code(400); // HTTP error, no parseable `error` code
    let oauth_url = oauth.spawn().await;
    let (pf, state) = spawn(oauth_url, upstream_url, STALE).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "gpt-5.6-sol"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 503);

    let acct = state.store.accounts().get("acct-1").await.unwrap().unwrap();
    assert_eq!(
        acct.status, "reauth_required",
        "an unclassifiable HTTP refresh failure must mark the account, not loop on it"
    );
}

#[tokio::test]
async fn upstream_error_yields_generic_502() {
    let upstream_url = spawn_failing_upstream().await;
    // Fresh account ⇒ no refresh; OAuth is never contacted.
    let (pf, _state) = spawn("http://127.0.0.1:9".to_string(), upstream_url, now()).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "gpt-5.6-sol"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 502);
    let body = resp.text().await.unwrap();
    assert_eq!(
        body, "upstream error",
        "the 502 body must be generic — no upstream status, URL, or token"
    );
}
