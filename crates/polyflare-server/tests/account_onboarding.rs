mod support;

use axum::extract::Form;
use axum::routing::post;
use axum::{Json, Router};
use base64::Engine as _;

fn jwt(claims: serde_json::Value) -> String {
    let encoder = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    format!(
        "{}.{}.sig",
        encoder.encode(br#"{"alg":"none"}"#),
        encoder.encode(serde_json::to_vec(&claims).unwrap())
    )
}

async fn oauth_token(
    Form(body): Form<std::collections::HashMap<String, String>>,
) -> Json<serde_json::Value> {
    assert_eq!(
        body.get("grant_type").map(String::as_str),
        Some("authorization_code")
    );
    assert!(body.get("code_verifier").is_some_and(|v| !v.is_empty()));
    Json(serde_json::json!({
        "access_token": "secret-access",
        "refresh_token": "secret-refresh",
        "id_token": jwt(serde_json::json!({
            "email": "new@example.test",
            "chatgpt_account_id": "chatgpt-new",
            "chatgpt_user_id": "user-new",
            "chatgpt_plan_type": "pro"
        }))
    }))
}

async fn mock_oauth() -> String {
    let app = Router::new().route("/oauth/token", post(oauth_token));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

async fn start_flow(
    client: &reqwest::Client,
    pf: &str,
    pool: Option<&str>,
) -> (serde_json::Value, String) {
    start_flow_with(client, pf, serde_json::json!({ "initial_pool": pool })).await
}

async fn start_flow_with(
    client: &reqwest::Client,
    pf: &str,
    body: serde_json::Value,
) -> (serde_json::Value, String) {
    let response = client
        .post(format!("{pf}/api/account-onboarding/codex"))
        .header("authorization", "Bearer secret")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.unwrap();
    let authorize = reqwest::Url::parse(body["authorize_url"].as_str().unwrap()).unwrap();
    let state = authorize
        .query_pairs()
        .find(|(k, _)| k == "state")
        .unwrap()
        .1
        .into_owned();
    (body, state)
}

async fn complete_flow(
    client: &reqwest::Client,
    pf: &str,
    flow_id: &str,
    state: &str,
) -> reqwest::Response {
    client
        .post(format!("{pf}/api/account-onboarding/{flow_id}/callback"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({
            "callback_url":
                format!("http://localhost:1455/auth/callback?code=one-time-code&state={state}")
        }))
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn onboarding_requires_auth_and_rejects_bad_pool_slug() {
    let (pf, _) = support::spawn_with_oauth_base(mock_oauth().await).await;
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{pf}/api/account-onboarding/codex"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 401);
    let response = client
        .post(format!("{pf}/api/account-onboarding/codex"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "initial_pool": "Bad pool" }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
}

#[tokio::test]
async fn callback_validates_state_then_inserts_without_returning_secrets() {
    let (pf, app_state) = support::spawn_with_oauth_base(mock_oauth().await).await;
    let client = reqwest::Client::new();
    let (flow, state) = start_flow(&client, &pf, Some("team-a")).await;
    let flow_id = flow["flow_id"].as_str().unwrap();
    let bad = client.post(format!("{pf}/api/account-onboarding/{flow_id}/callback"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "callback_url": "http://localhost:1455/auth/callback?code=x&state=wrong" }))
        .send().await.unwrap();
    assert_eq!(bad.status(), 400);

    let callback = format!("http://localhost:1455/auth/callback?code=one-time-code&state={state}");
    let response = client
        .post(format!("{pf}/api/account-onboarding/{flow_id}/callback"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "callback_url": callback }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let text = response.text().await.unwrap();
    assert!(!text.contains("secret-access"));
    assert!(!text.contains("secret-refresh"));
    let account = app_state
        .store
        .accounts()
        .get("codex_chatgpt-new")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(account.pool.as_deref(), Some("team-a"));

    let replay = client.post(format!("{pf}/api/account-onboarding/{flow_id}/callback"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "callback_url": format!("http://localhost:1455/auth/callback?code=again&state={state}") }))
        .send().await.unwrap();
    assert_eq!(replay.status(), 409);
}

#[tokio::test]
async fn matching_identity_is_refreshed_in_place_and_reactivated() {
    let (pf, app_state) = support::spawn_with_oauth_base(mock_oauth().await).await;
    sqlx::query(
        "UPDATE accounts SET chatgpt_account_id = 'chatgpt-new', status = 'reauth_required', \
         pool = 'existing-pool' WHERE id = 'acct-1'",
    )
    .execute(app_state.store.pool())
    .await
    .unwrap();
    let client = reqwest::Client::new();
    let (flow, state) = start_flow(&client, &pf, None).await;
    let flow_id = flow["flow_id"].as_str().unwrap();
    let response = client
        .post(format!("{pf}/api/account-onboarding/{flow_id}/callback"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({
            "callback_url": format!("http://localhost:1455/auth/callback?code=reauth&state={state}")
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(app_state.store.accounts().list().await.unwrap().len(), 1);
    let account = app_state
        .store
        .accounts()
        .get("acct-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(account.status, "active");
    assert_eq!(account.pool.as_deref(), Some("existing-pool"));
    let tokens = app_state
        .store
        .accounts()
        .decrypt_tokens("acct-1", &app_state.cipher)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(tokens.access_token, "secret-access");
}

#[tokio::test]
async fn targeted_reauth_start_rejects_an_unknown_account() {
    let (pf, _) = support::spawn_with_oauth_base(mock_oauth().await).await;
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{pf}/api/account-onboarding/codex"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "account_id": "no-such-account" }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 404);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["error"], "account_not_found");
}

#[tokio::test]
async fn targeted_reauth_repairs_the_intended_account() {
    let (pf, app_state) = support::spawn_with_oauth_base(mock_oauth().await).await;
    sqlx::query(
        "UPDATE accounts SET chatgpt_account_id = 'chatgpt-new', status = 'reauth_required' \
         WHERE id = 'acct-1'",
    )
    .execute(app_state.store.pool())
    .await
    .unwrap();
    let client = reqwest::Client::new();
    let (flow, state) =
        start_flow_with(&client, &pf, serde_json::json!({ "account_id": "acct-1" })).await;
    let response = complete_flow(&client, &pf, flow["flow_id"].as_str().unwrap(), &state).await;
    assert_eq!(response.status(), 200);
    assert_eq!(app_state.store.accounts().list().await.unwrap().len(), 1);
    let account = app_state
        .store
        .accounts()
        .get("acct-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(account.status, "active");
    let tokens = app_state
        .store
        .accounts()
        .decrypt_tokens("acct-1", &app_state.cipher)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(tokens.access_token, "secret-access");
}

#[tokio::test]
async fn targeted_reauth_matches_by_email_and_backfills_a_null_chatgpt_id() {
    let (pf, app_state) = support::spawn_with_oauth_base(mock_oauth().await).await;
    // The support account has chatgpt_account_id NULL; only a case-insensitive email match can
    // authorize the repair, and completion must UPDATE this row (backfilling the id) rather than
    // inserting a duplicate keyed on the exchanged ChatGPT id.
    sqlx::query(
        "UPDATE accounts SET email = 'NEW@Example.Test', status = 'reauth_required' \
         WHERE id = 'acct-1'",
    )
    .execute(app_state.store.pool())
    .await
    .unwrap();
    let client = reqwest::Client::new();
    let (flow, state) =
        start_flow_with(&client, &pf, serde_json::json!({ "account_id": "acct-1" })).await;
    let response = complete_flow(&client, &pf, flow["flow_id"].as_str().unwrap(), &state).await;
    assert_eq!(response.status(), 200);
    assert_eq!(app_state.store.accounts().list().await.unwrap().len(), 1);
    let account = app_state
        .store
        .accounts()
        .get("acct-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(account.status, "active");
    assert_eq!(account.chatgpt_account_id.as_deref(), Some("chatgpt-new"));
}

#[tokio::test]
async fn targeted_reauth_refuses_a_mismatched_seat_and_touches_nothing() {
    let (pf, app_state) = support::spawn_with_oauth_base(mock_oauth().await).await;
    sqlx::query(
        "UPDATE accounts SET chatgpt_account_id = 'chatgpt-other', status = 'reauth_required' \
         WHERE id = 'acct-1'",
    )
    .execute(app_state.store.pool())
    .await
    .unwrap();
    let client = reqwest::Client::new();
    let (flow, state) =
        start_flow_with(&client, &pf, serde_json::json!({ "account_id": "acct-1" })).await;
    let flow_id = flow["flow_id"].as_str().unwrap();
    let response = complete_flow(&client, &pf, flow_id, &state).await;
    assert_eq!(response.status(), 409);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["error"], "seat_mismatch");

    // The wrong-seat login changed NOTHING: no new account, target still parked with old tokens.
    assert_eq!(app_state.store.accounts().list().await.unwrap().len(), 1);
    let account = app_state
        .store
        .accounts()
        .get("acct-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(account.status, "reauth_required");
    let tokens = app_state
        .store
        .accounts()
        .decrypt_tokens("acct-1", &app_state.cipher)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(tokens.access_token, "a");

    // The flow is terminally failed with the specific code, so the dashboard can explain it.
    let status = client
        .get(format!("{pf}/api/account-onboarding/{flow_id}"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    let status_body: serde_json::Value = status.json().await.unwrap();
    assert_eq!(status_body["status"], "failed");
    assert_eq!(status_body["error_code"], "seat_mismatch");
}

// ---------------------------------------------------------------------------------------------
// Device-code flow + loopback listener
// ---------------------------------------------------------------------------------------------

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

async fn device_usercode() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "user_code": "ABCD-1234",
        "device_auth_id": "dev-auth-1",
        "interval": 1,
        "expires_in": 900
    }))
}

/// Pending for the first `threshold` polls (HTTP 403, like the real endpoint), then approves with
/// an authorization_code + code_verifier hand-off (the shape that exercises the standard token
/// endpoint too).
async fn device_token(
    axum::extract::State((hits, threshold)): axum::extract::State<(Arc<AtomicUsize>, usize)>,
) -> (axum::http::StatusCode, Json<serde_json::Value>) {
    let n = hits.fetch_add(1, Ordering::SeqCst);
    if n < threshold {
        return (
            axum::http::StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "status": "pending" })),
        );
    }
    (
        axum::http::StatusCode::OK,
        Json(serde_json::json!({
            "authorization_code": "device-one-time-code",
            "code_verifier": "device-code-verifier"
        })),
    )
}

/// Mock auth server with the device endpoints as well; approves after `pending_polls` polls.
async fn mock_oauth_with_device(pending_polls: usize) -> String {
    let app = Router::new()
        .route("/oauth/token", post(oauth_token))
        .route("/api/accounts/deviceauth/usercode", post(device_usercode))
        .route("/api/accounts/deviceauth/token", post(device_token))
        .with_state((Arc::new(AtomicUsize::new(0)), pending_polls));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

async fn wait_for_flow_terminal(
    client: &reqwest::Client,
    pf: &str,
    flow_id: &str,
) -> serde_json::Value {
    for _ in 0..100 {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let status: serde_json::Value = client
            .get(format!("{pf}/api/account-onboarding/{flow_id}"))
            .header("authorization", "Bearer secret")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if status["status"] == "completed" || status["status"] == "failed" {
            return status;
        }
    }
    panic!("flow {flow_id} never reached a terminal status");
}

#[tokio::test]
async fn device_flow_polls_until_approval_and_onboards() {
    let (pf, app_state) = support::spawn_with_oauth_base(mock_oauth_with_device(2).await).await;
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{pf}/api/account-onboarding/codex/device"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["user_code"], "ABCD-1234");
    assert!(body["verification_url"]
        .as_str()
        .unwrap()
        .ends_with("/codex/device"));
    let flow_id = body["flow_id"].as_str().unwrap();

    let status = wait_for_flow_terminal(&client, &pf, flow_id).await;
    assert_eq!(status["status"], "completed", "status: {status}");
    assert!(app_state
        .store
        .accounts()
        .get("codex_chatgpt-new")
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn device_flow_enforces_the_targeted_seat() {
    let (pf, app_state) = support::spawn_with_oauth_base(mock_oauth_with_device(0).await).await;
    sqlx::query(
        "UPDATE accounts SET chatgpt_account_id = 'chatgpt-other', status = 'reauth_required' \
         WHERE id = 'acct-1'",
    )
    .execute(app_state.store.pool())
    .await
    .unwrap();
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{pf}/api/account-onboarding/codex/device"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "account_id": "acct-1" }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.unwrap();
    let flow_id = body["flow_id"].as_str().unwrap();

    let status = wait_for_flow_terminal(&client, &pf, flow_id).await;
    assert_eq!(status["status"], "failed");
    assert_eq!(status["error_code"], "seat_mismatch");
    // Nothing changed: no new account, target still parked.
    assert_eq!(app_state.store.accounts().list().await.unwrap().len(), 1);
    let account = app_state
        .store
        .accounts()
        .get("acct-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(account.status, "reauth_required");
}

#[tokio::test]
async fn loopback_listener_completes_a_pending_browser_flow() {
    let (pf, app_state) = support::spawn_with_oauth_base(mock_oauth().await).await;
    let client = reqwest::Client::new();
    let (flow, state) = start_flow(&client, &pf, None).await;
    let flow_id = flow["flow_id"].as_str().unwrap();

    // The listener on an ephemeral port (the production path binds the fixed 1455).
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(polyflare_server::oauth_loopback::serve(
        listener,
        app_state.clone(),
    ));

    // A stale/unknown state is refused without touching anything.
    let bogus = client
        .get(format!(
            "http://{addr}/auth/callback?code=x&state=not-a-state"
        ))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(bogus.contains("no longer valid"), "body: {bogus}");

    // The real redirect completes the flow hands-free.
    let ok = client
        .get(format!(
            "http://{addr}/auth/callback?code=one-time-code&state={state}"
        ))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(ok.contains("Account connected"), "body: {ok}");
    assert!(!ok.contains("secret-access"), "must not leak tokens");

    let status: serde_json::Value = client
        .get(format!("{pf}/api/account-onboarding/{flow_id}"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["status"], "completed");
    assert!(app_state
        .store
        .accounts()
        .get("codex_chatgpt-new")
        .await
        .unwrap()
        .is_some());

    // A replayed redirect cannot resurrect the finished flow.
    let replay = client
        .get(format!(
            "http://{addr}/auth/callback?code=again&state={state}"
        ))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(replay.contains("no longer valid"), "body: {replay}");
}
