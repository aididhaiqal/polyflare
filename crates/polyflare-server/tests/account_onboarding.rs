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
