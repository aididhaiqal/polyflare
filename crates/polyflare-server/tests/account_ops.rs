//! The two dashboard account operations: force probe and credential export.

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::State;
use axum::http::Uri;
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;
use polyflare_store::{Account, PlainTokens};

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// Counts `/wham/usage` hits so a probe can be proven to have actually reached upstream.
#[derive(Clone, Default)]
struct UsageUpstream {
    usage_hits: Arc<AtomicUsize>,
}

async fn upstream(State(capture): State<UsageUpstream>, uri: Uri) -> Response {
    match uri.path() {
        "/backend-api/wham/usage" => {
            capture.usage_hits.fetch_add(1, Ordering::SeqCst);
            axum::Json(serde_json::json!({
                "rate_limit": {
                    "primary_window": {
                        "used_percent": 42.0,
                        "reset_at": now() + 5 * 24 * 3_600,
                        "limit_window_seconds": 604_800
                    },
                    "secondary_window": null
                }
            }))
            .into_response()
        }
        _ => axum::http::StatusCode::NOT_FOUND.into_response(),
    }
}

async fn spawn_upstream(capture: UsageUpstream) -> String {
    let app = Router::new()
        .route("/{*path}", any(upstream))
        .with_state(capture);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}/backend-api/codex")
}

/// An access token whose unverified `exp` is `exp_offset` seconds from now, so token-state
/// assertions are deterministic.
fn access_token(exp_offset: i64) -> String {
    use base64::Engine as _;
    let encoder = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    format!(
        "{}.{}.sig",
        encoder.encode(br#"{"alg":"none"}"#),
        encoder
            .encode(serde_json::to_vec(&serde_json::json!({ "exp": now() + exp_offset })).unwrap())
    )
}

fn codex_account(id: &str) -> Account {
    let mut account = support::account(id);
    account.chatgpt_account_id = Some(format!("chatgpt-{id}"));
    account
}

#[tokio::test]
async fn probe_fetches_live_usage_and_reports_token_health() {
    let capture = UsageUpstream::default();
    let (pf, app_state) = support::spawn(spawn_upstream(capture.clone()).await).await;
    // A comfortably-fresh token: the probe must NOT try to rotate it (no OAuth mock is running).
    app_state
        .store
        .accounts()
        .update_tokens(
            "acct-1",
            &PlainTokens {
                access_token: access_token(9 * 86_400),
                refresh_token: "r".into(),
                id_token: "i".into(),
            },
            &app_state.cipher,
            now(),
        )
        .await
        .unwrap();

    let response = reqwest::Client::new()
        .post(format!("{pf}/api/accounts/acct-1/probe"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.unwrap();

    assert_eq!(body["usage_refreshed"], true, "body: {body}");
    assert_eq!(
        body["token_rotated"], false,
        "a fresh token must not rotate"
    );
    assert_eq!(body["token_state"], "valid");
    assert_eq!(body["status"], "active");
    assert!(
        capture.usage_hits.load(Ordering::SeqCst) >= 1,
        "probe must reach upstream"
    );

    // The probe persisted what it fetched — the account detail now reports the live window.
    let detail: serde_json::Value = reqwest::Client::new()
        .get(format!("{pf}/api/accounts/acct-1"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let used = detail["quota_windows"][0]["used_percent"].as_f64().unwrap();
    assert!((used - 42.0).abs() < 0.001, "detail: {detail}");
}

#[tokio::test]
async fn probe_rejects_unknown_and_non_codex_accounts() {
    let (pf, app_state) = support::spawn(spawn_upstream(UsageUpstream::default()).await).await;
    let client = reqwest::Client::new();

    let missing = client
        .post(format!("{pf}/api/accounts/nope/probe"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);

    let mut anthropic = codex_account("acct-anthropic");
    anthropic.provider = "anthropic".into();
    app_state
        .store
        .accounts()
        .insert(
            &anthropic,
            &PlainTokens {
                access_token: "a".into(),
                refresh_token: "r".into(),
                id_token: String::new(),
            },
            &app_state.cipher,
        )
        .await
        .unwrap();
    let wrong_provider = client
        .post(format!("{pf}/api/accounts/acct-anthropic/probe"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(wrong_provider.status(), 400);
}

#[tokio::test]
async fn probe_requires_admin_auth() {
    let (pf, _) = support::spawn(spawn_upstream(UsageUpstream::default()).await).await;
    let response = reqwest::Client::new()
        .post(format!("{pf}/api/accounts/acct-1/probe"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 401);
}

#[tokio::test]
async fn export_auth_returns_the_codex_auth_json_shape_uncached() {
    let (pf, app_state) = support::spawn(spawn_upstream(UsageUpstream::default()).await).await;
    app_state
        .store
        .accounts()
        .update_tokens(
            "acct-1",
            &PlainTokens {
                access_token: "access-xyz".into(),
                refresh_token: "refresh-xyz".into(),
                id_token: "id-xyz".into(),
            },
            &app_state.cipher,
            1_700_000_000,
        )
        .await
        .unwrap();
    sqlx::query("UPDATE accounts SET chatgpt_account_id = 'chatgpt-acct-1' WHERE id = 'acct-1'")
        .execute(app_state.store.pool())
        .await
        .unwrap();

    let response = reqwest::Client::new()
        .post(format!("{pf}/api/accounts/acct-1/export-auth"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    // The credential must not be cacheable anywhere between here and the operator.
    let cache_control = response
        .headers()
        .get("cache-control")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(
        cache_control.contains("no-store"),
        "cache-control: {cache_control}"
    );

    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body["OPENAI_API_KEY"].is_null());
    assert_eq!(body["tokens"]["access_token"], "access-xyz");
    assert_eq!(body["tokens"]["refresh_token"], "refresh-xyz");
    assert_eq!(body["tokens"]["id_token"], "id-xyz");
    assert_eq!(body["tokens"]["account_id"], "chatgpt-acct-1");
    assert_eq!(body["last_refresh"], "2023-11-14T22:13:20Z");
}

#[tokio::test]
async fn export_auth_requires_admin_auth_and_a_real_codex_account() {
    let (pf, app_state) = support::spawn(spawn_upstream(UsageUpstream::default()).await).await;
    let client = reqwest::Client::new();

    // Unauthenticated callers never reach the credential.
    let unauthorized = client
        .post(format!("{pf}/api/accounts/acct-1/export-auth"))
        .send()
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), 401);
    let body = unauthorized.text().await.unwrap();
    assert!(!body.contains("refresh"), "401 body must not leak: {body}");

    let missing = client
        .post(format!("{pf}/api/accounts/nope/export-auth"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);

    // An account with no refresh token cannot produce a usable auth.json.
    let mut empty = codex_account("acct-no-refresh");
    empty.provider = "codex".into();
    app_state
        .store
        .accounts()
        .insert(
            &empty,
            &PlainTokens {
                access_token: "a".into(),
                refresh_token: String::new(),
                id_token: "i".into(),
            },
            &app_state.cipher,
        )
        .await
        .unwrap();
    let no_refresh = client
        .post(format!("{pf}/api/accounts/acct-no-refresh/export-auth"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(no_refresh.status(), 409);
}
