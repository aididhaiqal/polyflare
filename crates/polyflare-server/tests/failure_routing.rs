//! A2 (failure-reactive routing): a real upstream 429 must write the account's runtime cooldown so
//! the selector benches it on the NEXT request. Drives the full ingress → executor → record_failure
//! → runtime-overlay → eligibility loop against an inline 429 upstream.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::http::StatusCode;
use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{AccountId, AccountSnapshot, CapacityWeighted, Continuity};
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
        last_refresh: i64::MAX / 2, // never triggers a refresh
        created_at: 1,
        status: "active".to_string(),
        deactivation_reason: None,
        reset_at: None,
        blocked_at: None,
        security_work_authorized: false,
        provider: "codex".to_string(),
        pool: None,
    }
}

/// An inline upstream that always answers `POST /responses` with `429 Too Many Requests` +
/// `Retry-After: 90`.
async fn spawn_429_upstream() -> String {
    async fn too_many() -> axum::response::Response {
        (
            StatusCode::TOO_MANY_REQUESTS,
            [("retry-after", "90")],
            "rate limited",
        )
            .into_response()
    }
    use axum::response::IntoResponse;
    let app = axum::Router::new().route("/responses", axum::routing::post(too_many));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

async fn spawn(upstream_url: String) -> (String, Arc<AppState>) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[7u8; 32]).unwrap();
    store
        .accounts()
        .insert(
            &account("acct-1"),
            &PlainTokens {
                access_token: "a".into(),
                refresh_token: "r".into(),
                id_token: "i".into(),
            },
            &cipher,
        )
        .await
        .unwrap();
    std::mem::forget(dir);
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_secs(30),
    ));
    let state = Arc::new(AppState {
        codex_executor: Arc::new(CodexExecutor::new().unwrap()),
        anthropic_executor: Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap()),
        selector: Arc::new(CapacityWeighted),
        pool_selectors: Default::default(),
        continuity,
        store,
        cipher,
        oauth: OAuthClient::new("http://127.0.0.1:9".to_string()).unwrap(),
        upstream_base_url: upstream_url,
        anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),
        refresh_locks: Default::default(),
        capture_fingerprint_path: None,
        codex_version: Arc::new(polyflare_codex::CodexVersionCache::new().unwrap()),
        account_cache: Arc::new(polyflare_server::account_cache::AccountCache::new()),
        token_cache: Default::default(),
        runtime: Default::default(),
    });
    let app = build_app(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), state)
}

#[tokio::test]
async fn a_clean_completion_clears_prior_error_state() {
    // A3: record_success fires at TRUE stream completion (inside the watchdog observer). Pre-seed an
    // error on the account, then a request that completes cleanly must clear it — proving success is
    // recorded at completion, not (as the reverted version did) at stream START.
    let upstream = MockUpstream::new(vec![r#"{"type":"response.completed"}"#.to_string()]);
    let upstream_url = upstream.spawn().await;
    let (pf, state) = spawn(upstream_url).await;

    // Seed error_count = 2 (draining-tier territory) for the only account.
    state
        .runtime
        .record_transient_error(&AccountId::from("acct-1"), now());
    state
        .runtime
        .record_transient_error(&AccountId::from("acct-1"), now());
    let mut before = vec![AccountSnapshot::new("acct-1")];
    state.runtime.overlay(&mut before, now());
    assert_eq!(before[0].error_count, 2, "seeded");

    let resp = reqwest::Client::new()
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    // Drain the body to EOF so the server's ObservingStream reaches clean completion → record_success
    // (which awaits INLINE before yielding the terminal chunk, so it is done by the time this returns).
    let _ = resp.bytes().await.unwrap();

    let mut after = vec![AccountSnapshot::new("acct-1")];
    state.runtime.overlay(&mut after, now());
    assert_eq!(
        after[0].error_count, 0,
        "a clean completion cleared the pre-seeded error state"
    );
}

#[tokio::test]
async fn a_429_cools_the_account_down_and_benches_it_next_request() {
    let upstream = spawn_429_upstream().await;
    let (pf, state) = spawn(upstream).await;
    let client = reqwest::Client::new();
    let body = serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"});

    // Request 1: the only account 429s ⇒ the client sees a generic 502, and record_failure writes
    // the runtime cooldown for that account.
    let r1 = client
        .post(format!("{pf}/responses"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r1.status(), 502, "a 429 upstream surfaces as a generic 502");

    // The runtime state now carries a cooldown honoring the Retry-After (90s ≥ the 30s floor).
    let mut snaps = vec![AccountSnapshot::new("acct-1")];
    state.runtime.overlay(&mut snaps, now());
    assert_eq!(snaps[0].error_count, 1, "the 429 bumped the error count");
    assert_eq!(
        snaps[0].cooldown_until,
        Some(snaps[0].last_error_at.unwrap() + 90),
        "cooldown honors Retry-After: 90"
    );

    // Request 2: the account is in cooldown ⇒ eligibility excludes it ⇒ empty pool ⇒ 503 (NOT a
    // second 502 from routing to the still-benched account).
    let r2 = client
        .post(format!("{pf}/responses"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        r2.status(),
        503,
        "the cooled-down account is benched, so there is no eligible account"
    );
}
