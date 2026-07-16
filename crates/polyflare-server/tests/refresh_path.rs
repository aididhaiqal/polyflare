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
use polyflare_core::{CapacityWeighted, Continuity};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};
use polyflare_testkit::{MockOAuth, MockUpstream};
use std::time::Duration;

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
        provider: "codex".to_string(),
        pool: None,
    }
}

/// A JWT access token whose `exp` is `secs_from_now` seconds from now (unsigned; only `exp` is read
/// for refresh timing). Header `{"alg":"none"}`, payload `{"exp":<epoch>}`.
fn jwt_expiring_in(secs_from_now: i64) -> String {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine as _;
    let exp = now() + secs_from_now;
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"none"}"#);
    let payload = URL_SAFE_NO_PAD.encode(format!("{{\"exp\":{exp}}}").as_bytes());
    format!("{header}.{payload}.sig")
}

/// Spawn a store-backed polyflare server with one account (tokens `old-*`). Returns the base URL
/// and the shared `AppState` so the test can inspect the store after a request.
async fn spawn(
    oauth_url: String,
    upstream_url: String,
    last_refresh: i64,
) -> (String, Arc<AppState>) {
    spawn_with_access_token(oauth_url, upstream_url, last_refresh, "old-access").await
}

/// As [`spawn`], but with a caller-chosen stored access token (so a test can control whether the
/// refresh trigger comes from the token's `exp` or the `last_refresh` age-gate fallback).
async fn spawn_with_access_token(
    oauth_url: String,
    upstream_url: String,
    last_refresh: i64,
    access_token: &str,
) -> (String, Arc<AppState>) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[7u8; 32]).unwrap();
    store
        .accounts()
        .insert(
            &account("acct-1", last_refresh),
            &PlainTokens {
                access_token: access_token.to_string(),
                refresh_token: "old-refresh".to_string(),
                id_token: "old-id".to_string(),
            },
            &cipher,
        )
        .await
        .unwrap();
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_secs(30),
    ));
    std::mem::forget(dir);

    let state = Arc::new(AppState {
        codex_executor: Arc::new(CodexExecutor::new().unwrap()),
        anthropic_executor: Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap()),
        selector: Arc::new(CapacityWeighted),
        pool_selectors: Default::default(),
        continuity,
        store,
        cipher,
        oauth: OAuthClient::new(oauth_url).unwrap(),
        upstream_base_url: upstream_url,
        anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),
        refresh_locks: Default::default(),
        capture_fingerprint_path: None,
        codex_version: std::sync::Arc::new(polyflare_codex::CodexVersionCache::new().unwrap()),
        account_cache: std::sync::Arc::new(polyflare_server::account_cache::AccountCache::new()),
        token_cache: Default::default(),
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
async fn refresh_is_triggered_by_token_expiry_not_just_age() {
    // The account was refreshed just NOW (age gate would say "don't refresh"), but its access token
    // expires in 1 day — inside the 2-day refresh margin. So ONLY the exp-based trigger can fire the
    // refresh. Proves ingress times refresh off the token's actual `exp`, not the `last_refresh` age.
    let upstream = MockUpstream::new(vec![r#"{"type":"response.completed"}"#.to_string()]);
    let up_handle = upstream.clone();
    let upstream_url = upstream.spawn().await;

    let oauth = MockOAuth::ok("new-access", "new-refresh", VALID_JWT);
    let oauth_url = oauth.spawn().await;

    // Fresh by age (last_refresh = now) but the stored access token is near expiry.
    let near_expiry = jwt_expiring_in(86_400); // 1 day ⇒ within the 2-day margin
    let (pf, _state) = spawn_with_access_token(oauth_url, upstream_url, now(), &near_expiry).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // The refresh fired purely on `exp` ⇒ the NEW token reached the upstream.
    assert_eq!(
        up_handle.last_authorization().unwrap(),
        "Bearer new-access",
        "a near-expiry token must be refreshed even when last_refresh is fresh"
    );
}

#[tokio::test]
async fn fresh_token_far_from_expiry_is_not_refreshed() {
    // The mirror: a stored access token valid for 5 more days (outside the 2-day margin) and a fresh
    // last_refresh ⇒ NO refresh. Wire a hit-COUNTING OAuth mock and assert it is contacted ZERO
    // times — a dead port wouldn't prove this, since a wrongly-attempted refresh would fail as
    // Transport and fall through to the old token, passing the request regardless.
    let upstream = MockUpstream::new(vec![r#"{"type":"response.completed"}"#.to_string()]);
    let up_handle = upstream.clone();
    let upstream_url = upstream.spawn().await;

    // If ingress WRONGLY attempted a refresh, this mock records the hit (and would rotate the token).
    let oauth = MockOAuth::ok("should-not-be-used", "should-not-be-used", VALID_JWT);
    let oauth_handle = oauth.clone();
    let oauth_url = oauth.spawn().await;

    let far_from_expiry = jwt_expiring_in(5 * 86_400); // 5 days ⇒ outside the 2-day margin
    let (pf, _state) =
        spawn_with_access_token(oauth_url, upstream_url, now(), &far_from_expiry).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "a token far from expiry needs no refresh"
    );
    assert_eq!(
        oauth_handle.hit_count(),
        0,
        "a token far from expiry must trigger NO refresh call at all"
    );
    assert_eq!(
        up_handle.last_authorization().unwrap(),
        format!("Bearer {far_from_expiry}"),
        "the existing (unrefreshed) token must be forwarded verbatim"
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
async fn transient_refresh_failure_does_not_mark_account() {
    // `server_error` is NOT in the permanent set (classify_failure → Transient), so the endpoint
    // failure must NOT mark the account: it 503s this request but leaves the account `active` and
    // selectable, so the next request retries the refresh (retry-on-transient).
    let upstream = MockUpstream::new(vec![r#"{"type":"response.completed"}"#.to_string()]);
    let upstream_url = upstream.spawn().await;
    let oauth = MockOAuth::error(503, "server_error"); // transient-classified code
    let oauth_url = oauth.spawn().await;
    let (pf, state) = spawn(oauth_url, upstream_url, STALE).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "gpt-5.6-sol"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        503,
        "the refresh did not complete ⇒ 503 this request"
    );

    let acct = state.store.accounts().get("acct-1").await.unwrap().unwrap();
    assert_eq!(
        acct.status, "active",
        "a transient endpoint failure must NOT mark the account — it stays selectable to retry"
    );
}

/// F2: N concurrent requests that all select the SAME stale account must collapse into exactly
/// ONE call to the OAuth refresh endpoint (a singleflight per account), not one call per request.
/// Before the fix, all 8 requests observe staleness, all 8 call `refresh` with the account's
/// (same) stored refresh token — OpenAI-style refresh-token rotation means only the first would
/// have "succeeded" against a real endpoint, but even against this mock (which always returns the
/// same success payload) the bug is that the endpoint gets hit 8 times instead of 1, and any
/// endpoint that classifies reuse as a permanent failure would wrongly deactivate the account.
/// `MockOAuth`'s handler sleeps ~50ms before responding so all 8 requests reliably enter the stale
/// branch before the first refresh completes, forcing the race to actually occur.
#[tokio::test]
async fn concurrent_stale_requests_collapse_into_one_refresh() {
    let upstream = MockUpstream::new(vec![r#"{"type":"response.completed"}"#.to_string()]);
    let upstream_url = upstream.spawn().await;

    let oauth = MockOAuth::ok("new-access", "new-refresh", VALID_JWT);
    let oauth_handle = oauth.clone();
    let oauth_url = oauth.spawn().await;

    let (pf, state) = spawn(oauth_url, upstream_url, STALE).await;

    const N: usize = 8;
    let client = reqwest::Client::new();
    let futures = (0..N).map(|_| {
        let client = client.clone();
        let pf = pf.clone();
        tokio::spawn(async move {
            client
                .post(format!("{pf}/responses"))
                .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"}))
                .send()
                .await
                .unwrap()
        })
    });
    let responses = futures_util::future::join_all(futures).await;

    for r in responses {
        let resp = r.unwrap();
        assert_eq!(resp.status(), 200, "every concurrent request must succeed");
    }

    assert_eq!(
        oauth_handle.hit_count(),
        1,
        "the singleflight must collapse all 8 concurrent stale-refresh attempts into ONE call \
         to the OAuth endpoint"
    );

    let acct = state.store.accounts().get("acct-1").await.unwrap().unwrap();
    assert_eq!(
        acct.status, "active",
        "the account must never be wrongly marked reauth_required by a losing racer"
    );

    let toks = state
        .store
        .accounts()
        .decrypt_tokens("acct-1", &state.cipher)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        toks.refresh_token, "new-refresh",
        "the persisted token must be the single winner's rotated refresh token"
    );
}

/// F2 (failure-path single-mark): N concurrent requests on the SAME stale account whose refresh
/// PERMANENTLY fails must hit the OAuth endpoint exactly ONCE and mark the account once — not
/// re-hammer OAuth per waiter. The winner's refresh fails and marks the account non-active; every
/// waiter, after taking the per-account lock, re-reads the account, sees it is no longer `active`,
/// and bails without calling `refresh` again (which would present its own now-dead token, re-classify,
/// and re-mark — serialized amplification on a doomed account). Without the status re-check the
/// endpoint is hit 8 times; with it, exactly once.
#[tokio::test]
async fn concurrent_stale_requests_on_failing_account_mark_once() {
    let upstream = MockUpstream::new(vec![r#"{"type":"response.completed"}"#.to_string()]);
    let upstream_url = upstream.spawn().await;

    let oauth = MockOAuth::error(400, "invalid_grant"); // classified permanent (reauth_required)
    let oauth_handle = oauth.clone();
    let oauth_url = oauth.spawn().await;

    let (pf, state) = spawn(oauth_url, upstream_url, STALE).await;

    const N: usize = 8;
    let client = reqwest::Client::new();
    let futures = (0..N).map(|_| {
        let client = client.clone();
        let pf = pf.clone();
        tokio::spawn(async move {
            client
                .post(format!("{pf}/responses"))
                .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"}))
                .send()
                .await
                .unwrap()
        })
    });
    let responses = futures_util::future::join_all(futures).await;

    for r in responses {
        assert_eq!(
            r.unwrap().status(),
            503,
            "a doomed (dead-token) account serves 503 to every request"
        );
    }

    assert_eq!(
        oauth_handle.hit_count(),
        1,
        "a permanently-failing refresh must be attempted ONCE across all 8 waiters — the losers \
         must see the winner's mark and bail, not re-hit OAuth with their own dead token"
    );

    let acct = state.store.accounts().get("acct-1").await.unwrap().unwrap();
    assert_eq!(
        acct.status, "reauth_required",
        "the account is marked exactly once by the single winner"
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
