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

/// An inline upstream that always answers `POST /responses` with a fixed `status` and an OpenAI-
/// shaped error body carrying `code` (`{"error":{"code":"...","message":"..."}}`) — the exact shape
/// `polyflare_codex::executor::extract_error_code` parses (code only, never the message).
async fn spawn_error_code_upstream(status: u16, code: &'static str) -> String {
    use axum::response::IntoResponse;
    async fn respond(status: u16, code: &'static str) -> axum::response::Response {
        (
            StatusCode::from_u16(status).unwrap(),
            axum::Json(serde_json::json!({"error": {"code": code, "message": "do not persist this"}})),
        )
            .into_response()
    }
    let app = axum::Router::new().route(
        "/responses",
        axum::routing::post(move || respond(status, code)),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

/// An inline upstream that always answers `POST /responses` with a bare `500` and NO parseable
/// error code (plain text body) — the A7 regression guard: a non-permanent failure must still route
/// through the pre-existing transient-error path, unchanged.
async fn spawn_plain_500_upstream() -> String {
    async fn boom() -> axum::response::Response {
        (StatusCode::INTERNAL_SERVER_ERROR, "boom").into_response()
    }
    use axum::response::IntoResponse;
    let app = axum::Router::new().route("/responses", axum::routing::post(boom));
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
        admin_token: None,
        live_logs: false,
        log_bus: polyflare_server::log_bus::LogBus::new(1000),

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

/// A7: an upstream `401` carrying the reauth-required code `invalid_grant` must park the account
/// with a DURABLE `reauth_required` status (not just a runtime cooldown) — asserted via a direct
/// store read, not merely next-request exclusion — and must NOT also bump the transient
/// `error_count` / set a runtime `cooldown_until` (a terminal status supersedes health backoff; only
/// re-auth clears `reauth_required`, so a cooldown would wrongly auto-readmit a deauthed account).
#[tokio::test]
async fn a_401_invalid_grant_parks_a_durable_reauth_required_status() {
    let upstream = spawn_error_code_upstream(401, "invalid_grant").await;
    let (pf, state) = spawn(upstream).await;
    let body = serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"});

    let resp = reqwest::Client::new()
        .post(format!("{pf}/responses"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 502, "surfaces as the generic 502 like any other upstream failure");

    // Durable status write: read straight from the store, not the runtime overlay.
    let stored = state
        .store
        .accounts()
        .get("acct-1")
        .await
        .unwrap()
        .expect("account still exists");
    assert_eq!(
        stored.status, "reauth_required",
        "invalid_grant is a REAUTH_REQUIRED_FAILURE_CODES entry in classify_failure"
    );

    // The transient health-backoff fields must be untouched: no error_count bump, no cooldown.
    let mut snaps = vec![AccountSnapshot::new("acct-1")];
    state.runtime.overlay(&mut snaps, now());
    assert_eq!(
        snaps[0].error_count, 0,
        "a permanent/auth code must NOT also bump the transient error_count"
    );
    assert_eq!(
        snaps[0].cooldown_until, None,
        "cooldown_until stays null — only re-auth clears reauth_required"
    );
}

/// A7: `account_deactivated` maps to the OTHER permanent bucket — durable `deactivated`, not
/// `reauth_required` — proving the branch reads the FULL `classify_failure` table, not just the
/// reauth arm.
#[tokio::test]
async fn an_account_deactivated_code_parks_a_durable_deactivated_status() {
    let upstream = spawn_error_code_upstream(403, "account_deactivated").await;
    let (pf, state) = spawn(upstream).await;
    let body = serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"});

    let resp = reqwest::Client::new()
        .post(format!("{pf}/responses"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 502);

    let stored = state
        .store
        .accounts()
        .get("acct-1")
        .await
        .unwrap()
        .expect("account still exists");
    assert_eq!(stored.status, "deactivated");
}

/// A6 (resolved as retirement — see `record_quota_exceeded`'s doc comment and
/// `docs/PORTING-CODEXLB.md`'s A6 audit note for the full evidence trail): the REAL upstream wire
/// code for a quota-exhausted account is `insufficient_quota` (verified against `codex-rs`'s
/// `codex-api/src/sse/responses.rs:630` / `api_bridge.rs:21` and the `quota_exceeded_emits_single_
/// error_event` test in `codex-rs/core/tests/suite/quota_exceeded.rs`). Even when that exact code
/// DOES reach `FailureSignal.error_code` (this test forces it to, via the same
/// `spawn_error_code_upstream` helper A7's tests use), `classify_failure("insufficient_quota")` is
/// `Transient` (no quota bucket exists there — confirmed in `oauth.rs:105-123`), so the request path
/// must fall through to the ORDINARY status-keyed bucketing: a 429 still routes to
/// `record_rate_limit`, never to `record_quota_exceeded`. Distinguishing signal: `record_rate_limit`
/// bumps `error_count` and sets a cooldown at the 30s floor; `record_quota_exceeded` would leave
/// `error_count` at 0 and set a 120s cooldown instead — the two are asserted together so either
/// mistake (wrong counter OR wrong cooldown magnitude) fails this test.
#[tokio::test]
async fn an_insufficient_quota_code_on_a_429_still_routes_via_rate_limit_not_quota_exceeded() {
    let upstream = spawn_error_code_upstream(429, "insufficient_quota").await;
    let (pf, state) = spawn(upstream).await;
    let body = serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"});

    let resp = reqwest::Client::new()
        .post(format!("{pf}/responses"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 502, "surfaces as the generic 502 like any other upstream failure");

    // No durable park: Transient has no `.status()`, so A7's branch is a no-op here too.
    let stored = state
        .store
        .accounts()
        .get("acct-1")
        .await
        .unwrap()
        .expect("account still exists");
    assert_eq!(
        stored.status, "active",
        "a quota-shaped code with no classify_failure entry must not durably park the account"
    );

    let mut snaps = vec![AccountSnapshot::new("acct-1")];
    state.runtime.overlay(&mut snaps, now());
    assert_eq!(
        snaps[0].error_count, 1,
        "record_rate_limit bumped error_count — record_quota_exceeded (which does NOT bump it) was \
         not called"
    );
    assert_eq!(
        snaps[0].cooldown_until,
        Some(snaps[0].last_error_at.unwrap() + 30),
        "the 30s rate-limit floor (no Retry-After header here), NOT the 120s quota cooldown"
    );
}

/// A6 companion: the OTHER real quota wire code, `usage_not_included` (verified against `codex-rs`'s
/// `codex-api/src/sse/responses.rs:634` / `api_bridge.rs:22,112-113`), arriving on a NON-429 status
/// (403) still routes via the ordinary transient bucket (`record_transient_error`), never
/// `record_quota_exceeded`. Distinguishing signal: transient sets NO cooldown at all, while
/// `record_quota_exceeded` would set one (+120s) without bumping `error_count` — the opposite
/// signature, so either mistake fails this test too.
#[tokio::test]
async fn a_usage_not_included_code_on_a_403_still_routes_via_transient_not_quota_exceeded() {
    let upstream = spawn_error_code_upstream(403, "usage_not_included").await;
    let (pf, state) = spawn(upstream).await;
    let body = serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"});

    let resp = reqwest::Client::new()
        .post(format!("{pf}/responses"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 502);

    let stored = state
        .store
        .accounts()
        .get("acct-1")
        .await
        .unwrap()
        .expect("account still exists");
    assert_eq!(stored.status, "active");

    let mut snaps = vec![AccountSnapshot::new("acct-1")];
    state.runtime.overlay(&mut snaps, now());
    assert_eq!(
        snaps[0].error_count, 1,
        "record_transient_error bumped error_count"
    );
    assert_eq!(
        snaps[0].cooldown_until, None,
        "record_transient_error sets no cooldown — a 120s quota cooldown here would prove \
         record_quota_exceeded fired instead"
    );
}

/// A7 regression guard: a NON-permanent failure (a plain 500 with no parseable error code) must
/// route through the pre-existing transient-error path exactly as before A7 — durable `status`
/// stays `active`, and the runtime `error_count` (not a durable write) is what bumps.
#[tokio::test]
async fn a_plain_500_with_no_code_still_routes_transient_not_durable() {
    let upstream = spawn_plain_500_upstream().await;
    let (pf, state) = spawn(upstream).await;
    let body = serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"});

    let resp = reqwest::Client::new()
        .post(format!("{pf}/responses"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 502);

    let stored = state
        .store
        .accounts()
        .get("acct-1")
        .await
        .unwrap()
        .expect("account still exists");
    assert_eq!(
        stored.status, "active",
        "a non-permanent failure must NOT durably park the account"
    );

    let mut snaps = vec![AccountSnapshot::new("acct-1")];
    state.runtime.overlay(&mut snaps, now());
    assert_eq!(
        snaps[0].error_count, 1,
        "unchanged existing behavior: a plain 500 still bumps the transient error_count"
    );
}
