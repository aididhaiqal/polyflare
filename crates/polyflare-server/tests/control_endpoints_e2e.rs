//! D17 Task 3 (final) — e2e proof for the codex CONTROL-endpoint surface (`thread/goal/*`,
//! `agent-identities/jwks` (+ `wham/` variant), `memories/trace_summarize`) wired onto the real
//! `crate::app::build_app` stack, behind the D18 client-key gate, with soft session→owner
//! affinity (Task 2) and the generic unary forward primitive (Task 1).
//!
//! The headline test (`sentinel_body_is_forwarded_but_never_reaches_the_request_log`) is THE
//! inviolable: a control request's body is proxied upstream verbatim (content works end-to-end)
//! but the persisted `request_log` row — content-free by construction, per
//! `crate::observability::RequestLog` — must never contain it. This mirrors
//! `client_key_never_log_e2e.rs`'s sentinel-capture idiom exactly, just for the control-body path
//! instead of the client-key path.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{Continuity, Executor, RoundRobin};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_server::keys::sha256_hex;
use polyflare_server::session_key::header_session_key;
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};
use polyflare_testkit::MockControlUpstream;

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

async fn seed_account(store: &Store, cipher: &TokenCipher, id: &str, token: &str) {
    store
        .accounts()
        .insert(
            &account(id),
            &PlainTokens {
                access_token: token.into(),
                refresh_token: "r".into(),
                id_token: "i".into(),
            },
            cipher,
        )
        .await
        .unwrap();
}

/// Builds a real `AppState` (real `Store`, real `build_app`) wired at `upstream_base_url =
/// "{mock_base}/codex"` — the SAME shape Task 1's own `control_forward` tests use
/// (`crates/polyflare-codex/tests/control_forward.rs`), so `control_url`'s strip-then-rejoin
/// produces exactly `{mock_base}/codex/<path>` / `{mock_base}/wham/<path>`, matching
/// `MockControlUpstream::spawn`'s own routes.
async fn spawn_app(enforce_client_keys: bool, mock_base: &str) -> (String, Arc<AppState>) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    std::mem::forget(dir);
    let cipher = TokenCipher::from_key_bytes(&[9u8; 32]).unwrap();
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_secs(30),
    ));

    let state = Arc::new(AppState {
        codex_executor: Arc::new(CodexExecutor::new().unwrap()) as Arc<dyn Executor>,
        control_client: polyflare_codex::build_client().expect("build control_client"),
        anthropic_executor: Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap())
            as Arc<dyn Executor>,
        selector: Arc::new(RoundRobin),
        pool_selectors: Default::default(),
        continuity,
        store,
        cipher,
        oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(),
        upstream_base_url: format!("{mock_base}/codex"),
        anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),
        refresh_locks: Default::default(),
        capture_fingerprint_path: None,
        codex_version: Arc::new(polyflare_codex::CodexVersionCache::new().unwrap()),
        account_cache: Arc::new(polyflare_server::account_cache::AccountCache::new()),
        token_cache: Default::default(),
        runtime: Default::default(),
        admin_token: None,
        live_logs: false,
        log_bus: polyflare_server::log_bus::LogBus::new(1000),
        max_account_attempts: 3,
        failover_metrics: polyflare_server::observability::FailoverMetrics::new(),
        health_tier_metrics: polyflare_server::observability::HealthTierMetrics::new(),
        starvation_wait_budget: Duration::from_secs(60),
        starvation_heartbeat: Duration::from_secs(10),
        wake_jitter_ms: 0,
        starvation_metrics: polyflare_server::observability::StarvationMetrics::new(),
        stream_idle_timeout: Duration::from_secs(300),
        soft_drain_enabled: true,
        enforce_client_keys,
    });

    let app = build_app(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), state)
}

/// Mirrors `client_key_never_log_e2e.rs::insert_key_for_raw` exactly.
async fn insert_key_for_raw(store: &Store, raw: &str, label: &str) {
    let hash = sha256_hex(raw);
    let prefix: String = raw.chars().take(15).collect();
    store
        .api_keys()
        .create(&format!("key_{label}"), &hash, &prefix, Some(label), now())
        .await
        .unwrap();
}

async fn rows_eventually(store: &Store) -> Vec<polyflare_store::RequestLogRow> {
    let mut rows = Vec::new();
    for _ in 0..50 {
        rows = store.request_log().list(10, 0).await.unwrap();
        if !rows.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    rows
}

// -------------------------------------------------------------------------------------------
// THE HEADLINE: a control body carrying a SENTINEL is forwarded to the upstream (proving
// forwarding genuinely works), the mock's response is relayed back to the client (status +
// filtered headers + body), and the persisted `request_log` row NEVER contains the sentinel.
// -------------------------------------------------------------------------------------------

#[tokio::test]
async fn sentinel_body_is_forwarded_but_never_reaches_the_request_log() {
    const SENTINEL: &str = "SENTINEL_TRACE_BODY_98765";

    let mock = MockControlUpstream::new(200, r#"{"ok":true}"#)
        .with_header("etag", "abc123")
        .with_header("x-internal-secret", "must-never-reach-client");
    let mock_base = mock.clone().spawn().await;

    let (base, state) = spawn_app(true, &mock_base).await;
    seed_account(&state.store, &state.cipher, "acct-a", "tok-a").await;
    insert_key_for_raw(&state.store, "sk-pf-control-test", "control").await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/memories/trace_summarize"))
        .header("authorization", "Bearer sk-pf-control-test")
        .header("x-codex-turn-state", "ts-sentinel-session")
        .body(format!(r#"{{"trace":"{SENTINEL}"}}"#))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "the mock's scripted status is relayed");
    assert_eq!(
        resp.headers().get("etag").map(|v| v.to_str().unwrap()),
        Some("abc123"),
        "an allow-listed response header is relayed to the client"
    );
    assert!(
        resp.headers().get("x-internal-secret").is_none(),
        "a non-allow-listed response header must be dropped"
    );
    let body = resp.text().await.unwrap();
    assert_eq!(body, r#"{"ok":true}"#, "the mock's body is relayed verbatim");

    let recorded = mock.last_request().expect("the mock received a request");
    assert_eq!(recorded.path, "/codex/memories/trace_summarize");
    assert!(
        String::from_utf8_lossy(&recorded.body).contains(SENTINEL),
        "the mock actually received the sentinel body — forwarding genuinely worked"
    );

    let rows = rows_eventually(&state.store).await;
    assert_eq!(rows.len(), 1, "exactly one content-free request_log row: {rows:?}");
    let row = &rows[0];
    let row_debug = format!("{row:?}");
    assert!(
        !row_debug.contains(SENTINEL),
        "the persisted request_log row must NEVER contain the control body, got: {row_debug}"
    );
    assert_eq!(row.status, 200);
    assert_eq!(row.path, "codex_control_memories/trace_summarize");
    assert_eq!(row.account_id.as_deref(), Some("acct-a"));
}

// -------------------------------------------------------------------------------------------
// Soft session→owner affinity: a control request carrying a session header lands on the
// SESSION'S OWNER account (asserted via the mock's recorded bearer, which equals the owner's
// raw access token since `last_refresh` is fresh — no OAuth refresh in play).
// -------------------------------------------------------------------------------------------

#[tokio::test]
async fn control_request_with_session_header_lands_on_the_owner_account() {
    let mock = MockControlUpstream::new(200, r#"{"keys":[]}"#);
    let mock_base = mock.clone().spawn().await;

    let (base, state) = spawn_app(true, &mock_base).await;
    seed_account(&state.store, &state.cipher, "acct-a", "tok-a").await;
    seed_account(&state.store, &state.cipher, "acct-b", "tok-b").await;
    insert_key_for_raw(&state.store, "sk-pf-affinity-test", "affinity").await;

    // Seed a continuity session row (under the SAME derivation `header_session_key` uses) naming
    // "acct-b" as the owner.
    let headers_for_key = {
        let mut h = axum::http::HeaderMap::new();
        h.insert("x-codex-turn-state", "ts-owned-b".parse().unwrap());
        h
    };
    let sk = header_session_key(&headers_for_key, None).unwrap();
    let t = now();
    state
        .store
        .continuity()
        .ensure_session(&sk.value, "hard", t)
        .await
        .unwrap();
    state
        .store
        .continuity()
        .record_completion(&sk.value, "hard", "acct-b", "resp_owned", "fp", 1, t)
        .await
        .unwrap();

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{base}/agent-identities/jwks"))
        .header("authorization", "Bearer sk-pf-affinity-test")
        .header("x-codex-turn-state", "ts-owned-b")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let recorded = mock.last_request().expect("the mock received a request");
    assert_eq!(
        recorded
            .headers
            .get("authorization")
            .and_then(|v| v.to_str().ok()),
        Some("Bearer tok-b"),
        "the request landed on the session's OWNER account (acct-b), not a freshly-selected one"
    );
}

// -------------------------------------------------------------------------------------------
// D18 gate inheritance: control routes sit on the SAME gated `proxy` sub-router as
// `/responses`/`/v1/messages` — a keyless request is rejected exactly like the existing proxy
// surface, and a valid key is forwarded.
// -------------------------------------------------------------------------------------------

#[tokio::test]
async fn keyless_control_request_is_401_when_enforced() {
    let mock = MockControlUpstream::new(200, r#"{"ok":true}"#);
    let mock_base = mock.clone().spawn().await;

    let (base, state) = spawn_app(true, &mock_base).await;
    seed_account(&state.store, &state.cipher, "acct-a", "tok-a").await;
    insert_key_for_raw(&state.store, "sk-pf-gate-test", "gate").await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/memories/trace_summarize"))
        .body(r#"{"trace":"x"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        401,
        "no Authorization header ⇒ 401, inheriting the D18 gate from the proxy sub-router"
    );
    assert_eq!(mock.request_count(), 0, "an unauthenticated request must never reach the upstream");

    let resp = client
        .post(format!("{base}/memories/trace_summarize"))
        .header("authorization", "Bearer sk-pf-gate-test")
        .body(r#"{"trace":"x"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "a valid key is forwarded");
}

// -------------------------------------------------------------------------------------------
// jwks (both variants) + thread/goal forward + return correctly.
// -------------------------------------------------------------------------------------------

#[tokio::test]
async fn jwks_and_wham_jwks_are_forwarded_and_returned() {
    let mock = MockControlUpstream::new(200, r#"{"keys":["k1"]}"#);
    let mock_base = mock.clone().spawn().await;

    let (base, state) = spawn_app(true, &mock_base).await;
    seed_account(&state.store, &state.cipher, "acct-a", "tok-a").await;
    insert_key_for_raw(&state.store, "sk-pf-jwks-test", "jwks").await;

    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base}/agent-identities/jwks"))
        .header("authorization", "Bearer sk-pf-jwks-test")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), r#"{"keys":["k1"]}"#);
    assert_eq!(mock.last_request().unwrap().path, "/codex/agent-identities/jwks");

    let resp = client
        .get(format!("{base}/wham/agent-identities/jwks"))
        .header("authorization", "Bearer sk-pf-jwks-test")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        mock.last_request().unwrap().path,
        "/wham/agent-identities/jwks",
        "the wham variant joins WITHOUT a /codex/ segment"
    );
}

#[tokio::test]
async fn thread_goal_set_clear_get_are_forwarded() {
    let mock = MockControlUpstream::new(200, r#"{"goal":"be nice"}"#);
    let mock_base = mock.clone().spawn().await;

    let (base, state) = spawn_app(true, &mock_base).await;
    seed_account(&state.store, &state.cipher, "acct-a", "tok-a").await;
    insert_key_for_raw(&state.store, "sk-pf-goal-test", "goal").await;

    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base}/thread/goal/set"))
        .header("authorization", "Bearer sk-pf-goal-test")
        .body(r#"{"goal":"be nice"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(mock.last_request().unwrap().path, "/codex/thread/goal/set");
    assert_eq!(mock.last_request().unwrap().method, "POST");

    let resp = client
        .post(format!("{base}/thread/goal/clear"))
        .header("authorization", "Bearer sk-pf-goal-test")
        .body("")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(mock.last_request().unwrap().path, "/codex/thread/goal/clear");

    let resp = client
        .get(format!("{base}/thread/goal/get"))
        .header("authorization", "Bearer sk-pf-goal-test")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(mock.last_request().unwrap().path, "/codex/thread/goal/get");
    assert_eq!(mock.last_request().unwrap().method, "GET");
}

// -------------------------------------------------------------------------------------------
// Routing: the new control paths never shadow, and are never shadowed by, `/responses` /
// `/{pool}/responses` — a `POST /responses` still reaches the real Codex-native handler
// (proven by observing the request the mock actually received at its own root: `/codex/responses`
// for the native path, vs `/codex/memories/trace_summarize` for the control path — two distinct
// paths on the SAME mock/account, confirming axum dispatched each to its own handler).
// -------------------------------------------------------------------------------------------

#[tokio::test]
async fn responses_and_control_routes_do_not_shadow_each_other() {
    let mock = MockControlUpstream::new(200, r#"{"ok":true}"#);
    let mock_base = mock.clone().spawn().await;

    let (base, state) = spawn_app(true, &mock_base).await;
    seed_account(&state.store, &state.cipher, "acct-a", "tok-a").await;
    insert_key_for_raw(&state.store, "sk-pf-shadow-test", "shadow").await;

    let client = reqwest::Client::new();

    // A control route call — must land on `/codex/memories/trace_summarize`.
    let _ = client
        .post(format!("{base}/memories/trace_summarize"))
        .header("authorization", "Bearer sk-pf-shadow-test")
        .body(r#"{"trace":"x"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(
        mock.last_request().unwrap().path,
        "/codex/memories/trace_summarize"
    );

    // `/responses` — CodexExecutor sends its own outbound request; the mock's catch-all records
    // whatever path arrives regardless of how the SSE-parse ultimately resolves client-side. What
    // matters here is that the OUTBOUND request path is `/codex/responses` — proving `/responses`
    // reached the real native handler (`CodexExecutor::execute`), not one of the control handlers.
    let _ = client
        .post(format!("{base}/responses"))
        .header("authorization", "Bearer sk-pf-shadow-test")
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"}))
        .send()
        .await;
    assert_eq!(
        mock.last_request().unwrap().path,
        "/codex/responses",
        "POST /responses must reach the real responses handler, not a control route"
    );

    // A pooled control-adjacent path segment (`thread`) must not be swallowed by `/{pool}/responses`'s
    // param route: `/thread/goal/set` has second segment `goal`, never `responses`, so no collision
    // is structurally possible — but exercise it directly as the routing proof anyway.
    let _ = client
        .post(format!("{base}/thread/goal/set"))
        .header("authorization", "Bearer sk-pf-shadow-test")
        .body(r#"{"goal":"x"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(mock.last_request().unwrap().path, "/codex/thread/goal/set");
}
