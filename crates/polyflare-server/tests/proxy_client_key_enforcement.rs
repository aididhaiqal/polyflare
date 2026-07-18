//! D18 Task 4: the bind-address-aware posture's WIRING — the extracted `proxy` sub-router +
//! conditional `require_client_key` layer in `crate::app::build_app`. The posture DECISION logic
//! itself (`crate::posture::resolve_proxy_enforcement`) is unit-tested in `posture.rs`'s own
//! `#[cfg(test)]` module without a real server; THIS suite proves the wiring built on top of that
//! decision — given `AppState.enforce_client_keys` is `true` or `false`, does the real router
//! actually behave as the plan's Global Constraints require, end to end over real HTTP?
//!
//! Every EXEMPTION the plan calls out by name gets its own test:
//! - the GET-426 WS-fallback shim must stay reachable keyless EVEN WHEN enforcement is on (the
//!   critical one — a keyless WS probe must degrade to 426, not be rejected with 401),
//! - `/dashboard` static assets stay open regardless of the proxy posture,
//! - `/api/*` keeps its OWN `require_admin` gate, unaffected by (and not doubly covered by) the
//!   proxy layer — a valid CLIENT key must NOT unlock `/api/*`, and the admin token must NOT be
//!   accepted as a client key either.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{CapacityWeighted, Continuity, Executor};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_server::keys::create_key;
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};
use polyflare_testkit::MockUpstream;

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn codex_account(id: &str) -> Account {
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

fn tokens() -> PlainTokens {
    PlainTokens {
        access_token: "tok".to_string(),
        refresh_token: "r".to_string(),
        id_token: "i".to_string(),
    }
}

fn ok_events() -> Vec<String> {
    vec![
        r#"{"type":"response.output_text.delta","delta":"hi"}"#.to_string(),
        r#"{"type":"response.completed"}"#.to_string(),
    ]
}

/// Builds a real `AppState` (real `Store`, real `build_app`) with `enforce_client_keys` set as
/// requested and `POLYFLARE_ADMIN_TOKEN`-equivalent set to `Some("admin-secret")` (so the `/api/*`
/// unaffected-by-the-proxy-layer test has something to gate on). Returns the base URL, the state
/// (for creating keys / asserting store side effects), and the `Store` handle is reachable via
/// `state.store`.
async fn spawn(enforce_client_keys: bool) -> (String, Arc<AppState>) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    std::mem::forget(dir);
    let cipher = TokenCipher::from_key_bytes(&[7u8; 32]).unwrap();
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_secs(30),
    ));

    let mock = MockUpstream::new(ok_events());
    let upstream = mock.spawn().await;

    store
        .accounts()
        .insert(&codex_account("codex-1"), &tokens(), &cipher)
        .await
        .unwrap();

    let state = Arc::new(AppState {
        codex_executor: Arc::new(CodexExecutor::new().unwrap()) as Arc<dyn Executor>,
        control_client: polyflare_codex::build_client().expect("build control_client"),
        anthropic_executor: Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap())
            as Arc<dyn Executor>,
        selector: Arc::new(CapacityWeighted),
        pool_selectors: Default::default(),
        continuity,
        store,
        cipher,
        oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(),
        upstream_base_url: upstream,
        anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),
        refresh_locks: Default::default(),
        capture_fingerprint_path: None,
        codex_version: Arc::new(polyflare_codex::CodexVersionCache::new().unwrap()),
        account_cache: Arc::new(polyflare_server::account_cache::AccountCache::new()),
        token_cache: Default::default(),
        runtime: Default::default(),
        admin_token: Some("admin-secret".to_string()),
        live_logs: false,
        log_bus: polyflare_server::log_bus::LogBus::new(1000),
        max_account_attempts: 3,
        failover_metrics: polyflare_server::observability::FailoverMetrics::new(),
        health_tier_metrics: polyflare_server::observability::HealthTierMetrics::new(),
        starvation_wait_budget: Duration::from_secs(60),
        starvation_heartbeat: Duration::from_secs(10),
        wake_jitter_ms: 0,        inflight_penalty_pct: 2.5,

        starvation_metrics: polyflare_server::observability::StarvationMetrics::new(),
        stream_idle_timeout: std::time::Duration::from_secs(300),
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

fn responses_body() -> serde_json::Value {
    serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"})
}

// ---------------------------------------------------------------------------------------------
// (a) a key exists / enforcement on ⇒ keyless POST 401, valid-key POST reaches the handler.
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn enforced_keyless_post_responses_is_401() {
    let (base, state) = spawn(true).await;
    create_key(&state.store, Some("caller"), now()).await.unwrap();

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/responses"))
        .json(&responses_body())
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        401,
        "enforcement on + no Authorization header ⇒ 401, not routed to the real handler"
    );
}

#[tokio::test]
async fn enforced_valid_key_post_responses_reaches_the_handler() {
    let (base, state) = spawn(true).await;
    let created = create_key(&state.store, Some("caller"), now()).await.unwrap();

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/responses"))
        .header("authorization", format!("Bearer {}", created.raw))
        .json(&responses_body())
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "a valid enabled client key must reach the real proxy handler"
    );
}

#[tokio::test]
async fn enforced_pooled_post_responses_also_requires_a_key() {
    let (base, state) = spawn(true).await;
    create_key(&state.store, None, now()).await.unwrap();

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/pool-a/responses"))
        .json(&responses_body())
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        401,
        "the pooled proxy path must be covered by the same enforced layer as the bare path"
    );
}

#[tokio::test]
async fn enforced_v1_messages_also_requires_a_key() {
    let (base, state) = spawn(true).await;
    create_key(&state.store, None, now()).await.unwrap();

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/v1/messages"))
        .json(&serde_json::json!({"model": "claude-3", "messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401, "/v1/messages must be covered too");
}

// ---------------------------------------------------------------------------------------------
// (b) no keys + open posture (the loopback-open path, represented here directly via
//     enforce_client_keys: false — the posture DECISION that produces `false` is unit-tested in
//     posture.rs) ⇒ keyless POST reaches the handler unchanged (today's zero-config behavior).
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn unenforced_keyless_post_responses_reaches_the_handler() {
    let (base, _state) = spawn(false).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/responses"))
        .json(&responses_body())
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "enforcement off ⇒ the proxy surface behaves exactly as before D18 (open)"
    );
}

// ---------------------------------------------------------------------------------------------
// (c) THE critical exemption: the GET-426 WS-fallback shim stays keyless EVEN WHEN enforcement is
//     on. A keyless WS-handshake probe must degrade to 426, never be rejected with 401.
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn enforced_keyless_get_responses_is_426_not_401() {
    let (base, state) = spawn(true).await;
    create_key(&state.store, None, now()).await.unwrap();

    let client = reqwest::Client::new();
    let resp = client.get(format!("{base}/responses")).send().await.unwrap();
    assert_eq!(
        resp.status(),
        426,
        "the GET-426 WS-fallback shim must be exempt from client-key enforcement — a keyless \
         handshake probe degrades to HTTP-SSE (426), it must never see a 401"
    );
}

#[tokio::test]
async fn enforced_keyless_get_pooled_responses_is_426_not_401() {
    let (base, state) = spawn(true).await;
    create_key(&state.store, None, now()).await.unwrap();

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{base}/pool-a/responses"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 426, "the pooled GET-426 shim is exempt too");
}

// ---------------------------------------------------------------------------------------------
// (d) `/dashboard` static assets stay open regardless of the proxy posture.
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn dashboard_reachable_keyless_when_enforced() {
    let (base, state) = spawn(true).await;
    create_key(&state.store, None, now()).await.unwrap();

    let client = reqwest::Client::new();
    let resp = client.get(format!("{base}/dashboard")).send().await.unwrap();
    assert_ne!(
        resp.status(),
        401,
        "/dashboard must never be gated by the client-key proxy layer"
    );
    assert_ne!(resp.status(), 403);
}

#[tokio::test]
async fn dashboard_reachable_keyless_when_unenforced() {
    let (base, _state) = spawn(false).await;

    let client = reqwest::Client::new();
    let resp = client.get(format!("{base}/dashboard")).send().await.unwrap();
    assert_ne!(resp.status(), 401);
}

// ---------------------------------------------------------------------------------------------
// (e) `/api/*` keeps its OWN `require_admin` gate — unaffected by, and not doubly covered by, the
//     proxy layer. A valid CLIENT key must not unlock `/api/*`; the admin token must not work as a
//     client key either.
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn api_whoami_still_requires_the_admin_token_when_proxy_enforcement_is_on() {
    let (base, state) = spawn(true).await;
    create_key(&state.store, None, now()).await.unwrap();

    let client = reqwest::Client::new();
    // No Authorization at all ⇒ still 401 from require_admin (unaffected by the new proxy layer,
    // which doesn't cover /api/* at all).
    let resp = client.get(format!("{base}/api/whoami")).send().await.unwrap();
    assert_eq!(resp.status(), 401);

    // The correct admin token ⇒ still works, exactly as before D18.
    let resp = client
        .get(format!("{base}/api/whoami"))
        .header("authorization", "Bearer admin-secret")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "the admin token must still unlock /api/* unaffected by D18");
}

#[tokio::test]
async fn a_valid_client_key_does_not_unlock_api_whoami() {
    let (base, state) = spawn(true).await;
    let created = create_key(&state.store, None, now()).await.unwrap();

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{base}/api/whoami"))
        .header("authorization", format!("Bearer {}", created.raw))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        401,
        "a client API key must NOT double as an admin token — /api/* only accepts \
         POLYFLARE_ADMIN_TOKEN, the proxy layer must not leak into it"
    );
}

#[tokio::test]
async fn the_admin_token_does_not_unlock_the_enforced_proxy_surface() {
    let (base, state) = spawn(true).await;
    create_key(&state.store, None, now()).await.unwrap();

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/responses"))
        .header("authorization", "Bearer admin-secret")
        .json(&responses_body())
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        401,
        "the admin token must NOT double as a client key — require_client_key only accepts a hash \
         match against the api_keys table"
    );
}
