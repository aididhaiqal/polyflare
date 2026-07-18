//! TA6(b) Task 5: PROACTIVE capability resolution. Tasks 1-3 built the REACTIVE core (a
//! `cyber_policy` rejection auto-moves the session, then stamps it sticky so LATER turns
//! pre-filter — see `cyber_auto_move.rs` / `sticky_cyber_prefilter.rs`). This suite proves the
//! two ADDITIONAL, per-turn (never persisted) true-sources for
//! `SelectionCtx.require_security_work_authorized`:
//!
//!   1. a cyber-TAGGED POOL (`POLYFLARE_POOL_CAPABILITIES=slug:security_work`) pre-filters any
//!      request routed through `/{pool}/responses` for that slug, from turn 1 — no rejection
//!      needed to discover the requirement.
//!   2. the `X-PolyFlare-Capability: security_work` request header does the same on ANY route.
//!
//! And, critically, that neither of these can ever CLOBBER Task 3's sticky-cyber directive value:
//! a session already sticky-cyber (persisted via `set_required_capability`, simulating a prior
//! turn's reactive move) must STILL require the capability even when THIS turn's pool is untagged
//! and carries no header — the three sources are OR'd, never overwritten.
//!
//! Reuses `sticky_cyber_prefilter.rs`'s adversarial mock: it answers `cyber_policy` to ANY account
//! whose bearer token isn't the designated capable one. If proactive resolution regressed (the
//! pool/header signal were dropped, or the OR became an overwrite), an unfiltered pick could land
//! on the non-capable account and this suite would see either a relayed `cyber_policy` frame or a
//! SECOND upstream attempt — either proves the pre-filter isn't working from turn 1.

use std::convert::Infallible;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use tokio::sync::Mutex as AsyncMutex;

use axum::body::Body;
use axum::extract::{Json, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::Response;
use axum::routing::post;
use axum::Router;
use bytes::Bytes;
use futures_util::stream;
use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{CapacityWeighted, Continuity};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};

/// Serializes every test in this file around `POLYFLARE_POOL_CAPABILITIES` (a process-global env
/// var the ingress reads fresh per request, per-turn, with no caching — see
/// `polyflare_server::config::pool_requires_capability`'s doc). Rust's test harness runs the
/// `#[tokio::test]` fns below on separate threads IN THE SAME PROCESS by default; without this
/// lock, two tests setting/clearing the var concurrently would race. Each test acquires the guard
/// before touching the env var and holds it for its entire body (through its assertions, which
/// `.await` on the HTTP round trip), so the four tests below always run one at a time with respect
/// to this shared state. An async-aware `tokio::sync::Mutex` (not `std::sync::Mutex`) is used
/// specifically because the guard is held across `.await` points.
fn env_lock() -> &'static AsyncMutex<()> {
    static LOCK: OnceLock<AsyncMutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| AsyncMutex::new(()))
}

fn account(id: &str, pool: Option<&str>, security_work_authorized: bool) -> Account {
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
        last_refresh: i64::MAX / 2,
        created_at: 1,
        status: "active".to_string(),
        deactivation_reason: None,
        reset_at: None,
        blocked_at: None,
        security_work_authorized,
        provider: "codex".to_string(),
        pool: pool.map(|p| p.to_string()),
    }
}

fn tokens(access_token: &str) -> PlainTokens {
    PlainTokens {
        access_token: access_token.to_string(),
        refresh_token: "r".into(),
        id_token: "i".into(),
    }
}

/// Answers `cyber_policy` (content-safety-mirrored: the mock's `message` is never asserted on by
/// this suite) to any account OTHER than `capable_token`; a clean `response.completed` stream to
/// the capable account. Identical contract to `sticky_cyber_prefilter.rs`'s `StickyMock`.
#[derive(Clone)]
struct CapMock {
    capable_token: String,
    counter: Arc<AtomicU32>,
    tokens_seen: Arc<Mutex<Vec<String>>>,
}

async fn cap_handler(
    State(mock): State<CapMock>,
    headers: HeaderMap,
    Json(_body): Json<serde_json::Value>,
) -> Response {
    let auth = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    mock.tokens_seen.lock().unwrap().push(auth.clone());

    if auth != format!("Bearer {}", mock.capable_token) {
        let frame = r#"{"type":"response.failed","response":{"id":"resp_fatal_cyber","status":"failed","error":{"code":"cyber_policy","message":"classified — must never leak"}}}"#;
        let s = stream::once(async move {
            Ok::<Bytes, Infallible>(Bytes::from(format!("data: {frame}\n\n")))
        });
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/event-stream")
            .body(Body::from_stream(s))
            .unwrap();
    }

    let n = mock.counter.fetch_add(1, Ordering::SeqCst) + 1;
    let id = format!("resp_{n}");
    let created = format!(r#"{{"type":"response.created","response":{{"id":"{id}"}}}}"#);
    let completed = format!(r#"{{"type":"response.completed","response":{{"id":"{id}"}}}}"#);
    let s = stream::iter(vec![
        Ok::<Bytes, Infallible>(Bytes::from(format!("data: {created}\n\n"))),
        Ok(Bytes::from(format!("data: {completed}\n\n"))),
    ]);
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .body(Body::from_stream(s))
        .unwrap()
}

async fn spawn_cap_mock(capable_token: String) -> (String, CapMock) {
    let mock = CapMock {
        capable_token,
        counter: Arc::new(AtomicU32::new(0)),
        tokens_seen: Arc::new(Mutex::new(Vec::new())),
    };
    let app = Router::new()
        .route("/responses", post(cap_handler))
        .with_state(mock.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), mock)
}

async fn spawn_app(store: Store, cipher: TokenCipher, upstream_url: String) -> String {
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_secs(30),
    ));
    let state = Arc::new(AppState {
        enforce_client_keys: false,
        codex_executor: Arc::new(CodexExecutor::new().unwrap()),
        control_client: polyflare_codex::build_client().expect("build control_client"),
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
        max_account_attempts: 3,
        failover_metrics: polyflare_server::observability::FailoverMetrics::new(),
        health_tier_metrics: polyflare_server::observability::HealthTierMetrics::new(),
        starvation_wait_budget: std::time::Duration::from_secs(60),
        starvation_heartbeat: std::time::Duration::from_secs(10),
        wake_jitter_ms: 0,
        inflight_penalty_pct: 2.5,
        lease_metrics: polyflare_server::observability::LeaseMetrics::new(),
        upstream_request_metrics: polyflare_server::observability::UpstreamRequestMetrics::new(),
        rate_limit_metrics: polyflare_server::observability::RateLimitMetrics::new(),

        starvation_metrics: polyflare_server::observability::StarvationMetrics::new(),
        stream_idle_timeout: std::time::Duration::from_secs(300),
        soft_drain_enabled: true,
        request_log_retention_days: 0,
        usage_history_retention_days: 0,
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

/// (1) A request to a CYBER-TAGGED POOL resolves `require_security_work_authorized = true` and
/// pre-filters to the capable account from turn 1 — no rejection ever occurs, no second attempt.
#[tokio::test]
async fn cyber_tagged_pool_prefilters_from_turn_one() {
    let _guard = env_lock().lock().await;
    unsafe {
        std::env::set_var("POLYFLARE_POOL_CAPABILITIES", "cyber:security_work");
    }

    let capable_token = "cyber-pool-capable".to_string();
    let (upstream, mock) = spawn_cap_mock(capable_token.clone()).await;

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[31u8; 32]).unwrap();
    // Both accounts tagged into the SAME pool "cyber" — only the capable one may ever be hit.
    store
        .accounts()
        .insert(
            &account("non-capable", Some("cyber"), false),
            &tokens("non-capable-tok"),
            &cipher,
        )
        .await
        .unwrap();
    store
        .accounts()
        .insert(
            &account("capable-acct", Some("cyber"), true),
            &tokens(&capable_token),
            &cipher,
        )
        .await
        .unwrap();

    let pf = spawn_app(store, cipher, upstream).await;
    let client = reqwest::Client::new();

    let r = client
        .post(format!("{pf}/cyber/responses"))
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": [{"a": 1}]}))
        .send()
        .await
        .unwrap();

    assert_eq!(r.status(), 200, "the pool-resolved pre-filter succeeds");
    let body = r.text().await.unwrap();
    assert!(
        !body.contains("cyber_policy"),
        "no rejection is ever relayed — the pool tag pre-filtered before turn 1: {body}"
    );
    assert!(body.contains("response.completed"));

    let tokens_seen = mock.tokens_seen.lock().unwrap().clone();
    assert_eq!(
        tokens_seen.len(),
        1,
        "pool-tag resolution picks the capable account on the FIRST attempt: {tokens_seen:?}"
    );
    assert_eq!(tokens_seen[0], format!("Bearer {capable_token}"));

    unsafe {
        std::env::remove_var("POLYFLARE_POOL_CAPABILITIES");
    }
}

/// (2) The `X-PolyFlare-Capability: security_work` header pre-filters on the BARE (unpooled)
/// route, with no pool tagging involved at all.
#[tokio::test]
async fn capability_header_prefilters_on_the_bare_route() {
    let _guard = env_lock().lock().await;
    // No pool tagging in play for this test — confirm the env var is unset so only the header
    // source can be responsible for the pre-filter.
    unsafe {
        std::env::remove_var("POLYFLARE_POOL_CAPABILITIES");
    }

    let capable_token = "header-capable".to_string();
    let (upstream, mock) = spawn_cap_mock(capable_token.clone()).await;

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[32u8; 32]).unwrap();
    // Unpooled accounts — the bare `/responses` route selects over both.
    store
        .accounts()
        .insert(
            &account("non-capable", None, false),
            &tokens("non-capable-tok"),
            &cipher,
        )
        .await
        .unwrap();
    store
        .accounts()
        .insert(
            &account("capable-acct", None, true),
            &tokens(&capable_token),
            &cipher,
        )
        .await
        .unwrap();

    let pf = spawn_app(store, cipher, upstream).await;
    let client = reqwest::Client::new();

    let r = client
        .post(format!("{pf}/responses"))
        .header("X-PolyFlare-Capability", "security_work")
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": [{"a": 1}]}))
        .send()
        .await
        .unwrap();

    assert_eq!(r.status(), 200, "the header-resolved pre-filter succeeds");
    let body = r.text().await.unwrap();
    assert!(
        !body.contains("cyber_policy"),
        "no rejection is ever relayed — the header pre-filtered before turn 1: {body}"
    );
    assert!(body.contains("response.completed"));

    let tokens_seen = mock.tokens_seen.lock().unwrap().clone();
    assert_eq!(
        tokens_seen.len(),
        1,
        "header resolution picks the capable account on the FIRST attempt: {tokens_seen:?}"
    );
    assert_eq!(tokens_seen[0], format!("Bearer {capable_token}"));
}

/// (3) THE OR, NOT AN OVERWRITE: a session already sticky-cyber (Task 3's persisted flag,
/// simulating a prior turn's reactive move) is routed THIS turn through a pool that is NOT tagged
/// cyber, with no capability header. The sticky requirement must STILL hold — a non-cyber pool
/// must never turn OFF a sticky session's capability requirement. This is the regression that
/// proves Task 5 ORs its two new sources onto Task 3's directive value instead of overwriting it.
#[tokio::test]
async fn sticky_session_still_requires_capability_in_a_non_cyber_pool() {
    let _guard = env_lock().lock().await;
    // "cyber" is tagged, but this test routes through "general" — untagged — so the ONLY possible
    // source for the requirement is the session's sticky flag from Task 3.
    unsafe {
        std::env::set_var("POLYFLARE_POOL_CAPABILITIES", "cyber:security_work");
    }

    let capable_token = "sticky-in-general-capable".to_string();
    let (upstream, mock) = spawn_cap_mock(capable_token.clone()).await;

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[33u8; 32]).unwrap();
    // Both accounts tagged into pool "general" — NOT the cyber-tagged "cyber" pool.
    store
        .accounts()
        .insert(
            &account("non-capable", Some("general"), false),
            &tokens("non-capable-tok"),
            &cipher,
        )
        .await
        .unwrap();
    store
        .accounts()
        .insert(
            &account("capable-acct", Some("general"), true),
            &tokens(&capable_token),
            &cipher,
        )
        .await
        .unwrap();

    let session_header = "sess-sticky-general";
    let session_key =
        polyflare_server::session_key::sha256_hex(format!("session:{session_header}").as_bytes());
    // Simulate Task 2's stamp: a PRIOR turn on this session already moved to a capable account.
    store
        .continuity()
        .ensure_session(&session_key, "soft", 1)
        .await
        .unwrap();
    store
        .continuity()
        .set_required_capability(&session_key, "security_work", 1)
        .await
        .unwrap();

    let pf = spawn_app(store, cipher, upstream).await;
    let client = reqwest::Client::new();

    let r = client
        .post(format!("{pf}/general/responses"))
        .header("session_id", session_header)
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": [{"a": 1}]}))
        .send()
        .await
        .unwrap();

    assert_eq!(
        r.status(),
        200,
        "the sticky requirement survives a non-cyber pool"
    );
    let body = r.text().await.unwrap();
    assert!(
        !body.contains("cyber_policy"),
        "the sticky flag alone still pre-filters, even in an untagged pool: {body}"
    );

    let tokens_seen = mock.tokens_seen.lock().unwrap().clone();
    assert_eq!(
        tokens_seen.len(),
        1,
        "the non-cyber pool did NOT turn off the sticky requirement: {tokens_seen:?}"
    );
    assert_eq!(
        tokens_seen[0],
        format!("Bearer {capable_token}"),
        "a non-cyber pool must not clobber Task 3's sticky-cyber directive"
    );

    unsafe {
        std::env::remove_var("POLYFLARE_POOL_CAPABILITIES");
    }
}

/// (4) Regression: a non-cyber pool, no header, and a non-sticky (plain) session ⇒ the
/// requirement stays false — an ordinary request against a single, non-capable account routes
/// exactly as before (200, no filter, no spurious "no authorized account" refusal).
#[tokio::test]
async fn non_cyber_pool_no_header_no_sticky_session_stays_unfiltered() {
    let _guard = env_lock().lock().await;
    // "cyber" is tagged, but this test routes through "general" — untagged.
    unsafe {
        std::env::set_var("POLYFLARE_POOL_CAPABILITIES", "cyber:security_work");
    }

    let capable_token = "unused-capable-tok".to_string();
    let (upstream, mock) = spawn_cap_mock(capable_token).await;

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[34u8; 32]).unwrap();
    // Only a NON-capable account exists in "general" — if the flag were ever spuriously true,
    // this would 503 with "no authorized account available".
    store
        .accounts()
        .insert(
            &account("plain-acct", Some("general"), false),
            &tokens("plain-tok"),
            &cipher,
        )
        .await
        .unwrap();

    let pf = spawn_app(store, cipher, upstream).await;
    let client = reqwest::Client::new();

    let r = client
        .post(format!("{pf}/general/responses"))
        .header("session_id", "sess-plain-general")
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": [{"a": 1}]}))
        .send()
        .await
        .unwrap();

    assert_eq!(
        r.status(),
        200,
        "no source is true ⇒ never capability-filtered"
    );
    let tokens_seen = mock.tokens_seen.lock().unwrap().clone();
    assert_eq!(tokens_seen.len(), 1);
    assert_eq!(tokens_seen[0], "Bearer plain-tok");

    unsafe {
        std::env::remove_var("POLYFLARE_POOL_CAPABILITIES");
    }
}
