//! TA6(b) Task 3: persist a sticky-cyber flag on the session so LATER turns pre-filter to
//! capability-holding accounts FROM THE START, instead of re-hitting a `cyber_policy` rejection
//! every turn. Task 2 (`cyber_auto_move.rs`) proved the reactive move: an owner's rejection
//! reroutes to a capable account and re-homes ownership. This suite drives the FULL ingress to
//! prove the follow-on cost-once contract: once a session is stamped sticky, a subsequent turn's
//! selection is capability-filtered up front — `SelectionCtx.require_security_work_authorized` is
//! read straight off the session row, so the account is picked directly and NO second rejection
//! is ever needed to get there.
//!
//! The mock enforces this adversarially: it answers `cyber_policy` to ANY account that isn't the
//! capability-holder. If the pre-filter wiring regressed (the sticky flag were dropped, or never
//! threaded into `SelectionCtx`), an unfiltered pick could land on the non-capable account and
//! this suite would see either a `cyber_policy` frame relayed, or a SECOND upstream attempt (a
//! reject-then-reroute round trip) — either failure proves the cost is being paid again, not once.

use std::convert::Infallible;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

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

fn account(id: &str, security_work_authorized: bool) -> Account {
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
        pool: None,
    }
}

fn tokens(access_token: &str) -> PlainTokens {
    PlainTokens {
        access_token: access_token.to_string(),
        refresh_token: "r".into(),
        id_token: "i".into(),
    }
}

/// Answers `cyber_policy` (content-safety-mirrored: message is never asserted on) to any account
/// OTHER than `capable_token`; a clean `response.completed` stream to the capable account.
#[derive(Clone)]
struct StickyMock {
    capable_token: String,
    counter: Arc<AtomicU32>,
    tokens_seen: Arc<Mutex<Vec<String>>>,
}

async fn sticky_handler(
    State(mock): State<StickyMock>,
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

async fn spawn_sticky_mock(capable_token: String) -> (String, StickyMock) {
    let mock = StickyMock {
        capable_token,
        counter: Arc::new(AtomicU32::new(0)),
        tokens_seen: Arc::new(Mutex::new(Vec::new())),
    };
    let app = Router::new()
        .route("/responses", post(sticky_handler))
        .with_state(mock.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), mock)
}

async fn spawn_app(store: Store, cipher: TokenCipher, upstream_url: String) -> (String, Arc<AppState>) {
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
        starvation_metrics: polyflare_server::observability::StarvationMetrics::new(),
        stream_idle_timeout: std::time::Duration::from_secs(300),
        soft_drain_enabled: true,
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

/// (a) COST-ONCE: a session already stamped sticky-cyber (as if a prior turn's cyber move already
/// happened — Task 2's job, simulated directly via the repo so this suite tests Task 3 in
/// isolation) sends a fresh, UNANCHORED turn. With no anchor and no prior owner recorded, ordinary
/// ownership pinning plays NO part in the pick (`ContinuityDirective.pin_account` is `None`) — the
/// selector's full-pool pick is the ONLY thing that can land on the capable account. It does, on
/// the FIRST attempt, because `prepare` read the sticky flag and set
/// `SelectionCtx.require_security_work_authorized = true` for this turn.
#[tokio::test]
async fn sticky_session_selects_the_capable_account_directly_with_no_second_rejection() {
    let capable_token = "capable-tok".to_string();
    let (upstream, mock) = spawn_sticky_mock(capable_token.clone()).await;

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[21u8; 32]).unwrap();
    // Two accounts: a non-capable one (would earn a `cyber_policy` rejection) and the capable one.
    store
        .accounts()
        .insert(&account("non-capable", false), &tokens("non-capable-tok"), &cipher)
        .await
        .unwrap();
    store
        .accounts()
        .insert(&account("capable-acct", true), &tokens(&capable_token), &cipher)
        .await
        .unwrap();

    let session_header = "sess-sticky-a";
    let session_key =
        polyflare_server::session_key::sha256_hex(format!("session:{session_header}").as_bytes());
    // Simulate Task 2's stamp: a prior turn on this session already moved to a capable account.
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

    let (pf, _state) = spawn_app(store, cipher, upstream).await;
    let client = reqwest::Client::new();

    let r = client
        .post(format!("{pf}/responses"))
        .header("session_id", session_header)
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": [{"a": 1}]}))
        .send()
        .await
        .unwrap();

    assert_eq!(r.status(), 200, "the pre-filtered pick succeeds cleanly");
    let body = r.text().await.unwrap();
    assert!(
        !body.contains("cyber_policy"),
        "no rejection is ever relayed when the pre-filter already picked the capable account: {body}"
    );
    assert!(body.contains("response.completed"));

    // THE COST-ONCE PROOF: exactly ONE upstream attempt — the pre-filter landed on the capable
    // account immediately, no reject-then-reroute round trip was needed.
    let tokens_seen = mock.tokens_seen.lock().unwrap().clone();
    assert_eq!(
        tokens_seen.len(),
        1,
        "sticky pre-filter picks the capable account on the FIRST attempt: {tokens_seen:?}"
    );
    assert_eq!(tokens_seen[0], format!("Bearer {capable_token}"));
}

/// (b) Regression: a session with NO cyber history (never stamped) is never capability-filtered —
/// an ordinary request against a single, non-capable account routes exactly as before (200, no
/// filter, no spurious "no authorized account" refusal).
#[tokio::test]
async fn non_cyber_session_routes_normally_without_any_capability_filter() {
    let capable_token = "unused-capable-tok".to_string();
    let (upstream, mock) = spawn_sticky_mock(capable_token).await;

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[22u8; 32]).unwrap();
    // Only a NON-capable account exists — if the flag were ever spuriously true, this would 503.
    store
        .accounts()
        .insert(&account("plain-acct", false), &tokens("plain-tok"), &cipher)
        .await
        .unwrap();

    let (pf, _state) = spawn_app(store, cipher, upstream).await;
    let client = reqwest::Client::new();

    let r = client
        .post(format!("{pf}/responses"))
        .header("session_id", "sess-sticky-b")
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": [{"a": 1}]}))
        .send()
        .await
        .unwrap();

    assert_eq!(
        r.status(),
        200,
        "a non-cyber session is never capability-filtered"
    );
    let tokens_seen = mock.tokens_seen.lock().unwrap().clone();
    assert_eq!(tokens_seen.len(), 1);
    assert_eq!(tokens_seen[0], "Bearer plain-tok");
}
