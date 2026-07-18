//! M5a Task 8 — the milestone: `POLYFLARE_WS_UPSTREAM` wired into the REAL ingress stack (not the
//! executor in isolation). Two assertions matter here:
//!
//! 1. **Flag ON**: two sequential turns through `responses_handler` (via a real HTTP client hitting
//!    a real `axum::serve`d `build_app`) produce ONE WS handshake, the second turn's frame carries
//!    an anchor + delta-only input, and the client receives well-formed SSE both times.
//! 2. **Flag OFF (the regression net)**: `AppState.codex_executor` is `CodexExecutor` exactly as
//!    before this flag existed, driven against the HTTP `MockUpstream` — proving the flag's `false`
//!    branch is byte-for-byte today's behavior. (The FULL regression net is `cargo test
//!    --workspace`, especially the wedge suites; this test's flag-OFF case is a smoke-level proof
//!    that this specific AppState-construction path didn't change, not a substitute for that run.)
//!
//! `AppState` is built the way `tests/no_anchor_failover.rs:63-102` does, swapping in
//! `polyflare_server::app::build_codex_executor` (the M5a Task 8 selection seam) instead of a
//! hardcoded `Arc::new(CodexExecutor::new().unwrap())`.

use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use polyflare_codex::oauth::OAuthClient;
use polyflare_core::{CapacityWeighted, Continuity};
use polyflare_server::app::{build_app, build_codex_executor, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};
use polyflare_testkit::{MockUpstream, MockWsUpstream, ScriptedTurn};
use serde_json::json;

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

async fn drain(resp: reqwest::Response) -> String {
    let mut body = String::new();
    let mut s = resp.bytes_stream();
    while let Some(chunk) = s.next().await {
        body.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
    }
    body
}

/// Common `AppState` scaffolding both tests share: one active, unpooled Codex account, seeded into
/// a fresh store, wired against `upstream_base_url` (the mock's base URL — WS or HTTP, per
/// caller). Mirrors `tests/no_anchor_failover.rs:63-102`'s construction, with `codex_executor`
/// parameterized instead of hardcoded.
async fn spawn_state(
    codex_executor: Arc<dyn polyflare_core::Executor>,
    upstream_base_url: String,
) -> (String, Arc<AppState>) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[21u8; 32]).unwrap();
    store
        .accounts()
        .insert(
            &account("A"),
            &PlainTokens {
                access_token: "tokA".into(),
                refresh_token: "r".into(),
                id_token: "i".into(),
            },
            &cipher,
        )
        .await
        .unwrap();
    // Keep the tempdir alive for the server's lifetime (mirrors the existing tests' `std::mem::
    // forget(dir)` idiom — the OS reclaims it at process exit; these are short-lived test binaries).
    std::mem::forget(dir);
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_secs(30),
    ));
    let state = Arc::new(AppState {
        enforce_client_keys: false,
        codex_executor,
        control_client: polyflare_codex::build_client().expect("build control_client"),
        anthropic_executor: Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap()),
        selector: Arc::new(CapacityWeighted),
        pool_selectors: Default::default(),
        continuity,
        store,
        cipher: TokenCipher::from_key_bytes(&[21u8; 32]).unwrap(),
        oauth: OAuthClient::new("http://127.0.0.1:9".to_string()).unwrap(),
        upstream_base_url,
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
        starvation_wait_budget: std::time::Duration::from_secs(60),
        starvation_heartbeat: std::time::Duration::from_secs(10),
        starvation_metrics: polyflare_server::observability::StarvationMetrics::new(),
        stream_idle_timeout: std::time::Duration::from_secs(300),
        soft_drain_enabled: true,
        runtime: Default::default(),
    });
    let app = build_app(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), state)
}

/// **The milestone.** `POLYFLARE_WS_UPSTREAM` ON (`build_codex_executor(true)`): two sequential
/// turns through the REAL ingress stack (`responses_handler`, over real HTTP, exactly as a real
/// Codex client would call it) — not the executor driven in isolation — must:
/// - produce exactly ONE WS handshake to the mock upstream (connection reuse across turns),
/// - have the SECOND turn's wire frame carry the first turn's anchor and ONLY the new (delta) item,
/// - deliver well-formed SSE (`data: ...` containing `response.completed`) to the HTTP client both
///   times.
#[tokio::test]
async fn ws_flag_on_two_sequential_turns_reuse_one_handshake_and_send_a_delta() {
    let mock = MockWsUpstream::scripted(vec![
        ScriptedTurn::normal(vec![
            json!({"type": "response.output_text.delta", "delta": "hi"}).to_string(),
        ]),
        ScriptedTurn::normal(vec![
            json!({"type": "response.output_text.delta", "delta": "there"}).to_string(),
        ]),
    ]);
    let ws_base = mock.clone().spawn().await;

    let codex_executor = build_codex_executor(true).expect("build_codex_executor(true)");
    let (pf, _state) = spawn_state(codex_executor, ws_base).await;

    let client = reqwest::Client::new();

    // Turn 1: the client's first message. No anchor exists yet — a full (non-anchored) send.
    let r1 = client
        .post(format!("{pf}/responses"))
        .header("session_id", "sess-ws-e2e")
        .json(&json!({
            "model": "gpt-5.6-sol",
            "input": [
                {"role": "user", "content": "first message"},
            ],
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r1.status(), 200);
    let body1 = drain(r1).await;
    assert!(
        body1.starts_with("data: "),
        "turn 1 must be well-formed SSE: {body1}"
    );
    assert!(
        body1.contains("response.output_text.delta") && body1.contains("response.completed"),
        "turn 1 must carry the scripted delta + a completion: {body1}"
    );

    // Turn 2: the SAME session, with the real Codex-client behavior of resending the FULL
    // accumulated history (SPEC-M5-WEBSOCKET.md §2) — the prior message PLUS one new one.
    let r2 = client
        .post(format!("{pf}/responses"))
        .header("session_id", "sess-ws-e2e")
        .json(&json!({
            "model": "gpt-5.6-sol",
            "input": [
                {"role": "user", "content": "first message"},
                {"role": "user", "content": "second message"},
            ],
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r2.status(), 200);
    let body2 = drain(r2).await;
    assert!(
        body2.starts_with("data: "),
        "turn 2 must be well-formed SSE: {body2}"
    );
    assert!(
        body2.contains("response.output_text.delta") && body2.contains("response.completed"),
        "turn 2 must carry the scripted delta + a completion: {body2}"
    );

    // THE milestone: one handshake for two turns — the connection was reused, not re-dialed.
    assert_eq!(
        mock.handshake_count(),
        1,
        "two turns through the real ingress stack must reuse ONE WS connection"
    );

    let frames = mock.frames();
    assert_eq!(frames.len(), 2, "exactly one wire frame per turn");
    assert_eq!(
        frames[0].previous_response_id, None,
        "turn 1 has no prior response to anchor on"
    );
    assert_eq!(frames[0].input_len, 1, "turn 1: the one message it sent");
    assert_eq!(
        frames[1].previous_response_id,
        Some("resp_1".to_string()),
        "turn 2 must anchor on turn 1's completed response id — a real delta, not a full resend"
    );
    assert_eq!(
        frames[1].input_len, 1,
        "turn 2 must send ONLY the one new message, not the full 2-item history"
    );
}

/// **The regression net.** `POLYFLARE_WS_UPSTREAM` OFF (`build_codex_executor(false)`): the
/// selected executor is `CodexExecutor` exactly as before this flag existed, driven against the
/// HTTP `MockUpstream` — the same shape `tests/no_anchor_failover.rs` and every other pre-M5a
/// ingress test already exercises. This is a smoke-level proof that the flag's `false` branch
/// didn't change; the authoritative regression net is `cargo test --workspace` (this task's
/// verification step), especially the wedge suites (`wedge_regression`, `watchdog_race`,
/// `no_anchor_failover`, `signal_client`, `failure_routing`).
#[tokio::test]
async fn ws_flag_off_uses_the_http_mock_upstream_unchanged() {
    let mock = MockUpstream::with_ids(vec![
        r#"{"type":"response.output_text.delta","delta":"x"}"#.to_string(),
    ]);
    let http_base = mock.clone().spawn().await;

    let codex_executor = build_codex_executor(false).expect("build_codex_executor(false)");
    let (pf, _state) = spawn_state(codex_executor, http_base).await;

    let client = reqwest::Client::new();
    let r = client
        .post(format!("{pf}/responses"))
        .header("session_id", "sess-http-e2e")
        .json(&json!({"model": "m", "input": "hello"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let body = drain(r).await;
    assert!(
        body.contains("response.output_text.delta"),
        "flag OFF must still relay the HTTP mock's SSE content unchanged: {body}"
    );
}
