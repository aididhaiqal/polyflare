//! M5 capture-fixture mechanism, end-to-end: with `AppState.capture_fingerprint_path` set, a real
//! request through the ingress (both `/responses` and `/v1/messages`) must append a content-safe
//! structural fingerprint to the golden file — never the request's real bearer token, session/
//! thread/turn/window/installation ids, `model` string, or body content. Mirrors the
//! spawn-a-real-server harness `observability.rs` uses for the (related) content-safe request-log
//! guarantee; this is the same guarantee applied to `crate::fingerprint_capture` instead.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{CapacityWeighted, Continuity, Executor};
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

fn account(id: &str, provider: &str) -> Account {
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
        provider: provider.to_string(),
        pool: None,
    }
}

/// A canary bearer/session/turn-metadata value set: distinctive enough that if any of them ever
/// showed up in the golden capture file, it could only have come from this test's own request.
const FAKE_BEARER: &str = "sk-fake-e2e-canary-bearer-should-never-leak";
const FAKE_SESSION_ID: &str = "550e8400-e29b-41d4-a716-446655440000";
const FAKE_INSTALLATION_ID: &str = "install-e2e-canary-should-never-leak";
const FAKE_MODEL: &str = "gpt-5.6-sol-fp-capture-canary-model";
const FAKE_INPUT: &str = "e2e-conversation-content-canary-should-never-leak";

async fn spawn_polyflare(
    store: Store,
    upstream: String,
    capture_path: std::path::PathBuf,
) -> String {
    let cipher = TokenCipher::from_key_bytes(&[91u8; 32]).unwrap();
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_secs(30),
    ));
    let codex_executor: Arc<dyn Executor> = Arc::new(CodexExecutor::new().unwrap());
    let anthropic_executor: Arc<dyn Executor> =
        Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap());

    let state = Arc::new(AppState {
        enforce_client_keys: false,
        codex_executor,
        control_client: polyflare_codex::build_client().expect("build control_client"),
        anthropic_executor,
        selector: Arc::new(CapacityWeighted),
        pool_selectors: Default::default(),
        continuity,
        store,
        cipher,
        oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(),
        upstream_base_url: upstream,
        anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),
        refresh_locks: Default::default(),
        capture_fingerprint_path: Some(capture_path),
        codex_version: std::sync::Arc::new(polyflare_codex::CodexVersionCache::new().unwrap()),
        account_cache: std::sync::Arc::new(polyflare_server::account_cache::AccountCache::new()),
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
    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn responses_and_messages_requests_append_content_safe_goldens() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    store
        .accounts()
        .insert(
            &account("codex-1", "codex"),
            &PlainTokens {
                access_token: "tok".to_string(),
                refresh_token: "r".to_string(),
                id_token: "i".to_string(),
            },
            &TokenCipher::from_key_bytes(&[91u8; 32]).unwrap(),
        )
        .await
        .unwrap();
    let capture_path = dir.path().join("fingerprint_golden.jsonl");
    std::mem::forget(dir);

    let mock = MockUpstream::new(vec![
        r#"{"type":"response.output_text.delta","delta":"hi"}"#.to_string(),
        r#"{"type":"response.completed"}"#.to_string(),
    ]);
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(store, upstream, capture_path.clone()).await;

    let client = reqwest::Client::new();

    // A `/responses` request shaped like a real Codex CLI turn: a bearer, a session id, and a
    // JSON turn-metadata-style header carrying fake ids — exactly the content this capture
    // mechanism must never echo.
    let resp = client
        .post(format!("{pf}/responses"))
        .header("authorization", format!("Bearer {FAKE_BEARER}"))
        .header("session_id", FAKE_SESSION_ID)
        .header(
            "x-codex-turn-metadata",
            serde_json::json!({
                "installation_id": FAKE_INSTALLATION_ID,
                "session_id": FAKE_SESSION_ID,
            })
            .to_string(),
        )
        .header(
            "user-agent",
            "codex_cli_rs/9.9.9 (Test OS 1.2.3; testarch) test_term",
        )
        .json(&serde_json::json!({ "model": FAKE_MODEL, "input": FAKE_INPUT }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let _ = resp.bytes().await.unwrap();

    // A `/v1/messages` request too (no Anthropic account is seeded, so this 503s — but capture
    // fires unconditionally up front, before routing, so it must still be recorded).
    let resp2 = client
        .post(format!("{pf}/v1/messages"))
        .header("authorization", format!("Bearer {FAKE_BEARER}"))
        .json(&serde_json::json!({ "model": FAKE_MODEL }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp2.status(), 503);

    let content = std::fs::read_to_string(&capture_path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(
        lines.len(),
        2,
        "expected one golden JSON line per request: {content}"
    );

    let responses_record: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(responses_record["method"], "POST");
    assert_eq!(responses_record["path"], "/responses");
    let headers = responses_record["headers"].as_array().unwrap();
    let find = |name: &str| {
        headers
            .iter()
            .find(|h| h["name"] == name)
            .unwrap_or_else(|| panic!("missing header `{name}` in {headers:?}"))
    };
    assert_eq!(find("authorization")["value"], "<bearer redacted>");
    assert_eq!(find("session_id")["value"]["format"], "uuid");
    let turn_meta = find("x-codex-turn-metadata");
    assert_eq!(turn_meta["value"]["kind"], "json");
    let keys: Vec<&str> = turn_meta["value"]["keys"]
        .as_array()
        .unwrap()
        .iter()
        .map(|k| k.as_str().unwrap())
        .collect();
    assert!(keys.contains(&"installation_id"));
    assert!(keys.contains(&"session_id"));

    let messages_record: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
    assert_eq!(messages_record["method"], "POST");
    assert_eq!(messages_record["path"], "/v1/messages");

    // The content-safety guarantee: none of the real secret/id/content/model values sent above may
    // appear ANYWHERE in the golden file, in either record.
    for canary in [
        FAKE_BEARER,
        FAKE_SESSION_ID,
        FAKE_INSTALLATION_ID,
        FAKE_MODEL,
        FAKE_INPUT,
        "9.9.9",
        "1.2.3",
        "Bearer",
    ] {
        assert!(
            !content.contains(canary),
            "canary `{canary}` leaked into fingerprint golden: {content}"
        );
    }
}
