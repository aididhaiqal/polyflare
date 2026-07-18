//! SPEC-M5 §3.4: exactly one content-safe request-completion event must fire per request, and it
//! must never carry the request/response body, the client's `model` string, an account id, or a
//! bearer token. This hits the REAL `/responses` ingress path end-to-end (the same
//! spawn-a-real-server harness as `provider_dispatch.rs`) and captures `tracing` output with a
//! minimal custom `Subscriber` set as this test's thread-local default.
//!
//! `#[tokio::test]`'s default runtime flavor is `current_thread`, so the spawned server task runs
//! on the SAME OS thread as the test body — a thread-local `tracing` dispatcher set here is
//! therefore visible to the server's event emission too.

use std::sync::{Arc, Mutex};
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

/// A canary token value: distinctive enough that if it ever showed up in a log line, it could
/// only have come from here.
const SECRET_BEARER: &str = "super-secret-bearer-token-canary-should-never-leak";

fn tokens() -> PlainTokens {
    PlainTokens {
        access_token: SECRET_BEARER.to_string(),
        refresh_token: "r".to_string(),
        id_token: "i".to_string(),
    }
}

async fn spawn_polyflare(store: Store, upstream: String) -> String {
    let cipher = TokenCipher::from_key_bytes(&[77u8; 32]).unwrap();
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
        capture_fingerprint_path: None,
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

// --- a minimal capturing `tracing::Subscriber`, scoped to this test file --------------------
//
// Records every event on the `"polyflare_server::request"` target as a flat `field=value`
// string and ignores everything else (this crate's other tests, other crates' internal
// `tracing` traffic). Deliberately duplicated from (not shared with) the unit test in
// `polyflare_server::observability` — that one exercises `RequestLog::emit` directly; this one
// proves the real ingress wiring end-to-end.

struct Capture(Arc<Mutex<Vec<String>>>);

struct FieldVisitor(String);

impl tracing::field::Visit for FieldVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.push_str(&format!("{}={:?} ", field.name(), value));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.push_str(&format!("{}={} ", field.name(), value));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.0.push_str(&format!("{}={} ", field.name(), value));
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.0.push_str(&format!("{}={} ", field.name(), value));
    }
}

impl tracing::Subscriber for Capture {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.target() == "polyflare_server::request"
    }

    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        let mut visitor = FieldVisitor(String::new());
        event.record(&mut visitor);
        self.0.lock().unwrap().push(visitor.0);
    }

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}

#[tokio::test]
async fn responses_request_completion_event_is_content_safe() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let _guard = tracing::subscriber::set_default(Capture(captured.clone()));

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[77u8; 32]).unwrap();
    store
        .accounts()
        .insert(&account("codex-1", "codex"), &tokens(), &cipher)
        .await
        .unwrap();
    std::mem::forget(dir);

    let mock = MockUpstream::new(vec![
        r#"{"type":"response.output_text.delta","delta":"hi"}"#.to_string(),
        r#"{"type":"response.completed"}"#.to_string(),
    ]);
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(store, upstream).await;

    let secret_marker = "super-secret-conversation-marker-should-never-leak";
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({
            "model": "gpt-5.6-sol-canary-model-name",
            "input": secret_marker,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let _ = resp.bytes().await.unwrap();

    let events = captured.lock().unwrap();
    assert_eq!(
        events.len(),
        1,
        "expected exactly one request-completion event, got: {events:?}"
    );
    let line = &events[0];

    for expected in [
        "method=POST",
        "path=/responses",
        "provider=codex",
        "aliased=false",
        "status=200",
        "duration_ms=",
    ] {
        assert!(line.contains(expected), "missing `{expected}` in: {line}");
    }

    for forbidden in [
        secret_marker,
        "gpt-5.6-sol-canary-model-name",
        SECRET_BEARER,
        "codex-1",
        "Authorization",
        "Bearer",
        "input",
    ] {
        assert!(
            !line.contains(forbidden),
            "forbidden content `{forbidden}` leaked into request log: {line}"
        );
    }
}

#[tokio::test]
async fn responses_error_path_still_emits_a_content_safe_event() {
    // No account seeded at all: `/responses` 503s via the early `no_eligible` path, which never
    // resolves an account. The event must still fire, with the (structurally-fixed) `codex`
    // provider label and the real 503 status — not silently skipped just because nothing routed.
    let captured = Arc::new(Mutex::new(Vec::new()));
    let _guard = tracing::subscriber::set_default(Capture(captured.clone()));

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    std::mem::forget(dir);

    let mock = MockUpstream::new(vec![]);
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(store, upstream).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "gpt-5.6-sol"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 503);

    let events = captured.lock().unwrap();
    assert_eq!(
        events.len(),
        1,
        "expected exactly one request-completion event even on the no-eligible-account path"
    );
    let line = &events[0];
    assert!(line.contains("status=503"), "line: {line}");
    assert!(line.contains("provider=codex"), "line: {line}");
    assert!(line.contains("path=/responses"), "line: {line}");
}
