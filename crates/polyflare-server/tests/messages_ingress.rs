//! The native Anthropic-Messages ingress path: `/v1/messages` selects only Anthropic-provider
//! accounts and relays through `AnthropicExecutor`; continuity is a no-op (SPEC-M4 §3.7 — no
//! `previous_response_id`-style anchor exists for this backend, so the watchdog never arms).

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
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

fn anthropic_account(id: &str) -> Account {
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
        provider: "anthropic".to_string(),
        pool: None,
    }
}

fn codex_account(id: &str) -> Account {
    Account {
        provider: "codex".to_string(),
        ..anthropic_account(id)
    }
}

fn tokens() -> PlainTokens {
    PlainTokens {
        access_token: "tok".to_string(),
        refresh_token: "r".to_string(),
        id_token: "i".to_string(),
    }
}

async fn spawn_polyflare(store: Store, anthropic_upstream: String) -> String {
    spawn_polyflare_full(store, "http://127.0.0.1:9".to_string(), anthropic_upstream).await
}

/// Like `spawn_polyflare`, but also lets a test point the Codex upstream at a live mock (needed to
/// exercise the M4b-wiring cross-provider `/v1/messages` -> Codex path — `spawn_polyflare`'s
/// hardcoded dummy Codex address is fine for the native-only tests above, which never route there).
async fn spawn_polyflare_full(
    store: Store,
    codex_upstream: String,
    anthropic_upstream: String,
) -> String {
    let cipher = TokenCipher::from_key_bytes(&[21u8; 32]).unwrap();
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
        upstream_base_url: codex_upstream,
        anthropic_upstream_base_url: anthropic_upstream,
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
        health_tier_metrics: polyflare_server::observability::HealthTierMetrics::new(),
        starvation_wait_budget: std::time::Duration::from_secs(60),
        starvation_heartbeat: std::time::Duration::from_secs(10),
        wake_jitter_ms: 0,
        inflight_penalty_pct: 2.5,
        lease_metrics: polyflare_server::observability::LeaseMetrics::new(),

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

#[tokio::test]
async fn messages_relays_to_the_anthropic_executor() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    // Must match the cipher `spawn_polyflare` builds `AppState` with ([21u8; 32]) — otherwise
    // `resolve_core_account`'s `decrypt_tokens` fails and this 500s instead of routing (same
    // fix already applied to `provider_dispatch.rs`'s analogous fixture).
    let cipher = TokenCipher::from_key_bytes(&[21u8; 32]).unwrap();
    store
        .accounts()
        .insert(&anthropic_account("anthropic-1"), &tokens(), &cipher)
        .await
        .unwrap();
    std::mem::forget(dir);

    let mock = MockUpstream::new(vec![
        r#"{"type":"content_block_delta","delta":{"type":"text_delta","text":"hi"}}"#.to_string(),
        r#"{"type":"message_stop"}"#.to_string(),
    ]);
    let handle = mock.clone();
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(store, upstream).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{pf}/v1/messages"))
        .json(&serde_json::json!({
            // Deliberately NOT an opus/sonnet/haiku substring (M4b-wiring: those now alias to
            // Codex — see `messages_aliases_opus_to_codex_and_relays_translated_anthropic_sse`
            // below) so this exercises the genuinely-unaliased native Anthropic path.
            "model": "claude-3-5-legacy-model",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let mut body = String::new();
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        body.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
    }
    assert!(body.contains("content_block_delta"));
    assert!(body.contains("message_stop"));
    assert_eq!(
        handle.last_body().unwrap()["model"],
        "claude-3-5-legacy-model"
    );
}

#[tokio::test]
async fn messages_returns_503_when_pool_has_no_anthropic_account() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    std::mem::forget(dir);

    let pf = spawn_polyflare(store, "http://127.0.0.1:9".to_string()).await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{pf}/v1/messages"))
        // Deliberately NOT an opus/sonnet/haiku substring — see the model-choice comment in
        // `messages_relays_to_the_anthropic_executor` above.
        .json(&serde_json::json!({"model": "claude-3-5-legacy-model", "messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 503);
}

/// M4b-wiring, the headline cross-provider path: a `claude-opus-...` model string aliases to
/// Codex's `gpt-5.6-sol` @ high effort (`polyflare_server::alias::lookup_alias`). This asserts (a)
/// the upstream Codex mock received the remapped `model` + injected `reasoning.effort`, and (b)
/// the client-facing body is genuine Anthropic-Messages SSE (`message_start`/`content_block_*`/
/// `message_stop`), not the raw OpenAI-Responses shape the mock actually emitted.
#[tokio::test]
async fn messages_aliases_opus_to_codex_and_relays_translated_anthropic_sse() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[21u8; 32]).unwrap();
    store
        .accounts()
        .insert(&codex_account("codex-1"), &tokens(), &cipher)
        .await
        .unwrap();
    std::mem::forget(dir);

    // A scripted OpenAI-Responses turn: one text block, "hi".
    let mock = MockUpstream::new(vec![
        r#"{"type":"response.created","response":{"id":"resp_1","status":"in_progress","model":"gpt-5.6-sol","usage":null}}"#.to_string(),
        r#"{"type":"response.output_item.added","item":{"id":"item_1","type":"message","role":"assistant","content":[]}}"#.to_string(),
        r#"{"type":"response.content_part.added","item_id":"item_1","part":{"type":"output_text","text":"","annotations":[]}}"#.to_string(),
        r#"{"type":"response.output_text.delta","item_id":"item_1","delta":"hi"}"#.to_string(),
        r#"{"type":"response.output_text.done","item_id":"item_1","text":"hi"}"#.to_string(),
        r#"{"type":"response.completed","response":{"id":"resp_1","status":"completed","model":"gpt-5.6-sol","usage":{"output_tokens":1}}}"#.to_string(),
    ]);
    let handle = mock.clone();
    let codex_upstream = mock.spawn().await;
    let pf = spawn_polyflare_full(store, codex_upstream, "http://127.0.0.1:9".to_string()).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{pf}/v1/messages"))
        .json(&serde_json::json!({
            "model": "claude-opus-4-1-20250805",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // (a) the Codex upstream received the remapped model + injected reasoning effort.
    let sent = handle.last_body().unwrap();
    assert_eq!(sent["model"], "gpt-5.6-sol");
    assert_eq!(sent["reasoning"]["effort"], "high");

    // (b) the client sees Anthropic-Messages SSE, not the raw OpenAI-Responses shape.
    let mut body = String::new();
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        body.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
    }
    assert!(body.contains("message_start"), "body: {body}");
    assert!(body.contains("content_block_delta"), "body: {body}");
    assert!(body.contains("message_stop"), "body: {body}");
    assert!(
        !body.contains("response.output_text.delta"),
        "client must never see the raw OpenAI-Responses event shape: {body}"
    );
}

/// An aliased-to-Codex request with no Codex account in the pool: `filter_by_provider(Codex)`
/// leaves no candidates, so this 503s exactly like the native path's empty-pool case above — it
/// must NOT silently fall back to the (present) Anthropic account, since an aliased turn's
/// translated body is Codex-shaped and would be meaningless sent to an Anthropic backend.
#[tokio::test]
async fn messages_aliased_to_codex_returns_503_when_no_codex_account_is_seeded() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[21u8; 32]).unwrap();
    store
        .accounts()
        .insert(&anthropic_account("anthropic-1"), &tokens(), &cipher)
        .await
        .unwrap();
    std::mem::forget(dir);

    let pf = spawn_polyflare(store, "http://127.0.0.1:9".to_string()).await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{pf}/v1/messages"))
        .json(&serde_json::json!({
            "model": "claude-opus-4-1-20250805",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 503);
}
