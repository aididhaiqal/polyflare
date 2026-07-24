//! D14a Task 2 (final) — e2e proof for `POST /responses/compact`, the UNARY passthrough the real
//! Codex CLI emits (`codex-rs client.rs:159`) that PolyFlare previously 404'd. Modeled directly on
//! `tests/control_endpoints_e2e.rs` (same `spawn_app`/`MockControlUpstream`/sentinel idiom), but
//! covers the compact-specific crux: owner affinity here must be derived from the request BODY's
//! `prompt_cache_key` (via `crate::session_key::parse_inbound`), not just a header — a regression
//! that silently swapped in the header-only `resolve_control_account` would still compile and would
//! still "work" for the no-affinity cases, so the affinity test below is built to FAIL in that case
//! (see its doc for the exact mechanism).

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{Continuity, Executor, RoundRobin};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_server::keys::sha256_hex;
use polyflare_server::runtime_settings::{RuntimeSettings, RuntimeSettingsFields};
use polyflare_server::session_key::{header_session_key, parse_inbound_scoped};
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};
use polyflare_testkit::MockControlUpstream;

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// Mirrors `control_endpoints_e2e.rs::account`, but takes an explicit `email` so the
/// content-safety test can seed a grep-able sentinel email (mirroring `pace_e2e.rs`'s idiom).
fn account(id: &str, email: &str) -> Account {
    Account {
        id: id.to_string(),
        chatgpt_account_id: None,
        chatgpt_user_id: None,
        email: email.to_string(),
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

async fn seed_account(store: &Store, cipher: &TokenCipher, id: &str, email: &str, token: &str) {
    store
        .accounts()
        .insert(
            &account(id, email),
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
/// "{mock_base}/codex"` — identical shape to `control_endpoints_e2e.rs::spawn_app`, so
/// `control_url`'s strip-then-rejoin produces exactly `{mock_base}/codex/<path>`, matching
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
        runtime_settings: Arc::new(RuntimeSettings::new_from_fields(RuntimeSettingsFields {
            max_account_attempts: 3,
            starvation_wait_budget: Duration::from_secs(60),
            starvation_heartbeat: Duration::from_secs(10),
            wake_jitter_ms: 0,
            stream_idle_timeout: Duration::from_secs(300),
            inflight_penalty_pct: 2.5,
            soft_drain_enabled: true,
            request_log_retention_days: 0,
            usage_history_retention_days: 0,
            live_logs: false,
        })),
        ws_downstream: false,
        ws_relay_idle: polyflare_server::ws_relay::WsRelayIdlePolicy::default(),
        log_bus: polyflare_server::log_bus::LogBus::new(1000),
        failover_metrics: polyflare_server::observability::FailoverMetrics::new(),
        health_tier_metrics: polyflare_server::observability::HealthTierMetrics::new(),
        lease_metrics: polyflare_server::observability::LeaseMetrics::new(),
        upstream_request_metrics: polyflare_server::observability::UpstreamRequestMetrics::new(),
        rate_limit_metrics: polyflare_server::observability::RateLimitMetrics::new(),
        relay_metrics: polyflare_server::observability::RelayMetrics::new(),
        model_catalog: polyflare_server::model_catalog::floor_only_model_catalog(),

        starvation_metrics: polyflare_server::observability::StarvationMetrics::new(),
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

/// Mirrors `control_endpoints_e2e.rs::insert_key_for_raw` exactly.
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
// 1. Forwarding works (404 gone): a compact POST reaches the mock at `/codex/responses/compact`
//    (proving `control_url` produced the right suffix), and the client gets the mock's JSON body.
// -------------------------------------------------------------------------------------------

#[tokio::test]
async fn compact_is_forwarded_and_the_mock_response_is_relayed() {
    let mock = MockControlUpstream::new(200, r#"{"output":[{"type":"message"}]}"#)
        .with_header("x-codex-turn-state", "compact-turn-state-1");
    let mock_base = mock.clone().spawn().await;

    let (base, state) = spawn_app(true, &mock_base).await;
    seed_account(
        &state.store,
        &state.cipher,
        "acct-a",
        "u@example.test",
        "tok-a",
    )
    .await;
    insert_key_for_raw(&state.store, "sk-pf-compact-fwd-test", "compact-fwd").await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/responses/compact"))
        .header("authorization", "Bearer sk-pf-compact-fwd-test")
        .body(r#"{"model":"gpt-5.6-sol","input":"hi"}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "compact POST no longer 404s");
    assert_eq!(
        resp.headers()
            .get("x-codex-turn-state")
            .and_then(|value| value.to_str().ok()),
        Some("compact-turn-state-1"),
        "the compact response's per-turn routing state must reach Codex"
    );
    let body = resp.text().await.unwrap();
    assert_eq!(
        body, r#"{"output":[{"type":"message"}]}"#,
        "the mock's body is relayed verbatim"
    );

    let recorded = mock.last_request().expect("the mock received a request");
    assert_eq!(
        recorded.path, "/codex/responses/compact",
        "control_url produced the right upstream suffix"
    );
}

// -------------------------------------------------------------------------------------------
// 2. Content-safety: a sentinel in the compact body reaches the mock (forwarding genuinely
//    works) but NEVER the persisted request_log row — and neither does a seeded sentinel
//    account email/token.
// -------------------------------------------------------------------------------------------

#[tokio::test]
async fn sentinel_compact_body_is_forwarded_but_never_reaches_the_request_log() {
    const SENTINEL: &str = "SENTINEL_COMPACT_BODY_4242";
    const SENTINEL_EMAIL: &str = "sentinel-compact-user@example.test";
    const SENTINEL_TOKEN: &str = "sk-SENTINEL-COMPACT-TOKEN";

    let mock = MockControlUpstream::new(200, r#"{"output":[]}"#);
    let mock_base = mock.clone().spawn().await;

    let (base, state) = spawn_app(true, &mock_base).await;
    seed_account(
        &state.store,
        &state.cipher,
        "acct-a",
        SENTINEL_EMAIL,
        SENTINEL_TOKEN,
    )
    .await;
    insert_key_for_raw(
        &state.store,
        "sk-pf-compact-sentinel-test",
        "compact-sentinel",
    )
    .await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/responses/compact"))
        .header("authorization", "Bearer sk-pf-compact-sentinel-test")
        .body(format!(
            r#"{{"model":"gpt-5.6-sol","input":"hi","note":"{SENTINEL}"}}"#
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let recorded = mock.last_request().expect("the mock received a request");
    assert!(
        String::from_utf8_lossy(&recorded.body).contains(SENTINEL),
        "the mock actually received the sentinel body — forwarding genuinely worked"
    );

    let rows = rows_eventually(&state.store).await;
    assert_eq!(
        rows.len(),
        1,
        "exactly one content-free request_log row: {rows:?}"
    );
    let row = &rows[0];
    let row_debug = format!("{row:?}");
    assert!(
        !row_debug.contains(SENTINEL),
        "the persisted request_log row must NEVER contain the compact body, got: {row_debug}"
    );
    assert!(
        !row_debug.contains(SENTINEL_EMAIL),
        "the seeded sentinel email must never appear in the row, got: {row_debug}"
    );
    assert!(
        !row_debug.contains(SENTINEL_TOKEN),
        "the seeded sentinel token must never appear in the row, got: {row_debug}"
    );
    assert_eq!(row.status, 200);
    assert_eq!(row.path, "responses_compact");
    assert_eq!(row.account_id.as_deref(), Some("acct-a"));
    assert_eq!(
        row.model.as_deref(),
        Some("gpt-5.6-sol"),
        "the content-free model IS logged (mirrors /responses)"
    );
}

// -------------------------------------------------------------------------------------------
// 3. Owner affinity for a headerless compact request must still use the body's prompt_cache_key
//    as its soft fallback. Current Codex session-id + thread-id headers are the canonical durable
//    identity and intentionally do not change when prompt_cache_key changes.
// -------------------------------------------------------------------------------------------

#[tokio::test]
async fn compact_owner_affinity_is_derived_from_the_body_prompt_cache_key() {
    const PROMPT_CACHE_KEY: &str = "thread-compact-key-xyz";

    let mock = MockControlUpstream::new(200, r#"{"output":[]}"#);
    let mock_base = mock.clone().spawn().await;

    let (base, state) = spawn_app(true, &mock_base).await;
    seed_account(
        &state.store,
        &state.cipher,
        "acct-a",
        "a@example.test",
        "tok-a",
    )
    .await;
    seed_account(
        &state.store,
        &state.cipher,
        "acct-b",
        "b@example.test",
        "tok-b",
    )
    .await;
    insert_key_for_raw(
        &state.store,
        "sk-pf-compact-affinity-test",
        "compact-affinity",
    )
    .await;

    let headers_for_key = axum::http::HeaderMap::new();
    let compact_body = format!(
        r#"{{"model":"gpt-5.6-sol","input":"hi","prompt_cache_key":"{PROMPT_CACHE_KEY}"}}"#
    );
    let sk = parse_inbound_scoped(&headers_for_key, compact_body.as_bytes(), None)
        .expect("valid compact body")
        .ctx
        .session_key
        .expect("prompt_cache_key yields a soft affinity key");
    assert!(
        header_session_key(&headers_for_key, None).is_none(),
        "a header-only control derivation must have no affinity for this request"
    );

    let t = now();
    state
        .store
        .continuity()
        .ensure_session(&sk.value, "soft", t)
        .await
        .unwrap();
    state
        .store
        .continuity()
        .record_completion(
            &sk.value,
            "soft",
            "acct-b",
            "resp_compact_owned",
            "fp",
            1,
            t,
        )
        .await
        .unwrap();

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/responses/compact"))
        .header("authorization", "Bearer sk-pf-compact-affinity-test")
        .body(compact_body)
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
        "compact must land on the soft session owner derived from the body's prompt_cache_key"
    );

    let rows = rows_eventually(&state.store).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].account_id.as_deref(), Some("acct-b"));
}

// -------------------------------------------------------------------------------------------
// 4. D18 gate: compact inherits the proxy sub-router's client-key enforcement — keyless ⇒ 401,
//    never reaching the upstream; a valid key ⇒ 200.
// -------------------------------------------------------------------------------------------

#[tokio::test]
async fn keyless_compact_is_401_when_enforced_valid_key_is_200() {
    let mock = MockControlUpstream::new(200, r#"{"output":[]}"#);
    let mock_base = mock.clone().spawn().await;

    let (base, state) = spawn_app(true, &mock_base).await;
    seed_account(
        &state.store,
        &state.cipher,
        "acct-a",
        "u@example.test",
        "tok-a",
    )
    .await;
    insert_key_for_raw(&state.store, "sk-pf-compact-gate-test", "compact-gate").await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/responses/compact"))
        .body(r#"{"model":"gpt-5.6-sol","input":"hi"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        401,
        "no Authorization header ⇒ 401, inheriting the D18 gate from the proxy sub-router"
    );
    assert_eq!(
        mock.request_count(),
        0,
        "an unauthenticated compact request must never reach the upstream"
    );

    let resp = client
        .post(format!("{base}/responses/compact"))
        .header("authorization", "Bearer sk-pf-compact-gate-test")
        .body(r#"{"model":"gpt-5.6-sol","input":"hi"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "a valid key is forwarded");
}

// -------------------------------------------------------------------------------------------
// 5. Pool scoping: `/{pool}/responses/compact` selects only that pool's accounts.
// -------------------------------------------------------------------------------------------

#[tokio::test]
async fn pooled_compact_scopes_to_the_requested_pool() {
    let mock = MockControlUpstream::new(200, r#"{"output":[]}"#);
    let mock_base = mock.clone().spawn().await;

    let (base, state) = spawn_app(true, &mock_base).await;
    // "z-pooled" sorts AFTER the unpooled decoy — so a dropped `pool` scope would let RoundRobin's
    // ascending tiebreak pick the decoy instead (mirroring T1's
    // `owner_affine_core_fallback_is_scoped_to_the_given_pool` teeth idiom).
    let mut pooled = account("z-pooled", "z@example.test");
    pooled.pool = Some("p".to_string());
    state
        .store
        .accounts()
        .insert(
            &pooled,
            &PlainTokens {
                access_token: "tok-pooled".into(),
                refresh_token: "r".into(),
                id_token: "i".into(),
            },
            &state.cipher,
        )
        .await
        .unwrap();
    seed_account(
        &state.store,
        &state.cipher,
        "a-unpooled",
        "a@example.test",
        "tok-unpooled",
    )
    .await;
    insert_key_for_raw(&state.store, "sk-pf-compact-pool-test", "compact-pool").await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/p/responses/compact"))
        .header("authorization", "Bearer sk-pf-compact-pool-test")
        .body(r#"{"model":"gpt-5.6-sol","input":"hi"}"#)
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
        Some("Bearer tok-pooled"),
        "pool-scoped compact must never pick the unpooled decoy"
    );
}
