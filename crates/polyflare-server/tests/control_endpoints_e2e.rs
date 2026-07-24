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
use polyflare_server::runtime_settings::{RuntimeSettings, RuntimeSettingsFields};
use polyflare_server::session_key::header_session_key;
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};
use polyflare_testkit::{MockControlUpstream, MockOAuth};

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
    spawn_app_with_oauth(
        enforce_client_keys,
        mock_base,
        "http://127.0.0.1:9".to_string(),
    )
    .await
}

async fn spawn_app_with_oauth(
    enforce_client_keys: bool,
    mock_base: &str,
    oauth_url: String,
) -> (String, Arc<AppState>) {
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
        oauth: OAuthClient::new(oauth_url).unwrap(),
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
    assert_eq!(
        body, r#"{"ok":true}"#,
        "the mock's body is relayed verbatim"
    );

    let recorded = mock.last_request().expect("the mock received a request");
    assert_eq!(recorded.path, "/codex/memories/trace_summarize");
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
        "the persisted request_log row must NEVER contain the control body, got: {row_debug}"
    );
    assert_eq!(row.status, 200);
    assert_eq!(row.path, "codex_control_memories/trace_summarize");
    assert_eq!(row.account_id.as_deref(), Some("acct-a"));
}

// -------------------------------------------------------------------------------------------
// C11b Task 2: `control_route` bumps the content-free `upstream_request_metrics` counter,
// keyed by provider target and status, exactly once per real control request — the CONTROL traffic
// class of the 3 request-completion wrapper sites.
// -------------------------------------------------------------------------------------------

#[tokio::test]
async fn control_request_records_upstream_request_metric() {
    let mock = MockControlUpstream::new(200, r#"{"goal":"be nice"}"#);
    let mock_base = mock.clone().spawn().await;

    let (base, state) = spawn_app(true, &mock_base).await;
    seed_account(&state.store, &state.cipher, "acct-a", "tok-a").await;
    insert_key_for_raw(&state.store, "sk-pf-metrics-test", "metrics").await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/thread/goal/set"))
        .header("authorization", "Bearer sk-pf-metrics-test")
        .body(r#"{"goal":"be nice"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    assert_eq!(
        state.upstream_request_metrics.snapshot(),
        vec![(
            "codex".to_string(),
            "account".to_string(),
            "acct-a".to_string(),
            200,
            1,
        )],
        "control_route must record exactly one upstream_requests entry for the served account"
    );
}

#[tokio::test]
async fn successful_unary_control_request_holds_one_lease_and_clears_prior_health_errors() {
    let mock = MockControlUpstream::new(200, r#"{"goal":"ok"}"#);
    let mock_base = mock.clone().spawn().await;

    let (base, state) = spawn_app(false, &mock_base).await;
    seed_account(&state.store, &state.cipher, "acct-a", "tok-a").await;
    let id = polyflare_core::AccountId::from("acct-a");
    state.runtime.record_transient_error(&id, now());

    let response = reqwest::Client::new()
        .get(format!("{base}/thread/goal/get"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    assert_eq!(state.lease_metrics.acquired(), 1);
    assert_eq!(
        state.lease_metrics.released(),
        1,
        "the unary lease must be released after the full response has been buffered"
    );
    let mut snapshots = vec![polyflare_core::AccountSnapshot::new("acct-a")];
    state.runtime.overlay(&mut snapshots, now());
    assert_eq!(snapshots[0].in_flight, 0, "the unary lease must not leak");
    assert_eq!(
        snapshots[0].error_count, 0,
        "a successful provider outcome clears stale health errors"
    );
}

#[tokio::test]
async fn unary_429_honors_retry_after_and_requests_immediate_usage_refresh() {
    let mock = MockControlUpstream::new(
        429,
        r#"{"error":{"code":"rate_limit_exceeded","message":"ignored"}}"#,
    )
    .with_header("retry-after", "75");
    let mock_base = mock.clone().spawn().await;

    let (base, state) = spawn_app(false, &mock_base).await;
    seed_account(&state.store, &state.cipher, "acct-a", "tok-a").await;
    let (refresh_tx, mut refresh_rx) = tokio::sync::mpsc::unbounded_channel();
    state.runtime.register_usage_refresh(refresh_tx);

    let response = reqwest::Client::new()
        .post(format!("{base}/memories/trace_summarize"))
        .body(r#"{"trace":"x"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 429);
    assert_eq!(
        response
            .headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok()),
        Some("75")
    );

    let id = polyflare_core::AccountId::from("acct-a");
    let mut snapshots = vec![polyflare_core::AccountSnapshot::new("acct-a")];
    state.runtime.overlay(&mut snapshots, now());
    assert_eq!(snapshots[0].error_count, 1);
    let last_error_at = snapshots[0].last_error_at.expect("429 error timestamp");
    assert_eq!(
        snapshots[0].cooldown_until,
        Some(last_error_at + 75),
        "the unary path must preserve Retry-After in the shared cooldown policy"
    );
    assert_eq!(
        refresh_rx.try_recv().unwrap(),
        id,
        "capacity failures request an authoritative usage refresh"
    );
    assert_eq!(state.lease_metrics.acquired(), 1);
    assert_eq!(state.lease_metrics.released(), 1);
}

#[tokio::test]
async fn unary_5xx_penalizes_health_but_ordinary_request_4xx_is_neutral() {
    let failing = MockControlUpstream::new(503, r#"{"error":{"message":"unavailable"}}"#);
    let failing_base = failing.clone().spawn().await;
    let (base, state) = spawn_app(false, &failing_base).await;
    seed_account(&state.store, &state.cipher, "acct-a", "tok-a").await;

    let response = reqwest::Client::new()
        .get(format!("{base}/agent-identities/jwks"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 503);

    let id = polyflare_core::AccountId::from("acct-a");
    let mut snapshots = vec![polyflare_core::AccountSnapshot::new("acct-a")];
    state.runtime.overlay(&mut snapshots, now());
    assert_eq!(
        snapshots[0].error_count, 1,
        "a unary provider 5xx is a transient account-health failure"
    );

    let request_error = MockControlUpstream::new(400, r#"{"error":{"message":"bad request"}}"#);
    let request_error_base = request_error.clone().spawn().await;
    let (base, state) = spawn_app(false, &request_error_base).await;
    seed_account(&state.store, &state.cipher, "acct-a", "tok-a").await;
    state.runtime.record_transient_error(&id, now());

    let response = reqwest::Client::new()
        .get(format!("{base}/agent-identities/jwks"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);

    let mut snapshots = vec![polyflare_core::AccountSnapshot::new("acct-a")];
    state.runtime.overlay(&mut snapshots, now());
    assert_eq!(
        snapshots[0].error_count, 1,
        "an ordinary request-level 4xx must neither penalize nor clear account health"
    );
    assert_eq!(state.lease_metrics.acquired(), 1);
    assert_eq!(state.lease_metrics.released(), 1);
}

#[tokio::test]
async fn unary_quota_code_is_capacity_not_health_and_transport_loss_is_transient() {
    let quota = MockControlUpstream::new(
        400,
        r#"{"error":{"code":"insufficient_quota","message":"ignored"}}"#,
    );
    let quota_base = quota.clone().spawn().await;
    let (base, state) = spawn_app(false, &quota_base).await;
    seed_account(&state.store, &state.cipher, "acct-a", "tok-a").await;
    let (refresh_tx, mut refresh_rx) = tokio::sync::mpsc::unbounded_channel();
    state.runtime.register_usage_refresh(refresh_tx);

    let response = reqwest::Client::new()
        .post(format!("{base}/alpha/search"))
        .body(r#"{"query":"x"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);

    let id = polyflare_core::AccountId::from("acct-a");
    let mut snapshots = vec![polyflare_core::AccountSnapshot::new("acct-a")];
    state.runtime.overlay(&mut snapshots, now());
    assert_eq!(
        snapshots[0].error_count, 0,
        "quota exhaustion is capacity pressure, not an account-health error"
    );
    let remaining = snapshots[0]
        .cooldown_until
        .expect("quota cooldown")
        .saturating_sub(now());
    assert!(
        (119..=polyflare_server::runtime_state::QUOTA_EXCEEDED_COOLDOWN_SECS).contains(&remaining)
    );
    assert_eq!(refresh_rx.try_recv().unwrap(), id);

    let (base, state) = spawn_app(false, "http://127.0.0.1:9").await;
    seed_account(&state.store, &state.cipher, "acct-a", "tok-a").await;
    let response = reqwest::Client::new()
        .get(format!("{base}/agent-identities/jwks"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 502);

    let mut snapshots = vec![polyflare_core::AccountSnapshot::new("acct-a")];
    state.runtime.overlay(&mut snapshots, now());
    assert_eq!(
        snapshots[0].error_count, 1,
        "a unary transport loss is a transient account-health failure"
    );
    assert_eq!(state.lease_metrics.acquired(), 1);
    assert_eq!(state.lease_metrics.released(), 1);
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

#[tokio::test]
async fn pooled_memory_summary_isolated_from_same_session_owner_in_another_pool() {
    let mock = MockControlUpstream::new(200, r#"{"summary":"ok"}"#);
    let mock_base = mock.clone().spawn().await;

    let (base, state) = spawn_app(true, &mock_base).await;
    seed_account(&state.store, &state.cipher, "acct-global", "tok-global").await;
    seed_account(&state.store, &state.cipher, "acct-memory", "tok-memory").await;
    state
        .store
        .accounts()
        .update_pool("acct-memory", Some("memory"))
        .await
        .unwrap();
    insert_key_for_raw(&state.store, "sk-pf-pooled-memory", "pooled-memory").await;

    // The unscoped session belongs to acct-global. Pool scoping is part of the session identity,
    // so the same inbound turn-state on /memory/... must not inherit this out-of-pool owner.
    let headers_for_key = {
        let mut h = axum::http::HeaderMap::new();
        h.insert("x-codex-turn-state", "shared-turn-state".parse().unwrap());
        h
    };
    let unscoped_key = header_session_key(&headers_for_key, None).unwrap();
    let t = now();
    state
        .store
        .continuity()
        .ensure_session(&unscoped_key.value, "hard", t)
        .await
        .unwrap();
    state
        .store
        .continuity()
        .record_completion(
            &unscoped_key.value,
            "hard",
            "acct-global",
            "resp_global",
            "fp",
            1,
            t,
        )
        .await
        .unwrap();

    let response = reqwest::Client::new()
        .post(format!("{base}/memory/memories/trace_summarize"))
        .header("authorization", "Bearer sk-pf-pooled-memory")
        .header("x-codex-turn-state", "shared-turn-state")
        .body(r#"{"trace":"pool scoped"}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let recorded = mock.last_request().expect("the mock received a request");
    assert_eq!(recorded.path, "/codex/memories/trace_summarize");
    assert_eq!(
        recorded
            .headers
            .get("authorization")
            .and_then(|value| value.to_str().ok()),
        Some("Bearer tok-memory"),
        "pooled memories must select only an account in the requested pool"
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
    assert_eq!(
        mock.request_count(),
        0,
        "an unauthenticated request must never reach the upstream"
    );

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
    assert_eq!(
        mock.last_request().unwrap().path,
        "/codex/agent-identities/jwks"
    );

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
    assert_eq!(
        mock.last_request().unwrap().path,
        "/codex/thread/goal/clear"
    );

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

#[tokio::test]
async fn image_and_opt_in_search_endpoints_forward_exact_paths_bodies_and_pool_scope() {
    let mock = MockControlUpstream::new(200, r#"{"created":1,"data":[{"b64_json":"aW1hZ2U="}]}"#)
        .with_header("content-type", "application/json")
        .with_header("x-request-id", "upstream-aux-1");
    let mock_base = mock.clone().spawn().await;

    let (base, state) = spawn_app(true, &mock_base).await;
    seed_account(&state.store, &state.cipher, "acct-a", "tok-a").await;
    state
        .store
        .accounts()
        .update_pool("acct-a", Some("creative"))
        .await
        .unwrap();
    insert_key_for_raw(&state.store, "sk-pf-aux-test", "aux").await;

    let client = reqwest::Client::new();
    let cases = [
        (
            "/images/generations",
            "/codex/images/generations",
            r#"{"model":"gpt-image-1.5","prompt":"fox"}"#,
        ),
        (
            "/images/edits",
            "/codex/images/edits",
            r#"{"model":"gpt-image-1.5","prompt":"hat","images":[{"image_url":"data:image/png;base64,Zm9v"}]}"#,
        ),
        (
            "/alpha/search",
            "/codex/alpha/search",
            r#"{"id":"search-session","model":"gpt-5.6-sol","input":[]}"#,
        ),
        (
            "/creative/images/generations",
            "/codex/images/generations",
            r#"{"model":"gpt-image-1.5","prompt":"pool fox"}"#,
        ),
        (
            "/creative/alpha/search",
            "/codex/alpha/search",
            r#"{"id":"search-session","model":"gpt-5.6-sol","input":[]}"#,
        ),
    ];

    for (client_path, upstream_path, request_body) in cases {
        let response = client
            .post(format!("{base}{client_path}"))
            .header("authorization", "Bearer sk-pf-aux-test")
            .header("content-type", "application/json")
            .header("x-openai-originator", "codex_cli_rs")
            .body(request_body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200, "{client_path}");
        assert_eq!(
            response
                .headers()
                .get("x-request-id")
                .and_then(|value| value.to_str().ok()),
            Some("upstream-aux-1")
        );

        let recorded = mock
            .last_request()
            .expect("auxiliary request reached upstream");
        assert_eq!(recorded.path, upstream_path, "{client_path}");
        assert_eq!(recorded.method, "POST");
        assert_eq!(recorded.body.as_ref(), request_body.as_bytes());
        assert_eq!(
            recorded
                .headers
                .get("authorization")
                .and_then(|value| value.to_str().ok()),
            Some("Bearer tok-a"),
            "the selected account bearer replaces the PolyFlare client key"
        );
        assert_eq!(
            recorded
                .headers
                .get("content-type")
                .and_then(|value| value.to_str().ok()),
            Some("application/json")
        );
    }
}

#[tokio::test]
async fn image_401_forces_one_refresh_and_retries_on_the_same_account() {
    use axum::body::Bytes;
    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode};
    use axum::response::{IntoResponse, Response};
    use axum::routing::any;
    use std::sync::Mutex;

    async fn upstream(
        State(seen): State<Arc<Mutex<Vec<String>>>>,
        headers: HeaderMap,
        _body: Bytes,
    ) -> Response {
        let authorization = headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();
        seen.lock().unwrap().push(authorization.clone());
        if authorization == "Bearer refreshed-image-token" {
            (
                StatusCode::OK,
                [("content-type", "application/json")],
                r#"{"created":1,"data":[{"b64_json":"aW1hZ2U="}]}"#,
            )
                .into_response()
        } else {
            (
                StatusCode::UNAUTHORIZED,
                [("content-type", "application/json")],
                r#"{"error":{"code":"invalid_token"}}"#,
            )
                .into_response()
        }
    }

    let seen = Arc::new(Mutex::new(Vec::new()));
    let upstream_app = axum::Router::new()
        .route("/codex/{*path}", any(upstream))
        .with_state(seen.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, upstream_app).await.unwrap();
    });

    let oauth = MockOAuth::ok(
        "refreshed-image-token",
        "refreshed-image-refresh",
        "refreshed-image-id",
    );
    let oauth_handle = oauth.clone();
    let oauth_url = oauth.spawn().await;
    let (base, state) =
        spawn_app_with_oauth(true, &format!("http://{upstream_addr}"), oauth_url).await;
    seed_account(
        &state.store,
        &state.cipher,
        "acct-a",
        "rejected-image-token",
    )
    .await;
    insert_key_for_raw(&state.store, "sk-pf-image-auth-test", "image-auth").await;

    let response = reqwest::Client::new()
        .post(format!("{base}/images/generations"))
        .header("authorization", "Bearer sk-pf-image-auth-test")
        .json(&serde_json::json!({
            "model": "gpt-image-1.5",
            "prompt": "fox"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(oauth_handle.hit_count(), 1);
    assert_eq!(
        seen.lock().unwrap().as_slice(),
        [
            "Bearer rejected-image-token".to_string(),
            "Bearer refreshed-image-token".to_string()
        ],
        "the retry stays on the selected account and uses its refreshed bearer"
    );
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
