//! B5 Task 3 — Layer 1: serve the soonest `ErrorBackoff` account immediately (guarded, no wait)
//! when the eligible pool is empty. Drives the real ingress (`responses_handler_impl` via HTTP,
//! through `build_app`) with accounts pushed into `ErrorBackoff`/`Cooldown`/`HardBlocked` states via
//! `AppState.runtime` (the same live per-account routing state `failure_routing.rs` seeds), never
//! via a fabricated `AccountSnapshot` — this proves the REAL `standard_pool`/`eligibility`/
//! `soonest_recover`/`backoff_census` pipeline the ingress actually runs.
//!
//! Guard ported from codex-lb `logic.py:499-524`: serve-now only when there is more than one
//! capability-filtered error-backoff account, OR exactly one AND a capability-filtered
//! `HardBlocked` peer exists. See `docs/superpowers/plans/2026-07-18-b5-antistarvation.md` Task 3.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{stream, StreamExt};
use polyflare_codex::oauth::OAuthClient;
use polyflare_core::{
    Account, AccountId, CapacityWeighted, Continuity, ExecError, Executor, PreparedRequest,
    RequestCtx, ResponseStream,
};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_store::{PlainTokens, Store, TokenCipher};

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn account(id: &str, security_work_authorized: bool, status: &str) -> polyflare_store::Account {
    polyflare_store::Account {
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
        status: status.to_string(),
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

/// A stub `Executor` that always succeeds and records every account id it was called with, in
/// order — the exact assertion surface these tests need ("which account served the request").
#[derive(Default)]
struct RecordingExecutor {
    calls: std::sync::Mutex<Vec<String>>,
}

impl RecordingExecutor {
    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl Executor for RecordingExecutor {
    async fn execute(
        &self,
        _req: PreparedRequest,
        account: &Account,
        _ctx: &RequestCtx,
    ) -> Result<ResponseStream, ExecError> {
        self.calls
            .lock()
            .unwrap()
            .push(account.id.as_str().to_string());
        let created = r#"{"type":"response.created","response":{"id":"resp_1"}}"#;
        let completed = r#"{"type":"response.completed","response":{"id":"resp_1"}}"#;
        Ok(Box::pin(stream::iter(vec![
            Ok::<Bytes, ExecError>(Bytes::from(format!("data: {created}\n\n"))),
            Ok(Bytes::from(format!("data: {completed}\n\n"))),
        ])))
    }
}

async fn spawn_store() -> (Store, TokenCipher, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[99u8; 32]).unwrap();
    (store, cipher, dir)
}

fn build_state(
    store: Store,
    cipher: TokenCipher,
    executor: Arc<RecordingExecutor>,
) -> Arc<AppState> {
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_secs(30),
    ));
    Arc::new(AppState {
        enforce_client_keys: false,
        codex_executor: executor,
        anthropic_executor: Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap()),
        selector: Arc::new(CapacityWeighted),
        pool_selectors: Default::default(),
        continuity,
        store,
        cipher,
        oauth: OAuthClient::new("http://127.0.0.1:9".to_string()).unwrap(),
        upstream_base_url: "http://unused.invalid".to_string(),
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
        runtime: Default::default(),
    })
}

async fn spawn_app(state: Arc<AppState>) -> String {
    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

async fn drain(resp: reqwest::Response) -> String {
    let mut body = String::new();
    let mut s = resp.bytes_stream();
    while let Some(chunk) = s.next().await {
        match chunk {
            Ok(bytes) => body.push_str(&String::from_utf8_lossy(&bytes)),
            Err(_) => break,
        }
    }
    body
}

/// Push an account into `ErrorBackoff` (error_count >= 3, `last_error_at` recent enough that the
/// backoff window — 30s at error_count=3 — has not yet expired by the time the test's request
/// actually runs).
fn seed_error_backoff(state: &AppState, id: &str, last_error_at: i64) {
    let aid = AccountId::from(id);
    for _ in 0..3 {
        state.runtime.record_transient_error(&aid, last_error_at);
    }
}

/// (1) Empty eligible pool + 2 error-backoff accounts ⇒ served on the SOONEST one, no wait.
#[tokio::test]
async fn two_error_backoff_accounts_serves_the_soonest_one() {
    let (store, cipher, _dir) = spawn_store().await;
    store
        .accounts()
        .insert(&account("A", false, "active"), &tokens("tokA"), &cipher)
        .await
        .unwrap();
    store
        .accounts()
        .insert(&account("B", false, "active"), &tokens("tokB"), &cipher)
        .await
        .unwrap();
    let exec = Arc::new(RecordingExecutor::default());
    let state = build_state(store, cipher, exec.clone());

    let t0 = now();
    // A recovers strictly SOONER than B (both error_count=3, so backoff=30s; A's last_error_at is
    // earlier ⇒ A's recover_at = t0+30 < B's t0+40).
    seed_error_backoff(&state, "A", t0);
    seed_error_backoff(&state, "B", t0 + 10);

    let pf = spawn_app(state).await;
    let resp = reqwest::Client::new()
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "m", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "Layer 1 must serve, not 503");
    let body = drain(resp).await;
    assert!(
        body.contains("response.completed"),
        "clean stream relayed: {body}"
    );
    assert_eq!(
        exec.calls(),
        vec!["A".to_string()],
        "the soonest-to-recover account (A) is the one actually served"
    );
}

/// (2) Empty pool + 1 error-backoff + 1 HardBlocked (paused) peer ⇒ the guard IS satisfied
/// (count==1 && has_hardblocked) — serves the backoff account.
#[tokio::test]
async fn one_error_backoff_plus_hardblocked_peer_serves_the_backoff_account() {
    let (store, cipher, _dir) = spawn_store().await;
    store
        .accounts()
        .insert(
            &account("backoff-acct", false, "active"),
            &tokens("tok1"),
            &cipher,
        )
        .await
        .unwrap();
    store
        .accounts()
        .insert(
            &account("blocked-acct", false, "paused"),
            &tokens("tok2"),
            &cipher,
        )
        .await
        .unwrap();
    let exec = Arc::new(RecordingExecutor::default());
    let state = build_state(store, cipher, exec.clone());
    seed_error_backoff(&state, "backoff-acct", now());

    let pf = spawn_app(state).await;
    let resp = reqwest::Client::new()
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "m", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "the guard is satisfied (1 + hardblocked peer) ⇒ serve-now"
    );
    assert_eq!(
        exec.calls(),
        vec!["backoff-acct".to_string()],
        "the hardblocked account must NEVER be attempted — only the backoff account"
    );
}

/// (3) Empty pool + 1 LONE error-backoff account (no HardBlocked peer) ⇒ the guard is NOT
/// satisfied ⇒ Layer 1 does not serve-now ⇒ falls through to today's 503.
#[tokio::test]
async fn lone_error_backoff_with_no_hardblocked_peer_does_not_serve_now() {
    let (store, cipher, _dir) = spawn_store().await;
    store
        .accounts()
        .insert(
            &account("backoff-acct", false, "active"),
            &tokens("tok1"),
            &cipher,
        )
        .await
        .unwrap();
    let exec = Arc::new(RecordingExecutor::default());
    let state = build_state(store, cipher, exec.clone());
    seed_error_backoff(&state, "backoff-acct", now());

    let pf = spawn_app(state).await;
    let resp = reqwest::Client::new()
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "m", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        503,
        "a lone error-backoff account with no hardblocked peer must NOT be served-now"
    );
    assert!(
        exec.calls().is_empty(),
        "the lone backoff account must never be attempted when the guard fails"
    );
}

/// (4) Empty pool + only Cooldown accounts (rate_limited, `reset_at` in the future) ⇒ Layer 1 does
/// NOT serve-now (unchanged; this is still the assertion this test locks in). Layer 1 in isolation
/// would have 503'd here (this was this test's assertion before B5 Task 4 landed); NOW that Task 4
/// (`try_layer2_recovery_wait`) is wired at the SAME empty-pool site, a Cooldown-kind
/// `soonest_recover` result correctly falls through to Layer 2 instead — which commits its own 200
/// SSE and starts waiting (bounded by the production default 60s budget; both accounts here
/// recover far past that, at +3600s/+7200s, so the wait itself is not exercised or awaited here —
/// see `tests/starvation_layer2.rs` for the dedicated Layer 2 suite, including its own
/// budget-exceeded-in-band-error test using a short test-scale budget). This test's OWN scope is
/// narrower than that: it only proves Layer 1 itself never serves a Cooldown-kind candidate — the
/// executor is never attempted, which is what `exec.calls()` asserts below.
#[tokio::test]
async fn cooldown_only_accounts_do_not_serve_now_via_layer1() {
    let (store, cipher, _dir) = spawn_store().await;
    let mut a = account("A", false, "rate_limited");
    a.reset_at = Some(now() + 3600); // far in the future — still Cooldown at request time
    store
        .accounts()
        .insert(&a, &tokens("tokA"), &cipher)
        .await
        .unwrap();
    let mut b = account("B", false, "rate_limited");
    b.reset_at = Some(now() + 7200);
    store
        .accounts()
        .insert(&b, &tokens("tokB"), &cipher)
        .await
        .unwrap();
    let exec = Arc::new(RecordingExecutor::default());
    let state = build_state(store, cipher, exec.clone());

    let pf = spawn_app(state).await;
    let resp = reqwest::Client::new()
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "m", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "B5 Task 4: a Cooldown-kind candidate now falls through to Layer 2's keepalive wait \
         (which commits its own 200 immediately) instead of Layer 1's 503 — see this test's doc"
    );
    // Deliberately does NOT drain the response body here: the production 60s wait budget would
    // make this test hang for up to a real minute. `tests/starvation_layer2.rs` owns exercising
    // the wait/budget/splice behavior itself, with short, test-scale timing overrides.
    assert!(
        exec.calls().is_empty(),
        "no cooldown account may ever be served-now by Layer 1 (Layer 2's own wait hasn't even \
         started re-selecting yet at the point this test observes the response)"
    );
}

/// (5) SECURITY FLOOR: a cyber request with an empty eligible pool and only a NON-authorized
/// error-backoff account present ⇒ Layer 1 must NEVER serve it — `soonest_recover`/
/// `backoff_census`'s capability pre-filter excludes it before it can ever contribute.
#[tokio::test]
async fn cyber_request_never_serves_a_non_authorized_backoff_account() {
    let (store, cipher, _dir) = spawn_store().await;
    store
        .accounts()
        .insert(
            &account("non-authorized-backoff", false, "active"),
            &tokens("tok1"),
            &cipher,
        )
        .await
        .unwrap();
    let exec = Arc::new(RecordingExecutor::default());
    let state = build_state(store, cipher, exec.clone());
    seed_error_backoff(&state, "non-authorized-backoff", now());

    let pf = spawn_app(state).await;
    let resp = reqwest::Client::new()
        .post(format!("{pf}/responses"))
        .header("x-polyflare-capability", "security_work")
        .json(&serde_json::json!({"model": "m", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_client_error() || resp.status().as_u16() == 503,
        "expected a clean refusal, got {}",
        resp.status()
    );
    assert!(
        exec.calls().is_empty(),
        "a cyber request must NEVER serve a non-authorized error-backoff account, even under Layer 1"
    );
}

/// (6) Regression: a pool with a normal ELIGIBLE account ⇒ the ordinary Route path serves it
/// directly — Layer 1 is never reached (its call site, `RouteDecision::NoEligibleAccount`, is
/// never entered because `selector.pick` already found a candidate).
#[tokio::test]
async fn eligible_pool_uses_the_normal_path_layer1_never_triggers() {
    let (store, cipher, _dir) = spawn_store().await;
    store
        .accounts()
        .insert(
            &account("healthy", false, "active"),
            &tokens("tok1"),
            &cipher,
        )
        .await
        .unwrap();
    let exec = Arc::new(RecordingExecutor::default());
    let state = build_state(store, cipher, exec.clone());

    let pf = spawn_app(state).await;
    let resp = reqwest::Client::new()
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "m", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = drain(resp).await;
    assert!(body.contains("response.completed"));
    assert_eq!(exec.calls(), vec!["healthy".to_string()]);
}
