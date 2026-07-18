//! B4/B5 Task 5 (final): `POLYFLARE_MAX_ACCOUNT_ATTEMPTS` threaded through `AppState` at startup
//! (never a per-request `env::var`) + the content-free failover observability signal, exercised
//! through the REAL ingress stack (`build_app`, a real HTTP round trip — not the
//! `responses_handler_impl_for_test` seam Task 4's suite uses).
//!
//! Two things are proven here that `tests/failover_loop.rs` (Task 4) does not:
//! 1. A multi-account failover through the production `/responses` entrypoint emits a
//!    content-free failover signal (a `FailoverMetrics` counter bump + a `LogEvent` on the
//!    `log_bus`, mirroring the `tracing` event `crate::observability::FailoverSignal::emit`
//!    writes) — carrying ONLY the reason code, the two account ids, and the attempt number, NEVER
//!    a body/message/frame.
//! 2. `POLYFLARE_MAX_ACCOUNT_ATTEMPTS=1`, resolved via the REAL `config::max_account_attempts_from_env`
//!    (not a hardcoded test-seam parameter) and threaded into `AppState.max_account_attempts`,
//!    reproduces the one-shot regression through the production entrypoint.
//!
//! Scaffolding (deterministic selector + scripted stub executor) mirrors `tests/failover_loop.rs`
//! exactly — see that file's module doc for the rationale (a real `CapacityWeighted` selector is
//! seeded-random for tied weights, so a small deterministic `FirstEligible` selector is used
//! instead to make the attempt ORDER fully predictable).

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{stream, StreamExt};
use polyflare_codex::oauth::OAuthClient;
use polyflare_core::{
    Account, AccountId, AccountSnapshot, Continuity, ExecError, Executor, FailureSignal,
    PreparedRequest, RequestCtx, ResponseStream, SelectionCtx, Selector,
};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::config;
use polyflare_server::continuity::CodexContinuity;
use polyflare_store::{PlainTokens, Store, TokenCipher};

fn account(id: &str) -> polyflare_store::Account {
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
        status: "active".to_string(),
        deactivation_reason: None,
        reset_at: None,
        blocked_at: None,
        security_work_authorized: false,
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

/// Deterministic test selector: the first candidate in whatever order `candidates` arrives —
/// mirrors `tests/failover_loop.rs`'s `FirstEligible` (no security-work filtering needed here).
struct FirstEligible;
impl Selector for FirstEligible {
    fn pick(&self, candidates: &[AccountSnapshot], _ctx: &SelectionCtx) -> Option<AccountId> {
        candidates.first().map(|s| s.id.clone())
    }
    fn name(&self) -> &'static str {
        "first_eligible"
    }
}

/// One account's scripted response to a single `execute()` call.
#[derive(Clone)]
enum AttemptBehavior {
    Success,
    Fail(u16),
}

/// A test-only `Executor` keyed by `Account.id`, mirroring `tests/failover_loop.rs`'s
/// `FailoverStubExecutor`: a FIFO queue of behaviors per account, recording every attempted
/// account id in order.
#[derive(Default)]
struct StubExecutor {
    behaviors: Mutex<HashMap<String, VecDeque<AttemptBehavior>>>,
    calls: Mutex<Vec<String>>,
}

impl StubExecutor {
    fn new() -> Self {
        Self::default()
    }

    fn script(&self, id: &str, behaviors: Vec<AttemptBehavior>) {
        self.behaviors
            .lock()
            .unwrap()
            .insert(id.to_string(), behaviors.into_iter().collect());
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl Executor for StubExecutor {
    async fn execute(
        &self,
        _req: PreparedRequest,
        account: &Account,
        _ctx: &RequestCtx,
    ) -> Result<ResponseStream, ExecError> {
        self.calls.lock().unwrap().push(account.id.as_str().to_string());
        let behavior = {
            let mut map = self.behaviors.lock().unwrap();
            match map.get_mut(account.id.as_str()) {
                Some(q) if q.len() > 1 => q.pop_front().unwrap(),
                Some(q) => q.front().cloned().unwrap_or(AttemptBehavior::Success),
                None => AttemptBehavior::Success,
            }
        };
        match behavior {
            AttemptBehavior::Success => {
                let id = format!("resp_{}", account.id);
                let created = format!(r#"{{"type":"response.created","response":{{"id":"{id}"}}}}"#);
                let completed =
                    format!(r#"{{"type":"response.completed","response":{{"id":"{id}"}}}}"#);
                Ok(Box::pin(stream::iter(vec![
                    Ok::<Bytes, ExecError>(Bytes::from(format!("data: {created}\n\n"))),
                    Ok(Bytes::from(format!("data: {completed}\n\n"))),
                ])))
            }
            AttemptBehavior::Fail(status) => Err(ExecError::UpstreamStatus(FailureSignal {
                status,
                retry_after: None,
                error_code: None,
            })),
        }
    }
}

async fn spawn_store() -> (Store, TokenCipher, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[99u8; 32]).unwrap();
    (store, cipher, dir)
}

/// Builds a real `AppState` — same shape as `tests/failover_loop.rs::build_state` — with an
/// explicit, ALREADY-RESOLVED `max_account_attempts` (the Task 5 contract: config is resolved
/// ONCE, before `AppState` construction, never read per-request out of the ingress path).
fn build_state(
    store: Store,
    cipher: TokenCipher,
    executor: Arc<StubExecutor>,
    max_account_attempts: u32,
) -> Arc<AppState> {
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_secs(30),
    ));
    Arc::new(AppState {
        enforce_client_keys: false,
        codex_executor: executor,
        control_client: polyflare_codex::build_client().expect("build control_client"),
        anthropic_executor: Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap()),
        selector: Arc::new(FirstEligible),
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
        runtime: Default::default(),
        max_account_attempts,
        failover_metrics: polyflare_server::observability::FailoverMetrics::new(),
        health_tier_metrics: polyflare_server::observability::HealthTierMetrics::new(),
        starvation_wait_budget: std::time::Duration::from_secs(60),
        starvation_heartbeat: std::time::Duration::from_secs(10),
        wake_jitter_ms: 0,
        starvation_metrics: polyflare_server::observability::StarvationMetrics::new(),
        stream_idle_timeout: std::time::Duration::from_secs(300),
        soft_drain_enabled: true,
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

/// Serializes the one test in this file that mutates `POLYFLARE_MAX_ACCOUNT_ATTEMPTS` — mirrors
/// `crate::config`'s own env-lock pattern for the same var (env vars are process-global).
fn max_account_attempts_env_lock() -> &'static std::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
}

/// (1) THE e2e: a multi-account pool where the first-picked account (A) 429s and the next (B)
/// succeeds ⇒ the client receives B's CLEAN stream, AND a content-free failover signal fired —
/// the `FailoverMetrics` counter bumped by exactly one, and the `log_bus` carries a `"failover"`
/// event with the reason code + both account ids + the attempt number, and NOTHING else (no body,
/// no message, no frame content).
#[tokio::test]
async fn multi_account_failover_through_build_app_emits_content_free_signal() {
    let (store, cipher, _dir) = spawn_store().await;
    store
        .accounts()
        .insert(&account("A"), &tokens("tokA"), &cipher)
        .await
        .unwrap();
    store
        .accounts()
        .insert(&account("B"), &tokens("tokB"), &cipher)
        .await
        .unwrap();
    let exec = Arc::new(StubExecutor::new());
    exec.script("A", vec![AttemptBehavior::Fail(429)]);
    exec.script("B", vec![AttemptBehavior::Success]);
    // Default bound (3), same value `config::max_account_attempts_from_env` resolves when unset —
    // proving the production default path, not an arbitrary test constant.
    let state = build_state(store, cipher, exec.clone(), 3);
    let pf = spawn_app(state.clone()).await;

    assert_eq!(
        state.failover_metrics.total(),
        0,
        "no failover has happened yet"
    );

    let resp = reqwest::Client::new()
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "m", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "the client gets B's clean stream");
    let body = drain(resp).await;
    assert!(
        body.contains("response.completed") && body.contains("resp_B"),
        "B's clean completion relayed: {body}"
    );
    assert_eq!(
        exec.calls(),
        vec!["A".to_string(), "B".to_string()],
        "exactly 2 attempts, A then B"
    );

    // The metric: exactly one failover event recorded (A → B), not one per attempt/request.
    assert_eq!(
        state.failover_metrics.total(),
        1,
        "exactly one FailoverNext transition happened"
    );

    // The log-bus signal: subscribing AFTER the request still sees it via the ring-buffer
    // backfill (no need to have subscribed before the request raced it).
    let (backfill, _rx) = state.log_bus.subscribe();
    let failover_events: Vec<_> = backfill.iter().filter(|e| e.kind == "failover").collect();
    assert_eq!(
        failover_events.len(),
        1,
        "exactly one failover log event, got: {backfill:?}"
    );
    let ev = failover_events[0];
    assert!(
        ev.message.contains("reason=rate_limited"),
        "reason code missing/wrong: {}",
        ev.message
    );
    assert!(
        ev.message.contains("from=A") && ev.message.contains("to=B"),
        "account ids missing/wrong: {}",
        ev.message
    );
    assert!(
        ev.message.contains("attempt=2"),
        "attempt number missing/wrong (B is the 2nd upstream attempt): {}",
        ev.message
    );

    // CONTENT-SAFETY (Critical if violated): the signal must carry NOTHING from the request or
    // response bodies — no input text, no SSE frame shape, no response id, no bearer token.
    let msg_lc = ev.message.to_lowercase();
    for forbidden in [
        "\"hi\"",
        "input",
        "response.completed",
        "response.created",
        "resp_a",
        "resp_b",
        "data:",
        "toka",
        "tokb",
        "bearer",
        "authorization",
    ] {
        assert!(
            !msg_lc.contains(forbidden),
            "forbidden content `{forbidden}` leaked into the failover signal: {}",
            ev.message
        );
    }
}

/// (2) REGRESSION, config-driven (not the test seam): `POLYFLARE_MAX_ACCOUNT_ATTEMPTS=1`,
/// resolved through the REAL `config::max_account_attempts_from_env` and threaded into
/// `AppState.max_account_attempts` exactly as `ServeConfig`/`main.rs` do at startup, reproduces
/// today's one-shot behavior through the production `/responses` entrypoint: A's 429 surfaces
/// immediately as a 502, B (available and would succeed) is NEVER attempted, and no failover
/// signal fires.
#[tokio::test]
async fn env_var_max_attempts_one_is_config_driven_one_shot_regression() {
    let resolved = {
        let _guard = max_account_attempts_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("POLYFLARE_MAX_ACCOUNT_ATTEMPTS", "1");
        }
        // Resolve through the REAL config function — this is what `ServeConfig::from_env` calls
        // at process startup. The env var's job ends here: exactly like production, the resolved
        // value is baked into `AppState` once, and the request path below never touches
        // `std::env::var` again.
        let resolved = config::max_account_attempts_from_env();
        unsafe {
            std::env::remove_var("POLYFLARE_MAX_ACCOUNT_ATTEMPTS");
        }
        resolved
    };
    assert_eq!(resolved, 1, "the env var round-trips through the real config parser");

    let (store, cipher, _dir) = spawn_store().await;
    store
        .accounts()
        .insert(&account("A"), &tokens("tokA"), &cipher)
        .await
        .unwrap();
    store
        .accounts()
        .insert(&account("B"), &tokens("tokB"), &cipher)
        .await
        .unwrap();
    let exec = Arc::new(StubExecutor::new());
    exec.script("A", vec![AttemptBehavior::Fail(429)]);
    exec.script("B", vec![AttemptBehavior::Success]);
    let state = build_state(store, cipher, exec.clone(), resolved);
    let pf = spawn_app(state.clone()).await;

    let resp = reqwest::Client::new()
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "m", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        502,
        "max_account_attempts=1 (config-driven) surfaces the first failure immediately"
    );
    assert_eq!(
        exec.calls(),
        vec!["A".to_string()],
        "B is available and would succeed, yet must NEVER be attempted at max_account_attempts=1"
    );
    assert_eq!(
        state.failover_metrics.total(),
        0,
        "no failover transition happened — the bound stopped the loop before any retry"
    );
    let (backfill, _rx) = state.log_bus.subscribe();
    assert!(
        backfill.iter().all(|e| e.kind != "failover"),
        "no failover signal should fire for a one-shot request: {backfill:?}"
    );
}
