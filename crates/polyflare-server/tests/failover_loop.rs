//! B4 Task 4 (THE CRUX): the bounded cross-account failover loop, driven through the REAL ingress
//! (`responses_handler_impl`/`responses_handler_impl_for_test`), composing T1 (`failover_verdict`),
//! T2 (`exclude_tried`), and T3 (`CommitWitness`) exactly as `run_failover_loop` (`ingress.rs`)
//! implements them.
//!
//! Determinism: `CapacityWeighted` (the production selector) is seeded-random for tied weights, so
//! this suite uses a small deterministic test `Selector` (`FirstEligible`, mirroring
//! `no_anchor_failover.rs`'s `ExcludeReauth`) that always picks the first candidate — in the
//! account-id-ordered snapshot slice (`snapshot.rs`: "Candidate order is the account `list()` order
//! (`ORDER BY id`)") — satisfying the capability filter. This also makes `exclude_tried`'s
//! order-preservation (T2) load-bearing and observable: a selector bug that reordered or duplicated
//! candidates would produce the wrong attempt sequence, not just the wrong final answer.
//!
//! Upstream behavior is driven by a stub `Executor` (`FailoverStubExecutor`, mirroring
//! `commit_barrier.rs`'s `ByteThenErrorExecutor`/`ErrorFirstExecutor`) keyed by account id, rather
//! than a real mock HTTP server — this gives exact, deterministic control over each attempt's
//! outcome (a plain non-2xx failure, or a byte-then-mid-stream-drop) without network flakiness.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::http::HeaderMap;
use bytes::Bytes;
use futures_util::{stream, StreamExt};
use polyflare_codex::oauth::OAuthClient;
use polyflare_core::{
    Account, AccountId, AccountSnapshot, Continuity, ExecError, Executor, FailureSignal,
    PreparedRequest, RequestCtx, ResponseStream, SelectionCtx, Selector,
};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_server::ingress::responses_handler_impl_for_test;
use polyflare_server::runtime_settings::{RuntimeSettings, RuntimeSettingsFields};
use polyflare_store::{PlainTokens, Store, TokenCipher};

fn account(id: &str, security_work_authorized: bool) -> polyflare_store::Account {
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

/// Deterministic test selector: the FIRST candidate satisfying the capability filter, in whatever
/// order `candidates` arrives — mirrors the real selector's TA6 hard pre-filter
/// (`!ctx.require_security_work_authorized || s.security_work_authorized`) without the real
/// selector's seeded-random tie-break, so a multi-account pool's attempt ORDER is fully
/// predictable and `exclude_tried`'s order-preservation is directly exercised (see the module doc).
struct FirstEligible;
impl Selector for FirstEligible {
    fn pick(&self, candidates: &[AccountSnapshot], ctx: &SelectionCtx) -> Option<AccountId> {
        candidates
            .iter()
            .find(|s| !ctx.require_security_work_authorized || s.security_work_authorized)
            .map(|s| s.id.clone())
    }
    fn name(&self) -> &'static str {
        "first_eligible"
    }
}

/// One account's scripted response to a single `execute()` call.
#[derive(Clone)]
enum AttemptBehavior {
    /// A clean `response.created` -> `response.completed` stream.
    Success,
    /// `execute()` itself fails with a plain non-2xx status (no error code) — a pre-relay failure.
    Fail(u16),
    /// `execute()` succeeds and the stream yields ONE real content byte, then a mid-stream
    /// `ExecError::Stream` — the commit-barrier case.
    ByteThenDrop,
}

/// A test-only `Executor` keyed by `Account.id`: each account has a FIFO queue of
/// [`AttemptBehavior`]s (one per call), the LAST entry repeating if a queue is over-drawn. Records
/// every attempted account id, in order, so tests can assert the exact attempt sequence (which
/// accounts, how many, and that an excluded/non-authorized account was NEVER touched).
#[derive(Default)]
struct FailoverStubExecutor {
    behaviors: Mutex<HashMap<String, VecDeque<AttemptBehavior>>>,
    calls: Mutex<Vec<String>>,
}

impl FailoverStubExecutor {
    fn new() -> Self {
        Self::default()
    }

    /// Script `id`'s successive `execute()` calls. Missing entirely ⇒ always `Success`.
    fn script(&self, id: &str, behaviors: Vec<AttemptBehavior>) {
        self.behaviors
            .lock()
            .unwrap()
            .insert(id.to_string(), behaviors.into_iter().collect());
    }

    /// The ordered account ids every `execute()` call targeted.
    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl Executor for FailoverStubExecutor {
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
                let created =
                    format!(r#"{{"type":"response.created","response":{{"id":"{id}"}}}}"#);
                let completed =
                    format!(r#"{{"type":"response.completed","response":{{"id":"{id}"}}}}"#);
                Ok(ResponseStream::new(stream::iter(vec![
                    Ok::<Bytes, ExecError>(Bytes::from(format!("data: {created}\n\n"))),
                    Ok(Bytes::from(format!("data: {completed}\n\n"))),
                ])))
            }
            AttemptBehavior::Fail(status) => Err(ExecError::UpstreamStatus(FailureSignal {
                status,
                retry_after: None,
                error_code: None,
            })),
            AttemptBehavior::ByteThenDrop => {
                let first = Ok::<Bytes, ExecError>(Bytes::from_static(
                    b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n",
                ));
                let drop = Err(ExecError::Stream("mid-stream drop".into()));
                Ok(ResponseStream::new(stream::iter(vec![first, drop])))
            }
        }
    }
}

async fn spawn_store() -> (Store, TokenCipher, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[42u8; 32]).unwrap();
    (store, cipher, dir)
}

fn build_state(
    store: Store,
    cipher: TokenCipher,
    executor: Arc<FailoverStubExecutor>,
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
        runtime_settings: Arc::new(RuntimeSettings::new_from_fields(RuntimeSettingsFields {
            max_account_attempts: 3,
            starvation_wait_budget: std::time::Duration::from_secs(60),
            starvation_heartbeat: std::time::Duration::from_secs(10),
            wake_jitter_ms: 0,
            stream_idle_timeout: std::time::Duration::from_secs(300),
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
        runtime: Default::default(),
    })
}

/// Real HTTP round trip (via `build_app`) for the tests that use the DEFAULT bound (3).
async fn spawn_app(state: Arc<AppState>) -> String {
    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

/// Pulls the first `"id":"..."` value out of an SSE body — carries turn 1's `response.id` into
/// turn 2's `previous_response_id`, exactly as a real client would (mirrors `cyber_auto_move.rs`).
fn extract_id(body: &str) -> Option<String> {
    let idx = body.find("\"id\":\"")?;
    let rest = &body[idx + "\"id\":\"".len()..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

async fn drain(resp: reqwest::Response) -> String {
    let mut body = String::new();
    let mut s = resp.bytes_stream();
    // Tolerant: a mid-stream drop (test d) surfaces as a stream error to reqwest — stop draining
    // on the first error rather than panicking, since "the stream errors in-band" IS the assertion.
    while let Some(chunk) = s.next().await {
        match chunk {
            Ok(bytes) => body.push_str(&String::from_utf8_lossy(&bytes)),
            Err(_) => break,
        }
    }
    body
}

/// (a) A 429 -> B succeeds: the client gets B's stream, exactly 2 upstream attempts, A excluded
/// from attempt 2.
#[tokio::test]
async fn a_429_fails_over_to_b_which_succeeds() {
    let (store, cipher, _dir) = spawn_store().await;
    store
        .accounts()
        .insert(&account("A", false), &tokens("tokA"), &cipher)
        .await
        .unwrap();
    store
        .accounts()
        .insert(&account("B", false), &tokens("tokB"), &cipher)
        .await
        .unwrap();
    let exec = Arc::new(FailoverStubExecutor::new());
    exec.script("A", vec![AttemptBehavior::Fail(429)]);
    exec.script("B", vec![AttemptBehavior::Success]);
    let state = build_state(store, cipher, exec.clone());
    let pf = spawn_app(state).await;

    let resp = reqwest::Client::new()
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "m", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "the client gets B's clean stream");
    let body = drain(resp).await;
    assert!(
        body.contains("response.completed"),
        "B's clean completion relayed: {body}"
    );

    assert_eq!(
        exec.calls(),
        vec!["A".to_string(), "B".to_string()],
        "exactly 2 attempts, A then B — A excluded from attempt 2"
    );
}

/// (b) A, B, C all 429 with the default bound (3) -> surface after EXACTLY 3 attempts, no loop
/// past the bound.
#[tokio::test]
async fn all_429_surfaces_after_exactly_three_attempts() {
    let (store, cipher, _dir) = spawn_store().await;
    for id in ["A", "B", "C"] {
        store
            .accounts()
            .insert(&account(id, false), &tokens(&format!("tok{id}")), &cipher)
            .await
            .unwrap();
    }
    let exec = Arc::new(FailoverStubExecutor::new());
    for id in ["A", "B", "C"] {
        exec.script(id, vec![AttemptBehavior::Fail(429)]);
    }
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
        502,
        "exhausted after the bound: today's generic upstream-error response"
    );
    assert_eq!(
        exec.calls(),
        vec!["A".to_string(), "B".to_string(), "C".to_string()],
        "exactly 3 attempts — the loop must not run a 4th"
    );
}

/// (c) SECURITY FLOOR: a cyber-tagged request whose only capable account (A) 429s, with no OTHER
/// capable account in the pool (B exists but is not authorized) -> the distinct security 503, and
/// B (non-authorized) is NEVER attempted across the entire loop.
#[tokio::test]
async fn security_floor_never_attempts_a_non_authorized_account() {
    let (store, cipher, _dir) = spawn_store().await;
    store
        .accounts()
        .insert(&account("A", true), &tokens("tokA"), &cipher)
        .await
        .unwrap();
    store
        .accounts()
        .insert(&account("B", false), &tokens("tokB"), &cipher)
        .await
        .unwrap();
    let exec = Arc::new(FailoverStubExecutor::new());
    // A plain, ordinary retryable failure — NOT a `cyber_policy` rejection. This exercises B4's
    // general loop preserving `require_security_work_authorized`, not TA6(b)'s reactive reroute.
    exec.script("A", vec![AttemptBehavior::Fail(429)]);
    exec.script("B", vec![AttemptBehavior::Success]);
    let state = build_state(store, cipher, exec.clone());
    let pf = spawn_app(state).await;

    let resp = reqwest::Client::new()
        .post(format!("{pf}/responses"))
        .header("x-polyflare-capability", "security_work")
        .json(&serde_json::json!({"model": "m", "input": "hi"}))
        .send()
        .await
        .unwrap();

    assert_ne!(
        resp.status(),
        502,
        "the security floor is a DISTINCT error, not the generic upstream-error response"
    );
    assert!(
        resp.status().is_client_error() || resp.status().as_u16() == 503,
        "expected a clean 4xx/503-style refusal, got {}",
        resp.status()
    );
    let body = resp.text().await.unwrap();
    assert!(
        body.to_lowercase().contains("security") || body.to_lowercase().contains("authorized"),
        "body should clearly state no authorized account is available: {body}"
    );

    assert_eq!(
        exec.calls(),
        vec!["A".to_string()],
        "B is NOT authorized for security work and must NEVER be attempted, even on exhaustion"
    );
}

/// (d) COMMIT BARRIER: A relays a byte then drops mid-stream -> the error surfaces in-band (the
/// client keeps the byte it got, the stream then errors), and B is NEVER called.
#[tokio::test]
async fn commit_barrier_never_fails_over_after_a_relayed_byte() {
    let (store, cipher, _dir) = spawn_store().await;
    store
        .accounts()
        .insert(&account("A", false), &tokens("tokA"), &cipher)
        .await
        .unwrap();
    store
        .accounts()
        .insert(&account("B", false), &tokens("tokB"), &cipher)
        .await
        .unwrap();
    let exec = Arc::new(FailoverStubExecutor::new());
    exec.script("A", vec![AttemptBehavior::ByteThenDrop]);
    exec.script("B", vec![AttemptBehavior::Success]);
    let state = build_state(store, cipher, exec.clone());
    let pf = spawn_app(state).await;

    let outcome = reqwest::Client::new()
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "m", "input": "hi"}))
        .send()
        .await;
    // The HTTP status is committed the instant the (single) response starts streaming — 200, not a
    // 502/503 — because `execute_with_watchdog_tracked`'s Disarmed branch already returned
    // `Ok(stream)` by the time anything fails; the loop is structurally never entered (see
    // `run_failover_loop`'s doc in `ingress.rs`). A truncated chunked body can surface to a real
    // HTTP client at either layer — as a successful 200 whose body stream later errors, or (when
    // the truncation races the client's own header/framing read, as it can for a 1-chunk-then-drop
    // response this short) as a connection-level error out of `send()` itself. Both are the SAME
    // "surfaced in-band, never replayed" outcome from the server's point of view — the one
    // assertion that actually matters either way is `exec.calls()` below.
    match outcome {
        Ok(resp) => {
            assert_eq!(resp.status(), 200);
            let body = drain(resp).await;
            assert!(
                body.contains("\"hi\""),
                "the byte that WAS relayed must still reach the client: {body}"
            );
        }
        Err(e) => {
            assert!(
                e.is_request() || e.is_body() || e.is_decode(),
                "expected a connection/body-level error from the truncated response, got: {e:?}"
            );
        }
    }

    assert_eq!(
        exec.calls(),
        vec!["A".to_string()],
        "B must NEVER be called once A has relayed a byte — no double-relay"
    );
}

/// (e) CONTINUITY OWNERSHIP: a live-anchor pinned turn (a second turn with `previous_response_id`,
/// full-resend-shaped, hence Armed + `RecoveryPlan::ResendFull`) that fails must NOT fan out to a
/// new account — it surfaces exactly as before this task, today's generic upstream-error response.
#[tokio::test]
async fn live_anchor_pinned_turn_failure_is_not_fanned_out() {
    let (store, cipher, _dir) = spawn_store().await;
    store
        .accounts()
        .insert(&account("A", false), &tokens("tokA"), &cipher)
        .await
        .unwrap();
    let exec = Arc::new(FailoverStubExecutor::new());
    // Turn 1 succeeds (establishes A as the session/anchor owner); turn 2 (the live-anchor,
    // Armed attempt) fails.
    exec.script(
        "A",
        vec![AttemptBehavior::Success, AttemptBehavior::Fail(500)],
    );
    let state = build_state(store, cipher, exec.clone());
    let pf = spawn_app(state.clone()).await;
    let client = reqwest::Client::new();

    let r1 = client
        .post(format!("{pf}/responses"))
        .header("session_id", "sess-e")
        .json(&serde_json::json!({"model": "m", "input": [{"a": 1}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(r1.status(), 200);
    let body1 = r1.text().await.unwrap();
    let anchor_id = extract_id(&body1).expect("turn 1 emitted a response id");

    // B only shows up AFTER turn 1, so it can never affect turn 1's (deterministic) pick, and it
    // must never be attempted at all for this test — proving the CONTINUITY OWNERSHIP gate, not
    // just "B happened not to be picked".
    state
        .store
        .accounts()
        .insert(&account("B", false), &tokens("tokB"), &state.cipher)
        .await
        .unwrap();

    let r2 = client
        .post(format!("{pf}/responses"))
        .header("session_id", "sess-e")
        .json(&serde_json::json!({
            "model": "m",
            "previous_response_id": anchor_id,
            "input": [{"a": 1}, {"b": 2}],
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r2.status(),
        502,
        "a live-anchor pinned turn's failure surfaces exactly as before this task"
    );

    assert_eq!(
        exec.calls(),
        vec!["A".to_string(), "A".to_string()],
        "turn 1 + turn 2's failed live-anchor attempt on A — B is NEVER attempted"
    );
}

/// (f) TERMINAL: a 400 surfaces immediately with ZERO failover.
#[tokio::test]
async fn a_400_surfaces_immediately_with_zero_failover() {
    let (store, cipher, _dir) = spawn_store().await;
    store
        .accounts()
        .insert(&account("A", false), &tokens("tokA"), &cipher)
        .await
        .unwrap();
    store
        .accounts()
        .insert(&account("B", false), &tokens("tokB"), &cipher)
        .await
        .unwrap();
    let exec = Arc::new(FailoverStubExecutor::new());
    exec.script("A", vec![AttemptBehavior::Fail(400)]);
    exec.script("B", vec![AttemptBehavior::Success]);
    let state = build_state(store, cipher, exec.clone());
    let pf = spawn_app(state).await;

    let resp = reqwest::Client::new()
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "m", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 502, "a 400 surfaces exactly as before");

    assert_eq!(
        exec.calls(),
        vec!["A".to_string()],
        "zero failover for a request-terminal 400 — B must never be attempted"
    );
}

/// (g) REGRESSION: `max_attempts == 1` reproduces today's one-shot behavior EXACTLY — the clean-
/// rollback proof. A's failure is of a normally-RETRYABLE class (429), and B is available and would
/// succeed if ever tried, proving the bound (not pool exhaustion) is what stops the loop.
#[tokio::test]
async fn max_attempts_one_reproduces_the_one_shot_regression_exactly() {
    let (store, cipher, _dir) = spawn_store().await;
    store
        .accounts()
        .insert(&account("A", false), &tokens("tokA"), &cipher)
        .await
        .unwrap();
    store
        .accounts()
        .insert(&account("B", false), &tokens("tokB"), &cipher)
        .await
        .unwrap();
    let exec = Arc::new(FailoverStubExecutor::new());
    exec.script("A", vec![AttemptBehavior::Fail(429)]);
    exec.script("B", vec![AttemptBehavior::Success]);
    let state = build_state(store, cipher, exec.clone());

    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        "application/json".parse().unwrap(),
    );
    let body =
        Bytes::from(serde_json::to_vec(&serde_json::json!({"model": "m", "input": "hi"})).unwrap());
    let resp = responses_handler_impl_for_test(state, None, headers, body, 1).await;

    assert_eq!(
        resp.status(),
        502,
        "max_attempts=1 surfaces the first (retryable-class) failure immediately"
    );
    assert_eq!(
        exec.calls(),
        vec!["A".to_string()],
        "B is available and would succeed, yet must NEVER be attempted at max_attempts=1"
    );
}

/// Sanity: the test-seam handler itself, with the DEFAULT-equivalent bound (3), still fails over —
/// proving `responses_handler_impl_for_test` genuinely drives the same logic as the HTTP entrypoint
/// (not a stale/parallel copy), so test (g)'s max_attempts=1 result is a real regression proof and
/// not an artifact of the seam being wired differently.
#[tokio::test]
async fn test_seam_with_default_bound_still_fails_over() {
    let (store, cipher, _dir) = spawn_store().await;
    store
        .accounts()
        .insert(&account("A", false), &tokens("tokA"), &cipher)
        .await
        .unwrap();
    store
        .accounts()
        .insert(&account("B", false), &tokens("tokB"), &cipher)
        .await
        .unwrap();
    let exec = Arc::new(FailoverStubExecutor::new());
    exec.script("A", vec![AttemptBehavior::Fail(429)]);
    exec.script("B", vec![AttemptBehavior::Success]);
    let state = build_state(store, cipher, exec.clone());

    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        "application/json".parse().unwrap(),
    );
    let body =
        Bytes::from(serde_json::to_vec(&serde_json::json!({"model": "m", "input": "hi"})).unwrap());
    let resp = responses_handler_impl_for_test(state, None, headers, body, 3).await;

    assert_eq!(resp.status(), 200);
    assert_eq!(exec.calls(), vec!["A".to_string(), "B".to_string()]);
}

/// C9 Task 2 (THE CRUX): `run_failover_loop`'s A(fail)->B(succeed) cycle releases A's in-flight
/// lease before B is ever picked, and holds B's lease for the true lifetime of B's stream — no
/// double-count, no leak on A.
///
/// The precise timing this test exploits: `stream_response` wraps the returned `ResponseStream` in
/// `axum::body::Body::from_stream`, which is LAZY — the body is never polled just to construct the
/// `Response`. So the instant `responses_handler_impl_for_test(..).await` resolves (still holding
/// `resp`, its body untouched), B's `ObservingStream` — and the `InFlightGuard` embedded in it —
/// exists but has not yielded a single byte yet. This gives a deterministic (no timing race, no
/// custom pausable-stream fixture needed) window in which to observe "B's lease is genuinely held
/// while its stream is alive, not yet polled to completion" alongside "A's lease is already gone".
#[tokio::test]
async fn failover_releases_as_lease_before_b_is_picked_and_holds_bs_lease_while_streaming() {
    let (store, cipher, _dir) = spawn_store().await;
    store
        .accounts()
        .insert(&account("A", false), &tokens("tokA"), &cipher)
        .await
        .unwrap();
    store
        .accounts()
        .insert(&account("B", false), &tokens("tokB"), &cipher)
        .await
        .unwrap();
    let exec = Arc::new(FailoverStubExecutor::new());
    exec.script("A", vec![AttemptBehavior::Fail(429)]);
    exec.script("B", vec![AttemptBehavior::Success]);
    let state = build_state(store, cipher, exec.clone());

    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        "application/json".parse().unwrap(),
    );
    let body =
        Bytes::from(serde_json::to_vec(&serde_json::json!({"model": "m", "input": "hi"})).unwrap());
    let resp = responses_handler_impl_for_test(state.clone(), None, headers, body, 3).await;
    assert_eq!(resp.status(), 200, "B's stream is the client's response");
    assert_eq!(
        exec.calls(),
        vec!["A".to_string(), "B".to_string()],
        "A tried and failed, then B was picked and succeeded"
    );

    // Read the live runtime state DIRECTLY (in-process, no network) — before the response body is
    // ever polled.
    let mut snaps = vec![AccountSnapshot::new("A"), AccountSnapshot::new("B")];
    state.runtime.overlay(&mut snaps, 0);
    assert_eq!(
        snaps[0].in_flight, 0,
        "A's lease released the instant its failed attempt's own stack frame ended — strictly \
         before B was ever picked (never held past its failed attempt, never leaked)"
    );
    assert_eq!(
        snaps[1].in_flight, 1,
        "B's lease is genuinely held — inside the not-yet-polled ObservingStream this response's \
         body lazily wraps — proving the guard survived the handoff into the stream, not merely \
         until the function returned"
    );

    // Now drain B's stream to completion (clean EOF), dropping the ObservingStream.
    let _ = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("draining B's clean completion must not hang or fail");

    let mut snaps = vec![AccountSnapshot::new("A"), AccountSnapshot::new("B")];
    state.runtime.overlay(&mut snaps, 0);
    assert_eq!(
        snaps[0].in_flight, 0,
        "A's lease stays released (no resurrection, no double-count)"
    );
    assert_eq!(
        snaps[1].in_flight, 0,
        "B's lease releases once its stream is fully drained and dropped — no leak"
    );
}
