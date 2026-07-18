//! B5 Task 4 (THE CRUX) — Layer 2: the keepalive recovery-wait combinator. Drives the REAL ingress
//! (`responses_handler_impl_for_test_with_starvation_timing`, which runs the exact same logic as
//! the production `/responses` HTTP entrypoint — see `ingress.rs`'s doc on that seam) with accounts
//! seeded into a `Cooldown`-kind `InBackoff` verdict via a DURABLE `rate_limited` status + a short
//! `reset_at` (never via `RuntimeStates::record_rate_limit`, whose `RATE_LIMITED_MIN_COOLDOWN_SECS`
//! floor makes a short, test-scale cooldown unrepresentable — see the module doc on
//! "testability" below).
//!
//! Six tests, one per B5 Task 4 inviolable (see
//! `docs/superpowers/plans/2026-07-18-b5-antistarvation.md` Task 4's "Inviolables with a test
//! EACH"), plus a 7th added by the adversarial review (FIX 3, below):
//! 1. `post_200_in_band_error_when_budget_exceeded_before_recovery`
//! 2. `bounded_wait_never_exceeds_the_budget_even_if_the_account_never_recovers`
//! 3. `re_snapshot_after_the_wait_serves_a_now_recovered_account`
//! 4. `security_floor_is_preserved_across_the_wait_and_the_post_wait_reselect`
//! 5. `hardblocked_only_pool_never_waits_and_503s_fast`
//! 6. `keepalive_frames_in_the_wire_body_are_exactly_the_fixed_content_free_bytes`
//! 7. `overlay_drop_of_an_elapsed_runtime_cooldown_is_what_serves_the_account_after_the_wait` — a
//!    targeted regression the adversarial review flagged as missing: test (3) recovers via the
//!    DURABLE `rate_limited`/`reset_at` gate, which never exercises `RuntimeStates::overlay`
//!    (`runtime_state.rs:79-100`) at all. This test benches the account via an in-memory RUNTIME
//!    cooldown instead (seeded through the test-only `RuntimeStates::set_cooldown_until_for_test`
//!    seam, which bypasses `record_rate_limit`'s 30s floor) — see that test's own doc for the two
//!    distinct regressions it was verified to catch that test (3) cannot.
//!
//! # Testability — no real 10-60s sleep
//! `responses_handler_impl_for_test_with_starvation_timing` overrides Layer 2's wait budget +
//! heartbeat with test-scale `Duration`s (hundreds of ms), so every test here completes in low
//! single-digit real seconds. The RECOVERY itself is real wall-clock time passing a `reset_at`
//! epoch-seconds threshold — the durable `rate_limited`/`reset_at` gate (`select.rs::eligibility`)
//! has NO artificial floor (unlike `RuntimeStates::record_rate_limit`'s 30s floor), so a
//! `reset_at = now()+2` genuinely, natively recovers ~2 real seconds after the request starts, with
//! no test-side clock injection or extension of `polyflare-testkit` needed.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use axum::http::HeaderMap;
use bytes::Bytes;
use futures_util::{stream, StreamExt};
use polyflare_codex::oauth::OAuthClient;
use polyflare_core::{
    Account, AccountId, CapacityWeighted, Continuity, ExecError, Executor, PreparedRequest,
    RequestCtx, ResponseStream,
};
use polyflare_server::app::AppState;
use polyflare_server::continuity::CodexContinuity;
use polyflare_server::ingress::responses_handler_impl_for_test_with_starvation_timing;
use polyflare_server::starvation::StarvationOutcome;
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

/// A stub `Executor` that always succeeds and records every account id it was called with — the
/// same shape `starvation_layer1.rs`'s `RecordingExecutor` uses ("which account served this
/// request" is the exact assertion surface Layer 2's splice/security tests need).
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
        runtime: Default::default(),
    })
}

fn json_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        "application/json".parse().unwrap(),
    );
    headers
}

fn json_body() -> Bytes {
    // A distinctive marker so test (6) can prove it NEVER leaks into a keepalive frame.
    Bytes::from(
        serde_json::to_vec(&serde_json::json!({
            "model": "m",
            "input": "TOTALLY-SECRET-REQUEST-MARKER-12345"
        }))
        .unwrap(),
    )
}

/// Collects the full SSE body (bounded by an outer `tokio::time::timeout` at each call site — a
/// hang here would otherwise hang the test suite instead of failing it promptly) and returns it
/// alongside the already-known status code (captured before the body is consumed).
async fn collect_body(resp: axum::response::Response) -> (axum::http::StatusCode, String) {
    let status = resp.status();
    let mut data = resp.into_body().into_data_stream();
    let mut out = String::new();
    while let Some(chunk) = data.next().await {
        let bytes = chunk.expect(
            "B5 Task 4 inviolable: a Layer-2 stream item must NEVER be Err \
             (Global Constraint: POST-200 COMMIT — an Err item aborts the body ungracefully \
             instead of delivering an in-band SSE frame)",
        );
        out.push_str(&String::from_utf8_lossy(&bytes));
    }
    (status, out)
}

/// (1) INVIOLABLE — post-200 in-band error: a wait that recovers nothing within budget ⇒ 200 +
/// an in-band `response.failed` SSE frame carrying `StarvationOutcome::BudgetExceeded`'s reason
/// code, NEVER a late 4xx/5xx (there is only ever the ONE `Response`/status this test ever reads —
/// axum's `Response` object structurally cannot carry two statuses). No account is ever attempted.
///
/// REAL, not instant (B5 Task 4 adversarial review, FIX 1): pre-fix, `budget.as_secs()` truncated
/// any sub-second budget to 0, so a 700ms budget collapsed `target` to `wait_start` and the loop
/// broke on iteration 0 — this test passed while returning INSTANTLY with ZERO keepalives, never
/// exercising "emit keepalives → hit the budget ceiling → BudgetExceeded" at all. `budget = 2000ms`
/// is chosen to comfortably survive the worst case of `wait_start`'s OWN whole-second truncation
/// (`unix_now()` can silently eat up to ~999ms off the front of the wait) while still leaving room
/// for several `heartbeat = 300ms` ticks, so the keepalive-count assertion below is not flaky.
#[tokio::test]
async fn post_200_in_band_error_when_budget_exceeded_before_recovery() {
    let (store, cipher, _dir) = spawn_store().await;
    let mut a = account("A", false, "rate_limited");
    a.reset_at = Some(now() + 3600); // far beyond the test's budget — never recovers in time
    store
        .accounts()
        .insert(&a, &tokens("tokA"), &cipher)
        .await
        .unwrap();
    let exec = Arc::new(RecordingExecutor::default());
    let state = build_state(store, cipher, exec.clone());

    let budget = Duration::from_millis(2000);
    let heartbeat = Duration::from_millis(300);
    let start = Instant::now();
    let resp = tokio::time::timeout(
        Duration::from_secs(8),
        responses_handler_impl_for_test_with_starvation_timing(
            state,
            None,
            json_headers(),
            json_body(),
            3,
            budget,
            heartbeat,
        ),
    )
    .await
    .expect("Layer 2 must not hang past its own budget");

    let (status, body) = tokio::time::timeout(Duration::from_secs(5), collect_body(resp))
        .await
        .expect("draining the body must not hang");
    let elapsed = start.elapsed();

    assert_eq!(
        status, 200,
        "the wait already committed HTTP 200 before it knew the outcome"
    );
    assert!(
        !body.contains("response.completed"),
        "no real upstream stream was ever spliced in: {body}"
    );
    assert!(
        body.contains("event: response.failed"),
        "an in-band SSE error frame must be present: {body}"
    );
    assert!(
        body.contains(StarvationOutcome::BudgetExceeded.code()),
        "the specific budget-exceeded reason code must be present: {body}"
    );
    assert!(
        exec.calls().is_empty(),
        "budget exceeded before any re-select ever ran — no account is ever attempted"
    );

    // REAL-WAIT PROOF: multiple keepalives were actually emitted before the terminal frame — this
    // response was produced by genuinely running the budget-ceiling path, not an instant same-tick
    // return.
    let segments: Vec<&str> = body.split("\n\n").filter(|s| !s.is_empty()).collect();
    let keepalive_count = segments
        .iter()
        .filter(|s| s.starts_with(": keepalive"))
        .count();
    assert!(
        keepalive_count >= 2,
        "the wait must emit MULTIPLE keepalives before hitting the budget ceiling \
         (got {keepalive_count}): {body}"
    );
    assert!(
        segments
            .last()
            .is_some_and(|s| s.contains(StarvationOutcome::BudgetExceeded.code())),
        "the LAST frame on the wire must be the budget-exceeded error, not a trailing keepalive: \
         {body}"
    );
    assert!(
        elapsed >= Duration::from_millis(400),
        "the wait must genuinely take a while, not return instantly (elapsed={elapsed:?})"
    );
    assert!(
        elapsed < budget + Duration::from_secs(2),
        "the wait (elapsed={elapsed:?}) must stay bounded by the budget ({budget:?}), never \
         anywhere near the account's real 1-hour recovery time"
    );
}

/// (2) INVIOLABLE — bounded: the wait terminates ≤ budget even though the account NEVER recovers
/// (a 1-hour-out `reset_at`). Proven by measuring real wall-clock elapsed time end-to-end and
/// asserting it stays close to the (small, test-scale) budget — never anywhere near the account's
/// actual (never-reached) recovery time. The test itself completes in low real seconds.
///
/// REAL, not instant (B5 Task 4 adversarial review, FIX 1): pre-fix, `budget.as_secs()` truncated
/// the (then-)600ms budget to 0, so this test's wait collapsed to `wait_start` and returned
/// INSTANTLY — the `elapsed < budget + 3s` assertion below was trivially true whether or not the
/// budget ceiling was actually enforced, so a broken deadline check would have slipped straight
/// through. `budget = 2000ms` / `heartbeat = 300ms` are chosen the same way as sibling test (1):
/// large enough to survive `wait_start`'s own up-to-~999ms whole-second truncation and still leave
/// room for several real heartbeat ticks, so the keepalive-count assertion is not flaky.
#[tokio::test]
async fn bounded_wait_never_exceeds_the_budget_even_if_the_account_never_recovers() {
    let (store, cipher, _dir) = spawn_store().await;
    let mut a = account("A", false, "rate_limited");
    a.reset_at = Some(now() + 3600); // 1 hour out — the account never recovers within any
                                     // plausible test budget
    store
        .accounts()
        .insert(&a, &tokens("tokA"), &cipher)
        .await
        .unwrap();
    let exec = Arc::new(RecordingExecutor::default());
    let state = build_state(store, cipher, exec.clone());

    let budget = Duration::from_millis(2000);
    let heartbeat = Duration::from_millis(300);
    let start = Instant::now();
    let resp = tokio::time::timeout(
        Duration::from_secs(8),
        responses_handler_impl_for_test_with_starvation_timing(
            state,
            None,
            json_headers(),
            json_body(),
            3,
            budget,
            heartbeat,
        ),
    )
    .await
    .expect("Layer 2 must not hang past its own budget");
    let (status, body) = tokio::time::timeout(Duration::from_secs(5), collect_body(resp))
        .await
        .expect("draining the body must not hang — proves the stream terminates");
    let elapsed = start.elapsed();

    assert_eq!(status, 200);
    assert!(
        exec.calls().is_empty(),
        "the account never recovered — no account is ever attempted"
    );

    // REAL-WAIT PROOF: multiple keepalives were actually emitted — this is a genuine bounded wait,
    // not an instant return that happens to satisfy a loose elapsed-time inequality.
    let keepalive_count = body
        .split("\n\n")
        .filter(|s| s.starts_with(": keepalive"))
        .count();
    assert!(
        keepalive_count >= 2,
        "the wait must emit MULTIPLE keepalives before the budget ceiling stops it \
         (got {keepalive_count}): {body}"
    );
    assert!(
        elapsed >= Duration::from_millis(400),
        "the wait must genuinely take a while, not return instantly (elapsed={elapsed:?})"
    );
    assert!(
        elapsed < budget + Duration::from_secs(2),
        "the wait (elapsed={elapsed:?}) must stay close to the budget ({budget:?}), \
         never anywhere near the account's real 1-hour recovery time"
    );
}

/// (3) INVIOLABLE — RE-SNAPSHOT proves recovery (the load-bearing gotcha): an account on cooldown
/// until `T` with budget > `T` ⇒ after the (short, real) wait, it IS re-selected and served —
/// proving the post-wait re-select re-fetched + re-overlaid with a FRESH `now` rather than reusing
/// the stale pre-wait snapshot (which would have kept seeing the cooldown as still active forever).
#[tokio::test]
async fn re_snapshot_after_the_wait_serves_a_now_recovered_account() {
    let (store, cipher, _dir) = spawn_store().await;
    let mut a = account("A", false, "rate_limited");
    a.reset_at = Some(now() + 2); // recovers ~2 REAL seconds after the request starts
    store
        .accounts()
        .insert(&a, &tokens("tokA"), &cipher)
        .await
        .unwrap();
    let exec = Arc::new(RecordingExecutor::default());
    let state = build_state(store, cipher, exec.clone());

    let resp = responses_handler_impl_for_test_with_starvation_timing(
        state,
        None,
        json_headers(),
        json_body(),
        3,
        Duration::from_secs(6), // budget comfortably exceeds the 2s cooldown
        Duration::from_millis(300), // heartbeat ticks several times during the wait
    )
    .await;
    let (status, body) = tokio::time::timeout(Duration::from_secs(8), collect_body(resp))
        .await
        .expect("draining the body must not hang");

    assert_eq!(status, 200);
    assert!(
        body.contains("response.completed"),
        "the account recovered mid-wait and was actually served: {body}"
    );
    assert!(
        body.contains(": keepalive"),
        "the ~2s wait must have emitted at least one keepalive before recovering: {body}"
    );
    assert_eq!(
        exec.calls(),
        vec!["A".to_string()],
        "the re-recovered account is the one actually served"
    );
}

/// (4) INVIOLABLE — SECURITY FLOOR preserved across the wait AND the post-wait reselect: a cyber
/// request waits ONLY for the capable account (`soonest_recover`'s pre-wait filter) even though a
/// NON-authorized account recovers SOONER, and after the wait the re-select still serves ONLY the
/// capable account — never the non-authorized one, even though BOTH have recovered by the time the
/// wait ends. Proven via the executor's `.calls()` recorder (never the non-authorized id).
///
/// DETERMINISTIC, not ~50/50 (B5 Task 4 adversarial review, FIX 2): with two RECOVERED accounts of
/// equal weight, `CapacityWeighted`'s `WeightedIndex` (the production ingress always runs with
/// `SelectionCtx::rng_seed: None` — a real integration test has no seed hook to inject) would pick
/// between them genuinely at random, so if `require_security_work_authorized` were ever
/// (hypothetically) dropped from the post-wait `fresh_sel_ctx`, this test would only have caught it
/// ~half the time. Both accounts are driven to a ZERO secondary-credit weight below (a `"secondary"`
/// `usage_history` row at 100% used — `rate_limited` recovery zeros PRIMARY usage but explicitly NOT
/// secondary, see `select.rs::eligibility`'s `s.status == "rate_limited"` arm), which forces
/// `sample_weighted` down its `deterministic_min` fallback (`weights.iter().all(|w| *w <= 0.0)`) —
/// a strict, seed-free tiebreak that ultimately resolves by ascending account id. `non_auth`'s id
/// (`"aaa-non-auth"`) is chosen to sort BEFORE `"capable"`, so if the security filter were ever
/// dropped, `aaa-non-auth` would ALWAYS win the tiebreak and get served — making a dropped filter
/// fail this test's `exec.calls() == vec!["capable"]` assertion on EVERY run, not just half of them.
/// (With the filter intact — the actual production behavior — `standard_pool`'s capability
/// pre-filter reduces the post-wait pool to `capable` alone before any weighting runs at all, so
/// this weight-equalizing setup changes nothing about the real, correct code path.)
#[tokio::test]
async fn security_floor_is_preserved_across_the_wait_and_the_post_wait_reselect() {
    let (store, cipher, _dir) = spawn_store().await;
    let mut non_auth = account("aaa-non-auth", false, "rate_limited");
    non_auth.reset_at = Some(now() + 1); // recovers FIRST — but is never authorized
    store
        .accounts()
        .insert(&non_auth, &tokens("tok1"), &cipher)
        .await
        .unwrap();
    let mut capable = account("capable", true, "rate_limited");
    capable.reset_at = Some(now() + 2); // recovers SECOND — but IS authorized
    store
        .accounts()
        .insert(&capable, &tokens("tok2"), &cipher)
        .await
        .unwrap();
    // Zero out BOTH accounts' weight (see doc above) so a dropped security filter would resolve
    // deterministically by account id, not by a coin flip.
    let insert_at = now();
    store
        .accounts()
        .insert_usage_window("aaa-non-auth", "secondary", 100.0, None, None, insert_at)
        .await
        .unwrap();
    store
        .accounts()
        .insert_usage_window("capable", "secondary", 100.0, None, None, insert_at)
        .await
        .unwrap();
    let exec = Arc::new(RecordingExecutor::default());
    let state = build_state(store, cipher, exec.clone());

    let mut headers = json_headers();
    headers.insert("x-polyflare-capability", "security_work".parse().unwrap());
    let resp = responses_handler_impl_for_test_with_starvation_timing(
        state,
        None,
        headers,
        json_body(),
        3,
        Duration::from_secs(6),
        Duration::from_millis(300),
    )
    .await;
    let (status, body) = tokio::time::timeout(Duration::from_secs(8), collect_body(resp))
        .await
        .expect("draining the body must not hang");

    assert_eq!(status, 200);
    assert!(
        body.contains("response.completed"),
        "the capable account must be served once it recovers: {body}"
    );
    assert_eq!(
        exec.calls(),
        vec!["capable".to_string()],
        "the non-authorized account (which recovered SOONER) must NEVER be waited for or served \
         under a cyber ctx, before OR after the wait"
    );
}

/// (5) INVIOLABLE — HardBlocked is never a wait target: an all-`paused` (HardBlocked) pool ⇒
/// `soonest_recover` returns `None` ⇒ Layer 2 never applies ⇒ a fast, PRE-response 503 (no HTTP
/// 200 is ever committed). Proven by both the status AND by the elapsed time staying near-instant
/// (no keepalive loop of any kind ran).
#[tokio::test]
async fn hardblocked_only_pool_never_waits_and_503s_fast() {
    let (store, cipher, _dir) = spawn_store().await;
    store
        .accounts()
        .insert(
            &account("blocked-1", false, "paused"),
            &tokens("tok1"),
            &cipher,
        )
        .await
        .unwrap();
    store
        .accounts()
        .insert(
            &account("blocked-2", false, "reauth_required"),
            &tokens("tok2"),
            &cipher,
        )
        .await
        .unwrap();
    let exec = Arc::new(RecordingExecutor::default());
    let state = build_state(store, cipher, exec.clone());

    let start = Instant::now();
    let resp = responses_handler_impl_for_test_with_starvation_timing(
        state,
        None,
        json_headers(),
        json_body(),
        3,
        Duration::from_secs(60), // even a large budget must never be entered at all
        Duration::from_secs(10),
    )
    .await;
    let elapsed = start.elapsed();

    assert_eq!(
        resp.status(),
        503,
        "a fast, pre-response 503 — no wait ever begins"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "an all-HardBlocked pool must never enter ANY wait loop (elapsed={elapsed:?})"
    );
    assert!(
        exec.calls().is_empty(),
        "no HardBlocked account may ever be attempted"
    );
}

/// (6) INVIOLABLE — keepalive content-safety: every keepalive frame observed on the wire is
/// EXACTLY the fixed `": keepalive\n\n"` bytes — never the request's own content (asserted via a
/// distinctive marker string planted in the request body, `json_body()`'s
/// "TOTALLY-SECRET-REQUEST-MARKER-12345") and never an account id.
#[tokio::test]
async fn keepalive_frames_in_the_wire_body_are_exactly_the_fixed_content_free_bytes() {
    let (store, cipher, _dir) = spawn_store().await;
    let mut a = account("A", false, "rate_limited");
    a.reset_at = Some(now() + 2);
    store
        .accounts()
        .insert(&a, &tokens("tokA"), &cipher)
        .await
        .unwrap();
    let exec = Arc::new(RecordingExecutor::default());
    let state = build_state(store, cipher, exec.clone());

    let resp = responses_handler_impl_for_test_with_starvation_timing(
        state,
        None,
        json_headers(),
        json_body(),
        3,
        Duration::from_secs(6),
        Duration::from_millis(250),
    )
    .await;
    let (status, body) = tokio::time::timeout(Duration::from_secs(8), collect_body(resp))
        .await
        .expect("draining the body must not hang");
    assert_eq!(status, 200);

    // SSE frames are "\n\n"-delimited; every segment that IS a keepalive must be byte-identical to
    // the fixed frame body (`": keepalive"`, before the delimiter this split consumes) — never a
    // superset carrying extra bytes.
    let keepalive_segments: Vec<&str> = body
        .split("\n\n")
        .filter(|seg| seg.starts_with(": keepalive"))
        .collect();
    assert!(
        !keepalive_segments.is_empty(),
        "the ~2s wait must have produced at least one keepalive: {body}"
    );
    for seg in &keepalive_segments {
        assert_eq!(
            *seg, ": keepalive",
            "every keepalive segment must be EXACTLY the fixed frame, no extra content"
        );
    }
    assert!(
        !body.contains("TOTALLY-SECRET-REQUEST-MARKER-12345"),
        "the request's own content must never appear anywhere in the response stream: {body}"
    );

    let _ = exec.calls(); // sanity: executor handle stays usable; not asserted on here.
}

/// (7) REGRESSION (B5 Task 4 adversarial review, FIX 3) — proves the RE-SNAPSHOT's re-fetch +
/// re-overlay serves an account whose ONLY bench is an in-memory RUNTIME `cooldown_until`
/// (`RuntimeStates::overlay`, `runtime_state.rs:79-100` — `status = "active"` throughout, no
/// durable `rate_limited`/`reset_at` at all), a *different* code path from sibling test (3), which
/// recovers via the DURABLE gate instead. Verified (adversarially, by breaking + restoring the
/// production code — see the fix report) against TWO distinct regressions this test alone catches:
/// (a) freezing the post-wait re-select's `now` to the pre-wait `wait_start` (i.e. reusing the
/// pre-wait computation instead of a genuine re-fetch) turns this test red — it never recovers and
/// the stream ends in `StillNothing`; (b) dropping the PRE-wait `state.runtime.overlay(..)` call
/// entirely (so `soonest_recover` never even sees the runtime cooldown) ALSO turns this test red,
/// but differently — the account is served INSTANTLY with ZERO keepalives (Layer 2 never triggers
/// at all), which sibling test (3) is structurally incapable of catching since its bench is durable,
/// not runtime-overlay-dependent, at the pre-wait step.
///
/// Seeded via the test-only `RuntimeStates::set_cooldown_until_for_test` seam (added by this fix),
/// which bypasses `record_rate_limit`'s 30s floor so the cooldown can elapse on this suite's normal
/// fast timescale.
#[tokio::test]
async fn overlay_drop_of_an_elapsed_runtime_cooldown_is_what_serves_the_account_after_the_wait() {
    let (store, cipher, _dir) = spawn_store().await;
    // `status = "active"`, no `reset_at` — the DURABLE gate is fully open; the only benching signal
    // is the in-memory runtime cooldown seeded below.
    let a = account("A", false, "active");
    store
        .accounts()
        .insert(&a, &tokens("tokA"), &cipher)
        .await
        .unwrap();
    let exec = Arc::new(RecordingExecutor::default());
    let state = build_state(store, cipher, exec.clone());
    // Bench "A" via an in-memory RUNTIME cooldown that elapses ~2 REAL seconds after the request
    // starts — NOT `record_rate_limit` (whose 30s floor would make this un-fast), and NOT the
    // durable `rate_limited`/`reset_at` gate sibling test (3) already covers.
    state
        .runtime
        .set_cooldown_until_for_test(&AccountId::from("A"), now() + 2);

    let resp = responses_handler_impl_for_test_with_starvation_timing(
        state,
        None,
        json_headers(),
        json_body(),
        3,
        Duration::from_secs(6), // budget comfortably exceeds the 2s runtime cooldown
        Duration::from_millis(300), // heartbeat ticks several times during the wait
    )
    .await;
    let (status, body) = tokio::time::timeout(Duration::from_secs(8), collect_body(resp))
        .await
        .expect("draining the body must not hang");

    assert_eq!(status, 200);
    assert!(
        body.contains("response.completed"),
        "the account's runtime cooldown elapsed mid-wait and it was actually served — proving the \
         re-fetch + re-overlay-with-fresh-now saw the elapsed cooldown DROP (runtime_state.rs:96), \
         not a stale pre-wait overlay: {body}"
    );
    assert!(
        body.contains(": keepalive"),
        "the ~2s wait must have emitted at least one keepalive before recovering: {body}"
    );
    assert_eq!(
        exec.calls(),
        vec!["A".to_string()],
        "the re-recovered account is the one actually served"
    );
}
