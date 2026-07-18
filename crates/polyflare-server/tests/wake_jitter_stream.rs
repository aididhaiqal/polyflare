//! B10 Task 1 (THE CRUX) — Step (d): a stream-level, real-clock proof that
//! `polyflare_server::ingress::wake_jitter_offset_ms` actually desynchronizes TWO concurrent
//! Layer 2 waiters on the SAME recovering account, mirroring `starvation_layer2.rs`'s "no real
//! 10-60s sleep" testability doc (test-scale budget/heartbeat via
//! `responses_handler_impl_for_test_with_starvation_timing`; the RECOVERY itself is real
//! wall-clock time passing a short `reset_at`).
//!
//! Two tests:
//! 1. `two_concurrent_waiters_on_the_same_account_desync_when_jitter_is_positive` — with
//!    `wake_jitter_ms > 0` and two DIFFERENT session keys, the two waiters' re-select times
//!    (measured via a `RecordingExecutor` that timestamps every call) differ by roughly the
//!    predicted jitter gap, while BOTH still get served and BOTH serve the SAME account (jitter
//!    only changes WHEN, never WHICH account).
//! 2. `two_concurrent_waiters_on_the_same_account_stay_in_lockstep_when_jitter_is_zero` — the
//!    `wake_jitter_ms = 0` disable-lever baseline: the two waiters' re-select times stay close
//!    together (today's exact pre-B10 lockstep behavior), proving B10 changes nothing when off.
//!
//! B10 Task 2 additionally extends both tests (rather than adding new ones — the stream-level
//! desync + both-served properties were already fully proven by Task 1's two tests above) to
//! subscribe to `state.log_bus` BEFORE firing the waiters and assert the content-free
//! `wake_jitter_applied_ms` field on the resulting `StarvationSignal`/`LogEvent`s: (1) with
//! `wake_jitter_ms > 0` the two waiters' applied offsets DIFFER (an operator can see spreading is
//! actually active, not just infer it from timing), and (2) with `wake_jitter_ms = 0` both applied
//! offsets are `0` (the disable lever leaves the observable itself inert too). Both assertions also
//! reuse `observability.rs`'s forbidden-word content-safety idiom on the emitted messages.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use axum::http::HeaderMap;
use bytes::Bytes;
use futures_util::stream;
use polyflare_codex::oauth::OAuthClient;
use polyflare_core::{
    Account, CapacityWeighted, Continuity, ExecError, Executor, PreparedRequest, RequestCtx,
    ResponseStream,
};
use polyflare_server::app::AppState;
use polyflare_server::continuity::CodexContinuity;
use polyflare_server::ingress::{
    responses_handler_impl_for_test_with_starvation_timing, wake_jitter_offset_ms,
};
use polyflare_server::session_key::sha256_hex;
use polyflare_store::{PlainTokens, Store, TokenCipher};

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn account(id: &str, status: &str) -> polyflare_store::Account {
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

/// A stub `Executor` that always succeeds and records every account id it was called with
/// ALONGSIDE how long after `start` the call happened — the exact desync proof surface: WHICH
/// account served (must be identical across both waiters) and WHEN (must differ when jitter > 0,
/// must stay close when jitter == 0). Mirrors `starvation_layer2.rs`'s `RecordingExecutor`.
struct RecordingExecutor {
    start: Instant,
    calls: std::sync::Mutex<Vec<(String, Duration)>>,
}

impl RecordingExecutor {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            calls: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn calls(&self) -> Vec<(String, Duration)> {
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
            .push((account.id.as_str().to_string(), self.start.elapsed()));
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
    let cipher = TokenCipher::from_key_bytes(&[77u8; 32]).unwrap();
    (store, cipher, dir)
}

/// Builds a full `AppState`, mirroring `starvation_layer2.rs::build_state` exactly, but with an
/// explicit `wake_jitter_ms` — the ONE new field this task threads onto `AppState`
/// (`crate::config::wake_jitter_ms_from_env`'s resolved value in production;
/// `crate::ingress::layer2_wait_stream` reads it directly, no new call-chain parameter needed).
fn build_state(
    store: Store,
    cipher: TokenCipher,
    executor: Arc<RecordingExecutor>,
    wake_jitter_ms: u64,
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
        health_tier_metrics: polyflare_server::observability::HealthTierMetrics::new(),
        starvation_wait_budget: std::time::Duration::from_secs(60),
        starvation_heartbeat: std::time::Duration::from_secs(10),
        wake_jitter_ms,
        starvation_metrics: polyflare_server::observability::StarvationMetrics::new(),
        stream_idle_timeout: std::time::Duration::from_secs(300),
        soft_drain_enabled: true,
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
    Bytes::from(
        serde_json::to_vec(&serde_json::json!({
            "model": "m",
            "input": "wake-jitter-stream-marker"
        }))
        .unwrap(),
    )
}

/// B10 Task 2: drains every `kind == "starvation"` event currently queued on a `LogBus` receiver
/// (non-blocking — both waiters have already fully completed by the time this is called, so every
/// `StarvationSignal` they emitted is already published). Mirrors
/// `observability.rs::starvation_signal_log_event_*`'s content-safety idiom, applied here at the
/// e2e/`LogBus` layer instead of the unit-test/`tracing`-capture layer — the same struct, a
/// different, equally authoritative observation point.
fn drain_starvation_events(
    rx: &mut tokio::sync::broadcast::Receiver<polyflare_server::log_bus::LogEvent>,
) -> Vec<polyflare_server::log_bus::LogEvent> {
    let mut out = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        if ev.kind == "starvation" {
            out.push(ev);
        }
    }
    out
}

/// B10 Task 2: pulls the `wake_jitter_applied_ms=<N>` value out of a starvation `LogEvent.message`
/// — plain digit-parsing (no regex dependency in this crate), matching
/// `observability.rs::to_log_event`'s fixed `"... wake_jitter_applied_ms={}"` suffix exactly.
fn extract_wake_jitter_applied_ms(message: &str) -> u64 {
    let marker = "wake_jitter_applied_ms=";
    let start = message
        .find(marker)
        .unwrap_or_else(|| panic!("message missing `{marker}`: {message}"))
        + marker.len();
    let digits: String = message[start..]
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect();
    digits
        .parse()
        .unwrap_or_else(|_| panic!("could not parse wake_jitter_applied_ms from: {message}"))
}

/// B10 Task 2: the same forbidden-word content-safety check `observability.rs`'s
/// `starvation_signal_log_event_is_content_free` runs, reused here at the e2e layer — a starvation
/// `LogEvent` fired through the REAL Layer-2 wait path must be exactly as content-free as the
/// hand-built unit-test one.
fn assert_content_free(message: &str) {
    for forbidden in [
        "bearer", "body", "content", "delta", "text", "input", "message\":",
    ] {
        assert!(
            !message.to_lowercase().contains(forbidden),
            "forbidden content `{forbidden}` leaked into starvation log event: {message}"
        );
    }
}

async fn collect_body(resp: axum::response::Response) -> (axum::http::StatusCode, String) {
    let status = resp.status();
    let mut data = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("draining the body must not hang or fail");
    let mut out = String::new();
    out.push_str(&String::from_utf8_lossy(&std::mem::take(&mut data)));
    (status, out)
}

/// Picks two `x-codex-turn-state` header values (from a small deterministic candidate pool) whose
/// PREDICTED `wake_jitter_offset_ms` (over the SAME hash derivation `layer2_wait_stream` uses —
/// `sha256_hex("turn:{header}")`) differ by at least `min_gap_ms` within `wake_jitter_ms`'s window
/// — avoids a flaky test that happens to pick two keys that hash close together on a given run.
fn pick_desyncing_keys(wake_jitter_ms: u64, min_gap_ms: u64) -> (String, String) {
    let candidates: Vec<(String, u64)> = (0..32)
        .map(|i| {
            let k = format!("wj-key-{i}");
            let session_value = sha256_hex(format!("turn:{k}").as_bytes());
            let offset = wake_jitter_offset_ms(&session_value, wake_jitter_ms);
            (k, offset)
        })
        .collect();
    let (key_a, offset_a) = candidates
        .iter()
        .min_by_key(|(_, o)| *o)
        .cloned()
        .expect("non-empty candidate pool");
    let (key_b, offset_b) = candidates
        .iter()
        .max_by_key(|(_, o)| *o)
        .cloned()
        .expect("non-empty candidate pool");
    assert!(
        offset_b.abs_diff(offset_a) >= min_gap_ms,
        "test setup: need two candidate keys whose predicted offsets differ by >= {min_gap_ms}ms \
         within a {wake_jitter_ms}ms window (got {offset_a} vs {offset_b}) — widen the candidate \
         pool"
    );
    (key_a, key_b)
}

/// (1) Positive jitter desyncs the two waiters' wake times, while both still get served and both
/// serve the SAME account — jitter changes WHEN, never WHICH account (the plan's Global
/// Constraints).
#[tokio::test]
async fn two_concurrent_waiters_on_the_same_account_desync_when_jitter_is_positive() {
    let (store, cipher, _dir) = spawn_store().await;
    // Far-future placeholder — re-anchored to a real, short margin immediately before the request
    // fires below (mirrors `starvation_layer2.rs::re_snapshot_after_the_wait_serves_a_now_recovered_account`'s
    // technique: setup I/O latency + whole-second `reset_at` truncation must not eat the margin).
    let mut a = account("A", "rate_limited");
    a.reset_at = Some(now() + 3600);
    store
        .accounts()
        .insert(&a, &tokens("tokA"), &cipher)
        .await
        .unwrap();

    let exec = Arc::new(RecordingExecutor::new());
    let wake_jitter_ms = 4000u64;
    let state = build_state(store, cipher, exec.clone(), wake_jitter_ms);
    // B10 Task 2: subscribe BEFORE firing either waiter so no `StarvationSignal` `LogEvent` can be
    // published before this receiver exists (mirrors `LogBus::subscribe`'s own doc — the
    // ring-buffer lock makes "subscribe first" race-free either way, but this keeps ordering
    // obvious).
    let (_backfill, mut log_rx) = state.log_bus.subscribe();

    let fire_at = now();
    state
        .store
        .accounts()
        .update_status_and_reset("A", "rate_limited", Some(fire_at + 4))
        .await
        .unwrap();

    let (key_a, key_b) = pick_desyncing_keys(wake_jitter_ms, 1000);

    let budget = Duration::from_millis(9000);
    let heartbeat = Duration::from_millis(200);

    let mut headers_a = json_headers();
    headers_a.insert("x-codex-turn-state", key_a.parse().unwrap());
    let mut headers_b = json_headers();
    headers_b.insert("x-codex-turn-state", key_b.parse().unwrap());

    let call_a = responses_handler_impl_for_test_with_starvation_timing(
        state.clone(),
        None,
        headers_a,
        json_body(),
        3,
        budget,
        heartbeat,
    );
    let call_b = responses_handler_impl_for_test_with_starvation_timing(
        state.clone(),
        None,
        headers_b,
        json_body(),
        3,
        budget,
        heartbeat,
    );

    let (resp_a, resp_b) = tokio::time::timeout(Duration::from_secs(15), async {
        tokio::join!(call_a, call_b)
    })
    .await
    .expect("both concurrent waiters must complete within the outer timeout");

    let (status_a, body_a) = tokio::time::timeout(Duration::from_secs(5), collect_body(resp_a))
        .await
        .expect("draining waiter A's body must not hang");
    let (status_b, body_b) = tokio::time::timeout(Duration::from_secs(5), collect_body(resp_b))
        .await
        .expect("draining waiter B's body must not hang");

    assert_eq!(status_a, 200, "waiter A must be served: {body_a}");
    assert_eq!(status_b, 200, "waiter B must be served: {body_b}");
    assert!(
        body_a.contains("response.completed") && body_b.contains("response.completed"),
        "both waiters must eventually splice in the real recovered stream: a={body_a} b={body_b}"
    );

    let calls = exec.calls();
    assert_eq!(
        calls.len(),
        2,
        "both waiters must eventually be served exactly once each: {calls:?}"
    );
    assert!(
        calls.iter().all(|(id, _)| id == "A"),
        "jitter must never change WHICH account is served — both waiters must serve the SAME \
         account: {calls:?}"
    );

    let elapsed: Vec<Duration> = calls.iter().map(|(_, d)| *d).collect();
    let gap = elapsed[0].abs_diff(elapsed[1]);
    assert!(
        gap >= Duration::from_millis(400),
        "positive jitter must desync the two waiters' wake/re-select times (gap={gap:?}, \
         calls={calls:?})"
    );

    // B10 Task 2: the content-free observable — both waiters must have fired a `StarvationSignal`
    // recording the applied jitter, and (since `key_a`/`key_b` were chosen to hash to PREDICTED
    // offsets at least 1000ms apart within the 4000ms window) the two APPLIED offsets recorded on
    // the real, end-to-end-emitted signals must differ too — an operator can see spreading is
    // active straight from the signal, not just infer it from timing.
    let starvation_events = drain_starvation_events(&mut log_rx);
    assert_eq!(
        starvation_events.len(),
        2,
        "both waiters must each emit exactly one starvation signal: {starvation_events:?}"
    );
    let applied: Vec<u64> = starvation_events
        .iter()
        .map(|ev| {
            assert_content_free(&ev.message);
            assert!(
                ev.message.contains("reason=starvation_wait_recovered"),
                "both waiters were genuinely served, so both signals must carry the recovered \
                 reason: {}",
                ev.message
            );
            let applied_ms = extract_wake_jitter_applied_ms(&ev.message);
            assert!(
                applied_ms <= wake_jitter_ms,
                "applied jitter must never exceed the configured window: {applied_ms} > \
                 {wake_jitter_ms}"
            );
            applied_ms
        })
        .collect();
    assert_ne!(
        applied[0], applied[1],
        "the two waiters' RECORDED applied jitter must differ — the content-free observable must \
         actually reflect the desync, not just a constant: {applied:?}"
    );
}

/// (2) The `wake_jitter_ms = 0` disable-lever baseline: the two waiters' re-select times stay
/// close together — today's exact pre-B10 lockstep behavior is unchanged when the feature is off.
#[tokio::test]
async fn two_concurrent_waiters_on_the_same_account_stay_in_lockstep_when_jitter_is_zero() {
    let (store, cipher, _dir) = spawn_store().await;
    // Far-future placeholder, re-anchored right before firing — see the sibling test's doc.
    let mut a = account("A", "rate_limited");
    a.reset_at = Some(now() + 3600);
    store
        .accounts()
        .insert(&a, &tokens("tokA"), &cipher)
        .await
        .unwrap();

    let exec = Arc::new(RecordingExecutor::new());
    let state = build_state(store, cipher, exec.clone(), 0);
    // B10 Task 2: subscribe BEFORE firing either waiter — see the sibling test's doc.
    let (_backfill, mut log_rx) = state.log_bus.subscribe();

    let fire_at = now();
    state
        .store
        .accounts()
        .update_status_and_reset("A", "rate_limited", Some(fire_at + 4))
        .await
        .unwrap();

    let budget = Duration::from_millis(9000);
    let heartbeat = Duration::from_millis(200);

    let mut headers_a = json_headers();
    headers_a.insert("x-codex-turn-state", "wj-lockstep-a".parse().unwrap());
    let mut headers_b = json_headers();
    headers_b.insert("x-codex-turn-state", "wj-lockstep-b".parse().unwrap());

    let call_a = responses_handler_impl_for_test_with_starvation_timing(
        state.clone(),
        None,
        headers_a,
        json_body(),
        3,
        budget,
        heartbeat,
    );
    let call_b = responses_handler_impl_for_test_with_starvation_timing(
        state.clone(),
        None,
        headers_b,
        json_body(),
        3,
        budget,
        heartbeat,
    );

    let (resp_a, resp_b) = tokio::time::timeout(Duration::from_secs(15), async {
        tokio::join!(call_a, call_b)
    })
    .await
    .expect("both concurrent waiters must complete within the outer timeout");

    let (status_a, _) = tokio::time::timeout(Duration::from_secs(5), collect_body(resp_a))
        .await
        .expect("draining waiter A's body must not hang");
    let (status_b, _) = tokio::time::timeout(Duration::from_secs(5), collect_body(resp_b))
        .await
        .expect("draining waiter B's body must not hang");
    assert_eq!(status_a, 200);
    assert_eq!(status_b, 200);

    let calls = exec.calls();
    assert_eq!(calls.len(), 2, "both waiters must be served: {calls:?}");
    assert!(
        calls.iter().all(|(id, _)| id == "A"),
        "both waiters must serve the SAME account: {calls:?}"
    );

    let elapsed: Vec<Duration> = calls.iter().map(|(_, d)| *d).collect();
    let gap = elapsed[0].abs_diff(elapsed[1]);
    assert!(
        gap <= Duration::from_millis(500),
        "with wake_jitter_ms=0 the two waiters must wake at (statistically) the SAME instant — \
         today's exact pre-B10 lockstep behavior (gap={gap:?}, calls={calls:?})"
    );

    // B10 Task 2: the disable-lever baseline for the content-free observable too — with
    // `wake_jitter_ms=0` BOTH waiters' recorded applied jitter must be exactly `0` (never merely
    // "small" or "equal to each other by coincidence"), proving the observable itself goes fully
    // inert under the default, not just the timing.
    let starvation_events = drain_starvation_events(&mut log_rx);
    assert_eq!(
        starvation_events.len(),
        2,
        "both waiters must each emit exactly one starvation signal: {starvation_events:?}"
    );
    for ev in &starvation_events {
        assert_content_free(&ev.message);
        assert_eq!(
            extract_wake_jitter_applied_ms(&ev.message),
            0,
            "wake_jitter_ms=0 must record zero applied jitter on the signal: {}",
            ev.message
        );
    }
}
