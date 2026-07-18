//! B5 Task 5 — the content-free starvation observability signal + the disclosed
//! `outcome.account_id` gap fix. Companion to `tests/starvation_layer2.rs` (Task 4, frozen/reviewed
//! — this file does not touch it), driving the SAME real ingress seam
//! (`responses_handler_impl_for_test_with_starvation_timing`) with the SAME testability strategy
//! (a durable `rate_limited`/`reset_at` gate that recovers ~2 REAL wall-clock seconds after the
//! request starts — no clock injection needed).
//!
//! Two things proven here that Task 4's suite does not:
//! 1. A Layer 2 wait's terminal outcome fires `crate::observability::StarvationSignal` — the
//!    `StarvationMetrics` counter bumps, and a content-free `"starvation"` `LogEvent` lands on the
//!    `log_bus` carrying ONLY the reason code, the wait-target/served account ids, and the waited
//!    duration — NEVER a body/message/token.
//! 2. **The `outcome.account_id` fix**: in a multi-account pool where the WAIT TARGET
//!    (`soonest_recover`'s min-`recover_at` pick) and the post-wait SPLICED account (the actual
//!    `selector.pick` winner once both have recovered) DIFFER, the `StarvationSignal`'s
//!    `served_account` — the authoritative record, since `RouteOutcome`/`RequestLog` are finalized
//!    before the stream is ever polled (see `crate::observability::StarvationSignal`'s doc) —
//!    correctly reports the SPLICED account, not the wait target.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use axum::http::HeaderMap;
use bytes::Bytes;
use futures_util::{stream, StreamExt};
use polyflare_codex::oauth::OAuthClient;
use polyflare_core::{
    Account, AccountId, CapacityWeighted, Continuity, ExecError, Executor, PreparedRequest,
    RequestCtx, ResponseStream, RoundRobin, SelectionCtx, Selector,
};
use polyflare_server::app::AppState;
use polyflare_server::continuity::CodexContinuity;
use polyflare_server::ingress::responses_handler_impl_for_test_with_starvation_timing;
use polyflare_server::starvation::{StarvationOutcome, STARVATION_RECOVERED_REASON};
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

/// Same shape as `starvation_layer2.rs`'s `RecordingExecutor` — always succeeds, records every
/// account id it was called with.
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
    let cipher = TokenCipher::from_key_bytes(&[77u8; 32]).unwrap();
    (store, cipher, dir)
}

/// Like `starvation_layer2.rs`'s `build_state`, parameterized by the routing `Selector` (default
/// `CapacityWeighted` isn't deterministic under tied weights — the "differ" test below needs a
/// fully deterministic post-wait pick, so it swaps in `RoundRobin`).
fn build_state(
    store: Store,
    cipher: TokenCipher,
    executor: Arc<RecordingExecutor>,
    selector: Arc<dyn Selector>,
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
        selector,
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
        starvation_wait_budget: Duration::from_secs(60),
        starvation_heartbeat: Duration::from_secs(10),
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
    // A distinctive marker so the content-safety assertions can prove it never leaks.
    Bytes::from(
        serde_json::to_vec(&serde_json::json!({
            "model": "m",
            "input": "TOTALLY-SECRET-REQUEST-MARKER-98765"
        }))
        .unwrap(),
    )
}

async fn collect_body(resp: axum::response::Response) -> (axum::http::StatusCode, String) {
    let status = resp.status();
    let mut data = resp.into_body().into_data_stream();
    let mut out = String::new();
    while let Some(chunk) = data.next().await {
        let bytes = chunk.expect("a Layer-2 stream item must never be Err");
        out.push_str(&String::from_utf8_lossy(&bytes));
    }
    (status, out)
}

/// (1) The content-free starvation signal fires on a successful Layer-2 recovery: the
/// `StarvationMetrics` counter bumps by exactly one, and the `log_bus` carries exactly one
/// `"starvation"` event with the fixed recovered-reason code, the served account id, and a
/// nonzero `waited_ms` — never a body/message/token. Single-account pool, so the wait target and
/// the served account are necessarily the SAME id here (see test (2) below for the differ case).
#[tokio::test]
async fn layer2_recovery_fires_content_free_signal_with_waited_duration_and_served_account() {
    let (store, cipher, _dir) = spawn_store().await;
    let mut a = account("A", "rate_limited");
    a.reset_at = Some(now() + 2); // recovers ~2 real seconds after the request starts
    store
        .accounts()
        .insert(&a, &tokens("tokA"), &cipher)
        .await
        .unwrap();
    let exec = Arc::new(RecordingExecutor::default());
    let state = build_state(store, cipher, exec.clone(), Arc::new(CapacityWeighted));

    assert_eq!(state.starvation_metrics.total(), 0, "no wait has happened yet");

    let resp = responses_handler_impl_for_test_with_starvation_timing(
        state.clone(),
        None,
        json_headers(),
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
    assert!(body.contains("response.completed"));
    assert_eq!(exec.calls(), vec!["A".to_string()]);

    assert_eq!(
        state.starvation_metrics.total(),
        1,
        "exactly one Layer 2 wait terminal outcome recorded"
    );

    let (backfill, _rx) = state.log_bus.subscribe();
    let starvation_events: Vec<_> = backfill.iter().filter(|e| e.kind == "starvation").collect();
    assert_eq!(
        starvation_events.len(),
        1,
        "exactly one starvation log event, got: {backfill:?}"
    );
    let ev = starvation_events[0];
    assert_eq!(
        ev.account.as_deref(),
        Some("A"),
        "the served account is attributed"
    );
    assert!(ev.message.contains(STARVATION_RECOVERED_REASON));
    assert!(ev.message.contains("wait_target=A"));
    assert!(ev.message.contains("served=A"));
    assert!(
        ev.latency_ms.is_some_and(|ms| ms > 0),
        "waited_ms must be a genuine nonzero duration: {ev:?}"
    );

    // CONTENT-SAFETY: never a body, token, or request/response fragment.
    let msg_lc = ev.message.to_lowercase();
    for forbidden in [
        "totally-secret",
        "response.completed",
        "response.created",
        "resp_1",
        "data:",
        "toka",
        "bearer",
        "authorization",
    ] {
        assert!(
            !msg_lc.contains(forbidden),
            "forbidden content `{forbidden}` leaked into the starvation signal: {}",
            ev.message
        );
    }
}

/// (2) THE `outcome.account_id` FIX — the multi-account "differ" scenario: two accounts are
/// benched via an IDENTICAL in-memory runtime cooldown (a genuine tie in `soonest_recover`'s
/// `min_by_key`, which resolves ties to the FIRST account in `snapshots`' ascending-id order —
/// `store.rs`'s `ORDER BY id` — so `"a-acct"` is the WAIT TARGET). The pool uses `RoundRobin`
/// (deterministic: picks the LEAST-recently-selected account), and `"a-acct"` is pre-stamped as
/// JUST selected (a recent `last_selected_at`) before the request starts, while `"b-acct"` has
/// NEVER been selected (`last_selected_at: None`, treated as the oldest / most-preferred). By the
/// time both accounts have recovered (the wait ends when the tied `recover_at` is reached), the
/// post-wait re-select therefore deterministically picks `"b-acct"` — the account ACTUALLY served —
/// even though `"a-acct"` was the wait target. The `StarvationSignal.served_account` must report
/// `"b-acct"`, proving the fix: `RouteOutcome.account_id` (best-effort, structurally frozen at
/// commit time — see that field's doc) still shows the wait target `"a-acct"`, but the
/// authoritative signal shows the account that actually served the client.
#[tokio::test]
async fn layer2_wait_target_and_spliced_account_differ_signal_records_the_spliced_one() {
    let (store, cipher, _dir) = spawn_store().await;
    // Durable gate fully open (`status = "active"`, no `reset_at`) — the ONLY bench is the
    // in-memory runtime cooldown seeded below, mirroring `starvation_layer2.rs` test (7).
    let a = account("a-acct", "active");
    let b = account("b-acct", "active");
    store
        .accounts()
        .insert(&a, &tokens("tokA"), &cipher)
        .await
        .unwrap();
    store
        .accounts()
        .insert(&b, &tokens("tokB"), &cipher)
        .await
        .unwrap();
    let exec = Arc::new(RecordingExecutor::default());
    let state = build_state(store, cipher, exec.clone(), Arc::new(RoundRobin));

    // Both accounts recover at the SAME instant — a genuine tie for `soonest_recover`'s wait
    // target, resolved to the ascending-id-first account, "a-acct".
    let recover_epoch = now() + 2;
    state
        .runtime
        .set_cooldown_until_for_test(&AccountId::from("a-acct"), recover_epoch);
    state
        .runtime
        .set_cooldown_until_for_test(&AccountId::from("b-acct"), recover_epoch);
    // Pre-stamp "a-acct" as JUST selected, so RoundRobin's post-wait re-select deterministically
    // prefers "b-acct" (never selected ⇒ `last_selected_at: None` ⇒ treated as 0, the oldest).
    state
        .runtime
        .record_selected(&AccountId::from("a-acct"), now());

    // Sanity: confirm the WAIT TARGET really is "a-acct" (the ascending-id tie winner), not
    // "b-acct" — this is what `soonest_recover` alone would resolve to right now, independent of
    // the wait/re-select machinery.
    let snapshots = state
        .account_cache
        .snapshots(&state.store)
        .await
        .unwrap();
    let mut snaps: Vec<_> = (*snapshots).clone();
    state.runtime.overlay(&mut snaps, now());
    let sel_ctx = SelectionCtx {
        now: now(),
        require_security_work_authorized: false,
        rng_seed: None,
        session_id: None,
        tier: None,
    };
    let wait_target = state
        .selector
        .soonest_recover(&snaps, &sel_ctx)
        .expect("both accounts are Cooldown-kind");
    assert_eq!(
        wait_target.account_id.as_str(),
        "a-acct",
        "sanity check: the ascending-id tie winner is the wait target"
    );

    let resp = responses_handler_impl_for_test_with_starvation_timing(
        state.clone(),
        None,
        json_headers(),
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
    assert!(body.contains("response.completed"));
    assert_eq!(
        exec.calls(),
        vec!["b-acct".to_string()],
        "RoundRobin's post-wait re-select picks the never-selected account (\"b-acct\"), NOT the \
         wait target (\"a-acct\") — the two genuinely differ"
    );

    let (backfill, _rx) = state.log_bus.subscribe();
    let starvation_events: Vec<_> = backfill.iter().filter(|e| e.kind == "starvation").collect();
    assert_eq!(starvation_events.len(), 1, "got: {backfill:?}");
    let ev = starvation_events[0];
    assert_eq!(
        ev.account.as_deref(),
        Some("b-acct"),
        "THE FIX: the log records the SPLICED account (b-acct), not the wait target (a-acct)"
    );
    assert!(
        ev.message.contains("wait_target=a-acct"),
        "the wait target is still recorded, distinctly: {}",
        ev.message
    );
    assert!(
        ev.message.contains("served=b-acct"),
        "the served account is the authoritative attribution: {}",
        ev.message
    );
    assert_ne!(
        ev.account.as_deref(),
        Some("a-acct"),
        "must NEVER be misattributed to the wait target when they differ"
    );
}

/// (3) A FAILED Layer-2 wait (budget exceeded, nothing ever served) still fires the signal — with
/// the matching failure reason code and `served_account: None` (never a fabricated id) — and still
/// bumps the metric, proving "a wait happened" is tracked regardless of outcome.
#[tokio::test]
async fn layer2_budget_exceeded_fires_signal_with_no_served_account() {
    let (store, cipher, _dir) = spawn_store().await;
    let mut a = account("A", "rate_limited");
    a.reset_at = Some(now() + 3600); // never recovers within the test's budget
    store
        .accounts()
        .insert(&a, &tokens("tokA"), &cipher)
        .await
        .unwrap();
    let exec = Arc::new(RecordingExecutor::default());
    let state = build_state(store, cipher, exec.clone(), Arc::new(CapacityWeighted));

    let resp = responses_handler_impl_for_test_with_starvation_timing(
        state.clone(),
        None,
        json_headers(),
        json_body(),
        3,
        Duration::from_millis(2000),
        Duration::from_millis(300),
    )
    .await;
    let (status, _body) = tokio::time::timeout(Duration::from_secs(8), collect_body(resp))
        .await
        .expect("draining the body must not hang");
    assert_eq!(status, 200);
    assert!(exec.calls().is_empty());

    assert_eq!(state.starvation_metrics.total(), 1);
    let (backfill, _rx) = state.log_bus.subscribe();
    let starvation_events: Vec<_> = backfill.iter().filter(|e| e.kind == "starvation").collect();
    assert_eq!(starvation_events.len(), 1);
    let ev = starvation_events[0];
    assert_eq!(
        ev.account.as_deref(),
        Some("A"),
        "falls back to the wait target when nothing was ever served"
    );
    assert!(ev
        .message
        .contains(StarvationOutcome::BudgetExceeded.code()));
    assert!(ev.message.contains("served=none"));
}
