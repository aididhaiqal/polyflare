//! B4 Task 3: the commit barrier's detection primitive, exercised through the PUBLIC API
//! (`execute_with_watchdog_tracked`/`execute_recovery_tracked` + `CommitWitness`).
//!
//! The commit barrier (`docs/superpowers/plans/2026-07-18-phase-b-failover.md`, Global
//! Constraints): the (not-yet-built, Task 4) failover loop may retry ONLY before the first
//! response byte reaches the client. Once ANY byte is relayed, a mid-stream failure must be
//! surfaced in-band and NEVER replayed on another account. This suite proves the detection:
//! - a pre-relay failure (executor `Err`, or a first-frame reject before any byte) reports
//!   `committed == false`;
//! - a mid-stream failure AFTER >= 1 relayed byte reports `committed == true`;
//! - a clean completion is unaffected (still observes/succeeds; the flag doesn't interfere).
//!
//! `execute_with_watchdog`/`execute_recovery` (the convenience delegators the 5 wedge suites
//! `wedge_regression`/`watchdog_race`/`no_anchor_failover`/`signal_client`/`failure_routing`, plus
//! `cyber_policy_detection.rs`, call directly) are UNTOUCHED in shape and behavior — they still
//! delegate to the `_tracked` siblings internally (now always passing `None` for C9 Task 2's
//! `InFlightGuard` too) — so those suites needed zero edits and stay green by construction (their
//! call sites never changed). ingress.rs itself, as of C9 Task 2, calls the `_tracked` siblings
//! directly at every streaming selection site (to thread an acquired lease into the returned
//! stream) — see `watchdog.rs`'s and `ingress.rs`'s own docs for that threading.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::{stream, StreamExt};
use polyflare_codex::CodexExecutor;
use polyflare_core::{
    Account, AccountId, Continuity, ContinuityDirective, ExecError, Executor, NoopContinuity,
    Prepared, PreparedRequest, RecoveryPlan, RequestCtx, ResponseStream, WatchdogArm,
};
use polyflare_server::watchdog::{execute_with_watchdog_tracked, CommitWitness, WatchdogError};
use polyflare_testkit::MockUpstream;

fn core_account(base_url: String) -> Account {
    Account {
        id: "acct".into(),
        base_url,
        bearer_token: "tok".into(),
        chatgpt_account_id: None,
        is_fedramp: false,
    }
}

fn disarmed(body: serde_json::Value) -> Prepared {
    Prepared {
        req: PreparedRequest {
            body: Some(body),
            model: "m".into(),
            forward_headers: vec![],
            raw_body: None,
        },
        directive: ContinuityDirective {
            pin_account: None,
            watchdog: WatchdogArm::Disarmed,
            recovery: RecoveryPlan::None,
            session_key: None,
            require_security_work_authorized: false,
        },
    }
}

/// Anchor present + full-resend => Armed + ResendFull(anchor stripped). Mirrors
/// `watchdog_race.rs`'s `armed_full_resend` helper (same fixture shape).
fn armed_full_resend(body: serde_json::Value) -> Prepared {
    let mut stripped = body.clone();
    stripped
        .as_object_mut()
        .unwrap()
        .remove("previous_response_id");
    Prepared {
        req: PreparedRequest {
            body: Some(body),
            model: "m".into(),
            forward_headers: vec![],
            raw_body: None,
        },
        directive: ContinuityDirective {
            pin_account: None,
            watchdog: WatchdogArm::Armed {
                timeout: Duration::from_millis(500),
            },
            recovery: RecoveryPlan::ResendFull {
                anchorless_req: PreparedRequest {
                    body: Some(stripped),
                    model: "m".into(),
                    forward_headers: vec![],
                    raw_body: None,
                },
            },
            session_key: None,
            require_security_work_authorized: false,
        },
    }
}

async fn drain(stream: ResponseStream) -> String {
    let mut body = String::new();
    let mut s = stream;
    while let Some(chunk) = s.next().await {
        body.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
    }
    body
}

/// A test-only `Executor` whose `execute()` call itself fails (as if the TCP connect / handshake
/// never succeeded) — mirrors `watchdog_race.rs`'s `hard_upstream_error_is_watchdog_upstream`
/// fixture (an unreachable base URL). No `ResponseStream` is ever constructed.
fn unreachable_account() -> Account {
    core_account("http://127.0.0.1:1".into())
}

/// A test-only `Executor` that always succeeds at `execute` but whose returned `ResponseStream`
/// yields a transport ERROR as its FIRST item — mirrors `watchdog_race.rs`'s `ErrorFirstExecutor`.
/// Used here under an ARMED request so the failure is caught by the initial peek, before any
/// client byte is relayed (the "first-frame reject" case, distinct from an executor-level `Err`).
struct ErrorFirstExecutor {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Executor for ErrorFirstExecutor {
    async fn execute(
        &self,
        _req: PreparedRequest,
        _account: &Account,
        _ctx: &RequestCtx,
    ) -> Result<ResponseStream, ExecError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(ResponseStream::new(stream::once(async {
            Err(ExecError::Stream("connection reset".into()))
        })))
    }
}

/// A test-only `Executor` whose returned stream yields ONE real byte, then a hard error — the
/// "served a partial response then failed" case. Used under a DISARMED request (no watchdog peek,
/// mirrors `cyber_policy_detection.rs`'s documented Disarmed-path boundary: the executor's stream
/// is wrapped immediately, so this is the SAME code path a real mid-flight upstream drop takes).
struct ByteThenErrorExecutor;

#[async_trait]
impl Executor for ByteThenErrorExecutor {
    async fn execute(
        &self,
        _req: PreparedRequest,
        _account: &Account,
        _ctx: &RequestCtx,
    ) -> Result<ResponseStream, ExecError> {
        let first = Ok::<bytes::Bytes, ExecError>(bytes::Bytes::from_static(
            b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n",
        ));
        let drop = Err(ExecError::Stream("mid-stream drop".into()));
        Ok(ResponseStream::new(stream::iter(vec![first, drop])))
    }
}

/// Step 1 (failing-first) — case 1: a pre-relay failure where the executor's `execute()` call
/// itself errors (no stream ever exists) reports `committed == false`.
#[tokio::test]
async fn executor_err_before_any_stream_is_not_committed() {
    let exec = CodexExecutor::new().unwrap();
    let cont: Arc<dyn Continuity> = Arc::new(NoopContinuity);
    let commit = CommitWitness::new();

    let res = execute_with_watchdog_tracked(
        &exec,
        cont,
        armed_full_resend(
            serde_json::json!({"previous_response_id": "resp_a", "input": [{"a":1}]}),
        ),
        &unreachable_account(),
        AccountId::from("acct"),
        RequestCtx::default(),
        Default::default(),
        Duration::ZERO, // idle_timeout: disabled, not under test here
        3,
        commit.clone(),
        None, // C9 Task 2: no in-flight lease under test here — commit-barrier only.
    )
    .await;

    match res {
        Err(WatchdogError::Upstream(_)) => {}
        Err(other) => panic!("expected WatchdogError::Upstream, got {other:?}"),
        Ok(_) => panic!("an unreachable account must not yield Ok(stream)"),
    }
    assert!(
        !commit.is_committed(),
        "an executor-level Err must never mark the commit witness"
    );
}

/// Step 1 (failing-first) — case 2: a first-frame reject caught by the Armed peek (a hard error
/// arriving as the FIRST stream item, before the peek decides to relay) also reports
/// `committed == false` — nothing was relayed even though a `ResponseStream` briefly existed.
#[tokio::test]
async fn first_frame_reject_before_relay_is_not_committed() {
    let calls = Arc::new(AtomicUsize::new(0));
    let stub = ErrorFirstExecutor {
        calls: calls.clone(),
    };
    let cont: Arc<dyn Continuity> = Arc::new(NoopContinuity);
    let commit = CommitWitness::new();

    let res = tokio::time::timeout(
        Duration::from_secs(3),
        execute_with_watchdog_tracked(
            &stub,
            cont,
            armed_full_resend(
                serde_json::json!({"previous_response_id": "resp_a", "input": [{"a":1}]}),
            ),
            &core_account("http://unused.invalid".into()),
            AccountId::from("acct"),
            RequestCtx::default(),
            Default::default(),
            Duration::ZERO, // idle_timeout: disabled, not under test here
            3,
            commit.clone(),
            None, // C9 Task 2: no in-flight lease under test here — commit-barrier only.
        ),
    )
    .await
    .expect("bounded: a first-frame hard error must not hang");

    match res {
        Err(WatchdogError::Upstream(_)) => {}
        Err(other) => panic!("expected WatchdogError::Upstream, got {other:?}"),
        Ok(_) => panic!("a first-frame reject must not relay a stream to the caller"),
    }
    assert!(
        !commit.is_committed(),
        "a first-frame reject (before any byte relayed) must not mark the commit witness"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "sanity: exactly one execute() call, no recovery re-invoked it"
    );
}

/// Step 1 (failing-first) — case 3: a mid-stream failure AFTER >= 1 relayed byte reports
/// `committed == true`. This is the one case a `WatchdogError` can NEVER represent (see
/// `CommitWitness`'s doc in `watchdog.rs`) — the failure surfaces as an `Err` item INSIDE the
/// already-`Ok` stream, not as this function's `Result`. So the assertion is made on the SAME
/// witness clone, read AFTER draining the (successfully-returned) stream to its error.
#[tokio::test]
async fn mid_stream_failure_after_a_relayed_byte_is_committed() {
    let exec = ByteThenErrorExecutor;
    let cont: Arc<dyn Continuity> = Arc::new(NoopContinuity);
    let commit = CommitWitness::new();

    let stream = execute_with_watchdog_tracked(
        &exec,
        cont,
        disarmed(serde_json::json!({"input": [{"a":1}]})),
        &core_account("http://unused.invalid".into()),
        AccountId::from("acct"),
        RequestCtx::default(),
        Default::default(),
        Duration::ZERO, // idle_timeout: disabled, not under test here
        3,
        commit.clone(),
        None, // C9 Task 2: no in-flight lease under test here — commit-barrier only.
    )
    .await
    .expect("Disarmed relays immediately: Ok(stream), not a WatchdogError");

    assert!(
        !commit.is_committed(),
        "nothing has been polled yet right after the Ok(stream) return"
    );

    let mut s = stream;
    let first = s.next().await;
    assert!(
        matches!(first, Some(Ok(_))),
        "the first chunk relays cleanly"
    );
    assert!(
        commit.is_committed(),
        "committed the instant the first byte was yielded to the poller"
    );

    let second = s.next().await;
    assert!(
        matches!(second, Some(Err(_))),
        "the mid-stream drop is still forwarded, unchanged, to whoever polls the stream"
    );
    assert!(
        commit.is_committed(),
        "committed must still read true after the mid-stream failure — this is exactly the \
         signal Task 4 needs: never retry past this point"
    );
}

/// Regression: a clean `response.completed` completion is unaffected by the commit-tracking
/// addition — the stream still relays every byte, still ends with `response.completed`, and the
/// witness (correctly) ends up committed too (real bytes DID reach the client) without any of
/// that interfering with the success path.
#[tokio::test]
async fn clean_completion_still_observes_and_the_flag_does_not_interfere() {
    let mock = MockUpstream::with_ids(vec![
        r#"{"type":"response.output_text.delta","delta":"hi"}"#.to_string(),
    ]);
    let handle = mock.clone();
    let base = mock.spawn().await;
    let exec = CodexExecutor::new().unwrap();
    let cont: Arc<dyn Continuity> = Arc::new(NoopContinuity);
    let commit = CommitWitness::new();

    let prepared = disarmed(serde_json::json!({"input": [{"a":1},{"b":2}]}));
    let stream = execute_with_watchdog_tracked(
        &exec,
        cont,
        prepared,
        &core_account(base),
        AccountId::from("acct"),
        RequestCtx::default(),
        Default::default(),
        Duration::ZERO, // idle_timeout: disabled, not under test here
        3,
        commit.clone(),
        None, // C9 Task 2: no in-flight lease under test here — commit-barrier only.
    )
    .await
    .unwrap();

    let body = drain(stream).await;
    assert!(
        body.contains("response.completed"),
        "the clean-completion outcome is unaffected: {body}"
    );
    assert_eq!(handle.request_count(), 1, "no recovery/retry needed");
    assert!(
        commit.is_committed(),
        "a real completed turn did relay bytes, so the witness correctly reports committed"
    );
}
