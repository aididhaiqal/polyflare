//! TA6(b) Task 1: a `cyber_policy` rejection arrives as a `response.failed` SSE frame on a 200-OK
//! stream (codex-rs wire truth: `codex-api/src/sse/responses.rs` `is_cyber_policy_error` —
//! `error.code == "cyber_policy"`). `execute_with_watchdog`'s Armed branch already peeks the FIRST
//! upstream chunk before deciding to relay (the same seam the wedge fix's "ALIVE" check uses); this
//! suite proves that peek now also recognizes a cyber-policy terminal frame there and surfaces a
//! distinct, content-safe `WatchdogError::CapabilityRejection` instead of relaying it — WITHOUT
//! rerouting (that's Task 2) and WITHOUT touching any other outcome.
//!
//! Companion suites `wedge_regression`/`watchdog_race`/`no_anchor_failover`/`signal_client`/
//! `failure_routing` must stay green — this file adds new coverage, it does not replace them.

use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use polyflare_codex::CodexExecutor;
use polyflare_core::{
    Continuity, ContinuityDirective, NoopContinuity, Prepared, PreparedRequest, RecoveryPlan,
    RequestCtx, WatchdogArm,
};
use polyflare_server::watchdog::{execute_with_watchdog, WatchdogError};
use polyflare_testkit::MockUpstream;

const SENTINEL_MESSAGE: &str = "TOTALLY-SECRET-CONTENT-DO-NOT-LEAK-6f3a9c";

fn cyber_policy_frame(message: &str) -> String {
    format!(
        r#"{{"type":"response.failed","response":{{"id":"resp_fatal_cyber","status":"failed","error":{{"code":"cyber_policy","message":"{message}"}}}}}}"#
    )
}

fn non_cyber_failed_frame() -> String {
    r#"{"type":"response.failed","response":{"id":"resp_fatal_x","status":"failed","error":{"code":"server_is_overloaded","message":"try later"}}}"#.to_string()
}

fn created_frame(id: &str) -> String {
    format!(r#"{{"type":"response.created","response":{{"id":"{id}","status":"in_progress"}}}}"#)
}

fn completed_frame(id: &str) -> String {
    format!(r#"{{"type":"response.completed","response":{{"id":"{id}"}}}}"#)
}

fn content_delta_frame(delta: &str) -> String {
    format!(r#"{{"type":"response.output_text.delta","delta":"{delta}"}}"#)
}

fn core_account(base_url: String) -> polyflare_core::Account {
    polyflare_core::Account {
        id: "acct".into(),
        base_url,
        bearer_token: "tok".into(),
        chatgpt_account_id: None,
    }
}

/// Anchor present + full-resend => Armed + ResendFull(anchor stripped). Mirrors
/// `watchdog_race.rs`'s `armed_full_resend` helper exactly (same fixture shape).
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
        },
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
        },
    }
}

async fn drain(stream: polyflare_core::ResponseStream) -> String {
    let mut body = String::new();
    let mut s = stream;
    while let Some(chunk) = s.next().await {
        body.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
    }
    body
}

/// THE CRUX: a `cyber_policy` `response.failed` arriving as the very first upstream frame on an
/// Armed (anchor-bearing) request must be detected during the existing peek-before-relay and
/// surfaced as `WatchdogError::CapabilityRejection` — not relayed, not treated as "alive".
#[tokio::test]
async fn armed_first_frame_cyber_policy_is_detected_before_relay() {
    let mock = MockUpstream::new(vec![cyber_policy_frame(SENTINEL_MESSAGE)]);
    let handle = mock.clone();
    let base = mock.spawn().await;
    let exec = CodexExecutor::new().unwrap();
    let cont: Arc<dyn Continuity> = Arc::new(NoopContinuity);

    let prepared = armed_full_resend(
        serde_json::json!({"previous_response_id": "resp_a", "input": [{"a":1}]}),
    );
    let res = execute_with_watchdog(
        &exec,
        cont,
        prepared,
        &core_account(base),
        polyflare_core::AccountId::from("acct"),
        RequestCtx::default(),
        Default::default(),
    )
    .await;

    match res {
        Err(WatchdogError::CapabilityRejection { capability }) => {
            assert_eq!(capability, "security_work");
        }
        Err(other) => panic!("expected CapabilityRejection, got {other:?}"),
        Ok(_) => panic!("a first-frame cyber_policy rejection must NOT relay a stream"),
    }
    // No reroute/retry in this task: exactly one upstream attempt.
    assert_eq!(handle.request_count(), 1, "detect-only: no reselect/retry");
}

/// Content-safety: the frame's `message` must never appear in the surfaced error's `Display` or
/// `Debug` output (the only places a caller could log/print it).
#[tokio::test]
async fn cyber_policy_message_never_leaks_into_the_signal() {
    let mock = MockUpstream::new(vec![cyber_policy_frame(SENTINEL_MESSAGE)]);
    let base = mock.spawn().await;
    let exec = CodexExecutor::new().unwrap();
    let cont: Arc<dyn Continuity> = Arc::new(NoopContinuity);

    let prepared = armed_full_resend(
        serde_json::json!({"previous_response_id": "resp_a", "input": [{"a":1}]}),
    );
    let res = execute_with_watchdog(
        &exec,
        cont,
        prepared,
        &core_account(base),
        polyflare_core::AccountId::from("acct"),
        RequestCtx::default(),
        Default::default(),
    )
    .await;
    let err = match res {
        Err(e) => e,
        Ok(_) => panic!("cyber_policy first frame must error, not relay"),
    };

    let display = err.to_string();
    let debug = format!("{err:?}");
    assert!(
        !display.contains(SENTINEL_MESSAGE),
        "Display leaked the frame message: {display}"
    );
    assert!(
        !debug.contains(SENTINEL_MESSAGE),
        "Debug leaked the frame message: {debug}"
    );
}

/// Regression: a NON-cyber `response.failed` as the first frame must behave EXACTLY as before —
/// treated as "alive", rebuilt, and relayed untouched (no `CapabilityRejection`).
#[tokio::test]
async fn non_cyber_response_failed_is_unaffected() {
    let mock = MockUpstream::new(vec![non_cyber_failed_frame()]);
    let handle = mock.clone();
    let base = mock.spawn().await;
    let exec = CodexExecutor::new().unwrap();
    let cont: Arc<dyn Continuity> = Arc::new(NoopContinuity);

    let prepared = armed_full_resend(
        serde_json::json!({"previous_response_id": "resp_a", "input": [{"a":1}]}),
    );
    let stream = execute_with_watchdog(
        &exec,
        cont,
        prepared,
        &core_account(base),
        polyflare_core::AccountId::from("acct"),
        RequestCtx::default(),
        Default::default(),
    )
    .await
    .expect("a non-cyber response.failed must relay exactly as before");

    let body = drain(stream).await;
    assert!(
        body.contains("server_is_overloaded"),
        "non-cyber failure frame relayed untouched: {body}"
    );
    assert_eq!(handle.request_count(), 1);
}

/// Peek-before-relay boundary: if content was ALREADY relayed (a normal frame arrived first, then
/// a LATER chunk carries the cyber_policy failure), Task 1 must fall back to today's pass-through —
/// never double-relay, never swallow content the client already saw. Only the very first chunk is
/// ever inspected; a rejection arriving later streams through untouched, same as any other content.
#[tokio::test]
async fn cyber_policy_after_content_already_relayed_falls_back_to_pass_through() {
    let gap = Duration::from_millis(150);
    let mock = MockUpstream::chunked_with_gap(
        r#"{"type":"response.output_text.delta","delta":"already sent"}"#,
        vec![cyber_policy_frame(SENTINEL_MESSAGE)],
        gap,
    );
    let base = mock.spawn().await;
    let exec = CodexExecutor::new().unwrap();
    let cont: Arc<dyn Continuity> = Arc::new(NoopContinuity);

    // Armed timeout is comfortably longer than the injected gap so the race sees the first
    // (non-failing) chunk as "alive" well before the gap elapses, exactly like
    // `watchdog_race::relays_when_first_byte_arrives_before_timeout`.
    let prepared = armed_full_resend(
        serde_json::json!({"previous_response_id": "resp_a", "input": [{"a":1}]}),
    );
    let stream = execute_with_watchdog(
        &exec,
        cont,
        prepared,
        &core_account(base),
        polyflare_core::AccountId::from("acct"),
        RequestCtx::default(),
        Default::default(),
    )
    .await
    .expect("first chunk is plain content => alive => relay, no CapabilityRejection");

    let body = tokio::time::timeout(Duration::from_secs(3), drain(stream))
        .await
        .expect("bounded");
    assert!(
        body.contains("already sent"),
        "the already-relayed content made it through: {body}"
    );
    assert!(
        body.contains("cyber_policy"),
        "the later cyber_policy frame is passed through untouched (fallback), not swallowed: {body}"
    );
}

/// THE REGRESSION (empirically proven bug): real Codex wire ordering emits `response.created`
/// FIRST, then `response.failed(cyber_policy)` on a LATER chunk. The old code only inspected the
/// very FIRST peeked chunk, so `response.created` alone judged the stream "ALIVE", relayed it, and
/// never looked at the later cyber frame. `chunked_with_gap` guarantees the two frames arrive as
/// SEPARATE reads (the gap is a real `tokio::time::sleep` between them) so this isn't a coalescing
/// fluke like the single-frame `MockUpstream::new` fixture that hid the bug.
#[tokio::test]
async fn armed_created_then_later_cyber_policy_is_detected_before_relay() {
    let gap = Duration::from_millis(150);
    let mock = MockUpstream::chunked_with_gap(
        created_frame("resp_fatal_cyber"),
        vec![cyber_policy_frame(SENTINEL_MESSAGE)],
        gap,
    );
    let handle = mock.clone();
    let base = mock.spawn().await;
    let exec = CodexExecutor::new().unwrap();
    let cont: Arc<dyn Continuity> = Arc::new(NoopContinuity);

    let prepared = armed_full_resend(
        serde_json::json!({"previous_response_id": "resp_a", "input": [{"a":1}]}),
    );
    let res = tokio::time::timeout(
        Duration::from_secs(3),
        execute_with_watchdog(
            &exec,
            cont,
            prepared,
            &core_account(base),
            polyflare_core::AccountId::from("acct"),
            RequestCtx::default(),
            Default::default(),
        ),
    )
    .await
    .expect("bounded");

    match res {
        Err(WatchdogError::CapabilityRejection { capability }) => {
            assert_eq!(capability, "security_work");
        }
        Err(other) => panic!("expected CapabilityRejection, got {other:?}"),
        Ok(stream) => {
            let body = drain(stream).await;
            panic!("created-then-cyber (across chunks) must NOT relay a stream, got body: {body}");
        }
    }
    assert_eq!(handle.request_count(), 1, "detect-only: no reselect/retry");
}

/// Normal-turn regression: `created -> content delta -> completed` must relay EVERY frame, in
/// order, with nothing dropped or delayed-to-loss — proving the buffering added to scan past
/// lifecycle frames does not swallow them, hold the stream open past the first content frame, or
/// reorder anything. `Continuity::observe(Completed)` still fires exactly as before (asserted via
/// `NoopContinuity` not panicking / the stream completing cleanly).
#[tokio::test]
async fn armed_normal_stream_relays_every_frame_in_order() {
    let gap = Duration::from_millis(150);
    let mock = MockUpstream::chunked_with_gap(
        created_frame("resp_ok"),
        vec![content_delta_frame("hello"), completed_frame("resp_ok")],
        gap,
    );
    let base = mock.spawn().await;
    let exec = CodexExecutor::new().unwrap();
    let cont: Arc<dyn Continuity> = Arc::new(NoopContinuity);

    let prepared = armed_full_resend(
        serde_json::json!({"previous_response_id": "resp_a", "input": [{"a":1}]}),
    );
    let stream = execute_with_watchdog(
        &exec,
        cont,
        prepared,
        &core_account(base),
        polyflare_core::AccountId::from("acct"),
        RequestCtx::default(),
        Default::default(),
    )
    .await
    .expect("a normal created->content->completed turn must relay, not reject");

    let body = tokio::time::timeout(Duration::from_secs(3), drain(stream))
        .await
        .expect("bounded");

    let created_pos = body
        .find("response.created")
        .expect("created frame relayed");
    let delta_pos = body
        .find("response.output_text.delta")
        .expect("content delta relayed");
    let completed_pos = body
        .find("response.completed")
        .expect("completed frame relayed");
    assert!(
        created_pos < delta_pos && delta_pos < completed_pos,
        "frames must relay in order: {body}"
    );
    assert!(body.contains("\"hello\""), "delta payload relayed: {body}");
}

/// THE RE-REVIEW FINDING: the scan loop added to buffer past lifecycle frames did naked, un-timed
/// `stream.next().await` reads. If upstream sends `response.created` (satisfies the first-chunk
/// peek) then goes SILENT before any decisive frame, the scan parked forever — no HTTP status ever
/// sent to the client, holding the handler task + upstream socket with zero self-healing. Because
/// peek-before-relay holds across the WHOLE scan window (nothing has been relayed while scanning),
/// a scan-silence is exactly as recoverable as a first-chunk silence: drop the stream and route into
/// the SAME `ResendFull`/`SignalClient` recovery. This test proves the fix: bounded completion (via
/// `ResendFull` recovery — a second, anchor-stripped request), not an indefinite hang.
#[tokio::test]
async fn armed_created_then_silence_during_scan_recovers_via_resend() {
    let mock = MockUpstream::stall_after_first(created_frame("resp_stall"));
    let handle = mock.clone();
    let base = mock.spawn().await;
    let exec = CodexExecutor::new().unwrap();
    let cont: Arc<dyn Continuity> = Arc::new(NoopContinuity);

    let prepared = armed_full_resend(
        serde_json::json!({"previous_response_id": "resp_a", "input": [{"a":1}]}),
    );
    let res = tokio::time::timeout(
        Duration::from_secs(3),
        execute_with_watchdog(
            &exec,
            cont,
            prepared,
            &core_account(base),
            polyflare_core::AccountId::from("acct"),
            RequestCtx::default(),
            Default::default(),
        ),
    )
    .await
    .expect("scan-silence must recover within the bound (~timeout), not hang forever");

    let _stream =
        res.expect("post-created silence during the scan must recover (Ok(stream)), not error");
    assert_eq!(
        handle.request_count(),
        2,
        "post-created scan-silence must trigger ResendFull recovery: a 2nd upstream request"
    );
    let bodies = handle.bodies();
    assert!(
        bodies[1].get("previous_response_id").is_none(),
        "the recovery request strips the anchor: {:?}",
        bodies[1]
    );
}

/// Documents the Disarmed-path boundary explicitly: no anchor => `execute_with_watchdog` does not
/// peek before returning (unchanged from today), so a cyber_policy frame there is relayed
/// untouched — this task only adds detection to the Armed peek. A later task's proactive/sticky
/// pre-filter (TA6a/TA6b Task 3/5) is what closes this gap, not a reactive peek here.
#[tokio::test]
async fn disarmed_cyber_policy_is_unaffected_by_this_task() {
    let mock = MockUpstream::new(vec![cyber_policy_frame(SENTINEL_MESSAGE)]);
    let base = mock.spawn().await;
    let exec = CodexExecutor::new().unwrap();
    let cont: Arc<dyn Continuity> = Arc::new(NoopContinuity);

    let prepared = disarmed(serde_json::json!({"input": [{"a":1}]}));
    let stream = execute_with_watchdog(
        &exec,
        cont,
        prepared,
        &core_account(base),
        polyflare_core::AccountId::from("acct"),
        RequestCtx::default(),
        Default::default(),
    )
    .await
    .expect("Disarmed is untouched by this task: no peek, no CapabilityRejection");

    let body = drain(stream).await;
    assert!(body.contains("cyber_policy"), "relayed untouched: {body}");
}
