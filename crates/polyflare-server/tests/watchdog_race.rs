//! Unit/integration tests for the watchdog first-byte race, driving `execute_with_watchdog`
//! directly against a MockUpstream + CodexExecutor with a tiny N.

use std::sync::Arc;

use futures_util::StreamExt;
use polyflare_codex::CodexExecutor;
use polyflare_core::{
    Account, AccountId, Continuity, ContinuityDirective, NoopContinuity, Prepared, PreparedRequest,
    RecoveryPlan, RequestCtx, WatchdogArm,
};
use polyflare_server::watchdog::{execute_with_watchdog, WatchdogError};
use polyflare_testkit::MockUpstream;
use std::time::Duration;

fn core_account(base_url: String) -> Account {
    Account {
        id: "acct".into(),
        base_url,
        bearer_token: "tok".into(),
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

fn armed_full_resend(body: serde_json::Value) -> Prepared {
    // Anchor present + full-resend => Armed + ResendFull(anchor stripped).
    let mut stripped = body.clone();
    stripped
        .as_object_mut()
        .unwrap()
        .remove("previous_response_id");
    Prepared {
        req: PreparedRequest {
            body,
            model: "m".into(),
        },
        directive: ContinuityDirective {
            pin_account: None,
            watchdog: WatchdogArm::Armed {
                timeout: Duration::from_millis(150),
            },
            recovery: RecoveryPlan::ResendFull {
                anchorless_req: PreparedRequest {
                    body: stripped,
                    model: "m".into(),
                },
            },
            session_key: None,
        },
    }
}

#[tokio::test]
async fn relays_when_first_byte_arrives_before_timeout() {
    let mock = MockUpstream::with_ids(vec![
        r#"{"type":"response.output_text.delta","delta":"hi"}"#.to_string(),
    ]);
    let handle = mock.clone();
    let base = mock.spawn().await;
    let exec = CodexExecutor::new().unwrap();
    let cont: Arc<dyn Continuity> = Arc::new(NoopContinuity);

    // Anchor present but the mock (with_ids, not silent) responds promptly => alive => relay.
    let prepared = armed_full_resend(
        serde_json::json!({"previous_response_id": "resp_a", "input": [{"a":1},{"b":2}]}),
    );
    let stream = execute_with_watchdog(
        &exec,
        cont,
        prepared,
        &core_account(base),
        AccountId::from("acct"),
        RequestCtx::default(),
    )
    .await
    .unwrap();
    let body = drain(stream).await;
    assert!(body.contains("response.completed"));
    assert_eq!(handle.request_count(), 1, "no recovery needed");
}

#[tokio::test]
async fn recovers_on_silence_via_resend_full() {
    let mock = MockUpstream::silent_on_anchor(vec![
        r#"{"type":"response.output_text.delta","delta":"recovered"}"#.to_string(),
    ]);
    let handle = mock.clone();
    let base = mock.spawn().await;
    let exec = CodexExecutor::new().unwrap();
    let cont: Arc<dyn Continuity> = Arc::new(NoopContinuity);

    let orig = serde_json::json!({"previous_response_id": "resp_dead", "input": [{"a":1},{"b":2}]});
    let prepared = armed_full_resend(orig.clone());
    let stream = execute_with_watchdog(
        &exec,
        cont,
        prepared,
        &core_account(base),
        AccountId::from("acct"),
        RequestCtx::default(),
    )
    .await
    .unwrap();

    let done = tokio::time::timeout(Duration::from_secs(3), drain(stream))
        .await
        .expect("bounded");
    assert!(
        done.contains("response.completed"),
        "recovery stream completed"
    );
    assert_eq!(handle.request_count(), 2, "silent attempt + recovery");
    let bodies = handle.bodies();
    assert!(
        bodies[0].get("previous_response_id").is_some(),
        "1st carried the dead anchor"
    );
    assert!(
        bodies[1].get("previous_response_id").is_none(),
        "recovery stripped the anchor"
    );
    // R1: the recovery's input equals the client's input (never trimmed).
    assert_eq!(bodies[1]["input"], orig["input"], "full-resend not trimmed");
}

#[tokio::test]
async fn hard_upstream_error_is_watchdog_upstream() {
    let exec = CodexExecutor::new().unwrap();
    let cont: Arc<dyn Continuity> = Arc::new(NoopContinuity);
    // Unreachable upstream => execute() errors before any stream.
    let prepared = armed_full_resend(
        serde_json::json!({"previous_response_id": "resp_a", "input": [{"a":1},{"b":2}]}),
    );
    let res = execute_with_watchdog(
        &exec,
        cont,
        prepared,
        &core_account("http://127.0.0.1:1".into()),
        AccountId::from("acct"),
        RequestCtx::default(),
    )
    .await;
    assert!(matches!(res, Err(WatchdogError::Upstream)));
}

#[tokio::test]
async fn mid_race_transport_error_is_watchdog_upstream_and_does_not_recover() {
    // The C5 review flagged `execute_with_watchdog`'s `Ok(Some(Err(_)))` branch (a transport error
    // arriving as the FIRST stream item, mid-race — i.e. AFTER the upstream accepted the request
    // with 200 headers, unlike `hard_upstream_error_is_watchdog_upstream` above which never gets a
    // stream at all) as correct-but-untested. Per SPEC-M3 §Q4 this is a hard error: it must NOT
    // recover (no second/resend request) and must NOT relay (no stream handed to the caller).
    let mock = MockUpstream::error_first_on_anchor(vec![
        r#"{"type":"response.output_text.delta","delta":"unreachable"}"#.to_string(),
    ]);
    let handle = mock.clone();
    let base = mock.spawn().await;
    let exec = CodexExecutor::new().unwrap();
    let cont: Arc<dyn Continuity> = Arc::new(NoopContinuity);

    let prepared = armed_full_resend(
        serde_json::json!({"previous_response_id": "resp_a", "input": [{"a":1},{"b":2}]}),
    );
    let res = tokio::time::timeout(
        Duration::from_secs(3),
        execute_with_watchdog(
            &exec,
            cont,
            prepared,
            &core_account(base),
            AccountId::from("acct"),
            RequestCtx::default(),
        ),
    )
    .await
    .expect("bounded: a mid-race hard error must not hang");

    match res {
        Err(WatchdogError::Upstream) => {}
        Err(other) => panic!("expected WatchdogError::Upstream, got {other:?}"),
        Ok(_) => panic!("mid-race hard error must not relay a stream to the caller"),
    }
    assert_eq!(
        handle.request_count(),
        1,
        "mid-race hard error must NOT recover/retry"
    );
}
