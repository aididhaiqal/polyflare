//! Unit/integration tests for the watchdog first-byte race, driving `execute_with_watchdog`
//! directly against a MockUpstream + CodexExecutor with a tiny N.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::{stream, StreamExt};
use polyflare_codex::CodexExecutor;
use polyflare_core::{
    Account, AccountId, Continuity, ContinuityDirective, ExecError, Executor, NoopContinuity,
    Prepared, PreparedRequest, RecoveryPlan, RequestCtx, ResponseStream, WatchdogArm,
};
use polyflare_server::watchdog::{execute_with_watchdog, WatchdogError};
use polyflare_testkit::MockUpstream;
use std::time::Duration;

/// A test-only `Executor` that always succeeds at `execute` (as if the upstream accepted the
/// request with 200 headers) but whose returned `ResponseStream` yields a transport ERROR as its
/// FIRST item. This is the ONLY way to reach `execute_with_watchdog`'s `Ok(Some(Err(_)))` arm: the
/// real reqwest/HTTP path collapses an immediately-erroring body into a `.send()` failure (an
/// `execute()` Err before any stream exists), so it can never produce a first-item stream error.
/// The counter proves whether recovery re-invoked `execute` (it must NOT for a mid-race hard error).
struct ErrorFirstExecutor {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Executor for ErrorFirstExecutor {
    async fn execute(
        &self,
        _req: PreparedRequest,
        _account: &Account,
    ) -> Result<ResponseStream, ExecError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Box::pin(stream::once(async {
            Err(ExecError::Stream("connection reset".into()))
        })))
    }
}

fn core_account(base_url: String) -> Account {
    Account {
        id: "acct".into(),
        base_url,
        bearer_token: "tok".into(),
        chatgpt_account_id: None,
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
            body: Some(body),
            model: "m".into(),
            forward_headers: vec![],
            raw_body: None,
        },
        directive: ContinuityDirective {
            pin_account: None,
            watchdog: WatchdogArm::Armed {
                timeout: Duration::from_millis(150),
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
        Default::default(),
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
        Default::default(),
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
        Default::default(),
    )
    .await;
    assert!(matches!(res, Err(WatchdogError::Upstream(_))));
}

#[tokio::test]
async fn mid_race_first_item_error_is_watchdog_upstream_and_does_not_recover() {
    // The C5 review flagged `execute_with_watchdog`'s `Ok(Some(Err(_)))` branch (a transport error
    // arriving as the FIRST stream item, mid-race — i.e. AFTER a successful `execute()`, unlike
    // `hard_upstream_error_is_watchdog_upstream` above where `execute()` itself errors and no stream
    // exists) as correct-but-untested. This branch is unreachable through the reqwest/HTTP path (an
    // immediately-erroring body collapses into a `.send()` failure), so we drive it at the unit
    // level with a stub `Executor` that returns Ok(stream) whose first item is Err. Per SPEC-M3 §Q4
    // this is a hard error: it must return `Err(Upstream)`, must NOT relay a stream, and must NOT
    // recover — proven by the stub's `execute` being called EXACTLY ONCE (recovery would call it a
    // second time).
    let calls = Arc::new(AtomicUsize::new(0));
    let stub = ErrorFirstExecutor {
        calls: calls.clone(),
    };
    let cont: Arc<dyn Continuity> = Arc::new(NoopContinuity);

    let prepared = armed_full_resend(
        serde_json::json!({"previous_response_id": "resp_a", "input": [{"a":1},{"b":2}]}),
    );
    let res = tokio::time::timeout(
        Duration::from_secs(3),
        execute_with_watchdog(
            &stub,
            cont,
            prepared,
            &core_account("http://unused.invalid".into()),
            AccountId::from("acct"),
            RequestCtx::default(),
            Default::default(),
        ),
    )
    .await
    .expect("bounded: a mid-race hard error must not hang");

    match res {
        Err(WatchdogError::Upstream(_)) => {}
        Err(other) => panic!("expected WatchdogError::Upstream, got {other:?}"),
        Ok(_) => panic!("mid-race first-item error must not relay a stream to the caller"),
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "mid-race hard error must NOT recover/retry (execute called exactly once)"
    );
}
