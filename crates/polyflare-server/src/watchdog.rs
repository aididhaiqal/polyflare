//! The silence watchdog + ownership pre-filter. `execute_with_watchdog` races the FIRST upstream
//! chunk against N; on silence it drops the dead stream (cancel-safe) and recovers. Peek-before-
//! relay: no client byte is written until the first upstream chunk arrives, so a restart is always
//! safe. The Codex executor is untouched — this wraps it in the server.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_core::Stream;
use futures_util::stream::{self, StreamExt};
use polyflare_core::{
    Account, AccountId, AccountSnapshot, Continuity, ContinuityDirective, ExecError, Executor,
    Prepared, PreparedRequest, RecoveryPlan, RequestCtx, ResponseStream, SelectionCtx, Selector,
    SessionKey, TurnOutcome, WatchdogArm,
};

use crate::session_key::sha256_hex;

/// VERIFY-at-implementation (SPEC-M3 risk 7): capture the exact `previous_response_not_found` shape
/// the real Codex CLI / Claude Code self-heals from (codex-lb masking behavior or a live capture)
/// and finalize this payload. Tests assert on the code substring only, so they survive the change.
const SIGNAL_SSE: &[u8] = concat!(
    "event: response.failed\n",
    "data: {\"type\":\"response.failed\",\"response\":{\"error\":",
    "{\"code\":\"previous_response_not_found\",\"message\":\"anchor not resumable; resend full history\"}}}\n\n",
)
.as_bytes();

/// Where a request should go after ownership resolution.
pub enum RouteDecision {
    /// Execute normally on this account (owner eligible, or unowned pick).
    Route(AccountId),
    /// Owner is pinned but ineligible ⇒ recover (never a hard fail).
    Recover,
    /// Unowned and the pool is empty ⇒ 503.
    NoEligibleAccount,
}

/// Errors the watchdog surfaces. Generic — never leaks a token, URL, or internal `Display`.
#[derive(Debug, thiserror::Error)]
pub enum WatchdogError {
    #[error("upstream error")]
    Upstream,
    #[error("continuity recovery unavailable")]
    Continuity,
}

/// HARD ownership pre-filter: narrow candidates to the pinned owner BEFORE `Selector::pick` (no
/// Selector-trait change). Owner ineligible ⇒ `Recover`; unowned + empty ⇒ `NoEligibleAccount`.
pub fn apply_ownership(
    directive: &ContinuityDirective,
    candidates: &[AccountSnapshot],
    selector: &dyn Selector,
    ctx: &SelectionCtx,
) -> RouteDecision {
    match directive.pin_account.as_ref() {
        Some(owner) => {
            let narrowed: Vec<AccountSnapshot> = candidates
                .iter()
                .filter(|s| &s.id == owner)
                .cloned()
                .collect();
            match selector.pick(&narrowed, ctx) {
                Some(id) => RouteDecision::Route(id),
                None => RouteDecision::Recover,
            }
        }
        None => match selector.pick(candidates, ctx) {
            Some(id) => RouteDecision::Route(id),
            None => RouteDecision::NoEligibleAccount,
        },
    }
}

/// Diagnostic input fingerprint (sha256 hex of the `input` JSON) + item count. Not used to gate a
/// trim in M3-core (we never trim) — recorded by `observe` for diagnostics only.
fn input_fingerprint_and_count(body: &serde_json::Value) -> (String, u32) {
    let input = body.get("input");
    let count = match input {
        Some(serde_json::Value::Array(a)) => a.len() as u32,
        Some(_) => 1,
        None => 0,
    };
    let canon = input.map(|v| v.to_string()).unwrap_or_default();
    (sha256_hex(canon.as_bytes()), count)
}

/// Execute a prepared request under the watchdog. Disarmed (no anchor) ⇒ relay + sniff + observe
/// Completed. Armed ⇒ race the first byte: alive ⇒ rebuild + relay; hard error ⇒ observe Failed +
/// `Upstream`; silence/empty ⇒ drop the dead stream and recover per the directive.
pub async fn execute_with_watchdog(
    executor: &dyn Executor,
    continuity: Arc<dyn Continuity>,
    prepared: Prepared,
    account: &Account,
    account_id: AccountId,
    ctx: RequestCtx,
) -> Result<ResponseStream, WatchdogError> {
    let Prepared { req, directive } = prepared;
    let session_key = directive.session_key.clone();
    let (fp, count) = input_fingerprint_and_count(&req.body);

    match directive.watchdog {
        WatchdogArm::Disarmed => {
            // No anchor ⇒ cannot be silent. Relay + sniff + observe(Completed).
            let stream = executor
                .execute(req, account)
                .await
                .map_err(|_| WatchdogError::Upstream)?;
            Ok(wrap_stream(
                stream,
                continuity,
                ctx,
                account_id,
                session_key,
                OutcomeKind::Completed { fp, count },
            ))
        }
        WatchdogArm::Armed { timeout } => {
            let mut stream = executor
                .execute(req, account)
                .await
                .map_err(|_| WatchdogError::Upstream)?;
            match tokio::time::timeout(timeout, stream.next()).await {
                Ok(Some(Ok(first))) => {
                    // ALIVE: rebuild the full stream (peek-before-relay) + sniff + observe(Completed).
                    let rebuilt: ResponseStream = Box::pin(
                        stream::once(async move { Ok::<Bytes, ExecError>(first) }).chain(stream),
                    );
                    Ok(wrap_stream(
                        rebuilt,
                        continuity,
                        ctx,
                        account_id,
                        session_key,
                        OutcomeKind::Completed { fp, count },
                    ))
                }
                Ok(Some(Err(_))) => {
                    // Hard upstream error before any client byte ⇒ observe(Failed) + 502.
                    let _ = continuity
                        .observe(
                            TurnOutcome::Failed {
                                session_key: session_key.clone(),
                            },
                            &ctx,
                        )
                        .await;
                    Err(WatchdogError::Upstream)
                }
                Ok(None) | Err(_) => {
                    // Ok(None): upstream closed with zero events on an anchored req == dead anchor.
                    // Err(_): the N timeout elapsed == the wedge. Both ⇒ RECOVER. Drop = cancel.
                    drop(stream);
                    match directive.recovery {
                        RecoveryPlan::ResendFull { anchorless_req } => {
                            execute_recovery(
                                executor,
                                continuity,
                                anchorless_req,
                                account,
                                account_id,
                                ctx,
                                session_key,
                            )
                            .await
                        }
                        RecoveryPlan::SignalClient => {
                            Ok(
                                signal_client_stream(continuity, ctx, account_id, session_key)
                                    .await,
                            )
                        }
                        RecoveryPlan::None => Err(WatchdogError::Continuity),
                    }
                }
            }
        }
    }
}

/// Re-execute an anchor-stripped request (Strategy A). Anchorless ⇒ cannot be silent, so no second
/// watchdog. Sniffs the new id and observes `Recovered`.
pub async fn execute_recovery(
    executor: &dyn Executor,
    continuity: Arc<dyn Continuity>,
    anchorless_req: PreparedRequest,
    account: &Account,
    account_id: AccountId,
    ctx: RequestCtx,
    session_key: Option<SessionKey>,
) -> Result<ResponseStream, WatchdogError> {
    let stream = executor
        .execute(anchorless_req, account)
        .await
        .map_err(|_| WatchdogError::Upstream)?;
    Ok(wrap_stream(
        stream,
        continuity,
        ctx,
        account_id,
        session_key,
        OutcomeKind::Recovered,
    ))
}

/// Emit a synthetic `previous_response_not_found` (Strategy B) so the client self-heals with a full
/// resend. Observes `Recovered` (no new id) and returns a one-shot stream. No upstream call.
pub async fn signal_client_stream(
    continuity: Arc<dyn Continuity>,
    ctx: RequestCtx,
    account_id: AccountId,
    session_key: Option<SessionKey>,
) -> ResponseStream {
    let _ = continuity
        .observe(
            TurnOutcome::Recovered {
                session_key,
                account: account_id,
                new_response_id: None,
            },
            &ctx,
        )
        .await;
    Box::pin(stream::once(async move {
        Ok::<Bytes, ExecError>(Bytes::from_static(SIGNAL_SSE))
    }))
}

// ---- observe-on-stream-end + response-id sniffing ------------------------------------------------

#[derive(Clone)]
enum OutcomeKind {
    Completed { fp: String, count: u32 },
    Recovered,
}

fn build_outcome(
    kind: OutcomeKind,
    session_key: Option<SessionKey>,
    account: AccountId,
    id: Option<String>,
) -> TurnOutcome {
    match kind {
        OutcomeKind::Completed { fp, count } => TurnOutcome::Completed {
            session_key,
            account,
            response_id: id,
            input_fingerprint: fp,
            input_count: count,
            reasoning: None,
        },
        OutcomeKind::Recovered => TurnOutcome::Recovered {
            session_key,
            account,
            new_response_id: id,
        },
    }
}

/// Bounded, non-buffering sniffer for the streamed `response.id`. Accumulates ≤ 64 KiB until it can
/// parse a `response.created`/`response.completed` id, then stops accumulating and forwards bytes.
struct ResponseIdSniffer {
    buf: Vec<u8>,
    id: Option<String>,
    done: bool,
}

impl ResponseIdSniffer {
    fn new() -> Self {
        Self {
            buf: Vec::new(),
            id: None,
            done: false,
        }
    }

    fn feed(&mut self, bytes: &Bytes) {
        if self.done {
            return;
        }
        self.buf.extend_from_slice(bytes);
        if let Some(id) = extract_response_id(&self.buf) {
            self.id = Some(id);
            self.done = true;
            self.buf = Vec::new();
        } else if self.buf.len() > 64 * 1024 {
            self.done = true; // give up sniffing; stay non-buffering
            self.buf = Vec::new();
        }
    }

    fn take_id(&mut self) -> Option<String> {
        self.id.take()
    }
}

fn extract_response_id(buf: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(buf);
    for line in text.lines() {
        let Some(payload) = line.strip_prefix("data:").map(str::trim) else {
            continue;
        };
        if payload == "[DONE]" {
            continue;
        }
        let Ok(v) = serde_json::from_str::<serde_json::Value>(payload) else {
            continue;
        };
        let ty = v.get("type").and_then(|t| t.as_str()).unwrap_or_default();
        if ty == "response.created" || ty == "response.completed" {
            if let Some(id) = v
                .get("response")
                .and_then(|r| r.get("id"))
                .and_then(|i| i.as_str())
            {
                return Some(id.to_string());
            }
        }
    }
    None
}

enum ObserveState {
    Streaming,
    Observing(Pin<Box<dyn Future<Output = ()> + Send>>),
    Done,
}

/// Wraps a byte stream: forwards every chunk unchanged while sniffing the `response.id`, then — on
/// stream end — awaits `continuity.observe(...)` INLINE before yielding the terminal `None`. This
/// makes ownership deterministic (turn N's state is persisted before the client sees end-of-stream).
struct ObservingStream {
    inner: ResponseStream,
    sniffer: ResponseIdSniffer,
    continuity: Arc<dyn Continuity>,
    ctx: RequestCtx,
    account: AccountId,
    session_key: Option<SessionKey>,
    kind: OutcomeKind,
    state: ObserveState,
}

impl Stream for ObservingStream {
    type Item = Result<Bytes, ExecError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut(); // ObservingStream is Unpin (all fields Unpin)
        loop {
            match &mut this.state {
                ObserveState::Streaming => match this.inner.as_mut().poll_next(cx) {
                    Poll::Ready(Some(Ok(bytes))) => {
                        this.sniffer.feed(&bytes);
                        return Poll::Ready(Some(Ok(bytes)));
                    }
                    Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                    Poll::Ready(None) => {
                        let outcome = build_outcome(
                            this.kind.clone(),
                            this.session_key.clone(),
                            this.account.clone(),
                            this.sniffer.take_id(),
                        );
                        let continuity = this.continuity.clone();
                        let ctx = this.ctx.clone();
                        let fut = Box::pin(async move {
                            let _ = continuity.observe(outcome, &ctx).await;
                        });
                        this.state = ObserveState::Observing(fut);
                        // loop: poll the observe future this wakeup
                    }
                    Poll::Pending => return Poll::Pending,
                },
                ObserveState::Observing(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(()) => {
                        this.state = ObserveState::Done;
                        return Poll::Ready(None);
                    }
                    Poll::Pending => return Poll::Pending,
                },
                ObserveState::Done => return Poll::Ready(None),
            }
        }
    }
}

fn wrap_stream(
    inner: ResponseStream,
    continuity: Arc<dyn Continuity>,
    ctx: RequestCtx,
    account: AccountId,
    session_key: Option<SessionKey>,
    kind: OutcomeKind,
) -> ResponseStream {
    Box::pin(ObservingStream {
        inner,
        sniffer: ResponseIdSniffer::new(),
        continuity,
        ctx,
        account,
        session_key,
        kind,
        state: ObserveState::Streaming,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_id_from_response_created() {
        let sse = b"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_42\"}}\n\n";
        assert_eq!(extract_response_id(sse).as_deref(), Some("resp_42"));
    }

    #[test]
    fn sniffer_is_bounded_and_stops_after_found() {
        let mut s = ResponseIdSniffer::new();
        s.feed(&Bytes::from_static(
            b"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\"}}\n\n",
        ));
        assert_eq!(s.take_id().as_deref(), Some("resp_1"));
        assert!(s.done);
    }

    #[test]
    fn watchdog_error_display_is_generic() {
        assert_eq!(WatchdogError::Upstream.to_string(), "upstream error");
        assert_eq!(
            WatchdogError::Continuity.to_string(),
            "continuity recovery unavailable"
        );
    }
}
