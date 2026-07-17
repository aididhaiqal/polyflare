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

use crate::runtime_state::RuntimeStates;
use crate::session_key::sha256_hex;

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

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

/// Errors the watchdog surfaces. Generic `Display` — never leaks a token, URL, or internal detail.
/// `Upstream` carries an optional [`FailureSignal`] (the upstream status + `Retry-After`) purely for
/// the ingress's routing-health writeback; it is never rendered into a client-facing message.
#[derive(Debug, thiserror::Error)]
pub enum WatchdogError {
    #[error("upstream error")]
    Upstream(Option<polyflare_core::FailureSignal>),
    #[error("continuity recovery unavailable")]
    Continuity,
    /// A streamed `response.failed` carrying `error.code == "cyber_policy"` (codex-rs wire truth:
    /// `codex-api/src/sse/responses.rs` `is_cyber_policy_error`), detected via peek-before-relay —
    /// no client byte was written before this was returned. Content-safe: `capability` is a fixed
    /// label, never the frame's `message`. TA6(b) Task 2 consumes this to trigger a reselect onto a
    /// `security_work_authorized` account; THIS type only detects + surfaces — no reroute here.
    #[error("capability rejection: {capability}")]
    CapabilityRejection { capability: &'static str },
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

/// Diagnostic input fingerprint + item count. Not used to gate a trim in M3-core (we never trim) —
/// recorded by `observe` for diagnostics only, so the fingerprint BASIS just needs to be stable per
/// request. The COUNT is derived once at ingress and carried on `ctx` (`input_count`), so this never
/// re-reads the request body — which, on the native pass-through, is never materialized. The
/// FINGERPRINT hashes the original wire bytes when present (byte-identical to what the client sent,
/// no `input` re-serialize); otherwise (a built/translated body) it hashes the canonical `input`
/// serialization of that body.
fn input_fingerprint_and_count(req: &PreparedRequest, ctx: &RequestCtx) -> (String, u32) {
    let fp = match &req.raw_body {
        Some(raw) => sha256_hex(raw.as_ref()),
        None => sha256_hex(
            req.body
                .as_ref()
                .and_then(|b| b.get("input"))
                .map(|v| v.to_string())
                .unwrap_or_default()
                .as_bytes(),
        ),
    };
    (fp, ctx.input_count)
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
    runtime: Arc<RuntimeStates>,
) -> Result<ResponseStream, WatchdogError> {
    let Prepared { req, directive } = prepared;
    let session_key = directive.session_key.clone();
    let (fp, count) = input_fingerprint_and_count(&req, &ctx);

    match directive.watchdog {
        WatchdogArm::Disarmed => {
            // No anchor ⇒ cannot be silent. Relay + sniff + observe(Completed).
            let stream = executor
                .execute(req, account, &ctx)
                .await
                .map_err(|e| WatchdogError::Upstream(e.failure_signal()))?;
            Ok(wrap_stream(
                stream,
                continuity,
                ctx,
                account_id,
                session_key,
                OutcomeKind::Completed { fp, count },
                runtime,
            ))
        }
        WatchdogArm::Armed { timeout } => {
            let mut stream = executor
                .execute(req, account, &ctx)
                .await
                .map_err(|e| WatchdogError::Upstream(e.failure_signal()))?;
            match tokio::time::timeout(timeout, stream.next()).await {
                Ok(Some(Ok(first))) => {
                    // TA6(b) Task 1: a cyber-policy rejection fails the turn BEFORE producing
                    // output, so in the common case it IS this first frame — the same
                    // peek-before-relay point the wedge fix's "ALIVE" check already uses. Detect it
                    // HERE, before ever rebuilding/relaying, and surface a distinct, content-safe
                    // signal instead of treating it as alive. No reroute in this task (TA6b Task 2
                    // consumes the signal); a NON-cyber `response.failed` (or anything else) falls
                    // through to the unchanged "ALIVE" path below exactly as before.
                    if is_cyber_policy_rejection(&first) {
                        let _ = continuity
                            .observe(
                                TurnOutcome::Failed {
                                    session_key: session_key.clone(),
                                },
                                &ctx,
                            )
                            .await;
                        return Err(WatchdogError::CapabilityRejection {
                            capability: "security_work",
                        });
                    }
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
                        runtime,
                    ))
                }
                Ok(Some(Err(e))) => {
                    // Hard upstream error before any client byte ⇒ observe(Failed) + 502. Carry any
                    // failure signal (a non-2xx caught mid-stream) for the ingress writeback.
                    let signal = e.failure_signal();
                    let _ = continuity
                        .observe(
                            TurnOutcome::Failed {
                                session_key: session_key.clone(),
                            },
                            &ctx,
                        )
                        .await;
                    Err(WatchdogError::Upstream(signal))
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
                                runtime,
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
#[allow(clippy::too_many_arguments)] // internal fn; each param is a distinct, clearly-named handle.
pub async fn execute_recovery(
    executor: &dyn Executor,
    continuity: Arc<dyn Continuity>,
    anchorless_req: PreparedRequest,
    account: &Account,
    account_id: AccountId,
    ctx: RequestCtx,
    session_key: Option<SessionKey>,
    runtime: Arc<RuntimeStates>,
) -> Result<ResponseStream, WatchdogError> {
    let stream = executor
        .execute(anchorless_req, account, &ctx)
        .await
        .map_err(|e| WatchdogError::Upstream(e.failure_signal()))?;
    Ok(wrap_stream(
        stream,
        continuity,
        ctx,
        account_id,
        session_key,
        OutcomeKind::Recovered,
        runtime,
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

/// Returns `true` iff `buf` (raw SSE bytes from a single peeked chunk) contains a terminal
/// `response.failed` frame whose `response.error.code == "cyber_policy"` — the wire truth
/// (`codex-rs`'s `codex-api/src/sse/responses.rs` `is_cyber_policy_error`: `error.code.as_deref()
/// == Some("cyber_policy")`). Content-safety: reads ONLY `type` and the nested `response.error.code`
/// — the frame's `message` is never read into any local, returned, or logged value (mirrors
/// `polyflare_codex::executor::extract_error_code`'s code-only extraction for the non-2xx path).
/// Unparsable/absent/non-matching input is always `false` — never treated as an error here.
fn is_cyber_policy_rejection(buf: &[u8]) -> bool {
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
        if v.get("type").and_then(|t| t.as_str()) != Some("response.failed") {
            continue;
        }
        let code = v
            .get("response")
            .and_then(|r| r.get("error"))
            .and_then(|e| e.get("code"))
            .and_then(|c| c.as_str());
        if code == Some("cyber_policy") {
            return true;
        }
    }
    false
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
    /// A3: records the account's routing-health at TRUE stream completion — clean EOF ⇒ success
    /// (clear the error state), a mid-stream error ⇒ transient error. This is why success recording
    /// lives here and NOT at the `Ok(stream)` return: only here is the account's ACTUAL outcome
    /// known, and the synthetic `signal_client_stream` (not wrapped in `ObservingStream`) is
    /// correctly excluded.
    runtime: Arc<RuntimeStates>,
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
                    Poll::Ready(Some(Err(e))) => {
                        // A3: a mid-stream drop (after the first byte) — the account served a partial
                        // response then failed. Count it as a transient error, then forward the error.
                        this.runtime
                            .record_transient_error(&this.account, unix_now());
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Ready(None) => {
                        // A3: clean EOF ⇒ the account completed the turn — clear its error state so
                        // intermittent blips don't accumulate it into permanent backoff/drain.
                        this.runtime.record_success(&this.account);
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
    runtime: Arc<RuntimeStates>,
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
        runtime,
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
        assert_eq!(WatchdogError::Upstream(None).to_string(), "upstream error");
        assert_eq!(
            WatchdogError::Continuity.to_string(),
            "continuity recovery unavailable"
        );
    }

    #[test]
    fn detects_cyber_policy_response_failed() {
        let sse = concat!(
            "data: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_x\",",
            "\"error\":{\"code\":\"cyber_policy\",\"message\":\"do not leak this\"}}}\n\n",
        )
        .as_bytes();
        assert!(is_cyber_policy_rejection(sse));
    }

    #[test]
    fn ignores_non_cyber_response_failed() {
        let sse = concat!(
            "data: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_x\",",
            "\"error\":{\"code\":\"server_is_overloaded\",\"message\":\"slow down\"}}}\n\n",
        )
        .as_bytes();
        assert!(!is_cyber_policy_rejection(sse));
    }

    #[test]
    fn ignores_non_failed_frames_and_garbage() {
        assert!(!is_cyber_policy_rejection(
            b"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\"}}\n\n"
        ));
        assert!(!is_cyber_policy_rejection(b"data: [DONE]\n\n"));
        assert!(!is_cyber_policy_rejection(b"not sse at all"));
        assert!(!is_cyber_policy_rejection(b""));
    }

    #[test]
    fn capability_rejection_display_never_carries_a_message() {
        let err = WatchdogError::CapabilityRejection {
            capability: "security_work",
        };
        assert_eq!(err.to_string(), "capability rejection: security_work");
        assert!(!format!("{err:?}").contains("message"));
    }
}
