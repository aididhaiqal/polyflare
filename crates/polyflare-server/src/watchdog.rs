//! The silence watchdog + ownership pre-filter. `execute_with_watchdog` races the FIRST upstream
//! chunk against N; on silence it drops the dead stream (cancel-safe) and recovers. Peek-before-
//! relay: no client byte is written until the first upstream chunk arrives, so a restart is always
//! safe. The Codex executor is untouched — this wraps it in the server.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures_core::Stream;
use futures_util::stream::{self, StreamExt};
use polyflare_core::{
    Account, AccountId, AccountSnapshot, Continuity, ContinuityDirective, ExecError, Executor,
    Prepared, PreparedRequest, RecoveryPlan, RequestCtx, ResponseStream, SelectionCtx, Selector,
    SessionKey, TurnOutcome, WatchdogArm,
};

use crate::runtime_state::{InFlightGuard, RuntimeStates};
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
    #[error("upstream returned status {}", .0.signal.status)]
    UpstreamHttp(polyflare_core::UpstreamHttpError),
    #[error("continuity recovery unavailable")]
    Continuity,
    /// The same hashed Codex logical turn has already spent its aggregate process-wide generation
    /// budget. This is client-terminal and must never enter account failover.
    #[error("logical turn attempt budget exhausted")]
    AttemptBudgetExhausted,
    /// A streamed `response.failed` carrying `error.code == "cyber_policy"` (codex-rs wire truth:
    /// `codex-api/src/sse/responses.rs` `is_cyber_policy_error`), detected via peek-before-relay —
    /// no client byte was written before this was returned. Content-safe: `capability` is a fixed
    /// label, never the frame's `message`. TA6(b) Task 2 consumes this to trigger a reselect onto a
    /// `security_work_authorized` account; THIS type only detects + surfaces — no reroute here.
    #[error("capability rejection: {capability}")]
    CapabilityRejection { capability: &'static str },
}

fn map_executor_error(error: ExecError) -> WatchdogError {
    match error {
        ExecError::UpstreamHttp(response) => WatchdogError::UpstreamHttp(response),
        other => WatchdogError::Upstream(other.failure_signal()),
    }
}

fn consume_logical_turn_attempt(
    runtime: &RuntimeStates,
    ctx: &RequestCtx,
    max_attempts: u32,
) -> Result<(), WatchdogError> {
    runtime
        .try_consume_logical_turn_attempt(ctx.logical_turn_key.as_deref(), max_attempts, unix_now())
        .then_some(())
        .ok_or(WatchdogError::AttemptBudgetExhausted)
}

/// B4 Task 3 — the commit barrier's detection primitive: whether ANY response byte has reached
/// the client yet for a given attempt (`docs/superpowers/plans/2026-07-18-phase-b-failover.md`,
/// Global Constraints: "the loop may retry ONLY before the first byte reaches the client").
///
/// A cheap, `Clone`-and-share handle over a single `Arc<AtomicBool>`: the caller (Task 4's loop)
/// holds one clone and threads another into [`execute_with_watchdog_tracked`] /
/// [`execute_recovery_tracked`]; [`ObservingStream`] marks it the instant it successfully yields
/// its FIRST chunk (idempotent on every chunk after — a plain store, no branch needed). Starts
/// `false`.
///
/// # Why every `WatchdogError` is `committed == false` by construction
/// `execute_with_watchdog`/`execute_with_watchdog_tracked` and `execute_recovery`/
/// `execute_recovery_tracked` only ever return `Err` BEFORE constructing/returning the
/// `Ok(ResponseStream)` — every `Err` site in this module runs strictly before `wrap_stream` is
/// called (a pre-relay executor failure, or a first-frame reject/hard-error caught by the Armed
/// peek/scan, all documented at each call site below). Once these functions return `Ok`, their job
/// is done; nothing that happens LATER while the stream is polled (by `stream_response`/axum, well
/// outside this module) can flow back through their already-returned `Result`. So a witness paired
/// with any `Err` from these functions is, structurally, always still `false` when read — Task 4
/// never needs to inspect it for that path, only for the (separate) mid-stream case below.
///
/// It only ever becomes `true` once the returned `Ok(ResponseStream)` is ACTUALLY polled by
/// whoever consumes it and yields a real byte — i.e. strictly after the loop has already handed
/// the stream off and lost its one chance to retry (retrying past that point would double-relay,
/// the exact thing the commit barrier exists to prevent). This is why `committed` is threaded as
/// its own out-of-band signal rather than a field on `WatchdogError`: `WatchdogError` only ever
/// describes the pre-relay case (see above), while a mid-stream failure never produces a
/// `WatchdogError` at all — it's an `Err` item inside the already-`Ok` stream (see
/// `ObservingStream::poll_next`), a completely different point in the control flow.
#[derive(Clone, Default)]
pub struct CommitWitness(Arc<AtomicBool>);

impl CommitWitness {
    /// A fresh, not-yet-committed witness.
    pub fn new() -> Self {
        Self::default()
    }

    /// `true` once at least one byte of the tracked stream has been yielded to its poller.
    pub fn is_committed(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    /// Idempotent: called on every successfully-yielded chunk, not just the first — cheaper than a
    /// load-then-conditional-store, and correctness doesn't depend on it firing only once.
    fn mark(&self) {
        self.0.store(true, Ordering::Release);
    }
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
///
/// Signature-identical to every caller today (ingress.rs's three non-failover-loop call sites, and
/// the wedge test suites) — B4 Task 3 added the commit-barrier signal as a SEPARATE entry point,
/// [`execute_with_watchdog_tracked`], rather than a new parameter here, specifically so this
/// function's callers never had to change. This one simply discards a throwaway [`CommitWitness`];
/// behavior is byte-for-byte identical to before Task 3.
#[allow(clippy::too_many_arguments)] // mirrors `execute_with_watchdog_tracked`; one added handle.
pub async fn execute_with_watchdog(
    executor: &dyn Executor,
    continuity: Arc<dyn Continuity>,
    prepared: Prepared,
    account: &Account,
    account_id: AccountId,
    ctx: RequestCtx,
    runtime: Arc<RuntimeStates>,
    idle_timeout: Duration,
) -> Result<ResponseStream, WatchdogError> {
    execute_with_watchdog_tracked(
        executor,
        continuity,
        prepared,
        account,
        account_id,
        ctx,
        runtime,
        idle_timeout,
        3,
        CommitWitness::new(),
        // C9 Task 2: this convenience delegator's callers (the wedge/cyber suites, and every
        // ingress.rs site that has not been given a lease) never carry an `InFlightGuard` — its
        // signature is UNCHANGED by this task (see the module-level precedent set by B4 Task 3's
        // `CommitWitness` addition: new capabilities land on the `_tracked` sibling only).
        None,
    )
    .await
}

/// B4 Task 4 hook: identical relay/observe/recovery behavior to [`execute_with_watchdog`] — this
/// IS that function's body, just also threading a [`CommitWitness`] through to whichever stream
/// ends up wrapped (or leaving it unmarked on every pre-relay `Err` path — see the type's doc for
/// why that's always correct). Task 4's loop calls this variant so it can later ask
/// `commit.is_committed()` without re-deriving first-byte state.
#[allow(clippy::too_many_arguments)] // mirrors `execute_with_watchdog`; one added handle.
pub async fn execute_with_watchdog_tracked(
    executor: &dyn Executor,
    continuity: Arc<dyn Continuity>,
    prepared: Prepared,
    account: &Account,
    account_id: AccountId,
    ctx: RequestCtx,
    runtime: Arc<RuntimeStates>,
    idle_timeout: Duration,
    max_attempts: u32,
    commit: CommitWitness,
    // C9 Task 2: the in-flight lease the CALLER already acquired for `account_id` at selection
    // (`None` for a caller that never acquired one). Moves into whichever `wrap_stream`/
    // `recover_from_silence` branch below actually fires for THIS attempt — every other branch
    // (a pre-relay `Err` return) simply drops it when this function's scope ends, which is exactly
    // the release-on-failed-attempt behavior the crux requires.
    in_flight: Option<InFlightGuard>,
) -> Result<ResponseStream, WatchdogError> {
    consume_logical_turn_attempt(&runtime, &ctx, max_attempts)?;
    let Prepared { req, directive } = prepared;
    let session_key = directive.session_key.clone();
    let (fp, count) = input_fingerprint_and_count(&req, &ctx);

    match directive.watchdog {
        WatchdogArm::Disarmed => {
            // No anchor ⇒ cannot be silent. Relay + sniff + observe(Completed).
            let stream = executor
                .execute(req, account, &ctx)
                .await
                .map_err(map_executor_error)?;
            Ok(wrap_stream(
                stream,
                continuity,
                ctx,
                account_id,
                session_key,
                OutcomeKind::Completed { fp, count },
                runtime,
                idle_timeout,
                commit,
                in_flight,
            ))
        }
        WatchdogArm::Armed { timeout } => {
            let mut stream = executor
                .execute(req, account, &ctx)
                .await
                .map_err(map_executor_error)?;
            match tokio::time::timeout(timeout, stream.next()).await {
                Ok(Some(Ok(first))) => {
                    // TA6(b) Task 1 (fixed): real Codex wire ordering emits `response.created`
                    // FIRST and the cyber-policy `response.failed` on a LATER chunk — inspecting
                    // only this first peeked chunk (the original bug) let `response.created` alone
                    // be judged "ALIVE" and relayed, so the later cyber frame streamed straight to
                    // the client without ever being inspected. `scan_past_lifecycle` buffers
                    // (bounded, mirroring `ResponseIdSniffer`) past pure lifecycle frames
                    // (`response.created`/`response.in_progress`) until it reaches the first
                    // decisive frame — model content, any terminal frame, or an unrecognized type —
                    // and only THEN decides ALIVE. A `cyber_policy` `response.failed` seen before
                    // that point rejects with nothing relayed (peek-before-relay preserved: still no
                    // client byte written for a rejected turn). No reroute in this task (TA6b Task 2
                    // consumes the signal).
                    match scan_past_lifecycle(first, stream, timeout).await {
                        ScanOutcome::CyberPolicy => {
                            let _ = continuity
                                .observe(
                                    TurnOutcome::Failed {
                                        session_key: session_key.clone(),
                                    },
                                    &ctx,
                                )
                                .await;
                            Err(WatchdogError::CapabilityRejection {
                                capability: "security_work",
                            })
                        }
                        ScanOutcome::HardError(e) => {
                            // A hard error surfaced while scanning (before any client byte was
                            // relayed) is exactly the "hard upstream error before any client byte"
                            // case below — same handling.
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
                        ScanOutcome::Alive(rebuilt) => {
                            // ALIVE: sniff + observe(Completed), relaying every buffered frame (in
                            // order) chained with whatever the inner stream has left.
                            Ok(wrap_stream(
                                rebuilt,
                                continuity,
                                ctx,
                                account_id,
                                session_key,
                                OutcomeKind::Completed { fp, count },
                                runtime,
                                idle_timeout,
                                commit,
                                in_flight,
                            ))
                        }
                        ScanOutcome::Silence => {
                            // Re-review fix: a silence discovered DURING the scan (post-`created`,
                            // before any decisive frame) is recoverable exactly like a silence on
                            // the initial peek — nothing has been relayed yet either way. The
                            // scanned-past `ResponseStream` was moved into `scan_past_lifecycle` and
                            // is dropped here as it goes out of scope (cancel-safe), same as the
                            // explicit `drop(stream)` below. Route into the SAME recovery machinery.
                            recover_from_silence(
                                executor,
                                continuity,
                                directive.recovery,
                                account,
                                account_id,
                                ctx,
                                session_key,
                                runtime,
                                idle_timeout,
                                max_attempts,
                                commit,
                                in_flight,
                            )
                            .await
                        }
                    }
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
                    recover_from_silence(
                        executor,
                        continuity,
                        directive.recovery,
                        account,
                        account_id,
                        ctx,
                        session_key,
                        runtime,
                        idle_timeout,
                        max_attempts,
                        commit,
                        in_flight,
                    )
                    .await
                }
            }
        }
    }
}

/// Shared recovery dispatch for EVERY silence outcome — the initial peek's `Ok(None) | Err(_)` AND
/// the scan-loop's [`ScanOutcome::Silence`] (re-review fix) both call this, so there is exactly one
/// place that turns a `RecoveryPlan` into an outcome. Both call sites are valid here for the same
/// reason: in each, peek-before-relay held for the whole time the dead stream was alive — nothing
/// was ever relayed to the client — so restarting via `ResendFull`/`SignalClient` is clean.
/// `commit`: threaded through so the ONE branch that produces a real (re-)relay
/// (`RecoveryPlan::ResendFull`) can keep tracking commit state for THAT resend attempt — a
/// full-resend can itself mid-stream-fail after relaying bytes, same as any other attempt. The
/// other two branches never touch it: `SignalClient` emits a synthetic, always-succeeding one-shot
/// frame (no failure path to track), and `None` returns `Err` before any stream exists at all (the
/// witness is correctly left unmarked, i.e. still `false`, in both).
///
/// `in_flight` (C9 Task 2): same account, same attempt's lease — it continues straight through
/// into `ResendFull`'s `execute_recovery_tracked` call (still same-account, so no
/// release/reacquire needed here; that only happens on a genuine cross-account failover, which
/// lives in `ingress.rs::run_failover_loop`, one layer up). `SignalClient` doesn't relay any real
/// upstream bytes on this account for this turn, and `None` never reaches a stream at all — both
/// simply let the owned `in_flight` parameter drop when this function's scope ends, which is
/// exactly the correct release for "this account attempt is over, no stream was produced".
#[allow(clippy::too_many_arguments)] // internal fn; each param is a distinct, clearly-named handle.
async fn recover_from_silence(
    executor: &dyn Executor,
    continuity: Arc<dyn Continuity>,
    recovery: RecoveryPlan,
    account: &Account,
    account_id: AccountId,
    ctx: RequestCtx,
    session_key: Option<SessionKey>,
    runtime: Arc<RuntimeStates>,
    idle_timeout: Duration,
    max_attempts: u32,
    commit: CommitWitness,
    in_flight: Option<InFlightGuard>,
) -> Result<ResponseStream, WatchdogError> {
    match recovery {
        RecoveryPlan::ResendFull { anchorless_req } => {
            execute_recovery_tracked(
                executor,
                continuity,
                anchorless_req,
                account,
                account_id,
                ctx,
                session_key,
                runtime,
                idle_timeout,
                max_attempts,
                commit,
                in_flight,
            )
            .await
        }
        RecoveryPlan::SignalClient => {
            Ok(signal_client_stream(continuity, ctx, account_id, session_key).await)
        }
        RecoveryPlan::None => Err(WatchdogError::Continuity),
    }
}

/// Re-execute an anchor-stripped request (Strategy A). Anchorless ⇒ cannot be silent, so no second
/// watchdog. Sniffs the new id and observes `Recovered`. Threads a [`CommitWitness`] through to the
/// wrapped stream and an optional in-flight lease guard (C9) held for the stream's lifetime — see
/// [`execute_with_watchdog_tracked`]'s doc for the general shape.
///
/// (The former thin non-tracked `execute_recovery` delegator was removed in C9 once every call site
/// threaded a real `CommitWitness` + lease guard — all callers use this `_tracked` form directly.)
#[allow(clippy::too_many_arguments)] // internal fn; each param is a distinct, clearly-named handle.
pub async fn execute_recovery_tracked(
    executor: &dyn Executor,
    continuity: Arc<dyn Continuity>,
    anchorless_req: PreparedRequest,
    account: &Account,
    account_id: AccountId,
    ctx: RequestCtx,
    session_key: Option<SessionKey>,
    runtime: Arc<RuntimeStates>,
    idle_timeout: Duration,
    max_attempts: u32,
    commit: CommitWitness,
    // C9 Task 2: see `execute_with_watchdog_tracked`'s matching param doc — moves into the single
    // `wrap_stream` call below on success; drops here (this function's scope) on the `?`-propagated
    // pre-relay `Err` above.
    in_flight: Option<InFlightGuard>,
) -> Result<ResponseStream, WatchdogError> {
    consume_logical_turn_attempt(&runtime, &ctx, max_attempts)?;
    let stream = executor
        .execute(anchorless_req, account, &ctx)
        .await
        .map_err(map_executor_error)?;
    Ok(wrap_stream(
        stream,
        continuity,
        ctx,
        account_id,
        session_key,
        OutcomeKind::Recovered,
        runtime,
        idle_timeout,
        commit,
        in_flight,
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
    ResponseStream::new(stream::once(async move {
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
    terminal: TerminalOutcome,
) -> TurnOutcome {
    let TerminalOutcome::Completed { response_id } = terminal else {
        return TurnOutcome::Failed { session_key };
    };
    match kind {
        OutcomeKind::Completed { fp, count } => TurnOutcome::Completed {
            session_key,
            account,
            response_id,
            input_fingerprint: fp,
            input_count: count,
            reasoning: None,
        },
        OutcomeKind::Recovered => TurnOutcome::Recovered {
            session_key,
            account,
            new_response_id: response_id,
        },
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
enum TerminalOutcome {
    #[default]
    Pending,
    Completed {
        response_id: Option<String>,
    },
    Failed {
        kind: ProtocolFailureKind,
    },
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum ProtocolFailureKind {
    /// Stream ended without any terminal protocol event.
    #[default]
    TransportLoss,
    /// Account/server health fault that should contribute to transient backoff.
    Transient,
    /// Upstream per-account rate limit.
    RateLimited,
    /// Durable capacity exhaustion; trigger a usage refresh immediately.
    QuotaExceeded,
    /// Request-level terminal result (invalid input, policy, or an explicit incomplete response).
    Request,
}

/// Bounded, non-buffering sniffer for the streamed terminal outcome. A `response.created` id is
/// retained only as a candidate; continuation is committed exclusively after a matching
/// `response.completed`. Failed, incomplete, malformed, or clean EOF without completion are never
/// converted into reusable anchors.
struct ResponseIdSniffer {
    buf: Vec<u8>,
    created_id: Option<String>,
    terminal: TerminalOutcome,
    dropping_oversized_line: bool,
}

impl ResponseIdSniffer {
    fn new() -> Self {
        Self {
            buf: Vec::new(),
            created_id: None,
            terminal: TerminalOutcome::Pending,
            dropping_oversized_line: false,
        }
    }

    fn feed(&mut self, bytes: &Bytes) {
        if !matches!(self.terminal, TerminalOutcome::Pending) {
            return;
        }

        let mut incoming = bytes.as_ref();
        if self.dropping_oversized_line {
            let Some(end) = incoming.iter().position(|b| *b == b'\n') else {
                return;
            };
            incoming = &incoming[end + 1..];
            self.dropping_oversized_line = false;
        }
        self.buf.extend_from_slice(incoming);

        while let Some(end) = self.buf.iter().position(|b| *b == b'\n') {
            let line = self.buf.drain(..=end).collect::<Vec<_>>();
            self.observe_line(&line);
            if !matches!(self.terminal, TerminalOutcome::Pending) {
                self.buf.clear();
                return;
            }
        }

        if self.buf.len() > 64 * 1024 {
            // Skip only this oversized SSE line. Resume parsing after its newline so a later,
            // compact terminal event can still be recognized. If the terminal event itself is
            // oversized, the safe result at EOF is Failed—not a guessed completion.
            self.buf.clear();
            self.dropping_oversized_line = true;
        }
    }

    fn observe_line(&mut self, line: &[u8]) {
        let Ok(line) = std::str::from_utf8(line) else {
            return;
        };
        let Some(payload) = line.trim_end().strip_prefix("data:").map(str::trim) else {
            return;
        };
        if payload == "[DONE]" {
            return;
        }
        let Ok(v) = serde_json::from_str::<serde_json::Value>(payload) else {
            return;
        };
        let ty = v.get("type").and_then(|t| t.as_str()).unwrap_or_default();
        let response_id = v
            .pointer("/response/id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        match ty {
            "response.created" => self.created_id = response_id,
            "response.completed" => {
                self.terminal = TerminalOutcome::Completed {
                    response_id: response_id.or_else(|| self.created_id.clone()),
                };
            }
            "response.failed" => {
                let code = v
                    .pointer("/response/error/code")
                    .and_then(serde_json::Value::as_str);
                let kind = match code {
                    Some("rate_limit_exceeded") => ProtocolFailureKind::RateLimited,
                    Some("insufficient_quota" | "usage_not_included") => {
                        ProtocolFailureKind::QuotaExceeded
                    }
                    Some("server_is_overloaded" | "slow_down") => ProtocolFailureKind::Transient,
                    Some(
                        "context_length_exceeded"
                        | "invalid_prompt"
                        | "bio_policy"
                        | "cyber_policy",
                    ) => ProtocolFailureKind::Request,
                    // codex-rs treats all other response.failed errors as retryable.
                    _ => ProtocolFailureKind::Transient,
                };
                self.terminal = TerminalOutcome::Failed { kind };
            }
            "response.incomplete" => {
                self.terminal = TerminalOutcome::Failed {
                    kind: ProtocolFailureKind::Request,
                };
            }
            _ => {}
        }
    }

    fn finish(&mut self) -> TerminalOutcome {
        match std::mem::take(&mut self.terminal) {
            TerminalOutcome::Completed { response_id } => {
                TerminalOutcome::Completed { response_id }
            }
            TerminalOutcome::Pending => TerminalOutcome::Failed {
                kind: ProtocolFailureKind::TransportLoss,
            },
            TerminalOutcome::Failed { kind } => TerminalOutcome::Failed { kind },
        }
    }
}

/// Outcome of [`scan_past_lifecycle`].
enum ScanOutcome {
    /// Nothing decisive said "cyber" — relay everything buffered while scanning, chained with
    /// whatever the inner stream has left. This is the (renamed, otherwise unchanged) "ALIVE" path.
    Alive(ResponseStream),
    /// A `cyber_policy` `response.failed` frame appeared before any decisive frame. Peek-before-
    /// relay is preserved: nothing buffered during the scan is relayed.
    CyberPolicy,
    /// The inner stream produced a hard error while scanning, i.e. before anything was relayed —
    /// identical in every observable way to a hard error on the very first frame.
    HardError(ExecError),
    /// The scan-loop's per-read `timeout` elapsed before a decisive frame arrived (re-review
    /// finding: upstream sent `response.created` then went silent). Peek-before-relay holds across
    /// the WHOLE scan window — nothing buffered here has been relayed — so this is recoverable
    /// exactly like a first-chunk silence: the caller drops the stream and routes into the same
    /// `ResendFull`/`SignalClient` recovery as `Ok(None) | Err(_)` on the initial peek.
    Silence,
}

/// A single buffered frame's classification, once its `type` can be read.
enum ScanVerdict {
    /// A `response.failed` whose `error.code == "cyber_policy"` — the wire truth (`codex-rs`'s
    /// `codex-api/src/sse/responses.rs` `is_cyber_policy_error`: `error.code.as_deref() ==
    /// Some("cyber_policy")`).
    CyberPolicy,
    /// Anything else recognized as NOT a pure lifecycle frame: actual model content
    /// (`response.output_text.delta`, `response.output_item.added`, ...), any terminal frame that
    /// isn't the cyber rejection (`response.completed`, a non-cyber `response.failed`), or an
    /// unrecognized type. All of these get identical treatment — stop scanning, go ALIVE — so they
    /// don't need to be told apart any further than "not lifecycle, not cyber".
    Decisive,
}

/// Classifies one already-parsed SSE event `type` + payload. `response.created`/
/// `response.in_progress` are the ONLY types that keep the scan going — every real Codex turn
/// starts with these before anything else, and they never carry content or a terminal outcome.
/// Content-safety: reads ONLY `type` and, for `response.failed`, the nested `response.error.code`
/// — the frame's `message` is never read into any local, returned, or logged value (mirrors
/// `polyflare_codex::executor::extract_error_code`'s code-only extraction for the non-2xx path).
fn classify_frame(v: &serde_json::Value) -> Option<ScanVerdict> {
    let ty = v.get("type").and_then(|t| t.as_str()).unwrap_or_default();
    match ty {
        "response.created" | "response.in_progress" => None, // keep scanning
        "response.failed" => {
            let code = v
                .get("response")
                .and_then(|r| r.get("error"))
                .and_then(|e| e.get("code"))
                .and_then(|c| c.as_str());
            Some(if code == Some("cyber_policy") {
                ScanVerdict::CyberPolicy
            } else {
                ScanVerdict::Decisive
            })
        }
        _ => Some(ScanVerdict::Decisive),
    }
}

/// Scans `buf` (the accumulated raw SSE bytes buffered so far, across possibly several chunks) for
/// the first frame that decides anything. Returns `None` if every complete frame parsed so far is
/// pure lifecycle (or nothing parses yet, e.g. a chunk boundary split a line) — the caller should
/// buffer more and rescan. Mirrors `extract_response_id`'s tolerant, re-parse-the-whole-buffer-each-
/// time style: malformed/partial trailing JSON is silently skipped, never treated as decisive.
fn scan_buffered_frames(buf: &[u8]) -> Option<ScanVerdict> {
    let text = String::from_utf8_lossy(buf);
    for line in text.lines() {
        let Some(payload) = line.strip_prefix("data:").map(str::trim) else {
            continue;
        };
        if payload.is_empty() || payload == "[DONE]" {
            continue;
        }
        let Ok(v) = serde_json::from_str::<serde_json::Value>(payload) else {
            continue;
        };
        if let Some(verdict) = classify_frame(&v) {
            return Some(verdict);
        }
    }
    None
}

/// Bounded buffer cap for [`scan_past_lifecycle`] — mirrors `ResponseIdSniffer`'s give-up
/// threshold. A real turn always produces a decisive frame (content or terminal) within a handful
/// of small lifecycle frames; if a pathological upstream never does, give up scanning rather than
/// buffer unboundedly and fall back to the ALIVE path with whatever was collected.
const MAX_SCAN_BYTES: usize = 64 * 1024;

/// Buffers upstream chunks (bounded; see [`MAX_SCAN_BYTES`]) past pure lifecycle frames
/// (`response.created`/`response.in_progress`), scanning for the first DECISIVE frame — mirrors
/// `ResponseIdSniffer`'s accumulate-and-reparse buffering approach rather than reinventing SSE
/// reassembly. `first` is the chunk the caller already peeked (under the silence-watchdog timeout).
/// Each subsequent read is ALSO raced against `timeout` (the SAME `Duration` the first-chunk peek
/// used) — re-review finding: without this, a stream that sends `response.created` then goes
/// silent (nothing more, ever) parked this loop's naked `stream.next().await` forever, before any
/// HTTP status was sent to the client, with no self-healing. A genuinely-live stream that keeps
/// producing frames within `timeout` never observes this — the timeout only fires on real silence.
async fn scan_past_lifecycle(
    first: Bytes,
    mut stream: ResponseStream,
    timeout: Duration,
) -> ScanOutcome {
    let mut relay_chunks: Vec<Bytes> = vec![first.clone()];
    let mut scan_buf: Vec<u8> = first.to_vec();

    loop {
        match scan_buffered_frames(&scan_buf) {
            Some(ScanVerdict::CyberPolicy) => return ScanOutcome::CyberPolicy,
            Some(ScanVerdict::Decisive) => break,
            None => {
                if scan_buf.len() > MAX_SCAN_BYTES {
                    break; // bounded: give up scanning, treat as alive with what we have
                }
                match tokio::time::timeout(timeout, stream.next()).await {
                    Ok(Some(Ok(next))) => {
                        scan_buf.extend_from_slice(&next);
                        relay_chunks.push(next);
                    }
                    Ok(Some(Err(e))) => return ScanOutcome::HardError(e),
                    Ok(None) => break, // stream ended before a decisive frame; relay what we have
                    Err(_) => return ScanOutcome::Silence, // per-read timeout elapsed: silence
                }
            }
        }
    }

    let metadata = stream.metadata().clone();
    let rebuilt = ResponseStream::with_metadata(
        stream::iter(relay_chunks.into_iter().map(Ok::<Bytes, ExecError>)).chain(stream),
        metadata,
    );
    ScanOutcome::Alive(rebuilt)
}

enum ObserveState {
    Streaming,
    Observing(Pin<Box<dyn Future<Output = ()> + Send>>),
    Done,
}

/// Task 1: the mid-stream idle deadline — "no byte since". `Disabled` when the configured idle
/// timeout is zero (the disable lever, `POLYFLARE_STREAM_IDLE_TIMEOUT_SECS=0`): [`poll`](Self::poll)
/// then always reports `Poll::Pending` and never registers a timer, so the `Streaming` arm's
/// `Poll::Pending` path is byte-for-byte identical to before this task. `Armed` holds a resettable
/// `tokio::time::Sleep`, boxed+pinned so it can be reset in place (`Sleep::reset`) across polls from
/// `&mut self` without re-pinning — mirrors `ObserveState::Observing`'s existing
/// `Pin<Box<dyn Future...>>` idiom, the only precedent in this module for a manually-polled future
/// stored on a struct. `Pin<Box<T>>` is `Unpin` unconditionally (the pin-ness lives in the heap
/// allocation, independent of whether `T` itself is `Unpin` — `Sleep` is not), so this field does not
/// disturb `ObservingStream`'s existing "all fields Unpin" invariant.
enum IdleDeadline {
    Disabled,
    Armed {
        sleep: Pin<Box<tokio::time::Sleep>>,
        timeout: Duration,
    },
}

impl IdleDeadline {
    /// `timeout == Duration::ZERO` ⇒ disabled (today's behavior, no deadline at all). Otherwise
    /// armed with the deadline starting at construction time — this is what bounds the very FIRST
    /// byte too, for a Disarmed request with no pre-relay wedge timer of its own.
    fn new(timeout: Duration) -> Self {
        if timeout.is_zero() {
            IdleDeadline::Disabled
        } else {
            IdleDeadline::Armed {
                sleep: Box::pin(tokio::time::sleep(timeout)),
                timeout,
            }
        }
    }

    /// A byte just arrived — push the deadline back out to `now + timeout`. No-op when disabled.
    fn reset(&mut self) {
        if let IdleDeadline::Armed { sleep, timeout } = self {
            sleep.as_mut().reset(tokio::time::Instant::now() + *timeout);
        }
    }

    /// Poll the deadline. `Disabled` never fires (always `Poll::Pending`, registering nothing) —
    /// this is exactly what makes the disabled path indistinguishable from "no deadline at all".
    /// `Armed` polls the underlying `Sleep` directly, registering ITS waker so a wake from the timer
    /// alone re-polls this stream (the caller polls the inner stream too in the same turn, so a
    /// wake from either side re-polls — both wakers end up registered per turn).
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        match self {
            IdleDeadline::Disabled => Poll::Pending,
            IdleDeadline::Armed { sleep, .. } => sleep.as_mut().poll(cx),
        }
    }
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
    /// Task 1: "no byte since" deadline — reset on every successfully-yielded chunk, checked only
    /// when the inner stream reports `Poll::Pending`. See [`IdleDeadline`]'s doc.
    idle_deadline: IdleDeadline,
    /// B4 Task 3: marked the instant the FIRST chunk is successfully yielded below — a pure
    /// addition, read by no code in this struct. It does not gate, delay, or otherwise touch relay
    /// (every chunk is still forwarded unconditionally, in order) or the `record_transient_error`/
    /// `record_success`/`observe` calls, which fire exactly as before Task 3.
    commit: CommitWitness,
    /// C9 Task 2 (THE CRUX): the in-flight lease acquired at selection for this stream's account,
    /// held here PURELY so its own `Drop` (see [`InFlightGuard`]) fires whenever THIS
    /// `ObservingStream` is dropped — clean drain, client disconnect, mid-stream error, idle-
    /// timeout, or a panic unwind, uniformly, with ZERO change to `poll_next` below. NEVER READ
    /// (the leading underscore signals that — `#[allow(dead_code)]` is unnecessary because a
    /// struct field's `Drop` still runs even though nothing ever loads its value). Deliberately NOT
    /// an `impl Drop for ObservingStream`: a field's own `Drop` already does the release, so adding
    /// one here would be redundant machinery risking an accidental future touch to this struct's
    /// (wedge-sacred) poll logic.
    _in_flight: Option<InFlightGuard>,
}

impl Stream for ObservingStream {
    type Item = Result<Bytes, ExecError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut(); // ObservingStream is Unpin (all fields Unpin)
        loop {
            match &mut this.state {
                ObserveState::Streaming => match Pin::new(&mut this.inner).poll_next(cx) {
                    Poll::Ready(Some(Ok(bytes))) => {
                        // Task 1: a byte arrived — push the idle deadline back out. No-op when
                        // disabled. Ordered before `mark`/`feed`/return so a LATER poll's deadline
                        // check always sees the reset from this byte, never a stale one.
                        this.idle_deadline.reset();
                        // B4 Task 3: this IS "a byte reached the client" — the commit barrier.
                        // Idempotent, unconditional, and ordered before the return: by the time any
                        // LATER poll on this same stream can yield `Err` (the mid-stream-failure
                        // case), the witness is already `true`.
                        this.commit.mark();
                        let terminal_was_pending =
                            matches!(this.sniffer.terminal, TerminalOutcome::Pending);
                        this.sniffer.feed(&bytes);
                        // A completed generation is progress, not amplification: the same codex
                        // turn id covers every tool-call round of the user turn, so the NEXT
                        // round must start from a fresh aggregate budget. Cleared HERE at the
                        // frame's sighting — codex drops the connection the moment it sees
                        // `response.completed`, so an EOF-side clear loses a race the client is
                        // allowed to win (2026-07-24 live: 3 completed rounds whose clears were
                        // all dropped with the stream, then an instant 400 on round 4). The
                        // transition guard fires exactly once; failure terminals never clear.
                        if terminal_was_pending
                            && matches!(this.sniffer.terminal, TerminalOutcome::Completed { .. })
                        {
                            this.runtime
                                .clear_logical_turn_attempts(this.ctx.logical_turn_key.as_deref());
                        }
                        return Poll::Ready(Some(Ok(bytes)));
                    }
                    Poll::Ready(Some(Err(e))) => {
                        // A3: a mid-stream drop (after the first byte) — the account served a partial
                        // response then failed. Count it as a transient error, then forward the error.
                        this.runtime
                            .record_transient_error(&this.account, unix_now());
                        this.state = ObserveState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Ready(None) => {
                        // Transport EOF is not protocol success. Codex advances LastResponse only
                        // after response.completed, so failed/incomplete/unterminated streams must
                        // neither clear account errors nor advance continuity.
                        let terminal = this.sniffer.finish();
                        match &terminal {
                            TerminalOutcome::Completed { .. } => {
                                this.runtime.record_success(&this.account);
                                // The aggregate turn budget was already cleared when the
                                // `response.completed` frame was sighted in the chunk arm above
                                // (`Completed` only ever arises from `observe_line` — `finish()`
                                // turns a Pending EOF into TransportLoss, never Completed). NOT
                                // repeated here: a second clear could land after the next round
                                // already spent its attempt and would erase that live entry.
                            }
                            TerminalOutcome::Failed {
                                kind: ProtocolFailureKind::RateLimited,
                            } => {
                                this.runtime.record_stream_rate_limit(
                                    &this.account,
                                    None,
                                    unix_now(),
                                );
                                this.runtime.request_usage_refresh(&this.account);
                            }
                            TerminalOutcome::Failed {
                                kind: ProtocolFailureKind::QuotaExceeded,
                            } => {
                                this.runtime
                                    .record_quota_exceeded(&this.account, unix_now());
                                this.runtime.request_usage_refresh(&this.account);
                            }
                            TerminalOutcome::Failed {
                                kind:
                                    ProtocolFailureKind::Transient | ProtocolFailureKind::TransportLoss,
                            } => {
                                this.runtime
                                    .record_transient_error(&this.account, unix_now());
                            }
                            TerminalOutcome::Failed {
                                kind: ProtocolFailureKind::Request,
                            } => {}
                            // `finish()` always converts Pending to TransportLoss; keep the match
                            // exhaustive so that invariant remains local and explicit.
                            TerminalOutcome::Pending => {
                                this.runtime
                                    .record_transient_error(&this.account, unix_now());
                            }
                        }
                        let outcome = build_outcome(
                            this.kind.clone(),
                            this.session_key.clone(),
                            this.account.clone(),
                            terminal,
                        );
                        let continuity = this.continuity.clone();
                        let ctx = this.ctx.clone();
                        let fut = Box::pin(async move {
                            let _ = continuity.observe(outcome, &ctx).await;
                        });
                        this.state = ObserveState::Observing(fut);
                        // loop: poll the observe future this wakeup
                    }
                    Poll::Pending => {
                        // Task 1: the inner stream registered its waker above (it's a real
                        // `poll_next(cx)` call, not skipped) — now ALSO poll the idle deadline so a
                        // wake from EITHER side re-polls this stream. `Disabled` never resolves here
                        // (see `IdleDeadline::poll`), so this is a pure no-op — bare `Poll::Pending`,
                        // byte-for-byte as before this task — whenever the idle timeout is off.
                        return match this.idle_deadline.poll(cx) {
                            Poll::Ready(()) => {
                                // Genuine silence past the deadline. This is POST-commit (bytes were
                                // already relayed — commit-barrier doc on `CommitWitness`), so this
                                // TERMINATES the stream rather than recovering: a reselect here would
                                // double-relay. Treated like any other transient account fault.
                                this.runtime
                                    .record_transient_error(&this.account, unix_now());
                                // Review Minor 1: proactively free the inner stream HERE, a poll
                                // earlier than the consumer would otherwise force it — the next
                                // poll only reaches `ObserveState::Done => Poll::Ready(None)`, which
                                // never touches `this.inner` again, so without this the dead
                                // upstream's `reqwest` response body (and its socket) would sit held
                                // until this whole `ObservingStream` is dropped. Reassigning `inner`
                                // drops the old boxed stream inline, releasing it immediately.
                                // Mirrors the explicit `drop(stream)` on the pre-relay recovery path
                                // above (`Ok(None) | Err(_)` arm of the peek), just applied post-relay.
                                // Terminate semantics are UNCHANGED: still yields the idle `Err` here,
                                // then `Poll::Ready(None)` on the following poll.
                                this.inner = ResponseStream::new(stream::empty());
                                this.state = ObserveState::Done;
                                Poll::Ready(Some(Err(ExecError::Stream(
                                    "upstream idle timeout".into(),
                                ))))
                            }
                            Poll::Pending => Poll::Pending,
                        };
                    }
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

/// `idle_timeout`: Task 1's mid-stream idle deadline, `Duration::ZERO` = disabled (today's
/// behavior). Task 2 will resolve this from config (`POLYFLARE_STREAM_IDLE_TIMEOUT_SECS`,
/// startup-resolved into `AppState`, NOT re-read per request); every caller today passes an
/// explicit value (see each call site).
#[allow(clippy::too_many_arguments)] // internal fn; each param is a distinct, clearly-named handle.
fn wrap_stream(
    inner: ResponseStream,
    continuity: Arc<dyn Continuity>,
    ctx: RequestCtx,
    account: AccountId,
    session_key: Option<SessionKey>,
    kind: OutcomeKind,
    runtime: Arc<RuntimeStates>,
    idle_timeout: Duration,
    commit: CommitWitness,
    // C9 Task 2: the caller's in-flight lease (if any — `None` for callers that never acquired
    // one, e.g. every existing `execute_with_watchdog`/`execute_recovery` caller before this task)
    // moves into the returned stream's `_in_flight` field here, and ONLY here.
    in_flight: Option<InFlightGuard>,
) -> ResponseStream {
    let metadata = inner.metadata().clone();
    ResponseStream::with_metadata(
        ObservingStream {
            inner,
            sniffer: ResponseIdSniffer::new(),
            continuity,
            ctx,
            account,
            session_key,
            kind,
            state: ObserveState::Streaming,
            runtime,
            idle_deadline: IdleDeadline::new(idle_timeout),
            commit,
            _in_flight: in_flight,
        },
        metadata,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hard_owner_affinity_precedes_capacity_weighting() {
        let mut owner = AccountSnapshot::new("owner");
        owner.capacity_credits = Some(1.0);
        owner.secondary_used_percent = 99.0;
        let mut tempting = AccountSnapshot::new("tempting");
        tempting.capacity_credits = Some(1_000_000.0);
        tempting.secondary_used_percent = 0.0;
        let candidates = vec![owner, tempting];
        let directive = ContinuityDirective {
            pin_account: Some(AccountId::from("owner")),
            watchdog: WatchdogArm::Disarmed,
            recovery: RecoveryPlan::None,
            session_key: None,
            require_security_work_authorized: false,
        };

        for seed in 0..100 {
            let ctx = SelectionCtx {
                rng_seed: Some(seed),
                ..Default::default()
            };
            assert!(
                matches!(
                    apply_ownership(
                        &directive,
                        &candidates,
                        &polyflare_core::CapacityWeighted,
                        &ctx,
                    ),
                    RouteDecision::Route(id) if id == AccountId::from("owner")
                ),
                "capacity weighting must only see the already narrowed owner candidate"
            );
        }
    }

    #[test]
    fn response_created_alone_never_commits_continuation() {
        let mut s = ResponseIdSniffer::new();
        s.feed(&Bytes::from_static(
            b"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_42\"}}\n\n",
        ));
        assert_eq!(
            s.finish(),
            TerminalOutcome::Failed {
                kind: ProtocolFailureKind::TransportLoss
            }
        );
    }

    #[test]
    fn response_completed_commits_the_completed_response_id() {
        let mut s = ResponseIdSniffer::new();
        s.feed(&Bytes::from_static(
            b"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\"}}\n\n",
        ));
        s.feed(&Bytes::from_static(
            b"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\"}}\n\n",
        ));
        assert_eq!(
            s.finish(),
            TerminalOutcome::Completed {
                response_id: Some("resp_1".to_string())
            }
        );
    }

    #[test]
    fn response_failed_after_created_never_commits_the_created_id() {
        let mut s = ResponseIdSniffer::new();
        s.feed(&Bytes::from_static(
            b"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_poison\"}}\n\n",
        ));
        s.feed(&Bytes::from_static(
            b"data: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_poison\",\"error\":{\"code\":\"insufficient_quota\"}}}\n\n",
        ));
        assert_eq!(
            s.finish(),
            TerminalOutcome::Failed {
                kind: ProtocolFailureKind::QuotaExceeded
            }
        );
    }

    #[test]
    fn sniffer_resumes_after_an_oversized_non_terminal_line() {
        let mut s = ResponseIdSniffer::new();
        s.feed(&Bytes::from(vec![b'x'; 64 * 1024 + 1]));
        s.feed(&Bytes::from_static(
            b"\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_after_large\"}}\n\n",
        ));
        assert_eq!(
            s.finish(),
            TerminalOutcome::Completed {
                response_id: Some("resp_after_large".to_string())
            }
        );
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
    fn commit_witness_starts_uncommitted_and_marks_true() {
        let w = CommitWitness::new();
        assert!(!w.is_committed(), "a fresh witness starts uncommitted");
        w.mark();
        assert!(w.is_committed(), "mark() flips it to committed");
    }

    #[test]
    fn commit_witness_clones_share_the_same_underlying_flag() {
        // A clone must observe a mark made through the ORIGINAL (and vice versa) — this is the
        // property the caller relies on: hold one clone, thread another into the tracked watchdog
        // call, and read the held clone back later.
        let w = CommitWitness::new();
        let held = w.clone();
        assert!(!held.is_committed());
        w.mark();
        assert!(held.is_committed(), "a clone must see the mark");
    }

    /// B4 Task 3 Step 1 (failing-first): `ObservingStream` marks the [`CommitWitness`] on its FIRST
    /// successfully-yielded chunk, and the mark is still visible after a LATER mid-stream error —
    /// proving "committed" survives past the failure, exactly what Task 4's loop needs to read. This
    /// tests `ObservingStream` directly (white-box, same module) rather than through
    /// `execute_with_watchdog_tracked`, isolating the detection primitive from the watchdog/Armed
    /// peek machinery — the integration-level version (via the public API, a Disarmed request whose
    /// stream yields a byte then errors) lives in `tests/commit_barrier.rs`.
    #[tokio::test]
    async fn observing_stream_marks_commit_on_first_byte_and_it_survives_a_later_error() {
        let inner = ResponseStream::new(stream::iter(vec![
            Ok::<Bytes, ExecError>(Bytes::from_static(b"data: first\n\n")),
            Err(ExecError::Stream("mid-stream drop".into())),
        ]));
        let commit = CommitWitness::new();
        let mut observing = ObservingStream {
            inner,
            sniffer: ResponseIdSniffer::new(),
            continuity: Arc::new(polyflare_core::NoopContinuity),
            ctx: RequestCtx::default(),
            account: AccountId::from("acct"),
            session_key: None,
            kind: OutcomeKind::Completed {
                fp: String::new(),
                count: 0,
            },
            state: ObserveState::Streaming,
            runtime: Arc::new(RuntimeStates::new()),
            idle_deadline: IdleDeadline::new(Duration::ZERO), // disabled: not under test here
            commit: commit.clone(),
            _in_flight: None, // not under test here (C9 Task 2 has its own dedicated tests below)
        };

        assert!(
            !commit.is_committed(),
            "nothing relayed yet before the first poll"
        );
        let first = Pin::new(&mut observing).next().await;
        assert!(matches!(first, Some(Ok(_))), "first chunk relays cleanly");
        assert!(
            commit.is_committed(),
            "committed the instant the first byte was yielded"
        );

        let second = Pin::new(&mut observing).next().await;
        assert!(
            matches!(second, Some(Err(_))),
            "the mid-stream error is still forwarded unchanged"
        );
        assert!(
            commit.is_committed(),
            "committed stays true across the mid-stream failure — it must never un-commit"
        );

        // A3 regression: `record_transient_error` still fired for the mid-stream drop, unperturbed
        // by the new commit-tracking field.
        let mut tracked = vec![polyflare_core::AccountSnapshot::new("acct")];
        observing.runtime.overlay(&mut tracked, 0);
        assert_eq!(
            tracked[0].error_count, 1,
            "record_transient_error still bumped the count exactly once"
        );
    }

    /// C9 Task 2 (THE CRUX — leak-proof-release proof, client disconnect): dropping an
    /// `ObservingStream` BEFORE it is ever polled to completion (simulating a client disconnect
    /// mid-flight — axum drops the response body's stream the instant the client goes away, with
    /// no further poll) must still release the embedded `InFlightGuard`. Proves the field-Drop
    /// mechanism works for the ONE exit path `poll_next`'s own success/error/idle arms can never
    /// see (a drop that never reaches another poll at all) — the exact gap `record_success`/
    /// `record_transient_error` structurally cannot cover (see this module's top-level doc and the
    /// plan's "opposite disconnect-correctness" note).
    #[tokio::test]
    async fn dropping_the_stream_before_polling_to_completion_releases_the_in_flight_lease() {
        let rs = Arc::new(RuntimeStates::new());
        let id = AccountId::from("acct");
        let metrics = crate::observability::LeaseMetrics::new();
        let guard = rs.acquire_in_flight(&id, 0, &metrics);
        let mut snaps = vec![polyflare_core::AccountSnapshot::new("acct")];
        rs.overlay(&mut snaps, 0);
        assert_eq!(
            snaps[0].in_flight, 1,
            "the lease is held before the stream ever exists"
        );
        assert_eq!(
            metrics.acquired(),
            1,
            "acquire_in_flight bumped the counter"
        );
        assert_eq!(metrics.released(), 0, "not yet released");

        // A stream with plenty left to yield — never polled at all here, mirroring a client that
        // disconnects before the server even gets to relay the first byte.
        let inner = ResponseStream::new(stream::iter(vec![
            Ok::<Bytes, ExecError>(Bytes::from_static(b"data: first\n\n")),
            Ok(Bytes::from_static(b"data: second\n\n")),
        ]));
        let observing = ObservingStream {
            inner,
            sniffer: ResponseIdSniffer::new(),
            continuity: Arc::new(polyflare_core::NoopContinuity),
            ctx: RequestCtx::default(),
            account: id.clone(),
            session_key: None,
            kind: OutcomeKind::Completed {
                fp: String::new(),
                count: 0,
            },
            state: ObserveState::Streaming,
            runtime: rs.clone(),
            idle_deadline: IdleDeadline::new(Duration::ZERO),
            commit: CommitWitness::new(),
            _in_flight: Some(guard),
        };

        // The disconnect: drop the whole stream WITHOUT ever calling `.next()`/`poll_next`.
        drop(observing);

        let mut snaps = vec![polyflare_core::AccountSnapshot::new("acct")];
        rs.overlay(&mut snaps, 0);
        assert_eq!(
            snaps[0].in_flight, 0,
            "the lease releases on an unpolled drop — a client disconnect must never leak it"
        );
        assert_eq!(
            metrics.released(),
            1,
            "the guard's Drop bumped the release counter even on an unpolled disconnect"
        );
    }

    /// C9 Task 2: a clean drain (poll to `Poll::Ready(None)`, then drop) releases the lease AND
    /// still fires `record_success` — the field-Drop addition must not interfere with, precede, or
    /// replace the existing wedge bookkeeping in `poll_next`'s `Poll::Ready(None)` arm (that arm is
    /// UNCHANGED by this task; both properties are asserted together as the regression proof).
    #[tokio::test]
    async fn clean_drain_releases_the_lease_and_record_success_still_fires() {
        let rs = Arc::new(RuntimeStates::new());
        let id = AccountId::from("acct");
        // Pre-seed an error so `record_success` actually clearing it (error_count -> 0) is
        // observable, not merely "never touched" — mirrors the existing idle-timeout test's idiom.
        rs.record_transient_error(&id, 0);
        let metrics = crate::observability::LeaseMetrics::new();
        let guard = rs.acquire_in_flight(&id, 0, &metrics);

        let inner = ResponseStream::new(stream::iter(vec![Ok::<Bytes, ExecError>(
            Bytes::from_static(
                b"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\"}}\n\n",
            ),
        )]));
        let mut observing = ObservingStream {
            inner,
            sniffer: ResponseIdSniffer::new(),
            continuity: Arc::new(polyflare_core::NoopContinuity),
            ctx: RequestCtx::default(),
            account: id.clone(),
            session_key: None,
            kind: OutcomeKind::Completed {
                fp: String::new(),
                count: 0,
            },
            state: ObserveState::Streaming,
            runtime: rs.clone(),
            idle_deadline: IdleDeadline::new(Duration::ZERO),
            commit: CommitWitness::new(),
            _in_flight: Some(guard),
        };

        let first = Pin::new(&mut observing).next().await;
        assert!(
            matches!(first, Some(Ok(_))),
            "the one real chunk relays cleanly"
        );
        let end = Pin::new(&mut observing).next().await;
        assert!(
            end.is_none(),
            "clean EOF: Poll::Ready(None) after observe completes"
        );

        // Still holding `observing` here (not yet dropped) — the lease must still be alive; the
        // field's own `Drop` is what releases it, not `poll_next` reaching `Done`.
        let mut snaps = vec![polyflare_core::AccountSnapshot::new("acct")];
        rs.overlay(&mut snaps, 0);
        assert_eq!(
            snaps[0].in_flight, 1,
            "polling to completion alone does not release the lease — only dropping the stream does"
        );
        assert_eq!(
            snaps[0].error_count, 0,
            "record_success fired on clean EOF and cleared the pre-seeded error — unaffected by \
             the new lease field"
        );

        drop(observing);
        let mut snaps = vec![polyflare_core::AccountSnapshot::new("acct")];
        rs.overlay(&mut snaps, 0);
        assert_eq!(
            snaps[0].in_flight, 0,
            "dropping the fully-drained stream releases the lease"
        );
    }

    #[tokio::test]
    async fn request_level_response_failed_does_not_record_or_observe_success() {
        let runtime = Arc::new(RuntimeStates::new());
        let account = AccountId::from("acct");
        runtime.record_transient_error(&account, 0);
        let spy = SpyContinuity::default();
        let last_outcome = spy.last_outcome.clone();
        let inner = ResponseStream::new(stream::iter(vec![Ok::<Bytes, ExecError>(
            Bytes::from_static(
                b"data: {\"type\":\"response.created\",\"response\":{\"id\":\"poison\"}}\n\n\
                  data: {\"type\":\"response.failed\",\"response\":{\"id\":\"poison\",\"error\":{\"code\":\"context_length_exceeded\"}}}\n\n",
            ),
        )]));
        let mut observing = wrap_stream(
            inner,
            Arc::new(spy),
            RequestCtx::default(),
            account.clone(),
            None,
            OutcomeKind::Completed {
                fp: "fp".to_string(),
                count: 1,
            },
            runtime.clone(),
            Duration::ZERO,
            CommitWitness::new(),
            None,
        );

        while observing.next().await.is_some() {}

        assert_eq!(
            *last_outcome.lock().unwrap(),
            Some("failed"),
            "transport EOF after response.failed must observe a failed turn"
        );
        let mut snapshots = vec![AccountSnapshot::new("acct")];
        runtime.overlay(&mut snapshots, 0);
        assert_eq!(
            snapshots[0].error_count, 1,
            "a request-level response.failed must neither reward nor penalize the account"
        );
    }

    #[tokio::test]
    async fn streamed_capacity_failures_update_routing_and_request_fresh_usage() {
        for (code, expect_error, expect_cooldown) in [
            ("rate_limit_exceeded", 1, true),
            ("insufficient_quota", 0, true),
        ] {
            let runtime = Arc::new(RuntimeStates::new());
            let account = AccountId::from(format!("acct-{code}"));
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            runtime.register_usage_refresh(tx);
            let frame = format!(
                "data: {{\"type\":\"response.failed\",\"response\":{{\"error\":{{\"code\":\"{code}\"}}}}}}\n\n"
            );
            let inner = ResponseStream::new(stream::iter(vec![Ok::<Bytes, ExecError>(
                Bytes::from(frame),
            )]));
            let mut observing = wrap_stream(
                inner,
                Arc::new(polyflare_core::NoopContinuity),
                RequestCtx::default(),
                account.clone(),
                None,
                OutcomeKind::Completed {
                    fp: String::new(),
                    count: 0,
                },
                runtime.clone(),
                Duration::ZERO,
                CommitWitness::new(),
                None,
            );
            while observing.next().await.is_some() {}

            let mut snapshots = vec![AccountSnapshot::new(account.as_str())];
            runtime.overlay(&mut snapshots, unix_now());
            assert_eq!(snapshots[0].error_count, expect_error, "{code}");
            assert_eq!(
                snapshots[0].cooldown_until.is_some(),
                expect_cooldown,
                "{code}"
            );
            assert_eq!(rx.try_recv().unwrap(), account, "{code}");
        }
    }

    #[tokio::test]
    async fn clean_eof_without_terminal_event_is_a_transport_failure() {
        let runtime = Arc::new(RuntimeStates::new());
        let account = AccountId::from("acct-unterminated");
        let inner = ResponseStream::new(stream::iter(vec![Ok::<Bytes, ExecError>(
            Bytes::from_static(
                b"data: {\"type\":\"response.created\",\"response\":{\"id\":\"never_done\"}}\n\n",
            ),
        )]));
        let mut observing = wrap_stream(
            inner,
            Arc::new(polyflare_core::NoopContinuity),
            RequestCtx::default(),
            account.clone(),
            None,
            OutcomeKind::Completed {
                fp: String::new(),
                count: 0,
            },
            runtime.clone(),
            Duration::ZERO,
            CommitWitness::new(),
            None,
        );
        while observing.next().await.is_some() {}

        let mut snapshots = vec![AccountSnapshot::new(account.as_str())];
        runtime.overlay(&mut snapshots, unix_now());
        assert_eq!(snapshots[0].error_count, 1);
    }

    /// C9 Task 2: a mid-stream error (the `Poll::Ready(Some(Err(_)))` arm) forwards the error
    /// unchanged (as before this task) and, once the stream is dropped, releases the lease.
    /// `record_transient_error` still fires — same "unchanged bookkeeping" proof as the clean-drain
    /// test above, for the error arm instead of the success arm.
    #[tokio::test]
    async fn mid_stream_error_releases_the_lease_and_record_transient_error_still_fires() {
        let rs = Arc::new(RuntimeStates::new());
        let id = AccountId::from("acct");
        let metrics = crate::observability::LeaseMetrics::new();
        let guard = rs.acquire_in_flight(&id, 0, &metrics);

        let inner = ResponseStream::new(stream::iter(vec![
            Ok::<Bytes, ExecError>(Bytes::from_static(b"data: first\n\n")),
            Err(ExecError::Stream("mid-stream drop".into())),
        ]));
        let mut observing = ObservingStream {
            inner,
            sniffer: ResponseIdSniffer::new(),
            continuity: Arc::new(polyflare_core::NoopContinuity),
            ctx: RequestCtx::default(),
            account: id.clone(),
            session_key: None,
            kind: OutcomeKind::Completed {
                fp: String::new(),
                count: 0,
            },
            state: ObserveState::Streaming,
            runtime: rs.clone(),
            idle_deadline: IdleDeadline::new(Duration::ZERO),
            commit: CommitWitness::new(),
            _in_flight: Some(guard),
        };

        let first = Pin::new(&mut observing).next().await;
        assert!(
            matches!(first, Some(Ok(_))),
            "the first chunk relays cleanly"
        );
        let second = Pin::new(&mut observing).next().await;
        assert!(
            matches!(second, Some(Err(_))),
            "the mid-stream error is forwarded unchanged"
        );

        let mut snaps = vec![polyflare_core::AccountSnapshot::new("acct")];
        rs.overlay(&mut snaps, 0);
        assert_eq!(
            snaps[0].in_flight, 1,
            "the lease is still held right after the error item — releasing is the DROP's job"
        );
        assert_eq!(
            snaps[0].error_count, 1,
            "record_transient_error fired for the mid-stream drop, unaffected by the new lease field"
        );

        drop(observing);
        let mut snaps = vec![polyflare_core::AccountSnapshot::new("acct")];
        rs.overlay(&mut snaps, 0);
        assert_eq!(
            snaps[0].in_flight, 0,
            "dropping the stream after a mid-stream error still releases the lease — no leak on \
             the failure path either"
        );
    }

    /// Regression: a pre-relay failure never marks the witness — proven directly against
    /// `CommitWitness`'s own semantics (a witness that's never had `mark()` called on it, or a
    /// clone of one, always reports `false`). The public-API version (an executor `Err` through
    /// `execute_with_watchdog_tracked`) lives in `tests/commit_barrier.rs`.
    #[test]
    fn a_witness_never_marked_stays_uncommitted() {
        let w = CommitWitness::new();
        // Simulates every pre-relay `Err` path in this module: the witness is constructed, handed
        // in, and the function returns `Err` WITHOUT ever calling `wrap_stream` (so `.mark()` is
        // never reached) — see `CommitWitness`'s doc for the exhaustive list of such sites.
        assert!(!w.is_committed());
    }

    #[test]
    fn scan_detects_cyber_policy_response_failed() {
        let sse = concat!(
            "data: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_x\",",
            "\"error\":{\"code\":\"cyber_policy\",\"message\":\"do not leak this\"}}}\n\n",
        )
        .as_bytes();
        assert!(matches!(
            scan_buffered_frames(sse),
            Some(ScanVerdict::CyberPolicy)
        ));
    }

    #[test]
    fn scan_treats_non_cyber_response_failed_as_decisive() {
        let sse = concat!(
            "data: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_x\",",
            "\"error\":{\"code\":\"server_is_overloaded\",\"message\":\"slow down\"}}}\n\n",
        )
        .as_bytes();
        assert!(matches!(
            scan_buffered_frames(sse),
            Some(ScanVerdict::Decisive)
        ));
    }

    #[test]
    fn scan_treats_content_frame_as_decisive() {
        let sse = b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n";
        assert!(matches!(
            scan_buffered_frames(sse),
            Some(ScanVerdict::Decisive)
        ));
    }

    #[test]
    fn scan_keeps_scanning_past_lifecycle_frames_and_garbage() {
        // response.created / response.in_progress: pure lifecycle, never decisive on their own.
        assert!(scan_buffered_frames(
            b"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\"}}\n\n"
        )
        .is_none());
        assert!(scan_buffered_frames(
            b"data: {\"type\":\"response.in_progress\",\"response\":{\"id\":\"resp_1\"}}\n\n"
        )
        .is_none());
        // Both together: still nothing decisive.
        assert!(scan_buffered_frames(
            concat!(
                "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\"}}\n\n",
                "data: {\"type\":\"response.in_progress\",\"response\":{\"id\":\"resp_1\"}}\n\n",
            )
            .as_bytes()
        )
        .is_none());
        assert!(scan_buffered_frames(b"data: [DONE]\n\n").is_none());
        assert!(scan_buffered_frames(b"not sse at all").is_none());
        assert!(scan_buffered_frames(b"").is_none());
    }

    #[test]
    fn scan_finds_the_decisive_frame_after_buffered_lifecycle_frames() {
        // created (keep scanning) followed by a cyber_policy failure in the SAME buffer (as if two
        // chunks had already been concatenated) — proves the loop looks PAST the first frame.
        let sse = concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\"}}\n\n",
            "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"cyber_policy\"}}}\n\n",
        )
        .as_bytes();
        assert!(matches!(
            scan_buffered_frames(sse),
            Some(ScanVerdict::CyberPolicy)
        ));
    }

    #[test]
    fn capability_rejection_display_never_carries_a_message() {
        let err = WatchdogError::CapabilityRejection {
            capability: "security_work",
        };
        assert_eq!(err.to_string(), "capability rejection: security_work");
        assert!(!format!("{err:?}").contains("message"));
    }

    // ---- Task 1: mid-stream idle deadline (THE CRUX) ---------------------------------------

    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex as StdMutex;

    use async_trait::async_trait;
    use polyflare_core::ContinuityError;

    fn idle_test_account() -> Account {
        Account {
            id: "acct".into(),
            base_url: "http://unused.invalid".into(),
            bearer_token: "tok".into(),
            chatgpt_account_id: None,
            is_fedramp: false,
        }
    }

    fn idle_test_disarmed(body: serde_json::Value) -> Prepared {
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

    /// A test-only `Executor` whose stream yields exactly ONE real byte, then never yields again
    /// (no `Poll::Ready` of any kind — genuine silence, not a clean EOF). Counts its own calls so
    /// tests can assert no second attempt was ever made (the commit barrier: this module never
    /// reselects post-relay).
    struct ByteThenStallExecutor {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Executor for ByteThenStallExecutor {
        async fn execute(
            &self,
            _req: PreparedRequest,
            _account: &Account,
            _ctx: &RequestCtx,
        ) -> Result<ResponseStream, ExecError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let first = Ok::<Bytes, ExecError>(Bytes::from_static(b"data: first\n\n"));
            Ok(ResponseStream::new(
                stream::once(async move { first })
                    .chain(stream::pending::<Result<Bytes, ExecError>>()),
            ))
        }
    }

    #[tokio::test]
    async fn tracked_execution_rejects_a_second_request_for_the_same_logical_turn() {
        let calls = Arc::new(AtomicUsize::new(0));
        let executor = ByteThenStallExecutor {
            calls: calls.clone(),
        };
        let runtime = Arc::new(RuntimeStates::new());
        let ctx = RequestCtx {
            logical_turn_key: Some("hashed-logical-turn".to_string()),
            ..RequestCtx::default()
        };
        let prepared = idle_test_disarmed(serde_json::json!({"input": []}));

        let first = execute_with_watchdog_tracked(
            &executor,
            Arc::new(polyflare_core::NoopContinuity),
            prepared.clone(),
            &idle_test_account(),
            AccountId::from("acct"),
            ctx.clone(),
            runtime.clone(),
            Duration::ZERO,
            1,
            CommitWitness::new(),
            None,
        )
        .await;
        assert!(first.is_ok());
        drop(first);

        let second = execute_with_watchdog_tracked(
            &executor,
            Arc::new(polyflare_core::NoopContinuity),
            prepared,
            &idle_test_account(),
            AccountId::from("acct"),
            ctx,
            runtime,
            Duration::ZERO,
            1,
            CommitWitness::new(),
            None,
        )
        .await;
        let error = second.err().expect("the second attempt must be rejected");
        assert!(matches!(&error, WatchdogError::AttemptBudgetExhausted));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(!error.to_string().contains("hashed-logical-turn"));
    }

    /// The budget bounds attempts WITHOUT progress, not generations: codex reuses one turn id for
    /// every tool-call round of a user turn, so a stream that terminates with
    /// `response.completed` must clear the turn's spent attempts — otherwise round
    /// `max_account_attempts + 1` of a healthy tool loop is rejected as amplification (the
    /// 2026-07-24 live regression: 3 completed rounds, then an instant 400).
    #[tokio::test]
    async fn completed_stream_clears_the_logical_turn_budget_for_the_next_round() {
        let runtime = Arc::new(RuntimeStates::new());
        let ctx = RequestCtx {
            logical_turn_key: Some("hashed-logical-turn".to_string()),
            ..RequestCtx::default()
        };
        // The round's own attempt (spent at execute entry) exhausts a limit of 1.
        assert!(runtime.try_consume_logical_turn_attempt(ctx.logical_turn_key.as_deref(), 1, 0));
        assert!(!runtime.try_consume_logical_turn_attempt(ctx.logical_turn_key.as_deref(), 1, 0));

        let inner = ResponseStream::new(stream::once(async {
            Ok::<Bytes, ExecError>(Bytes::from_static(
                b"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\"}}\n\n",
            ))
        }));
        let mut wrapped = wrap_stream(
            inner,
            Arc::new(polyflare_core::NoopContinuity) as Arc<dyn Continuity>,
            ctx.clone(),
            AccountId::from("acct"),
            None,
            OutcomeKind::Completed {
                fp: String::new(),
                count: 0,
            },
            runtime.clone(),
            Duration::ZERO, // idle watchdog disabled: not under test here
            CommitWitness::new(),
            None,
        );
        while let Some(item) = wrapped.next().await {
            item.expect("the completing stream must relay cleanly");
        }

        assert!(
            runtime.try_consume_logical_turn_attempt(ctx.logical_turn_key.as_deref(), 1, 1),
            "a forwarded response.completed must reset the aggregate budget for the next round"
        );
    }

    /// The clear must happen when the `response.completed` FRAME is relayed, not at the post-EOF
    /// poll: codex drops the connection the moment it sees the terminal frame, so the wrapper is
    /// dropped without ever being polled to `Poll::Ready(None)`. (The 2026-07-24 second live
    /// regression: rounds logged `protocol_outcome=completed`, yet the budget kept accumulating —
    /// every EOF-side clear lost the disconnect race — until round 4 hit an instant 400.)
    #[tokio::test]
    async fn budget_clears_at_completed_frame_sighting_even_if_client_drops_before_eof() {
        let runtime = Arc::new(RuntimeStates::new());
        let ctx = RequestCtx {
            logical_turn_key: Some("hashed-logical-turn".to_string()),
            ..RequestCtx::default()
        };
        // The round's own attempt (spent at execute entry) exhausts a limit of 1.
        assert!(runtime.try_consume_logical_turn_attempt(ctx.logical_turn_key.as_deref(), 1, 0));
        assert!(!runtime.try_consume_logical_turn_attempt(ctx.logical_turn_key.as_deref(), 1, 0));

        // The terminal frame, then genuine silence — never a clean EOF. The client hangs up
        // after the completed frame, so the wrapper is dropped mid-`Streaming`.
        let inner = ResponseStream::new(
            stream::once(async {
                Ok::<Bytes, ExecError>(Bytes::from_static(
                    b"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\"}}\n\n",
                ))
            })
            .chain(stream::pending::<Result<Bytes, ExecError>>()),
        );
        let mut wrapped = wrap_stream(
            inner,
            Arc::new(polyflare_core::NoopContinuity) as Arc<dyn Continuity>,
            ctx.clone(),
            AccountId::from("acct"),
            None,
            OutcomeKind::Completed {
                fp: String::new(),
                count: 0,
            },
            runtime.clone(),
            Duration::ZERO, // idle watchdog disabled: not under test here
            CommitWitness::new(),
            None,
        );
        wrapped
            .next()
            .await
            .expect("the completed frame must be relayed")
            .expect("the completed frame must relay cleanly");
        drop(wrapped); // the client disconnect: no EOF poll ever happens

        assert!(
            runtime.try_consume_logical_turn_attempt(ctx.logical_turn_key.as_deref(), 1, 1),
            "sighting the completed frame must reset the aggregate budget even when the client \
             disconnects before EOF"
        );
    }

    /// A `Continuity` that only counts `observe` calls — used to prove `observe` DID/DID NOT run,
    /// without depending on `NoopContinuity`'s (silent) internals.
    #[derive(Clone, Default)]
    struct SpyContinuity {
        observe_calls: Arc<AtomicUsize>,
        last_outcome: Arc<StdMutex<Option<&'static str>>>,
    }

    #[async_trait]
    impl Continuity for SpyContinuity {
        async fn prepare(
            &self,
            _req: PreparedRequest,
            _ctx: &RequestCtx,
        ) -> Result<Prepared, ContinuityError> {
            unimplemented!("not exercised by these tests — only observe() is under test")
        }

        async fn observe(
            &self,
            outcome: TurnOutcome,
            _ctx: &RequestCtx,
        ) -> Result<(), ContinuityError> {
            self.observe_calls.fetch_add(1, Ordering::SeqCst);
            *self.last_outcome.lock().unwrap() = Some(match outcome {
                TurnOutcome::Completed { .. } => "completed",
                TurnOutcome::Recovered { .. } => "recovered",
                TurnOutcome::Failed { .. } => "failed",
            });
            Ok(())
        }

        async fn mark_required_capability(
            &self,
            _session_key: &SessionKey,
            _capability: &'static str,
        ) -> Result<(), ContinuityError> {
            unimplemented!("not exercised by these tests — only observe() is under test")
        }
    }

    /// Step 1 (failing-first): a stream that relays one byte then goes genuinely silent forever
    /// must be TERMINATED by `ObservingStream` once the configured idle window elapses — proving
    /// the gap this task closes (`Poll::Pending` returned forever today). Bounded by an outer
    /// `tokio::time::timeout` so a regression (no idle path) fails as "timed out", never hangs
    /// CI. Uses a short (250ms) idle window so the test is fast.
    #[tokio::test]
    async fn wrap_stream_terminates_after_genuine_mid_stream_silence() {
        let idle = Duration::from_millis(250);
        let inner = ResponseStream::new(
            stream::once(async { Ok::<Bytes, ExecError>(Bytes::from_static(b"data: first\n\n")) })
                .chain(stream::pending::<Result<Bytes, ExecError>>()),
        );
        let runtime = Arc::new(RuntimeStates::new());
        let mut wrapped = wrap_stream(
            inner,
            Arc::new(polyflare_core::NoopContinuity),
            RequestCtx::default(),
            AccountId::from("acct"),
            None,
            OutcomeKind::Completed {
                fp: String::new(),
                count: 0,
            },
            runtime.clone(),
            idle,
            CommitWitness::new(),
            None,
        );

        tokio::time::timeout(Duration::from_secs(3), async {
            let first = wrapped.next().await;
            assert!(
                matches!(first, Some(Ok(ref b)) if b.as_ref() == b"data: first\n\n"),
                "the first byte relays cleanly before any silence: {first:?}"
            );

            let start = tokio::time::Instant::now();
            let second = wrapped.next().await;
            let elapsed = start.elapsed();
            match second {
                Some(Err(ExecError::Stream(msg))) => {
                    assert_eq!(
                        msg, "upstream idle timeout",
                        "the idle error is a fixed, content-free reason string"
                    );
                }
                other => panic!("expected an idle-timeout Err, got {other:?}"),
            }
            assert!(
                elapsed >= idle,
                "the idle error must not fire before the configured window: {elapsed:?} < {idle:?}"
            );

            let third = wrapped.next().await;
            assert!(
                third.is_none(),
                "after the idle error the stream is terminal (Done): {third:?}"
            );
        })
        .await
        .expect(
            "bounded: a genuine mid-stream stall must terminate within the idle window, not hang",
        );

        let mut tracked = vec![AccountSnapshot::new("acct")];
        runtime.overlay(&mut tracked, 0);
        assert_eq!(
            tracked[0].error_count, 1,
            "record_transient_error fired exactly once for the idle timeout"
        );
    }

    /// Same gap, driven through the PUBLIC API (`execute_with_watchdog_tracked`, Disarmed) so the
    /// commit-barrier claim is checked for real: after the idle timeout, `commit.is_committed()`
    /// must read `true` (a byte WAS relayed) and the executor must have been called EXACTLY ONCE —
    /// no reselect/retry, because retrying post-commit would double-relay.
    #[tokio::test]
    async fn idle_timeout_is_post_commit_and_never_triggers_a_second_attempt() {
        let calls = Arc::new(AtomicUsize::new(0));
        let exec = ByteThenStallExecutor {
            calls: calls.clone(),
        };
        let commit = CommitWitness::new();
        let runtime = Arc::new(RuntimeStates::new());

        let stream = execute_with_watchdog_tracked(
            &exec,
            Arc::new(polyflare_core::NoopContinuity),
            idle_test_disarmed(serde_json::json!({"input": [{"a":1}]})),
            &idle_test_account(),
            AccountId::from("acct"),
            RequestCtx::default(),
            runtime,
            Duration::from_millis(250),
            3,
            commit.clone(),
            None,
        )
        .await
        .expect("Disarmed relays immediately: Ok(stream)");

        let mut s = stream;
        tokio::time::timeout(Duration::from_secs(3), async {
            let first = s.next().await;
            assert!(matches!(first, Some(Ok(_))), "first byte relays cleanly");
            assert!(
                commit.is_committed(),
                "committed the instant the byte was relayed"
            );

            let second = s.next().await;
            assert!(
                matches!(second, Some(Err(ExecError::Stream(_)))),
                "the idle timeout surfaces as an Err item inside the already-Ok stream: {second:?}"
            );
        })
        .await
        .expect("bounded: must not hang");

        assert!(
            commit.is_committed(),
            "commit stays true — the idle path never un-commits"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "no reselect/second attempt: a mid-stream idle timeout is post-commit"
        );
    }

    /// **No spurious timeout.** A stream producing a byte every `idle/2`, then a clean EOF, must
    /// complete NORMALLY — no idle error, `record_success` fires (clearing a pre-seeded error),
    /// and `observe` runs. This proves the deadline genuinely RESETS on every byte rather than
    /// being some fixed budget for the whole stream.
    #[tokio::test]
    async fn no_spurious_idle_timeout_when_bytes_keep_arriving_within_the_window() {
        let idle = Duration::from_millis(200);
        let gap = idle / 2;
        // 4 live chunks, each within `gap` of the last, then a clean end (`None`).
        let chunks = vec![
            Bytes::from_static(b"data: chunk-0\n\n"),
            Bytes::from_static(b"data: chunk-1\n\n"),
            Bytes::from_static(b"data: chunk-2\n\n"),
            Bytes::from_static(
                b"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\"}}\n\n",
            ),
        ];
        let inner = ResponseStream::new(stream::unfold(
            chunks.into_iter(),
            move |mut it| async move {
                let next = it.next()?;
                tokio::time::sleep(gap).await;
                Some((Ok::<Bytes, ExecError>(next), it))
            },
        ));

        let runtime = Arc::new(RuntimeStates::new());
        // Pre-seed an error so a later `error_count == 0` proves `record_success` actually ran
        // (not merely "was never touched").
        runtime.record_transient_error(&AccountId::from("acct"), 0);
        let observe_calls = Arc::new(AtomicUsize::new(0));
        let continuity: Arc<dyn Continuity> = Arc::new(SpyContinuity {
            observe_calls: observe_calls.clone(),
            last_outcome: Default::default(),
        });

        let mut wrapped = wrap_stream(
            inner,
            continuity,
            RequestCtx::default(),
            AccountId::from("acct"),
            None,
            OutcomeKind::Completed {
                fp: String::new(),
                count: 0,
            },
            runtime.clone(),
            idle,
            CommitWitness::new(),
            None,
        );

        tokio::time::timeout(Duration::from_secs(3), async {
            let mut yielded = 0;
            while let Some(item) = wrapped.next().await {
                match item {
                    Ok(_) => yielded += 1,
                    Err(e) => panic!("no idle error must fire while bytes keep arriving: {e}"),
                }
            }
            assert_eq!(yielded, 4, "all 4 live chunks relayed before the clean end");
        })
        .await
        .expect("bounded: a genuinely-alive stream must complete, not hang");

        let mut tracked = vec![AccountSnapshot::new("acct")];
        runtime.overlay(&mut tracked, 0);
        assert_eq!(
            tracked[0].error_count, 0,
            "record_success cleared the pre-seeded error on clean completion"
        );
        assert_eq!(
            observe_calls.load(Ordering::SeqCst),
            1,
            "continuity.observe ran exactly once at true stream end"
        );
    }

    /// **Disabled (timeout 0).** A byte-then-stall stream with the idle timeout DISABLED must
    /// behave byte-for-byte as today: bare `Poll::Pending` forever, no idle termination. Proven by
    /// racing the second poll against a SHORT external timeout and asserting THAT elapses (the
    /// stream itself never resolves) — this keeps the test bounded without ever depending on the
    /// stream actually finishing.
    #[tokio::test]
    async fn disabled_idle_timeout_never_terminates_a_stalled_stream() {
        let inner = ResponseStream::new(
            stream::once(async { Ok::<Bytes, ExecError>(Bytes::from_static(b"data: first\n\n")) })
                .chain(stream::pending::<Result<Bytes, ExecError>>()),
        );
        let runtime = Arc::new(RuntimeStates::new());
        let mut wrapped = wrap_stream(
            inner,
            Arc::new(polyflare_core::NoopContinuity),
            RequestCtx::default(),
            AccountId::from("acct"),
            None,
            OutcomeKind::Completed {
                fp: String::new(),
                count: 0,
            },
            runtime.clone(),
            Duration::ZERO, // disabled
            CommitWitness::new(),
            None,
        );

        let first = wrapped.next().await;
        assert!(matches!(first, Some(Ok(_))), "first byte still relays");

        // The disabled path must NOT resolve within this short window — if it does (idle error
        // or anything else), the outer `timeout` here would return `Ok(_)` instead of `Err`
        // (elapsed), and the assert below catches that as a real failure, bounded.
        let raced = tokio::time::timeout(Duration::from_millis(300), wrapped.next()).await;
        assert!(
            raced.is_err(),
            "disabled (timeout=0) must never terminate a genuine stall: {raced:?}"
        );

        let mut tracked = vec![AccountSnapshot::new("acct")];
        runtime.overlay(&mut tracked, 0);
        assert_eq!(
            tracked[0].error_count, 0,
            "disabled path never calls record_transient_error"
        );
    }

    /// A byte-then-stall inner stream that flips `dropped` the instant it is DROPPED (not merely
    /// exhausted) — used to prove Task 2's Minor 1: on idle-terminate, `ObservingStream` frees
    /// `this.inner` proactively (a poll earlier than the consumer forcing it), rather than holding
    /// the dead upstream's socket until the whole `ObservingStream` itself is later dropped.
    struct DropSpyStall {
        yielded: bool,
        dropped: Arc<AtomicBool>,
    }

    impl Stream for DropSpyStall {
        type Item = Result<Bytes, ExecError>;
        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let this = self.get_mut();
            if !this.yielded {
                this.yielded = true;
                Poll::Ready(Some(Ok(Bytes::from_static(b"data: first\n\n"))))
            } else {
                Poll::Pending // genuine silence — never resolves again
            }
        }
    }

    impl Drop for DropSpyStall {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    /// Minor 1 (from the Task 1 review): on idle-terminate, `ObservingStream` must free its inner
    /// stream PROACTIVELY — the instant the idle deadline fires, not merely whenever the whole
    /// `ObservingStream` eventually gets dropped. Proven directly: the inner stream's `Drop` flips
    /// a flag, and this test asserts that flag is set immediately after the idle `Err` poll
    /// returns — BEFORE the consumer ever polls again for the terminal `None`. Terminate semantics
    /// are unchanged (still `Err` then `None`), asserted alongside.
    #[tokio::test]
    async fn idle_terminate_proactively_drops_the_inner_stream() {
        let idle = Duration::from_millis(200);
        let dropped = Arc::new(AtomicBool::new(false));
        let inner = ResponseStream::new(DropSpyStall {
            yielded: false,
            dropped: dropped.clone(),
        });
        let runtime = Arc::new(RuntimeStates::new());
        let mut wrapped = wrap_stream(
            inner,
            Arc::new(polyflare_core::NoopContinuity),
            RequestCtx::default(),
            AccountId::from("acct"),
            None,
            OutcomeKind::Completed {
                fp: String::new(),
                count: 0,
            },
            runtime,
            idle,
            CommitWitness::new(),
            None,
        );

        tokio::time::timeout(Duration::from_secs(3), async {
            let first = wrapped.next().await;
            assert!(matches!(first, Some(Ok(_))), "first byte relays cleanly");
            assert!(
                !dropped.load(Ordering::SeqCst),
                "inner must still be alive right after the first byte — nothing to free yet"
            );

            let second = wrapped.next().await;
            assert!(
                matches!(second, Some(Err(ExecError::Stream(_)))),
                "the idle timeout still surfaces as an Err item: {second:?}"
            );
            assert!(
                dropped.load(Ordering::SeqCst),
                "the inner stream must be dropped THE MOMENT the idle Err is yielded — before the \
                 consumer's next poll, not after"
            );

            let third = wrapped.next().await;
            assert!(
                third.is_none(),
                "terminate semantics unchanged: idle Err THEN a clean None: {third:?}"
            );
        })
        .await
        .expect("bounded: must not hang");
    }
}
