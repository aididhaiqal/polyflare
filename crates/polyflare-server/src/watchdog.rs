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
pub async fn execute_with_watchdog(
    executor: &dyn Executor,
    continuity: Arc<dyn Continuity>,
    prepared: Prepared,
    account: &Account,
    account_id: AccountId,
    ctx: RequestCtx,
    runtime: Arc<RuntimeStates>,
) -> Result<ResponseStream, WatchdogError> {
    execute_with_watchdog_tracked(
        executor,
        continuity,
        prepared,
        account,
        account_id,
        ctx,
        runtime,
        CommitWitness::new(),
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
    commit: CommitWitness,
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
                commit,
            ))
        }
        WatchdogArm::Armed { timeout } => {
            let mut stream = executor
                .execute(req, account, &ctx)
                .await
                .map_err(|e| WatchdogError::Upstream(e.failure_signal()))?;
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
                                commit,
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
                                commit,
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
                        commit,
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
    commit: CommitWitness,
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
                commit,
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
/// watchdog. Sniffs the new id and observes `Recovered`.
///
/// Signature-identical to before B4 Task 3 (see [`execute_with_watchdog`]'s doc for why) — a thin
/// delegator over [`execute_recovery_tracked`] with a throwaway [`CommitWitness`].
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
    execute_recovery_tracked(
        executor,
        continuity,
        anchorless_req,
        account,
        account_id,
        ctx,
        session_key,
        runtime,
        CommitWitness::new(),
    )
    .await
}

/// B4 Task 4 hook: identical behavior to [`execute_recovery`], additionally threading a
/// [`CommitWitness`] through to the wrapped stream — see [`execute_with_watchdog_tracked`]'s doc
/// for the general shape.
#[allow(clippy::too_many_arguments)] // mirrors `execute_recovery`; one added handle.
pub async fn execute_recovery_tracked(
    executor: &dyn Executor,
    continuity: Arc<dyn Continuity>,
    anchorless_req: PreparedRequest,
    account: &Account,
    account_id: AccountId,
    ctx: RequestCtx,
    session_key: Option<SessionKey>,
    runtime: Arc<RuntimeStates>,
    commit: CommitWitness,
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
        commit,
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

    let rebuilt: ResponseStream =
        Box::pin(stream::iter(relay_chunks.into_iter().map(Ok::<Bytes, ExecError>)).chain(stream));
    ScanOutcome::Alive(rebuilt)
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
    /// B4 Task 3: marked the instant the FIRST chunk is successfully yielded below — a pure
    /// addition, read by no code in this struct. It does not gate, delay, or otherwise touch relay
    /// (every chunk is still forwarded unconditionally, in order) or the `record_transient_error`/
    /// `record_success`/`observe` calls, which fire exactly as before Task 3.
    commit: CommitWitness,
}

impl Stream for ObservingStream {
    type Item = Result<Bytes, ExecError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut(); // ObservingStream is Unpin (all fields Unpin)
        loop {
            match &mut this.state {
                ObserveState::Streaming => match this.inner.as_mut().poll_next(cx) {
                    Poll::Ready(Some(Ok(bytes))) => {
                        // B4 Task 3: this IS "a byte reached the client" — the commit barrier.
                        // Idempotent, unconditional, and ordered before the return: by the time any
                        // LATER poll on this same stream can yield `Err` (the mid-stream-failure
                        // case), the witness is already `true`.
                        this.commit.mark();
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

#[allow(clippy::too_many_arguments)] // internal fn; each param is a distinct, clearly-named handle.
fn wrap_stream(
    inner: ResponseStream,
    continuity: Arc<dyn Continuity>,
    ctx: RequestCtx,
    account: AccountId,
    session_key: Option<SessionKey>,
    kind: OutcomeKind,
    runtime: Arc<RuntimeStates>,
    commit: CommitWitness,
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
        commit,
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
        let inner: ResponseStream = Box::pin(stream::iter(vec![
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
            commit: commit.clone(),
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
}
