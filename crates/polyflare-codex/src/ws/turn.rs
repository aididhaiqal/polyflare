//! The per-turn WS stream (M5a Task 5, `docs/superpowers/plans/
//! 2026-07-17-polyflare-m5a-upstream-websocket.md` "Task 5: The turn stream — ends while the
//! socket stays open").
//!
//! **This is the milestone.** Everything else in M5a is scaffolding around one property: two
//! sequential turns over the same [`WsConn`] must leave `MockWsUpstream::handshake_count() == 1`.
//! If that doesn't hold, the connection isn't reused, no anchor survives across turns, no delta
//! (Task 6) is ever possible, and WS delivers nothing over HTTP-SSE that HTTP-SSE didn't already
//! have.
//!
//! ## The two non-obvious constraints this module exists to satisfy
//!
//! 1. **The per-turn stream ENDS; the socket does NOT.** [`turn_stream`] returns a
//!    [`ResponseStream`] that terminates (`Poll::Ready(None)`) at the terminal frame
//!    (`response.completed` / `.failed` / `.incomplete`) or at a classified error/anchor-miss —
//!    without ever closing the underlying WS connection. `Continuity::observe` (which writes the
//!    `response_id → owner` anchor map) and the routing-health `record_success`/
//!    `record_transient_error` writeback both fire from `ObservingStream` at TRUE stream end
//!    (`polyflare-server/src/watchdog.rs:393-417`) — a socket that outlives the turn must still
//!    end its *stream*, or ownership stops being recorded and every turn re-anchors, which is
//!    exactly the wedge failure this milestone exists to prevent.
//! 2. **The stream's items are SSE bytes, not raw WS frames.** [`ResponseStream`] is
//!    `Pin<Box<dyn Stream<Item = Result<Bytes, ExecError>> + Send>>`
//!    (`polyflare-core/src/types.rs:87`), and the watchdog's `ResponseIdSniffer` +
//!    `TranslatingStream::feed_line` both parse `data:`-prefixed lines out of the raw `Bytes`.
//!    Every yielded chunk here goes through Task 4's `codec::frame_to_sse` — never a raw WS
//!    payload.
//!
//! ## How the socket stays open across turns: one `tokio::sync::Mutex`, held for one turn
//!
//! Ground truth §4: "**Strictly one in-flight turn per socket.**... `stream_request` holds
//! `stream.lock().await` for the entire lifetime of the response stream... There is no
//! request-id in the wire protocol — correlation is implicit." [`SharedWsConn`] is
//! `Arc<tokio::sync::Mutex<WsConn>>`; [`turn_stream`] acquires an *owned* lock guard
//! (`Mutex::lock_owned`, satisfying `ResponseStream`'s `'static` bound) for the turn's whole
//! duration and only drops it — releasing the connection for reuse — once the stream reaches
//! `State::Done` (a terminal frame, a classified error, an anchor-miss, or the socket closing
//! mid-turn). This is a direct, 1:1 translation of codex's own concurrency model: the mutex IS
//! the "cache slot" a real connection cache (Task 7, not built here) would park a `SharedWsConn`
//! handle in — the caller just calls [`turn_stream`] again on the same handle for the next turn.
//!
//! ## `FrameClass::AnchorMiss` — classified here, NOT recovered here
//!
//! Per this task's explicit scope: "`AnchorMiss` handling (the recovery itself) is Task 7's, not
//! yours; classify it and let it surface." This module calls `classify()` (never re-derives frame
//! semantics) and, on `AnchorMiss`, ends the turn's stream with an `ExecError::Stream` describing
//! the condition — it does NOT strip the anchor and resend on the same socket (that's Task 7's
//! recovery logic). Ending the stream (rather than looping forever) still satisfies constraint 1
//! above: the connection's lock is released the same way it would be for any other terminal
//! outcome, so a future recovery attempt can immediately reuse the same still-open socket.
//!
//! ## Content-safety
//!
//! `envelope` (the outbound `response.create` body) and every received frame's text carry
//! conversation content. Neither [`TurnStream`] nor its private `State` enum derives `Debug`, and
//! nothing in this module logs a frame or the envelope — mirrors `codec.rs`'s and `delta.rs`'s own
//! convention (`polyflare-core/src/types.rs:42-50`'s redaction precedent).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_core::Stream;
use serde_json::Value;
use tokio::sync::{Mutex, OwnedMutexGuard};

use polyflare_core::{ExecError, ResponseStream};

use super::codec::{classify, frame_to_sse, FrameClass};
use super::conn::WsConn;
use super::delta::{item_hashes, non_input_fingerprint};

/// Substring markers embedded in the `ExecError::Stream` messages this module produces for the
/// turn-stream-level conditions Task 7's `ws::executor::CodexWsExecutor` recovers from
/// (`AnchorMiss`, `ConnectionLimitReached`, and a close/end before any terminal frame). Shared
/// between the PRODUCER (this module) and the ONLY consumer (`ws::executor`'s
/// `classify_recovery`) so the two can never drift out of sync — a deliberate, documented choice
/// over adding a `code` field to `ExecError`/`FailureSignal` (which would ripple into every other
/// `Executor` impl and caller in the workspace for a need that is WS-only).
pub(crate) const ANCHOR_MISS_MARKER: &str = "previous_response_not_found";
pub(crate) const CONNECTION_LIMIT_MARKER: &str = "websocket_connection_limit_reached";
pub(crate) const SOCKET_CLOSED_MARKER: &str = "closed by server before response.completed";

/// A [`WsConn`] shared across turns: `Arc` so a caller (eventually Task 7's connection cache) can
/// hold one handle across many sequential [`turn_stream`] calls, `tokio::sync::Mutex` so exactly
/// one turn is ever in flight on the underlying socket at a time (ground truth §4) and so the lock
/// guard held across a turn's `.await` points can be made `'static` via `lock_owned` — required by
/// [`ResponseStream`]'s own `'static`-implying `Pin<Box<dyn Stream<..> + Send>>` shape.
pub type SharedWsConn = Arc<Mutex<WsConn>>;

/// Wrap a freshly-connected [`WsConn`] for sharing across turns. A thin, deliberately-boring
/// convenience — Task 7's connection cache is free to construct `Arc::new(Mutex::new(conn))`
/// itself instead; this just avoids every caller (including this module's own tests) repeating it.
pub fn shared_conn(conn: WsConn) -> SharedWsConn {
    Arc::new(Mutex::new(conn))
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// A read step's outcome: the guard threaded back through (so the NEXT step can keep using the
/// same connection without re-acquiring the lock) paired with what `recv_frame` returned.
type ReadOutcome = (OwnedMutexGuard<WsConn>, Result<Option<String>, ExecError>);

/// One step of [`TurnStream`]'s state machine. Both variants carry the [`OwnedMutexGuard`] as part
/// of their future's `Output` so it threads through every `.await` point of the turn without ever
/// being dropped mid-turn — the guard (and therefore the connection's lock) is dropped ONLY when
/// the state becomes [`Done`](TurnState::Done), which is the single point this module "parks the
/// connection back" for reuse.
enum TurnState {
    /// Awaiting the initial `response.create` send. On success, carries the guard forward into
    /// `Reading`; on failure, the guard is dropped here (send failed, nothing to read).
    Sending(BoxFuture<Result<OwnedMutexGuard<WsConn>, ExecError>>),
    /// Awaiting the next frame off the socket.
    Reading(BoxFuture<ReadOutcome>),
    /// The turn is over. The socket is untouched; only this `TurnStream`'s hold on the connection
    /// (the guard, dropped on the transition INTO this state) has ended.
    Done,
}

/// A `Stream` over one WS turn's frames, re-framed as SSE `Bytes` — the concrete type behind the
/// [`ResponseStream`] [`turn_stream`] returns. Never constructed directly by a caller.
struct TurnStream {
    state: TurnState,
}

impl Stream for TurnStream {
    type Item = Result<Bytes, ExecError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // `TurnStream` is Unpin: `state` is a plain enum whose only heap indirection is
        // `Pin<Box<dyn Future + Send>>`, which is itself always `Unpin` regardless of what it
        // points to (mirrors `polyflare-server/src/watchdog.rs`'s `ObservingStream` comment).
        let this = self.get_mut();
        loop {
            match &mut this.state {
                TurnState::Sending(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(guard)) => {
                        this.state = TurnState::Reading(Box::pin(read_next(guard)));
                    }
                    Poll::Ready(Err(e)) => {
                        this.state = TurnState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                TurnState::Reading(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready((mut guard, Ok(Some(text)))) => {
                        let Ok(value) = serde_json::from_str::<Value>(&text) else {
                            // Malformed frame from the backend: drop it silently, keep reading —
                            // mirrors `codec::frame_to_sse`'s own "unparseable => drop" choice
                            // rather than propagating garbage downstream.
                            this.state = TurnState::Reading(Box::pin(read_next(guard)));
                            continue;
                        };
                        match classify(&value) {
                            FrameClass::Event => {
                                let bytes = frame_to_sse(&text);
                                this.state = TurnState::Reading(Box::pin(read_next(guard)));
                                if let Some(bytes) = bytes {
                                    return Poll::Ready(Some(Ok(bytes)));
                                }
                                // Re-serialization somehow failed for a value that just parsed
                                // fine (should not happen in practice) — drop and keep reading,
                                // same defensive default as the malformed-frame branch above.
                                continue;
                            }
                            FrameClass::Terminal => {
                                // Ground truth §3 (`client.rs:1998-2018`): `LastResponse` comes
                                // from `response.completed`'s `response.id` on THIS connection —
                                // Task 6's delta planning reads this back off `WsConn`.
                                if let Some(id) =
                                    value.pointer("/response/id").and_then(Value::as_str)
                                {
                                    guard.last_response_id = Some(id.to_string());
                                }
                                this.state = TurnState::Done; // guard dropped: parked, unlocked.
                                return Poll::Ready(frame_to_sse(&text).map(Ok));
                            }
                            FrameClass::AnchorMiss => {
                                // Classified, not recovered — see module doc. `guard` drops here.
                                this.state = TurnState::Done;
                                return Poll::Ready(Some(Err(ExecError::Stream(format!(
                                    "{ANCHOR_MISS_MARKER}: anchor miss on this turn (recovery is \
                                     ws::executor's CodexWsExecutor's concern, not resolved by \
                                     the turn stream itself)"
                                )))));
                            }
                            FrameClass::ConnectionLimitReached => {
                                // Same treatment as AnchorMiss above: classified here, recovered by
                                // `ws::executor::CodexWsExecutor` (reconnect + full resend,
                                // bounded), never by this stream itself.
                                this.state = TurnState::Done;
                                return Poll::Ready(Some(Err(ExecError::Stream(format!(
                                    "{CONNECTION_LIMIT_MARKER}: server's WS connection cap hit \
                                     (recovery is ws::executor's CodexWsExecutor's concern)"
                                )))));
                            }
                            FrameClass::Error(e) => {
                                this.state = TurnState::Done; // `guard` drops here.
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                    }
                    Poll::Ready((_guard, Ok(None))) => {
                        // Ground truth §3 (`responses_websocket.rs:800-804`): a `Close` frame (no
                        // close-code inspection) or the stream simply ending both mean "closed
                        // before any terminal frame" — the shape `watchdog.rs`'s `record_
                        // transient_error` writeback (`:393-399`) needs to see as a genuine error,
                        // not a silent `None`. `_guard` drops here — in THIS case the underlying
                        // socket is actually gone too (the peer closed it), so there is nothing
                        // left to "park"; that distinction is Task 7's reconnect concern, not
                        // this stream's.
                        this.state = TurnState::Done;
                        return Poll::Ready(Some(Err(ExecError::Stream(format!(
                            "websocket {SOCKET_CLOSED_MARKER}"
                        )))));
                    }
                    Poll::Ready((_guard, Err(e))) => {
                        this.state = TurnState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                TurnState::Done => return Poll::Ready(None),
            }
        }
    }
}

async fn read_next(mut guard: OwnedMutexGuard<WsConn>) -> ReadOutcome {
    let result = guard.recv_frame().await;
    (guard, result)
}

/// Send `envelope` on an ALREADY-HELD `guard` and update the connection's delta-tracking state —
/// the shared body behind both [`turn_stream`] (which acquires the guard itself, lazily, at first
/// poll) and [`turn_stream_with_guard`] (which takes a guard the caller acquired earlier, to widen
/// the critical section to also cover planning — see that function's doc for why).
async fn send_and_track(
    mut guard: OwnedMutexGuard<WsConn>,
    envelope: &Value,
) -> Result<OwnedMutexGuard<WsConn>, ExecError> {
    guard.send_frame(envelope).await?;
    // **THE FAILURE THAT MUST NOT HAPPEN** (M5a Task 7): record what the socket now HOLDS
    // — the full accumulated history, not merely the wire suffix — so `delta::plan_request`
    // can compute a real strict-extension next turn. This MUST happen right here,
    // immediately after `send_frame` succeeds and nowhere else — forgetting it makes every
    // future turn on this connection silently plan `Full` forever: no error, no test
    // failure, just the milestone's entire benefit evaporating while everything appears to
    // work (see `delta.rs`'s module doc's "Who must set the hashes" section and
    // `WsConn::last_item_hashes`'s own doc for the exact mechanism).
    //
    // `envelope`'s `input` is only the WIRE payload just sent: the full history on a
    // `Full` plan, but only the new suffix on an `Incremental` one
    // (`executor.rs::plan_and_build` / `codec::build_response_create`). Recording just
    // `item_hashes(envelope)` unconditionally — the original, buggy version of this line
    // — is therefore correct after a `Full` send but WRONG after an `Incremental` one: it
    // would overwrite the full history with only the suffix's hashes, so the very next
    // turn's real full-history body (SPEC-M5-WEBSOCKET.md §2: the HTTP client always
    // resends full history) has no valid prefix to extend and silently falls back to
    // `Full` — the exact "delta works for one turn, then reverts every OTHER turn" defect
    // this fix closes. So: on an incremental send, APPEND the suffix's hashes onto the
    // prior-full vector already sitting in `guard.last_item_hashes` (untouched since the
    // last time this closure ran); on a full send (or the first turn, when there is no
    // prior state), the sent hashes ARE the full history, so they simply replace it.
    let sent = item_hashes(envelope);
    let full = match (
        envelope.get("previous_response_id").is_some(),
        guard.last_item_hashes.take(),
    ) {
        (true, Some(mut prev)) => {
            prev.extend(sent);
            prev
        }
        _ => sent,
    };
    guard.last_non_input_fingerprint = Some(non_input_fingerprint(envelope));
    guard.last_input_count = Some(full.len() as u32);
    guard.last_item_hashes = Some(full);
    Ok(guard)
}

/// Drive ONE turn over `conn`: send `envelope` (an already-built `response.create` request — Task
/// 4's `codec::build_response_create` output, already anchored/deltad by Task 6's
/// `delta::plan_request` if applicable), then yield every received frame re-framed as SSE bytes
/// (Task 4's `codec::frame_to_sse`) until the terminal frame / a classified error / an anchor-miss,
/// at which point the stream ends (`Poll::Ready(None)` on the NEXT poll after the last item) while
/// `conn`'s socket stays open, unlocked, and ready for a second [`turn_stream`] call on the same
/// handle — the milestone this whole task exists to prove.
///
/// The send itself is lazy: nothing happens on the wire until the returned stream is first polled
/// (matching every other `ResponseStream`-returning path in this codebase — no work happens before
/// the caller starts consuming). This entry point acquires `conn`'s lock itself, at first poll —
/// used directly by this module's own tests and any caller with no separate planning step to widen
/// the critical section over. `ws::executor::CodexWsExecutor::drive_turn` (M5a Task 8) uses
/// [`turn_stream_with_guard`] instead — see that function's doc.
pub fn turn_stream(conn: SharedWsConn, envelope: Value) -> ResponseStream {
    Box::pin(TurnStream {
        state: TurnState::Sending(Box::pin(async move {
            let guard = conn.lock_owned().await;
            send_and_track(guard, &envelope).await
        })),
    })
}

/// Like [`turn_stream`], but the caller has ALREADY acquired `conn`'s lock (an
/// [`OwnedMutexGuard`]) and hands it straight in, instead of this function acquiring it itself.
///
/// # Why this exists: closing the plan-vs-send race (M5a Task 8)
/// `ws::executor::CodexWsExecutor::drive_turn` must PLAN the next envelope (`delta::
/// plan_request_for_conn`, reading `conn`'s `last_response_id`/`last_item_hashes`/
/// `last_non_input_fingerprint`) before it can build the `response.create` body this function
/// sends. If planning locks-and-releases separately from this function's own (former) internal
/// `lock_owned()`, there is a real gap between the two: a second concurrent turn on the SAME
/// session key (same cached [`SharedWsConn`]) can plan against the identical pre-send state,
/// build its own envelope, and only THEN queue behind the first turn's send — meaning by the time
/// the second turn's envelope actually reaches the wire, the connection's real state has already
/// moved past what that envelope was planned against (a stale anchor, a suffix that no longer
/// starts where the connection's history now ends). Ground truth §4 ("strictly one in-flight turn
/// per socket") already prevented two turns from being IN FLIGHT on the wire at once — the gap was
/// specifically between planning and sending, not between two sends.
///
/// The fix: the caller acquires `conn`'s lock ONCE, holds it across BOTH the plan (a synchronous
/// read of the guard) and the send (this function), and only [`turn_stream`]/[`send_and_track`]
/// (via reaching `TurnState::Done`) ever drops it. A second concurrent `drive_turn` call on the
/// same session key blocks at its own lock acquisition until the first turn is fully done —
/// exactly mirroring ground truth §4's per-socket exclusivity, just widened to cover planning too.
/// Turns on a DIFFERENT session key use a DIFFERENT `SharedWsConn` (a different `Mutex`), so this
/// never serializes unrelated sessions against each other.
pub(crate) fn turn_stream_with_guard(
    guard: OwnedMutexGuard<WsConn>,
    envelope: Value,
) -> ResponseStream {
    Box::pin(TurnStream {
        state: TurnState::Sending(Box::pin(
            async move { send_and_track(guard, &envelope).await },
        )),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use polyflare_core::Account;
    use polyflare_testkit::{MockWsUpstream, ScriptedTurn};
    use serde_json::json;

    fn test_account(base_url: String) -> Account {
        Account {
            id: "acct-1".into(),
            base_url,
            bearer_token: "secret-bearer-abc".into(),
            chatgpt_account_id: None,
        }
    }

    fn envelope(input: Vec<Value>, previous_response_id: Option<&str>) -> Value {
        let mut body = json!({
            "type": "response.create",
            "model": "gpt-5.6-sol",
            "input": input,
        });
        if let Some(id) = previous_response_id {
            body["previous_response_id"] = json!(id);
        }
        body
    }

    /// Parse one yielded SSE `Bytes` chunk back to its JSON `Value`, asserting the exact
    /// `data: {json}\n\n` shape Task 4's `frame_to_sse` produces.
    fn parse_sse(bytes: &Bytes) -> Value {
        let text = String::from_utf8(bytes.to_vec()).expect("utf8");
        let payload = text
            .strip_prefix("data: ")
            .expect("must carry the SSE `data: ` prefix")
            .strip_suffix("\n\n")
            .expect("must end with the SSE blank-line terminator");
        serde_json::from_str(payload).expect("payload is valid JSON")
    }

    #[tokio::test]
    async fn scripted_turn_yields_sse_events_then_ends_at_the_terminal_frame() {
        let mock = MockWsUpstream::new(ScriptedTurn::normal(vec![
            json!({"type": "response.output_text.delta", "delta": "hi"}).to_string(),
        ]));
        let base = mock.clone().spawn().await;
        let conn = WsConn::connect(&test_account(base), &[])
            .await
            .expect("connect");
        let shared = shared_conn(conn);

        let mut stream = turn_stream(
            shared,
            envelope(vec![json!({"role": "user", "content": "hi"})], None),
        );

        let first = stream
            .next()
            .await
            .expect("expected the delta event")
            .expect("ok");
        let first = parse_sse(&first);
        assert_eq!(first["type"], "response.output_text.delta");
        assert_eq!(first["delta"], "hi");

        let second = stream
            .next()
            .await
            .expect("expected the terminal frame")
            .expect("ok");
        let second = parse_sse(&second);
        assert_eq!(second["type"], "response.completed");
        assert_eq!(second["response"]["id"], "resp_1");

        // The stream must actually END here — no further items, not even a stray `None` that
        // resolves after more polls.
        assert!(
            stream.next().await.is_none(),
            "the stream must end (Poll::Ready(None)) right after the terminal frame"
        );
        assert_eq!(mock.handshake_count(), 1);
    }

    #[tokio::test]
    async fn second_turn_on_the_same_conn_reuses_the_connection() {
        // THE central test: two sequential turns over ONE WsConn must leave handshake_count() at
        // 1 — the milestone the whole M5a WS transport exists to prove. If this doesn't hold, no
        // delta is ever possible and WS delivers nothing over HTTP-SSE.
        let mock = MockWsUpstream::scripted(vec![
            ScriptedTurn::normal(vec![]),
            ScriptedTurn::normal(vec![]),
        ]);
        let base = mock.clone().spawn().await;
        let conn = WsConn::connect(&test_account(base), &[])
            .await
            .expect("connect");
        let shared = shared_conn(conn);

        // Turn 1: full history, no anchor.
        let mut stream1 = turn_stream(
            shared.clone(),
            envelope(vec![json!({"role": "user", "content": "first"})], None),
        );
        let mut first_id = None;
        while let Some(item) = stream1.next().await {
            let v = parse_sse(&item.expect("ok"));
            if v["type"] == "response.completed" {
                first_id = v["response"]["id"].as_str().map(str::to_string);
            }
        }
        assert_eq!(first_id.as_deref(), Some("resp_1"));
        drop(stream1); // release the guard explicitly before starting turn 2

        // Turn 2, SAME shared handle: an anchored delta.
        let mut stream2 = turn_stream(
            shared.clone(),
            envelope(
                vec![json!({"role": "user", "content": "second"})],
                Some("resp_1"),
            ),
        );
        let mut second_id = None;
        while let Some(item) = stream2.next().await {
            let v = parse_sse(&item.expect("ok"));
            if v["type"] == "response.completed" {
                second_id = v["response"]["id"].as_str().map(str::to_string);
            }
        }
        assert_eq!(second_id.as_deref(), Some("resp_2"));

        // The proof: ONE handshake for TWO turns.
        assert_eq!(mock.handshake_count(), 1);
        let frames = mock.frames();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].previous_response_id, None);
        assert_eq!(frames[1].previous_response_id, Some("resp_1".to_string()));
    }

    #[tokio::test]
    async fn last_response_id_is_captured_onto_the_conn_from_the_completed_frame() {
        let mock = MockWsUpstream::new(ScriptedTurn::normal(vec![]));
        let base = mock.clone().spawn().await;
        let conn = WsConn::connect(&test_account(base), &[])
            .await
            .expect("connect");
        let shared = shared_conn(conn);
        assert!(shared.lock().await.last_response_id.is_none());

        let mut stream = turn_stream(shared.clone(), envelope(vec![], None));
        while stream.next().await.is_some() {}

        assert_eq!(
            shared.lock().await.last_response_id.as_deref(),
            Some("resp_1"),
            "must capture the exact id from response.completed's response.id"
        );
    }

    #[tokio::test]
    async fn close_mid_stream_surfaces_as_a_stream_error() {
        let mock = MockWsUpstream::new(ScriptedTurn::close_mid_stream(vec![
            json!({"type": "response.output_text.delta", "delta": "partial"}).to_string(),
        ]));
        let base = mock.clone().spawn().await;
        let conn = WsConn::connect(&test_account(base), &[])
            .await
            .expect("connect");
        let shared = shared_conn(conn);

        let mut stream = turn_stream(
            shared,
            envelope(vec![json!({"role": "user", "content": "hi"})], None),
        );

        // The pre-close delta event still arrives, forwarded as normal.
        let first = stream
            .next()
            .await
            .expect("expected the delta event")
            .expect("ok");
        assert_eq!(parse_sse(&first)["delta"], "partial");

        // Then the close itself, surfaced as the required error shape — so
        // `watchdog.rs:393-399`'s writeback (mid-stream error ⇒ transient error) counts it
        // correctly instead of a silent `None` being mistaken for a clean completion.
        let second = stream
            .next()
            .await
            .expect("expected an error item, not a silent end-of-stream");
        match second {
            Err(ExecError::Stream(msg)) => {
                assert!(msg.contains("closed"), "unexpected message: {msg}");
            }
            other => panic!("expected Err(ExecError::Stream(..)), got {other:?}"),
        }

        assert!(
            stream.next().await.is_none(),
            "the stream must end right after surfacing the close"
        );
    }
}
