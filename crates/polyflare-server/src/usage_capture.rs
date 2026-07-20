//! Content-free extraction of the token `usage` object from a codex `response.completed` stream
//! frame (Task 2 of the live-usage-+-cost-capture sub-project; a later task's stream wrapper
//! consumes [`parse_response_usage`] to observe usage without touching content).
//!
//! # Content safety (the whole point)
//! [`parse_response_usage`] reads ONLY the four numeric usage fields — `usage.input_tokens`,
//! `usage.output_tokens`, `usage.input_tokens_details.cached_tokens`,
//! `usage.output_tokens_details.reasoning_tokens` — plus the frame's own `type` discriminant and
//! the presence of a `response.usage` object. It never reads, copies, logs, or returns any
//! content/text field (`output_text`, `content`, `delta`, `instructions`, ...); those bytes are
//! never even inspected, only skipped over by `serde_json::Value`'s structural indexing.
//!
//! JSON shape (mirrors codex-lb's `_normalize_usage`, `pricing.py:58-89`):
//! ```json
//! {
//!   "type": "response.completed",
//!   "response": {
//!     "usage": {
//!       "input_tokens": 8380,
//!       "output_tokens": 120,
//!       "input_tokens_details": { "cached_tokens": 6912 },
//!       "output_tokens_details": { "reasoning_tokens": 40 }
//!     }
//!   }
//! }
//! ```
//!
//! # SSE boundary
//! This function takes a bare JSON object string. If frames arrive over SSE as `data: {...}`
//! lines, stripping the `data: ` prefix is the CALLER's job (the stream wrapper added in a later
//! task) — this parser stays pure JSON-in and never handles SSE framing itself.

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use bytes::Bytes;
use futures_core::Stream;
use serde_json::Value;

/// The four numeric token counts pulled from a `response.completed` frame's `usage` object.
/// Every field is `Option` because a real frame may omit any of them; content is never carried
/// here, only counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ResponseUsage {
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cached_input_tokens: Option<i64>,
    pub reasoning_tokens: Option<i64>,
}

/// Parse one JSON stream frame and, if it is a `response.completed` frame carrying a
/// `response.usage` object, return the four numeric usage counts. Returns `None` for any other
/// frame type, malformed JSON, or a `response.completed` frame with no `usage` object.
///
/// Reads ONLY the numeric usage fields (see module docs) — never any content/text field.
pub fn parse_response_usage(frame_json: &str) -> Option<ResponseUsage> {
    let value: Value = serde_json::from_str(frame_json).ok()?;

    if value.get("type")?.as_str()? != "response.completed" {
        return None;
    }

    let usage = value.get("response")?.get("usage")?;
    if !usage.is_object() {
        return None;
    }

    Some(ResponseUsage {
        input_tokens: usage.get("input_tokens").and_then(Value::as_i64),
        output_tokens: usage.get("output_tokens").and_then(Value::as_i64),
        cached_input_tokens: usage
            .get("input_tokens_details")
            .and_then(|d| d.get("cached_tokens"))
            .and_then(Value::as_i64),
        reasoning_tokens: usage
            .get("output_tokens_details")
            .and_then(|d| d.get("reasoning_tokens"))
            .and_then(Value::as_i64),
    })
}

/// Usage + first-token latency observed on a passthrough stream (see [`UsageCapturingStream`]).
/// `usage` is `None` if no `response.completed` frame carrying a `usage` object was ever
/// observed (e.g. the client disconnected before completion, or the upstream never sent one).
/// `ttft_ms` is `None` if the stream never yielded a single item (empty stream, or dropped before
/// the first byte arrived).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CapturedUsage {
    pub usage: Option<ResponseUsage>,
    pub ttft_ms: Option<i64>,
}

/// Byte-for-byte passthrough wrapper around an upstream SSE byte stream.
///
/// # Passthrough (the whole point)
/// Every `Ok(bytes)` item yielded by the wrapped `inner` stream is forwarded to the caller
/// completely UNCHANGED — same `Bytes` handle, same order, no buffering/delay. This wrapper must
/// never alter, drop, reorder, or delay a single client byte: doing so would corrupt the response
/// the client sees or break upstream fingerprinting. All capture below is a side-observation of
/// bytes that are forwarded regardless of what (if anything) is parsed out of them.
///
/// # Capture
/// While forwarding, each chunk is *observed* (never mutated):
/// - the elapsed time from construction ([`UsageCapturingStream::new`]) to the FIRST yielded item
///   is recorded once as `ttft_ms` (time-to-first-token);
/// - each yielded chunk is decoded as UTF-8 and split into lines; an optional `data: ` SSE prefix
///   is stripped from each line before handing it to [`parse_response_usage`]. The LAST
///   successful parse across the whole stream is kept as the final `usage` (a stream can contain
///   more than one `response.completed`-shaped frame in principle; only the final one is real).
///
/// Parsing is strictly best-effort and never observable to the caller: malformed UTF-8, blank SSE
/// keep-alive lines, and non-`response.completed`/malformed-JSON lines are silently ignored — the
/// enclosing chunk is still forwarded unchanged either way. This wrapper never blocks and never
/// panics on malformed input.
///
/// # `on_done`
/// `on_done` fires with the final [`CapturedUsage`] EXACTLY ONCE, whichever happens first:
/// - the inner stream ends normally (`poll_next` returns `Ready(None)`), or
/// - the wrapper is dropped before the inner stream ends (client disconnect mid-stream).
///
/// Both paths funnel through the same `Option::take`-guarded call ([`Self::fire_on_done`]), so
/// whichever happens first consumes the `Option` and the other becomes a no-op.
pub struct UsageCapturingStream<S> {
    inner: S,
    on_done: Option<Box<dyn FnOnce(CapturedUsage) + Send>>,
    start: Instant,
    ttft_ms: Option<i64>,
    usage: Option<ResponseUsage>,
}

impl<S> UsageCapturingStream<S> {
    /// Wrap `inner`, starting the TTFT clock now. `on_done` is called exactly once — on normal
    /// stream end or on drop — with the usage/TTFT captured so far (see struct docs).
    pub fn new(inner: S, on_done: impl FnOnce(CapturedUsage) + Send + 'static) -> Self {
        Self {
            inner,
            on_done: Some(Box::new(on_done)),
            start: Instant::now(),
            ttft_ms: None,
            usage: None,
        }
    }

    /// Scan one yielded chunk for a `response.completed` frame, updating `self.usage` on a
    /// successful parse. Read-only w.r.t. `bytes` — the passthrough copy handed to the caller in
    /// `poll_next` is the same original `Bytes` handle, never touched by this function.
    fn observe(&mut self, bytes: &Bytes) {
        let Ok(text) = std::str::from_utf8(bytes) else {
            return; // best-effort: not UTF-8, silently skip; the chunk still passes through
        };
        for line in text.lines() {
            let payload = line.strip_prefix("data: ").unwrap_or(line);
            if let Some(usage) = parse_response_usage(payload) {
                self.usage = Some(usage); // keep the LAST successful parse
            }
        }
    }

    /// Fire `on_done` with the usage/TTFT captured so far, guarded by `Option::take` so it never
    /// double-fires regardless of whether this is called from `poll_next`'s `Ready(None)` arm or
    /// from `Drop::drop` (or, in principle, both).
    fn fire_on_done(&mut self) {
        if let Some(on_done) = self.on_done.take() {
            on_done(CapturedUsage {
                usage: self.usage,
                ttft_ms: self.ttft_ms,
            });
        }
    }
}

impl<S, E> Stream for UsageCapturingStream<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
{
    type Item = Result<Bytes, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // `UsageCapturingStream<S>` is Unpin whenever `S: Unpin` (every other field is a plain
        // Unpin type — `Box<dyn FnOnce(..) + Send>`, `Instant`, `Option<i64>`,
        // `Option<ResponseUsage>` — mirroring `TranslatingStream`'s all-Unpin-fields idiom in
        // `translate_stream.rs`), so plain `get_mut` + `Pin::new` is sufficient; no unsafe pin
        // projection needed.
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => {
                if this.ttft_ms.is_none() {
                    this.ttft_ms = Some(this.start.elapsed().as_millis() as i64);
                }
                this.observe(&bytes);
                // UNCHANGED: same `Bytes` handle we received, byte-for-byte passthrough.
                Poll::Ready(Some(Ok(bytes)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => {
                this.fire_on_done(); // normal end: exactly-once fire #1
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Client-disconnect path: if the stream is dropped before `poll_next` ever observes
/// `Ready(None)` (e.g. the HTTP client hangs up mid-response), `on_done` still fires here. The
/// `Option::take` inside `fire_on_done` makes this safe to call unconditionally — if
/// `poll_next` already fired it, `self.on_done` is already `None` and this is a no-op.
impl<S> Drop for UsageCapturingStream<S> {
    fn drop(&mut self) {
        self.fire_on_done(); // disconnect end: exactly-once fire #2 (mutually exclusive with #1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // NOTE: the brief's example test imports `futures::StreamExt` / `futures::stream::iter`, but
    // this crate depends on `futures-core`/`futures-util` (no plain `futures` crate) — see
    // `translate_stream.rs`'s tests for the same idiom. Adapted accordingly.
    use futures_util::stream::{self, StreamExt};
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn passes_bytes_through_and_captures_usage() {
        let frames = vec![
            Ok::<_, std::io::Error>(Bytes::from(
                "data: {\"type\":\"response.created\"}\n\n",
            )),
            Ok(Bytes::from(
                "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"usage\":{\"input_tokens\":8380,\"output_tokens\":120,\"input_tokens_details\":{\"cached_tokens\":6912},\"output_tokens_details\":{\"reasoning_tokens\":40}}}}\n\n",
            )),
        ];
        let captured = Arc::new(Mutex::new(None));
        let c2 = captured.clone();
        let s = UsageCapturingStream::new(stream::iter(frames), move |cu| {
            *c2.lock().unwrap() = Some(cu)
        });
        let out: Vec<_> = s.map(|r| r.unwrap()).collect().await;
        // passthrough: exact bytes preserved
        assert_eq!(
            out[0],
            Bytes::from("data: {\"type\":\"response.created\"}\n\n")
        );
        let cu = captured.lock().unwrap().take().unwrap();
        assert_eq!(cu.usage.unwrap().input_tokens, Some(8380));
        assert!(cu.ttft_ms.is_some());
    }

    #[tokio::test]
    async fn on_done_fires_exactly_once_on_drop_mid_stream() {
        // Riskiest part of this task: on_done must fire on a client disconnect (drop before the
        // inner stream ends), guarded so it never double-fires. This test never lets the stream
        // reach `Ready(None)` — it drops the wrapper after consuming exactly one item — so the
        // ONLY way `on_done` can have run is the `Drop` impl.
        let frames = vec![
            Ok::<_, std::io::Error>(Bytes::from("data: {\"type\":\"response.created\"}\n\n")),
            Ok(Bytes::from("data: never consumed\n\n")),
        ];
        let captured = Arc::new(Mutex::new(None));
        let c2 = captured.clone();
        let mut s = UsageCapturingStream::new(stream::iter(frames), move |cu| {
            *c2.lock().unwrap() = Some(cu)
        });

        let first = s.next().await;
        assert!(
            first.is_some(),
            "first item must still be yielded before drop"
        );
        assert!(
            captured.lock().unwrap().is_none(),
            "on_done must not fire before the stream ends or is dropped"
        );

        drop(s);

        let cu = captured
            .lock()
            .unwrap()
            .take()
            .expect("on_done must fire exactly once on drop (client disconnect)");
        assert!(
            cu.ttft_ms.is_some(),
            "ttft is recorded from the one item we did consume"
        );
    }

    #[test]
    fn parses_usage_from_completed_frame() {
        let f = r#"{"type":"response.completed","response":{"id":"resp_1","usage":{"input_tokens":8380,"output_tokens":120,"input_tokens_details":{"cached_tokens":6912},"output_tokens_details":{"reasoning_tokens":40}}}}"#;
        let u = parse_response_usage(f).unwrap();
        assert_eq!(u.input_tokens, Some(8380));
        assert_eq!(u.output_tokens, Some(120));
        assert_eq!(u.cached_input_tokens, Some(6912));
        assert_eq!(u.reasoning_tokens, Some(40));
    }

    #[test]
    fn non_completed_frame_is_none() {
        assert!(
            parse_response_usage(r#"{"type":"response.output_text.delta","delta":"hi"}"#).is_none()
        );
    }

    #[test]
    fn completed_without_usage_is_none() {
        assert!(
            parse_response_usage(r#"{"type":"response.completed","response":{"id":"r"}}"#)
                .is_none()
        );
    }
}
