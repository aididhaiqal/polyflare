//! Content-free extraction of the token `usage` object from a codex `response.completed` stream
//! frame (Task 2 of the live-usage-+-cost-capture sub-project; a later task's stream wrapper
//! consumes [`parse_response_usage`] to observe usage without touching content).
//!
//! # Content safety (the whole point)
//! [`parse_response_usage`] reads ONLY bounded numeric usage fields — standard input/output,
//! cached input, reasoning output, and provider-reported orchestration input/output/cache counts
//! — plus the frame's own `type` discriminant and the presence of a `response.usage` object. It
//! never reads, copies, logs, or returns any content/text field (`output_text`, `content`, `delta`,
//! `instructions`, ...); those bytes are never even inspected, only skipped over by
//! `serde_json::Value`'s structural indexing.
//!
//! JSON shape (mirrors codex-lb's `_normalize_usage`, `pricing.py:58-89`):
//! ```json
//! {
//!   "type": "response.completed",
//!   "response": {
//!     "usage": {
//!       "input_tokens": 8380,
//!       "output_tokens": 120,
//!       "total_tokens": 8500,
//!       "input_tokens_details": { "cached_tokens": 6912, "cache_write_tokens": 256 },
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

/// The canonical numeric token counts pulled from a `response.completed` frame's `usage` object.
/// Every field is `Option` because a real frame may omit any of them; content is never carried
/// here, only counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ResponseUsage {
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cached_input_tokens: Option<i64>,
    pub cache_write_input_tokens: Option<i64>,
    pub reasoning_tokens: Option<i64>,
    pub reported_total_tokens: Option<i64>,
    pub orchestration_input_tokens: Option<i64>,
    pub orchestration_output_tokens: Option<i64>,
    pub orchestration_cached_input_tokens: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CaptureProtocol {
    Responses,
    AnthropicMessages,
}

fn non_negative_i64(value: &Value) -> Option<i64> {
    value.as_i64().filter(|value| *value >= 0)
}

fn sum_present(values: impl IntoIterator<Item = Option<i64>>) -> Option<i64> {
    let mut total = 0_i64;
    let mut found = false;
    for value in values.into_iter().flatten() {
        total = total.checked_add(value)?;
        found = true;
    }
    found.then_some(total)
}

fn max_present(current: Option<i64>, incoming: Option<i64>) -> Option<i64> {
    match (current, incoming) {
        (Some(current), Some(incoming)) => Some(current.max(incoming)),
        (current, incoming) => current.or(incoming),
    }
}

/// Convert authoritative terminal usage into the same compute-pressure scale used by the
/// pre-route estimator: uncached input 1x, cached input 1/8x, and autoregressive output 4x.
/// Negative/missing token fields are rejected rather than allowed to cancel valid positive values.
pub fn pressure_equivalent_tokens(usage: ResponseUsage) -> Option<u64> {
    let input = u64::try_from(usage.input_tokens?).ok()?;
    let output = u64::try_from(usage.output_tokens?).ok()?;
    let cached = usage
        .cached_input_tokens
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or(0)
        .min(input);
    Some(
        input
            .saturating_sub(cached)
            .saturating_add(cached.div_ceil(8))
            .saturating_add(output.saturating_mul(4)),
    )
}

/// The service tier an upstream reported serving a turn at, normalised to a bounded vocabulary.
///
/// Bounded on purpose: the raw value is an upstream-controlled string, and this rides the same
/// content-safe telemetry path as everything else here. An unrecognised tier becomes
/// [`ServedTier::Other`] rather than being forwarded verbatim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServedTier {
    Priority,
    Flex,
    Default,
    Other,
}

impl ServedTier {
    fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            // `fast` is the Codex-side spelling of the priority tier.
            "priority" | "fast" | "scale" => ServedTier::Priority,
            "flex" => ServedTier::Flex,
            "default" | "auto" | "standard" => ServedTier::Default,
            _ => ServedTier::Other,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            ServedTier::Priority => "priority",
            ServedTier::Flex => "flex",
            ServedTier::Default => "default",
            ServedTier::Other => "other",
        }
    }

    pub fn is_priority(self) -> bool {
        matches!(self, ServedTier::Priority)
    }
}

/// The tier a `response.completed` frame reports the turn was SERVED at, if it carries one.
///
/// This is the only trustworthy answer to "was priority actually honoured": the request's own
/// `service_tier` says what was asked for, and an upstream is free to serve something else.
pub fn parse_response_service_tier(frame_json: &str) -> Option<ServedTier> {
    let value: Value = serde_json::from_str(frame_json).ok()?;
    if value.get("type")?.as_str()? != "response.completed" {
        return None;
    }
    let raw = value.get("response")?.get("service_tier")?.as_str()?.trim();
    (!raw.is_empty()).then(|| ServedTier::parse(raw))
}

/// Parse one JSON stream frame and, if it is a `response.completed` frame carrying a
/// `response.usage` object, return its canonical numeric usage counts. Returns `None` for any other
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
        input_tokens: usage.get("input_tokens").and_then(non_negative_i64),
        output_tokens: usage.get("output_tokens").and_then(non_negative_i64),
        cached_input_tokens: usage
            .get("input_tokens_details")
            .and_then(|d| d.get("cached_tokens"))
            .and_then(non_negative_i64),
        cache_write_input_tokens: usage
            .get("input_tokens_details")
            .and_then(|d| d.get("cache_write_tokens"))
            .and_then(non_negative_i64),
        reasoning_tokens: usage
            .get("output_tokens_details")
            .and_then(|d| d.get("reasoning_tokens"))
            .and_then(non_negative_i64),
        reported_total_tokens: usage.get("total_tokens").and_then(non_negative_i64),
        orchestration_input_tokens: usage
            .get("orchestration_input_tokens")
            .or_else(|| {
                usage
                    .get("input_tokens_details")
                    .and_then(|d| d.get("orchestration_input_tokens"))
            })
            .or_else(|| {
                usage
                    .get("orchestration_tokens_details")
                    .and_then(|d| d.get("input_tokens"))
            })
            .and_then(non_negative_i64),
        orchestration_output_tokens: usage
            .get("orchestration_output_tokens")
            .or_else(|| {
                usage
                    .get("output_tokens_details")
                    .and_then(|d| d.get("orchestration_output_tokens"))
            })
            .or_else(|| {
                usage
                    .get("orchestration_tokens_details")
                    .and_then(|d| d.get("output_tokens"))
            })
            .and_then(non_negative_i64),
        orchestration_cached_input_tokens: usage
            .get("orchestration_cached_input_tokens")
            .or_else(|| {
                usage
                    .get("input_tokens_details")
                    .and_then(|d| d.get("orchestration_input_cached_tokens"))
            })
            .or_else(|| {
                usage
                    .get("input_tokens_details")
                    .and_then(|d| d.get("orchestration_cached_input_tokens"))
            })
            .or_else(|| {
                usage
                    .get("orchestration_tokens_details")
                    .and_then(|d| d.get("cached_input_tokens"))
            })
            .and_then(non_negative_i64),
    })
}

/// Parse Anthropic Messages usage into the same canonical counters used by Responses telemetry.
///
/// Anthropic reports prompt-cache reads and writes separately from ordinary `input_tokens`.
/// Canonical `input_tokens` therefore includes all three categories, matching the Responses
/// contract where cached tokens are a subset of total input. This lets the shared pricing path
/// charge ordinary/cache-write input at the normal input rate and cache reads at the cached rate.
/// Detect an Anthropic `event: error` frame that a status-200 stream can smuggle mid-response, and
/// map its `error.type` to the equivalent HTTP status: `rate_limit_error` → 429, `overloaded_error`
/// → 529. Any other error type (or a non-error frame) is `None` — only the two the router acts on
/// are surfaced, so an ordinary `invalid_request_error` never benches the account.
pub(crate) fn parse_anthropic_stream_error(frame_json: &str) -> Option<u16> {
    let value: Value = serde_json::from_str(frame_json).ok()?;
    if value.get("type")?.as_str()? != "error" {
        return None;
    }
    match value.get("error")?.get("type")?.as_str()? {
        "rate_limit_error" => Some(429),
        "overloaded_error" => Some(529),
        _ => None,
    }
}

pub(crate) fn parse_anthropic_usage(frame_json: &str) -> Option<ResponseUsage> {
    let value: Value = serde_json::from_str(frame_json).ok()?;
    let event_type = value.get("type")?.as_str()?;
    let usage = match event_type {
        "message_start" => value.get("message")?.get("usage")?,
        "message_delta" | "message_stop" | "message" => value.get("usage")?,
        _ => return None,
    };
    let usage = usage.as_object()?;
    let cache_write_input_tokens = usage
        .get("cache_creation_input_tokens")
        .and_then(non_negative_i64)
        .or_else(|| {
            usage
                .get("cache_creation")
                .and_then(Value::as_object)
                .and_then(|parts| sum_present(parts.values().map(non_negative_i64)))
        });
    let cached_input_tokens = usage
        .get("cache_read_input_tokens")
        .and_then(non_negative_i64);
    let uncached_input_tokens = usage.get("input_tokens").and_then(non_negative_i64);
    let input_tokens = sum_present([
        uncached_input_tokens,
        cache_write_input_tokens,
        cached_input_tokens,
    ]);
    let output_tokens = usage.get("output_tokens").and_then(non_negative_i64);

    Some(ResponseUsage {
        input_tokens,
        output_tokens,
        cached_input_tokens,
        cache_write_input_tokens,
        reported_total_tokens: input_tokens
            .zip(output_tokens)
            .and_then(|(input, output)| input.checked_add(output)),
        ..ResponseUsage::default()
    })
}

/// Whether an SSE JSON payload carries the first generated output fragment. Control/lifecycle
/// frames such as `response.created` do not count as TTFT. Codex output modalities use
/// `response.*.delta` events for generated text, reasoning summaries, refusals, tool arguments,
/// and other streamed output fragments.
pub(crate) fn is_output_delta(frame_json: &str) -> bool {
    serde_json::from_str::<Value>(frame_json)
        .ok()
        .and_then(|value| value.get("type")?.as_str().map(str::to_owned))
        .is_some_and(|event_type| {
            event_type.starts_with("response.") && event_type.ends_with(".delta")
        })
}

fn is_anthropic_output_delta(frame_json: &str) -> bool {
    serde_json::from_str::<Value>(frame_json)
        .ok()
        .and_then(|value| value.get("type")?.as_str().map(str::to_owned))
        .as_deref()
        == Some("content_block_delta")
}

/// Usage + first-token latency observed on a passthrough stream (see [`UsageCapturingStream`]).
/// `usage` is `None` if no `response.completed` frame carrying a `usage` object was ever
/// observed (e.g. the client disconnected before completion, or the upstream never sent one).
/// `ttft_ms` is `None` if the stream never yielded a complete `response.*.delta` output event
/// (empty/control-only stream, or dropped before the first generated fragment arrived).
/// `duration_ms` is the end-to-end request duration measured from the SAME origin as `ttft_ms`
/// (the `start: Instant` passed into [`UsageCapturingStream::new`] — the route's own clock, not
/// the wrapper's construction time) to stream end (normal completion or drop). `None` only in the
/// vacuous case where `fire_on_done` never runs (it always does, exactly once — see that fn's
/// docs), so in practice this is always `Some` once `on_done` fires.
/// `protocol_outcome` is the first bounded terminal event observed, or `transport_lost` for an
/// upstream error/terminal-less EOF and `cancelled` when the downstream drops the wrapper early.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapturedUsage {
    pub usage: Option<ResponseUsage>,
    /// The tier the upstream REPORTED serving, when the terminal frame carried one. `None` means
    /// the upstream said nothing — unknown, which must never be read as a downgrade.
    pub served_tier: Option<ServedTier>,
    pub ttft_ms: Option<i64>,
    pub duration_ms: Option<i64>,
    pub protocol_outcome: polyflare_store::RequestProtocolOutcome,
    /// When a status-200 Anthropic stream smuggled an `event: error` frame carrying a
    /// `rate_limit_error` (→ 429) or `overloaded_error` (→ 529), the equivalent HTTP status so the
    /// caller can cool the account exactly as it would on a real 429/529. `None` for a clean stream.
    /// Mirrors better-ccflare's mid-stream SSE rate-limit sniffer.
    pub stream_error_status: Option<u16>,
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
/// - the elapsed time from construction ([`UsageCapturingStream::new`]) to the first complete
///   `response.*.delta` event is recorded once as `ttft_ms` (time-to-first-generated-token);
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
/// - the inner stream yields an error, or
/// - the wrapper is dropped before the inner stream ends (client disconnect mid-stream).
///
/// Both paths funnel through the same `Option::take`-guarded call ([`Self::fire_on_done`]), so
/// whichever happens first consumes the `Option` and the other becomes a no-op.
pub struct UsageCapturingStream<S> {
    inner: S,
    on_done: Option<Box<dyn FnOnce(CapturedUsage) + Send>>,
    protocol: CaptureProtocol,
    start: Instant,
    ttft_ms: Option<i64>,
    usage: Option<ResponseUsage>,
    /// The tier the terminal frame reported. Set once, from the frame that also carries usage.
    served_tier: Option<ServedTier>,
    terminal_outcome: Option<polyflare_store::RequestProtocolOutcome>,
    /// First mid-stream Anthropic error status observed (429 rate_limit / 529 overloaded), if any.
    stream_error_status: Option<u16>,
    eof_outcome: polyflare_store::RequestProtocolOutcome,
    /// Side-buffer of raw SSE bytes accumulated ACROSS chunks, so a `data: {...}` line split by
    /// the transport (real codex `response.completed` frames are ~20 KB, transport chunks are
    /// ~8 KB) can still be reassembled into a complete line before parsing. Never handed to the
    /// caller — `poll_next` always yields the original chunk `Bytes` unchanged; this is a
    /// parse-side copy only. Bounded by [`MAX_PENDING`] against a malformed/unterminated stream
    /// growing memory without limit.
    pending: Vec<u8>,
}

/// Cap on `UsageCapturingStream::pending`: if this many bytes accumulate with no `\n` yet seen
/// (i.e. a single "line" that never completes), the buffer is dropped rather than grown further.
/// A well-formed `response.completed` frame is ~20 KB, so 1 MiB is generously above any real
/// frame; this only guards against a pathological/malformed upstream stream.
const MAX_PENDING: usize = 1 << 20;

impl<S> UsageCapturingStream<S> {
    /// Wrap `inner`, measuring TTFT and the end-to-end `duration_ms` from `start`. `start` is the
    /// CALLER's clock origin (in production, the route handler's own `Instant::now()`, captured
    /// before any route/setup work) — NOT re-taken here — so that `ttft_ms` (this wrapper) and
    /// `duration_ms` (this wrapper) and the route's own pre-existing duration measurement all share
    /// one origin. Passing a stale/shared `start` is intentional and required for
    /// `derive_tps(duration_ms, ttft_ms, tokens)` in `read_api.rs` to be meaningful: the two
    /// values must be offsets from the SAME instant, or the derived tokens/sec is nonsense (the
    /// bug this shared-origin design fixes). `on_done` is called exactly once — on normal stream
    /// end or on drop — with the usage/TTFT/duration captured so far (see struct docs).
    pub fn new(
        inner: S,
        start: Instant,
        on_done: impl FnOnce(CapturedUsage) + Send + 'static,
    ) -> Self {
        Self::new_with_eof_outcome(
            inner,
            start,
            polyflare_store::RequestProtocolOutcome::TransportLost,
            on_done,
        )
    }

    /// Build a byte-preserving observer whose normal terminal-less EOF has a caller-supplied
    /// classification. Native SSE uses [`Self::new`] and therefore treats missing protocol
    /// terminals as transport loss. A buffered non-SSE HTTP response already has a final status,
    /// so ingress supplies `Completed` or `Failed` instead.
    pub fn new_with_eof_outcome(
        inner: S,
        start: Instant,
        eof_outcome: polyflare_store::RequestProtocolOutcome,
        on_done: impl FnOnce(CapturedUsage) + Send + 'static,
    ) -> Self {
        Self::new_for_protocol(
            inner,
            start,
            CaptureProtocol::Responses,
            eof_outcome,
            on_done,
        )
    }

    /// Observe an Anthropic Messages response while preserving every response byte. Streaming
    /// `message_start` and `message_delta` usage is merged until `message_stop`; a buffered
    /// top-level `message` body is handled by the same observer.
    pub fn new_anthropic_with_eof_outcome(
        inner: S,
        start: Instant,
        eof_outcome: polyflare_store::RequestProtocolOutcome,
        on_done: impl FnOnce(CapturedUsage) + Send + 'static,
    ) -> Self {
        Self::new_for_protocol(
            inner,
            start,
            CaptureProtocol::AnthropicMessages,
            eof_outcome,
            on_done,
        )
    }

    fn new_for_protocol(
        inner: S,
        start: Instant,
        protocol: CaptureProtocol,
        eof_outcome: polyflare_store::RequestProtocolOutcome,
        on_done: impl FnOnce(CapturedUsage) + Send + 'static,
    ) -> Self {
        Self {
            inner,
            on_done: Some(Box::new(on_done)),
            protocol,
            start,
            ttft_ms: None,
            usage: None,
            served_tier: None,
            terminal_outcome: None,
            stream_error_status: None,
            eof_outcome,
            pending: Vec::new(),
        }
    }

    /// Scan one yielded chunk for a `response.completed` frame, updating `self.usage` on a
    /// successful parse. Read-only w.r.t. `bytes` — the passthrough copy handed to the caller in
    /// `poll_next` is the same original `Bytes` handle, never touched by this function.
    ///
    /// A real codex `response.completed` frame (~20 KB) can be split by the transport across
    /// several `poll_next` chunks (~8 KB each), landing its `data: {...}` line's JSON in pieces
    /// that are each individually invalid. So this buffers bytes ACROSS calls in `self.pending`
    /// and only parses COMPLETE lines (terminated by `\n`), retaining any trailing incomplete
    /// line for the next chunk.
    fn observe(&mut self, bytes: &Bytes) {
        self.pending.extend_from_slice(bytes);

        // Drain every complete line (up to and including each `\n`) out of `pending`, parsing
        // each; retain whatever trails the last `\n` (an incomplete line spanning into the next
        // chunk) for next time.
        while let Some(newline_at) = self.pending.iter().position(|&b| b == b'\n') {
            let line_bytes: Vec<u8> = self.pending.drain(..=newline_at).collect();
            let line_bytes = &line_bytes[..line_bytes.len() - 1]; // drop the trailing `\n`
            self.process_line(line_bytes);
        }

        // Bound `pending`: if a single line has grown past MAX_PENDING with no `\n` in sight,
        // this is either a pathological/malformed stream or a frame far larger than any real
        // `response.completed` — drop the buffer rather than let memory grow unbounded.
        if self.pending.len() > MAX_PENDING {
            self.pending.clear();
        }
    }

    /// Decode one complete line (bytes up to, excluding, a `\n`) as UTF-8, strip an optional
    /// `data: ` SSE prefix, and inspect only the bounded event type plus numeric usage fields.
    /// Buffering as bytes and decoding per-line (rather than decoding the whole buffer as UTF-8 up
    /// front) means a multi-byte UTF-8 character split across a chunk boundary can never corrupt a
    /// *complete* reassembled line — only the still-pending incomplete tail is ever mid-character.
    fn process_line(&mut self, line_bytes: &[u8]) {
        let Ok(s) = std::str::from_utf8(line_bytes) else {
            return; // best-effort: not UTF-8, silently skip; the chunk still passes through
        };
        let payload = s.strip_prefix("data:").map(str::trim_start).unwrap_or(s);
        let output_delta = match self.protocol {
            CaptureProtocol::Responses => is_output_delta(payload),
            CaptureProtocol::AnthropicMessages => is_anthropic_output_delta(payload),
        };
        if self.ttft_ms.is_none() && output_delta {
            self.ttft_ms = Some(self.start.elapsed().as_millis() as i64);
        }
        match self.protocol {
            CaptureProtocol::Responses => {
                if let Some(usage) = parse_response_usage(payload) {
                    self.usage = Some(usage); // keep the LAST successful parse
                }
            }
            CaptureProtocol::AnthropicMessages => {
                if let Some(incoming) = parse_anthropic_usage(payload) {
                    let usage = self.usage.get_or_insert_with(ResponseUsage::default);
                    usage.input_tokens = max_present(usage.input_tokens, incoming.input_tokens);
                    usage.output_tokens = max_present(usage.output_tokens, incoming.output_tokens);
                    usage.cached_input_tokens =
                        max_present(usage.cached_input_tokens, incoming.cached_input_tokens);
                    usage.cache_write_input_tokens = max_present(
                        usage.cache_write_input_tokens,
                        incoming.cache_write_input_tokens,
                    );
                    usage.reported_total_tokens =
                        max_present(usage.reported_total_tokens, incoming.reported_total_tokens);
                }
            }
        }
        if self.served_tier.is_none() {
            // Only a terminal frame carries the served tier; a request-echo elsewhere in the
            // stream must not be mistaken for it.
            self.served_tier = parse_response_service_tier(payload);
        }
        if self.terminal_outcome.is_none() {
            let event_type = serde_json::from_str::<Value>(payload)
                .ok()
                .and_then(|value| value.get("type")?.as_str().map(str::to_owned));
            self.terminal_outcome = match self.protocol {
                CaptureProtocol::Responses => match event_type.as_deref() {
                    Some("response.completed") => {
                        Some(polyflare_store::RequestProtocolOutcome::Completed)
                    }
                    Some("response.failed") => {
                        Some(polyflare_store::RequestProtocolOutcome::Failed)
                    }
                    Some("response.incomplete") => {
                        Some(polyflare_store::RequestProtocolOutcome::Incomplete)
                    }
                    _ => None,
                },
                CaptureProtocol::AnthropicMessages => match event_type.as_deref() {
                    Some("message_stop" | "message") => {
                        Some(polyflare_store::RequestProtocolOutcome::Completed)
                    }
                    Some("error") => Some(polyflare_store::RequestProtocolOutcome::Failed),
                    _ => None,
                },
            };
        }
        // Sniff a status-200 stream for a smuggled rate-limit/overload error frame, so the account
        // is cooled as if the upstream had returned a real 429/529 (see `stream_error_status`).
        if self.protocol == CaptureProtocol::AnthropicMessages && self.stream_error_status.is_none()
        {
            self.stream_error_status = parse_anthropic_stream_error(payload);
        }
    }

    /// Fire `on_done` with the usage/TTFT captured so far, guarded by `Option::take` so it never
    /// double-fires regardless of whether this is called from `poll_next`'s `Ready(None)` arm or
    /// from `Drop::drop` (or, in principle, both).
    ///
    /// Best-effort: before reading `self.usage`, attempts to parse whatever remains in `pending`
    /// as a final unterminated line — the last frame's `data: {...}` line may not end in `\n`.
    fn fire_on_done(&mut self, fallback: polyflare_store::RequestProtocolOutcome) {
        if !self.pending.is_empty() {
            let remainder = std::mem::take(&mut self.pending);
            self.process_line(&remainder);
        }
        if self.protocol == CaptureProtocol::AnthropicMessages {
            if let Some(usage) = self.usage.as_mut() {
                usage.reported_total_tokens = usage
                    .input_tokens
                    .zip(usage.output_tokens)
                    .and_then(|(input, output)| input.checked_add(output));
            }
        }
        if let Some(on_done) = self.on_done.take() {
            on_done(CapturedUsage {
                usage: self.usage,
                served_tier: self.served_tier,
                ttft_ms: self.ttft_ms,
                duration_ms: Some(self.start.elapsed().as_millis() as i64),
                protocol_outcome: self.terminal_outcome.unwrap_or(fallback),
                stream_error_status: self.stream_error_status,
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
        // `Option<ResponseUsage>`, `Vec<u8>` — mirroring `TranslatingStream`'s all-Unpin-fields
        // idiom in `translate_stream.rs`), so plain `get_mut` + `Pin::new` is sufficient; no
        // unsafe pin projection needed.
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => {
                this.observe(&bytes);
                // UNCHANGED: same `Bytes` handle we received, byte-for-byte passthrough.
                Poll::Ready(Some(Ok(bytes)))
            }
            Poll::Ready(Some(Err(e))) => {
                this.fire_on_done(polyflare_store::RequestProtocolOutcome::TransportLost);
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                this.fire_on_done(this.eof_outcome);
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
        self.fire_on_done(polyflare_store::RequestProtocolOutcome::Cancelled);
    }
}

#[cfg(test)]
mod served_tier_tests {
    use super::*;

    #[test]
    fn the_served_tier_comes_only_from_a_terminal_frame() {
        assert_eq!(
            parse_response_service_tier(
                r#"{"type":"response.completed","response":{"service_tier":"priority"}}"#
            ),
            Some(ServedTier::Priority)
        );
        // `fast` is the Codex spelling of priority; billing must treat them alike.
        assert_eq!(
            parse_response_service_tier(
                r#"{"type":"response.completed","response":{"service_tier":"fast"}}"#
            ),
            Some(ServedTier::Priority)
        );
        assert_eq!(
            parse_response_service_tier(
                r#"{"type":"response.completed","response":{"service_tier":"default"}}"#
            ),
            Some(ServedTier::Default)
        );
        // A non-terminal frame echoing the request must not be mistaken for what was served.
        assert_eq!(
            parse_response_service_tier(
                r#"{"type":"response.created","response":{"service_tier":"priority"}}"#
            ),
            None
        );
        // No tier reported at all is unknown, not a downgrade.
        assert_eq!(
            parse_response_service_tier(r#"{"type":"response.completed","response":{}}"#),
            None
        );
        assert_eq!(
            parse_response_service_tier(
                r#"{"type":"response.completed","response":{"service_tier":""}}"#
            ),
            None
        );
    }

    #[test]
    fn an_unknown_tier_is_bounded_rather_than_forwarded_verbatim() {
        // The raw value is upstream-controlled, so it must never reach telemetry as-is.
        assert_eq!(
            parse_response_service_tier(
                r#"{"type":"response.completed","response":{"service_tier":"experimental-tier-xyz"}}"#
            ),
            Some(ServedTier::Other)
        );
        assert_eq!(ServedTier::Other.as_str(), "other");
        assert!(!ServedTier::Other.is_priority());
        assert!(ServedTier::Priority.is_priority());
        assert!(!ServedTier::Default.is_priority());
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
            Ok::<_, std::io::Error>(Bytes::from("data: {\"type\":\"response.created\"}\n\n")),
            Ok(Bytes::from(
                "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n",
            )),
            Ok(Bytes::from(
                "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"usage\":{\"input_tokens\":8380,\"output_tokens\":120,\"input_tokens_details\":{\"cached_tokens\":6912},\"output_tokens_details\":{\"reasoning_tokens\":40}}}}\n\n",
            )),
        ];
        let captured = Arc::new(Mutex::new(None));
        let c2 = captured.clone();
        let s = UsageCapturingStream::new(stream::iter(frames), Instant::now(), move |cu| {
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
        assert_eq!(
            cu.protocol_outcome,
            polyflare_store::RequestProtocolOutcome::Completed
        );
    }

    #[tokio::test]
    async fn anthropic_stream_merges_cache_usage_and_terminal_output() {
        let frames = vec![
            Ok::<_, std::io::Error>(Bytes::from_static(
                b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":80,\"cache_creation_input_tokens\":10,\"cache_read_input_tokens\":20,\"output_tokens\":0}}}\n\n",
            )),
            Ok(Bytes::from_static(
                b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\n",
            )),
            Ok(Bytes::from_static(
                b"event: message_delta\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":25}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
            )),
        ];
        let expected = frames
            .iter()
            .map(|frame| frame.as_ref().unwrap().clone())
            .collect::<Vec<_>>();
        let captured = Arc::new(Mutex::new(None));
        let captured_2 = captured.clone();
        let stream = UsageCapturingStream::new_anthropic_with_eof_outcome(
            stream::iter(frames),
            Instant::now(),
            polyflare_store::RequestProtocolOutcome::TransportLost,
            move |usage| *captured_2.lock().unwrap() = Some(usage),
        );
        let actual = stream.map(|chunk| chunk.unwrap()).collect::<Vec<_>>().await;

        assert_eq!(
            actual, expected,
            "the Anthropic observer must preserve bytes"
        );
        let captured = captured.lock().unwrap().take().unwrap();
        let usage = captured.usage.unwrap();
        assert_eq!(usage.input_tokens, Some(110));
        assert_eq!(usage.cached_input_tokens, Some(20));
        assert_eq!(usage.cache_write_input_tokens, Some(10));
        assert_eq!(usage.output_tokens, Some(25));
        assert_eq!(usage.reported_total_tokens, Some(135));
        assert!(captured.ttft_ms.is_some());
        assert_eq!(
            captured.protocol_outcome,
            polyflare_store::RequestProtocolOutcome::Completed
        );
        assert_eq!(captured.stream_error_status, None, "a clean stream has no error status");
    }

    #[test]
    fn parse_anthropic_stream_error_maps_only_the_two_actionable_types() {
        assert_eq!(
            parse_anthropic_stream_error(r#"{"type":"error","error":{"type":"rate_limit_error"}}"#),
            Some(429)
        );
        assert_eq!(
            parse_anthropic_stream_error(r#"{"type":"error","error":{"type":"overloaded_error"}}"#),
            Some(529)
        );
        // An ordinary request error must NOT cool the account.
        assert_eq!(
            parse_anthropic_stream_error(
                r#"{"type":"error","error":{"type":"invalid_request_error"}}"#
            ),
            None
        );
        // A non-error frame is never a bench signal.
        assert_eq!(parse_anthropic_stream_error(r#"{"type":"message_stop"}"#), None);
    }

    #[tokio::test]
    async fn a_mid_stream_overloaded_error_on_a_200_stream_is_sniffed() {
        // A status-200 stream that emits an `event: error` overloaded frame must be caught so the
        // account is cooled — exactly the silent-failure case a status-code-only proxy misses.
        let frames = vec![
            Ok::<_, std::io::Error>(Bytes::from_static(
                b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":50,\"output_tokens\":0}}}\n\n",
            )),
            Ok(Bytes::from_static(
                b"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n",
            )),
        ];
        let expected = frames
            .iter()
            .map(|f| f.as_ref().unwrap().clone())
            .collect::<Vec<_>>();
        let captured = Arc::new(Mutex::new(None));
        let captured_2 = captured.clone();
        let stream = UsageCapturingStream::new_anthropic_with_eof_outcome(
            stream::iter(frames),
            Instant::now(),
            polyflare_store::RequestProtocolOutcome::TransportLost,
            move |u| *captured_2.lock().unwrap() = Some(u),
        );
        let actual = stream.map(|c| c.unwrap()).collect::<Vec<_>>().await;
        assert_eq!(actual, expected, "bytes must still pass through unchanged");
        let cu = captured.lock().unwrap().take().unwrap();
        assert_eq!(cu.stream_error_status, Some(529), "overloaded_error → 529");
    }

    #[tokio::test]
    async fn buffered_anthropic_message_captures_top_level_usage() {
        let captured = Arc::new(Mutex::new(None));
        let captured_2 = captured.clone();
        let body = UsageCapturingStream::new_anthropic_with_eof_outcome(
            stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from_static(
                b"{\"type\":\"message\",\"usage\":{\"input_tokens\":5,\"cache_read_input_tokens\":3,\"output_tokens\":2}}",
            ))]),
            Instant::now(),
            polyflare_store::RequestProtocolOutcome::Completed,
            move |usage| *captured_2.lock().unwrap() = Some(usage),
        );
        let _: Vec<_> = body.collect().await;

        let captured = captured.lock().unwrap().take().unwrap();
        assert_eq!(
            captured.usage,
            Some(ResponseUsage {
                input_tokens: Some(8),
                output_tokens: Some(2),
                cached_input_tokens: Some(3),
                reported_total_tokens: Some(10),
                ..ResponseUsage::default()
            })
        );
        assert_eq!(
            captured.protocol_outcome,
            polyflare_store::RequestProtocolOutcome::Completed
        );
    }

    #[tokio::test]
    async fn captures_failed_and_incomplete_terminal_outcomes() {
        for (event, expected) in [
            (
                "response.failed",
                polyflare_store::RequestProtocolOutcome::Failed,
            ),
            (
                "response.incomplete",
                polyflare_store::RequestProtocolOutcome::Incomplete,
            ),
        ] {
            let frames = vec![Ok::<_, std::io::Error>(Bytes::from(format!(
                "data: {{\"type\":\"{event}\",\"response\":{{\"id\":\"r\"}}}}\n\n"
            )))];
            let captured = Arc::new(Mutex::new(None));
            let c2 = captured.clone();
            let s = UsageCapturingStream::new(stream::iter(frames), Instant::now(), move |cu| {
                *c2.lock().unwrap() = Some(cu)
            });
            let _: Vec<_> = s.collect().await;

            assert_eq!(
                captured.lock().unwrap().take().unwrap().protocol_outcome,
                expected
            );
        }
    }

    #[tokio::test]
    async fn terminal_less_eof_is_transport_lost_but_early_drop_is_cancelled() {
        let eof_capture = Arc::new(Mutex::new(None));
        let eof_capture_2 = eof_capture.clone();
        let eof_stream = UsageCapturingStream::new(
            stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from(
                "data: {\"type\":\"response.created\"}\n\n",
            ))]),
            Instant::now(),
            move |cu| *eof_capture_2.lock().unwrap() = Some(cu),
        );
        let _: Vec<_> = eof_stream.collect().await;
        assert_eq!(
            eof_capture.lock().unwrap().take().unwrap().protocol_outcome,
            polyflare_store::RequestProtocolOutcome::TransportLost
        );

        let cancel_capture = Arc::new(Mutex::new(None));
        let cancel_capture_2 = cancel_capture.clone();
        let mut cancel_stream = UsageCapturingStream::new(
            stream::iter(vec![
                Ok::<_, std::io::Error>(Bytes::from("data: {\"type\":\"response.created\"}\n\n")),
                Ok(Bytes::from("data: never consumed\n\n")),
            ]),
            Instant::now(),
            move |cu| *cancel_capture_2.lock().unwrap() = Some(cu),
        );
        cancel_stream.next().await.unwrap().unwrap();
        drop(cancel_stream);
        assert_eq!(
            cancel_capture
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .protocol_outcome,
            polyflare_store::RequestProtocolOutcome::Cancelled
        );
    }

    #[tokio::test]
    async fn buffered_http_body_uses_its_status_derived_eof_outcome() {
        let captured = Arc::new(Mutex::new(None));
        let captured_2 = captured.clone();
        let body = UsageCapturingStream::new_with_eof_outcome(
            stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from_static(
                b"{\"error\":{\"code\":\"invalid_request\"}}",
            ))]),
            Instant::now(),
            polyflare_store::RequestProtocolOutcome::Failed,
            move |cu| *captured_2.lock().unwrap() = Some(cu),
        );
        let _: Vec<_> = body.collect().await;
        assert_eq!(
            captured.lock().unwrap().take().unwrap().protocol_outcome,
            polyflare_store::RequestProtocolOutcome::Failed
        );
    }

    #[tokio::test]
    async fn upstream_stream_error_is_transport_lost() {
        let captured = Arc::new(Mutex::new(None));
        let c2 = captured.clone();
        let mut s = UsageCapturingStream::new(
            stream::iter(vec![Err::<Bytes, _>(std::io::Error::other("lost"))]),
            Instant::now(),
            move |cu| *c2.lock().unwrap() = Some(cu),
        );

        assert!(s.next().await.unwrap().is_err());
        assert_eq!(
            captured.lock().unwrap().take().unwrap().protocol_outcome,
            polyflare_store::RequestProtocolOutcome::TransportLost
        );
    }

    #[tokio::test]
    async fn ttft_waits_for_first_output_delta_not_first_sse_frame() {
        let frames = vec![
            Ok::<_, std::io::Error>(Bytes::from("data: {\"type\":\"response.created\"}\n\n")),
            Ok(Bytes::from(
                "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n",
            )),
        ];
        let captured = Arc::new(Mutex::new(None));
        let c2 = captured.clone();
        let mut s = UsageCapturingStream::new(stream::iter(frames), Instant::now(), move |cu| {
            *c2.lock().unwrap() = Some(cu)
        });

        s.next().await.unwrap().unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        s.next().await.unwrap().unwrap();
        assert!(s.next().await.is_none());

        let ttft_ms = captured.lock().unwrap().take().unwrap().ttft_ms;
        assert!(
            ttft_ms.is_some_and(|value| value >= 10),
            "response.created must not set TTFT; expected the later output delta, got {ttft_ms:?}"
        );
    }

    /// Live-row-tps-basis fix: `duration_ms` must be measured from the SAME `start` the caller
    /// passes into `new` (the route's own clock origin), not from this wrapper's own construction
    /// time — so it shares an origin with `ttft_ms` and `derive_tps` in `read_api.rs` gets a
    /// sane (not inflated) result. This is the crux regression test for the bug: before the fix,
    /// `duration_ms` didn't exist on `CapturedUsage` at all.
    #[tokio::test]
    async fn duration_ms_is_measured_from_the_passed_start() {
        let frames = vec![Ok::<_, std::io::Error>(Bytes::from(
            "data: {\"type\":\"response.created\"}\n\n",
        ))];
        let captured = Arc::new(Mutex::new(None));
        let c2 = captured.clone();
        let start = Instant::now();
        let s = UsageCapturingStream::new(stream::iter(frames), start, move |cu| {
            *c2.lock().unwrap() = Some(cu)
        });
        let _out: Vec<_> = s.map(|r| r.unwrap()).collect().await;
        let cu = captured.lock().unwrap().take().unwrap();
        assert!(
            cu.duration_ms.is_some(),
            "duration_ms must be recorded once the stream ends"
        );
        assert_eq!(
            cu.ttft_ms, None,
            "a response.created control frame is not a generated token"
        );
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
        let mut s = UsageCapturingStream::new(stream::iter(frames), Instant::now(), move |cu| {
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
        assert_eq!(cu.ttft_ms, None, "no output delta was consumed before drop");
    }

    #[test]
    fn parses_usage_from_completed_frame() {
        let f = r#"{"type":"response.completed","response":{"id":"resp_1","usage":{"input_tokens":8380,"output_tokens":120,"total_tokens":8500,"input_tokens_details":{"cached_tokens":6912,"cache_write_tokens":256},"output_tokens_details":{"reasoning_tokens":40}}}}"#;
        let u = parse_response_usage(f).unwrap();
        assert_eq!(u.input_tokens, Some(8380));
        assert_eq!(u.output_tokens, Some(120));
        assert_eq!(u.cached_input_tokens, Some(6912));
        assert_eq!(u.cache_write_input_tokens, Some(256));
        assert_eq!(u.reasoning_tokens, Some(40));
        assert_eq!(u.reported_total_tokens, Some(8500));
    }

    #[test]
    fn ignores_negative_usage_counts_instead_of_persisting_invalid_evidence() {
        let u = parse_response_usage(
            r#"{"type":"response.completed","response":{"usage":{"input_tokens":10,"output_tokens":-1,"total_tokens":-2,"input_tokens_details":{"cached_tokens":-3,"cache_write_tokens":-4},"output_tokens_details":{"reasoning_tokens":-5}}}}"#,
        )
        .unwrap();
        assert_eq!(
            u,
            ResponseUsage {
                input_tokens: Some(10),
                ..ResponseUsage::default()
            }
        );
    }

    #[test]
    fn parses_sakana_orchestration_usage_from_token_detail_objects() {
        let f = r#"{"type":"response.completed","response":{"usage":{"input_tokens":100,"output_tokens":25,"input_tokens_details":{"cached_tokens":20,"orchestration_input_tokens":10,"orchestration_input_cached_tokens":3},"output_tokens_details":{"orchestration_output_tokens":4}}}}"#;
        let u = parse_response_usage(f).unwrap();
        assert_eq!(u.orchestration_input_tokens, Some(10));
        assert_eq!(u.orchestration_cached_input_tokens, Some(3));
        assert_eq!(u.orchestration_output_tokens, Some(4));
    }

    #[test]
    fn preserves_legacy_orchestration_usage_shapes() {
        let top_level = parse_response_usage(
            r#"{"type":"response.completed","response":{"usage":{"orchestration_input_tokens":10,"orchestration_output_tokens":4,"orchestration_cached_input_tokens":3}}}"#,
        )
        .unwrap();
        assert_eq!(top_level.orchestration_input_tokens, Some(10));
        assert_eq!(top_level.orchestration_cached_input_tokens, Some(3));
        assert_eq!(top_level.orchestration_output_tokens, Some(4));

        let detail_object = parse_response_usage(
            r#"{"type":"response.completed","response":{"usage":{"orchestration_tokens_details":{"input_tokens":11,"output_tokens":5,"cached_input_tokens":2}}}}"#,
        )
        .unwrap();
        assert_eq!(detail_object.orchestration_input_tokens, Some(11));
        assert_eq!(detail_object.orchestration_cached_input_tokens, Some(2));
        assert_eq!(detail_object.orchestration_output_tokens, Some(5));
    }

    #[test]
    fn pressure_equivalent_discounts_cached_input_and_weights_output() {
        let pressure = pressure_equivalent_tokens(ResponseUsage {
            input_tokens: Some(100_000),
            output_tokens: Some(2_000),
            cached_input_tokens: Some(80_000),
            reasoning_tokens: None,
            ..Default::default()
        });
        assert_eq!(
            pressure,
            Some(38_000),
            "20k uncached + 10k cached-equivalent + 8k output-equivalent"
        );
    }

    #[test]
    fn pressure_equivalent_rejects_negative_or_missing_required_usage() {
        assert_eq!(
            pressure_equivalent_tokens(ResponseUsage {
                input_tokens: Some(-1),
                output_tokens: Some(10),
                cached_input_tokens: None,
                reasoning_tokens: None,
                ..Default::default()
            }),
            None
        );
        assert_eq!(
            pressure_equivalent_tokens(ResponseUsage {
                input_tokens: Some(10),
                output_tokens: None,
                cached_input_tokens: None,
                reasoning_tokens: None,
                ..Default::default()
            }),
            None
        );
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

    #[tokio::test]
    async fn captures_usage_from_a_completed_frame_split_across_chunks() {
        let full = "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"usage\":{\"input_tokens\":8380,\"output_tokens\":120,\"input_tokens_details\":{\"cached_tokens\":6912},\"output_tokens_details\":{\"reasoning_tokens\":40}}}}\n";
        let (a, b) = full.split_at(50); // split the single data: line mid-JSON across two chunks
        let frames = vec![
            Ok::<_, std::io::Error>(Bytes::from(a.to_owned())),
            Ok(Bytes::from(b.to_owned())),
        ];
        let captured = Arc::new(Mutex::new(None));
        let c2 = captured.clone();
        let s = UsageCapturingStream::new(stream::iter(frames), Instant::now(), move |cu| {
            *c2.lock().unwrap() = Some(cu)
        });
        let out: Vec<_> = s.map(|r| r.unwrap()).collect().await;
        // passthrough still byte-identical for BOTH split halves
        assert_eq!(out[0], Bytes::from(a.to_owned()));
        assert_eq!(out[1], Bytes::from(b.to_owned()));
        // usage reassembled across the split
        assert_eq!(
            captured
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .usage
                .unwrap()
                .input_tokens,
            Some(8380)
        );
    }
}
