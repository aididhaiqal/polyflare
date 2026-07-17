//! The WS frame codec (M5a Task 4): build the outbound `response.create` envelope, re-serialize
//! received frames into the SSE bytes the rest of the stack already understands, and classify a
//! received frame into an outcome the (later) turn stream / executor act on.
//!
//! **Pure functions only.** No connection, no turn stream, no delta-planning, no `Executor` impl —
//! those are Tasks 5-7. Everything here operates on already-parsed/already-received values; no
//! network I/O.
//!
//! **Wire authority:** every shape below is cited to `docs/WS-GROUND-TRUTH-CODEX.md` (source facts,
//! cited to real `codex-rs` file:line) or to a *live-measured* fact from that doc's §5 / the proven
//! `ws_body()` in `crates/polyflare-server/examples/ws_vs_sse_probe.rs`. Nothing here is invented.
//!
//! **Content-safety:** frames and the outbound body carry conversation content. Nothing in this
//! module implements or derives `Debug` for a type that would print one — `build_response_create`
//! and `frame_to_sse` return bare `serde_json::Value`/`Bytes` (already unavoidably content-bearing
//! by their nature, same as `PreparedRequest::body`), and this module itself never logs a frame.
//! Any caller that wraps these in a struct must redact it in `Debug`, mirroring `PreparedRequest`
//! (`polyflare-core/src/types.rs:42-50`).

use bytes::Bytes;
use serde_json::Value;

use polyflare_core::{ExecError, FailureSignal};

/// The outcome of classifying one received (already-parsed) frame.
///
/// This is a deliberate **four-way** split, one variant more than the plan's shorthand
/// "Event / Terminal / Error(ExecError)" list — see the `AnchorMiss` doc below for why: the dead-
/// anchor case must be distinguishable from a generic error using only this return value, or the
/// caller (Task 7) would have to re-derive `error.code` from the raw frame itself, which reopens
/// exactly the status-only-keying bug this milestone exists to close.
#[derive(Debug)]
pub enum FrameClass {
    /// A non-terminal event frame to re-frame as SSE and pass through as-is (`response.created`,
    /// `.output_text.delta`, etc.), OR a frame of a type this codec does not recognize at all.
    /// Ground truth §3 (`sse/responses.rs:467-469`): unknown types are ignored, never fatal — so
    /// "unrecognized" and "known non-terminal event" collapse to the same safe default here rather
    /// than needing a separate "unknown" variant that callers would have to treat identically to
    /// `Event` anyway.
    Event,
    /// A terminal frame: `response.completed` / `response.failed` / `response.incomplete`. The
    /// stream ends after this one (`Poll::Ready(None)`, Task 5) — still re-framed as SSE and passed
    /// through first, since even a terminal *failure* frame carries content the client's own
    /// stream parser needs to see (e.g. a quota/context-window `response.failed`).
    Terminal,
    /// The wrapped WS-only error envelope (ground truth §3/§5) with
    /// `error.code == "previous_response_not_found"` — the dead-anchor case. **Recoverable**: Task 7
    /// strips the anchor and resends full history on the same socket; this must never reach the
    /// client as an error. Split out from `Error` specifically because ground truth §5 shows a
    /// dead anchor and a genuine bad request share the identical `status: 400` envelope shape —
    /// only `code` tells them apart, and this variant IS that discrimination, made once, here,
    /// rather than left for every future caller to re-derive from the raw frame.
    AnchorMiss,
    /// The wrapped WS-only error envelope with `error.code ==
    /// "websocket_connection_limit_reached"` (ground truth §2's server 60-minute connection cap).
    /// **Recoverable, same reasoning as [`FrameClass::AnchorMiss`]**: Task 7 reconnects and
    /// full-resends, bounded; this must never reach the client. Given its own variant rather than
    /// left inside the generic [`FrameClass::Error`] bucket for the identical reason `AnchorMiss`
    /// is: `ExecError::UpstreamStatus` carries only a numeric `status` (ground truth doesn't pin
    /// one for this envelope — a real backend could reuse any status alongside this `code`), so a
    /// caller reading only the resulting `ExecError` could never distinguish "reconnect and retry"
    /// from "surface this 429/400 to the client" without this classification happening HERE, once,
    /// while `code` is still in hand.
    ConnectionLimitReached,
    /// Any other hard error: a genuine wrapped error envelope (any `code` other than
    /// `previous_response_not_found`), keyed into the same [`ExecError::UpstreamStatus`] shape the
    /// HTTP-SSE executor already produces (`executor.rs:150-155`) so the existing health-writeback
    /// path (`record_rate_limit` / `record_transient_error` / 502-to-client) keeps working
    /// unchanged underneath a different transport.
    Error(ExecError),
}

/// Build the outbound `response.create` WS envelope (ground truth §3's `ResponseCreateWsRequest`,
/// and the PROVEN shape in `ws_vs_sse_probe.rs::ws_body()`).
///
/// `body` carries every non-input, non-anchor, non-generate field the caller already assembled
/// (`model`, `instructions`, `tools`, `tool_choice`, `parallel_tool_calls`, `reasoning`, `store`,
/// `stream`, `include`, `service_tier`, `prompt_cache_key`, `text`, `client_metadata`, ...) — this
/// function only owns the four fields that vary per turn: `type` (always inserted), `input`
/// (always overwritten with the caller-supplied slice — the full history OR the strict-extension
/// delta, Task 6's decision, not this function's), `previous_response_id` (inserted from `anchor`
/// when `Some`, or actively REMOVED — not left present or set to `null` — when `None`, since a
/// stale `previous_response_id` key surviving from a cloned/reused `body` would silently resurrect
/// an anchor this call was told to omit), and `generate` (inserted only when `Some`; removed
/// otherwise — ground truth §6: `generate` is sent only on prewarm, omitted for every normal turn).
///
/// # Panics
/// If `body` is not a JSON object. Every real caller (a `PreparedRequest::body`-shaped `Value`) is
/// always an object; a non-object here is a caller bug, not a runtime condition to recover from.
pub fn build_response_create(
    body: &Value,
    anchor: Option<&str>,
    input: &[Value],
    generate: Option<bool>,
) -> Value {
    let mut envelope = body.clone();
    let obj = envelope
        .as_object_mut()
        .expect("build_response_create: body must be a JSON object");

    obj.insert("type".to_string(), Value::String("response.create".to_string()));
    obj.insert("input".to_string(), Value::Array(input.to_vec()));

    match anchor {
        Some(id) => {
            obj.insert(
                "previous_response_id".to_string(),
                Value::String(id.to_string()),
            );
        }
        None => {
            // Actively remove, don't just skip inserting: `body` may be a clone of a PREVIOUS
            // turn's envelope (or any caller-assembled value) that already carries a
            // `previous_response_id` key from an earlier anchor. Omission-by-absence must win.
            obj.remove("previous_response_id");
        }
    }

    match generate {
        Some(g) => {
            obj.insert("generate".to_string(), Value::Bool(g));
        }
        None => {
            obj.remove("generate");
        }
    }

    envelope
}

/// Re-serialize one received WS text frame into the SSE bytes the rest of the stack already
/// parses: `TranslatingStream::feed_line` (`translate_stream.rs:58-79`) and the watchdog's
/// `ResponseIdSniffer` (`watchdog.rs:329-353`) both scan for `data:`-prefixed lines inside a raw
/// `Bytes` chunk. A WS text frame is exactly one JSON value with no wrapping `data:`/blank-line
/// framing of its own, so it must go through this before either of those can see it — handing the
/// raw WS payload through would silently break both (they'd never find a `data:` line at all).
///
/// Parses `frame` as JSON and re-serializes it (rather than wrapping the original bytes verbatim)
/// so a value containing an embedded newline (which JSON string escaping never permits, but a
/// non-JSON or oddly-encoded frame might) can never produce a bytes chunk with more than one
/// logical SSE line — `feed_line`'s line-oriented scan depends on that.
///
/// Returns `None` if `frame` is not valid JSON: a malformed frame should be dropped, not forwarded
/// as a `data:` line the client's own JSON parser would then choke on downstream.
pub fn frame_to_sse(frame: &str) -> Option<Bytes> {
    let value: Value = serde_json::from_str(frame).ok()?;
    let compact = serde_json::to_string(&value).ok()?;
    Some(Bytes::from(format!("data: {compact}\n\n")))
}

/// The WS-only wrapped error envelope's assumed `headers` map key for `Retry-After` seconds.
///
/// **Known-unverified** (per `.superpowers/sdd/m5a-task-2-report.md`): ground truth §3 only cites
/// the envelope's generic `"headers":{...}` field, never a specific key name or casing. This
/// lowercase `"retry-after"` matches the testkit mock's placeholder
/// (`ws_mock.rs::ScriptedTurn::rate_limited_429`) but is NOT itself ground-truth-cited. Re-verify
/// against a live 429-over-WS capture before trusting this in production; if a capture shows a
/// different key, this is the one line to change.
const ENVELOPE_RETRY_AFTER_KEY: &str = "retry-after";

/// Parse the envelope's `headers` map the way `retry_after_secs` reads the real HTTP `Retry-After`
/// header (`executor.rs:34-40`): numeric-seconds only (no HTTP-date form), trimmed, non-negative.
/// Same semantics, different carrier — a JSON string value inside `headers` rather than an actual
/// HTTP response header.
fn envelope_retry_after(headers: &Value) -> Option<i64> {
    headers
        .get(ENVELOPE_RETRY_AFTER_KEY)
        .and_then(Value::as_str)
        .and_then(|s| s.trim().parse::<i64>().ok())
        .filter(|&s| s >= 0)
}

/// Classify one already-received, already-parsed frame.
///
/// Frame-type dispatch first (ground truth §3): the three terminal event-type strings become
/// [`FrameClass::Terminal`]; anything else falls through to [`FrameClass::Event`] EXCEPT the
/// WS-only wrapped error envelope (`"type":"error"`), which gets its own branch.
///
/// Inside that branch, **`error.code` is inspected before `status` is mapped to anything** — the
/// one invariant ground truth §5 (live-measured) makes load-bearing: a dead anchor and a genuine
/// bad request share the identical `status: 400` envelope shape, and only `code` tells them apart.
/// Reversing this order (map `status` first, special-case `code` only for the leftover "how do I
/// interpret this 400" question) would be the same bug either way, but doing `code` first make the
/// discrimination the FIRST thing this function does, not a Tuesday afternoon's optional adjustment.
pub fn classify(frame: &Value) -> FrameClass {
    let frame_type = frame.get("type").and_then(Value::as_str).unwrap_or("");

    match frame_type {
        "response.completed" | "response.failed" | "response.incomplete" => FrameClass::Terminal,
        "error" => {
            let code = frame
                .pointer("/error/code")
                .and_then(Value::as_str)
                .unwrap_or("");

            // MUST come before any status-based mapping below (see fn doc + this module's header
            // doc). This is the discrimination ground truth §5 says only `code` can make.
            if code == "previous_response_not_found" {
                return FrameClass::AnchorMiss;
            }
            // Same precedence tier as the anchor-miss check above — see `ConnectionLimitReached`'s
            // doc for why this also needs a dedicated variant rather than falling into the generic
            // status-keyed `Error` bucket below.
            if code == "websocket_connection_limit_reached" {
                return FrameClass::ConnectionLimitReached;
            }

            let status = frame.get("status").and_then(Value::as_u64).unwrap_or(0) as u16;
            let retry_after = frame.get("headers").and_then(envelope_retry_after);
            FrameClass::Error(ExecError::UpstreamStatus(FailureSignal {
                status,
                retry_after,
                error_code: None,
            }))
        }
        // Ground truth §3 (`sse/responses.rs:467-469`): unknown types are ignored, never fatal.
        // Every OTHER known-but-non-terminal event type (`response.created`,
        // `.output_item.added/.done`, `.output_text.delta`, `.reasoning_*`, `response.metadata`,
        // `codex.rate_limits`, ...) also lands here: this codec's job is re-frame-and-pass-through
        // for all of them, not per-type handling (that's the turn stream's / a future R3 task's
        // concern), so "known non-terminal" and "unrecognized" are deliberately the same case.
        _ => FrameClass::Event,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ---- build_response_create ----------------------------------------------------------

    #[test]
    fn anchorless_build_omits_previous_response_id_key_entirely() {
        let body = json!({
            "model": "gpt-5.6-sol",
            "instructions": "be helpful",
            "store": false,
            "stream": true,
        });
        let input = vec![json!({"role": "user", "content": "hi"})];

        let out = build_response_create(&body, None, &input, None);

        let obj = out.as_object().expect("object");
        assert!(
            !obj.contains_key("previous_response_id"),
            "the KEY must be absent, not present-with-null, when anchor is None: {out}"
        );
        assert_eq!(out["type"], json!("response.create"));
        assert_eq!(out["input"], json!(input));
        assert_eq!(out["model"], json!("gpt-5.6-sol"));
        assert!(
            !obj.contains_key("generate"),
            "generate must be omitted unless explicitly set: {out}"
        );
    }

    #[test]
    fn anchorless_build_strips_a_stale_previous_response_id_from_a_reused_body() {
        // Guards the "actively remove, don't just skip inserting" branch: a `body` that already
        // carries a previous_response_id (e.g. cloned from a prior turn's envelope) must not let
        // it survive when this call is told anchor = None.
        let body = json!({
            "model": "m",
            "previous_response_id": "resp_stale_from_a_previous_turn",
        });

        let out = build_response_create(&body, None, &[], None);

        assert!(
            !out.as_object().unwrap().contains_key("previous_response_id"),
            "a stale previous_response_id already present on `body` must be actively stripped, \
             not left behind, when anchor is None: {out}"
        );
    }

    #[test]
    fn anchored_build_sets_previous_response_id_and_carries_only_the_delta_input() {
        let body = json!({
            "model": "gpt-5.6-sol",
            "store": false,
        });
        // The delta only — NOT the full conversation history, proving this function carries
        // exactly what it's given rather than silently including anything else.
        let delta_input = vec![json!({"role": "user", "content": "just the new turn"})];

        let out = build_response_create(&body, Some("resp_abc123"), &delta_input, None);

        assert_eq!(out["previous_response_id"], json!("resp_abc123"));
        assert_eq!(out["input"], json!(delta_input));
        assert_eq!(
            out["input"].as_array().unwrap().len(),
            1,
            "must carry ONLY the delta input, not a fuller history: {out}"
        );
    }

    #[test]
    fn generate_is_omitted_unless_explicitly_set() {
        let body = json!({"model": "m"});

        let without = build_response_create(&body, None, &[], None);
        assert!(!without.as_object().unwrap().contains_key("generate"));

        let with_false = build_response_create(&body, None, &[], Some(false));
        assert_eq!(with_false["generate"], json!(false));

        let with_true = build_response_create(&body, None, &[], Some(true));
        assert_eq!(with_true["generate"], json!(true));
    }

    #[test]
    fn build_preserves_non_input_non_anchor_fields_from_body() {
        // Everything else the ws_body() probe / ground truth §3 lists as part of
        // ResponseCreateWsRequest must survive untouched: this function only owns
        // type/input/previous_response_id/generate.
        let body = json!({
            "model": "gpt-5.6-sol",
            "instructions": "be helpful",
            "tool_choice": "auto",
            "parallel_tool_calls": false,
            "reasoning": {"effort": "low"},
            "store": false,
            "stream": true,
            "include": [],
            "prompt_cache_key": "nonce-xyz",
        });

        let out = build_response_create(&body, None, &[], None);

        assert_eq!(out["instructions"], json!("be helpful"));
        assert_eq!(out["tool_choice"], json!("auto"));
        assert_eq!(out["parallel_tool_calls"], json!(false));
        assert_eq!(out["reasoning"], json!({"effort": "low"}));
        assert_eq!(out["store"], json!(false));
        assert_eq!(out["stream"], json!(true));
        assert_eq!(out["include"], json!([]));
        assert_eq!(out["prompt_cache_key"], json!("nonce-xyz"));
    }

    // ---- frame_to_sse ---------------------------------------------------------------------

    #[test]
    fn frame_to_sse_produces_exactly_data_colon_json_double_newline() {
        let frame = r#"{"type":"response.output_text.delta","delta":"hi"}"#;

        let bytes = frame_to_sse(frame).expect("valid JSON frame must produce Some");
        let text = String::from_utf8(bytes.to_vec()).expect("utf8");

        assert!(
            text.starts_with("data: "),
            "must start with the SSE `data: ` prefix: {text:?}"
        );
        assert!(
            text.ends_with("\n\n"),
            "must end with a blank-line terminator: {text:?}"
        );
        // Exactly one logical line's worth of content between prefix and terminator.
        let payload = text
            .strip_prefix("data: ")
            .unwrap()
            .strip_suffix("\n\n")
            .unwrap();
        assert!(
            !payload.contains('\n'),
            "payload must be a single line: {text:?}"
        );
        let round_tripped: Value = serde_json::from_str(payload).expect("payload is valid JSON");
        assert_eq!(round_tripped, json!({"type":"response.output_text.delta","delta":"hi"}));
    }

    #[test]
    fn frame_to_sse_normalizes_whitespace_so_embedded_newlines_cannot_split_the_line() {
        // A frame with internal formatting whitespace (not a raw embedded newline inside a JSON
        // string value, which JSON forbids unescaped anyway, but pretty-printed multi-line JSON as
        // a whole) must come out as a SINGLE compact SSE line, not one `data:` line per source line.
        let frame = "{\n  \"type\": \"response.created\",\n  \"response\": {\"id\": \"resp_1\"}\n}";

        let bytes = frame_to_sse(frame).expect("valid (if pretty-printed) JSON must produce Some");
        let text = String::from_utf8(bytes.to_vec()).unwrap();

        assert_eq!(
            text.trim_end_matches('\n').lines().count(),
            1,
            "must collapse to exactly one logical `data:` line (trailing blank-line \
             terminator aside): {text:?}"
        );
        assert!(text.starts_with("data: {"));
    }

    #[test]
    fn frame_to_sse_returns_none_for_invalid_json() {
        assert_eq!(frame_to_sse("not json at all"), None);
        assert_eq!(frame_to_sse(""), None);
        assert_eq!(frame_to_sse("{unterminated"), None);
    }

    // ---- classify: terminal frames ---------------------------------------------------------

    #[test]
    fn classify_maps_response_completed_to_terminal() {
        let frame = json!({"type": "response.completed", "response": {"id": "resp_1"}});
        assert!(matches!(classify(&frame), FrameClass::Terminal));
    }

    #[test]
    fn classify_maps_response_failed_to_terminal() {
        let frame = json!({
            "type": "response.failed",
            "response": {"error": {"code": "context_window_exceeded", "message": "too long"}}
        });
        assert!(matches!(classify(&frame), FrameClass::Terminal));
    }

    #[test]
    fn classify_maps_response_incomplete_to_terminal() {
        let frame = json!({"type": "response.incomplete", "response": {"id": "resp_1"}});
        assert!(matches!(classify(&frame), FrameClass::Terminal));
    }

    // ---- classify: the load-bearing anchor-miss discrimination -----------------------------

    #[test]
    fn classify_maps_the_dead_anchor_envelope_to_anchor_miss_not_error() {
        // Ground truth §5, live-measured, verbatim shape.
        let frame = json!({
            "type": "error",
            "error": {
                "type": "invalid_request_error",
                "code": "previous_response_not_found",
                "message": "Previous response with id 'resp_xyz' not found.",
                "param": "previous_response_id"
            },
            "status": 400
        });

        assert!(
            matches!(classify(&frame), FrameClass::AnchorMiss),
            "a dead anchor must classify as the dedicated recoverable case, not a generic Error"
        );
    }

    #[test]
    fn classify_maps_connection_limit_reached_to_its_own_variant_not_generic_error() {
        // Ground truth §2/§5: the server 60-minute connection cap. No specific numeric status is
        // pinned by ground truth, so this test deliberately uses one (409) that would otherwise be
        // indistinguishable from an arbitrary generic error if `code` weren't checked first.
        let frame = json!({
            "type": "error",
            "error": {"code": "websocket_connection_limit_reached", "message": "cap reached"},
            "status": 409
        });

        assert!(
            matches!(classify(&frame), FrameClass::ConnectionLimitReached),
            "must classify as the dedicated recoverable case, not a generic Error"
        );
    }

    #[test]
    fn classify_maps_a_genuine_400_with_a_different_code_to_error_not_anchor_miss() {
        // Same `status: 400` as the anchor-miss case above — only `code` differs. This is the
        // exact pair ground truth §5 warns must not be conflated.
        let frame = json!({
            "type": "error",
            "error": {
                "type": "invalid_request_error",
                "code": "invalid_value",
                "message": "some other genuinely bad request"
            },
            "status": 400
        });

        match classify(&frame) {
            FrameClass::Error(ExecError::UpstreamStatus(sig)) => {
                assert_eq!(sig.status, 400);
            }
            other => panic!("expected Error(UpstreamStatus{{status:400}}), got {other:?}"),
        }
    }

    #[test]
    fn classify_carries_status_and_retry_after_for_a_429_envelope() {
        let frame = json!({
            "type": "error",
            "error": {"code": "rate_limit_exceeded", "message": "rate limit exceeded"},
            "status": 429,
            "headers": {"retry-after": "37"}
        });

        match classify(&frame) {
            FrameClass::Error(ExecError::UpstreamStatus(sig)) => {
                assert_eq!(sig.status, 429);
                assert_eq!(sig.retry_after, Some(37));
            }
            other => panic!("expected Error(UpstreamStatus{{status:429,retry_after:Some(37)}}), got {other:?}"),
        }
    }

    #[test]
    fn classify_treats_a_missing_or_unparseable_retry_after_as_none() {
        let no_headers = json!({
            "type": "error",
            "error": {"code": "rate_limit_exceeded", "message": "m"},
            "status": 429
        });
        match classify(&no_headers) {
            FrameClass::Error(ExecError::UpstreamStatus(sig)) => assert_eq!(sig.retry_after, None),
            other => panic!("expected Error(UpstreamStatus), got {other:?}"),
        }

        let garbage_header = json!({
            "type": "error",
            "error": {"code": "rate_limit_exceeded", "message": "m"},
            "status": 429,
            "headers": {"retry-after": "not-a-number"}
        });
        match classify(&garbage_header) {
            FrameClass::Error(ExecError::UpstreamStatus(sig)) => assert_eq!(sig.retry_after, None),
            other => panic!("expected Error(UpstreamStatus), got {other:?}"),
        }

        let negative_header = json!({
            "type": "error",
            "error": {"code": "rate_limit_exceeded", "message": "m"},
            "status": 429,
            "headers": {"retry-after": "-5"}
        });
        match classify(&negative_header) {
            FrameClass::Error(ExecError::UpstreamStatus(sig)) => assert_eq!(sig.retry_after, None),
            other => panic!("expected Error(UpstreamStatus), got {other:?}"),
        }
    }

    // ---- classify: unknown types are ignored, never fatal ----------------------------------

    #[test]
    fn classify_maps_an_unknown_frame_type_to_event_not_an_error() {
        // Ground truth §3 (`sse/responses.rs:467-469`): unknown types are ignored, never fatal.
        // Simulates the server adding a brand-new frame type after this code was written.
        let frame = json!({"type": "a.brand.new.frame.type.from.the.future", "payload": 123});
        assert!(matches!(classify(&frame), FrameClass::Event));
    }

    #[test]
    fn classify_maps_known_non_terminal_event_types_to_event() {
        for ty in [
            "response.created",
            "response.output_item.added",
            "response.output_item.done",
            "response.output_text.delta",
            "response.reasoning_text.delta",
            "response.metadata",
            "codex.rate_limits",
        ] {
            let frame = json!({"type": ty});
            assert!(
                matches!(classify(&frame), FrameClass::Event),
                "expected Event for type {ty}"
            );
        }
    }

    #[test]
    fn classify_treats_a_frame_with_no_type_field_at_all_as_event_not_a_panic() {
        let frame = json!({"payload": "no type field here"});
        assert!(matches!(classify(&frame), FrameClass::Event));
    }
}
