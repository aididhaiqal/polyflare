//! B5 Task 4 — Layer 2 (the keepalive recovery-wait combinator): the content-safe, pure pieces.
//!
//! The actual wait + re-select + splice orchestration lives in `ingress.rs`
//! (`try_layer2_recovery_wait` / `layer2_wait_stream`) — it needs `ingress.rs`'s private
//! `resolve_core_account`/`execute_recovery` helpers, so this module deliberately does NOT
//! duplicate that machinery. What lives here is everything that must be independently reviewable
//! for content-safety in isolation: the keepalive frame, the in-band SSE error frame + its reason
//! codes, and the (currently const, Task-5-will-config-ify) wait-timing defaults.
//!
//! See `docs/superpowers/plans/2026-07-18-b5-antistarvation.md` Task 4 + its Global Constraints
//! ("POST-200 COMMIT", "BOUNDED BUDGET", "RE-SNAPSHOT AFTER THE WAIT").

use std::time::Duration;

use bytes::Bytes;

/// codex-lb's exact keepalive frame (`retry.py`/`support.py`'s
/// `_iter_account_capacity_recovery_wait`): a comment-only SSE line carrying no `data:` field at
/// all — inert to any SSE/JSON parser on the client, and structurally incapable of carrying
/// content. This is the ENTIRE keepalive payload; nothing is ever appended to it.
pub const KEEPALIVE_FRAME: &[u8] = b": keepalive\n\n";

/// Task 4 hardcoded these (the plan's "use a const default 60 for now" for the budget; codex-lb's
/// `retry.py` HEARTBEAT=10s for the heartbeat). Task 5 wires `POLYFLARE_STARVATION_WAIT_BUDGET_SECS`
/// (clamped to codex-lb's `[MIN=1s, MAX=300s]`, with `0` as a documented DISABLE lever — see
/// `crate::config::starvation_wait_budget_secs_from_env`'s doc) and
/// `POLYFLARE_STARVATION_HEARTBEAT_SECS` (clamped to `[1, budget]`) over these, resolved ONCE into
/// `AppState`/`ServeConfig` at startup — NOT a per-request env read. **These two consts now serve
/// ONLY as the two test seams' explicit defaults** (`ingress::responses_handler_impl_for_test` /
/// `responses_handler_impl_for_test_with_starvation_timing`'s fallback), so the test suite never
/// performs a real 10-60s sleep by accident; the PRODUCTION entrypoint
/// (`ingress::responses_handler_impl`) reads `AppState.starvation_wait_budget`/
/// `starvation_heartbeat` instead, never these consts.
pub const DEFAULT_WAIT_BUDGET: Duration = Duration::from_secs(60);
pub const DEFAULT_HEARTBEAT: Duration = Duration::from_secs(10);

/// Why the post-200 wait+retry could not splice a real upstream stream — a content-free REASON
/// CODE, never a body/message/upstream token (content-safety). Threaded into
/// [`in_band_error_frame`]'s fixed `error.code` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StarvationOutcome {
    /// The bounded wait budget elapsed before `recover_at` was reached (Global Constraint:
    /// BOUNDED BUDGET — never an unbounded wait).
    BudgetExceeded,
    /// The wait reached its target (`recover_at`, or the budget deadline), but the re-run
    /// selection over freshly re-fetched + re-overlaid snapshots still found nothing servable.
    StillNothing,
    /// A recovered account WAS re-selected + resolved, but the post-wait executor call itself
    /// failed (an ordinary upstream failure, just discovered after the 200 was already sent).
    ExecutorError,
}

impl StarvationOutcome {
    /// The fixed reason-code string threaded into [`in_band_error_frame`]'s `error.code` field —
    /// `pub` so the integration test suite (`tests/starvation_layer2.rs`, a separate crate) can
    /// assert against the SAME source of truth instead of duplicating the literal.
    pub fn code(self) -> &'static str {
        match self {
            StarvationOutcome::BudgetExceeded => "starvation_wait_budget_exceeded",
            StarvationOutcome::StillNothing => "starvation_wait_recovered_nothing",
            StarvationOutcome::ExecutorError => "starvation_wait_executor_error",
        }
    }
}

/// The in-band SSE error frame emitted when the post-200 wait/retry gives up — Global Constraint:
/// POST-200 COMMIT. Once HTTP 200 has been sent, a late HTTP status is impossible; every failure
/// path after that point must be an in-band frame instead, NEVER a dropped/`Err` stream item (an
/// `Err` item would abort the chunked/HTTP-2 body ungracefully rather than deliver a parseable
/// frame the client's own `response.failed` handling already understands).
///
/// Reuses the EXACT `response.failed` shape `watchdog::signal_client_stream`'s `SIGNAL_SSE`
/// constant already sends for a synthetic client-facing failure (the silence-recovery signal), so
/// a client's existing `response.failed` handling — already exercised on every silence-recovery —
/// applies here unchanged. `error.message` is a FIXED, content-free sentence, never upstream text;
/// `error.code` is one of [`StarvationOutcome`]'s three fixed labels.
pub fn in_band_error_frame(outcome: StarvationOutcome) -> Bytes {
    Bytes::from(format!(
        "event: response.failed\ndata: {{\"type\":\"response.failed\",\"response\":{{\"error\":\
         {{\"code\":\"{}\",\"message\":\"starvation recovery wait ended without a servable \
         account\"}}}}}}\n\n",
        outcome.code()
    ))
}

/// The Anthropic `/v1/messages` streaming equivalent of [`in_band_error_frame`]: a single
/// `event: error` SSE frame in Anthropic's shape (`{"type":"error","error":{"type":..,"message":..}}`,
/// see `polyflare_anthropic::translate::AnthropicToResponses`'s `build_error_event`), for the
/// Anthropic empty-pool Layer-2 paths (native + aliased). **Content-free by construction**: a FIXED
/// Anthropic error `type` (`overloaded_error` — the closest match for "no capacity available") plus a
/// FIXED sentence carrying ONLY the compile-time [`StarvationOutcome::code`] label — never upstream
/// error text, unlike the translator's `build_error_event`.
pub fn anthropic_in_band_error_frame(outcome: StarvationOutcome) -> Bytes {
    Bytes::from(format!(
        "event: error\ndata: {{\"type\":\"error\",\"error\":{{\"type\":\"overloaded_error\",\
         \"message\":\"starvation recovery wait ended without a servable account ({})\"}}}}\n\n",
        outcome.code()
    ))
}

/// The Anthropic keepalive: a typed `ping` event (what the real Anthropic streaming API emits),
/// used in place of the Codex `: keepalive` SSE comment on the Anthropic Layer-2 wait paths.
pub fn anthropic_ping_frame() -> Bytes {
    Bytes::from_static(b"event: ping\ndata: {\"type\":\"ping\"}\n\n")
}

/// B5 Task 5: the fixed reason label for a Layer 2 wait's SUCCESS terminal — a real account was
/// re-selected, resolved, and a real upstream stream was spliced in. The counterpart to
/// [`StarvationOutcome`]'s three FAILURE reason codes (which only cover the ways a wait can fail);
/// threaded into `crate::observability::StarvationSignal::reason` from the splice site in
/// `crate::ingress::layer2_wait_stream`. A fixed `&'static str`, never built from request/response
/// content (content-safety).
pub const STARVATION_RECOVERED_REASON: &str = "starvation_wait_recovered";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keepalive_frame_is_exactly_the_fixed_content_free_bytes() {
        assert_eq!(KEEPALIVE_FRAME, b": keepalive\n\n");
        // No `data:` field at all — content-safety: structurally incapable of carrying a body.
        assert!(!KEEPALIVE_FRAME.starts_with(b"data:"));
    }

    #[test]
    fn in_band_error_frame_carries_only_the_fixed_reason_code_and_message() {
        for outcome in [
            StarvationOutcome::BudgetExceeded,
            StarvationOutcome::StillNothing,
            StarvationOutcome::ExecutorError,
        ] {
            let frame = in_band_error_frame(outcome);
            let s = String::from_utf8(frame.to_vec()).unwrap();
            assert!(s.starts_with("event: response.failed\n"));
            assert!(s.contains("\"type\":\"response.failed\""));
            assert!(s.contains(outcome.code()));
            assert!(
                s.contains("starvation recovery wait ended without a servable account"),
                "fixed, content-free message: {s}"
            );
            assert!(s.ends_with("\n\n"));
        }
    }

    #[test]
    fn the_three_outcomes_have_distinct_codes() {
        let codes: std::collections::HashSet<&str> = [
            StarvationOutcome::BudgetExceeded.code(),
            StarvationOutcome::StillNothing.code(),
            StarvationOutcome::ExecutorError.code(),
        ]
        .into_iter()
        .collect();
        assert_eq!(codes.len(), 3);
    }

    #[test]
    fn anthropic_error_frame_is_the_anthropic_error_event_shape_and_content_free() {
        let frame = anthropic_in_band_error_frame(StarvationOutcome::BudgetExceeded);
        let s = std::str::from_utf8(&frame).unwrap();
        // Anthropic SSE error event shape: `event: error\ndata: {"type":"error","error":{...}}\n\n`
        assert!(s.starts_with("event: error\n"));
        assert!(s.contains("\"type\":\"error\""));
        assert!(s.contains("\"error\":{"));
        // A valid, fixed Anthropic error type — never upstream text.
        assert!(s.contains("\"type\":\"overloaded_error\""));
        // The message is a FIXED sentence carrying only our own fixed outcome code label.
        assert!(s.contains(StarvationOutcome::BudgetExceeded.code()));
        assert!(s.ends_with("\n\n"));
        // The three fixed outcome codes are the ONLY variable content.
        for oc in [
            StarvationOutcome::BudgetExceeded,
            StarvationOutcome::StillNothing,
            StarvationOutcome::ExecutorError,
        ] {
            let f = anthropic_in_band_error_frame(oc);
            let t = std::str::from_utf8(&f).unwrap();
            assert!(t.contains(oc.code()));
        }
    }

    #[test]
    fn anthropic_ping_frame_is_a_typed_ping_event() {
        let frame = anthropic_ping_frame();
        let s = std::str::from_utf8(&frame).unwrap();
        assert_eq!(s, "event: ping\ndata: {\"type\":\"ping\"}\n\n");
    }
}
