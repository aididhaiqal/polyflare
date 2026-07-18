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
use polyflare_core::ExecError;

/// codex-lb's exact keepalive frame (`retry.py`/`support.py`'s
/// `_iter_account_capacity_recovery_wait`): a comment-only SSE line carrying no `data:` field at
/// all — inert to any SSE/JSON parser on the client, and structurally incapable of carrying
/// content. This is the ENTIRE keepalive payload; nothing is ever appended to it.
pub const KEEPALIVE_FRAME: &[u8] = b": keepalive\n\n";

/// Task 4 hardcodes these (the plan's "use a const default 60 for now" for the budget; codex-lb's
/// `retry.py` HEARTBEAT=10s for the heartbeat). Task 5 wires
/// `POLYFLARE_STARVATION_WAIT_BUDGET_SECS` (clamped to codex-lb's `[MIN=1s, MAX=300s]`) and
/// `POLYFLARE_STARVATION_HEARTBEAT_SECS` over these, resolved ONCE into `AppState` at startup —
/// NOT a per-request env read. Until then, every production call site uses these two consts
/// directly; only the test seam (`ingress::responses_handler_impl_for_test_with_starvation_timing`)
/// overrides them, to keep the test suite from ever performing a real 10-60s sleep.
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

/// A single keepalive item, pre-wrapped as the `ResponseStream` item type — mirrors
/// `watchdog::signal_client_stream`'s established idiom: a synthetic frame is always yielded as
/// `Ok`, never `Err` (see [`in_band_error_frame`]'s doc for why).
pub fn keepalive_item() -> Result<Bytes, ExecError> {
    Ok(Bytes::from_static(KEEPALIVE_FRAME))
}

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
    fn keepalive_item_wraps_the_frame_as_ok() {
        let item = keepalive_item();
        assert!(item.is_ok());
        assert_eq!(item.unwrap(), Bytes::from_static(KEEPALIVE_FRAME));
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
}
