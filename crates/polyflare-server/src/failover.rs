//! B4 Task 1 — the retryable-vs-terminal failover verdict classifier.
//!
//! A PURE function: given a failed request's [`WatchdogError`], whether any attempts remain, and
//! whether a byte has already been relayed downstream ("committed"), decide whether the ingress
//! failover loop (Task 4) should retry on the next eligible account or surface the error to the
//! client as-is. No loop, no exclusion, no ingress wiring here — see the Task 4 plan for that.
//!
//! Ports codex-lb's `failover_decision` (`core/balancer/logic.py:1156-1168`): `downstream_visible`
//! (→ our `committed`) wins first, then `candidates_remaining <= 0` (→ our `!attempts_left`), then
//! a failure-class table decides `failover_next` vs `surface`.
//!
//! # No second classification
//! The failure-class table below MUST mirror `record_failure`'s buckets (`ingress.rs:110-132`) —
//! the function that writes the account-health signal for the SAME failure. Drift between "how we
//! bench the account" and "whether we retry the request" would be a bug: an account-health bucket
//! and a retry-eligibility bucket that disagree on what a 429 or a 5xx means. Concretely, both
//! functions:
//! - check `error_code` via [`classify_failure`] BEFORE looking at `status` (permanent-auth codes
//!   take priority over the raw status);
//! - treat `status == 429` as the rate-limit bucket;
//! - treat 5xx / 401 / 403 / 408 as the transient/account-health bucket;
//! - treat a `None` signal (transport failure / mid-stream drop with no parsed status) as transient;
//! - leave other 4xx (400/404/422/…) OUT of the account-health signal (`record_failure`'s
//!   `Some(_) => {}` arm) — this classifier maps that same "other 4xx" bucket to `Surface`
//!   (request-terminal: retrying elsewhere won't help a malformed/unprocessable request), which is
//!   the retry-side analog of "don't bench the account for it".
//!
//! The one place the two functions necessarily diverge: `record_failure` writes a DURABLE terminal
//! *account* status for a permanent-auth code and stops (the account is done). This classifier
//! returns [`FailoverVerdict::FailoverNext`] for the exact same code — the account is terminal, but
//! the REQUEST can still succeed on a different account. Account-terminal ≠ request-terminal.
//!
//! # `WatchdogError::CapabilityRejection` is out of scope
//! TA6(b) owns its own reroute (`reroute_cyber_rejection`, `ingress.rs:459-526`) for a capability
//! rejection — a fixed reselect onto a `security_work_authorized` account, not the general N-account
//! failover loop. Structurally, `execute_with_watchdog`'s only `CapabilityRejection` caller
//! (`ingress.rs`'s route match arm) intercepts and dispatches to `reroute_cyber_rejection` BEFORE
//! `record_failure`/`failover_verdict` would ever see it — this function is never invoked with a
//! `CapabilityRejection` on that path. Still, the match below must be exhaustive, so it maps
//! `CapabilityRejection` to `Surface` (the conservative, non-looping default) rather than panicking,
//! and a test below documents the expectation so the two paths can't silently start fighting over
//! the same error if a future call site changes.
//!
//! # `WatchdogError::Continuity`
//! Not enumerated in the plan's rule list (which speaks only to `Upstream`'s `FailureSignal`
//! buckets). `RecoveryPlan::None` (the sole producer, `watchdog.rs:301`) means continuity determined
//! there is NO resend strategy for this request at all — a session/directive-level dead end, not an
//! account-health problem. `record_failure` already treats it as a non-health signal (its
//! `let WatchdogError::Upstream(signal) = err else { return; }` early-return skips it entirely, same
//! as `CapabilityRejection`). Retrying on a different account would not change the fact that there
//! is no resend strategy, so this classifier maps it to `Surface` — the conservative choice that
//! keeps today's behavior (a `Continuity` error currently surfaces as a plain 502, never retried).

use polyflare_codex::oauth::classify_failure;
use polyflare_core::FailureSignal;

use crate::watchdog::WatchdogError;

/// The failover loop's (Task 4) verdict for a failed request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailoverVerdict {
    /// Surface the error to the client as-is. The loop ends (or never starts).
    Surface,
    /// Retry on the next eligible account (bounded, excluding tried accounts — Tasks 2/4).
    FailoverNext,
}

/// Decide whether a failed request should fail over to the next account or surface to the client.
///
/// `committed`: a byte was already relayed downstream for this attempt — the commit barrier. NEVER
/// replay past this point (a second response for the same client turn would be irreconcilable).
/// `attempts_left`: whether the bounded loop (Task 4) has another attempt available.
///
/// Order matters and mirrors codex-lb `failover_decision`: `committed` wins over everything (even a
/// 429, which would otherwise be retryable), then `attempts_left`, then the failure-class table.
pub fn failover_verdict(
    err: &WatchdogError,
    attempts_left: bool,
    committed: bool,
) -> FailoverVerdict {
    if committed {
        return FailoverVerdict::Surface;
    }
    if !attempts_left {
        return FailoverVerdict::Surface;
    }
    match err {
        WatchdogError::Upstream(signal) => classify_upstream(signal.as_ref()),
        // TA6(b)'s own reroute owns this — see the module doc. Conservative default: no fan-out.
        WatchdogError::CapabilityRejection { .. } => FailoverVerdict::Surface,
        // No resend strategy exists for this request at all (session/directive-level dead end, not
        // an account problem) — see the module doc.
        WatchdogError::Continuity => FailoverVerdict::Surface,
    }
}

/// The `WatchdogError::Upstream` failure-class table — mirrors `record_failure`'s buckets exactly
/// (see the module doc for the bucket-by-bucket correspondence).
fn classify_upstream(signal: Option<&FailureSignal>) -> FailoverVerdict {
    let Some(sig) = signal else {
        // Transport failure / mid-stream drop with no parsed status: `record_failure`'s `None`
        // arm treats this as a transient account-health error; here it's request-retryable.
        return FailoverVerdict::FailoverNext;
    };

    // Permanent-auth codes take priority over the raw status, exactly as `record_failure` checks
    // `error_code` first. Account-terminal (the account is parked with a durable status) but
    // REQUEST-retryable: another account can still serve this request.
    if let Some(code) = &sig.error_code {
        if classify_failure(code).status().is_some() {
            return FailoverVerdict::FailoverNext;
        }
    }

    match sig.status {
        // rate_limit.
        429 => FailoverVerdict::FailoverNext,
        // transient: 5xx / bad-credential / request-timeout.
        s if (500..=599).contains(&s) => FailoverVerdict::FailoverNext,
        401 | 403 | 408 => FailoverVerdict::FailoverNext,
        // request-terminal 4xx (400/404/422/…): retrying elsewhere won't help a malformed or
        // unprocessable request. Mirrors `record_failure`'s `Some(_) => {}` (not an account-health
        // signal) — the retry-side analog is "don't retry it either".
        _ => FailoverVerdict::Surface,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signal(status: u16, error_code: Option<&str>) -> FailureSignal {
        FailureSignal {
            status,
            retry_after: None,
            error_code: error_code.map(str::to_string),
        }
    }

    #[test]
    fn rate_limit_fails_over() {
        let err = WatchdogError::Upstream(Some(signal(429, None)));
        assert_eq!(
            failover_verdict(&err, true, false),
            FailoverVerdict::FailoverNext
        );
    }

    #[test]
    fn server_error_fails_over() {
        let err = WatchdogError::Upstream(Some(signal(500, None)));
        assert_eq!(
            failover_verdict(&err, true, false),
            FailoverVerdict::FailoverNext
        );
    }

    #[test]
    fn permanent_auth_code_fails_over_account_terminal_but_request_retryable() {
        // "invalid_grant" classifies as FailureClass::ReauthRequired (`classify_failure`) — the
        // account is parked durably, but a DIFFERENT account can still serve this request.
        let err = WatchdogError::Upstream(Some(signal(401, Some("invalid_grant"))));
        assert_eq!(
            failover_verdict(&err, true, false),
            FailoverVerdict::FailoverNext
        );
    }

    #[test]
    fn bad_request_surfaces() {
        let err = WatchdogError::Upstream(Some(signal(400, None)));
        assert_eq!(failover_verdict(&err, true, false), FailoverVerdict::Surface);
    }

    #[test]
    fn transport_error_with_no_status_fails_over() {
        let err = WatchdogError::Upstream(None);
        assert_eq!(
            failover_verdict(&err, true, false),
            FailoverVerdict::FailoverNext
        );
    }

    #[test]
    fn committed_surfaces_even_for_a_retryable_429() {
        let err = WatchdogError::Upstream(Some(signal(429, None)));
        assert_eq!(failover_verdict(&err, true, true), FailoverVerdict::Surface);
    }

    #[test]
    fn no_attempts_left_surfaces_even_for_a_retryable_429() {
        let err = WatchdogError::Upstream(Some(signal(429, None)));
        assert_eq!(
            failover_verdict(&err, false, false),
            FailoverVerdict::Surface
        );
    }

    #[test]
    fn other_terminal_4xx_surface() {
        for status in [404, 422] {
            let err = WatchdogError::Upstream(Some(signal(status, None)));
            assert_eq!(
                failover_verdict(&err, true, false),
                FailoverVerdict::Surface,
                "status {status} should surface"
            );
        }
    }

    #[test]
    fn account_terminal_deactivated_code_also_fails_over() {
        // "account_deactivated" classifies as FailureClass::Deactivated — still account-terminal,
        // request-retryable, same as the ReauthRequired case above.
        let err = WatchdogError::Upstream(Some(signal(403, Some("account_deactivated"))));
        assert_eq!(
            failover_verdict(&err, true, false),
            FailoverVerdict::FailoverNext
        );
    }

    #[test]
    fn transient_401_403_408_fail_over() {
        for status in [401, 403, 408] {
            let err = WatchdogError::Upstream(Some(signal(status, None)));
            assert_eq!(
                failover_verdict(&err, true, false),
                FailoverVerdict::FailoverNext,
                "status {status} should fail over"
            );
        }
    }

    #[test]
    fn capability_rejection_is_kept_out_of_this_classifier() {
        // TA6(b) owns its own reroute; this classifier's conservative default is Surface (no
        // fan-out) — see the module doc for why this path is not actually reachable in practice.
        let err = WatchdogError::CapabilityRejection {
            capability: "security_work_authorized",
        };
        assert_eq!(failover_verdict(&err, true, false), FailoverVerdict::Surface);
    }

    #[test]
    fn continuity_error_surfaces() {
        let err = WatchdogError::Continuity;
        assert_eq!(failover_verdict(&err, true, false), FailoverVerdict::Surface);
    }

    #[test]
    fn committed_wins_over_no_attempts_left_too() {
        // Both terminal conditions present: still Surface (order doesn't matter for the outcome,
        // but committed is checked first per the doc).
        let err = WatchdogError::Upstream(Some(signal(429, None)));
        assert_eq!(
            failover_verdict(&err, false, true),
            FailoverVerdict::Surface
        );
    }
}
