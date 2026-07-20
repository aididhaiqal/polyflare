//! Content-free upstream-signal classifier for the WS-downstream relay pump (Phase 2/3 Task 1).
//!
//! Later tasks (not this one) use [`classify_upstream_signal`]'s result to decide whether a
//! backend WS frame should be forwarded verbatim, intercepted for a same-account re-dial
//! (`UpstreamSignal::ConnectionLimit`), or benched-and-re-selected onto a different account
//! (`UpstreamSignal::Error`). This module only classifies; it recovers nothing.
//!
//! **Content-free (inviolable):** [`classify_upstream_signal`] reads exactly four fields —
//! `type`, `error.code`, `status`, and `headers["retry-after"]` — from the frame's parsed JSON.
//! It never reads, copies, or logs `error.message` or any other field, and never logs the frame
//! text itself. There is no `tracing`/`log`/`println!`/`eprintln!` anywhere in this module.
//!
//! The two error-code constants this module matches against
//! (`polyflare_codex::ws::WS_CONNECTION_LIMIT_CODE` / `WS_ANCHOR_MISS_CODE`) are re-exported from
//! `polyflare-codex`'s `turn.rs`, which already defines them for the executor's own recovery path
//! — reused here, not redefined, so the two crates' notion of these codes can never drift apart.

use polyflare_codex::ws::{WS_ANCHOR_MISS_CODE, WS_CONNECTION_LIMIT_CODE};
use polyflare_core::FailureSignal;

/// What a raw backend WS text frame means to the relay pump, classified without ever reading
/// conversation content.
// A seam held for Phase 2/3's later tasks: the pump's re-dial / re-select loop matches on this.
// Not yet referenced from non-test code.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum UpstreamSignal {
    /// An ordinary response frame (or anything not a recognized error envelope) — forward
    /// verbatim; the pump's existing completed-id sniff still applies.
    Normal,
    /// `websocket_connection_limit_reached` — the relay must INTERCEPT this frame (never forward
    /// it downstream) and re-dial the same account.
    ConnectionLimit,
    /// `previous_response_not_found` — forward verbatim; the client (codex CLI) resolves this by
    /// stripping the anchor and resending, exactly as it does over HTTP-SSE.
    AnchorMissing,
    /// Any other error envelope — bench the account and re-select (move-or-retry).
    Error(FailureSignal),
}

/// Classify a raw backend WS text frame. Parses `text` as JSON; any parse failure or a `type`
/// other than `"error"` is [`UpstreamSignal::Normal`] (ordinary response frames — deltas,
/// `response.completed`, etc. — all take this path). Otherwise reads `error.code`, `status`
/// (default `0` if missing/non-numeric), and `headers["retry-after"]` (tolerant of a JSON string
/// or number; missing/unparseable is `None`) and maps the code to the matching variant.
///
/// Never reads `error.message` — see the module doc's content-free contract.
// A seam held for Phase 2/3's later tasks (the pump calls this per received frame). Covered by
// this module's unit tests now.
#[allow(dead_code)]
pub(crate) fn classify_upstream_signal(text: &str) -> UpstreamSignal {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(text) else {
        return UpstreamSignal::Normal;
    };
    if v.get("type").and_then(|t| t.as_str()) != Some("error") {
        return UpstreamSignal::Normal;
    }

    let code = v
        .get("error")
        .and_then(|e| e.get("code"))
        .and_then(|c| c.as_str());
    let status = v.get("status").and_then(|s| s.as_u64()).unwrap_or(0) as u16;
    let retry_after = v
        .get("headers")
        .and_then(|h| h.get("retry-after"))
        .and_then(|r| {
            r.as_i64()
                .or_else(|| r.as_str().and_then(|s| s.parse().ok()))
        });

    match code {
        Some(WS_CONNECTION_LIMIT_CODE) => UpstreamSignal::ConnectionLimit,
        Some(WS_ANCHOR_MISS_CODE) => UpstreamSignal::AnchorMissing,
        _ => UpstreamSignal::Error(FailureSignal {
            status,
            retry_after,
            error_code: code.map(str::to_string),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normal_response_frame_is_normal() {
        assert!(matches!(
            classify_upstream_signal(r#"{"type":"response.output_text.delta","delta":"x"}"#),
            UpstreamSignal::Normal
        ));
        assert!(matches!(
            classify_upstream_signal(r#"{"type":"response.completed","response":{"id":"resp_1"}}"#),
            UpstreamSignal::Normal
        ));
    }

    #[test]
    fn connection_limit_is_intercepted() {
        let f = r#"{"type":"error","status":409,"error":{"code":"websocket_connection_limit_reached","message":"the websocket connection limit was reached"},"headers":{}}"#;
        assert!(matches!(
            classify_upstream_signal(f),
            UpstreamSignal::ConnectionLimit
        ));
    }

    #[test]
    fn previous_response_not_found_is_anchor_missing() {
        let f = r#"{"type":"error","status":400,"error":{"code":"previous_response_not_found","message":"Previous response with id 'resp_x' not found."},"headers":{}}"#;
        assert!(matches!(
            classify_upstream_signal(f),
            UpstreamSignal::AnchorMissing
        ));
    }

    #[test]
    fn rate_limit_carries_status_and_retry_after() {
        let f = r#"{"type":"error","status":429,"error":{"code":"rate_limit_exceeded","message":"rate limit exceeded"},"headers":{"retry-after":"60"}}"#;
        match classify_upstream_signal(f) {
            UpstreamSignal::Error(sig) => {
                assert_eq!(sig.status, 429);
                assert_eq!(sig.retry_after, Some(60));
                assert_eq!(sig.error_code.as_deref(), Some("rate_limit_exceeded"));
            }
            _ => panic!("expected Error"),
        }
    }

    #[test]
    fn retry_after_parses_a_bare_json_number() {
        // Real backends may send retry-after as a JSON number, not a string — the tolerant parse
        // must handle both (the string form is covered above).
        let f = r#"{"type":"error","status":429,"error":{"code":"rate_limit_exceeded"},"headers":{"retry-after":45}}"#;
        match classify_upstream_signal(f) {
            UpstreamSignal::Error(sig) => assert_eq!(sig.retry_after, Some(45)),
            _ => panic!("expected Error"),
        }
    }

    #[test]
    fn malformed_or_non_error_is_normal() {
        assert!(matches!(
            classify_upstream_signal("not json"),
            UpstreamSignal::Normal
        ));
        assert!(matches!(
            classify_upstream_signal(r#"{"type":"error"}"#),
            UpstreamSignal::Error(_)
        )); // missing code still an error envelope
    }
}
