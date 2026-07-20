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

#[cfg(test)]
mod tests {
    use super::*;

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
