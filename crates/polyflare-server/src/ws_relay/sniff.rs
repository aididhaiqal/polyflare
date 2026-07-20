//! Content-free `response.completed` id sniff for the WS-downstream relay pump (Task 6).
//!
//! WS frames are RAW JSON objects on the wire — unlike the HTTP path's SSE `data:` lines
//! (`crate::watchdog::extract_response_id`), there is no line-prefix framing to strip here. This
//! mirrors that function's tolerant "parse just `type` + `/response/id`" shape, but for a bare JSON
//! frame, and deliberately narrower: only `response.completed` triggers a hit. `response.created`
//! (which the HTTP sniffer also accepts, since it just needs *an* id early) is NOT sniffed here —
//! the WS relay's ownership write must land only once the turn is truly done, so a stray
//! `response.completed`-shaped `response.created` frame can never race the real completion.
//!
//! **Content-free:** reads exactly two fields (`type`, `response.id`) and returns only the id
//! string. `text` — the full frame body, which IS conversation content — is never logged or
//! persisted anywhere in this module.

use serde_json::Value;

/// If `text` is a `response.completed` WS frame, return its `response.id`. Any other `type`,
/// malformed JSON, or a missing/non-string id all return `None` — tolerant, never panics.
pub(crate) fn sniff_completed_id(text: &str) -> Option<String> {
    let v: Value = serde_json::from_str(text).ok()?;
    if v.get("type").and_then(|t| t.as_str()) != Some("response.completed") {
        return None;
    }
    v.get("response")?.get("id")?.as_str().map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sniff_completed_id_returns_id_from_response_completed() {
        let frame = r#"{"type":"response.completed","response":{"id":"resp_42"}}"#;
        assert_eq!(sniff_completed_id(frame).as_deref(), Some("resp_42"));
    }

    /// The crux exclusion: `response.created` carries an id too (the HTTP sniffer accepts it), but
    /// the WS relay must NOT extract from it — only the true terminal frame feeds ownership.
    #[test]
    fn sniff_completed_id_never_extracts_from_response_created() {
        let frame = r#"{"type":"response.created","response":{"id":"resp_42"}}"#;
        assert_eq!(
            sniff_completed_id(frame),
            None,
            "response.created must never be sniffed, even though it carries an id"
        );
    }

    #[test]
    fn sniff_completed_id_ignores_unrelated_frame_types() {
        let frame = r#"{"type":"response.output_text.delta","delta":"hi"}"#;
        assert_eq!(sniff_completed_id(frame), None);
    }

    #[test]
    fn sniff_completed_id_returns_none_on_malformed_json() {
        assert_eq!(sniff_completed_id("not json"), None);
        assert_eq!(sniff_completed_id(""), None);
    }

    #[test]
    fn sniff_completed_id_returns_none_when_id_missing() {
        let frame = r#"{"type":"response.completed","response":{}}"#;
        assert_eq!(sniff_completed_id(frame), None);
        let frame_no_response = r#"{"type":"response.completed"}"#;
        assert_eq!(sniff_completed_id(frame_no_response), None);
    }
}
