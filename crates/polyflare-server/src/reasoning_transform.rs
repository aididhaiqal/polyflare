//! Cross-provider reasoning-envelope transform (reactive, phase 1).
//!
//! A `reasoning` input item's `encrypted_content` is sealed by the platform that produced it and
//! is undecryptable everywhere else, so a thread that switches platforms (fugu → codex) hard-fails
//! upstream with `invalid_encrypted_content` on every resend — the history itself is poisoned for
//! that target. The pump reacts to that exact error code by rewriting the buffered in-flight
//! frame once: every `reasoning` item is removed, and any plaintext `summary` it carried is
//! preserved in place as an ordinary assistant message, which is the only fragment of a foreign
//! model's reasoning the target could ever use. Items are only transformed reactively — healthy
//! same-platform traffic is never rewritten, so its bytes (and prompt-cache behavior) are
//! untouched.
//!
//! **Content-free (inviolable):** this module parses and rewrites the frame in memory and returns
//! it to the pump. It never logs, persists, or copies any item text, id, or summary anywhere
//! else. There is no `tracing`/`log`/`println!`/`eprintln!` in this module.

use serde_json::{json, Value};

/// The upstream error code proving an envelope in the request cannot be decrypted by the target
/// platform (observed live 2026-07-25: fugu-minted `rs_*` items replayed to the codex backend).
pub(crate) const INVALID_ENCRYPTED_CONTENT_CODE: &str = "invalid_encrypted_content";

/// Rewrite a buffered `response.create` client frame so it no longer carries reasoning envelopes.
///
/// Returns `Some(frame)` with every `input` item of type `"reasoning"` removed; an item whose
/// `summary` holds at least one non-empty `summary_text` is replaced IN PLACE by an assistant
/// `message` item carrying that text, so the informational content survives the platform switch.
/// Every other item and every other envelope field (including `previous_response_id`) passes
/// through byte-preserved (modulo JSON re-serialization).
///
/// Returns `None` when a resend would be pointless: the frame is not a generating
/// `response.create`, has no `input` array, contains no reasoning items, or is not valid JSON.
/// The caller must NOT retry on `None` — there is nothing left to fix.
pub(crate) fn strip_unverifiable_reasoning(frame: &str) -> Option<String> {
    let mut value: Value = serde_json::from_str(frame).ok()?;
    {
        let object = value.as_object_mut()?;
        if object.get("type").and_then(Value::as_str) != Some("response.create") {
            return None;
        }
        if object.get("generate").and_then(Value::as_bool) == Some(false) {
            return None;
        }
    }
    if !strip_reasoning_from_input(&mut value) {
        return None;
    }
    Some(value.to_string())
}

/// The HTTP-ingress twin of [`strip_unverifiable_reasoning`], for a `/responses` REQUEST BODY.
///
/// Needed because the same poisoned history reaches upstream over BOTH transports: the WS relay
/// carries a `response.create` frame, while the HTTP path sends a bare Responses body with no
/// `type` field — so the frame-shape guard above would reject it. Observed live 2026-07-25: after a
/// transport drop the client fell back to HTTP and the turn 400'd with `invalid_encrypted_content`
/// on a path the relay-only transform never saw.
///
/// Same contract: `Some(bytes)` when at least one `reasoning` item was removed (its plaintext
/// summary preserved in place as an assistant message), `None` when there is nothing to fix.
pub(crate) fn strip_unverifiable_reasoning_body(body: &[u8]) -> Option<Vec<u8>> {
    let mut value: Value = serde_json::from_slice(body).ok()?;
    if !value.is_object() {
        return None;
    }
    if !strip_reasoning_from_input(&mut value) {
        return None;
    }
    serde_json::to_vec(&value).ok()
}

/// Rewrite `value["input"]` in place, dropping every `reasoning` item and re-inserting any plaintext
/// summary it carried as an ordinary assistant message. Returns whether anything was removed.
///
/// The single source of truth for BOTH transports' transforms — the WS frame and the HTTP body
/// differ only in their envelope, never in how a poisoned `input` array must be repaired.
fn strip_reasoning_from_input(value: &mut Value) -> bool {
    let Some(object) = value.as_object_mut() else {
        return false;
    };
    let Some(input) = object.get_mut("input").and_then(Value::as_array_mut) else {
        return false;
    };

    let mut transformed_any = false;
    let mut rewritten: Vec<Value> = Vec::with_capacity(input.len());
    for item in input.drain(..) {
        if item.get("type").and_then(Value::as_str) != Some("reasoning") {
            rewritten.push(item);
            continue;
        }
        transformed_any = true;
        if let Some(text) = reasoning_summary_text(&item) {
            rewritten.push(json!({
                "type": "message",
                "role": "assistant",
                "content": [{
                    "type": "output_text",
                    "text": format!("[prior model reasoning summary]\n{text}"),
                }],
            }));
        }
        // Summary-less reasoning items are dropped outright: the envelope was their only content
        // and it is undecryptable here by definition.
    }
    *input = rewritten;
    transformed_any
}

/// The concatenated non-empty `summary_text` blocks of one reasoning item, or `None` when the
/// item carries no readable summary at all.
fn reasoning_summary_text(item: &Value) -> Option<String> {
    let blocks = item.get("summary")?.as_array()?;
    let mut parts: Vec<&str> = Vec::new();
    for block in blocks {
        if block.get("type").and_then(Value::as_str) != Some("summary_text") {
            continue;
        }
        if let Some(text) = block.get("text").and_then(Value::as_str) {
            if !text.trim().is_empty() {
                parts.push(text);
            }
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reasoning_item(id: &str, summary: Option<&str>, encrypted: &str) -> Value {
        let summary_blocks = match summary {
            Some(text) => json!([{ "type": "summary_text", "text": text }]),
            None => json!([]),
        };
        json!({
            "type": "reasoning",
            "id": id,
            "summary": summary_blocks,
            "encrypted_content": encrypted,
        })
    }

    fn frame_with_input(input: Vec<Value>) -> String {
        json!({
            "type": "response.create",
            "model": "gpt-5.6-sol",
            "previous_response_id": "resp_anchor_1",
            "store": false,
            "input": input,
        })
        .to_string()
    }

    #[test]
    fn summarized_reasoning_becomes_assistant_text_and_envelope_is_gone() {
        let frame = frame_with_input(vec![
            json!({"role": "user", "content": "hello"}),
            reasoning_item(
                "rs_foreign_1",
                Some("weighed two approaches"),
                "gAAAA-sealed",
            ),
            json!({"type": "function_call", "call_id": "call_1", "name": "shell", "arguments": "{}"}),
        ]);
        let out = strip_unverifiable_reasoning(&frame).expect("must transform");
        assert!(!out.contains("encrypted_content"), "envelope must be gone");
        assert!(
            !out.contains("rs_foreign_1"),
            "reasoning item id must be gone"
        );
        assert!(
            out.contains("weighed two approaches"),
            "summary text must survive as message content"
        );
        let value: Value = serde_json::from_str(&out).unwrap();
        let input = value["input"].as_array().unwrap();
        assert_eq!(
            input.len(),
            3,
            "replacement stays in place, others preserved"
        );
        assert_eq!(input[1]["type"], "message");
        assert_eq!(input[1]["role"], "assistant");
        assert_eq!(input[2]["type"], "function_call", "tool history untouched");
        assert_eq!(
            value["previous_response_id"], "resp_anchor_1",
            "anchor passes through untouched"
        );
    }

    #[test]
    fn summary_less_reasoning_is_dropped_outright() {
        let frame = frame_with_input(vec![
            json!({"role": "user", "content": "hi"}),
            reasoning_item("rs_foreign_2", None, "gAAAA-sealed"),
        ]);
        let out = strip_unverifiable_reasoning(&frame).expect("must transform");
        let value: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(value["input"].as_array().unwrap().len(), 1);
        assert!(!out.contains("encrypted_content"));
    }

    /// The HTTP body has no `type: response.create` envelope — the frame-shaped guard must not
    /// reject it (the 2026-07-25 gap: the client fell back to HTTP and got no repair at all).
    #[test]
    fn http_bodies_are_transformed_without_a_frame_envelope() {
        let body = json!({
            "model": "gpt-5.6-sol",
            "store": false,
            "input": [
                {"role": "user", "content": "hi"},
                reasoning_item("rs_http", Some("prior chain"), "gAAAA-sealed"),
            ],
        })
        .to_string();

        assert!(
            strip_unverifiable_reasoning(&body).is_none(),
            "the WS entrypoint must still require a response.create frame"
        );

        let out = strip_unverifiable_reasoning_body(body.as_bytes()).expect("must transform");
        let value: Value = serde_json::from_slice(&out).unwrap();
        let input = value["input"].as_array().unwrap();
        assert_eq!(input.len(), 2, "user message + summary replacement");
        assert_eq!(input[1]["role"], "assistant");
        assert!(!String::from_utf8_lossy(&out).contains("encrypted_content"));
        assert!(String::from_utf8_lossy(&out).contains("prior chain"));
        assert_eq!(value["model"], "gpt-5.6-sol", "other fields untouched");
    }

    #[test]
    fn http_bodies_without_reasoning_are_none() {
        let body = json!({"model": "m", "input": [{"role": "user", "content": "hi"}]}).to_string();
        assert!(strip_unverifiable_reasoning_body(body.as_bytes()).is_none());
        assert!(strip_unverifiable_reasoning_body(b"not json").is_none());
        assert!(strip_unverifiable_reasoning_body(b"[1,2,3]").is_none());
    }

    #[test]
    fn frames_without_reasoning_items_are_none() {
        let frame = frame_with_input(vec![json!({"role": "user", "content": "hello"})]);
        assert!(strip_unverifiable_reasoning(&frame).is_none());
    }

    #[test]
    fn non_generating_and_non_create_frames_are_none() {
        let mut v: Value =
            serde_json::from_str(&frame_with_input(vec![reasoning_item("rs_1", None, "x")]))
                .unwrap();
        v["generate"] = json!(false);
        assert!(strip_unverifiable_reasoning(&v.to_string()).is_none());

        v["generate"] = json!(true);
        v["type"] = json!("response.cancel");
        assert!(strip_unverifiable_reasoning(&v.to_string()).is_none());

        assert!(strip_unverifiable_reasoning("not json").is_none());
    }
}
