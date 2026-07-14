//! Anthropic Messages → OpenAI-Responses translator (SPEC-M4 §3.4 stateful 1->N seam). This file
//! builds the mapping in two layers: `map_request` (this task) does the doc-verified *mechanical*
//! request-body field mapping (SPEC-M4 §3.6's "mechanical direction") — model-alias remap and
//! reasoning-effort payload-override are explicitly deferred (SPEC-M4 U2, M4b-wiring), so `model`
//! passes through unchanged here. `AnthropicToResponses` (added on top of this module) is the
//! stateful streaming response-event translator (SPEC-M4 §3.5).

use serde_json::{json, Value};

/// Map an Anthropic Messages request body to an OpenAI-Responses request body. Mechanical only —
/// no model-alias remap, no payload-override (SPEC-M4 U2, deferred to M4b-wiring).
#[allow(dead_code)] // consumed by AnthropicToResponses::translate_request in Task 3
fn map_request(body: Value) -> Value {
    let model = body.get("model").cloned().unwrap_or(Value::Null);
    let system = body.get("system").cloned();
    let messages = body
        .get("messages")
        .cloned()
        .unwrap_or_else(|| Value::Array(vec![]));
    let stream = body.get("stream").cloned().unwrap_or(Value::Bool(false));
    let max_tokens = body.get("max_tokens").cloned();
    let tools = body.get("tools").cloned();

    let mut out = json!({
        "model": model,
        "input": messages,
        "stream": stream,
    });
    let map = out.as_object_mut().expect("json! object literal");
    if let Some(sys) = system {
        map.insert("instructions".to_string(), sys);
    }
    if let Some(mt) = max_tokens {
        map.insert("max_output_tokens".to_string(), mt);
    }
    if let Some(t) = tools {
        map.insert("tools".to_string(), t);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_model_messages_stream_and_max_tokens() {
        let body = json!({
            "model": "claude-opus-4-1-20250805",
            "messages": [{"role": "user", "content": [{"type": "text", "text": "hi"}]}],
            "stream": true,
            "max_tokens": 1024
        });
        let out = map_request(body);
        assert_eq!(out["model"], json!("claude-opus-4-1-20250805"));
        assert_eq!(out["stream"], json!(true));
        assert_eq!(out["max_output_tokens"], json!(1024));
        assert_eq!(
            out["input"],
            json!([{"role": "user", "content": [{"type": "text", "text": "hi"}]}])
        );
    }

    #[test]
    fn maps_system_prompt_to_instructions() {
        let body = json!({
            "model": "claude-sonnet-4-5-20250929",
            "system": "You are a helpful assistant.",
            "messages": [],
            "stream": true
        });
        let out = map_request(body);
        assert_eq!(out["instructions"], json!("You are a helpful assistant."));
    }

    #[test]
    fn passes_tools_through_when_present() {
        let tools = json!([{"name": "get_weather", "input_schema": {"type": "object"}}]);
        let body = json!({
            "model": "claude-opus-4-1-20250805",
            "messages": [],
            "stream": true,
            "tools": tools.clone()
        });
        let out = map_request(body);
        assert_eq!(out["tools"], tools);
    }

    #[test]
    fn omits_optional_fields_when_absent() {
        let body = json!({"model": "claude-haiku-4-5-20251001", "messages": [], "stream": false});
        let out = map_request(body);
        assert!(out.get("instructions").is_none());
        assert!(out.get("max_output_tokens").is_none());
        assert!(out.get("tools").is_none());
    }

    #[test]
    fn defaults_stream_false_when_absent() {
        let body = json!({"model": "claude-opus-4-1-20250805", "messages": []});
        let out = map_request(body);
        assert_eq!(out["stream"], json!(false));
    }

    #[test]
    fn does_not_remap_model_alias() {
        // SPEC-M4 U2: the exact opus/sonnet/haiku -> sol/terra/luna pairs are pending user
        // confirmation. `map_request` must never guess at a remap.
        let body = json!({"model": "claude-opus-4-1-20250805", "messages": []});
        let out = map_request(body);
        assert_eq!(out["model"], json!("claude-opus-4-1-20250805"));
    }
}
