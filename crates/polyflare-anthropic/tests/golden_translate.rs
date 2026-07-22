//! Golden replay tests for `AnthropicToResponses`'s response-event direction (SPEC-M4 §3.4/§3.5,
//! inverted — see `crates/polyflare-anthropic/src/translate.rs`'s file-level doc comment). Feeds a
//! full **OpenAI-Responses** SSE event sequence (the Codex backend's reply shape) and asserts the
//! emitted **Anthropic-Messages** SSE sequence (the Claude client's expected shape). Fixtures below
//! are SYNTHETIC — built directly from SPEC-M4 §3.5's doc-verified event-mapping table (inverted),
//! not captured from a real Claude/Codex request. Real-capture validation is a later, separate
//! refinement (SPEC-M4 U4) — these fixtures only prove the mapping matches the documented table.

use polyflare_anthropic::AnthropicToResponses;
use polyflare_core::Translator;
use serde_json::{json, Value};

/// Feed a full OpenAI-Responses SSE event sequence through a FRESH translator instance (never
/// reused across turns, per SPEC-M4 §3.4) and flatten every emitted `Vec<Value>` into one ordered
/// sequence, in the order the events were produced.
fn replay(events: Vec<Value>) -> Vec<Value> {
    let mut t = AnthropicToResponses::new();
    let mut out = Vec::new();
    for event in events {
        out.extend(t.translate_response_event(event));
    }
    out
}

#[test]
fn text_only_turn_reassembles_and_maps_usage() {
    let events = vec![
        json!({
            "type": "response.created",
            "response": {"id": "resp_abc", "object": "response", "status": "in_progress", "model": "sol", "output": [], "usage": Value::Null}
        }),
        json!({
            "type": "response.in_progress",
            "response": {"id": "resp_abc", "status": "in_progress", "model": "sol", "usage": Value::Null}
        }),
        json!({
            "type": "response.output_item.added", "output_index": 0,
            "item": {"id": "item_1", "type": "message", "status": "in_progress", "role": "assistant", "content": []}
        }),
        json!({
            "type": "response.content_part.added", "item_id": "item_1", "output_index": 0, "content_index": 0,
            "part": {"type": "output_text", "text": "", "annotations": []}
        }),
        json!({"type": "response.output_text.delta", "item_id": "item_1", "output_index": 0, "content_index": 0, "delta": "Hello", "logprobs": []}),
        json!({"type": "response.output_text.delta", "item_id": "item_1", "output_index": 0, "content_index": 0, "delta": " world", "logprobs": []}),
        json!({"type": "response.output_text.done", "item_id": "item_1", "output_index": 0, "content_index": 0, "text": "Hello world"}),
        json!({
            "type": "response.content_part.done", "item_id": "item_1", "output_index": 0, "content_index": 0,
            "part": {"type": "output_text", "text": "Hello world", "annotations": []}
        }),
        json!({
            "type": "response.output_item.done", "output_index": 0,
            "item": {"id": "item_1", "type": "message", "status": "completed", "role": "assistant", "content": [{"type": "output_text", "text": "Hello world", "annotations": []}]}
        }),
        json!({
            "type": "response.completed",
            "response": {
                "id": "resp_abc", "status": "completed", "model": "sol", "output": [],
                "usage": {
                    "input_tokens": 25, "output_tokens": 9,
                    "input_tokens_details": {"cached_tokens": 0},
                    "output_tokens_details": {"reasoning_tokens": 0},
                    "total_tokens": 34
                }
            }
        }),
    ];

    let out = replay(events);

    let types: Vec<&str> = out.iter().map(|e| e["type"].as_str().unwrap()).collect();
    assert_eq!(
        types,
        vec![
            "message_start",
            "content_block_start",
            "content_block_delta",
            "content_block_delta",
            "content_block_stop",
            // response.content_part.done + response.output_item.done are absorbed -- Anthropic's
            // content_block_stop fires exactly once per block, on the FIRST done-family event.
            "message_delta",
            "message_stop",
        ]
    );

    // message.id is minted once (at message_start).
    let msg_id = out[0]["message"]["id"].as_str().unwrap().to_string();
    assert!(!msg_id.is_empty());
    assert_eq!(out[0]["message"]["model"], json!("sol"));
    assert_eq!(out[0]["message"]["role"], json!("assistant"));
    assert_eq!(out[0]["message"]["content"], json!([]));

    // No sequence_number/item_id/output_index/logprobs anywhere -- Anthropic has none of these.
    for e in &out {
        assert!(e.get("sequence_number").is_none());
        assert!(e.get("item_id").is_none());
        assert!(e.get("logprobs").is_none());
    }

    // Anthropic's content_block_stop carries no accumulated text (the deltas already delivered it).
    assert!(out[4].get("text").is_none());

    assert_eq!(out[1]["index"], json!(0));
    assert_eq!(out[1]["content_block"], json!({"type": "text", "text": ""}));
    assert_eq!(
        out[2]["delta"],
        json!({"type": "text_delta", "text": "Hello"})
    );
    assert_eq!(
        out[3]["delta"],
        json!({"type": "text_delta", "text": " world"})
    );
    assert_eq!(out[4], json!({"type": "content_block_stop", "index": 0}));

    assert_eq!(out[5]["delta"]["stop_reason"], json!("end_turn"));
    assert_eq!(out[5]["delta"]["stop_sequence"], Value::Null);
    let usage = &out[5]["usage"];
    assert_eq!(usage["input_tokens"], json!(25));
    assert_eq!(usage["output_tokens"], json!(9));
    assert!(
        usage.get("total_tokens").is_none(),
        "Anthropic usage has no total_tokens"
    );

    assert_eq!(out[6], json!({"type": "message_stop"}));
}

#[test]
fn tool_use_turn_reassembles_arguments_and_call_id() {
    let events = vec![
        json!({
            "type": "response.created",
            "response": {"id": "resp_def", "status": "in_progress", "model": "sol", "usage": Value::Null}
        }),
        json!({
            "type": "response.output_item.added", "output_index": 0,
            "item": {"id": "item_fc", "type": "function_call", "status": "in_progress", "call_id": "call_abc123", "name": "get_weather", "arguments": ""}
        }),
        json!({"type": "response.function_call_arguments.delta", "item_id": "item_fc", "output_index": 0, "delta": "{\"loc"}),
        json!({"type": "response.function_call_arguments.delta", "item_id": "item_fc", "output_index": 0, "delta": "ation\":\"SF\"}"}),
        json!({"type": "response.function_call_arguments.done", "item_id": "item_fc", "output_index": 0, "arguments": "{\"location\":\"SF\"}"}),
        json!({
            "type": "response.output_item.done", "output_index": 0,
            "item": {"id": "item_fc", "type": "function_call", "status": "completed", "call_id": "call_abc123", "name": "get_weather", "arguments": "{\"location\":\"SF\"}"}
        }),
        json!({
            "type": "response.completed",
            "response": {"id": "resp_def", "status": "completed", "model": "sol", "usage": {"input_tokens": 40, "output_tokens": 12}}
        }),
    ];

    let out = replay(events);

    let types: Vec<&str> = out.iter().map(|e| e["type"].as_str().unwrap()).collect();
    assert_eq!(
        types,
        vec![
            "message_start",
            "content_block_start",
            "content_block_delta",
            "content_block_delta",
            "content_block_stop",
            "message_delta",
            "message_stop",
        ]
    );

    assert_eq!(out[1]["index"], json!(0));
    assert_eq!(
        out[1]["content_block"],
        json!({"type": "tool_use", "id": "call_abc123", "name": "get_weather", "input": {}})
    );
    assert_eq!(
        out[2]["delta"],
        json!({"type": "input_json_delta", "partial_json": "{\"loc"})
    );
    assert_eq!(
        out[3]["delta"],
        json!({"type": "input_json_delta", "partial_json": "ation\":\"SF\"}"})
    );
    assert_eq!(out[4], json!({"type": "content_block_stop", "index": 0}));
    assert!(
        out[4].get("arguments").is_none(),
        "Anthropic's stop carries no accumulated arguments"
    );

    assert_eq!(out[5]["usage"]["input_tokens"], json!(40));
    assert_eq!(out[5]["usage"]["output_tokens"], json!(12));
}

#[test]
fn thinking_then_text_turn_separates_block_indices() {
    let events = vec![
        json!({
            "type": "response.created",
            "response": {"id": "resp_ghi", "status": "in_progress", "model": "sol", "usage": Value::Null}
        }),
        json!({
            "type": "response.output_item.added", "output_index": 0,
            "item": {"id": "item_r", "type": "reasoning", "status": "in_progress", "summary": []}
        }),
        json!({"type": "response.reasoning_summary_text.delta", "item_id": "item_r", "output_index": 0, "summary_index": 0, "delta": "Let me "}),
        json!({"type": "response.reasoning_summary_text.delta", "item_id": "item_r", "output_index": 0, "summary_index": 0, "delta": "think..."}),
        json!({"type": "response.reasoning_summary_text.done", "item_id": "item_r", "output_index": 0, "summary_index": 0, "text": "Let me think..."}),
        json!({
            "type": "response.output_item.done", "output_index": 0,
            "item": {"id": "item_r", "type": "reasoning", "status": "completed", "summary": [{"type": "summary_text", "text": "Let me think..."}]}
        }),
        json!({
            "type": "response.output_item.added", "output_index": 1,
            "item": {"id": "item_t", "type": "message", "status": "in_progress", "role": "assistant", "content": []}
        }),
        json!({
            "type": "response.content_part.added", "item_id": "item_t", "output_index": 1, "content_index": 0,
            "part": {"type": "output_text", "text": "", "annotations": []}
        }),
        json!({"type": "response.output_text.delta", "item_id": "item_t", "output_index": 1, "content_index": 0, "delta": "42", "logprobs": []}),
        json!({"type": "response.output_text.done", "item_id": "item_t", "output_index": 1, "content_index": 0, "text": "42"}),
        json!({
            "type": "response.output_item.done", "output_index": 1,
            "item": {"id": "item_t", "type": "message", "status": "completed", "role": "assistant", "content": [{"type": "output_text", "text": "42", "annotations": []}]}
        }),
        json!({
            "type": "response.completed",
            "response": {
                "id": "resp_ghi", "status": "completed", "model": "sol",
                "usage": {"input_tokens": 30, "output_tokens": 15, "input_tokens_details": {"cached_tokens": 5}}
            }
        }),
    ];

    let out = replay(events);

    let types: Vec<&str> = out.iter().map(|e| e["type"].as_str().unwrap()).collect();
    assert_eq!(
        types,
        vec![
            "message_start",
            "content_block_start", // thinking block opens (index 0)
            "content_block_delta",
            "content_block_delta",
            "content_block_stop", // index 0 -- fires on reasoning_summary_text.done, not repeated on output_item.done
            "content_block_start", // text block opens (index 1)
            "content_block_delta",
            "content_block_stop", // index 1
            "message_delta",
            "message_stop",
        ]
    );

    // Distinct Anthropic indices assigned in the order each block actually opened.
    assert_eq!(out[1]["index"], json!(0));
    assert_eq!(out[1]["content_block"]["type"], json!("thinking"));
    assert_eq!(out[4]["index"], json!(0));
    assert_eq!(out[5]["index"], json!(1));
    assert_eq!(out[5]["content_block"]["type"], json!("text"));
    assert_eq!(out[7]["index"], json!(1));

    assert_eq!(
        out[2]["delta"],
        json!({"type": "thinking_delta", "thinking": "Let me "})
    );
    assert_eq!(
        out[3]["delta"],
        json!({"type": "thinking_delta", "thinking": "think..."})
    );
    assert_eq!(out[6]["delta"], json!({"type": "text_delta", "text": "42"}));

    let usage = &out[8]["usage"];
    assert_eq!(usage["input_tokens"], json!(30));
    assert_eq!(usage["output_tokens"], json!(15));
    assert_eq!(usage["cache_read_input_tokens"], json!(5));
}

#[test]
fn incomplete_status_maps_to_max_tokens_stop_reason() {
    let events = vec![
        json!({
            "type": "response.created",
            "response": {"id": "resp_jkl", "status": "in_progress", "model": "sol", "usage": Value::Null}
        }),
        json!({
            "type": "response.output_item.added", "output_index": 0,
            "item": {"id": "item_1", "type": "message", "status": "in_progress", "role": "assistant", "content": []}
        }),
        json!({
            "type": "response.content_part.added", "item_id": "item_1", "output_index": 0, "content_index": 0,
            "part": {"type": "output_text", "text": "", "annotations": []}
        }),
        json!({"type": "response.output_text.delta", "item_id": "item_1", "delta": "partial"}),
        json!({
            "type": "response.incomplete",
            "response": {
                "id": "resp_jkl", "status": "incomplete", "model": "sol",
                "incomplete_details": {"reason": "max_output_tokens"},
                "usage": {"output_tokens": 5}
            }
        }),
    ];

    let out = replay(events);
    let message_delta = out
        .iter()
        .find(|e| e["type"] == json!("message_delta"))
        .unwrap();
    assert_eq!(message_delta["delta"]["stop_reason"], json!("max_tokens"));
    assert_eq!(message_delta["usage"]["output_tokens"], json!(5));
}

#[test]
fn response_failed_replaces_terminal_events_with_a_single_error_event() {
    let events = vec![
        json!({
            "type": "response.created",
            "response": {"id": "resp_mno", "status": "in_progress", "model": "sol", "usage": Value::Null}
        }),
        json!({
            "type": "response.failed",
            "response": {"id": "resp_mno", "status": "failed", "error": {"code": "overloaded", "message": "Overloaded"}}
        }),
    ];

    let out = replay(events);
    assert_eq!(out.len(), 2, "message_start then a single error event");
    assert_eq!(
        out[1],
        json!({"type": "error", "error": {"type": "overloaded", "message": "Overloaded"}})
    );
}

#[test]
fn each_turn_gets_a_fresh_translator_with_no_cross_turn_state() {
    let base_events = |resp_id: &str| {
        vec![
            json!({"type": "response.created", "response": {"id": resp_id, "status": "in_progress", "model": "sol", "usage": Value::Null}}),
            json!({"type": "response.output_item.added", "output_index": 0, "item": {"id": "item_1", "type": "message", "role": "assistant", "content": []}}),
            json!({"type": "response.content_part.added", "item_id": "item_1", "output_index": 0, "content_index": 0, "part": {"type": "output_text", "text": ""}}),
            json!({"type": "response.output_text.delta", "item_id": "item_1", "delta": "hi"}),
            json!({"type": "response.output_text.done", "item_id": "item_1", "text": "hi"}),
            json!({"type": "response.completed", "response": {"id": resp_id, "status": "completed", "usage": {"output_tokens": 1}}}),
        ]
    };

    let first = replay(base_events("resp_a"));
    let second = replay(base_events("resp_b"));

    // message.id is freshly minted per turn -- never reused across turns.
    assert_ne!(first[0]["message"]["id"], second[0]["message"]["id"]);

    // Both turns independently start their own block-index counter at 0 (a fresh instance per
    // turn, per SPEC-M4 §3.4) -- not a continuation of one shared counter.
    assert_eq!(first[1]["index"], json!(0));
    assert_eq!(second[1]["index"], json!(0));
}

#[test]
fn request_translation_does_not_remap_model_alias() {
    let mut t = AnthropicToResponses::new();
    let body = json!({
        "model": "claude-opus-4-1-20250805",
        "system": "Be concise.",
        "messages": [{"role": "user", "content": [{"type": "text", "text": "2+2?"}]}],
        "stream": true,
        "max_tokens": 512
    });
    let out = t.translate_request(body);
    // Model-alias remap (opus -> sol, SPEC-M4 U2) is deferred to M4b-wiring; the standalone
    // translator must never guess at it.
    assert_eq!(out["model"], json!("claude-opus-4-1-20250805"));
    assert_eq!(out["instructions"], json!("Be concise."));
    // Codex backend-api/responses contract (live-verified): store:false + stream:true always, and
    // the client's max_tokens is dropped (Codex rejects max_output_tokens).
    assert_eq!(out["store"], json!(false));
    assert_eq!(out["stream"], json!(true));
    assert!(out.get("max_output_tokens").is_none());
}
