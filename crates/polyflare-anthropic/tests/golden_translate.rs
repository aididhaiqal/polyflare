//! Golden replay tests for `AnthropicToResponses` (SPEC-M4 §3.4/§3.5). Fixtures below are
//! SYNTHETIC — built directly from SPEC-M4 §3.5's doc-verified event-mapping table, not captured
//! from a real Claude/Codex request. Real-capture validation is a later, separate refinement
//! (SPEC-M4 U4) — these fixtures only prove the mapping matches the documented table.

use polyflare_anthropic::AnthropicToResponses;
use polyflare_core::Translator;
use serde_json::{json, Value};

/// Feed a full Anthropic SSE event sequence through a FRESH translator instance (never reused
/// across turns, per SPEC-M4 §3.4) and flatten every emitted `Vec<Value>` into one ordered
/// sequence, in the order the events were produced.
fn replay(events: Vec<Value>) -> Vec<Value> {
    let mut t = AnthropicToResponses::new();
    let mut out = Vec::new();
    for event in events {
        out.extend(t.translate_response_event(event));
    }
    out
}

/// Every emitted event must carry a `sequence_number`, and the full sequence must be strictly
/// increasing -- this is the property golden replay must hold regardless of fixture shape.
fn assert_sequence_numbers_monotonic(events: &[Value]) {
    let mut prev: Option<u64> = None;
    for (i, e) in events.iter().enumerate() {
        let seq = e["sequence_number"]
            .as_u64()
            .unwrap_or_else(|| panic!("event {i} ({:?}) missing sequence_number", e["type"]));
        if let Some(p) = prev {
            assert!(
                seq > p,
                "sequence_number must strictly increase: event {i} ({:?}) has {seq} <= previous {p}",
                e["type"]
            );
        }
        prev = Some(seq);
    }
}

#[test]
fn text_only_turn_reassembles_and_maps_usage() {
    let events = vec![
        json!({
            "type": "message_start",
            "message": {
                "id": "msg_01XYZ",
                "model": "claude-opus-4-1-20250805",
                "role": "assistant",
                "content": [],
                "usage": {"input_tokens": 25, "output_tokens": 1}
            }
        }),
        json!({"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}}),
        json!({"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": "Hello"}}),
        json!({"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": " world"}}),
        json!({"type": "content_block_stop", "index": 0}),
        json!({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn", "stop_sequence": null},
            "usage": {"output_tokens": 8}
        }),
        json!({"type": "message_stop"}),
    ];

    let out = replay(events);
    assert_sequence_numbers_monotonic(&out);

    let types: Vec<&str> = out.iter().map(|e| e["type"].as_str().unwrap()).collect();
    assert_eq!(
        types,
        vec![
            "response.created",
            "response.in_progress",
            "response.output_item.added",
            "response.content_part.added",
            "response.output_text.delta",
            "response.output_text.delta",
            "response.output_text.done",
            "response.content_part.done",
            "response.output_item.done",
            "response.completed",
        ]
    );

    // response.id is minted once and identical across every event that carries a `response`.
    let resp_id = out[0]["response"]["id"].as_str().unwrap().to_string();
    assert_eq!(out[1]["response"]["id"], json!(resp_id));
    assert_eq!(out.last().unwrap()["response"]["id"], json!(resp_id));

    // item.id is minted once (at output_item.added) and identical everywhere it recurs.
    let item_id = out[2]["item"]["id"].as_str().unwrap().to_string();
    assert_eq!(out[3]["item_id"], json!(item_id));
    assert_eq!(out[4]["item_id"], json!(item_id));
    assert_eq!(out[6]["item_id"], json!(item_id));
    assert_eq!(out[8]["item"]["id"], json!(item_id));

    // reassembled text: no buffering across the network boundary -- each delta emitted
    // immediately -- but the FINAL accumulated string is correct at .done/.completed.
    assert_eq!(out[6]["text"], json!("Hello world"));
    assert_eq!(
        out.last().unwrap()["response"]["output"][0]["content"][0]["text"],
        json!("Hello world")
    );

    let usage = &out.last().unwrap()["response"]["usage"];
    assert_eq!(usage["input_tokens"], json!(25));
    assert_eq!(usage["output_tokens"], json!(8));
    assert_eq!(usage["total_tokens"], json!(33));
    assert_eq!(
        out.last().unwrap()["response"]["status"],
        json!("completed")
    );
}

#[test]
fn tool_use_turn_reassembles_arguments_and_call_id() {
    let events = vec![
        json!({
            "type": "message_start",
            "message": {"id": "msg_02ABC", "model": "claude-opus-4-1-20250805", "role": "assistant", "content": [], "usage": {"input_tokens": 40, "output_tokens": 1}}
        }),
        json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "tool_use", "id": "toolu_01AAA", "name": "get_weather", "input": {}}
        }),
        json!({"type": "content_block_delta", "index": 0, "delta": {"type": "input_json_delta", "partial_json": "{\"loc"}}),
        json!({"type": "content_block_delta", "index": 0, "delta": {"type": "input_json_delta", "partial_json": "ation\":\"SF\"}"}}),
        json!({"type": "content_block_stop", "index": 0}),
        json!({"type": "message_delta", "delta": {"stop_reason": "tool_use"}, "usage": {"output_tokens": 12}}),
        json!({"type": "message_stop"}),
    ];

    let out = replay(events);
    assert_sequence_numbers_monotonic(&out);

    let types: Vec<&str> = out.iter().map(|e| e["type"].as_str().unwrap()).collect();
    assert_eq!(
        types,
        vec![
            "response.created",
            "response.in_progress",
            "response.output_item.added",
            "response.function_call_arguments.delta",
            "response.function_call_arguments.delta",
            "response.function_call_arguments.done",
            "response.output_item.done",
            "response.completed",
        ]
    );

    // No content_part.added/.done anywhere -- tool_use blocks never open one.
    assert!(!types.contains(&"response.content_part.added"));
    assert!(!types.contains(&"response.content_part.done"));

    assert_eq!(out[2]["item"]["call_id"], json!("toolu_01AAA"));
    assert_eq!(out[2]["item"]["name"], json!("get_weather"));
    assert_eq!(out[5]["arguments"], json!("{\"location\":\"SF\"}"));
    assert_eq!(out[6]["item"]["arguments"], json!("{\"location\":\"SF\"}"));

    let final_output = &out.last().unwrap()["response"]["output"];
    assert_eq!(final_output[0]["type"], json!("function_call"));
    assert_eq!(final_output[0]["call_id"], json!("toolu_01AAA"));
    assert_eq!(final_output[0]["arguments"], json!("{\"location\":\"SF\"}"));
}

#[test]
fn thinking_then_text_turn_separates_output_indices_and_drops_signature() {
    let events = vec![
        json!({
            "type": "message_start",
            "message": {"id": "msg_03DEF", "model": "claude-opus-4-1-20250805", "role": "assistant", "content": [], "usage": {"input_tokens": 30, "output_tokens": 1}}
        }),
        json!({"type": "content_block_start", "index": 0, "content_block": {"type": "thinking", "thinking": "", "signature": ""}}),
        json!({"type": "content_block_delta", "index": 0, "delta": {"type": "thinking_delta", "thinking": "Let me "}}),
        json!({"type": "content_block_delta", "index": 0, "delta": {"type": "thinking_delta", "thinking": "think..."}}),
        json!({"type": "content_block_delta", "index": 0, "delta": {"type": "signature_delta", "signature": "abc123sig"}}),
        json!({"type": "content_block_stop", "index": 0}),
        json!({"type": "content_block_start", "index": 1, "content_block": {"type": "text", "text": ""}}),
        json!({"type": "content_block_delta", "index": 1, "delta": {"type": "text_delta", "text": "42"}}),
        json!({"type": "content_block_stop", "index": 1}),
        json!({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn"},
            "usage": {"output_tokens": 15, "cache_read_input_tokens": 5}
        }),
        json!({"type": "message_stop"}),
    ];

    let out = replay(events);
    assert_sequence_numbers_monotonic(&out);

    let types: Vec<&str> = out.iter().map(|e| e["type"].as_str().unwrap()).collect();
    // signature_delta produced ZERO events -- confirm the one-to-zero mapping by absence, not
    // just by counting: no event in the whole sequence carries a "signature" field.
    assert!(out.iter().all(|e| e.get("signature").is_none()));
    assert_eq!(
        types,
        vec![
            "response.created",
            "response.in_progress",
            "response.output_item.added", // thinking block opens (index 0)
            "response.reasoning_summary_text.delta",
            "response.reasoning_summary_text.delta",
            "response.reasoning_summary_text.done",
            "response.output_item.done",
            "response.output_item.added", // text block opens (index 1)
            "response.content_part.added",
            "response.output_text.delta",
            "response.output_text.done",
            "response.content_part.done",
            "response.output_item.done",
            "response.completed",
        ]
    );

    // the thinking block and the text block get DISTINCT output_index values, matching
    // Anthropic's distinct flat `index` values (0 and 1) -- each content block is its own item.
    assert_eq!(out[2]["output_index"], json!(0));
    assert_eq!(out[7]["output_index"], json!(1));
    assert_ne!(out[2]["item"]["id"], out[7]["item"]["id"]);

    assert_eq!(out[5]["text"], json!("Let me think..."));
    assert_eq!(out[10]["text"], json!("42"));

    let final_output = &out.last().unwrap()["response"]["output"];
    assert_eq!(final_output[0]["type"], json!("reasoning"));
    assert_eq!(
        final_output[0]["summary"][0]["text"],
        json!("Let me think...")
    );
    assert_eq!(final_output[1]["type"], json!("message"));
    assert_eq!(final_output[1]["content"][0]["text"], json!("42"));

    // usage merge: input_tokens from message_start (30) survives message_delta's partial usage
    // object (output_tokens + cache_read_input_tokens only); cache_read maps to cached_tokens.
    let usage = &out.last().unwrap()["response"]["usage"];
    assert_eq!(usage["input_tokens"], json!(30));
    assert_eq!(usage["output_tokens"], json!(15));
    assert_eq!(usage["input_tokens_details"]["cached_tokens"], json!(5));
    assert_eq!(usage["total_tokens"], json!(45));
}

#[test]
fn each_turn_gets_a_fresh_translator_with_no_cross_turn_state() {
    let base_events = |msg_id: &str| {
        vec![
            json!({"type": "message_start", "message": {"id": msg_id, "model": "claude-opus-4-1-20250805", "usage": {"input_tokens": 5, "output_tokens": 0}}}),
            json!({"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}}),
            json!({"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": "hi"}}),
            json!({"type": "content_block_stop", "index": 0}),
            json!({"type": "message_delta", "delta": {"stop_reason": "end_turn"}, "usage": {"output_tokens": 1}}),
            json!({"type": "message_stop"}),
        ]
    };

    let first = replay(base_events("msg_a"));
    let second = replay(base_events("msg_b"));

    // Both turns start their own sequence_number counter at the same point (a fresh instance
    // per turn, per SPEC-M4 §3.4) -- the two turns' sequence_numbers are independent, not a
    // continuation of one shared counter.
    assert_eq!(
        first[0]["sequence_number"], second[0]["sequence_number"],
        "each turn's translator must start its own sequence_number counter from scratch"
    );

    // response.id and item.id are freshly minted per turn -- never reused across turns.
    assert_ne!(first[0]["response"]["id"], second[0]["response"]["id"]);
    assert_ne!(first[2]["item"]["id"], second[2]["item"]["id"]);
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
    assert_eq!(out["max_output_tokens"], json!(512));
    assert_eq!(out["stream"], json!(true));
}
