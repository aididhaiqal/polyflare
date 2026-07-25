//! OpenAI Responses client to Anthropic Messages backend translation.
//!
//! This adapter is intentionally separate from `AnthropicToResponses`: both are stateful per
//! turn, but their request and response directions are inverse and their stream bookkeeping is
//! incompatible.

use std::collections::HashMap;

use polyflare_core::Translator;
use rand::Rng;
use serde_json::{json, Value};

#[derive(Clone, Debug, PartialEq, Eq)]
enum BlockKind {
    Text,
    ToolUse,
    Thinking,
}

#[derive(Clone)]
struct BlockState {
    kind: BlockKind,
    item_id: String,
    call_id: Option<String>,
    name: Option<String>,
    buffer: String,
}

#[derive(Default)]
pub struct ResponsesToAnthropic {
    seq: u64,
    response_id: Option<String>,
    model: Option<Value>,
    blocks: HashMap<u64, BlockState>,
    order: Vec<u64>,
    usage: Option<Value>,
    stop_reason: Option<String>,
}

impl ResponsesToAnthropic {
    pub fn new() -> Self {
        Self::default()
    }

    fn next_seq(&mut self) -> u64 {
        let sequence = self.seq;
        self.seq += 1;
        sequence
    }

    fn merge_usage(&mut self, incoming: &Value) {
        let usage = self.usage.get_or_insert_with(|| json!({}));
        if let (Some(current), Some(incoming)) = (usage.as_object_mut(), incoming.as_object()) {
            for (key, value) in incoming {
                current.insert(key.clone(), value.clone());
            }
        }
    }

    fn on_message_start(&mut self, event: &Value) -> Vec<Value> {
        let message = event.get("message").cloned().unwrap_or(Value::Null);
        let response_id = synth_id("resp");
        let model = message.get("model").cloned().unwrap_or(Value::Null);
        self.response_id = Some(response_id.clone());
        self.model = Some(model.clone());
        if let Some(usage) = message.get("usage") {
            self.merge_usage(usage);
        }
        let response = json!({
            "id": response_id,
            "object": "response",
            "status": "in_progress",
            "model": model,
            "output": [],
            "usage": Value::Null,
        });
        vec![
            json!({
                "type": "response.created",
                "sequence_number": self.next_seq(),
                "response": response.clone(),
            }),
            json!({
                "type": "response.in_progress",
                "sequence_number": self.next_seq(),
                "response": response,
            }),
        ]
    }

    fn on_content_block_start(&mut self, event: &Value) -> Vec<Value> {
        let Some(index) = block_index(event) else {
            return vec![];
        };
        let block = event.get("content_block").cloned().unwrap_or(Value::Null);
        let block_type = block.get("type").and_then(Value::as_str).unwrap_or("");
        let (state, item) = match block_type {
            "text" => {
                let item_id = synth_id("msg");
                (
                    BlockState {
                        kind: BlockKind::Text,
                        item_id: item_id.clone(),
                        call_id: None,
                        name: None,
                        buffer: String::new(),
                    },
                    json!({
                        "id": item_id,
                        "type": "message",
                        "status": "in_progress",
                        "role": "assistant",
                        "content": [],
                    }),
                )
            }
            "tool_use" => {
                let item_id = synth_id("fc");
                let call_id = block
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let name = block
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                (
                    BlockState {
                        kind: BlockKind::ToolUse,
                        item_id: item_id.clone(),
                        call_id: Some(call_id.clone()),
                        name: Some(name.clone()),
                        buffer: String::new(),
                    },
                    json!({
                        "id": item_id,
                        "type": "function_call",
                        "status": "in_progress",
                        "call_id": call_id,
                        "name": name,
                        "arguments": "",
                    }),
                )
            }
            "thinking" => {
                let item_id = synth_id("rs");
                (
                    BlockState {
                        kind: BlockKind::Thinking,
                        item_id: item_id.clone(),
                        call_id: None,
                        name: None,
                        buffer: String::new(),
                    },
                    json!({
                        "id": item_id,
                        "type": "reasoning",
                        "status": "in_progress",
                        "summary": [],
                    }),
                )
            }
            _ => return vec![],
        };
        let item_id = state.item_id.clone();
        let kind = state.kind.clone();
        self.blocks.insert(index, state);
        self.order.push(index);

        let mut events = vec![json!({
            "type": "response.output_item.added",
            "sequence_number": self.next_seq(),
            "output_index": index,
            "item": item,
        })];
        if kind == BlockKind::Text {
            events.push(json!({
                "type": "response.content_part.added",
                "sequence_number": self.next_seq(),
                "item_id": item_id,
                "output_index": index,
                "content_index": 0,
                "part": {"type": "output_text", "text": "", "annotations": []},
            }));
        }
        events
    }

    fn on_content_block_delta(&mut self, event: &Value) -> Vec<Value> {
        let Some(index) = block_index(event) else {
            return vec![];
        };
        let delta = event.get("delta").cloned().unwrap_or(Value::Null);
        let delta_type = delta.get("type").and_then(Value::as_str).unwrap_or("");
        let Some(block) = self.blocks.get_mut(&index) else {
            return vec![];
        };
        let (event_type, value, extra) = match delta_type {
            "text_delta" => (
                "response.output_text.delta",
                delta.get("text").and_then(Value::as_str).unwrap_or(""),
                json!({"content_index": 0, "logprobs": []}),
            ),
            "input_json_delta" => (
                "response.function_call_arguments.delta",
                delta
                    .get("partial_json")
                    .and_then(Value::as_str)
                    .unwrap_or(""),
                json!({}),
            ),
            "thinking_delta" => (
                "response.reasoning_summary_text.delta",
                delta.get("thinking").and_then(Value::as_str).unwrap_or(""),
                json!({"summary_index": 0}),
            ),
            _ => return vec![],
        };
        block.buffer.push_str(value);
        let item_id = block.item_id.clone();
        let sequence_number = self.next_seq();
        let mut mapped = json!({
            "type": event_type,
            "sequence_number": sequence_number,
            "item_id": item_id,
            "output_index": index,
            "delta": value,
        });
        if let (Some(mapped), Some(extra)) = (mapped.as_object_mut(), extra.as_object()) {
            mapped.extend(extra.clone());
        }
        vec![mapped]
    }

    fn on_content_block_stop(&mut self, event: &Value) -> Vec<Value> {
        let Some(index) = block_index(event) else {
            return vec![];
        };
        let Some(block) = self.blocks.get(&index).cloned() else {
            return vec![];
        };
        match block.kind {
            BlockKind::Text => vec![
                json!({
                    "type": "response.output_text.done",
                    "sequence_number": self.next_seq(),
                    "item_id": block.item_id,
                    "output_index": index,
                    "content_index": 0,
                    "text": block.buffer,
                }),
                json!({
                    "type": "response.content_part.done",
                    "sequence_number": self.next_seq(),
                    "item_id": block.item_id,
                    "output_index": index,
                    "content_index": 0,
                    "part": {"type": "output_text", "text": block.buffer, "annotations": []},
                }),
                json!({
                    "type": "response.output_item.done",
                    "sequence_number": self.next_seq(),
                    "output_index": index,
                    "item": {
                        "id": block.item_id,
                        "type": "message",
                        "status": "completed",
                        "role": "assistant",
                        "content": [{"type": "output_text", "text": block.buffer, "annotations": []}],
                    },
                }),
            ],
            BlockKind::ToolUse => vec![
                json!({
                    "type": "response.function_call_arguments.done",
                    "sequence_number": self.next_seq(),
                    "item_id": block.item_id,
                    "output_index": index,
                    "arguments": block.buffer,
                }),
                json!({
                    "type": "response.output_item.done",
                    "sequence_number": self.next_seq(),
                    "output_index": index,
                    "item": completed_item(&block),
                }),
            ],
            BlockKind::Thinking => vec![
                json!({
                    "type": "response.reasoning_summary_text.done",
                    "sequence_number": self.next_seq(),
                    "item_id": block.item_id,
                    "output_index": index,
                    "summary_index": 0,
                    "text": block.buffer,
                }),
                json!({
                    "type": "response.output_item.done",
                    "sequence_number": self.next_seq(),
                    "output_index": index,
                    "item": completed_item(&block),
                }),
            ],
        }
    }

    fn on_message_delta(&mut self, event: &Value) -> Vec<Value> {
        self.stop_reason = event
            .get("delta")
            .and_then(|delta| delta.get("stop_reason"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| self.stop_reason.clone());
        if let Some(usage) = event.get("usage") {
            self.merge_usage(usage);
        }
        vec![]
    }

    fn on_message_stop(&mut self) -> Vec<Value> {
        let incomplete = self.stop_reason.as_deref() == Some("max_tokens");
        let status = if incomplete {
            "incomplete"
        } else {
            "completed"
        };
        let output = self
            .order
            .iter()
            .filter_map(|index| self.blocks.get(index))
            .map(completed_item)
            .collect::<Vec<_>>();
        let mut response = json!({
            "id": self.response_id.clone().unwrap_or_default(),
            "object": "response",
            "status": status,
            "model": self.model.clone().unwrap_or(Value::Null),
            "output": output,
            "usage": self.usage.as_ref().map(map_usage).unwrap_or(Value::Null),
        });
        if incomplete {
            response["incomplete_details"] = json!({"reason": "max_output_tokens"});
        }
        vec![json!({
            "type": if incomplete { "response.incomplete" } else { "response.completed" },
            "sequence_number": self.next_seq(),
            "response": response,
        })]
    }
}

impl Translator for ResponsesToAnthropic {
    fn translate_request(&mut self, body: Value) -> Value {
        map_request(body)
    }

    fn translate_response_event(&mut self, event: Value) -> Vec<Value> {
        match event.get("type").and_then(Value::as_str).unwrap_or("") {
            "message_start" => self.on_message_start(&event),
            "content_block_start" => self.on_content_block_start(&event),
            "content_block_delta" => self.on_content_block_delta(&event),
            "content_block_stop" => self.on_content_block_stop(&event),
            "message_delta" => self.on_message_delta(&event),
            "message_stop" => self.on_message_stop(),
            "error" => vec![json!({
                "type": "error",
                "sequence_number": self.next_seq(),
                "code": event.pointer("/error/type").and_then(Value::as_str).unwrap_or("api_error"),
                "message": event.pointer("/error/message").and_then(Value::as_str).unwrap_or(""),
            })],
            _ => vec![],
        }
    }
}

impl std::fmt::Debug for ResponsesToAnthropic {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResponsesToAnthropic")
            .field("sequence", &self.seq)
            .field("response_id", &self.response_id)
            .field(
                "blocks",
                &format!("[{} block(s) redacted]", self.blocks.len()),
            )
            .field("stop_reason", &self.stop_reason)
            .finish()
    }
}

fn map_request(body: Value) -> Value {
    let mut messages = Vec::new();
    match body.get("input") {
        Some(Value::String(text)) => {
            messages.push(json!({"role": "user", "content": text}));
        }
        Some(Value::Array(items)) => {
            for item in items {
                map_input_item(item, &mut messages);
            }
        }
        _ => {}
    }
    let mut output = json!({
        "model": body.get("model").cloned().unwrap_or(Value::Null),
        "messages": messages,
        "stream": body.get("stream").cloned().unwrap_or(Value::Bool(false)),
        "max_tokens": body
            .get("max_output_tokens")
            .cloned()
            .unwrap_or_else(|| json!(4096)),
    });
    let object = output.as_object_mut().expect("object literal");
    if let Some(instructions) = body.get("instructions") {
        object.insert("system".into(), instructions.clone());
    }
    if let Some(tools) = body.get("tools").and_then(Value::as_array) {
        object.insert(
            "tools".into(),
            Value::Array(
                tools
                    .iter()
                    .filter(|tool| tool.get("type").and_then(Value::as_str) == Some("function"))
                    .map(|tool| {
                        json!({
                            "name": tool.get("name").cloned().unwrap_or(Value::Null),
                            "description": tool.get("description").cloned().unwrap_or(Value::Null),
                            "input_schema": tool.get("parameters").cloned().unwrap_or_else(|| json!({})),
                        })
                    })
                    .collect(),
            ),
        );
    }
    if let Some(tool_choice) = body.get("tool_choice") {
        object.insert("tool_choice".into(), map_tool_choice(tool_choice));
    }
    output
}

fn map_input_item(item: &Value, messages: &mut Vec<Value>) {
    match item.get("type").and_then(Value::as_str) {
        Some("message") | None
            if item
                .get("role")
                .and_then(Value::as_str)
                .is_some_and(|role| {
                    matches!(role, "user" | "assistant" | "system" | "developer")
                }) =>
        {
            let role = if item.get("role").and_then(Value::as_str) == Some("assistant") {
                "assistant"
            } else {
                "user"
            };
            let content = match item.get("content") {
                Some(Value::String(text)) => vec![json!({"type": "text", "text": text})],
                Some(Value::Array(parts)) => parts
                    .iter()
                    .filter_map(map_content_part)
                    .collect::<Vec<_>>(),
                _ => vec![],
            };
            if !content.is_empty() {
                messages.push(json!({"role": role, "content": content}));
            }
        }
        Some("function_call") => {
            let input = item
                .get("arguments")
                .and_then(Value::as_str)
                .and_then(|value| serde_json::from_str(value).ok())
                .unwrap_or_else(|| json!({}));
            messages.push(json!({
                "role": "assistant",
                "content": [{
                    "type": "tool_use",
                    "id": item.get("call_id").cloned().unwrap_or(Value::Null),
                    "name": item.get("name").cloned().unwrap_or(Value::Null),
                    "input": input,
                }],
            }));
        }
        Some("function_call_output") => {
            messages.push(json!({
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": item.get("call_id").cloned().unwrap_or(Value::Null),
                    "content": map_tool_output(item.get("output")),
                }],
            }));
        }
        _ => {}
    }
}

fn map_tool_choice(tool_choice: &Value) -> Value {
    match tool_choice {
        Value::String(value) if value == "auto" => json!({"type": "auto"}),
        Value::String(value) if value == "required" => json!({"type": "any"}),
        Value::String(value) if value == "none" => json!({"type": "none"}),
        Value::Object(object) if object.get("type").and_then(Value::as_str) == Some("function") => {
            json!({
                "type": "tool",
                "name": object.get("name").cloned().unwrap_or(Value::String(String::new())),
            })
        }
        _ => tool_choice.clone(),
    }
}

fn map_tool_output(output: Option<&Value>) -> Value {
    match output {
        Some(Value::Array(parts)) => Value::Array(
            parts
                .iter()
                .filter_map(map_content_part)
                .collect::<Vec<_>>(),
        ),
        Some(output) => output.clone(),
        None => Value::String(String::new()),
    }
}

fn map_content_part(part: &Value) -> Option<Value> {
    match part.get("type").and_then(Value::as_str) {
        Some("input_text" | "output_text") => Some(json!({
            "type": "text",
            "text": part.get("text").cloned().unwrap_or(Value::String(String::new())),
        })),
        Some("input_image") => {
            let image_url = part.get("image_url").and_then(Value::as_str)?;
            if let Some(data) = image_url.strip_prefix("data:") {
                let (metadata, encoded) = data.split_once(',')?;
                let media_type = metadata.strip_suffix(";base64")?;
                Some(json!({
                    "type": "image",
                    "source": {"type": "base64", "media_type": media_type, "data": encoded},
                }))
            } else {
                Some(json!({
                    "type": "image",
                    "source": {"type": "url", "url": image_url},
                }))
            }
        }
        _ => None,
    }
}

fn completed_item(block: &BlockState) -> Value {
    match block.kind {
        BlockKind::Text => json!({
            "id": block.item_id,
            "type": "message",
            "status": "completed",
            "role": "assistant",
            "content": [{"type": "output_text", "text": block.buffer, "annotations": []}],
        }),
        BlockKind::ToolUse => json!({
            "id": block.item_id,
            "type": "function_call",
            "status": "completed",
            "call_id": block.call_id.clone().unwrap_or_default(),
            "name": block.name.clone().unwrap_or_default(),
            "arguments": block.buffer,
        }),
        BlockKind::Thinking => json!({
            "id": block.item_id,
            "type": "reasoning",
            "status": "completed",
            "summary": [{"type": "summary_text", "text": block.buffer}],
        }),
    }
}

fn synth_id(prefix: &str) -> String {
    let mut rng = rand::rng();
    let bytes: [u8; 12] = rng.random();
    let hex = bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("{prefix}_{hex}")
}

fn block_index(event: &Value) -> Option<u64> {
    event.get("index").and_then(Value::as_u64)
}

fn map_usage(anthropic: &Value) -> Value {
    let input_tokens = anthropic
        .get("input_tokens")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let output_tokens = anthropic
        .get("output_tokens")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let cached_tokens = anthropic
        .get("cache_read_input_tokens")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let reasoning_tokens = anthropic
        .get("thinking_tokens")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    json!({
        "input_tokens": input_tokens,
        "input_tokens_details": {"cached_tokens": cached_tokens},
        "output_tokens": output_tokens,
        "output_tokens_details": {"reasoning_tokens": reasoning_tokens},
        "total_tokens": input_tokens + output_tokens,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_responses_request_with_history_tools_images_and_token_cap() {
        let mut translator = ResponsesToAnthropic::new();
        let mapped = translator.translate_request(json!({
            "model": "claude-sonnet-4",
            "instructions": "Be precise",
            "max_output_tokens": 1234,
            "stream": true,
            "input": [
                {"type": "message", "role": "user", "content": [
                    {"type": "input_text", "text": "inspect"},
                    {"type": "input_image", "image_url": "data:image/png;base64,AA=="}
                ]},
                {"type": "function_call", "call_id": "call_1", "name": "read", "arguments": "{\"path\":\"a\"}"},
                {"type": "function_call_output", "call_id": "call_1", "output": "ok"}
            ],
            "tools": [{"type": "function", "name": "read", "description": "Read", "parameters": {"type": "object"}}]
            ,"tool_choice": {"type": "function", "name": "read"}
        }));
        assert_eq!(mapped["system"], "Be precise");
        assert_eq!(mapped["max_tokens"], 1234);
        assert_eq!(
            mapped["messages"][0]["content"][1]["source"]["type"],
            "base64"
        );
        assert_eq!(mapped["messages"][1]["content"][0]["type"], "tool_use");
        assert_eq!(mapped["messages"][2]["content"][0]["type"], "tool_result");
        assert_eq!(mapped["tools"][0]["input_schema"]["type"], "object");
        assert_eq!(mapped["tool_choice"], json!({"type":"tool","name":"read"}));
    }

    #[test]
    fn maps_anthropic_stream_lifecycle_text_tools_usage_and_incomplete() {
        let mut translator = ResponsesToAnthropic::new();
        assert_eq!(
            translator
                .translate_response_event(json!({
                    "type": "message_start",
                    "message": {"model": "claude", "usage": {"input_tokens": 10}}
                }))
                .len(),
            2
        );
        translator.translate_response_event(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "tool_use", "id": "toolu_1", "name": "read", "input": {}}
        }));
        let delta = translator.translate_response_event(json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "input_json_delta", "partial_json": "{\"path\":\"a\"}"}
        }));
        assert_eq!(delta[0]["type"], "response.function_call_arguments.delta");
        translator.translate_response_event(json!({
            "type": "content_block_stop",
            "index": 0
        }));
        translator.translate_response_event(json!({
            "type": "message_delta",
            "delta": {"stop_reason": "max_tokens"},
            "usage": {"output_tokens": 4, "cache_read_input_tokens": 3}
        }));
        let terminal = translator.translate_response_event(json!({"type": "message_stop"}));
        assert_eq!(terminal[0]["type"], "response.incomplete");
        assert_eq!(terminal[0]["response"]["usage"]["total_tokens"], 14);
        assert_eq!(
            terminal[0]["response"]["incomplete_details"]["reason"],
            "max_output_tokens"
        );
    }
}
