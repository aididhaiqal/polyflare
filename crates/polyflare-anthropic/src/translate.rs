//! Anthropic Messages → OpenAI-Responses translator (SPEC-M4 §3.4 stateful 1->N seam). This file
//! builds the mapping in two layers: `map_request` (this task) does the doc-verified *mechanical*
//! request-body field mapping (SPEC-M4 §3.6's "mechanical direction") — model-alias remap and
//! reasoning-effort payload-override are explicitly deferred (SPEC-M4 U2, M4b-wiring), so `model`
//! passes through unchanged here. `AnthropicToResponses` (added on top of this module) is the
//! stateful streaming response-event translator (SPEC-M4 §3.5).

use std::collections::HashMap;

use polyflare_core::Translator;
use rand::Rng;
use serde_json::{json, Value};

/// Map an Anthropic Messages request body to an OpenAI-Responses request body.
///
/// **Envelope** (top-level field renames): `model` passthrough (no alias remap — SPEC-M4 U2),
/// `system`→`instructions`, `stream` passthrough, `max_tokens`→`max_output_tokens`.
///
/// **Content/tool-shape transform** (T5 — closes the gap this doc comment used to flag): `messages`
/// and `tools` are no longer copied verbatim; both are reshaped into their doc-verified
/// OpenAI-Responses request shapes (confirmed against the `openai/openai-openapi` spec —
/// `FunctionTool`, `InputMessage`/`OutputMessage`, `FunctionToolCall`, `FunctionCallOutputItemParam`
/// components):
///   - `messages` → `input` (array of Responses input items). Each Anthropic message's `content`
///     (a string, or an array of blocks) is mapped per-block: `text` → an `input_text` part on
///     `user`-role messages, an `output_text` part (with `annotations: []`) on `assistant`-role
///     messages, packed into a `{"type":"message","role":…,"content":[…]}` item; `image` (base64 or
///     url source) → an `input_image` part (`image_url` as a `data:`-URL for base64, best-effort —
///     `document`/PDF blocks are not handled). Anthropic nests `tool_use`/`tool_result` *inside* a
///     message's content, but Responses represents them as **top-level, sibling** input items — they
///     are flattened out: `tool_use` → a `function_call` item (`arguments` JSON-stringified from
///     `input`), `tool_result` → a `function_call_output` item (`output` as a string, or an array of
///     `input_text`/`input_image` parts when `content` is itself a block array; `is_error` has no
///     Responses field and is dropped). `thinking` blocks are **dropped**, not translated to a
///     `reasoning` item: a Responses `reasoning` item's `id`/`encrypted_content` must be the exact
///     opaque values the model produced for the model to resume that chain of thought, and
///     Anthropic's `thinking`/`signature` carries no such value — fabricating one would misrepresent
///     state rather than merely omit it.
///   - `tools`: Anthropic `{name, description, input_schema}` → Responses `{type:"function", name,
///     description, parameters}` (flat — confirmed no nested `function` wrapper key, unlike Chat
///     Completions). `description` is omitted when absent (nullable in the spec); `parameters`
///     defaults to `{}` when `input_schema` is absent. The spec's `FunctionTool.required` also lists
///     `strict` (nullable), which this mapping does not emit — out of scope per the task directive.
///
/// ⚠️ **Real-capture end-to-end validation still pending (U4).** Several shape choices above were
/// resolved against the *documented* schema where the doc itself is ambiguous or internally
/// inconsistent, and need confirming against a real Responses backend before the runtime
/// Anthropic→Codex path (M4b-wiring) depends on them:
///   - an `assistant`-role history message built from `output_text` parts (no `id`/`status`) matches
///     neither sub-schema exactly: the strict `Item`→`OutputMessage` variant requires `id` + `status`
///     (which we omit), while the lenient `EasyInputMessage` variant allows omitting them but expects
///     `input_text`/`input_image`/`input_file` parts (not `output_text`) for its content list. This
///     mapping follows the task's explicit directive (`output_text` for assistant history) as the
///     documented default; whether a real backend accepts the resulting hybrid is unverified;
///   - `thinking` blocks are dropped entirely rather than represented in any form — unverified
///     whether the live Codex backend needs *something* in their place to avoid re-deriving already
///     "paid for" reasoning;
///   - `tool_result` `content` given as an array maps to a Responses `output` content-part array
///     (`input_text`/`input_image`); an empty/all-unrecognized block array degrades to `output: []`,
///     unverified against a real backend's minimum-shape expectations.
///
/// The response-side translator (`AnthropicToResponses`, §3.5) is complete and independent of this
/// module doc's scope.
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
        "input": map_messages(&messages),
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
        map.insert("tools".to_string(), map_tools(&t));
    }
    out
}

/// Map Anthropic `messages` to an OpenAI-Responses `input` array, flattening `tool_use`/
/// `tool_result` blocks out of their enclosing message into top-level `function_call`/
/// `function_call_output` items (see `map_request`'s doc comment for the full rationale).
fn map_messages(messages: &Value) -> Value {
    let mut items: Vec<Value> = Vec::new();
    let Some(arr) = messages.as_array() else {
        return Value::Array(items);
    };

    for message in arr {
        let role = message
            .get("role")
            .and_then(|v| v.as_str())
            .unwrap_or("user");

        match message.get("content") {
            Some(Value::String(text)) => {
                items.push(message_item(role, vec![text_part(role, text)]));
            }
            Some(Value::Array(blocks)) => {
                let mut buffer: Vec<Value> = Vec::new();
                for block in blocks {
                    let block_type = block.get("type").and_then(|v| v.as_str()).unwrap_or("");
                    match block_type {
                        "text" => {
                            let text = block.get("text").and_then(|v| v.as_str()).unwrap_or("");
                            buffer.push(text_part(role, text));
                        }
                        "image" => {
                            if let Some(part) = image_part(block) {
                                buffer.push(part);
                            }
                        }
                        "tool_use" => {
                            flush_message_buffer(&mut items, &mut buffer, role);
                            items.push(function_call_item(block));
                        }
                        "tool_result" => {
                            flush_message_buffer(&mut items, &mut buffer, role);
                            items.push(function_call_output_item(block));
                        }
                        // `thinking` (and any unrecognized block type) is dropped -- see the
                        // module doc comment's rationale (no valid reasoning-item id to replay).
                        _ => {}
                    }
                }
                flush_message_buffer(&mut items, &mut buffer, role);
            }
            _ => {}
        }
    }

    Value::Array(items)
}

/// Flush any buffered content parts into a `message` input item, if non-empty. Called both
/// mid-message (before a `tool_use`/`tool_result` block flattens to a sibling top-level item) and
/// at message end, so ordering between text/image parts and flattened tool items is preserved.
fn flush_message_buffer(items: &mut Vec<Value>, buffer: &mut Vec<Value>, role: &str) {
    if !buffer.is_empty() {
        items.push(message_item(role, std::mem::take(buffer)));
    }
}

fn message_item(role: &str, content: Vec<Value>) -> Value {
    json!({"type": "message", "role": role, "content": content})
}

/// A `text` block's part shape depends on which role it renders under: `output_text` (with
/// `annotations: []`) for `assistant` history, `input_text` for everything else (`user`/`system`/
/// `developer`).
fn text_part(role: &str, text: &str) -> Value {
    if role == "assistant" {
        json!({"type": "output_text", "text": text, "annotations": []})
    } else {
        json!({"type": "input_text", "text": text})
    }
}

/// Map an Anthropic `image` content block to a Responses `input_image` part. Best-effort: a
/// `base64` source becomes a `data:` URL; a `url` source passes the URL through; any other/missing
/// source is dropped (returns `None`).
fn image_part(block: &Value) -> Option<Value> {
    let source = block.get("source")?;
    let source_type = source.get("type").and_then(|v| v.as_str())?;
    let image_url = match source_type {
        "base64" => {
            let media_type = source
                .get("media_type")
                .and_then(|v| v.as_str())
                .unwrap_or("image/png");
            let data = source.get("data").and_then(|v| v.as_str())?;
            format!("data:{media_type};base64,{data}")
        }
        "url" => source.get("url").and_then(|v| v.as_str())?.to_string(),
        _ => return None,
    };
    Some(json!({"type": "input_image", "image_url": image_url, "detail": "auto"}))
}

/// Flatten an Anthropic `tool_use` block into a top-level Responses `function_call` item.
/// `input` (a JSON object) is JSON-stringified into `arguments` per the Responses schema.
fn function_call_item(block: &Value) -> Value {
    let call_id = block
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let name = block
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let input = block.get("input").cloned().unwrap_or_else(|| json!({}));
    let arguments = serde_json::to_string(&input).unwrap_or_else(|_| "{}".to_string());
    json!({
        "type": "function_call",
        "call_id": call_id,
        "name": name,
        "arguments": arguments,
    })
}

/// Flatten an Anthropic `tool_result` block into a top-level Responses `function_call_output`
/// item. `content` is a string (passed through as `output`) or an array of blocks (mapped to an
/// `output` array of `input_text`/`input_image` parts). `is_error` has no Responses field and is
/// dropped (see the module doc comment).
fn function_call_output_item(block: &Value) -> Value {
    let call_id = block
        .get("tool_use_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let output = match block.get("content") {
        Some(Value::String(s)) => json!(s),
        Some(Value::Array(blocks)) => {
            let parts: Vec<Value> = blocks
                .iter()
                .filter_map(|b| {
                    let block_type = b.get("type").and_then(|v| v.as_str()).unwrap_or("");
                    match block_type {
                        "text" => b
                            .get("text")
                            .and_then(|v| v.as_str())
                            .map(|text| json!({"type": "input_text", "text": text})),
                        "image" => image_part(b),
                        _ => None,
                    }
                })
                .collect();
            json!(parts)
        }
        _ => json!(""),
    };
    json!({
        "type": "function_call_output",
        "call_id": call_id,
        "output": output,
    })
}

/// Map Anthropic `tools` to the Responses `tools` shape: `{name, description, input_schema}` →
/// `{type:"function", name, description, parameters}` (flat, no nested `function` wrapper — see
/// the module doc comment).
fn map_tools(tools: &Value) -> Value {
    let Some(arr) = tools.as_array() else {
        return tools.clone();
    };

    let mapped: Vec<Value> = arr
        .iter()
        .map(|tool| {
            let name = tool.get("name").cloned().unwrap_or(Value::Null);
            let description = tool.get("description").cloned();
            let parameters = tool
                .get("input_schema")
                .cloned()
                .unwrap_or_else(|| json!({}));

            let mut mapped_tool = json!({
                "type": "function",
                "name": name,
                "parameters": parameters,
            });
            if let Some(desc) = description {
                mapped_tool
                    .as_object_mut()
                    .expect("json! object literal")
                    .insert("description".to_string(), desc);
            }
            mapped_tool
        })
        .collect();

    Value::Array(mapped)
}

/// The kind of an open Anthropic content block, tracked so `content_block_delta`/`_stop` know
/// which OpenAI-Responses event family to emit.
#[derive(Clone, Debug, PartialEq, Eq)]
enum BlockKind {
    Text,
    ToolUse,
    Thinking,
}

/// Per-block per-turn state: the synthesized OpenAI item id, the tool call_id/name (tool_use
/// only), and the buffered accumulated text/arguments (SPEC-M4 §3.5: "full accumulated
/// text/arguments [S] (buffered across deltas)").
#[derive(Clone, Debug)]
struct BlockState {
    kind: BlockKind,
    item_id: String,
    call_id: Option<String>,
    name: Option<String>,
    buffer: String,
}

/// Stateful per-turn Anthropic→OpenAI-Responses translator (SPEC-M4 §3.4/§3.5). Construct a
/// fresh instance per turn via `AnthropicToResponses::new()` — never reuse one across requests.
#[derive(Default)]
pub struct AnthropicToResponses {
    seq: u64,
    response_id: Option<String>,
    model: Option<Value>,
    blocks: HashMap<u64, BlockState>,
    order: Vec<u64>,
    usage: Option<Value>,
    stop_reason: Option<String>,
}

impl AnthropicToResponses {
    pub fn new() -> Self {
        Self::default()
    }

    fn next_seq(&mut self) -> u64 {
        let n = self.seq;
        self.seq += 1;
        n
    }

    /// Shallow-merge an incoming Anthropic `usage` object into accumulated per-turn usage.
    /// Anthropic splits usage across `message_start` (typically `input_tokens`) and each
    /// `message_delta` (typically `output_tokens`, updated cumulatively) — merging (rather than
    /// overwriting) means a partial `message_delta.usage` never drops a field only seen at
    /// `message_start` (see "Spec gaps hit while planning", item 6).
    fn merge_usage(&mut self, incoming: &Value) {
        let entry = self.usage.get_or_insert_with(|| json!({}));
        if let (Some(obj), Some(inc_obj)) = (entry.as_object_mut(), incoming.as_object()) {
            for (k, v) in inc_obj {
                obj.insert(k.clone(), v.clone());
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

        let created_seq = self.next_seq();
        let created = json!({
            "type": "response.created",
            "sequence_number": created_seq,
            "response": response.clone(),
        });
        let in_progress_seq = self.next_seq();
        let in_progress = json!({
            "type": "response.in_progress",
            "sequence_number": in_progress_seq,
            "response": response,
        });
        vec![created, in_progress]
    }

    fn on_content_block_start(&mut self, event: &Value) -> Vec<Value> {
        let Some(idx) = block_index(event) else {
            return vec![];
        };
        let block = event.get("content_block").cloned().unwrap_or(Value::Null);
        let kind_str = block.get("type").and_then(|v| v.as_str()).unwrap_or("");

        match kind_str {
            "text" => {
                let item_id = synth_id("msg");
                self.blocks.insert(
                    idx,
                    BlockState {
                        kind: BlockKind::Text,
                        item_id: item_id.clone(),
                        call_id: None,
                        name: None,
                        buffer: String::new(),
                    },
                );
                self.order.push(idx);

                let item = json!({
                    "id": item_id,
                    "type": "message",
                    "status": "in_progress",
                    "role": "assistant",
                    "content": [],
                });
                let added_seq = self.next_seq();
                let item_added = json!({
                    "type": "response.output_item.added",
                    "sequence_number": added_seq,
                    "output_index": idx,
                    "item": item,
                });

                let part = json!({"type": "output_text", "text": "", "annotations": []});
                let part_seq = self.next_seq();
                let part_added = json!({
                    "type": "response.content_part.added",
                    "sequence_number": part_seq,
                    "item_id": item_id,
                    "output_index": idx,
                    "content_index": 0,
                    "part": part,
                });

                vec![item_added, part_added]
            }
            "tool_use" => {
                let call_id = block
                    .get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let name = block
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let item_id = synth_id("fc");
                self.blocks.insert(
                    idx,
                    BlockState {
                        kind: BlockKind::ToolUse,
                        item_id: item_id.clone(),
                        call_id: Some(call_id.clone()),
                        name: Some(name.clone()),
                        buffer: String::new(),
                    },
                );
                self.order.push(idx);

                let item = json!({
                    "id": item_id,
                    "type": "function_call",
                    "status": "in_progress",
                    "call_id": call_id,
                    "name": name,
                    "arguments": "",
                });
                let seq = self.next_seq();
                vec![json!({
                    "type": "response.output_item.added",
                    "sequence_number": seq,
                    "output_index": idx,
                    "item": item,
                })]
            }
            "thinking" => {
                let item_id = synth_id("rs");
                self.blocks.insert(
                    idx,
                    BlockState {
                        kind: BlockKind::Thinking,
                        item_id: item_id.clone(),
                        call_id: None,
                        name: None,
                        buffer: String::new(),
                    },
                );
                self.order.push(idx);

                let item = json!({
                    "id": item_id,
                    "type": "reasoning",
                    "status": "in_progress",
                    "summary": [],
                });
                let seq = self.next_seq();
                vec![json!({
                    "type": "response.output_item.added",
                    "sequence_number": seq,
                    "output_index": idx,
                    "item": item,
                })]
            }
            _ => vec![],
        }
    }

    fn on_content_block_delta(&mut self, event: &Value) -> Vec<Value> {
        let Some(idx) = block_index(event) else {
            return vec![];
        };
        let delta = event.get("delta").cloned().unwrap_or(Value::Null);
        let delta_type = delta.get("type").and_then(|v| v.as_str()).unwrap_or("");
        let Some(block) = self.blocks.get_mut(&idx) else {
            return vec![];
        };

        match delta_type {
            "text_delta" => {
                let text = delta
                    .get("text")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                block.buffer.push_str(&text);
                let item_id = block.item_id.clone();
                let seq = self.next_seq();
                vec![json!({
                    "type": "response.output_text.delta",
                    "sequence_number": seq,
                    "item_id": item_id,
                    "output_index": idx,
                    "content_index": 0,
                    "delta": text,
                    "logprobs": [],
                })]
            }
            "input_json_delta" => {
                let partial = delta
                    .get("partial_json")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                block.buffer.push_str(&partial);
                let item_id = block.item_id.clone();
                let seq = self.next_seq();
                vec![json!({
                    "type": "response.function_call_arguments.delta",
                    "sequence_number": seq,
                    "item_id": item_id,
                    "output_index": idx,
                    "delta": partial,
                })]
            }
            "thinking_delta" => {
                let text = delta
                    .get("thinking")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                block.buffer.push_str(&text);
                let item_id = block.item_id.clone();
                let seq = self.next_seq();
                vec![json!({
                    "type": "response.reasoning_summary_text.delta",
                    "sequence_number": seq,
                    "item_id": item_id,
                    "output_index": idx,
                    "summary_index": 0,
                    "delta": text,
                })]
            }
            // signature_delta (one-to-zero, SPEC-M4 §3.5: no OpenAI event carries a reasoning
            // signature) and any unrecognized delta type both emit nothing.
            _ => vec![],
        }
    }

    fn on_content_block_stop(&mut self, event: &Value) -> Vec<Value> {
        let Some(idx) = block_index(event) else {
            return vec![];
        };
        let Some(block) = self.blocks.get(&idx).cloned() else {
            return vec![];
        };

        match block.kind {
            BlockKind::Text => {
                let text_done_seq = self.next_seq();
                let text_done = json!({
                    "type": "response.output_text.done",
                    "sequence_number": text_done_seq,
                    "item_id": block.item_id,
                    "output_index": idx,
                    "content_index": 0,
                    "text": block.buffer,
                });
                let part = json!({"type": "output_text", "text": block.buffer, "annotations": []});
                let part_done_seq = self.next_seq();
                let part_done = json!({
                    "type": "response.content_part.done",
                    "sequence_number": part_done_seq,
                    "item_id": block.item_id,
                    "output_index": idx,
                    "content_index": 0,
                    "part": part,
                });
                let item = json!({
                    "id": block.item_id,
                    "type": "message",
                    "status": "completed",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": block.buffer, "annotations": []}],
                });
                let item_done_seq = self.next_seq();
                let item_done = json!({
                    "type": "response.output_item.done",
                    "sequence_number": item_done_seq,
                    "output_index": idx,
                    "item": item,
                });
                vec![text_done, part_done, item_done]
            }
            BlockKind::ToolUse => {
                let args_done_seq = self.next_seq();
                let args_done = json!({
                    "type": "response.function_call_arguments.done",
                    "sequence_number": args_done_seq,
                    "item_id": block.item_id,
                    "output_index": idx,
                    "arguments": block.buffer,
                });
                let item = json!({
                    "id": block.item_id,
                    "type": "function_call",
                    "status": "completed",
                    "call_id": block.call_id.clone().unwrap_or_default(),
                    "name": block.name.clone().unwrap_or_default(),
                    "arguments": block.buffer,
                });
                let item_done_seq = self.next_seq();
                let item_done = json!({
                    "type": "response.output_item.done",
                    "sequence_number": item_done_seq,
                    "output_index": idx,
                    "item": item,
                });
                vec![args_done, item_done]
            }
            BlockKind::Thinking => {
                let summary_done_seq = self.next_seq();
                let summary_done = json!({
                    "type": "response.reasoning_summary_text.done",
                    "sequence_number": summary_done_seq,
                    "item_id": block.item_id,
                    "output_index": idx,
                    "summary_index": 0,
                    "text": block.buffer,
                });
                let item = json!({
                    "id": block.item_id,
                    "type": "reasoning",
                    "status": "completed",
                    "summary": [{"type": "summary_text", "text": block.buffer}],
                });
                let item_done_seq = self.next_seq();
                let item_done = json!({
                    "type": "response.output_item.done",
                    "sequence_number": item_done_seq,
                    "output_index": idx,
                    "item": item,
                });
                vec![summary_done, item_done]
            }
        }
    }

    fn on_message_delta(&mut self, event: &Value) -> Vec<Value> {
        if let Some(sr) = event
            .get("delta")
            .and_then(|d| d.get("stop_reason"))
            .and_then(|v| v.as_str())
        {
            self.stop_reason = Some(sr.to_string());
        }
        if let Some(usage) = event.get("usage") {
            self.merge_usage(usage);
        }
        // Folds into the terminal `response.completed`/`.incomplete` at `message_stop` (SPEC-M4
        // §3.5) -- no immediate client-visible event.
        vec![]
    }

    fn on_message_stop(&mut self, _event: &Value) -> Vec<Value> {
        let status = match self.stop_reason.as_deref() {
            Some("max_tokens") => "incomplete",
            _ => "completed",
        };

        let mut output = Vec::new();
        for idx in &self.order {
            if let Some(block) = self.blocks.get(idx) {
                let item = match block.kind {
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
                };
                output.push(item);
            }
        }

        let usage = self.usage.as_ref().map(map_usage).unwrap_or(Value::Null);

        let mut response = json!({
            "id": self.response_id.clone().unwrap_or_default(),
            "object": "response",
            "status": status,
            "model": self.model.clone().unwrap_or(Value::Null),
            "output": output,
            "usage": usage,
        });
        if status == "incomplete" {
            response["incomplete_details"] = json!({"reason": "max_output_tokens"});
        }

        let event_type = if status == "incomplete" {
            "response.incomplete"
        } else {
            "response.completed"
        };
        let seq = self.next_seq();
        vec![json!({"type": event_type, "sequence_number": seq, "response": response})]
    }

    fn on_error(&mut self, event: &Value) -> Vec<Value> {
        let error = event.get("error").cloned().unwrap_or(Value::Null);
        let code = error
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("api_error");
        let message = error.get("message").and_then(|v| v.as_str()).unwrap_or("");
        let seq = self.next_seq();
        vec![json!({
            "type": "error",
            "sequence_number": seq,
            "code": code,
            "message": message,
        })]
    }
}

/// Mint a fresh synthesized id (`resp_...`, `msg_...`, `fc_...`, `rs_...`) — Anthropic's stream
/// carries none of `response.id`/`item.id`/`call_id` (SPEC-M4 §3.5), so these must be minted.
fn synth_id(prefix: &str) -> String {
    let mut rng = rand::rng();
    let bytes: [u8; 12] = rng.random();
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    format!("{prefix}_{hex}")
}

/// Read the flat Anthropic content-block `index` off a `content_block_start`/`_delta`/`_stop`
/// event.
fn block_index(event: &Value) -> Option<u64> {
    event.get("index").and_then(|v| v.as_u64())
}

/// Map accumulated Anthropic usage to OpenAI-Responses usage (SPEC-M4 §3.5's usage table).
/// `total_tokens` has no Anthropic equivalent and is synthesized as `input + output`.
fn map_usage(anthropic: &Value) -> Value {
    let input_tokens = anthropic
        .get("input_tokens")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let output_tokens = anthropic
        .get("output_tokens")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let cached_tokens = anthropic
        .get("cache_read_input_tokens")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let reasoning_tokens = anthropic
        .get("thinking_tokens")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    json!({
        "input_tokens": input_tokens,
        "input_tokens_details": {"cached_tokens": cached_tokens},
        "output_tokens": output_tokens,
        "output_tokens_details": {"reasoning_tokens": reasoning_tokens},
        "total_tokens": input_tokens + output_tokens,
    })
}

impl Translator for AnthropicToResponses {
    fn translate_request(&mut self, body: Value) -> Value {
        map_request(body)
    }

    fn translate_response_event(&mut self, event: Value) -> Vec<Value> {
        let ty = event.get("type").and_then(|v| v.as_str()).unwrap_or("");
        match ty {
            "message_start" => self.on_message_start(&event),
            "content_block_start" => self.on_content_block_start(&event),
            "content_block_delta" => self.on_content_block_delta(&event),
            "content_block_stop" => self.on_content_block_stop(&event),
            "message_delta" => self.on_message_delta(&event),
            "message_stop" => self.on_message_stop(&event),
            "error" => self.on_error(&event),
            // `ping` (keepalive) and any unrecognized event type: no client-visible mapping.
            _ => vec![],
        }
    }
}

// `blocks` buffers accumulated assistant text / tool-call arguments / extended-thinking content
// per turn and must never be printed in clear via `{:?}` (mirrors `PreparedRequest`/
// `ReasoningItems` in `polyflare-core::types`).
impl std::fmt::Debug for AnthropicToResponses {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnthropicToResponses")
            .field("seq", &self.seq)
            .field("response_id", &self.response_id)
            .field("model", &self.model)
            .field(
                "blocks",
                &format!("[{} block(s) redacted]", self.blocks.len()),
            )
            .field("stop_reason", &self.stop_reason)
            .field("usage", &self.usage)
            .finish()
    }
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
        // Content blocks are transformed, not copied verbatim (T5): a `text` block on a `user`
        // message becomes an `input_text` part inside a Responses `message` input item.
        assert_eq!(
            out["input"],
            json!([{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hi"}]}])
        );
    }

    #[test]
    fn maps_string_content_user_turn_to_single_input_text_part() {
        // Doc-shaped Anthropic request: `content` may be a plain string, not a block array.
        let body = json!({
            "model": "claude-opus-4-1-20250805",
            "messages": [{"role": "user", "content": "hi"}],
            "stream": false
        });
        let out = map_request(body);
        assert_eq!(
            out["input"],
            json!([{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hi"}]}])
        );
    }

    #[test]
    fn maps_string_content_assistant_turn_to_single_output_text_part() {
        let body = json!({
            "model": "claude-opus-4-1-20250805",
            "messages": [{"role": "assistant", "content": "hi there"}],
            "stream": false
        });
        let out = map_request(body);
        assert_eq!(
            out["input"],
            json!([{
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "hi there", "annotations": []}]
            }])
        );
    }

    #[test]
    fn maps_multi_turn_assistant_text_and_tool_use_tool_result_round_trip() {
        // Doc-shaped: user text -> assistant text+tool_use -> user tool_result. Anthropic nests
        // tool_use/tool_result INSIDE a message's content blocks; Responses requires them as
        // TOP-LEVEL function_call/function_call_output items, flattened out of the message.
        let body = json!({
            "model": "claude-opus-4-1-20250805",
            "messages": [
                {"role": "user", "content": [{"type": "text", "text": "What's the weather in SF?"}]},
                {"role": "assistant", "content": [
                    {"type": "text", "text": "Let me check."},
                    {"type": "tool_use", "id": "toolu_01AAA", "name": "get_weather", "input": {"location": "SF"}}
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "toolu_01AAA", "content": "Sunny, 72F"}
                ]}
            ],
            "stream": false
        });
        let out = map_request(body);
        assert_eq!(
            out["input"],
            json!([
                {"type": "message", "role": "user", "content": [
                    {"type": "input_text", "text": "What's the weather in SF?"}
                ]},
                {"type": "message", "role": "assistant", "content": [
                    {"type": "output_text", "text": "Let me check.", "annotations": []}
                ]},
                {"type": "function_call", "call_id": "toolu_01AAA", "name": "get_weather", "arguments": "{\"location\":\"SF\"}"},
                {"type": "function_call_output", "call_id": "toolu_01AAA", "output": "Sunny, 72F"}
            ])
        );
    }

    #[test]
    fn tool_use_only_message_emits_no_empty_message_item() {
        // A message whose only block is tool_use must flatten to *just* the function_call item --
        // no empty {"type":"message", "content":[]} wrapper alongside it.
        let body = json!({
            "model": "claude-opus-4-1-20250805",
            "messages": [
                {"role": "assistant", "content": [
                    {"type": "tool_use", "id": "toolu_1", "name": "noop", "input": {}}
                ]}
            ],
            "stream": false
        });
        let out = map_request(body);
        assert_eq!(
            out["input"],
            json!([{"type": "function_call", "call_id": "toolu_1", "name": "noop", "arguments": "{}"}])
        );
    }

    #[test]
    fn maps_image_block_base64_source_to_input_image_data_url() {
        let body = json!({
            "model": "claude-opus-4-1-20250805",
            "messages": [{"role": "user", "content": [
                {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "AAAA"}}
            ]}],
            "stream": false
        });
        let out = map_request(body);
        assert_eq!(
            out["input"],
            json!([{"type": "message", "role": "user", "content": [
                {"type": "input_image", "image_url": "data:image/png;base64,AAAA", "detail": "auto"}
            ]}])
        );
    }

    #[test]
    fn drops_thinking_blocks_no_reasoning_item_synthesized() {
        // A synthesized `reasoning` item would need a stable `id`/`encrypted_content` the model
        // actually produced (see the updated module doc comment) -- Anthropic's `thinking` block
        // carries neither, so fabricating one would misrepresent state. Dropped, not translated.
        let body = json!({
            "model": "claude-opus-4-1-20250805",
            "messages": [{"role": "assistant", "content": [
                {"type": "thinking", "thinking": "reasoning...", "signature": "sig"},
                {"type": "text", "text": "42"}
            ]}],
            "stream": false
        });
        let out = map_request(body);
        assert_eq!(
            out["input"],
            json!([{"type": "message", "role": "assistant", "content": [
                {"type": "output_text", "text": "42", "annotations": []}
            ]}])
        );
    }

    #[test]
    fn tool_result_with_array_content_maps_text_blocks_to_input_text_output_parts() {
        let body = json!({
            "model": "claude-opus-4-1-20250805",
            "messages": [{"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "toolu_9", "content": [
                    {"type": "text", "text": "Sunny, 72F"}
                ]}
            ]}],
            "stream": false
        });
        let out = map_request(body);
        assert_eq!(
            out["input"],
            json!([{
                "type": "function_call_output",
                "call_id": "toolu_9",
                "output": [{"type": "input_text", "text": "Sunny, 72F"}]
            }])
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
    fn maps_tools_to_responses_flat_function_shape() {
        // Anthropic {name, description, input_schema} -> Responses {type:"function", name,
        // description, parameters} (flat -- no nested "function" wrapper key, verified against
        // the openai-openapi FunctionTool component).
        let tools = json!([
            {
                "name": "get_weather",
                "description": "Get the weather for a location",
                "input_schema": {"type": "object", "properties": {"location": {"type": "string"}}}
            }
        ]);
        let body = json!({
            "model": "claude-opus-4-1-20250805",
            "messages": [],
            "stream": true,
            "tools": tools
        });
        let out = map_request(body);
        assert_eq!(
            out["tools"],
            json!([
                {
                    "type": "function",
                    "name": "get_weather",
                    "description": "Get the weather for a location",
                    "parameters": {"type": "object", "properties": {"location": {"type": "string"}}}
                }
            ])
        );
    }

    #[test]
    fn maps_tool_without_description_omits_description_field() {
        let tools = json!([{"name": "noop", "input_schema": {"type": "object"}}]);
        let body = json!({
            "model": "claude-opus-4-1-20250805",
            "messages": [],
            "stream": true,
            "tools": tools
        });
        let out = map_request(body);
        assert_eq!(
            out["tools"],
            json!([{"type": "function", "name": "noop", "parameters": {"type": "object"}}])
        );
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

    #[test]
    fn message_start_emits_created_then_in_progress_with_synthesized_response_id() {
        let mut t = AnthropicToResponses::new();
        let events = t.translate_response_event(json!({
            "type": "message_start",
            "message": {
                "id": "msg_01XYZ",
                "model": "claude-opus-4-1-20250805",
                "role": "assistant",
                "content": [],
                "usage": {"input_tokens": 25, "output_tokens": 1}
            }
        }));

        assert_eq!(
            events.len(),
            2,
            "message_start must emit exactly 2 events immediately"
        );
        assert_eq!(events[0]["type"], json!("response.created"));
        assert_eq!(events[1]["type"], json!("response.in_progress"));

        let seq0 = events[0]["sequence_number"].as_u64().unwrap();
        let seq1 = events[1]["sequence_number"].as_u64().unwrap();
        assert!(
            seq1 > seq0,
            "sequence_number must be monotonically increasing"
        );

        let resp_id = events[0]["response"]["id"].as_str().unwrap().to_string();
        assert!(!resp_id.is_empty());
        assert_eq!(events[1]["response"]["id"], json!(resp_id));
        assert_eq!(
            events[0]["response"]["model"],
            json!("claude-opus-4-1-20250805")
        );
        assert_eq!(events[0]["response"]["status"], json!("in_progress"));
        assert_eq!(events[0]["response"]["usage"], Value::Null);
    }

    #[test]
    fn content_block_start_text_emits_item_added_then_part_added() {
        let mut t = AnthropicToResponses::new();
        t.translate_response_event(json!({
            "type": "message_start",
            "message": {"id": "msg_1", "model": "claude-opus-4-1-20250805", "usage": {"input_tokens": 10, "output_tokens": 0}}
        }));
        let events = t.translate_response_event(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": ""}
        }));

        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["type"], json!("response.output_item.added"));
        assert_eq!(events[0]["output_index"], json!(0));
        assert_eq!(events[0]["item"]["type"], json!("message"));
        assert_eq!(events[0]["item"]["status"], json!("in_progress"));
        let item_id = events[0]["item"]["id"].as_str().unwrap().to_string();
        assert!(!item_id.is_empty());

        assert_eq!(events[1]["type"], json!("response.content_part.added"));
        assert_eq!(events[1]["item_id"], json!(item_id));
        assert_eq!(events[1]["output_index"], json!(0));
        assert_eq!(events[1]["content_index"], json!(0));
        assert_eq!(events[1]["part"]["type"], json!("output_text"));
    }

    #[test]
    fn content_block_start_tool_use_emits_only_item_added_with_call_id_from_anthropic() {
        let mut t = AnthropicToResponses::new();
        t.translate_response_event(json!({
            "type": "message_start",
            "message": {"id": "msg_2", "model": "claude-opus-4-1-20250805", "usage": {"input_tokens": 10, "output_tokens": 0}}
        }));
        let events = t.translate_response_event(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "tool_use", "id": "toolu_01AAA", "name": "get_weather", "input": {}}
        }));

        assert_eq!(
            events.len(),
            1,
            "tool_use opens no content_part — only output_item.added"
        );
        assert_eq!(events[0]["type"], json!("response.output_item.added"));
        assert_eq!(events[0]["output_index"], json!(0));
        assert_eq!(events[0]["item"]["type"], json!("function_call"));
        assert_eq!(events[0]["item"]["call_id"], json!("toolu_01AAA"));
        assert_eq!(events[0]["item"]["name"], json!("get_weather"));
        assert_eq!(events[0]["item"]["arguments"], json!(""));
    }

    #[test]
    fn content_block_start_thinking_emits_only_item_added_reasoning() {
        let mut t = AnthropicToResponses::new();
        t.translate_response_event(json!({
            "type": "message_start",
            "message": {"id": "msg_3", "model": "claude-opus-4-1-20250805", "usage": {"input_tokens": 10, "output_tokens": 0}}
        }));
        let events = t.translate_response_event(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "thinking", "thinking": "", "signature": ""}
        }));

        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], json!("response.output_item.added"));
        assert_eq!(events[0]["item"]["type"], json!("reasoning"));
        assert_eq!(events[0]["item"]["status"], json!("in_progress"));
    }

    fn started_text_translator() -> AnthropicToResponses {
        let mut t = AnthropicToResponses::new();
        t.translate_response_event(json!({
            "type": "message_start",
            "message": {"id": "msg_1", "model": "claude-opus-4-1-20250805", "usage": {"input_tokens": 10, "output_tokens": 0}}
        }));
        t.translate_response_event(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": ""}
        }));
        t
    }

    #[test]
    fn text_delta_emits_output_text_delta_immediately_per_event() {
        let mut t = started_text_translator();
        let e1 = t.translate_response_event(json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": "Hello"}
        }));
        assert_eq!(e1.len(), 1);
        assert_eq!(e1[0]["type"], json!("response.output_text.delta"));
        assert_eq!(e1[0]["delta"], json!("Hello"));
        assert_eq!(e1[0]["content_index"], json!(0));
        assert_eq!(e1[0]["logprobs"], json!([]));

        let e2 = t.translate_response_event(json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": " world"}
        }));
        assert_eq!(e2.len(), 1);
        assert_eq!(e2[0]["delta"], json!(" world"));
        assert!(
            e2[0]["sequence_number"].as_u64().unwrap() > e1[0]["sequence_number"].as_u64().unwrap()
        );
    }

    #[test]
    fn input_json_delta_emits_function_call_arguments_delta() {
        let mut t = AnthropicToResponses::new();
        t.translate_response_event(json!({
            "type": "message_start",
            "message": {"id": "msg_2", "model": "claude-opus-4-1-20250805", "usage": {"input_tokens": 10, "output_tokens": 0}}
        }));
        t.translate_response_event(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "tool_use", "id": "toolu_01AAA", "name": "get_weather", "input": {}}
        }));
        let events = t.translate_response_event(json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "input_json_delta", "partial_json": "{\"location\":\"SF\"}"}
        }));
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0]["type"],
            json!("response.function_call_arguments.delta")
        );
        assert_eq!(events[0]["delta"], json!("{\"location\":\"SF\"}"));
    }

    #[test]
    fn thinking_delta_emits_reasoning_summary_text_delta_with_summary_index_zero() {
        let mut t = AnthropicToResponses::new();
        t.translate_response_event(json!({
            "type": "message_start",
            "message": {"id": "msg_3", "model": "claude-opus-4-1-20250805", "usage": {"input_tokens": 10, "output_tokens": 0}}
        }));
        t.translate_response_event(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "thinking", "thinking": "", "signature": ""}
        }));
        let events = t.translate_response_event(json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "thinking_delta", "thinking": "Let me think..."}
        }));
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0]["type"],
            json!("response.reasoning_summary_text.delta")
        );
        assert_eq!(events[0]["summary_index"], json!(0));
        assert_eq!(events[0]["delta"], json!("Let me think..."));
    }

    #[test]
    fn signature_delta_emits_nothing() {
        let mut t = AnthropicToResponses::new();
        t.translate_response_event(json!({
            "type": "message_start",
            "message": {"id": "msg_4", "model": "claude-opus-4-1-20250805", "usage": {"input_tokens": 10, "output_tokens": 0}}
        }));
        t.translate_response_event(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "thinking", "thinking": "", "signature": ""}
        }));
        let events = t.translate_response_event(json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "signature_delta", "signature": "abc123sig"}
        }));
        assert_eq!(
            events,
            Vec::<Value>::new(),
            "signature_delta is one-to-zero"
        );
    }

    #[test]
    fn content_block_stop_text_emits_done_triad_with_full_accumulated_text() {
        let mut t = started_text_translator();
        t.translate_response_event(json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": "Hello"}
        }));
        t.translate_response_event(json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": " world"}
        }));
        let events = t.translate_response_event(json!({"type": "content_block_stop", "index": 0}));

        assert_eq!(events.len(), 3);
        assert_eq!(events[0]["type"], json!("response.output_text.done"));
        assert_eq!(events[0]["text"], json!("Hello world"));
        assert_eq!(events[1]["type"], json!("response.content_part.done"));
        assert_eq!(events[1]["part"]["text"], json!("Hello world"));
        assert_eq!(events[2]["type"], json!("response.output_item.done"));
        assert_eq!(events[2]["item"]["status"], json!("completed"));
        assert_eq!(
            events[2]["item"]["content"][0]["text"],
            json!("Hello world")
        );
    }

    #[test]
    fn content_block_stop_tool_use_emits_only_two_done_events() {
        let mut t = AnthropicToResponses::new();
        t.translate_response_event(json!({
            "type": "message_start",
            "message": {"id": "msg_2", "model": "claude-opus-4-1-20250805", "usage": {"input_tokens": 10, "output_tokens": 0}}
        }));
        t.translate_response_event(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "tool_use", "id": "toolu_01AAA", "name": "get_weather", "input": {}}
        }));
        t.translate_response_event(json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "input_json_delta", "partial_json": "{\"location\":\"SF\"}"}
        }));
        let events = t.translate_response_event(json!({"type": "content_block_stop", "index": 0}));

        assert_eq!(events.len(), 2, "tool_use has no content_part to close");
        assert_eq!(
            events[0]["type"],
            json!("response.function_call_arguments.done")
        );
        assert_eq!(events[0]["arguments"], json!("{\"location\":\"SF\"}"));
        assert_eq!(events[1]["type"], json!("response.output_item.done"));
        assert_eq!(events[1]["item"]["call_id"], json!("toolu_01AAA"));
        assert_eq!(
            events[1]["item"]["arguments"],
            json!("{\"location\":\"SF\"}")
        );
    }

    #[test]
    fn content_block_stop_thinking_emits_only_two_done_events() {
        let mut t = AnthropicToResponses::new();
        t.translate_response_event(json!({
            "type": "message_start",
            "message": {"id": "msg_3", "model": "claude-opus-4-1-20250805", "usage": {"input_tokens": 10, "output_tokens": 0}}
        }));
        t.translate_response_event(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "thinking", "thinking": "", "signature": ""}
        }));
        t.translate_response_event(json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "thinking_delta", "thinking": "Let me think..."}
        }));
        let events = t.translate_response_event(json!({"type": "content_block_stop", "index": 0}));

        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0]["type"],
            json!("response.reasoning_summary_text.done")
        );
        assert_eq!(events[0]["text"], json!("Let me think..."));
        assert_eq!(events[1]["type"], json!("response.output_item.done"));
        assert_eq!(
            events[1]["item"]["summary"][0]["text"],
            json!("Let me think...")
        );
    }

    #[test]
    fn message_delta_emits_nothing_but_buffers_stop_reason_and_usage() {
        let mut t = started_text_translator();
        let events = t.translate_response_event(json!({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn", "stop_sequence": null},
            "usage": {"output_tokens": 8}
        }));
        assert_eq!(
            events,
            Vec::<Value>::new(),
            "message_delta folds into the terminal event only"
        );
    }

    #[test]
    fn message_stop_emits_completed_with_merged_usage_and_assembled_output() {
        let mut t = started_text_translator();
        t.translate_response_event(json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": "42"}
        }));
        t.translate_response_event(json!({"type": "content_block_stop", "index": 0}));
        t.translate_response_event(json!({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn"},
            "usage": {"output_tokens": 8}
        }));
        let events = t.translate_response_event(json!({"type": "message_stop"}));

        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], json!("response.completed"));
        let response = &events[0]["response"];
        assert_eq!(response["status"], json!("completed"));
        // input_tokens came from message_start (10), never overwritten by message_delta's
        // output_tokens-only usage object -- proves the merge strategy (gap 6).
        assert_eq!(response["usage"]["input_tokens"], json!(10));
        assert_eq!(response["usage"]["output_tokens"], json!(8));
        assert_eq!(response["usage"]["total_tokens"], json!(18));
        assert_eq!(response["output"][0]["type"], json!("message"));
        assert_eq!(response["output"][0]["content"][0]["text"], json!("42"));
    }

    #[test]
    fn message_stop_maps_max_tokens_to_incomplete() {
        let mut t = started_text_translator();
        t.translate_response_event(json!({"type": "content_block_stop", "index": 0}));
        t.translate_response_event(json!({
            "type": "message_delta",
            "delta": {"stop_reason": "max_tokens"},
            "usage": {"output_tokens": 5}
        }));
        let events = t.translate_response_event(json!({"type": "message_stop"}));

        assert_eq!(events[0]["type"], json!("response.incomplete"));
        assert_eq!(events[0]["response"]["status"], json!("incomplete"));
        assert_eq!(
            events[0]["response"]["incomplete_details"]["reason"],
            json!("max_output_tokens")
        );
    }

    #[test]
    fn usage_maps_cache_read_and_thinking_tokens() {
        let mut t = started_text_translator();
        t.translate_response_event(json!({"type": "content_block_stop", "index": 0}));
        t.translate_response_event(json!({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn"},
            "usage": {"output_tokens": 15, "cache_read_input_tokens": 5, "thinking_tokens": 3}
        }));
        let events = t.translate_response_event(json!({"type": "message_stop"}));
        let usage = &events[0]["response"]["usage"];
        assert_eq!(usage["input_tokens_details"]["cached_tokens"], json!(5));
        assert_eq!(usage["output_tokens_details"]["reasoning_tokens"], json!(3));
    }

    #[test]
    fn mid_stream_error_passes_through_type_as_code() {
        let mut t = AnthropicToResponses::new();
        let events = t.translate_response_event(json!({
            "type": "error",
            "error": {"type": "overloaded_error", "message": "Overloaded"}
        }));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], json!("error"));
        assert_eq!(events[0]["code"], json!("overloaded_error"));
        assert_eq!(events[0]["message"], json!("Overloaded"));
    }

    #[test]
    fn ping_emits_nothing() {
        let mut t = AnthropicToResponses::new();
        let events = t.translate_response_event(json!({"type": "ping"}));
        assert_eq!(events, Vec::<Value>::new());
    }

    #[test]
    fn debug_redacts_accumulated_block_text() {
        let mut t = AnthropicToResponses::new();
        t.translate_response_event(json!({
            "type": "message_start",
            "message": {"id": "msg_1", "model": "claude-opus-4-1-20250805", "usage": {"input_tokens": 1, "output_tokens": 1}}
        }));
        t.translate_response_event(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": ""}
        }));
        t.translate_response_event(json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": "super-secret-user-conversation"}
        }));

        let s = format!("{t:?}");
        assert!(
            !s.contains("super-secret-user-conversation"),
            "Debug must never leak accumulated block text: {s}"
        );
        assert!(
            s.contains("redacted"),
            "Debug should mark blocks redacted: {s}"
        );
    }
}
