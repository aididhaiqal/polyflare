//! The M4b headline cross-format translator: Anthropic-Messages client <-> OpenAI-Responses
//! (Codex) backend (SPEC-M4 §3.4's stateful 1->N seam). `AnthropicToResponses` implements BOTH
//! `Translator` methods for this one client<->backend pairing, and each method runs the opposite
//! direction across the wire:
//!   - `translate_request`: Anthropic-Messages -> OpenAI-Responses (client format -> backend
//!     format). `map_request` (below) does the doc-verified *mechanical* request-body field
//!     mapping (SPEC-M4 §3.6's "mechanical direction") -- model-alias remap and reasoning-effort
//!     payload-override are explicitly deferred (SPEC-M4 U2, M4b-wiring), so `model` passes
//!     through unchanged here.
//!   - `translate_response_event`: OpenAI-Responses SSE -> Anthropic-Messages SSE (backend's
//!     reply -> client format, SPEC-M4 §3.5 inverted). The Codex backend replies in
//!     OpenAI-Responses shape; the Claude client expects Anthropic-Messages SSE. This is the
//!     stateful streaming response-event translator: it holds per-turn state (whether
//!     `message_start` has fired yet, a monotonic Anthropic content-block-index counter, and a
//!     small per-OpenAI-item index/stopped map keyed by OpenAI's `item_id`) and turns each
//!     incoming OpenAI event into zero, one, or two outgoing Anthropic events, non-buffering
//!     (each event's outputs are emitted as soon as it arrives -- the *only* thing held across
//!     calls is this small bookkeeping state, never accumulated response text).
//!
//! (An earlier revision of this file had `translate_response_event` built backwards -- mapping
//! Anthropic Messages SSE -> OpenAI-Responses SSE, which is actually the *inverse* M4c direction
//! (OpenAI client -> Anthropic backend), T2-deferred. That logic is not this struct's job; it
//! remains available in git history if M4c is picked up later.)

use std::collections::HashMap;

use polyflare_core::Translator;
use rand::Rng;
use serde_json::{json, Value};

/// Map an Anthropic Messages request body to an OpenAI-Responses request body.
///
/// **Envelope** (top-level fields): `model` passthrough (alias remap happens later, in the core
/// outgoing rewrite — SPEC-M4 U2), `system`→`instructions`. The Codex `backend-api/responses`
/// contract is enforced here (verified live, not the generic OpenAI schema): `store:false` +
/// `stream:true` are always set, and the client's `max_tokens` is dropped (Codex rejects
/// `max_output_tokens`). See `map_request`'s body for the exact upstream-400 messages that pin each.
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
/// ⚠️ **Partial live validation (U4).** The request ENVELOPE is now verified end-to-end against the
/// real Codex backend (2026-07-22): the `store:false`/`stream:true`/no-`max_output_tokens` contract,
/// plus a text turn, a system prompt, multi-turn history, and a tool call all round-trip 200 through
/// the aliased `/v1/messages`→Codex path. The following per-block *shape* choices were still resolved
/// against the *documented* (ambiguous/inconsistent) schema and remain unconfirmed at that level:
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
/// The response-side translator (`AnthropicToResponses`, §3.5 inverted) is a separate concern from
/// this module doc's scope — see the file-level doc comment above.
fn map_request(body: Value) -> Value {
    let model = body.get("model").cloned().unwrap_or(Value::Null);
    let system = body.get("system").cloned();
    let messages = body
        .get("messages")
        .cloned()
        .unwrap_or_else(|| Value::Array(vec![]));
    let tools = body.get("tools").cloned();

    // The Codex `backend-api/responses` contract (verified LIVE against the real backend, 2026-07-22
    // — NOT the generic OpenAI Responses schema this mapper was first built to). The backend hard-
    // rejects a non-conforming body with a `400 {"detail": "..."}`:
    //   - `store` MUST be `false`  ("Store must be set to false"),
    //   - `stream` MUST be `true`  ("Stream must be set to true"),
    //   - `max_output_tokens` is UNSUPPORTED ("Unsupported parameter: max_output_tokens").
    // So the client's `stream`/`max_tokens` are deliberately NOT forwarded: PolyFlare always streams
    // upstream (and only ever speaks SSE to the client — see `ingress`), and the Anthropic token cap
    // has no Codex equivalent, so it is dropped rather than sent as the rejected `max_output_tokens`.
    let mut out = json!({
        "model": model,
        "input": map_messages(&messages),
        "store": false,
        "stream": true,
    });
    let map = out.as_object_mut().expect("json! object literal");
    if let Some(sys) = system {
        map.insert("instructions".to_string(), sys);
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

/// Per-OpenAI-item per-turn state for the response-event direction: the Anthropic flat
/// content-block `index` assigned when this item's block opened, and whether a
/// `content_block_stop` has already been emitted for it.
///
/// Unlike the (deferred, inverse) Anthropic→OpenAI direction, this state buffers **no response
/// content**: Anthropic's `content_block_stop` carries no accumulated text (the deltas already
/// delivered it — see `on_block_done` below), so there is nothing to reassemble at block-close
/// time, only a small index/stopped bookkeeping map.
#[derive(Clone, Copy, Debug)]
struct BlockState {
    index: u64,
    stopped: bool,
}

/// Stateful per-turn OpenAI-Responses→Anthropic-Messages response-event translator (SPEC-M4
/// §3.4/§3.5, inverted — see the file-level doc comment). Construct a fresh instance per turn via
/// `AnthropicToResponses::new()` — never reuse one across requests.
#[derive(Default)]
pub struct AnthropicToResponses {
    /// Anthropic's `message_start` must be emitted exactly once per turn, on the first
    /// `response.created`/`response.in_progress` seen (SPEC-M4 §3.5 inverted: OpenAI sends both
    /// back-to-back with the same response snapshot; Anthropic has only one start event).
    message_start_emitted: bool,
    /// Anthropic needs its own flat content-block index, synthesized as blocks open (a
    /// monotonic counter) — OpenAI's `output_index`/`content_index`/`summary_index` have no
    /// Anthropic equivalent and are dropped, but we still need *some* index, assigned in the
    /// order each OpenAI item actually opens its Anthropic-visible block.
    next_block_index: u64,
    /// Keyed by the OpenAI item id (`item.id` on `output_item.added`/`.done`, `item_id` on the
    /// delta/part/done family). Used to look up which Anthropic index a later delta/stop event
    /// belongs to, and to de-duplicate the several OpenAI "done" sub-events (`output_text.done`,
    /// `content_part.done`, `function_call_arguments.done`, `reasoning_summary_text.done`,
    /// `output_item.done`) that all collapse into a single Anthropic `content_block_stop`.
    blocks: HashMap<String, BlockState>,
}

impl AnthropicToResponses {
    pub fn new() -> Self {
        Self::default()
    }

    /// Assign the next Anthropic block index to `item_id` and record it as open (not yet
    /// stopped). Returns the assigned index.
    fn open_block(&mut self, item_id: String) -> u64 {
        let index = self.next_block_index;
        self.next_block_index += 1;
        self.blocks.insert(
            item_id,
            BlockState {
                index,
                stopped: false,
            },
        );
        index
    }

    /// `response.created` + `response.in_progress` → `message_start`, emitted only once (SPEC-M4
    /// §3.5 inverted). OpenAI's `response.usage` is `null` at this stage in practice (usage is
    /// only known at `response.completed`), so the synthesized `message_start.message.usage`
    /// defaults to zeros when absent — a genuine architecture mismatch (Anthropic's
    /// `message_start` is documented to carry real `input_tokens` up front; OpenAI has none yet
    /// at this point in the stream). Flagged for U4 live-capture confirmation.
    fn on_response_started(&mut self, event: &Value) -> Vec<Value> {
        if self.message_start_emitted {
            return vec![];
        }
        self.message_start_emitted = true;

        let response = event.get("response").cloned().unwrap_or(Value::Null);
        let model = response.get("model").cloned().unwrap_or(Value::Null);
        let usage = match response.get("usage") {
            Some(u) if !u.is_null() => map_usage_from_openai(u),
            _ => json!({"input_tokens": 0, "output_tokens": 0}),
        };

        vec![json!({
            "type": "message_start",
            "message": {
                "id": synth_id("msg"),
                "type": "message",
                "role": "assistant",
                "model": model,
                "content": [],
                "usage": usage,
            },
        })]
    }

    /// `response.output_item.added`: `function_call` and `reasoning` items open their Anthropic
    /// block (and get their index) immediately, mirroring how they get no separate "part added"
    /// event on the OpenAI side. A `message` item opens no Anthropic-visible event yet — its block
    /// (and index) opens at the paired `response.content_part.added`, which always follows in the
    /// real event order and carries the `item_id` needed to correlate them.
    fn on_output_item_added(&mut self, event: &Value) -> Vec<Value> {
        let item = event.get("item").cloned().unwrap_or(Value::Null);
        let item_type = item.get("type").and_then(|v| v.as_str()).unwrap_or("");
        let item_id = item
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        match item_type {
            "function_call" => {
                let call_id = item
                    .get("call_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let name = item
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let index = self.open_block(item_id);
                vec![json!({
                    "type": "content_block_start",
                    "index": index,
                    "content_block": {"type": "tool_use", "id": call_id, "name": name, "input": {}},
                })]
            }
            "reasoning" => {
                let index = self.open_block(item_id);
                vec![json!({
                    "type": "content_block_start",
                    "index": index,
                    "content_block": {"type": "thinking", "thinking": ""},
                })]
            }
            // "message" (deferred to content_part.added) and any unrecognized item type: no
            // Anthropic-visible event yet.
            _ => vec![],
        }
    }

    /// `response.content_part.added` (`output_text` parts only) → opens the Anthropic text
    /// block: `content_block_start {index, content_block:{type:"text", text:""}}`.
    fn on_content_part_added(&mut self, event: &Value) -> Vec<Value> {
        let part = event.get("part").cloned().unwrap_or(Value::Null);
        if part.get("type").and_then(|v| v.as_str()) != Some("output_text") {
            return vec![];
        }
        let item_id = event
            .get("item_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if self.blocks.contains_key(&item_id) {
            // Defensive: a second content_part.added for an already-open item must not reopen
            // (and re-index) the block.
            return vec![];
        }
        let index = self.open_block(item_id);
        vec![json!({
            "type": "content_block_start",
            "index": index,
            "content_block": {"type": "text", "text": ""},
        })]
    }

    /// `response.output_text.delta` → `content_block_delta {index, delta:{type:"text_delta",
    /// text}}` (1:1, immediate — no buffering).
    fn on_output_text_delta(&mut self, event: &Value) -> Vec<Value> {
        let Some(block) = self.lookup_flat_item(event) else {
            return vec![];
        };
        let text = event
            .get("delta")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        vec![json!({
            "type": "content_block_delta",
            "index": block.index,
            "delta": {"type": "text_delta", "text": text},
        })]
    }

    /// `response.function_call_arguments.delta` → `content_block_delta
    /// {index, delta:{type:"input_json_delta", partial_json}}` (1:1, `partial_json` passed
    /// through as-is).
    fn on_function_call_arguments_delta(&mut self, event: &Value) -> Vec<Value> {
        let Some(block) = self.lookup_flat_item(event) else {
            return vec![];
        };
        let partial_json = event
            .get("delta")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        vec![json!({
            "type": "content_block_delta",
            "index": block.index,
            "delta": {"type": "input_json_delta", "partial_json": partial_json},
        })]
    }

    /// `response.reasoning_summary_text.delta` (or the raw `response.reasoning_text.delta` —
    /// SPEC-M4 §3.5/§7 flags the exact target event as VERIFY-gated; both are treated as
    /// equivalent input here since it's genuinely unconfirmed which one a live Codex backend
    /// sends) → `content_block_delta {index, delta:{type:"thinking_delta", thinking}}`.
    fn on_reasoning_delta(&mut self, event: &Value) -> Vec<Value> {
        let Some(block) = self.lookup_flat_item(event) else {
            return vec![];
        };
        let thinking = event
            .get("delta")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        vec![json!({
            "type": "content_block_delta",
            "index": block.index,
            "delta": {"type": "thinking_delta", "thinking": thinking},
        })]
    }

    /// The OpenAI "done" family for one item — `response.output_text.done`,
    /// `.function_call_arguments.done`, `.reasoning_summary_text.done`/`.reasoning_text.done`,
    /// `.content_part.done`, `.output_item.done` — all collapse into a **single** Anthropic
    /// `content_block_stop {index}` (SPEC-M4 §3.5 inverted: Anthropic's stop carries no
    /// accumulated text, so there is nothing to differentiate between these sub-events other than
    /// "this block is done"). Whichever done-family event for a given item arrives first triggers
    /// the stop; the `stopped` flag guards every subsequent one for the same item from re-firing.
    fn on_block_done(&mut self, event: &Value) -> Vec<Value> {
        let item_id =
            if event.get("type").and_then(|v| v.as_str()) == Some("response.output_item.done") {
                event
                    .get("item")
                    .and_then(|i| i.get("id"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string()
            } else {
                event
                    .get("item_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string()
            };

        let Some(block) = self.blocks.get_mut(&item_id) else {
            return vec![];
        };
        if block.stopped {
            return vec![];
        }
        block.stopped = true;
        let index = block.index;
        vec![json!({"type": "content_block_stop", "index": index})]
    }

    /// `response.completed` (or `.incomplete`) → Anthropic `message_delta` (folding in
    /// `stop_reason` + cumulative usage) followed by `message_stop` (SPEC-M4 §3.5 inverted).
    /// `status`→`stop_reason`: `completed`→`end_turn`, `incomplete`→`max_tokens`; every other/
    /// unrecognized status defaults to `end_turn` — no canonical table exists either direction
    /// (mirrors the forward direction's same fallback-style simplification, e.g. there is no
    /// OpenAI status signal distinguishing a tool-call-ending turn for an Anthropic `tool_use`
    /// stop_reason); flagged for U4 live-capture confirmation.
    fn on_response_completed(&mut self, event: &Value) -> Vec<Value> {
        let response = event.get("response").cloned().unwrap_or(Value::Null);
        let status = response
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("completed");
        let stop_reason = match status {
            "incomplete" => "max_tokens",
            _ => "end_turn",
        };
        let usage = response
            .get("usage")
            .map(map_usage_from_openai)
            .unwrap_or_else(|| json!({"output_tokens": 0}));

        vec![
            json!({
                "type": "message_delta",
                "delta": {"stop_reason": stop_reason, "stop_sequence": Value::Null},
                "usage": usage,
            }),
            json!({"type": "message_stop"}),
        ]
    }

    /// `response.failed` → Anthropic `error` event, reading the nested `response.error` object.
    fn on_response_failed(&mut self, event: &Value) -> Vec<Value> {
        let error = event
            .get("response")
            .and_then(|r| r.get("error"))
            .cloned()
            .unwrap_or(Value::Null);
        build_error_event(&error)
    }

    /// A bare mid-stream `error` event (flat `{"type":"error","code":..,"message":..}`, no nested
    /// `error` object — distinct from `response.failed`'s shape) → Anthropic `error` event.
    fn on_error(&mut self, event: &Value) -> Vec<Value> {
        build_error_event(event)
    }

    /// Look up per-item state off an event's flat `item_id` field (used by every delta/done event
    /// EXCEPT `response.output_item.done`, which nests the id under `item.id` — see
    /// `on_block_done`).
    fn lookup_flat_item(&self, event: &Value) -> Option<BlockState> {
        let item_id = event.get("item_id").and_then(|v| v.as_str())?;
        self.blocks.get(item_id).copied()
    }
}

/// Mint a fresh synthesized Anthropic message id (`msg_...`) — OpenAI's `response.id` uses a
/// different prefix convention (`resp_...`) and Anthropic clients expect their own.
fn synth_id(prefix: &str) -> String {
    let mut rng = rand::rng();
    let bytes: [u8; 12] = rng.random();
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    format!("{prefix}_{hex}")
}

/// Build an Anthropic `error` event from an OpenAI-shaped error object (whether nested under
/// `response.error` or a bare mid-stream `error` event). `code`/`type`→ Anthropic `error.type`: no
/// canonical mapping exists in either direction's doc (SPEC-M4 §3.5/§7 flags this the other way
/// too), so the OpenAI code/type string passes through verbatim.
fn build_error_event(error: &Value) -> Vec<Value> {
    let error_type = error
        .get("code")
        .or_else(|| error.get("type"))
        .and_then(|v| v.as_str())
        .unwrap_or("api_error");
    let message = error.get("message").and_then(|v| v.as_str()).unwrap_or("");
    vec![json!({
        "type": "error",
        "error": {"type": error_type, "message": message},
    })]
}

/// Map OpenAI-Responses cumulative usage to Anthropic usage shape (SPEC-M4 §3.5's usage table,
/// inverted). `total_tokens` has no Anthropic-side equivalent and is dropped (Anthropic's `usage`
/// object never reports a total); Anthropic's `cache_creation_input_tokens` has no OpenAI-side
/// source and is never populated (lossy, the same gap SPEC-M4 documents the other way).
fn map_usage_from_openai(openai: &Value) -> Value {
    let output_tokens = openai
        .get("output_tokens")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let mut usage = json!({"output_tokens": output_tokens});
    let obj = usage.as_object_mut().expect("json! object literal");
    if let Some(input_tokens) = openai.get("input_tokens").and_then(|v| v.as_i64()) {
        obj.insert("input_tokens".to_string(), json!(input_tokens));
    }
    if let Some(cached) = openai
        .get("input_tokens_details")
        .and_then(|d| d.get("cached_tokens"))
        .and_then(|v| v.as_i64())
    {
        obj.insert("cache_read_input_tokens".to_string(), json!(cached));
    }
    if let Some(reasoning) = openai
        .get("output_tokens_details")
        .and_then(|d| d.get("reasoning_tokens"))
        .and_then(|v| v.as_i64())
    {
        obj.insert("thinking_tokens".to_string(), json!(reasoning));
    }
    usage
}

impl Translator for AnthropicToResponses {
    fn translate_request(&mut self, body: Value) -> Value {
        map_request(body)
    }

    fn translate_response_event(&mut self, event: Value) -> Vec<Value> {
        let ty = event.get("type").and_then(|v| v.as_str()).unwrap_or("");
        match ty {
            "response.created" | "response.in_progress" => self.on_response_started(&event),
            "response.output_item.added" => self.on_output_item_added(&event),
            "response.content_part.added" => self.on_content_part_added(&event),
            "response.output_text.delta" => self.on_output_text_delta(&event),
            "response.function_call_arguments.delta" => {
                self.on_function_call_arguments_delta(&event)
            }
            "response.reasoning_summary_text.delta" | "response.reasoning_text.delta" => {
                self.on_reasoning_delta(&event)
            }
            "response.output_text.done"
            | "response.function_call_arguments.done"
            | "response.reasoning_summary_text.done"
            | "response.reasoning_text.done"
            | "response.content_part.done"
            | "response.output_item.done" => self.on_block_done(&event),
            "response.completed" | "response.incomplete" => self.on_response_completed(&event),
            "response.failed" => self.on_response_failed(&event),
            "error" => self.on_error(&event),
            // `ping` (keepalive) and any unrecognized event type: no client-visible mapping.
            _ => vec![],
        }
    }
}

// `blocks` holds no response content in this direction (Anthropic's content_block_stop needs no
// accumulated buffer — see `BlockState`'s doc comment), but the Debug impl stays manual and
// redacting anyway: it documents the invariant explicitly (mirrors `PreparedRequest`/
// `ReasoningItems` in `polyflare-core::types`) so that any future field holding streamed text
// must be a conscious, redacted addition rather than an accidental `#[derive(Debug)]` leak.
impl std::fmt::Debug for AnthropicToResponses {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnthropicToResponses")
            .field("message_start_emitted", &self.message_start_emitted)
            .field("next_block_index", &self.next_block_index)
            .field(
                "blocks",
                &format!("[{} block(s) redacted]", self.blocks.len()),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enforces_codex_contract_store_false_stream_true_no_max_output_tokens() {
        // The Codex backend-api/responses contract (live-verified): a translated body ALWAYS carries
        // `store:false` + `stream:true`, and NEVER `max_output_tokens` — even when the client sent a
        // `max_tokens`, since Codex hard-rejects that parameter. `model` passes through (aliased later).
        let body = json!({
            "model": "claude-opus-4-1-20250805",
            "messages": [{"role": "user", "content": [{"type": "text", "text": "hi"}]}],
            "stream": false,
            "max_tokens": 1024
        });
        let out = map_request(body);
        assert_eq!(out["model"], json!("claude-opus-4-1-20250805"));
        assert_eq!(out["store"], json!(false));
        assert_eq!(out["stream"], json!(true), "stream is forced true regardless of client");
        assert!(
            out.get("max_output_tokens").is_none(),
            "max_output_tokens must never be sent (Codex rejects it)"
        );
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
    fn always_streams_upstream_even_when_client_omits_stream() {
        // Codex requires `stream:true`; PolyFlare only ever speaks SSE to the client anyway, so the
        // client's stream preference (here: absent) never suppresses upstream streaming.
        let body = json!({"model": "claude-opus-4-1-20250805", "messages": []});
        let out = map_request(body);
        assert_eq!(out["stream"], json!(true));
        assert_eq!(out["store"], json!(false));
    }

    #[test]
    fn does_not_remap_model_alias() {
        // SPEC-M4 U2: the exact opus/sonnet/haiku -> sol/terra/luna pairs are pending user
        // confirmation. `map_request` must never guess at a remap.
        let body = json!({"model": "claude-opus-4-1-20250805", "messages": []});
        let out = map_request(body);
        assert_eq!(out["model"], json!("claude-opus-4-1-20250805"));
    }

    // ---- translate_response_event: OpenAI-Responses SSE -> Anthropic-Messages SSE ----

    fn response_created(model: &str) -> Value {
        json!({
            "type": "response.created",
            "response": {"id": "resp_1", "object": "response", "status": "in_progress", "model": model, "output": [], "usage": Value::Null}
        })
    }

    #[test]
    fn response_created_emits_message_start_once() {
        let mut t = AnthropicToResponses::new();
        let events = t.translate_response_event(response_created("claude-opus-4-1-20250805"));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], json!("message_start"));
        assert_eq!(events[0]["message"]["type"], json!("message"));
        assert_eq!(events[0]["message"]["role"], json!("assistant"));
        assert_eq!(
            events[0]["message"]["model"],
            json!("claude-opus-4-1-20250805")
        );
        assert_eq!(events[0]["message"]["content"], json!([]));
        assert!(!events[0]["message"]["id"].as_str().unwrap().is_empty());
        // No sequence_number, no `response` wrapper -- Anthropic events carry neither.
        assert!(events[0].get("sequence_number").is_none());

        // response.in_progress with the same snapshot must NOT re-emit message_start.
        let events2 = t.translate_response_event(json!({
            "type": "response.in_progress",
            "response": {"id": "resp_1", "status": "in_progress", "model": "claude-opus-4-1-20250805", "usage": Value::Null}
        }));
        assert_eq!(events2, Vec::<Value>::new());
    }

    fn started_text_translator() -> AnthropicToResponses {
        let mut t = AnthropicToResponses::new();
        t.translate_response_event(response_created("claude-opus-4-1-20250805"));
        t.translate_response_event(json!({
            "type": "response.output_item.added",
            "output_index": 0,
            "item": {"id": "item_1", "type": "message", "status": "in_progress", "role": "assistant", "content": []}
        }));
        t.translate_response_event(json!({
            "type": "response.content_part.added",
            "item_id": "item_1",
            "output_index": 0,
            "content_index": 0,
            "part": {"type": "output_text", "text": "", "annotations": []}
        }));
        t
    }

    #[test]
    fn output_item_added_message_emits_nothing_content_part_added_opens_text_block() {
        let mut t = AnthropicToResponses::new();
        t.translate_response_event(response_created("claude-opus-4-1-20250805"));
        let item_added = t.translate_response_event(json!({
            "type": "response.output_item.added",
            "output_index": 0,
            "item": {"id": "item_1", "type": "message", "status": "in_progress", "role": "assistant", "content": []}
        }));
        assert_eq!(
            item_added,
            Vec::<Value>::new(),
            "message items open no Anthropic event until content_part.added"
        );

        let events = t.translate_response_event(json!({
            "type": "response.content_part.added",
            "item_id": "item_1",
            "output_index": 0,
            "content_index": 0,
            "part": {"type": "output_text", "text": "", "annotations": []}
        }));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], json!("content_block_start"));
        assert_eq!(events[0]["index"], json!(0));
        assert_eq!(events[0]["content_block"]["type"], json!("text"));
        assert_eq!(events[0]["content_block"]["text"], json!(""));
    }

    #[test]
    fn output_item_added_function_call_opens_tool_use_block_immediately() {
        let mut t = AnthropicToResponses::new();
        t.translate_response_event(response_created("claude-opus-4-1-20250805"));
        let events = t.translate_response_event(json!({
            "type": "response.output_item.added",
            "output_index": 0,
            "item": {"id": "item_fc", "type": "function_call", "status": "in_progress", "call_id": "call_abc123", "name": "get_weather", "arguments": ""}
        }));
        assert_eq!(
            events.len(),
            1,
            "tool_use opens no content_part — only content_block_start"
        );
        assert_eq!(events[0]["type"], json!("content_block_start"));
        assert_eq!(events[0]["index"], json!(0));
        assert_eq!(events[0]["content_block"]["type"], json!("tool_use"));
        assert_eq!(events[0]["content_block"]["id"], json!("call_abc123"));
        assert_eq!(events[0]["content_block"]["name"], json!("get_weather"));
        assert_eq!(events[0]["content_block"]["input"], json!({}));
    }

    #[test]
    fn output_item_added_reasoning_opens_thinking_block_immediately() {
        let mut t = AnthropicToResponses::new();
        t.translate_response_event(response_created("claude-opus-4-1-20250805"));
        let events = t.translate_response_event(json!({
            "type": "response.output_item.added",
            "output_index": 0,
            "item": {"id": "item_r", "type": "reasoning", "status": "in_progress", "summary": []}
        }));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], json!("content_block_start"));
        assert_eq!(events[0]["content_block"]["type"], json!("thinking"));
        assert_eq!(events[0]["content_block"]["thinking"], json!(""));
    }

    #[test]
    fn output_text_delta_emits_content_block_delta_immediately_per_event() {
        let mut t = started_text_translator();
        let e1 = t.translate_response_event(json!({
            "type": "response.output_text.delta",
            "item_id": "item_1",
            "output_index": 0,
            "content_index": 0,
            "delta": "Hello",
            "logprobs": []
        }));
        assert_eq!(e1.len(), 1);
        assert_eq!(e1[0]["type"], json!("content_block_delta"));
        assert_eq!(e1[0]["index"], json!(0));
        assert_eq!(e1[0]["delta"]["type"], json!("text_delta"));
        assert_eq!(e1[0]["delta"]["text"], json!("Hello"));
        // OpenAI's item_id/output_index/content_index/logprobs have no Anthropic slot.
        assert!(e1[0].get("item_id").is_none());
        assert!(e1[0].get("logprobs").is_none());

        let e2 = t.translate_response_event(json!({
            "type": "response.output_text.delta",
            "item_id": "item_1",
            "delta": " world"
        }));
        assert_eq!(e2[0]["delta"]["text"], json!(" world"));
    }

    #[test]
    fn function_call_arguments_delta_emits_input_json_delta() {
        let mut t = AnthropicToResponses::new();
        t.translate_response_event(response_created("claude-opus-4-1-20250805"));
        t.translate_response_event(json!({
            "type": "response.output_item.added",
            "item": {"id": "item_fc", "type": "function_call", "call_id": "call_abc123", "name": "get_weather", "arguments": ""}
        }));
        let events = t.translate_response_event(json!({
            "type": "response.function_call_arguments.delta",
            "item_id": "item_fc",
            "delta": "{\"location\":\"SF\"}"
        }));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["delta"]["type"], json!("input_json_delta"));
        assert_eq!(
            events[0]["delta"]["partial_json"],
            json!("{\"location\":\"SF\"}")
        );
    }

    #[test]
    fn reasoning_summary_text_delta_emits_thinking_delta() {
        let mut t = AnthropicToResponses::new();
        t.translate_response_event(response_created("claude-opus-4-1-20250805"));
        t.translate_response_event(json!({
            "type": "response.output_item.added",
            "item": {"id": "item_r", "type": "reasoning", "summary": []}
        }));
        let events = t.translate_response_event(json!({
            "type": "response.reasoning_summary_text.delta",
            "item_id": "item_r",
            "delta": "Let me think..."
        }));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], json!("content_block_delta"));
        assert_eq!(events[0]["delta"]["type"], json!("thinking_delta"));
        assert_eq!(events[0]["delta"]["thinking"], json!("Let me think..."));
    }

    #[test]
    fn reasoning_text_delta_alias_also_emits_thinking_delta() {
        // SPEC-M4 §3.5/§7: which OpenAI event actually carries reasoning text (summary vs raw) is
        // VERIFY-gated; both are accepted as equivalent input.
        let mut t = AnthropicToResponses::new();
        t.translate_response_event(response_created("claude-opus-4-1-20250805"));
        t.translate_response_event(json!({
            "type": "response.output_item.added",
            "item": {"id": "item_r", "type": "reasoning", "summary": []}
        }));
        let events = t.translate_response_event(json!({
            "type": "response.reasoning_text.delta",
            "item_id": "item_r",
            "delta": "hmm"
        }));
        assert_eq!(events[0]["delta"]["type"], json!("thinking_delta"));
        assert_eq!(events[0]["delta"]["thinking"], json!("hmm"));
    }

    #[test]
    fn text_block_done_family_collapses_to_single_content_block_stop() {
        let mut t = started_text_translator();
        t.translate_response_event(json!({
            "type": "response.output_text.delta", "item_id": "item_1", "delta": "hi"
        }));

        let first = t.translate_response_event(json!({
            "type": "response.output_text.done", "item_id": "item_1", "output_index": 0, "content_index": 0, "text": "hi"
        }));
        assert_eq!(first.len(), 1);
        assert_eq!(first[0]["type"], json!("content_block_stop"));
        assert_eq!(first[0]["index"], json!(0));
        // Anthropic's content_block_stop carries no accumulated text.
        assert!(first[0].get("text").is_none());

        let second = t.translate_response_event(json!({
            "type": "response.content_part.done", "item_id": "item_1", "output_index": 0, "content_index": 0,
            "part": {"type": "output_text", "text": "hi", "annotations": []}
        }));
        assert_eq!(
            second,
            Vec::<Value>::new(),
            "already stopped -- must not double-fire"
        );

        let third = t.translate_response_event(json!({
            "type": "response.output_item.done", "output_index": 0,
            "item": {"id": "item_1", "type": "message", "status": "completed", "content": [{"type": "output_text", "text": "hi", "annotations": []}]}
        }));
        assert_eq!(
            third,
            Vec::<Value>::new(),
            "already stopped -- must not double-fire"
        );
    }

    #[test]
    fn tool_use_done_family_collapses_to_single_content_block_stop() {
        let mut t = AnthropicToResponses::new();
        t.translate_response_event(response_created("claude-opus-4-1-20250805"));
        t.translate_response_event(json!({
            "type": "response.output_item.added",
            "item": {"id": "item_fc", "type": "function_call", "call_id": "call_abc123", "name": "get_weather"}
        }));
        t.translate_response_event(json!({
            "type": "response.function_call_arguments.delta", "item_id": "item_fc", "delta": "{}"
        }));

        let first = t.translate_response_event(json!({
            "type": "response.function_call_arguments.done", "item_id": "item_fc", "arguments": "{}"
        }));
        assert_eq!(first.len(), 1);
        assert_eq!(first[0]["type"], json!("content_block_stop"));

        let second = t.translate_response_event(json!({
            "type": "response.output_item.done",
            "item": {"id": "item_fc", "type": "function_call", "status": "completed", "call_id": "call_abc123", "name": "get_weather", "arguments": "{}"}
        }));
        assert_eq!(second, Vec::<Value>::new());
    }

    #[test]
    fn message_stop_family_not_fed_before_open_emits_nothing() {
        let mut t = AnthropicToResponses::new();
        let events = t.translate_response_event(json!({
            "type": "response.output_text.done", "item_id": "never_opened", "text": "x"
        }));
        assert_eq!(events, Vec::<Value>::new());
    }

    #[test]
    fn response_completed_emits_message_delta_then_message_stop_with_mapped_usage() {
        let mut t = started_text_translator();
        t.translate_response_event(json!({
            "type": "response.output_text.delta", "item_id": "item_1", "delta": "42"
        }));
        t.translate_response_event(json!({
            "type": "response.output_text.done", "item_id": "item_1", "text": "42"
        }));

        let events = t.translate_response_event(json!({
            "type": "response.completed",
            "response": {
                "id": "resp_1", "status": "completed", "model": "claude-opus-4-1-20250805",
                "output": [], "usage": {
                    "input_tokens": 25, "output_tokens": 9,
                    "input_tokens_details": {"cached_tokens": 5},
                    "output_tokens_details": {"reasoning_tokens": 3},
                    "total_tokens": 34
                }
            }
        }));

        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["type"], json!("message_delta"));
        assert_eq!(events[0]["delta"]["stop_reason"], json!("end_turn"));
        assert_eq!(events[0]["delta"]["stop_sequence"], Value::Null);
        assert_eq!(events[0]["usage"]["output_tokens"], json!(9));
        assert_eq!(events[0]["usage"]["input_tokens"], json!(25));
        assert_eq!(events[0]["usage"]["cache_read_input_tokens"], json!(5));
        assert_eq!(events[0]["usage"]["thinking_tokens"], json!(3));
        // Anthropic's usage has no total_tokens field.
        assert!(events[0]["usage"].get("total_tokens").is_none());

        assert_eq!(events[1], json!({"type": "message_stop"}));
    }

    #[test]
    fn response_incomplete_maps_to_max_tokens_stop_reason() {
        let mut t = started_text_translator();
        let events = t.translate_response_event(json!({
            "type": "response.incomplete",
            "response": {
                "id": "resp_1", "status": "incomplete", "model": "claude-opus-4-1-20250805",
                "incomplete_details": {"reason": "max_output_tokens"},
                "usage": {"output_tokens": 5}
            }
        }));
        assert_eq!(events[0]["delta"]["stop_reason"], json!("max_tokens"));
    }

    #[test]
    fn thinking_then_text_turn_assigns_distinct_indices_in_open_order() {
        let mut t = AnthropicToResponses::new();
        t.translate_response_event(response_created("claude-opus-4-1-20250805"));

        let reasoning_start = t.translate_response_event(json!({
            "type": "response.output_item.added",
            "item": {"id": "item_r", "type": "reasoning", "summary": []}
        }));
        assert_eq!(reasoning_start[0]["index"], json!(0));

        t.translate_response_event(json!({
            "type": "response.reasoning_summary_text.delta", "item_id": "item_r", "delta": "Let me think..."
        }));
        let reasoning_stop = t.translate_response_event(json!({
            "type": "response.reasoning_summary_text.done", "item_id": "item_r", "text": "Let me think..."
        }));
        assert_eq!(reasoning_stop[0]["index"], json!(0));

        t.translate_response_event(json!({
            "type": "response.output_item.added",
            "output_index": 1,
            "item": {"id": "item_t", "type": "message", "role": "assistant", "content": []}
        }));
        let text_start = t.translate_response_event(json!({
            "type": "response.content_part.added",
            "item_id": "item_t",
            "part": {"type": "output_text", "text": "", "annotations": []}
        }));
        assert_eq!(text_start[0]["index"], json!(1));
        assert_ne!(reasoning_start[0]["index"], text_start[0]["index"]);
    }

    #[test]
    fn response_failed_emits_anthropic_error_event() {
        let mut t = AnthropicToResponses::new();
        let events = t.translate_response_event(json!({
            "type": "response.failed",
            "response": {"id": "resp_1", "status": "failed", "error": {"code": "server_error", "message": "boom"}}
        }));
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0],
            json!({"type": "error", "error": {"type": "server_error", "message": "boom"}})
        );
    }

    #[test]
    fn mid_stream_error_event_passes_through_code_as_error_type() {
        let mut t = AnthropicToResponses::new();
        let events = t.translate_response_event(json!({
            "type": "error",
            "code": "rate_limit_exceeded",
            "message": "Too many requests"
        }));
        assert_eq!(
            events[0],
            json!({"type": "error", "error": {"type": "rate_limit_exceeded", "message": "Too many requests"}})
        );
    }

    #[test]
    fn ping_emits_nothing() {
        let mut t = AnthropicToResponses::new();
        let events = t.translate_response_event(json!({"type": "ping"}));
        assert_eq!(events, Vec::<Value>::new());
    }

    #[test]
    fn each_turn_gets_a_fresh_translator_with_no_cross_turn_state() {
        let first = {
            let mut t = AnthropicToResponses::new();
            t.translate_response_event(response_created("claude-opus-4-1-20250805"))
        };
        let second = {
            let mut t = AnthropicToResponses::new();
            t.translate_response_event(response_created("claude-opus-4-1-20250805"))
        };
        // message.id is freshly minted per turn -- never reused across turns.
        assert_ne!(first[0]["message"]["id"], second[0]["message"]["id"]);
    }

    #[test]
    fn debug_redacts_block_state_defensively() {
        let mut t = started_text_translator();
        t.translate_response_event(json!({
            "type": "response.output_text.delta",
            "item_id": "item_1",
            "delta": "super-secret-user-conversation"
        }));

        let s = format!("{t:?}");
        assert!(
            !s.contains("super-secret-user-conversation"),
            "Debug must never leak streamed content: {s}"
        );
        assert!(
            s.contains("redacted"),
            "Debug should mark blocks redacted: {s}"
        );
    }
}
