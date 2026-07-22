//! `MessageCollector`: folds the Anthropic-Messages response *events* that
//! `AnthropicToResponses::translate_response_event` emits (see `translate.rs`) into a single
//! Anthropic `Message` JSON object — the shape a non-streaming (`stream:false`) `/v1/messages`
//! client must receive instead of SSE (SPEC-M4 Outcome 3).
//!
//! This is a pure fold over already-translated Anthropic events: it does no translation of its
//! own (that remains `AnthropicToResponses`'s job) and does not know or care where the events came
//! from (a live stream, a test script, anything). The server crate's `collect_message` module is
//! what actually drains a `ResponseStream` of SSE bytes and feeds each `data:` line's JSON into
//! `push` — see that module for the byte-level plumbing.
//!
//! Content-free discipline: the collector holds one in-flight turn's assembled text/thinking/
//! tool-input ONLY long enough to serialize it via `finish` — the caller must never log the
//! collected `Message` or any of its content.

use serde_json::{json, Value};

/// One open (or finished) Anthropic content block, keyed by its Anthropic `index`.
enum Block {
    Text(String),
    Thinking(String),
    ToolUse {
        id: String,
        name: String,
        /// The `input_json_delta` fragments accumulated so far, concatenated as-received. Parsed
        /// into a JSON object only once, at finalization (`content_block_stop`, or `finish` for a
        /// block that never saw one) — never re-parsed per fragment.
        input_acc: String,
    },
    /// A block whose `content_block_stop` has already been folded — its final JSON shape is
    /// computed once and cached here so a defensive double-`content_block_stop` (should one ever
    /// arrive) is a no-op rather than re-finalizing.
    Done(Value),
}

fn finalize_block(block: &Block) -> Value {
    match block {
        Block::Text(text) => json!({"type": "text", "text": text}),
        Block::Thinking(thinking) => json!({"type": "thinking", "thinking": thinking}),
        Block::ToolUse {
            id,
            name,
            input_acc,
        } => {
            // Empty or unparseable accumulated input -> `{}` (never surface a parse error to the
            // client, and never guess at a non-object shape).
            let input = serde_json::from_str::<Value>(input_acc)
                .ok()
                .filter(|v| v.is_object())
                .unwrap_or_else(|| json!({}));
            json!({"type": "tool_use", "id": id, "name": name, "input": input})
        }
        Block::Done(value) => value.clone(),
    }
}

/// Folds a sequence of Anthropic response events (`message_start`, `content_block_start/delta/
/// stop`, `message_delta`, `message_stop`) into a single Anthropic `Message` object. Construct
/// fresh per turn via `new()`, `push` every event in arrival order, then `finish()` once.
pub struct MessageCollector {
    message: Value,
    /// Indexed by the Anthropic content-block `index` (monotonic 0,1,2… as blocks open — see the
    /// module doc comment). Slots are `None` only for an index that was never opened, which should
    /// not happen in a well-formed event sequence; `finish` skips any that are.
    blocks: Vec<Option<Block>>,
}

impl MessageCollector {
    pub fn new() -> Self {
        Self {
            // An empty object, not `Value::Null`: `serde_json::Value`'s `IndexMut<&str>` auto-
            // inserts a missing key on an `Object`, but panics/no-ops on `Null` — seeding an object
            // up front means every later `self.message["field"] = ...` assignment (including
            // before `message_start` ever arrives, which should not happen but is defensive) just
            // works.
            message: json!({}),
            blocks: Vec::new(),
        }
    }

    /// Fold one Anthropic response event into the in-progress Message. Any event whose `type` is
    /// not one of the recognized fold actions is ignored.
    pub fn push(&mut self, event: &Value) {
        match event.get("type").and_then(|v| v.as_str()).unwrap_or("") {
            "message_start" => self.on_message_start(event),
            "content_block_start" => self.on_content_block_start(event),
            "content_block_delta" => self.on_content_block_delta(event),
            "content_block_stop" => self.on_content_block_stop(event),
            "message_delta" => self.on_message_delta(event),
            // "message_stop" is terminal, no-op; any other/unknown event type is ignored.
            _ => {}
        }
    }

    /// Consume the collector and return the assembled Anthropic `Message`, with `content` folded
    /// into ascending index order.
    pub fn finish(mut self) -> Value {
        let mut content = Vec::with_capacity(self.blocks.len());
        for block in self.blocks.drain(..).flatten() {
            content.push(finalize_block(&block));
        }
        self.message["content"] = Value::Array(content);
        self.message
    }

    /// Seed the base Message from `event["message"]` — carries `id`, `type`, `role`, `model`, and
    /// `usage` (overwritten by a later `message_delta`'s terminal usage, if one arrives).
    /// `stop_reason`/`stop_sequence` default to `null` (Anthropic's own default for an in-progress
    /// Message) until/unless a `message_delta` sets them.
    fn on_message_start(&mut self, event: &Value) {
        let message = event.get("message").cloned().unwrap_or_else(|| json!({}));
        self.message = json!({
            "id": message.get("id").cloned().unwrap_or(Value::Null),
            "type": "message",
            "role": message.get("role").cloned().unwrap_or_else(|| json!("assistant")),
            "model": message.get("model").cloned().unwrap_or(Value::Null),
            "content": [],
            "stop_reason": Value::Null,
            "stop_sequence": Value::Null,
            "usage": message.get("usage").cloned().unwrap_or_else(|| json!({})),
        });
    }

    /// Open a content block at `event["index"]`, per `event["content_block"]["type"]`: `text` and
    /// `thinking` accumulate their text/thinking string; `tool_use` accumulates its input-JSON
    /// string separately (see `on_content_block_delta`). Any other/unrecognized block type is
    /// ignored (no slot opened — a later delta/stop for that index is then also a no-op).
    fn on_content_block_start(&mut self, event: &Value) {
        let Some(index) = event.get("index").and_then(|v| v.as_u64()) else {
            return;
        };
        let content_block = event
            .get("content_block")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let block = match content_block.get("type").and_then(|v| v.as_str()) {
            Some("text") => Block::Text(String::new()),
            Some("thinking") => Block::Thinking(String::new()),
            Some("tool_use") => Block::ToolUse {
                id: content_block
                    .get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                name: content_block
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                input_acc: String::new(),
            },
            _ => return,
        };
        self.set_block(index as usize, block);
    }

    /// Append a delta to the block open at `event["index"]`, per `event["delta"]["type"]`:
    /// `text_delta` -> that block's text, `input_json_delta` -> that tool_use block's
    /// input-accumulator string, `thinking_delta` -> that block's thinking. A delta type that
    /// doesn't match the block's kind (or an index with no open block) is ignored.
    fn on_content_block_delta(&mut self, event: &Value) {
        let Some(index) = event.get("index").and_then(|v| v.as_u64()) else {
            return;
        };
        let Some(Some(block)) = self.blocks.get_mut(index as usize) else {
            return;
        };
        let delta = event.get("delta").cloned().unwrap_or_else(|| json!({}));
        let delta_type = delta.get("type").and_then(|v| v.as_str()).unwrap_or("");
        match (block, delta_type) {
            (Block::Text(text), "text_delta") => {
                text.push_str(delta.get("text").and_then(|v| v.as_str()).unwrap_or(""));
            }
            (Block::ToolUse { input_acc, .. }, "input_json_delta") => {
                input_acc.push_str(
                    delta
                        .get("partial_json")
                        .and_then(|v| v.as_str())
                        .unwrap_or(""),
                );
            }
            (Block::Thinking(thinking), "thinking_delta") => {
                thinking.push_str(delta.get("thinking").and_then(|v| v.as_str()).unwrap_or(""));
            }
            _ => {}
        }
    }

    /// Finalize the block at `event["index"]`: for a `tool_use` block, parse the accumulated
    /// input-string into a JSON object (empty/unparseable -> `{}`); for `text`/`thinking`, the
    /// accumulated string is already the finished shape. A repeat stop for an already-finalized
    /// index (or an index with no open block) is a no-op.
    fn on_content_block_stop(&mut self, event: &Value) {
        let Some(index) = event.get("index").and_then(|v| v.as_u64()) else {
            return;
        };
        let Some(slot) = self.blocks.get_mut(index as usize) else {
            return;
        };
        let Some(block) = slot else {
            return;
        };
        let finished = finalize_block(block);
        *slot = Some(Block::Done(finished));
    }

    /// `delta.stop_reason`/`delta.stop_sequence` set on the Message; `event["usage"]` replaces the
    /// Message's `usage` (the terminal usage from `message_delta` is authoritative — see the
    /// module doc comment).
    fn on_message_delta(&mut self, event: &Value) {
        let delta = event.get("delta").cloned().unwrap_or_else(|| json!({}));
        if let Some(stop_reason) = delta.get("stop_reason") {
            self.message["stop_reason"] = stop_reason.clone();
        }
        if let Some(stop_sequence) = delta.get("stop_sequence") {
            self.message["stop_sequence"] = stop_sequence.clone();
        }
        if let Some(usage) = event.get("usage") {
            self.message["usage"] = usage.clone();
        }
    }

    /// Grow `blocks` to hold `index`, if needed, then open a fresh block there. Indices are
    /// monotonic (0,1,2…) in a well-formed event sequence, so this is normally a plain push; the
    /// resize is defensive against a gap.
    fn set_block(&mut self, index: usize, block: Block) {
        if self.blocks.len() <= index {
            self.blocks.resize_with(index + 1, || None);
        }
        self.blocks[index] = Some(block);
    }
}

impl Default for MessageCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message_start(id: &str) -> Value {
        json!({
            "type": "message_start",
            "message": {
                "id": id,
                "type": "message",
                "role": "assistant",
                "model": "claude-opus-4-1-20250805",
                "content": [],
                "usage": {"input_tokens": 10, "output_tokens": 0},
            }
        })
    }

    #[test]
    fn folds_a_text_only_turn_into_one_text_block() {
        let mut c = MessageCollector::new();
        c.push(&message_start("msg_1"));
        c.push(&json!({
            "type": "content_block_start", "index": 0,
            "content_block": {"type": "text", "text": ""}
        }));
        c.push(&json!({
            "type": "content_block_delta", "index": 0,
            "delta": {"type": "text_delta", "text": "Hello"}
        }));
        c.push(&json!({
            "type": "content_block_delta", "index": 0,
            "delta": {"type": "text_delta", "text": " world"}
        }));
        c.push(&json!({"type": "content_block_stop", "index": 0}));
        c.push(&json!({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn", "stop_sequence": null},
            "usage": {"input_tokens": 10, "output_tokens": 5}
        }));
        c.push(&json!({"type": "message_stop"}));

        let msg = c.finish();
        assert_eq!(msg["id"], json!("msg_1"));
        assert_eq!(msg["type"], json!("message"));
        assert_eq!(msg["role"], json!("assistant"));
        assert_eq!(msg["model"], json!("claude-opus-4-1-20250805"));
        assert_eq!(msg["stop_reason"], json!("end_turn"));
        assert_eq!(msg["stop_sequence"], Value::Null);
        assert_eq!(
            msg["usage"],
            json!({"input_tokens": 10, "output_tokens": 5})
        );
        assert_eq!(
            msg["content"],
            json!([{"type": "text", "text": "Hello world"}])
        );
    }

    #[test]
    fn folds_a_tool_use_turn_reassembling_input_json_delta_fragments() {
        let mut c = MessageCollector::new();
        c.push(&message_start("msg_2"));
        c.push(&json!({
            "type": "content_block_start", "index": 0,
            "content_block": {"type": "tool_use", "id": "toolu_1", "name": "get_weather", "input": {}}
        }));
        c.push(&json!({
            "type": "content_block_delta", "index": 0,
            "delta": {"type": "input_json_delta", "partial_json": "{\"city\":"}
        }));
        c.push(&json!({
            "type": "content_block_delta", "index": 0,
            "delta": {"type": "input_json_delta", "partial_json": "\"Tokyo\"}"}
        }));
        c.push(&json!({"type": "content_block_stop", "index": 0}));
        c.push(&json!({
            "type": "message_delta",
            "delta": {"stop_reason": "tool_use", "stop_sequence": null},
            "usage": {"input_tokens": 20, "output_tokens": 8}
        }));
        c.push(&json!({"type": "message_stop"}));

        let msg = c.finish();
        assert_eq!(msg["stop_reason"], json!("tool_use"));
        assert_eq!(
            msg["content"],
            json!([{
                "type": "tool_use", "id": "toolu_1", "name": "get_weather",
                "input": {"city": "Tokyo"}
            }])
        );
    }

    #[test]
    fn tool_use_block_with_no_input_fragments_defaults_input_to_empty_object() {
        let mut c = MessageCollector::new();
        c.push(&message_start("msg_3"));
        c.push(&json!({
            "type": "content_block_start", "index": 0,
            "content_block": {"type": "tool_use", "id": "toolu_2", "name": "noop", "input": {}}
        }));
        c.push(&json!({"type": "content_block_stop", "index": 0}));
        c.push(&json!({
            "type": "message_delta",
            "delta": {"stop_reason": "tool_use", "stop_sequence": null},
            "usage": {}
        }));

        let msg = c.finish();
        assert_eq!(msg["content"][0]["input"], json!({}));
    }

    #[test]
    fn multiple_blocks_preserve_ascending_index_order() {
        let mut c = MessageCollector::new();
        c.push(&message_start("msg_4"));
        c.push(&json!({
            "type": "content_block_start", "index": 0,
            "content_block": {"type": "thinking", "thinking": ""}
        }));
        c.push(&json!({
            "type": "content_block_delta", "index": 0,
            "delta": {"type": "thinking_delta", "thinking": "hmm"}
        }));
        c.push(&json!({"type": "content_block_stop", "index": 0}));
        c.push(&json!({
            "type": "content_block_start", "index": 1,
            "content_block": {"type": "text", "text": ""}
        }));
        c.push(&json!({
            "type": "content_block_delta", "index": 1,
            "delta": {"type": "text_delta", "text": "42"}
        }));
        c.push(&json!({"type": "content_block_stop", "index": 1}));
        c.push(&json!({
            "type": "content_block_start", "index": 2,
            "content_block": {"type": "tool_use", "id": "toolu_3", "name": "noop", "input": {}}
        }));
        c.push(&json!({"type": "content_block_stop", "index": 2}));
        c.push(&json!({
            "type": "message_delta",
            "delta": {"stop_reason": "tool_use", "stop_sequence": null},
            "usage": {}
        }));
        c.push(&json!({"type": "message_stop"}));

        let msg = c.finish();
        assert_eq!(
            msg["content"],
            json!([
                {"type": "thinking", "thinking": "hmm"},
                {"type": "text", "text": "42"},
                {"type": "tool_use", "id": "toolu_3", "name": "noop", "input": {}}
            ])
        );
    }

    #[test]
    fn unknown_event_type_is_ignored() {
        let mut c = MessageCollector::new();
        c.push(&message_start("msg_5"));
        c.push(&json!({"type": "ping"}));
        let msg = c.finish();
        assert_eq!(msg["id"], json!("msg_5"));
        assert_eq!(msg["content"], json!([]));
    }
}
