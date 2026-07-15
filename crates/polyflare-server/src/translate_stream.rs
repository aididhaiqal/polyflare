//! M4b-wiring: the translating response-stream wrapper. Wraps an upstream `ResponseStream` of
//! OpenAI-Responses SSE bytes (the Codex executor's native wire format) and yields
//! Anthropic-Messages SSE bytes, driving a per-turn stateful `Translator`
//! (`polyflare_anthropic::AnthropicToResponses` in practice — SPEC-M4 §3.4/§3.5).
//!
//! Non-buffering: each upstream SSE frame is parsed and translated as soon as it arrives. Two
//! small pieces of state are carried across `poll_next` calls, and NEITHER ever accumulates
//! response *content*:
//!   - `line_buf`: the tail of a `data:` line that hasn't seen its terminating `\n` yet (a chunk
//!     boundary can split a line anywhere) — bounded by one line's worth of bytes.
//!   - `ready`: a short queue of already-translated Anthropic SSE frames waiting to be handed to
//!     the caller one `poll_next` at a time (one upstream event can produce zero, one, or many
//!     Anthropic events — see `Translator::translate_response_event`) — bounded by one upstream
//!     chunk's worth of *already-translated, already-serialized* output frames, not raw text.
//!
//! Non-`data:` SSE lines (blank keep-alive lines, `:`-comment keep-alives), the `data: [DONE]`
//! sentinel, and unparseable JSON payloads are dropped — there is no Anthropic-side equivalent to
//! translate them into.

use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_core::Stream;
use polyflare_core::{ExecError, ResponseStream, Translator};
use serde_json::Value;

/// Wrap `inner` (upstream OpenAI-Responses SSE bytes) with `translator`, yielding
/// Anthropic-Messages SSE bytes. `translator` must be the SAME instance whose `translate_request`
/// produced the request this stream is the response to — it carries per-turn state (message id,
/// content-block indices) that only makes sense threaded through one turn end-to-end.
pub fn wrap_translating_stream(
    inner: ResponseStream,
    translator: Box<dyn Translator>,
) -> ResponseStream {
    Box::pin(TranslatingStream {
        inner,
        translator,
        line_buf: Vec::new(),
        ready: VecDeque::new(),
        inner_done: false,
    })
}

struct TranslatingStream {
    inner: ResponseStream,
    translator: Box<dyn Translator>,
    line_buf: Vec<u8>,
    ready: VecDeque<Bytes>,
    inner_done: bool,
}

impl TranslatingStream {
    /// Handle one complete SSE line (sans trailing `\n`/`\r\n`). Only a `data:` line carrying
    /// parseable JSON produces output; everything else (blank lines, `:`-comment keep-alives,
    /// `[DONE]`, malformed JSON) is dropped.
    fn feed_line(&mut self, line: &[u8]) {
        let text = String::from_utf8_lossy(line);
        let Some(payload) = text.strip_prefix("data:") else {
            return;
        };
        let payload = payload.trim();
        if payload.is_empty() || payload == "[DONE]" {
            return;
        }
        let Ok(event) = serde_json::from_str::<Value>(payload) else {
            return;
        };
        for out_event in self.translator.translate_response_event(event) {
            let ty = out_event
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("message");
            let data = serde_json::to_string(&out_event).unwrap_or_else(|_| "{}".to_string());
            self.ready
                .push_back(Bytes::from(format!("event: {ty}\ndata: {data}\n\n")));
        }
    }

    /// Append newly-arrived bytes to `line_buf`, draining and translating every complete line it
    /// now contains. Any trailing partial line (no `\n` yet) stays in `line_buf` for the next
    /// chunk.
    fn feed_chunk(&mut self, chunk: &[u8]) {
        self.line_buf.extend_from_slice(chunk);
        loop {
            let Some(pos) = self.line_buf.iter().position(|&b| b == b'\n') else {
                break;
            };
            let line: Vec<u8> = self.line_buf.drain(..=pos).collect();
            let line = &line[..line.len() - 1]; // drop the trailing '\n'
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            self.feed_line(line);
        }
    }
}

impl Stream for TranslatingStream {
    type Item = Result<Bytes, ExecError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut(); // TranslatingStream is Unpin (all fields Unpin)
        loop {
            if let Some(frame) = this.ready.pop_front() {
                return Poll::Ready(Some(Ok(frame)));
            }
            if this.inner_done {
                return Poll::Ready(None);
            }
            match this.inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(bytes))) => {
                    this.feed_chunk(&bytes);
                    // loop: either this chunk produced ready frames (emit next iteration), or it
                    // was a keep-alive/partial line and we poll `inner` again.
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => {
                    this.inner_done = true;
                    // loop: drain any already-ready frames before yielding the terminal `None`.
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream::{self, StreamExt};
    use polyflare_anthropic::AnthropicToResponses;
    use serde_json::json;

    fn sse_chunk(payload: &Value) -> Bytes {
        Bytes::from(format!("data: {payload}\n\n"))
    }

    fn scripted_stream(chunks: Vec<Bytes>) -> ResponseStream {
        Box::pin(stream::iter(chunks.into_iter().map(Ok::<Bytes, ExecError>)))
    }

    #[tokio::test]
    async fn translates_full_turn_into_ordered_anthropic_sse_frames() {
        let chunks = vec![
            sse_chunk(&json!({
                "type": "response.created",
                "response": {"id": "resp_1", "status": "in_progress", "model": "gpt-5.6-sol", "usage": Value::Null}
            })),
            sse_chunk(&json!({
                "type": "response.output_item.added",
                "item": {"id": "item_1", "type": "message", "role": "assistant", "content": []}
            })),
            sse_chunk(&json!({
                "type": "response.content_part.added",
                "item_id": "item_1",
                "part": {"type": "output_text", "text": "", "annotations": []}
            })),
            sse_chunk(&json!({
                "type": "response.output_text.delta", "item_id": "item_1", "delta": "Hello"
            })),
            sse_chunk(&json!({
                "type": "response.output_text.delta", "item_id": "item_1", "delta": " world"
            })),
            sse_chunk(&json!({
                "type": "response.output_text.done", "item_id": "item_1", "text": "Hello world"
            })),
            sse_chunk(&json!({
                "type": "response.content_part.done", "item_id": "item_1",
                "part": {"type": "output_text", "text": "Hello world", "annotations": []}
            })),
            sse_chunk(&json!({
                "type": "response.output_item.done",
                "item": {"id": "item_1", "type": "message", "status": "completed", "content": []}
            })),
            sse_chunk(&json!({
                "type": "response.completed",
                "response": {"id": "resp_1", "status": "completed", "model": "gpt-5.6-sol", "usage": {"output_tokens": 2}}
            })),
        ];

        let translator: Box<dyn Translator> = Box::new(AnthropicToResponses::new());
        let stream = wrap_translating_stream(scripted_stream(chunks), translator);
        let frames: Vec<Bytes> = stream.map(|r| r.unwrap()).collect().await;

        // output_item.added(message), content_part.done, and output_item.done all collapse to
        // nothing (see the module doc comment + AnthropicToResponses's own doc comments) -- 9
        // upstream events -> 7 Anthropic events.
        let types: Vec<String> = frames
            .iter()
            .map(|b| {
                let text = String::from_utf8(b.to_vec()).unwrap();
                text.lines()
                    .next()
                    .unwrap()
                    .trim_start_matches("event: ")
                    .to_string()
            })
            .collect();
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

        // Every frame is `event: <type>\ndata: <json>\n\n`, and the JSON's own `type` field
        // matches the `event:` line.
        for frame in &frames {
            let text = String::from_utf8(frame.to_vec()).unwrap();
            assert!(text.ends_with("\n\n"));
            let mut lines = text.lines();
            let event_line = lines.next().unwrap();
            let data_line = lines.next().unwrap();
            let ty = event_line.trim_start_matches("event: ");
            let payload: Value =
                serde_json::from_str(data_line.trim_start_matches("data: ")).unwrap();
            assert_eq!(payload["type"], json!(ty));
        }

        let full_text = frames
            .iter()
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .collect::<String>();
        assert!(full_text.contains(r#""text":"Hello""#));
        assert!(full_text.contains(r#""text":" world""#));
        // Not the OpenAI-Responses shape the upstream sent.
        assert!(!full_text.contains("response.output_text.delta"));
    }

    #[tokio::test]
    async fn reassembles_a_data_line_split_across_two_chunks() {
        let full_line = format!(
            "data: {}\n\n",
            json!({"type": "response.output_text.delta", "item_id": "item_1", "delta": "Hi"})
        );
        let bytes = full_line.into_bytes();
        let split_at = bytes.len() / 2;
        let (first, second) = bytes.split_at(split_at);

        // Prime the translator so this delta resolves against an already-open block (index 0).
        let mut translator = AnthropicToResponses::new();
        translator.translate_response_event(json!({
            "type": "response.created",
            "response": {"id": "resp_1", "status": "in_progress", "model": "m", "usage": Value::Null}
        }));
        translator.translate_response_event(json!({
            "type": "response.output_item.added",
            "item": {"id": "item_1", "type": "message", "role": "assistant", "content": []}
        }));
        translator.translate_response_event(json!({
            "type": "response.content_part.added",
            "item_id": "item_1",
            "part": {"type": "output_text", "text": "", "annotations": []}
        }));

        let chunks = vec![Bytes::from(first.to_vec()), Bytes::from(second.to_vec())];
        let stream = wrap_translating_stream(scripted_stream(chunks), Box::new(translator));
        let frames: Vec<Bytes> = stream.map(|r| r.unwrap()).collect().await;

        assert_eq!(
            frames.len(),
            1,
            "a data: line split mid-chunk must reassemble into exactly one event, not zero/two"
        );
        let text = String::from_utf8(frames[0].to_vec()).unwrap();
        assert!(text.contains(r#""text":"Hi""#));
    }

    #[tokio::test]
    async fn drops_keepalive_comment_and_done_sentinel_lines() {
        let chunks = vec![
            Bytes::from_static(b": keep-alive\n\n"),
            Bytes::from_static(b"data: [DONE]\n\n"),
        ];
        let translator: Box<dyn Translator> = Box::new(AnthropicToResponses::new());
        let stream = wrap_translating_stream(scripted_stream(chunks), translator);
        let frames: Vec<Bytes> = stream.map(|r| r.unwrap()).collect().await;
        assert!(frames.is_empty());
    }

    #[tokio::test]
    async fn unparseable_json_payload_is_dropped_not_errored() {
        let chunks = vec![Bytes::from_static(b"data: not-json\n\n")];
        let translator: Box<dyn Translator> = Box::new(AnthropicToResponses::new());
        let stream = wrap_translating_stream(scripted_stream(chunks), translator);
        let frames: Vec<Result<Bytes, ExecError>> = stream.collect().await;
        assert!(frames.is_empty());
    }
}
