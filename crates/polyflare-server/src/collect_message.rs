//! Non-streaming `/v1/messages` support (M4 Outcome 3): buffer an ALREADY-TRANSLATED
//! Anthropic-Messages SSE `ResponseStream` — the same `event: <ty>\ndata: <json>\n\n` frames
//! `crate::translate_stream::wrap_translating_stream` yields — into a single Anthropic `Message`
//! JSON object, for a `stream:false` client (Anthropic's Messages API default: a non-streaming
//! request gets back one JSON `Message`, not SSE).
//!
//! This module does no translation of its own — it only drains frames the translator already
//! produced, mirroring `translate_stream::TranslatingStream::feed_line`'s exact line-splitting
//! discipline (split on `\n`, strip a `data:` prefix, skip blank/`[DONE]`, `serde_json::from_str`)
//! so the two consumers (stream straight to the client, or fold into one Message) agree on what a
//! frame means.
//!
//! Content-free discipline: a stream error propagates as the same `ExecError` the streaming path
//! already carries — the caller (`crate::ingress`) turns that into the same generic, content-safe
//! `(StatusCode::BAD_GATEWAY, "upstream error")` the streaming Err arm uses, NEVER a partial
//! Message and never upstream prose. This function itself never logs the collected Message or any
//! response text.

use bytes::Bytes;
use futures_util::StreamExt;
use polyflare_anthropic::MessageCollector;
use polyflare_core::{ExecError, ResponseStream};

/// Buffer `stream` fully, folding every `data:` line's JSON event into a [`MessageCollector`], and
/// return the assembled Anthropic `Message`. On a stream item `Err`, stop immediately and
/// propagate that `ExecError` — never returning a partially-assembled Message.
pub(crate) async fn collect_anthropic_message(
    mut stream: ResponseStream,
) -> Result<serde_json::Value, ExecError> {
    let mut collector = MessageCollector::new();
    let mut line_buf: Vec<u8> = Vec::new();

    while let Some(chunk) = stream.next().await {
        let bytes: Bytes = chunk?;
        line_buf.extend_from_slice(&bytes);
        while let Some(pos) = line_buf.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = line_buf.drain(..=pos).collect();
            let line = &line[..line.len() - 1]; // drop the trailing '\n'
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            feed_line(&mut collector, line);
        }
    }

    Ok(collector.finish())
}

/// Handle one complete SSE line (sans trailing `\n`/`\r\n`) — mirrors
/// `translate_stream::TranslatingStream::feed_line` exactly: only a `data:` line carrying
/// parseable JSON is folded; everything else (non-`data:` lines, blank payload, `[DONE]`,
/// malformed JSON) is dropped.
fn feed_line(collector: &mut MessageCollector, line: &[u8]) {
    let text = String::from_utf8_lossy(line);
    let Some(payload) = text.strip_prefix("data:") else {
        return;
    };
    let payload = payload.trim();
    if payload.is_empty() || payload == "[DONE]" {
        return;
    }
    let Ok(event) = serde_json::from_str::<serde_json::Value>(payload) else {
        return;
    };
    collector.push(&event);
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;
    use serde_json::json;

    fn sse_frame(event: &serde_json::Value) -> Bytes {
        let ty = event
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("message");
        Bytes::from(format!("event: {ty}\ndata: {event}\n\n"))
    }

    fn ok_stream(chunks: Vec<Bytes>) -> ResponseStream {
        ResponseStream::new(stream::iter(chunks.into_iter().map(Ok::<Bytes, ExecError>)))
    }

    #[tokio::test]
    async fn collects_a_full_text_turn_into_a_single_message() {
        let chunks = vec![
            sse_frame(&json!({
                "type": "message_start",
                "message": {
                    "id": "msg_1", "type": "message", "role": "assistant", "model": "m",
                    "content": [], "usage": {"input_tokens": 5, "output_tokens": 0}
                }
            })),
            sse_frame(&json!({
                "type": "content_block_start", "index": 0,
                "content_block": {"type": "text", "text": ""}
            })),
            sse_frame(&json!({
                "type": "content_block_delta", "index": 0,
                "delta": {"type": "text_delta", "text": "Hi"}
            })),
            sse_frame(&json!({"type": "content_block_stop", "index": 0})),
            sse_frame(&json!({
                "type": "message_delta",
                "delta": {"stop_reason": "end_turn", "stop_sequence": null},
                "usage": {"input_tokens": 5, "output_tokens": 1}
            })),
            sse_frame(&json!({"type": "message_stop"})),
        ];

        let msg = collect_anthropic_message(ok_stream(chunks)).await.unwrap();
        assert_eq!(msg["id"], json!("msg_1"));
        assert_eq!(msg["stop_reason"], json!("end_turn"));
        assert_eq!(msg["content"], json!([{"type": "text", "text": "Hi"}]));
    }

    #[tokio::test]
    async fn a_data_line_split_across_two_chunks_reassembles() {
        let full = format!(
            "data: {}\n\n",
            json!({
                "type": "message_start",
                "message": {"id": "msg_2", "role": "assistant", "model": "m", "usage": {}}
            })
        );
        let bytes = full.into_bytes();
        let split_at = bytes.len() / 2;
        let (first, second) = bytes.split_at(split_at);
        let chunks = vec![Bytes::from(first.to_vec()), Bytes::from(second.to_vec())];

        let msg = collect_anthropic_message(ok_stream(chunks)).await.unwrap();
        assert_eq!(msg["id"], json!("msg_2"));
    }

    #[tokio::test]
    async fn drops_keepalive_comment_and_done_sentinel_lines() {
        let chunks = vec![
            Bytes::from_static(b": keep-alive\n\n"),
            Bytes::from_static(b"data: [DONE]\n\n"),
        ];
        let msg = collect_anthropic_message(ok_stream(chunks)).await.unwrap();
        // Neither line folds into anything -- an empty (never message_start-seeded) Message, with
        // no content blocks.
        assert_eq!(msg["content"], json!([]));
    }

    #[tokio::test]
    async fn a_mid_stream_error_propagates_not_a_partial_message() {
        let chunks: Vec<Result<Bytes, ExecError>> = vec![
            Ok(sse_frame(&json!({
                "type": "message_start",
                "message": {"id": "msg_3", "role": "assistant", "model": "m", "usage": {}}
            }))),
            Ok(sse_frame(&json!({
                "type": "content_block_start", "index": 0,
                "content_block": {"type": "text", "text": ""}
            }))),
            Err(ExecError::Stream("connection reset".to_string())),
        ];
        let stream = ResponseStream::new(stream::iter(chunks));

        let result = collect_anthropic_message(stream).await;
        assert!(result.is_err(), "a stream error must propagate as Err");
    }
}
