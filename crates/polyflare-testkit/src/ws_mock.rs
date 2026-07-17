//! Test support: a scriptable WebSocket mock upstream, mirroring [`crate::MockUpstream`]'s idiom
//! (`spawn() -> String`, `Arc<Mutex<..>>` recorders, scripted-response modes) but speaking the
//! codex WS wire protocol instead of HTTP-SSE.
//!
//! The wire shapes scripted here are not invented: every frame this mock can emit matches
//! `docs/WS-GROUND-TRUTH-CODEX.md` §3 (framing) — the wrapped error envelope
//! (`{"type":"error","status":u16,"error":{"code","message"},"headers":{...}}`), the terminal
//! `response.failed`/`response.completed` shapes, and the `previous_response_not_found` code +
//! message string, which is the exact literal `watchdog.rs`'s Strategy B already synthesizes on
//! the HTTP-SSE path (`watchdog.rs:34-36`) — reused here so both transports' tests can assert the
//! same string.
//!
//! Content-safety: [`RecordedFrame`] retains only structural facts (an anchor id — an opaque
//! backend-issued identifier, not conversation content — and an item count), never the frame's
//! `input` payload. Nothing here derives `Debug` over a full frame or request body.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use serde_json::{json, Value};
use tokio::net::TcpListener;

/// One received `response.create` frame, reduced to the two facts that prove a "delta" was
/// actually a delta rather than a silent full resend — never the frame's content.
#[derive(Clone, PartialEq, Eq)]
pub struct RecordedFrame {
    /// The frame's `previous_response_id`, if it carried one. An opaque backend-issued id (like a
    /// `SessionKey`'s hash), not conversation content.
    pub previous_response_id: Option<String>,
    /// `input` array length (0 if `input` was absent or not a JSON array).
    pub input_len: usize,
}

/// A scripted response for the next (or every, once repeating) `response.create` turn a
/// [`MockWsUpstream`] receives. Every variant maps directly to a frame shape in
/// `docs/WS-GROUND-TRUTH-CODEX.md` §3.
#[derive(Clone)]
pub enum ScriptedTurn {
    /// A normal turn: emit `events` verbatim as WS text frames, then a terminal
    /// `response.completed` carrying a freshly generated `resp_N` id.
    Turn { events: Vec<String> },
    /// A terminal `response.failed` carrying `error.code` / `error.message` — no preceding
    /// `events`. Use [`ScriptedTurn::previous_response_not_found`] for the specific anchor-miss
    /// case Task 6/7 script against.
    Failed { code: String, message: String },
    /// The WS-only wrapped error envelope (ground truth §3):
    /// `{"type":"error","status":u16,"error":{"code","message"},"headers":{...}}`.
    ErrorEnvelope {
        status: u16,
        code: String,
        message: String,
        headers: Vec<(String, String)>,
    },
    /// Emit `events_before_close` (non-terminal — no `response.completed`/`.failed`), then close
    /// the socket. Models "close mid-stream, before any terminal frame".
    CloseMidStream { events_before_close: Vec<String> },
    /// Accept the frame (it IS recorded) and never send anything back. Models a stall past the
    /// client's idle timeout.
    Stall,
}

impl ScriptedTurn {
    /// A normal turn: `events` streamed as WS text frames, then `response.completed` with a
    /// generated id.
    pub fn normal(events: Vec<String>) -> Self {
        ScriptedTurn::Turn { events }
    }

    /// `response.failed` carrying `previous_response_not_found` — the exact code + message
    /// `watchdog.rs`'s Strategy B synthesizes on the HTTP-SSE path (`watchdog.rs:34-36`), reused
    /// verbatim so WS tests can share the same string-matching assertions.
    pub fn previous_response_not_found() -> Self {
        ScriptedTurn::Failed {
            code: "previous_response_not_found".to_string(),
            message: "anchor not resumable; resend full history".to_string(),
        }
    }

    /// The wrapped error envelope, pre-filled for `websocket_connection_limit_reached` (ground
    /// truth §2/§5's server 60-minute connection cap). The ground-truth doc does not pin a
    /// numeric HTTP-shaped status for this envelope, so the caller supplies whatever their test
    /// needs to assert against.
    pub fn connection_limit_reached(status: u16) -> Self {
        ScriptedTurn::ErrorEnvelope {
            status,
            code: "websocket_connection_limit_reached".to_string(),
            message: "the websocket connection limit was reached".to_string(),
            headers: Vec::new(),
        }
    }

    /// The wrapped error envelope, pre-filled for a 429 carrying `Retry-After` inside the
    /// envelope's own `headers` map (ground truth §3's `"headers":{...}` field) rather than a real
    /// HTTP response header — this is the shape Task 7's 429 test parses `retry_after` out of.
    pub fn rate_limited_429(retry_after_secs: u64) -> Self {
        ScriptedTurn::ErrorEnvelope {
            status: 429,
            code: "rate_limit_exceeded".to_string(),
            message: "rate limit exceeded".to_string(),
            headers: vec![("retry-after".to_string(), retry_after_secs.to_string())],
        }
    }

    /// Non-terminal `events_before_close`, then the socket closes without ever sending a terminal
    /// frame.
    pub fn close_mid_stream(events_before_close: Vec<String>) -> Self {
        ScriptedTurn::CloseMidStream {
            events_before_close,
        }
    }

    /// Accept the frame (recorded) and never respond.
    pub fn stall() -> Self {
        ScriptedTurn::Stall
    }
}

/// A scriptable WS mock upstream: serves a `GET /responses` WebSocket upgrade, records every
/// received `response.create` frame's structural facts, and plays a queued script of
/// [`ScriptedTurn`]s across however many turns arrive — on the SAME connection or across
/// reconnects, since the script is shared at the mock level, not per-socket.
#[derive(Clone)]
pub struct MockWsUpstream {
    script: Arc<Mutex<Vec<ScriptedTurn>>>,
    handshake_count: Arc<AtomicUsize>,
    frames: Arc<Mutex<Vec<RecordedFrame>>>,
    id_counter: Arc<AtomicU32>,
}

impl MockWsUpstream {
    /// A mock whose every turn follows the same scripted behavior.
    pub fn new(turn: ScriptedTurn) -> Self {
        Self::scripted(vec![turn])
    }

    /// A mock that plays through `script` in order — one entry consumed per `response.create`
    /// frame received, across any connection; the last entry repeats once the rest are exhausted
    /// (so a test can script N turns and not worry about a stray extra request panicking it).
    pub fn scripted(script: Vec<ScriptedTurn>) -> Self {
        assert!(
            !script.is_empty(),
            "MockWsUpstream::scripted requires at least one ScriptedTurn"
        );
        Self {
            script: Arc::new(Mutex::new(script)),
            handshake_count: Arc::new(AtomicUsize::new(0)),
            frames: Arc::new(Mutex::new(Vec::new())),
            id_counter: Arc::new(AtomicU32::new(0)),
        }
    }

    /// How many WS connections (handshakes) this mock has accepted. The central proof of
    /// connection reuse: two turns over one socket must leave this at `1`.
    pub fn handshake_count(&self) -> usize {
        self.handshake_count.load(Ordering::SeqCst)
    }

    /// Every recorded `response.create` frame, in receipt order.
    pub fn frames(&self) -> Vec<RecordedFrame> {
        self.frames.lock().unwrap().clone()
    }

    /// The most recently received frame's `input` array length, if any frame has been recorded.
    pub fn last_frame_input_len(&self) -> Option<usize> {
        self.frames.lock().unwrap().last().map(|f| f.input_len)
    }

    /// The most recently received frame's `previous_response_id`, if any frame has been recorded
    /// (flattens "no frame yet" and "last frame had no anchor" to the same `None` — use
    /// [`Self::frames`] when the distinction matters).
    pub fn last_frame_anchor(&self) -> Option<String> {
        self.frames
            .lock()
            .unwrap()
            .last()
            .and_then(|f| f.previous_response_id.clone())
    }

    fn record_frame(&self, body: &Value) {
        let previous_response_id = body
            .get("previous_response_id")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let input_len = body
            .get("input")
            .and_then(|v| v.as_array())
            .map(Vec::len)
            .unwrap_or(0);
        self.frames.lock().unwrap().push(RecordedFrame {
            previous_response_id,
            input_len,
        });
    }

    /// Pop the next scripted turn, leaving the last entry in place once the script is down to one
    /// (so it repeats for any further turns instead of panicking on an empty `Vec`).
    fn next_turn(&self) -> ScriptedTurn {
        let mut script = self.script.lock().unwrap();
        if script.len() > 1 {
            script.remove(0)
        } else {
            script[0].clone()
        }
    }

    /// Bind an ephemeral port, serve in a background task, and return the base URL
    /// (`ws://127.0.0.1:PORT`, no path — callers connect to `{base}/responses`, mirroring the real
    /// upstream's URL shape per `WS-GROUND-TRUTH-CODEX.md` §1).
    pub async fn spawn(self) -> String {
        let app = Router::new()
            .route("/responses", get(ws_handler))
            .with_state(self);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("ws://{addr}")
    }
}

async fn ws_handler(
    State(mock): State<MockWsUpstream>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, mock))
}

async fn handle_socket(mut socket: WebSocket, mock: MockWsUpstream) {
    // Counted here, inside the upgrade closure, so it reflects genuinely ESTABLISHED WS sessions
    // rather than merely-attempted upgrade requests.
    mock.handshake_count.fetch_add(1, Ordering::SeqCst);

    loop {
        let msg = match socket.recv().await {
            Some(Ok(msg)) => msg,
            _ => return, // client disconnected or errored
        };
        let text = match msg {
            Message::Text(t) => t,
            Message::Close(_) => return,
            _ => continue,
        };
        let body: Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(_) => continue,
        };
        mock.record_frame(&body);

        match mock.next_turn() {
            ScriptedTurn::Turn { events } => {
                for e in &events {
                    if socket.send(Message::Text(e.clone().into())).await.is_err() {
                        return;
                    }
                }
                let n = mock.id_counter.fetch_add(1, Ordering::SeqCst) + 1;
                let id = format!("resp_{n}");
                let completed =
                    json!({"type":"response.completed","response":{"id": id}}).to_string();
                if socket.send(Message::Text(completed.into())).await.is_err() {
                    return;
                }
                // Loop back around: the socket stays open for a POSSIBLE next turn (the whole
                // point of the connection-reuse proof) instead of closing after one exchange.
            }
            ScriptedTurn::Failed { code, message } => {
                let frame = json!({
                    "type": "response.failed",
                    "response": {"error": {"code": code, "message": message}},
                })
                .to_string();
                let _ = socket.send(Message::Text(frame.into())).await;
            }
            ScriptedTurn::ErrorEnvelope {
                status,
                code,
                message,
                headers,
            } => {
                let headers_obj: serde_json::Map<String, Value> = headers
                    .into_iter()
                    .map(|(k, v)| (k, Value::String(v)))
                    .collect();
                let frame = json!({
                    "type": "error",
                    "status": status,
                    "error": {"code": code, "message": message},
                    "headers": headers_obj,
                })
                .to_string();
                let _ = socket.send(Message::Text(frame.into())).await;
            }
            ScriptedTurn::CloseMidStream {
                events_before_close,
            } => {
                for e in &events_before_close {
                    if socket.send(Message::Text(e.clone().into())).await.is_err() {
                        return;
                    }
                }
                let _ = socket.send(Message::Close(None)).await;
                return;
            }
            ScriptedTurn::Stall => {
                // Never respond. Still race the client's own recv so this task doesn't outlive a
                // client that has already gone away.
                tokio::select! {
                    _ = std::future::pending::<()>() => {}
                    _ = socket.recv() => {}
                }
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use std::time::Duration;
    use tokio_tungstenite::tungstenite::Message as TMessage;

    fn create_frame(input_items: usize, previous_response_id: Option<&str>) -> String {
        let input: Vec<Value> = (0..input_items)
            .map(|i| json!({"type": "message", "role": "user", "content": format!("item-{i}")}))
            .collect();
        let mut body = json!({
            "type": "response.create",
            "model": "gpt-5.6-sol",
            "input": input,
        });
        if let Some(p) = previous_response_id {
            body["previous_response_id"] = json!(p);
        }
        body.to_string()
    }

    async fn connect(base: &str) -> tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    > {
        let (ws, _resp) = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect("connect");
        ws
    }

    #[tokio::test]
    async fn normal_turn_yields_scripted_events_then_completed_with_id() {
        let mock = MockWsUpstream::new(ScriptedTurn::normal(vec![
            json!({"type": "response.output_text.delta", "delta": "hi"}).to_string(),
        ]));
        let base = mock.clone().spawn().await;
        let mut ws = connect(&base).await;

        ws.send(TMessage::Text(create_frame(1, None).into()))
            .await
            .unwrap();

        let mut saw_delta = false;
        let mut resp_id = None;
        while let Some(Ok(TMessage::Text(t))) = ws.next().await {
            let v: Value = serde_json::from_str(&t).unwrap();
            if v["type"] == "response.output_text.delta" {
                saw_delta = true;
            }
            if v["type"] == "response.completed" {
                resp_id = v["response"]["id"].as_str().map(str::to_string);
                break;
            }
        }

        assert!(saw_delta, "expected the scripted delta event to arrive");
        assert_eq!(resp_id.as_deref(), Some("resp_1"));
        assert_eq!(mock.handshake_count(), 1);
        assert_eq!(mock.last_frame_input_len(), Some(1));
        assert_eq!(mock.last_frame_anchor(), None);
    }

    #[tokio::test]
    async fn two_turns_on_one_socket_reuse_the_connection() {
        let mock = MockWsUpstream::scripted(vec![
            ScriptedTurn::normal(vec![]),
            ScriptedTurn::normal(vec![]),
        ]);
        let base = mock.clone().spawn().await;
        let mut ws = connect(&base).await;

        // Turn 1: full input, no anchor.
        ws.send(TMessage::Text(create_frame(3, None).into()))
            .await
            .unwrap();
        let mut first_id = None;
        while let Some(Ok(TMessage::Text(t))) = ws.next().await {
            let v: Value = serde_json::from_str(&t).unwrap();
            if v["type"] == "response.completed" {
                first_id = v["response"]["id"].as_str().map(str::to_string);
                break;
            }
        }
        assert_eq!(first_id.as_deref(), Some("resp_1"));

        // Turn 2, SAME socket: an anchored delta of just one new item.
        ws.send(TMessage::Text(create_frame(1, Some("resp_1")).into()))
            .await
            .unwrap();
        let mut second_id = None;
        while let Some(Ok(TMessage::Text(t))) = ws.next().await {
            let v: Value = serde_json::from_str(&t).unwrap();
            if v["type"] == "response.completed" {
                second_id = v["response"]["id"].as_str().map(str::to_string);
                break;
            }
        }
        assert_eq!(second_id.as_deref(), Some("resp_2"));

        // The proof: ONE handshake for TWO turns.
        assert_eq!(mock.handshake_count(), 1);

        let frames = mock.frames();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].previous_response_id, None);
        assert_eq!(frames[0].input_len, 3);
        assert_eq!(frames[1].previous_response_id, Some("resp_1".to_string()));
        assert_eq!(frames[1].input_len, 1);
    }

    #[tokio::test]
    async fn previous_response_not_found_is_a_terminal_response_failed() {
        let mock = MockWsUpstream::new(ScriptedTurn::previous_response_not_found());
        let base = mock.clone().spawn().await;
        let mut ws = connect(&base).await;

        ws.send(TMessage::Text(create_frame(1, Some("resp_dead")).into()))
            .await
            .unwrap();

        let TMessage::Text(t) = ws.next().await.unwrap().unwrap() else {
            panic!("expected a text frame");
        };
        let v: Value = serde_json::from_str(&t).unwrap();
        assert_eq!(v["type"], "response.failed");
        assert_eq!(v["response"]["error"]["code"], "previous_response_not_found");
        assert_eq!(
            v["response"]["error"]["message"],
            "anchor not resumable; resend full history"
        );
    }

    #[tokio::test]
    async fn connection_limit_reached_is_a_wrapped_error_envelope() {
        let mock = MockWsUpstream::new(ScriptedTurn::connection_limit_reached(409));
        let base = mock.clone().spawn().await;
        let mut ws = connect(&base).await;

        ws.send(TMessage::Text(create_frame(1, None).into()))
            .await
            .unwrap();

        let TMessage::Text(t) = ws.next().await.unwrap().unwrap() else {
            panic!("expected a text frame");
        };
        let v: Value = serde_json::from_str(&t).unwrap();
        assert_eq!(v["type"], "error");
        assert_eq!(v["status"], 409);
        assert_eq!(v["error"]["code"], "websocket_connection_limit_reached");
        assert!(v["headers"].is_object());
    }

    #[tokio::test]
    async fn rate_limited_429_carries_retry_after_in_the_envelope_headers() {
        let mock = MockWsUpstream::new(ScriptedTurn::rate_limited_429(37));
        let base = mock.clone().spawn().await;
        let mut ws = connect(&base).await;

        ws.send(TMessage::Text(create_frame(1, None).into()))
            .await
            .unwrap();

        let TMessage::Text(t) = ws.next().await.unwrap().unwrap() else {
            panic!("expected a text frame");
        };
        let v: Value = serde_json::from_str(&t).unwrap();
        assert_eq!(v["type"], "error");
        assert_eq!(v["status"], 429);
        assert_eq!(v["headers"]["retry-after"], "37");
    }

    #[tokio::test]
    async fn close_mid_stream_ends_before_any_terminal_frame() {
        let mock = MockWsUpstream::new(ScriptedTurn::close_mid_stream(vec![json!({
            "type": "response.output_text.delta",
            "delta": "partial",
        })
        .to_string()]));
        let base = mock.clone().spawn().await;
        let mut ws = connect(&base).await;

        ws.send(TMessage::Text(create_frame(1, None).into()))
            .await
            .unwrap();

        let mut saw_terminal = false;
        let closed;
        loop {
            match ws.next().await {
                Some(Ok(TMessage::Text(t))) => {
                    let v: Value = serde_json::from_str(&t).unwrap();
                    if v["type"] == "response.completed" || v["type"] == "response.failed" {
                        saw_terminal = true;
                    }
                }
                Some(Ok(TMessage::Close(_))) | None => {
                    closed = true;
                    break;
                }
                Some(Ok(_)) => {}
                Some(Err(_)) => {
                    closed = true;
                    break;
                }
            }
        }
        assert!(!saw_terminal, "must close BEFORE any terminal frame");
        assert!(closed, "the socket must actually close");
        assert_eq!(mock.frames().len(), 1);
    }

    #[tokio::test]
    async fn stall_accepts_the_frame_but_never_responds() {
        let mock = MockWsUpstream::new(ScriptedTurn::stall());
        let base = mock.clone().spawn().await;
        let mut ws = connect(&base).await;

        ws.send(TMessage::Text(create_frame(1, None).into()))
            .await
            .unwrap();

        // The frame IS recorded even though nothing comes back.
        // (Poll a few times since recording happens on the server task, not synchronously with send.)
        for _ in 0..20 {
            if mock.frames().len() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(mock.frames().len(), 1);
        assert_eq!(mock.last_frame_input_len(), Some(1));

        let result = tokio::time::timeout(Duration::from_millis(200), ws.next()).await;
        assert!(
            result.is_err(),
            "a stalled mock must never send a reply before the client gives up"
        );
    }
}
