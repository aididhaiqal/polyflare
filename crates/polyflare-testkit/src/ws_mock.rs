//! Test support: a scriptable WebSocket mock upstream, mirroring [`crate::MockUpstream`]'s idiom
//! (`spawn() -> String`, `Arc<Mutex<..>>` recorders, scripted-response modes) but speaking the
//! codex WS wire protocol instead of HTTP-SSE.
//!
//! **Evidence provenance (per `WS-GROUND-TRUTH-CODEX.md`'s scope note — do not conflate the two):**
//! most shapes here are *source facts*, cited to `docs/WS-GROUND-TRUTH-CODEX.md` §3 (framing) — the
//! generic wrapped error envelope (`{"type":"error","status":u16,"error":{"code","message"},
//! "headers":{...}}`) and the terminal `response.completed`/`response.failed` shapes. One shape,
//! [`ScriptedTurn::previous_response_not_found`], is instead a *live-measured fact*: it is NOT
//! `response.failed` (an earlier, corrected revision of the ground-truth doc got this wrong by
//! inferring server behavior from the client's lack of a special case — see that doc's §5 and its
//! scope note for the exact trap). Its actual shape comes from probing the real backend
//! (`docs/TRANSPORT-FINDINGS-2026-07-17.md` §3, `crates/polyflare-server/examples/ws_wedge_demo.rs`),
//! not from `watchdog.rs`'s `SIGNAL_SSE` — that constant is a different thing, the HTTP-SSE frame
//! PolyFlare *synthesizes downstream to its own client*, itself flagged VERIFY-at-implementation
//! (`watchdog.rs:29-33`), and not an authority for this mock's upstream-facing shape.
//!
//! Content-safety: [`RecordedFrame`] retains only structural facts (an anchor id — an opaque
//! backend-issued identifier, not conversation content — and an item count, plus the replayed
//! `x-codex-turn-state` — itself a content-free server-issued routing token, never conversation
//! content), never the frame's `input` payload. Nothing here derives `Debug` over a full frame or
//! request body. The ONE exception is the OPT-IN raw-frame capture ([`MockWsUpstream::
//! capturing_raw_frames`], OFF by default): the WS-downstream **relay** forwards the client's frame
//! byte-for-byte, and its VERBATIM-fidelity test must assert the mock received those exact bytes
//! (key order / whitespace preserved), so an explicitly opted-in test may stash the raw text of the
//! synthetic frames it itself constructed. The default mock keeps the content-free discipline.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::{HeaderName, HeaderValue, StatusCode};
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
    /// The `client_metadata["x-codex-turn-state"]` this frame carried, if any — the replayed
    /// server-issued routing token (`ws::executor::plan_and_build_locked`). A content-free routing
    /// token, never conversation content. `None` when the frame carried no turn-state key. Lets a
    /// test prove the per-turn replay semantics: the upgrade token appears on the FIRST turn over a
    /// socket and is ABSENT on every later turn (it is a one-shot consumed by the first turn).
    pub turn_state: Option<String>,
}

/// A scripted response for the next (or every, once repeating) `response.create` turn a
/// [`MockWsUpstream`] receives. Every variant maps directly to a frame shape in
/// `docs/WS-GROUND-TRUTH-CODEX.md` §3.
#[derive(Clone)]
pub enum ScriptedTurn {
    /// A normal turn: emit `events` verbatim as WS text frames, then a terminal
    /// `response.completed` carrying a freshly generated `resp_N` id.
    Turn { events: Vec<String> },
    /// A normal turn whose terminal `response.completed` includes the numeric usage object emitted
    /// by the real Codex WS API. Kept separate from `Turn` so existing relay tests retain their
    /// minimal historical fixture while telemetry tests can exercise the complete wire shape.
    TurnWithUsage {
        events: Vec<String>,
        input_tokens: i64,
        output_tokens: i64,
        cached_input_tokens: i64,
        reasoning_tokens: i64,
    },
    /// A terminal `response.failed` carrying `error.code` / `error.message` — no preceding
    /// `events`. This is a genuine terminal-failure shape (ground truth §3: `ContextWindowExceeded`
    /// / `QuotaExceeded` / etc.) — NOT what a dead anchor emits; see
    /// [`ScriptedTurn::previous_response_not_found`] for that (it is a wrapped error envelope, not
    /// this variant).
    Failed { code: String, message: String },
    /// A terminal `response.incomplete`. The socket remains open for later turns, matching the
    /// reusable upstream connection contract.
    Incomplete { reason: String },
    /// The WS-only wrapped error envelope (ground truth §3):
    /// `{"type":"error","status":u16,"error":{"code","message",..error_extra},"headers":{...}}`.
    ErrorEnvelope {
        status: u16,
        code: String,
        message: String,
        /// Extra fields nested inside `error` beyond `code`/`message` (e.g. the live-probed
        /// `type`/`param` on [`ScriptedTurn::previous_response_not_found`]). Empty for the other
        /// envelope constructors since ground truth doesn't cite these fields for them.
        error_extra: Vec<(String, String)>,
        headers: Vec<(String, String)>,
    },
    /// Emit ordinary response events before a wrapped error envelope. This models an error after
    /// client-visible progress, where a proxy must not retry or replay the turn.
    ErrorAfterEvents {
        events: Vec<String>,
        status: u16,
        code: String,
        message: String,
    },
    /// Emit `events_before_close` (non-terminal — no `response.completed`/`.failed`), then close
    /// the socket. Models "close mid-stream, before any terminal frame".
    CloseMidStream { events_before_close: Vec<String> },
    /// A COMPLETE normal turn (events + terminal `response.completed`), after which the server
    /// closes the socket — a between-turns upstream death (idle reap / server-side teardown while
    /// parked), as opposed to [`ScriptedTurn::CloseMidStream`]'s in-turn drop.
    TurnThenClose { events: Vec<String> },
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

    /// A normal, COMPLETED turn after which the server closes the socket — a between-turns
    /// upstream death. Lets a test prove the relay's honest-close mirror: the downstream must
    /// close too, instead of hiding a dead anchor behind a still-open client socket.
    pub fn normal_then_close(events: Vec<String>) -> Self {
        ScriptedTurn::TurnThenClose { events }
    }

    /// A successful turn with final token accounting in `response.completed`.
    pub fn normal_with_usage(
        events: Vec<String>,
        input_tokens: i64,
        output_tokens: i64,
        cached_input_tokens: i64,
        reasoning_tokens: i64,
    ) -> Self {
        ScriptedTurn::TurnWithUsage {
            events,
            input_tokens,
            output_tokens,
            cached_input_tokens,
            reasoning_tokens,
        }
    }

    /// The wrapped error envelope a dead `previous_response_id` actually gets, per the
    /// **live-measured fact** in `docs/TRANSPORT-FINDINGS-2026-07-17.md` §3 (confirmed against the
    /// real backend by `crates/polyflare-server/examples/ws_wedge_demo.rs`, both cross-account and
    /// same-account fresh-reattach) — NOT a terminal `response.failed`. An earlier revision of
    /// `WS-GROUND-TRUTH-CODEX.md` (and of this mock) asserted `response.failed`; that was an
    /// inference from the client having no special-case handling, contradicted by the probe, and
    /// corrected 2026-07-17 (see that doc's §5). The captured shape:
    /// ```json
    /// {"type":"error","error":{"type":"invalid_request_error","code":"previous_response_not_found",
    ///  "message":"Previous response with id 'resp_...' not found.","param":"previous_response_id"},"status":400}
    /// ```
    /// `dead_anchor` is interpolated into the message purely for realism (the real server echoes
    /// the specific dead id back) — callers must NOT assert on the message string. The `code`
    /// field is the only verified, stable part of this shape; assert on it alone, matching the
    /// existing precedent for `watchdog.rs`'s `SIGNAL_SSE` (its own tests key off the `code`
    /// substring for the identical reason: message text is provisional).
    pub fn previous_response_not_found(dead_anchor: impl Into<String>) -> Self {
        let dead_anchor = dead_anchor.into();
        ScriptedTurn::ErrorEnvelope {
            status: 400,
            code: "previous_response_not_found".to_string(),
            message: format!("Previous response with id '{dead_anchor}' not found."),
            error_extra: vec![
                ("type".to_string(), "invalid_request_error".to_string()),
                ("param".to_string(), "previous_response_id".to_string()),
            ],
            headers: Vec::new(),
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
            error_extra: Vec::new(),
            headers: Vec::new(),
        }
    }

    /// The wrapped error envelope, pre-filled for a 429 carrying `Retry-After` inside the
    /// envelope's own `headers` map (ground truth §3's `"headers":{...}` field) rather than a real
    /// HTTP response header — this is the shape Task 7's 429 test parses `retry_after` out of.
    ///
    /// **UNVERIFIED surface, disclosed:** neither the `"retry-after"` header key's name/casing nor
    /// `error.code == "rate_limit_exceeded"` is cited anywhere in ground truth — §3 only documents
    /// the envelope's `"headers":{...}` field generically, never a specific key, and never a 429
    /// `code` string. Both are plausible-but-invented placeholders pending a live capture of a real
    /// 429 over WS (nothing like `ws_wedge_demo.rs` has forced one yet). If a future capture shows a
    /// different key/casing or code, this is the one spot that needs to change.
    pub fn rate_limited_429(retry_after_secs: u64) -> Self {
        ScriptedTurn::ErrorEnvelope {
            status: 429,
            code: "rate_limit_exceeded".to_string(),
            message: "rate limit exceeded".to_string(),
            error_extra: Vec::new(),
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

    pub fn incomplete(reason: impl Into<String>) -> Self {
        ScriptedTurn::Incomplete {
            reason: reason.into(),
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
    handshake_attempt: Arc<AtomicUsize>,
    frames: Arc<Mutex<Vec<RecordedFrame>>>,
    handshake_authorizations: Arc<Mutex<Vec<Option<String>>>>,
    id_counter: Arc<AtomicU32>,
    /// When set, every upgrade attempt is answered with a plain HTTP 426 instead of upgrading —
    /// see [`Self::rejecting_handshake`].
    reject_handshake: bool,
    /// When set, the WS UPGRADE (101) response carries this value as its `x-codex-turn-state`
    /// header — the PRIMARY turn-state capture path the real backend uses
    /// (`responses_websocket.rs:529-535`). A content-free server-issued routing token. Lets a test
    /// drive the per-turn "consume the upgrade token on the first turn, send nothing on later turns"
    /// replay path end to end. `None` (the default) sends no such header. See
    /// [`Self::with_upgrade_turn_state`].
    upgrade_turn_state: Option<String>,
    /// Optional per-handshake upgrade-response headers. The final entry repeats after the
    /// sequence is exhausted, matching the scripted-turn behavior. This is test-only support for
    /// proving that a transparent proxy reconnect does not hide a changed upstream handshake
    /// contract from its already-upgraded downstream client.
    upgrade_response_headers: Arc<Vec<Vec<(String, String)>>>,
    /// Optional authorization requirement per handshake attempt. `None` accepts any bearer; a
    /// `Some` value rejects mismatches with HTTP 401 before upgrading. The final entry repeats.
    handshake_required_authorizations: Arc<Vec<Option<String>>>,
    /// When set (opt-in, via [`Self::capturing_raw_frames`]), [`handle_socket`] stashes each received
    /// frame's RAW text — byte-for-byte as it arrived on the wire — into [`Self::raw_frames`], so a
    /// relay VERBATIM-fidelity test can assert the proxy forwarded the client's frame UNCHANGED (key
    /// order / whitespace preserved, no serde reparse). OFF by default so the mock's content-free
    /// `RecordedFrame`-only recording is the norm (see the module content-safety note). `false` here.
    capture_raw_frames: bool,
    /// The raw received-frame text buffer, populated only when [`Self::capture_raw_frames`] is on.
    /// Read via [`Self::raw_frames`].
    raw_frames: Arc<Mutex<Vec<String>>>,
    /// Count of WS protocol `Ping` frames received across every socket this mock has served —
    /// read via [`Self::ping_count`]. Lets a keepalive test prove the relay actually pings a
    /// parked upstream (axum auto-pongs; the mock only needs to observe).
    ping_count: Arc<AtomicUsize>,
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
            handshake_attempt: Arc::new(AtomicUsize::new(0)),
            frames: Arc::new(Mutex::new(Vec::new())),
            handshake_authorizations: Arc::new(Mutex::new(Vec::new())),
            id_counter: Arc::new(AtomicU32::new(0)),
            reject_handshake: false,
            upgrade_turn_state: None,
            upgrade_response_headers: Arc::new(Vec::new()),
            handshake_required_authorizations: Arc::new(Vec::new()),
            capture_raw_frames: false,
            raw_frames: Arc::new(Mutex::new(Vec::new())),
            ping_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// How many WS protocol `Ping` frames have arrived across every socket served so far.
    pub fn ping_count(&self) -> usize {
        self.ping_count.load(Ordering::SeqCst)
    }

    /// Answer every WS UPGRADE with `x-codex-turn-state: {token}` on the 101 response header — the
    /// PRIMARY turn-state capture path the real backend uses (`responses_websocket.rs:529-535`,
    /// mirrored by `WsConn::connect_detailed_with_timeout`). A builder-style override so a test can
    /// prove the per-turn replay semantics: the captured upgrade token is a ONE-SHOT consumed by the
    /// FIRST turn on the socket, so it appears in that turn's frame `client_metadata` and is ABSENT
    /// from every later turn's frame on the same reused socket. Content-free routing token.
    pub fn with_upgrade_turn_state(mut self, token: impl Into<String>) -> Self {
        self.upgrade_turn_state = Some(token.into());
        self
    }

    /// Configure allowlisted upgrade-response metadata per accepted handshake. When more
    /// handshakes occur than entries, the last entry repeats.
    pub fn with_upgrade_response_headers(mut self, sequence: Vec<Vec<(String, String)>>) -> Self {
        assert!(
            !sequence.is_empty(),
            "upgrade-response header sequence must not be empty"
        );
        self.upgrade_response_headers = Arc::new(sequence);
        self
    }

    /// Configure accepted Authorization values by handshake attempt. The final entry repeats.
    pub fn with_handshake_authorizations(mut self, sequence: Vec<Option<String>>) -> Self {
        assert!(
            !sequence.is_empty(),
            "handshake authorization sequence must not be empty"
        );
        self.handshake_required_authorizations = Arc::new(sequence);
        self
    }

    /// Opt in to stashing every received frame's RAW text (byte-for-byte as it arrived on the wire)
    /// so a WS-downstream **relay** test can prove VERBATIM fidelity: that the proxy forwarded the
    /// client's frame UNCHANGED — same key order, same interior whitespace — with no serde reparse
    /// (which would sort keys / drop formatting and break the codex wire fingerprint). OFF by default
    /// (the mock's content-free `RecordedFrame`-only recording is the norm); a test opts in only to
    /// assert on the synthetic frames it itself constructed. Read the buffer via [`Self::raw_frames`].
    pub fn capturing_raw_frames(mut self) -> Self {
        self.capture_raw_frames = true;
        self
    }

    /// Every received frame's RAW text, in receipt order — populated only when
    /// [`Self::capturing_raw_frames`] was set (empty otherwise). A verbatim test asserts this equals
    /// exactly the bytes it sent.
    pub fn raw_frames(&self) -> Vec<String> {
        self.raw_frames.lock().unwrap().clone()
    }

    /// A mock that answers every WS upgrade attempt with a plain HTTP 426 (`Upgrade Required`)
    /// instead of upgrading — no socket is ever established, so [`Self::handshake_count`] stays at
    /// `0`. Models `WS-GROUND-TRUTH-CODEX.md` §5's ONLY `FallbackToHttp` trigger: HTTP 426 at
    /// handshake time, checked before any frame is sent (`client.rs:1596-1600`). Task 7/SPEC-M5's
    /// fallback table needs this exercised ("handshake 426 → HTTP-SSE for this session"). The
    /// script is never consulted since no `response.create` can ever arrive; a placeholder turn is
    /// still required to satisfy [`Self::scripted`]'s non-empty invariant.
    pub fn rejecting_handshake() -> Self {
        let mut mock = Self::scripted(vec![ScriptedTurn::stall()]);
        mock.reject_handshake = true;
        mock
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

    /// Authorization header observed on each accepted handshake, in attempt order.
    pub fn handshake_authorizations(&self) -> Vec<Option<String>> {
        self.handshake_authorizations.lock().unwrap().clone()
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
        // The replayed content-free routing token, if this frame carried one — read out of the
        // frame's `client_metadata` (where the WS path replays it, never as a top-level field).
        let turn_state = body
            .pointer("/client_metadata/x-codex-turn-state")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        self.frames.lock().unwrap().push(RecordedFrame {
            previous_response_id,
            input_len,
            turn_state,
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
    headers: axum::http::HeaderMap,
) -> axum::response::Response {
    if mock.reject_handshake {
        // Ground truth §5: `FallbackToHttp`'s ONLY trigger is HTTP 426 at handshake time, checked
        // BEFORE any frame is sent. Answering here — before `on_upgrade` — means no socket is ever
        // established for this connection attempt.
        return (StatusCode::UPGRADE_REQUIRED, "Upgrade Required").into_response();
    }
    let attempt = mock.handshake_attempt.fetch_add(1, Ordering::SeqCst);
    let authorization = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    mock.handshake_authorizations
        .lock()
        .unwrap()
        .push(authorization.clone());
    let required_authorization = mock
        .handshake_required_authorizations
        .get(attempt)
        .or_else(|| mock.handshake_required_authorizations.last())
        .cloned()
        .flatten();
    if required_authorization
        .as_ref()
        .is_some_and(|required| authorization.as_ref() != Some(required))
    {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let upgrade_response_headers = mock
        .upgrade_response_headers
        .get(attempt)
        .or_else(|| mock.upgrade_response_headers.last())
        .cloned()
        .unwrap_or_default();
    let upgrade_turn_state = mock.upgrade_turn_state.clone();
    let mut response = ws
        .on_upgrade(move |socket| handle_socket(socket, mock))
        .into_response();
    // Primary turn-state capture path (`responses_websocket.rs:529-535`): stamp the server-issued
    // `x-codex-turn-state` onto the 101 UPGRADE response so `WsConn::connect_detailed_with_timeout`
    // captures it into `upgrade_turn_state`. Content-free routing token. Only when configured via
    // `with_upgrade_turn_state`.
    if let Some(ts) = upgrade_turn_state {
        if let Ok(value) = HeaderValue::from_str(&ts) {
            response
                .headers_mut()
                .insert(HeaderName::from_static("x-codex-turn-state"), value);
        }
    }
    for (name, value) in upgrade_response_headers {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(&value),
        ) {
            response.headers_mut().insert(name, value);
        }
    }
    response
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
            Message::Ping(_) => {
                // Observed for keepalive tests (axum already auto-pongs at the protocol layer).
                mock.ping_count.fetch_add(1, Ordering::SeqCst);
                continue;
            }
            _ => continue,
        };
        // Opt-in VERBATIM proof: stash the raw wire bytes BEFORE any parse, so a relay test can
        // assert the frame arrived byte-identical (key order / whitespace intact). Off by default.
        if mock.capture_raw_frames {
            mock.raw_frames
                .lock()
                .unwrap()
                .push(text.as_str().to_owned());
        }
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
            ScriptedTurn::TurnThenClose { events } => {
                for e in &events {
                    if socket.send(Message::Text(e.clone().into())).await.is_err() {
                        return;
                    }
                }
                let n = mock.id_counter.fetch_add(1, Ordering::SeqCst) + 1;
                let id = format!("resp_{n}");
                let completed =
                    json!({"type":"response.completed","response":{"id": id}}).to_string();
                let _ = socket.send(Message::Text(completed.into())).await;
                // Between-turns server-side death: the turn completed cleanly, THEN the server
                // closes. `return` drops the socket (axum sends the close handshake).
                return;
            }
            ScriptedTurn::TurnWithUsage {
                events,
                input_tokens,
                output_tokens,
                cached_input_tokens,
                reasoning_tokens,
            } => {
                for e in &events {
                    if socket.send(Message::Text(e.clone().into())).await.is_err() {
                        return;
                    }
                }
                let n = mock.id_counter.fetch_add(1, Ordering::SeqCst) + 1;
                let id = format!("resp_{n}");
                let completed = json!({
                    "type": "response.completed",
                    "response": {
                        "id": id,
                        "usage": {
                            "input_tokens": input_tokens,
                            "output_tokens": output_tokens,
                            "input_tokens_details": {"cached_tokens": cached_input_tokens},
                            "output_tokens_details": {"reasoning_tokens": reasoning_tokens}
                        }
                    }
                })
                .to_string();
                if socket.send(Message::Text(completed.into())).await.is_err() {
                    return;
                }
            }
            // `Failed` and `ErrorEnvelope` both deliberately leave the socket OPEN afterward (the
            // loop falls through, no `return`): this mock only ever models client-driven reconnect
            // — ground truth §2's "ordinary reconnect" — never server-side termination. A caller
            // that wants the socket to actually close should script `CloseMidStream` instead.
            ScriptedTurn::Failed { code, message } => {
                let frame = json!({
                    "type": "response.failed",
                    "response": {"error": {"code": code, "message": message}},
                })
                .to_string();
                let _ = socket.send(Message::Text(frame.into())).await;
            }
            ScriptedTurn::Incomplete { reason } => {
                let frame = json!({
                    "type": "response.incomplete",
                    "response": {
                        "id": "resp_incomplete",
                        "incomplete_details": {"reason": reason}
                    },
                })
                .to_string();
                let _ = socket.send(Message::Text(frame.into())).await;
            }
            ScriptedTurn::ErrorEnvelope {
                status,
                code,
                message,
                error_extra,
                headers,
            } => {
                let headers_obj: serde_json::Map<String, Value> = headers
                    .into_iter()
                    .map(|(k, v)| (k, Value::String(v)))
                    .collect();
                let mut error_obj = serde_json::Map::new();
                error_obj.insert("code".to_string(), Value::String(code));
                error_obj.insert("message".to_string(), Value::String(message));
                for (k, v) in error_extra {
                    error_obj.insert(k, Value::String(v));
                }
                let frame = json!({
                    "type": "error",
                    "status": status,
                    "error": error_obj,
                    "headers": headers_obj,
                })
                .to_string();
                let _ = socket.send(Message::Text(frame.into())).await;
            }
            ScriptedTurn::ErrorAfterEvents {
                events,
                status,
                code,
                message,
            } => {
                for event in events {
                    if socket.send(Message::Text(event.into())).await.is_err() {
                        return;
                    }
                }
                let frame = json!({
                    "type": "error",
                    "status": status,
                    "error": {"code": code, "message": message},
                    "headers": {},
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

    async fn connect(
        base: &str,
    ) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>
    {
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
    async fn previous_response_not_found_is_a_wrapped_error_envelope_with_status_400() {
        // Live-probe-backed (TRANSPORT-FINDINGS-2026-07-17.md §3): a dead anchor is the wrapped
        // error envelope with status 400 — NOT a terminal `response.failed`.
        let mock = MockWsUpstream::new(ScriptedTurn::previous_response_not_found("resp_dead"));
        let base = mock.clone().spawn().await;
        let mut ws = connect(&base).await;

        ws.send(TMessage::Text(create_frame(1, Some("resp_dead")).into()))
            .await
            .unwrap();

        let TMessage::Text(t) = ws.next().await.unwrap().unwrap() else {
            panic!("expected a text frame");
        };
        let v: Value = serde_json::from_str(&t).unwrap();
        assert_eq!(v["type"], "error");
        assert_eq!(v["status"], 400);
        // Assert the `code` field ONLY — same precedent as `watchdog.rs`'s SIGNAL_SSE tests
        // (message text is provisional/caller-specific, never asserted verbatim).
        assert_eq!(v["error"]["code"], "previous_response_not_found");
    }

    #[tokio::test]
    async fn third_turn_repeats_the_last_scripted_entry_with_real_values() {
        // `next_turn()`'s "last entry repeats past script exhaustion" is what Task 7's
        // bounded-retry/reconnect tests depend on hardest — this proves it with a 2-entry script
        // driven for a 3rd turn on the SAME socket.
        let mock = MockWsUpstream::scripted(vec![
            ScriptedTurn::normal(vec![]),
            ScriptedTurn::rate_limited_429(5),
        ]);
        let base = mock.clone().spawn().await;
        let mut ws = connect(&base).await;

        // Turn 1 consumes entry 0 (the only entry that ever gets POPPED, since the script drops
        // to length 1 afterward).
        ws.send(TMessage::Text(create_frame(1, None).into()))
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

        // Turn 2: the script is down to its last entry (`rate_limited_429`) — served without
        // being removed.
        ws.send(TMessage::Text(create_frame(1, Some("resp_1")).into()))
            .await
            .unwrap();
        let TMessage::Text(t2) = ws.next().await.unwrap().unwrap() else {
            panic!("expected a text frame");
        };
        let v2: Value = serde_json::from_str(&t2).unwrap();
        assert_eq!(v2["status"], 429);
        assert_eq!(v2["headers"]["retry-after"], "5");

        // Turn 3 — PAST script exhaustion, the case this test exists for. `next_turn()` must
        // repeat the LAST entry (`rate_limited_429`) again, not panic on an empty `Vec` and not
        // wrongly cycle back to entry 0 (which would emit a SECOND `response.completed` instead).
        ws.send(TMessage::Text(create_frame(1, Some("resp_1")).into()))
            .await
            .unwrap();
        let TMessage::Text(t3) = ws.next().await.unwrap().unwrap() else {
            panic!("expected a text frame");
        };
        let v3: Value = serde_json::from_str(&t3).unwrap();
        assert_eq!(v3["type"], "error");
        assert_eq!(v3["status"], 429);
        assert_eq!(v3["error"]["code"], "rate_limit_exceeded");
        assert_eq!(v3["headers"]["retry-after"], "5");

        // All three turns happened on the SAME socket — the repeat is not a reconnect.
        assert_eq!(mock.handshake_count(), 1);
        assert_eq!(mock.frames().len(), 3);
    }

    #[tokio::test]
    async fn rejecting_handshake_returns_426_and_never_upgrades() {
        let mock = MockWsUpstream::rejecting_handshake();
        let base = mock.clone().spawn().await;

        let err = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect_err("the mock must refuse the upgrade, not accept it");
        let tokio_tungstenite::tungstenite::Error::Http(response) = err else {
            panic!("expected an HTTP-level rejection from the failed handshake");
        };
        assert_eq!(response.status().as_u16(), 426);
        // No socket was ever established.
        assert_eq!(mock.handshake_count(), 0);
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
