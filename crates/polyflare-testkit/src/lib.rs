//! Test support: scriptable mock upstreams for e2e tests.

pub mod ws_mock;
pub use ws_mock::{MockWsUpstream, RecordedFrame, ScriptedTurn};

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Json, State};
use axum::http::{header, HeaderMap, Method, StatusCode, Uri};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, post};
use axum::Router;
use bytes::Bytes;
use futures_util::stream::{self, StreamExt};
use tokio::net::TcpListener;

/// The response behavior of a `MockUpstream`.
#[derive(Clone)]
enum MockMode {
    /// Legacy: always stream the fixed `events` as SSE `data:` frames (with keep-alive). Never
    /// injects a `response.id`. Used by the M1/M2 pass-through tests unchanged.
    Scripted,
    /// Emit `response.created`(resp_N) + `events` + `response.completed`(resp_N), generating a
    /// fresh `resp_N` id per response. If `silent_on_anchor` and the body carries
    /// `previous_response_id`, instead return 200 headers then a never-yielding body (no
    /// keep-alive) — the wedge (silence-after-accept).
    WithIds { silent_on_anchor: bool },
    /// Always respond with the given non-2xx HTTP status and raw body verbatim (no SSE, no id
    /// injection) — used to drive the HTTP executor's error-body code-extraction path
    /// (failure-code writeback Task 2).
    Error { status: u16, body: String },
    /// Emit `first_chunk` as a single SSE `data:` frame, then never yield again — no EOF, no
    /// further frames, no keep-alive. Distinct from `silent_on_anchor` (which never sends even the
    /// first byte): this mode DOES deliver one frame, then goes silent — simulating a real upstream
    /// that sends `response.created` and then stalls, exercising the watchdog scan-loop's per-read
    /// timeout rather than the initial first-chunk peek timeout.
    Stall { first_chunk: String },
}

/// A scriptable mock upstream: serves `POST /responses`, records every request body + the last
/// `Authorization` header, and streams SSE per its [`MockMode`].
#[derive(Clone)]
pub struct MockUpstream {
    events: Arc<Vec<String>>,
    mode: MockMode,
    bodies: Arc<Mutex<Vec<serde_json::Value>>>,
    last_authorization: Arc<Mutex<Option<String>>>,
    /// The full request `HeaderMap` of the most recent request — used by the Codex
    /// fingerprint-parity gate to inspect PolyFlare's egress header STRUCTURE (names + shapes),
    /// not just the single `authorization` header `last_authorization` already exposed.
    last_headers: Arc<Mutex<Option<HeaderMap>>>,
    emitted_ids: Arc<Mutex<Vec<String>>>,
    counter: Arc<AtomicU32>,
    /// If set (Scripted mode only), sleep this long after emitting `events[0]` and before
    /// emitting `events[1..]`. Simulates a real upstream's inter-chunk gap so callers can assert
    /// a relay forwards the first chunk immediately instead of buffering the whole stream.
    gap_after_first: Option<Duration>,
}

impl MockUpstream {
    fn build(events: Vec<String>, mode: MockMode) -> Self {
        Self {
            events: Arc::new(events),
            mode,
            bodies: Arc::new(Mutex::new(Vec::new())),
            last_authorization: Arc::new(Mutex::new(None)),
            last_headers: Arc::new(Mutex::new(None)),
            emitted_ids: Arc::new(Mutex::new(Vec::new())),
            counter: Arc::new(AtomicU32::new(0)),
            gap_after_first: None,
        }
    }

    /// Legacy scripted mode: stream `events` verbatim (no id injection).
    pub fn new(events: Vec<String>) -> Self {
        Self::build(events, MockMode::Scripted)
    }

    /// Like `new`, but emits `first_chunk` immediately, then sleeps `gap` before emitting
    /// `later_chunks` back-to-back. Used to test that a relay forwards the first chunk as soon as
    /// upstream sends it rather than buffering the whole response. Scripted mode only (no id
    /// injection).
    pub fn chunked_with_gap(
        first_chunk: impl Into<String>,
        later_chunks: Vec<String>,
        gap: Duration,
    ) -> Self {
        let mut events = vec![first_chunk.into()];
        events.extend(later_chunks);
        let mut mock = Self::build(events, MockMode::Scripted);
        mock.gap_after_first = Some(gap);
        mock
    }

    /// Always respond, injecting `response.created`/`response.completed` with a generated id.
    pub fn with_ids(events: Vec<String>) -> Self {
        Self::build(
            events,
            MockMode::WithIds {
                silent_on_anchor: false,
            },
        )
    }

    /// Respond with ids for anchorless requests; go silent (200 + no body) when the request
    /// carries `previous_response_id` — the wedge.
    pub fn silent_on_anchor(events: Vec<String>) -> Self {
        Self::build(
            events,
            MockMode::WithIds {
                silent_on_anchor: true,
            },
        )
    }

    /// Always respond with `status` and `body` verbatim (`content-type: application/json`) — no
    /// scripting, no id injection. Used to test error-body parsing (e.g. the structured
    /// `{"error":{"code":...}}` shape, or an oversized body exercising a bounded read).
    pub fn error_status(status: u16, body: impl Into<String>) -> Self {
        Self::build(
            vec![],
            MockMode::Error {
                status,
                body: body.into(),
            },
        )
    }

    /// Emit `first_chunk` once, then never yield again (no EOF). Used to test a post-first-chunk
    /// upstream stall (e.g. `response.created` followed by silence) — the watchdog scan-loop's
    /// per-read timeout, not the initial first-chunk peek timeout.
    pub fn stall_after_first(first_chunk: impl Into<String>) -> Self {
        Self::build(
            vec![],
            MockMode::Stall {
                first_chunk: first_chunk.into(),
            },
        )
    }

    /// The most recent request body, if any.
    pub fn last_body(&self) -> Option<serde_json::Value> {
        self.bodies.lock().unwrap().last().cloned()
    }

    /// Every recorded request body, in order.
    pub fn bodies(&self) -> Vec<serde_json::Value> {
        self.bodies.lock().unwrap().clone()
    }

    /// How many requests the mock has received.
    pub fn request_count(&self) -> usize {
        self.bodies.lock().unwrap().len()
    }

    /// The `Authorization` header of the most recent request (e.g. `"Bearer <token>"`).
    pub fn last_authorization(&self) -> Option<String> {
        self.last_authorization.lock().unwrap().clone()
    }

    /// The full request `HeaderMap` of the most recent request. Used by the Codex
    /// fingerprint-parity gate to capture PolyFlare's egress header structure.
    pub fn last_headers(&self) -> Option<HeaderMap> {
        self.last_headers.lock().unwrap().clone()
    }

    /// The `response.id`s the mock has emitted, in order.
    pub fn emitted_response_ids(&self) -> Vec<String> {
        self.emitted_ids.lock().unwrap().clone()
    }

    /// Bind an ephemeral port, serve in a background task, and return the base URL.
    pub async fn spawn(self) -> String {
        let app = Router::new()
            .route("/responses", post(handler))
            .route("/v1/messages", post(handler))
            // Match the raised polyflare-server body limit so large-body e2e tests
            // don't 413 against the mock upstream itself. Test infra only.
            .layer(DefaultBodyLimit::max(100 * 1024 * 1024))
            .with_state(self);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }
}

/// One recorded request against a [`MockControlUpstream`]: everything the D17 control-forward
/// primitive is expected to have sent, so tests can assert on the real values it produced (final
/// path, method, the `Authorization`/`chatgpt-account-id` headers, the body) rather than just
/// "did it not panic".
#[derive(Clone, Debug)]
pub struct RecordedControlRequest {
    /// e.g. `"POST"`, `"GET"`.
    pub method: String,
    /// The request path as received by the mock, e.g. `"/codex/memories/trace_summarize"` or
    /// `"/wham/agent-identities/jwks"` — NOT including the mock's own host/port, so a test can
    /// assert the control-forward primitive built the right join (`/codex/...` vs `/wham/...`).
    pub path: String,
    pub headers: HeaderMap,
    pub body: Bytes,
}

/// A scriptable mock CONTROL endpoint — the codex "control" surface (`thread/goal/*`,
/// `agent-identities/jwks`, `memories/trace_summarize`, ...), distinct from the streaming
/// `/responses` transport [`MockUpstream`] mocks. Serves both the `/codex/*path` and `/wham/*path`
/// shapes the D17 forward primitive's URL join (`polyflare_codex::control_url`) is required to
/// produce, records the most recent request's method/path/headers/body, and returns one scripted
/// status+body+headers response. Mirrors `MockUpstream`'s spawn/record idiom.
#[derive(Clone)]
pub struct MockControlUpstream {
    status: u16,
    body: Bytes,
    /// Extra response headers to send verbatim — tests use this to script BOTH an allow-listed
    /// header (e.g. `etag`) and a non-allow-listed one (e.g. `x-internal-secret`) in the same
    /// response, to prove the forward primitive's header filter drops the latter.
    extra_headers: Vec<(String, String)>,
    last_request: Arc<Mutex<Option<RecordedControlRequest>>>,
    request_count: Arc<AtomicUsize>,
}

impl MockControlUpstream {
    /// A mock that always returns `status` with `body` verbatim, no extra headers.
    pub fn new(status: u16, body: impl Into<Bytes>) -> Self {
        Self {
            status,
            body: body.into(),
            extra_headers: Vec::new(),
            last_request: Arc::new(Mutex::new(None)),
            request_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Adds one response header the mock will send back, in addition to `status`/`body`. Chainable
    /// — call repeatedly to script multiple headers (e.g. one allow-listed, one not).
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.extra_headers.push((name.into(), value.into()));
        self
    }

    /// The most recently received request, if any.
    pub fn last_request(&self) -> Option<RecordedControlRequest> {
        self.last_request.lock().unwrap().clone()
    }

    /// How many requests the mock has received.
    pub fn request_count(&self) -> usize {
        self.request_count.load(Ordering::SeqCst)
    }

    /// Bind an ephemeral port, serve in a background task, and return the base URL. The returned
    /// base URL is a bare `http://host:port` — a test's `Account::base_url` should be built as
    /// `format!("{base}/backend-api/codex")` (or `{base}/backend-api`) to exercise
    /// `control_url`'s normalization exactly as production `base_url` values do.
    pub async fn spawn(self) -> String {
        let app = Router::new()
            .route("/codex/{*path}", any(control_handler))
            .route("/wham/{*path}", any(control_handler))
            .with_state(self);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }
}

async fn control_handler(
    State(mock): State<MockControlUpstream>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    *mock.last_request.lock().unwrap() = Some(RecordedControlRequest {
        method: method.to_string(),
        path: uri.path().to_string(),
        headers,
        body,
    });
    mock.request_count.fetch_add(1, Ordering::SeqCst);

    let mut builder =
        Response::builder().status(StatusCode::from_u16(mock.status).unwrap_or(StatusCode::OK));
    for (name, value) in &mock.extra_headers {
        builder = builder.header(name, value);
    }
    builder.body(Body::from(mock.body.clone())).unwrap()
}

fn sse_frame(payload: &str) -> Bytes {
    Bytes::from(format!("data: {payload}\n\n"))
}

async fn handler(
    State(mock): State<MockUpstream>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let has_anchor = body.get("previous_response_id").is_some();
    mock.bodies.lock().unwrap().push(body);
    *mock.last_authorization.lock().unwrap() = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    *mock.last_headers.lock().unwrap() = Some(headers.clone());

    match mock.mode {
        MockMode::Scripted => {
            let events = (*mock.events).clone();
            let gap_after_first = mock.gap_after_first;
            // `idx` tracks the position of the item about to be yielded so the gap lands exactly
            // once, between event 0 and event 1 (i.e. after the first chunk, before the rest). A
            // no-op (immediate stream::iter equivalent) when `gap_after_first` is `None`.
            let s = stream::unfold(
                (events.into_iter(), 0usize),
                move |(mut iter, idx)| async move {
                    let item = iter.next()?;
                    if idx == 1 {
                        if let Some(gap) = gap_after_first {
                            tokio::time::sleep(gap).await;
                        }
                    }
                    Some((
                        Ok::<Event, Infallible>(Event::default().data(item)),
                        (iter, idx + 1),
                    ))
                },
            );
            Sse::new(s).keep_alive(KeepAlive::default()).into_response()
        }
        MockMode::WithIds { silent_on_anchor } => {
            if silent_on_anchor && has_anchor {
                // The wedge: 200 headers, then a body that never yields a byte (no keep-alive).
                let pending = stream::pending::<Result<Bytes, std::io::Error>>();
                return Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "text/event-stream")
                    .body(Body::from_stream(pending))
                    .unwrap();
            }
            let n = mock.counter.fetch_add(1, Ordering::SeqCst) + 1;
            let id = format!("resp_{n}");
            mock.emitted_ids.lock().unwrap().push(id.clone());
            let mut frames: Vec<Bytes> = Vec::new();
            frames.push(sse_frame(&format!(
                r#"{{"type":"response.created","response":{{"id":"{id}"}}}}"#
            )));
            for e in mock.events.iter() {
                frames.push(sse_frame(e));
            }
            frames.push(sse_frame(&format!(
                r#"{{"type":"response.completed","response":{{"id":"{id}"}}}}"#
            )));
            let s = stream::iter(frames.into_iter().map(Ok::<Bytes, std::io::Error>));
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "text/event-stream")
                .body(Body::from_stream(s))
                .unwrap()
        }
        MockMode::Error { status, body } => Response::builder()
            .status(StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .unwrap(),
        MockMode::Stall { first_chunk } => {
            let first = sse_frame(&first_chunk);
            let s = stream::once(async move { Ok::<Bytes, std::io::Error>(first) })
                .chain(stream::pending::<Result<Bytes, std::io::Error>>());
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "text/event-stream")
                .body(Body::from_stream(s))
                .unwrap()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mock_emits_events_and_records_body() {
        let mock = MockUpstream::new(vec!["one".to_string(), "two".to_string()]);
        let handle = mock.clone();
        let base = mock.spawn().await;

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("{base}/responses"))
            .json(&serde_json::json!({"model": "gpt-5.6-sol"}))
            .send()
            .await
            .unwrap();
        let text = resp.text().await.unwrap();

        assert!(text.contains("data: one"));
        assert!(text.contains("data: two"));
        assert_eq!(handle.last_body().unwrap()["model"], "gpt-5.6-sol");
    }

    #[tokio::test]
    async fn chunked_with_gap_delays_only_between_first_and_later_chunks() {
        use futures_util::StreamExt;

        let gap = Duration::from_millis(200);
        let mock = MockUpstream::chunked_with_gap(
            "first",
            vec!["second".to_string(), "third".to_string()],
            gap,
        );
        let base = mock.spawn().await;

        let client = reqwest::Client::new();
        let start = tokio::time::Instant::now();
        let resp = client
            .post(format!("{base}/responses"))
            .json(&serde_json::json!({"model": "gpt-5.6-sol"}))
            .send()
            .await
            .unwrap();

        let mut stream = resp.bytes_stream();
        let first = stream.next().await.unwrap().unwrap();
        let t_first = start.elapsed();
        assert!(String::from_utf8_lossy(&first).contains("data: first"));
        // The first chunk must arrive well before the gap elapses (no buffering of it).
        assert!(
            t_first < gap / 2,
            "t_first={t_first:?} should be well under half of gap={gap:?}"
        );

        let mut rest = String::new();
        while let Some(chunk) = stream.next().await {
            rest.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
        }
        let t_full = start.elapsed();
        assert!(rest.contains("data: second") && rest.contains("data: third"));
        // The full stream must take at least the injected gap (sanity: the gap really happened).
        assert!(
            t_full >= gap,
            "t_full={t_full:?} should be at least gap={gap:?}"
        );
    }
}

/// A scriptable mock of the OpenAI OAuth token endpoint (`POST /oauth/token`). Records the
/// request body and returns either a success token payload or an error status + code. Test infra
/// only — never used in production wiring.
#[derive(Clone)]
pub struct MockOAuth {
    response: Arc<OAuthResponse>,
    last_body: Arc<Mutex<Option<serde_json::Value>>>,
    hit_count: Arc<AtomicUsize>,
}

/// The scripted response for a `MockOAuth`.
#[derive(Clone)]
pub enum OAuthResponse {
    Ok {
        access_token: String,
        refresh_token: String,
        id_token: String,
    },
    Error {
        status: u16,
        code: String,
    },
    /// A non-2xx status whose body carries NO parseable `error` code (an empty JSON object),
    /// exercising the `OAuthError::Endpoint { code: None }` path.
    ErrorNoCode {
        status: u16,
    },
}

impl MockOAuth {
    /// A mock that returns HTTP 200 with the given tokens.
    pub fn ok(
        access_token: impl Into<String>,
        refresh_token: impl Into<String>,
        id_token: impl Into<String>,
    ) -> Self {
        Self {
            response: Arc::new(OAuthResponse::Ok {
                access_token: access_token.into(),
                refresh_token: refresh_token.into(),
                id_token: id_token.into(),
            }),
            last_body: Arc::new(Mutex::new(None)),
            hit_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// A mock that returns the given error status with `{"error": code}`.
    pub fn error(status: u16, code: impl Into<String>) -> Self {
        Self {
            response: Arc::new(OAuthResponse::Error {
                status,
                code: code.into(),
            }),
            last_body: Arc::new(Mutex::new(None)),
            hit_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// A mock that returns the given error status with a body carrying no `error` code (an empty
    /// JSON object) — drives the `OAuthError::Endpoint { code: None }` classification path.
    pub fn error_no_code(status: u16) -> Self {
        Self {
            response: Arc::new(OAuthResponse::ErrorNoCode { status }),
            last_body: Arc::new(Mutex::new(None)),
            hit_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// The JSON body of the most recent request, if any.
    pub fn last_body(&self) -> Option<serde_json::Value> {
        self.last_body.lock().unwrap().clone()
    }

    /// How many times `POST /oauth/token` has been hit. Used by singleflight tests (F2) to assert
    /// that N concurrent stale-refresh requests for the same account collapse into exactly one
    /// call to the OAuth endpoint.
    pub fn hit_count(&self) -> usize {
        self.hit_count.load(Ordering::SeqCst)
    }

    /// Bind an ephemeral port, serve in a background task, and return the base URL.
    pub async fn spawn(self) -> String {
        let app = Router::new()
            .route("/oauth/token", post(oauth_handler))
            .with_state(self);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }
}

async fn oauth_handler(
    State(mock): State<MockOAuth>,
    Json(body): Json<serde_json::Value>,
) -> (StatusCode, Json<serde_json::Value>) {
    mock.hit_count.fetch_add(1, Ordering::SeqCst);
    *mock.last_body.lock().unwrap() = Some(body);
    // A small delay so concurrent callers racing on the same stale account reliably overlap
    // (all observe staleness before the first refresh completes) — this is what makes
    // singleflight tests (F2) actually exercise the race instead of serializing by luck.
    tokio::time::sleep(Duration::from_millis(50)).await;
    match &*mock.response {
        OAuthResponse::Ok {
            access_token,
            refresh_token,
            id_token,
        } => (
            StatusCode::OK,
            Json(serde_json::json!({
                "access_token": access_token,
                "refresh_token": refresh_token,
                "id_token": id_token,
                "token_type": "Bearer",
                "expires_in": 3600,
            })),
        ),
        OAuthResponse::Error { status, code } => (
            StatusCode::from_u16(*status).unwrap_or(StatusCode::BAD_REQUEST),
            Json(serde_json::json!({ "error": code })),
        ),
        OAuthResponse::ErrorNoCode { status } => (
            StatusCode::from_u16(*status).unwrap_or(StatusCode::BAD_REQUEST),
            Json(serde_json::json!({})),
        ),
    }
}
