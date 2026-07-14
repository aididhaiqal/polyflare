//! Test support: scriptable mock upstreams for e2e tests.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Json, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use bytes::Bytes;
use futures_util::stream;
use tokio::net::TcpListener;

/// The response behavior of a `MockUpstream`.
#[derive(Clone)]
enum MockMode {
    /// Legacy: always stream the fixed `events` as SSE `data:` frames (with keep-alive). Never
    /// injects a `response.id`. Used by the M1/M2 pass-through tests unchanged.
    Scripted,
    /// Emit `response.created`(resp_N) + `events` + `response.completed`(resp_N), generating a
    /// fresh `resp_N` id per response, UNLESS the request carries `previous_response_id`, in which
    /// case `anchor_behavior` decides what happens instead.
    WithIds { anchor_behavior: AnchorBehavior },
}

/// How a `WithIds` mock behaves on an anchor-bearing (`previous_response_id`) request.
#[derive(Clone, Copy)]
enum AnchorBehavior {
    /// Respond exactly like an anchorless request (ids assigned normally).
    Normal,
    /// 200 headers, then a body stream that never yields a byte (no keep-alive) — the silent
    /// wedge (silence-after-accept, no error, just nothing).
    Silent,
    /// 200 headers, then a body stream whose first (and only) item is a transport-level error —
    /// the "200-then-error-first-byte" mid-race hard-error case. Distinct from `Silent`: this
    /// yields a genuine transport error rather than yielding nothing at all.
    ErrorFirst,
}

/// A scriptable mock upstream: serves `POST /responses`, records every request body + the last
/// `Authorization` header, and streams SSE per its [`MockMode`].
#[derive(Clone)]
pub struct MockUpstream {
    events: Arc<Vec<String>>,
    mode: MockMode,
    bodies: Arc<Mutex<Vec<serde_json::Value>>>,
    last_authorization: Arc<Mutex<Option<String>>>,
    emitted_ids: Arc<Mutex<Vec<String>>>,
    counter: Arc<AtomicU32>,
}

impl MockUpstream {
    fn build(events: Vec<String>, mode: MockMode) -> Self {
        Self {
            events: Arc::new(events),
            mode,
            bodies: Arc::new(Mutex::new(Vec::new())),
            last_authorization: Arc::new(Mutex::new(None)),
            emitted_ids: Arc::new(Mutex::new(Vec::new())),
            counter: Arc::new(AtomicU32::new(0)),
        }
    }

    /// Legacy scripted mode: stream `events` verbatim (no id injection).
    pub fn new(events: Vec<String>) -> Self {
        Self::build(events, MockMode::Scripted)
    }

    /// Always respond, injecting `response.created`/`response.completed` with a generated id.
    pub fn with_ids(events: Vec<String>) -> Self {
        Self::build(
            events,
            MockMode::WithIds {
                anchor_behavior: AnchorBehavior::Normal,
            },
        )
    }

    /// Respond with ids for anchorless requests; go silent (200 + no body, ever) when the request
    /// carries `previous_response_id` — the wedge.
    pub fn silent_on_anchor(events: Vec<String>) -> Self {
        Self::build(
            events,
            MockMode::WithIds {
                anchor_behavior: AnchorBehavior::Silent,
            },
        )
    }

    /// Respond with ids for anchorless requests; on an anchor-bearing request, return 200 headers
    /// then a body stream whose first (and only) item is a transport-level error — a mid-race
    /// hard-error, distinct from `silent_on_anchor` (which yields nothing at all).
    pub fn error_first_on_anchor(events: Vec<String>) -> Self {
        Self::build(
            events,
            MockMode::WithIds {
                anchor_behavior: AnchorBehavior::ErrorFirst,
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

    /// The `response.id`s the mock has emitted, in order.
    pub fn emitted_response_ids(&self) -> Vec<String> {
        self.emitted_ids.lock().unwrap().clone()
    }

    /// Bind an ephemeral port, serve in a background task, and return the base URL.
    pub async fn spawn(self) -> String {
        let app = Router::new()
            .route("/responses", post(handler))
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

    match mock.mode {
        MockMode::Scripted => {
            let events = (*mock.events).clone();
            let s = stream::iter(
                events
                    .into_iter()
                    .map(|d| Ok::<Event, Infallible>(Event::default().data(d))),
            );
            Sse::new(s).keep_alive(KeepAlive::default()).into_response()
        }
        MockMode::WithIds { anchor_behavior } => {
            if has_anchor {
                match anchor_behavior {
                    AnchorBehavior::Silent => {
                        // The wedge: 200 headers, then a body that never yields a byte (no
                        // keep-alive).
                        let pending = stream::pending::<Result<Bytes, std::io::Error>>();
                        return Response::builder()
                            .status(StatusCode::OK)
                            .header(header::CONTENT_TYPE, "text/event-stream")
                            .body(Body::from_stream(pending))
                            .unwrap();
                    }
                    AnchorBehavior::ErrorFirst => {
                        // 200 headers, then a body stream whose first (and only) item is a
                        // transport-level error — the mid-race hard-error case.
                        let err_stream = stream::once(async {
                            Err::<Bytes, std::io::Error>(std::io::Error::new(
                                std::io::ErrorKind::ConnectionReset,
                                "mock transport error mid-race",
                            ))
                        });
                        return Response::builder()
                            .status(StatusCode::OK)
                            .header(header::CONTENT_TYPE, "text/event-stream")
                            .body(Body::from_stream(err_stream))
                            .unwrap();
                    }
                    AnchorBehavior::Normal => {}
                }
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
}

/// A scriptable mock of the OpenAI OAuth token endpoint (`POST /oauth/token`). Records the
/// request body and returns either a success token payload or an error status + code. Test infra
/// only — never used in production wiring.
#[derive(Clone)]
pub struct MockOAuth {
    response: Arc<OAuthResponse>,
    last_body: Arc<Mutex<Option<serde_json::Value>>>,
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
        }
    }

    /// A mock that returns the given error status with a body carrying no `error` code (an empty
    /// JSON object) — drives the `OAuthError::Endpoint { code: None }` classification path.
    pub fn error_no_code(status: u16) -> Self {
        Self {
            response: Arc::new(OAuthResponse::ErrorNoCode { status }),
            last_body: Arc::new(Mutex::new(None)),
        }
    }

    /// The JSON body of the most recent request, if any.
    pub fn last_body(&self) -> Option<serde_json::Value> {
        self.last_body.lock().unwrap().clone()
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
    *mock.last_body.lock().unwrap() = Some(body);
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
