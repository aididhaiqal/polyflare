//! Test support: scriptable mock upstreams for e2e tests.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{DefaultBodyLimit, Json, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::post;
use axum::Router;
use futures_util::stream::{self, Stream};
use tokio::net::TcpListener;

/// A scriptable mock upstream: serves `POST /responses`, records the request body + the
/// `Authorization` header, and streams back a fixed list of SSE `data:` payloads.
#[derive(Clone)]
pub struct MockUpstream {
    events: Arc<Vec<String>>,
    last_body: Arc<Mutex<Option<serde_json::Value>>>,
    last_authorization: Arc<Mutex<Option<String>>>,
    /// If set, sleep this long after emitting `events[0]` and before emitting `events[1..]`.
    /// Used to simulate a real upstream's inter-chunk gap so callers can assert a relay forwards
    /// the first chunk immediately instead of buffering the whole stream.
    gap_after_first: Option<Duration>,
}

impl MockUpstream {
    pub fn new(events: Vec<String>) -> Self {
        Self {
            events: Arc::new(events),
            last_body: Arc::new(Mutex::new(None)),
            last_authorization: Arc::new(Mutex::new(None)),
            gap_after_first: None,
        }
    }

    /// Like `new`, but emits `first_chunk` immediately, then sleeps `gap` before emitting
    /// `later_chunks` back-to-back. Used to test that a relay forwards the first chunk as soon as
    /// upstream sends it rather than buffering the whole response.
    pub fn chunked_with_gap(
        first_chunk: impl Into<String>,
        later_chunks: Vec<String>,
        gap: Duration,
    ) -> Self {
        let mut events = vec![first_chunk.into()];
        events.extend(later_chunks);
        Self {
            events: Arc::new(events),
            last_body: Arc::new(Mutex::new(None)),
            last_authorization: Arc::new(Mutex::new(None)),
            gap_after_first: Some(gap),
        }
    }

    /// The JSON body of the most recent request, if any.
    pub fn last_body(&self) -> Option<serde_json::Value> {
        self.last_body.lock().unwrap().clone()
    }

    /// The `Authorization` header of the most recent request, if any (e.g. `"Bearer <token>"`).
    pub fn last_authorization(&self) -> Option<String> {
        self.last_authorization.lock().unwrap().clone()
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

async fn handler(
    State(mock): State<MockUpstream>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    *mock.last_body.lock().unwrap() = Some(body);
    *mock.last_authorization.lock().unwrap() = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let events = (*mock.events).clone();
    let gap_after_first = mock.gap_after_first;
    // `idx` tracks the position of the item about to be yielded so the gap lands exactly once,
    // between event 0 and event 1 (i.e. after the first chunk, before the rest).
    let stream = stream::unfold(
        (events.into_iter(), 0usize),
        move |(mut iter, idx)| async move {
            let item = iter.next()?;
            if idx == 1 {
                if let Some(gap) = gap_after_first {
                    tokio::time::sleep(gap).await;
                }
            }
            Some((Ok(Event::default().data(item)), (iter, idx + 1)))
        },
    );
    Sse::new(stream).keep_alive(KeepAlive::default())
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
