//! Test support: scriptable mock upstreams for e2e tests.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

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
}

impl MockUpstream {
    pub fn new(events: Vec<String>) -> Self {
        Self {
            events: Arc::new(events),
            last_body: Arc::new(Mutex::new(None)),
            last_authorization: Arc::new(Mutex::new(None)),
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
    let stream = stream::iter(events.into_iter().map(|d| Ok(Event::default().data(d))));
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
