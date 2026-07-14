//! Test support: scriptable mock upstreams for e2e tests.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::extract::{DefaultBodyLimit, Json, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::post;
use axum::Router;
use futures_util::stream::{self, Stream};
use tokio::net::TcpListener;

/// A scriptable mock upstream: serves `POST /responses`, records the request body,
/// and streams back a fixed list of SSE `data:` payloads.
#[derive(Clone)]
pub struct MockUpstream {
    events: Arc<Vec<String>>,
    last_body: Arc<Mutex<Option<serde_json::Value>>>,
}

impl MockUpstream {
    pub fn new(events: Vec<String>) -> Self {
        Self {
            events: Arc::new(events),
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
    Json(body): Json<serde_json::Value>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    *mock.last_body.lock().unwrap() = Some(body);
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
