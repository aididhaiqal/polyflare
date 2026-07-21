//! `GET /api/logs/stream`: a flag-gated Server-Sent-Events endpoint that drains
//! [`crate::log_bus::LogBus`] — backfill first, then live events, with a 15s heartbeat.
//!
//! Gated on [`crate::app::AppState::live_logs`] (`POLYFLARE_LIVE_LOGS`): `404` when disabled, so
//! the capability is discoverable (via `/api/capabilities`) rather than silently absent. Content
//! safety: the stream carries only [`crate::log_bus::LogEvent`] — never request/response bodies
//! or conversation content — because that's all `LogBus` ever holds (see its module doc).

use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use futures_util::stream::{self, StreamExt};

use crate::app::AppState;
use crate::log_bus::LogEvent;

/// Streams the content-free log bus as SSE frames: the current backfill snapshot first (oldest
/// first), then live events as they're published. `404`s when `AppState::live_logs` is off.
pub async fn logs_stream_handler(State(s): State<Arc<AppState>>) -> Response {
    if !s.runtime_settings.live_logs() {
        return (StatusCode::NOT_FOUND, "live logs disabled").into_response();
    }
    let (backfill, rx) = s.log_bus.subscribe();
    let backfill = stream::iter(backfill.into_iter().map(sse_ok));
    let live = tokio_stream::wrappers::BroadcastStream::new(rx)
        .filter_map(|r| async move { r.ok().map(sse_ok) });
    let body = backfill.chain(live);
    Sse::new(body)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}

/// Serializes a single `LogEvent` into an SSE `data:` frame. Serialization failure (which
/// shouldn't happen for this content-free, always-`Serialize`-derived type) falls back to an
/// empty frame rather than dropping the event or panicking.
fn sse_ok(ev: LogEvent) -> Result<Event, std::convert::Infallible> {
    Ok(Event::default().data(serde_json::to_string(&ev).unwrap_or_default()))
}
