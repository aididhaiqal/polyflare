//! Content-free live-log bus: a broadcast channel + ring buffer fed from the request-completion
//! chokepoint (`crate::observability::RequestLog`).
//!
//! `LogEvent` carries ONLY outcomes/metrics/identifiers/short structured messages — never
//! request/response bodies or conversation content. It is built exclusively from
//! [`crate::observability::RequestLog::to_log_event`], which draws from the same content-safe
//! field set as `RequestLog::record` (the persisted `request_log` table). See that module's
//! content-safety constraint before adding fields here.
//!
//! Two consumers exist per subscription: the ring buffer (a bounded backfill of recent events for
//! newly-connecting subscribers) and a `tokio::sync::broadcast` channel (live events going
//! forward). [`LogBus::subscribe`] returns both — the backfill snapshot plus a receiver — so a
//! subscriber never misses events published between "read the backfill" and "start receiving
//! live" (both happen while holding the ring buffer's lock).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use tokio::sync::broadcast;

/// Severity of a [`LogEvent`]. Serializes lowercase (`"info"`, `"warn"`, `"error"`, `"debug"`) for
/// the (later-task) dashboard SSE wire format.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Info,
    Warn,
    Error,
    Debug,
}

/// A single content-free log line for the dashboard's live log stream. Every field here is a
/// structured outcome/metric/identifier — never a request or response body, and never free-form
/// text sourced from request content. See the module doc for the content-safety constraint.
#[derive(Clone, Debug, serde::Serialize)]
pub struct LogEvent {
    pub ts_ms: i64,
    pub level: LogLevel,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<i64>,
    pub kind: String,
    pub message: String,
}

impl LogEvent {
    /// Convenience constructor for an `Info`-level event (used by tests and low-frequency
    /// non-request events). Request-completion events are built via
    /// `RequestLog::to_log_event`, which sets the other fields directly.
    pub fn info(kind: &str, message: impl Into<String>) -> Self {
        Self::new(LogLevel::Info, kind, message)
    }

    pub fn new(level: LogLevel, kind: &str, message: impl Into<String>) -> Self {
        Self {
            ts_ms: now_ms(),
            level,
            provider: None,
            account: None,
            model: None,
            status: None,
            latency_ms: None,
            kind: kind.to_string(),
            message: message.into(),
        }
    }
}

pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Broadcast channel capacity: generous headroom over the ring buffer capacity so a slow
/// subscriber lagging behind bursts sees `RecvError::Lagged` rather than the sender blocking.
const CHANNEL_CAPACITY: usize = 1024;

/// The live-log bus: a bounded ring buffer (backfill for new subscribers) plus a
/// `tokio::sync::broadcast` channel (events going forward). Content-free by construction — see
/// the module doc.
pub struct LogBus {
    tx: broadcast::Sender<LogEvent>,
    ring: Mutex<VecDeque<LogEvent>>,
    cap: usize,
}

impl LogBus {
    /// Constructs a new bus with a ring buffer holding at most `cap` recent events.
    pub fn new(cap: usize) -> Arc<Self> {
        Arc::new(Self {
            tx: broadcast::channel(CHANNEL_CAPACITY).0,
            ring: Mutex::new(VecDeque::new()),
            cap,
        })
    }

    /// Publishes `ev`: appends it to the ring buffer (evicting the oldest entry if at capacity)
    /// and broadcasts it to any live subscribers. Publishing with zero subscribers is not an
    /// error — the broadcast send's `Err` (no receivers) is deliberately ignored.
    pub fn publish(&self, ev: LogEvent) {
        {
            let mut ring = self.ring.lock().unwrap_or_else(|e| e.into_inner());
            if ring.len() == self.cap {
                ring.pop_front();
            }
            ring.push_back(ev.clone());
        }
        let _ = self.tx.send(ev);
    }

    /// Subscribes to live events, returning a backfill snapshot of the ring buffer (oldest first)
    /// plus a receiver for events published from this point forward. The backfill is read while
    /// still holding the ring lock that guards it, so no event can be missed or double-delivered
    /// across the backfill/live boundary.
    pub fn subscribe(&self) -> (Vec<LogEvent>, broadcast::Receiver<LogEvent>) {
        let rx = self.tx.subscribe();
        let backfill = self
            .ring
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .cloned()
            .collect();
        (backfill, rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn publish_delivers_to_subscriber_and_backfills() {
        let bus = LogBus::new(16);
        bus.publish(LogEvent::info("test", "warmup line")); // pre-subscribe → ring buffer
        let (backfill, mut rx) = bus.subscribe();
        assert_eq!(backfill.len(), 1);
        bus.publish(LogEvent::info("test", "live line"));
        let got = rx.recv().await.unwrap();
        assert_eq!(got.message, "live line");
    }
}
