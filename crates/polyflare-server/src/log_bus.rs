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
//! live".
//!
//! Both [`LogBus::publish`] and [`LogBus::subscribe`] serialize on the *same* ring-buffer mutex,
//! and each does its ring-buffer mutation/read and its broadcast send/subscribe while holding that
//! one lock. That gives a single, total ordering between every publish and every subscribe: for
//! any given event and any given subscriber, the event is either already in that subscriber's
//! backfill snapshot (published before the lock-protected `subscribe` critical section ran) or it
//! will arrive live over the broadcast receiver (published after) — never both, never neither.
//! This is what rules out the duplicate/missed delivery at the backfill/live boundary that a
//! publish-then-unlock-then-send (or subscribe-then-lock) ordering would allow.

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
    ///
    /// The ring push and the broadcast send happen under the *same* ring-buffer lock as
    /// `subscribe`'s snapshot+subscribe, so this event is atomically either visible to a given
    /// subscriber's backfill or delivered to it live — never both. See the module doc.
    pub fn publish(&self, ev: LogEvent) {
        let mut ring = self.ring.lock().unwrap_or_else(|e| e.into_inner());
        if ring.len() == self.cap {
            ring.pop_front();
        }
        ring.push_back(ev.clone());
        // `broadcast::Sender::send` is synchronous and non-blocking (it never awaits), so holding
        // the std `Mutex` across this call is safe — no await-across-lock, no deadlock risk.
        let _ = self.tx.send(ev);
    }

    /// Subscribes to live events, returning a backfill snapshot of the ring buffer (oldest first)
    /// plus a receiver for events published from this point forward.
    ///
    /// The broadcast subscription and the ring snapshot are both taken while holding the same
    /// ring-buffer lock that `publish` holds across its push+send, so no event can be missed or
    /// double-delivered across the backfill/live boundary. See the module doc.
    pub fn subscribe(&self) -> (Vec<LogEvent>, broadcast::Receiver<LogEvent>) {
        let ring = self.ring.lock().unwrap_or_else(|e| e.into_inner());
        let rx = self.tx.subscribe();
        let backfill = ring.iter().cloned().collect();
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

    /// Covers the publish/subscribe boundary directly: an event published *before* `subscribe`
    /// must appear in the backfill snapshot exactly once and must NOT also arrive live, while an
    /// event published *after* `subscribe` must arrive live exactly once and must NOT also appear
    /// in the backfill. `publish` and `subscribe` now serialize on the same ring-buffer lock (each
    /// doing its ring mutation/read together with its broadcast send/subscribe under that one
    /// lock), which is what gives this exactly-once guarantee at the boundary — a one-sided fix
    /// that only serialized `publish`'s internals (or only `subscribe`'s) would not.
    #[tokio::test]
    async fn boundary_event_is_not_duplicated_across_backfill_and_live() {
        let bus = LogBus::new(16);

        // Pre-subscribe event: must land in the backfill snapshot only.
        bus.publish(LogEvent::info("boundary", "pre-subscribe event"));
        let (backfill, mut rx) = bus.subscribe();
        assert_eq!(backfill.len(), 1);
        assert_eq!(backfill[0].message, "pre-subscribe event");

        // Post-subscribe event: must arrive live only.
        bus.publish(LogEvent::info("boundary", "post-subscribe event"));
        let live = rx.recv().await.unwrap();
        assert_eq!(live.message, "post-subscribe event");

        // The pre-subscribe event must NOT also show up live — draining any further immediately
        // available messages should surface nothing (in particular, not a duplicate of the
        // pre-subscribe event).
        assert!(matches!(
            rx.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));

        // The post-subscribe event must NOT also show up in a fresh backfill snapshot taken
        // before any further publishes — a second subscriber right now should see only the two
        // published events, each exactly once, in order.
        let (backfill2, _rx2) = bus.subscribe();
        assert_eq!(backfill2.len(), 2);
        assert_eq!(backfill2[0].message, "pre-subscribe event");
        assert_eq!(backfill2[1].message, "post-subscribe event");
    }
}
