//! Thin, content-safe request observability (SPEC-M5 §3.4).
//!
//! Exactly ONE structured `info` event is emitted per request, at completion — never on the hot
//! per-chunk streaming path (see `crate::ingress`'s handler wrappers). The event carries ONLY:
//! HTTP method, ingress path, the resolved backend provider, whether the request was
//! model-aliased, the client-facing outcome status, and duration in milliseconds.
//!
//! It must NEVER carry a token/bearer, an account id, a session/thread/turn id, the request or
//! response body, or the client's `model` string. When in doubt, leave it out — do not add fields
//! to `RequestLog` without re-reading SPEC-M5 §3.4's content-safety constraint.
//!
//! The SAME `RequestLog` also produces the content-free row persisted to the `request_log` table
//! (the dashboard's history backend) via [`RequestLog::record`] — deliberately routed through this
//! one struct so there is a single content-safety chokepoint for both the ephemeral event and the
//! durable row. The constraint above binds the persisted record identically.

use axum::http::StatusCode;
use polyflare_core::Provider;

/// A request's content-safe completion facts. Emitted as ONE `tracing::info!` event via
/// [`RequestLog::emit`] — everything on this type is structurally safe to log (no free-form
/// strings sourced from the request itself).
pub struct RequestLog {
    pub method: &'static str,
    pub path: &'static str,
    pub provider: Provider,
    pub aliased: bool,
    pub status: StatusCode,
    pub duration_ms: u64,
}

impl RequestLog {
    /// Emit this request's completion event. Uses an explicit `target` (rather than the default
    /// module path) so callers — including tests — can isolate this specific event from any
    /// other crate's unrelated `tracing` traffic.
    pub fn emit(&self) {
        tracing::info!(
            target: "polyflare_server::request",
            method = self.method,
            path = self.path,
            provider = %self.provider,
            aliased = self.aliased,
            status = self.status.as_u16(),
            duration_ms = self.duration_ms,
            "request completed"
        );
    }

    /// The content-free persistable form of this request outcome, for the `request_log` table (the
    /// dashboard's history backend). It carries EXACTLY the same audited field set as [`Self::emit`]
    /// — this method exists so `RequestLog` stays the single content-safety chokepoint for both the
    /// ephemeral log event and the persisted row. `requested_at` is supplied by the caller (unix
    /// epoch seconds). Adding a field here means adding it to [`Self`] and re-checking the
    /// content-safety constraint above — never add a request-derived free-form string.
    pub fn record(&self, requested_at: i64) -> polyflare_store::RequestLogRecord {
        polyflare_store::RequestLogRecord {
            requested_at,
            provider: self.provider.to_string(),
            method: self.method.to_string(),
            path: self.path.to_string(),
            aliased: self.aliased,
            status: self.status.as_u16(),
            duration_ms: self.duration_ms as i64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// A minimal `tracing::Subscriber` that records every event on the
    /// `"polyflare_server::request"` target as a flat `field=value` string and ignores
    /// everything else. Enough to assert our one content-safe event fired with exactly the
    /// expected fields and nothing more.
    struct Capture(Arc<Mutex<Vec<String>>>);

    struct FieldVisitor(String);

    impl tracing::field::Visit for FieldVisitor {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0.push_str(&format!("{}={:?} ", field.name(), value));
        }

        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.0.push_str(&format!("{}={} ", field.name(), value));
        }

        fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
            self.0.push_str(&format!("{}={} ", field.name(), value));
        }

        fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
            self.0.push_str(&format!("{}={} ", field.name(), value));
        }
    }

    impl tracing::Subscriber for Capture {
        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            metadata.target() == "polyflare_server::request"
        }

        fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

        fn event(&self, event: &tracing::Event<'_>) {
            let mut visitor = FieldVisitor(String::new());
            event.record(&mut visitor);
            self.0.lock().unwrap().push(visitor.0);
        }

        fn enter(&self, _span: &tracing::span::Id) {}

        fn exit(&self, _span: &tracing::span::Id) {}
    }

    #[test]
    fn request_completion_event_carries_only_safe_fields() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let dispatch = tracing::Dispatch::new(Capture(captured.clone()));

        tracing::dispatcher::with_default(&dispatch, || {
            RequestLog {
                method: "POST",
                path: "/responses",
                provider: Provider::Codex,
                aliased: false,
                status: StatusCode::OK,
                duration_ms: 42,
            }
            .emit();
        });

        let events = captured.lock().unwrap();
        assert_eq!(
            events.len(),
            1,
            "expected exactly one request-completion event, got: {events:?}"
        );
        let line = &events[0];

        for expected in [
            "method=POST",
            "path=/responses",
            "provider=codex",
            "aliased=false",
            "status=200",
            "duration_ms=42",
        ] {
            assert!(line.contains(expected), "missing `{expected}` in: {line}");
        }

        // The whole point of this feature: the event must never carry a token, account id,
        // session id, or any request content. Assert none of that ever shows up.
        for forbidden in [
            "bearer",
            "token",
            "sess_",
            "acct_",
            "session",
            "model",
            "input",
            "conversation",
        ] {
            assert!(
                !line.to_lowercase().contains(forbidden),
                "forbidden content `{forbidden}` leaked into request log: {line}"
            );
        }
    }

    #[test]
    fn events_outside_the_request_target_are_ignored() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let dispatch = tracing::Dispatch::new(Capture(captured.clone()));

        tracing::dispatcher::with_default(&dispatch, || {
            tracing::info!(target: "some_other_crate", "unrelated noise");
        });

        assert!(
            captured.lock().unwrap().is_empty(),
            "Capture must ignore events outside its target"
        );
    }
}
