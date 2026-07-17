//! Thin, content-safe request observability (SPEC-M5 §3.4).
//!
//! Exactly ONE structured `info` event is emitted per request, at completion — never on the hot
//! per-chunk streaming path (see `crate::ingress`'s handler wrappers). The event carries ONLY:
//! HTTP method, ingress path, the resolved backend provider, whether the request was
//! model-aliased, the client-facing outcome status, and duration in milliseconds.
//!
//! It must NEVER carry a bearer token, a session/thread/turn id, or the request/response body.
//! `account_id` and `model` ARE carried (added by Task 12, SPEC-M5 §5/§6.7): both are content-free
//! identifiers — a stable account row id and the requested/served model string, never conversation
//! content — needed to populate the dashboard's per-account and per-model breakdowns. When in
//! doubt about any OTHER field, leave it out — do not add fields to `RequestLog` without
//! re-reading SPEC-M5 §3.4's content-safety constraint.
//!
//! The SAME `RequestLog` also produces the content-free row persisted to the `request_log` table
//! (the dashboard's history backend) via [`RequestLog::record`] — deliberately routed through this
//! one struct so there is a single content-safety chokepoint for both the ephemeral event and the
//! durable row. The constraint above binds the persisted record identically.

use axum::http::StatusCode;
use polyflare_core::Provider;

use crate::log_bus::{self, LogEvent, LogLevel};

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
    /// The account that served (or was selected/attempted to serve) this request — a stable row
    /// id, never a token or session identifier.
    pub account_id: Option<String>,
    /// The requested (native path) or resolved target (translated/aliased path) model string.
    pub model: Option<String>,
    /// `reasoning.effort` for this request, when known (native facts or the alias's override).
    pub reasoning_effort: Option<String>,
    /// The account's subscription/service tier, when known. Not populated by this task.
    pub service_tier: Option<String>,
    /// The wire transport this request rode in on (`"http"` today; `"ws"` lands with the WS
    /// milestone).
    pub transport: Option<String>,
    /// Time to first token, in milliseconds, when observed from the response stream.
    pub ttft_ms: Option<i64>,
    /// Total tokens for this request, when observed from the response stream.
    pub total_tokens: Option<i64>,
    /// Cached tokens for this request, when observed from the response stream.
    pub cached_tokens: Option<i64>,
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
            account_id: self.account_id.clone(),
            model: self.model.clone(),
            reasoning_effort: self.reasoning_effort.clone(),
            service_tier: self.service_tier.clone(),
            transport: self.transport.clone(),
            ttft_ms: self.ttft_ms,
            total_tokens: self.total_tokens,
            cached_tokens: self.cached_tokens,
        }
    }

    /// The content-free live-log-bus form of this request outcome (see `crate::log_bus`). Draws
    /// from EXACTLY the same field set as [`Self::record`] — this is the same content-safety
    /// chokepoint feeding a second sink (an ephemeral broadcast + ring buffer instead of the
    /// durable `request_log` table). `account`/`model` are populated from `self.account_id`/
    /// `self.model` now that `RequestLog` carries them.
    pub fn to_log_event(&self) -> LogEvent {
        let status = self.status.as_u16();
        let level = if status == 429 || status >= 500 {
            LogLevel::Warn
        } else if status >= 400 {
            LogLevel::Error
        } else {
            LogLevel::Info
        };
        let provider = self.provider.to_string();
        LogEvent {
            ts_ms: log_bus::now_ms(),
            level,
            provider: Some(provider.clone()),
            account: self.account_id.clone(),
            model: self.model.clone(),
            status: Some(status),
            latency_ms: Some(self.duration_ms as i64),
            kind: "request".to_string(),
            message: format!("req {status} · {provider} · {}ms", self.duration_ms),
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
                account_id: None,
                model: None,
                reasoning_effort: None,
                service_tier: None,
                transport: None,
                ttft_ms: None,
                total_tokens: None,
                cached_tokens: None,
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
