//! Per-turn telemetry for the WS-downstream relay.
//!
//! One downstream WebSocket carries many `response.create` turns, so the HTTP route-level
//! completion wrapper cannot be reused at the handshake boundary. This module mirrors that wrapper
//! at the turn boundary instead: a bounded shallow read of each client `response.create`, timing
//! observation of upstream event discriminants, and one content-free request-log row when a
//! terminal frame is forwarded.
//!
//! Content safety is identical to `crate::observability::RequestLog`: only bounded routing fields,
//! numeric usage/timing, and generated ids leave this module. Input/output content is never
//! returned by an observer, logged, or persisted.

use std::collections::HashMap;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use axum::http::{HeaderMap, StatusCode};
use polyflare_core::{AccountId, Provider, SessionKey};
use serde_json::value::RawValue;
use serde_json::Value;

use crate::app::AppState;
use crate::observability::RequestLog;
use crate::runtime_state::InFlightGuard;
use crate::session_key::parse_inbound_scoped;
use crate::usage_capture::{
    is_output_delta, parse_response_usage, pressure_equivalent_tokens, ResponseUsage,
};
use polyflare_store::RequestProtocolOutcome;

/// A live user-visible WS turn. Same-account replay keeps this value intact so reconnect time is
/// correctly included in end-to-end latency and the turn is still recorded exactly once.
pub(crate) struct WsTurnTelemetry {
    started_at: Instant,
    model: Option<String>,
    reasoning_effort: Option<String>,
    service_tier: Option<String>,
    subagent: Option<String>,
    session_key: Option<String>,
    logical_turn_key: Option<String>,
    estimated_tokens: u32,
    ttft_ms: Option<i64>,
    /// Prewarm is real upstream work and therefore holds admission capacity, but it is not a
    /// user-visible request-log row.
    log_request: bool,
    /// One lease per generating turn, not per long-lived socket. Its `Drop` releases selection
    /// pressure on every terminal and teardown path.
    _in_flight: Option<InFlightGuard>,
}

/// The terminal facts extracted from an upstream frame.
pub(crate) struct WsTurnTerminal {
    pub status: StatusCode,
    pub usage: Option<ResponseUsage>,
    pub routing: WsRoutingOutcome,
    pub protocol_outcome: RequestProtocolOutcome,
}

#[derive(Clone, Copy)]
pub(crate) enum WsRoutingOutcome {
    Completed,
    /// The terminal was already classified and written back by the pump's shared failure path, or
    /// is a request-level/synthetic client-resend terminal that must not penalize the account.
    TerminalNoWriteback,
    /// The turn ended without a client-visible protocol terminal because its transport vanished
    /// or bounded reconnect recovery failed.
    TransportLoss,
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Start accounting for every `response.create`. Codex's `generate:false` prewarm is a real
/// protocol request whose completed response may anchor the next turn, so it consumes admission
/// capacity even though it is omitted from user-facing request history.
pub(crate) fn start_turn(
    headers: &HeaderMap,
    frame: &str,
    session_key: &SessionKey,
    pool: Option<&str>,
) -> Option<WsTurnTelemetry> {
    let fields: HashMap<String, &RawValue> = serde_json::from_str(frame).ok()?;
    let event_type = fields
        .get("type")
        .and_then(|raw| serde_json::from_str::<String>(raw.get()).ok());
    if event_type.as_deref() != Some("response.create") {
        return None;
    }
    let generate = fields
        .get("generate")
        .and_then(|raw| serde_json::from_str::<bool>(raw.get()).ok())
        .unwrap_or(true);
    // Reuse the native HTTP ingress parser so model/effort/tier/subagent semantics cannot drift
    // between transports. It shallow-parses the top-level object and never materializes content.
    let facts = parse_inbound_scoped(headers, frame.as_bytes(), pool)?;
    Some(WsTurnTelemetry {
        started_at: Instant::now(),
        model: (!facts.model.is_empty()).then_some(facts.model),
        reasoning_effort: facts.effort,
        service_tier: facts.service_tier,
        subagent: facts.ctx.subagent,
        session_key: Some(session_key.value.clone()),
        logical_turn_key: facts.ctx.logical_turn_key,
        estimated_tokens: facts.ctx.estimated_tokens,
        ttft_ms: None,
        log_request: generate,
        _in_flight: None,
    })
}

impl WsTurnTelemetry {
    pub(crate) fn logical_turn_key(&self) -> Option<&str> {
        self.log_request
            .then_some(self.logical_turn_key.as_deref())
            .flatten()
    }

    /// Hold exactly one live-capacity lease for this generating turn. Owner selection is stamped
    /// at the handshake/reselection boundary; a turn on an already-pinned socket is not a new pick.
    pub(crate) async fn track_in_flight(
        &mut self,
        state: &AppState,
        account_id: &AccountId,
    ) -> bool {
        let now = unix_now();
        let pressure_units = state.runtime.request_pressure_units(self.estimated_tokens);
        self._in_flight = state
            .runtime
            .acquire_pinned_in_flight_weighted(
                account_id,
                now,
                &state.lease_metrics,
                pressure_units,
            )
            .await;
        self._in_flight.is_some()
    }

    /// Observe an upstream frame after the pump has decided it is client-visible. Returns a
    /// terminal outcome for `response.completed`, `response.failed`, or a wrapped `error`.
    pub(crate) fn observe(&mut self, frame: &str) -> Option<WsTurnTerminal> {
        if self.ttft_ms.is_none() && is_output_delta(frame) {
            self.ttft_ms = Some(self.started_at.elapsed().as_millis() as i64);
        }

        let value: Value = serde_json::from_str(frame).ok()?;
        match value.get("type").and_then(Value::as_str)? {
            "response.completed" => Some(WsTurnTerminal {
                status: StatusCode::OK,
                usage: parse_response_usage(frame),
                routing: WsRoutingOutcome::Completed,
                protocol_outcome: RequestProtocolOutcome::Completed,
            }),
            "response.failed" => Some(WsTurnTerminal {
                status: StatusCode::BAD_GATEWAY,
                usage: parse_response_usage(frame),
                routing: WsRoutingOutcome::TerminalNoWriteback,
                protocol_outcome: RequestProtocolOutcome::Failed,
            }),
            "response.incomplete" => Some(WsTurnTerminal {
                status: StatusCode::BAD_GATEWAY,
                usage: None,
                routing: WsRoutingOutcome::TerminalNoWriteback,
                protocol_outcome: RequestProtocolOutcome::Incomplete,
            }),
            "error" => {
                let status = value
                    .get("status")
                    .and_then(Value::as_u64)
                    .and_then(|raw| u16::try_from(raw).ok())
                    .and_then(|raw| StatusCode::from_u16(raw).ok())
                    .unwrap_or(StatusCode::BAD_GATEWAY);
                Some(WsTurnTerminal {
                    status,
                    usage: None,
                    routing: WsRoutingOutcome::TerminalNoWriteback,
                    protocol_outcome: RequestProtocolOutcome::Failed,
                })
            }
            _ => None,
        }
    }

    /// Persist and publish the same observability shape as an HTTP-SSE completion.
    pub(crate) async fn finish(self, state: &AppState, account_id: &str, terminal: WsTurnTerminal) {
        let health_id = AccountId::from(account_id);
        match terminal.routing {
            WsRoutingOutcome::Completed => {
                state.runtime.record_success(&health_id);
            }
            WsRoutingOutcome::TransportLoss => {
                crate::ingress::bench_account_for_failure(state, &health_id, None, unix_now())
                    .await;
            }
            WsRoutingOutcome::TerminalNoWriteback => {}
        }
        if !self.log_request {
            return;
        }
        let usage = terminal.usage.unwrap_or_default();
        if let Some(actual_tokens) = pressure_equivalent_tokens(usage) {
            state
                .runtime
                .record_actual_pressure(self.estimated_tokens, actual_tokens);
        }
        let total_tokens = usage.reported_total_tokens.or_else(|| {
            usage
                .input_tokens
                .zip(usage.output_tokens)
                .map(|(input, output)| input.saturating_add(output))
        });
        let cost = self
            .model
            .as_deref()
            .and_then(polyflare_core::pricing::pricing_for_model)
            .zip(usage.input_tokens)
            .zip(usage.output_tokens)
            .map(|((pricing, input), output)| {
                polyflare_core::pricing::cost_usd(
                    pricing,
                    input,
                    output,
                    usage.cached_input_tokens.unwrap_or(0),
                    self.service_tier.as_deref(),
                )
            });
        let duration_ms = self.started_at.elapsed().as_millis() as u64;
        let request_id = format!("{:032x}", rand::random::<u128>());
        let log = RequestLog {
            method: "WS",
            path: "/responses".to_string(),
            provider: Provider::Codex.to_string(),
            aliased: false,
            status: terminal.status,
            duration_ms,
            account_id: Some(account_id.to_string()),
            target_kind: Some("account".to_string()),
            provider_credential_id: None,
            model: self.model,
            upstream_model: None,
            upstream_transport: Some("codex_ws".to_string()),
            reasoning_effort: self.reasoning_effort,
            service_tier: self.service_tier,
            transport: Some("ws".to_string()),
            ttft_ms: self.ttft_ms,
            total_tokens,
            cached_tokens: usage.cached_input_tokens,
            subagent: self.subagent,
            request_id: Some(request_id.clone()),
            session_key: self.session_key,
        };

        log.emit();
        state.log_bus.publish(log.to_log_event());
        state.upstream_request_metrics.record_target(
            &log.provider,
            "account",
            log.account_id.as_deref(),
            log.status.as_u16(),
        );

        let mut record = log.record(unix_now());
        record.input_tokens = usage.input_tokens;
        record.output_tokens = usage.output_tokens;
        record.cached_input_tokens = usage.cached_input_tokens;
        record.reasoning_tokens = usage.reasoning_tokens;
        record.cost_usd = cost;
        record.latency_first_token_ms = log.ttft_ms;
        record.protocol_outcome = Some(terminal.protocol_outcome);
        crate::ingress::queue_persist_request_log(&state.store, record);
        if let Err(error) = state
            .store
            .enqueue_request_usage(polyflare_store::RequestUsageUpdate {
                request_id,
                input_tokens: usage.input_tokens,
                output_tokens: usage.output_tokens,
                cached_input_tokens: usage.cached_input_tokens,
                cache_write_input_tokens: usage.cache_write_input_tokens,
                reasoning_tokens: usage.reasoning_tokens,
                reported_total_tokens: usage.reported_total_tokens,
                orchestration_input_tokens: usage.orchestration_input_tokens,
                orchestration_output_tokens: usage.orchestration_output_tokens,
                orchestration_cached_input_tokens: usage.orchestration_cached_input_tokens,
                cost_usd: cost,
                latency_first_token_ms: log.ttft_ms,
                duration_ms: Some(duration_ms as i64),
                protocol_outcome: terminal.protocol_outcome,
            })
        {
            tracing::warn!(%error, "websocket request usage queue failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accounts_for_prewarm_but_ignores_non_create_frames() {
        let headers = HeaderMap::new();
        assert!(start_turn(
            &headers,
            r#"{"type":"response.create","generate":false,"model":"gpt-5.6-sol"}"#,
            &super::super::session::ws_session_key(&headers, None),
            None,
        )
        .is_some());
        assert!(start_turn(
            &headers,
            r#"{"type":"response.metadata"}"#,
            &super::super::session::ws_session_key(&headers, None),
            None,
        )
        .is_none());
    }

    #[test]
    fn terminal_parser_keeps_priority_metadata_and_numeric_usage() {
        let mut headers = HeaderMap::new();
        headers.insert("x-openai-subagent", "review".parse().unwrap());
        headers.insert("session-id", "session-telemetry-a".parse().unwrap());
        headers.insert("thread-id", "thread-telemetry-a".parse().unwrap());
        headers.insert("x-codex-window-id", "window-telemetry-a".parse().unwrap());
        let expected_session_key = super::super::session::ws_session_key(&headers, None).value;
        let mut turn = start_turn(
            &headers,
            r#"{"type":"response.create","model":"gpt-5.6-sol","reasoning":{"effort":"high"},"service_tier":"priority","input":[]}"#,
            &super::super::session::ws_session_key(&headers, None),
            None,
        )
        .unwrap();
        assert!(turn
            .observe(r#"{"type":"response.output_text.delta","delta":"ignored"}"#)
            .is_none());
        let terminal = turn
            .observe(
                r#"{"type":"response.completed","response":{"usage":{"input_tokens":100,"output_tokens":20,"total_tokens":120,"input_tokens_details":{"cached_tokens":80,"cache_write_tokens":7},"output_tokens_details":{"reasoning_tokens":5}}}}"#,
            )
            .unwrap();
        assert_eq!(terminal.status, StatusCode::OK);
        assert_eq!(terminal.protocol_outcome, RequestProtocolOutcome::Completed);
        let usage = terminal.usage.unwrap();
        assert_eq!(usage.cached_input_tokens, Some(80));
        assert_eq!(usage.cache_write_input_tokens, Some(7));
        assert_eq!(usage.reported_total_tokens, Some(120));
        assert_eq!(turn.model.as_deref(), Some("gpt-5.6-sol"));
        assert_eq!(turn.service_tier.as_deref(), Some("priority"));
        assert_eq!(turn.subagent.as_deref(), Some("review"));
        assert_eq!(
            turn.session_key.as_deref(),
            Some(expected_session_key.as_str()),
            "request telemetry must link to the relay's exact continuity session"
        );
        assert!(turn.ttft_ms.is_some());
    }

    #[test]
    fn wrapped_error_preserves_bounded_http_status() {
        let headers = HeaderMap::new();
        let mut turn = start_turn(
            &headers,
            r#"{"type":"response.create","model":"gpt-5.6-sol","input":[]}"#,
            &super::super::session::ws_session_key(&headers, None),
            None,
        )
        .unwrap();
        let terminal = turn
            .observe(
                r#"{"type":"error","status":429,"error":{"code":"rate_limit_exceeded","message":"not retained"}}"#,
            )
            .unwrap();
        assert_eq!(terminal.status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(terminal.protocol_outcome, RequestProtocolOutcome::Failed);
        assert_eq!(terminal.usage, None);
    }

    #[test]
    fn pooled_ws_turn_key_matches_http_scope_and_differs_across_pools() {
        let mut headers = HeaderMap::new();
        headers.insert("session-id", "session-a".parse().unwrap());
        headers.insert("thread-id", "thread-a".parse().unwrap());
        let metadata = r#"{"turn_id":"turn-a"}"#;
        headers.insert("x-codex-turn-metadata", metadata.parse().unwrap());
        let ws_frame =
            r#"{"type":"response.create","input":[],"client_metadata":{"turn_id":"turn-a"}}"#;
        let session_key = super::super::session::ws_session_key(&headers, Some("premium"));

        let premium_ws = start_turn(&headers, ws_frame, &session_key, Some("premium")).unwrap();
        let basic_ws = start_turn(&headers, ws_frame, &session_key, Some("basic")).unwrap();
        let premium_http = parse_inbound_scoped(
            &headers,
            br#"{"type":"response.create","input":[]}"#,
            Some("premium"),
        )
        .unwrap();

        assert_eq!(
            premium_ws.logical_turn_key(),
            premium_http.ctx.logical_turn_key.as_deref()
        );
        assert_ne!(
            premium_ws.logical_turn_key(),
            basic_ws.logical_turn_key(),
            "different pool boundaries must not share an aggregate budget"
        );
    }
}
