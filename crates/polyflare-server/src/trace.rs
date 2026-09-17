//! Content-free per-request debug trace: what PolyFlare sent upstream and what came back, one
//! line per phase, keyed by a trace id that is also the request_log row's `request_id` on the WS
//! path. Off by default (`debug_trace` setting / `POLYFLARE_DEBUG_TRACE`), live-toggled through
//! the settings API. Built on 2026-09-17 to settle, from real traffic, whether the upstream's
//! "at capacity" refusals track the account, the transport, or the request shape (full-history
//! resend vs anchored delta) — the request_log alone cannot say, because a refused request has no
//! usage row and a relayed refusal looks like a success.
//!
//! **Content-free (inviolable):** every field here is a size, a count, a duration, a frame TYPE
//! name, an error CODE, a status, or a bounded identifier prefix. Never `error.message`, never
//! `input`, never a delta. `scripts/polyflare-trace-report` tabulates the lines one request per
//! row.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::log_bus::{LogBus, LogEvent, LogLevel};
use crate::runtime_settings::RuntimeSettings;

struct Sink {
    settings: Arc<RuntimeSettings>,
    log_bus: Arc<LogBus>,
}

static SINK: OnceLock<Sink> = OnceLock::new();
static SEQ: AtomicU64 = AtomicU64::new(1);

/// Frame lines per turn after which only terminal frames are still emitted (a long generation
/// has thousands of non-delta frames — `output_item.added/done`, `content_part.*` — and the
/// question the trace answers is decided in the first few).
const MAX_FRAME_LINES: u32 = 24;

/// Install once at app build. A second call (tests spawn several apps per process) is a no-op:
/// the first app's settings decide, and in tests the trace is never on.
pub fn install(settings: Arc<RuntimeSettings>, log_bus: Arc<LogBus>) {
    let _ = SINK.set(Sink { settings, log_bus });
}

pub fn enabled() -> bool {
    SINK.get().is_some_and(|s| s.settings.debug_trace())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// What PolyFlare is about to send, as shape only.
pub struct RequestFacts<'a> {
    pub model: &'a str,
    pub effort: Option<&'a str>,
    /// The client sent a `previous_response_id` (a delta on a server-side anchor).
    pub anchored: bool,
    /// PolyFlare rebuilt this as an anchorless full-history resend.
    pub full_resend: bool,
    pub input_count: u32,
    pub estimated_tokens: u32,
    pub body_bytes: usize,
    pub session_key: Option<&'a str>,
    pub turn_key: Option<&'a str>,
    pub subagent: Option<&'a str>,
}

pub struct Usage {
    pub input: Option<i64>,
    pub cached: Option<i64>,
    pub output: Option<i64>,
}

/// One upstream attempt, from send to terminal. Dropped without `terminal` ⇒ the report shows the
/// request line alone, which itself says "never reached a terminal".
pub struct TraceTurn {
    pub id: String,
    started: Instant,
    transport: &'static str,
    account: String,
    model: String,
    session: Option<String>,
    frames: u32,
    frame_lines: u32,
    first_output: Option<(String, i64)>,
    last_error_code: Option<String>,
    finished: bool,
}

/// A trace dropped without a terminal is a stream torn down before its terminal frame — the
/// client hung up, the relay was cancelled, or the process is shutting down. Say so, with what
/// was seen, rather than leaving the request line dangling.
impl Drop for TraceTurn {
    fn drop(&mut self) {
        if !self.finished {
            self.emit_terminal("dropped", None, None, None);
        }
    }
}

fn short(s: Option<&str>) -> String {
    s.map(|s| s.chars().take(12).collect())
        .unwrap_or_else(|| "-".to_string())
}

impl TraceTurn {
    pub fn begin(transport: &'static str, account: &str, facts: RequestFacts<'_>) -> Option<Self> {
        if !enabled() {
            return None;
        }
        let id = format!(
            "{:08x}{:08x}",
            now_ms() as u32,
            SEQ.fetch_add(1, Ordering::Relaxed) as u32 ^ rand::random::<u32>()
        );
        let turn = Self {
            id,
            started: Instant::now(),
            transport,
            account: account.to_string(),
            model: facts.model.to_string(),
            session: facts.session_key.map(|s| s.chars().take(12).collect()),
            frames: 0,
            frame_lines: 0,
            first_output: None,
            last_error_code: None,
            finished: false,
        };
        tracing::info!(
            target: "polyflare_server::trace",
            trace = %turn.id,
            phase = "request",
            transport,
            account = %turn.account,
            model = facts.model,
            effort = facts.effort.unwrap_or("-"),
            anchored = facts.anchored,
            full_resend = facts.full_resend,
            input_count = facts.input_count,
            estimated_tokens = facts.estimated_tokens,
            body_bytes = facts.body_bytes,
            session = %short(facts.session_key),
            turn_key = %short(facts.turn_key),
            subagent = facts.subagent.unwrap_or("-"),
            "trace"
        );
        turn.publish(
            LogLevel::Debug,
            None,
            format!(
                "request {} {} anchored={} full_resend={} items={} est_tokens={} body_bytes={}",
                transport,
                facts.model,
                facts.anchored,
                facts.full_resend,
                facts.input_count,
                facts.estimated_tokens,
                facts.body_bytes
            ),
        );
        Some(turn)
    }

    /// A relay-internal event on this attempt (a ladder rung, an account move) — the upstream
    /// refusal the relay is reacting to, by code and status, and where it is going next.
    pub fn note(&mut self, event: &str, code: Option<&str>, status: Option<u16>, account: &str) {
        if account != self.account {
            self.account = account.to_string();
        }
        tracing::info!(
            target: "polyflare_server::trace",
            trace = %self.id,
            phase = "note",
            event,
            error_code = code.unwrap_or("-"),
            status = status.unwrap_or(0),
            account,
            elapsed_ms = self.started.elapsed().as_millis() as i64,
            "trace"
        );
        self.publish(
            LogLevel::Debug,
            status,
            format!(
                "note {event} code={} status={} account={account}",
                code.unwrap_or("-"),
                status.unwrap_or(0)
            ),
        );
    }

    /// An upstream frame, by type. Deltas are counted, never logged.
    pub fn frame(&mut self, ty: &str, bytes: usize, code: Option<&str>, status: Option<u16>) {
        self.frames += 1;
        let elapsed_ms = self.started.elapsed().as_millis() as i64;
        let is_delta = ty.ends_with(".delta");
        if self.first_output.is_none()
            && !is_delta
            && !matches!(
                ty,
                "response.created" | "response.in_progress" | "keepalive" | "ping"
            )
            && !ty.starts_with("codex.")
        {
            self.first_output = Some((ty.to_string(), elapsed_ms));
        }
        if self.first_output.is_none() && is_delta {
            self.first_output = Some((ty.to_string(), elapsed_ms));
        }
        if code.is_some() {
            self.last_error_code = code.map(str::to_string);
        }
        let is_terminal = matches!(
            ty,
            "response.completed" | "response.failed" | "response.incomplete" | "error"
        );
        if is_delta || (self.frame_lines >= MAX_FRAME_LINES && !is_terminal) {
            return;
        }
        self.frame_lines += 1;
        tracing::info!(
            target: "polyflare_server::trace",
            trace = %self.id,
            phase = "frame",
            n = self.frames,
            frame_type = ty,
            bytes,
            error_code = code.unwrap_or("-"),
            status = status.unwrap_or(0),
            elapsed_ms,
            "trace"
        );
    }

    /// The attempt's end. `outcome` is a short fixed vocabulary word (`completed`, `refused`,
    /// `failed`, `transport_loss`, `stream_error`, `idle_timeout`, `capacity_pre_commit`, …).
    pub fn terminal(
        mut self,
        outcome: &str,
        code: Option<&str>,
        status: Option<u16>,
        usage: Option<Usage>,
    ) {
        self.emit_terminal(outcome, code, status, usage.as_ref());
        self.finished = true;
    }

    fn emit_terminal(
        &self,
        outcome: &str,
        code: Option<&str>,
        status: Option<u16>,
        usage: Option<&Usage>,
    ) {
        let elapsed_ms = self.started.elapsed().as_millis() as i64;
        let code = code.or(self.last_error_code.as_deref());
        let (first_type, first_ms) = self
            .first_output
            .as_ref()
            .map(|(t, ms)| (t.as_str(), *ms))
            .unwrap_or(("-", -1));
        let (input, cached, output) = usage
            .map(|u| {
                (
                    u.input.unwrap_or(-1),
                    u.cached.unwrap_or(-1),
                    u.output.unwrap_or(-1),
                )
            })
            .unwrap_or((-1, -1, -1));
        tracing::info!(
            target: "polyflare_server::trace",
            trace = %self.id,
            phase = "terminal",
            transport = self.transport,
            account = %self.account,
            outcome,
            error_code = code.unwrap_or("-"),
            status = status.unwrap_or(0),
            frames = self.frames,
            first_output = first_type,
            first_output_ms = first_ms,
            elapsed_ms,
            input_tokens = input,
            cached_input_tokens = cached,
            output_tokens = output,
            "trace"
        );
        self.publish(
            LogLevel::Debug,
            status,
            format!(
                "terminal {outcome} code={} status={} frames={} first_output={first_type}@{first_ms}ms \
                 elapsed={elapsed_ms}ms in={input} cached={cached} out={output}",
                code.unwrap_or("-"),
                status.unwrap_or(0),
                self.frames,
            ),
        );
    }

    fn publish(&self, level: LogLevel, status: Option<u16>, message: String) {
        if let Some(sink) = SINK.get() {
            sink.log_bus.publish(LogEvent {
                ts_ms: now_ms(),
                level,
                provider: Some("codex".to_string()),
                account: Some(self.account.clone()),
                target_kind: None,
                target_id: None,
                model: Some(self.model.clone()),
                status,
                latency_ms: Some(self.started.elapsed().as_millis() as i64),
                subagent: None,
                request_id: Some(self.id.clone()),
                session_key: self.session.clone(),
                kind: "trace".to_string(),
                message,
            });
        }
    }
}

/// Parse one SSE `data:` payload / WS text frame into the bounded fields the trace records:
/// the frame type, an error code, a status. Never reads `error.message`.
pub fn frame_facts(payload: &str) -> Option<(String, Option<String>, Option<u16>)> {
    let v: serde_json::Value = serde_json::from_str(payload).ok()?;
    let ty = v.get("type").and_then(|t| t.as_str())?.to_string();
    let code = v
        .pointer("/error/code")
        .or_else(|| v.pointer("/response/error/code"))
        .and_then(|c| c.as_str())
        .map(str::to_string);
    let status = v
        .get("status")
        .and_then(|s| s.as_u64())
        .and_then(|s| u16::try_from(s).ok());
    Some((ty, code, status))
}

/// Usage out of a `response.completed` frame, the three numbers the trace reports.
pub fn usage_facts(payload: &str) -> Option<Usage> {
    let u = crate::usage_capture::parse_response_usage(payload)?;
    Some(Usage {
        input: u.input_tokens,
        cached: u.cached_input_tokens,
        output: u.output_tokens,
    })
}
