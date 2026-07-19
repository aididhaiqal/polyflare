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
//! content — needed to populate the dashboard's per-account and per-model breakdowns. `subagent`
//! (sub-agent identity feature, Task 3) is carried the same way: a bounded role-label slug read off
//! `x-openai-subagent` (`review`/`compact`/`memory_consolidation`/`collab_spawn`, or `None` for the
//! main agent) — a fixed enum-like value, never a request-derived free-form string, so it is
//! content-free by the same reasoning as `model`. When in doubt about any OTHER field, leave it
//! out — do not add fields to `RequestLog` without re-reading SPEC-M5 §3.4's content-safety
//! constraint.
//!
//! The SAME `RequestLog` also produces the content-free row persisted to the `request_log` table
//! (the dashboard's history backend) via [`RequestLog::record`] — deliberately routed through this
//! one struct so there is a single content-safety chokepoint for both the ephemeral event and the
//! durable row. The constraint above binds the persisted record identically.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

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
    /// The codex sub-agent role label from `x-openai-subagent` (`review`/`compact`/
    /// `memory_consolidation`/`collab_spawn`), or `None` for the main agent. A bounded role slug —
    /// routing metadata, never conversation content — same content-safety class as `model`.
    pub subagent: Option<String>,
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
            subagent: self.subagent.clone(),
        }
    }

    /// The content-free live-log-bus form of this request outcome (see `crate::log_bus`). Draws
    /// from EXACTLY the same field set as [`Self::record`] — this is the same content-safety
    /// chokepoint feeding a second sink (an ephemeral broadcast + ring buffer instead of the
    /// durable `request_log` table). `account`/`model`/`subagent` are populated from
    /// `self.account_id`/`self.model`/`self.subagent` now that `RequestLog` carries them.
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
            subagent: self.subagent.clone(),
            kind: "request".to_string(),
            message: format!("req {status} · {provider} · {}ms", self.duration_ms),
        }
    }
}

/// B4/B5 Task 5: a process-global, content-free counter of cross-account failover events. In
/// memory only (like `RuntimeStates`/`LogBus`) — resets on restart. Incremented exactly ONCE per
/// `FailoverVerdict::FailoverNext` transition actually taken by `crate::ingress::run_failover_loop`
/// (i.e. once per real cross-account retry, never per mere classification or per request), so its
/// total is the failover RATE an operator can watch — the visibility the porting doc calls for
/// (codex-lb wedged invisibly; this is the fix's whole point).
#[derive(Default)]
pub struct FailoverMetrics {
    total: AtomicU64,
}

impl FailoverMetrics {
    /// A fresh, zeroed counter, `Arc`-wrapped to match `LogBus::new`'s and `RuntimeStates`'s
    /// `AppState`-field shape.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Records one failover event, returning the new total.
    pub fn record(&self) -> u64 {
        self.total.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// The current total (test/dashboard read path).
    pub fn total(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }
}

/// B4/B5 Task 5: the content-free signal for one actual cross-account failover (a
/// `FailoverVerdict::FailoverNext` transition). Carries ONLY a fixed reason-code label (see
/// `crate::failover::failover_reason_code` — never a raw upstream message/body), the two account
/// row ids involved (content-free identifiers, same class as `RequestLog::account_id`), and the
/// 1-indexed attempt number the request is now making. NEVER a body/message/frame — a leak here is
/// Critical (see the plan's Global Constraints content-safety rule). Emitted from exactly one call
/// site: `crate::ingress::run_failover_loop`, right after a fresh account is picked to retry on.
pub struct FailoverSignal<'a> {
    pub reason: &'static str,
    pub from_account: &'a str,
    pub to_account: &'a str,
    pub attempt: u32,
}

impl FailoverSignal<'_> {
    /// Emits the structured `tracing` event (target `polyflare_server::failover`, isolable from
    /// other crates' traffic exactly like `RequestLog::emit`'s `polyflare_server::request` target).
    pub fn emit(&self) {
        tracing::warn!(
            target: "polyflare_server::failover",
            reason = self.reason,
            from_account = self.from_account,
            to_account = self.to_account,
            attempt = self.attempt,
            "cross-account failover"
        );
    }

    /// The content-free live-log-bus form (see `crate::log_bus`) — same sink `RequestLog` feeds,
    /// so the dashboard's live log stream shows failover events inline with request completions.
    /// `message` is built ENTIRELY from the fixed reason code + the two account ids + the attempt
    /// number — never from request/response content, exactly like `RequestLog::to_log_event`'s
    /// `message`.
    pub fn to_log_event(&self) -> LogEvent {
        LogEvent {
            ts_ms: log_bus::now_ms(),
            level: LogLevel::Warn,
            provider: None,
            account: Some(self.to_account.to_string()),
            model: None,
            status: None,
            latency_ms: None,
            subagent: None,
            kind: "failover".to_string(),
            message: format!(
                "failover reason={} from={} to={} attempt={}",
                self.reason, self.from_account, self.to_account, self.attempt
            ),
        }
    }
}

/// B5 Task 5: a process-global, content-free counter of Layer 2 keepalive-wait TERMINAL outcomes
/// (see `crate::ingress::layer2_wait_stream`). In memory only (like `FailoverMetrics`/
/// `RuntimeStates`/`LogBus`) — resets on restart. Incremented exactly ONCE per wait's terminal exit
/// (recovered-and-spliced, budget-exceeded, still-nothing, or executor-error — every `return`/
/// stream-end site in `layer2_wait_stream`), mirroring `FailoverMetrics`'s "once per real
/// transition, never per mere classification" contract, so its total is a genuine count of "how
/// many times did a client actually sit through a Layer 2 wait."
#[derive(Default)]
pub struct StarvationMetrics {
    total: AtomicU64,
}

impl StarvationMetrics {
    /// A fresh, zeroed counter, `Arc`-wrapped to match `FailoverMetrics::new`'s `AppState`-field
    /// shape.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Records one Layer 2 wait's terminal outcome, returning the new total.
    pub fn record(&self) -> u64 {
        self.total.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// The current total (test/dashboard read path).
    pub fn total(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }
}

/// B5 Task 5: the content-free signal for one Layer 2 keepalive-wait's terminal outcome — emitted
/// from INSIDE `crate::ingress::layer2_wait_stream`'s generator, at the moment the outcome is
/// actually known (the same "emit at the real transition, not at commit time" discipline
/// `FailoverSignal` already establishes for cross-account failover).
///
/// **This is the fix for the disclosed `outcome.account_id` observability gap** (B5 Task 4's
/// report, "Known limitation, not fixed here"): `RouteOutcome`/`RequestLog` for a Layer-2-served
/// request are both finalized SYNCHRONOUSLY inside `responses_handler_impl_with_max_attempts`,
/// BEFORE `layer2_wait_stream`'s generator body is ever polled by axum — i.e. before the wait, the
/// re-select, or the splice have even started. Structurally, `RouteOutcome.account_id` can
/// therefore only ever record the WAIT TARGET (the account `soonest_recover` was waiting for at
/// commit time), never the account the post-wait re-select actually spliced in — which CAN differ
/// in a multi-account pool (a different, also-recovered account may win the post-wait `pick`).
/// Restructuring the ingress to defer `RequestLog` emission until the stream drains would be a much
/// larger, riskier change to the reviewed B5 Task 4 control flow than this task's scope allows (see
/// the plan's Global Constraints: "Do NOT change the Layer 2 control flow beyond ... emitting the
/// signal"). Emitting a SEPARATE, authoritative signal at the splice point — the option the B5 Task
/// 5 brief itself names — is the minimal correct fix: `served_account` on this struct carries the
/// ACTUAL account that served the request (`Some` only on a genuine splice; `None` on every failure
/// terminal, since no account ever actually served the client in those cases), distinct from
/// `wait_target_account` (the best-effort id `RouteOutcome` still carries). Both are the same
/// content-free id class `RequestLog::account_id`/`FailoverSignal`'s account ids already use — NEVER
/// a body/message/frame, and `waited_ms` is a plain duration, never upstream content.
pub struct StarvationSignal<'a> {
    /// `crate::starvation::STARVATION_RECOVERED_REASON` on success, or one of
    /// `crate::starvation::StarvationOutcome::code()`'s three fixed labels on failure.
    pub reason: &'static str,
    /// The account `soonest_recover` selected as the wait target at commit time.
    pub wait_target_account: &'a str,
    /// The account actually spliced in and serving the client — `Some` ONLY on a genuine post-wait
    /// splice success, `None` on every failure terminal (budget-exceeded / still-nothing /
    /// executor-error), since no account ever actually served the client in those cases.
    pub served_account: Option<&'a str>,
    /// Wall-clock milliseconds actually spent waiting (from the moment the generator started, to
    /// this terminal outcome) — a plain duration, never derived from request/response content.
    pub waited_ms: u64,
    /// B10 Task 2: the per-waiter herd-damping jitter offset COMPUTED for THIS wait —
    /// `crate::ingress::wake_jitter_offset_ms(request_key, wake_jitter_ms)`'s return value, a plain
    /// `u64` count of milliseconds in `[0, wake_jitter_ms]`. This is the computed offset, not the
    /// budget-clipped EFFECTIVE delay: in the rare case where the wake target already sits at the
    /// wait-budget ceiling (`target_ms == budget_deadline_ms`), the offset is clamped away and the
    /// effective added delay is smaller (or 0) — this field still reports the raw computed value, so
    /// read it as "how much spread was configured/active for this waiter," not a precise delta. In
    /// the common recover-within-budget case it equals the actual applied delay. Content-free by
    /// construction: a bounded integer derived from a hash of the session key, never the key itself,
    /// a body, or any request/response content — same class as `waited_ms`. Lets an operator see
    /// herd-damping is active (and roughly how spread out concurrent waiters on the same account
    /// are) straight from the existing starvation signal, without a new signal type. Always `0`
    /// when `POLYFLARE_STARVATION_WAKE_JITTER_MS` is unset/`0` (the disable lever) — see
    /// `crate::ingress::wake_jitter_offset_ms`'s doc.
    pub wake_jitter_applied_ms: u64,
}

impl StarvationSignal<'_> {
    /// Emits the structured `tracing` event (target `polyflare_server::starvation`, isolable from
    /// other crates' traffic exactly like `RequestLog::emit`/`FailoverSignal::emit`'s own targets).
    pub fn emit(&self) {
        tracing::warn!(
            target: "polyflare_server::starvation",
            reason = self.reason,
            wait_target_account = self.wait_target_account,
            served_account = self.served_account.unwrap_or(""),
            waited_ms = self.waited_ms,
            wake_jitter_applied_ms = self.wake_jitter_applied_ms,
            "layer 2 starvation wait"
        );
    }

    /// The content-free live-log-bus form (see `crate::log_bus`) — same sink `RequestLog`/
    /// `FailoverSignal` feed, so the dashboard's live log stream shows starvation waits inline.
    /// `account` prefers the SERVED account (the authoritative fix) and falls back to the wait
    /// target only when nothing was ever actually served — never `None` outright, so a starvation
    /// event is always attributable to at least the account it was scoped to.
    pub fn to_log_event(&self) -> LogEvent {
        let account = self
            .served_account
            .map(str::to_string)
            .unwrap_or_else(|| self.wait_target_account.to_string());
        LogEvent {
            ts_ms: log_bus::now_ms(),
            level: LogLevel::Warn,
            provider: None,
            account: Some(account),
            model: None,
            status: None,
            latency_ms: Some(self.waited_ms as i64),
            subagent: None,
            kind: "starvation".to_string(),
            message: format!(
                "starvation wait reason={} wait_target={} served={} waited_ms={} wake_jitter_applied_ms={}",
                self.reason,
                self.wait_target_account,
                self.served_account.unwrap_or("none"),
                self.waited_ms,
                self.wake_jitter_applied_ms
            ),
        }
    }
}

/// B8 Task 4: a process-global, content-free counter of health-tier soft-drain TRANSITIONS (an
/// account actually changing tier — HEALTHY↔DRAINING↔PROBING). In memory only (like
/// `FailoverMetrics`/`StarvationMetrics`/`RuntimeStates`/`LogBus`) — resets on restart. Incremented
/// exactly ONCE per real tier change (`from != to`) actually applied at one of the sites that calls
/// [`emit_health_tier_signal`] — never per mere evaluation that leaves the tier where it was.
///
/// **Scope (B8 review, Finding 3 — read before treating this as a complete churn rate):** this
/// counter/signal covers only PRE-STREAM failures (`crate::ingress::record_failure`) and the
/// usage-refresh poller's transitions (`crate::usage_refresh`, both the codex and, since the
/// Finding 1 fix, the non-codex pass). It does NOT cover mid-stream funnel transitions: the
/// watchdog's own `record_success`/`record_transient_error` calls (`crate::watchdog`'s
/// `ObservingStream::poll_next`) discard the returned `HealthTierTransition` — so a mid-stream
/// `error_drain` (a stream that fails after the first byte, or times out idle) and essentially ALL
/// funnel-driven `probe_promote` recoveries (`record_success` is only ever called from the watchdog)
/// silently update the tier without incrementing this counter or emitting a signal. This is an
/// accepted, deliberate scope choice — threading the log bus/metrics handles into the
/// `ObservingStream` machinery to close it is disproportionate to the gap — not an oversight; see
/// `crate::watchdog`'s `poll_next` for where the transition is dropped. Full recovery telemetry
/// (counting every tier change, not just the pre-stream-failure and poller ones) is a follow-up, not
/// part of B8.
#[derive(Default)]
pub struct HealthTierMetrics {
    total: AtomicU64,
}

impl HealthTierMetrics {
    /// A fresh, zeroed counter, `Arc`-wrapped to match `FailoverMetrics::new`'s / `StarvationMetrics::new`'s
    /// `AppState`-field shape.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Records one health-tier transition, returning the new total.
    pub fn record(&self) -> u64 {
        self.total.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// The current total (test/dashboard read path).
    pub fn total(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }
}

/// B8 Task 4: the content-free signal for one actual health-tier soft-drain transition (a real
/// `from != to` tier change — an account entering/leaving DRAINING or PROBING). Carries ONLY a
/// fixed reason-code label (see below — never a raw upstream message/body/usage number), the single
/// account row id involved (a content-free identifier, same class as `RequestLog::account_id` /
/// `FailoverSignal`'s ids), and the two tier NUMBERS (`0` HEALTHY / `1` DRAINING / `2` PROBING).
/// NEVER a body/message/frame/token, and never a raw usage percentage — a leak here is Critical (see
/// the plan's Global Constraints content-safety rule).
///
/// `reason` is one of five FIXED `&'static str` labels chosen at the transition edge:
/// - `"usage_drain"`  — the poller drove a HEALTHY/PROBING account into DRAINING because usage% hit
///   a threshold.
/// - `"error_drain"`  — an account entered DRAINING because of the error-flapping signal (the
///   runtime funnel, or the poller when only the error condition — not usage — was true).
/// - `"quiet_promote"`— the poller promoted a DRAINING account to PROBING after the quiet timer
///   elapsed with usage back below threshold.
/// - `"probe_promote"`— a PROBING account was promoted back to HEALTHY after its success streak
///   completed.
/// - `"disabled_reset"`— the `POLYFLARE_SOFT_DRAIN_ENABLED=0` disable lever forced a non-HEALTHY
///   account back to HEALTHY.
///
/// Emitted (via [`emit_health_tier_signal`]) from the call sites that own the log bus/metrics
/// handles: `crate::ingress::record_failure` (pre-stream failures) and `crate::usage_refresh`'s
/// poller loop (both the codex `refresh_account` usage-driven edge and, since the B8-review
/// Finding 1 fix, the disjoint non-codex error-driven pass). **Not** emitted for mid-stream
/// transitions the watchdog's funnel calls produce — see [`HealthTierMetrics`]'s doc for that scope
/// gap.
pub struct HealthTierSignal<'a> {
    pub account_id: &'a str,
    pub from_tier: u8,
    pub to_tier: u8,
    pub reason: &'static str,
}

impl HealthTierSignal<'_> {
    /// Emits the structured `tracing` event (target `polyflare_server::health_tier`, isolable from
    /// other crates' traffic exactly like `RequestLog::emit`/`FailoverSignal::emit`'s own targets).
    pub fn emit(&self) {
        tracing::warn!(
            target: "polyflare_server::health_tier",
            reason = self.reason,
            account_id = self.account_id,
            from_tier = self.from_tier,
            to_tier = self.to_tier,
            "health-tier transition"
        );
    }

    /// The content-free live-log-bus form (see `crate::log_bus`) — same sink `RequestLog`/
    /// `FailoverSignal`/`StarvationSignal` feed, so the dashboard's live log stream shows soft-drain
    /// transitions inline. `message` is built ENTIRELY from the fixed reason code + the account id +
    /// the two tier numbers — never from request/response content or a raw usage percentage.
    pub fn to_log_event(&self) -> LogEvent {
        LogEvent {
            ts_ms: log_bus::now_ms(),
            level: LogLevel::Warn,
            provider: None,
            account: Some(self.account_id.to_string()),
            model: None,
            status: None,
            latency_ms: None,
            subagent: None,
            kind: "health_tier".to_string(),
            message: format!(
                "health_tier reason={} account={} from={} to={}",
                self.reason, self.account_id, self.from_tier, self.to_tier
            ),
        }
    }
}

/// B8 Task 4: emit one [`HealthTierSignal`] — the `tracing` event, the `log_bus` event, and the
/// [`HealthTierMetrics`] bump — at a single call site, mirroring the exact triple
/// `crate::ingress::emit_starvation_signal` / `run_failover_loop` already perform for their signals
/// (`emit()` + `log_bus.publish(..)` + `metrics.record()`, together, at the real transition). Takes
/// primitives (never the `runtime_state` transition struct) so `observability` stays free of a
/// `runtime_state` dependency. Called ONLY on an actual tier change (`from != to`); the callers pass
/// a transition they already confirmed non-empty.
pub fn emit_health_tier_signal(
    log_bus: &crate::log_bus::LogBus,
    metrics: &HealthTierMetrics,
    account_id: &str,
    from_tier: u8,
    to_tier: u8,
    reason: &'static str,
) {
    let signal = HealthTierSignal {
        account_id,
        from_tier,
        to_tier,
        reason,
    };
    signal.emit();
    log_bus.publish(signal.to_log_event());
    metrics.record();
}

/// C9 Task 4: a process-global, content-free pair of counters for the in-flight lease lifecycle
/// (see `crate::runtime_state::RuntimeStates::acquire_in_flight` / `crate::runtime_state::
/// InFlightGuard`). In memory only (like `FailoverMetrics`/`StarvationMetrics`/`HealthTierMetrics`)
/// — resets on restart. `acquired` is bumped exactly once per `acquire_in_flight` call (one per
/// streaming selection attempt); `released` is bumped exactly once per `InFlightGuard::drop` — i.e.
/// once per way that lease actually ends (clean drain, client disconnect, mid-stream error,
/// idle-timeout, or a failover reselect's dropped pre-stream attempt). Carries NO content — counts
/// only, same class as every other counter in this module.
///
/// Deliberately two independent monotonic counters rather than a single live gauge: `acquired -
/// released` (see [`Self::current`]) already recovers the instantaneous in-flight total for a
/// dashboard, while keeping both cumulative totals visible separately lets an operator distinguish
/// "traffic volume" from "steady-state concurrency" and, if `released` ever permanently lags
/// `acquired` by a growing margin, spot a leaked-guard regression (Task 1-2's leak-proof guarantee
/// should make that impossible in practice, but the counters make a violation observable rather
/// than silent).
#[derive(Default)]
pub struct LeaseMetrics {
    acquired: AtomicU64,
    released: AtomicU64,
}

impl LeaseMetrics {
    /// A fresh, zeroed pair of counters, `Arc`-wrapped to match `FailoverMetrics::new`'s /
    /// `StarvationMetrics::new`'s / `HealthTierMetrics::new`'s `AppState`-field shape.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Records one lease acquisition (one `acquire_in_flight` call), returning the new total.
    pub fn record_acquire(&self) -> u64 {
        self.acquired.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Records one lease release (one `InFlightGuard::drop`), returning the new total.
    pub fn record_release(&self) -> u64 {
        self.released.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// The cumulative acquire count (test/dashboard read path).
    pub fn acquired(&self) -> u64 {
        self.acquired.load(Ordering::Relaxed)
    }

    /// The cumulative release count (test/dashboard read path).
    pub fn released(&self) -> u64 {
        self.released.load(Ordering::Relaxed)
    }

    /// The derived instantaneous in-flight total (`acquired - released`), saturating at `0` so a
    /// transient read race between the two independent atomics can never underflow into a bogus
    /// huge `u64` — a dashboard gauge reading, not a source of truth (the authoritative live count
    /// per-account is `RuntimeState.in_flight`; this is the process-wide aggregate view).
    pub fn current(&self) -> u64 {
        self.acquired().saturating_sub(self.released())
    }
}

/// C11b Task 1: a process-global, content-free counter of completed proxied requests, labeled by
/// `(account_id, status)`. In memory only (like `FailoverMetrics`/`StarvationMetrics`/
/// `HealthTierMetrics`/`LeaseMetrics`) — resets on restart. Bumped exactly ONCE per client request,
/// at each of the 3 request-completion wrapper sites (`control_route`/`responses_route`/
/// `messages_route`), from the same content-free `RequestLog` those sites already build — never
/// derived from the `request_log` table (which C12 prunes; a pruned-derived counter would
/// decrement, breaking Prometheus counter monotonicity). `account_id` is the opaque store-row id
/// (same content-free class `RequestLog::account_id` already carries) — `None` (e.g. a
/// 503-no-eligible outcome, no account was ever selected) is stored under the `""` key rather than
/// dropped, mirroring the existing `pool: None → pool=""` render convention (`metrics.rs`), so a
/// no-eligible-account rate stays visible to an operator. `status` is the numeric HTTP status code
/// only — never an upstream error message/body. Cardinality is bounded: accounts are
/// operator-managed (tens), statuses are a small fixed HTTP-code set.
#[derive(Default)]
pub struct UpstreamRequestMetrics {
    inner: RwLock<HashMap<(String, u16), u64>>,
}

impl UpstreamRequestMetrics {
    /// A fresh, empty map, `Arc`-wrapped to match `FailoverMetrics::new`'s / `LeaseMetrics::new`'s
    /// `AppState`-field shape.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Records one completed request for `(account_id, status)`. `account_id: None` is stored
    /// under the `""` key (see the struct doc). Recovers from a poisoned lock (a prior writer
    /// panic) rather than panicking itself, mirroring `RuntimeStates::overlay`'s
    /// `.unwrap_or_else(|e| e.into_inner())` idiom — a metrics bump must never be the thing that
    /// takes down a request-completion path.
    pub fn record(&self, account_id: Option<&str>, status: u16) {
        let mut map = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let entry = map
            .entry((account_id.unwrap_or("").to_string(), status))
            .or_insert(0);
        *entry += 1;
    }

    /// A cloned-out snapshot of every `(account_id, status, count)` recorded so far (test/render
    /// read path — see `crate::metrics`).
    pub fn snapshot(&self) -> Vec<(String, u16, u64)> {
        let map = self.inner.read().unwrap_or_else(|e| e.into_inner());
        map.iter()
            .map(|((account_id, status), count)| (account_id.clone(), *status, *count))
            .collect()
    }
}

/// C11b Task 1: a process-global, content-free counter of 429 rate-limit writebacks, labeled by a
/// FIXED `type` string (`"upstream"` when the upstream supplied a `Retry-After`, `"backoff"` when
/// PolyFlare computed its own exponential backoff). In memory only (like `UpstreamRequestMetrics`
/// above) — resets on restart. Bumped exactly ONCE per `RuntimeStates::record_rate_limit` call —
/// the single true 429 chokepoint (all ~10 `record_failure` callers funnel through the one
/// `sig.status == 429` branch). `type` is never an upstream error message/body/retry-after value —
/// only one of the two fixed labels, so cardinality is a constant 2.
#[derive(Default)]
pub struct RateLimitMetrics {
    inner: RwLock<HashMap<&'static str, u64>>,
}

impl RateLimitMetrics {
    /// A fresh, empty map, `Arc`-wrapped to match `UpstreamRequestMetrics::new`'s `AppState`-field
    /// shape.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Records one 429 writeback of `kind` (`"upstream"` or `"backoff"`). Recovers from a poisoned
    /// lock rather than panicking, exactly like `UpstreamRequestMetrics::record`.
    pub fn record(&self, kind: &'static str) {
        let mut map = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let entry = map.entry(kind).or_insert(0);
        *entry += 1;
    }

    /// A cloned-out snapshot of every `(type, count)` recorded so far (test/render read path — see
    /// `crate::metrics`).
    pub fn snapshot(&self) -> Vec<(String, u64)> {
        let map = self.inner.read().unwrap_or_else(|e| e.into_inner());
        map.iter()
            .map(|(kind, count)| (kind.to_string(), *count))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// A minimal `tracing::Subscriber` that records every event on `.1`'s target as a flat
    /// `field=value` string and ignores everything else. Enough to assert our one content-safe
    /// event fired with exactly the expected fields and nothing more. Parameterized by target
    /// (rather than hardcoded to `"polyflare_server::request"`) so it also covers the
    /// `"polyflare_server::failover"` signal below.
    struct Capture(Arc<Mutex<Vec<String>>>, &'static str);

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
            metadata.target() == self.1
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
        let dispatch =
            tracing::Dispatch::new(Capture(captured.clone(), "polyflare_server::request"));

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
                subagent: None,
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

    /// Task 3 (sub-agent identity): `RequestLog::record`/`RequestLog::to_log_event` — the two
    /// content-safety chokepoints feeding the persisted `request_log` table and the live-log-bus —
    /// carry `subagent` through unchanged, exactly like `account_id`/`model` already do. `emit()`'s
    /// tracing event is untouched by this field (see `request_completion_event_carries_only_safe_fields`
    /// above — `account_id`/`model`/`subagent` are deliberately NOT part of that narrower event).
    #[test]
    fn record_and_to_log_event_carry_subagent_like_model_and_account_id() {
        let log = RequestLog {
            method: "POST",
            path: "/responses",
            provider: Provider::Codex,
            aliased: false,
            status: StatusCode::OK,
            duration_ms: 42,
            account_id: Some("acct-1".to_string()),
            model: Some("gpt-5.6-sol".to_string()),
            reasoning_effort: None,
            service_tier: None,
            transport: Some("http".to_string()),
            ttft_ms: None,
            total_tokens: None,
            cached_tokens: None,
            subagent: Some("review".to_string()),
        };

        let record = log.record(100);
        assert_eq!(record.subagent.as_deref(), Some("review"));
        assert_eq!(record.account_id.as_deref(), Some("acct-1"));
        assert_eq!(record.model.as_deref(), Some("gpt-5.6-sol"));

        let ev = log.to_log_event();
        assert_eq!(ev.subagent.as_deref(), Some("review"));
        assert_eq!(ev.account.as_deref(), Some("acct-1"));
        assert_eq!(ev.model.as_deref(), Some("gpt-5.6-sol"));

        // Never a bearer/token/session id/raw body — `subagent` is a bounded role slug, not
        // request/response content, same content-safety class as every other field here.
        for forbidden in ["bearer", "token", "sess_", "session", "body", "input"] {
            assert!(
                !ev.message.to_lowercase().contains(forbidden),
                "forbidden content `{forbidden}` leaked into request log event: {}",
                ev.message
            );
        }
    }

    /// The main agent (no `x-openai-subagent` header) round-trips `subagent: None` through both
    /// sinks — a missing label must never be silently substituted with an empty string or a
    /// placeholder that could be mistaken for a real role slug.
    #[test]
    fn record_and_to_log_event_carry_none_subagent_for_the_main_agent() {
        let log = RequestLog {
            method: "POST",
            path: "/responses",
            provider: Provider::Codex,
            aliased: false,
            status: StatusCode::OK,
            duration_ms: 10,
            account_id: None,
            model: None,
            reasoning_effort: None,
            service_tier: None,
            transport: None,
            ttft_ms: None,
            total_tokens: None,
            cached_tokens: None,
            subagent: None,
        };

        assert_eq!(log.record(0).subagent, None);
        assert_eq!(log.to_log_event().subagent, None);
    }

    #[test]
    fn events_outside_the_request_target_are_ignored() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let dispatch =
            tracing::Dispatch::new(Capture(captured.clone(), "polyflare_server::request"));

        tracing::dispatcher::with_default(&dispatch, || {
            tracing::info!(target: "some_other_crate", "unrelated noise");
        });

        assert!(
            captured.lock().unwrap().is_empty(),
            "Capture must ignore events outside its target"
        );
    }

    #[test]
    fn failover_metrics_counts_exactly_the_recorded_events() {
        let m = FailoverMetrics::new();
        assert_eq!(m.total(), 0, "starts at zero");
        assert_eq!(m.record(), 1);
        assert_eq!(m.record(), 2);
        assert_eq!(m.total(), 2);
    }

    /// The failover signal's `tracing` event carries reason + both account ids + the attempt
    /// number ONLY — this is the content-safety-critical assertion: a leak here is Critical per
    /// the plan's Global Constraints.
    #[test]
    fn failover_signal_event_carries_only_reason_ids_and_attempt() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let dispatch =
            tracing::Dispatch::new(Capture(captured.clone(), "polyflare_server::failover"));

        tracing::dispatcher::with_default(&dispatch, || {
            FailoverSignal {
                reason: "rate_limited",
                from_account: "acct-a",
                to_account: "acct-b",
                attempt: 2,
            }
            .emit();
        });

        let events = captured.lock().unwrap();
        assert_eq!(events.len(), 1, "expected exactly one failover event");
        let line = &events[0];
        for expected in [
            "reason=rate_limited",
            "from_account=acct-a",
            "to_account=acct-b",
            "attempt=2",
        ] {
            assert!(line.contains(expected), "missing `{expected}` in: {line}");
        }
    }

    /// The failover signal's `LogEvent` (dashboard live-log form) must never carry a body,
    /// message, or frame — only the reason code, account ids, and attempt number, exactly like
    /// the `tracing` event.
    #[test]
    fn failover_signal_log_event_is_content_free() {
        let ev = FailoverSignal {
            reason: "transient",
            from_account: "acct-a",
            to_account: "acct-b",
            attempt: 3,
        }
        .to_log_event();

        assert_eq!(ev.kind, "failover");
        assert_eq!(ev.account.as_deref(), Some("acct-b"));
        assert!(ev.message.contains("reason=transient"));
        assert!(ev.message.contains("from=acct-a"));
        assert!(ev.message.contains("to=acct-b"));
        assert!(ev.message.contains("attempt=3"));

        for forbidden in [
            "bearer",
            "body",
            "content",
            "delta",
            "text",
            "input",
            "message\":",
        ] {
            assert!(
                !ev.message.to_lowercase().contains(forbidden),
                "forbidden content `{forbidden}` leaked into failover log event: {}",
                ev.message
            );
        }
    }

    #[test]
    fn starvation_metrics_counts_exactly_the_recorded_events() {
        let m = StarvationMetrics::new();
        assert_eq!(m.total(), 0, "starts at zero");
        assert_eq!(m.record(), 1);
        assert_eq!(m.record(), 2);
        assert_eq!(m.total(), 2);
    }

    /// The starvation signal's `tracing` event carries reason + both account fields + the waited
    /// duration ONLY — this is the content-safety-critical assertion, same class as
    /// `failover_signal_event_carries_only_reason_ids_and_attempt`.
    #[test]
    fn starvation_signal_event_carries_only_reason_ids_and_waited_ms() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let dispatch =
            tracing::Dispatch::new(Capture(captured.clone(), "polyflare_server::starvation"));

        tracing::dispatcher::with_default(&dispatch, || {
            StarvationSignal {
                reason: crate::starvation::STARVATION_RECOVERED_REASON,
                wait_target_account: "acct-a",
                served_account: Some("acct-b"),
                waited_ms: 1234,
                wake_jitter_applied_ms: 777,
            }
            .emit();
        });

        let events = captured.lock().unwrap();
        assert_eq!(events.len(), 1, "expected exactly one starvation event");
        let line = &events[0];
        for expected in [
            "reason=starvation_wait_recovered",
            "wait_target_account=acct-a",
            "served_account=acct-b",
            "waited_ms=1234",
            "wake_jitter_applied_ms=777",
        ] {
            assert!(line.contains(expected), "missing `{expected}` in: {line}");
        }
    }

    /// A failure terminal (`served_account: None`) emits an empty `served_account` field — never a
    /// panic, never a substituted value that could be mistaken for a real account id.
    #[test]
    fn starvation_signal_event_with_no_served_account_emits_empty_field() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let dispatch =
            tracing::Dispatch::new(Capture(captured.clone(), "polyflare_server::starvation"));

        tracing::dispatcher::with_default(&dispatch, || {
            StarvationSignal {
                reason: crate::starvation::StarvationOutcome::BudgetExceeded.code(),
                wait_target_account: "acct-a",
                served_account: None,
                waited_ms: 60000,
                wake_jitter_applied_ms: 0,
            }
            .emit();
        });

        let events = captured.lock().unwrap();
        let line = &events[0];
        assert!(line.contains("reason=starvation_wait_budget_exceeded"));
        assert!(line.contains("served_account= ") || line.contains("served_account=\"\""));
    }

    /// The starvation signal's `LogEvent` (dashboard live-log form) must never carry a body,
    /// message, or frame — only the reason code, both account fields, and the waited duration,
    /// exactly like `failover_signal_log_event_is_content_free`. On success, `account` prefers the
    /// SERVED account — the fix for the disclosed `outcome.account_id` gap.
    #[test]
    fn starvation_signal_log_event_prefers_served_account_and_is_content_free() {
        let ev = StarvationSignal {
            reason: crate::starvation::STARVATION_RECOVERED_REASON,
            wait_target_account: "acct-a",
            served_account: Some("acct-b"),
            waited_ms: 2000,
            wake_jitter_applied_ms: 350,
        }
        .to_log_event();

        assert_eq!(ev.kind, "starvation");
        assert_eq!(
            ev.account.as_deref(),
            Some("acct-b"),
            "the SERVED account (not the wait target) is the authoritative attribution"
        );
        assert!(ev.message.contains("wait_target=acct-a"));
        assert!(ev.message.contains("served=acct-b"));
        assert!(ev.message.contains("waited_ms=2000"));
        assert!(ev.message.contains("wake_jitter_applied_ms=350"));

        for forbidden in [
            "bearer",
            "body",
            "content",
            "delta",
            "text",
            "input",
            "message\":",
        ] {
            assert!(
                !ev.message.to_lowercase().contains(forbidden),
                "forbidden content `{forbidden}` leaked into starvation log event: {}",
                ev.message
            );
        }
    }

    #[test]
    fn health_tier_metrics_counts_exactly_the_recorded_events() {
        let m = HealthTierMetrics::new();
        assert_eq!(m.total(), 0, "starts at zero");
        assert_eq!(m.record(), 1);
        assert_eq!(m.record(), 2);
        assert_eq!(m.total(), 2);
    }

    /// The health-tier signal's `tracing` event carries reason + the account id + the two tier
    /// numbers ONLY — this is the content-safety-critical assertion: a leak here (a body, a token,
    /// or a raw usage percentage) is Critical per the plan's Global Constraints.
    #[test]
    fn health_tier_signal_event_carries_only_reason_id_and_tiers() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let dispatch =
            tracing::Dispatch::new(Capture(captured.clone(), "polyflare_server::health_tier"));

        tracing::dispatcher::with_default(&dispatch, || {
            HealthTierSignal {
                account_id: "acct-a",
                from_tier: 0,
                to_tier: 1,
                reason: "usage_drain",
            }
            .emit();
        });

        let events = captured.lock().unwrap();
        assert_eq!(events.len(), 1, "expected exactly one health-tier event");
        let line = &events[0];
        for expected in [
            "reason=usage_drain",
            "account_id=acct-a",
            "from_tier=0",
            "to_tier=1",
        ] {
            assert!(line.contains(expected), "missing `{expected}` in: {line}");
        }
        // Never a body/token/session/usage percentage.
        for forbidden in [
            "bearer", "token", "sess_", "session", "percent", "used", "body", "content",
        ] {
            assert!(
                !line.to_lowercase().contains(forbidden),
                "forbidden content `{forbidden}` leaked into health-tier event: {line}"
            );
        }
    }

    /// The health-tier signal's `LogEvent` (dashboard live-log form) must never carry a body,
    /// message, frame, or usage number — only the reason code, account id, and tier numbers,
    /// exactly like the `tracing` event.
    #[test]
    fn health_tier_signal_log_event_is_content_free() {
        let ev = HealthTierSignal {
            account_id: "acct-a",
            from_tier: 1,
            to_tier: 2,
            reason: "quiet_promote",
        }
        .to_log_event();

        assert_eq!(ev.kind, "health_tier");
        assert_eq!(ev.account.as_deref(), Some("acct-a"));
        assert_eq!(ev.model, None);
        assert_eq!(ev.status, None);
        assert!(ev.message.contains("reason=quiet_promote"));
        assert!(ev.message.contains("account=acct-a"));
        assert!(ev.message.contains("from=1"));
        assert!(ev.message.contains("to=2"));

        for forbidden in [
            "bearer",
            "body",
            "content",
            "delta",
            "text",
            "input",
            "percent",
            "used",
            "message\":",
        ] {
            assert!(
                !ev.message.to_lowercase().contains(forbidden),
                "forbidden content `{forbidden}` leaked into health-tier log event: {}",
                ev.message
            );
        }
    }

    /// A failure terminal falls back to the wait-target account for `LogEvent.account` — always
    /// attributable to at least the account the wait was scoped to, never `None` outright.
    #[test]
    fn starvation_signal_log_event_falls_back_to_wait_target_when_nothing_served() {
        let ev = StarvationSignal {
            reason: crate::starvation::StarvationOutcome::StillNothing.code(),
            wait_target_account: "acct-a",
            served_account: None,
            waited_ms: 500,
            wake_jitter_applied_ms: 0,
        }
        .to_log_event();

        assert_eq!(ev.account.as_deref(), Some("acct-a"));
        assert!(ev.message.contains("served=none"));
    }

    #[test]
    fn lease_metrics_counts_acquire_and_release_independently() {
        let m = LeaseMetrics::new();
        assert_eq!(m.acquired(), 0, "starts at zero");
        assert_eq!(m.released(), 0, "starts at zero");
        assert_eq!(m.current(), 0);

        assert_eq!(m.record_acquire(), 1);
        assert_eq!(m.record_acquire(), 2);
        assert_eq!(m.acquired(), 2);
        assert_eq!(m.released(), 0);
        assert_eq!(m.current(), 2, "2 acquired, 0 released ⇒ 2 in flight");

        assert_eq!(m.record_release(), 1);
        assert_eq!(m.acquired(), 2);
        assert_eq!(m.released(), 1);
        assert_eq!(m.current(), 1, "2 acquired, 1 released ⇒ 1 in flight");

        assert_eq!(m.record_release(), 2);
        assert_eq!(m.current(), 0, "balanced acquire/release ⇒ 0 in flight");
    }

    /// A stray release (more releases than acquires — should never happen given the leak-proof
    /// guard, but the counter is a plain accumulator, not itself the enforcement mechanism) must
    /// saturate rather than underflow/panic.
    #[test]
    fn lease_metrics_current_saturates_when_released_exceeds_acquired() {
        let m = LeaseMetrics::new();
        m.record_release();
        m.record_release();
        assert_eq!(m.acquired(), 0);
        assert_eq!(m.released(), 2);
        assert_eq!(m.current(), 0, "saturates at 0, never underflows");
    }

    // C11b Task 1: `UpstreamRequestMetrics` — labeled by `(account_id, status)`.

    #[test]
    fn upstream_request_metrics_records_and_dedupes_by_account_and_status() {
        let m = UpstreamRequestMetrics::new();
        m.record(Some("a"), 200);
        m.record(Some("a"), 200);
        let snapshot = m.snapshot();
        assert_eq!(
            snapshot,
            vec![("a".to_string(), 200, 2)],
            "same (account_id, status) accumulates into one entry"
        );
    }

    #[test]
    fn upstream_request_metrics_distinct_account_or_status_are_distinct_keys() {
        let m = UpstreamRequestMetrics::new();
        m.record(Some("a"), 200);
        m.record(Some("b"), 200);
        m.record(Some("a"), 500);
        let mut snapshot = m.snapshot();
        snapshot.sort();
        assert_eq!(
            snapshot,
            vec![
                ("a".to_string(), 200, 1),
                ("a".to_string(), 500, 1),
                ("b".to_string(), 200, 1),
            ]
        );
    }

    #[test]
    fn upstream_request_metrics_none_account_id_records_under_empty_string_key() {
        let m = UpstreamRequestMetrics::new();
        m.record(None, 503);
        let snapshot = m.snapshot();
        assert_eq!(
            snapshot,
            vec![("".to_string(), 503, 1)],
            "None account_id (e.g. 503-no-eligible) must still be visible, keyed as \"\""
        );
    }

    #[test]
    fn upstream_request_metrics_is_monotonic() {
        let m = UpstreamRequestMetrics::new();
        for i in 1..=5u64 {
            m.record(Some("a"), 200);
            let snapshot = m.snapshot();
            let (_, _, count) = snapshot
                .iter()
                .find(|(id, status, _)| id == "a" && *status == 200)
                .expect("entry present");
            assert_eq!(*count, i, "count only ever increases, never resets");
        }
    }

    #[test]
    fn upstream_request_metrics_empty_snapshot_is_empty_vec() {
        let m = UpstreamRequestMetrics::new();
        assert_eq!(m.snapshot(), Vec::<(String, u16, u64)>::new());
    }

    // C11b Task 1: `RateLimitMetrics` — labeled by a fixed `type` string.

    #[test]
    fn rate_limit_metrics_records_distinct_kinds_and_increments() {
        let m = RateLimitMetrics::new();
        m.record("upstream");
        m.record("backoff");
        m.record("upstream");
        let mut snapshot = m.snapshot();
        snapshot.sort();
        assert_eq!(
            snapshot,
            vec![("backoff".to_string(), 1), ("upstream".to_string(), 2),]
        );
    }

    #[test]
    fn rate_limit_metrics_empty_snapshot_is_empty_vec() {
        let m = RateLimitMetrics::new();
        assert_eq!(m.snapshot(), Vec::<(String, u64)>::new());
    }
}
