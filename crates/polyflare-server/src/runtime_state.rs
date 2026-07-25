//! Live per-account routing runtime state — the failure-driven inputs the selector reads
//! (`error_count` / `cooldown_until` / `last_error_at` / `last_selected_at`) but the durable store
//! does NOT carry. codex-lb keeps the analog in-memory (`_runtime`); PolyFlare does too.
//!
//! # Why in-memory + a read-time overlay (not the account cache)
//! These fields churn on essentially every request (an error, a cooldown, a selection). Baking them
//! into the `AccountCache`'s built `Vec` would force an O(accounts) sqlite rebuild on every mutation
//! (see `account_cache.rs`, which explicitly reserves runtime counters for "a separate overlay
//! layered on at read time"). So this is a plain `RwLock<HashMap>` that ingress [`overlay`]s onto the
//! (cheap, already-cloned) filtered snapshot slice right before `Selector::pick` — the eligibility
//! logic in `select.rs` already reads these fields, it just never saw non-neutral values until now.
//!
//! Only the COARSE, durable state (`status` / `reset_at`) is persisted to the accounts table (via
//! `AccountRepo`); the fine runtime counters live only here and reset on restart — exactly codex-lb's
//! split.
//!
//! [`overlay`]: RuntimeStates::overlay

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use polyflare_core::{AccountId, AccountSnapshot, SelectionCtx, Selector};

use crate::observability::{LeaseMetrics, RateLimitMetrics};

/// Base for the exponential error backoff, in milliseconds (`0.2s * 2^(n-1)` — codex-lb
/// `retry.py:51-77`). The eligibility error-backoff gate (`select.rs`) uses the same shape.
const BACKOFF_BASE_MS: i64 = 200;
/// Cap on the exponent so a large `error_count` can't overflow / produce an absurd delay.
const BACKOFF_MAX_SHIFT: u32 = 16;
const PRESSURE_TOKENS_PER_UNIT: u64 = 16_384;
const DEFAULT_MAX_REQUEST_PRESSURE_UNITS: u32 = 16;
/// Floor on a rate-limit cooldown (codex-lb `RATE_LIMITED_MIN_COOLDOWN_SECONDS`): even a tiny
/// upstream `Retry-After` benches the account for at least this long.
pub const RATE_LIMITED_MIN_COOLDOWN_SECS: i64 = 30;
/// Ceiling on any single cooldown — clamps a pathological / hostile upstream `Retry-After` (mirrors
/// ccflare's 24h reset clamp) so it can neither pin an account off for days nor overflow `now + delay`.
pub const MAX_COOLDOWN_SECS: i64 = 24 * 3600;
/// Fixed cooldown applied to a quota-exceeded account (codex-lb `QUOTA_EXCEEDED_COOLDOWN_SECONDS`).
pub const QUOTA_EXCEEDED_COOLDOWN_SECS: i64 = 120;

/// Health-tier soft-drain thresholds — a byte-faithful port of codex-lb's
/// `app/core/balancer/logic.py:84-93`. See [`evaluate_health_tier`] for the transition table these
/// feed. Tier values themselves are the plain `u8`s `0` (HEALTHY), `1` (DRAINING), `2` (PROBING) —
/// codex-lb's own enum is likewise a flat int tier, not worth a Rust enum for three states that only
/// ever move through [`evaluate_health_tier`].
///
/// Primary usage threshold (percent) at/above which an account should soft-drain.
pub const DRAIN_PRIMARY_PCT: f64 = 85.0;
/// Secondary usage threshold (percent) at/above which an account should soft-drain.
pub const DRAIN_SECONDARY_PCT: f64 = 90.0;
/// Window (seconds) within which recent errors count toward the error-flapping drain condition.
pub const DRAIN_ERROR_WINDOW_SECS: i64 = 60;
/// Error count at/above which (within the window) an account is considered flapping.
pub const DRAIN_ERROR_COUNT: u32 = 2;
/// Minimum time (seconds) a DRAINING account must sit quiet (`should_drain` false) before it may be
/// promoted to PROBING.
pub const PROBE_QUIET_SECS: i64 = 60;
/// Consecutive successes required while PROBING before promotion back to HEALTHY.
pub const PROBE_SUCCESS_STREAK_REQUIRED: u32 = 3;

/// B8 Task 4: the five FIXED, content-free reason labels a health-tier transition can carry — a
/// leaf reason code the caller hands to `crate::observability::HealthTierSignal`. Authored HERE (at
/// the transition edge, where the "why" is unambiguous) rather than reconstructed at the emit site,
/// but kept as plain `&'static str` data so `runtime_state` takes NO dependency on `observability`.
/// The poller drove a HEALTHY/PROBING account into DRAINING because usage% crossed a threshold.
pub const REASON_USAGE_DRAIN: &str = "usage_drain";
/// An account entered DRAINING because of the error-flapping signal (the funnel always; the poller
/// when the error condition — not usage — was the sole drain cause).
pub const REASON_ERROR_DRAIN: &str = "error_drain";
/// The poller promoted a DRAINING account to PROBING after the quiet timer elapsed below threshold.
pub const REASON_QUIET_PROMOTE: &str = "quiet_promote";
/// A PROBING account was promoted back to HEALTHY after its success streak completed.
pub const REASON_PROBE_PROMOTE: &str = "probe_promote";
/// The `POLYFLARE_SOFT_DRAIN_ENABLED=0` disable lever forced a non-HEALTHY account back to HEALTHY.
pub const REASON_DISABLED_RESET: &str = "disabled_reset";

/// A logical turn's aggregate attempt history only needs to cover Codex's immediate retry window.
/// Sliding expiry bounds process memory while still joining HTTP retries, WS replay, and transport
/// fallback for the same active turn.
const LOGICAL_TURN_ATTEMPT_TTL_SECS: i64 = 15 * 60;
/// Defense-in-depth bound for hostile high-cardinality metadata. Keys are already fixed-size
/// SHA-256 hex digests, but the number of distinct logical turns must be bounded too.
const LOGICAL_TURN_ATTEMPT_MAX_KEYS: usize = 16_384;
const LOGICAL_TURN_ATTEMPT_CLEANUP_INTERVAL_SECS: i64 = 60;

#[derive(Clone, Copy)]
struct LogicalTurnAttempts {
    consumed: u32,
    expires_at: i64,
}

#[derive(Default)]
struct LogicalTurnAttemptRegistry {
    entries: HashMap<String, LogicalTurnAttempts>,
    next_cleanup_at: i64,
}

/// B8 Task 4: one actual health-tier change (`from != to`), returned UPWARD by the funnel /
/// poller methods so the CALLER — which owns the `log_bus` + `HealthTierMetrics` handles — emits the
/// content-free `crate::observability::HealthTierSignal`. Returned as `Some` ONLY when a real tier
/// transition was applied; a no-op evaluation (tier unchanged, or a refused funnel promotion)
/// returns `None`, so a caller emits exactly once per genuine transition, never per mere evaluation.
/// Carries no usage/error content — just the two tier numbers and a fixed [`REASON_USAGE_DRAIN`]-
/// class label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HealthTierTransition {
    pub from: u8,
    pub to: u8,
    pub reason: &'static str,
}

/// Wall-clock seconds since the Unix epoch. `record_success`'s signature is fixed (callers, e.g.
/// `watchdog.rs`, invoke it with no `now` param — out of this task's file scope to change), but the
/// PROBING-streak / error-driven health-tier evaluation it now performs needs a clock reading. Mirrors
/// the identical private-`unix_now`-per-file idiom already used elsewhere in this crate (`watchdog.rs`,
/// `ingress.rs`, `usage_refresh.rs`, ...).
fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Deterministic exponential backoff in SECONDS for the n-th consecutive error (n ≥ 1). No jitter
/// here — the active same-account retry jitter is a separate concern (porting item B10); this is the
/// exclusion window the selector reads, which must be stable per (error_count, last_error_at).
pub fn backoff_secs(error_count: u32) -> i64 {
    let shift = error_count.saturating_sub(1).min(BACKOFF_MAX_SHIFT);
    (BACKOFF_BASE_MS * (1i64 << shift)) / 1000
}

/// The live routing state for one account. All fields default to the neutral "healthy" values, so a
/// never-seen account (absent from the map) overlays as a no-op.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeState {
    pub error_count: u32,
    pub last_error_at: Option<i64>,
    pub cooldown_until: Option<i64>,
    pub last_selected_at: Option<i64>,
    /// The soft-drain health tier: `0` HEALTHY, `1` DRAINING, `2` PROBING. Written by
    /// [`Self::transition`]; read by `overlay` (a later task) onto the snapshot for
    /// `select.rs`'s `health_tier_pool`. Defaults to `0` (HEALTHY) — neutral, like every other field.
    pub health_tier: u8,
    /// When the account most recently entered DRAINING (any non-DRAINING → DRAINING edge). Feeds the
    /// DRAINING→PROBING quiet-timer promotion. Cleared on return to HEALTHY.
    pub drain_entered_at: Option<i64>,
    /// Consecutive successes recorded while PROBING. Reaching
    /// [`PROBE_SUCCESS_STREAK_REQUIRED`] promotes PROBING→HEALTHY; any error while PROBING resets it.
    pub probe_success_streak: u32,
    /// C9 Task 1: live count of requests currently in flight on this account, written exclusively
    /// via [`RuntimeStates::acquire_in_flight`]'s [`InFlightGuard`] (increment on acquire, decrement
    /// on the guard's `Drop`). Read by `overlay` onto the snapshot (mirrors `health_tier`) for
    /// `select.rs`'s soft in-flight penalty (a later task). While `> 0`, [`RuntimeState::is_neutral`]
    /// must return `false` — an in-flight account can never be GC'd out of the map mid-request, or
    /// the count would be lost. Defaults to `0`, which stays neutral like every other field.
    pub in_flight: u32,
    /// Bounded token-volume units held by the live requests. A large turn can contribute several
    /// units while still counting as exactly one request in `in_flight`.
    pub in_flight_pressure: u32,
    /// Long-lived upstream WebSocket connections, including idle sockets between turns.
    pub open_ws: u32,
}

impl RuntimeState {
    /// `true` when this state carries no signal — used to drop empty entries so the map doesn't grow
    /// unbounded with accounts that recovered.
    ///
    /// C9 Task 1: this derived `==` comparison already keeps an entry with `in_flight > 0` non-neutral
    /// (it differs from `RuntimeState::default()`, whose `in_flight` is `0`) — no hand-written special
    /// case needed. A defaulted `in_flight == 0` with every other field also at its default stays
    /// neutral, same as before this field was added.
    fn is_neutral(&self) -> bool {
        *self == RuntimeState::default()
    }

    /// Apply the write-back edges for a newly computed `new_tier` (from [`evaluate_health_tier`]),
    /// then store it. A byte-faithful port of codex-lb's writeback
    /// (`app/modules/proxy/load_balancer.py:2220-2249`):
    /// - entering DRAINING from any other tier (`new_tier == 1 && self.health_tier != 1`) stamps
    ///   `drain_entered_at = now` and resets `probe_success_streak = 0`.
    /// - reaching HEALTHY (`new_tier == 0`) clears `drain_entered_at` and `probe_success_streak`.
    /// - the DRAINING→PROBING edge touches neither aux field — `drain_entered_at` must survive that
    ///   transition (a PROBING account that bounces back to DRAINING needs its original quiet-timer
    ///   semantics reset via the entering-DRAINING branch above, not a stale stamp).
    ///
    /// Callers (a later task) invoke this under the `mutate` lock after computing `new_tier`.
    pub fn transition(&mut self, new_tier: u8, now: i64) {
        if new_tier == 1 && self.health_tier != 1 {
            self.drain_entered_at = Some(now);
            self.probe_success_streak = 0;
        } else if new_tier == 0 {
            self.drain_entered_at = None;
            self.probe_success_streak = 0;
        }
        self.health_tier = new_tier;
    }

    /// Apply an ERROR-ONLY health-tier re-evaluation from the per-request funnel (B8 Task 2 —
    /// `record_success` / `record_transient_error` / `record_rate_limit`). The funnel only ever
    /// knows about the error signal (`compute_should_drain` is called with `used_percent`/
    /// `secondary_percent = None`), never usage — that's the poller's (Task 3) exclusive signal.
    ///
    /// **THE CARE POINT (plan Task 2, "the funnel has no usage%"):** an account can be DRAINING
    /// purely because of USAGE (poller-set), with zero errors. If this fn is invoked right after a
    /// success clears the error state (or after an error that's alone not yet `should_drain`), the
    /// ERROR-ONLY `should_drain` is `false` even though the account is genuinely still draining by
    /// usage — a signal this fn cannot see. Naively feeding that `false` into
    /// [`evaluate_health_tier`] would let its DRAINING branch's quiet-timer fire
    /// (`now - drain_entered_at >= PROBE_QUIET_SECS`) and wrongly promote DRAINING→PROBING from a
    /// blind spot. So: compute the candidate `new_tier` as normal, but refuse to apply it when it
    /// is specifically a DRAINING(1)→PROBING(2) promotion — leave the tier DRAINING untouched in
    /// that one case (the poller, which has usage%, owns that demotion exclusively). Every other
    /// edge (HEALTHY→DRAINING, PROBING→DRAINING, PROBING→HEALTHY via streak, and any no-op) is
    /// safe to apply from the error-only view because it only ever moves the tier toward MORE
    /// drained, or completes an already-independently-tracked probe streak.
    ///
    /// B8 Task 4: returns the applied [`HealthTierTransition`] (with a funnel-appropriate reason)
    /// when the tier actually changed, so the CALLER can emit the content-free observability signal;
    /// `None` on a no-op or on the refused DRAINING→PROBING promotion. The funnel only ever produces
    /// an ENTERING-DRAINING edge (`to == 1` ⇒ [`REASON_ERROR_DRAIN`], since it can only see the error
    /// signal) or a PROBING→HEALTHY streak completion (`to == 0` ⇒ [`REASON_PROBE_PROMOTE`]); it can
    /// never produce a usage-drain or a quiet-promote (that's the poller's exclusive province).
    fn apply_funnel_transition(
        &mut self,
        should_drain: bool,
        now: i64,
    ) -> Option<HealthTierTransition> {
        let from = self.health_tier;
        let new_tier = evaluate_health_tier(
            from,
            should_drain,
            self.drain_entered_at,
            self.probe_success_streak,
            false, // frozen: the funnel only fires on completed/failed requests, i.e. not blocked.
            now,
        );
        if from == 1 && new_tier == 2 {
            // THE CARE POINT: never let an error-only view promote DRAINING -> PROBING.
            return None;
        }
        self.transition(new_tier, now);
        if from == new_tier {
            return None;
        }
        let reason = match new_tier {
            1 => REASON_ERROR_DRAIN,
            0 => REASON_PROBE_PROMOTE,
            // Unreachable from the funnel (it refuses ->PROBING and never usage-drains); defensive.
            _ => return None,
        };
        Some(HealthTierTransition {
            from,
            to: new_tier,
            reason,
        })
    }
}

/// Compute `should_drain` from the three OR'd conditions — a byte-faithful port of codex-lb's
/// `evaluate_health_tier` should-drain predicate (`app/core/balancer/logic.py:1181-1239`). Usage
/// percentages are `Option<f64>` because the caller may not have a fresh usage reading (e.g. the
/// per-request funnel, which only ever sees the error condition); `None` makes that condition `false`,
/// never true.
///
/// - `used_percent >= `[`DRAIN_PRIMARY_PCT`] (inclusive — a boundary hit already counts as drain).
/// - `secondary_percent >= `[`DRAIN_SECONDARY_PCT`] (inclusive).
/// - `error_count >= `[`DRAIN_ERROR_COUNT`] AND `last_error_at` is set AND the error is still within
///   [`DRAIN_ERROR_WINDOW_SECS`] (strict `<` — a window-aged error no longer counts).
pub fn compute_should_drain(
    used_percent: Option<f64>,
    secondary_percent: Option<f64>,
    error_count: u32,
    last_error_at: Option<i64>,
    now: i64,
) -> bool {
    let primary_drain = used_percent.is_some_and(|pct| pct >= DRAIN_PRIMARY_PCT);
    let secondary_drain = secondary_percent.is_some_and(|pct| pct >= DRAIN_SECONDARY_PCT);
    let error_flapping = error_count >= DRAIN_ERROR_COUNT
        && last_error_at.is_some_and(|at| now - at < DRAIN_ERROR_WINDOW_SECS);
    primary_drain || secondary_drain || error_flapping
}

/// Pure port of codex-lb's `evaluate_health_tier`
/// (`app/core/balancer/logic.py:1181-1239`). Returns the NEW tier only — the caller applies the
/// `drain_entered_at`/`probe_success_streak` writeback edges via [`RuntimeState::transition`]. No
/// clock reads or I/O: `now` is a param, and the aux state is passed in rather than read from a lock,
/// so this can be unit-tested and called from any evaluation site (funnel or poller) without coupling
/// to how that site stores state.
///
/// `frozen` — `true` when the account's durable status is one of codex-lb's blocked statuses
/// (rate_limited / quota_exceeded / paused / reauth_required / deactivated); the CALLER decides this
/// (this fn has no notion of account status). While frozen, no transition happens at all — the stored
/// tier passes through unchanged, matching codex-lb's "no transition while blocked" rule.
///
/// Transition table (unchanged when `frozen`):
/// - HEALTHY (`0`): `should_drain` ⇒ DRAINING; else stays HEALTHY.
/// - DRAINING (`1`): `should_drain` ⇒ stays DRAINING; else if `drain_entered_at` is set AND
///   `now - drain_entered_at >= `[`PROBE_QUIET_SECS`] ⇒ PROBING; else stays DRAINING (in particular, an
///   unset `drain_entered_at` can never promote — there is nothing to time from).
/// - PROBING (`2`): `should_drain` ⇒ DRAINING; else if `probe_success_streak >= `
///   [`PROBE_SUCCESS_STREAK_REQUIRED`] ⇒ HEALTHY; else stays PROBING.
pub fn evaluate_health_tier(
    current_tier: u8,
    should_drain: bool,
    drain_entered_at: Option<i64>,
    probe_success_streak: u32,
    frozen: bool,
    now: i64,
) -> u8 {
    if frozen {
        return current_tier;
    }
    match current_tier {
        1 => {
            // DRAINING
            if should_drain {
                1
            } else if drain_entered_at.is_some_and(|at| now - at >= PROBE_QUIET_SECS) {
                2
            } else {
                1
            }
        }
        2 => {
            // PROBING
            if should_drain {
                1
            } else if probe_success_streak >= PROBE_SUCCESS_STREAK_REQUIRED {
                0
            } else {
                2
            }
        }
        _ => {
            // HEALTHY (0, and any unrecognized value treated as healthy)
            if should_drain {
                1
            } else {
                0
            }
        }
    }
}

/// Concurrent map of per-account runtime state. Cheap reads (overlay) under a shared lock; brief
/// exclusive locks on the (rare) mutation path.
pub struct RuntimeStates {
    inner: RwLock<HashMap<AccountId, RuntimeState>>,
    logical_turn_attempts: Mutex<LogicalTurnAttemptRegistry>,
    usage_refresh_tx: RwLock<Option<tokio::sync::mpsc::UnboundedSender<AccountId>>>,
    /// At most one queued/in-progress refresh per account. `true` means another signal arrived
    /// while that refresh was outstanding and one follow-up pass is required.
    usage_refresh_pending: Mutex<HashMap<AccountId, bool>>,
    usage_refresh_coalesced: AtomicU64,
    cooldown_persist_tx: RwLock<Option<tokio::sync::mpsc::UnboundedSender<AccountId>>>,
    /// Latest durable cooldown write per account. The channel above carries at most one wakeup
    /// per key; repeated rate-limit/quota signals overwrite this value instead of growing an
    /// unbounded queue while SQLite is slow.
    cooldown_persist_pending: Mutex<HashMap<AccountId, RoutingCooldownWrite>>,
    admission_limits: AdmissionLimits,
    admission_changed: tokio::sync::Notify,
    admission_metrics: AdmissionMetrics,
    pressure_calibration: PressureCalibration,
}

/// Hard process-local admission bounds. Zero disables the corresponding bound.
///
/// The defaults match codex-lb's response-create and upstream-WebSocket bulkheads. These are
/// deliberately concurrency caps, not quota-derived weights: weekly credits do not prove how many
/// simultaneous requests an upstream account can safely sustain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdmissionLimits {
    pub global_in_flight: u32,
    pub account_in_flight: u32,
    pub global_in_flight_pressure: u32,
    pub account_in_flight_pressure: u32,
    pub global_open_ws: u32,
    pub account_open_ws: u32,
    pub owner_recovery_reserve: u32,
    pub owner_recovery_pressure_reserve: u32,
    pub wait_timeout: Duration,
}

impl Default for AdmissionLimits {
    fn default() -> Self {
        Self {
            global_in_flight: 256,
            account_in_flight: 4,
            global_in_flight_pressure: 1_024,
            account_in_flight_pressure: 16,
            global_open_ws: 128,
            account_open_ws: 8,
            owner_recovery_reserve: 1,
            owner_recovery_pressure_reserve: 4,
            wait_timeout: Duration::from_secs(10),
        }
    }
}

impl Default for RuntimeStates {
    fn default() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            logical_turn_attempts: Mutex::new(LogicalTurnAttemptRegistry::default()),
            usage_refresh_tx: RwLock::new(None),
            usage_refresh_pending: Mutex::new(HashMap::new()),
            usage_refresh_coalesced: AtomicU64::new(0),
            cooldown_persist_tx: RwLock::new(None),
            cooldown_persist_pending: Mutex::new(HashMap::new()),
            admission_limits: AdmissionLimits::default(),
            admission_changed: tokio::sync::Notify::new(),
            admission_metrics: AdmissionMetrics::default(),
            pressure_calibration: PressureCalibration::default(),
        }
    }
}

/// Fixed-cardinality rolling calibration between the cheap pre-route estimate and terminal usage.
/// The ratio is stored in thousandths and updated with a 1/8 EWMA. Samples are clamped so one
/// malformed or unusual terminal cannot make subsequent admission weights collapse or explode.
struct PressureCalibration {
    ratio_milli: AtomicU64,
    samples: AtomicU64,
    estimated_tokens: AtomicU64,
    actual_pressure_tokens: AtomicU64,
}

impl Default for PressureCalibration {
    fn default() -> Self {
        Self {
            ratio_milli: AtomicU64::new(1_000),
            samples: AtomicU64::new(0),
            estimated_tokens: AtomicU64::new(0),
            actual_pressure_tokens: AtomicU64::new(0),
        }
    }
}

#[derive(Clone, Copy)]
enum AdmissionLane {
    NewRequest,
    OwnerRequest,
    NewSocket,
    OwnerSocket,
}

impl AdmissionLane {
    fn labels(self) -> (&'static str, &'static str) {
        match self {
            Self::NewRequest => ("request", "new"),
            Self::OwnerRequest => ("request", "owner"),
            Self::NewSocket => ("websocket", "new"),
            Self::OwnerSocket => ("websocket", "owner"),
        }
    }
}

#[derive(Default)]
struct AdmissionLaneMetrics {
    waiters: AtomicU64,
    waits: AtomicU64,
    acquired_after_wait: AtomicU64,
    timeouts: AtomicU64,
    ineligible: AtomicU64,
    cancelled: AtomicU64,
    wait_milliseconds: AtomicU64,
    owner_recovery: AtomicU64,
}

impl AdmissionLaneMetrics {
    fn snapshot(&self, lane: AdmissionLane) -> AdmissionMetricSnapshot {
        let (work, scope) = lane.labels();
        AdmissionMetricSnapshot {
            work,
            scope,
            waiters: self.waiters.load(Ordering::Relaxed),
            waits: self.waits.load(Ordering::Relaxed),
            acquired_after_wait: self.acquired_after_wait.load(Ordering::Relaxed),
            timeouts: self.timeouts.load(Ordering::Relaxed),
            ineligible: self.ineligible.load(Ordering::Relaxed),
            cancelled: self.cancelled.load(Ordering::Relaxed),
            wait_milliseconds: self.wait_milliseconds.load(Ordering::Relaxed),
            owner_recovery: self.owner_recovery.load(Ordering::Relaxed),
        }
    }
}

#[derive(Default)]
struct AdmissionMetrics {
    new_request: AdmissionLaneMetrics,
    owner_request: AdmissionLaneMetrics,
    new_socket: AdmissionLaneMetrics,
    owner_socket: AdmissionLaneMetrics,
}

impl AdmissionMetrics {
    fn lane(&self, lane: AdmissionLane) -> &AdmissionLaneMetrics {
        match lane {
            AdmissionLane::NewRequest => &self.new_request,
            AdmissionLane::OwnerRequest => &self.owner_request,
            AdmissionLane::NewSocket => &self.new_socket,
            AdmissionLane::OwnerSocket => &self.owner_socket,
        }
    }

    fn start_wait(&self, lane: AdmissionLane) -> AdmissionWaitGuard<'_> {
        let metrics = self.lane(lane);
        metrics.waits.fetch_add(1, Ordering::Relaxed);
        metrics.waiters.fetch_add(1, Ordering::Relaxed);
        AdmissionWaitGuard {
            metrics,
            started_at: Instant::now(),
            finished: false,
        }
    }

    fn record_ineligible(&self, lane: AdmissionLane) {
        self.lane(lane).ineligible.fetch_add(1, Ordering::Relaxed);
    }

    fn record_owner_recovery(&self, lane: AdmissionLane) {
        self.lane(lane)
            .owner_recovery
            .fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> Vec<AdmissionMetricSnapshot> {
        [
            AdmissionLane::NewRequest,
            AdmissionLane::OwnerRequest,
            AdmissionLane::NewSocket,
            AdmissionLane::OwnerSocket,
        ]
        .into_iter()
        .map(|lane| self.lane(lane).snapshot(lane))
        .collect()
    }
}

enum AdmissionWaitOutcome {
    Acquired,
    Timeout,
    Ineligible,
}

struct AdmissionWaitGuard<'a> {
    metrics: &'a AdmissionLaneMetrics,
    started_at: Instant,
    finished: bool,
}

impl AdmissionWaitGuard<'_> {
    fn finish(mut self, outcome: AdmissionWaitOutcome) {
        match outcome {
            AdmissionWaitOutcome::Acquired => {
                self.metrics
                    .acquired_after_wait
                    .fetch_add(1, Ordering::Relaxed);
            }
            AdmissionWaitOutcome::Timeout => {
                self.metrics.timeouts.fetch_add(1, Ordering::Relaxed);
            }
            AdmissionWaitOutcome::Ineligible => {
                self.metrics.ineligible.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.finished = true;
    }
}

impl Drop for AdmissionWaitGuard<'_> {
    fn drop(&mut self) {
        self.metrics.waiters.fetch_sub(1, Ordering::Relaxed);
        self.metrics.wait_milliseconds.fetch_add(
            self.started_at.elapsed().as_millis() as u64,
            Ordering::Relaxed,
        );
        if !self.finished {
            self.metrics.cancelled.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// One fixed-cardinality, content-free admission lane exposed to Prometheus and the dashboard.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmissionMetricSnapshot {
    pub work: &'static str,
    pub scope: &'static str,
    pub waiters: u64,
    pub waits: u64,
    pub acquired_after_wait: u64,
    pub timeouts: u64,
    pub ineligible: u64,
    pub cancelled: u64,
    pub wait_milliseconds: u64,
    pub owner_recovery: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct PressureCalibrationSnapshot {
    pub ratio: f64,
    pub samples: u64,
    pub estimated_tokens: u64,
    pub actual_pressure_tokens: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoutingCooldownWrite {
    pub account_id: AccountId,
    pub cooldown_until: i64,
    pub reason: &'static str,
    pub updated_at: i64,
}

/// C9 Task 1: a leak-proof RAII lease on one account's `in_flight` count, returned by
/// [`RuntimeStates::acquire_in_flight`]. Holds the `Arc<RuntimeStates>` the entry lives in (the map
/// is process-lifetime, so a strong clone is simplest — no `Weak` upgrade-failure case to handle) plus
/// the leased [`AccountId`]. `Drop` is the ENTIRE release mechanism: it fires on every way this value
/// stops existing — falling out of scope, an early explicit `drop(guard)`, a panic unwind — which is
/// exactly the "release on every exit path" guarantee C9's crux requires (a later task embeds this as
/// a field of `ObservingStream` so a client disconnect / mid-stream error / panic all release too, with
/// zero change to the stream's poll logic).
///
/// Deliberately NOT `Clone`/`Copy`: Rust's ownership model already guarantees `drop` runs at most once
/// for a given value (there is no way to "double-drop" a live `InFlightGuard` short of `mem::forget`,
/// which by construction skips `Drop` entirely rather than double-firing it) — so a single decrement is
/// structural, not something this type needs to defend against with extra bookkeeping. `#[must_use]`
/// flags the easy-to-write bug of acquiring a lease and immediately discarding the guard as a temporary
/// (which would release it before the caller ever holds it for the request).
///
/// C9 Task 4: also holds an `Arc<LeaseMetrics>` handle so its `Drop` can bump the content-free
/// `released` counter — see [`RuntimeStates::acquire_in_flight`]'s doc for why this is threaded in
/// at acquire time (a call-site parameter) rather than stored on `RuntimeStates` itself.
#[must_use]
pub struct InFlightGuard {
    runtime: Arc<RuntimeStates>,
    id: AccountId,
    pressure_units: u32,
    metrics: Arc<LeaseMetrics>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        // saturating_sub: never underflow even if in_flight was somehow already at 0 for this id
        // (e.g. a bug elsewhere zeroed it, or the entry was never observed above 0) — a stray
        // decrement below zero would be a worse defect than a silently-clamped no-op.
        self.runtime.mutate(&self.id, |rt| {
            rt.in_flight = rt.in_flight.saturating_sub(1);
            rt.in_flight_pressure = rt.in_flight_pressure.saturating_sub(self.pressure_units);
        });
        self.runtime.admission_changed.notify_waiters();
        // C9 Task 4: the release counter. This fires on EVERY way this guard stops existing —
        // clean drain, client disconnect, mid-stream error, idle-timeout, or a failover reselect's
        // dropped pre-stream attempt — the exact same leak-proof coverage the `in_flight` decrement
        // above already has, since both live in this one `Drop` impl.
        self.metrics.record_release();
    }
}

#[must_use]
pub struct WsSocketGuard {
    runtime: Arc<RuntimeStates>,
    id: AccountId,
}

impl Drop for WsSocketGuard {
    fn drop(&mut self) {
        self.runtime.mutate(&self.id, |rt| {
            rt.open_ws = rt.open_ws.saturating_sub(1);
        });
        self.runtime.admission_changed.notify_waiters();
    }
}

impl RuntimeStates {
    /// Atomically spend one generation attempt from the process-wide budget for a hashed logical
    /// turn. Missing turn identity deliberately bypasses this aggregate layer and retains the
    /// caller's existing per-request retry bound.
    ///
    /// `limit` is the same live `max_account_attempts` policy used by the HTTP failover loop.
    /// Zero is treated as one defensively, matching the configuration clamp.
    pub fn try_consume_logical_turn_attempt(
        &self,
        logical_turn_key: Option<&str>,
        limit: u32,
        now: i64,
    ) -> bool {
        let Some(key) = logical_turn_key else {
            return true;
        };
        let limit = limit.max(1);
        let mut registry = self
            .logical_turn_attempts
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if now >= registry.next_cleanup_at
            || registry.entries.len() >= LOGICAL_TURN_ATTEMPT_MAX_KEYS
        {
            registry.entries.retain(|_, entry| entry.expires_at > now);
            registry.next_cleanup_at =
                now.saturating_add(LOGICAL_TURN_ATTEMPT_CLEANUP_INTERVAL_SECS);
        }

        if let Some(entry) = registry.entries.get_mut(key) {
            if entry.consumed >= limit {
                return false;
            }
            entry.consumed = entry.consumed.saturating_add(1);
            entry.expires_at = now.saturating_add(LOGICAL_TURN_ATTEMPT_TTL_SECS);
            return true;
        }

        // Preserve budgets already protecting active turns. Evicting one here would let a
        // high-cardinality client erase another turn's spent attempts and resume amplification.
        if registry.entries.len() >= LOGICAL_TURN_ATTEMPT_MAX_KEYS {
            return false;
        }
        registry.entries.insert(
            key.to_owned(),
            LogicalTurnAttempts {
                consumed: 1,
                expires_at: now.saturating_add(LOGICAL_TURN_ATTEMPT_TTL_SECS),
            },
        );
        true
    }

    /// Refund an attempt that an upstream rejected at authentication before model sampling could
    /// begin. This is intentionally narrow: transport failures, 429s, 5xx responses, replay, and
    /// account failover all remain charged.
    pub fn refund_logical_turn_attempt(&self, logical_turn_key: Option<&str>) {
        let Some(key) = logical_turn_key else {
            return;
        };
        let mut registry = self
            .logical_turn_attempts
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let remove = registry.entries.get_mut(key).is_some_and(|entry| {
            entry.consumed = entry.consumed.saturating_sub(1);
            entry.consumed == 0
        });
        if remove {
            registry.entries.remove(key);
        }
    }

    /// Release a logical turn's spent attempts after a forwarded terminal `response.completed`.
    ///
    /// Codex's `turn_id` spans EVERY generation of a user turn (one per tool-call round — the
    /// submission id in `codex-rs` `session/turn_context.rs`), not just retries of a failing one.
    /// A completed generation is real progress: the next `response.create` under the same turn id
    /// is new work, so it must start from a fresh budget. Without this, any turn with more than
    /// `max_account_attempts` tool rounds inside the TTL is rejected mid-loop despite zero
    /// failures. Failure paths never reach here, so a turn that keeps erroring still accumulates
    /// spend across client retries — the amplification bound this budget exists for.
    pub fn clear_logical_turn_attempts(&self, logical_turn_key: Option<&str>) {
        let Some(key) = logical_turn_key else {
            return;
        };
        self.logical_turn_attempts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entries
            .remove(key);
    }

    /// Register the background usage poller's immediate-refresh channel. Replacing a prior sender
    /// is intentional during test/server reconstruction; request paths always use the latest live
    /// worker.
    pub fn register_usage_refresh(&self, tx: tokio::sync::mpsc::UnboundedSender<AccountId>) {
        *self
            .usage_refresh_tx
            .write()
            .unwrap_or_else(|e| e.into_inner()) = Some(tx);
    }

    pub fn register_cooldown_persistence(&self, tx: tokio::sync::mpsc::UnboundedSender<AccountId>) {
        *self
            .cooldown_persist_tx
            .write()
            .unwrap_or_else(|e| e.into_inner()) = Some(tx);
    }

    fn persist_cooldown(&self, write: RoutingCooldownWrite) {
        let account_id = write.account_id.clone();
        let should_wake = self
            .cooldown_persist_pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(account_id.clone(), write)
            .is_none();
        if !should_wake {
            return;
        }
        if let Some(tx) = self
            .cooldown_persist_tx
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            if tx.send(account_id.clone()).is_ok() {
                return;
            }
            self.cooldown_persist_pending
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&account_id);
            tracing::warn!("routing cooldown persistence worker is unavailable");
        } else {
            self.cooldown_persist_pending
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&account_id);
        }
    }

    pub(crate) fn pending_cooldown(&self, account_id: &AccountId) -> Option<RoutingCooldownWrite> {
        self.cooldown_persist_pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(account_id)
            .cloned()
    }

    pub(crate) fn pending_cooldowns(&self) -> Vec<RoutingCooldownWrite> {
        self.cooldown_persist_pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect()
    }

    /// Complete one durable cooldown write. The exact snapshot is removed only when no newer
    /// coalesced value replaced it while SQLite was busy. Returns `true` when another pass is
    /// required for that account.
    pub(crate) fn finish_cooldown_persist(&self, persisted: &RoutingCooldownWrite) -> bool {
        let mut pending = self
            .cooldown_persist_pending
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if pending
            .get(&persisted.account_id)
            .is_some_and(|current| current == persisted)
        {
            pending.remove(&persisted.account_id);
        }
        pending.contains_key(&persisted.account_id)
    }

    /// Ask the background poller to refresh one account immediately after a protocol-level
    /// capacity failure. This is best-effort and non-blocking; the regular ten-minute sweep
    /// remains the fallback when no worker is registered or the process is shutting down.
    pub fn request_usage_refresh(&self, id: &AccountId) {
        {
            let mut pending = self
                .usage_refresh_pending
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(dirty) = pending.get_mut(id) {
                *dirty = true;
                self.usage_refresh_coalesced.fetch_add(1, Ordering::Relaxed);
                return;
            }
            pending.insert(id.clone(), false);
        }
        let tx = self
            .usage_refresh_tx
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(tx) = tx {
            if tx.send(id.clone()).is_ok() {
                return;
            }
        }
        self.usage_refresh_pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(id);
    }

    /// Complete one immediate usage refresh. Returns `true` when signals arrived during the
    /// outstanding pass and exactly one follow-up refresh should run before releasing the key.
    pub fn finish_usage_refresh(&self, id: &AccountId) -> bool {
        let mut pending = self
            .usage_refresh_pending
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        match pending.get_mut(id) {
            Some(dirty) if *dirty => {
                *dirty = false;
                true
            }
            Some(_) => {
                pending.remove(id);
                false
            }
            None => false,
        }
    }

    pub fn usage_refresh_coalesced(&self) -> u64 {
        self.usage_refresh_coalesced.load(Ordering::Relaxed)
    }

    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_admission_limits(admission_limits: AdmissionLimits) -> Self {
        Self {
            admission_limits,
            ..Self::default()
        }
    }

    /// Convert the content-free token estimate into bounded admission units using the rolling
    /// terminal-usage calibration. A request always costs at least one unit. The maximum is the
    /// ordinary per-account pressure budget when configured, so a single large new request can
    /// still make progress without consuming the owner-only recovery reserve.
    pub fn request_pressure_units(&self, estimated_tokens: u32) -> u32 {
        let ratio = self
            .pressure_calibration
            .ratio_milli
            .load(Ordering::Relaxed);
        let calibrated = u64::from(estimated_tokens.max(1))
            .saturating_mul(ratio)
            .div_ceil(1_000);
        let units = calibrated.div_ceil(PRESSURE_TOKENS_PER_UNIT).max(1);
        let ordinary_limit = self
            .admission_limits
            .account_in_flight_pressure
            .saturating_sub(self.admission_limits.owner_recovery_pressure_reserve);
        let max_units = if self.admission_limits.account_in_flight_pressure == 0 {
            DEFAULT_MAX_REQUEST_PRESSURE_UNITS
        } else {
            ordinary_limit.max(1)
        };
        u32::try_from(units).unwrap_or(u32::MAX).min(max_units)
    }

    /// Reconcile a completed request's cheap estimate with authoritative terminal usage. This does
    /// not retroactively move a completed lease; it calibrates subsequent requests before routing.
    pub fn record_actual_pressure(&self, estimated_tokens: u32, actual_pressure_tokens: u64) {
        if estimated_tokens == 0 || actual_pressure_tokens == 0 {
            return;
        }
        let estimated = u64::from(estimated_tokens);
        let sample_ratio = actual_pressure_tokens
            .saturating_mul(1_000)
            .checked_div(estimated)
            .unwrap_or(1_000)
            .clamp(250, 4_000);
        self.pressure_calibration
            .ratio_milli
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |previous| {
                Some(
                    previous
                        .saturating_mul(7)
                        .saturating_add(sample_ratio)
                        .div_ceil(8),
                )
            })
            .expect("pressure calibration update always returns Some");
        self.pressure_calibration
            .samples
            .fetch_add(1, Ordering::Relaxed);
        self.pressure_calibration
            .estimated_tokens
            .fetch_add(estimated, Ordering::Relaxed);
        self.pressure_calibration
            .actual_pressure_tokens
            .fetch_add(actual_pressure_tokens, Ordering::Relaxed);
    }

    /// Patch the live runtime fields onto each snapshot from the map. Snapshots for accounts with no
    /// entry are left at their neutral defaults. Called by ingress on the already-filtered (cloned)
    /// slice right before selection — never touches the account cache.
    pub fn overlay(&self, snapshots: &mut [AccountSnapshot], now: i64) {
        // Recover from a poisoned lock (a prior writer panic) rather than cascading the panic into a
        // pool-wide routing DoS: the mutations are trivial field writes, so the data is always a
        // valid `RuntimeState` even if a writer unwound mid-update.
        let map = self.inner.read().unwrap_or_else(|e| e.into_inner());
        for snap in snapshots.iter_mut() {
            if let Some(rt) = map.get(&snap.id) {
                snap.error_count = rt.error_count;
                snap.last_error_at = rt.last_error_at;
                // Supply the cooldown ONLY while it is still active. An ELAPSED cooldown must NOT be
                // handed to the selector: its `now >= cd ⇒ clear error state` branch (`select.rs`)
                // would otherwise re-fire on EVERY read (the stored timestamp never changes),
                // permanently zeroing `eff_error_count` and silently disabling transient-error
                // benching + drain demotion for any account that has ever been 429'd. Dropping the
                // expired cooldown here lets `error_count` accumulate normally again; the 429's own
                // error contribution is cleared for real by `record_success` on the next completed
                // turn (or the entry is GC'd once neutral).
                let runtime_cooldown = rt.cooldown_until.filter(|&cd| now < cd);
                snap.cooldown_until = match (snap.cooldown_until, runtime_cooldown) {
                    (Some(durable), Some(runtime)) => Some(durable.max(runtime)),
                    (durable, runtime) => durable.or(runtime),
                };
                snap.last_selected_at = rt.last_selected_at;
                // B8 Task 2: expose the live soft-drain tier so select.rs's already-built
                // health_tier_pool sees real values instead of the always-0 default. An absent
                // entry leaves the snapshot at its neutral `health_tier: 0` default (the loop
                // above already skips it entirely).
                snap.health_tier = rt.health_tier;
                // C9 Task 1: expose the live in-flight lease count. An absent entry leaves the
                // snapshot at its neutral `in_flight: 0` default (the loop above already skips it).
                snap.in_flight = rt.in_flight;
                snap.in_flight_pressure = rt.in_flight_pressure;
                snap.open_ws = rt.open_ws;
            }
        }
    }

    /// Apply `f` to `id`'s entry (creating it from default), then drop it if it decayed back to
    /// neutral so the map stays bounded. Returns whatever `f` produced (B8 Task 4: the funnel/poller
    /// methods return the applied [`HealthTierTransition`] through this seam so the caller can emit
    /// the observability signal — the return is captured BEFORE the neutral-GC check).
    fn mutate<R>(&self, id: &AccountId, f: impl FnOnce(&mut RuntimeState) -> R) -> R {
        let mut map = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let entry = map.entry(id.clone()).or_default();
        let out = f(entry);
        if entry.is_neutral() {
            map.remove(id);
        }
        out
    }

    /// Record a rate-limit (429) hit: bump the error count, stamp the error time, and bench the
    /// account until `now + delay` (floored at [`RATE_LIMITED_MIN_COOLDOWN_SECS`]). `retry_after` is
    /// the upstream `Retry-After`, if any; absent ⇒ exponential backoff on the new error count. The
    /// LATER of an existing cooldown and the new one wins (never shorten a bench).
    ///
    /// C11b Task 2: `rate_limit_metrics` is the content-free [`RateLimitMetrics`] handle (an
    /// `AppState` field) this call bumps once, keyed by `"upstream"` when `retry_after` was
    /// upstream-supplied or `"backoff"` when PolyFlare computed its own exponential delay — the
    /// single true 429 chokepoint (all `record_failure` callers funnel through the one
    /// `sig.status == 429` branch that calls this). Threaded in as a call-site PARAMETER, exactly
    /// mirroring [`Self::acquire_in_flight`]'s `metrics: &Arc<LeaseMetrics>` precedent (see that
    /// method's doc for why: `RuntimeStates`'s own construction stays metrics-free, so none of the
    /// existing `AppState`-builder call sites needed to change — only this method's callers gained
    /// one argument).
    pub fn record_rate_limit(
        &self,
        id: &AccountId,
        retry_after: Option<i64>,
        now: i64,
        rate_limit_metrics: &RateLimitMetrics,
    ) -> Option<HealthTierTransition> {
        rate_limit_metrics.record(if retry_after.is_some() {
            "upstream"
        } else {
            "backoff"
        });
        self.apply_rate_limit(id, retry_after, now)
    }

    /// Record a rate-limit discovered inside a streamed terminal frame. Unlike an HTTP 429, that
    /// frame has already reached the client and is classified after stream creation, where the
    /// request-level metrics handle is unavailable. Routing state is still updated exactly once.
    pub fn record_stream_rate_limit(
        &self,
        id: &AccountId,
        retry_after: Option<i64>,
        now: i64,
    ) -> Option<HealthTierTransition> {
        self.apply_rate_limit(id, retry_after, now)
    }

    fn apply_rate_limit(
        &self,
        id: &AccountId,
        retry_after: Option<i64>,
        now: i64,
    ) -> Option<HealthTierTransition> {
        let (transition, cooldown_until) = self.mutate(id, |rt| {
            rt.error_count = rt.error_count.saturating_add(1);
            rt.last_error_at = Some(now);
            // Clamp the (upstream-controlled) delay into [floor, ceiling] — the floor guarantees a
            // real bench, the ceiling defuses a hostile `Retry-After`; `saturating_add` then can't
            // overflow `i64`.
            let delay = retry_after
                .unwrap_or_else(|| backoff_secs(rt.error_count))
                .clamp(RATE_LIMITED_MIN_COOLDOWN_SECS, MAX_COOLDOWN_SECS);
            let until = now.saturating_add(delay);
            rt.cooldown_until = Some(rt.cooldown_until.map_or(until, |c| c.max(until)));
            // B8 Task 2: error-driven health-tier re-evaluation (see `apply_funnel_transition`'s
            // doc for the care point about never promoting DRAINING->PROBING from here). A PROBING
            // account resets its streak on any error, same as `record_transient_error`.
            if rt.health_tier == 2 {
                rt.probe_success_streak = 0;
            }
            let should_drain =
                compute_should_drain(None, None, rt.error_count, rt.last_error_at, now);
            (
                rt.apply_funnel_transition(should_drain, now),
                rt.cooldown_until.expect("rate limit sets cooldown"),
            )
        });
        self.persist_cooldown(RoutingCooldownWrite {
            account_id: id.clone(),
            cooldown_until,
            reason: "rate_limit",
            updated_at: now,
        });
        transition
    }

    /// Record a quota-exceeded hit: bench for [`QUOTA_EXCEEDED_COOLDOWN_SECS`] WITHOUT bumping
    /// `error_count` (quota is a capacity signal, not a health error — bumping it would double-
    /// penalize a merely-full account into the drain tier). The later cooldown wins.
    ///
    /// Used by both pre-stream HTTP errors and streamed `response.failed` terminal events. The
    /// immediate cooldown prevents another new session selecting known-full capacity while the
    /// asynchronous usage refresh obtains the authoritative reset window and durable gate.
    pub fn record_quota_exceeded(&self, id: &AccountId, now: i64) {
        let cooldown_until = self.mutate(id, |rt| {
            let until = now.saturating_add(QUOTA_EXCEEDED_COOLDOWN_SECS);
            rt.cooldown_until = Some(rt.cooldown_until.map_or(until, |c| c.max(until)));
            rt.cooldown_until.expect("quota cooldown is set")
        });
        self.persist_cooldown(RoutingCooldownWrite {
            account_id: id.clone(),
            cooldown_until,
            reason: "quota",
            updated_at: now,
        });
    }

    /// Record a transient error (5xx / connection): bump the error count + stamp the time. No
    /// cooldown — the selector's error-backoff gate (`error_count ≥ 3`) handles exclusion.
    pub fn record_transient_error(&self, id: &AccountId, now: i64) -> Option<HealthTierTransition> {
        self.mutate(id, |rt| {
            rt.error_count = rt.error_count.saturating_add(1);
            rt.last_error_at = Some(now);
            // B8 Task 2: error-driven health-tier re-evaluation. See `apply_funnel_transition`'s
            // doc for the care point (never promote DRAINING->PROBING from an error-only view).
            if rt.health_tier == 2 {
                rt.probe_success_streak = 0;
            }
            let should_drain =
                compute_should_drain(None, None, rt.error_count, rt.last_error_at, now);
            rt.apply_funnel_transition(should_drain, now)
        })
    }

    /// Record a successful completion: clear the error state (error_count → 0, last_error_at → None)
    /// — MANDATORY, or errors accumulate forever and every account eventually looks unhealthy.
    /// Leaves `cooldown_until` alone (it expires on its own) and does NOT touch `last_selected_at`:
    /// that marker is owned by [`record_selected`] (stamped at SELECTION), so an error-only entry
    /// that succeeds decays back to neutral here and is GC'd — keeping the map bounded.
    ///
    /// B8 Task 2: also advances the PROBING streak. If the account is currently PROBING (`health_tier
    /// == 2`), this success bumps `probe_success_streak`; reaching
    /// [`PROBE_SUCCESS_STREAK_REQUIRED`] then promotes PROBING→HEALTHY (clearing the aux fields) via
    /// [`RuntimeState::apply_funnel_transition`], evaluated with the ERROR-ONLY `should_drain`
    /// (`used_percent`/`secondary_percent = None`) computed AFTER the error state above was cleared
    /// — so a success can never itself look like an error-drain. `apply_funnel_transition`'s care
    /// point still applies here too: it refuses to let this call promote an unrelated DRAINING
    /// account to PROBING (see its doc) — only the PROBING->HEALTHY streak edge, or a no-op, can
    /// result from a success.
    pub fn record_success(&self, id: &AccountId) -> Option<HealthTierTransition> {
        self.mutate(id, |rt| {
            rt.error_count = 0;
            rt.last_error_at = None;
            if rt.health_tier == 2 {
                rt.probe_success_streak = rt.probe_success_streak.saturating_add(1);
            }
            let now = unix_now();
            let should_drain =
                compute_should_drain(None, None, rt.error_count, rt.last_error_at, now);
            rt.apply_funnel_transition(should_drain, now)
        })
    }

    /// B8 Task 3: the FULL (usage + error) health-tier evaluation, run by the usage-refresh
    /// poller (`crate::usage_refresh::refresh_account`) on every `used_percent`/
    /// `secondary_used_percent` refresh cycle (≤600s). This is the ONLY evaluation site that owns
    /// the DRAINING→PROBING quiet-timer demotion (`evaluate_health_tier`'s DRAINING branch): the
    /// funnel (`apply_funnel_transition`, Task 2) deliberately refuses that edge because it never
    /// sees usage%, so a purely usage-drained account could otherwise get stuck in DRAINING
    /// forever once its errors clear. Here, `should_drain` is computed from the REAL usage
    /// reading, so `!should_drain` genuinely means "healthy by every known signal" and the
    /// quiet-timer promotion is safe to apply.
    ///
    /// - `enabled == false` (the `POLYFLARE_SOFT_DRAIN_ENABLED` disable lever, resolved once at
    ///   startup into `ServeConfig`/`AppState` — never read per-request) forces the tier to
    ///   HEALTHY and clears the aux fields via [`RuntimeState::transition`]`(0, now)` — codex-lb's
    ///   disable path (`load_balancer.py:2245-2249`) — WITHOUT touching `error_count`/
    ///   `last_error_at`: the funnel owns those, and clobbering them here would let a poller cycle
    ///   silently erase evidence the error-backoff gate (`select.rs`) still needs. This branch
    ///   runs BEFORE the `frozen` check — the disable lever is unconditional, matching codex-lb.
    /// - Otherwise: compute the FULL `should_drain` (usage OR the entry's existing error state —
    ///   `compute_should_drain` reads `rt.error_count`/`rt.last_error_at` as-is, never mutating
    ///   them), run [`evaluate_health_tier`] with `frozen = status_frozen` (the caller's blocked-
    ///   status set: rate_limited/quota_exceeded/paused/reauth_required/deactivated — while
    ///   frozen, no transition happens at all, matching the pure fn's contract), then apply the
    ///   writeback via [`RuntimeState::transition`].
    ///
    /// Runs under the `mutate` write lock, so it composes safely with a concurrent funnel call on
    /// the same account. `mutate`'s GC still applies afterward: a HEALTHY, zero-error entry is
    /// dropped, keeping the map bounded.
    pub fn evaluate_with_usage(
        &self,
        id: &AccountId,
        used_percent: Option<f64>,
        secondary_percent: Option<f64>,
        status_frozen: bool,
        enabled: bool,
        now: i64,
    ) -> Option<HealthTierTransition> {
        self.mutate(id, |rt| {
            if !enabled {
                // Disable lever: force HEALTHY + clear aux, unconditionally. Deliberately leaves
                // error_count/last_error_at alone — the funnel owns those.
                let from = rt.health_tier;
                rt.transition(0, now);
                return (from != 0).then_some(HealthTierTransition {
                    from,
                    to: 0,
                    reason: REASON_DISABLED_RESET,
                });
            }
            let should_drain = compute_should_drain(
                used_percent,
                secondary_percent,
                rt.error_count,
                rt.last_error_at,
                now,
            );
            let from = rt.health_tier;
            let new_tier = evaluate_health_tier(
                from,
                should_drain,
                rt.drain_entered_at,
                rt.probe_success_streak,
                status_frozen,
                now,
            );
            rt.transition(new_tier, now);
            if from == new_tier {
                return None;
            }
            // B8 Task 4 reason mapping at the poller edge. An ENTERING-DRAINING edge is labelled by
            // WHICH signal caused it: recompute the usage-only `should_drain` (error_count=0) — if
            // usage alone would drain, it's a `usage_drain`, else the lingering error state is the
            // cause (`error_drain`). The two promotions are unambiguous.
            let reason = match new_tier {
                1 => {
                    let usage_only =
                        compute_should_drain(used_percent, secondary_percent, 0, None, now);
                    if usage_only {
                        REASON_USAGE_DRAIN
                    } else {
                        REASON_ERROR_DRAIN
                    }
                }
                2 => REASON_QUIET_PROMOTE,
                0 => REASON_PROBE_PROMOTE,
                _ => return None,
            };
            Some(HealthTierTransition {
                from,
                to: new_tier,
                reason,
            })
        })
    }

    /// Stamp the last-selected time (the `round_robin` tiebreak + a liveness marker). Cheap; called
    /// on every selection.
    pub fn record_selected(&self, id: &AccountId, now: i64) {
        self.mutate(id, |rt| rt.last_selected_at = Some(now));
    }

    /// C9 Task 1: acquire a leak-proof in-flight lease on `id` — increments `RuntimeState.in_flight`
    /// now and returns an [`InFlightGuard`] whose `Drop` decrements it exactly once, whenever/however
    /// this guard's owner (a later task: the request's `ObservingStream`) goes away. Takes `self` as
    /// `&Arc<Self>` (not `&self`) because the returned guard must own a live handle back into this map
    /// to release later, and `RuntimeStates` lives for the process — `Arc::clone` is the simplest
    /// correct handle (see [`InFlightGuard`]'s doc for why `Weak` isn't needed here).
    ///
    /// `now` is accepted for symmetry with every other `mutate`-driving method on this type (and
    /// reserved for a possible future TTL-stamp / stale-reclaim sweep — see the plan's Task 4
    /// follow-ups) but is not otherwise read by v1's pure increment.
    ///
    /// C9 Task 4: `metrics` is the content-free [`LeaseMetrics`] handle (an `AppState` field) that
    /// this call bumps `acquired` on, and that the returned [`InFlightGuard`] carries forward so its
    /// `Drop` can later bump `released` — the ONLY way to get a bump on every release path (a plain
    /// `Drop` has no caller to report back to). Threaded in as a call-site PARAMETER (from
    /// `crate::ingress`'s `state.lease_metrics`) rather than stored as a field on `RuntimeStates`
    /// itself: `RuntimeStates`'s own construction (`::new`/`Default`) stays metrics-free and
    /// unchanged, so none of the ~40 existing `AppState`-builder call sites that already write
    /// `runtime: Default::default()` needed to change for this task — only the ~20 call sites that
    /// actually invoke `acquire_in_flight` gained one argument. This is a deliberate, documented
    /// exception to this module's usual "no dependency on `observability`" discipline (see this
    /// file's `REASON_USAGE_DRAIN` doc for that precedent): `LeaseMetrics` is a pure content-free
    /// counter with no risk of the coupling that precedent was guarding against, and returning a
    /// value "upward" (the pattern `HealthTierTransition` uses) cannot work here — nobody is
    /// necessarily present to act on a return value at the moment an `InFlightGuard` silently drops.
    pub fn acquire_in_flight(
        self: &Arc<Self>,
        id: &AccountId,
        now: i64,
        metrics: &Arc<LeaseMetrics>,
    ) -> InFlightGuard {
        self.acquire_in_flight_weighted(id, now, metrics, 1)
    }

    pub fn acquire_in_flight_weighted(
        self: &Arc<Self>,
        id: &AccountId,
        now: i64,
        metrics: &Arc<LeaseMetrics>,
        pressure_units: u32,
    ) -> InFlightGuard {
        let _ = now;
        let pressure_units = pressure_units.max(1);
        self.mutate(id, |rt| {
            rt.in_flight = rt.in_flight.saturating_add(1);
            rt.in_flight_pressure = rt.in_flight_pressure.saturating_add(pressure_units);
        });
        metrics.record_acquire();
        InFlightGuard {
            runtime: Arc::clone(self),
            id: id.clone(),
            pressure_units,
            metrics: Arc::clone(metrics),
        }
    }

    pub fn acquire_open_ws(self: &Arc<Self>, id: &AccountId) -> WsSocketGuard {
        self.mutate(id, |rt| {
            rt.open_ws = rt.open_ws.saturating_add(1);
        });
        WsSocketGuard {
            runtime: Arc::clone(self),
            id: id.clone(),
        }
    }

    /// Reserve ordinary (non-owner-recovery) work on a specific account if both hard limits allow
    /// it. The recovery reserve is intentionally withheld here; only
    /// [`Self::acquire_pinned_in_flight`] may consume that final per-account slot.
    pub fn try_acquire_in_flight(
        self: &Arc<Self>,
        id: &AccountId,
        now: i64,
        metrics: &Arc<LeaseMetrics>,
    ) -> Option<InFlightGuard> {
        self.try_acquire_in_flight_weighted(id, now, metrics, 1)
    }

    pub fn try_acquire_in_flight_weighted(
        self: &Arc<Self>,
        id: &AccountId,
        now: i64,
        metrics: &Arc<LeaseMetrics>,
        pressure_units: u32,
    ) -> Option<InFlightGuard> {
        let pressure_units = pressure_units.max(1);
        let acquired = {
            let mut map = self.inner.write().unwrap_or_else(|e| e.into_inner());
            let global = map
                .values()
                .fold(0u32, |total, state| total.saturating_add(state.in_flight));
            let global_pressure = map.values().fold(0u32, |total, state| {
                total.saturating_add(state.in_flight_pressure)
            });
            let account = map.get(id).map_or(0, |state| state.in_flight);
            let account_pressure = map.get(id).map_or(0, |state| state.in_flight_pressure);
            let ordinary_limit = self
                .admission_limits
                .account_in_flight
                .saturating_sub(self.admission_limits.owner_recovery_reserve);
            let ordinary_pressure_limit = self
                .admission_limits
                .account_in_flight_pressure
                .saturating_sub(self.admission_limits.owner_recovery_pressure_reserve);
            let global_available = self.admission_limits.global_in_flight == 0
                || global < self.admission_limits.global_in_flight;
            let account_available =
                self.admission_limits.account_in_flight == 0 || account < ordinary_limit;
            let global_pressure_available = self.admission_limits.global_in_flight_pressure == 0
                || global_pressure.saturating_add(pressure_units)
                    <= self.admission_limits.global_in_flight_pressure;
            let account_pressure_available = self.admission_limits.account_in_flight_pressure == 0
                || account_pressure.saturating_add(pressure_units) <= ordinary_pressure_limit;
            if global_available
                && account_available
                && global_pressure_available
                && account_pressure_available
            {
                let runtime = map.entry(id.clone()).or_default();
                runtime.last_selected_at = Some(now);
                runtime.in_flight = runtime.in_flight.saturating_add(1);
                runtime.in_flight_pressure =
                    runtime.in_flight_pressure.saturating_add(pressure_units);
                true
            } else {
                false
            }
        };
        if !acquired {
            return None;
        }
        metrics.record_acquire();
        Some(InFlightGuard {
            runtime: Arc::clone(self),
            id: id.clone(),
            pressure_units,
            metrics: Arc::clone(metrics),
        })
    }

    /// Wait for capacity on an exact continuation owner. This never considers another account:
    /// response IDs and turn-state tokens are account-affine, so cross-account spill would trade a
    /// short capacity delay for a guaranteed continuity hazard.
    pub async fn acquire_pinned_in_flight(
        self: &Arc<Self>,
        id: &AccountId,
        now: i64,
        metrics: &Arc<LeaseMetrics>,
    ) -> Option<InFlightGuard> {
        self.acquire_pinned_in_flight_weighted(id, now, metrics, 1)
            .await
    }

    pub async fn acquire_pinned_in_flight_weighted(
        self: &Arc<Self>,
        id: &AccountId,
        now: i64,
        metrics: &Arc<LeaseMetrics>,
        pressure_units: u32,
    ) -> Option<InFlightGuard> {
        let pressure_units = pressure_units.max(1);
        let deadline = tokio::time::Instant::now() + self.admission_limits.wait_timeout;
        let mut wait: Option<AdmissionWaitGuard<'_>> = None;
        loop {
            // Register before checking under the lock so a release between the check and await
            // leaves a stored notification instead of stranding this waiter until timeout.
            let changed = self.admission_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let acquired = {
                let mut map = self.inner.write().unwrap_or_else(|e| e.into_inner());
                let global = map
                    .values()
                    .fold(0u32, |total, state| total.saturating_add(state.in_flight));
                let global_pressure = map.values().fold(0u32, |total, state| {
                    total.saturating_add(state.in_flight_pressure)
                });
                let account = map.get(id).map_or(0, |state| state.in_flight);
                let account_pressure = map.get(id).map_or(0, |state| state.in_flight_pressure);
                let global_available = self.admission_limits.global_in_flight == 0
                    || global < self.admission_limits.global_in_flight;
                let account_available = self.admission_limits.account_in_flight == 0
                    || account < self.admission_limits.account_in_flight;
                let global_pressure_available = self.admission_limits.global_in_flight_pressure
                    == 0
                    || global_pressure.saturating_add(pressure_units)
                        <= self.admission_limits.global_in_flight_pressure;
                let account_pressure_available = self.admission_limits.account_in_flight_pressure
                    == 0
                    || account_pressure.saturating_add(pressure_units)
                        <= self.admission_limits.account_in_flight_pressure;
                if global_available
                    && account_available
                    && global_pressure_available
                    && account_pressure_available
                {
                    let runtime = map.entry(id.clone()).or_default();
                    runtime.last_selected_at = Some(now);
                    runtime.in_flight = runtime.in_flight.saturating_add(1);
                    runtime.in_flight_pressure =
                        runtime.in_flight_pressure.saturating_add(pressure_units);
                    let ordinary_limit = self
                        .admission_limits
                        .account_in_flight
                        .saturating_sub(self.admission_limits.owner_recovery_reserve);
                    let ordinary_pressure_limit = self
                        .admission_limits
                        .account_in_flight_pressure
                        .saturating_sub(self.admission_limits.owner_recovery_pressure_reserve);
                    Some(
                        (self.admission_limits.account_in_flight > 0 && account >= ordinary_limit)
                            || (self.admission_limits.account_in_flight_pressure > 0
                                && account_pressure.saturating_add(pressure_units)
                                    > ordinary_pressure_limit),
                    )
                } else {
                    None
                }
            };
            if let Some(used_recovery_slot) = acquired {
                metrics.record_acquire();
                if used_recovery_slot {
                    self.admission_metrics
                        .record_owner_recovery(AdmissionLane::OwnerRequest);
                }
                if let Some(wait) = wait.take() {
                    wait.finish(AdmissionWaitOutcome::Acquired);
                }
                return Some(InFlightGuard {
                    runtime: Arc::clone(self),
                    id: id.clone(),
                    pressure_units,
                    metrics: Arc::clone(metrics),
                });
            }
            if wait.is_none() {
                wait = Some(
                    self.admission_metrics
                        .start_wait(AdmissionLane::OwnerRequest),
                );
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                wait.take()
                    .expect("owner request wait starts before timeout")
                    .finish(AdmissionWaitOutcome::Timeout);
                return None;
            }
        }
    }

    pub async fn acquire_pinned_open_ws(
        self: &Arc<Self>,
        id: &AccountId,
        now: i64,
    ) -> Option<WsSocketGuard> {
        let deadline = tokio::time::Instant::now() + self.admission_limits.wait_timeout;
        let mut wait: Option<AdmissionWaitGuard<'_>> = None;
        loop {
            let changed = self.admission_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let acquired = {
                let mut map = self.inner.write().unwrap_or_else(|e| e.into_inner());
                let global = map
                    .values()
                    .fold(0u32, |total, state| total.saturating_add(state.open_ws));
                let account = map.get(id).map_or(0, |state| state.open_ws);
                let global_available = self.admission_limits.global_open_ws == 0
                    || global < self.admission_limits.global_open_ws;
                let account_available = self.admission_limits.account_open_ws == 0
                    || account < self.admission_limits.account_open_ws;
                if global_available && account_available {
                    let runtime = map.entry(id.clone()).or_default();
                    runtime.last_selected_at = Some(now);
                    runtime.open_ws = runtime.open_ws.saturating_add(1);
                    let ordinary_limit = self
                        .admission_limits
                        .account_open_ws
                        .saturating_sub(self.admission_limits.owner_recovery_reserve);
                    Some(self.admission_limits.account_open_ws > 0 && account >= ordinary_limit)
                } else {
                    None
                }
            };
            if let Some(used_recovery_slot) = acquired {
                if used_recovery_slot {
                    self.admission_metrics
                        .record_owner_recovery(AdmissionLane::OwnerSocket);
                }
                if let Some(wait) = wait.take() {
                    wait.finish(AdmissionWaitOutcome::Acquired);
                }
                return Some(WsSocketGuard {
                    runtime: Arc::clone(self),
                    id: id.clone(),
                });
            }
            if wait.is_none() {
                wait = Some(
                    self.admission_metrics
                        .start_wait(AdmissionLane::OwnerSocket),
                );
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                wait.take()
                    .expect("owner socket wait starts before timeout")
                    .finish(AdmissionWaitOutcome::Timeout);
                return None;
            }
        }
    }

    /// Atomically overlay current routing pressure, select an account, and reserve its in-flight
    /// lease. New-session callers use this instead of the racy
    /// `overlay -> pick -> record_selected -> acquire_in_flight` sequence, where concurrent
    /// requests could all observe the same pre-reservation snapshot and dogpile one account.
    ///
    /// The selector is synchronous and pure, so no await or external I/O occurs while the routing
    /// state write lock is held. Pinned continuation owners deliberately keep using the ordinary
    /// acquire path: ownership is a hard protocol constraint, not a load-balancing choice.
    pub fn select_and_acquire(
        self: &Arc<Self>,
        snapshots: &mut [AccountSnapshot],
        selector: &dyn Selector,
        ctx: &SelectionCtx,
        now: i64,
        metrics: &Arc<LeaseMetrics>,
    ) -> Option<(AccountId, InFlightGuard)> {
        let pressure_units = ctx.request_pressure_units.max(1);
        let id = {
            let mut map = self.inner.write().unwrap_or_else(|e| e.into_inner());
            if self.admission_limits.global_in_flight > 0
                && map.values().fold(0u32, |total, runtime| {
                    total.saturating_add(runtime.in_flight)
                }) >= self.admission_limits.global_in_flight
            {
                return None;
            }
            if self.admission_limits.global_in_flight_pressure > 0
                && map
                    .values()
                    .fold(0u32, |total, runtime| {
                        total.saturating_add(runtime.in_flight_pressure)
                    })
                    .saturating_add(pressure_units)
                    > self.admission_limits.global_in_flight_pressure
            {
                return None;
            }
            for snap in snapshots.iter_mut() {
                if let Some(rt) = map.get(&snap.id) {
                    snap.error_count = rt.error_count;
                    snap.last_error_at = rt.last_error_at;
                    let runtime_cooldown = rt.cooldown_until.filter(|&cd| now < cd);
                    snap.cooldown_until = match (snap.cooldown_until, runtime_cooldown) {
                        (Some(durable), Some(runtime)) => Some(durable.max(runtime)),
                        (durable, runtime) => durable.or(runtime),
                    };
                    snap.last_selected_at = rt.last_selected_at;
                    snap.health_tier = rt.health_tier;
                    snap.in_flight = rt.in_flight;
                    snap.in_flight_pressure = rt.in_flight_pressure;
                    snap.open_ws = rt.open_ws;
                }
            }
            let available: Vec<_> = snapshots
                .iter()
                .filter(|snapshot| {
                    let ordinary_limit = self
                        .admission_limits
                        .account_in_flight
                        .saturating_sub(self.admission_limits.owner_recovery_reserve);
                    let ordinary_pressure_limit = self
                        .admission_limits
                        .account_in_flight_pressure
                        .saturating_sub(self.admission_limits.owner_recovery_pressure_reserve);
                    let count_available = self.admission_limits.account_in_flight == 0
                        || map.get(&snapshot.id).map_or(0, |runtime| runtime.in_flight)
                            < ordinary_limit;
                    let pressure_available = self.admission_limits.account_in_flight_pressure == 0
                        || map
                            .get(&snapshot.id)
                            .map_or(0, |runtime| runtime.in_flight_pressure)
                            .saturating_add(pressure_units)
                            <= ordinary_pressure_limit;
                    count_available && pressure_available
                })
                .cloned()
                .collect();
            let id = selector.pick(&available, ctx)?;
            let rt = map.entry(id.clone()).or_default();
            rt.last_selected_at = Some(now);
            rt.in_flight = rt.in_flight.saturating_add(1);
            rt.in_flight_pressure = rt.in_flight_pressure.saturating_add(pressure_units);
            id
        };

        metrics.record_acquire();
        let guard = InFlightGuard {
            runtime: Arc::clone(self),
            id: id.clone(),
            pressure_units,
            metrics: Arc::clone(metrics),
        };
        Some((id, guard))
    }

    /// Queue otherwise-eligible new work behind the hard admission cap for the configured bounded
    /// window. A genuinely ineligible/empty pool returns immediately; only capacity saturation
    /// waits for a guard drop notification.
    pub async fn select_and_acquire_wait(
        self: &Arc<Self>,
        snapshots: &mut [AccountSnapshot],
        selector: &dyn Selector,
        ctx: &SelectionCtx,
        now: i64,
        metrics: &Arc<LeaseMetrics>,
    ) -> Option<(AccountId, InFlightGuard)> {
        let deadline = tokio::time::Instant::now() + self.admission_limits.wait_timeout;
        let mut wait: Option<AdmissionWaitGuard<'_>> = None;
        loop {
            let changed = self.admission_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();

            // Probe without hard-cap filtering. If the normal eligibility pipeline rejects every
            // account, waiting for a lease release cannot help and would only delay the real 503.
            let mut eligibility_probe = snapshots.to_vec();
            self.overlay(&mut eligibility_probe, now);
            if selector.pick(&eligibility_probe, ctx).is_none() {
                if let Some(wait) = wait.take() {
                    wait.finish(AdmissionWaitOutcome::Ineligible);
                } else {
                    self.admission_metrics
                        .record_ineligible(AdmissionLane::NewRequest);
                }
                return None;
            }
            if let Some(reservation) =
                self.select_and_acquire(snapshots, selector, ctx, now, metrics)
            {
                if let Some(wait) = wait.take() {
                    wait.finish(AdmissionWaitOutcome::Acquired);
                }
                return Some(reservation);
            }
            if wait.is_none() {
                wait = Some(self.admission_metrics.start_wait(AdmissionLane::NewRequest));
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                wait.take()
                    .expect("new request wait starts before timeout")
                    .finish(AdmissionWaitOutcome::Timeout);
                return None;
            }
        }
    }

    /// Atomically overlay routing pressure, select a new WebSocket owner, and reserve that
    /// socket's pressure before another concurrent handshake can select from the same snapshot.
    ///
    /// This is the open-socket counterpart to [`Self::select_and_acquire`]. A WebSocket does not
    /// hold an in-flight turn while idle, so admission reserves `open_ws`; each generating turn
    /// acquires its own ordinary in-flight lease in the relay pump.
    pub fn select_and_acquire_open_ws(
        self: &Arc<Self>,
        snapshots: &mut [AccountSnapshot],
        selector: &dyn Selector,
        ctx: &SelectionCtx,
        now: i64,
    ) -> Option<(AccountId, WsSocketGuard)> {
        let id = {
            let mut map = self.inner.write().unwrap_or_else(|e| e.into_inner());
            if self.admission_limits.global_open_ws > 0
                && map
                    .values()
                    .fold(0u32, |total, runtime| total.saturating_add(runtime.open_ws))
                    >= self.admission_limits.global_open_ws
            {
                return None;
            }
            for snap in snapshots.iter_mut() {
                if let Some(rt) = map.get(&snap.id) {
                    snap.error_count = rt.error_count;
                    snap.last_error_at = rt.last_error_at;
                    let runtime_cooldown = rt.cooldown_until.filter(|&cd| now < cd);
                    snap.cooldown_until = match (snap.cooldown_until, runtime_cooldown) {
                        (Some(durable), Some(runtime)) => Some(durable.max(runtime)),
                        (durable, runtime) => durable.or(runtime),
                    };
                    snap.last_selected_at = rt.last_selected_at;
                    snap.health_tier = rt.health_tier;
                    snap.in_flight = rt.in_flight;
                    snap.in_flight_pressure = rt.in_flight_pressure;
                    snap.open_ws = rt.open_ws;
                }
            }
            let available: Vec<_> = snapshots
                .iter()
                .filter(|snapshot| {
                    let ordinary_limit = self
                        .admission_limits
                        .account_open_ws
                        .saturating_sub(self.admission_limits.owner_recovery_reserve);
                    self.admission_limits.account_open_ws == 0
                        || map.get(&snapshot.id).map_or(0, |runtime| runtime.open_ws)
                            < ordinary_limit
                })
                .cloned()
                .collect();
            let id = selector.pick(&available, ctx)?;
            let rt = map.entry(id.clone()).or_default();
            rt.last_selected_at = Some(now);
            rt.open_ws = rt.open_ws.saturating_add(1);
            id
        };

        let guard = WsSocketGuard {
            runtime: Arc::clone(self),
            id: id.clone(),
        };
        Some((id, guard))
    }

    pub async fn select_and_acquire_open_ws_wait(
        self: &Arc<Self>,
        snapshots: &mut [AccountSnapshot],
        selector: &dyn Selector,
        ctx: &SelectionCtx,
        now: i64,
    ) -> Option<(AccountId, WsSocketGuard)> {
        let deadline = tokio::time::Instant::now() + self.admission_limits.wait_timeout;
        let mut wait: Option<AdmissionWaitGuard<'_>> = None;
        loop {
            let changed = self.admission_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();

            let mut eligibility_probe = snapshots.to_vec();
            self.overlay(&mut eligibility_probe, now);
            if selector.pick(&eligibility_probe, ctx).is_none() {
                if let Some(wait) = wait.take() {
                    wait.finish(AdmissionWaitOutcome::Ineligible);
                } else {
                    self.admission_metrics
                        .record_ineligible(AdmissionLane::NewSocket);
                }
                return None;
            }
            if let Some(reservation) =
                self.select_and_acquire_open_ws(snapshots, selector, ctx, now)
            {
                if let Some(wait) = wait.take() {
                    wait.finish(AdmissionWaitOutcome::Acquired);
                }
                return Some(reservation);
            }
            if wait.is_none() {
                wait = Some(self.admission_metrics.start_wait(AdmissionLane::NewSocket));
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                wait.take()
                    .expect("new socket wait starts before timeout")
                    .finish(AdmissionWaitOutcome::Timeout);
                return None;
            }
        }
    }

    pub fn admission_metrics_snapshot(&self) -> Vec<AdmissionMetricSnapshot> {
        self.admission_metrics.snapshot()
    }

    pub fn pressure_calibration_snapshot(&self) -> PressureCalibrationSnapshot {
        PressureCalibrationSnapshot {
            ratio: self
                .pressure_calibration
                .ratio_milli
                .load(Ordering::Relaxed) as f64
                / 1_000.0,
            samples: self.pressure_calibration.samples.load(Ordering::Relaxed),
            estimated_tokens: self
                .pressure_calibration
                .estimated_tokens
                .load(Ordering::Relaxed),
            actual_pressure_tokens: self
                .pressure_calibration
                .actual_pressure_tokens
                .load(Ordering::Relaxed),
        }
    }

    /// TEST-ONLY seam (B5 Task 4 adversarial review, FIX 3): stamp `cooldown_until` on an account's
    /// runtime entry directly, bypassing [`Self::record_rate_limit`]'s [`RATE_LIMITED_MIN_COOLDOWN_SECS`]
    /// floor. That floor makes a short, test-scale in-memory cooldown unrepresentable via the normal
    /// recording API — exactly what's needed to exercise `overlay`'s elapsed-`cooldown_until` DROP
    /// (this module's doc, and the `overlay` body above) on a fast test timescale, distinctly from
    /// the durable `rate_limited`/`reset_at` gate (`select.rs::eligibility`). No production call site
    /// uses this; it exists solely for `polyflare-server`'s integration test suite.
    pub fn set_cooldown_until_for_test(&self, id: &AccountId, cooldown_until: i64) {
        self.mutate(id, |rt| rt.cooldown_until = Some(cooldown_until));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logical_turn_attempt_budget_is_atomic_across_concurrent_requests() {
        let runtime = Arc::new(RuntimeStates::new());
        let barrier = Arc::new(std::sync::Barrier::new(32));
        let mut workers = Vec::new();

        for _ in 0..32 {
            let runtime = runtime.clone();
            let barrier = barrier.clone();
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                runtime.try_consume_logical_turn_attempt(Some("hashed-turn-key"), 3, 1_000)
            }));
        }

        let admitted = workers
            .into_iter()
            .filter_map(|worker| worker.join().expect("attempt worker").then_some(()))
            .count();
        assert_eq!(
            admitted, 3,
            "the process-wide limit must be consumed atomically"
        );
    }

    #[test]
    fn logical_turn_attempt_budget_exhausts_and_resets_after_ttl() {
        let runtime = RuntimeStates::new();
        let key = Some("hashed-turn-key");

        assert!(runtime.try_consume_logical_turn_attempt(key, 2, 1_000));
        assert!(runtime.try_consume_logical_turn_attempt(key, 2, 1_001));
        assert!(!runtime.try_consume_logical_turn_attempt(key, 2, 1_002));
        assert!(
            runtime.try_consume_logical_turn_attempt(key, 2, 1_001 + LOGICAL_TURN_ATTEMPT_TTL_SECS),
            "an expired logical turn must start with a fresh budget"
        );
    }

    #[test]
    fn missing_logical_turn_key_bypasses_the_aggregate_budget() {
        let runtime = RuntimeStates::new();
        for _ in 0..100 {
            assert!(runtime.try_consume_logical_turn_attempt(None, 1, 1_000));
        }
    }

    #[test]
    fn completed_generation_clears_the_logical_turn_budget_for_the_next_round() {
        let runtime = RuntimeStates::new();
        let key = Some("hashed-turn-key");

        // Codex reuses one turn id for every tool-call round of a user turn: exhaust the budget
        // with successful rounds, then prove a forwarded completion resets it for round N+1.
        assert!(runtime.try_consume_logical_turn_attempt(key, 2, 1_000));
        assert!(runtime.try_consume_logical_turn_attempt(key, 2, 1_001));
        assert!(!runtime.try_consume_logical_turn_attempt(key, 2, 1_002));
        runtime.clear_logical_turn_attempts(key);
        assert!(
            runtime.try_consume_logical_turn_attempt(key, 2, 1_003),
            "a completed generation must reset the aggregate budget for the turn's next round"
        );
        runtime.clear_logical_turn_attempts(None); // missing identity stays a no-op
        runtime.clear_logical_turn_attempts(Some("never-consumed")); // absent key stays a no-op
    }

    #[test]
    fn rejected_auth_attempt_can_be_refunded_without_expanding_the_limit() {
        let runtime = RuntimeStates::new();
        let key = Some("hashed-turn-key");

        assert!(runtime.try_consume_logical_turn_attempt(key, 1, 1_000));
        runtime.refund_logical_turn_attempt(key);
        assert!(
            runtime.try_consume_logical_turn_attempt(key, 1, 1_001),
            "a bearer rejected before sampling must not spend the generation budget"
        );
        assert!(!runtime.try_consume_logical_turn_attempt(key, 1, 1_002));
    }

    fn snap(id: &str) -> AccountSnapshot {
        AccountSnapshot::new(id)
    }

    fn admission_lane(runtime: &RuntimeStates, work: &str, scope: &str) -> AdmissionMetricSnapshot {
        runtime
            .admission_metrics_snapshot()
            .into_iter()
            .find(|lane| lane.work == work && lane.scope == scope)
            .expect("fixed admission lane")
    }

    /// Test-only seam: directly mutate an account's runtime entry, bypassing the funnel API, so a
    /// test can seed an arbitrary starting state (e.g. a PROBING or DRAINING entry with a specific
    /// `drain_entered_at`) without needing a production code path to reach it. `mod tests` is a
    /// descendant of this module, so it may reach the private `inner` field directly — no new public
    /// API surface needed.
    fn seed(rs: &RuntimeStates, id: &AccountId, f: impl FnOnce(&mut RuntimeState)) {
        let mut map = rs.inner.write().unwrap();
        let entry = map.entry(id.clone()).or_default();
        f(entry);
    }

    /// Test-only seam: read an account's raw runtime entry (or `None` if absent/GC'd), for
    /// asserting on aux state (`drain_entered_at`/`probe_success_streak`) that `overlay` never
    /// exposes on the snapshot.
    fn peek(rs: &RuntimeStates, id: &AccountId) -> Option<RuntimeState> {
        let map = rs.inner.read().unwrap();
        map.get(id).cloned()
    }

    #[test]
    fn backoff_grows_and_is_bounded() {
        assert_eq!(backoff_secs(1), 0); // 200ms → 0s
        assert_eq!(backoff_secs(4), 1); // 200ms·8 = 1.6s → 1s
                                        // Huge counts don't overflow — the shift is capped.
        assert!(backoff_secs(u32::MAX) > 0);
    }

    #[test]
    fn overlay_is_a_noop_for_unknown_accounts() {
        let rs = RuntimeStates::new();
        let mut snaps = vec![snap("a"), snap("b")];
        rs.overlay(&mut snaps, 1000);
        assert_eq!(snaps[0].error_count, 0);
        assert_eq!(snaps[0].cooldown_until, None);
    }

    #[test]
    fn record_rate_limit_sets_cooldown_error_and_floor() {
        let rs = RuntimeStates::new();
        let id = AccountId::from("a");
        let metrics = RateLimitMetrics::new();
        // No Retry-After ⇒ backoff, but floored to the 30s minimum.
        rs.record_rate_limit(&id, None, 1000, &metrics);
        let mut snaps = vec![snap("a")];
        rs.overlay(&mut snaps, 1000);
        assert_eq!(snaps[0].error_count, 1);
        assert_eq!(snaps[0].last_error_at, Some(1000));
        assert_eq!(
            snaps[0].cooldown_until,
            Some(1000 + RATE_LIMITED_MIN_COOLDOWN_SECS),
            "sub-floor delay is raised to the 30s floor"
        );
    }

    #[test]
    fn record_rate_limit_honors_retry_after_and_never_shortens() {
        let rs = RuntimeStates::new();
        let id = AccountId::from("a");
        let metrics = RateLimitMetrics::new();
        rs.record_rate_limit(&id, Some(600), 1000, &metrics); // cooldown until 1600
        rs.record_rate_limit(&id, Some(60), 1010, &metrics); // shorter → must NOT shorten the bench
        let mut snaps = vec![snap("a")];
        rs.overlay(&mut snaps, 1000);
        assert_eq!(snaps[0].cooldown_until, Some(1600), "later cooldown wins");
        assert_eq!(snaps[0].error_count, 2);
    }

    // --- C11b Task 2: `record_rate_limit` bumps the threaded `RateLimitMetrics` handle exactly
    // once per call, keyed by whether `retry_after` was upstream-supplied vs computed backoff. ---

    #[test]
    fn record_rate_limit_with_retry_after_bumps_upstream_kind() {
        let rs = RuntimeStates::new();
        let id = AccountId::from("a");
        let metrics = RateLimitMetrics::new();
        rs.record_rate_limit(&id, Some(600), 1000, &metrics);
        assert_eq!(
            metrics.snapshot(),
            vec![("upstream".to_string(), 1)],
            "an upstream-supplied Retry-After records the \"upstream\" kind"
        );
    }

    #[test]
    fn record_rate_limit_without_retry_after_bumps_backoff_kind() {
        let rs = RuntimeStates::new();
        let id = AccountId::from("a");
        let metrics = RateLimitMetrics::new();
        rs.record_rate_limit(&id, None, 1000, &metrics);
        assert_eq!(
            metrics.snapshot(),
            vec![("backoff".to_string(), 1)],
            "no Retry-After (computed exponential backoff) records the \"backoff\" kind"
        );
    }

    #[test]
    fn record_rate_limit_bumps_metrics_once_per_call_across_kinds() {
        let rs = RuntimeStates::new();
        let id = AccountId::from("a");
        let metrics = RateLimitMetrics::new();
        rs.record_rate_limit(&id, Some(60), 1000, &metrics);
        rs.record_rate_limit(&id, None, 1010, &metrics);
        rs.record_rate_limit(&id, Some(60), 1020, &metrics);
        let mut snapshot = metrics.snapshot();
        snapshot.sort();
        assert_eq!(
            snapshot,
            vec![("backoff".to_string(), 1), ("upstream".to_string(), 2)],
            "exactly one bump per record_rate_limit call, keyed by kind"
        );
    }

    #[test]
    fn rate_limit_clamps_hostile_retry_after_and_never_overflows() {
        // A huge Retry-After with a near-max `now` must saturate, not panic/overflow.
        let rs = RuntimeStates::new();
        let id = AccountId::from("a");
        let metrics = RateLimitMetrics::new();
        rs.record_rate_limit(&id, Some(i64::MAX), i64::MAX - 10, &metrics);
        let mut snaps = vec![snap("a")];
        rs.overlay(&mut snaps, 1000);
        assert_eq!(
            snaps[0].cooldown_until,
            Some(i64::MAX),
            "saturating_add caps, no overflow"
        );

        // A finite-but-excessive Retry-After (48h) is clamped to the 24h ceiling.
        let rs2 = RuntimeStates::new();
        rs2.record_rate_limit(&id, Some(48 * 3600), 1000, &metrics);
        let mut s2 = vec![snap("a")];
        rs2.overlay(&mut s2, 1000);
        assert_eq!(
            s2[0].cooldown_until,
            Some(1000 + MAX_COOLDOWN_SECS),
            "48h Retry-After clamped to the 24h ceiling"
        );
    }

    #[test]
    fn expired_cooldown_is_dropped_so_transient_benching_survives_a_prior_429() {
        // Regression: an ELAPSED cooldown must NOT be handed to the selector. Otherwise select.rs's
        // `now >= cd ⇒ clear error state` branch re-fires on every read (the stored timestamp never
        // changes), permanently zeroing error_count and disabling transient-error benching for any
        // account that has ever been 429'd.
        let rs = RuntimeStates::new();
        let id = AccountId::from("a");
        let metrics = RateLimitMetrics::new();
        rs.record_rate_limit(&id, Some(30), 1000, &metrics); // cooldown until 1030 (error_count → 1)
                                                             // Within the cooldown window ⇒ it IS supplied (the account is benched).
        let mut during = vec![snap("a")];
        rs.overlay(&mut during, 1000);
        assert_eq!(during[0].cooldown_until, Some(1030));
        // The account then reaches the backoff threshold on transient errors after the cooldown.
        rs.record_transient_error(&id, 1040);
        rs.record_transient_error(&id, 1041); // error_count now 3 in the map
        let mut after = vec![snap("a")];
        rs.overlay(&mut after, 1050); // now > 1030 ⇒ cooldown elapsed
        assert_eq!(
            after[0].cooldown_until, None,
            "an elapsed cooldown is dropped, not re-supplied"
        );
        assert_eq!(
            after[0].error_count, 3,
            "error_count stays visible so the select.rs backoff gate can fire"
        );
    }

    #[test]
    fn record_quota_does_not_bump_error_count() {
        let rs = RuntimeStates::new();
        let id = AccountId::from("a");
        rs.record_quota_exceeded(&id, 1000);
        let mut snaps = vec![snap("a")];
        rs.overlay(&mut snaps, 1000);
        assert_eq!(
            snaps[0].error_count, 0,
            "quota is a capacity signal, not an error"
        );
        assert_eq!(
            snaps[0].cooldown_until,
            Some(1000 + QUOTA_EXCEEDED_COOLDOWN_SECS)
        );
    }

    #[test]
    fn cooldown_persistence_coalesces_repeated_writes_per_account() {
        let rs = RuntimeStates::new();
        let id = AccountId::from("a");
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        rs.register_cooldown_persistence(tx);

        for now in 0..600 {
            rs.record_quota_exceeded(&id, now);
        }

        assert_eq!(rx.try_recv().unwrap(), id);
        assert_eq!(
            rx.try_recv().unwrap_err(),
            tokio::sync::mpsc::error::TryRecvError::Empty,
            "only one wakeup may be queued for repeated writes to one account"
        );
        let latest = rs
            .pending_cooldown(&AccountId::from("a"))
            .expect("the latest write remains pending");
        assert_eq!(latest.updated_at, 599);
        assert_eq!(latest.cooldown_until, 599 + QUOTA_EXCEEDED_COOLDOWN_SECS);
        assert_eq!(latest.reason, "quota");
    }

    #[test]
    fn cooldown_persistence_keeps_failed_write_pending_and_preserves_newer_value() {
        let rs = RuntimeStates::new();
        let id = AccountId::from("a");
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        rs.register_cooldown_persistence(tx);

        rs.record_quota_exceeded(&id, 10);
        assert_eq!(rx.try_recv().unwrap(), id);
        let first = rs.pending_cooldown(&AccountId::from("a")).unwrap();
        assert_eq!(first.updated_at, 10);

        rs.record_quota_exceeded(&AccountId::from("a"), 20);
        assert_eq!(
            rx.try_recv().unwrap_err(),
            tokio::sync::mpsc::error::TryRecvError::Empty,
            "an in-progress key stays coalesced"
        );
        assert!(
            rs.finish_cooldown_persist(&first),
            "persisting the stale snapshot must leave the newer value pending"
        );
        let second = rs.pending_cooldown(&AccountId::from("a")).unwrap();
        assert_eq!(second.updated_at, 20);
        assert!(
            !rs.finish_cooldown_persist(&second),
            "persisting the current snapshot must clear the pending key"
        );
        assert!(rs.pending_cooldown(&AccountId::from("a")).is_none());
    }

    #[test]
    fn record_success_clears_error_state_and_gcs_the_now_neutral_entry() {
        let rs = RuntimeStates::new();
        let id = AccountId::from("a");
        rs.record_transient_error(&id, 1000);
        rs.record_transient_error(&id, 1001);
        let mut snaps = vec![snap("a")];
        rs.overlay(&mut snaps, 1000);
        assert_eq!(snaps[0].error_count, 2);

        // Success clears the error state; with no selection stamp (record_selected wasn't called),
        // the entry is now fully neutral and is GC'd — the overlay finds nothing and leaves defaults.
        rs.record_success(&id);
        let mut snaps2 = vec![snap("a")];
        rs.overlay(&mut snaps2, 1002);
        assert_eq!(snaps2[0].error_count, 0);
        assert_eq!(snaps2[0].last_error_at, None);
        assert_eq!(snaps2[0].last_selected_at, None);
    }

    #[test]
    fn record_selected_owns_last_selected_and_survives_a_success() {
        // record_selected (called at selection) is the sole source of last_selected_at; a later
        // success clears only the error state, so a selected account's entry persists for the
        // round_robin tiebreak.
        let rs = RuntimeStates::new();
        let id = AccountId::from("a");
        rs.record_selected(&id, 500);
        rs.record_transient_error(&id, 1000);
        rs.record_success(&id);
        let mut snaps = vec![snap("a")];
        rs.overlay(&mut snaps, 2000);
        assert_eq!(snaps[0].error_count, 0, "success cleared the error");
        assert_eq!(
            snaps[0].last_selected_at,
            Some(500),
            "the selection stamp survives (round_robin needs it)"
        );
    }

    // --- B8 Task 1: evaluate_health_tier / compute_should_drain / transition / is_neutral ---

    #[test]
    fn healthy_transitions_on_should_drain() {
        // (a) HEALTHY + should_drain ⇒ DRAINING; HEALTHY + !drain ⇒ HEALTHY.
        assert_eq!(evaluate_health_tier(0, true, None, 0, false, 1000), 1);
        assert_eq!(evaluate_health_tier(0, false, None, 0, false, 1000), 0);
    }

    #[test]
    fn draining_transitions() {
        // (b) DRAINING + should_drain ⇒ DRAINING (stays, regardless of the timer).
        assert_eq!(evaluate_health_tier(1, true, Some(900), 0, false, 1000), 1);
        // DRAINING + !drain + now-drain_entered_at >= 60 ⇒ PROBING.
        assert_eq!(evaluate_health_tier(1, false, Some(940), 0, false, 1000), 2); // diff == 60
        assert_eq!(evaluate_health_tier(1, false, Some(500), 0, false, 1000), 2); // diff > 60
                                                                                  // DRAINING + !drain + < 60 ⇒ stays DRAINING.
        assert_eq!(evaluate_health_tier(1, false, Some(950), 0, false, 1000), 1); // diff == 50
                                                                                  // DRAINING + !drain + drain_entered_at == None ⇒ stays DRAINING (no promote without a stamp).
        assert_eq!(evaluate_health_tier(1, false, None, 0, false, 1000), 1);
    }

    #[test]
    fn probing_transitions() {
        // (c) PROBING + should_drain ⇒ DRAINING.
        assert_eq!(evaluate_health_tier(2, true, None, 5, false, 1000), 1);
        // PROBING + streak >= 3 ⇒ HEALTHY.
        assert_eq!(evaluate_health_tier(2, false, None, 3, false, 1000), 0);
        assert_eq!(evaluate_health_tier(2, false, None, 9, false, 1000), 0);
        // PROBING + streak < 3 ⇒ stays PROBING.
        assert_eq!(evaluate_health_tier(2, false, None, 2, false, 1000), 2);
        assert_eq!(evaluate_health_tier(2, false, None, 0, false, 1000), 2);
    }

    #[test]
    fn frozen_status_freezes_the_tier_regardless_of_should_drain() {
        // (d) frozen ⇒ returns current_tier unchanged, no matter what should_drain/aux state says.
        assert_eq!(evaluate_health_tier(0, true, None, 0, true, 1000), 0);
        assert_eq!(evaluate_health_tier(1, false, Some(900), 0, true, 1000), 1);
        assert_eq!(evaluate_health_tier(2, true, None, 5, true, 1000), 2);
    }

    #[test]
    fn compute_should_drain_conditions() {
        // (e) each OR condition independently true, plus the strict boundary checks.
        assert!(
            compute_should_drain(Some(85.0), None, 0, None, 1000),
            "used% == 85 exactly is >= threshold"
        );
        assert!(
            compute_should_drain(None, Some(90.0), 0, None, 1000),
            "secondary% == 90 exactly is >= threshold"
        );
        assert!(
            compute_should_drain(None, None, 2, Some(941), 1000),
            "error_count=2, now-last_error_at == 59 < 60"
        );
        assert!(
            !compute_should_drain(None, None, 2, Some(940), 1000),
            "now-last_error_at == 60 is NOT < 60 (strict)"
        );
        assert!(
            !compute_should_drain(None, None, 2, Some(939), 1000),
            "now-last_error_at == 61, well outside the window"
        );
        assert!(
            !compute_should_drain(None, None, 1, Some(999), 1000),
            "error_count=1 is below the DRAIN_ERROR_COUNT threshold even though recent"
        );
        assert!(
            !compute_should_drain(None, None, 0, None, 1000),
            "all None/0 ⇒ no condition fires"
        );
    }

    #[test]
    fn transition_writeback_edges() {
        // (f) HEALTHY -> DRAINING stamps drain_entered_at + resets streak.
        let mut rt = RuntimeState {
            probe_success_streak: 7,
            ..RuntimeState::default()
        };
        rt.transition(1, 1000);
        assert_eq!(rt.health_tier, 1);
        assert_eq!(rt.drain_entered_at, Some(1000));
        assert_eq!(rt.probe_success_streak, 0);

        // DRAINING -> PROBING leaves drain_entered_at intact (no stamp/clear on this edge).
        rt.transition(2, 1100);
        assert_eq!(rt.health_tier, 2);
        assert_eq!(
            rt.drain_entered_at,
            Some(1000),
            "drain_entered_at survives DRAINING->PROBING"
        );

        // -> HEALTHY clears both aux fields.
        rt.probe_success_streak = 5;
        rt.transition(0, 1200);
        assert_eq!(rt.health_tier, 0);
        assert_eq!(rt.drain_entered_at, None);
        assert_eq!(rt.probe_success_streak, 0);

        // PROBING -> DRAINING (re-entering DRAINING from PROBING) also stamps fresh, per the
        // "any non-DRAINING -> DRAINING" edge definition.
        let mut rt2 = RuntimeState {
            health_tier: 2,
            drain_entered_at: Some(50),
            probe_success_streak: 2,
            ..RuntimeState::default()
        };
        rt2.transition(1, 2000);
        assert_eq!(rt2.health_tier, 1);
        assert_eq!(rt2.drain_entered_at, Some(2000));
        assert_eq!(rt2.probe_success_streak, 0);
    }

    #[test]
    fn is_neutral_true_for_defaulted_state_including_new_health_fields() {
        // (g) a freshly-defaulted state (incl. health_tier/drain_entered_at/probe_success_streak) is
        // still neutral, so a healthy account's entry is still GC'd by `mutate`.
        let rt = RuntimeState::default();
        assert!(rt.is_neutral());
        assert_eq!(rt.health_tier, 0);
        assert_eq!(rt.drain_entered_at, None);
        assert_eq!(rt.probe_success_streak, 0);
    }

    #[test]
    fn transient_errors_accumulate_for_the_backoff_gate() {
        let rs = RuntimeStates::new();
        let id = AccountId::from("a");
        for t in 0..3 {
            rs.record_transient_error(&id, 1000 + t);
        }
        let mut snaps = vec![snap("a")];
        rs.overlay(&mut snaps, 1000);
        assert_eq!(
            snaps[0].error_count, 3,
            "reaches the select.rs error-backoff threshold"
        );
        assert_eq!(snaps[0].cooldown_until, None, "transient sets no cooldown");
    }

    // --- B8 Task 2: error-driven evaluation in the funnel + overlay copy ---

    #[test]
    fn overlay_copies_health_tier_and_defaults_absent_entry_to_zero() {
        // (e) overlay copies health_tier for a known entry; an absent entry stays 0.
        let rs = RuntimeStates::new();
        let id = AccountId::from("a");
        rs.record_transient_error(&id, 1000);
        rs.record_transient_error(&id, 1001); // error_count=2 within 60s ⇒ error-drain ⇒ DRAINING
        let mut snaps = vec![snap("a"), snap("b")];
        rs.overlay(&mut snaps, 1001);
        assert_eq!(
            snaps[0].health_tier, 1,
            "known entry: tier copied from runtime state"
        );
        assert_eq!(
            snaps[1].health_tier, 0,
            "absent entry: stays at the neutral default"
        );
    }

    #[test]
    fn two_transient_errors_within_window_drain_the_account() {
        // (a) error_count reaches 2 within 60s via record_transient_error ⇒ overlay health_tier == 1.
        let rs = RuntimeStates::new();
        let id = AccountId::from("a");
        rs.record_transient_error(&id, 1000);
        rs.record_transient_error(&id, 1010);
        let mut snaps = vec![snap("a")];
        rs.overlay(&mut snaps, 1010);
        assert_eq!(snaps[0].health_tier, 1, "2 errors within 60s ⇒ DRAINING");
    }

    #[test]
    fn record_rate_limit_also_drains_on_error_flapping() {
        // record_rate_limit must wire the same error-driven transition as record_transient_error.
        let rs = RuntimeStates::new();
        let id = AccountId::from("a");
        let metrics = RateLimitMetrics::new();
        rs.record_rate_limit(&id, Some(5), 1000, &metrics); // error_count=1, cooldown clamped to floor
        rs.record_rate_limit(&id, Some(5), 1010, &metrics); // error_count=2, within 60s ⇒ error-drain
        let mut snaps = vec![snap("a")];
        rs.overlay(&mut snaps, 1010);
        assert_eq!(
            snaps[0].health_tier, 1,
            "2 rate-limit hits within 60s ⇒ DRAINING"
        );
    }

    #[test]
    fn probing_streak_of_three_successes_promotes_to_healthy_and_clears_aux() {
        // (b) seed a PROBING entry, 3x record_success ⇒ health_tier == 0 (HEALTHY) + aux cleared.
        let rs = RuntimeStates::new();
        let id = AccountId::from("a");
        rs.record_transient_error(&id, 500); // create the entry
        seed(&rs, &id, |rt| {
            rt.health_tier = 2; // PROBING
            rt.drain_entered_at = Some(100);
            rt.probe_success_streak = 0;
            rt.error_count = 0;
            rt.last_error_at = None;
        });
        rs.record_success(&id);
        rs.record_success(&id);
        rs.record_success(&id); // 3rd success completes the streak ⇒ HEALTHY
        let mut snaps = vec![snap("a")];
        rs.overlay(&mut snaps, 2000);
        assert_eq!(
            snaps[0].health_tier, 0,
            "3 successes while PROBING ⇒ HEALTHY"
        );
        assert_eq!(snaps[0].error_count, 0);
        assert_eq!(snaps[0].last_error_at, None);
        assert_eq!(
            peek(&rs, &id),
            None,
            "HEALTHY + cleared aux + zero error state is fully neutral ⇒ GC'd from the map"
        );
    }

    #[test]
    fn probing_streak_below_three_stays_probing() {
        let rs = RuntimeStates::new();
        let id = AccountId::from("a");
        rs.record_transient_error(&id, 500);
        seed(&rs, &id, |rt| {
            rt.health_tier = 2;
            rt.drain_entered_at = Some(100);
            rt.probe_success_streak = 0;
            rt.error_count = 0;
            rt.last_error_at = None;
        });
        rs.record_success(&id);
        rs.record_success(&id); // only 2 successes ⇒ still PROBING
        let mut snaps = vec![snap("a")];
        rs.overlay(&mut snaps, 2000);
        assert_eq!(
            snaps[0].health_tier, 2,
            "2 successes while PROBING ⇒ stays PROBING"
        );
    }

    #[test]
    fn probing_error_resets_streak_and_may_drain() {
        // (c) a PROBING account that gets a record_transient_error ⇒ streak reset to 0 (and tier
        // DRAINING if error-drain fires).
        let rs = RuntimeStates::new();
        let id = AccountId::from("a");
        rs.record_transient_error(&id, 500);
        seed(&rs, &id, |rt| {
            rt.health_tier = 2; // PROBING
            rt.drain_entered_at = Some(100);
            rt.probe_success_streak = 2;
            rt.error_count = 0;
            rt.last_error_at = None;
        });
        // One error alone isn't enough to error-drain (needs 2 within 60s), so tier stays PROBING,
        // but the streak must reset to 0.
        rs.record_transient_error(&id, 1000);
        seed(&rs, &id, |rt| {
            assert_eq!(
                rt.probe_success_streak, 0,
                "streak reset on any error while PROBING"
            );
            assert_eq!(rt.health_tier, 2, "single error alone doesn't error-drain");
        });
        // A second error within 60s DOES error-drain ⇒ PROBING -> DRAINING.
        rs.record_transient_error(&id, 1010);
        let mut snaps = vec![snap("a")];
        rs.overlay(&mut snaps, 1010);
        assert_eq!(
            snaps[0].health_tier, 1,
            "2nd error within 60s while PROBING ⇒ DRAINING"
        );
    }

    #[test]
    fn draining_account_is_not_promoted_to_probing_by_a_usage_blind_success() {
        // (d) THE CARE POINT: an account whose tier is DRAINING with drain_entered_at older than
        // 60s, then a record_success with NO usage signal, must NOT be promoted to PROBING via the
        // funnel — only the poller (which sees usage) may perform that quiet-timer promotion.
        let rs = RuntimeStates::new();
        let id = AccountId::from("a");
        rs.record_transient_error(&id, 500);
        seed(&rs, &id, |rt| {
            rt.health_tier = 1; // DRAINING
            rt.drain_entered_at = Some(100); // far more than PROBE_QUIET_SECS (60) before `now`
            rt.probe_success_streak = 0;
            rt.error_count = 0;
            rt.last_error_at = None;
        });
        rs.record_success(&id); // now = irrelevant here, but overlay below uses 2000 (>> 100+60)
        let mut snaps = vec![snap("a")];
        rs.overlay(&mut snaps, 2000);
        assert_eq!(
            snaps[0].health_tier, 1,
            "the funnel must never promote DRAINING->PROBING; only the poller (Task 3) does, \
             because only it has usage%"
        );
    }

    // --- B8 Task 3: usage-driven evaluation in the poller (evaluate_with_usage) ---

    #[test]
    fn evaluate_with_usage_drains_a_healthy_account_on_high_usage_and_stamps_drain_entered_at() {
        // (a) used%=90, no errors, HEALTHY ⇒ DRAINING + drain_entered_at stamped to `now`.
        let rs = RuntimeStates::new();
        let id = AccountId::from("a");
        rs.evaluate_with_usage(&id, Some(90.0), None, false, true, 1000);
        let mut snaps = vec![snap("a")];
        rs.overlay(&mut snaps, 1000);
        assert_eq!(snaps[0].health_tier, 1, "used% >= 85 ⇒ DRAINING");
        assert_eq!(
            peek(&rs, &id).unwrap().drain_entered_at,
            Some(1000),
            "entering DRAINING stamps drain_entered_at"
        );
    }

    #[test]
    fn evaluate_with_usage_promotes_draining_to_probing_once_quiet_and_below_threshold() {
        // (b) THE THING THE FUNNEL CAN'T DO: a DRAINING account whose drain_entered_at is >= 60s
        // old, with usage now BELOW threshold and no errors ⇒ PROBING (the poller's quiet-timer
        // demotion — it has usage%, so it can safely see !should_drain).
        let rs = RuntimeStates::new();
        let id = AccountId::from("a");
        seed(&rs, &id, |rt| {
            rt.health_tier = 1; // DRAINING
            rt.drain_entered_at = Some(1000); // 61s before the `now` used below
        });
        rs.evaluate_with_usage(&id, Some(10.0), None, false, true, 1061);
        let mut snaps = vec![snap("a")];
        rs.overlay(&mut snaps, 1061);
        assert_eq!(
            snaps[0].health_tier, 2,
            "quiet (>=60s) + usage below threshold + no errors ⇒ PROBING"
        );
    }

    #[test]
    fn evaluate_with_usage_disabled_forces_healthy_and_clears_aux_even_at_high_usage() {
        // (c) enabled=false ⇒ forces HEALTHY + clears aux, even when used%=99 (would otherwise
        // drain). Does NOT clobber existing error state (a separate, funnel-owned signal).
        let rs = RuntimeStates::new();
        let id = AccountId::from("a");
        seed(&rs, &id, |rt| {
            rt.health_tier = 1; // DRAINING
            rt.drain_entered_at = Some(500);
            rt.probe_success_streak = 2;
            rt.error_count = 3;
            rt.last_error_at = Some(900);
        });
        rs.evaluate_with_usage(&id, Some(99.0), None, false, false, 1000);
        let entry = peek(&rs, &id).expect("non-neutral: error state survives");
        assert_eq!(
            entry.health_tier, 0,
            "disabled ⇒ forced HEALTHY regardless of usage"
        );
        assert_eq!(entry.drain_entered_at, None, "aux cleared");
        assert_eq!(entry.probe_success_streak, 0, "aux cleared");
        assert_eq!(
            entry.error_count, 3,
            "error state is NOT clobbered by the disable path"
        );
        assert_eq!(
            entry.last_error_at,
            Some(900),
            "error state is NOT clobbered"
        );
    }

    #[test]
    fn evaluate_with_usage_frozen_status_leaves_tier_unchanged_at_high_usage() {
        // (d) a frozen (blocked-status) account's tier passes through UNCHANGED regardless of
        // should_drain — matches evaluate_health_tier's frozen contract (Task 1).
        let rs = RuntimeStates::new();
        let id = AccountId::from("a");
        seed(&rs, &id, |rt| {
            rt.health_tier = 2; // PROBING
            rt.probe_success_streak = 1;
        });
        rs.evaluate_with_usage(&id, Some(99.0), None, true, true, 1000);
        let mut snaps = vec![snap("a")];
        rs.overlay(&mut snaps, 1000);
        assert_eq!(
            snaps[0].health_tier, 2,
            "frozen ⇒ tier unchanged despite used%=99"
        );
    }

    // --- C9 Task 1: the leak-proof InFlightGuard + in_flight runtime field + overlay ---

    #[test]
    fn acquire_in_flight_increments_and_dropping_guards_decrements_and_gcs() {
        // (a) two acquires ⇒ in_flight 2; dropping one ⇒ 1; dropping the other ⇒ 0 and the entry
        // is fully GC'd from the map (peek == None) since a neutral entry decays out.
        let rs = Arc::new(RuntimeStates::new());
        let id = AccountId::from("a");
        let metrics = LeaseMetrics::new();

        let guard1 = rs.acquire_in_flight(&id, 1000, &metrics);
        assert_eq!(
            peek(&rs, &id).map(|rt| rt.in_flight),
            Some(1),
            "first acquire ⇒ in_flight 1"
        );

        let guard2 = rs.acquire_in_flight(&id, 1000, &metrics);
        assert_eq!(
            peek(&rs, &id).map(|rt| rt.in_flight),
            Some(2),
            "second acquire ⇒ in_flight 2"
        );

        drop(guard1);
        assert_eq!(
            peek(&rs, &id).map(|rt| rt.in_flight),
            Some(1),
            "dropping one guard decrements to 1; entry survives (still non-neutral)"
        );

        drop(guard2);
        assert_eq!(
            peek(&rs, &id),
            None,
            "dropping the last guard decrements to 0; the now-fully-neutral entry is GC'd"
        );

        // C9 Task 4: the threaded LeaseMetrics handle saw both acquires and both releases.
        assert_eq!(metrics.acquired(), 2, "one bump per acquire_in_flight call");
        assert_eq!(metrics.released(), 2, "one bump per guard Drop");
        assert_eq!(metrics.current(), 0, "balanced ⇒ 0 derived in-flight");
    }

    #[test]
    fn overlay_copies_in_flight_and_defaults_absent_entry_to_zero() {
        // (b) overlay copies in_flight for a known entry; an absent entry stays at the neutral 0.
        let rs = Arc::new(RuntimeStates::new());
        let id = AccountId::from("a");
        let metrics = LeaseMetrics::new();
        let _guard = rs.acquire_in_flight(&id, 1000, &metrics);

        let mut snaps = vec![snap("a"), snap("b")];
        rs.overlay(&mut snaps, 1000);
        assert_eq!(
            snaps[0].in_flight, 1,
            "known entry: in_flight copied from runtime state"
        );
        assert_eq!(
            snaps[1].in_flight, 0,
            "absent entry: stays at the neutral default"
        );
    }

    #[test]
    fn is_neutral_is_false_while_in_flight_positive_and_true_once_it_returns_to_zero() {
        // (c) is_neutral must respect in_flight: non-neutral while > 0 (never GC'd mid-flight),
        // neutral again once back at 0 with no other signal.
        let mut rt = RuntimeState {
            in_flight: 1,
            ..RuntimeState::default()
        };
        assert!(!rt.is_neutral(), "in_flight > 0 must never be neutral");

        rt.in_flight = 0;
        assert!(
            rt.is_neutral(),
            "in_flight back at 0 with nothing else set ⇒ neutral"
        );

        // End-to-end via the map: while a guard is held, the account's entry must survive `mutate`'s
        // neutral-GC (e.g. record_selected on some other bookkeeping) rather than vanish mid-flight.
        let rs = Arc::new(RuntimeStates::new());
        let id = AccountId::from("a");
        let metrics = LeaseMetrics::new();
        let guard = rs.acquire_in_flight(&id, 1000, &metrics);
        rs.record_selected(&id, 1000); // triggers another mutate/GC-check cycle
        assert_eq!(
            peek(&rs, &id).map(|rt| rt.in_flight),
            Some(1),
            "entry with in_flight > 0 survives a mutate cycle instead of being GC'd"
        );
        drop(guard);
    }

    #[test]
    fn guard_is_not_clone_and_decrements_exactly_once() {
        // (d) InFlightGuard is not Clone/Copy (a single owned value ⇒ Drop can fire at most once by
        // Rust's ownership model). Two independent acquires simulate "two guards for one account";
        // each must decrement exactly once on its own drop, never double-counting the other's release.
        let rs = Arc::new(RuntimeStates::new());
        let id = AccountId::from("a");
        let metrics = LeaseMetrics::new();

        let guard_a = rs.acquire_in_flight(&id, 1000, &metrics);
        let guard_b = rs.acquire_in_flight(&id, 1000, &metrics);
        assert_eq!(peek(&rs, &id).map(|rt| rt.in_flight), Some(2));

        drop(guard_a);
        assert_eq!(
            peek(&rs, &id).map(|rt| rt.in_flight),
            Some(1),
            "dropping guard_a decrements exactly once, leaving guard_b's lease intact"
        );

        drop(guard_b);
        assert_eq!(
            peek(&rs, &id),
            None,
            "dropping guard_b releases the last lease ⇒ GC'd"
        );
    }

    #[test]
    fn in_flight_decrement_saturates_and_cannot_underflow() {
        // (e) a stray decrement below zero must clamp at 0, never wrap/panic. Simulate via the
        // test-only seed seam: an entry already at in_flight == 0 that still gets decremented
        // (e.g. a defensive extra release) must not underflow to u32::MAX.
        let rs = Arc::new(RuntimeStates::new());
        let id = AccountId::from("a");
        seed(&rs, &id, |rt| {
            rt.in_flight = 0;
            rt.error_count = 1; // keep the entry non-neutral so it isn't GC'd before we can inspect it
        });
        rs.mutate(&id, |rt| {
            rt.in_flight = rt.in_flight.saturating_sub(1);
        });
        assert_eq!(
            peek(&rs, &id).map(|rt| rt.in_flight),
            Some(0),
            "saturating_sub clamps at 0 instead of underflowing"
        );
    }

    #[test]
    fn select_and_acquire_reserves_before_the_next_pick() {
        struct ObserveInFlight(std::sync::atomic::AtomicUsize);

        impl Selector for ObserveInFlight {
            fn pick(
                &self,
                candidates: &[AccountSnapshot],
                _ctx: &SelectionCtx,
            ) -> Option<AccountId> {
                match self.0.fetch_add(1, Ordering::SeqCst) {
                    0 => {
                        assert!(candidates.iter().all(|candidate| candidate.in_flight == 0));
                        Some(AccountId::from("a"))
                    }
                    1 => {
                        let first = candidates
                            .iter()
                            .find(|candidate| candidate.id.as_str() == "a")
                            .expect("the first account remains a candidate");
                        assert_eq!(
                            first.in_flight, 1,
                            "the second selection must observe the first request reservation"
                        );
                        assert_eq!(
                            first.last_selected_at,
                            Some(1_000),
                            "reservation pressure must not be encoded by distorting the stored \
                             selection timestamp"
                        );
                        Some(AccountId::from("b"))
                    }
                    call => panic!("unexpected selector call {call}"),
                }
            }

            fn name(&self) -> &'static str {
                "observe_in_flight"
            }
        }

        let runtime = Arc::new(RuntimeStates::new());
        let metrics = LeaseMetrics::new();
        let selector = ObserveInFlight(std::sync::atomic::AtomicUsize::new(0));
        let ctx = SelectionCtx {
            now: 1_000,
            inflight_penalty_pct: 2.5,
            ..SelectionCtx::default()
        };
        let mut guards = Vec::new();
        let mut picks = Vec::new();

        for _ in 0..2 {
            let mut snapshots = vec![snap("a"), snap("b")];
            let (id, guard) = runtime
                .select_and_acquire(&mut snapshots, &selector, &ctx, 1_000, &metrics)
                .expect("an account remains eligible");
            picks.push(id.to_string());
            guards.push(guard);
        }

        assert_eq!(
            picks,
            ["a", "b"],
            "each reservation must be visible before the next selector runs"
        );
        assert_eq!(metrics.acquired(), 2);
        drop(guards);
        assert_eq!(metrics.released(), 2);
    }

    #[test]
    fn select_and_acquire_open_ws_reserves_before_the_next_pick() {
        struct ObserveOpenWs(std::sync::atomic::AtomicUsize);

        impl Selector for ObserveOpenWs {
            fn pick(
                &self,
                candidates: &[AccountSnapshot],
                _ctx: &SelectionCtx,
            ) -> Option<AccountId> {
                match self.0.fetch_add(1, Ordering::SeqCst) {
                    0 => {
                        assert!(candidates.iter().all(|candidate| candidate.open_ws == 0));
                        Some(AccountId::from("A"))
                    }
                    1 => {
                        let first = candidates
                            .iter()
                            .find(|candidate| candidate.id.as_str() == "A")
                            .expect("the first account remains a candidate");
                        assert_eq!(
                            first.open_ws, 1,
                            "the second selection must observe the first socket reservation"
                        );
                        assert_eq!(
                            first.last_selected_at,
                            Some(1_000),
                            "socket pressure must not be encoded by distorting the stored selection \
                             timestamp"
                        );
                        Some(AccountId::from("B"))
                    }
                    call => panic!("unexpected selector call {call}"),
                }
            }

            fn name(&self) -> &'static str {
                "observe_open_ws"
            }
        }

        let runtime = Arc::new(RuntimeStates::default());
        let selector = ObserveOpenWs(std::sync::atomic::AtomicUsize::new(0));
        let ctx = SelectionCtx {
            now: 1_000,
            inflight_penalty_pct: 2.5,
            ..SelectionCtx::default()
        };
        let mut snapshots = vec![AccountSnapshot::new("A"), AccountSnapshot::new("B")];

        let (first, first_guard) = runtime
            .select_and_acquire_open_ws(&mut snapshots, &selector, &ctx, 1_000)
            .expect("first socket owner");
        let (second, _second_guard) = runtime
            .select_and_acquire_open_ws(&mut snapshots, &selector, &ctx, 1_000)
            .expect("second socket owner");

        assert_eq!(first, AccountId::from("A"));
        assert_eq!(second, AccountId::from("B"));
        drop(first_guard);
    }

    #[test]
    fn select_and_acquire_skips_an_account_at_its_hard_limit() {
        struct FirstCandidate;

        impl Selector for FirstCandidate {
            fn pick(
                &self,
                candidates: &[AccountSnapshot],
                _ctx: &SelectionCtx,
            ) -> Option<AccountId> {
                candidates.first().map(|candidate| candidate.id.clone())
            }

            fn name(&self) -> &'static str {
                "first_candidate"
            }
        }

        let runtime = Arc::new(RuntimeStates::new());
        let metrics = LeaseMetrics::new();
        let account_a = AccountId::from("a");
        let mut held = Vec::new();
        for _ in 0..4 {
            held.push(runtime.acquire_in_flight(&account_a, 1_000, &metrics));
        }
        let mut snapshots = vec![snap("a"), snap("b")];

        let (picked, _guard) = runtime
            .select_and_acquire(
                &mut snapshots,
                &FirstCandidate,
                &SelectionCtx::default(),
                1_000,
                &metrics,
            )
            .expect("the unsaturated account remains available");

        assert_eq!(picked, AccountId::from("b"));
        drop(held);
    }

    #[test]
    fn select_and_acquire_rejects_when_the_global_hard_limit_is_full() {
        struct FirstCandidate;

        impl Selector for FirstCandidate {
            fn pick(
                &self,
                candidates: &[AccountSnapshot],
                _ctx: &SelectionCtx,
            ) -> Option<AccountId> {
                candidates.first().map(|candidate| candidate.id.clone())
            }

            fn name(&self) -> &'static str {
                "first_candidate"
            }
        }

        let runtime = Arc::new(RuntimeStates::new());
        let metrics = LeaseMetrics::new();
        let mut held = Vec::new();
        for index in 0..256 {
            held.push(runtime.acquire_in_flight(
                &AccountId::from(format!("busy-{index}")),
                1_000,
                &metrics,
            ));
        }
        let mut snapshots = vec![snap("otherwise-idle")];

        assert!(
            runtime
                .select_and_acquire(
                    &mut snapshots,
                    &FirstCandidate,
                    &SelectionCtx::default(),
                    1_000,
                    &metrics,
                )
                .is_none(),
            "global saturation must reject before invoking an upstream account"
        );
        drop(held);
    }

    #[test]
    fn select_and_acquire_open_ws_skips_an_account_at_its_hard_limit() {
        struct FirstCandidate;

        impl Selector for FirstCandidate {
            fn pick(
                &self,
                candidates: &[AccountSnapshot],
                _ctx: &SelectionCtx,
            ) -> Option<AccountId> {
                candidates.first().map(|candidate| candidate.id.clone())
            }

            fn name(&self) -> &'static str {
                "first_candidate"
            }
        }

        let runtime = Arc::new(RuntimeStates::new());
        let account_a = AccountId::from("a");
        let mut held = Vec::new();
        for _ in 0..8 {
            held.push(runtime.acquire_open_ws(&account_a));
        }
        let mut snapshots = vec![snap("a"), snap("b")];

        let (picked, _guard) = runtime
            .select_and_acquire_open_ws(
                &mut snapshots,
                &FirstCandidate,
                &SelectionCtx::default(),
                1_000,
            )
            .expect("the unsaturated websocket account remains available");

        assert_eq!(picked, AccountId::from("b"));
        drop(held);
    }

    #[tokio::test]
    async fn pinned_in_flight_waits_for_and_reacquires_only_its_owner() {
        let runtime = Arc::new(RuntimeStates::with_admission_limits(AdmissionLimits {
            global_in_flight: 8,
            account_in_flight: 1,
            wait_timeout: Duration::from_secs(1),
            ..AdmissionLimits::default()
        }));
        let metrics = LeaseMetrics::new();
        let owner = AccountId::from("owner");
        let held = runtime.acquire_in_flight(&owner, 1_000, &metrics);
        let waiter_runtime = runtime.clone();
        let waiter_metrics = metrics.clone();
        let waiter_owner = owner.clone();
        let waiter = tokio::spawn(async move {
            waiter_runtime
                .acquire_pinned_in_flight(&waiter_owner, 1_001, &waiter_metrics)
                .await
        });

        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "a saturated owner must wait instead of bypassing its cap"
        );
        let waiting = admission_lane(&runtime, "request", "owner");
        assert_eq!(waiting.waiters, 1);
        assert_eq!(waiting.waits, 1);
        drop(held);

        let acquired = waiter
            .await
            .expect("wait task")
            .expect("owner capacity becomes available");
        let after = admission_lane(&runtime, "request", "owner");
        assert_eq!(after.waiters, 0);
        assert_eq!(after.acquired_after_wait, 1);
        assert_eq!(after.timeouts, 0);
        assert_eq!(peek(&runtime, &owner).map(|state| state.in_flight), Some(1));
        drop(acquired);
    }

    #[tokio::test]
    async fn pinned_in_flight_times_out_without_rerouting() {
        let runtime = Arc::new(RuntimeStates::with_admission_limits(AdmissionLimits {
            global_in_flight: 8,
            account_in_flight: 1,
            wait_timeout: Duration::from_millis(10),
            ..AdmissionLimits::default()
        }));
        let metrics = LeaseMetrics::new();
        let owner = AccountId::from("owner");
        let _held = runtime.acquire_in_flight(&owner, 1_000, &metrics);

        assert!(
            runtime
                .acquire_pinned_in_flight(&owner, 1_001, &metrics)
                .await
                .is_none(),
            "bounded owner wait must return capacity exhaustion, never another account"
        );
        let lane = admission_lane(&runtime, "request", "owner");
        assert_eq!(lane.waiters, 0);
        assert_eq!(lane.waits, 1);
        assert_eq!(lane.timeouts, 1);
        assert_eq!(lane.acquired_after_wait, 0);
    }

    #[tokio::test]
    async fn recovery_reserve_cannot_be_consumed_by_ordinary_work() {
        struct FirstCandidate;

        impl Selector for FirstCandidate {
            fn pick(
                &self,
                candidates: &[AccountSnapshot],
                _ctx: &SelectionCtx,
            ) -> Option<AccountId> {
                candidates.first().map(|candidate| candidate.id.clone())
            }

            fn name(&self) -> &'static str {
                "first_candidate"
            }
        }

        let runtime = Arc::new(RuntimeStates::with_admission_limits(AdmissionLimits {
            global_in_flight: 8,
            account_in_flight: 1,
            owner_recovery_reserve: 1,
            wait_timeout: Duration::from_millis(1),
            ..AdmissionLimits::default()
        }));
        let metrics = LeaseMetrics::new();
        let owner = AccountId::from("owner");
        let mut snapshots = vec![snap("owner")];

        assert!(
            runtime
                .select_and_acquire_wait(
                    &mut snapshots,
                    &FirstCandidate,
                    &SelectionCtx::default(),
                    1_000,
                    &metrics,
                )
                .await
                .is_none(),
            "ordinary work must not consume the only reserved slot"
        );
        assert!(
            runtime
                .acquire_pinned_in_flight(&owner, 1_000, &metrics)
                .await
                .is_some(),
            "the pinned owner may consume its reserved slot"
        );
        let new_lane = admission_lane(&runtime, "request", "new");
        assert_eq!(new_lane.waits, 1);
        assert_eq!(new_lane.timeouts, 1);
        let owner_lane = admission_lane(&runtime, "request", "owner");
        assert_eq!(owner_lane.owner_recovery, 1);
    }

    #[test]
    fn weighted_admission_routes_around_pressure_even_below_request_count_cap() {
        struct FirstCandidate;
        impl Selector for FirstCandidate {
            fn pick(
                &self,
                candidates: &[AccountSnapshot],
                _ctx: &SelectionCtx,
            ) -> Option<AccountId> {
                candidates.first().map(|candidate| candidate.id.clone())
            }

            fn name(&self) -> &'static str {
                "first_candidate"
            }
        }

        let runtime = Arc::new(RuntimeStates::with_admission_limits(AdmissionLimits {
            global_in_flight: 32,
            account_in_flight: 8,
            global_in_flight_pressure: 32,
            account_in_flight_pressure: 8,
            owner_recovery_reserve: 1,
            owner_recovery_pressure_reserve: 2,
            ..AdmissionLimits::default()
        }));
        let metrics = LeaseMetrics::new();
        let mut snapshots = vec![snap("a"), snap("b")];
        let ctx = SelectionCtx {
            request_pressure_units: 6,
            ..SelectionCtx::default()
        };

        let (first, first_guard) = runtime
            .select_and_acquire(&mut snapshots, &FirstCandidate, &ctx, 1_000, &metrics)
            .expect("first large request fits");
        assert_eq!(first.as_str(), "a");

        let (second, second_guard) = runtime
            .select_and_acquire(&mut snapshots, &FirstCandidate, &ctx, 1_001, &metrics)
            .expect("second large request routes to another account");
        assert_eq!(
            second.as_str(),
            "b",
            "weighted pressure must prevent dogpiling even though a has only one request"
        );

        let mut overlaid = vec![snap("a"), snap("b")];
        runtime.overlay(&mut overlaid, 1_001);
        assert_eq!(overlaid[0].in_flight, 1);
        assert_eq!(overlaid[0].in_flight_pressure, 6);
        assert_eq!(overlaid[1].in_flight_pressure, 6);

        drop(first_guard);
        drop(second_guard);
        runtime.overlay(&mut overlaid, 1_002);
        assert_eq!(overlaid[0].in_flight_pressure, 0);
        assert_eq!(overlaid[1].in_flight_pressure, 0);
    }

    #[test]
    fn terminal_usage_calibrates_future_pressure_without_unbounded_keys() {
        let runtime = RuntimeStates::new();
        let estimate = 16_384;
        assert_eq!(runtime.request_pressure_units(estimate), 1);

        runtime.record_actual_pressure(estimate, 65_536);

        assert_eq!(
            runtime.request_pressure_units(estimate),
            2,
            "the bounded EWMA must increase future pressure after a four-times underestimate"
        );
        assert_eq!(
            runtime.pressure_calibration.samples.load(Ordering::Relaxed),
            1
        );
    }

    #[tokio::test]
    async fn pinned_owner_can_use_reserved_pressure_that_new_work_cannot_consume() {
        let runtime = Arc::new(RuntimeStates::with_admission_limits(AdmissionLimits {
            global_in_flight: 32,
            account_in_flight: 8,
            global_in_flight_pressure: 32,
            account_in_flight_pressure: 8,
            owner_recovery_reserve: 1,
            owner_recovery_pressure_reserve: 2,
            ..AdmissionLimits::default()
        }));
        let metrics = LeaseMetrics::new();
        let owner = AccountId::from("owner");
        let ordinary = runtime
            .try_acquire_in_flight_weighted(&owner, 1_000, &metrics, 6)
            .expect("ordinary request fills the non-reserved pressure budget");

        assert!(
            runtime
                .try_acquire_in_flight_weighted(&owner, 1_001, &metrics, 1)
                .is_none(),
            "new work must not consume owner-reserved pressure"
        );
        let owner_recovery = runtime
            .acquire_pinned_in_flight_weighted(&owner, 1_001, &metrics, 2)
            .await
            .expect("pinned continuation may use the pressure reserve");
        assert_eq!(
            admission_lane(&runtime, "request", "owner").owner_recovery,
            1
        );

        drop(owner_recovery);
        drop(ordinary);
    }

    #[tokio::test]
    async fn cancelled_owner_wait_releases_the_waiter_gauge() {
        let runtime = Arc::new(RuntimeStates::with_admission_limits(AdmissionLimits {
            global_in_flight: 8,
            account_in_flight: 1,
            wait_timeout: Duration::from_secs(10),
            ..AdmissionLimits::default()
        }));
        let metrics = LeaseMetrics::new();
        let owner = AccountId::from("owner");
        let _held = runtime.acquire_in_flight(&owner, 1_000, &metrics);
        let waiter_runtime = runtime.clone();
        let waiter_metrics = metrics.clone();
        let waiter_owner = owner.clone();
        let waiter = tokio::spawn(async move {
            waiter_runtime
                .acquire_pinned_in_flight(&waiter_owner, 1_001, &waiter_metrics)
                .await
        });

        for _ in 0..10 {
            if admission_lane(&runtime, "request", "owner").waiters == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(admission_lane(&runtime, "request", "owner").waiters, 1);
        waiter.abort();
        let _ = waiter.await;

        let lane = admission_lane(&runtime, "request", "owner");
        assert_eq!(lane.waiters, 0);
        assert_eq!(lane.cancelled, 1);
        assert_eq!(lane.timeouts, 0);
    }

    #[test]
    fn usage_refresh_signals_are_keyed_and_coalesced_while_one_is_outstanding() {
        let runtime = RuntimeStates::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        runtime.register_usage_refresh(tx);
        let id = AccountId::from("account-a");

        for _ in 0..100 {
            runtime.request_usage_refresh(&id);
        }

        assert_eq!(rx.try_recv().unwrap(), id);
        assert!(
            rx.try_recv().is_err(),
            "only one queue item may exist for an account"
        );
        assert_eq!(runtime.usage_refresh_coalesced(), 99);
        assert!(
            runtime.finish_usage_refresh(&AccountId::from("account-a")),
            "signals received during the pass request exactly one follow-up"
        );
        assert!(
            !runtime.finish_usage_refresh(&AccountId::from("account-a")),
            "the clean follow-up releases the keyed single-flight slot"
        );
    }

    #[test]
    fn idle_websocket_counts_as_admission_pressure_until_socket_guard_drops() {
        let runtime = Arc::new(RuntimeStates::new());
        let id = AccountId::from("account-a");
        let guard = runtime.acquire_open_ws(&id);

        let mut snapshots = vec![snap("account-a")];
        runtime.overlay(&mut snapshots, 1_000);
        assert_eq!(
            snapshots[0].in_flight, 0,
            "an idle socket is not an active request"
        );
        assert_eq!(
            snapshots[0].open_ws, 1,
            "an idle upstream socket must be visible to load selection"
        );

        drop(guard);
        let mut after = vec![snap("account-a")];
        runtime.overlay(&mut after, 1_000);
        assert_eq!(after[0].in_flight, 0);
        assert_eq!(after[0].open_ws, 0);
    }
}
