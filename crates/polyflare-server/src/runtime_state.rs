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
use std::sync::{Arc, RwLock};

use polyflare_core::{AccountId, AccountSnapshot};

/// Base for the exponential error backoff, in milliseconds (`0.2s * 2^(n-1)` — codex-lb
/// `retry.py:51-77`). The eligibility error-backoff gate (`select.rs`) uses the same shape.
const BACKOFF_BASE_MS: i64 = 200;
/// Cap on the exponent so a large `error_count` can't overflow / produce an absurd delay.
const BACKOFF_MAX_SHIFT: u32 = 16;
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
    fn apply_funnel_transition(&mut self, should_drain: bool, now: i64) -> Option<HealthTierTransition> {
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
#[derive(Default)]
pub struct RuntimeStates {
    inner: RwLock<HashMap<AccountId, RuntimeState>>,
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
#[must_use]
pub struct InFlightGuard {
    runtime: Arc<RuntimeStates>,
    id: AccountId,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        // saturating_sub: never underflow even if in_flight was somehow already at 0 for this id
        // (e.g. a bug elsewhere zeroed it, or the entry was never observed above 0) — a stray
        // decrement below zero would be a worse defect than a silently-clamped no-op.
        self.runtime.mutate(&self.id, |rt| {
            rt.in_flight = rt.in_flight.saturating_sub(1);
        });
    }
}

impl RuntimeStates {
    pub fn new() -> Self {
        Self::default()
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
                snap.cooldown_until = rt.cooldown_until.filter(|&cd| now < cd);
                snap.last_selected_at = rt.last_selected_at;
                // B8 Task 2: expose the live soft-drain tier so select.rs's already-built
                // health_tier_pool sees real values instead of the always-0 default. An absent
                // entry leaves the snapshot at its neutral `health_tier: 0` default (the loop
                // above already skips it entirely).
                snap.health_tier = rt.health_tier;
                // C9 Task 1: expose the live in-flight lease count. An absent entry leaves the
                // snapshot at its neutral `in_flight: 0` default (the loop above already skips it).
                snap.in_flight = rt.in_flight;
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
    pub fn record_rate_limit(
        &self,
        id: &AccountId,
        retry_after: Option<i64>,
        now: i64,
    ) -> Option<HealthTierTransition> {
        self.mutate(id, |rt| {
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
            rt.apply_funnel_transition(should_drain, now)
        })
    }

    /// Record a quota-exceeded hit: bench for [`QUOTA_EXCEEDED_COOLDOWN_SECS`] WITHOUT bumping
    /// `error_count` (quota is a capacity signal, not a health error — bumping it would double-
    /// penalize a merely-full account into the drain tier). The later cooldown wins.
    ///
    /// **A6 (failure-code writeback plan, Task 5): deliberately NOT called from the request-failure
    /// path** (`ingress::record_failure`) — this is a considered retirement, not an oversight. The
    /// real upstream wire codes for quota exhaustion are `insufficient_quota` and
    /// `usage_not_included` (verified against `codex-rs`: `codex-api/src/sse/responses.rs:630,634`,
    /// `api_bridge.rs:21-22,112-113`, and the `quota_exceeded_emits_single_error_event` test in
    /// `codex-rs/core/tests/suite/quota_exceeded.rs`). On this codebase's actual wire path neither
    /// code can ever reach `FailureSignal.error_code`:
    /// - `insufficient_quota` arrives ONLY inside a `response.failed` terminal SSE/WS frame — both
    ///   `polyflare_codex::ws::codec::classify` (`FrameClass::Terminal`) and the HTTP-SSE relay
    ///   deliberately reframe that frame as SSE and pass it through to the CLIENT instead of turning
    ///   it into an `ExecError` (see `ws/codec.rs`'s `FrameClass::Terminal` doc and the
    ///   `terminal_response_failed_is_reframed_as_sse_and_passed_through` test) — it never becomes a
    ///   `WatchdogError::Upstream(_)` at all.
    /// - `usage_not_included` CAN arrive as a raw pre-stream HTTP 429, but only under the JSON key
    ///   `error.type` (`codex-rs`'s `UsageErrorBody` has no `code` field — `api_bridge.rs:208-218`);
    ///   `polyflare_codex::executor::extract_error_code` reads ONLY `error.code` (plus a `detail`-
    ///   token fallback), so this shape yields `error_code: None`, by design.
    ///
    /// So there is no reliable (or even any) request-path quota signal to route through today — a
    /// code-keyed branch here would be dead code from day one, worse than this function's current
    /// standing as untriggered-but-reachable infrastructure. The durable `quota_exceeded` status is
    /// instead owned entirely by the `usage_refresh.rs` poller, which derives it from actual
    /// used-percent windows (a strictly more reliable signal than scraping an error code) every
    /// ≤600s. This function is kept (not deleted) as tested, correct building-block infrastructure
    /// that a future architecture change (e.g. the TA6(b) cyber-move, if it ever parses
    /// `response.failed` content) could legitimately wire up.
    pub fn record_quota_exceeded(&self, id: &AccountId, now: i64) {
        self.mutate(id, |rt| {
            let until = now.saturating_add(QUOTA_EXCEEDED_COOLDOWN_SECS);
            rt.cooldown_until = Some(rt.cooldown_until.map_or(until, |c| c.max(until)));
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
    pub fn acquire_in_flight(self: &Arc<Self>, id: &AccountId, now: i64) -> InFlightGuard {
        let _ = now;
        self.mutate(id, |rt| {
            rt.in_flight = rt.in_flight.saturating_add(1);
        });
        InFlightGuard {
            runtime: Arc::clone(self),
            id: id.clone(),
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

    fn snap(id: &str) -> AccountSnapshot {
        AccountSnapshot::new(id)
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
        // No Retry-After ⇒ backoff, but floored to the 30s minimum.
        rs.record_rate_limit(&id, None, 1000);
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
        rs.record_rate_limit(&id, Some(600), 1000); // cooldown until 1600
        rs.record_rate_limit(&id, Some(60), 1010); // shorter → must NOT shorten the bench
        let mut snaps = vec![snap("a")];
        rs.overlay(&mut snaps, 1000);
        assert_eq!(snaps[0].cooldown_until, Some(1600), "later cooldown wins");
        assert_eq!(snaps[0].error_count, 2);
    }

    #[test]
    fn rate_limit_clamps_hostile_retry_after_and_never_overflows() {
        // A huge Retry-After with a near-max `now` must saturate, not panic/overflow.
        let rs = RuntimeStates::new();
        let id = AccountId::from("a");
        rs.record_rate_limit(&id, Some(i64::MAX), i64::MAX - 10);
        let mut snaps = vec![snap("a")];
        rs.overlay(&mut snaps, 1000);
        assert_eq!(
            snaps[0].cooldown_until,
            Some(i64::MAX),
            "saturating_add caps, no overflow"
        );

        // A finite-but-excessive Retry-After (48h) is clamped to the 24h ceiling.
        let rs2 = RuntimeStates::new();
        rs2.record_rate_limit(&id, Some(48 * 3600), 1000);
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
        rs.record_rate_limit(&id, Some(30), 1000); // cooldown until 1030 (error_count → 1)
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
        assert_eq!(snaps[0].health_tier, 1, "known entry: tier copied from runtime state");
        assert_eq!(snaps[1].health_tier, 0, "absent entry: stays at the neutral default");
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
        rs.record_rate_limit(&id, Some(5), 1000); // error_count=1, cooldown clamped to floor
        rs.record_rate_limit(&id, Some(5), 1010); // error_count=2, within 60s ⇒ error-drain
        let mut snaps = vec![snap("a")];
        rs.overlay(&mut snaps, 1010);
        assert_eq!(snaps[0].health_tier, 1, "2 rate-limit hits within 60s ⇒ DRAINING");
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
        assert_eq!(snaps[0].health_tier, 0, "3 successes while PROBING ⇒ HEALTHY");
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
        assert_eq!(snaps[0].health_tier, 2, "2 successes while PROBING ⇒ stays PROBING");
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
            assert_eq!(rt.probe_success_streak, 0, "streak reset on any error while PROBING");
            assert_eq!(rt.health_tier, 2, "single error alone doesn't error-drain");
        });
        // A second error within 60s DOES error-drain ⇒ PROBING -> DRAINING.
        rs.record_transient_error(&id, 1010);
        let mut snaps = vec![snap("a")];
        rs.overlay(&mut snaps, 1010);
        assert_eq!(snaps[0].health_tier, 1, "2nd error within 60s while PROBING ⇒ DRAINING");
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
        assert_eq!(entry.health_tier, 0, "disabled ⇒ forced HEALTHY regardless of usage");
        assert_eq!(entry.drain_entered_at, None, "aux cleared");
        assert_eq!(entry.probe_success_streak, 0, "aux cleared");
        assert_eq!(entry.error_count, 3, "error state is NOT clobbered by the disable path");
        assert_eq!(entry.last_error_at, Some(900), "error state is NOT clobbered");
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
        assert_eq!(snaps[0].health_tier, 2, "frozen ⇒ tier unchanged despite used%=99");
    }

    // --- C9 Task 1: the leak-proof InFlightGuard + in_flight runtime field + overlay ---

    #[test]
    fn acquire_in_flight_increments_and_dropping_guards_decrements_and_gcs() {
        // (a) two acquires ⇒ in_flight 2; dropping one ⇒ 1; dropping the other ⇒ 0 and the entry
        // is fully GC'd from the map (peek == None) since a neutral entry decays out.
        let rs = Arc::new(RuntimeStates::new());
        let id = AccountId::from("a");

        let guard1 = rs.acquire_in_flight(&id, 1000);
        assert_eq!(
            peek(&rs, &id).map(|rt| rt.in_flight),
            Some(1),
            "first acquire ⇒ in_flight 1"
        );

        let guard2 = rs.acquire_in_flight(&id, 1000);
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
    }

    #[test]
    fn overlay_copies_in_flight_and_defaults_absent_entry_to_zero() {
        // (b) overlay copies in_flight for a known entry; an absent entry stays at the neutral 0.
        let rs = Arc::new(RuntimeStates::new());
        let id = AccountId::from("a");
        let _guard = rs.acquire_in_flight(&id, 1000);

        let mut snaps = vec![snap("a"), snap("b")];
        rs.overlay(&mut snaps, 1000);
        assert_eq!(snaps[0].in_flight, 1, "known entry: in_flight copied from runtime state");
        assert_eq!(snaps[1].in_flight, 0, "absent entry: stays at the neutral default");
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
        assert!(rt.is_neutral(), "in_flight back at 0 with nothing else set ⇒ neutral");

        // End-to-end via the map: while a guard is held, the account's entry must survive `mutate`'s
        // neutral-GC (e.g. record_selected on some other bookkeeping) rather than vanish mid-flight.
        let rs = Arc::new(RuntimeStates::new());
        let id = AccountId::from("a");
        let guard = rs.acquire_in_flight(&id, 1000);
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

        let guard_a = rs.acquire_in_flight(&id, 1000);
        let guard_b = rs.acquire_in_flight(&id, 1000);
        assert_eq!(peek(&rs, &id).map(|rt| rt.in_flight), Some(2));

        drop(guard_a);
        assert_eq!(
            peek(&rs, &id).map(|rt| rt.in_flight),
            Some(1),
            "dropping guard_a decrements exactly once, leaving guard_b's lease intact"
        );

        drop(guard_b);
        assert_eq!(peek(&rs, &id), None, "dropping guard_b releases the last lease ⇒ GC'd");
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
}
