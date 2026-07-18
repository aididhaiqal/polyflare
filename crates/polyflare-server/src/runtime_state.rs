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
use std::sync::RwLock;

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
}

impl RuntimeState {
    /// `true` when this state carries no signal — used to drop empty entries so the map doesn't grow
    /// unbounded with accounts that recovered.
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
            }
        }
    }

    /// Apply `f` to `id`'s entry (creating it from default), then drop it if it decayed back to
    /// neutral so the map stays bounded.
    fn mutate(&self, id: &AccountId, f: impl FnOnce(&mut RuntimeState)) {
        let mut map = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let entry = map.entry(id.clone()).or_default();
        f(entry);
        if entry.is_neutral() {
            map.remove(id);
        }
    }

    /// Record a rate-limit (429) hit: bump the error count, stamp the error time, and bench the
    /// account until `now + delay` (floored at [`RATE_LIMITED_MIN_COOLDOWN_SECS`]). `retry_after` is
    /// the upstream `Retry-After`, if any; absent ⇒ exponential backoff on the new error count. The
    /// LATER of an existing cooldown and the new one wins (never shorten a bench).
    pub fn record_rate_limit(&self, id: &AccountId, retry_after: Option<i64>, now: i64) {
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
        });
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
    pub fn record_transient_error(&self, id: &AccountId, now: i64) {
        self.mutate(id, |rt| {
            rt.error_count = rt.error_count.saturating_add(1);
            rt.last_error_at = Some(now);
        });
    }

    /// Record a successful completion: clear the error state (error_count → 0, last_error_at → None)
    /// — MANDATORY, or errors accumulate forever and every account eventually looks unhealthy.
    /// Leaves `cooldown_until` alone (it expires on its own) and does NOT touch `last_selected_at`:
    /// that marker is owned by [`record_selected`] (stamped at SELECTION), so an error-only entry
    /// that succeeds decays back to neutral here and is GC'd — keeping the map bounded.
    pub fn record_success(&self, id: &AccountId) {
        self.mutate(id, |rt| {
            rt.error_count = 0;
            rt.last_error_at = None;
        });
    }

    /// Stamp the last-selected time (the `round_robin` tiebreak + a liveness marker). Cheap; called
    /// on every selection.
    pub fn record_selected(&self, id: &AccountId, now: i64) {
        self.mutate(id, |rt| rt.last_selected_at = Some(now));
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
}
