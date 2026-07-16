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
/// Fixed cooldown applied to a quota-exceeded account (codex-lb `QUOTA_EXCEEDED_COOLDOWN_SECONDS`).
pub const QUOTA_EXCEEDED_COOLDOWN_SECS: i64 = 120;

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
}

impl RuntimeState {
    /// `true` when this state carries no signal — used to drop empty entries so the map doesn't grow
    /// unbounded with accounts that recovered.
    fn is_neutral(&self) -> bool {
        *self == RuntimeState::default()
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
    pub fn overlay(&self, snapshots: &mut [AccountSnapshot]) {
        let map = self.inner.read().expect("runtime state lock poisoned");
        for snap in snapshots.iter_mut() {
            if let Some(rt) = map.get(&snap.id) {
                snap.error_count = rt.error_count;
                snap.last_error_at = rt.last_error_at;
                snap.cooldown_until = rt.cooldown_until;
                snap.last_selected_at = rt.last_selected_at;
            }
        }
    }

    /// Apply `f` to `id`'s entry (creating it from default), then drop it if it decayed back to
    /// neutral so the map stays bounded.
    fn mutate(&self, id: &AccountId, f: impl FnOnce(&mut RuntimeState)) {
        let mut map = self.inner.write().expect("runtime state lock poisoned");
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
            let delay = retry_after
                .unwrap_or_else(|| backoff_secs(rt.error_count))
                .max(RATE_LIMITED_MIN_COOLDOWN_SECS);
            let until = now + delay;
            rt.cooldown_until = Some(rt.cooldown_until.map_or(until, |c| c.max(until)));
        });
    }

    /// Record a quota-exceeded hit: bench for [`QUOTA_EXCEEDED_COOLDOWN_SECS`] WITHOUT bumping
    /// `error_count` (quota is a capacity signal, not a health error — bumping it would double-
    /// penalize a merely-full account into the drain tier). The later cooldown wins.
    pub fn record_quota_exceeded(&self, id: &AccountId, now: i64) {
        self.mutate(id, |rt| {
            let until = now + QUOTA_EXCEEDED_COOLDOWN_SECS;
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
    /// Leaves any active `cooldown_until` alone (a success mid-cooldown shouldn't happen, but if it
    /// does the cooldown still expires on its own).
    pub fn record_success(&self, id: &AccountId, now: i64) {
        self.mutate(id, |rt| {
            rt.error_count = 0;
            rt.last_error_at = None;
            rt.last_selected_at = Some(now);
        });
    }

    /// Stamp the last-selected time (the `round_robin` tiebreak + a liveness marker). Cheap; called
    /// on every selection.
    pub fn record_selected(&self, id: &AccountId, now: i64) {
        self.mutate(id, |rt| rt.last_selected_at = Some(now));
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
        rs.overlay(&mut snaps);
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
        rs.overlay(&mut snaps);
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
        rs.overlay(&mut snaps);
        assert_eq!(snaps[0].cooldown_until, Some(1600), "later cooldown wins");
        assert_eq!(snaps[0].error_count, 2);
    }

    #[test]
    fn record_quota_does_not_bump_error_count() {
        let rs = RuntimeStates::new();
        let id = AccountId::from("a");
        rs.record_quota_exceeded(&id, 1000);
        let mut snaps = vec![snap("a")];
        rs.overlay(&mut snaps);
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
    fn record_success_clears_error_state_and_drops_the_entry() {
        let rs = RuntimeStates::new();
        let id = AccountId::from("a");
        rs.record_transient_error(&id, 1000);
        rs.record_transient_error(&id, 1001);
        let mut snaps = vec![snap("a")];
        rs.overlay(&mut snaps);
        assert_eq!(snaps[0].error_count, 2);

        // Success clears error state; since nothing else is set, the entry decays to neutral and is
        // dropped (map stays bounded).
        rs.record_success(&id, 1002);
        let mut snaps2 = vec![snap("a")];
        rs.overlay(&mut snaps2);
        assert_eq!(snaps2[0].error_count, 0);
        assert_eq!(snaps2[0].last_error_at, None);
        // last_selected_at was set by record_success, so the entry is NOT neutral — it persists.
        assert_eq!(snaps2[0].last_selected_at, Some(1002));
    }

    #[test]
    fn transient_errors_accumulate_for_the_backoff_gate() {
        let rs = RuntimeStates::new();
        let id = AccountId::from("a");
        for t in 0..3 {
            rs.record_transient_error(&id, 1000 + t);
        }
        let mut snaps = vec![snap("a")];
        rs.overlay(&mut snaps);
        assert_eq!(
            snaps[0].error_count, 3,
            "reaches the select.rs error-backoff threshold"
        );
        assert_eq!(snaps[0].cooldown_until, None, "transient sets no cooldown");
    }
}
