//! The default `capacity_weighted` account selector — a faithful port of codex-lb's `logic.py`
//! scoring (see docs/reference/codex-lb-port-reference.md §Selector algorithm). Pure and
//! deterministic given a seeded RNG: no I/O, no clock reads (time enters via `SelectionCtx::now`,
//! randomness via `SelectionCtx::rng_seed`).

use rand::distr::weighted::WeightedIndex;
use rand::distr::Distribution;
use rand::rngs::StdRng;
use rand::SeedableRng;

use crate::traits::Selector;
use crate::types::{AccountId, AccountSnapshot, SelectionCtx};

/// Secondary-window plan capacity (credits). Source: port reference §Plan capacity.
fn plan_capacity_secondary(plan: &str) -> f64 {
    match plan {
        "free" => 1134.0,
        "plus" | "business" | "team" | "edu" => 7560.0,
        "pro" | "enterprise" => 50400.0,
        "prolite" => 37800.0,
        // Unknown plans fall back to the plus-tier capacity (a safe mid value).
        _ => 7560.0,
    }
}

/// Error backoff = min(300, 30 * 2^(error_count-3)) seconds, for error_count >= 3.
fn error_backoff_secs(error_count: u32) -> i64 {
    let exp = error_count.saturating_sub(3).min(20); // cap the shift to avoid overflow
    let raw = 30i64.saturating_mul(1i64 << exp);
    raw.min(300)
}

/// An eligible candidate: a borrowed snapshot + its post-recovery effective usage.
#[derive(Clone, Copy)]
struct Candidate<'a> {
    snap: &'a AccountSnapshot,
    eff_used: f64,
    eff_secondary_used: f64,
}

impl Candidate<'_> {
    /// remaining_secondary_credits = max(0, capacity * (1 - min(secondary_used%,100)/100)).
    fn remaining_secondary_credits(&self) -> f64 {
        let capacity = self
            .snap
            .capacity_credits
            .unwrap_or_else(|| plan_capacity_secondary(&self.snap.plan_type));
        (capacity * (1.0 - self.eff_secondary_used.min(100.0) / 100.0)).max(0.0)
    }

    /// should_drain if used%>=85 OR secondary%>=90 OR (error_count>=2 within 60s of last error).
    fn should_drain(&self, now: i64) -> bool {
        self.eff_used >= 85.0
            || self.eff_secondary_used >= 90.0
            || (self.snap.error_count >= 2
                && self.snap.last_error_at.is_some_and(|t| now - t <= 60))
    }

    /// Effective health tier: base tier, bumped to at least `draining`(1) when `should_drain`.
    fn effective_tier(&self, now: i64) -> u8 {
        if self.should_drain(now) {
            self.snap.health_tier.max(1)
        } else {
            self.snap.health_tier
        }
    }
}

/// Eligibility hard-filter (port reference step 1). `None` ⇒ skip; `Some(Candidate)` with usage
/// zeroed for auto-recovered rate/quota accounts.
fn eligibility(s: &AccountSnapshot, now: i64) -> Option<Candidate<'_>> {
    match s.status.as_str() {
        // Terminal / operator-held: never eligible.
        "reauth_required" | "deactivated" | "paused" => return None,
        // Rate/quota limited: eligible only once the reset time has passed (usage zeroed).
        "rate_limited" | "quota_exceeded" => match s.reset_at {
            Some(reset) if now >= reset => {
                return Some(Candidate {
                    snap: s,
                    eff_used: 0.0,
                    eff_secondary_used: 0.0,
                });
            }
            _ => return None,
        },
        // active (or any other value) → fall through to the cooldown/backoff gates.
        _ => {}
    }

    // Generic cooldown gate.
    if let Some(cd) = s.cooldown_until {
        if now < cd {
            return None;
        }
    }

    // Error backoff (only once error_count >= 3, measured from the last error time).
    if s.error_count >= 3 {
        if let Some(last) = s.last_error_at {
            if now < last + error_backoff_secs(s.error_count) {
                return None;
            }
        }
    }

    Some(Candidate {
        snap: s,
        eff_used: s.used_percent,
        eff_secondary_used: s.secondary_used_percent,
    })
}

/// Health-tier pooling (step 2): prefer healthy(0), then probing(2), then draining(1) — mirrors
/// codex-lb's `healthy or probing or draining or available`.
fn health_tier_pool<'a>(pool: &[Candidate<'a>], now: i64) -> Vec<Candidate<'a>> {
    for tier in [0u8, 2, 1] {
        let group: Vec<Candidate> = pool
            .iter()
            .copied()
            .filter(|c| c.effective_tier(now) == tier)
            .collect();
        if !group.is_empty() {
            return group;
        }
    }
    pool.to_vec()
}

/// Burn/normal/preserve waterfall (step 3): drain burn_first, then normal, then preserve.
fn policy_waterfall<'a>(pool: &[Candidate<'a>]) -> Vec<Candidate<'a>> {
    for policy in ["burn_first", "normal", "preserve"] {
        let group: Vec<Candidate> = pool
            .iter()
            .copied()
            .filter(|c| c.snap.routing_policy == policy)
            .collect();
        if !group.is_empty() {
            return group;
        }
    }
    pool.to_vec()
}

/// Deterministic tiebreak (all-zero weights): min by
/// `(-remaining_secondary_credits, secondary_used%, primary_used%, last_selected_at, account_id)`.
fn deterministic_min<'a, 'b>(pool: &'a [Candidate<'b>]) -> &'a Candidate<'b> {
    pool.iter()
        .min_by(|a, b| {
            // -remaining ascending == remaining descending.
            b.remaining_secondary_credits()
                .total_cmp(&a.remaining_secondary_credits())
                .then(a.eff_secondary_used.total_cmp(&b.eff_secondary_used))
                .then(a.eff_used.total_cmp(&b.eff_used))
                .then(
                    a.snap
                        .last_selected_at
                        .unwrap_or(0)
                        .cmp(&b.snap.last_selected_at.unwrap_or(0)),
                )
                .then(a.snap.id.as_str().cmp(b.snap.id.as_str()))
        })
        .expect("pool is non-empty")
}

/// Weighted-random pick by remaining secondary credits (step 4). All-zero weights fall back to
/// the deterministic tiebreak. The RNG is seeded from `ctx.rng_seed` when present (parity).
fn weighted_pick(pool: &[Candidate<'_>], ctx: &SelectionCtx) -> Option<AccountId> {
    if pool.is_empty() {
        return None;
    }
    let weights: Vec<f64> = pool
        .iter()
        .map(Candidate::remaining_secondary_credits)
        .collect();

    if weights.iter().all(|w| *w <= 0.0) {
        return Some(deterministic_min(pool).snap.id.clone());
    }

    let dist = match WeightedIndex::new(&weights) {
        Ok(d) => d,
        // Defensive: any weight error (e.g. all-zero slipping through) → deterministic pick.
        Err(_) => return Some(deterministic_min(pool).snap.id.clone()),
    };

    let idx = match ctx.rng_seed {
        Some(seed) => dist.sample(&mut StdRng::seed_from_u64(seed)),
        None => dist.sample(&mut rand::rng()),
    };
    Some(pool[idx].snap.id.clone())
}

/// The default selector: (S3 ordering) continuity-ownership + session-affinity are M3 no-op
/// passthroughs here; TA6 capability pre-filter → eligibility → health-tier → policy waterfall →
/// capacity-weighted pick.
#[derive(Debug, Default, Clone, Copy)]
pub struct CapacityWeighted;

impl Selector for CapacityWeighted {
    fn pick(&self, candidates: &[AccountSnapshot], ctx: &SelectionCtx) -> Option<AccountId> {
        let now = ctx.now;

        // (S3 steps 1–2) continuity-ownership + session-affinity: M3 hard pre-filters; in M2b
        // they are no-op passthroughs (every candidate passes).

        // TA6 capability hard pre-filter (above scoring), then eligibility hard-filter.
        let eligible: Vec<Candidate> = candidates
            .iter()
            .filter(|s| !ctx.require_security_work_authorized || s.security_work_authorized)
            .filter_map(|s| eligibility(s, now))
            .collect();
        if eligible.is_empty() {
            return None;
        }

        // Health-tier pooling, then burn/normal/preserve waterfall.
        let pool = health_tier_pool(&eligible, now);
        let pool = policy_waterfall(&pool);

        // Capacity-weighted random pick (deterministic under a seed).
        weighted_pick(&pool, ctx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::Selector;
    use crate::types::{AccountSnapshot, SelectionCtx};

    fn ctx(now: i64, seed: u64) -> SelectionCtx {
        SelectionCtx {
            now,
            require_security_work_authorized: false,
            rng_seed: Some(seed),
            session_id: None,
        }
    }

    fn snap(id: &str, plan: &str, secondary_used: f64) -> AccountSnapshot {
        let mut s = AccountSnapshot::new(id);
        s.plan_type = plan.to_string();
        s.secondary_used_percent = secondary_used;
        s
    }

    #[test]
    fn skips_terminal_and_paused_accounts() {
        let sel = CapacityWeighted;
        for status in ["reauth_required", "deactivated", "paused"] {
            let mut s = snap("a", "plus", 0.0);
            s.status = status.to_string();
            assert!(
                sel.pick(&[s], &ctx(1000, 1)).is_none(),
                "status {status} must be ineligible"
            );
        }
    }

    #[test]
    fn rate_limited_recovers_only_after_reset() {
        let sel = CapacityWeighted;
        let mut s = snap("a", "plus", 50.0);
        s.status = "rate_limited".to_string();
        s.reset_at = Some(2000);
        assert!(
            sel.pick(&[s.clone()], &ctx(1500, 1)).is_none(),
            "before reset"
        );
        assert_eq!(
            sel.pick(&[s], &ctx(2000, 1)).unwrap().as_str(),
            "a",
            "at reset"
        );
    }

    #[test]
    fn cooldown_blocks_until_expiry() {
        let sel = CapacityWeighted;
        let mut s = snap("a", "plus", 0.0);
        s.cooldown_until = Some(5000);
        assert!(sel.pick(&[s.clone()], &ctx(4999, 1)).is_none());
        assert_eq!(sel.pick(&[s], &ctx(5000, 1)).unwrap().as_str(), "a");
    }

    #[test]
    fn error_backoff_blocks_within_window() {
        let sel = CapacityWeighted;
        let mut s = snap("a", "plus", 0.0);
        s.error_count = 4; // backoff = min(300, 30*2^(4-3)) = 60s
        s.last_error_at = Some(1000);
        assert!(
            sel.pick(&[s.clone()], &ctx(1030, 1)).is_none(),
            "within 60s"
        );
        assert_eq!(
            sel.pick(&[s], &ctx(1061, 1)).unwrap().as_str(),
            "a",
            "past 60s"
        );
    }

    #[test]
    fn ta6_filters_to_authorized_accounts() {
        let sel = CapacityWeighted;
        let a = snap("a", "plus", 0.0);
        let mut b = snap("b", "plus", 0.0);
        b.security_work_authorized = true;
        let c = SelectionCtx {
            now: 0,
            require_security_work_authorized: true,
            rng_seed: Some(1),
            session_id: None,
        };
        assert_eq!(sel.pick(&[a, b], &c).unwrap().as_str(), "b");
    }

    #[test]
    fn ta6_none_authorized_yields_no_account() {
        let sel = CapacityWeighted;
        let a = snap("a", "plus", 0.0);
        let c = SelectionCtx {
            now: 0,
            require_security_work_authorized: true,
            rng_seed: Some(1),
            session_id: None,
        };
        assert!(sel.pick(&[a], &c).is_none());
    }

    #[test]
    fn burn_first_drains_before_normal_and_preserve() {
        let sel = CapacityWeighted;
        let mut burn = snap("burn", "plus", 10.0);
        burn.routing_policy = "burn_first".to_string();
        let normal = snap("normal", "plus", 10.0);
        let mut preserve = snap("preserve", "plus", 10.0);
        preserve.routing_policy = "preserve".to_string();
        // burn_first is the only pool considered when present.
        assert_eq!(
            sel.pick(&[normal, preserve, burn], &ctx(0, 7))
                .unwrap()
                .as_str(),
            "burn"
        );
    }

    #[test]
    fn should_drain_deprioritizes_maxed_account_when_a_healthy_one_exists() {
        let sel = CapacityWeighted;
        let healthy = snap("healthy", "plus", 10.0);
        let maxed = snap("maxed", "plus", 95.0); // secondary% >= 90 → should_drain → tier 1
        for seed in 0..20u64 {
            assert_eq!(
                sel.pick(&[healthy.clone(), maxed.clone()], &ctx(0, seed))
                    .unwrap()
                    .as_str(),
                "healthy"
            );
        }
    }

    #[test]
    fn weighted_pick_is_reproducible_under_a_fixed_seed() {
        let sel = CapacityWeighted;
        let a = snap("a", "plus", 0.0);
        let b = snap("b", "pro", 0.0);
        let first = sel.pick(&[a.clone(), b.clone()], &ctx(0, 42)).unwrap();
        let second = sel.pick(&[a, b], &ctx(0, 42)).unwrap();
        assert_eq!(first, second, "same seed ⇒ same pick");
    }

    #[test]
    fn higher_capacity_account_wins_more_often_across_seeds() {
        let sel = CapacityWeighted;
        let big = snap("big", "pro", 0.0); // capacity 50400
        let small = snap("small", "free", 0.0); // capacity 1134
        let mut big_wins = 0;
        for seed in 0..1000u64 {
            if sel
                .pick(&[big.clone(), small.clone()], &ctx(0, seed))
                .unwrap()
                .as_str()
                == "big"
            {
                big_wins += 1;
            }
        }
        assert!(
            big_wins > 900,
            "expected big to dominate, got {big_wins}/1000"
        );
    }

    #[test]
    fn all_zero_weights_fall_back_to_account_id_tiebreak() {
        let sel = CapacityWeighted;
        // both fully used (secondary 100%) → remaining credits 0 → deterministic min;
        // equal on every key except account_id → lexicographically-smaller "aaa" wins.
        let a = snap("aaa", "plus", 100.0);
        let b = snap("bbb", "plus", 100.0);
        assert_eq!(sel.pick(&[b, a], &ctx(0, 5)).unwrap().as_str(), "aaa");
    }
}
