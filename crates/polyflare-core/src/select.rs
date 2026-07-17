//! The default `capacity_weighted` account selector — a faithful port of codex-lb's `logic.py`
//! scoring (see docs/reference/codex-lb-port-reference.md §Selector algorithm). Pure and
//! deterministic given a seeded RNG: no I/O, no clock reads (time enters via `SelectionCtx::now`,
//! randomness via `SelectionCtx::rng_seed`).

use std::sync::Arc;

use rand::distr::weighted::WeightedIndex;
use rand::distr::Distribution;
use rand::rngs::StdRng;
use rand::SeedableRng;

use crate::traits::Selector;
use crate::types::{AccountId, AccountSnapshot, SelectionCtx, Tier};

/// Plan-type alias map (codex-lb `CAPACITY_PLAN_ALIASES`, logic.py:73-81). Input must already be
/// trimmed + lowercased. `None` ⇒ not an alias (the caller keeps the normalized string as-is).
fn plan_alias(normalized: &str) -> Option<&'static str> {
    match normalized {
        "education" | "k12" => Some("edu"),
        "guest" | "go" | "free_workspace" | "quorum" | "unknown" => Some("free"),
        _ => None,
    }
}

/// Secondary-window plan capacity (credits). Faithful port of codex-lb
/// `_fallback_secondary_capacity_credits` (logic.py:350-356) over `PLAN_CAPACITY_CREDITS_SECONDARY`
/// (usage/__init__.py:31-40): normalize (`trim().to_lowercase()`), apply the alias map, then map to
/// capacity. An empty or unrecognized plan falls back to the `free` tier (1134.0) —
/// `UNKNOWN_PLAN_FALLBACK = "free"` in codex-lb, NOT the plus tier.
fn plan_capacity_secondary(plan: &str) -> f64 {
    let normalized = plan.trim().to_lowercase();
    // resolved = alias(normalized) OR (normalized if non-empty else "free") — logic.py:352.
    let resolved = match plan_alias(&normalized) {
        Some(alias) => alias,
        None if normalized.is_empty() => "free",
        None => normalized.as_str(),
    };
    match resolved {
        "free" => 1134.0,
        "plus" | "business" | "team" | "edu" => 7560.0,
        "pro" | "enterprise" => 50400.0,
        "prolite" => 37800.0,
        // Any still-unrecognized plan → the free tier (codex-lb UNKNOWN_PLAN_FALLBACK), NOT plus.
        _ => 1134.0,
    }
}

/// Error backoff = min(300, 30 * 2^(error_count-3)) seconds, for error_count >= 3.
fn error_backoff_secs(error_count: u32) -> i64 {
    let exp = error_count.saturating_sub(3).min(20); // cap the shift to avoid overflow
    let raw = 30i64.saturating_mul(1i64 << exp);
    raw.min(300)
}

/// An eligible candidate: a borrowed snapshot + its post-recovery *effective* state. The `eff_*`
/// fields carry the values codex-lb's eligibility loop mutates on the account (usage zeroed on
/// reset-recovery, error state cleared on recovery/cooldown-expiry/backoff-expiry) so the
/// downstream health-tier + weighting stages read the recovered state, not the raw snapshot.
#[derive(Clone, Copy)]
struct Candidate<'a> {
    snap: &'a AccountSnapshot,
    eff_used: f64,
    eff_secondary_used: f64,
    eff_error_count: u32,
    eff_last_error_at: Option<i64>,
}

impl Candidate<'_> {
    /// The account's total secondary-window capacity (per-account override, else plan-derived).
    fn capacity(&self) -> f64 {
        self.snap
            .capacity_credits
            .unwrap_or_else(|| plan_capacity_secondary(&self.snap.plan_type))
    }

    /// remaining_secondary_credits = max(0, capacity * (1 - min(secondary_used%,100)/100)).
    fn remaining_secondary_credits(&self) -> f64 {
        (self.capacity() * (1.0 - self.eff_secondary_used.min(100.0) / 100.0)).max(0.0)
    }

    /// How "warm" the account is (highest current usage across windows) — drives `fill_first`
    /// prompt-cache locality (saturate the warmest still-eligible account).
    fn warmth(&self) -> f64 {
        self.eff_used.max(self.eff_secondary_used)
    }

    /// should_drain if used%>=85 OR secondary%>=90 OR (error_count>=2 within 60s of last error).
    /// Reads the *effective* (post-recovery) error state — a recovered rate_limited account whose
    /// `error_count` was zeroed must not be marked draining by its stale count (parity with
    /// codex-lb `evaluate_health_tier`, which runs after the eligibility loop mutates the state).
    /// The 60s window is strict `<` (codex-lb `DRAIN_ERROR_WINDOW_SECONDS`, evaluate_health_tier).
    fn should_drain(&self, now: i64) -> bool {
        self.eff_used >= 85.0
            || self.eff_secondary_used >= 90.0
            || (self.eff_error_count >= 2 && self.eff_last_error_at.is_some_and(|t| now - t < 60))
    }

    /// Effective health tier: base tier, bumped to at least `draining`(1) when `should_drain`.
    /// NOTE (M2b scope): live health-tier tracking is M3, so `health_tier` is 0 for every snapshot
    /// in practice here and `max(1)` faithfully mirrors codex-lb's HEALTHY→DRAINING transition. A
    /// base-`probing`(2) account that `should_drain` is NOT pushed down to `draining`(1) yet
    /// (codex-lb's evaluate_health_tier does `PROBING→DRAINING`); that path only exists once M3
    /// populates non-zero base tiers — documented here so the simplification is intentional.
    fn effective_tier(&self, now: i64) -> u8 {
        if self.should_drain(now) {
            self.snap.health_tier.max(1)
        } else {
            self.snap.health_tier
        }
    }
}

/// Eligibility hard-filter (port reference step 1; faithful to codex-lb `logic.py:437-483`).
/// `None` ⇒ skip. The gates are SEQUENTIAL: reset-recovery only mutates the account's effective
/// state (usage/error), it does NOT admit early — a recovered account still falls through the
/// cooldown and error-backoff gates below (exactly like `logic.py`, where recovery mutates the
/// `state` then control continues to the remaining `if` checks).
///
/// INTENTIONALLY DEFERRED: codex-lb's anti-starvation backoff-fallback (logic.py:485-548) — when
/// the eligible pool is empty but accounts sit in error-backoff, it serves the soonest-to-recover
/// instead of failing. M2b is fail-closed (empty pool ⇒ `pick` returns `None` ⇒ a 503 in the
/// M2b-2 server) which is acceptable for the MVP. Porting the fallback later needs a richer
/// eligibility result than `Option<Candidate>` (e.g. an enum `Eligible | InBackoff { recover_at }
/// | HardBlocked`) so `pick` can pick the earliest-recovering backoff candidate — out of scope here.
fn eligibility(s: &AccountSnapshot, now: i64) -> Option<Candidate<'_>> {
    // Terminal / operator-held: never eligible (logic.py:444-447).
    if matches!(
        s.status.as_str(),
        "reauth_required" | "deactivated" | "paused"
    ) {
        return None;
    }

    // Effective (post-recovery) state, seeded from the raw snapshot; recovery/cooldown/backoff
    // mutate these below, mirroring the field writes codex-lb makes on the live `state`.
    let mut eff_used = s.used_percent;
    let mut eff_secondary_used = s.secondary_used_percent;
    let mut eff_error_count = s.error_count;
    let mut eff_last_error_at = s.last_error_at;

    // rate_limited: recover iff the reset time has passed, else skip. Recovery zeros PRIMARY usage
    // + error_count — but NOT secondary usage (logic.py:448-455).
    if s.status == "rate_limited" {
        match s.reset_at {
            Some(reset) if now >= reset => {
                eff_used = 0.0;
                eff_error_count = 0;
            }
            _ => return None,
        }
    }

    // quota_exceeded: recover iff the reset time has passed, else skip. Recovery zeros PRIMARY +
    // SECONDARY usage — but NOT error_count (logic.py:456-463).
    if s.status == "quota_exceeded" {
        match s.reset_at {
            Some(reset) if now >= reset => {
                eff_used = 0.0;
                eff_secondary_used = 0.0;
            }
            _ => return None,
        }
    }

    // Cooldown gate (logic.py:464-469): if the cooldown has expired, clear it AND the error state
    // (error_count/last_error_at); if it is still active, skip. Applies to recovered accounts too.
    if let Some(cd) = s.cooldown_until {
        if now >= cd {
            eff_error_count = 0;
            eff_last_error_at = None;
        } else {
            return None;
        }
    }

    // Error-backoff gate (logic.py:470-483): only once error_count >= 3, measured from the last
    // error time. While inside the backoff window → skip; once expired → clear the error state so
    // recovery is not penalised by a stale count.
    if eff_error_count >= 3 {
        if let Some(last) = eff_last_error_at {
            if now < last + error_backoff_secs(eff_error_count) {
                return None;
            }
        }
        eff_error_count = 0;
        eff_last_error_at = None;
    }

    Some(Candidate {
        snap: s,
        eff_used,
        eff_secondary_used,
        eff_error_count,
        eff_last_error_at,
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

/// Sample one account from `pool` by `weights` (seeded from `ctx.rng_seed` when present, for
/// parity/determinism). All-zero or invalid weights fall back to the deterministic tiebreak. The
/// weight vector must align 1:1 with `pool`.
fn sample_weighted(
    pool: &[Candidate<'_>],
    weights: &[f64],
    ctx: &SelectionCtx,
) -> Option<AccountId> {
    if pool.is_empty() {
        return None;
    }
    if weights.iter().all(|w| *w <= 0.0) {
        return Some(deterministic_min(pool).snap.id.clone());
    }
    let dist = match WeightedIndex::new(weights) {
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

/// Weighted-random pick by remaining secondary credits (capacity_weighted step 4).
fn weighted_pick(pool: &[Candidate<'_>], ctx: &SelectionCtx) -> Option<AccountId> {
    let weights: Vec<f64> = pool
        .iter()
        .map(Candidate::remaining_secondary_credits)
        .collect();
    sample_weighted(pool, &weights, ctx)
}

/// The shared pre-weighting pipeline for the "pool-first" strategies (capacity_weighted,
/// usage_weighted, round_robin, fill_first, sequential_drain): TA6 capability pre-filter →
/// eligibility hard-filter → health-tier pooling → burn/normal/preserve waterfall. Returns the
/// final candidate pool (empty ⇒ no eligible account). Continuity-ownership + session-affinity are
/// applied by the ingress BEFORE the selector (a hard pre-filter), not here.
fn standard_pool<'a>(candidates: &'a [AccountSnapshot], ctx: &SelectionCtx) -> Vec<Candidate<'a>> {
    let eligible: Vec<Candidate> = candidates
        .iter()
        .filter(|s| !ctx.require_security_work_authorized || s.security_work_authorized)
        .filter_map(|s| eligibility(s, ctx.now))
        .collect();
    if eligible.is_empty() {
        return Vec::new();
    }
    let pool = health_tier_pool(&eligible, ctx.now);
    policy_waterfall(&pool)
}

/// The lexicographic min of `pool` by `key` ascending, then account id ascending (deterministic).
fn deterministic_by<'a, 'b, F>(pool: &'a [Candidate<'b>], key: F) -> Option<AccountId>
where
    F: Fn(&Candidate<'b>) -> f64,
{
    pool.iter()
        .min_by(|a, b| {
            key(a)
                .total_cmp(&key(b))
                .then(a.snap.id.as_str().cmp(b.snap.id.as_str()))
        })
        .map(|c| c.snap.id.clone())
}

/// The default selector: TA6 capability pre-filter → eligibility → health-tier → policy waterfall →
/// capacity-weighted random pick (weighted by remaining weekly credits). Deterministic under a seed.
#[derive(Debug, Default, Clone, Copy)]
pub struct CapacityWeighted;

impl Selector for CapacityWeighted {
    fn pick(&self, candidates: &[AccountSnapshot], ctx: &SelectionCtx) -> Option<AccountId> {
        weighted_pick(&standard_pool(candidates, ctx), ctx)
    }

    fn name(&self) -> &'static str {
        RoutingStrategy::CapacityWeighted.name()
    }
}

/// Deterministic: the least weekly-used eligible account (even utilization; testable). Tiebreak id.
#[derive(Debug, Default, Clone, Copy)]
pub struct UsageWeighted;

impl Selector for UsageWeighted {
    fn pick(&self, candidates: &[AccountSnapshot], ctx: &SelectionCtx) -> Option<AccountId> {
        deterministic_by(&standard_pool(candidates, ctx), |c| c.eff_secondary_used)
    }

    fn name(&self) -> &'static str {
        RoutingStrategy::UsageWeighted.name()
    }
}

/// Deterministic: the least-recently-selected eligible account (round-robin fairness). NOTE:
/// `last_selected_at` is not yet live-tracked (always `None`), so until it is written on selection
/// this degenerates to the id tiebreak — intentional, documented, and correct the moment tracking
/// lands.
#[derive(Debug, Default, Clone, Copy)]
pub struct RoundRobin;

impl Selector for RoundRobin {
    fn pick(&self, candidates: &[AccountSnapshot], ctx: &SelectionCtx) -> Option<AccountId> {
        deterministic_by(&standard_pool(candidates, ctx), |c| {
            c.snap.last_selected_at.unwrap_or(0) as f64
        })
    }

    fn name(&self) -> &'static str {
        RoutingStrategy::RoundRobin.name()
    }
}

/// Deterministic: saturate the WARMEST eligible account (highest current usage) for prompt-cache
/// locality — max `warmth`, tiebreak id.
#[derive(Debug, Default, Clone, Copy)]
pub struct FillFirst;

impl Selector for FillFirst {
    fn pick(&self, candidates: &[AccountSnapshot], ctx: &SelectionCtx) -> Option<AccountId> {
        // max warmth == min(-warmth); the shared helper does the id tiebreak.
        deterministic_by(&standard_pool(candidates, ctx), |c| -c.warmth())
    }

    fn name(&self) -> &'static str {
        RoutingStrategy::FillFirst.name()
    }
}

/// Deterministic: the SMALLEST-capacity eligible account first — burn cheap/throwaway accounts
/// before the big ones. Tiebreak id.
#[derive(Debug, Default, Clone, Copy)]
pub struct SequentialDrain;

impl Selector for SequentialDrain {
    fn pick(&self, candidates: &[AccountSnapshot], ctx: &SelectionCtx) -> Option<AccountId> {
        deterministic_by(&standard_pool(candidates, ctx), Candidate::capacity)
    }

    fn name(&self) -> &'static str {
        RoutingStrategy::SequentialDrain.name()
    }
}

/// Above this weekly-used%, a High-tier (opus) turn strongly deprioritizes the account (keep fresh
/// headroom for expensive orchestration).
const HIGH_RESERVE_CEIL: f64 = 70.0;
/// A small floor so a fully-fresh account stays weakly reachable for Low-tier (haiku) packing.
const LOW_FLOOR: f64 = 100.0;

/// Tier-aware routing (Phase 2 of the routing design): eligibility + policy waterfall, then a
/// TIER-STEERED weighted pick. It deliberately SKIPS the health-tier hard pool so Low-tier searchers
/// can reach near-limit accounts — the per-tier weights carry the health/fill preference instead:
/// - **High** (opus orchestrator): weight ≈ remaining credits, strongly deprioritizing near-limit
///   (`secondary_used ≥ HIGH_RESERVE_CEIL`) and draining accounts → fresh/preserved capacity.
/// - **Medium** (sonnet): weight ≈ remaining credits (soft draining penalty) — like capacity_weighted.
/// - **Low** (haiku searcher): weight ≈ CONSUMED credits (+`LOW_FLOOR`) → packs onto near-limit
///   accounts, sparing fresh capacity for expensive turns.
///
/// Absent `ctx.tier` is treated as Medium. Session soft-pin (cache locality for anchor-less first
/// turns) is Phase 3 and not implemented here — anchor ownership already covers resumed turns.
#[derive(Debug, Default, Clone, Copy)]
pub struct CacheAffinityTier;

impl CacheAffinityTier {
    fn tier_weight(c: &Candidate, tier: Tier, now: i64) -> f64 {
        let remaining = c.remaining_secondary_credits();
        let consumed = (c.capacity() - remaining).max(0.0);
        let draining = c.should_drain(now);
        let w = match tier {
            Tier::High => {
                let base = if c.eff_secondary_used < HIGH_RESERVE_CEIL {
                    remaining
                } else {
                    remaining * 0.05
                };
                if draining {
                    base * 0.05
                } else {
                    base
                }
            }
            Tier::Medium => {
                if draining {
                    remaining * 0.1
                } else {
                    remaining
                }
            }
            Tier::Low => consumed + LOW_FLOOR,
        };
        w.max(0.0)
    }
}

impl Selector for CacheAffinityTier {
    fn pick(&self, candidates: &[AccountSnapshot], ctx: &SelectionCtx) -> Option<AccountId> {
        // Eligibility + policy waterfall (respect burn/preserve). Skip the health-tier hard pool so
        // Low tier can pack onto near-limit accounts — weights carry the health preference.
        let eligible: Vec<Candidate> = candidates
            .iter()
            .filter(|s| !ctx.require_security_work_authorized || s.security_work_authorized)
            .filter_map(|s| eligibility(s, ctx.now))
            .collect();
        if eligible.is_empty() {
            return None;
        }
        let pool = policy_waterfall(&eligible);
        let tier = ctx.tier.unwrap_or(Tier::Medium);
        let weights: Vec<f64> = pool
            .iter()
            .map(|c| Self::tier_weight(c, tier, ctx.now))
            .collect();
        sample_weighted(&pool, &weights, ctx)
    }

    fn name(&self) -> &'static str {
        RoutingStrategy::CacheAffinityTier.name()
    }
}

/// The config-selectable routing strategies. `CapacityWeighted` is the default. Each maps to a
/// stateless `Selector` behind the existing trait seam — no per-strategy state, so building one is
/// cheap (every selector is a ZST).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum RoutingStrategy {
    #[default]
    CapacityWeighted,
    UsageWeighted,
    RoundRobin,
    FillFirst,
    SequentialDrain,
    CacheAffinityTier,
}

impl RoutingStrategy {
    /// Parse a snake_case config string; `None` ⇒ unrecognized.
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_lowercase().as_str() {
            "capacity_weighted" => Some(Self::CapacityWeighted),
            "usage_weighted" => Some(Self::UsageWeighted),
            "round_robin" => Some(Self::RoundRobin),
            "fill_first" => Some(Self::FillFirst),
            "sequential_drain" => Some(Self::SequentialDrain),
            "cache_affinity_tier" => Some(Self::CacheAffinityTier),
            _ => None,
        }
    }

    /// The canonical snake_case name.
    pub fn name(&self) -> &'static str {
        match self {
            Self::CapacityWeighted => "capacity_weighted",
            Self::UsageWeighted => "usage_weighted",
            Self::RoundRobin => "round_robin",
            Self::FillFirst => "fill_first",
            Self::SequentialDrain => "sequential_drain",
            Self::CacheAffinityTier => "cache_affinity_tier",
        }
    }

    /// Every strategy's canonical name (for config help / the dashboard).
    pub fn all() -> [RoutingStrategy; 6] {
        [
            Self::CapacityWeighted,
            Self::UsageWeighted,
            Self::RoundRobin,
            Self::FillFirst,
            Self::SequentialDrain,
            Self::CacheAffinityTier,
        ]
    }

    /// Build the selector for this strategy (cheap — every selector is a ZST).
    pub fn selector(&self) -> Arc<dyn Selector> {
        match self {
            Self::CapacityWeighted => Arc::new(CapacityWeighted),
            Self::UsageWeighted => Arc::new(UsageWeighted),
            Self::RoundRobin => Arc::new(RoundRobin),
            Self::FillFirst => Arc::new(FillFirst),
            Self::SequentialDrain => Arc::new(SequentialDrain),
            Self::CacheAffinityTier => Arc::new(CacheAffinityTier),
        }
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
            tier: None,
        }
    }

    fn ctx_tier(now: i64, seed: u64, tier: Tier) -> SelectionCtx {
        SelectionCtx {
            tier: Some(tier),
            ..ctx(now, seed)
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
            tier: None,
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
            tier: None,
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

    #[test]
    fn recovered_account_still_blocked_by_active_cooldown() {
        // Faithfulness regression (logic.py:448-469): a rate_limited account whose reset has
        // passed auto-recovers, but recovery does NOT admit it early — it still falls through the
        // cooldown gate. With a cooldown that is still active, it must NOT be selected.
        let sel = CapacityWeighted;
        let mut s = snap("a", "plus", 50.0);
        s.status = "rate_limited".to_string();
        s.reset_at = Some(1000); // reset has passed at now=1000 → recovers
        s.cooldown_until = Some(2000); // …but cooldown is still active
        assert!(
            sel.pick(&[s.clone()], &ctx(1000, 1)).is_none(),
            "recovered but cooldown still active ⇒ still gated"
        );
        // Once the cooldown also expires, the recovered account becomes selectable.
        assert_eq!(
            sel.pick(&[s], &ctx(2000, 1)).unwrap().as_str(),
            "a",
            "recovered + cooldown expired ⇒ eligible"
        );
    }

    #[test]
    fn rate_limit_recovery_zeroes_error_count_clearing_backoff_and_drain() {
        // Faithfulness regression (logic.py:450-452): rate_limited recovery zeroes error_count.
        // With the recovery now falling through the backoff gate, that zeroing is what keeps the
        // account eligible (an un-zeroed error_count=5 would trip the backoff) AND stops the stale
        // count from marking it draining.
        let sel = CapacityWeighted;
        let mut recovered = snap("recovered", "plus", 10.0);
        recovered.status = "rate_limited".to_string();
        recovered.reset_at = Some(1000);
        recovered.error_count = 5; // un-zeroed ⇒ backoff = min(300, 30*2^2) = 120s at now=1000
        recovered.last_error_at = Some(1000); // …and within 60s ⇒ would mark draining too

        // (a) Eligible on its own: error_count zeroed ⇒ not held by the backoff gate.
        assert_eq!(
            sel.pick(&[recovered.clone()], &ctx(1000, 1))
                .unwrap()
                .as_str(),
            "recovered",
            "recovery zeroes error_count ⇒ not backoff-held"
        );

        // (b) Not draining: paired with a healthy peer it stays in the same (healthy) tier, so it
        // still wins on some seeds. A stale error_count would have dropped it to `draining`, which
        // would be excluded whenever a healthy account exists ⇒ it could never win.
        let healthy = snap("healthy", "plus", 10.0);
        let recovered_wins = (0..50u64)
            .filter(|&seed| {
                sel.pick(&[recovered.clone(), healthy.clone()], &ctx(1000, seed))
                    .unwrap()
                    .as_str()
                    == "recovered"
            })
            .count();
        assert!(
            recovered_wins > 0,
            "recovered account must share the healthy tier (not draining), won {recovered_wins}/50"
        );
    }

    #[test]
    fn probing_tier_preferred_over_draining_tier() {
        // Health-tier ordering (logic.py:598-601): with no healthy accounts, probing(2) is
        // preferred over draining(1). Deterministic across seeds — no weighting involved.
        let sel = CapacityWeighted;
        let mut probing = snap("probing", "plus", 10.0);
        probing.health_tier = 2;
        let mut draining = snap("draining", "plus", 10.0);
        draining.health_tier = 1;
        for seed in 0..20u64 {
            assert_eq!(
                sel.pick(&[draining.clone(), probing.clone()], &ctx(0, seed))
                    .unwrap()
                    .as_str(),
                "probing",
                "probing(2) must outrank draining(1)"
            );
        }
    }

    #[test]
    fn plan_capacity_normalizes_case_and_whitespace() {
        // codex-lb normalizes `plan.strip().lower()` before lookup.
        assert_eq!(plan_capacity_secondary("  Pro  "), 50400.0);
        assert_eq!(plan_capacity_secondary("PLUS"), 7560.0);
        assert_eq!(plan_capacity_secondary("Free"), 1134.0);
    }

    #[test]
    fn plan_capacity_applies_aliases() {
        // CAPACITY_PLAN_ALIASES (logic.py:73-81).
        assert_eq!(plan_capacity_secondary("guest"), 1134.0); // → free
        assert_eq!(plan_capacity_secondary("go"), 1134.0); // → free
        assert_eq!(plan_capacity_secondary("free_workspace"), 1134.0); // → free
        assert_eq!(plan_capacity_secondary("quorum"), 1134.0); // → free
        assert_eq!(plan_capacity_secondary("unknown"), 1134.0); // → free
        assert_eq!(plan_capacity_secondary("education"), 7560.0); // → edu
        assert_eq!(plan_capacity_secondary("K12"), 7560.0); // → edu (case-normalized)
    }

    #[test]
    fn plan_capacity_unknown_and_empty_default_to_free() {
        // UNKNOWN_PLAN_FALLBACK = "free" (1134), NOT the plus tier.
        assert_eq!(plan_capacity_secondary("banana"), 1134.0);
        assert_eq!(plan_capacity_secondary(""), 1134.0);
        assert_eq!(plan_capacity_secondary("   "), 1134.0);
    }

    // ---- Strategy factory ----

    #[test]
    fn routing_strategy_parses_names_round_trip_and_defaults() {
        for s in RoutingStrategy::all() {
            assert_eq!(RoutingStrategy::parse(s.name()), Some(s));
        }
        assert_eq!(
            RoutingStrategy::parse("  Capacity_Weighted "),
            Some(RoutingStrategy::CapacityWeighted)
        );
        assert_eq!(RoutingStrategy::parse("bogus"), None);
        assert_eq!(
            RoutingStrategy::default(),
            RoutingStrategy::CapacityWeighted
        );
    }

    // ---- Deterministic ported strategies ----

    #[test]
    fn usage_weighted_picks_least_weekly_used() {
        let low = snap("low", "pro", 20.0);
        let high = snap("high", "pro", 70.0);
        assert_eq!(
            UsageWeighted
                .pick(&[high, low], &ctx(0, 1))
                .unwrap()
                .as_str(),
            "low"
        );
    }

    #[test]
    fn fill_first_saturates_the_warmest_eligible_account() {
        // "warm" but still eligible (secondary 80% < the 90% drain line) beats a fresh one.
        let warm = snap("warm", "pro", 80.0);
        let fresh = snap("fresh", "pro", 5.0);
        assert_eq!(
            FillFirst.pick(&[fresh, warm], &ctx(0, 1)).unwrap().as_str(),
            "warm"
        );
    }

    #[test]
    fn sequential_drain_burns_the_smallest_capacity_first() {
        let big = snap("big", "pro", 0.0); // capacity 50400
        let small = snap("small", "free", 0.0); // capacity 1134
        assert_eq!(
            SequentialDrain
                .pick(&[big, small], &ctx(0, 1))
                .unwrap()
                .as_str(),
            "small"
        );
    }

    // ---- Tier-aware (cache_affinity_tier) ----

    #[test]
    fn low_tier_packs_onto_the_near_limit_account() {
        // A haiku searcher (Low) should pack onto the busy account, sparing the fresh one — the
        // OPPOSITE of capacity_weighted, which favors the account with more headroom.
        let busy = snap("busy", "pro", 75.0); // lots consumed
        let fresh = snap("fresh", "pro", 5.0); // lots remaining
        let mut busy_wins = 0;
        for seed in 0..400u64 {
            if CacheAffinityTier
                .pick(
                    &[busy.clone(), fresh.clone()],
                    &ctx_tier(0, seed, Tier::Low),
                )
                .unwrap()
                .as_str()
                == "busy"
            {
                busy_wins += 1;
            }
        }
        assert!(
            busy_wins > 300,
            "Low tier should pack onto the busy account, got {busy_wins}/400"
        );
    }

    #[test]
    fn high_tier_prefers_fresh_capacity_over_near_limit() {
        // An opus orchestrator (High) should land on the fresh account, avoiding the near-limit one
        // (secondary 80% > HIGH_RESERVE_CEIL 70 ⇒ 0.05x weight).
        let near_limit = snap("near", "pro", 80.0);
        let fresh = snap("fresh", "pro", 10.0);
        let mut fresh_wins = 0;
        for seed in 0..400u64 {
            if CacheAffinityTier
                .pick(
                    &[near_limit.clone(), fresh.clone()],
                    &ctx_tier(0, seed, Tier::High),
                )
                .unwrap()
                .as_str()
                == "fresh"
            {
                fresh_wins += 1;
            }
        }
        assert!(
            fresh_wins > 350,
            "High tier should prefer fresh capacity, got {fresh_wins}/400"
        );
    }

    #[test]
    fn cache_affinity_absent_tier_behaves_like_medium_capacity_weighting() {
        // No tier ⇒ Medium ⇒ weight ~ remaining credits, so the higher-headroom account dominates
        // (same shape as capacity_weighted).
        let big = snap("big", "pro", 0.0);
        let small = snap("small", "free", 0.0);
        let mut big_wins = 0;
        for seed in 0..500u64 {
            if CacheAffinityTier
                .pick(&[big.clone(), small.clone()], &ctx(0, seed))
                .unwrap()
                .as_str()
                == "big"
            {
                big_wins += 1;
            }
        }
        assert!(
            big_wins > 450,
            "absent tier ⇒ medium ⇒ headroom-weighted, got {big_wins}/500"
        );
    }

    #[test]
    fn all_strategies_respect_eligibility_and_empty_pool() {
        // A paused account is ineligible under every strategy → no pick.
        let mut paused = snap("p", "pro", 0.0);
        paused.status = "paused".to_string();
        for strat in RoutingStrategy::all() {
            assert!(
                strat
                    .selector()
                    .pick(&[paused.clone()], &ctx(0, 1))
                    .is_none(),
                "{} must skip a paused account",
                strat.name()
            );
        }
    }
}
