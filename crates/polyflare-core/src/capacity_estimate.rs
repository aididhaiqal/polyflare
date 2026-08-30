//! Observed per-account capacity, derived from how fast an account's quota moves per unit of work.
//!
//! # Why this has to be measured rather than read
//!
//! The upstream usage endpoint reports a PERCENTAGE and a reset time — never a limit, a total, or a
//! remaining count (probed live 2026-08-30 across three accounts: `used_percent`,
//! `limit_window_seconds`, `reset_after_seconds`, `reset_at`, and a `credits` object that is inert
//! on a subscription plan). So absolute capacity is genuinely unknowable, and
//! `select::plan_capacity_secondary`'s per-plan table is the only starting point available.
//!
//! That table assumes every account on a plan is the same size. Measured over one working day, five
//! accounts all labelled `pro`, all ~94% cache, all near-identical cost per request, differed by
//! **2.4x** in quota burned per million input tokens — one hit 100% and dropped out of the pool for
//! the week while three sat under 30%. A static table cannot express that.
//!
//! # What is measured
//!
//! Absolute capacity stays unknown; only the RATIO between accounts matters for weighting, and that
//! is observable. Quota consumed per unit of work is inversely proportional to capacity: an account
//! that burns twice the percentage for the same tokens holds half the allowance.
//!
//! # Handling resets
//!
//! `used_percent` falls back to zero when a window rolls. A drop, or a change of `reset_at`, ends
//! the current segment rather than contributing a negative delta — the same rule
//! [`crate::depletion`] applies to its time-based rate. Only within-segment increases count.

use serde::Serialize;

/// One observation: an account's quota reading alongside the cumulative work it had served.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CapacitySample {
    /// Quota consumed for the window, 0-100.
    pub used_percent: f64,
    /// Cumulative work served by this account at this instant, in any consistent unit (input
    /// tokens is the natural one — it is what the upstream meters).
    pub cumulative_work: f64,
    /// The window's reset instant. A change ends the segment even without a visible drop, which
    /// catches a roll that happens to land on the same percentage.
    pub reset_at: Option<i64>,
}

/// Work below which a burn figure is not trusted. One account had served 2 requests since its
/// window rolled; at that sample size `used_percent`'s whole-number quantisation alone spans a
/// 2x error, which would have benched a healthy account on noise.
pub const MIN_WORK_FOR_ESTIMATE: f64 = 2_000_000.0;

/// Quota movement below which the estimate is refused. `used_percent` arrives quantised to whole
/// numbers, so a 1-point move carries up to 100% relative error.
pub const MIN_PERCENT_FOR_ESTIMATE: f64 = 3.0;

/// Bounds on how far an observed capacity may depart from its plan default. A measurement is
/// evidence, not proof: clamping keeps a bad window from benching a healthy account outright or
/// from concentrating the pool onto one that merely looks cheap.
pub const MIN_CAPACITY_FACTOR: f64 = 0.25;
pub const MAX_CAPACITY_FACTOR: f64 = 4.0;

/// Quota percent consumed per unit of work, summed over segments uninterrupted by a window reset.
///
/// `None` when the evidence is too thin to be worth acting on — the caller then keeps the plan
/// default, which is the honest answer rather than a guess.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct ObservedBurn {
    /// Quota percent per unit of work.
    pub percent_per_work: f64,
    /// Quota movement the figure rests on.
    pub observed_percent: f64,
    /// Work the figure rests on.
    pub observed_work: f64,
}

/// Total in-segment quota movement per unit of work across `samples` (chronological).
pub fn observed_burn(samples: &[CapacitySample]) -> Option<ObservedBurn> {
    let mut total_percent = 0.0;
    let mut total_work = 0.0;

    for pair in samples.windows(2) {
        let (prev, next) = (pair[0], pair[1]);

        let window_rolled = match (prev.reset_at, next.reset_at) {
            (Some(a), Some(b)) => a != b,
            _ => false,
        };
        let delta_percent = next.used_percent - prev.used_percent;
        let delta_work = next.cumulative_work - prev.cumulative_work;

        // A roll, a drop, or work running backwards (a counter reset) ends the segment: none of
        // those describe consumption, and folding them in would understate the burn.
        if window_rolled || delta_percent < 0.0 || delta_work < 0.0 {
            continue;
        }
        total_percent += delta_percent;
        total_work += delta_work;
    }

    if total_work < MIN_WORK_FOR_ESTIMATE || total_percent < MIN_PERCENT_FOR_ESTIMATE {
        return None;
    }
    Some(ObservedBurn {
        percent_per_work: total_percent / total_work,
        observed_percent: total_percent,
        observed_work: total_work,
    })
}

/// Capacity implied by `burn`, expressed against a reference burn that maps to `plan_capacity`.
///
/// Burn is inversely proportional to capacity: half the quota per token means twice the allowance.
/// The result is clamped to [`MIN_CAPACITY_FACTOR`]..[`MAX_CAPACITY_FACTOR`] of the plan default.
pub fn capacity_from_burn(burn: f64, reference_burn: f64, plan_capacity: f64) -> Option<f64> {
    if !burn.is_finite() || burn <= 0.0 || !reference_burn.is_finite() || reference_burn <= 0.0 {
        return None;
    }
    if !plan_capacity.is_finite() || plan_capacity <= 0.0 {
        return None;
    }
    let factor = (reference_burn / burn).clamp(MIN_CAPACITY_FACTOR, MAX_CAPACITY_FACTOR);
    Some(plan_capacity * factor)
}

/// The reference burn a pool is scaled against: the MEDIAN observed burn.
///
/// Median rather than mean, and rather than the minimum: one account that briefly looks very cheap
/// (a window rolling mid-measurement, a burst of fully-cached turns) would drag a
/// minimum-referenced pool toward itself and starve every other account. The median moves only when
/// most of the pool moves.
pub fn reference_burn(burns: &[f64]) -> Option<f64> {
    let mut usable: Vec<f64> = burns
        .iter()
        .copied()
        .filter(|b| b.is_finite() && *b > 0.0)
        .collect();
    if usable.is_empty() {
        return None;
    }
    usable.sort_by(f64::total_cmp);
    let mid = usable.len() / 2;
    Some(if usable.len().is_multiple_of(2) {
        (usable[mid - 1] + usable[mid]) / 2.0
    } else {
        usable[mid]
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(used: f64, work: f64, reset: Option<i64>) -> CapacitySample {
        CapacitySample {
            used_percent: used,
            cumulative_work: work,
            reset_at: reset,
        }
    }

    #[test]
    fn burn_is_percent_per_unit_of_work() {
        // 10 percentage points over 5M tokens.
        let s = vec![
            sample(0.0, 0.0, Some(1)),
            sample(5.0, 2_500_000.0, Some(1)),
            sample(10.0, 5_000_000.0, Some(1)),
        ];
        let burn = observed_burn(&s).expect("enough evidence");
        assert!((burn.percent_per_work - 10.0 / 5_000_000.0).abs() < 1e-12);
        assert_eq!(burn.observed_percent, 10.0);
    }

    /// The whole point of segmenting: a window roll drops `used_percent` back toward zero, and
    /// counting that as negative consumption would understate the burn of a busy account.
    #[test]
    fn a_window_roll_never_contributes_negative_consumption() {
        let s = vec![
            sample(90.0, 0.0, Some(1)),
            sample(98.0, 4_000_000.0, Some(1)),
            // roll: percent falls AND reset_at changes
            sample(2.0, 4_500_000.0, Some(2)),
            sample(8.0, 7_000_000.0, Some(2)),
        ];
        let burn = observed_burn(&s).expect("enough evidence");
        // 8 points pre-roll + 6 post-roll = 14; the -96 crossing is discarded, not folded in.
        assert_eq!(burn.observed_percent, 14.0);
        assert!(burn.percent_per_work > 0.0);
    }

    /// A roll that happens to land on the same percentage has no drop to detect — `reset_at` is
    /// what catches it.
    #[test]
    fn a_roll_at_an_identical_percentage_is_still_segmented() {
        let s = vec![
            sample(50.0, 0.0, Some(1)),
            sample(50.0, 9_000_000.0, Some(2)), // rolled, coincidentally equal
            sample(56.0, 12_000_000.0, Some(2)),
        ];
        let burn = observed_burn(&s).expect("enough evidence");
        assert_eq!(
            burn.observed_percent, 6.0,
            "only the post-roll segment may count"
        );
        assert_eq!(burn.observed_work, 3_000_000.0);
    }

    /// Thin evidence must yield nothing rather than a confident wrong number: one account had
    /// served 2 requests since its window rolled, where quantisation alone spans a 2x error.
    #[test]
    fn thin_evidence_is_refused_rather_than_guessed() {
        let too_little_work = vec![sample(0.0, 0.0, Some(1)), sample(9.0, 1_000.0, Some(1))];
        assert!(observed_burn(&too_little_work).is_none());

        let too_little_movement = vec![
            sample(0.0, 0.0, Some(1)),
            sample(1.0, 50_000_000.0, Some(1)),
        ];
        assert!(observed_burn(&too_little_movement).is_none());
    }

    #[test]
    fn capacity_is_inverse_to_burn() {
        let plan = 50_400.0;
        // Burning half the reference => twice the capacity.
        let big = capacity_from_burn(0.05, 0.10, plan).unwrap();
        assert!((big - 100_800.0).abs() < 1e-9);
        // Burning double => half.
        let small = capacity_from_burn(0.20, 0.10, plan).unwrap();
        assert!((small - 25_200.0).abs() < 1e-9);
        // Equal => unchanged.
        let same = capacity_from_burn(0.10, 0.10, plan).unwrap();
        assert!((same - plan).abs() < 1e-9);
    }

    /// A measurement is evidence, not proof. Without clamping, one anomalous window could bench a
    /// healthy account or funnel the whole pool onto one that merely looked cheap.
    #[test]
    fn capacity_is_clamped_against_an_extreme_measurement() {
        let plan = 50_400.0;
        let absurdly_cheap = capacity_from_burn(0.0001, 0.10, plan).unwrap();
        assert!((absurdly_cheap - plan * MAX_CAPACITY_FACTOR).abs() < 1e-9);
        let absurdly_costly = capacity_from_burn(100.0, 0.10, plan).unwrap();
        assert!((absurdly_costly - plan * MIN_CAPACITY_FACTOR).abs() < 1e-9);
    }

    #[test]
    fn degenerate_inputs_yield_no_capacity() {
        assert!(capacity_from_burn(0.0, 0.1, 50_400.0).is_none());
        assert!(capacity_from_burn(0.1, 0.0, 50_400.0).is_none());
        assert!(capacity_from_burn(f64::NAN, 0.1, 50_400.0).is_none());
        assert!(capacity_from_burn(0.1, 0.1, 0.0).is_none());
    }

    /// Median, not minimum: one account that briefly looks very cheap must not drag the reference
    /// down and starve the rest of the pool.
    #[test]
    fn reference_is_the_median_not_the_extreme() {
        let burns = vec![0.001, 0.07, 0.07, 0.10, 0.17];
        assert_eq!(reference_burn(&burns), Some(0.07));
        assert_eq!(reference_burn(&[]), None);
        assert_eq!(reference_burn(&[0.0, -1.0]), None);
    }

    /// End to end on the shape actually measured on 2026-08-30: the account that capped burns
    /// ~2.4x the cheapest, and must come out with materially less capacity than it.
    #[test]
    fn the_measured_pool_spread_produces_ordered_capacities() {
        let plan = 50_400.0;
        let capped_burn = 0.17; // codex_754e4ebf — hit 100%
        let cheap_burn = 0.07; // a79168e7 / 13b35db4 — sat at ~25%
        let reference = reference_burn(&[capped_burn, 0.13, 0.10, cheap_burn, cheap_burn]).unwrap();

        let capped = capacity_from_burn(capped_burn, reference, plan).unwrap();
        let cheap = capacity_from_burn(cheap_burn, reference, plan).unwrap();
        assert!(
            cheap > capped,
            "the account that survived the week must be weighted above the one that capped"
        );
        assert!((cheap / capped - capped_burn / cheap_burn).abs() < 1e-9);
    }
}
