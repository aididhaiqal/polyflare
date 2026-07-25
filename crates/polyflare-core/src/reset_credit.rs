//! Pure reset-credit fleet planning. No I/O and no secrets.

#[derive(Debug, Clone)]
pub struct ResetCreditCandidateInput {
    pub account_id: String,
    pub capacity_credits: f64,
    pub weekly_used_percent: f64,
    pub weekly_reset_at: Option<i64>,
    pub available_credits: i64,
    pub earliest_credit_expires_at: Option<i64>,
    pub snapshot_fetched_at: i64,
    pub eligible: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResetCreditRecommendation {
    RedeemNow,
    RedeemBeforeExpiry,
    Hold,
    WaitForNaturalReset,
    LowBenefit,
    NoCredit,
    Unavailable,
}

#[derive(Debug, Clone)]
pub struct ResetCreditCandidate {
    pub account_id: String,
    pub recommendation: ResetCreditRecommendation,
    pub reason: &'static str,
    pub recoverable_credits: f64,
    pub time_weighted_value: f64,
}

pub fn optimize_reset_credits(
    inputs: &[ResetCreditCandidateInput],
    now: i64,
    stale_after_seconds: i64,
) -> Vec<ResetCreditCandidate> {
    let stale_after_seconds = stale_after_seconds.max(1);
    let mut rows: Vec<_> = inputs
        .iter()
        .map(|input| {
            let used = input.weekly_used_percent.clamp(0.0, 100.0);
            let recoverable = if input.capacity_credits.is_finite() {
                input.capacity_credits.max(0.0) * used / 100.0
            } else {
                0.0
            };
            let reset_in = input.weekly_reset_at.map(|reset| reset - now);
            let is_stale = input.snapshot_fetched_at <= 0
                || now.saturating_sub(input.snapshot_fetched_at) > stale_after_seconds;
            let missing_evidence = !input.capacity_credits.is_finite()
                || input.capacity_credits <= 0.0
                || input.weekly_reset_at.is_none();

            let (recommendation, reason) = if !input.eligible || is_stale || missing_evidence {
                (
                    ResetCreditRecommendation::Unavailable,
                    "Fresh eligible account, quota, and credit evidence is required",
                )
            } else if input.available_credits <= 0 {
                (
                    ResetCreditRecommendation::NoCredit,
                    "No banked reset credit is currently available",
                )
            } else if reset_in.is_some_and(|seconds| seconds <= 3_600) {
                (
                    ResetCreditRecommendation::WaitForNaturalReset,
                    "The weekly limit resets naturally within one hour",
                )
            } else if recoverable <= input.capacity_credits * 0.05 {
                (
                    ResetCreditRecommendation::LowBenefit,
                    "Less than five percent of weekly capacity would be recovered",
                )
            } else if input
                .earliest_credit_expires_at
                .is_some_and(|expiry| expiry <= now + 300)
            {
                (
                    ResetCreditRecommendation::RedeemNow,
                    "The earliest credit expires within five minutes",
                )
            } else if matches!(
                (input.earliest_credit_expires_at, input.weekly_reset_at),
                (Some(expiry), Some(reset)) if expiry < reset
            ) {
                (
                    ResetCreditRecommendation::RedeemBeforeExpiry,
                    "The earliest credit expires before the natural weekly reset",
                )
            } else if used >= 80.0 && reset_in.is_some_and(|seconds| seconds >= 6 * 3_600) {
                (
                    ResetCreditRecommendation::RedeemNow,
                    "High weekly usage with meaningful time remaining before reset",
                )
            } else {
                (
                    ResetCreditRecommendation::Hold,
                    "Keep the credit until demand or expiry makes redemption worthwhile",
                )
            };

            let time_fraction = reset_in.unwrap_or_default().clamp(0, 7 * 24 * 3_600) as f64
                / (7 * 24 * 3_600) as f64;
            let time_weighted_value = if recommendation == ResetCreditRecommendation::Unavailable {
                0.0
            } else {
                recoverable * time_fraction
            };

            ResetCreditCandidate {
                account_id: input.account_id.clone(),
                recommendation,
                reason,
                recoverable_credits: recoverable,
                time_weighted_value,
            }
        })
        .collect();

    rows.sort_by(|left, right| {
        recommendation_rank(left.recommendation)
            .cmp(&recommendation_rank(right.recommendation))
            .then_with(|| {
                right
                    .time_weighted_value
                    .total_cmp(&left.time_weighted_value)
            })
            .then_with(|| left.account_id.cmp(&right.account_id))
    });
    rows
}

fn recommendation_rank(value: ResetCreditRecommendation) -> u8 {
    match value {
        ResetCreditRecommendation::RedeemNow
        | ResetCreditRecommendation::RedeemBeforeExpiry
        | ResetCreditRecommendation::Hold => 0,
        ResetCreditRecommendation::LowBenefit => 1,
        ResetCreditRecommendation::WaitForNaturalReset => 2,
        ResetCreditRecommendation::NoCredit => 3,
        ResetCreditRecommendation::Unavailable => 4,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(
        id: &str,
        capacity: f64,
        used: f64,
        reset_in_hours: i64,
        expiry_in_hours: Option<i64>,
    ) -> ResetCreditCandidateInput {
        let now = 1_000_000;
        ResetCreditCandidateInput {
            account_id: id.to_string(),
            capacity_credits: capacity,
            weekly_used_percent: used,
            weekly_reset_at: Some(now + reset_in_hours * 3_600),
            available_credits: 1,
            earliest_credit_expires_at: expiry_in_hours.map(|hours| now + hours * 3_600),
            snapshot_fetched_at: now - 30,
            eligible: true,
        }
    }

    #[test]
    fn ranks_real_recovered_capacity_and_penalizes_an_imminent_natural_reset() {
        let now = 1_000_000;
        let rows = optimize_reset_credits(
            &[
                candidate("plus-six-days", 7_560.0, 95.0, 144, Some(240)),
                candidate("plus-thirty-minutes", 7_560.0, 95.0, 0, Some(240)),
                candidate("pro-thirty-percent", 50_400.0, 30.0, 120, Some(240)),
            ],
            now,
            120,
        );

        assert_eq!(rows[0].account_id, "pro-thirty-percent");
        assert_eq!(rows[1].account_id, "plus-six-days");
        assert_eq!(
            rows[2].recommendation,
            ResetCreditRecommendation::WaitForNaturalReset
        );
        assert!((rows[0].recoverable_credits - 15_120.0).abs() < 0.01);
        assert!((rows[1].recoverable_credits - 7_182.0).abs() < 0.01);
    }

    #[test]
    fn expiry_before_natural_reset_is_preserved_as_an_explicit_action() {
        let now = 1_000_000;
        let rows = optimize_reset_credits(
            &[candidate("expiring", 7_560.0, 50.0, 72, Some(2))],
            now,
            120,
        );

        assert_eq!(
            rows[0].recommendation,
            ResetCreditRecommendation::RedeemBeforeExpiry
        );
    }

    #[test]
    fn stale_or_ineligible_evidence_never_recommends_spending_a_credit() {
        let now = 1_000_000;
        let mut stale = candidate("stale", 7_560.0, 99.0, 100, Some(200));
        stale.snapshot_fetched_at = now - 121;
        let mut paused = candidate("paused", 7_560.0, 99.0, 100, Some(200));
        paused.eligible = false;

        let rows = optimize_reset_credits(&[stale, paused], now, 120);
        assert!(rows
            .iter()
            .all(|row| { row.recommendation == ResetCreditRecommendation::Unavailable }));
    }
}
