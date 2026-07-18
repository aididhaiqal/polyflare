//! Pure pool-wide "WeeklyCreditPace" simulation primitives — faithful port of the sim in
//! codex-lb `app/modules/dashboard/weekly_pace.py`. No I/O, no state. v1 hardcodes
//! `working_days = None` (the linear schedule — codex-lb's own default), so the weekend-stepping
//! helpers are deliberately omitted; the operator-configurable working-days + smoothing settings
//! are a deferred follow-up (they'd need new settings columns). Dashboard-read-only; feeds no routing.
//!
//! The pure sim primitives above are consumed by [`build_weekly_credit_pace`] below, the pool-wide
//! aggregation entry point.

use crate::depletion::{ewma_update, EwmaState, UsageSample, DEFAULT_ALPHA};
use serde::Serialize;

pub const RECENT_BURN_WINDOW_SECS: i64 = 6 * 3600;
pub(crate) const MIN_FRESHNESS_SECS: f64 = 300.0;
pub(crate) const FRESHNESS_MISSED_REFRESH_CYCLES: f64 = 3.0;
pub(crate) const PRO_WEEKLY_CAPACITY_CREDITS: f64 = 50_400.0;

/// A single account in the pool drain simulation (credits + its own reset schedule, in ms).
#[derive(Debug, Clone, Copy)]
pub(crate) struct SimAccount {
    pub full_credits: f64,
    pub balance_credits: f64,
    pub reset_at_ms: f64,
    pub window_ms: f64,
}

/// The sim's output.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Projection {
    pub projected_shortfall_credits: f64,
    pub projected_depletion_hours: Option<f64>,
    pub projected_minimum_remaining_credits: f64,
}

/// freshness cutoff seconds = max(300, refresh_interval · 3).
pub(crate) fn freshness_seconds(refresh_interval_secs: i64) -> f64 {
    (refresh_interval_secs as f64 * FRESHNESS_MISSED_REFRESH_CYCLES).max(MIN_FRESHNESS_SECS)
}

/// Per-account forecast burn rate in **credits/hour** from the last 6h of samples via the EWMA.
/// `None` if fewer than 2 recent rows or the rate never establishes. `%/s → credits/hr` uses
/// `rate · full_credits · 36` (= rate% · full/100 · 3600).
///
/// Precondition: `rows` must be ordered oldest-first by `recorded_at` — the EWMA is replayed in
/// order, same as the sibling `compute_depletion_for_account`.
pub(crate) fn recent_burn_rate_credits_per_hour(
    rows: &[UsageSample],
    full_credits: f64,
    now: i64,
) -> Option<f64> {
    let recent_start = now - RECENT_BURN_WINDOW_SECS;
    let recent: Vec<&UsageSample> = rows
        .iter()
        .filter(|r| r.recorded_at >= recent_start && r.recorded_at <= now)
        .collect();
    if recent.len() < 2 {
        return None;
    }
    let mut state: Option<EwmaState> = None;
    for r in &recent {
        state = Some(ewma_update(
            state,
            r.used_percent,
            r.recorded_at as f64,
            DEFAULT_ALPHA,
            r.reset_at,
        ));
    }
    let rate = state?.rate?;
    Some((rate * full_credits * 36.0).max(0.0))
}

/// Average remaining credits over the last `smoothing_window_minutes`, restricted to rows in the
/// SAME window (reset_at + window_minutes) as the latest row. Falls back to `current_remaining_credits`
/// when there are no qualifying recent rows.
pub(crate) fn smoothed_remaining_credits(
    rows: &[UsageSample],
    full_credits: f64,
    current_remaining_credits: f64,
    now: i64,
    smoothing_window_minutes: i64,
) -> f64 {
    let smoothing_start = now - smoothing_window_minutes * 60;
    let Some(latest) = rows.last() else {
        return current_remaining_credits;
    };
    let latest_reset_at = latest.reset_at;
    let latest_window_minutes = latest.window_minutes;

    let mut total_remaining = 0.0;
    let mut sample_count = 0u32;
    for r in rows {
        if r.recorded_at < smoothing_start || r.recorded_at > now {
            continue;
        }
        if latest_reset_at.is_some() && r.reset_at != latest_reset_at {
            continue;
        }
        if latest_window_minutes.is_some() && r.window_minutes != latest_window_minutes {
            continue;
        }
        if !r.used_percent.is_finite() {
            continue;
        }
        let used = r.used_percent.clamp(0.0, 100.0);
        total_remaining += full_credits * (1.0 - used / 100.0);
        sample_count += 1;
    }
    if sample_count == 0 {
        return current_remaining_credits;
    }
    (total_remaining / sample_count as f64).clamp(0.0, full_credits)
}

/// Roll a reset boundary forward past `now_ms` by whole windows.
pub(crate) fn advance_reset_at(reset_at_ms: f64, window_ms: f64, now_ms: f64) -> f64 {
    if reset_at_ms > now_ms {
        return reset_at_ms;
    }
    let missed = ((now_ms - reset_at_ms) / window_ms).floor() as i64 + 1;
    reset_at_ms + missed as f64 * window_ms
}

fn total_balance(accounts: &[SimAccount]) -> f64 {
    accounts.iter().map(|a| a.balance_credits).sum()
}

/// Drain `amount_credits` from accounts, soonest-reset first.
fn consume_balance(accounts: &mut [SimAccount], amount_credits: f64) {
    let mut order: Vec<usize> = (0..accounts.len()).collect();
    order.sort_by(|&a, &b| accounts[a].reset_at_ms.total_cmp(&accounts[b].reset_at_ms));
    let mut remaining = amount_credits;
    for i in order {
        if remaining <= 0.0 {
            return;
        }
        let consumed = accounts[i].balance_credits.min(remaining);
        accounts[i].balance_credits -= consumed;
        remaining -= consumed;
    }
}

/// The discrete-event pool sim: events are per-account weekly RESET boundaries. Simulates a single
/// pooled burn rate draining the accounts (soonest-reset first), each refilling to full at its own
/// reset, over a 2×max-window horizon. Answers "does the pool run dry before enough resets refill it?".
pub(crate) fn project_weekly_pool(
    accounts: &[SimAccount],
    now_ms: f64,
    forecast_burn_rate_credits_per_hour: Option<f64>,
) -> Projection {
    let total_remaining = total_balance(accounts);
    let burn = match forecast_burn_rate_credits_per_hour {
        Some(b) if b > 0.0 => b,
        _ => {
            return Projection {
                projected_shortfall_credits: 0.0,
                projected_depletion_hours: None,
                projected_minimum_remaining_credits: total_remaining,
            }
        }
    };

    let burn_per_ms = burn / 3_600_000.0;
    let mut sim: Vec<SimAccount> = accounts.to_vec();
    let max_window = accounts.iter().map(|a| a.window_ms).fold(0.0_f64, f64::max);
    let horizon_ms = now_ms + max_window * 2.0;
    let mut cursor_ms = now_ms;
    let mut minimum_remaining = total_remaining;

    while cursor_ms < horizon_ms {
        sim.sort_by(|a, b| a.reset_at_ms.total_cmp(&b.reset_at_ms));
        let next_reset_at_ms = sim[0].reset_at_ms;
        let next_event_at_ms = next_reset_at_ms.min(horizon_ms);
        let interval_ms = (next_event_at_ms - cursor_ms).max(0.0);
        let interval_burn = burn_per_ms * interval_ms;
        let bal = total_balance(&sim);

        if interval_burn > bal {
            let depletion_wait_ms = if burn_per_ms > 0.0 {
                bal / burn_per_ms
            } else {
                0.0
            };
            return Projection {
                projected_shortfall_credits: interval_burn - bal,
                projected_depletion_hours: Some(
                    (cursor_ms - now_ms + depletion_wait_ms) / 3_600_000.0,
                ),
                projected_minimum_remaining_credits: 0.0,
            };
        }

        consume_balance(&mut sim, interval_burn);
        minimum_remaining = minimum_remaining.min(total_balance(&sim));
        cursor_ms = next_event_at_ms;
        if cursor_ms >= horizon_ms {
            break;
        }
        // refill the account whose reset we just hit (index 0 after the sort).
        sim[0].balance_credits = sim[0].full_credits;
        sim[0].reset_at_ms += sim[0].window_ms;
        minimum_remaining = minimum_remaining.min(total_balance(&sim));
    }

    Projection {
        projected_shortfall_credits: 0.0,
        projected_depletion_hours: None,
        projected_minimum_remaining_credits: minimum_remaining,
    }
}

/// One account's inputs to the pool pace calc. `full_credits` is the plan-derived (or per-account
/// override) secondary-window capacity; `used_percent` is the latest secondary used%; the remaining
/// credits are derived as `full · (1 - clamp(used%,0,100)/100)`. All fields content-free.
#[derive(Debug, Clone)]
pub struct PaceAccountInput {
    pub account_id: String,
    pub status_eligible: bool,
    pub full_credits: f64,
    pub used_percent: f64,
    pub reset_at: Option<i64>,
    pub window_minutes: Option<i64>,
    pub secondary_history: Vec<UsageSample>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PaceStatus {
    OnTrack,
    Ahead,
    Behind,
    Danger,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Confidence {
    High,
    Medium,
    Low,
}

/// Pool-wide weekly credit pace. All fields content-free (credits/percentages/hours/counts).
#[derive(Debug, Clone, Copy, Serialize)]
pub struct WeeklyCreditPaceReport {
    pub total_full_credits: f64,
    pub total_actual_remaining_credits: f64,
    pub total_expected_remaining_credits: f64,
    pub actual_used_percent: f64,
    pub scheduled_used_percent: f64,
    pub delta_percent: f64,
    pub schedule_gap_credits: f64,
    pub smoothed_delta_percent: f64,
    pub smoothed_schedule_gap_credits: f64,
    pub projected_shortfall_credits: f64,
    pub pause_for_break_even_hours: Option<f64>,
    pub pace_multiplier: Option<f64>,
    pub throttle_to_percent: Option<f64>,
    pub reduce_by_percent: Option<f64>,
    pub pro_account_equivalent_to_cover_over_plan: Option<f64>,
    pub pro_accounts_to_cover_over_plan: Option<i64>,
    pub projected_depletion_hours: Option<f64>,
    pub projected_minimum_remaining_credits: f64,
    pub forecast_burn_rate_credits_per_hour: Option<f64>,
    pub scheduled_burn_rate_credits_per_hour: f64,
    pub status: PaceStatus,
    pub account_count: i64,
    pub stale_account_count: i64,
    pub inactive_account_count: i64,
    pub confidence: Confidence,
}

/// `now`/`reset_at` are unix seconds; the sim works in ms internally. `working_days` is fixed to the
/// linear schedule (v1). Returns `None` when no eligible, fresh, positive-capacity account remains.
pub fn build_weekly_credit_pace(
    accounts: &[PaceAccountInput],
    now: i64,
    refresh_interval_secs: i64,
    smoothing_window_minutes: i64,
) -> Option<WeeklyCreditPaceReport> {
    let now_ms = now as f64 * 1000.0;
    let freshness_cutoff = now - freshness_seconds(refresh_interval_secs) as i64;

    let mut sim_inputs: Vec<SimAccount> = Vec::new();
    let mut stale = 0i64;
    let mut inactive = 0i64;
    let mut rate_samples = 0i64;
    let mut total_full = 0.0;
    let mut total_actual_remaining = 0.0;
    let mut total_smoothed_remaining = 0.0;
    let mut total_expected_remaining = 0.0;
    let mut scheduled_burn = 0.0;
    let mut forecast_burn = 0.0;

    for a in accounts {
        // _weekly_timing: require positive capacity, a reset, positive window.
        let (Some(reset_at), Some(window_minutes)) = (a.reset_at, a.window_minutes) else {
            continue;
        };
        if a.full_credits <= 0.0 || window_minutes <= 0 {
            continue;
        }
        if !a.status_eligible {
            inactive += 1;
            continue;
        }
        // freshness: latest secondary row must be newer than the cutoff.
        let latest = a.secondary_history.last();
        match latest {
            Some(r) if r.recorded_at >= freshness_cutoff => {}
            _ => {
                stale += 1;
                continue;
            }
        }

        let full = a.full_credits;
        let actual_remaining =
            (full * (1.0 - a.used_percent.clamp(0.0, 100.0) / 100.0)).clamp(0.0, full);
        let window_ms = window_minutes as f64 * 60_000.0;
        let reset_at_ms = advance_reset_at(reset_at as f64 * 1000.0, window_ms, now_ms);

        // linear schedule (working_days = None): used fraction = clamp(elapsed/window, 0, 1).
        let window_start_ms = reset_at_ms - window_ms;
        let elapsed_ms = (now_ms - window_start_ms).clamp(0.0, window_ms);
        let used_schedule_fraction = if elapsed_ms <= 0.0 {
            0.0
        } else {
            elapsed_ms / window_ms
        };
        let expected_remaining = full * (1.0 - used_schedule_fraction);

        let account_rate = recent_burn_rate_credits_per_hour(&a.secondary_history, full, now);
        let smoothed_remaining = smoothed_remaining_credits(
            &a.secondary_history,
            full,
            actual_remaining,
            now,
            smoothing_window_minutes,
        );

        total_full += full;
        total_actual_remaining += actual_remaining;
        total_smoothed_remaining += smoothed_remaining;
        total_expected_remaining += expected_remaining;
        // working_days = None => working_schedule_share_per_hour = 3_600_000/window_ms.
        scheduled_burn += full * (3_600_000.0 / window_ms);
        if let Some(rate) = account_rate {
            rate_samples += 1;
            forecast_burn += rate;
        }

        sim_inputs.push(SimAccount {
            full_credits: full,
            balance_credits: actual_remaining,
            reset_at_ms,
            window_ms,
        });
    }

    if sim_inputs.is_empty() || total_full <= 0.0 {
        return None;
    }

    let actual_used_percent = 100.0 * (total_full - total_actual_remaining) / total_full;
    let scheduled_used_percent = 100.0 * (total_full - total_expected_remaining) / total_full;
    let delta_percent = actual_used_percent - scheduled_used_percent;
    let schedule_gap_credits = (total_expected_remaining - total_actual_remaining).max(0.0);
    let smoothed_used_percent = 100.0 * (total_full - total_smoothed_remaining) / total_full;
    let smoothed_delta_percent = smoothed_used_percent - scheduled_used_percent;
    let smoothed_schedule_gap_credits =
        (total_expected_remaining - total_smoothed_remaining).max(0.0);

    let forecast_rate = if rate_samples > 0 {
        Some(forecast_burn)
    } else {
        None
    };
    let projection = project_weekly_pool(&sim_inputs, now_ms, forecast_rate);
    let shortfall = projection.projected_shortfall_credits;

    let pace_multiplier = match forecast_rate {
        Some(r) if scheduled_burn > 0.0 => Some(r / scheduled_burn),
        _ => None,
    };
    let pause_for_break_even_hours = match forecast_rate {
        Some(r) if r > 0.0 && shortfall > 0.0 => Some(shortfall / r),
        _ => None,
    };
    let throttle_to_percent = match forecast_rate {
        Some(r) if r > 0.0 && scheduled_burn > 0.0 && shortfall > 0.0 => {
            Some(((scheduled_burn / r) * 100.0).clamp(0.0, 100.0))
        }
        _ => None,
    };
    let reduce_by_percent = throttle_to_percent.map(|t| 100.0 - t);
    let pro_equivalent = if shortfall > 0.0 {
        Some(shortfall / PRO_WEEKLY_CAPACITY_CREDITS)
    } else {
        None
    };
    let pro_accounts = pro_equivalent.map(|e| e.ceil() as i64);

    let status = if shortfall > 0.0 {
        PaceStatus::Danger
    } else if smoothed_delta_percent < -5.0 {
        PaceStatus::Behind
    } else if smoothed_delta_percent > 5.0 {
        PaceStatus::Ahead
    } else {
        PaceStatus::OnTrack
    };
    let account_count = sim_inputs.len() as i64;
    let confidence = if rate_samples >= account_count && stale == 0 {
        Confidence::High
    } else if rate_samples > 0 {
        Confidence::Medium
    } else {
        Confidence::Low
    };

    Some(WeeklyCreditPaceReport {
        total_full_credits: total_full,
        total_actual_remaining_credits: total_actual_remaining,
        total_expected_remaining_credits: total_expected_remaining,
        actual_used_percent,
        scheduled_used_percent,
        delta_percent,
        schedule_gap_credits,
        smoothed_delta_percent,
        smoothed_schedule_gap_credits,
        projected_shortfall_credits: shortfall,
        pause_for_break_even_hours,
        pace_multiplier,
        throttle_to_percent,
        reduce_by_percent,
        pro_account_equivalent_to_cover_over_plan: pro_equivalent,
        pro_accounts_to_cover_over_plan: pro_accounts,
        projected_depletion_hours: projection.projected_depletion_hours,
        projected_minimum_remaining_credits: projection.projected_minimum_remaining_credits,
        forecast_burn_rate_credits_per_hour: forecast_rate,
        scheduled_burn_rate_credits_per_hour: scheduled_burn,
        status,
        account_count,
        stale_account_count: stale,
        inactive_account_count: inactive,
        confidence,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::depletion::UsageSample;

    fn sim(full: f64, bal: f64, reset_ms: f64, window_ms: f64) -> SimAccount {
        SimAccount {
            full_credits: full,
            balance_credits: bal,
            reset_at_ms: reset_ms,
            window_ms,
        }
    }

    #[test]
    fn no_burn_rate_returns_full_remaining_no_depletion() {
        let accts = [sim(7560.0, 5000.0, 1_000.0, 100.0)];
        let p = project_weekly_pool(&accts, 0.0, None);
        assert_eq!(p.projected_shortfall_credits, 0.0);
        assert_eq!(p.projected_depletion_hours, None);
        assert_eq!(p.projected_minimum_remaining_credits, 5000.0);
    }

    #[test]
    fn exhausts_before_reset_reports_shortfall_and_hours() {
        // 1 account, 3600 credits, single weekly window; burn 7200 credits/hr.
        // window_ms huge so no reset inside horizon relative to burn: pool drains in 0.5h.
        let week_ms = 7.0 * 24.0 * 3_600_000.0;
        let accts = [sim(3600.0, 3600.0, week_ms, week_ms)];
        let p = project_weekly_pool(&accts, 0.0, Some(7200.0));
        assert!(p.projected_shortfall_credits > 0.0);
        // depletion ≈ 3600/7200 = 0.5h
        assert!((p.projected_depletion_hours.unwrap() - 0.5).abs() < 1e-6);
        assert_eq!(p.projected_minimum_remaining_credits, 0.0);
    }

    #[test]
    fn refill_at_reset_survives() {
        // small balance but a reset well inside the horizon refills to full, so a modest
        // burn never exhausts. reset in 1h, window 1h, full 10000, burn 100/hr.
        let hour_ms = 3_600_000.0;
        let accts = [sim(10_000.0, 10_000.0, hour_ms, hour_ms)];
        let p = project_weekly_pool(&accts, 0.0, Some(100.0));
        assert_eq!(p.projected_shortfall_credits, 0.0);
        assert_eq!(p.projected_depletion_hours, None);
        assert!(p.projected_minimum_remaining_credits > 0.0);
    }

    #[test]
    fn advance_reset_at_rolls_past_now() {
        // reset 5 windows in the past => advanced to the next future boundary.
        let w = 1000.0;
        assert_eq!(advance_reset_at(2000.0, w, 500.0), 2000.0); // already future
                                                                // now=5500, reset=2000, window=1000 => missed=(5500-2000)//1000 +1 = 3+1=4 => 2000+4000=6000
        assert_eq!(advance_reset_at(2000.0, w, 5500.0), 6000.0);
    }

    #[test]
    fn burn_rate_needs_two_recent_rows() {
        let now = 1_000_000;
        let one = [UsageSample {
            used_percent: 10.0,
            reset_at: Some(now + 100),
            window_minutes: Some(10_080),
            recorded_at: now - 100,
        }];
        assert_eq!(recent_burn_rate_credits_per_hour(&one, 7560.0, now), None);
    }

    #[test]
    fn burn_rate_scales_percent_per_sec_to_credits_per_hour() {
        let now = 1_000_000;
        // two rows 600s apart, +10% => rate 0.016667 %/s; credits/hr = rate * full * 36
        let rows = [
            UsageSample {
                used_percent: 40.0,
                reset_at: Some(now + 3600),
                window_minutes: Some(10_080),
                recorded_at: now - 600,
            },
            UsageSample {
                used_percent: 50.0,
                reset_at: Some(now + 3600),
                window_minutes: Some(10_080),
                recorded_at: now,
            },
        ];
        let r = recent_burn_rate_credits_per_hour(&rows, 7560.0, now).unwrap();
        // 0.0166667 * 7560 * 36 = 4536
        assert!((r - 4536.0).abs() < 1.0);
    }

    #[test]
    fn burn_rate_excludes_rows_older_than_6h() {
        let now = 1_000_000;
        let rows = [
            UsageSample {
                used_percent: 5.0,
                reset_at: Some(now + 3600),
                window_minutes: Some(10_080),
                recorded_at: now - 7 * 3600,
            }, // >6h old, dropped
            UsageSample {
                used_percent: 40.0,
                reset_at: Some(now + 3600),
                window_minutes: Some(10_080),
                recorded_at: now - 600,
            },
            UsageSample {
                used_percent: 50.0,
                reset_at: Some(now + 3600),
                window_minutes: Some(10_080),
                recorded_at: now,
            },
        ];
        // only the two recent rows count -> same 4536 as above
        let r = recent_burn_rate_credits_per_hour(&rows, 7560.0, now).unwrap();
        assert!((r - 4536.0).abs() < 1.0);
    }

    #[test]
    fn smoothed_remaining_averages_recent_same_window() {
        let now = 1_000_000;
        let rows = [
            UsageSample {
                used_percent: 40.0,
                reset_at: Some(9_000),
                window_minutes: Some(10_080),
                recorded_at: now - 600,
            },
            UsageSample {
                used_percent: 50.0,
                reset_at: Some(9_000),
                window_minutes: Some(10_080),
                recorded_at: now,
            },
        ];
        // full 1000 => remaining rows 600 and 500 => avg 550
        let s = smoothed_remaining_credits(&rows, 1000.0, 500.0, now, 30);
        assert!((s - 550.0).abs() < 1e-6);
    }

    #[test]
    fn smoothed_remaining_empty_returns_current() {
        let now = 1_000_000;
        assert_eq!(
            smoothed_remaining_credits(&[], 1000.0, 500.0, now, 30),
            500.0
        );
    }

    #[test]
    fn freshness_is_max_300_and_3x_interval() {
        assert_eq!(freshness_seconds(600), 1800.0); // 600*3
        assert_eq!(freshness_seconds(10), 300.0); // floor
    }

    #[test]
    fn multi_account_drains_soonest_reset_first_and_refills_correct_account() {
        // Regression pin for the multi-account sim crux: draining must be soonest-reset-first,
        // and the refill-at-boundary must land on the SAME account whose reset just fired. A
        // single-account sim test can never catch either bug (there's only one account to drain
        // or refill), so this test uses two.
        //
        // A: resets in 1h (soonest), near-empty (200 of 1500 full). B: resets in 3h (outside the
        // 2h horizon, so it never itself refills), ample balance (5000 of 5000 full). Burn is a
        // constant 2000 credits/hr, low enough that the pool never exhausts either way -- but
        // WHICH account absorbs each interval's burn changes how much gets restored when A hits
        // its 1h refill: a fully-drained A gets a big injection back to full; an untouched A gets
        // a small one. That difference then shows up in the *next* interval's trough.
        //
        // Hand-traced expectation (soonest-first == correct):
        //   iter1 [0,1h): burn=2000 drains A(200->0) then overflows 1800 onto B(5000->3200); trough 3200
        //   refill A: 0->1500 (full 1500 injection);                    total = 1500+3200 = 4700
        //   iter2 [1h,2h): burn=2000 drains A(1500->0) then overflows 500 onto B(3200->2700); trough 2700
        //   => projected_minimum_remaining_credits = 2700, no shortfall.
        //
        // A newest-first (wrong) drain order instead lets B -- which alone has enough balance --
        // absorb each interval's burn, leaving A untouched (still near-empty) at the 1h refill.
        // That's only a 1300 injection (1500-200), so the pool re-enters iter2 lower (4500, not
        // 4700) and the iter2 trough comes out at 2500, not 2700. Verified in this task's RED
        // proof: reversing consume_balance's drain order flips this assertion to 2500 (see the
        // task-3 report for the exact mutation + before/after `cargo test` output).
        let hour_ms = 3_600_000.0;
        let a = sim(1_500.0, 200.0, hour_ms, hour_ms); // soonest reset, near-empty
        let b = sim(5_000.0, 5_000.0, 3.0 * hour_ms, hour_ms); // later reset (outside horizon), ample balance
        let p = project_weekly_pool(&[a, b], 0.0, Some(2_000.0));
        assert_eq!(p.projected_shortfall_credits, 0.0);
        assert_eq!(p.projected_depletion_hours, None);
        assert_eq!(p.projected_minimum_remaining_credits, 2700.0);
    }
}

#[cfg(test)]
mod agg_tests {
    use super::*;
    use crate::depletion::UsageSample;

    fn acct(
        id: &str,
        full: f64,
        used: f64,
        reset_at: i64,
        rows: Vec<UsageSample>,
    ) -> PaceAccountInput {
        PaceAccountInput {
            account_id: id.to_string(),
            status_eligible: true,
            full_credits: full,
            used_percent: used,
            reset_at: Some(reset_at),
            window_minutes: Some(10_080),
            secondary_history: rows,
        }
    }

    fn row(used: f64, reset_at: i64, recorded_at: i64) -> UsageSample {
        UsageSample {
            used_percent: used,
            reset_at: Some(reset_at),
            window_minutes: Some(10_080),
            recorded_at,
        }
    }

    #[test]
    fn none_when_no_eligible_fresh_accounts() {
        let now = 1_000_000;
        // stale: latest row far older than freshness cutoff
        let a = acct(
            "a",
            7560.0,
            50.0,
            now + 3600,
            vec![row(50.0, now + 3600, now - 100_000)],
        );
        assert!(build_weekly_credit_pace(&[a], now, 600, 30).is_none());
    }

    #[test]
    fn ineligible_status_is_counted_inactive_not_paced() {
        let now = 1_000_000;
        let mut a = acct(
            "a",
            7560.0,
            50.0,
            now + 3600,
            vec![row(40.0, now + 3600, now - 600), row(50.0, now + 3600, now)],
        );
        a.status_eligible = false;
        assert!(build_weekly_credit_pace(&[a], now, 600, 30).is_none());
    }

    #[test]
    fn happy_path_reports_used_percent_and_status() {
        let now = 1_000_000;
        let reset = now + 6 * 24 * 3600; // ~6 days out (near week start)
        let a = acct(
            "a",
            10_000.0,
            50.0,
            reset,
            vec![row(40.0, reset, now - 600), row(50.0, reset, now)],
        );
        let r = build_weekly_credit_pace(&[a], now, 600, 30).expect("report");
        assert_eq!(r.account_count, 1);
        assert_eq!(r.total_full_credits, 10_000.0);
        // actual_used ~ 50% (remaining derived from used_percent)
        assert!((r.actual_used_percent - 50.0).abs() < 1e-6);
        // early in the window (reset ~6d out of 7d window) => scheduled_used_percent is low => actual > scheduled => "ahead" or "danger"
        assert!(r.forecast_burn_rate_credits_per_hour.is_some());
        assert!(matches!(
            r.confidence,
            Confidence::High | Confidence::Medium
        ));
    }

    #[test]
    fn pool_shortfall_sets_danger_status() {
        let now = 1_000_000;
        let reset = now + 7 * 24 * 3600; // full window ahead => low scheduled use
                                         // steep burn: +40% over 600s on a small pool => huge credits/hr => shortfall
        let a = acct(
            "a",
            1000.0,
            90.0,
            reset,
            vec![row(50.0, reset, now - 600), row(90.0, reset, now)],
        );
        let r = build_weekly_credit_pace(&[a], now, 600, 30).expect("report");
        assert!(r.projected_shortfall_credits > 0.0);
        assert_eq!(r.status, PaceStatus::Danger);
        assert!(r.pro_accounts_to_cover_over_plan.unwrap() >= 1);
    }
}
