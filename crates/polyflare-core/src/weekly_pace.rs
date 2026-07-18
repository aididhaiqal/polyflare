//! Pure pool-wide "WeeklyCreditPace" simulation primitives — faithful port of the sim in
//! codex-lb `app/modules/dashboard/weekly_pace.py`. No I/O, no state. v1 hardcodes
//! `working_days = None` (the linear schedule — codex-lb's own default), so the weekend-stepping
//! helpers are deliberately omitted; the operator-configurable working-days + smoothing settings
//! are a deferred follow-up (they'd need new settings columns). Dashboard-read-only; feeds no routing.
//!
//! `#[allow(dead_code)]` throughout: this module lands ahead of the assembler that will call it
//! (added to this same file in a follow-up task), so today these `pub(crate)` primitives are only
//! reachable from this module's own `#[cfg(test)]` block — which doesn't count for the plain
//! (non-test) build's dead-code analysis. Remove the allows once the assembler wires them in.

#![allow(dead_code)]

use crate::depletion::{ewma_update, EwmaState, UsageSample, DEFAULT_ALPHA};

pub(crate) const RECENT_BURN_WINDOW_SECS: i64 = 6 * 3600;
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
