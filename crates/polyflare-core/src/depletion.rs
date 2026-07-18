//! Pure EWMA credit-depletion forecasting — faithful port of codex-lb
//! `app/core/usage/depletion.py` + `depletion_service.compute_depletion_for_account`.
//! No I/O, no state: the server rebuilds the estimator from the stored usage-history
//! time series on each read (codex-lb's in-memory `_ewma_states` cache is a pure
//! optimization, deliberately omitted here). Dashboard-read-only; feeds no routing.

use serde::Serialize;

pub const RISK_WARNING: f64 = 0.60;
pub const RISK_DANGER: f64 = 0.80;
pub const RISK_CRITICAL: f64 = 0.95;
pub const DEFAULT_ALPHA: f64 = 0.4;

/// Per-window EWMA estimator state. `rate` is smoothed d(used_percent)/dt in **used-percent
/// per second** (0-100 scale), `None` until >= 2 successive in-window samples establish it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EwmaState {
    pub rate: Option<f64>,
    pub last_used_percent: f64,
    pub last_timestamp: f64,
    pub last_reset_at: Option<i64>,
}

/// EWMA update. Smooths raw_rate = max(Δused% / Δt, 0) with `rate = α·raw + (1-α)·prev`.
/// Resets (rate→None, reseed) on a used% DROP or a window change (reset_at differs, both non-null).
/// A zero Δt duplicate sample is ignored.
pub fn ewma_update(
    state: Option<EwmaState>,
    used_percent: f64,
    timestamp: f64,
    alpha: f64,
    reset_at: Option<i64>,
) -> EwmaState {
    let Some(state) = state else {
        return EwmaState {
            rate: None,
            last_used_percent: used_percent,
            last_timestamp: timestamp,
            last_reset_at: reset_at,
        };
    };

    let dt = timestamp - state.last_timestamp;
    if dt == 0.0 {
        return state;
    }

    let window_changed = match (reset_at, state.last_reset_at) {
        (Some(a), Some(b)) => a != b,
        _ => false,
    };
    let drop = state.last_used_percent - used_percent;
    if drop > 0.0 || window_changed {
        return EwmaState {
            rate: None,
            last_used_percent: used_percent,
            last_timestamp: timestamp,
            last_reset_at: reset_at,
        };
    }

    let delta_percent = used_percent - state.last_used_percent;
    let raw_rate = (delta_percent / dt).max(0.0);
    let rate = match state.rate {
        None => raw_rate,
        Some(prev) => alpha * raw_rate + (1.0 - alpha) * prev,
    };
    EwmaState {
        rate: Some(rate),
        last_used_percent: used_percent,
        last_timestamp: timestamp,
        last_reset_at: reset_at,
    }
}

/// Dimensionless burn rate: current_rate / sustainable_rate, where sustainable = remaining%/secs.
/// `>1` = burning faster than budget. 0 if current_rate or secs is 0.
pub fn compute_burn_rate(
    current_rate: f64,
    remaining_percent: f64,
    seconds_until_reset: f64,
) -> f64 {
    if current_rate == 0.0 || seconds_until_reset == 0.0 {
        return 0.0;
    }
    let sustainable_rate = remaining_percent / seconds_until_reset;
    if sustainable_rate == 0.0 {
        return 0.0;
    }
    current_rate / sustainable_rate
}

/// Projected end-of-window fill as a 0..1 fraction: min((used% + max(rate,0)·secs)/100, 1).
pub fn compute_depletion_risk(
    used_percent: f64,
    rate_per_second: f64,
    seconds_until_reset: f64,
) -> f64 {
    let effective_rate = rate_per_second.max(0.0);
    let projected = used_percent + effective_rate * seconds_until_reset;
    (projected / 100.0).min(1.0)
}

/// The linear "budget line": clamp(elapsed/total, 0, 1) · 100. 0 when total is 0.
pub fn compute_safe_usage_percent(seconds_elapsed: f64, total_window_seconds: f64) -> f64 {
    if total_window_seconds == 0.0 {
        return 0.0;
    }
    let progress = seconds_elapsed / total_window_seconds;
    progress.clamp(0.0, 1.0) * 100.0
}

/// Risk band. Plain `>=` comparisons, no hysteresis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RiskLevel {
    Safe,
    Warning,
    Danger,
    Critical,
}

pub fn classify_risk(risk: f64) -> RiskLevel {
    if risk >= RISK_CRITICAL {
        RiskLevel::Critical
    } else if risk >= RISK_DANGER {
        RiskLevel::Danger
    } else if risk >= RISK_WARNING {
        RiskLevel::Warning
    } else {
        RiskLevel::Safe
    }
}

/// Worst-case (max) risk across accounts; 0.0 if empty.
pub fn aggregate_risks(risks: &[f64]) -> f64 {
    if risks.is_empty() {
        0.0
    } else {
        risks.iter().copied().fold(f64::NEG_INFINITY, f64::max)
    }
}

/// One usage sample (core-local mirror of the store's `WindowUsage`; `polyflare-core` has no
/// store dependency, so the server maps `WindowUsage` → `UsageSample`). `recorded_at`/`reset_at`
/// are unix seconds.
#[derive(Debug, Clone, Copy)]
pub struct UsageSample {
    pub used_percent: f64,
    pub reset_at: Option<i64>,
    pub window_minutes: Option<i64>,
    pub recorded_at: i64,
}

/// Assembled per-account depletion forecast. All fields content-free.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct DepletionForecast {
    pub risk: f64,
    pub risk_level: RiskLevel,
    pub rate_per_second: f64,
    pub burn_rate: f64,
    pub used_percent: f64,
    pub safe_usage_percent: f64,
    pub seconds_until_reset: i64,
    pub seconds_until_exhaustion: Option<f64>,
    pub projected_exhaustion_at: Option<i64>,
}

/// Rebuild the EWMA from a window's successive samples (ordered oldest-first) and assemble a
/// forecast against `now` (unix seconds). Faithful port of codex-lb
/// `depletion_service.compute_depletion_for_account`, stateless. Returns `None` when: fewer than
/// 2 samples, the rate never establishes, or the window has already reset (stale used%).
pub fn compute_depletion_for_account(
    samples: &[UsageSample],
    now: i64,
) -> Option<DepletionForecast> {
    if samples.len() < 2 {
        return None;
    }
    let mut state: Option<EwmaState> = None;
    for s in samples {
        state = Some(ewma_update(
            state,
            s.used_percent,
            s.recorded_at as f64,
            DEFAULT_ALPHA,
            s.reset_at,
        ));
    }
    let rate = state?.rate?;

    let latest = samples.last()?;
    let used_percent = latest.used_percent;

    let mut seconds_until_reset = 0.0_f64;
    if let Some(reset_at) = latest.reset_at {
        seconds_until_reset = (reset_at - now).max(0) as f64;
        if seconds_until_reset == 0.0 {
            return None; // window already reset — stale used% is meaningless
        }
    } else if let Some(wm) = latest.window_minutes {
        seconds_until_reset = (wm * 60) as f64;
    }

    let total_window_seconds = latest
        .window_minutes
        .map(|wm| (wm * 60) as f64)
        .unwrap_or(0.0);
    let seconds_elapsed = (total_window_seconds - seconds_until_reset).max(0.0);

    let risk = compute_depletion_risk(used_percent, rate, seconds_until_reset);
    let risk_level = classify_risk(risk);
    let burn_rate = compute_burn_rate(rate, 100.0 - used_percent, seconds_until_reset);
    let safe_usage_percent = compute_safe_usage_percent(seconds_elapsed, total_window_seconds);

    let (seconds_until_exhaustion, projected_exhaustion_at) =
        if rate > 0.0 && seconds_until_reset > 0.0 {
            let secs = (100.0 - used_percent) / rate;
            if secs <= seconds_until_reset {
                (Some(secs), Some(now + secs as i64))
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

    Some(DepletionForecast {
        risk,
        risk_level,
        rate_per_second: rate,
        burn_rate,
        used_percent,
        safe_usage_percent,
        seconds_until_reset: seconds_until_reset as i64,
        seconds_until_exhaustion,
        projected_exhaustion_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // ewma_update: first sample seeds with rate=None.
    #[test]
    fn first_sample_seeds_no_rate() {
        let s = ewma_update(None, 10.0, 1_000.0, DEFAULT_ALPHA, Some(5_000));
        assert_eq!(s.rate, None);
        assert_eq!(s.last_used_percent, 10.0);
        assert_eq!(s.last_reset_at, Some(5_000));
    }

    // Second valid sample: first rate = raw_rate (no smoothing on the first step).
    #[test]
    fn second_sample_sets_raw_rate() {
        let s1 = ewma_update(None, 10.0, 0.0, DEFAULT_ALPHA, Some(5_000));
        let s2 = ewma_update(Some(s1), 20.0, 10.0, DEFAULT_ALPHA, Some(5_000));
        // raw_rate = (20-10)/(10-0) = 1.0 %/s
        assert_eq!(s2.rate, Some(1.0));
    }

    // Third sample smooths: rate = 0.4*raw + 0.6*prev.
    #[test]
    fn third_sample_smooths() {
        let s1 = ewma_update(None, 10.0, 0.0, DEFAULT_ALPHA, Some(5_000));
        let s2 = ewma_update(Some(s1), 20.0, 10.0, DEFAULT_ALPHA, Some(5_000)); // rate 1.0
        let s3 = ewma_update(Some(s2), 22.0, 20.0, DEFAULT_ALPHA, Some(5_000)); // raw=0.2
                                                                                // 0.4*0.2 + 0.6*1.0 = 0.68
        assert!((s3.rate.unwrap() - 0.68).abs() < 1e-9);
    }

    // dt == 0 is ignored (returns state unchanged).
    #[test]
    fn zero_dt_ignored() {
        let s1 = ewma_update(None, 10.0, 5.0, DEFAULT_ALPHA, Some(5_000));
        let s2 = ewma_update(Some(s1), 99.0, 5.0, DEFAULT_ALPHA, Some(5_000));
        assert_eq!(s2, s1);
    }

    // reset-on-drop: used% drops => rate resets to None, reseeds from the new sample.
    #[test]
    fn drop_resets() {
        let s1 = ewma_update(None, 50.0, 0.0, DEFAULT_ALPHA, Some(5_000));
        let s2 = ewma_update(Some(s1), 60.0, 10.0, DEFAULT_ALPHA, Some(5_000)); // rate 1.0
        let s3 = ewma_update(Some(s2), 5.0, 20.0, DEFAULT_ALPHA, Some(5_000)); // drop
        assert_eq!(s3.rate, None);
        assert_eq!(s3.last_used_percent, 5.0);
    }

    // window_changed (reset_at differs, both non-null) resets even without a drop.
    #[test]
    fn window_change_resets() {
        let s1 = ewma_update(None, 50.0, 0.0, DEFAULT_ALPHA, Some(5_000));
        let s2 = ewma_update(Some(s1), 60.0, 10.0, DEFAULT_ALPHA, Some(5_000)); // rate 1.0
        let s3 = ewma_update(Some(s2), 70.0, 20.0, DEFAULT_ALPHA, Some(9_999)); // new window
        assert_eq!(s3.rate, None);
    }

    // raw_rate is clamped at >= 0 (no negative rate even if delta_percent<0 without a full drop guard).
    // (delta<0 IS a drop, so this asserts the max(...,0) belt-and-suspenders via a flat step.)
    #[test]
    fn flat_step_zero_rate() {
        let s1 = ewma_update(None, 30.0, 0.0, DEFAULT_ALPHA, Some(5_000));
        let s2 = ewma_update(Some(s1), 30.0, 10.0, DEFAULT_ALPHA, Some(5_000)); // no drop, delta 0
        assert_eq!(s2.rate, Some(0.0));
    }

    #[test]
    fn burn_rate_math() {
        // current 0.01 %/s, remaining 40%, 2000s left => sustainable = 40/2000 = 0.02; burn = 0.5
        assert!((compute_burn_rate(0.01, 40.0, 2000.0) - 0.5).abs() < 1e-9);
        assert_eq!(compute_burn_rate(0.0, 40.0, 2000.0), 0.0);
        assert_eq!(compute_burn_rate(0.01, 40.0, 0.0), 0.0);
    }

    #[test]
    fn depletion_risk_math() {
        // used 50, rate 0.01 %/s, 2000s => projected = 50 + 20 = 70 => risk 0.70
        assert!((compute_depletion_risk(50.0, 0.01, 2000.0) - 0.70).abs() < 1e-9);
        // clamps at 1.0
        assert_eq!(compute_depletion_risk(90.0, 1.0, 100.0), 1.0);
        // negative rate treated as 0
        assert!((compute_depletion_risk(50.0, -1.0, 2000.0) - 0.50).abs() < 1e-9);
    }

    #[test]
    fn safe_usage_line() {
        assert_eq!(compute_safe_usage_percent(0.0, 100.0), 0.0);
        assert_eq!(compute_safe_usage_percent(50.0, 100.0), 50.0);
        assert_eq!(compute_safe_usage_percent(200.0, 100.0), 100.0); // clamped
        assert_eq!(compute_safe_usage_percent(50.0, 0.0), 0.0); // zero window guard
    }

    #[test]
    fn classify_thresholds() {
        assert_eq!(classify_risk(0.0), RiskLevel::Safe);
        assert_eq!(classify_risk(0.60), RiskLevel::Warning);
        assert_eq!(classify_risk(0.80), RiskLevel::Danger);
        assert_eq!(classify_risk(0.95), RiskLevel::Critical);
        assert_eq!(classify_risk(0.59), RiskLevel::Safe);
    }

    #[test]
    fn aggregate_is_max() {
        assert_eq!(aggregate_risks(&[0.1, 0.9, 0.4]), 0.9);
        assert_eq!(aggregate_risks(&[]), 0.0);
    }

    // assembler: needs >= 2 samples.
    #[test]
    fn assembler_needs_two_samples() {
        let one = [UsageSample {
            used_percent: 10.0,
            reset_at: Some(10_000),
            window_minutes: Some(10_080),
            recorded_at: 100,
        }];
        assert!(compute_depletion_for_account(&one, 200).is_none());
    }

    // assembler: happy path — rising usage, reset in future => a forecast with a risk level.
    #[test]
    fn assembler_happy_path() {
        let now = 1_000_000;
        let samples = [
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
        let f = compute_depletion_for_account(&samples, now).expect("forecast");
        assert_eq!(f.used_percent, 50.0);
        assert!(f.rate_per_second > 0.0);
        assert_eq!(f.seconds_until_reset, 3600);
        // rate = (50-40)/600 = 0.016667 %/s; over 3600s => +60 => projected 110 => risk clamps 1.0
        assert_eq!(f.risk_level, RiskLevel::Critical);
    }

    // assembler: window already reset (seconds_until_reset == 0) => None (stale).
    #[test]
    fn assembler_reset_window_is_none() {
        let now = 1_000_000;
        let samples = [
            UsageSample {
                used_percent: 40.0,
                reset_at: Some(now - 10),
                window_minutes: Some(10_080),
                recorded_at: now - 600,
            },
            UsageSample {
                used_percent: 50.0,
                reset_at: Some(now - 5),
                window_minutes: Some(10_080),
                recorded_at: now,
            },
        ];
        // both reset_at differ (window_changed) => rate resets to None on the 2nd sample => None anyway;
        // use equal reset_at in the past to isolate the seconds_until_reset==0 branch:
        let samples2 = [
            UsageSample {
                used_percent: 40.0,
                reset_at: Some(now - 5),
                window_minutes: Some(10_080),
                recorded_at: now - 600,
            },
            UsageSample {
                used_percent: 50.0,
                reset_at: Some(now - 5),
                window_minutes: Some(10_080),
                recorded_at: now,
            },
        ];
        assert!(compute_depletion_for_account(&samples, now).is_none());
        assert!(compute_depletion_for_account(&samples2, now).is_none());
    }
}
