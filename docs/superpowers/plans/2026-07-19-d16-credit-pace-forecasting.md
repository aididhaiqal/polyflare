# D16 — WeeklyCreditPace + EWMA Depletion Forecasting Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give PolyFlare codex-lb's credit-depletion forecasting — a per-account EWMA burn-rate/risk estimator and a pool-wide "WeeklyCreditPace" discrete-event simulation — surfaced as content-free read-only dashboard/API data.

**Architecture:** Two new *pure* modules in `polyflare-core` (`depletion.rs` = the EWMA math; `weekly_pace.rs` = the pool sim + aggregation), fed by the usage-history time series PolyFlare already stores. `polyflare-core` is a leaf crate (no `polyflare-store` dep), so the pure modules take a core-local `UsageSample` input; the server (which depends on both) maps the store's `WindowUsage` rows to `UsageSample` and exposes the results via two admin-gated read endpoints plus a dashboard surface. Dashboard-only — **touches no routing code** (codex-lb's EWMA depletion and WeeklyCreditPace are never referenced under `app/core/balancer/`; the routing-facing `_weekly_pace_floor_pct` is a *different*, self-contained linear-schedule feature and is out of scope).

**Tech Stack:** Rust, sqlx (SQLite), axum 0.8, Vite+React dashboard (rust-embed). Ports codex-lb `app/core/usage/depletion.py`, `app/modules/usage/depletion_service.py::compute_depletion_for_account`, and `app/modules/dashboard/weekly_pace.py`.

## Global Constraints

- **Content-free forever:** every D16 output is numeric (percentages, credits, hours, burn rates, counts) + opaque `account_id` + risk/status/confidence enums. **NEVER surface `email`** or any conversation content — mirror the `/metrics` and `/api/*` discipline (the read structs must not even *have* an email field). This is inviolable.
- **Never log tokens/bearers.** No new code reads token blobs.
- **Additive only, no migration.** The `usage_history` schema already has every column D16 needs (`recorded_at`, `"window"`, `used_percent`, `reset_at`, `window_minutes`). The operator-configurable working-days + smoothing-minutes *settings columns* are the ONLY thing that would need a migration — they are **deferred** (v1 hardcodes `working_days = None` → the linear schedule, `smoothing_window_minutes = 30`).
- **New feature defaults on / zero-config** (it's read-only observability; nothing to gate). No new `POLYFLARE_*` env var in v1.
- **Faithful port.** Constants copied verbatim: `RISK_WARNING=0.60`, `RISK_DANGER=0.80`, `RISK_CRITICAL=0.95`, `DEFAULT_ALPHA=0.4`, `PRO_WEEKLY_CAPACITY_CREDITS=50400.0`, `RECENT_BURN_WINDOW=6h`, `MIN_FRESHNESS_SECONDS=300.0`, `FRESHNESS_MISSED_REFRESH_CYCLES=3.0`, pace-eligible statuses = `{active, rate_limited, quota_exceeded}`, `%/s → credits/hr` factor `= full_credits * 36.0` (i.e. `rate%/s * full/100 * 3600`).
- **Workspace stays fmt+clippy clean.** Run `cargo fmt --all` and `cargo clippy --all-targets` before each commit; the gate is `cargo test --workspace` + clippy + fmt-check.
- **Wedge fix is sacred** — no task touches `ObservingStream::poll_next`, continuity, or the executor path. D16 is read-side only.

---

## File Structure

- **Create** `crates/polyflare-core/src/depletion.rs` — pure EWMA depletion math + `compute_depletion_for_account` assembler (T1).
- **Modify** `crates/polyflare-core/src/lib.rs` — add `pub mod depletion;` and `pub mod weekly_pace;`.
- **Modify** `crates/polyflare-store/src/account.rs` — add `usage_history_full_since` (T2).
- **Create** `crates/polyflare-core/src/weekly_pace.rs` — pure pool sim primitives (T3) + `build_weekly_credit_pace` aggregation + report struct (T4).
- **Modify** `crates/polyflare-core/src/select.rs` — make `plan_capacity_secondary` `pub(crate)` so `weekly_pace.rs` reuses it (T4).
- **Modify** `crates/polyflare-server/src/read_api.rs` — add `forecast` field to the trends response + new `pace_handler` (T5).
- **Modify** `crates/polyflare-server/src/app.rs` — register `GET /api/pace` on the admin-gated `/api/*` router (T5).
- **Create** `crates/polyflare-server/tests/pace_e2e.rs` — content-safety + shape e2e (T5).
- **Modify** dashboard `src/` (api.ts, queries.ts, a new Pace card + risk badge) + rebuild `dist/` (T6).

---

## Task 1: Pure EWMA depletion core (`depletion.rs`)

**Files:**
- Create: `crates/polyflare-core/src/depletion.rs`
- Modify: `crates/polyflare-core/src/lib.rs` (add `pub mod depletion;`)
- Test: inline `#[cfg(test)] mod tests` in `depletion.rs`

**Interfaces:**
- Consumes: nothing (leaf, pure).
- Produces (later tasks + server rely on these exact names/types):
  - `pub const DEFAULT_ALPHA: f64 = 0.4;` (and `RISK_WARNING/DANGER/CRITICAL`)
  - `pub struct EwmaState { pub rate: Option<f64>, pub last_used_percent: f64, pub last_timestamp: f64, pub last_reset_at: Option<i64> }` (derive `Debug, Clone, Copy, PartialEq`)
  - `pub fn ewma_update(state: Option<EwmaState>, used_percent: f64, timestamp: f64, alpha: f64, reset_at: Option<i64>) -> EwmaState`
  - `pub fn compute_burn_rate(current_rate: f64, remaining_percent: f64, seconds_until_reset: f64) -> f64`
  - `pub fn compute_depletion_risk(used_percent: f64, rate_per_second: f64, seconds_until_reset: f64) -> f64`
  - `pub fn compute_safe_usage_percent(seconds_elapsed: f64, total_window_seconds: f64) -> f64`
  - `pub enum RiskLevel { Safe, Warning, Danger, Critical }` (derive `Debug, Clone, Copy, PartialEq, Eq, serde::Serialize`, `#[serde(rename_all = "lowercase")]`)
  - `pub fn classify_risk(risk: f64) -> RiskLevel`
  - `pub fn aggregate_risks(risks: &[f64]) -> f64`
  - `pub struct UsageSample { pub used_percent: f64, pub reset_at: Option<i64>, pub window_minutes: Option<i64>, pub recorded_at: i64 }` (derive `Debug, Clone, Copy`)
  - `pub struct DepletionForecast { pub risk: f64, pub risk_level: RiskLevel, pub rate_per_second: f64, pub burn_rate: f64, pub used_percent: f64, pub safe_usage_percent: f64, pub seconds_until_reset: i64, pub seconds_until_exhaustion: Option<f64>, pub projected_exhaustion_at: Option<i64> }` (derive `Debug, Clone, Copy, serde::Serialize`)
  - `pub fn compute_depletion_for_account(samples: &[UsageSample], now: i64) -> Option<DepletionForecast>`

- [ ] **Step 1: Write the failing tests**

Add to `crates/polyflare-core/src/depletion.rs` (the module will not exist yet — that's expected; write the whole file's test block first via a stub-then-fill flow, but the canonical failing test is the numeric core). Create the file with just the test module and empty `use super::*;` targets:

```rust
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
        let s3 = ewma_update(Some(s2), 5.0, 20.0, DEFAULT_ALPHA, Some(5_000));  // drop
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
        assert_eq!(compute_safe_usage_percent(50.0, 0.0), 0.0);      // zero window guard
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
        let one = [UsageSample { used_percent: 10.0, reset_at: Some(10_000), window_minutes: Some(10_080), recorded_at: 100 }];
        assert!(compute_depletion_for_account(&one, 200).is_none());
    }

    // assembler: happy path — rising usage, reset in future => a forecast with a risk level.
    #[test]
    fn assembler_happy_path() {
        let now = 1_000_000;
        let samples = [
            UsageSample { used_percent: 40.0, reset_at: Some(now + 3600), window_minutes: Some(10_080), recorded_at: now - 600 },
            UsageSample { used_percent: 50.0, reset_at: Some(now + 3600), window_minutes: Some(10_080), recorded_at: now },
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
            UsageSample { used_percent: 40.0, reset_at: Some(now - 10), window_minutes: Some(10_080), recorded_at: now - 600 },
            UsageSample { used_percent: 50.0, reset_at: Some(now - 5), window_minutes: Some(10_080), recorded_at: now },
        ];
        // both reset_at differ (window_changed) => rate resets to None on the 2nd sample => None anyway;
        // use equal reset_at in the past to isolate the seconds_until_reset==0 branch:
        let samples2 = [
            UsageSample { used_percent: 40.0, reset_at: Some(now - 5), window_minutes: Some(10_080), recorded_at: now - 600 },
            UsageSample { used_percent: 50.0, reset_at: Some(now - 5), window_minutes: Some(10_080), recorded_at: now },
        ];
        assert!(compute_depletion_for_account(&samples, now).is_none());
        assert!(compute_depletion_for_account(&samples2, now).is_none());
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p polyflare-core --lib depletion 2>&1 | tail -20`
Expected: FAIL — the module has no non-test items yet (compile errors: `ewma_update` not found, etc.).

- [ ] **Step 3: Write the implementation** (prepend above the `#[cfg(test)]` block)

```rust
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
        return EwmaState { rate: None, last_used_percent: used_percent, last_timestamp: timestamp, last_reset_at: reset_at };
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
        return EwmaState { rate: None, last_used_percent: used_percent, last_timestamp: timestamp, last_reset_at: reset_at };
    }

    let delta_percent = used_percent - state.last_used_percent;
    let raw_rate = (delta_percent / dt).max(0.0);
    let rate = match state.rate {
        None => raw_rate,
        Some(prev) => alpha * raw_rate + (1.0 - alpha) * prev,
    };
    EwmaState { rate: Some(rate), last_used_percent: used_percent, last_timestamp: timestamp, last_reset_at: reset_at }
}

/// Dimensionless burn rate: current_rate / sustainable_rate, where sustainable = remaining%/secs.
/// `>1` = burning faster than budget. 0 if current_rate or secs is 0.
pub fn compute_burn_rate(current_rate: f64, remaining_percent: f64, seconds_until_reset: f64) -> f64 {
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
pub fn compute_depletion_risk(used_percent: f64, rate_per_second: f64, seconds_until_reset: f64) -> f64 {
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
    risks.iter().copied().fold(f64::NEG_INFINITY, f64::max).max(0.0).min(1.0).max(0.0)
        // simpler: max of slice, else 0. Guard empty:
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
pub fn compute_depletion_for_account(samples: &[UsageSample], now: i64) -> Option<DepletionForecast> {
    if samples.len() < 2 {
        return None;
    }
    let mut state: Option<EwmaState> = None;
    for s in samples {
        state = Some(ewma_update(state, s.used_percent, s.recorded_at as f64, DEFAULT_ALPHA, s.reset_at));
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

    let total_window_seconds = latest.window_minutes.map(|wm| (wm * 60) as f64).unwrap_or(0.0);
    let seconds_elapsed = (total_window_seconds - seconds_until_reset).max(0.0);

    let risk = compute_depletion_risk(used_percent, rate, seconds_until_reset);
    let risk_level = classify_risk(risk);
    let burn_rate = compute_burn_rate(rate, 100.0 - used_percent, seconds_until_reset);
    let safe_usage_percent = compute_safe_usage_percent(seconds_elapsed, total_window_seconds);

    let (seconds_until_exhaustion, projected_exhaustion_at) = if rate > 0.0 && seconds_until_reset > 0.0 {
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
```

**NOTE for the implementer:** the `aggregate_risks` body above is sketched awkwardly — implement it cleanly as: `if risks.is_empty() { 0.0 } else { risks.iter().copied().fold(f64::NEG_INFINITY, f64::max) }`. Keep the "max, else 0.0" semantics exactly.

Then add to `crates/polyflare-core/src/lib.rs`: `pub mod depletion;` (alphabetical order near `pub mod continuity;`).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p polyflare-core --lib depletion 2>&1 | tail -20`
Expected: PASS (all ~15 tests). Then `cargo clippy -p polyflare-core --all-targets 2>&1 | tail -5` clean, `cargo fmt --all`.

- [ ] **Step 5: Commit**

```bash
git add crates/polyflare-core/src/depletion.rs crates/polyflare-core/src/lib.rs
git commit -m "feat(core): pure EWMA credit-depletion core + per-account forecast assembler (D16 T1)"
```

---

## Task 2: Store — full usage-history time series read (`usage_history_full_since`)

**Files:**
- Modify: `crates/polyflare-store/src/account.rs` (add method near `usage_history_since`, ~line 508)
- Test: inline `#[cfg(test)] mod tests` in `account.rs`

**Interfaces:**
- Consumes: existing `WindowUsage { used_percent, reset_at, window_minutes, recorded_at }` (already defined, `account.rs:86-92`).
- Produces: `pub async fn usage_history_full_since(&self, account_id: &str, since_ts: i64) -> Result<Vec<(String, WindowUsage)>, StoreError>` — every primary/secondary row at/after `since_ts`, oldest-first, each paired with its window name. The existing `usage_history_since` (3-tuple, drops `reset_at`/`window_minutes`) stays as-is for the trend-point series; this new method carries the full tuple the EWMA + pace sim need.

- [ ] **Step 1: Write the failing test**

Add inside `account.rs`'s `#[cfg(test)] mod tests` (reuse the existing test harness pattern — look at the neighboring `usage_history_since` / `insert_usage_window` tests for the in-memory store setup helper):

```rust
#[tokio::test]
async fn usage_history_full_since_carries_reset_and_window() {
    let store = test_store().await; // reuse the module's existing helper
    let repo = store.accounts();
    // seed an account + two secondary rows with distinct reset_at/window_minutes
    seed_account(&repo, "acct-1").await; // reuse existing helper if present, else inline insert
    repo.insert_usage_window("acct-1", "secondary", 40.0, Some(9_000), Some(10_080), 1_000).await.unwrap();
    repo.insert_usage_window("acct-1", "secondary", 50.0, Some(9_000), Some(10_080), 1_600).await.unwrap();
    repo.insert_usage_window("acct-1", "primary", 12.0, Some(5_000), Some(300), 1_600).await.unwrap();

    let rows = repo.usage_history_full_since("acct-1", 0).await.unwrap();
    assert_eq!(rows.len(), 3);
    // oldest first
    assert_eq!(rows[0].0, "secondary");
    assert_eq!(rows[0].1.used_percent, 40.0);
    assert_eq!(rows[0].1.reset_at, Some(9_000));
    assert_eq!(rows[0].1.window_minutes, Some(10_080));
    assert_eq!(rows[0].1.recorded_at, 1_000);
    // since_ts filter excludes older rows
    let recent = repo.usage_history_full_since("acct-1", 1_500).await.unwrap();
    assert_eq!(recent.len(), 2);
    assert!(recent.iter().all(|(_, w)| w.recorded_at >= 1_500));
}
```

**Implementer note:** match the existing test module's actual setup helpers (`test_store`, account seeding). If `insert_usage_window`'s signature differs from `(account_id, window, used_percent, reset_at, window_minutes, recorded_at)`, adapt the calls — verify against `account.rs:357-379`.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p polyflare-store usage_history_full_since 2>&1 | tail -15`
Expected: FAIL — method `usage_history_full_since` not found.

- [ ] **Step 3: Write the implementation** (add after `usage_history_since`, ~`account.rs:523`)

```rust
    /// Every `usage_history` row for `account_id` at/after `since_ts` (unix seconds), oldest-first,
    /// each paired with its window name — the full tuple (`used_percent`, `reset_at`,
    /// `window_minutes`, `recorded_at`) the depletion EWMA + weekly-pace sim need (unlike
    /// [`usage_history_since`], which drops `reset_at`/`window_minutes` for the trend-point series).
    /// Only rows in a known window (`"primary"`/`"secondary"`) are returned.
    pub async fn usage_history_full_since(
        &self,
        account_id: &str,
        since_ts: i64,
    ) -> Result<Vec<(String, WindowUsage)>, StoreError> {
        let rows: Vec<(String, f64, Option<i64>, Option<i64>, i64)> = sqlx::query_as(
            "SELECT \"window\", used_percent, reset_at, window_minutes, recorded_at \
             FROM usage_history \
             WHERE account_id = ? AND recorded_at >= ? AND \"window\" IN ('primary', 'secondary') \
             ORDER BY recorded_at ASC",
        )
        .bind(account_id)
        .bind(since_ts)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(window, used_percent, reset_at, window_minutes, recorded_at)| {
                (window, WindowUsage { used_percent, reset_at, window_minutes, recorded_at })
            })
            .collect())
    }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p polyflare-store usage_history_full_since 2>&1 | tail -15`
Expected: PASS. Then `cargo clippy -p polyflare-store --all-targets` clean + `cargo fmt --all`.

- [ ] **Step 5: Commit**

```bash
git add crates/polyflare-store/src/account.rs
git commit -m "feat(store): usage_history_full_since (full window tuple time series for D16) (D16 T2)"
```

---

## Task 3: Pure weekly-pace sim primitives (`weekly_pace.rs`)

**Files:**
- Create: `crates/polyflare-core/src/weekly_pace.rs`
- Modify: `crates/polyflare-core/src/lib.rs` (add `pub mod weekly_pace;`)
- Test: inline `#[cfg(test)] mod tests`

**Interfaces:**
- Consumes: `crate::depletion::{EwmaState, ewma_update, UsageSample, DEFAULT_ALPHA}`.
- Produces (used by T4 in the same file — keep these `pub(crate)` unless T5 needs them, then `pub`):
  - `pub(crate) const RECENT_BURN_WINDOW_SECS: i64 = 6 * 3600;`
  - `pub(crate) const MIN_FRESHNESS_SECS: f64 = 300.0;`
  - `pub(crate) const FRESHNESS_MISSED_REFRESH_CYCLES: f64 = 3.0;`
  - `pub(crate) const PRO_WEEKLY_CAPACITY_CREDITS: f64 = 50_400.0;`
  - `pub(crate) struct SimAccount { pub full_credits: f64, pub balance_credits: f64, pub reset_at_ms: f64, pub window_ms: f64 }`
  - `pub(crate) struct Projection { pub projected_shortfall_credits: f64, pub projected_depletion_hours: Option<f64>, pub projected_minimum_remaining_credits: f64 }`
  - `pub(crate) fn recent_burn_rate_credits_per_hour(rows: &[UsageSample], full_credits: f64, now: i64) -> Option<f64>`
  - `pub(crate) fn smoothed_remaining_credits(rows: &[UsageSample], full_credits: f64, current_remaining_credits: f64, now: i64, smoothing_window_minutes: i64) -> f64`
  - `pub(crate) fn advance_reset_at(reset_at_ms: f64, window_ms: f64, now_ms: f64) -> f64`
  - `pub(crate) fn project_weekly_pool(accounts: &[SimAccount], now_ms: f64, forecast_burn_rate_credits_per_hour: Option<f64>) -> Projection`
  - `pub(crate) fn freshness_seconds(refresh_interval_secs: i64) -> f64`

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::depletion::UsageSample;

    fn sim(full: f64, bal: f64, reset_ms: f64, window_ms: f64) -> SimAccount {
        SimAccount { full_credits: full, balance_credits: bal, reset_at_ms: reset_ms, window_ms }
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
        let one = [UsageSample { used_percent: 10.0, reset_at: Some(now + 100), window_minutes: Some(10_080), recorded_at: now - 100 }];
        assert_eq!(recent_burn_rate_credits_per_hour(&one, 7560.0, now), None);
    }

    #[test]
    fn burn_rate_scales_percent_per_sec_to_credits_per_hour() {
        let now = 1_000_000;
        // two rows 600s apart, +10% => rate 0.016667 %/s; credits/hr = rate * full * 36
        let rows = [
            UsageSample { used_percent: 40.0, reset_at: Some(now + 3600), window_minutes: Some(10_080), recorded_at: now - 600 },
            UsageSample { used_percent: 50.0, reset_at: Some(now + 3600), window_minutes: Some(10_080), recorded_at: now },
        ];
        let r = recent_burn_rate_credits_per_hour(&rows, 7560.0, now).unwrap();
        // 0.0166667 * 7560 * 36 = 4536
        assert!((r - 4536.0).abs() < 1.0);
    }

    #[test]
    fn burn_rate_excludes_rows_older_than_6h() {
        let now = 1_000_000;
        let rows = [
            UsageSample { used_percent: 5.0, reset_at: Some(now + 3600), window_minutes: Some(10_080), recorded_at: now - 7 * 3600 }, // >6h old, dropped
            UsageSample { used_percent: 40.0, reset_at: Some(now + 3600), window_minutes: Some(10_080), recorded_at: now - 600 },
            UsageSample { used_percent: 50.0, reset_at: Some(now + 3600), window_minutes: Some(10_080), recorded_at: now },
        ];
        // only the two recent rows count -> same 4536 as above
        let r = recent_burn_rate_credits_per_hour(&rows, 7560.0, now).unwrap();
        assert!((r - 4536.0).abs() < 1.0);
    }

    #[test]
    fn smoothed_remaining_averages_recent_same_window() {
        let now = 1_000_000;
        let rows = [
            UsageSample { used_percent: 40.0, reset_at: Some(9_000), window_minutes: Some(10_080), recorded_at: now - 600 },
            UsageSample { used_percent: 50.0, reset_at: Some(9_000), window_minutes: Some(10_080), recorded_at: now },
        ];
        // full 1000 => remaining rows 600 and 500 => avg 550
        let s = smoothed_remaining_credits(&rows, 1000.0, 500.0, now, 30);
        assert!((s - 550.0).abs() < 1e-6);
    }

    #[test]
    fn smoothed_remaining_empty_returns_current() {
        let now = 1_000_000;
        assert_eq!(smoothed_remaining_credits(&[], 1000.0, 500.0, now, 30), 500.0);
    }

    #[test]
    fn freshness_is_max_300_and_3x_interval() {
        assert_eq!(freshness_seconds(600), 1800.0); // 600*3
        assert_eq!(freshness_seconds(10), 300.0);   // floor
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p polyflare-core --lib weekly_pace 2>&1 | tail -20`
Expected: FAIL — module items not defined.

- [ ] **Step 3: Write the implementation**

```rust
//! Pure pool-wide "WeeklyCreditPace" simulation primitives — faithful port of the sim in
//! codex-lb `app/modules/dashboard/weekly_pace.py`. No I/O, no state. v1 hardcodes
//! `working_days = None` (the linear schedule — codex-lb's own default), so the weekend-stepping
//! helpers are deliberately omitted; the operator-configurable working-days + smoothing settings
//! are a deferred follow-up (they'd need new settings columns). Dashboard-read-only; feeds no routing.

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
pub(crate) fn recent_burn_rate_credits_per_hour(rows: &[UsageSample], full_credits: f64, now: i64) -> Option<f64> {
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
        state = Some(ewma_update(state, r.used_percent, r.recorded_at as f64, DEFAULT_ALPHA, r.reset_at));
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
            let depletion_wait_ms = if burn_per_ms > 0.0 { bal / burn_per_ms } else { 0.0 };
            return Projection {
                projected_shortfall_credits: interval_burn - bal,
                projected_depletion_hours: Some((cursor_ms - now_ms + depletion_wait_ms) / 3_600_000.0),
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
```

**Implementer care point (adversarial-review crux):** `consume_balance` and the refill step both depend on soonest-reset ordering. The Python re-sorts inside `_consume_balance` AND at the top of each loop; the Rust `consume_balance` sorts an index vector locally (leaving the caller's slice order intact), while the loop sorts `sim` in place so `sim[0]` is the account being refilled. Verify by tracing the `refill_at_reset_survives` test: after `consume_balance`, `sim` is still sorted by reset_at from the loop-top sort (consume didn't reorder it), so `sim[0]` is correctly the soonest-reset account to refill. If you change `consume_balance` to sort `sim` in place, you must re-sort before the refill.

Add `pub mod weekly_pace;` to `lib.rs`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p polyflare-core --lib weekly_pace 2>&1 | tail -20`
Expected: PASS (all ~11 tests). Then clippy + fmt.

- [ ] **Step 5: Commit**

```bash
git add crates/polyflare-core/src/weekly_pace.rs crates/polyflare-core/src/lib.rs
git commit -m "feat(core): weekly-pace discrete-event pool sim primitives (D16 T3)"
```

---

## Task 4: Pace aggregation + report (`build_weekly_credit_pace`)

**Files:**
- Modify: `crates/polyflare-core/src/weekly_pace.rs` (add the public aggregation API + report struct)
- Modify: `crates/polyflare-core/src/select.rs` (change `fn plan_capacity_secondary` → `pub(crate) fn plan_capacity_secondary`)
- Test: inline tests in `weekly_pace.rs`

**Interfaces:**
- Consumes: T3 primitives, `crate::select::plan_capacity_secondary` (now `pub(crate)`), `crate::depletion::UsageSample`.
- Produces:
  - `pub struct PaceAccountInput { pub account_id: String, pub status_eligible: bool, pub full_credits: f64, pub used_percent: f64, pub reset_at: Option<i64>, pub window_minutes: Option<i64>, pub secondary_history: Vec<UsageSample> }`
  - `pub enum PaceStatus { OnTrack, Ahead, Behind, Danger }` (Serialize, `#[serde(rename_all="snake_case")]`)
  - `pub enum Confidence { High, Medium, Low }` (Serialize, `#[serde(rename_all="lowercase")]`)
  - `pub struct WeeklyCreditPaceReport { ... }` (Serialize — full field list below)
  - `pub fn build_weekly_credit_pace(accounts: &[PaceAccountInput], now: i64, refresh_interval_secs: i64, smoothing_window_minutes: i64) -> Option<WeeklyCreditPaceReport>`
  - `pub use crate::select::plan_capacity_secondary;` is NOT needed; the server derives `full_credits` itself (see T5). Keep the map reuse internal.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod agg_tests {
    use super::*;
    use crate::depletion::UsageSample;

    fn acct(id: &str, full: f64, used: f64, reset_at: i64, rows: Vec<UsageSample>) -> PaceAccountInput {
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
        UsageSample { used_percent: used, reset_at: Some(reset_at), window_minutes: Some(10_080), recorded_at }
    }

    #[test]
    fn none_when_no_eligible_fresh_accounts() {
        let now = 1_000_000;
        // stale: latest row far older than freshness cutoff
        let a = acct("a", 7560.0, 50.0, now + 3600, vec![row(50.0, now + 3600, now - 100_000)]);
        assert!(build_weekly_credit_pace(&[a], now, 600, 30).is_none());
    }

    #[test]
    fn ineligible_status_is_counted_inactive_not_paced() {
        let now = 1_000_000;
        let mut a = acct("a", 7560.0, 50.0, now + 3600, vec![row(40.0, now + 3600, now - 600), row(50.0, now + 3600, now)]);
        a.status_eligible = false;
        assert!(build_weekly_credit_pace(&[a], now, 600, 30).is_none());
    }

    #[test]
    fn happy_path_reports_used_percent_and_status() {
        let now = 1_000_000;
        let reset = now + 6 * 24 * 3600; // ~6 days out (near week start)
        let a = acct("a", 10_000.0, 50.0, reset, vec![row(40.0, reset, now - 600), row(50.0, reset, now)]);
        let r = build_weekly_credit_pace(&[a], now, 600, 30).expect("report");
        assert_eq!(r.account_count, 1);
        assert_eq!(r.total_full_credits, 10_000.0);
        // actual_used ~ 50% (remaining derived from used_percent)
        assert!((r.actual_used_percent - 50.0).abs() < 1e-6);
        // early in the window (reset ~6d out of 7d window) => scheduled_used_percent is low => actual > scheduled => "ahead" or "danger"
        assert!(r.forecast_burn_rate_credits_per_hour.is_some());
        assert!(matches!(r.confidence, Confidence::High | Confidence::Medium));
    }

    #[test]
    fn pool_shortfall_sets_danger_status() {
        let now = 1_000_000;
        let reset = now + 7 * 24 * 3600; // full window ahead => low scheduled use
        // steep burn: +40% over 600s on a small pool => huge credits/hr => shortfall
        let a = acct("a", 1000.0, 90.0, reset, vec![row(50.0, reset, now - 600), row(90.0, reset, now)]);
        let r = build_weekly_credit_pace(&[a], now, 600, 30).expect("report");
        assert!(r.projected_shortfall_credits > 0.0);
        assert_eq!(r.status, PaceStatus::Danger);
        assert!(r.pro_accounts_to_cover_over_plan.unwrap() >= 1);
    }
}
```

- [ ] **Step 2: Run to verify fail**

Run: `cargo test -p polyflare-core --lib weekly_pace::agg_tests 2>&1 | tail -20`
Expected: FAIL — `build_weekly_credit_pace` / `PaceAccountInput` not defined.

- [ ] **Step 3: Implement**

First flip the visibility in `select.rs`: change `fn plan_capacity_secondary(plan: &str) -> f64` to `pub(crate) fn plan_capacity_secondary(plan: &str) -> f64` (the server won't call it directly — it maps plan→capacity via the same logic — but keeping it `pub(crate)` documents the shared source; if the server needs it later, promote to `pub`). *If the implementer finds it cleaner for the server (T5) to call this, promote to `pub` and re-export from `lib.rs`.*

Then append to `weekly_pace.rs`:

```rust
use serde::Serialize;

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
        let actual_remaining = (full * (1.0 - a.used_percent.clamp(0.0, 100.0) / 100.0)).clamp(0.0, full);
        let window_ms = window_minutes as f64 * 60_000.0;
        let reset_at_ms = advance_reset_at(reset_at as f64 * 1000.0, window_ms, now_ms);

        // linear schedule (working_days = None): used fraction = clamp(elapsed/window, 0, 1).
        let window_start_ms = reset_at_ms - window_ms;
        let elapsed_ms = (now_ms - window_start_ms).clamp(0.0, window_ms);
        let used_schedule_fraction = if elapsed_ms <= 0.0 { 0.0 } else { elapsed_ms / window_ms };
        let expected_remaining = full * (1.0 - used_schedule_fraction);

        let account_rate = recent_burn_rate_credits_per_hour(&a.secondary_history, full, now);
        let smoothed_remaining = smoothed_remaining_credits(&a.secondary_history, full, actual_remaining, now, smoothing_window_minutes);

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

        sim_inputs.push(SimAccount { full_credits: full, balance_credits: actual_remaining, reset_at_ms, window_ms });
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
    let smoothed_schedule_gap_credits = (total_expected_remaining - total_smoothed_remaining).max(0.0);

    let forecast_rate = if rate_samples > 0 { Some(forecast_burn) } else { None };
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
    let pro_equivalent = if shortfall > 0.0 { Some(shortfall / PRO_WEEKLY_CAPACITY_CREDITS) } else { None };
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
```

**Note:** `WeeklyCreditPaceReport` derives `Copy` — verify it stays `Copy` (all fields are `f64`/`Option<f64>`/`i64`/enums). If any field becomes non-`Copy`, drop the `Copy` derive.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p polyflare-core --lib weekly_pace 2>&1 | tail -20`
Expected: PASS (T3 + T4 tests). Then `cargo clippy -p polyflare-core --all-targets` clean + fmt.

- [ ] **Step 5: Commit**

```bash
git add crates/polyflare-core/src/weekly_pace.rs crates/polyflare-core/src/select.rs
git commit -m "feat(core): build_weekly_credit_pace aggregation + report (D16 T4)"
```

---

## Task 5: Read-API wiring — per-account forecast + `GET /api/pace`

**Files:**
- Modify: `crates/polyflare-server/src/read_api.rs` (add `forecast` to `TrendsView` + build it in `account_trends_handler`; add `pace_handler` + views)
- Modify: `crates/polyflare-server/src/app.rs` (register `GET /api/pace` on the admin-gated `/api/*` router, ~line 292)
- Test: create `crates/polyflare-server/tests/pace_e2e.rs`

**Interfaces:**
- Consumes: `polyflare_core::depletion::{compute_depletion_for_account, UsageSample, DepletionForecast}`, `polyflare_core::weekly_pace::{build_weekly_credit_pace, PaceAccountInput, WeeklyCreditPaceReport}`, the T2 store method `usage_history_full_since`, `latest_usage`, `state.account_cache.snapshots`, `state.runtime.overlay`, `state.store.accounts()`.
- Produces: `GET /api/pace` (admin-gated) returning the `WeeklyCreditPaceReport` (or `{ "pace": null }` when `None`), and a `forecast: Option<DepletionForecast>` field on the existing `/api/accounts/{id}/trends` response.

**Content-safety (inviolable):** the new views carry ONLY numeric fields + `account_id` (opaque) + enums. Do NOT add an `email` field. The e2e test seeds a real email + token and asserts they never appear in either endpoint's response body.

- [ ] **Step 1: Write the failing e2e test**

Create `crates/polyflare-server/tests/pace_e2e.rs`. Model it on `crates/polyflare-server/tests/dashboard_api.rs` / the `/api/*` admin-gate tests (reuse their `build_app` harness + admin-token helper). It must cover: (a) `/api/pace` requires admin (401 keyless under enforcement), (b) with a seeded account that has ≥2 fresh secondary rows, `/api/pace` returns `200` with a numeric `total_full_credits` and a `status`, (c) content-safety: a seeded sentinel email + token never appear in `/api/pace` or `/api/accounts/{id}/trends` bodies.

```rust
// Skeleton — adapt harness calls to the real helpers in dashboard_api.rs.
#[tokio::test]
async fn pace_requires_admin_and_is_content_safe() {
    // 1. build_app with enforcement + admin token (reuse the dashboard_api harness)
    // 2. seed an account "acct-1" with email "sentinel-email@example.test" and a token blob "sk-SENTINEL-TOKEN"
    // 3. insert >=2 fresh secondary usage rows (recorded_at = now-600, now; used 40 -> 50; reset_at now+6d)
    // 4. GET /api/pace with NO auth -> 401
    // 5. GET /api/pace WITH admin bearer -> 200; body has "total_full_credits" and "status"
    // 6. assert body does NOT contain "sentinel-email@example.test" nor "sk-SENTINEL-TOKEN"
    // 7. GET /api/accounts/acct-1/trends WITH admin bearer -> 200; body has "forecast";
    //    assert body does NOT contain the sentinel email/token
}
```

Write it out fully against the actual harness (the implementer must open `dashboard_api.rs` to copy the exact `build_app`, admin-header, and seeding helpers — do not invent helper names).

- [ ] **Step 2: Run to verify fail**

Run: `cargo test -p polyflare-server --test pace_e2e 2>&1 | tail -25`
Expected: FAIL — `/api/pace` route 404s and `forecast` field absent.

- [ ] **Step 3: Implement**

In `read_api.rs`:

1. Add the `forecast` field to `TrendsView`:
```rust
#[derive(Serialize)]
struct TrendsView {
    account_id: String,
    primary: Vec<Point>,
    secondary: Vec<Point>,
    forecast: Option<polyflare_core::depletion::DepletionForecast>,
}
```

2. In `account_trends_handler`, after building `primary`/`secondary`, compute the secondary forecast from the FULL history (use the T2 method — reuse the same lookback):
```rust
    // Build the per-account secondary-window depletion forecast (content-free).
    let full_rows = state
        .store
        .accounts()
        .usage_history_full_since(&id, now - TRENDS_LOOKBACK_SECS)
        .await
        .unwrap_or_default();
    let samples: Vec<polyflare_core::depletion::UsageSample> = full_rows
        .iter()
        .filter(|(w, _)| w == "secondary")
        .map(|(_, u)| polyflare_core::depletion::UsageSample {
            used_percent: u.used_percent,
            reset_at: u.reset_at,
            window_minutes: u.window_minutes,
            recorded_at: u.recorded_at,
        })
        .collect();
    let forecast = polyflare_core::depletion::compute_depletion_for_account(&samples, now);
```
and add `forecast` to the returned `TrendsView`.

3. Add the pace handler + views (near the pools handler):
```rust
/// `GET /api/pace` — pool-wide WeeklyCreditPace forecast (admin-gated). `{ "pace": null }` when
/// there is no eligible, fresh, positive-capacity account. Content-free: credits/percentages/hours/
/// counts + status/confidence enums only — NEVER any email or conversation content.
pub async fn pace_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let now = unix_now();
    let snapshots = match state.account_cache.snapshots(&state.store).await {
        Ok(s) => s,
        Err(_) => return Response::error(),
    };
    let mut snapshots = (*snapshots).clone();
    state.runtime.overlay(&mut snapshots, now);

    let mut inputs: Vec<polyflare_core::weekly_pace::PaceAccountInput> = Vec::new();
    for snap in &snapshots {
        // secondary-window capacity: per-account override else plan-derived (same rule select.rs uses).
        let full_credits = snap
            .capacity_credits
            .unwrap_or_else(|| polyflare_core::select::plan_capacity_secondary(&snap.plan_type));
        // latest secondary usage for reset_at + window_minutes; full history for burn/smoothing.
        let full_rows = state
            .store
            .accounts()
            .usage_history_full_since(&snap.id, now - polyflare_core::weekly_pace::RECENT_BURN_WINDOW_SECS)
            .await
            .unwrap_or_default();
        let secondary_history: Vec<polyflare_core::depletion::UsageSample> = full_rows
            .iter()
            .filter(|(w, _)| w == "secondary")
            .map(|(_, u)| polyflare_core::depletion::UsageSample {
                used_percent: u.used_percent,
                reset_at: u.reset_at,
                window_minutes: u.window_minutes,
                recorded_at: u.recorded_at,
            })
            .collect();
        let latest = state.store.accounts().latest_usage(&snap.id).await.ok().and_then(|u| u.secondary);
        let (reset_at, window_minutes) = latest.map(|w| (w.reset_at, w.window_minutes)).unwrap_or((None, None));
        inputs.push(polyflare_core::weekly_pace::PaceAccountInput {
            account_id: snap.id.clone(),
            status_eligible: matches!(snap.status.as_str(), "active" | "rate_limited" | "quota_exceeded"),
            full_credits,
            used_percent: snap.secondary_used_percent,
            reset_at,
            window_minutes,
            secondary_history,
        });
    }

    // 600s = the usage poller's REFRESH_INTERVAL; 30 = codex-lb's default smoothing window.
    let report = polyflare_core::weekly_pace::build_weekly_credit_pace(&inputs, now, 600, 30);
    Response::ok(PaceView { pace: report })
}

#[derive(Serialize)]
struct PaceView {
    pace: Option<polyflare_core::weekly_pace::WeeklyCreditPaceReport>,
}
```

**Implementer notes:**
- `plan_capacity_secondary` and `RECENT_BURN_WINDOW_SECS` must be reachable: promote `plan_capacity_secondary` to `pub` in `select.rs` and re-export as needed (`pub use` in `lib.rs` or `pub fn`), and change `RECENT_BURN_WINDOW_SECS` from `pub(crate)` to `pub` in `weekly_pace.rs`. Adjust the T3/T4 visibility accordingly (this is the one cross-crate reach — make exactly these two items `pub`, leave the rest `pub(crate)`).
- Confirm `AccountSnapshot` exposes `id: String`, `status: String`, `secondary_used_percent: f64`, `capacity_credits: Option<f64>`, `plan_type: String` (verified: `types.rs:286-309`). `snap.id` may be an `AccountId` newtype — use `snap.id.as_str()`/`.to_string()` as the type requires.
- The per-account `usage_history_full_since` call inside the loop is one extra SELECT per account on an admin-gated, human-triggered dashboard read (not the hot proxy path) — acceptable. Do NOT add caching in v1.

4. Register the route in `app.rs` (add to the admin-gated `/api/*` router block, near line 292):
```rust
        .route("/api/pace", get(crate::read_api::pace_handler))
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p polyflare-server --test pace_e2e 2>&1 | tail -25`
Expected: PASS. Then `cargo test -p polyflare-server 2>&1 | tail -15` (no regressions), clippy + fmt.

- [ ] **Step 5: Commit**

```bash
git add crates/polyflare-server/src/read_api.rs crates/polyflare-server/src/app.rs crates/polyflare-server/tests/pace_e2e.rs crates/polyflare-core/src/weekly_pace.rs crates/polyflare-core/src/select.rs crates/polyflare-core/src/lib.rs
git commit -m "feat(server): /api/pace + per-account depletion forecast on trends, content-safe (D16 T5)"
```

---

## Task 6: Dashboard surface (pace card + per-account risk)

**Files:**
- Modify: `crates/polyflare-server/dashboard/src/lib/api.ts` (types + fetchers for `/api/pace` + the trends `forecast`)
- Modify: `crates/polyflare-server/dashboard/src/lib/queries.ts` (a `usePace` query, ~30s poll)
- Modify: the Overview page (or a new small Pace card component) to render the pool pace summary; add a risk badge to the accounts/trends view.
- Rebuild + commit `crates/polyflare-server/dashboard/dist/`.

**Interfaces:**
- Consumes: `GET /api/pace` → `{ pace: WeeklyCreditPaceReport | null }`; `GET /api/accounts/{id}/trends` → now includes `forecast: DepletionForecast | null`.

**Constraints:** dark-ops aesthetic, flare-amber accent, **NO emoji**, content-safety notice consistent with sibling pages. Mirror the existing Sessions/Requests page patterns (look at `src/pages/Sessions.tsx` and `src/lib/queries.ts` for the exact query + styling conventions before writing).

- [ ] **Step 1: Add the API types + fetchers** in `api.ts`:
```ts
export type RiskLevel = "safe" | "warning" | "danger" | "critical";
export interface DepletionForecast {
  risk: number;
  risk_level: RiskLevel;
  rate_per_second: number;
  burn_rate: number;
  used_percent: number;
  safe_usage_percent: number;
  seconds_until_reset: number;
  seconds_until_exhaustion: number | null;
  projected_exhaustion_at: number | null;
}
export type PaceStatus = "on_track" | "ahead" | "behind" | "danger";
export interface WeeklyCreditPaceReport {
  total_full_credits: number;
  total_actual_remaining_credits: number;
  actual_used_percent: number;
  scheduled_used_percent: number;
  delta_percent: number;
  projected_shortfall_credits: number;
  projected_depletion_hours: number | null;
  forecast_burn_rate_credits_per_hour: number | null;
  scheduled_burn_rate_credits_per_hour: number;
  status: PaceStatus;
  confidence: "high" | "medium" | "low";
  account_count: number;
  stale_account_count: number;
  inactive_account_count: number;
  // (include the remaining fields as needed by the card)
}
export async function fetchPace(): Promise<{ pace: WeeklyCreditPaceReport | null }> {
  return apiGet("/api/pace"); // reuse the existing apiGet helper (verify its name)
}
```

- [ ] **Step 2: Add the query** in `queries.ts` (mirror an existing `useX` query, 30s `refetchInterval`).

- [ ] **Step 3: Render a Pace card** on the Overview page: total capacity, actual-vs-scheduled used %, a status pill (`on_track`=muted, `ahead`=success, `behind`=warn, `danger`=critical/amber), projected depletion hours (or "no shortfall"), confidence, and the stale/inactive counts. Add a small risk badge (`safe/warning/danger/critical`) wherever per-account trends render, driven by `trends.forecast?.risk_level`. Keep it compact and consistent with sibling pages; add the standard content-safety footer note.

- [ ] **Step 4: Build + verify + commit the dist**
```bash
cd crates/polyflare-server/dashboard && bun run build 2>&1 | tail -5
cd - && cargo build -p polyflare-server 2>&1 | tail -3   # re-embed
cargo test -p polyflare-server --test dashboard 2>&1 | tail -8   # SPA serving still green
git add crates/polyflare-server/dashboard/src crates/polyflare-server/dashboard/dist
git commit -m "feat(dashboard): weekly-pace card + per-account depletion risk badge (D16 T6)"
```

---

## Self-Review

**1. Spec coverage** (against the porting note + codex-lb source):
- EWMA core (alpha 0.4 d(used%)/dt, reset-on-drop, burn-rate, projected exhaustion, safe/warning/danger/critical 0.60/0.80/0.95) → **T1** ✓ (all six functions + thresholds + assembler).
- WeeklyCreditPace discrete-event sim → **T3** (sim primitives) + **T4** (aggregation) ✓.
- "per-account weekly usage rows (capacity/remaining/reset/window)" → capacity plan-derived (T4/T5), reset/window/used% from `usage_history` via **T2** ✓.
- "additive, grow the usage schema feature-by-feature" → no migration; T2 is a read method only ✓.
- Surfacing (dashboard-only) → **T5** (API) + **T6** (UI) ✓.
- Deferred faithfully & documented: operator-configurable working-days + smoothing-minutes settings columns (would need a migration) — v1 hardcodes `working_days=None`, `smoothing=30`. The in-memory EWMA cache (pure optimization) omitted — stateless rebuild. These are the only omissions; both are non-behavioral for the linear-schedule default.

**2. Placeholder scan:** the `aggregate_risks` body in T1 Step 3 is intentionally flagged with an implementer note to write it cleanly (`if empty {0.0} else {max}`) — not a silent placeholder. The T5 e2e and T6 UI steps reference "the existing harness/helpers" by name-to-be-verified (`apiGet`, `build_app`, `test_store`, `seed_account`) — these are real helpers the implementer must read from the cited sibling files rather than invent; each step names the exact file to copy from. No TBD/TODO/"handle edge cases" placeholders remain.

**3. Type consistency:** `UsageSample` (T1) is consumed unchanged by T3/T4/T5. `DepletionForecast` (T1) surfaced by T5/T6. `WeeklyCreditPaceReport`/`PaceStatus`/`Confidence`/`PaceAccountInput` (T4) consumed by T5/T6. `usage_history_full_since -> Vec<(String, WindowUsage)>` (T2) consumed by T5. Visibility: `plan_capacity_secondary` + `RECENT_BURN_WINDOW_SECS` promoted to `pub` in T5 (called out explicitly). Field names match across T1→T6 (`rate_per_second`, `seconds_until_reset`, `projected_depletion_hours`, `smoothed_delta_percent`, etc.).

**Adversarial-review crux (flag for the reviewer):** **T3 `project_weekly_pool`** — the discrete-event sim's consume/refill ordering (soonest-reset-first drain + refilling `sim[0]` after the loop-top sort) is the one place a subtle port error hides. Trace it against the codex-lb `_project_weekly_pool`/`_consume_balance` with the `refill_at_reset_survives` + `exhausts_before_reset` tests. **T5 content-safety** — assert the sentinel email/token never reach either endpoint body (the inviolable).
