//! Resolve stored usage windows into the two kinds the dashboard, selector, and gate reason about
//! — the **5h** and **weekly** limits — by each window's actual DURATION, not by the slot
//! (`"primary"`/`"secondary"`) it happens to be stored in.
//!
//! Why: upstream moves a window between slots. Right now (a Codex promo with no 5h limit) it emits
//! the *weekly* limit in the `primary` slot and stops sending `secondary` entirely; when the 5h
//! limit returns, `primary` becomes the 5h window again. Keying off the slot therefore mislabels
//! the data (the live weekly shows up under a "5h" heading) and renders a slot upstream abandoned
//! as if it were current (the stale `secondary`). Classifying by duration fixes the label, and a
//! freshness cutoff (`stale`) stops abandoned windows from reading as live.

use polyflare_store::{UsageSnapshot, WindowUsage};

/// A window whose latest row is older than this many seconds is STALE — upstream stopped sending it
/// (or the server/token has been failing), so it must not render as current. 30 min tolerates a
/// few missed refresh cycles (the loop runs every 600s) without flagging healthy data.
pub const STALE_AFTER_SECS: i64 = 1800;

/// A window shorter than this (in minutes) is the 5h limit; at or above it, the weekly limit. The
/// real windows are 300 min (5h) and 10080 min (weekly); any split strictly between them works.
const SHORT_WINDOW_MAX_MINUTES: i64 = 1440;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowKind {
    FiveHour,
    Weekly,
}

/// One resolved limit window as its consumers want it: the usage percent, its reset epoch, and
/// whether the data is stale (last refresh older than [`STALE_AFTER_SECS`]).
#[derive(Debug, Clone)]
pub struct ResolvedWindow {
    pub used_percent: f64,
    pub window_minutes: Option<i64>,
    pub reset_at: Option<i64>,
    pub recorded_at: i64,
    pub stale: bool,
}

/// The 5h + weekly windows resolved by duration. Either is `None` when upstream isn't reporting a
/// window of that kind at all (e.g. `five_hour` during the current no-5h-limit promo).
#[derive(Debug, Clone, Default)]
pub struct ResolvedUsage {
    pub five_hour: Option<ResolvedWindow>,
    pub weekly: Option<ResolvedWindow>,
}

/// Classify a window by its duration, falling back to its slot only when the duration is unknown
/// (`window_minutes` absent) — then the historical slot convention (primary = 5h, secondary =
/// weekly) is the best available guess.
fn classify(window: &WindowUsage, slot_is_primary: bool) -> WindowKind {
    match window.window_minutes {
        Some(m) if m < SHORT_WINDOW_MAX_MINUTES => WindowKind::FiveHour,
        Some(_) => WindowKind::Weekly,
        None if slot_is_primary => WindowKind::FiveHour,
        None => WindowKind::Weekly,
    }
}

/// Resolve the slot-keyed snapshot into 5h + weekly windows. For each kind, among the windows that
/// classify to it, the one with the newest `recorded_at` wins — so a live window in an unexpected
/// slot beats a stale one in the "expected" slot. `now` is unix-epoch seconds (drives `stale`).
pub fn resolve(usage: &UsageSnapshot, now: i64) -> ResolvedUsage {
    let mut candidates: Vec<(WindowKind, &WindowUsage)> = Vec::new();
    if let Some(w) = &usage.primary {
        candidates.push((classify(w, true), w));
    }
    if let Some(w) = &usage.secondary {
        candidates.push((classify(w, false), w));
    }

    let pick = |kind: WindowKind| -> Option<ResolvedWindow> {
        candidates
            .iter()
            .filter(|(k, _)| *k == kind)
            .max_by_key(|(_, w)| w.recorded_at)
            .map(|(_, w)| ResolvedWindow {
                used_percent: w.used_percent,
                window_minutes: w.window_minutes,
                reset_at: w.reset_at,
                recorded_at: w.recorded_at,
                stale: now - w.recorded_at > STALE_AFTER_SECS,
            })
    };

    ResolvedUsage {
        five_hour: pick(WindowKind::FiveHour),
        weekly: pick(WindowKind::Weekly),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn win(used: f64, reset: i64, minutes: Option<i64>, recorded_at: i64) -> WindowUsage {
        WindowUsage {
            used_percent: used,
            reset_at: Some(reset),
            window_minutes: minutes,
            recorded_at,
        }
    }

    const NOW: i64 = 1_000_000;
    const FRESH: i64 = NOW - 60; // 1 min ago
    const OLD: i64 = NOW - 300_000; // ~3.5 days ago

    #[test]
    fn promo_state_weekly_in_primary_slot_no_5h() {
        // Current upstream: weekly (10080 min) lives in the primary slot and is fresh; the
        // secondary slot holds only the old imported weekly. Expect: live weekly, no 5h.
        let usage = UsageSnapshot {
            primary: Some(win(44.0, 555, Some(10080), FRESH)),
            secondary: Some(win(55.0, 111, Some(10080), OLD)),
        };
        let r = resolve(&usage, NOW);
        assert!(r.five_hour.is_none(), "no 300-min window exists → no 5h");
        let weekly = r
            .weekly
            .expect("weekly resolved from whichever slot holds it");
        assert_eq!(
            weekly.used_percent, 44.0,
            "the FRESH weekly (primary) wins, not the stale one"
        );
        assert_eq!(weekly.reset_at, Some(555));
        assert!(!weekly.stale);
    }

    #[test]
    fn normal_state_5h_in_primary_weekly_in_secondary() {
        // When the 5h limit returns to its usual slot, both resolve to their natural kinds.
        let usage = UsageSnapshot {
            primary: Some(win(20.0, 300, Some(300), FRESH)),
            secondary: Some(win(60.0, 999, Some(10080), FRESH)),
        };
        let r = resolve(&usage, NOW);
        assert_eq!(r.five_hour.unwrap().used_percent, 20.0);
        assert_eq!(r.weekly.unwrap().used_percent, 60.0);
    }

    #[test]
    fn only_imported_data_is_flagged_stale() {
        // No live refresh yet: the single (old, imported) weekly is surfaced but marked stale.
        let usage = UsageSnapshot {
            primary: Some(win(30.0, 111, Some(10080), OLD)),
            secondary: None,
        };
        let r = resolve(&usage, NOW);
        assert!(r.five_hour.is_none());
        let weekly = r.weekly.unwrap();
        assert!(weekly.stale, "imported-only data must not read as live");
        assert_eq!(weekly.used_percent, 30.0);
    }

    #[test]
    fn unknown_duration_falls_back_to_slot() {
        let usage = UsageSnapshot {
            primary: Some(win(10.0, 1, None, FRESH)),
            secondary: Some(win(20.0, 2, None, FRESH)),
        };
        let r = resolve(&usage, NOW);
        assert_eq!(
            r.five_hour.unwrap().used_percent,
            10.0,
            "primary slot → 5h fallback"
        );
        assert_eq!(
            r.weekly.unwrap().used_percent,
            20.0,
            "secondary slot → weekly fallback"
        );
    }

    #[test]
    fn empty_snapshot_resolves_to_nothing() {
        let r = resolve(&UsageSnapshot::default(), NOW);
        assert!(r.five_hour.is_none() && r.weekly.is_none());
    }
}
