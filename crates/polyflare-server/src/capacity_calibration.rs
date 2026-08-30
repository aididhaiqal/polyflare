//! Periodically re-derives each account's real capacity from observed quota burn, so the selector
//! stops weighting five differently-sized accounts as if they were identical.
//!
//! The estimator itself is pure ([`polyflare_core::capacity_estimate`]); this is the I/O half —
//! gathering samples from the store, and caching the result so the per-request snapshot path never
//! touches history.
//!
//! # Why it is periodic rather than per-request
//!
//! `assemble_snapshots` runs on the request path. Deriving burn needs a scan of `usage_history`
//! joined against cumulative `request_log` work — cheap every few minutes, absurd per request.
//!
//! # Modes
//!
//! Off by default. `shadow` logs what it WOULD apply while changing no routing, which is how this
//! should be run first: capacity weighting moves live traffic between accounts, and three fixes in
//! this area on 2026-08-29 each looked right until real traffic disagreed.

use std::collections::HashMap;
use std::sync::RwLock;

use polyflare_core::capacity_estimate::{
    capacity_from_burn, observed_burn, reference_burn, CapacitySample,
};
use polyflare_core::select::plan_capacity_secondary;
use polyflare_store::Store;

/// How the derived capacities are used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CalibrationMode {
    /// Derive nothing; the selector keeps the per-plan constant.
    Off,
    /// Derive and log, but hand the selector nothing. Proves the numbers before they move traffic.
    Shadow,
    /// Derive and apply.
    On,
}

impl CalibrationMode {
    pub fn from_env(raw: Option<&str>) -> Self {
        match raw.map(str::trim) {
            Some(v) if v.eq_ignore_ascii_case("on") || v == "1" => CalibrationMode::On,
            Some(v) if v.eq_ignore_ascii_case("shadow") => CalibrationMode::Shadow,
            // Anything else — absent, empty, misspelt — leaves routing exactly as it is today.
            // A typo must never silently start moving traffic.
            _ => CalibrationMode::Off,
        }
    }
}

/// Trailing span of history each pass considers. Long enough to cross a weekly window roll (so a
/// freshly-reset account still has evidence), short enough that a plan change is picked up in days
/// rather than weeks.
const LOOKBACK_SECS: i64 = 3 * 24 * 3600;

/// The weekly window, in minutes, as `usage_history` records it.
const WEEKLY_WINDOW_MINUTES: i64 = 10080;

/// The process-wide cache the snapshot path reads. A global rather than an `AppState` field for
/// the same reason `reactive_auth::REPLICA_MODE` and the session governor are: the reader sits deep
/// in `assemble_snapshots`, and threading a parameter through the account-cache layers to reach it
/// would touch far more code than the feature is worth.
pub static CAPACITIES: std::sync::LazyLock<CapacityCache> =
    std::sync::LazyLock::new(CapacityCache::default);

/// The configured mode, read once at startup.
pub static MODE: std::sync::LazyLock<CalibrationMode> = std::sync::LazyLock::new(|| {
    CalibrationMode::from_env(
        std::env::var("POLYFLARE_CAPACITY_CALIBRATION")
            .ok()
            .as_deref(),
    )
});

/// Derived capacities, read by the snapshot path.
#[derive(Default)]
pub struct CapacityCache {
    by_account: RwLock<HashMap<String, f64>>,
}

impl CapacityCache {
    pub fn get(&self, account_id: &str) -> Option<f64> {
        self.by_account
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(account_id)
            .copied()
    }

    pub fn replace(&self, next: HashMap<String, f64>) {
        *self.by_account.write().unwrap_or_else(|e| e.into_inner()) = next;
    }

    pub fn snapshot(&self) -> HashMap<String, f64> {
        self.by_account
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

/// One account's derived capacity plus the evidence behind it, for logging and the API.
#[derive(Debug, Clone)]
pub struct DerivedCapacity {
    pub account_id: String,
    pub plan_capacity: f64,
    pub derived_capacity: f64,
    pub percent_per_mtok: f64,
    pub observed_percent: f64,
    pub observed_mtok: f64,
}

/// Re-derive every account's capacity from the trailing window.
///
/// Accounts whose evidence is too thin are absent from the result, which leaves the selector on the
/// plan constant for them — the honest outcome rather than a guess from noise.
pub async fn derive_capacities(store: &Store) -> Result<Vec<DerivedCapacity>, sqlx::Error> {
    let cutoff = crate::capacity_calibration::unix_now() - LOOKBACK_SECS;

    // `used_percent` readings paired with the cumulative work the account had served by then.
    // Correlated subquery: fine at this cadence, never on the request path.
    let rows: Vec<(String, f64, Option<i64>, f64, String)> = sqlx::query_as(
        r#"
        SELECT u.account_id,
               u.used_percent,
               u.reset_at,
               (SELECT COALESCE(SUM(r.input_tokens), 0)
                  FROM request_log r
                 WHERE r.account_id = u.account_id
                   AND r.requested_at <= u.recorded_at
                   AND r.requested_at > ?1) AS cumulative_work,
               COALESCE(a.plan_type, '')
          FROM usage_history u
          JOIN accounts a ON a.id = u.account_id
         WHERE u.window_minutes = ?2
           AND u.recorded_at > ?1
         ORDER BY u.account_id, u.recorded_at
        "#,
    )
    .bind(cutoff)
    .bind(WEEKLY_WINDOW_MINUTES)
    .fetch_all(store.pool())
    .await?;

    let mut samples: HashMap<String, (Vec<CapacitySample>, String)> = HashMap::new();
    for (account_id, used_percent, reset_at, cumulative_work, plan_type) in rows {
        let entry = samples
            .entry(account_id)
            .or_insert_with(|| (Vec::new(), plan_type));
        entry.0.push(CapacitySample {
            used_percent,
            cumulative_work,
            reset_at,
        });
    }

    // Burn first for every account, so the pool reference is a median over the whole pool rather
    // than something each account is scaled against individually.
    let mut burns: Vec<(String, f64, f64, f64, String)> = Vec::new();
    for (account_id, (series, plan_type)) in &samples {
        if let Some(burn) = observed_burn(series) {
            burns.push((
                account_id.clone(),
                burn.percent_per_work,
                burn.observed_percent,
                burn.observed_work,
                plan_type.clone(),
            ));
        }
    }

    let Some(reference) = reference_burn(&burns.iter().map(|b| b.1).collect::<Vec<_>>()) else {
        return Ok(Vec::new());
    };

    let mut out = Vec::with_capacity(burns.len());
    for (account_id, burn, observed_percent, observed_work, plan_type) in burns {
        let plan_capacity = plan_capacity_secondary(&plan_type);
        let Some(derived) = capacity_from_burn(burn, reference, plan_capacity) else {
            continue;
        };
        out.push(DerivedCapacity {
            account_id,
            plan_capacity,
            derived_capacity: derived,
            percent_per_mtok: burn * 1_000_000.0,
            observed_percent,
            observed_mtok: observed_work / 1_000_000.0,
        });
    }
    out.sort_by(|a, b| b.percent_per_mtok.total_cmp(&a.percent_per_mtok));
    Ok(out)
}

/// Run one pass and, unless shadowing, publish it. Logs the full table either way — content-free
/// (account ids and numbers), and it is the only record of why routing weights moved.
pub async fn run_pass(store: &Store, cache: &CapacityCache, mode: CalibrationMode) {
    if mode == CalibrationMode::Off {
        return;
    }
    let derived = match derive_capacities(store).await {
        Ok(d) => d,
        Err(error) => {
            tracing::warn!(%error, "capacity calibration pass failed; keeping previous capacities");
            return;
        }
    };
    if derived.is_empty() {
        tracing::info!(
            target: "capacity_calibration",
            "no account had enough evidence; every account keeps its plan capacity"
        );
        return;
    }

    for d in &derived {
        tracing::info!(
            target: "capacity_calibration",
            account_id = %d.account_id,
            plan_capacity = d.plan_capacity,
            derived_capacity = d.derived_capacity,
            factor = d.derived_capacity / d.plan_capacity,
            percent_per_mtok = d.percent_per_mtok,
            evidence_percent = d.observed_percent,
            evidence_mtok = d.observed_mtok,
            applied = mode == CalibrationMode::On,
            "capacity derived from observed burn"
        );
    }

    if mode == CalibrationMode::On {
        cache.replace(
            derived
                .into_iter()
                .map(|d| (d.account_id, d.derived_capacity))
                .collect(),
        );
    }
}

/// How often capacities are re-derived. Quota moves over hours, so this only needs to be well
/// inside a weekly window — frequent enough to notice a plan change within a day, rare enough that
/// the correlated scan never matters.
const PASS_INTERVAL_SECS: u64 = 15 * 60;

/// Start the periodic pass. A no-op when calibration is off, so the default build spawns nothing.
pub fn spawn_capacity_calibration(state: std::sync::Arc<crate::app::AppState>) {
    let mode = *MODE;
    if mode == CalibrationMode::Off {
        return;
    }
    tracing::info!(
        target: "capacity_calibration",
        ?mode,
        interval_secs = PASS_INTERVAL_SECS,
        "per-account capacity calibration enabled"
    );
    tokio::spawn(async move {
        // A first pass immediately, so a restart does not spend 15 minutes on plan constants.
        run_pass(&state.store, &CAPACITIES, mode).await;
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(PASS_INTERVAL_SECS));
        ticker.tick().await; // the immediate first tick, already served above
        loop {
            ticker.tick().await;
            run_pass(&state.store, &CAPACITIES, mode).await;
        }
    });
}

fn unix_now() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_defaults_to_off_and_never_starts_on_a_typo() {
        assert_eq!(CalibrationMode::from_env(None), CalibrationMode::Off);
        assert_eq!(CalibrationMode::from_env(Some("")), CalibrationMode::Off);
        // A misspelling must not silently begin moving live traffic.
        assert_eq!(
            CalibrationMode::from_env(Some("shdow")),
            CalibrationMode::Off
        );
        assert_eq!(
            CalibrationMode::from_env(Some("true")),
            CalibrationMode::Off
        );
    }

    #[test]
    fn mode_parses_the_two_active_settings() {
        assert_eq!(
            CalibrationMode::from_env(Some("shadow")),
            CalibrationMode::Shadow
        );
        assert_eq!(
            CalibrationMode::from_env(Some("SHADOW")),
            CalibrationMode::Shadow
        );
        assert_eq!(CalibrationMode::from_env(Some("on")), CalibrationMode::On);
        assert_eq!(CalibrationMode::from_env(Some("1")), CalibrationMode::On);
    }

    #[test]
    fn cache_round_trips_and_reports_absent_accounts() {
        let cache = CapacityCache::default();
        assert_eq!(cache.get("nobody"), None);
        cache.replace(HashMap::from([("acct".to_string(), 28_604.0)]));
        assert_eq!(cache.get("acct"), Some(28_604.0));
        assert_eq!(
            cache.get("nobody"),
            None,
            "an unmeasured account stays on its plan capacity"
        );
    }
}
