//! Live-editable Settings subsystem Task 2: `RuntimeSettings`, an atomic holder for the 10
//! live-editable `ServeConfig` fields. Task 1's pure `clamp_<field>` fns (`crate::config`) are the
//! single source of truth for each field's bound; `set` re-validates every write through the SAME
//! fns the boot path (`ServeConfig::from_env`) uses — never a second copy of a bound that can
//! drift. A later task wires this into `AppState` + the settings PATCH endpoint; this module only
//! holds the live values and validates writes.
//!
//! **Ordering:** every load/store is `Ordering::Relaxed` — each of the 10 fields is an
//! independently atomic cell with no cross-field invariant that needs a stronger ordering, save
//! one: `starvation_heartbeat`'s clamp reads the CURRENT `starvation_wait_budget` atomic (via
//! [`RuntimeSettings::starvation_wait_budget`]) at the moment of the `set` call, not the value
//! `RuntimeSettings` was constructed with — so a heartbeat write is always bounded by whatever
//! budget is visible at that instant, and a later budget edit does not retroactively re-validate
//! an already-stored heartbeat.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use crate::config::{
    clamp_inflight_penalty_pct, clamp_max_account_attempts, clamp_request_log_retention_days,
    clamp_starvation_heartbeat_secs, clamp_starvation_wait_budget_secs,
    clamp_stream_idle_timeout_secs, clamp_usage_history_retention_days, clamp_wake_jitter_ms,
    ServeConfig,
};

/// A raw value submitted to [`RuntimeSettings::set`], before clamping. The three variants mirror
/// the three atomic families this module holds: every `u32`- or `u64`-backed field (counts, ms,
/// secs) takes `U64` and narrows to the field's width; the one `f64`-backed field
/// (`inflight_penalty_pct`) takes `F64`; the two flags take `Bool`. `set` rejects a value
/// submitted against the wrong field's kind (`SettingsError::WrongKind`) rather than silently
/// coercing it (e.g. truncating an `F64` into an integer field).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SettingValue {
    U64(u64),
    F64(f64),
    Bool(bool),
}

/// Errors [`RuntimeSettings::set`] can return. Generic `Display` — carries only the
/// caller-supplied key name, never any other detail.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SettingsError {
    /// `key` is not one of the 10 live-editable fields (either misspelled, or a real
    /// `ServeConfig` field that is not live-editable).
    #[error("unknown or non-live setting key: {0}")]
    UnknownKey(String),
    /// `key` is a live-editable field, but the `SettingValue` variant supplied does not match
    /// that field's kind (e.g. a `Bool` for a numeric field).
    #[error("wrong value kind for setting key: {0}")]
    WrongKind(String),
}

/// Atomic holder for the 10 live-editable `ServeConfig` fields (live-editable Settings subsystem,
/// Task 2). Seeded once from an already-clamped `ServeConfig`
/// (`ServeConfig::from_env` already ran every field through its `clamp_<field>` fn at boot), then
/// mutated only via [`RuntimeSettings::set`], which re-validates through the SAME clamp fns —
/// one source of truth for both the boot path and the (later) live PATCH path. A later task wires
/// this into `AppState` + the settings endpoints.
pub struct RuntimeSettings {
    max_account_attempts: AtomicU32,
    starvation_wait_budget: AtomicU32,
    starvation_heartbeat: AtomicU32,
    wake_jitter_ms: AtomicU64,
    stream_idle_timeout: AtomicU64,
    /// Stored as `f64::to_bits` — there is no `AtomicF64` in `std`; see
    /// [`RuntimeSettings::inflight_penalty_pct`] for the `from_bits` read side.
    inflight_penalty_pct: AtomicU64,
    soft_drain_enabled: AtomicBool,
    request_log_retention_days: AtomicU32,
    usage_history_retention_days: AtomicU32,
    live_logs: AtomicBool,
    // Restart-required settings are immutable snapshots of what this process actually applied at
    // boot. PATCH persists a configured value for the next boot but deliberately does not mutate
    // these fields, allowing GET /api/settings to report an honest pending-restart state.
    client_websocket_enabled: bool,
    http_requests_use_upstream_websocket: bool,
    http_upstream_websocket_ping: bool,
    websocket_idle_ping_secs: u64,
    websocket_idle_budget_secs: u64,
}

/// Named-field seed for [`RuntimeSettings::new_from_fields`] — see that fn's doc. Field names/
/// types mirror `ServeConfig`'s (and the former `AppState`'s) 10 live-editable fields exactly.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RuntimeSettingsFields {
    pub max_account_attempts: u32,
    pub starvation_wait_budget: Duration,
    pub starvation_heartbeat: Duration,
    pub wake_jitter_ms: u64,
    pub stream_idle_timeout: Duration,
    pub inflight_penalty_pct: f64,
    pub soft_drain_enabled: bool,
    pub request_log_retention_days: u32,
    pub usage_history_retention_days: u32,
    pub live_logs: bool,
}

/// Narrow a `u64` submitted via `SettingValue::U64` into the `u32` an atomic field actually
/// stores. A value above `u32::MAX` saturates rather than truncates — an absurd input (e.g.
/// `u32::MAX + 2`) must saturate to the largest representable count and then hit the field's own
/// `clamp_<field>` ceiling, never silently wrap around to a small, unrelated number.
fn narrow_u32(n: u64) -> u32 {
    u32::try_from(n).unwrap_or(u32::MAX)
}

impl RuntimeSettings {
    /// Seed every atomic from an already-clamped `ServeConfig` (durations → `.as_secs()`, the
    /// `f64` field → `to_bits`). Does not itself re-clamp — `ServeConfig::from_env` (or the
    /// caller, for a hand-built config) is responsible for that; `set` is what re-validates on
    /// every LIVE write after construction.
    pub fn new(cfg: &ServeConfig) -> Self {
        Self {
            max_account_attempts: AtomicU32::new(cfg.max_account_attempts),
            starvation_wait_budget: AtomicU32::new(cfg.starvation_wait_budget.as_secs() as u32),
            starvation_heartbeat: AtomicU32::new(cfg.starvation_heartbeat.as_secs() as u32),
            wake_jitter_ms: AtomicU64::new(cfg.wake_jitter_ms),
            stream_idle_timeout: AtomicU64::new(cfg.stream_idle_timeout.as_secs()),
            inflight_penalty_pct: AtomicU64::new(cfg.inflight_penalty_pct.to_bits()),
            soft_drain_enabled: AtomicBool::new(cfg.soft_drain_enabled),
            request_log_retention_days: AtomicU32::new(cfg.request_log_retention_days),
            usage_history_retention_days: AtomicU32::new(cfg.usage_history_retention_days),
            live_logs: AtomicBool::new(cfg.live_logs),
            client_websocket_enabled: cfg.client_websocket_enabled,
            http_requests_use_upstream_websocket: cfg.http_requests_use_upstream_websocket,
            http_upstream_websocket_ping: cfg.http_upstream_websocket_ping,
            websocket_idle_ping_secs: cfg
                .websocket_idle_policy
                .ping_interval
                .map_or(0, |duration| duration.as_secs()),
            websocket_idle_budget_secs: cfg.websocket_idle_policy.idle_budget.as_secs(),
        }
    }

    /// Task 4 test seam: build directly from the 10 field values, bypassing `ServeConfig` — for
    /// the many test harnesses across the crate that construct `AppState` by hand (rather than
    /// through `ServeConfig::from_env`/`serve`) and therefore have no `ServeConfig` to pass to
    /// [`RuntimeSettings::new`]. Named-field input (`RuntimeSettingsFields`), not positional
    /// args, so a large mechanical call-site migration can't silently transpose two same-typed
    /// fields (e.g. the two `Duration`s or the two retention `u32`s). Does not itself clamp —
    /// mirrors `new`'s contract: callers pass already-in-range values, exactly as those same call
    /// sites already did as bare `AppState` field literals before this task.
    pub fn new_from_fields(f: RuntimeSettingsFields) -> Self {
        Self {
            max_account_attempts: AtomicU32::new(f.max_account_attempts),
            starvation_wait_budget: AtomicU32::new(f.starvation_wait_budget.as_secs() as u32),
            starvation_heartbeat: AtomicU32::new(f.starvation_heartbeat.as_secs() as u32),
            wake_jitter_ms: AtomicU64::new(f.wake_jitter_ms),
            stream_idle_timeout: AtomicU64::new(f.stream_idle_timeout.as_secs()),
            inflight_penalty_pct: AtomicU64::new(f.inflight_penalty_pct.to_bits()),
            soft_drain_enabled: AtomicBool::new(f.soft_drain_enabled),
            request_log_retention_days: AtomicU32::new(f.request_log_retention_days),
            usage_history_retention_days: AtomicU32::new(f.usage_history_retention_days),
            live_logs: AtomicBool::new(f.live_logs),
            client_websocket_enabled: true,
            http_requests_use_upstream_websocket: false,
            http_upstream_websocket_ping: false,
            websocket_idle_ping_secs: 30,
            websocket_idle_budget_secs: 1500,
        }
    }

    pub fn max_account_attempts(&self) -> u32 {
        self.max_account_attempts.load(Ordering::Relaxed)
    }

    pub fn starvation_wait_budget(&self) -> Duration {
        Duration::from_secs(self.starvation_wait_budget.load(Ordering::Relaxed) as u64)
    }

    pub fn starvation_heartbeat(&self) -> Duration {
        Duration::from_secs(self.starvation_heartbeat.load(Ordering::Relaxed) as u64)
    }

    pub fn wake_jitter_ms(&self) -> u64 {
        self.wake_jitter_ms.load(Ordering::Relaxed)
    }

    pub fn stream_idle_timeout(&self) -> Duration {
        Duration::from_secs(self.stream_idle_timeout.load(Ordering::Relaxed))
    }

    pub fn inflight_penalty_pct(&self) -> f64 {
        f64::from_bits(self.inflight_penalty_pct.load(Ordering::Relaxed))
    }

    pub fn soft_drain_enabled(&self) -> bool {
        self.soft_drain_enabled.load(Ordering::Relaxed)
    }

    pub fn request_log_retention_days(&self) -> u32 {
        self.request_log_retention_days.load(Ordering::Relaxed)
    }

    pub fn usage_history_retention_days(&self) -> u32 {
        self.usage_history_retention_days.load(Ordering::Relaxed)
    }

    pub fn live_logs(&self) -> bool {
        self.live_logs.load(Ordering::Relaxed)
    }

    pub fn client_websocket_enabled(&self) -> bool {
        self.client_websocket_enabled
    }

    pub fn http_requests_use_upstream_websocket(&self) -> bool {
        self.http_requests_use_upstream_websocket
    }

    pub fn http_upstream_websocket_ping(&self) -> bool {
        self.http_upstream_websocket_ping
    }

    pub fn websocket_idle_ping_secs(&self) -> u64 {
        self.websocket_idle_ping_secs
    }

    pub fn websocket_idle_budget_secs(&self) -> u64 {
        self.websocket_idle_budget_secs
    }

    /// Validate + apply a live write to one of the 10 fields. Unknown/non-live `key` ⇒
    /// `UnknownKey`; a known key with the wrong `SettingValue` kind ⇒ `WrongKind`; otherwise the
    /// value is clamped via that field's `clamp_<field>` fn (`starvation_heartbeat` clamps
    /// against the CURRENT `starvation_wait_budget`, read live — see the module doc's Ordering
    /// note), stored, and the clamped value is returned stringified so the caller can persist
    /// exactly what was applied.
    pub fn set(&self, key: &str, raw: SettingValue) -> Result<String, SettingsError> {
        match key {
            "max_account_attempts" => {
                let n = narrow_u32(expect_u64(key, raw)?);
                let clamped = clamp_max_account_attempts(n);
                self.max_account_attempts.store(clamped, Ordering::Relaxed);
                Ok(clamped.to_string())
            }
            "starvation_wait_budget" => {
                let n = narrow_u32(expect_u64(key, raw)?);
                let clamped = clamp_starvation_wait_budget_secs(n);
                self.starvation_wait_budget
                    .store(clamped, Ordering::Relaxed);
                Ok(clamped.to_string())
            }
            "starvation_heartbeat" => {
                let n = narrow_u32(expect_u64(key, raw)?);
                let budget_secs = self.starvation_wait_budget().as_secs() as u32;
                let clamped = clamp_starvation_heartbeat_secs(n, budget_secs);
                self.starvation_heartbeat.store(clamped, Ordering::Relaxed);
                Ok(clamped.to_string())
            }
            "wake_jitter_ms" => {
                let n = expect_u64(key, raw)?;
                let clamped = clamp_wake_jitter_ms(n);
                self.wake_jitter_ms.store(clamped, Ordering::Relaxed);
                Ok(clamped.to_string())
            }
            "stream_idle_timeout" => {
                let n = expect_u64(key, raw)?;
                let clamped = clamp_stream_idle_timeout_secs(n);
                self.stream_idle_timeout.store(clamped, Ordering::Relaxed);
                Ok(clamped.to_string())
            }
            "inflight_penalty_pct" => {
                let n = expect_f64(key, raw)?;
                let clamped = clamp_inflight_penalty_pct(n);
                self.inflight_penalty_pct
                    .store(clamped.to_bits(), Ordering::Relaxed);
                Ok(clamped.to_string())
            }
            "soft_drain_enabled" => {
                let b = expect_bool(key, raw)?;
                self.soft_drain_enabled.store(b, Ordering::Relaxed);
                Ok(b.to_string())
            }
            "request_log_retention_days" => {
                let n = narrow_u32(expect_u64(key, raw)?);
                let clamped = clamp_request_log_retention_days(n);
                self.request_log_retention_days
                    .store(clamped, Ordering::Relaxed);
                Ok(clamped.to_string())
            }
            "usage_history_retention_days" => {
                let n = narrow_u32(expect_u64(key, raw)?);
                let clamped = clamp_usage_history_retention_days(n);
                self.usage_history_retention_days
                    .store(clamped, Ordering::Relaxed);
                Ok(clamped.to_string())
            }
            "live_logs" => {
                let b = expect_bool(key, raw)?;
                self.live_logs.store(b, Ordering::Relaxed);
                Ok(b.to_string())
            }
            _ => Err(SettingsError::UnknownKey(key.to_string())),
        }
    }
}

/// Task 4 (wiring): parse a persisted `settings` table row's string `value` into the
/// [`SettingValue`] variant [`RuntimeSettings::set`] expects for `key`, per that field's kind
/// (see `SettingValue`'s doc for the three families). Pure and total: an unknown `key` (not one of
/// the 10 live-editable fields) or a `value` that fails to parse as that field's kind both yield
/// `None` — the caller ([`overlay_persisted_settings`]) treats `None` as "skip this row," never a
/// panic, and never a partial/best-guess apply. A row that PARSES but is out of range (e.g.
/// `max_account_attempts=99999`) is NOT this function's concern — it returns `Some`, and `set`'s
/// own `clamp_<field>` re-validation (already the single source of truth for every bound) handles
/// that at apply time, exactly as it does for a live PATCH.
pub fn parse_setting_value(key: &str, s: &str) -> Option<SettingValue> {
    match key {
        "soft_drain_enabled" | "live_logs" => s.parse::<bool>().ok().map(SettingValue::Bool),
        "inflight_penalty_pct" => s.parse::<f64>().ok().map(SettingValue::F64),
        "max_account_attempts"
        | "starvation_wait_budget"
        | "starvation_heartbeat"
        | "wake_jitter_ms"
        | "stream_idle_timeout"
        | "request_log_retention_days"
        | "usage_history_retention_days" => s.parse::<u64>().ok().map(SettingValue::U64),
        _ => None,
    }
}

/// Task 4 (wiring): the startup overlay — apply every persisted `settings` row in `overlay` onto
/// `rs`, layering DB overrides (from a prior live PATCH, a later task) on top of the env/file-
/// resolved defaults `rs` was already seeded with ([`RuntimeSettings::new`]). Each row is parsed
/// via [`parse_setting_value`] and, if it parses, applied via `rs.set` (re-validated/clamped
/// exactly like a live PATCH — never a second, divergent bound). A row that fails to parse
/// (unknown key, or a value of the wrong shape for its field) is skipped — `set`'s own `Result` is
/// also ignored here (a `WrongKind`/`UnknownKey` from `set` can't actually occur once
/// `parse_setting_value` has already matched `key` to the right `SettingValue` variant, but
/// discarding it rather than `unwrap`ing keeps this function infallible against any future
/// divergence between the two). Content-free: only the key name is ever logged, never the value —
/// mirrors every other content-safety chokepoint in this crate, even though these particular
/// values are non-secret config knobs, not conversation content.
///
/// Applied in `read_api::LIVE_KEYS_ORDER` — the SAME fixed canonical order (with
/// `starvation_wait_budget` before `starvation_heartbeat`) `write_api::patch_settings_handler`
/// uses for a live PATCH — never `overlay`'s own `HashMap` iteration order, which is
/// nondeterministic across runs. Without this, a persisted budget+heartbeat pair applied
/// heartbeat-first would clamp against the still-seeded default budget rather than the
/// about-to-be-applied persisted one, settling on a different (wrong) heartbeat depending on
/// hash-iteration luck — see `crate::runtime_settings`'s module doc's Ordering note. A key in
/// `overlay` that isn't one of the 10 live keys is never visited (there is no persisted row for
/// it to skip past), matching `parse_setting_value`'s `None` for an unknown key.
pub fn overlay_persisted_settings(rs: &RuntimeSettings, overlay: &HashMap<String, String>) {
    for key in crate::read_api::LIVE_KEYS_ORDER {
        let Some(raw) = overlay.get(*key) else {
            continue;
        };
        match parse_setting_value(key, raw) {
            Some(v) => {
                let _ = rs.set(key, v);
            }
            None => {
                tracing::warn!(key = %key, "settings overlay: skipping unparseable persisted row");
            }
        }
    }
}

/// Kind-check helper for `set`'s integer-family fields: only `SettingValue::U64` is accepted —
/// see the `SettingValue` doc for why an `F64`/`Bool` submitted against a `U64` field is a hard
/// `WrongKind` rejection, never a silent coercion.
fn expect_u64(key: &str, raw: SettingValue) -> Result<u64, SettingsError> {
    match raw {
        SettingValue::U64(n) => Ok(n),
        _ => Err(SettingsError::WrongKind(key.to_string())),
    }
}

/// Kind-check helper for `set`'s one `f64`-backed field (`inflight_penalty_pct`): only
/// `SettingValue::F64` is accepted.
fn expect_f64(key: &str, raw: SettingValue) -> Result<f64, SettingsError> {
    match raw {
        SettingValue::F64(n) => Ok(n),
        _ => Err(SettingsError::WrongKind(key.to_string())),
    }
}

/// Kind-check helper for `set`'s two flag fields: only `SettingValue::Bool` is accepted.
fn expect_bool(key: &str, raw: SettingValue) -> Result<bool, SettingsError> {
    match raw {
        SettingValue::Bool(b) => Ok(b),
        _ => Err(SettingsError::WrongKind(key.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ServeConfig;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::time::Duration;

    /// A `ServeConfig` with distinct, easily-recognizable values for each of the 10 live-editable
    /// fields (so a seeding bug that swaps two fields is caught, not silently masked by equal
    /// values). The other (non-live) fields are filled with harmless placeholders — this module
    /// never reads them.
    fn test_config() -> ServeConfig {
        ServeConfig {
            bind_addr: "127.0.0.1:0".to_string(),
            upstream_base_url: String::new(),
            anthropic_upstream_base_url: String::new(),
            auth_base_url: String::new(),
            db_path: PathBuf::from(":memory:"),
            key_path: PathBuf::from(":memory:"),
            continuity_watchdog: Duration::from_secs(1),
            capture_fingerprint_path: None,
            routing_strategy: Default::default(),
            pool_strategies: HashMap::new(),
            admin_token: None,
            live_logs: false,
            http_requests_use_upstream_websocket: false,
            client_websocket_enabled: false,
            websocket_idle_policy: crate::ws_relay::WsRelayIdlePolicy::default(),
            http_upstream_websocket_ping: false,
            max_account_attempts: 5,
            starvation_wait_budget: Duration::from_secs(120),
            starvation_heartbeat: Duration::from_secs(15),
            allow_unauthenticated_remote: false,
            stream_idle_timeout: Duration::from_secs(45),
            soft_drain_enabled: true,
            wake_jitter_ms: 250,
            inflight_penalty_pct: 3.5,
            admission_limits: Default::default(),
            request_log_retention_days: 30,
            usage_history_retention_days: 45,
            model_catalog_ttl_secs: 3600,
            model_catalog_enabled: true,
        }
    }

    #[test]
    fn new_seeds_every_getter_from_config() {
        let cfg = test_config();
        let rs = RuntimeSettings::new(&cfg);
        assert_eq!(rs.max_account_attempts(), 5);
        assert_eq!(rs.starvation_wait_budget(), Duration::from_secs(120));
        assert_eq!(rs.starvation_heartbeat(), Duration::from_secs(15));
        assert_eq!(rs.wake_jitter_ms(), 250);
        assert_eq!(rs.stream_idle_timeout(), Duration::from_secs(45));
        assert_eq!(rs.inflight_penalty_pct(), 3.5);
        assert!(rs.soft_drain_enabled());
        assert_eq!(rs.request_log_retention_days(), 30);
        assert_eq!(rs.usage_history_retention_days(), 45);
        assert!(!rs.live_logs());
        assert!(!rs.client_websocket_enabled());
        assert!(!rs.http_requests_use_upstream_websocket());
        assert!(!rs.http_upstream_websocket_ping());
        assert_eq!(rs.websocket_idle_ping_secs(), 30);
        assert_eq!(rs.websocket_idle_budget_secs(), 1500);
    }

    #[test]
    fn persisted_websocket_settings_become_the_effective_boot_snapshot() {
        let mut cfg = test_config();
        let values = HashMap::from([
            ("client_websocket_enabled".to_string(), "true".to_string()),
            (
                "http_requests_use_upstream_websocket".to_string(),
                "true".to_string(),
            ),
            (
                "http_upstream_websocket_ping".to_string(),
                "true".to_string(),
            ),
            ("websocket_idle_ping_secs".to_string(), "1".to_string()),
            (
                "websocket_idle_budget_secs".to_string(),
                "90000".to_string(),
            ),
            // A deprecated alias must not override its canonical replacement.
            ("ws_upstream".to_string(), "false".to_string()),
        ]);

        crate::config::overlay_persisted_websocket_settings(&mut cfg, &values);
        let rs = RuntimeSettings::new(&cfg);

        assert!(rs.client_websocket_enabled());
        assert!(rs.http_requests_use_upstream_websocket());
        assert!(rs.http_upstream_websocket_ping());
        assert_eq!(rs.websocket_idle_ping_secs(), 5);
        assert_eq!(rs.websocket_idle_budget_secs(), 86_400);
    }

    #[test]
    fn set_max_account_attempts_zero_clamps_to_one() {
        let rs = RuntimeSettings::new(&test_config());
        let stored = rs
            .set("max_account_attempts", SettingValue::U64(0))
            .unwrap();
        assert_eq!(stored, "1");
        assert_eq!(rs.max_account_attempts(), 1);
    }

    #[test]
    fn set_max_account_attempts_passes_through_in_range() {
        let rs = RuntimeSettings::new(&test_config());
        let stored = rs
            .set("max_account_attempts", SettingValue::U64(7))
            .unwrap();
        assert_eq!(stored, "7");
        assert_eq!(rs.max_account_attempts(), 7);
    }

    #[test]
    fn set_starvation_wait_budget_above_max_clamps_to_three_hundred() {
        let rs = RuntimeSettings::new(&test_config());
        let stored = rs
            .set("starvation_wait_budget", SettingValue::U64(9999))
            .unwrap();
        assert_eq!(stored, "300");
        assert_eq!(rs.starvation_wait_budget(), Duration::from_secs(300));
    }

    #[test]
    fn set_starvation_wait_budget_zero_is_the_disable_lever() {
        let rs = RuntimeSettings::new(&test_config());
        let stored = rs
            .set("starvation_wait_budget", SettingValue::U64(0))
            .unwrap();
        assert_eq!(stored, "0");
        assert_eq!(rs.starvation_wait_budget(), Duration::ZERO);
    }

    #[test]
    fn set_starvation_heartbeat_above_budget_clamps_to_current_budget() {
        // test_config seeds starvation_wait_budget = 120s.
        let rs = RuntimeSettings::new(&test_config());
        let stored = rs
            .set("starvation_heartbeat", SettingValue::U64(9999))
            .unwrap();
        assert_eq!(stored, "120");
        assert_eq!(rs.starvation_heartbeat(), Duration::from_secs(120));
    }

    #[test]
    fn set_starvation_heartbeat_clamps_against_the_live_budget_not_the_original_config() {
        let rs = RuntimeSettings::new(&test_config());
        // Lower the budget first via `set`, not the original config value.
        rs.set("starvation_wait_budget", SettingValue::U64(10))
            .unwrap();
        let stored = rs
            .set("starvation_heartbeat", SettingValue::U64(9999))
            .unwrap();
        assert_eq!(stored, "10");
        assert_eq!(rs.starvation_heartbeat(), Duration::from_secs(10));
    }

    #[test]
    fn set_wake_jitter_ms_above_max_clamps_to_thirty_thousand() {
        let rs = RuntimeSettings::new(&test_config());
        let stored = rs
            .set("wake_jitter_ms", SettingValue::U64(999_999))
            .unwrap();
        assert_eq!(stored, "30000");
        assert_eq!(rs.wake_jitter_ms(), 30_000);
    }

    #[test]
    fn set_stream_idle_timeout_above_max_clamps_to_one_hour() {
        let rs = RuntimeSettings::new(&test_config());
        let stored = rs
            .set("stream_idle_timeout", SettingValue::U64(999_999))
            .unwrap();
        assert_eq!(stored, "3600");
        assert_eq!(rs.stream_idle_timeout(), Duration::from_secs(3600));
    }

    #[test]
    fn set_inflight_penalty_pct_ninety_nine_clamps_to_fifty() {
        let rs = RuntimeSettings::new(&test_config());
        let stored = rs
            .set("inflight_penalty_pct", SettingValue::F64(99.0))
            .unwrap();
        assert_eq!(stored, "50");
        assert_eq!(rs.inflight_penalty_pct(), 50.0);
    }

    #[test]
    fn set_inflight_penalty_pct_negative_clamps_to_zero() {
        let rs = RuntimeSettings::new(&test_config());
        let stored = rs
            .set("inflight_penalty_pct", SettingValue::F64(-5.0))
            .unwrap();
        assert_eq!(stored, "0");
        assert_eq!(rs.inflight_penalty_pct(), 0.0);
    }

    #[test]
    fn set_inflight_penalty_pct_round_trips_through_bits() {
        let rs = RuntimeSettings::new(&test_config());
        rs.set("inflight_penalty_pct", SettingValue::F64(12.75))
            .unwrap();
        assert_eq!(rs.inflight_penalty_pct(), 12.75);
        assert_eq!(rs.inflight_penalty_pct().to_bits(), 12.75_f64.to_bits());
    }

    #[test]
    fn set_request_log_retention_days_above_max_clamps_to_thirty_six_fifty() {
        let rs = RuntimeSettings::new(&test_config());
        let stored = rs
            .set("request_log_retention_days", SettingValue::U64(99_999))
            .unwrap();
        assert_eq!(stored, "3650");
        assert_eq!(rs.request_log_retention_days(), 3650);
    }

    #[test]
    fn set_usage_history_retention_days_above_max_clamps_to_thirty_six_fifty() {
        let rs = RuntimeSettings::new(&test_config());
        let stored = rs
            .set("usage_history_retention_days", SettingValue::U64(99_999))
            .unwrap();
        assert_eq!(stored, "3650");
        assert_eq!(rs.usage_history_retention_days(), 3650);
    }

    #[test]
    fn set_soft_drain_enabled_flips_the_flag() {
        let rs = RuntimeSettings::new(&test_config());
        assert!(rs.soft_drain_enabled());
        let stored = rs
            .set("soft_drain_enabled", SettingValue::Bool(false))
            .unwrap();
        assert_eq!(stored, "false");
        assert!(!rs.soft_drain_enabled());
    }

    #[test]
    fn set_live_logs_flips_the_flag() {
        let rs = RuntimeSettings::new(&test_config());
        assert!(!rs.live_logs());
        let stored = rs.set("live_logs", SettingValue::Bool(true)).unwrap();
        assert_eq!(stored, "true");
        assert!(rs.live_logs());
    }

    #[test]
    fn set_unknown_key_is_rejected() {
        let rs = RuntimeSettings::new(&test_config());
        let err = rs
            .set("not_a_real_setting", SettingValue::Bool(true))
            .unwrap_err();
        assert_eq!(
            err,
            SettingsError::UnknownKey("not_a_real_setting".to_string())
        );
    }

    #[test]
    fn set_live_logs_with_a_u64_is_wrong_kind() {
        let rs = RuntimeSettings::new(&test_config());
        let err = rs.set("live_logs", SettingValue::U64(1)).unwrap_err();
        assert_eq!(err, SettingsError::WrongKind("live_logs".to_string()));
    }

    #[test]
    fn set_max_account_attempts_with_a_bool_is_wrong_kind() {
        let rs = RuntimeSettings::new(&test_config());
        let err = rs
            .set("max_account_attempts", SettingValue::Bool(true))
            .unwrap_err();
        assert_eq!(
            err,
            SettingsError::WrongKind("max_account_attempts".to_string())
        );
    }

    #[test]
    fn set_inflight_penalty_pct_with_a_u64_is_wrong_kind() {
        let rs = RuntimeSettings::new(&test_config());
        let err = rs
            .set("inflight_penalty_pct", SettingValue::U64(10))
            .unwrap_err();
        assert_eq!(
            err,
            SettingsError::WrongKind("inflight_penalty_pct".to_string())
        );
    }

    #[test]
    fn set_wake_jitter_ms_with_an_f64_is_wrong_kind() {
        let rs = RuntimeSettings::new(&test_config());
        let err = rs
            .set("wake_jitter_ms", SettingValue::F64(1.0))
            .unwrap_err();
        assert_eq!(err, SettingsError::WrongKind("wake_jitter_ms".to_string()));
    }

    // --- Task 4 (wiring): `parse_setting_value` / `overlay_persisted_settings` ---

    #[test]
    fn parse_setting_value_parses_each_kind_family() {
        assert_eq!(
            parse_setting_value("max_account_attempts", "7"),
            Some(SettingValue::U64(7))
        );
        assert_eq!(
            parse_setting_value("live_logs", "true"),
            Some(SettingValue::Bool(true))
        );
        assert_eq!(
            parse_setting_value("soft_drain_enabled", "false"),
            Some(SettingValue::Bool(false))
        );
        assert_eq!(
            parse_setting_value("inflight_penalty_pct", "12.5"),
            Some(SettingValue::F64(12.5))
        );
    }

    #[test]
    fn parse_setting_value_unknown_key_is_none() {
        assert_eq!(parse_setting_value("not_a_real_setting", "1"), None);
    }

    #[test]
    fn parse_setting_value_unparseable_value_is_none() {
        assert_eq!(
            parse_setting_value("max_account_attempts", "notanint"),
            None
        );
        assert_eq!(parse_setting_value("live_logs", "notabool"), None);
        assert_eq!(
            parse_setting_value("inflight_penalty_pct", "notafloat"),
            None
        );
    }

    #[test]
    fn overlay_persisted_settings_applies_a_known_row_beating_the_seeded_default() {
        let rs = RuntimeSettings::new(&test_config()); // seeds max_account_attempts = 5
        let mut overlay = HashMap::new();
        overlay.insert("max_account_attempts".to_string(), "7".to_string());
        overlay_persisted_settings(&rs, &overlay);
        assert_eq!(rs.max_account_attempts(), 7);
    }

    #[test]
    fn overlay_persisted_settings_flips_a_bool_key() {
        let rs = RuntimeSettings::new(&test_config()); // seeds live_logs = false
        let mut overlay = HashMap::new();
        overlay.insert("live_logs".to_string(), "true".to_string());
        overlay_persisted_settings(&rs, &overlay);
        assert!(rs.live_logs());
    }

    #[test]
    fn overlay_persisted_settings_skips_an_invalid_row_leaving_the_seeded_default() {
        let rs = RuntimeSettings::new(&test_config()); // seeds max_account_attempts = 5
        let mut overlay = HashMap::new();
        overlay.insert("max_account_attempts".to_string(), "notanint".to_string());
        overlay_persisted_settings(&rs, &overlay);
        assert_eq!(
            rs.max_account_attempts(),
            5,
            "an unparseable persisted row is skipped, not applied or defaulted to 0"
        );
    }

    #[test]
    fn overlay_persisted_settings_skips_an_unknown_key_without_affecting_known_ones() {
        let rs = RuntimeSettings::new(&test_config());
        let mut overlay = HashMap::new();
        overlay.insert("max_account_attempts".to_string(), "7".to_string());
        overlay.insert("not_a_real_setting".to_string(), "whatever".to_string());
        overlay_persisted_settings(&rs, &overlay);
        assert_eq!(rs.max_account_attempts(), 7);
    }

    #[test]
    fn overlay_persisted_settings_applies_budget_before_heartbeat_regardless_of_hashmap_insertion_order(
    ) {
        // test_config seeds starvation_wait_budget = 120s, starvation_heartbeat = 15s. The
        // persisted overlay below carries budget=20/heartbeat=80: a heartbeat-first apply would
        // clamp 80 against the still-seeded 120s budget (settling at 80, which then exceeds the
        // budget once it lands at 20 -- wrong and dependent on the HashMap's nondeterministic
        // iteration order). Inserted heartbeat-before-budget here (the "wrong" order) to prove
        // the fixed `read_api::LIVE_KEYS_ORDER` application order is what governs, not insertion
        // order or hash iteration order.
        let rs = RuntimeSettings::new(&test_config());
        let mut overlay = HashMap::new();
        overlay.insert("starvation_heartbeat".to_string(), "80".to_string());
        overlay.insert("starvation_wait_budget".to_string(), "20".to_string());

        overlay_persisted_settings(&rs, &overlay);

        assert_eq!(rs.starvation_wait_budget(), Duration::from_secs(20));
        assert!(
            rs.starvation_heartbeat() <= rs.starvation_wait_budget(),
            "heartbeat ({:?}) must be clamped to the persisted budget ({:?}), not the seeded \
             default budget (120s)",
            rs.starvation_heartbeat(),
            rs.starvation_wait_budget()
        );
        assert_eq!(rs.starvation_heartbeat(), Duration::from_secs(20));
    }

    #[test]
    fn overlay_persisted_settings_on_empty_overlay_leaves_every_seeded_default() {
        let cfg = test_config();
        let rs = RuntimeSettings::new(&cfg);
        overlay_persisted_settings(&rs, &HashMap::new());
        assert_eq!(rs.max_account_attempts(), 5);
        assert_eq!(rs.starvation_wait_budget(), Duration::from_secs(120));
        assert!(!rs.live_logs());
    }
}
