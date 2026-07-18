//! Runtime usage-refresh loop: polls each Codex account's rate-limit windows so routing runs on
//! LIVE usage + so the reset times (5h + weekly) are current, instead of the frozen numbers the
//! importer left. Mirrors codex-lb's approach — poll `GET {backend-api}/wham/usage`, parse
//! `rate_limit.{primary_window, secondary_window}`, persist window rows + the routing gate. Closes
//! the "routing runs on frozen imported usage" gap from the feature audit.
//!
//! # The 5h window is often absent
//! Upstream stopped emitting the short (primary/5h) window for current plans. A MISSING primary is
//! treated as available, never as blocked (mirrors codex-lb `quota.py`) — the reliable gate is the
//! secondary (weekly) window. So the read side shows "5h: not reported" rather than a fake 100%.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Deserialize;

use polyflare_core::AccountId;
use polyflare_store::{Account, AccountRepo, TokenCipher};

use crate::app::AppState;
use crate::runtime_state::RuntimeStates;

/// How often to poll each account's usage. Windows move slowly (5h / weekly), so this is generous.
const REFRESH_INTERVAL: Duration = Duration::from_secs(600);
/// Account statuses the usage refresh is allowed to move between. It must NEVER resurrect a
/// `reauth_required` / `paused` / `deactivated` account just because its usage looks fine.
const USAGE_CONTROLLED: &[&str] = &["active", "rate_limited", "quota_exceeded"];
/// B8 Task 3: codex-lb's blocked-status set for the health-tier "frozen" check
/// (`app/core/balancer/logic.py:1181-1239`'s frozen predicate) — used ONLY to gate
/// `RuntimeStates::evaluate_with_usage`'s transition, distinct from [`USAGE_CONTROLLED`] (which
/// gates the durable status GATE itself). Deliberately includes `rate_limited`/`quota_exceeded`
/// even though those two ARE usage-controlled (the gate can move an account into/out of them): a
/// hard-blocked/benched account is not a soft-drain PREFERENCE candidate at all — `select.rs`'s
/// `eligibility()` already excludes it outright, so no health-tier transition is meaningful while
/// one of these statuses holds (matches codex-lb's `evaluate_health_tier` frozen contract exactly).
const HEALTH_TIER_FROZEN_STATUSES: &[&str] = &[
    "rate_limited",
    "quota_exceeded",
    "paused",
    "reauth_required",
    "deactivated",
];

/// The `/wham/usage` response (only the fields we use; `extra` ignored).
#[derive(Deserialize, Default)]
struct UsagePayload {
    rate_limit: Option<RateLimitPayload>,
}

#[derive(Deserialize, Default)]
struct RateLimitPayload {
    primary_window: Option<UsageWindow>,
    secondary_window: Option<UsageWindow>,
}

/// One rate-limit window as codex reports it.
#[derive(Deserialize, Clone, Default)]
struct UsageWindow {
    used_percent: Option<f64>,
    /// Absolute unix-epoch SECONDS when the window resets (the canonical reset time).
    reset_at: Option<i64>,
    limit_window_seconds: Option<i64>,
}

/// The `/wham/usage` URL. It lives at the `/backend-api` root, NOT under `/codex`, so from an
/// upstream base like `https://chatgpt.com/backend-api/codex` we truncate at `/backend-api`.
fn usage_url(upstream_base: &str) -> String {
    let base = upstream_base.trim_end_matches('/');
    const MARKER: &str = "/backend-api";
    match base.find(MARKER) {
        Some(idx) => format!("{}/wham/usage", &base[..idx + MARKER.len()]),
        None => format!("{base}{MARKER}/wham/usage"),
    }
}

/// A payload window is the short (5h) limit when its duration is under a day; otherwise it's the
/// weekly limit. An unknown duration is treated as weekly (the durable limit). The real windows are
/// 5h (18000s) and weekly (604800s).
fn is_five_hour(w: &UsageWindow) -> bool {
    w.limit_window_seconds
        .map(|s| s < 24 * 3600)
        .unwrap_or(false)
}

/// Map the present windows onto `(status, reset_at)`, classified by DURATION not slot: upstream
/// currently emits the weekly window in the `primary` slot (no 5h limit), so gating on the slot
/// would mislabel an exhausted weekly as `rate_limited`. The weekly (long) window exhausted ->
/// `quota_exceeded` with the weekly reset; the 5h (short) exhausted -> `rate_limited` with the 5h
/// reset; otherwise `active` (gate cleared). An ABSENT window never gates.
fn derive_gate(
    primary: Option<&UsageWindow>,
    secondary: Option<&UsageWindow>,
) -> (&'static str, Option<i64>) {
    let exhausted = |w: &UsageWindow| w.used_percent.map(|p| p >= 100.0).unwrap_or(false);
    // Split the up-to-two present windows into weekly vs 5h by duration.
    let mut weekly: Option<&UsageWindow> = None;
    let mut five_hour: Option<&UsageWindow> = None;
    for w in [primary, secondary].into_iter().flatten() {
        if is_five_hour(w) {
            five_hour = Some(w);
        } else {
            weekly = Some(w);
        }
    }
    if let Some(w) = weekly {
        if exhausted(w) {
            return ("quota_exceeded", w.reset_at);
        }
    }
    if let Some(w) = five_hour {
        if exhausted(w) {
            return ("rate_limited", w.reset_at);
        }
    }
    ("active", None)
}

/// B8 Task 3: split the up-to-two present windows into `(five_hour_used_percent,
/// weekly_used_percent)`, classified by DURATION not slot — the same distinction [`derive_gate`]
/// already makes (upstream currently emits the weekly window in the `primary` slot for plans with
/// no 5h limit). Feeds `RuntimeStates::evaluate_with_usage`'s `used_percent`/`secondary_percent`
/// params directly: the 85% primary threshold applies to the REAL 5h window, the 90% secondary
/// threshold to the REAL weekly window, regardless of which JSON slot each arrived in.
fn split_usage_by_duration(
    primary: Option<&UsageWindow>,
    secondary: Option<&UsageWindow>,
) -> (Option<f64>, Option<f64>) {
    let mut five_hour_used = None;
    let mut weekly_used = None;
    for w in [primary, secondary].into_iter().flatten() {
        if is_five_hour(w) {
            five_hour_used = w.used_percent;
        } else {
            weekly_used = w.used_percent;
        }
    }
    (five_hour_used, weekly_used)
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Refresh one account's usage: fetch `/wham/usage`, persist the present windows, update the
/// routing gate (only if the account is in a usage-controlled status), and — B8 Task 3 — run the
/// FULL usage-driven health-tier evaluation (`RuntimeStates::evaluate_with_usage`; see that
/// method's doc for why this is the only site that may promote DRAINING→PROBING). A stale-token
/// 401 (or any non-2xx) is skipped silently — the next real request refreshes the token.
#[allow(clippy::too_many_arguments)] // internal fn; B8 Task 4 added the log-bus + metrics emit handles.
async fn refresh_account(
    repo: &AccountRepo,
    cipher: &TokenCipher,
    http: &reqwest::Client,
    upstream_base: &str,
    account: &Account,
    runtime: &RuntimeStates,
    soft_drain_enabled: bool,
    log_bus: &crate::log_bus::LogBus,
    health_tier_metrics: &crate::observability::HealthTierMetrics,
) -> Result<(), Box<dyn std::error::Error>> {
    let tokens = match repo.decrypt_tokens(&account.id, cipher).await? {
        Some(t) => t,
        None => return Ok(()),
    };
    let mut req = http
        .get(usage_url(upstream_base))
        .header("Authorization", format!("Bearer {}", tokens.access_token))
        .header("Accept", "application/json");
    if let Some(cid) = &account.chatgpt_account_id {
        req = req.header("chatgpt-account-id", cid);
    }
    let resp = req.send().await?;
    if !resp.status().is_success() {
        return Ok(());
    }
    let payload: UsagePayload = resp.json().await.unwrap_or_default();
    let rl = payload.rate_limit.unwrap_or_default();
    let now = unix_now();

    for (window, w) in [
        ("primary", &rl.primary_window),
        ("secondary", &rl.secondary_window),
    ] {
        if let Some(w) = w {
            repo.insert_usage_window(
                &account.id,
                window,
                w.used_percent.unwrap_or(0.0),
                w.reset_at,
                w.limit_window_seconds.map(|s| s / 60),
                now,
            )
            .await?;
        }
    }

    // Only move between usage-controlled statuses; never touch reauth_required/paused/deactivated.
    let mut effective_status: &str = &account.status;
    if USAGE_CONTROLLED.contains(&account.status.as_str()) {
        let (status, reset_at) =
            derive_gate(rl.primary_window.as_ref(), rl.secondary_window.as_ref());
        repo.update_status_and_reset(&account.id, status, reset_at)
            .await?;
        effective_status = status;
    }

    // B8 Task 3: the authoritative periodic usage-driven health-tier evaluation — the ONLY site
    // that may promote a purely usage-drained account from DRAINING to PROBING (it has both the
    // fresh usage% AND, via the runtime entry, the current error state). `status_frozen` uses the
    // POST-refresh effective status (the one that was just persisted, if it changed) so a
    // just-benched account is correctly frozen this same cycle rather than one cycle late.
    let status_frozen = HEALTH_TIER_FROZEN_STATUSES.contains(&effective_status);
    let (five_hour_used, weekly_used) =
        split_usage_by_duration(rl.primary_window.as_ref(), rl.secondary_window.as_ref());
    let transition = runtime.evaluate_with_usage(
        &AccountId::from(account.id.as_str()),
        five_hour_used,
        weekly_used,
        status_frozen,
        soft_drain_enabled,
        now,
    );
    // B8 Task 4: the usage-driven edge — the ONLY site that can emit a `usage_drain`, a
    // `quiet_promote`, or a `disabled_reset`. Emit the content-free health-tier signal only on a
    // real tier change (`evaluate_with_usage` returns `Some` only then).
    if let Some(t) = transition {
        crate::observability::emit_health_tier_signal(
            log_bus,
            health_tier_metrics,
            &account.id,
            t.from,
            t.to,
            t.reason,
        );
    }

    Ok(())
}

/// B8 review Finding 1: the poller's error-driven health-tier evaluation for ONE NON-codex account.
/// Codex accounts get the FULL usage+error evaluation inside [`refresh_account`] (it polls real
/// `used_percent` from `/wham/usage`); a non-codex account (e.g. Anthropic) has no such usage source
/// at all, so `refresh_account` is never called for it — but the per-request funnel
/// (`RuntimeState::apply_funnel_transition`) deliberately REFUSES the DRAINING→PROBING quiet-timer
/// promotion for every provider (it never sees usage, so it can't tell a usage-drain from an
/// error-drain). Without this pass, a non-codex account that error-flaps into DRAINING would never
/// see a poller cycle at all and would be stranded there until restart.
///
/// This calls the SAME [`RuntimeStates::evaluate_with_usage`] the codex path uses, but with
/// `used_percent`/`secondary_percent = None` — i.e. purely error-driven `should_drain`. That still
/// gives the account the quiet-timer DRAINING→PROBING promotion, the PROBING→HEALTHY streak (already
/// funnel-driven), and the `soft_drain_enabled` disable lever, closing the stranding gap while never
/// pretending to know a usage percentage it doesn't have.
///
/// `status_frozen` reuses [`HEALTH_TIER_FROZEN_STATUSES`] — the exact same blocked-status set the
/// codex path passes to `evaluate_with_usage`, so a benched/paused/deactivated non-codex account is
/// frozen identically to a benched codex account (no duplicated set).
fn evaluate_non_codex_health_tier(
    runtime: &RuntimeStates,
    account: &Account,
    soft_drain_enabled: bool,
    now: i64,
) -> Option<crate::runtime_state::HealthTierTransition> {
    let status_frozen = HEALTH_TIER_FROZEN_STATUSES.contains(&account.status.as_str());
    runtime.evaluate_with_usage(
        &AccountId::from(account.id.as_str()),
        None,
        None,
        status_frozen,
        soft_drain_enabled,
        now,
    )
}

/// Spawn the background usage-refresh loop: every [`REFRESH_INTERVAL`], poll each Codex account.
pub fn spawn_usage_refresh(state: Arc<AppState>) {
    let http = match reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, "usage refresh: could not build http client; disabled");
            return;
        }
    };
    tokio::spawn(async move {
        loop {
            let repo = state.store.accounts();
            let accounts = repo.list().await.unwrap_or_default();
            for account in accounts.iter().filter(|a| a.provider == "codex") {
                if let Err(e) = refresh_account(
                    &repo,
                    &state.cipher,
                    &http,
                    &state.upstream_base_url,
                    account,
                    &state.runtime,
                    state.soft_drain_enabled,
                    &state.log_bus,
                    &state.health_tier_metrics,
                )
                .await
                {
                    tracing::warn!(error = %e, "usage refresh failed for an account");
                }
            }
            // B8 review Finding 1: NON-codex accounts (disjoint filter from the codex loop above —
            // never re-evaluates a codex account, which would clobber its real-usage-driven drain
            // with a None-usage view). No HTTP usage poll for them (no usage source exists); this
            // just runs the poller's error-driven tier evaluation so they get the same
            // recovery/disable-lever coverage as codex accounts instead of being stranded in
            // DRAINING once the funnel refuses the quiet-timer promotion.
            for account in accounts.iter().filter(|a| a.provider != "codex") {
                let transition = evaluate_non_codex_health_tier(
                    &state.runtime,
                    account,
                    state.soft_drain_enabled,
                    unix_now(),
                );
                if let Some(t) = transition {
                    crate::observability::emit_health_tier_signal(
                        &state.log_bus,
                        &state.health_tier_metrics,
                        &account.id,
                        t.from,
                        t.to,
                        t.reason,
                    );
                }
            }
            // Warm the account snapshot cache off the request path: this cycle's usage/status writes
            // bumped the account generation, so rebuild the O(accounts) snapshot pool HERE instead of
            // making the next real request pay it. (Idempotent — `snapshots` single-flights the
            // rebuild, and the token cache is untouched by usage writes now, so it stays warm.)
            let _ = state.account_cache.snapshots(&state.store).await;
            // Evict expired decrypted tokens each cycle: since usage writes no longer wipe the token
            // cache, this bounds an idle account's token lifetime to ~one refresh interval past its
            // TTL (independent of any cache miss triggering the insert-time sweep).
            state.token_cache.sweep(unix_now());
            tokio::time::sleep(REFRESH_INTERVAL).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usage_url_targets_backend_api_root_not_codex() {
        assert_eq!(
            usage_url("https://chatgpt.com/backend-api/codex"),
            "https://chatgpt.com/backend-api/wham/usage"
        );
        assert_eq!(
            usage_url("https://chatgpt.com/backend-api/codex/"),
            "https://chatgpt.com/backend-api/wham/usage"
        );
        // No /backend-api in the base -> append it.
        assert_eq!(
            usage_url("https://example.test"),
            "https://example.test/backend-api/wham/usage"
        );
    }

    /// A 5h (short) window: 18000s duration.
    fn five_h(used: f64, reset: i64) -> UsageWindow {
        UsageWindow {
            used_percent: Some(used),
            reset_at: Some(reset),
            limit_window_seconds: Some(5 * 3600),
        }
    }

    /// A weekly (long) window: 604800s duration.
    fn weekly(used: f64, reset: i64) -> UsageWindow {
        UsageWindow {
            used_percent: Some(used),
            reset_at: Some(reset),
            limit_window_seconds: Some(7 * 24 * 3600),
        }
    }

    #[test]
    fn exhausted_weekly_gates_quota_exceeded_with_weekly_reset() {
        let (status, reset) = derive_gate(Some(&five_h(10.0, 111)), Some(&weekly(100.0, 999)));
        assert_eq!(status, "quota_exceeded");
        assert_eq!(reset, Some(999));
    }

    #[test]
    fn exhausted_5h_gates_rate_limited() {
        let (status, reset) = derive_gate(Some(&five_h(100.0, 111)), Some(&weekly(50.0, 999)));
        assert_eq!(status, "rate_limited");
        assert_eq!(reset, Some(111));
    }

    #[test]
    fn absent_5h_is_available_not_blocked() {
        // The 5h window is missing (upstream stopped reporting it); weekly is fine -> active.
        let (status, reset) = derive_gate(None, Some(&weekly(40.0, 999)));
        assert_eq!(status, "active");
        assert_eq!(reset, None);
    }

    #[test]
    fn promo_weekly_in_primary_slot_gates_quota_exceeded_not_rate_limited() {
        // Current upstream: the weekly window arrives in the PRIMARY slot with no secondary. It
        // must be gated as an exhausted WEEKLY (quota_exceeded), not mislabeled `rate_limited` by
        // its slot. This is the regression the duration-aware gate fixes.
        let (status, reset) = derive_gate(Some(&weekly(100.0, 777)), None);
        assert_eq!(status, "quota_exceeded");
        assert_eq!(reset, Some(777));
    }

    #[test]
    fn all_clear_is_active() {
        let (status, reset) = derive_gate(Some(&five_h(10.0, 1)), Some(&weekly(20.0, 2)));
        assert_eq!(status, "active");
        assert_eq!(reset, None);
    }

    #[test]
    fn parses_wham_usage_payload() {
        let json = serde_json::json!({
            "plan_type": "pro",
            "rate_limit": {
                "secondary_window": {"used_percent": 73.5, "reset_at": 1783900000, "limit_window_seconds": 604800}
            }
        });
        let p: UsagePayload = serde_json::from_value(json).unwrap();
        let rl = p.rate_limit.unwrap();
        assert!(rl.primary_window.is_none(), "5h absent");
        let s = rl.secondary_window.unwrap();
        assert_eq!(s.used_percent, Some(73.5));
        assert_eq!(s.reset_at, Some(1783900000));
        assert_eq!(s.limit_window_seconds, Some(604800));
    }

    // --- B8 review Finding 1: non-codex accounts must not be stranded in DRAINING ---

    fn account(id: &str, provider: &str, status: &str) -> Account {
        Account {
            id: id.to_string(),
            chatgpt_account_id: None,
            chatgpt_user_id: None,
            email: "u@example.test".to_string(),
            alias: None,
            workspace_id: None,
            workspace_label: None,
            seat_type: None,
            plan_type: "pro".to_string(),
            routing_policy: "normal".to_string(),
            last_refresh: 0,
            created_at: 0,
            status: status.to_string(),
            deactivation_reason: None,
            reset_at: None,
            blocked_at: None,
            security_work_authorized: false,
            provider: provider.to_string(),
            pool: None,
        }
    }

    /// THE regression this finding fixes: a non-codex account driven to DRAINING purely by the
    /// error funnel (2 `record_transient_error` within 60s) has no other recovery path — the funnel
    /// itself refuses the DRAINING->PROBING quiet-timer promotion for every provider (that's
    /// `apply_funnel_transition`'s care point, not a codex-only rule), and `refresh_account`'s HTTP
    /// usage poll never runs for a non-codex account. `evaluate_non_codex_health_tier` is the poller
    /// pass that rescues it: called with `used_percent = None` (no usage source), it still owns the
    /// quiet-timer demotion because it's invoked from the poller side of the split, not the funnel
    /// side.
    #[test]
    fn non_codex_account_error_drained_is_not_stranded_in_draining() {
        let runtime = RuntimeStates::new();
        let acct = account("anthro-a", "anthropic", "active");
        let id = AccountId::from(acct.id.as_str());

        let t = runtime.record_transient_error(&id, 1000);
        assert_eq!(t, None, "the FIRST error alone does not reach the drain threshold");
        let t = runtime
            .record_transient_error(&id, 1010)
            .expect("the SECOND error within 60s crosses the error-drain threshold");
        assert_eq!((t.from, t.to, t.reason), (0, 1, "error_drain"));

        // Well past PROBE_QUIET_SECS (60) since drain_entered_at (stamped at 1010), no further
        // errors: the poller pass must promote DRAINING -> PROBING.
        let now_quiet = 1010 + 61;
        let transition = evaluate_non_codex_health_tier(&runtime, &acct, true, now_quiet)
            .expect("the non-codex poller pass promotes the quiet, error-drained account");
        assert_eq!(
            (transition.from, transition.to, transition.reason),
            (1, 2, "quiet_promote"),
            "the poller pass owns the DRAINING->PROBING demotion for non-codex too, exactly like \
             the codex path's evaluate_with_usage call"
        );

        let mut snaps = vec![polyflare_core::AccountSnapshot::new("anthro-a")];
        runtime.overlay(&mut snaps, now_quiet);
        assert_eq!(
            snaps[0].health_tier, 2,
            "not stranded: promoted to PROBING by the non-codex poller pass"
        );

        // Close the loop: 3 successes while PROBING (funnel-owned, unaffected by this finding)
        // complete the recovery back to HEALTHY.
        runtime.record_success(&id);
        runtime.record_success(&id);
        runtime.record_success(&id);
        let mut snaps2 = vec![polyflare_core::AccountSnapshot::new("anthro-a")];
        runtime.overlay(&mut snaps2, now_quiet);
        assert_eq!(
            snaps2[0].health_tier, 0,
            "full recovery: PROBING -> HEALTHY via the funnel's success streak"
        );
    }

    /// The `soft_drain_enabled` disable lever must reach non-codex accounts too, exactly like it
    /// reaches codex accounts via `evaluate_with_usage` in `refresh_account`.
    #[test]
    fn non_codex_pass_disabled_forces_healthy() {
        let runtime = RuntimeStates::new();
        let acct = account("anthro-b", "anthropic", "active");
        let id = AccountId::from(acct.id.as_str());

        runtime.record_transient_error(&id, 2000);
        runtime
            .record_transient_error(&id, 2010)
            .expect("2nd error within 60s crosses the error-drain threshold");
        let mut mid = vec![polyflare_core::AccountSnapshot::new("anthro-b")];
        runtime.overlay(&mut mid, 2010);
        assert_eq!(mid[0].health_tier, 1, "funnel drained anthro-b (not flag-gated)");

        let transition = evaluate_non_codex_health_tier(&runtime, &acct, false, 2010)
            .expect("the disable lever forces a real HEALTHY transition");
        assert_eq!(
            (transition.from, transition.to, transition.reason),
            (1, 0, "disabled_reset")
        );

        let mut snaps = vec![polyflare_core::AccountSnapshot::new("anthro-b")];
        runtime.overlay(&mut snaps, 2010);
        assert_eq!(
            snaps[0].health_tier, 0,
            "disable lever reaches non-codex accounts too, not just codex"
        );
    }

    /// A frozen (blocked-status) non-codex account's tier must pass through unchanged, matching the
    /// codex path's contract exactly (same `HEALTH_TIER_FROZEN_STATUSES` set): even well past the
    /// quiet timer, a frozen account is not promoted DRAINING->PROBING.
    #[test]
    fn non_codex_pass_respects_frozen_statuses() {
        let runtime = RuntimeStates::new();
        // Drive to DRAINING via the funnel while still `active` (status is irrelevant to the
        // funnel's own error-only evaluation).
        let id = AccountId::from("anthro-c");
        runtime.record_transient_error(&id, 3000);
        runtime
            .record_transient_error(&id, 3010)
            .expect("2nd error within 60s crosses the error-drain threshold");

        // Now the account is (durably) frozen, e.g. rate_limited. Even well past the quiet timer,
        // the poller pass must NOT promote it.
        let acct_frozen = account("anthro-c", "anthropic", "rate_limited");
        let now_quiet = 3010 + 61;
        let transition = evaluate_non_codex_health_tier(&runtime, &acct_frozen, true, now_quiet);
        assert_eq!(transition, None, "frozen: no transition even though the quiet timer elapsed");

        let mut snaps = vec![polyflare_core::AccountSnapshot::new("anthro-c")];
        runtime.overlay(&mut snaps, now_quiet);
        assert_eq!(
            snaps[0].health_tier, 1,
            "tier stays DRAINING while frozen, not promoted to PROBING"
        );
    }
}
