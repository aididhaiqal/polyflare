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

use polyflare_store::{Account, AccountRepo, TokenCipher};

use crate::app::AppState;

/// How often to poll each account's usage. Windows move slowly (5h / weekly), so this is generous.
const REFRESH_INTERVAL: Duration = Duration::from_secs(600);
/// Account statuses the usage refresh is allowed to move between. It must NEVER resurrect a
/// `reauth_required` / `paused` / `deactivated` account just because its usage looks fine.
const USAGE_CONTROLLED: &[&str] = &["active", "rate_limited", "quota_exceeded"];

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

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Refresh one account's usage: fetch `/wham/usage`, persist the present windows, and update the
/// routing gate (only if the account is in a usage-controlled status). A stale-token 401 (or any
/// non-2xx) is skipped silently — the next real request refreshes the token.
async fn refresh_account(
    repo: &AccountRepo,
    cipher: &TokenCipher,
    http: &reqwest::Client,
    upstream_base: &str,
    account: &Account,
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
    if USAGE_CONTROLLED.contains(&account.status.as_str()) {
        let (status, reset_at) =
            derive_gate(rl.primary_window.as_ref(), rl.secondary_window.as_ref());
        repo.update_status_and_reset(&account.id, status, reset_at)
            .await?;
    }
    Ok(())
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
                )
                .await
                {
                    tracing::warn!(error = %e, "usage refresh failed for an account");
                }
            }
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
}
