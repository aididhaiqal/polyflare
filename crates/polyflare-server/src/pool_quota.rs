use axum::http::HeaderMap;
use polyflare_core::{
    select::plan_capacity_secondary, AccountSnapshot, Provider, QuotaWindowSnapshot,
};

const FIVE_HOUR_MINUTES: i64 = 300;
const WEEKLY_MINUTES: i64 = 10_080;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SyntheticQuotaWindow {
    pub used_percent: f64,
    pub window_minutes: i64,
    pub reset_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SyntheticPoolQuota {
    pub primary: Option<SyntheticQuotaWindow>,
    pub secondary: SyntheticQuotaWindow,
}

pub(crate) fn synthesize(
    snapshots: &[AccountSnapshot],
    provider: Provider,
    pool: Option<&str>,
    require_security_work_authorized: bool,
) -> Option<SyntheticPoolQuota> {
    let included: Vec<&AccountSnapshot> = snapshots
        .iter()
        .filter(|snapshot| snapshot.provider == provider)
        .filter(|snapshot| {
            pool.is_none_or(|slug| snapshot.pools.iter().any(|membership| membership == slug))
        })
        .filter(|snapshot| !require_security_work_authorized || snapshot.security_work_authorized)
        .filter(|snapshot| {
            !matches!(
                snapshot.status.as_str(),
                "paused" | "reauth_required" | "deactivated"
            )
        })
        .collect();
    if included.is_empty() {
        return None;
    }

    // Weekly capacity is the stable denominator used by the selector and is expected on every
    // Codex account. If any included account lacks fresh weekly evidence, returning no aggregate
    // is safer than publishing an optimistic partial pool.
    let secondary = aggregate_window(
        &included,
        |snapshot| snapshot.weekly_quota.as_ref(),
        WEEKLY_MINUTES,
    )?;
    // A five-hour limit is not universal (upstream promotions can remove it). Publish it only when
    // every included account is currently governed by a fresh short window.
    let primary = aggregate_window(
        &included,
        |snapshot| snapshot.five_hour_quota.as_ref(),
        FIVE_HOUR_MINUTES,
    );

    Some(SyntheticPoolQuota { primary, secondary })
}

/// The latest real member reset for one synthesized window.
///
/// A pool whose accounts reset at different times has no single upstream reset timestamp. The
/// transport quota events intentionally omit one in that case. WHAM's account-usage schema,
/// however, requires a reset for every reported window. The latest real member reset is the
/// conservative moment by which the whole included pool has replenished; it is derived only when
/// every included account has fresh evidence and a real reset, never from a fabricated clock.
pub(crate) fn conservative_reset_at(
    snapshots: &[AccountSnapshot],
    provider: Provider,
    pool: Option<&str>,
    require_security_work_authorized: bool,
    window_minutes: i64,
) -> Option<i64> {
    let included: Vec<&AccountSnapshot> = snapshots
        .iter()
        .filter(|snapshot| snapshot.provider == provider)
        .filter(|snapshot| {
            pool.is_none_or(|slug| snapshot.pools.iter().any(|membership| membership == slug))
        })
        .filter(|snapshot| !require_security_work_authorized || snapshot.security_work_authorized)
        .filter(|snapshot| {
            !matches!(
                snapshot.status.as_str(),
                "paused" | "reauth_required" | "deactivated"
            )
        })
        .collect();
    if included.is_empty() {
        return None;
    }

    included
        .into_iter()
        .map(|snapshot| match window_minutes {
            FIVE_HOUR_MINUTES => snapshot.five_hour_quota.as_ref(),
            WEEKLY_MINUTES => snapshot.weekly_quota.as_ref(),
            _ => None,
        })
        .map(|window| {
            let window = window?;
            (!window.stale && window.window_minutes == Some(window_minutes))
                .then_some(window.reset_at)
                .flatten()
        })
        .collect::<Option<Vec<_>>>()
        .and_then(|resets| resets.into_iter().max())
}

pub(crate) fn apply_http_headers(headers: &mut HeaderMap, quota: &SyntheticPoolQuota) -> bool {
    let has_selected_quota = headers.contains_key("x-codex-primary-used-percent")
        || headers.contains_key("x-codex-secondary-used-percent");
    if !has_selected_quota {
        return false;
    }

    let selected_headers: Vec<(axum::http::HeaderName, String, axum::http::HeaderValue)> = headers
        .iter()
        .filter_map(|(name, value)| {
            let suffix = name.as_str().strip_prefix("x-codex-")?;
            (suffix.starts_with("primary-")
                || suffix.starts_with("secondary-")
                || suffix.starts_with("credits-")
                || suffix == "limit-name")
                .then(|| {
                    (
                        name.clone(),
                        format!("x-polyflare-selected-{suffix}"),
                        value.clone(),
                    )
                })
        })
        .collect();

    for (source_name, selected_name, value) in selected_headers {
        headers.remove(source_name);
        if let Ok(selected_name) = selected_name.parse::<axum::http::HeaderName>() {
            headers.insert(selected_name, value);
        }
    }
    // Current codex-rs discovers non-default meter families by a `primary-used-percent` header,
    // then parses both windows. A zero-only primary is deliberately parsed as no window while
    // keeping a genuine secondary-only selected-account meter discoverable.
    if !headers.contains_key("x-polyflare-selected-primary-used-percent") {
        insert_header(headers, "x-polyflare-selected-primary-used-percent", "0");
    }
    insert_header(
        headers,
        "x-polyflare-selected-limit-name",
        "Selected account",
    );
    insert_header(headers, "x-codex-limit-name", "PolyFlare pool");
    if let Some(primary) = &quota.primary {
        insert_window_headers(headers, "x-codex-primary", primary);
    }
    insert_window_headers(headers, "x-codex-secondary", &quota.secondary);
    true
}

pub(crate) fn rewrite_ws_event(
    payload: &str,
    quota: &SyntheticPoolQuota,
) -> Option<(String, String)> {
    let mut selected: serde_json::Value = serde_json::from_str(payload).ok()?;
    if selected.get("type").and_then(serde_json::Value::as_str) != Some("codex.rate_limits")
        || !selected
            .get("rate_limits")
            .is_some_and(serde_json::Value::is_object)
    {
        return None;
    }
    let selected_object = selected.as_object_mut()?;
    selected_object.insert(
        "metered_limit_name".to_string(),
        serde_json::Value::String("polyflare_selected".to_string()),
    );
    selected_object.insert(
        "limit_name".to_string(),
        serde_json::Value::String("Selected account".to_string()),
    );

    let mut rate_limits = serde_json::Map::new();
    if let Some(primary) = &quota.primary {
        rate_limits.insert("primary".to_string(), window_json(primary));
    }
    rate_limits.insert("secondary".to_string(), window_json(&quota.secondary));
    let aggregate = serde_json::json!({
        "type": "codex.rate_limits",
        "metered_limit_name": "codex",
        "limit_name": "PolyFlare pool",
        "rate_limits": rate_limits,
    });

    Some((
        serde_json::to_string(&selected).ok()?,
        serde_json::to_string(&aggregate).ok()?,
    ))
}

fn insert_window_headers(headers: &mut HeaderMap, prefix: &str, window: &SyntheticQuotaWindow) {
    insert_header(
        headers,
        &format!("{prefix}-used-percent"),
        &window.used_percent.to_string(),
    );
    insert_header(
        headers,
        &format!("{prefix}-window-minutes"),
        &window.window_minutes.to_string(),
    );
    if let Some(reset_at) = window.reset_at {
        insert_header(
            headers,
            &format!("{prefix}-reset-at"),
            &reset_at.to_string(),
        );
    }
}

fn insert_header(headers: &mut HeaderMap, name: &str, value: &str) {
    let Ok(name) = name.parse::<axum::http::HeaderName>() else {
        return;
    };
    let Ok(value) = axum::http::HeaderValue::from_str(value) else {
        return;
    };
    headers.insert(name, value);
}

fn window_json(window: &SyntheticQuotaWindow) -> serde_json::Value {
    let mut value = serde_json::json!({
        "used_percent": window.used_percent,
        "window_minutes": window.window_minutes,
    });
    if let Some(reset_at) = window.reset_at {
        value["reset_at"] = serde_json::Value::from(reset_at);
    }
    value
}

fn aggregate_window(
    accounts: &[&AccountSnapshot],
    quota: impl Fn(&AccountSnapshot) -> Option<&QuotaWindowSnapshot>,
    canonical_window_minutes: i64,
) -> Option<SyntheticQuotaWindow> {
    let mut total_capacity = 0.0;
    let mut total_used_capacity = 0.0;
    let mut common_reset: Option<Option<i64>> = None;

    for account in accounts {
        let window = quota(account)?;
        if window.stale || !window.used_percent.is_finite() {
            return None;
        }
        let capacity = account
            .capacity_credits
            .unwrap_or_else(|| plan_capacity_secondary(&account.plan_type));
        if !capacity.is_finite() || capacity <= 0.0 {
            return None;
        }
        let used_percent = window.used_percent.clamp(0.0, 100.0);
        total_capacity += capacity;
        total_used_capacity += capacity * used_percent / 100.0;
        match common_reset {
            None => common_reset = Some(window.reset_at),
            Some(reset) if reset == window.reset_at => {}
            Some(_) => common_reset = Some(None),
        }
    }

    if !total_capacity.is_finite() || total_capacity <= 0.0 {
        return None;
    }

    Some(SyntheticQuotaWindow {
        used_percent: (100.0 * total_used_capacity / total_capacity).clamp(0.0, 100.0),
        window_minutes: canonical_window_minutes,
        reset_at: common_reset.flatten(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use polyflare_core::QuotaWindowSnapshot;

    const NOW: i64 = 1_000_000;

    fn window(used_percent: f64, minutes: i64, reset_at: Option<i64>) -> QuotaWindowSnapshot {
        QuotaWindowSnapshot {
            used_percent,
            window_minutes: Some(minutes),
            reset_at,
            recorded_at: NOW - 60,
            stale: false,
        }
    }

    fn account(id: &str, plan: &str, weekly_used: f64) -> AccountSnapshot {
        let mut snapshot = AccountSnapshot::new(id);
        snapshot.plan_type = plan.to_string();
        snapshot.weekly_quota = Some(window(weekly_used, 10_080, Some(2_000_000)));
        snapshot.secondary_used_percent = weekly_used;
        snapshot
    }

    #[test]
    fn weights_remaining_capacity_across_mixed_plans() {
        let snapshots = vec![account("plus", "plus", 50.0), account("pro", "pro", 10.0)];

        let quota = synthesize(&snapshots, Provider::Codex, None, false).expect("pool quota");

        // (7,560 × 50% used + 50,400 × 10% used) / 57,960 = 15.217...
        assert!((quota.secondary.used_percent - 15.2173913043).abs() < 1e-9);
        assert_eq!(quota.secondary.window_minutes, 10_080);
        assert_eq!(quota.secondary.reset_at, Some(2_000_000));
    }

    #[test]
    fn scopes_by_pool_provider_and_security_capability() {
        let mut included = account("included", "plus", 20.0);
        included.pools = vec!["secure".to_string(), "shared".to_string()];
        included.security_work_authorized = true;

        let mut wrong_pool = account("wrong-pool", "pro", 99.0);
        wrong_pool.pools = vec!["other".to_string()];
        wrong_pool.security_work_authorized = true;

        let mut wrong_provider = account("wrong-provider", "pro", 99.0);
        wrong_provider.provider = Provider::Anthropic;
        wrong_provider.pools = vec!["secure".to_string()];
        wrong_provider.security_work_authorized = true;

        let mut incapable = account("incapable", "pro", 99.0);
        incapable.pools = vec!["secure".to_string()];

        let quota = synthesize(
            &[included, wrong_pool, wrong_provider, incapable],
            Provider::Codex,
            Some("secure"),
            true,
        )
        .expect("scoped quota");

        assert_eq!(quota.secondary.used_percent, 20.0);
    }

    #[test]
    fn omits_five_hour_when_any_included_account_has_no_fresh_window() {
        let mut first = account("first", "plus", 20.0);
        first.five_hour_quota = Some(window(40.0, 300, Some(1_100_000)));
        let second = account("promo-no-five-hour", "plus", 20.0);

        let quota = synthesize(&[first, second], Provider::Codex, None, false)
            .expect("weekly remains available");

        assert_eq!(quota.primary, None);
    }

    #[test]
    fn refuses_synthesis_when_weekly_evidence_is_stale_or_missing() {
        let fresh = account("fresh", "plus", 20.0);
        let mut stale = account("stale", "plus", 40.0);
        stale.weekly_quota.as_mut().unwrap().stale = true;

        assert_eq!(
            synthesize(&[fresh, stale], Provider::Codex, None, false),
            None
        );
    }

    #[test]
    fn excludes_terminal_accounts_but_retains_recoverable_exhaustion() {
        let active = account("active", "plus", 0.0);
        let mut paused = account("paused", "pro", 100.0);
        paused.status = "paused".to_string();
        let mut exhausted = account("exhausted", "plus", 100.0);
        exhausted.status = "quota_exceeded".to_string();
        exhausted.reset_at = Some(2_000_000);

        let quota = synthesize(&[active, paused, exhausted], Provider::Codex, None, false)
            .expect("active and recoverable accounts");

        assert_eq!(quota.secondary.used_percent, 50.0);
    }

    #[test]
    fn clamps_numeric_percentages_and_omits_mismatched_reset() {
        let below_zero = account("below", "plus", -20.0);
        let mut above_hundred = account("above", "plus", 140.0);
        above_hundred.weekly_quota.as_mut().unwrap().reset_at = Some(3_000_000);

        let quota = synthesize(&[below_zero, above_hundred], Provider::Codex, None, false)
            .expect("clamped quota");

        assert_eq!(quota.secondary.used_percent, 50.0);
        assert_eq!(quota.secondary.reset_at, None);
    }

    #[test]
    fn conservative_reset_uses_latest_real_member_reset_and_requires_complete_evidence() {
        let first = account("first", "plus", 20.0);
        let mut second = account("second", "pro", 40.0);
        second.weekly_quota.as_mut().unwrap().reset_at = Some(3_000_000);

        assert_eq!(
            conservative_reset_at(
                &[first.clone(), second.clone()],
                Provider::Codex,
                None,
                false,
                WEEKLY_MINUTES
            ),
            Some(3_000_000)
        );

        second.weekly_quota.as_mut().unwrap().reset_at = None;
        assert_eq!(
            conservative_reset_at(
                &[first, second],
                Provider::Codex,
                None,
                false,
                WEEKLY_MINUTES
            ),
            None
        );
    }

    #[test]
    fn malformed_non_finite_evidence_falls_back() {
        let mut malformed = account("nan", "plus", f64::NAN);
        malformed.weekly_quota.as_mut().unwrap().used_percent = f64::NAN;

        assert_eq!(synthesize(&[malformed], Provider::Codex, None, false), None);
    }

    fn complete_quota() -> SyntheticPoolQuota {
        SyntheticPoolQuota {
            primary: Some(SyntheticQuotaWindow {
                used_percent: 25.0,
                window_minutes: 300,
                reset_at: Some(1_100_000),
            }),
            secondary: SyntheticQuotaWindow {
                used_percent: 40.0,
                window_minutes: 10_080,
                reset_at: None,
            },
        }
    }

    #[test]
    fn http_headers_make_pool_canonical_and_preserve_selected_account() {
        let mut headers = HeaderMap::new();
        for (name, value) in [
            ("x-codex-primary-used-percent", "70"),
            ("x-codex-primary-window-minutes", "300"),
            ("x-codex-primary-reset-at", "123"),
            ("x-codex-secondary-used-percent", "80"),
            ("x-codex-secondary-window-minutes", "10080"),
            ("x-codex-credits-has-credits", "true"),
            ("x-codex-credits-unlimited", "false"),
            ("x-codex-credits-balance", "12.34"),
        ] {
            headers.insert(
                name.parse::<axum::http::HeaderName>().unwrap(),
                HeaderValue::from_static(value),
            );
        }

        assert!(apply_http_headers(&mut headers, &complete_quota()));

        assert_eq!(headers["x-codex-limit-name"], "PolyFlare pool");
        assert_eq!(headers["x-codex-primary-used-percent"], "25");
        assert_eq!(headers["x-codex-secondary-used-percent"], "40");
        assert!(headers.get("x-codex-secondary-reset-at").is_none());
        assert!(headers.get("x-codex-credits-has-credits").is_none());
        assert_eq!(
            headers["x-polyflare-selected-limit-name"],
            "Selected account"
        );
        assert_eq!(headers["x-polyflare-selected-primary-used-percent"], "70");
        assert_eq!(headers["x-polyflare-selected-secondary-used-percent"], "80");
        assert_eq!(headers["x-polyflare-selected-credits-has-credits"], "true");
    }

    #[test]
    fn http_rewrite_requires_real_selected_account_quota() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "content-type",
            HeaderValue::from_static("text/event-stream"),
        );
        let before = headers.clone();

        assert!(!apply_http_headers(&mut headers, &complete_quota()));
        assert_eq!(headers, before);
    }

    #[test]
    fn http_secondary_only_selected_meter_keeps_a_primary_discovery_sentinel() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-codex-secondary-used-percent",
            HeaderValue::from_static("80"),
        );
        headers.insert(
            "x-codex-secondary-window-minutes",
            HeaderValue::from_static("10080"),
        );

        assert!(apply_http_headers(&mut headers, &complete_quota()));

        // Current codex-rs discovers arbitrary meter families by their `primary-used-percent`
        // header, then parses both windows. A zero-only primary parses as no window while making
        // the real selected-account secondary window discoverable.
        assert_eq!(headers["x-polyflare-selected-primary-used-percent"], "0");
        assert_eq!(headers["x-polyflare-selected-secondary-used-percent"], "80");
    }

    #[test]
    fn websocket_rewrite_emits_selected_then_canonical_pool_meter() {
        let upstream = serde_json::json!({
            "type": "codex.rate_limits",
            "plan_type": "plus",
            "rate_limits": {
                "primary": {
                    "used_percent": 70.0,
                    "window_minutes": 300,
                    "reset_at": 123
                },
                "secondary": {
                    "used_percent": 80.0,
                    "window_minutes": 10080,
                    "reset_at": 456
                }
            },
            "credits": {
                "has_credits": true,
                "unlimited": false,
                "balance": "12.34"
            }
        })
        .to_string();

        let (selected, aggregate) =
            rewrite_ws_event(&upstream, &complete_quota()).expect("rate-limit rewrite");
        let selected: serde_json::Value = serde_json::from_str(&selected).unwrap();
        let aggregate: serde_json::Value = serde_json::from_str(&aggregate).unwrap();

        assert_eq!(selected["metered_limit_name"], "polyflare_selected");
        assert_eq!(selected["rate_limits"]["primary"]["used_percent"], 70.0);
        assert_eq!(selected["credits"]["balance"], "12.34");
        assert_eq!(aggregate["metered_limit_name"], "codex");
        assert_eq!(aggregate["limit_name"], "PolyFlare pool");
        assert_eq!(aggregate["rate_limits"]["primary"]["used_percent"], 25.0);
        assert_eq!(aggregate["rate_limits"]["secondary"]["used_percent"], 40.0);
        assert!(aggregate.get("credits").is_none());
        assert!(aggregate.get("plan_type").is_none());
    }

    #[test]
    fn websocket_non_rate_limit_frames_are_not_rewritten() {
        assert_eq!(
            rewrite_ws_event(
                r#"{"type":"response.output_text.delta","delta":"hello"}"#,
                &complete_quota()
            ),
            None
        );
    }
}
