//! Anthropic subscription rate-limit signals from the `anthropic-ratelimit-unified-*` response
//! headers.
//!
//! Anthropic reports subscription throttling through a "unified" header set, distinct from the
//! per-minute `anthropic-ratelimit-requests|tokens-*` headers that are API-key-only. These are the
//! signals better-ccflare reads to pool subscription accounts:
//!
//! - `anthropic-ratelimit-unified-status`  — `allowed` | `allowed_warning` | `queueing_soft` |
//!   `rate_limited` | `blocked` | `queueing_hard` | `payment_required`
//! - `anthropic-ratelimit-unified-reset`   — unix seconds at which the limit clears
//! - `anthropic-ratelimit-unified-remaining` — remaining budget (unitless; not always present)
//!
//! This module turns a rejected response into a `FailureSignal` whose `retry_after` is the real
//! reset time, so PolyFlare's existing cooldown machinery routes traffic away from a limited
//! account until it recovers — the core of load-balancing across several Claude accounts.

use polyflare_core::FailureSignal;
use reqwest::header::HeaderMap;

/// Unified-status values that mean the account is HARD-limited right now — it cannot serve until
/// the reset. Soft statuses (`allowed_warning`, `queueing_soft`) are advisory and must NOT bench an
/// account. Mirrors better-ccflare's `HARD_LIMIT_STATUSES`.
const HARD_LIMIT_STATUSES: &[&str] = &[
    "rate_limited",
    "blocked",
    "queueing_hard",
    "payment_required",
];

/// The longest cooldown a single reset header may impose, so a malformed or absurd value cannot
/// park an account for days. 24h matches ccflare's clamp.
const MAX_RESET_SECONDS: i64 = 24 * 60 * 60;

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

/// The unified-status header, lowercased, if present.
pub fn unified_status(headers: &HeaderMap) -> Option<String> {
    header(headers, "anthropic-ratelimit-unified-status").map(|s| s.trim().to_lowercase())
}

fn is_hard_limit(status: Option<&str>) -> bool {
    status.is_some_and(|s| HARD_LIMIT_STATUSES.contains(&s))
}

/// Seconds from `now` until the unified-reset header's absolute time, clamped to a sane window.
/// `None` when the header is absent, unparseable, in the past, or beyond the clamp.
fn seconds_until_reset(headers: &HeaderMap, now: i64) -> Option<i64> {
    let reset_at = header(headers, "anthropic-ratelimit-unified-reset")?
        .trim()
        .parse::<i64>()
        .ok()?;
    let delta = reset_at.checked_sub(now)?;
    (1..=MAX_RESET_SECONDS).contains(&delta).then_some(delta)
}

/// The standard `Retry-After`, in seconds, when it is a non-negative integer.
fn retry_after_header(headers: &HeaderMap, _now: i64) -> Option<i64> {
    header(headers, "retry-after")?
        .trim()
        .parse::<i64>()
        .ok()
        .filter(|&s| s >= 0)
}

/// Build the `FailureSignal` for a rejected Anthropic response.
///
/// `retry_after` prefers the unified reset (the real per-account recovery time) over the generic
/// `Retry-After`, falling back to it when the unified header is absent. `error_code` carries the
/// unified status when present — content-safe (a fixed vocabulary, never a message body) — so a
/// hard limit is distinguishable downstream from an ordinary 5xx.
pub fn failure_signal(status: u16, headers: &HeaderMap, now: i64) -> FailureSignal {
    let unified = unified_status(headers);
    let retry_after =
        seconds_until_reset(headers, now).or_else(|| retry_after_header(headers, now));
    // A 429/529, or a hard-limit unified status, is the rate-limited case; otherwise pass the
    // upstream code through. The unified status (when present) is the most specific label.
    let error_code = unified.clone().or_else(|| match status {
        429 => Some("rate_limited".to_string()),
        529 => Some("overloaded".to_string()),
        _ => None,
    });
    let _ = is_hard_limit(unified.as_deref()); // reserved: hard-vs-soft drives future health tiers.
    FailureSignal {
        status,
        retry_after,
        error_code,
    }
}

#[cfg(test)]
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

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.insert(
                reqwest::header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                reqwest::header::HeaderValue::from_str(value).unwrap(),
            );
        }
        map
    }

    #[test]
    fn unified_reset_becomes_retry_after_seconds() {
        let now = 1_000_000;
        let h = headers(&[
            ("anthropic-ratelimit-unified-status", "rate_limited"),
            ("anthropic-ratelimit-unified-reset", "1000300"),
        ]);
        let signal = failure_signal(429, &h, now);
        assert_eq!(
            signal.retry_after,
            Some(300),
            "reset 300s in the future → 300s cooldown"
        );
        assert_eq!(signal.error_code.as_deref(), Some("rate_limited"));
    }

    #[test]
    fn unified_reset_is_preferred_over_generic_retry_after() {
        let now = 1_000_000;
        let h = headers(&[
            ("anthropic-ratelimit-unified-reset", "1000120"),
            ("retry-after", "5"),
        ]);
        assert_eq!(
            failure_signal(429, &h, now).retry_after,
            Some(120),
            "the per-account unified reset is more accurate than a generic Retry-After"
        );
    }

    #[test]
    fn generic_retry_after_is_the_fallback_when_no_unified_reset() {
        let now = 1_000_000;
        let h = headers(&[("retry-after", "42")]);
        assert_eq!(failure_signal(429, &h, now).retry_after, Some(42));
    }

    #[test]
    fn a_past_or_absurd_reset_is_ignored() {
        let now = 1_000_000;
        // In the past.
        let past = headers(&[("anthropic-ratelimit-unified-reset", "999999")]);
        assert_eq!(failure_signal(429, &past, now).retry_after, None);
        // Beyond the 24h clamp.
        let far = headers(&[("anthropic-ratelimit-unified-reset", "2000000")]);
        assert_eq!(failure_signal(429, &far, now).retry_after, None);
    }

    #[test]
    fn a_soft_status_still_reports_itself_but_is_not_a_hard_limit() {
        let h = headers(&[("anthropic-ratelimit-unified-status", "allowed_warning")]);
        let signal = failure_signal(400, &h, unix_now());
        assert_eq!(signal.error_code.as_deref(), Some("allowed_warning"));
        assert!(!is_hard_limit(unified_status(&h).as_deref()));
    }

    #[test]
    fn hard_limit_statuses_are_recognised() {
        for status in [
            "rate_limited",
            "blocked",
            "queueing_hard",
            "payment_required",
        ] {
            assert!(is_hard_limit(Some(status)), "{status} is a hard limit");
        }
        for status in ["allowed", "allowed_warning", "queueing_soft"] {
            assert!(!is_hard_limit(Some(status)), "{status} is not a hard limit");
        }
    }

    #[test]
    fn a_plain_5xx_with_no_headers_passes_through() {
        let signal = failure_signal(503, &HeaderMap::new(), unix_now());
        assert_eq!(signal.status, 503);
        assert_eq!(signal.retry_after, None);
        assert_eq!(signal.error_code, None);
    }
}
