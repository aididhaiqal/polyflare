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

/// `Retry-After` given as an HTTP-date (RFC 7231 IMF-fixdate, e.g. `Wed, 21 Oct 2026 07:28:00 GMT`)
/// rather than a delta — converted to seconds from `now`, clamped to the sane window.
fn retry_after_http_date(headers: &HeaderMap, now: i64) -> Option<i64> {
    let raw = header(headers, "retry-after")?.trim();
    // An HTTP-date always ends in the obsolete zone name `GMT`; chrono's RFC 2822 parser wants a
    // numeric offset, so normalize `GMT` → `+0000` (they denote the same instant).
    let normalized = raw.replace(" GMT", " +0000");
    let ts = chrono::DateTime::parse_from_rfc2822(&normalized)
        .ok()?
        .timestamp();
    let delta = ts.checked_sub(now)?;
    (1..=MAX_RESET_SECONDS).contains(&delta).then_some(delta)
}

/// `x-ratelimit-reset` as absolute unix-epoch seconds — the last rung of the 529 reset ladder,
/// converted to a delta from `now` and clamped.
fn x_ratelimit_reset(headers: &HeaderMap, now: i64) -> Option<i64> {
    let reset_at = header(headers, "x-ratelimit-reset")?
        .trim()
        .parse::<i64>()
        .ok()?;
    let delta = reset_at.checked_sub(now)?;
    (1..=MAX_RESET_SECONDS).contains(&delta).then_some(delta)
}

/// The best available reset time, walking the full ladder better-ccflare uses for a 529: the
/// per-account unified reset first (most accurate), then a numeric `Retry-After`, then an HTTP-date
/// `Retry-After`, then the generic `x-ratelimit-reset`. `None` means no usable reset was found — the
/// caller then applies a floor cooldown so an overloaded upstream still routes away briefly.
fn reset_ladder(headers: &HeaderMap, now: i64) -> Option<i64> {
    seconds_until_reset(headers, now)
        .or_else(|| retry_after_header(headers, now))
        .or_else(|| retry_after_http_date(headers, now))
        .or_else(|| x_ratelimit_reset(headers, now))
}

/// True when the overage (pay-as-you-go) billing is disabled because the account is out of
/// purchased credits. This is a SCOPED billing signal — the account's plan itself may still serve
/// other models/requests — distinct from an account-wide rate limit. Mirrors better-ccflare's
/// `isAnthropicOutOfCredits`.
fn is_out_of_credits(headers: &HeaderMap) -> bool {
    header(headers, "anthropic-ratelimit-unified-overage-disabled-reason")
        .map(|v| v.trim().eq_ignore_ascii_case("out_of_credits"))
        .unwrap_or(false)
}

/// Build the `FailureSignal` for a rejected Anthropic response.
///
/// `retry_after` prefers the unified reset (the real per-account recovery time) over the generic
/// `Retry-After`, falling back to it when the unified header is absent. `error_code` carries the
/// unified status when present — content-safe (a fixed vocabulary, never a message body) — so a
/// hard limit is distinguishable downstream from an ordinary 5xx.
///
/// `out_of_credits` takes precedence ONLY when the response is not also an account-wide hard limit:
/// a genuine plan-limit `rate_limited` (which co-carries the overage header for accounts with no
/// credits) must still cool the account until its reset, not fail over without benching, or an
/// exhausted account would be retried in a tight loop.
pub fn failure_signal(status: u16, headers: &HeaderMap, now: i64) -> FailureSignal {
    let unified = unified_status(headers);
    let retry_after = reset_ladder(headers, now);
    let hard = is_hard_limit(unified.as_deref());
    let error_code = if is_out_of_credits(headers) && !hard {
        // Scoped overage/credit rejection, not an account-wide limit → distinguishable so the
        // router fails over WITHOUT benching this account.
        Some("out_of_credits".to_string())
    } else {
        // A 429/529, or a hard-limit unified status, is the rate-limited case; otherwise pass the
        // upstream code through. The unified status (when present) is the most specific label.
        unified.clone().or_else(|| match status {
            429 => Some("rate_limited".to_string()),
            529 => Some("overloaded".to_string()),
            _ => None,
        })
    };
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
    fn the_529_ladder_falls_through_to_x_ratelimit_reset() {
        let now = 1_000_000;
        // No unified-reset, no numeric/date retry-after → the x-ratelimit-reset rung supplies it.
        let h = headers(&[("x-ratelimit-reset", "1000090")]);
        let signal = failure_signal(529, &h, now);
        assert_eq!(signal.retry_after, Some(90));
        assert_eq!(signal.error_code.as_deref(), Some("overloaded"));
    }

    #[test]
    fn retry_after_as_an_http_date_is_parsed() {
        // now = 2026-08-19T00:00:00Z = 1_787_097_600 (a Wednesday); the date is 120s later.
        let now = 1_787_097_600;
        let h = headers(&[("retry-after", "Wed, 19 Aug 2026 00:02:00 GMT")]);
        assert_eq!(failure_signal(529, &h, now).retry_after, Some(120));
    }

    #[test]
    fn the_ladder_prefers_unified_reset_over_x_ratelimit_reset() {
        let now = 1_000_000;
        let h = headers(&[
            ("anthropic-ratelimit-unified-reset", "1000030"),
            ("x-ratelimit-reset", "1000090"),
        ]);
        assert_eq!(failure_signal(429, &h, now).retry_after, Some(30));
    }

    #[test]
    fn out_of_credits_without_a_hard_limit_is_labelled_for_failover() {
        // Overage disabled for lack of credits, and the account is NOT account-wide rate-limited
        // → a scoped billing rejection the router should fail over on without benching.
        let h = headers(&[(
            "anthropic-ratelimit-unified-overage-disabled-reason",
            "out_of_credits",
        )]);
        assert_eq!(
            failure_signal(400, &h, unix_now()).error_code.as_deref(),
            Some("out_of_credits")
        );
    }

    #[test]
    fn a_hard_rate_limit_wins_over_out_of_credits() {
        // A genuine plan-limit 429 co-carries the overage header for a no-credits account, but it
        // must still cool the account — the hard `rate_limited` status takes precedence.
        let h = headers(&[
            ("anthropic-ratelimit-unified-status", "rate_limited"),
            (
                "anthropic-ratelimit-unified-overage-disabled-reason",
                "out_of_credits",
            ),
        ]);
        assert_eq!(
            failure_signal(429, &h, unix_now()).error_code.as_deref(),
            Some("rate_limited")
        );
    }

    #[test]
    fn a_plain_5xx_with_no_headers_passes_through() {
        let signal = failure_signal(503, &HeaderMap::new(), unix_now());
        assert_eq!(signal.status, 503);
        assert_eq!(signal.retry_after, None);
        assert_eq!(signal.error_code, None);
    }
}
