//! Per-client-session request-rate circuit breaker.
//!
//! One runaway client session — most often a subagent fan-out that spirals — can pour requests into
//! the pool and burn every account's upstream quota before any per-account limit notices, because
//! the load is spread thin across accounts. This governor caps how many requests a single client
//! session (`x-claude-code-session-id`) may make within a rolling hour, rejecting the excess with a
//! local `429` BEFORE account selection so it costs no upstream quota.
//!
//! Ported from better-ccflare's `session-governor`. Process-local (a per-session counter is cheap
//! and a restart resetting it is harmless). Disabled by default: the enforce limit is `0` until an
//! operator sets one, so out of the box this is a warn-only tripwire, never a rejecter.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

/// The rolling window over which requests are counted.
const WINDOW_SECS: i64 = 60 * 60;
/// Cap on tracked sessions, bounding memory against a flood of distinct session ids. When exceeded,
/// idle sessions are swept and, if still over, the least-recently-active are evicted.
const MAX_TRACKED_SESSIONS: usize = 2048;
/// Cap on retained timestamps per session, bounding memory for one pathological session.
const MAX_TRACKED_TIMES: usize = 20_000;

struct SessionWindow {
    /// Admitted request timestamps (unix seconds), ascending. Rejected requests are NOT recorded.
    times: Vec<i64>,
    /// Whether the warn threshold has already been logged this window (deduped to one warning).
    warned: bool,
    /// Last request seen — admitted OR rejected. Eviction keys off this (not `times`), so a session
    /// that is being rejected and keeps retrying is not mistaken for idle and evicted.
    last_seen: i64,
}

/// The outcome of recording one request against a session.
pub struct SessionVerdict {
    /// This request's 1-based position in the session's current window.
    pub count: u64,
    /// The enforce limit in effect (0 = enforcement off).
    pub enforce_limit: u32,
    /// Whether this request is rejected (over the enforce limit).
    pub rejected: bool,
    /// Seconds until the oldest in-window request ages out — the `Retry-After` for a rejection.
    pub retry_after_secs: i64,
    /// Whether the caller should emit the once-per-window warning (crossed the warn threshold).
    pub should_warn: bool,
}

/// Process-local per-session sliding-window counter.
#[derive(Default)]
pub struct SessionGovernor {
    sessions: Mutex<HashMap<String, SessionWindow>>,
}

/// The process-wide governor. Mirrors better-ccflare's module-level singleton: the counter is
/// intentionally process-local (cheap, and a restart resetting it is harmless), so it lives here as
/// a lazily-initialized static rather than threaded through `AppState`.
static GLOBAL: LazyLock<SessionGovernor> = LazyLock::new(SessionGovernor::new);

/// Access the process-wide session governor.
pub fn global() -> &'static SessionGovernor {
    &GLOBAL
}

impl SessionGovernor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one request for `session_key`. Returns `None` for an empty key (anonymous traffic is
    /// ungoverned). `warn_limit`/`enforce_limit` are the LIVE thresholds; `0` disables that check.
    ///
    /// A rejected request consumes no budget (its timestamp is not recorded), so a session that is
    /// over its limit and retrying cannot lock itself out permanently — it recovers the moment its
    /// oldest in-window request ages past the hour.
    pub fn record(
        &self,
        session_key: &str,
        now: i64,
        warn_limit: u32,
        enforce_limit: u32,
    ) -> Option<SessionVerdict> {
        if session_key.is_empty() {
            return None;
        }
        let mut sessions = self.sessions.lock().expect("session governor lock poisoned");
        if sessions.len() >= MAX_TRACKED_SESSIONS {
            evict(&mut sessions, now);
        }
        let window = sessions
            .entry(session_key.to_string())
            .or_insert_with(|| SessionWindow {
                times: Vec::new(),
                warned: false,
                last_seen: now,
            });
        window.last_seen = now;

        // Drop the expired prefix (O(expired), not an O(n) full filter) — `times` is ascending.
        let cutoff = now - WINDOW_SECS;
        let expired = window.times.iter().take_while(|&&t| t <= cutoff).count();
        if expired > 0 {
            window.times.drain(..expired);
        }
        if window.times.is_empty() {
            window.warned = false;
        }

        let count = window.times.len() as u64 + 1;
        let rejected = enforce_limit > 0 && count > enforce_limit as u64;
        if !rejected && window.times.len() < MAX_TRACKED_TIMES {
            window.times.push(now);
        }
        let retry_after_secs = if rejected {
            (window.times.first().copied().unwrap_or(now) + WINDOW_SECS - now).max(1)
        } else {
            0
        };
        let should_warn = !rejected && warn_limit > 0 && count >= warn_limit as u64 && !window.warned;
        if should_warn {
            window.warned = true;
        }
        Some(SessionVerdict {
            count,
            enforce_limit,
            rejected,
            retry_after_secs,
            should_warn,
        })
    }
}

/// Sweep idle sessions (nothing seen within the window); if still at the cap, evict the
/// least-recently-active first — deliberately keeping the busiest offenders tracked.
fn evict(sessions: &mut HashMap<String, SessionWindow>, now: i64) {
    let cutoff = now - WINDOW_SECS;
    sessions.retain(|_, w| w.last_seen > cutoff);
    if sessions.len() >= MAX_TRACKED_SESSIONS {
        let mut by_age: Vec<(String, i64)> = sessions
            .iter()
            .map(|(k, w)| (k.clone(), w.last_seen))
            .collect();
        by_age.sort_by_key(|(_, last_seen)| *last_seen);
        let to_remove = sessions.len() - MAX_TRACKED_SESSIONS + 1;
        for (key, _) in by_age.into_iter().take(to_remove) {
            sessions.remove(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anonymous_traffic_is_ungoverned() {
        let g = SessionGovernor::new();
        assert!(g.record("", 1000, 300, 10).is_none());
    }

    #[test]
    fn disabled_by_default_never_rejects() {
        let g = SessionGovernor::new();
        // enforce_limit 0 → never rejected, however many requests.
        for i in 0..1000 {
            let v = g.record("s", 1000 + i, 300, 0).unwrap();
            assert!(!v.rejected);
        }
    }

    #[test]
    fn enforces_over_the_limit_within_the_window() {
        let g = SessionGovernor::new();
        // enforce=3: first 3 pass, 4th+ rejected.
        for i in 1..=3 {
            let v = g.record("s", 1000, 0, 3).unwrap();
            assert!(!v.rejected, "request {i} within limit");
            assert_eq!(v.count, i);
        }
        let v = g.record("s", 1000, 0, 3).unwrap();
        assert!(v.rejected, "4th request over the limit is rejected");
        assert_eq!(v.count, 4);
        assert!(v.retry_after_secs >= 1);
    }

    #[test]
    fn rejected_requests_consume_no_budget() {
        let g = SessionGovernor::new();
        // enforce=1: 1 passes, then reject many — the count must not keep climbing past 2, because
        // rejects aren't recorded, so the session isn't permanently locked out.
        assert!(!g.record("s", 1000, 0, 1).unwrap().rejected);
        for _ in 0..50 {
            let v = g.record("s", 1000, 0, 1).unwrap();
            assert!(v.rejected);
            assert_eq!(v.count, 2, "each reject sees exactly the 1 admitted + itself, never more");
        }
    }

    #[test]
    fn the_window_slides_so_old_requests_age_out() {
        let g = SessionGovernor::new();
        // Fill the limit at t=1000.
        for _ in 0..3 {
            g.record("s", 1000, 0, 3);
        }
        assert!(g.record("s", 1000, 0, 3).unwrap().rejected);
        // An hour and a second later, the old ones have aged out → allowed again.
        let v = g.record("s", 1000 + WINDOW_SECS + 1, 0, 3).unwrap();
        assert!(!v.rejected, "requests older than the window no longer count");
        assert_eq!(v.count, 1);
    }

    #[test]
    fn warns_once_per_window_at_the_warn_threshold() {
        let g = SessionGovernor::new();
        // warn=2, enforce off. The 2nd request warns; the 3rd does not (deduped).
        assert!(!g.record("s", 1000, 2, 0).unwrap().should_warn);
        assert!(g.record("s", 1000, 2, 0).unwrap().should_warn);
        assert!(!g.record("s", 1000, 2, 0).unwrap().should_warn);
    }
}
