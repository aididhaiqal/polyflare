//! WS-downstream relay Phase 2/3, Task 2: a bounded, same-account upstream re-dial helper.
//!
//! [`redial_upstream`] is the one shared primitive later tasks build reconnect-vs-move on: given an
//! already-resolved `account` (the conversation's pinned owner, or a freshly re-selected one), it
//! re-attempts [`dial_owner_upstream`] (Phase-1, `owner.rs`) up to [`MAX_REDIAL_ATTEMPTS`] times,
//! pausing [`REDIAL_BACKOFF`] between attempts, and hands back the first successful [`WsConn`] — or
//! `None` if every attempt failed. It does not decide WHICH account to dial (that's the caller's
//! job, Tasks 4-6's reconnect-vs-move logic) and it does not distinguish WHY a dial failed
//! (`RelayError`'s variants are discarded here) — a bounded retry loop is all this does.
//!
//! **Content-free (inviolable):** never logs the account id, the headers, or any dial error. No
//! `tracing`/`log`/`println!`/`eprintln!` anywhere in this module.

use axum::http::HeaderMap;
use polyflare_codex::WsConn;
use polyflare_core::Account;

use super::owner::dial_owner_upstream;

/// The maximum number of upstream dial attempts [`redial_upstream`] makes before giving up.
pub(crate) const MAX_REDIAL_ATTEMPTS: u32 = 3;

/// The pause between successive redial attempts (skipped after the final attempt).
pub(crate) const REDIAL_BACKOFF: std::time::Duration = std::time::Duration::from_millis(250);

/// Re-dial `account`'s upstream WS, up to [`MAX_REDIAL_ATTEMPTS`] with [`REDIAL_BACKOFF`] between
/// tries. Returns the first successfully established [`WsConn`], or `None` if every attempt failed.
///
/// Content-free: never logs the account, headers, or any error — `dial_owner_upstream`'s `Err` is
/// discarded outright, not even matched on.
pub(crate) async fn redial_upstream(headers: &HeaderMap, account: &Account) -> Option<WsConn> {
    for attempt in 0..MAX_REDIAL_ATTEMPTS {
        if let Ok(conn) = dial_owner_upstream(headers, account).await {
            return Some(conn);
        }
        if attempt + 1 < MAX_REDIAL_ATTEMPTS {
            tokio::time::sleep(REDIAL_BACKOFF).await;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::http::HeaderMap;
    use polyflare_testkit::{MockWsUpstream, ScriptedTurn};

    /// A live mock upstream is dialed successfully.
    #[tokio::test]
    async fn redial_upstream_succeeds_against_a_live_mock() {
        let mock = MockWsUpstream::new(ScriptedTurn::normal(vec![]));
        let base = mock.clone().spawn().await; // ws://host:port
        let account = Account {
            id: "acct-redial".to_string(),
            base_url: base,
            bearer_token: "owner-bearer".to_string(),
            chatgpt_account_id: Some("owner-cid".to_string()),
        };
        let headers = HeaderMap::new();

        let conn = redial_upstream(&headers, &account).await;
        assert!(
            conn.is_some(),
            "redial_upstream must succeed against a live mock upstream"
        );
        assert_eq!(
            mock.handshake_count(),
            1,
            "exactly one successful upgrade for a first-attempt success"
        );
    }

    /// An un-dialable base (a closed loopback port — nothing listens on it, so every attempt fails
    /// fast with a connection refusal) exhausts all attempts and returns `None`, bounded by
    /// `MAX_REDIAL_ATTEMPTS`/`REDIAL_BACKOFF` so this test stays sub-second. A failed dial never
    /// completes a handshake, so `handshake_count` cannot distinguish "tried once" from "tried
    /// MAX_REDIAL_ATTEMPTS times" here — the meaningful assertion is that the call returns `None`
    /// (proving it gives up rather than looping forever), wrapped in a generous timeout as an
    /// explicit boundedness proof.
    #[tokio::test]
    async fn redial_upstream_gives_up_after_max_attempts_against_a_closed_port() {
        let account = Account {
            id: "acct-redial-fail".to_string(),
            base_url: "http://127.0.0.1:1".to_string(),
            bearer_token: "owner-bearer".to_string(),
            chatgpt_account_id: None,
        };
        let headers = HeaderMap::new();

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            redial_upstream(&headers, &account),
        )
        .await
        .expect("redial_upstream must return within the timeout, not hang");
        assert!(
            result.is_none(),
            "every attempt against a closed port must fail, yielding None"
        );
    }
}
