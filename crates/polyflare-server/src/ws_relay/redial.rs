//! WS-downstream relay Phase 2/3, Task 2: a bounded, same-account upstream re-dial helper.
//!
//! [`redial_upstream`] is the one shared primitive later tasks build reconnect-vs-move on: given an
//! already-resolved `account` (the conversation's pinned owner, or a freshly re-selected one), it
//! re-attempts [`dial_owner_upstream`] (Phase-1, `owner.rs`) up to [`MAX_REDIAL_ATTEMPTS`] times,
//! pausing [`REDIAL_BACKOFF`] between attempts, and hands back the first successful [`WsConn`] whose
//! client-visible upgrade contract matches the original downstream `101`. Its typed outcome keeps
//! an HTTP 401 distinct so the caller can refresh instead of retrying a stale bearer, and keeps
//! contract drift distinct from ordinary dial exhaustion.
//!
//! **Content-free (inviolable):** never logs the account id, the headers, or any dial error. No
//! `tracing`/`log`/`println!`/`eprintln!` anywhere in this module.

use axum::http::HeaderMap;
use polyflare_codex::{WsConn, WsRelayContract};
use polyflare_core::Account;

use super::owner::dial_owner_upstream;
use super::owner::RelayError;

pub(crate) enum RedialOutcome {
    Connected(Box<WsConn>),
    Unauthorized,
    ContractDrift,
    Unavailable,
}

/// The maximum number of upstream dial attempts [`redial_upstream`] makes before giving up.
pub(crate) const MAX_REDIAL_ATTEMPTS: u32 = 3;

/// The pause between successive redial attempts (skipped after the final attempt).
pub(crate) const REDIAL_BACKOFF: std::time::Duration = std::time::Duration::from_millis(250);

/// Re-dial `account`'s upstream WS, up to [`MAX_REDIAL_ATTEMPTS`] with [`REDIAL_BACKOFF`] between
/// tries. Returns the first successfully established [`WsConn`] only when its server model, models
/// ETag, and reasoning capability match the original downstream handshake. Contract drift returns
/// [`RedialOutcome::ContractDrift`] without retrying because another hidden handshake cannot repair
/// the already-issued downstream `101`. A handshake 401 similarly returns immediately as
/// [`RedialOutcome::Unauthorized`] so the caller can perform one synchronized token refresh.
///
/// Content-free: never logs the account, headers, contract values, or any error.
#[cfg(test)]
pub(crate) async fn redial_upstream(
    headers: &HeaderMap,
    account: &Account,
    expected_contract: &WsRelayContract,
) -> RedialOutcome {
    redial_upstream_with_models_etag(headers, account, expected_contract, None).await
}

/// Redial while comparing the catalog identity visible to the downstream client. `None` means
/// ordinary account-native comparison; `Some(value)` replaces the candidate's raw account ETag
/// with the pool virtual ETag (where `value` itself may be absent for a cold scoped catalog).
pub(crate) async fn redial_upstream_with_models_etag(
    headers: &HeaderMap,
    account: &Account,
    expected_contract: &WsRelayContract,
    models_etag_override: Option<Option<String>>,
) -> RedialOutcome {
    for attempt in 0..MAX_REDIAL_ATTEMPTS {
        match dial_owner_upstream(headers, account).await {
            Ok(conn)
                if models_etag_override.clone().map_or_else(
                    || conn.relay_contract().clone(),
                    |etag| conn.relay_contract().clone().with_models_etag(etag),
                ) == *expected_contract =>
            {
                return RedialOutcome::Connected(Box::new(conn));
            }
            Ok(_) => return RedialOutcome::ContractDrift,
            Err(RelayError::Upstream(polyflare_core::ExecError::UpstreamHttp(response)))
                if response.signal.status == 401 =>
            {
                return RedialOutcome::Unauthorized;
            }
            Err(_) => {}
        }
        if attempt + 1 < MAX_REDIAL_ATTEMPTS {
            tokio::time::sleep(REDIAL_BACKOFF).await;
        }
    }
    RedialOutcome::Unavailable
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
            is_fedramp: false,
        };
        let headers = HeaderMap::new();

        let expected_contract = WsRelayContract::default();
        let conn = redial_upstream(&headers, &account, &expected_contract).await;
        assert!(
            matches!(conn, RedialOutcome::Connected(_)),
            "redial_upstream must succeed against a live mock upstream"
        );
        assert_eq!(
            mock.handshake_count(),
            1,
            "exactly one successful upgrade for a first-attempt success"
        );
    }

    #[tokio::test]
    async fn pooled_redial_compares_the_current_virtual_catalog_etag() {
        let mock = MockWsUpstream::new(ScriptedTurn::normal(vec![]));
        let base = mock.clone().spawn().await;
        let account = Account {
            id: "acct-pooled-redial".to_string(),
            base_url: base,
            bearer_token: "owner-bearer".to_string(),
            chatgpt_account_id: Some("owner-cid".to_string()),
            is_fedramp: false,
        };
        let headers = HeaderMap::new();
        let expected =
            WsRelayContract::default().with_models_etag(Some("\"polyflare-pool-v1\"".to_string()));

        let stable = redial_upstream_with_models_etag(
            &headers,
            &account,
            &expected,
            Some(Some("\"polyflare-pool-v1\"".to_string())),
        )
        .await;
        assert!(matches!(stable, RedialOutcome::Connected(_)));

        let changed = redial_upstream_with_models_etag(
            &headers,
            &account,
            &expected,
            Some(Some("\"polyflare-pool-v2\"".to_string())),
        )
        .await;
        assert!(
            matches!(changed, RedialOutcome::ContractDrift),
            "another pool member changing the virtual ETag must close the stale downstream socket"
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
            is_fedramp: false,
        };
        let headers = HeaderMap::new();

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            redial_upstream(&headers, &account, &WsRelayContract::default()),
        )
        .await
        .expect("redial_upstream must return within the timeout, not hang");
        assert!(
            matches!(result, RedialOutcome::Unavailable),
            "every attempt against a closed port must fail, yielding None"
        );
    }
}
