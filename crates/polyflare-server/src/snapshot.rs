//! Assemble the selector's per-account snapshots from the durable store: each `Account` joined
//! with its latest `usage_history` row per window. Runtime fields (health tier, in-flight,
//! error/cooldown timestamps) are live-tracked later and default to neutral values here.

use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use polyflare_core::{AccountSnapshot, Provider, QuotaWindowSnapshot};
use polyflare_store::{Store, StoreError};

use crate::usage_windows::resolve;

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Build one `AccountSnapshot` per stored account. Capacity is derived from `plan_type` inside
/// the selector (no per-account override in M2b, so `capacity_credits` stays `None`).
///
/// Candidate order is the account `list()` order (`ORDER BY id` — deterministic, stable across
/// calls). The selector samples over this input order for seed-reproducible picks (same input
/// order + same seed ⇒ same pick), so callers must not reorder the returned `Vec` before passing
/// it to the selector.
pub async fn assemble_snapshots(store: &Store) -> Result<Vec<AccountSnapshot>, StoreError> {
    let repo = store.accounts();
    let accounts = repo.list().await?;
    let auth_modes = repo.list_auth_modes().await?;
    let mut snapshots = Vec::with_capacity(accounts.len());
    for account in accounts {
        // The `provider` column is NOT NULL with a DB-level default and only this crate's
        // `AccountRepo` ever writes it (always a known `Provider::Display` value). An unparseable
        // value therefore means data written outside the app's control: its backend is unknown, so
        // it cannot be routed to ANY pool. Exclude it from selection entirely — failing closed here
        // keeps this consistent with `resolve_core_account` (which also rejects an unknown provider)
        // and avoids surfacing a zombie candidate that would only hard-fail at resolve time.
        let provider = match Provider::from_str(&account.provider) {
            Ok(p) => p,
            Err(_) => continue,
        };
        let usage = repo.latest_usage(&account.id).await?;
        // Resolve by DURATION, not slot: the weekly-usage weight the selector reads must track the
        // real weekly window even when upstream moves it into the `primary` slot (see
        // `crate::usage_windows`). The freshest window of each kind wins, so a live weekly beats a
        // stale one left in the "expected" slot.
        let resolved = resolve(&usage, unix_now());
        let mut snap = AccountSnapshot::new(account.id.as_str());
        snap.status = account.status;
        snap.used_percent = resolved.five_hour.as_ref().map_or(0.0, |w| w.used_percent);
        snap.secondary_used_percent = resolved.weekly.as_ref().map_or(0.0, |w| w.used_percent);
        snap.five_hour_quota = resolved.five_hour.map(|window| QuotaWindowSnapshot {
            used_percent: window.used_percent,
            window_minutes: window.window_minutes,
            reset_at: window.reset_at,
            recorded_at: window.recorded_at,
            stale: window.stale,
        });
        snap.weekly_quota = resolved.weekly.map(|window| QuotaWindowSnapshot {
            used_percent: window.used_percent,
            window_minutes: window.window_minutes,
            reset_at: window.reset_at,
            recorded_at: window.recorded_at,
            stale: window.stale,
        });
        snap.reset_at = account.reset_at;
        snap.cooldown_until = repo.routing_cooldown(&account.id).await?;
        snap.usage_cap_percent = account.usage_cap_percent;
        snap.usage_cap_override = account.usage_cap_override;
        snap.routing_policy = account.routing_policy;
        snap.plan_type = account.plan_type;
        snap.security_work_authorized = account.security_work_authorized;
        snap.provider = provider;
        // An account whose auth mode this binary cannot identify is treated as a subscription
        // grant: that is the restrictive reading, and guessing the permissive one would risk
        // sending synthesized traffic on a credential that never authorized it.
        snap.subscription_oauth = !matches!(
            auth_modes.get(&account.id),
            Some(polyflare_store::AuthMode::CodexOauth)
                | Some(polyflare_store::AuthMode::StaticBearer)
        );
        snap.pools = repo.list_pools(&account.id).await?;
        snap.pool = account.pool.or_else(|| snap.pools.first().cloned());
        snapshots.push(snap);
    }
    Ok(snapshots)
}

/// Narrow candidates to one provider's pool. M4a has no cross-format translator (that's M4b), so
/// each ingress path must call this before `Selector::pick` — a request can only ever be routed to
/// an account whose provider matches the ingress path's own wire format.
pub fn filter_by_provider(
    snapshots: &[AccountSnapshot],
    provider: Provider,
) -> Vec<AccountSnapshot> {
    snapshots
        .iter()
        .filter(|s| s.provider == provider)
        .cloned()
        .collect()
}

/// Narrow candidates to a named account pool. `None` (the bare ingress paths — `/responses`,
/// `/v1/messages`) matches ALL accounts, so pre-pool routing is unchanged. `Some(slug)` (a
/// `/{pool}/...` path) matches ONLY accounts tagged with exactly that slug — an unpooled account
/// (`pool = None`) is reachable solely via the bare paths, never a named slug. Applied AFTER
/// `filter_by_provider` on the same shared snapshot slice, so both narrowings compose without a
/// per-pool cache.
pub fn filter_by_pool(snapshots: &[AccountSnapshot], pool: Option<&str>) -> Vec<AccountSnapshot> {
    match pool {
        None => snapshots.to_vec(),
        Some(slug) => snapshots
            .iter()
            .filter(|s| s.pools.iter().any(|membership| membership == slug))
            .cloned()
            .collect(),
    }
}

/// The provider + pool narrowings in a SINGLE pass — semantically identical to `filter_by_provider`
/// followed by `filter_by_pool`, but clones the surviving snapshots ONCE instead of building an
/// intermediate Vec. The ingress hot path uses this; the two functions above remain for callers
/// (and tests) that narrow by only one axis.
pub fn filter_by_provider_and_pool(
    snapshots: &[AccountSnapshot],
    provider: Provider,
    pool: Option<&str>,
) -> Vec<AccountSnapshot> {
    snapshots
        .iter()
        .filter(|s| {
            s.provider == provider
                && pool.is_none_or(|slug| s.pools.iter().any(|membership| membership == slug))
        })
        .cloned()
        .collect()
}

/// What kind of client traffic a `/v1/messages` request carries.
///
/// The two paths share `messages_handler_native`, but they are NOT interchangeable with respect to
/// which accounts may serve them, so the distinction is a parameter rather than a comment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessagesTraffic {
    /// A genuine Claude client request, admitted by `claude_wire::admit_native_request` and
    /// forwarded byte-for-byte with only the credential swapped.
    ClaudeNative,
    /// A request PolyFlare synthesized by translating another protocol (e.g. an OpenAI-Responses
    /// client served from an Anthropic pool). The bytes are ours, not a first-party client's.
    Translated,
}

/// Narrow candidates to those whose credential may serve this kind of traffic.
///
/// A subscription-OAuth grant authorizes a specific first-party client shape. Serving translated
/// traffic on one would mean presenting PolyFlare-synthesized bytes as that client — so those
/// accounts are excluded from translated traffic entirely. API-key and static-bearer accounts have
/// no such constraint and serve both.
///
/// This is the structural expression of "the translator is for API-key accounts only". It lives in
/// selection rather than in a check at egress so that a translated request can never reach a
/// subscription account at all: the account is not a candidate in the first place, and the request
/// fails with "no eligible account" instead of being sent on a credential that did not permit it.
pub fn filter_by_traffic_eligibility(
    snapshots: &[AccountSnapshot],
    traffic: MessagesTraffic,
) -> Vec<AccountSnapshot> {
    match traffic {
        MessagesTraffic::ClaudeNative => snapshots.to_vec(),
        MessagesTraffic::Translated => snapshots
            .iter()
            .filter(|s| !s.subscription_oauth)
            .cloned()
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(id: &str, pool: Option<&str>) -> AccountSnapshot {
        let mut s = AccountSnapshot::new(id);
        s.pool = pool.map(str::to_string);
        s.pools = pool.into_iter().map(str::to_string).collect();
        s
    }

    #[test]
    fn bare_path_matches_all_accounts_regardless_of_pool() {
        let snaps = vec![
            snap("a", None),
            snap("b", Some("p1")),
            snap("c", Some("p2")),
        ];
        let got = filter_by_pool(&snaps, None);
        assert_eq!(got.len(), 3, "None matches every account (backward compat)");
    }

    #[test]
    fn named_slug_matches_only_that_pool() {
        let snaps = vec![
            snap("a", None),
            snap("b", Some("p1")),
            snap("c", Some("p1")),
        ];
        let got = filter_by_pool(&snaps, Some("p1"));
        let ids: Vec<&str> = got.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(ids, vec!["b", "c"], "unpooled + other pools excluded");
    }

    #[test]
    fn unknown_slug_matches_nothing() {
        let snaps = vec![snap("a", None), snap("b", Some("p1"))];
        assert!(filter_by_pool(&snaps, Some("does-not-exist")).is_empty());
    }

    #[test]
    fn one_account_can_match_multiple_named_pools() {
        let mut account = snap("shared", Some("p1"));
        account.pools.push("p2".to_string());
        assert_eq!(filter_by_pool(&[account.clone()], Some("p1")).len(), 1);
        assert_eq!(filter_by_pool(&[account], Some("p2")).len(), 1);
    }
}

#[cfg(test)]
mod traffic_eligibility_tests {
    use super::*;

    fn account(id: &str, subscription_oauth: bool) -> AccountSnapshot {
        let mut snap = AccountSnapshot::new(id);
        snap.provider = Provider::Anthropic;
        snap.subscription_oauth = subscription_oauth;
        snap
    }

    #[test]
    fn translated_traffic_never_selects_a_subscription_oauth_account() {
        let candidates = vec![
            account("api-key-account", false),
            account("subscription-account", true),
        ];

        // Native pass-through may use either: a real client request is exactly what a subscription
        // grant authorizes, and an API-key account serves it happily too.
        let native = filter_by_traffic_eligibility(&candidates, MessagesTraffic::ClaudeNative);
        assert_eq!(native.len(), 2);

        // Translated traffic may use only the API-key account.
        let translated = filter_by_traffic_eligibility(&candidates, MessagesTraffic::Translated);
        assert_eq!(
            translated
                .iter()
                .map(|s| s.id.as_str().to_string())
                .collect::<Vec<_>>(),
            vec!["api-key-account".to_string()]
        );
    }

    #[test]
    fn a_subscription_only_pool_starves_translated_traffic_rather_than_serving_it() {
        let candidates = vec![
            account("subscription-a", true),
            account("subscription-b", true),
        ];
        // An empty candidate set makes the request fail with "no eligible account". That is the
        // intended outcome: failing the request is correct, sending it on a credential that never
        // authorized this client shape is not.
        assert!(filter_by_traffic_eligibility(&candidates, MessagesTraffic::Translated).is_empty());
        assert_eq!(
            filter_by_traffic_eligibility(&candidates, MessagesTraffic::ClaudeNative).len(),
            2
        );
    }
}
