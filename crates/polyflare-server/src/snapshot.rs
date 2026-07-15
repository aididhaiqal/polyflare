//! Assemble the selector's per-account snapshots from the durable store: each `Account` joined
//! with its latest `usage_history` row per window. Runtime fields (health tier, in-flight,
//! error/cooldown timestamps) are live-tracked later and default to neutral values here.

use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use polyflare_core::{AccountSnapshot, Provider};
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
        snap.reset_at = account.reset_at;
        snap.routing_policy = account.routing_policy;
        snap.plan_type = account.plan_type;
        snap.security_work_authorized = account.security_work_authorized;
        snap.provider = provider;
        snap.pool = account.pool;
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
            .filter(|s| s.pool.as_deref() == Some(slug))
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
}
