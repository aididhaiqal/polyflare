//! Assemble the selector's per-account snapshots from the durable store: each `Account` joined
//! with its latest `usage_history` row per window. Runtime fields (health tier, in-flight,
//! error/cooldown timestamps) are live-tracked later and default to neutral values here.

use std::str::FromStr;

use polyflare_core::{AccountSnapshot, Provider};
use polyflare_store::{Store, StoreError};

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
        let usage = repo.latest_usage(&account.id).await?;
        let mut snap = AccountSnapshot::new(account.id.as_str());
        snap.status = account.status;
        snap.used_percent = usage.primary.as_ref().map_or(0.0, |w| w.used_percent);
        snap.secondary_used_percent = usage.secondary.as_ref().map_or(0.0, |w| w.used_percent);
        snap.reset_at = account.reset_at;
        snap.routing_policy = account.routing_policy;
        snap.plan_type = account.plan_type;
        snap.security_work_authorized = account.security_work_authorized;
        // Defensive default: the `provider` column is NOT NULL with a DB-level default, and only
        // this crate's `AccountRepo` ever writes it, so an unparseable value means data written
        // outside the app's control — fail safe to `Codex` rather than dropping the account.
        snap.provider = Provider::from_str(&account.provider).unwrap_or(Provider::Codex);
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
