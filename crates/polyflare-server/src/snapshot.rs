//! Assemble the selector's per-account snapshots from the durable store: each `Account` joined
//! with its latest `usage_history` row per window. Runtime fields (health tier, in-flight,
//! error/cooldown timestamps) are live-tracked later and default to neutral values here.

use polyflare_core::AccountSnapshot;
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
        snapshots.push(snap);
    }
    Ok(snapshots)
}
