//! In-memory account-snapshot cache: serves the selector's per-account snapshots from memory
//! instead of rebuilding them from sqlite on every request.
//!
//! # Why
//! `assemble_snapshots` (see `crate::snapshot`) runs an O(accounts) `list()` + a per-account usage
//! join. It ran on EVERY request — cheap warm (sqlite is in-process), but O(N) and disk-bound cold.
//! The account pool barely changes between requests, so this caches the built `Vec<AccountSnapshot>`
//! and only rebuilds it when a write actually changes account state (or a short TTL elapses).
//!
//! # What it does NOT hold
//! - Decrypted tokens — never cached (the picked account is still decrypted fresh per request in
//!   `resolve_core_account`, preserving the "encrypted at rest" posture). Only non-secret snapshot
//!   metadata lives here.
//! - Live per-request runtime counters (in-flight / cooldown / health) — those churn every request;
//!   when M3 lands they belong in a separate overlay layered on at read time, not in this pool cache.
//!
//! # Automatic invalidation via the store's write generation
//! Freshness is driven by [`Store::account_generation`] — an in-process counter every `AccountRepo`
//! write bumps. Each read compares the store's current generation against the one the cache was
//! built at; any write (from any caller — refresh path, a future admin API, the usage scheduler)
//! advances it and forces a rebuild, so no caller can forget to invalidate. [`AccountCache::invalidate`]
//! remains as a manual force-refresh on top of that. Because PolyFlare is one process the counter is
//! a plain atomic — no cross-process coordination (codex-lb needs a shared-DB version-counter poller
//! only because its k8s prod option runs 3–20 replicas on Postgres; a single binary has no such need).

use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use polyflare_core::AccountSnapshot;
use polyflare_store::{Store, StoreError};

use crate::snapshot::assemble_snapshots;

/// TTL backstop only — on-write [`AccountCache::invalidate`] is the primary freshness mechanism;
/// this merely bounds staleness if a write site is ever missed. (codex-lb uses the same 5s value.)
const TTL: Duration = Duration::from_secs(5);

struct Cached {
    snapshots: Arc<Vec<AccountSnapshot>>,
    built_at: Instant,
    /// The store's account-write generation this pool was built at. A later read whose current
    /// generation differs knows a write happened and rebuilds.
    generation: u64,
}

/// Caches the selector-input snapshot `Vec` behind a TTL + on-write invalidation, single-flighting
/// rebuilds. Holds no store handle — the store is passed to [`Self::snapshots`] at call time (the
/// caller already owns it on `AppState`), which keeps this free of any `Store` clone/ownership.
#[derive(Default)]
pub struct AccountCache {
    /// Sync-lockable value cell for zero-I/O hot-path reads.
    cached: RwLock<Option<Cached>>,
    /// Single-flight guard so a cold/expired miss under load rebuilds ONCE, not once per request.
    rebuild_lock: tokio::sync::Mutex<()>,
}

impl AccountCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// The selector's per-account snapshots: served from memory when fresh, else rebuilt from
    /// `store` once (single-flighted) via `assemble_snapshots`. Returns an `Arc` clone so callers
    /// share one built `Vec` without copying it.
    pub async fn snapshots(&self, store: &Store) -> Result<Arc<Vec<AccountSnapshot>>, StoreError> {
        // Read the generation BEFORE assembling: if a write races the rebuild, the pool is tagged
        // with the pre-write generation, so the NEXT read (higher generation) rebuilds — worst case
        // an extra rebuild, never a stale serve.
        let generation = store.account_generation();
        if let Some(s) = self.fresh(generation) {
            return Ok(s);
        }
        let _guard = self.rebuild_lock.lock().await;
        // Re-check under the single-flight lock: a peer may have rebuilt while we waited.
        let generation = store.account_generation();
        if let Some(s) = self.fresh(generation) {
            return Ok(s);
        }
        let snapshots = Arc::new(assemble_snapshots(store).await?);
        *self.cached.write().expect("account cache lock poisoned") = Some(Cached {
            snapshots: snapshots.clone(),
            built_at: Instant::now(),
            generation,
        });
        Ok(snapshots)
    }

    /// Manually force the next [`Self::snapshots`] to rebuild. Writes already invalidate
    /// automatically via the store's generation counter; this is a belt-and-suspenders override
    /// (e.g. after a change that doesn't go through `AccountRepo`).
    pub fn invalidate(&self) {
        *self.cached.write().expect("account cache lock poisoned") = None;
    }

    /// The cached snapshots if present, still at the current write generation, and within the TTL.
    fn fresh(&self, current_generation: u64) -> Option<Arc<Vec<AccountSnapshot>>> {
        let guard = self.cached.read().expect("account cache lock poisoned");
        match guard.as_ref() {
            Some(c) if c.generation == current_generation && c.built_at.elapsed() < TTL => {
                Some(c.snapshots.clone())
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polyflare_store::{Account, PlainTokens, TokenCipher};

    fn account(id: &str) -> Account {
        Account {
            id: id.to_string(),
            chatgpt_account_id: None,
            chatgpt_user_id: None,
            email: "u@example.test".to_string(),
            alias: None,
            workspace_id: None,
            workspace_label: None,
            seat_type: None,
            plan_type: "pro".to_string(),
            routing_policy: "normal".to_string(),
            last_refresh: 0,
            created_at: 0,
            status: "active".to_string(),
            deactivation_reason: None,
            reset_at: None,
            blocked_at: None,
            security_work_authorized: false,
            provider: "codex".to_string(),
        }
    }

    async fn insert(store: &Store, cipher: &TokenCipher, id: &str) {
        store
            .accounts()
            .insert(
                &account(id),
                &PlainTokens {
                    access_token: "a".into(),
                    refresh_token: "r".into(),
                    id_token: "i".into(),
                },
                cipher,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn serves_cached_pool_until_a_write_bumps_the_generation() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("s.db")).await.unwrap();
        let cipher = TokenCipher::from_key_bytes(&[7u8; 32]).unwrap();
        insert(&store, &cipher, "a1").await;

        let cache = AccountCache::new();
        let first = cache.snapshots(&store).await.unwrap();
        assert_eq!(first.len(), 1);

        // No write since: a second read serves the exact same cached `Arc` — no rebuild, no query.
        let second = cache.snapshots(&store).await.unwrap();
        assert!(
            Arc::ptr_eq(&first, &second),
            "no write happened; the cache must serve the same pool without rebuilding"
        );

        // A write bumps the store's generation, so the next read auto-rebuilds and sees the new
        // account — with NO explicit invalidate() (the whole point of the generation counter: any
        // write, from any caller, invalidates).
        insert(&store, &cipher, "a2").await;
        let third = cache.snapshots(&store).await.unwrap();
        assert!(
            !Arc::ptr_eq(&first, &third),
            "a write must force a rebuild (new Arc)"
        );
        assert_eq!(
            third.len(),
            2,
            "rebuild must see the account added since caching"
        );
    }

    #[tokio::test]
    async fn manual_invalidate_forces_a_rebuild() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("s.db")).await.unwrap();
        let cipher = TokenCipher::from_key_bytes(&[9u8; 32]).unwrap();
        insert(&store, &cipher, "a1").await;

        let cache = AccountCache::new();
        let first = cache.snapshots(&store).await.unwrap();
        cache.invalidate();
        let second = cache.snapshots(&store).await.unwrap();
        assert!(
            !Arc::ptr_eq(&first, &second),
            "invalidate() must force the next read to rebuild even with no write"
        );
        assert_eq!(second.len(), 1);
    }
}
