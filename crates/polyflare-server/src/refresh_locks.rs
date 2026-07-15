//! Per-account OAuth refresh singleflight coordination (F2). Concurrent requests that select the
//! SAME stale account must not all call the OAuth refresh endpoint with the same (soon-to-be-dead)
//! refresh token — OpenAI rotates the refresh token on first use, so a second concurrent call would
//! present a dead token and get the account wrongly marked `reauth_required`. `RefreshLocks` hands
//! out one `tokio::sync::Mutex` per `AccountId` so `resolve_core_account` can serialize the refresh
//! for a given account while leaving unrelated accounts' refreshes fully concurrent.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::Mutex as AsyncMutex;

use polyflare_core::AccountId;

/// A registry of per-account async locks, created lazily on first use.
#[derive(Default)]
pub struct RefreshLocks {
    map: Mutex<HashMap<AccountId, Arc<AsyncMutex<()>>>>,
}

impl RefreshLocks {
    /// Returns the per-account async lock, creating it on first use. The std `Mutex` is held only
    /// to get-or-insert the `Arc` (never across `.await`); the returned `tokio::sync::Mutex` is the
    /// actual refresh lock the caller should `.lock().await` around the refresh-if-stale sequence.
    pub fn handle(&self, id: &AccountId) -> Arc<AsyncMutex<()>> {
        let mut map = self.map.lock().expect("refresh_locks mutex poisoned");
        map.entry(id.clone())
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handle_returns_same_lock_for_same_account() {
        let locks = RefreshLocks::default();
        let a = AccountId::from("acct-1");
        let h1 = locks.handle(&a);
        let h2 = locks.handle(&a);
        assert!(Arc::ptr_eq(&h1, &h2), "same account must share one lock");
    }

    #[test]
    fn handle_returns_distinct_locks_for_distinct_accounts() {
        let locks = RefreshLocks::default();
        let a = AccountId::from("acct-1");
        let b = AccountId::from("acct-2");
        let h1 = locks.handle(&a);
        let h2 = locks.handle(&b);
        assert!(
            !Arc::ptr_eq(&h1, &h2),
            "distinct accounts must get distinct locks"
        );
    }

    #[tokio::test]
    async fn lock_actually_serializes() {
        let locks = RefreshLocks::default();
        let a = AccountId::from("acct-1");
        let lock = locks.handle(&a);
        let guard = lock.lock().await;
        // A second attempt to lock the same account's handle must not succeed immediately.
        let lock2 = locks.handle(&a);
        assert!(
            lock2.try_lock().is_err(),
            "the lock must be held while a guard is outstanding"
        );
        drop(guard);
        assert!(
            lock2.try_lock().is_ok(),
            "the lock must be free after the guard drops"
        );
    }
}
