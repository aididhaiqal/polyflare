//! In-memory decrypted-token cache: keeps `resolve_core_account`'s result (the account row + its
//! decrypted tokens) OFF the SQLite hot path, so a cached-account request does zero DB reads and
//! zero decrypts. Entries expire on a short TTL AND are cleared whenever the store's account
//! generation bumps (any account write — token refresh, status/pool/usage change), so a rotated
//! token is never served.
//!
//! # Security posture
//! This holds decrypted OAuth tokens in memory. That is the same secret already present in process
//! memory each request (we decrypt for the bearer regardless) and the same secret the real Codex
//! CLI keeps in *plaintext* at `~/.codex/auth.json` — plus PolyFlare's own at-rest key sits next to
//! the DB, so an attacker who can read this process's memory has already lost the game via those.
//! The cache only widens the in-memory window, so the mitigation is proportionate hygiene, not
//! enclaves: `PlainTokens` is `ZeroizeOnDrop`, so every evicted/expired entry (and every clone the
//! request uses) is wiped from memory on drop; `PlainTokens`' `Debug` is redacted so it never logs;
//! and the TTL bounds how long any token lives here — expired entries are swept on the next
//! `insert` (any cache miss) and cleared wholesale on every generation bump (the usage-refresh loop
//! bumps it ~every 600s), so an idle token doesn't linger indefinitely. (`mlock`-ing the pages to
//! bar swap is a possible follow-up, but it's the only thing that would improve on the `auth.json`
//! baseline.)

use std::collections::HashMap;
use std::sync::Mutex;

use polyflare_store::{Account, PlainTokens};

/// How long a resolved (account, tokens) entry may be served before a fresh DB read — a backstop on
/// staleness and on token lifetime in memory. Generation-invalidation (below) is the correctness
/// gate; the TTL is defense in depth.
const TTL_SECS: i64 = 300;

struct Entry {
    account: Account,
    tokens: PlainTokens,
    expires_at: i64,
}

struct Cache {
    /// The store account-generation these entries were populated under. A mismatch ⇒ every entry is
    /// potentially stale, so the whole map is cleared (and its tokens zeroized on drop).
    generation: u64,
    entries: HashMap<String, Entry>,
}

/// Process-local cache of resolved (account row, decrypted tokens), keyed by account id.
pub struct TokenCache {
    inner: Mutex<Cache>,
}

impl Default for TokenCache {
    fn default() -> Self {
        Self::new()
    }
}

impl TokenCache {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Cache {
                generation: 0,
                entries: HashMap::new(),
            }),
        }
    }

    /// The cached (account, tokens) for `id` if the store generation still matches AND the entry is
    /// unexpired; otherwise `None` (the caller loads from the store and `insert`s). `now` is
    /// unix-epoch seconds (the request's own clock — no extra syscall).
    ///
    /// The store generation is MONOTONIC (an `AtomicU64` that only counts up per account write), so
    /// generation handling is monotonic too: a NEWER generation clears all entries (they may be
    /// stale — tokens zeroized on drop); an OLDER generation means the caller captured its value
    /// before a write this cache has already advanced past, so its view is stale → treat as a miss
    /// WITHOUT disturbing the fresher cache (never regress `c.generation`).
    pub fn get(&self, id: &str, store_generation: u64, now: i64) -> Option<(Account, PlainTokens)> {
        let mut c = self.inner.lock().unwrap();
        if store_generation > c.generation {
            c.entries.clear();
            c.generation = store_generation;
            return None;
        }
        if store_generation < c.generation {
            return None; // caller's generation is stale; don't serve, don't regress
        }
        match c.entries.get(id) {
            Some(e) if e.expires_at > now => Some((e.account.clone(), e.tokens.clone())),
            Some(_) => {
                c.entries.remove(id); // expired → drop (zeroizes)
                None
            }
            None => None,
        }
    }

    /// Cache a freshly-loaded (account, tokens) for `id` under the current store generation. A NEWER
    /// generation clears the map first; an OLDER generation (the caller loaded under a since-
    /// superseded generation) is DROPPED — inserting it would regress the cache and re-poison it
    /// with a possibly-stale entry. Also opportunistically evicts any expired entries so an idle
    /// token doesn't outlive its TTL in memory just because it's never read again.
    pub fn insert(
        &self,
        id: &str,
        account: Account,
        tokens: PlainTokens,
        store_generation: u64,
        now: i64,
    ) {
        let mut c = self.inner.lock().unwrap();
        if store_generation < c.generation {
            return; // stale caller — do not regress the cache
        }
        if store_generation > c.generation {
            c.entries.clear();
            c.generation = store_generation;
        }
        // Opportunistic sweep: drop (and zeroize) anything already past its TTL. O(accounts), cheap.
        c.entries.retain(|_, e| e.expires_at > now);
        c.entries.insert(
            id.to_string(),
            Entry {
                account,
                tokens,
                expires_at: now + TTL_SECS,
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokens(access: &str) -> PlainTokens {
        PlainTokens {
            access_token: access.to_string(),
            refresh_token: "r".to_string(),
            id_token: "i".to_string(),
        }
    }

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
            pool: None,
        }
    }

    #[test]
    fn hit_within_ttl_and_same_generation() {
        let cache = TokenCache::new();
        cache.insert("a", account("a"), tokens("tok"), 1, 1000);
        let got = cache.get("a", 1, 1200).expect("hit within ttl");
        assert_eq!(got.1.access_token, "tok");
        assert_eq!(got.0.id, "a");
    }

    #[test]
    fn miss_after_ttl_expiry() {
        let cache = TokenCache::new();
        cache.insert("a", account("a"), tokens("tok"), 1, 1000);
        assert!(cache.get("a", 1, 1000 + TTL_SECS + 1).is_none(), "expired");
    }

    #[test]
    fn generation_bump_invalidates_all_entries() {
        let cache = TokenCache::new();
        cache.insert("a", account("a"), tokens("old"), 1, 1000);
        // A token refresh (etc.) bumps the store generation → the whole cache is stale.
        assert!(
            cache.get("a", 2, 1100).is_none(),
            "generation mismatch clears"
        );
        // And it stays cleared under the new generation until repopulated.
        assert!(cache.get("a", 2, 1100).is_none());
        cache.insert("a", account("a"), tokens("new"), 2, 1100);
        assert_eq!(cache.get("a", 2, 1150).unwrap().1.access_token, "new");
    }

    #[test]
    fn miss_for_unknown_id() {
        let cache = TokenCache::new();
        cache.insert("a", account("a"), tokens("tok"), 1, 1000);
        assert!(cache.get("b", 1, 1000).is_none());
    }

    #[test]
    fn stale_generation_does_not_regress_or_poison_the_cache() {
        // A request that captured an OLD generation (before a concurrent write bumped it) must not
        // clobber the fresher cache with its stale entry (the generation-regression race).
        let cache = TokenCache::new();
        // A peer populated the cache with the NEW token at generation 6.
        cache.insert("a", account("a"), tokens("new"), 6, 1000);
        // A slow request that loaded under generation 5 tries to insert its (now stale) entry.
        cache.insert("a", account("a"), tokens("stale"), 5, 1000);
        // The cache must still hold the gen-6 value, at generation 6.
        assert_eq!(
            cache.get("a", 6, 1050).unwrap().1.access_token,
            "new",
            "stale insert must not regress the cache"
        );
        // A get carrying the stale generation 5 is a miss and must not disturb the fresher cache.
        assert!(
            cache.get("a", 5, 1050).is_none(),
            "stale get → miss, no clobber"
        );
        assert_eq!(cache.get("a", 6, 1050).unwrap().1.access_token, "new");
    }

    #[test]
    fn insert_sweeps_expired_entries() {
        // An idle entry past its TTL is evicted on the next insert (of any id), bounding token
        // lifetime even if it's never read again.
        let cache = TokenCache::new();
        cache.insert("idle", account("idle"), tokens("old"), 1, 1000);
        // Much later, a different account is resolved → the insert sweeps the expired "idle" entry.
        cache.insert(
            "other",
            account("other"),
            tokens("t"),
            1,
            1000 + TTL_SECS + 1,
        );
        assert!(
            cache.get("idle", 1, 1000 + TTL_SECS + 2).is_none(),
            "swept on the later insert"
        );
    }

    #[test]
    fn plaintokens_is_zeroize_on_drop() {
        // Compile-time proof that dropping a cache entry wipes its tokens.
        fn assert_zeroize_on_drop<T: zeroize::ZeroizeOnDrop>() {}
        assert_zeroize_on_drop::<PlainTokens>();
    }
}
