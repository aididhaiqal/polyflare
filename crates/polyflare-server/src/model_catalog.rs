//! Live upstream Codex model-catalog cache: keeps `/models` fresh instead of only the compiled-in
//! static bootstrap floor, by periodically fetching the real upstream catalog and merging it onto
//! the floor.
//!
//! Mirrors `polyflare_codex::codex_version::CodexVersionCache` exactly (see that module's doc
//! comment): a `RwLock<Option<Cached>>` value cell for zero-I/O hot-path reads, a
//! `tokio::sync::Mutex<()>` single-flight guard around the actual refresh, a TTL, and a fallback
//! ladder — fresh cached -> live fetch -> stale cached -> static floor. The floor is NEVER removed
//! by a merge and this cache NEVER returns an empty catalog.
//!
//! # This module (Task 1 of D15)
//! Only the cache primitive + the pure merge fn + `ModelSource` trait are here, exercised via a
//! `#[cfg(test)]` `StubSource` (no mocking crate, no real network — mirrors `codex_version.rs`'s
//! `StubSource`). The production `HttpModelSource` (account-bearer upstream fetch) and the wiring
//! into `catalog.rs`/`AppState`/config are later tasks in the same plan.

use std::sync::RwLock;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tracing::warn;

/// Default TTL for tests exercising the fallback ladder; production wiring (a later task) will
/// pass an explicit config-derived TTL into [`ModelCatalogCache::new`].
#[cfg(test)]
const DEFAULT_TTL: Duration = Duration::from_secs(3600);

/// One model from the upstream catalog. Superset of `catalog.rs`'s static `CatalogModel`; extra
/// fields optional and only populated when the live upstream fetch supplies them (kept minimal —
/// YAGNI — rather than porting codex-lb's full field set).
#[derive(Clone, Debug, PartialEq)]
pub struct UpstreamModel {
    pub slug: String,
    pub display_name: String,
    /// Context window size in tokens, when the upstream entry advertises one.
    pub context_window: Option<u64>,
    /// Whether this model prefers the WebSocket transport, when upstream advertises it.
    pub prefer_websockets: Option<bool>,
}

/// An async source of the upstream model catalog. Abstracted so the cache's TTL / single-flight /
/// fallback logic is unit-testable without real network I/O; production uses `HttpModelSource`
/// (a later task). Mirrors `codex_version::VersionSource`'s exact mechanism (this crate's
/// `async-trait` idiom, not a hand-rolled boxed future).
#[async_trait]
pub trait ModelSource: Send + Sync {
    /// Returns the freshly-fetched upstream catalog, or `None` if the fetch failed in any way
    /// (transport error, non-2xx, parse failure, no active account, etc).
    async fn fetch(&self) -> Option<Vec<UpstreamModel>>;
}

struct Cached {
    models: Vec<UpstreamModel>,
    fetched_at: Instant,
}

/// Caches the merged (upstream-onto-floor) model catalog behind a TTL, single-flighting refreshes
/// and degrading gracefully — down to the static floor — when upstream is unavailable.
pub struct ModelCatalogCache {
    ttl: Duration,
    /// Sync-lockable value cell for zero-I/O hot-path reads (`cached_or_fallback`). Kept separate
    /// from `refresh_lock` so a sync reader never blocks on an in-flight fetch.
    cached: RwLock<Option<Cached>>,
    /// Single-flight guard: only one refresh touches the network at a time (concurrent
    /// `get_or_refresh` callers on a cold/expired cache collapse to one upstream fetch).
    refresh_lock: tokio::sync::Mutex<()>,
    source: Box<dyn ModelSource>,
    /// The static bootstrap floor. Always present in every returned catalog; a merge never removes
    /// a floor slug even when upstream omits it.
    floor: Vec<UpstreamModel>,
}

impl ModelCatalogCache {
    pub fn new(source: Box<dyn ModelSource>, ttl: Duration, floor: Vec<UpstreamModel>) -> Self {
        Self {
            ttl,
            cached: RwLock::new(None),
            refresh_lock: tokio::sync::Mutex::new(()),
            source,
            floor,
        }
    }

    /// The refresh cadence — a background warmer should re-call `get_or_refresh` on this interval
    /// to keep the cache fresh for the sync hot path.
    pub fn refresh_interval(&self) -> Duration {
        self.ttl
    }

    /// Resolve the merged catalog, refreshing from upstream when the cache is cold or older than
    /// the TTL. Fallback ladder on refresh failure: fresh cache -> live fetch (merged onto floor)
    /// -> stale cache -> static floor. Concurrent callers single-flight through one fetch. Never
    /// returns an empty catalog.
    pub async fn get_or_refresh(&self) -> Vec<UpstreamModel> {
        if let Some(models) = self.fresh_cached() {
            return models;
        }

        let _guard = self.refresh_lock.lock().await;
        // Re-check under the lock: a concurrent caller may have refreshed while we waited.
        if let Some(models) = self.fresh_cached() {
            return models;
        }

        match self.source.fetch().await {
            Some(upstream) => {
                let merged = merge_onto_floor(&upstream, &self.floor);
                *self
                    .cached
                    .write()
                    .expect("model catalog cache lock poisoned") = Some(Cached {
                    models: merged.clone(),
                    fetched_at: Instant::now(),
                });
                merged
            }
            None => {
                // Upstream fetch failed. Prefer a stale-but-real cached value over the floor.
                if let Some(stale) = self
                    .cached
                    .read()
                    .expect("model catalog cache lock poisoned")
                    .as_ref()
                {
                    warn!(
                        stale_model_count = stale.models.len(),
                        "model catalog upstream unavailable; using stale cached catalog"
                    );
                    return stale.models.clone();
                }
                warn!(
                    floor_model_count = self.floor.len(),
                    "model catalog upstream unavailable and cache cold; using static floor"
                );
                self.floor.clone()
            }
        }
    }

    /// Synchronous, zero-I/O read for the `/models` hot path: the cached catalog (even if past its
    /// TTL) or the static floor when never warmed. Never blocks on the network, never empty.
    pub fn cached_or_fallback(&self) -> Vec<UpstreamModel> {
        self.cached
            .read()
            .expect("model catalog cache lock poisoned")
            .as_ref()
            .map(|c| c.models.clone())
            .unwrap_or_else(|| self.floor.clone())
    }

    /// The cached catalog if present and within the TTL, else `None`.
    fn fresh_cached(&self) -> Option<Vec<UpstreamModel>> {
        let guard = self
            .cached
            .read()
            .expect("model catalog cache lock poisoned");
        match guard.as_ref() {
            Some(c) if c.fetched_at.elapsed() < self.ttl => Some(c.models.clone()),
            _ => None,
        }
    }
}

/// Merge the live `upstream` catalog onto the static `floor`: start from `floor`, then upsert each
/// upstream model by slug (upstream wins on collision), appending any upstream-only slugs. Floor
/// slugs are NEVER removed, even when upstream omits them. Deterministic order: floor order is
/// preserved (with upstream's data substituted in on collision), then new upstream slugs are
/// appended in upstream order.
pub fn merge_onto_floor(upstream: &[UpstreamModel], floor: &[UpstreamModel]) -> Vec<UpstreamModel> {
    let mut merged: Vec<UpstreamModel> = floor.to_vec();

    for u in upstream {
        if let Some(existing) = merged.iter_mut().find(|m| m.slug == u.slug) {
            *existing = u.clone();
        } else {
            merged.push(u.clone());
        }
    }

    merged
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::*;

    fn model(slug: &str, name: &str) -> UpstreamModel {
        UpstreamModel {
            slug: slug.to_string(),
            display_name: name.to_string(),
            context_window: None,
            prefer_websockets: None,
        }
    }

    fn slugs(models: &[UpstreamModel]) -> Vec<&str> {
        models.iter().map(|m| m.slug.as_str()).collect()
    }

    // --- merge_onto_floor (pure fn) ---

    #[test]
    fn merge_onto_floor_preserves_floor_only_slug() {
        let floor = vec![model("gpt-5.5", "GPT-5.5"), model("gpt-5.4", "GPT-5.4")];
        let upstream = vec![model("gpt-5.6-sol", "GPT-5.6 Sol")];
        let merged = merge_onto_floor(&upstream, &floor);
        assert_eq!(slugs(&merged), vec!["gpt-5.5", "gpt-5.4", "gpt-5.6-sol"]);
    }

    #[test]
    fn merge_onto_floor_appends_upstream_only_slug() {
        let floor = vec![model("gpt-5.5", "GPT-5.5")];
        let upstream = vec![
            model("gpt-5.5", "GPT-5.5"),
            model("gpt-5.7-nova", "GPT-5.7 Nova"),
        ];
        let merged = merge_onto_floor(&upstream, &floor);
        assert_eq!(slugs(&merged), vec!["gpt-5.5", "gpt-5.7-nova"]);
    }

    #[test]
    fn merge_onto_floor_upstream_wins_on_collision() {
        let floor = vec![UpstreamModel {
            slug: "gpt-5.5".to_string(),
            display_name: "GPT-5.5".to_string(),
            context_window: None,
            prefer_websockets: None,
        }];
        let upstream = vec![UpstreamModel {
            slug: "gpt-5.5".to_string(),
            display_name: "GPT-5.5 (enriched)".to_string(),
            context_window: Some(128_000),
            prefer_websockets: Some(true),
        }];
        let merged = merge_onto_floor(&upstream, &floor);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].display_name, "GPT-5.5 (enriched)");
        assert_eq!(merged[0].context_window, Some(128_000));
        assert_eq!(merged[0].prefer_websockets, Some(true));
    }

    #[test]
    fn merge_onto_floor_never_removes_floor_slugs_when_upstream_empty() {
        let floor = vec![model("gpt-5.5", "GPT-5.5"), model("gpt-5.4", "GPT-5.4")];
        let merged = merge_onto_floor(&[], &floor);
        assert_eq!(slugs(&merged), vec!["gpt-5.5", "gpt-5.4"]);
    }

    /// A [`ModelSource`] returning a scripted value and counting its calls (mirrors
    /// `codex_version::tests::StubSource`).
    struct StubSource {
        value: std::sync::Mutex<Option<Vec<UpstreamModel>>>,
        calls: AtomicUsize,
        delay: Duration,
    }

    impl StubSource {
        fn new(value: Option<Vec<UpstreamModel>>) -> Arc<Self> {
            Arc::new(Self {
                value: std::sync::Mutex::new(value),
                calls: AtomicUsize::new(0),
                delay: Duration::ZERO,
            })
        }
        fn with_delay(value: Option<Vec<UpstreamModel>>, delay: Duration) -> Arc<Self> {
            Arc::new(Self {
                value: std::sync::Mutex::new(value),
                calls: AtomicUsize::new(0),
                delay,
            })
        }
        fn set(&self, value: Option<Vec<UpstreamModel>>) {
            *self.value.lock().unwrap() = value;
        }
        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl ModelSource for StubSource {
        async fn fetch(&self) -> Option<Vec<UpstreamModel>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            self.value.lock().unwrap().clone()
        }
    }

    /// Boxable wrapper so a shared `Arc<StubSource>` (kept by the test to assert on) can also be
    /// the cache's owned `Box<dyn ModelSource>`.
    struct SharedSource(Arc<StubSource>);
    #[async_trait]
    impl ModelSource for SharedSource {
        async fn fetch(&self) -> Option<Vec<UpstreamModel>> {
            self.0.fetch().await
        }
    }

    fn cache_with(
        stub: &Arc<StubSource>,
        ttl: Duration,
        floor: Vec<UpstreamModel>,
    ) -> ModelCatalogCache {
        ModelCatalogCache::new(Box::new(SharedSource(stub.clone())), ttl, floor)
    }

    fn default_floor() -> Vec<UpstreamModel> {
        vec![model("gpt-5.5", "GPT-5.5"), model("gpt-5.4", "GPT-5.4")]
    }

    // --- (a) fresh fetch merges upstream onto floor ---

    #[tokio::test]
    async fn get_or_refresh_merges_fresh_fetch_onto_floor() {
        let stub = StubSource::new(Some(vec![
            model("gpt-5.5", "GPT-5.5 (enriched)"), // collision -> upstream wins
            model("gpt-5.6-sol", "GPT-5.6 Sol"),    // upstream-only -> appended
        ]));
        let cache = cache_with(&stub, DEFAULT_TTL, default_floor());
        let models = cache.get_or_refresh().await;
        assert_eq!(slugs(&models), vec!["gpt-5.5", "gpt-5.4", "gpt-5.6-sol"]);
        // floor-only slug survives
        assert!(models.iter().any(|m| m.slug == "gpt-5.4"));
        // collision took upstream's display_name
        let gpt55 = models.iter().find(|m| m.slug == "gpt-5.5").unwrap();
        assert_eq!(gpt55.display_name, "GPT-5.5 (enriched)");
    }

    // --- (b) TTL expiry re-fetches ---

    #[tokio::test]
    async fn get_or_refresh_refetches_after_ttl_expiry() {
        let stub = StubSource::new(Some(vec![model("gpt-5.6-sol", "GPT-5.6 Sol")]));
        let cache = cache_with(&stub, Duration::from_millis(5), default_floor());
        let first = cache.get_or_refresh().await;
        assert!(first.iter().any(|m| m.slug == "gpt-5.6-sol"));
        assert_eq!(stub.calls(), 1);

        stub.set(Some(vec![model("gpt-5.7-nova", "GPT-5.7 Nova")]));
        tokio::time::sleep(Duration::from_millis(10)).await;

        let second = cache.get_or_refresh().await;
        assert!(second.iter().any(|m| m.slug == "gpt-5.7-nova"));
        assert_eq!(stub.calls(), 2);
    }

    #[tokio::test]
    async fn get_or_refresh_within_ttl_does_not_refetch() {
        let stub = StubSource::new(Some(vec![model("gpt-5.6-sol", "GPT-5.6 Sol")]));
        let cache = cache_with(&stub, DEFAULT_TTL, default_floor());
        cache.get_or_refresh().await;
        cache.get_or_refresh().await;
        assert_eq!(stub.calls(), 1);
    }

    // --- (c) single-flight ---

    #[tokio::test]
    async fn concurrent_get_or_refresh_single_flights_one_fetch() {
        // A slow fetch forces overlap: all callers find a cold cache and queue on the refresh
        // lock; only the first fetches, the rest re-check and get the fresh value.
        let stub = StubSource::with_delay(
            Some(vec![model("gpt-5.6-sol", "GPT-5.6 Sol")]),
            Duration::from_millis(30),
        );
        let cache = Arc::new(cache_with(&stub, DEFAULT_TTL, default_floor()));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let c = cache.clone();
            handles.push(tokio::spawn(async move { c.get_or_refresh().await }));
        }
        for h in handles {
            let models = h.await.unwrap();
            assert!(models.iter().any(|m| m.slug == "gpt-5.6-sol"));
        }
        assert_eq!(
            stub.calls(),
            1,
            "concurrent callers must collapse to one fetch"
        );
    }

    // --- (d) source None => stale then floor, never empty ---

    #[tokio::test]
    async fn get_or_refresh_falls_back_to_floor_when_source_fails_cold() {
        let stub = StubSource::new(None);
        let floor = default_floor();
        let cache = cache_with(&stub, DEFAULT_TTL, floor.clone());
        let models = cache.get_or_refresh().await;
        assert_eq!(models, floor);
        assert!(!models.is_empty());
    }

    #[tokio::test]
    async fn get_or_refresh_prefers_stale_cache_over_floor_when_source_fails() {
        let stub = StubSource::new(Some(vec![model("gpt-5.6-sol", "GPT-5.6 Sol")]));
        let cache = cache_with(&stub, Duration::from_millis(5), default_floor());
        let warmed = cache.get_or_refresh().await; // warm
        assert!(warmed.iter().any(|m| m.slug == "gpt-5.6-sol"));

        stub.set(None); // upstream now fails
        tokio::time::sleep(Duration::from_millis(10)).await; // expire TTL

        // Expired AND source fails -> serve the stale-but-real merged value, not the floor.
        let stale = cache.get_or_refresh().await;
        assert!(stale.iter().any(|m| m.slug == "gpt-5.6-sol"));
        assert!(!stale.is_empty());
    }

    // --- (e) cached_or_fallback: sync, zero-I/O ---

    #[test]
    fn cached_or_fallback_returns_floor_when_unwarmed() {
        let stub = StubSource::new(Some(vec![model("gpt-5.6-sol", "GPT-5.6 Sol")]));
        let floor = default_floor();
        let cache = cache_with(&stub, DEFAULT_TTL, floor.clone());
        // No get_or_refresh() call yet -> sync read must return the floor, no network.
        assert_eq!(cache.cached_or_fallback(), floor);
        assert_eq!(stub.calls(), 0);
    }

    #[tokio::test]
    async fn cached_or_fallback_returns_cached_after_warm() {
        let stub = StubSource::new(Some(vec![model("gpt-5.6-sol", "GPT-5.6 Sol")]));
        let cache = cache_with(&stub, DEFAULT_TTL, default_floor());
        cache.get_or_refresh().await;
        let models = cache.cached_or_fallback();
        assert!(models.iter().any(|m| m.slug == "gpt-5.6-sol"));
        assert!(models.iter().any(|m| m.slug == "gpt-5.4")); // floor still present
    }

    #[test]
    fn refresh_interval_returns_configured_ttl() {
        let stub = StubSource::new(None);
        let cache = cache_with(&stub, Duration::from_secs(42), default_floor());
        assert_eq!(cache.refresh_interval(), Duration::from_secs(42));
    }
}
