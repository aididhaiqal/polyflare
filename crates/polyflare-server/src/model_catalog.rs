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

use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tracing::warn;

use polyflare_codex::CodexVersionCache;
use polyflare_store::{Store, TokenCipher};

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
    /// Context window size in tokens, when the upstream entry advertises one. Task 3 renders this
    /// into both `/models` response shapes (`CodexModelEntry.context_window` and the OpenAI item's
    /// `metadata.context_window`) when present — see `catalog.rs`'s `CatalogModel`.
    pub context_window: Option<u64>,
    /// Whether this model prefers the WebSocket transport, when upstream advertises it. Task 3
    /// renders this the same way as `context_window` above (`CodexModelEntry.prefer_websockets` /
    /// `metadata.prefer_websockets`).
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

/// Build the upstream `/models` URL: `{base}/models?client_version={version}`. `base` already
/// includes `/codex` (`config.rs`'s `upstream_base_url`, default `https://chatgpt.com/backend-api/
/// codex`), so only `/models?...` is appended — mirrors `usage_refresh.rs`'s pure `usage_url`
/// helper (testable without any HTTP).
pub fn models_url(base: &str, version: &str) -> String {
    format!(
        "{}/models?client_version={}",
        base.trim_end_matches('/'),
        percent_encode_query_value(version)
    )
}

/// Minimal percent-encoding for a single query VALUE (not a whole URL): unreserved characters
/// (letters, digits, `-`, `_`, `.`, `~`) pass through unchanged; everything else becomes `%XX`.
/// Codex version strings are validated `X.Y.Z` triples before ever reaching here (see
/// `CodexVersionCache::cached_or_fallback` / `is_semver_triple`), so in practice this never changes
/// the input — it exists purely as a defensive guard against a malformed/unvalidated version ever
/// injecting a stray `&`/`#`/space into the query string.
fn percent_encode_query_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for b in value.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

/// Parse the upstream `/models` response body into `UpstreamModel`s. The upstream shape is
/// `{"models": [...]}` (confirmed against codex-lb's `model_fetcher.py`'s `data["models"]`
/// parsing); each entry needs at least a non-empty string `slug` — everything else is optional
/// enrichment. A malformed entry (not a JSON object, or missing/empty/non-string `slug`) is
/// SKIPPED, never causes a panic. Missing/non-array `models`, or non-object top-level JSON, yields
/// an empty `Vec` — the caller (`HttpModelSource::fetch`) treats an empty result as "no usable
/// upstream data" and returns `None`, so the cache keeps serving the stale value or static floor
/// rather than collapsing to an empty catalog.
pub fn parse_models(json: &serde_json::Value) -> Vec<UpstreamModel> {
    json.get("models")
        .and_then(serde_json::Value::as_array)
        .map(|entries| entries.iter().filter_map(parse_one_model).collect())
        .unwrap_or_default()
}

/// Parse a single upstream model entry. `display_name` falls back to the slug when upstream omits
/// it; `context_window`/`prefer_websockets` are optional enrichment and are simply `None` when
/// upstream doesn't supply them (never an error).
fn parse_one_model(entry: &serde_json::Value) -> Option<UpstreamModel> {
    let obj = entry.as_object()?;
    let slug = obj.get("slug").and_then(serde_json::Value::as_str)?;
    if slug.is_empty() {
        return None;
    }
    let display_name = obj
        .get("display_name")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(slug)
        .to_string();
    Some(UpstreamModel {
        slug: slug.to_string(),
        display_name,
        context_window: obj
            .get("context_window")
            .and_then(serde_json::Value::as_u64),
        prefer_websockets: obj
            .get("prefer_websockets")
            .and_then(serde_json::Value::as_bool),
    })
}

/// The production [`ModelSource`]: fetches the live upstream catalog using ONE active codex
/// account's bearer token (no per-plan fan-out for v1 — mirrors `usage_refresh.rs`'s account-bearer
/// fetch pattern exactly: pick an account, `decrypt_tokens`, `Authorization: Bearer` +
/// `chatgpt-account-id` headers; see that module's `refresh_account` for the precedent).
///
/// Content-safety: the decrypted access token is used ONLY as the `Authorization` header value —
/// it is never passed to `tracing::warn!`/`info!`/etc anywhere in this struct (see the
/// `fetch_never_logs_the_access_token` structural test below).
pub struct HttpModelSource {
    client: reqwest::Client,
    store: Store,
    cipher: TokenCipher,
    /// Upstream base URL, e.g. `https://chatgpt.com/backend-api/codex` — already includes
    /// `/codex` (`config.rs`'s `upstream_base_url`), so [`models_url`] appends only `/models?...`.
    base_url: String,
    version_cache: Arc<CodexVersionCache>,
}

impl HttpModelSource {
    /// Builds its own dedicated `reqwest::Client` with a 15s timeout (mirrors
    /// `usage_refresh::spawn_usage_refresh`'s client, not shared with the executor's pinned-TLS
    /// client — this is a control-plane call, not provider egress).
    pub fn new(
        store: Store,
        cipher: TokenCipher,
        base_url: String,
        version_cache: Arc<CodexVersionCache>,
    ) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()?;
        Ok(Self {
            client,
            store,
            cipher,
            base_url,
            version_cache,
        })
    }
}

#[async_trait]
impl ModelSource for HttpModelSource {
    /// Fetch the live catalog via one active codex account. Returns `None` — never panics, never
    /// logs the token — on: no active codex account, undecryptable/missing tokens, transport
    /// error, non-2xx, an invalid JSON body, or a parsed-but-empty model list (an empty parse is
    /// treated as "no usable data", not "here is an empty catalog", so [`ModelCatalogCache`] keeps
    /// serving its stale value or the static floor instead of collapsing to nothing).
    async fn fetch(&self) -> Option<Vec<UpstreamModel>> {
        let accounts = match self.store.accounts().list().await {
            Ok(a) => a,
            Err(e) => {
                warn!(error = %e, "model catalog fetch: could not list accounts");
                return None;
            }
        };
        let account = accounts
            .iter()
            .find(|a| a.status == "active" && a.provider == "codex")?;

        let tokens = match self
            .store
            .accounts()
            .decrypt_tokens(&account.id, &self.cipher)
            .await
        {
            Ok(Some(t)) => t,
            Ok(None) => return None,
            Err(e) => {
                warn!(error = %e, "model catalog fetch: could not decrypt account tokens");
                return None;
            }
        };

        let version = self.version_cache.cached_or_fallback();
        let url = models_url(&self.base_url, &version);
        let mut req = self
            .client
            .get(url)
            .header("Authorization", format!("Bearer {}", tokens.access_token))
            .header("Accept", "application/json");
        if let Some(cid) = &account.chatgpt_account_id {
            req = req.header("chatgpt-account-id", cid);
        }

        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "model catalog fetch: upstream transport error");
                return None;
            }
        };
        if !resp.status().is_success() {
            warn!(
                status = %resp.status(),
                "model catalog fetch: upstream non-2xx"
            );
            return None;
        }
        let body: serde_json::Value = match resp.json().await {
            Ok(b) => b,
            Err(e) => {
                warn!(error = %e, "model catalog fetch: invalid JSON body from upstream");
                return None;
            }
        };

        let models = parse_models(&body);
        if models.is_empty() {
            warn!("model catalog fetch: parsed zero models from upstream; treating as unavailable");
            return None;
        }
        Some(models)
    }
}

/// A [`ModelSource`] that always reports failure. Used for the
/// `POLYFLARE_MODEL_CATALOG_ENABLED=false` production disable path (Task 3) and by test/dev
/// harnesses that need a working `AppState.model_catalog` without ever touching the network: since
/// `fetch` always returns `None`, [`ModelCatalogCache::get_or_refresh`]/`cached_or_fallback` always
/// serve the static floor (never blocks, never fetches, never empty).
struct NoneSource;

#[async_trait]
impl ModelSource for NoneSource {
    async fn fetch(&self) -> Option<Vec<UpstreamModel>> {
        None
    }
}

/// Builds a floor-only `ModelCatalogCache`: a [`NoneSource`] over `floor`, so the cache always
/// serves exactly `floor` (`cached_or_fallback`/`get_or_refresh` never fetch, never differ from
/// today's static-catalog behavior). This is both the disabled-feature production path AND the
/// shape every `AppState` test-construction site wants (mirrors how `CodexVersionCache::new()`'s
/// unwarmed static-version floor is used identically in prod and tests).
pub fn floor_only_cache(floor: Vec<UpstreamModel>) -> ModelCatalogCache {
    ModelCatalogCache::new(Box::new(NoneSource), Duration::from_secs(3600), floor)
}

/// Convenience wrapper combining [`floor_only_cache`] with `catalog::codex_bootstrap_floor()` (the
/// current static bootstrap slugs) — the exact `Arc<ModelCatalogCache>` value every `AppState`
/// test/dev construction site wants for its `model_catalog` field when it doesn't care about a
/// live upstream fetch. The floor is NEVER empty (`codex_bootstrap_floor` always yields the 5
/// static slugs; see that function's own non-empty assertion/test), so this can never regress
/// `/models` to an empty catalog.
pub fn floor_only_model_catalog() -> Arc<ModelCatalogCache> {
    Arc::new(floor_only_cache(crate::catalog::codex_bootstrap_floor()))
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

    // --- Task 2: models_url (pure) ---

    #[test]
    fn models_url_builds_correct_query() {
        assert_eq!(
            models_url("https://chatgpt.com/backend-api/codex", "0.144.4"),
            "https://chatgpt.com/backend-api/codex/models?client_version=0.144.4"
        );
    }

    #[test]
    fn models_url_trims_trailing_slash_on_base() {
        assert_eq!(
            models_url("https://chatgpt.com/backend-api/codex/", "0.144.4"),
            "https://chatgpt.com/backend-api/codex/models?client_version=0.144.4"
        );
    }

    #[test]
    fn models_url_percent_encodes_a_non_triple_version_defensively() {
        // Versions are validated `X.Y.Z` before reaching here, but the URL builder itself must not
        // inject a raw space/`&` into the query string if it ever received something unvalidated.
        assert_eq!(
            models_url("https://example.test/codex", "weird version&x=1"),
            "https://example.test/codex/models?client_version=weird%20version%26x%3D1"
        );
    }

    // --- Task 2: parse_models (pure) ---

    /// A realistic upstream fixture mirroring codex-lb's `data["models"]` shape
    /// (`model_fetcher.py`): one fully-populated entry, one with only the required `slug`, and
    /// three malformed entries (missing slug, non-object, empty slug) that must all be skipped
    /// without panicking.
    fn sample_models_json() -> serde_json::Value {
        serde_json::json!({
            "models": [
                {
                    "slug": "gpt-5.6-sol",
                    "display_name": "GPT-5.6 Sol",
                    "context_window": 400_000,
                    "prefer_websockets": true
                },
                {
                    "slug": "gpt-5.7-nova"
                },
                {
                    "display_name": "No Slug Here"
                },
                "not-an-object",
                {
                    "slug": "",
                    "display_name": "Empty Slug"
                }
            ]
        })
    }

    #[test]
    fn parse_models_parses_realistic_fixture_and_skips_malformed_entries() {
        let models = parse_models(&sample_models_json());
        assert_eq!(
            models.len(),
            2,
            "the 3 malformed entries (no slug, non-object, empty slug) are skipped without panic"
        );

        let sol = models
            .iter()
            .find(|m| m.slug == "gpt-5.6-sol")
            .expect("fully-populated entry parses");
        assert_eq!(sol.display_name, "GPT-5.6 Sol");
        assert_eq!(sol.context_window, Some(400_000));
        assert_eq!(sol.prefer_websockets, Some(true));

        let nova = models
            .iter()
            .find(|m| m.slug == "gpt-5.7-nova")
            .expect("slug-only entry tolerates missing optional fields");
        assert_eq!(
            nova.display_name, "gpt-5.7-nova",
            "missing display_name falls back to the slug"
        );
        assert_eq!(nova.context_window, None);
        assert_eq!(nova.prefer_websockets, None);
    }

    #[test]
    fn parse_models_missing_models_key_returns_empty() {
        assert_eq!(
            parse_models(&serde_json::json!({"other": []})),
            Vec::<UpstreamModel>::new()
        );
    }

    #[test]
    fn parse_models_garbage_or_wrong_shaped_json_returns_empty() {
        assert_eq!(
            parse_models(&serde_json::json!("just a string")),
            Vec::<UpstreamModel>::new()
        );
        assert_eq!(
            parse_models(&serde_json::json!(null)),
            Vec::<UpstreamModel>::new()
        );
        assert_eq!(
            parse_models(&serde_json::json!({"models": "not-an-array"})),
            Vec::<UpstreamModel>::new()
        );
        assert_eq!(
            parse_models(&serde_json::json!({"models": []})),
            Vec::<UpstreamModel>::new()
        );
    }

    // --- Task 2: content-safety — fetch() must never log the token ---

    /// Structural guard (no real HTTP / no real Store needed): scans this file's own source for
    /// every tracing log-macro call site and asserts none of them interpolate the decrypted access
    /// token. `HttpModelSource::fetch`'s only use of `tokens.access_token` must remain the
    /// `Authorization` header value built two lines above its one `warn!`-adjacent branch — this
    /// test fails loudly if a future edit ever logs the token instead.
    #[test]
    fn fetch_never_logs_the_access_token() {
        let src = include_str!("model_catalog.rs");
        for (i, line) in src.lines().enumerate() {
            let is_log_line = ["warn!(", "info!(", "error!(", "debug!(", "trace!("]
                .iter()
                .any(|m| line.contains(m));
            if !is_log_line {
                continue;
            }
            let lower = line.to_lowercase();
            assert!(
                !lower.contains("access_token") && !lower.contains("bearer"),
                "line {} logs sensitive token material: {}",
                i + 1,
                line
            );
        }
    }
}
