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

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tracing::warn;

use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexVersionCache;
use polyflare_core::AccountId;
use polyflare_store::{Account, Store, TokenCipher};

use crate::reactive_auth::ReactiveAuth;
use crate::refresh_locks::RefreshLocks;

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
    /// Context window size in tokens, when the upstream entry advertises one. Rendered into the
    /// OpenAI item's `metadata.context_window` when present — see `catalog.rs`'s `CatalogModel`.
    /// The Codex `models` array (Task 2) instead emits `raw` verbatim and does not use this field.
    pub context_window: Option<u64>,
    /// Whether this model prefers the WebSocket transport, when upstream advertises it. Same
    /// OpenAI-only rendering as `context_window` above (`metadata.prefer_websockets`).
    pub prefer_websockets: Option<bool>,
    /// The full upstream `/models` entry as received, preserved verbatim for the `/models`
    /// renderer.
    pub raw: serde_json::Value,
}

/// A successful upstream catalog fetch, including the ETag that Codex compares with the
/// `X-Models-Etag` advertised by response streams.
#[derive(Clone, Debug, PartialEq)]
pub struct FetchedCatalog {
    pub models: Vec<UpstreamModel>,
    pub etag: Option<String>,
}

/// One authoritative catalog fetched with one exact account's credentials.
#[derive(Clone, Debug, PartialEq)]
pub struct AccountCatalog {
    pub account_id: String,
    pub catalog: FetchedCatalog,
}

/// The safe catalog advertised for an exact account scope. Unlike the root cache, a scoped
/// catalog does not merge the bootstrap floor into authoritative data: every returned model must
/// be supported by every account in the scope. The floor is used only when no complete
/// authoritative scoped fetch (or stale scoped value) is available.
#[derive(Clone, Debug, PartialEq)]
pub struct ScopedCatalog {
    pub models: Vec<UpstreamModel>,
    pub etag: Option<String>,
}

/// An async source of the upstream model catalog. Abstracted so the cache's TTL / single-flight /
/// fallback logic is unit-testable without real network I/O; production uses `HttpModelSource`
/// (a later task). Mirrors `codex_version::VersionSource`'s exact mechanism (this crate's
/// `async-trait` idiom, not a hand-rolled boxed future).
#[async_trait]
pub trait ModelSource: Send + Sync {
    /// Returns the freshly-fetched upstream catalog, or `None` if the fetch failed in any way
    /// (transport error, non-2xx, parse failure, no active account, etc).
    async fn fetch(&self) -> Option<FetchedCatalog>;

    /// Fetch authoritative catalogs for whichever exact account ids in `account_ids` succeeded.
    /// Returning a subset is intentional: the cache retains those per-account facts for overlapping
    /// scopes, but it publishes an exact root/pool projection only when every requested member is
    /// fresh. The default is deliberately unavailable; production overrides this with per-account
    /// authenticated requests, while simple root-only test sources need not pretend to be
    /// account-aware.
    async fn fetch_scoped(&self, _account_ids: &[String]) -> Option<Vec<AccountCatalog>> {
        None
    }
}

struct Cached {
    models: Vec<UpstreamModel>,
    etag: Option<String>,
    fetched_at: Instant,
}

struct CachedAccount {
    catalog: FetchedCatalog,
    fetched_at: Instant,
}

/// Caches the merged (upstream-onto-floor) model catalog behind a TTL, single-flighting refreshes
/// and degrading gracefully — down to the static floor — when upstream is unavailable.
pub struct ModelCatalogCache {
    ttl: Duration,
    /// Sync-lockable value cell for zero-I/O hot-path reads (`cached_or_fallback`). Kept separate
    /// from `refresh_lock` so a sync reader never blocks on an in-flight fetch.
    cached: RwLock<Option<Cached>>,
    /// Account-scope-keyed catalogs. Keys are sorted/deduplicated account ids, which makes the
    /// cache and virtual ETag insensitive to store/query ordering.
    scoped: RwLock<HashMap<Vec<String>, Cached>>,
    /// Authoritative per-account catalogs reused across overlapping root/pool scopes. Exact-scope
    /// projections remain separate so a failed refresh can fall back only to a previously proven
    /// identity for that same scope.
    account_catalogs: RwLock<HashMap<String, CachedAccount>>,
    /// Short negative-cache window for failed account discovery. Without this, startup/root/pool
    /// overlap can immediately retry the same unavailable account once per scope.
    account_retry_after: RwLock<HashMap<String, Instant>>,
    /// Last authoritative model-slug set per account, populated by complete scoped fetches.
    account_models: RwLock<HashMap<String, HashSet<String>>>,
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
            scoped: RwLock::new(HashMap::new()),
            account_catalogs: RwLock::new(HashMap::new()),
            account_retry_after: RwLock::new(HashMap::new()),
            account_models: RwLock::new(HashMap::new()),
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
            Some(fetched) => {
                let merged = merge_onto_floor(&fetched.models, &self.floor);
                *self
                    .cached
                    .write()
                    .expect("model catalog cache lock poisoned") = Some(Cached {
                    models: merged.clone(),
                    etag: fetched.etag,
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

    /// Return the ETag belonging to the cached live catalog. A never-warmed/floor-only cache has
    /// no authoritative upstream ETag.
    pub fn cached_etag(&self) -> Option<String> {
        self.cached
            .read()
            .expect("model catalog cache lock poisoned")
            .as_ref()
            .and_then(|cached| cached.etag.clone())
    }

    /// Resolve a catalog for an exact set of accounts. A complete live fetch is intersected by
    /// slug so the result advertises only models supported by every member. The resulting ETag is
    /// a PolyFlare virtual ETag derived from the normalized scope and every member catalog; an
    /// upstream ETag is never exposed as the scoped identity.
    pub async fn get_or_refresh_scoped(&self, account_ids: &[String]) -> ScopedCatalog {
        let scope = normalize_account_ids(account_ids);
        if scope.is_empty() {
            return self.scoped_floor();
        }
        if let Some(catalog) = self.fresh_scoped(&scope) {
            return catalog;
        }

        let _guard = self.refresh_lock.lock().await;
        if let Some(catalog) = self.fresh_scoped(&scope) {
            return catalog;
        }

        let missing = self.missing_or_expired_accounts(&scope);
        let fetched = if missing.is_empty() {
            None
        } else {
            self.source.fetch_scoped(&missing).await
        };
        let mut succeeded = HashSet::new();
        if let Some(fetched) = fetched {
            let expected: HashSet<&str> = missing.iter().map(String::as_str).collect();
            let mut seen = HashSet::new();
            let fetched_at = Instant::now();
            let mut account_catalogs = self
                .account_catalogs
                .write()
                .expect("per-account model catalog cache lock poisoned");
            let mut support = self
                .account_models
                .write()
                .expect("model support cache lock poisoned");
            for account in fetched {
                if !expected.contains(account.account_id.as_str())
                    || !seen.insert(account.account_id.clone())
                    || account.catalog.models.is_empty()
                {
                    continue;
                }
                succeeded.insert(account.account_id.clone());
                support.insert(
                    account.account_id.clone(),
                    account
                        .catalog
                        .models
                        .iter()
                        .map(|model| model.slug.clone())
                        .collect(),
                );
                account_catalogs.insert(
                    account.account_id,
                    CachedAccount {
                        catalog: account.catalog,
                        fetched_at,
                    },
                );
            }
        }
        if !missing.is_empty() {
            let retry_at = Instant::now() + self.failed_refresh_retry_delay();
            let mut retry_after = self
                .account_retry_after
                .write()
                .expect("model catalog retry cache lock poisoned");
            for account_id in &missing {
                if succeeded.contains(account_id) {
                    retry_after.remove(account_id);
                } else {
                    retry_after.insert(account_id.clone(), retry_at);
                }
            }
        }

        let fresh_projection =
            self.fresh_account_catalogs(&scope)
                .and_then(|(account_catalogs, oldest_fetch)| {
                    build_scoped_catalog(&scope, account_catalogs)
                        .map(|catalog| (catalog, oldest_fetch))
                });
        if let Some((catalog, oldest_fetch)) = fresh_projection {
            self.scoped
                .write()
                .expect("model catalog scoped cache lock poisoned")
                .insert(
                    scope,
                    Cached {
                        models: catalog.models.clone(),
                        etag: catalog.etag.clone(),
                        fetched_at: oldest_fetch,
                    },
                );
            return catalog;
        }

        if let Some(stale) = self
            .scoped
            .read()
            .expect("model catalog scoped cache lock poisoned")
            .get(&scope)
        {
            warn!(
                account_count = scope.len(),
                stale_model_count = stale.models.len(),
                "scoped model catalog unavailable; using stale scoped catalog"
            );
            return ScopedCatalog {
                models: stale.models.clone(),
                etag: stale.etag.clone(),
            };
        }

        warn!(
            account_count = scope.len(),
            floor_model_count = self.floor.len(),
            "scoped model catalog unavailable and cache cold; using static floor"
        );
        self.scoped_floor()
    }

    /// Return the virtual ETag for an already-warmed exact account scope without network I/O.
    /// Pooled response and WebSocket handshakes use this to keep `X-Models-Etag` aligned with the
    /// corresponding `/{pool}/models` response. A cold scope returns `None`; callers must remove
    /// the account-native upstream ETag rather than advertise the wrong catalog identity.
    pub fn cached_scoped_etag(&self, account_ids: &[String]) -> Option<String> {
        let scope = normalize_account_ids(account_ids);
        self.scoped
            .read()
            .expect("model catalog scoped cache lock poisoned")
            .get(&scope)
            .and_then(|cached| cached.etag.clone())
    }

    /// `None` means the account catalog has not been authoritatively fetched yet and routing
    /// remains permissive; `Some(false)` is a hard entitlement exclusion.
    pub fn account_supports_model(&self, account_id: &str, model: &str) -> Option<bool> {
        self.account_models
            .read()
            .expect("model support cache lock poisoned")
            .get(account_id)
            .map(|models| models.contains(model))
    }

    fn fresh_scoped(&self, scope: &[String]) -> Option<ScopedCatalog> {
        let guard = self
            .scoped
            .read()
            .expect("model catalog scoped cache lock poisoned");
        match guard.get(scope) {
            Some(cached) if cached.fetched_at.elapsed() < self.ttl => Some(ScopedCatalog {
                models: cached.models.clone(),
                etag: cached.etag.clone(),
            }),
            _ => None,
        }
    }

    fn missing_or_expired_accounts(&self, scope: &[String]) -> Vec<String> {
        let catalogs = self
            .account_catalogs
            .read()
            .expect("per-account model catalog cache lock poisoned");
        let retry_after = self
            .account_retry_after
            .read()
            .expect("model catalog retry cache lock poisoned");
        let now = Instant::now();
        scope
            .iter()
            .filter(|account_id| {
                let needs_refresh = catalogs
                    .get(account_id.as_str())
                    .is_none_or(|cached| cached.fetched_at.elapsed() >= self.ttl);
                let retry_allowed = retry_after
                    .get(account_id.as_str())
                    .is_none_or(|retry_at| now >= *retry_at);
                needs_refresh && retry_allowed
            })
            .cloned()
            .collect()
    }

    fn failed_refresh_retry_delay(&self) -> Duration {
        self.ttl
            .min(Duration::from_secs(30))
            .max(Duration::from_millis(10))
    }

    fn fresh_account_catalogs(&self, scope: &[String]) -> Option<(Vec<AccountCatalog>, Instant)> {
        let guard = self
            .account_catalogs
            .read()
            .expect("per-account model catalog cache lock poisoned");
        let mut oldest_fetch = Instant::now();
        let mut catalogs = Vec::with_capacity(scope.len());
        for account_id in scope {
            let cached = guard.get(account_id)?;
            if cached.fetched_at.elapsed() >= self.ttl {
                return None;
            }
            oldest_fetch = oldest_fetch.min(cached.fetched_at);
            catalogs.push(AccountCatalog {
                account_id: account_id.clone(),
                catalog: cached.catalog.clone(),
            });
        }
        Some((catalogs, oldest_fetch))
    }

    fn scoped_floor(&self) -> ScopedCatalog {
        ScopedCatalog {
            models: self.floor.clone(),
            etag: None,
        }
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

fn normalize_account_ids(account_ids: &[String]) -> Vec<String> {
    let mut scope = account_ids.to_vec();
    scope.sort();
    scope.dedup();
    scope
}

/// Validate a complete exact-scope fetch, intersect its model slugs, and derive the scope's
/// deterministic virtual ETag. The first account in sorted-id order supplies the raw model entry;
/// all account catalogs still participate in the ETag so entitlement/metadata changes cannot
/// retain a stale scoped identity.
fn build_scoped_catalog(
    scope: &[String],
    mut account_catalogs: Vec<AccountCatalog>,
) -> Option<ScopedCatalog> {
    account_catalogs.sort_by(|a, b| a.account_id.cmp(&b.account_id));
    let fetched_ids: Vec<&str> = account_catalogs
        .iter()
        .map(|catalog| catalog.account_id.as_str())
        .collect();
    let expected_ids: Vec<&str> = scope.iter().map(String::as_str).collect();
    if fetched_ids != expected_ids
        || account_catalogs
            .iter()
            .any(|catalog| catalog.catalog.models.is_empty())
    {
        return None;
    }

    let mut common: HashSet<&str> = account_catalogs[0]
        .catalog
        .models
        .iter()
        .map(|model| model.slug.as_str())
        .collect();
    for account in &account_catalogs[1..] {
        let supported: HashSet<&str> = account
            .catalog
            .models
            .iter()
            .map(|model| model.slug.as_str())
            .collect();
        common.retain(|slug| supported.contains(slug));
    }

    let mut models: Vec<UpstreamModel> = account_catalogs[0]
        .catalog
        .models
        .iter()
        .filter(|model| common.contains(model.slug.as_str()))
        .cloned()
        .collect();
    models.sort_by(|a, b| a.slug.cmp(&b.slug));

    Some(ScopedCatalog {
        etag: Some(virtual_scope_etag(&account_catalogs)),
        models,
    })
}

fn virtual_scope_etag(account_catalogs: &[AccountCatalog]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"polyflare-model-scope-v1\0");
    for account in account_catalogs {
        hasher.update(account.account_id.as_bytes());
        hasher.update(b"\0");
        if let Some(etag) = &account.catalog.etag {
            hasher.update(etag.as_bytes());
        }
        hasher.update(b"\0");
        let mut models = account.catalog.models.iter().collect::<Vec<_>>();
        models.sort_by(|a, b| a.slug.cmp(&b.slug));
        for model in models {
            hasher.update(model.slug.as_bytes());
            hasher.update(b"\0");
            if let Ok(raw) = serde_json::to_vec(&model.raw) {
                hasher.update(raw);
            }
            hasher.update(b"\0");
        }
    }
    format!("\"polyflare-{}\"", hex::encode(hasher.finalize()))
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
        raw: entry.clone(),
    })
}

/// The production [`ModelSource`]. The root fetch preserves its original behavior by using one
/// active Codex account; scoped fetches use every exact account id supplied by the pool handler.
/// Successful member results survive a partial refresh, but an exact scope still fails closed
/// unless all member catalogs are fresh. Each request follows `usage_refresh.rs`'s account-bearer
/// pattern: `decrypt_tokens`, `Authorization: Bearer` + `chatgpt-account-id`, with one synchronized
/// same-account refresh and retry after an upstream 401.
///
/// Content-safety: the decrypted access token is used ONLY as the `Authorization` header value —
/// it is never passed to `tracing::warn!`/`info!`/etc anywhere in this struct (see the
/// `fetch_never_logs_the_access_token` structural test below).
pub struct HttpModelSource {
    client: reqwest::Client,
    store: Store,
    cipher: TokenCipher,
    reactive_auth: ReactiveAuth,
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
        oauth: OAuthClient,
        refresh_locks: RefreshLocks,
    ) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()?;
        let reactive_auth = ReactiveAuth::new(
            store.clone(),
            cipher.clone(),
            oauth,
            refresh_locks,
            base_url.clone(),
        );
        Ok(Self {
            client,
            store,
            cipher,
            reactive_auth,
            base_url,
            version_cache,
        })
    }

    async fn send_catalog_request(
        &self,
        access_token: &str,
        chatgpt_account_id: Option<&str>,
        is_fedramp: bool,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let version = self.version_cache.cached_or_fallback();
        let url = models_url(&self.base_url, &version);
        let mut request = self
            .client
            .get(url)
            .header("Authorization", format!("Bearer {access_token}"))
            .header("Accept", "application/json");
        if let Some(chatgpt_account_id) = chatgpt_account_id {
            request = request.header("chatgpt-account-id", chatgpt_account_id);
        }
        if is_fedramp {
            request = request.header("x-openai-fedramp", "true");
        }
        request.send().await
    }

    async fn parse_catalog_response(
        &self,
        account_id: &str,
        response: reqwest::Response,
    ) -> Option<FetchedCatalog> {
        if !response.status().is_success() {
            warn!(
                account_id,
                status = %response.status(),
                "model catalog fetch: upstream non-2xx"
            );
            return None;
        }
        let etag = response
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let body: serde_json::Value = match response.json().await {
            Ok(body) => body,
            Err(error) => {
                warn!(
                    account_id,
                    error = %error,
                    "model catalog fetch: invalid JSON body from upstream"
                );
                return None;
            }
        };

        let models = parse_models(&body);
        if models.is_empty() {
            warn!(
                account_id,
                "model catalog fetch: parsed zero models from upstream; treating as unavailable"
            );
            return None;
        }
        Some(FetchedCatalog { models, etag })
    }

    async fn fetch_account(&self, account: &Account) -> Option<FetchedCatalog> {
        let tokens = match self
            .store
            .accounts()
            .decrypt_tokens(&account.id, &self.cipher)
            .await
        {
            Ok(Some(tokens)) => tokens,
            Ok(None) => return None,
            Err(error) => {
                warn!(
                    account_id = %account.id,
                    error = %error,
                    "model catalog fetch: could not decrypt account tokens"
                );
                return None;
            }
        };

        let response = match self
            .send_catalog_request(
                &tokens.access_token,
                account.chatgpt_account_id.as_deref(),
                polyflare_codex::oauth::is_fedramp_account(&tokens.id_token),
            )
            .await
        {
            Ok(response) => response,
            Err(error) => {
                warn!(
                    account_id = %account.id,
                    error = %error,
                    "model catalog fetch: upstream transport error"
                );
                return None;
            }
        };

        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            drop(response);
            let picked = AccountId::from(account.id.as_str());
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_secs() as i64)
                .unwrap_or(0);
            match self
                .reactive_auth
                .refresh_after_unauthorized(&picked, &tokens.access_token, now)
                .await
            {
                Ok(Some(refreshed)) => {
                    let retry = match self
                        .send_catalog_request(
                            &refreshed.bearer_token,
                            refreshed.chatgpt_account_id.as_deref(),
                            refreshed.is_fedramp,
                        )
                        .await
                    {
                        Ok(response) => response,
                        Err(error) => {
                            warn!(
                                account_id = %account.id,
                                error = %error,
                                "model catalog fetch: retry transport error"
                            );
                            return None;
                        }
                    };
                    return self.parse_catalog_response(&account.id, retry).await;
                }
                Ok(None) | Err(_) => return None,
            }
        }

        self.parse_catalog_response(&account.id, response).await
    }
}

#[async_trait]
impl ModelSource for HttpModelSource {
    /// Fetch the live catalog via one active codex account. Returns `None` — never panics, never
    /// logs the token — on: no active codex account, undecryptable/missing tokens, transport
    /// error, non-2xx, an invalid JSON body, or a parsed-but-empty model list (an empty parse is
    /// treated as "no usable data", not "here is an empty catalog", so [`ModelCatalogCache`] keeps
    /// serving its stale value or the static floor instead of collapsing to nothing).
    async fn fetch(&self) -> Option<FetchedCatalog> {
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
        self.fetch_account(account).await
    }

    async fn fetch_scoped(&self, account_ids: &[String]) -> Option<Vec<AccountCatalog>> {
        let scope = normalize_account_ids(account_ids);
        if scope.is_empty() {
            return None;
        }

        let accounts = match self.store.accounts().list().await {
            Ok(accounts) => accounts,
            Err(error) => {
                warn!(error = %error, "scoped model catalog fetch: could not list accounts");
                return None;
            }
        };
        let by_id: HashMap<&str, &Account> = accounts
            .iter()
            .filter(|account| account.status == "active" && account.provider == "codex")
            .map(|account| (account.id.as_str(), account))
            .collect();
        let exact_accounts: Vec<&Account> = scope
            .iter()
            .map(|account_id| by_id.get(account_id.as_str()).copied())
            .collect::<Option<_>>()?;

        let fetched =
            futures_util::future::join_all(exact_accounts.into_iter().map(|account| async move {
                self.fetch_account(account)
                    .await
                    .map(|catalog| AccountCatalog {
                        account_id: account.id.clone(),
                        catalog,
                    })
            }))
            .await;
        Some(fetched.into_iter().flatten().collect())
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
    async fn fetch(&self) -> Option<FetchedCatalog> {
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
            raw: serde_json::json!({"slug": slug, "display_name": name}),
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
            raw: serde_json::json!({"slug": "gpt-5.5", "display_name": "GPT-5.5"}),
        }];
        let upstream = vec![UpstreamModel {
            slug: "gpt-5.5".to_string(),
            display_name: "GPT-5.5 (enriched)".to_string(),
            context_window: Some(128_000),
            prefer_websockets: Some(true),
            raw: serde_json::json!({"slug": "gpt-5.5", "display_name": "GPT-5.5 (enriched)"}),
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
        async fn fetch(&self) -> Option<FetchedCatalog> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            self.value
                .lock()
                .unwrap()
                .clone()
                .map(|models| FetchedCatalog { models, etag: None })
        }
    }

    /// Boxable wrapper so a shared `Arc<StubSource>` (kept by the test to assert on) can also be
    /// the cache's owned `Box<dyn ModelSource>`.
    struct SharedSource(Arc<StubSource>);
    #[async_trait]
    impl ModelSource for SharedSource {
        async fn fetch(&self) -> Option<FetchedCatalog> {
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

    #[tokio::test]
    async fn cached_etag_is_absent_for_floor_and_retained_for_live_catalog() {
        struct EtagSource;
        #[async_trait]
        impl ModelSource for EtagSource {
            async fn fetch(&self) -> Option<FetchedCatalog> {
                Some(FetchedCatalog {
                    models: vec![model("gpt-5.6-sol", "GPT-5.6 Sol")],
                    etag: Some("\"models-v2\"".to_string()),
                })
            }
        }

        let cache = ModelCatalogCache::new(Box::new(EtagSource), DEFAULT_TTL, default_floor());
        assert_eq!(cache.cached_etag(), None);
        cache.get_or_refresh().await;
        assert_eq!(cache.cached_etag().as_deref(), Some("\"models-v2\""));
    }

    #[test]
    fn refresh_interval_returns_configured_ttl() {
        let stub = StubSource::new(None);
        let cache = cache_with(&stub, Duration::from_secs(42), default_floor());
        assert_eq!(cache.refresh_interval(), Duration::from_secs(42));
    }

    #[tokio::test]
    async fn overlapping_scopes_fetch_each_account_only_once_within_ttl() {
        struct ScopedCountingSource {
            calls: std::sync::Mutex<Vec<Vec<String>>>,
        }

        #[async_trait]
        impl ModelSource for ScopedCountingSource {
            async fn fetch(&self) -> Option<FetchedCatalog> {
                None
            }

            async fn fetch_scoped(&self, account_ids: &[String]) -> Option<Vec<AccountCatalog>> {
                self.calls.lock().unwrap().push(account_ids.to_vec());
                Some(
                    account_ids
                        .iter()
                        .map(|account_id| AccountCatalog {
                            account_id: account_id.clone(),
                            catalog: FetchedCatalog {
                                models: vec![model("gpt-common", "GPT Common")],
                                etag: Some(format!("\"{account_id}\"")),
                            },
                        })
                        .collect(),
                )
            }
        }

        let source = Arc::new(ScopedCountingSource {
            calls: std::sync::Mutex::new(Vec::new()),
        });
        struct SharedScopedSource(Arc<ScopedCountingSource>);
        #[async_trait]
        impl ModelSource for SharedScopedSource {
            async fn fetch(&self) -> Option<FetchedCatalog> {
                self.0.fetch().await
            }

            async fn fetch_scoped(&self, account_ids: &[String]) -> Option<Vec<AccountCatalog>> {
                self.0.fetch_scoped(account_ids).await
            }
        }

        let cache = ModelCatalogCache::new(
            Box::new(SharedScopedSource(source.clone())),
            Duration::from_millis(100),
            default_floor(),
        );
        let root = vec!["acct-a".to_string(), "acct-b".to_string()];
        let pool = vec!["acct-a".to_string()];

        assert!(cache.get_or_refresh_scoped(&root).await.etag.is_some());
        assert!(cache.get_or_refresh_scoped(&pool).await.etag.is_some());

        assert_eq!(
            *source.calls.lock().unwrap(),
            vec![root.clone()],
            "a pool wholly contained in a freshly warmed root scope must reuse the per-account \
             catalogs instead of issuing another authenticated upstream request"
        );

        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(cache.get_or_refresh_scoped(&pool).await.etag.is_some());
        assert!(cache.get_or_refresh_scoped(&root).await.etag.is_some());
        assert_eq!(
            *source.calls.lock().unwrap(),
            vec![root, pool, vec!["acct-b".to_string()],],
            "after TTL expiry, refreshing a narrow pool must not extend the other member's \
             freshness; the following root refresh fetches only the still-expired account"
        );
    }

    #[tokio::test]
    async fn successful_members_survive_a_partial_scoped_refresh() {
        struct PartiallyAvailableSource {
            calls: std::sync::Mutex<Vec<Vec<String>>>,
        }

        #[async_trait]
        impl ModelSource for PartiallyAvailableSource {
            async fn fetch(&self) -> Option<FetchedCatalog> {
                None
            }

            async fn fetch_scoped(&self, account_ids: &[String]) -> Option<Vec<AccountCatalog>> {
                self.calls.lock().unwrap().push(account_ids.to_vec());
                Some(
                    account_ids
                        .iter()
                        .filter(|account_id| account_id.as_str() != "acct-b")
                        .map(|account_id| AccountCatalog {
                            account_id: account_id.clone(),
                            catalog: FetchedCatalog {
                                models: vec![model("gpt-common", "GPT Common")],
                                etag: Some(format!("\"{account_id}\"")),
                            },
                        })
                        .collect(),
                )
            }
        }

        let source = Arc::new(PartiallyAvailableSource {
            calls: std::sync::Mutex::new(Vec::new()),
        });
        struct SharedPartialSource(Arc<PartiallyAvailableSource>);
        #[async_trait]
        impl ModelSource for SharedPartialSource {
            async fn fetch(&self) -> Option<FetchedCatalog> {
                self.0.fetch().await
            }

            async fn fetch_scoped(&self, account_ids: &[String]) -> Option<Vec<AccountCatalog>> {
                self.0.fetch_scoped(account_ids).await
            }
        }

        let cache = ModelCatalogCache::new(
            Box::new(SharedPartialSource(source.clone())),
            Duration::from_secs(60),
            default_floor(),
        );
        let root = vec!["acct-a".to_string(), "acct-b".to_string()];

        let unavailable_root = cache.get_or_refresh_scoped(&root).await;
        assert_eq!(
            unavailable_root.etag, None,
            "an incomplete root must not be published as authoritative"
        );
        assert_eq!(
            cache.account_supports_model("acct-a", "gpt-common"),
            Some(true),
            "the successfully fetched member remains authoritative even when a peer failed"
        );

        let account_a = cache.get_or_refresh_scoped(&["acct-a".to_string()]).await;
        assert!(account_a.etag.is_some());
        let still_unavailable_root = cache.get_or_refresh_scoped(&root).await;
        assert_eq!(still_unavailable_root.etag, None);
        assert_eq!(
            *source.calls.lock().unwrap(),
            vec![root],
            "overlapping scopes must reuse the successful member and briefly suppress an immediate \
             retry storm against the unavailable member"
        );
    }

    #[tokio::test]
    async fn failed_refresh_keeps_only_the_same_exact_scopes_stale_projection() {
        struct MutableScopedSource {
            catalogs: std::sync::Mutex<HashMap<String, FetchedCatalog>>,
        }

        #[async_trait]
        impl ModelSource for MutableScopedSource {
            async fn fetch(&self) -> Option<FetchedCatalog> {
                None
            }

            async fn fetch_scoped(&self, account_ids: &[String]) -> Option<Vec<AccountCatalog>> {
                let catalogs = self.catalogs.lock().unwrap();
                account_ids
                    .iter()
                    .map(|account_id| {
                        catalogs
                            .get(account_id)
                            .cloned()
                            .map(|catalog| AccountCatalog {
                                account_id: account_id.clone(),
                                catalog,
                            })
                    })
                    .collect()
            }
        }

        fn fetched_for_test(etag: &str, slug: &str) -> FetchedCatalog {
            FetchedCatalog {
                models: vec![model(slug, slug)],
                etag: Some(etag.to_string()),
            }
        }

        let source = Arc::new(MutableScopedSource {
            catalogs: std::sync::Mutex::new(HashMap::from([
                (
                    "acct-a".to_string(),
                    fetched_for_test("\"a-old\"", "gpt-common"),
                ),
                (
                    "acct-b".to_string(),
                    fetched_for_test("\"b-old\"", "gpt-common"),
                ),
            ])),
        });
        struct SharedMutableSource(Arc<MutableScopedSource>);
        #[async_trait]
        impl ModelSource for SharedMutableSource {
            async fn fetch(&self) -> Option<FetchedCatalog> {
                self.0.fetch().await
            }

            async fn fetch_scoped(&self, account_ids: &[String]) -> Option<Vec<AccountCatalog>> {
                self.0.fetch_scoped(account_ids).await
            }
        }

        let cache = ModelCatalogCache::new(
            Box::new(SharedMutableSource(source.clone())),
            Duration::from_millis(20),
            default_floor(),
        );
        let root = vec!["acct-a".to_string(), "acct-b".to_string()];
        let narrow = vec!["acct-a".to_string()];
        let original_root = cache.get_or_refresh_scoped(&root).await;
        tokio::time::sleep(Duration::from_millis(30)).await;

        *source.catalogs.lock().unwrap() = HashMap::from([(
            "acct-a".to_string(),
            fetched_for_test("\"a-new\"", "gpt-a-new"),
        )]);
        let refreshed_narrow = cache.get_or_refresh_scoped(&narrow).await;
        let stale_root = cache.get_or_refresh_scoped(&root).await;

        assert!(refreshed_narrow.etag.is_some());
        assert_ne!(refreshed_narrow.etag, original_root.etag);
        assert_eq!(
            stale_root, original_root,
            "a failed root refresh must retain the previously proven root projection rather than \
             borrowing the newly warmed narrow scope"
        );
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
    fn parse_models_preserves_raw_entry() {
        let j = serde_json::json!({"models":[{
            "slug":"gpt-5.6-sol","display_name":"Sol",
            "supported_reasoning_levels":[],"visibility":"list","supported_in_api":true,"priority":1
        }]});
        let parsed = parse_models(&j);
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].slug, "gpt-5.6-sol");
        // the FULL entry is preserved, not just the 4 convenience fields:
        assert_eq!(parsed[0].raw, j["models"][0]);
        assert_eq!(
            parsed[0].raw["supported_reasoning_levels"],
            serde_json::json!([])
        );
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

    #[tokio::test]
    async fn catalog_401_refreshes_once_and_retries_the_same_account() {
        use axum::extract::State;
        use axum::http::{HeaderMap, StatusCode};
        use axum::response::IntoResponse;
        use axum::routing::get;
        use axum::{Json, Router};
        use polyflare_testkit::MockOAuth;

        type CapturedIdentity = (String, Option<String>, Option<String>);

        async fn models(
            State(identities): State<Arc<std::sync::Mutex<Vec<CapturedIdentity>>>>,
            headers: HeaderMap,
        ) -> axum::response::Response {
            let authorization = headers
                .get(reqwest::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_string();
            let account_id = headers
                .get("chatgpt-account-id")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let fedramp = headers
                .get("x-openai-fedramp")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            identities
                .lock()
                .unwrap()
                .push((authorization.clone(), account_id, fedramp));
            if authorization == "Bearer new-access" {
                return (
                    StatusCode::OK,
                    Json(serde_json::json!({
                        "models": [{
                            "slug": "gpt-authorized",
                            "display_name": "GPT Authorized",
                            "supported_reasoning_levels": []
                        }]
                    })),
                )
                    .into_response();
            }
            StatusCode::UNAUTHORIZED.into_response()
        }

        let identities = Arc::new(std::sync::Mutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new()
            .route("/models", get(models))
            .with_state(identities.clone());
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let fedramp_id_token = concat!(
            "eyJhbGciOiJub25lIn0.",
            "eyJodHRwczovL2FwaS5vcGVuYWkuY29tL2F1dGgiOnsiY2hhdGdwdF9hY2NvdW50X2lz",
            "X2ZlZHJhbXAiOnRydWV9fQ.sig"
        );
        let oauth = MockOAuth::ok("new-access", "new-refresh", fedramp_id_token);
        let oauth_url = oauth.clone().spawn().await;
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let cipher = TokenCipher::from_key_bytes(&[47u8; 32]).unwrap();
        store
            .accounts()
            .insert(
                &Account {
                    id: "acct-a".to_string(),
                    chatgpt_account_id: Some("chatgpt-a".to_string()),
                    chatgpt_user_id: None,
                    email: "a@example.test".to_string(),
                    alias: None,
                    workspace_id: None,
                    workspace_label: None,
                    seat_type: None,
                    plan_type: "plus".to_string(),
                    routing_policy: "eligible".to_string(),
                    last_refresh: 1,
                    created_at: 1,
                    status: "active".to_string(),
                    deactivation_reason: None,
                    reset_at: None,
                    blocked_at: None,
                    security_work_authorized: false,
                    provider: "codex".to_string(),
                    pool: None,
                },
                &polyflare_store::PlainTokens {
                    access_token: "old-access".to_string(),
                    refresh_token: "old-refresh".to_string(),
                    id_token: "old-id".to_string(),
                },
                &cipher,
            )
            .await
            .unwrap();

        let source = HttpModelSource::new(
            store.clone(),
            cipher.clone(),
            upstream,
            Arc::new(CodexVersionCache::new().unwrap()),
            OAuthClient::new(oauth_url).unwrap(),
            RefreshLocks::default(),
        )
        .unwrap();
        let fetched = source
            .fetch_scoped(&["acct-a".to_string()])
            .await
            .expect("the source returns its successful account subset");

        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].catalog.models[0].slug, "gpt-authorized");
        assert_eq!(
            *identities.lock().unwrap(),
            [
                (
                    "Bearer old-access".to_string(),
                    Some("chatgpt-a".to_string()),
                    None
                ),
                (
                    "Bearer new-access".to_string(),
                    Some("chatgpt-a".to_string()),
                    Some("true".to_string())
                )
            ],
            "the 401 retry must stay on the same account and atomically adopt its refreshed \
             bearer/account-id/FedRAMP identity"
        );
        assert_eq!(oauth.hit_count(), 1);
        let (_, stored_tokens) = store
            .accounts()
            .get_with_tokens("acct-a", &cipher)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored_tokens.access_token, "new-access");
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
