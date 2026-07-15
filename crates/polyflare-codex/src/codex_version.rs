//! Runtime resolution of the latest `codex-rs` CLI release version, so PolyFlare's synthesized
//! Codex User-Agent tracks the real fleet's current release instead of a hardcoded constant that
//! silently goes stale (a version tell against a newer real codex).
//!
//! Ports codex-lb's `CodexVersionCache` (`app/core/clients/codex_version.py`): resolve the latest
//! version from the GitHub releases API, fall back to the npm registry when GitHub anonymously
//! rate-limits (GitHub's API 403s far more readily than npm's — codex-lb issue #664), cache it with
//! a TTL, and degrade through a stale cache to the hardcoded [`CODEX_CLI_VERSION`] floor when both
//! sources fail. It never `git pull`s a repo — it's an HTTP GET of the latest release *version
//! string*.
//!
//! # Where this is (and is not) used
//! Only the TRANSLATED (synthesized) egress path consumes this — a Claude request routed to the
//! Codex pool, where PolyFlare invents a codex identity. The NATIVE `/responses` forward path
//! relays a real Codex client's own User-Agent (with its own genuine version) untouched, so it
//! neither needs nor uses this. See [`crate::codex_headers::codex_user_agent`].
//!
//! # Hot-path discipline
//! [`CodexVersionCache::cached_or_fallback`] is a synchronous, zero-I/O read for the header-build
//! hot path — it returns whatever is cached (even slightly stale) or the [`CODEX_CLI_VERSION`]
//! floor, and never blocks on the network. The cache is warmed out-of-band by a background refresh
//! task (see the server's `serve`), so a request never pays the GitHub/npm fetch latency.
//!
//! # Fingerprint-drift guard
//! A codex *patch* bump is proven fingerprint-stable (re-captured 0.144.1 → 0.144.4: identical
//! header set / turn-metadata keys / UA format — only the version string moved). A *minor/major*
//! bump could change the header or turn-metadata structure this crate's synthesis mirrors, so when
//! the resolved version's `major.minor` outpaces [`CODEX_CLI_VERSION`]'s, [`get_version`] logs a
//! warning to prompt a re-capture rather than silently advancing the version ahead of the synthesis.
//!
//! [`get_version`]: CodexVersionCache::get_version

use std::sync::RwLock;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tracing::warn;

use crate::codex_headers::CODEX_CLI_VERSION;

const GITHUB_RELEASES_URL: &str = "https://api.github.com/repos/openai/codex/releases/latest";
const NPM_REGISTRY_URL: &str = "https://registry.npmjs.org/@openai/codex/latest";
const FETCH_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_TTL: Duration = Duration::from_secs(3600);

/// An async source of the latest codex release version string. Abstracted so the cache's TTL /
/// single-flight / fallback logic is unit-testable without real network I/O; production uses
/// [`HttpVersionSource`].
#[async_trait]
trait VersionSource: Send + Sync {
    /// Returns the latest version (already validated as an `X.Y.Z` triple), or `None` if every
    /// upstream source failed.
    async fn fetch(&self) -> Option<String>;
}

struct Cached {
    version: String,
    fetched_at: Instant,
}

/// Caches the latest resolved codex version behind a TTL, single-flighting refreshes and degrading
/// gracefully when upstream sources are unavailable.
pub struct CodexVersionCache {
    ttl: Duration,
    /// Sync-lockable value cell for zero-I/O hot-path reads ([`Self::cached_or_fallback`]). Kept
    /// separate from [`Self::refresh_lock`] so a sync reader never blocks on an in-flight fetch.
    cached: RwLock<Option<Cached>>,
    /// Single-flight guard: only one refresh touches the network at a time (concurrent
    /// [`Self::get_version`] callers on a cold/expired cache collapse to one upstream fetch).
    refresh_lock: tokio::sync::Mutex<()>,
    source: Box<dyn VersionSource>,
}

impl CodexVersionCache {
    /// Production cache: resolves from the GitHub releases API with an npm-registry fallback, on a
    /// 1-hour TTL. Fails only if the HTTP client can't be built.
    pub fn new() -> Result<Self, reqwest::Error> {
        Ok(Self::with_source(
            Box::new(HttpVersionSource::new()?),
            DEFAULT_TTL,
        ))
    }

    fn with_source(source: Box<dyn VersionSource>, ttl: Duration) -> Self {
        Self {
            ttl,
            cached: RwLock::new(None),
            refresh_lock: tokio::sync::Mutex::new(()),
            source,
        }
    }

    /// The refresh cadence — a background warmer should re-call [`Self::get_version`] on this
    /// interval to keep the cache fresh for the sync hot path.
    pub fn refresh_interval(&self) -> Duration {
        self.ttl
    }

    /// Resolve the latest version, refreshing from upstream when the cache is cold or older than
    /// the TTL. Fallback ladder on refresh failure: fresh cache → live fetch → stale cache →
    /// [`CODEX_CLI_VERSION`] floor. Concurrent callers single-flight through one fetch.
    pub async fn get_version(&self) -> String {
        if let Some(v) = self.fresh_cached() {
            return v;
        }

        let _guard = self.refresh_lock.lock().await;
        // Re-check under the lock: a concurrent caller may have refreshed while we waited.
        if let Some(v) = self.fresh_cached() {
            return v;
        }

        match self.source.fetch().await {
            Some(version) => {
                self.warn_if_fingerprint_drift(&version);
                *self
                    .cached
                    .write()
                    .expect("codex version cache lock poisoned") = Some(Cached {
                    version: version.clone(),
                    fetched_at: Instant::now(),
                });
                version
            }
            None => {
                // Upstream sources failed. Prefer a stale-but-real cached value over the floor.
                if let Some(stale) = self
                    .cached
                    .read()
                    .expect("codex version cache lock poisoned")
                    .as_ref()
                {
                    warn!(
                        stale_version = %stale.version,
                        "codex version sources unavailable; using stale cached version"
                    );
                    return stale.version.clone();
                }
                warn!(
                    fallback = %CODEX_CLI_VERSION,
                    "codex version sources unavailable and cache cold; using hardcoded floor"
                );
                CODEX_CLI_VERSION.to_string()
            }
        }
    }

    /// Synchronous, zero-I/O read for the header-build hot path: the cached version (even if past
    /// its TTL) or the [`CODEX_CLI_VERSION`] floor when never warmed. Never blocks on the network.
    pub fn cached_or_fallback(&self) -> String {
        self.cached
            .read()
            .expect("codex version cache lock poisoned")
            .as_ref()
            .map(|c| c.version.clone())
            .unwrap_or_else(|| CODEX_CLI_VERSION.to_string())
    }

    /// The cached version if present and within the TTL, else `None`.
    fn fresh_cached(&self) -> Option<String> {
        let guard = self
            .cached
            .read()
            .expect("codex version cache lock poisoned");
        match guard.as_ref() {
            Some(c) if c.fetched_at.elapsed() < self.ttl => Some(c.version.clone()),
            _ => None,
        }
    }

    /// Warn when the resolved version's `major.minor` differs from the capture-verified floor's —
    /// the synthesized fingerprint is only verified through [`CODEX_CLI_VERSION`], and a
    /// minor/major bump may have changed the header/turn-metadata structure (a patch bump has not).
    fn warn_if_fingerprint_drift(&self, resolved: &str) {
        if major_minor(resolved) != major_minor(CODEX_CLI_VERSION) {
            warn!(
                resolved_version = %resolved,
                capture_verified_through = %CODEX_CLI_VERSION,
                "codex-rs {resolved} differs in minor/major from the capture-verified fingerprint \
                 floor {CODEX_CLI_VERSION}; the synthesized egress fingerprint is verified only \
                 through {CODEX_CLI_VERSION} — re-capture (POLYFLARE_CAPTURE_FINGERPRINT) to \
                 confirm the header/turn-metadata structure before trusting the newer version"
            );
        }
    }
}

/// `(major, minor)` of an `X.Y.Z` string, or `None` if it isn't shaped like one.
fn major_minor(version: &str) -> Option<(&str, &str)> {
    let mut parts = version.split('.');
    match (parts.next(), parts.next()) {
        (Some(major), Some(minor)) => Some((major, minor)),
        _ => None,
    }
}

/// Validates the strict `X.Y.Z` release shape codex-lb requires (no `v` prefix, no pre-release
/// suffix) — the same `^\d+\.\d+\.\d+$` guard, so a malformed/HTML upstream body is rejected rather
/// than injected into the User-Agent.
fn is_semver_triple(value: &str) -> bool {
    let parts: Vec<&str> = value.split('.').collect();
    parts.len() == 3
        && parts
            .iter()
            .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
}

/// Extracts + validates the release version from the GitHub `releases/latest` JSON (`.name`).
fn parse_github_release_name(json: &serde_json::Value) -> Option<String> {
    let name = json.get("name")?.as_str()?;
    is_semver_triple(name).then(|| name.to_string())
}

/// Extracts + validates the version from the npm `@openai/codex/latest` JSON (`.version`).
fn parse_npm_version(json: &serde_json::Value) -> Option<String> {
    let version = json.get("version")?.as_str()?;
    is_semver_triple(version).then(|| version.to_string())
}

/// The production [`VersionSource`]: GitHub releases API, then npm registry as fallback.
struct HttpVersionSource {
    client: reqwest::Client,
    github_url: String,
    npm_url: String,
}

impl HttpVersionSource {
    fn new() -> Result<Self, reqwest::Error> {
        // The GitHub API rejects requests without a User-Agent, so set a self-identifying one. This
        // is a control-plane call to github/npm, NOT egress to a provider — no fingerprint concern,
        // so it uses reqwest's default TLS rather than the executor's pinned rustls.
        let client = reqwest::Client::builder()
            .user_agent(concat!("polyflare/", env!("CARGO_PKG_VERSION")))
            .timeout(FETCH_TIMEOUT)
            .build()?;
        Ok(Self {
            client,
            github_url: GITHUB_RELEASES_URL.to_string(),
            npm_url: NPM_REGISTRY_URL.to_string(),
        })
    }

    async fn fetch_github(&self) -> Option<String> {
        let resp = self
            .client
            .get(&self.github_url)
            .header("Accept", "application/vnd.github+json")
            .send()
            .await
            .ok()?;
        if !resp.status().is_success() {
            warn!(status = %resp.status(), "GitHub releases API non-200");
            return None;
        }
        let json: serde_json::Value = resp.json().await.ok()?;
        parse_github_release_name(&json)
    }

    async fn fetch_npm(&self) -> Option<String> {
        let resp = self
            .client
            .get(&self.npm_url)
            .header("Accept", "application/json")
            .send()
            .await
            .ok()?;
        if !resp.status().is_success() {
            warn!(status = %resp.status(), "npm registry non-200 for @openai/codex");
            return None;
        }
        let json: serde_json::Value = resp.json().await.ok()?;
        parse_npm_version(&json)
    }
}

#[async_trait]
impl VersionSource for HttpVersionSource {
    async fn fetch(&self) -> Option<String> {
        if let Some(v) = self.fetch_github().await {
            return Some(v);
        }
        self.fetch_npm().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::*;

    #[test]
    fn is_semver_triple_accepts_release_shapes_only() {
        assert!(is_semver_triple("0.144.4"));
        assert!(is_semver_triple("1.0.0"));
        assert!(is_semver_triple("10.20.300"));
        // Rejected: prefix, pre-release, wrong arity, non-digits, empty segments.
        assert!(!is_semver_triple("v0.144.4"));
        assert!(!is_semver_triple("0.144.4-beta.1"));
        assert!(!is_semver_triple("0.144"));
        assert!(!is_semver_triple("0.144.4.1"));
        assert!(!is_semver_triple("0.x.4"));
        assert!(!is_semver_triple("0..4"));
        assert!(!is_semver_triple(""));
    }

    #[test]
    fn parse_github_extracts_and_validates_name() {
        let ok = serde_json::json!({"name": "0.144.4", "tag_name": "rust-v0.144.4"});
        assert_eq!(parse_github_release_name(&ok).as_deref(), Some("0.144.4"));
        // Non-triple name (some releases name themselves "rust-v..") is rejected.
        let bad = serde_json::json!({"name": "rust-v0.144.4"});
        assert_eq!(parse_github_release_name(&bad), None);
        assert_eq!(parse_github_release_name(&serde_json::json!({})), None);
    }

    #[test]
    fn parse_npm_extracts_and_validates_version() {
        let ok = serde_json::json!({"version": "0.144.4", "name": "@openai/codex"});
        assert_eq!(parse_npm_version(&ok).as_deref(), Some("0.144.4"));
        let bad = serde_json::json!({"version": "not-a-version"});
        assert_eq!(parse_npm_version(&bad), None);
    }

    /// A [`VersionSource`] returning a scripted value and counting its calls.
    struct StubSource {
        value: std::sync::Mutex<Option<String>>,
        calls: AtomicUsize,
        delay: Duration,
    }

    impl StubSource {
        fn new(value: Option<&str>) -> Arc<Self> {
            Arc::new(Self {
                value: std::sync::Mutex::new(value.map(str::to_string)),
                calls: AtomicUsize::new(0),
                delay: Duration::ZERO,
            })
        }
        fn with_delay(value: Option<&str>, delay: Duration) -> Arc<Self> {
            Arc::new(Self {
                value: std::sync::Mutex::new(value.map(str::to_string)),
                calls: AtomicUsize::new(0),
                delay,
            })
        }
        fn set(&self, value: Option<&str>) {
            *self.value.lock().unwrap() = value.map(str::to_string);
        }
        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl VersionSource for StubSource {
        async fn fetch(&self) -> Option<String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            self.value.lock().unwrap().clone()
        }
    }

    /// Boxable wrapper so a shared `Arc<StubSource>` (kept by the test to assert on) can also be the
    /// cache's owned `Box<dyn VersionSource>`.
    struct SharedSource(Arc<StubSource>);
    #[async_trait]
    impl VersionSource for SharedSource {
        async fn fetch(&self) -> Option<String> {
            self.0.fetch().await
        }
    }

    fn cache_with(stub: &Arc<StubSource>, ttl: Duration) -> CodexVersionCache {
        CodexVersionCache::with_source(Box::new(SharedSource(stub.clone())), ttl)
    }

    #[test]
    fn cached_or_fallback_returns_floor_when_unwarmed() {
        let stub = StubSource::new(Some("9.9.9"));
        let cache = cache_with(&stub, DEFAULT_TTL);
        // No get_version() call yet → sync read must return the compiled-in floor, no network.
        assert_eq!(cache.cached_or_fallback(), CODEX_CLI_VERSION);
        assert_eq!(stub.calls(), 0);
    }

    #[tokio::test]
    async fn get_version_fetches_caches_and_serves_sync_reads() {
        let stub = StubSource::new(Some("1.2.3"));
        let cache = cache_with(&stub, DEFAULT_TTL);
        assert_eq!(cache.get_version().await, "1.2.3");
        // Now warmed: the sync hot-path read sees it too.
        assert_eq!(cache.cached_or_fallback(), "1.2.3");
        // Second call within TTL is served from cache, not a re-fetch.
        assert_eq!(cache.get_version().await, "1.2.3");
        assert_eq!(stub.calls(), 1);
    }

    #[tokio::test]
    async fn get_version_falls_back_to_floor_when_source_fails_cold() {
        let stub = StubSource::new(None);
        let cache = cache_with(&stub, DEFAULT_TTL);
        assert_eq!(cache.get_version().await, CODEX_CLI_VERSION);
    }

    #[tokio::test]
    async fn get_version_prefers_stale_cache_over_floor_when_source_fails() {
        let stub = StubSource::with_delay(Some("2.5.0"), Duration::ZERO);
        let cache = cache_with(&stub, Duration::from_millis(5));
        assert_eq!(cache.get_version().await, "2.5.0"); // warm
        stub.set(None); // sources now fail
        tokio::time::sleep(Duration::from_millis(10)).await; // expire TTL
                                                             // Cache is expired AND the source fails → serve the stale-but-real value, not the floor.
        assert_eq!(cache.get_version().await, "2.5.0");
    }

    #[tokio::test]
    async fn get_version_refetches_after_ttl_expiry() {
        let stub = StubSource::new(Some("3.0.0"));
        let cache = cache_with(&stub, Duration::from_millis(5));
        assert_eq!(cache.get_version().await, "3.0.0");
        stub.set(Some("3.0.1"));
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(cache.get_version().await, "3.0.1");
        assert_eq!(stub.calls(), 2);
    }

    #[tokio::test]
    async fn concurrent_get_version_single_flights_one_fetch() {
        // A slow fetch forces overlap: all callers find a cold cache and queue on the refresh lock;
        // only the first fetches, the rest re-check and get the fresh value.
        let stub = StubSource::with_delay(Some("4.4.4"), Duration::from_millis(30));
        let cache = Arc::new(cache_with(&stub, DEFAULT_TTL));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let c = cache.clone();
            handles.push(tokio::spawn(async move { c.get_version().await }));
        }
        for h in handles {
            assert_eq!(h.await.unwrap(), "4.4.4");
        }
        assert_eq!(
            stub.calls(),
            1,
            "concurrent callers must collapse to one fetch"
        );
    }
}
