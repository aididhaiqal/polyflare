# D15 — Live Upstream Model-Catalog Fetch/Merge (single-node) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use `- [ ]`.

**Goal:** Keep `/models` fresh instead of a compiled-in static list: periodically fetch the upstream Codex model
catalog (`GET {upstream}/models?client_version=`, using an active account's bearer), merge it onto the static
bootstrap floor (floor never removed), cache it with a TTL + single-flight, and serve the merged catalog — falling
back to the static floor on any failure or when there are no accounts. Single-node in-memory cache only (skip
codex-lb's leader-election + DB-snapshot machinery — D15 itself scopes to "single-node refresh loop first").

**Architecture:** A `ModelCatalogCache` mirroring the proven `CodexVersionCache` (`polyflare-codex/src/codex_version.rs`)
exactly — `RwLock<Option<Cached>>` + TTL + `tokio::sync::Mutex` single-flight + a `cached_or_fallback()` sync hot-path
read + a fallback ladder (fresh → live fetch → stale → static floor). Fetch is behind a `ModelSource` trait (the
repo's established mock idiom — no mocking crate) so CI never hits real upstream. A background-warm `tokio::spawn`
loop (like `codex_version` / `usage_refresh`) refreshes it. `catalog.rs::build_catalog` reads the cache instead of
only the static `codex_bootstrap()`. Disabled/no-accounts/fetch-failure ⇒ today's static behavior, never a broken/
empty `/models`.

**Authority — the D15 scoping study + codex-lb ground truth (this session), file:line cites:**
- Requirement `docs/PORTING-CODEXLB.md:265-270` (D15, LOW/medium): port the fetcher
  (`GET {base}/codex/models?client_version=`, Bearer + chatgpt-account-id) + a periodic refresh that merges the
  upstream catalog onto the golden/static bootstrap floor. "Single-node refresh loop first." codex-lb ref
  `core/clients/model_fetcher.py`, `core/openai/model_refresh_scheduler.py`.
- codex-lb mechanism (verified): `fetch_models_for_plan` (`model_fetcher.py:104-190`) — `GET {base}/codex/models?
  client_version={version}`, `Authorization: Bearer {access_token}` + `chatgpt-account-id`, 15s timeout, parses
  `data["models"]` → `UpstreamModel` (`model_registry.py:27-47`); non-2xx/timeout ⇒ `ModelFetchError` (caught).
  Bootstrap floor `_BOOTSTRAP_STATIC_MODELS` (`model_registry.py:265+`) ALWAYS present; on all-fetch-failed, does NOT
  clear the registry (keeps last-good/floor); zero-accounts ⇒ floor only. The scheduler's leader-election +
  DB-persisted snapshot + invalidation bus are MULTI-NODE machinery PolyFlare does NOT need (single-node).
- PolyFlare `/models` today = STATIC (`crates/polyflare-server/src/catalog.rs`, whole file): `codex_bootstrap()`
  (`catalog.rs:36-53`) = hardcoded `const SLUGS: &[(&str,&str)]` of 5 entries (gpt-5.6-sol/terra/luna, gpt-5.5,
  gpt-5.4; id+display_name only). `build_catalog()` (`catalog.rs:57-83`) merges static + `crate::alias::synthetic_models()`
  (real-upstream-wins on id collision). Routes `/models`, `/backend-api/codex/models` → `codex_models_handler`
  (`catalog.rs:165`), `/v1/models` → `v1_models_handler` (`catalog.rs:171`), ungated (`app.rs:387-392`). NO
  registry/cache/fetch exists. The file's own comment (`catalog.rs:12`) anticipates D15 as the unbuilt follow-up.
  Response shapes: OpenAI `{object:"list",data:[...]}` + Codex `{object:"list",models:[...],data:[...]}`.
- Pattern to MIRROR — `CodexVersionCache` (`polyflare-codex/src/codex_version.rs`): `trait VersionSource { async fn
  fetch(&self) -> Option<String> }` (mock via `StubSource`, no mocking crate); `struct { ttl, cached: RwLock<Option<Cached>>,
  refresh_lock: tokio::sync::Mutex<()>, source: Box<dyn VersionSource> }`; `get_version()` (`:102-146`, TTL + single-
  flight re-check-under-lock + ladder fresh→fetch→stale→floor); `cached_or_fallback()` (`:150-157`, sync zero-I/O);
  `refresh_interval()`; held `AppState.codex_version: Arc<CodexVersionCache>` (`app.rs:69`), background-warmed
  `main.rs:190-199` (`tokio::spawn { loop { cache.get_version().await; sleep(refresh_interval).await } }`), read hot
  at `ingress.rs:2223` via `cached_or_fallback()`.
- Account-bearer fetch pattern — `usage_refresh.rs:279-347` (`spawn_usage_refresh`): builds a `reqwest::Client`,
  `state.store.accounts().list()`, filters `provider=="codex"`, `repo.decrypt_tokens` per account → `Authorization:
  Bearer {access_token}` + `chatgpt-account-id` (`:168-173`). D15 picks ANY single active codex account (no plan
  fan-out for v1). NO token ever logged (usage_refresh precedent).
- Upstream URL: `upstream_base_url` default `https://chatgpt.com/backend-api/codex` (`config.rs:13`,
  `POLYFLARE_UPSTREAM_URL`) → D15 URL `GET {upstream_base_url}/models?client_version={version}` (base already
  includes `/codex`). `client_version` from `state.codex_version.cached_or_fallback()`. Config idiom
  `POLYFLARE_*_from_env()` (`config.rs:487-514` etc., fail-safe malformed, clamp).

## Global Constraints

- **Never serve a broken/empty `/models` (inviolable).** The bootstrap static floor (`codex_bootstrap()` + synthetic
  aliases) is ALWAYS present and NEVER removed by the merge. Any of {feature disabled, no active accounts, fetch
  error, parse error, empty upstream result} ⇒ serve exactly today's static-catalog behavior. A test proves each
  failure mode still yields the full static list.
- **Merge = upstream ONTO the floor, floor wins nothing / upstream wins on slug collision, floor entries never
  dropped.** Dedup by slug/id. Upstream adds new slugs + enriches known ones; the static floor slugs always remain
  even if upstream omits them (matches codex-lb's always-present bootstrap). Preserve `build_catalog`'s existing
  real-wins-over-synthetic-alias behavior.
- **TTL + single-flight (inviolable, mirror CodexVersionCache).** Never hammer upstream per request. Hot-path read
  is the sync `cached_or_fallback()` (zero I/O, returns stale-or-floor, never blocks a `/models` request). Refresh
  happens in the background-warm loop + lazily under the single-flight lock.
- **Fetch is trait-behind (`ModelSource`) — CI never hits real upstream.** Unit tests use a stub source (mirror
  `StubSource`). No mocking crate. The URL/header builders are pure testable fns (like `usage_refresh`'s `usage_url`).
- **Content-safety:** the bearer token used to fetch is NEVER logged (usage_refresh precedent). The model catalog
  itself is non-sensitive metadata. The fetch account is picked internally; no account content is surfaced.
- **Disable lever + default.** `POLYFLARE_MODEL_CATALOG_ENABLED` (default... decide: ON to actually deliver the
  feature, OR OFF to be conservative — RECOMMEND default ON since the fallback is airtight; but a disabled path =
  today's static-only behavior = clean rollback). `POLYFLARE_MODEL_CATALOG_TTL_SECS` (default 3600, clamp sane).
- **Additive, no routing impact.** `/models` is advertised metadata, NOT consulted by the executor for routing — so
  this touches no selection/failover/wedge path. The 5 wedge/cyber/failover/starvation suites MUST stay green.
- Verify each task: `cargo test --workspace` green + `cargo clippy --workspace --all-targets -- -D warnings` +
  `cargo fmt --all -- --check` clean (run `cargo fmt` before committing).

---

### Task 1: `UpstreamModel` + `ModelSource` trait + `ModelCatalogCache` (mirror CodexVersionCache)

**Files:** a new `crates/polyflare-server/src/model_catalog.rs` (or extend `catalog.rs` — decide; a new module is
cleaner given catalog.rs is the render layer); tests in that file.

**Read first:** `codex_version.rs` FULLY (the exact cache shape to copy) + `catalog.rs`'s `codex_bootstrap()` +
`ModelEntry`/whatever struct it renders from (so `UpstreamModel` maps cleanly into the render layer).

**Interfaces — Produces:**
```rust
/// One model from the upstream catalog. Superset of catalog.rs's static entry; extra fields optional.
#[derive(Clone, Debug, PartialEq)]
pub struct UpstreamModel {
    pub slug: String,
    pub display_name: String,
    // optional enrichment (None when the static floor supplies the entry):
    pub context_window: Option<u64>,
    pub prefer_websockets: Option<bool>,
    // ... add only the fields catalog.rs's render can actually USE today; keep minimal (YAGNI — do NOT port
    // codex-lb's full 20-field UpstreamModel unless catalog.rs renders it). Confirm what the /models response
    // includes and carry just those + slug/display_name.
}
#[async_trait::async_trait] // or a boxed-future trait like VersionSource uses — mirror codex_version.rs EXACTLY
pub trait ModelSource: Send + Sync {
    async fn fetch(&self) -> Option<Vec<UpstreamModel>>;   // None on any failure (mirrors VersionSource::fetch -> Option)
}
pub struct ModelCatalogCache { /* ttl, cached: RwLock<Option<Cached>>, refresh_lock: Mutex<()>, source: Box<dyn ModelSource>, floor: Vec<UpstreamModel> */ }
impl ModelCatalogCache {
    pub fn new(source: Box<dyn ModelSource>, ttl: Duration, floor: Vec<UpstreamModel>) -> Self;
    pub async fn get_or_refresh(&self) -> Vec<UpstreamModel>;   // TTL + single-flight + ladder fresh→fetch(merge onto floor)→stale→floor
    pub fn cached_or_fallback(&self) -> Vec<UpstreamModel>;     // sync zero-I/O: cached (even stale) merged, else floor
    pub fn refresh_interval(&self) -> Duration;
}
```
- The merge (a pure fn `merge_onto_floor(upstream, floor) -> Vec<UpstreamModel>`): start from `floor`, then for each
  upstream model upsert by slug (upstream wins on collision, adds new slugs); floor slugs never removed. Unit-test it.
- `get_or_refresh`: TTL check → if fresh return cached; else single-flight (`refresh_lock`, re-check under lock) →
  `source.fetch().await`; `Some(models)` ⇒ store `merge_onto_floor(models, floor)` fresh + return; `None` ⇒ return
  stale cache if any, else `floor`. NEVER return empty. Mirror `get_version` structurally.
- `cached_or_fallback`: sync read of the cache (stale ok) or `floor`.

- [ ] **Step 1:** Failing tests (mirror codex_version.rs's tests with a StubSource): (a) fresh fetch merges upstream
      onto floor (a floor-only slug survives, an upstream-only slug appears, a collision takes upstream); (b) TTL
      expiry triggers re-fetch; (c) single-flight — concurrent `get_or_refresh` calls the source once (use an
      atomic counter StubSource); (d) source returns None ⇒ falls back to stale, then to floor (never empty); (e)
      `cached_or_fallback` sync returns floor before any fetch, cached after; (f) `merge_onto_floor` pure-fn cases.
- [ ] **Step 2:** Run — fail. **Step 3:** Implement mirroring CodexVersionCache. **Step 4:** Green.
- [ ] **Step 5:** Commit: `feat(server): ModelCatalogCache (TTL + single-flight + static-floor fallback, mirrors CodexVersionCache)`

---

### Task 2: `HttpModelSource` (account-bearer upstream fetch) + parse

**Files:** `crates/polyflare-server/src/model_catalog.rs` (the `HttpModelSource` impl + pure URL/header/parse fns);
tests.

**Read first:** `usage_refresh.rs`'s account-list + decrypt_tokens + bearer/account-id header building (`:168-173`)
and its pure `usage_url`-style helper; `config.rs` `upstream_base_url`; how `state.codex_version.cached_or_fallback()`
gives the client_version.

**Interfaces — Produces:** `HttpModelSource { client, store, base_url, version_cache }` implementing `ModelSource`:
- `fetch()`: pick ONE active codex account (`store.accounts().list()` → filter `status=="active" && provider=="codex"`
  → first; NONE ⇒ return `None` [⇒ floor]). `decrypt_tokens` → bearer + `chatgpt-account-id`. `GET {base_url}/models?
  client_version={version}` (version from the version cache), 15s timeout. Non-2xx/timeout/transport/parse-fail ⇒
  `None` (logged content-free: a warn with status/table, NEVER the token). Parse `data["models"]` (or the real key —
  confirm from codex-lb's shape / a captured response) into `Vec<UpstreamModel>`.
- Pure testable helpers: `models_url(base, version) -> String` and `parse_models(json: &Value) -> Vec<UpstreamModel>`
  (unit-test these WITHOUT real HTTP — mirror `usage_refresh`'s pure-helper tests). The `fetch()` HTTP path itself is
  covered structurally + by the Task 1 StubSource (real upstream can't run in CI).

- [ ] **Step 1:** Failing tests: (a) `models_url` builds `{base}/models?client_version={v}` correctly; (b)
      `parse_models` parses a sample upstream JSON (mirror codex-lb's `data["models"]` shape — build a realistic
      fixture) into the right `UpstreamModel`s, and tolerates missing optional fields / a malformed entry (skips it,
      doesn't panic); (c) an empty/garbage JSON ⇒ empty Vec (⇒ caller treats as no-op, keeps floor). Content-safety:
      assert no token is logged (the fetch fn takes the token but never logs it — structural).
- [ ] **Step 2:** Run — fail. **Step 3:** Implement `HttpModelSource` + the pure helpers. **Step 4:** Green.
- [ ] **Step 5:** Commit: `feat(server): HttpModelSource — account-bearer upstream model fetch + parse`

---

### Task 3: Wire into `/models` + background-warm + config + e2e

**Files:** `crates/polyflare-server/src/catalog.rs` (`build_catalog` reads the cache), `app.rs` (`AppState.model_catalog:
Arc<ModelCatalogCache>` field), `main.rs`/serve (construct + background-warm), `config.rs`
(`POLYFLARE_MODEL_CATALOG_TTL_SECS` + `_ENABLED`); tests + e2e.

**Read first:** `build_catalog()` (`catalog.rs:57-83`) — how it currently assembles static + synthetic aliases; the
`AppState` construction (~45 sites for the new field — mechanical); `main.rs`'s codex_version background-warm block
to mirror.

**Implement:**
- `AppState.model_catalog: Arc<ModelCatalogCache>` (built in main/serve with `HttpModelSource` when
  `POLYFLARE_MODEL_CATALOG_ENABLED` else a floor-only/static source; the floor = the current `codex_bootstrap()` as
  `Vec<UpstreamModel>`). Background-warm `tokio::spawn` loop mirroring `main.rs:190-199` (only if enabled). ~45
  mechanical AppState test-site insertions (mirror the last feature's field additions).
- `catalog.rs::build_catalog` (or the handlers): read `state.model_catalog.cached_or_fallback()` for the codex model
  set INSTEAD of only `codex_bootstrap()`, then keep the existing synthetic-alias merge (real-upstream-wins). The
  handlers already take `State<Arc<AppState>>` (confirm) — if `build_catalog` is a free fn today, thread the cache in.
- Config: `model_catalog_ttl_secs` (default 3600, clamp e.g. [60, 86400]) + `model_catalog_enabled` (default ON;
  malformed ⇒ ON) on `ServeConfig`, via the existing `_from_env` idiom.

- [ ] **Step 1:** Failing tests: (a) e2e `GET /models` with the cache holding a merged catalog (seed via a
      StubSource `ModelCatalogCache`) ⇒ the response includes both floor slugs AND the stubbed upstream slug. (b)
      disabled / no-accounts / fetch-None ⇒ `GET /models` returns exactly the static floor (+ synthetic aliases) —
      today's behavior, never empty. (c) config parse (ttl unset⇒3600/=30 clamp/malformed⇒3600; enabled
      unset⇒true/=0⇒false). (d) the synthetic-alias merge still applies over the cached upstream set.
- [ ] **Step 2:** Run — fail. **Step 3:** Wire the field + warm loop + build_catalog read + config. **Step 4:**
      Green; all suites green (no routing path touched); clippy + fmt clean.
- [ ] **Step 5:** Commit: `feat(server): serve merged live model catalog on /models + POLYFLARE_MODEL_CATALOG_* config`

---

## Suggested order

1 (cache primitive, mirror CodexVersionCache) → 2 (HTTP source + parse) → 3 (wire + warm + config + e2e). After
Task 3, `/models` serves the live upstream catalog merged onto the static floor, refreshed on a TTL with single-
flight, falling back airtight to the static list on any failure/disable/no-accounts — no routing path touched, no
broken `/models` possible. Mark D15 DONE in `PORTING-CODEXLB.md` (single-node; leader-election/DB-snapshot +
per-plan catalogs deferred as multi-node/dashboard concerns). Follow-ups (not this plan): per-plan catalogs +
cross-account merge (codex-lb's dashboard need); persisted snapshot for restart-warm.
