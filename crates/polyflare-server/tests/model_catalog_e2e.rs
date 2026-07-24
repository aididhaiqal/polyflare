//! D15 Task 3 e2e: `/models` serves the live upstream model catalog (merged onto the static floor)
//! off `AppState.model_catalog`, and degrades airtight to exactly the static floor when the cache
//! holds no live data. Claude translation aliases remain route-only and are never advertised.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::routing::get;
use axum::Router;
use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{CapacityWeighted, Continuity};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_server::model_catalog::{
    floor_only_model_catalog, AccountCatalog, FetchedCatalog, ModelCatalogCache, ModelSource,
    UpstreamModel,
};
use polyflare_server::runtime_settings::{RuntimeSettings, RuntimeSettingsFields};
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};

/// A [`ModelSource`] that always returns a fixed, pre-scripted upstream catalog — the external
/// (integration-test-crate) equivalent of `model_catalog.rs`'s internal `#[cfg(test)] StubSource`.
struct FixedSource(Vec<UpstreamModel>);

#[async_trait]
impl ModelSource for FixedSource {
    async fn fetch(&self) -> Option<FetchedCatalog> {
        Some(FetchedCatalog {
            models: self.0.clone(),
            etag: Some("\"fixture-models-etag\"".to_string()),
        })
    }

    async fn fetch_scoped(&self, account_ids: &[String]) -> Option<Vec<AccountCatalog>> {
        Some(
            account_ids
                .iter()
                .map(|account_id| AccountCatalog {
                    account_id: account_id.clone(),
                    catalog: FetchedCatalog {
                        models: self.0.clone(),
                        etag: Some("\"fixture-models-etag\"".to_string()),
                    },
                })
                .collect(),
        )
    }
}

/// A per-account source used to prove that scoped catalogs are built from exactly the accounts
/// named by the caller rather than from whichever global account happened to warm first.
struct ScopedFixedSource(std::collections::HashMap<String, FetchedCatalog>);

#[async_trait]
impl ModelSource for ScopedFixedSource {
    async fn fetch(&self) -> Option<FetchedCatalog> {
        self.0.values().next().cloned()
    }

    async fn fetch_scoped(&self, account_ids: &[String]) -> Option<Vec<AccountCatalog>> {
        account_ids
            .iter()
            .map(|account_id| {
                self.0
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

fn full_model(slug: &str) -> UpstreamModel {
    UpstreamModel {
        slug: slug.to_string(),
        display_name: slug.to_string(),
        context_window: None,
        prefer_websockets: None,
        raw: serde_json::json!({
            "slug": slug,
            "display_name": slug,
            "supported_reasoning_levels": [{"effort": "medium", "description": "x"}],
            "visibility": "list",
            "supported_in_api": true,
            "priority": 1
        }),
    }
}

fn fetched(etag: &str, slugs: &[&str]) -> FetchedCatalog {
    FetchedCatalog {
        models: slugs.iter().map(|slug| full_model(slug)).collect(),
        etag: Some(etag.to_string()),
    }
}

async fn test_state(model_catalog: Arc<ModelCatalogCache>) -> Arc<AppState> {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    std::mem::forget(dir);
    let cipher = TokenCipher::from_key_bytes(&[21u8; 32]).unwrap();
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_secs(30),
    ));
    Arc::new(AppState {
        enforce_client_keys: false,
        codex_executor: Arc::new(CodexExecutor::new().unwrap()),
        control_client: polyflare_codex::build_client().expect("build control_client"),
        anthropic_executor: Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap()),
        selector: Arc::new(CapacityWeighted),
        pool_selectors: Default::default(),
        continuity,
        store,
        cipher,
        oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(),
        upstream_base_url: "http://127.0.0.1:9".to_string(),
        anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),
        refresh_locks: Default::default(),
        capture_fingerprint_path: None,
        codex_version: Arc::new(polyflare_codex::CodexVersionCache::new().unwrap()),
        account_cache: Arc::new(polyflare_server::account_cache::AccountCache::new()),
        token_cache: Default::default(),
        runtime: Default::default(),
        admin_token: None,
        runtime_settings: Arc::new(RuntimeSettings::new_from_fields(RuntimeSettingsFields {
            max_account_attempts: 3,
            starvation_wait_budget: Duration::from_secs(60),
            starvation_heartbeat: Duration::from_secs(10),
            wake_jitter_ms: 0,
            stream_idle_timeout: Duration::from_secs(300),
            inflight_penalty_pct: 2.5,
            soft_drain_enabled: true,
            request_log_retention_days: 0,
            usage_history_retention_days: 0,
            live_logs: false,
        })),
        ws_downstream: false,
        ws_relay_idle: polyflare_server::ws_relay::WsRelayIdlePolicy::default(),
        log_bus: polyflare_server::log_bus::LogBus::new(1000),
        failover_metrics: polyflare_server::observability::FailoverMetrics::new(),
        health_tier_metrics: polyflare_server::observability::HealthTierMetrics::new(),
        lease_metrics: polyflare_server::observability::LeaseMetrics::new(),
        upstream_request_metrics: polyflare_server::observability::UpstreamRequestMetrics::new(),
        rate_limit_metrics: polyflare_server::observability::RateLimitMetrics::new(),
        relay_metrics: polyflare_server::observability::RelayMetrics::new(),
        starvation_metrics: polyflare_server::observability::StarvationMetrics::new(),
        model_catalog,
    })
}

async fn spawn_app(app: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

/// Builds a minimal, real `AppState`/`build_app` server with the given `model_catalog`, seeding no
/// accounts (this endpoint is ungated and doesn't touch the store). Returns the server's base URL.
async fn spawn_with_catalog(model_catalog: Arc<ModelCatalogCache>) -> String {
    spawn_app(build_app(test_state(model_catalog).await)).await
}

async fn spawn_with_catalog_and_active_account(model_catalog: Arc<ModelCatalogCache>) -> String {
    let state = test_state(model_catalog).await;
    state
        .store
        .accounts()
        .insert(
            &account("active-catalog-account"),
            &PlainTokens {
                access_token: "access".to_string(),
                refresh_token: "refresh".to_string(),
                id_token: "id".to_string(),
            },
            &state.cipher,
        )
        .await
        .unwrap();
    spawn_app(build_app(state)).await
}

fn account(id: &str) -> Account {
    Account {
        id: id.to_string(),
        chatgpt_account_id: Some(format!("chatgpt-{id}")),
        chatgpt_user_id: None,
        email: format!("{id}@example.test"),
        alias: None,
        workspace_id: None,
        workspace_label: None,
        seat_type: None,
        plan_type: "plus".to_string(),
        routing_policy: "eligible".to_string(),
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

/// (a) The cache holds a merged catalog (floor + a stubbed upstream-only slug) — `GET /models`
/// must include the stub's upstream slug's full `ModelInfo` verbatim in `models` (Task 2: the
/// codex `models` array only carries entries with a `supported_reasoning_levels` marker — the
/// static floor's minimal placeholders are correctly omitted there), while `data` (OpenAI shape)
/// carries the complete merged catalog regardless.
#[tokio::test]
async fn models_endpoint_serves_merged_catalog_with_stub_upstream_slug() {
    let floor = polyflare_server::catalog::codex_bootstrap_floor();
    let mut upstream = floor.clone();
    upstream.push(UpstreamModel {
        slug: "gpt-5.7-nova".to_string(),
        display_name: "GPT-5.7 Nova".to_string(),
        context_window: Some(500_000),
        prefer_websockets: Some(true),
        raw: serde_json::json!({
            "slug": "gpt-5.7-nova",
            "display_name": "GPT-5.7 Nova",
            "context_window": 500_000,
            "prefer_websockets": true,
            "supported_reasoning_levels": [{"effort": "medium", "description": "x"}],
            "visibility": "list",
            "supported_in_api": true,
            "priority": 1
        }),
    });
    let stub = FixedSource(upstream);
    let cache = ModelCatalogCache::new(Box::new(stub), Duration::from_secs(3600), floor);
    // Warm it BEFORE serving — a fresh cache's sync `cached_or_fallback()` would still be the
    // floor until something calls `get_or_refresh()` at least once.
    cache.get_or_refresh().await;

    let base = spawn_with_catalog_and_active_account(Arc::new(cache)).await;
    let resp = reqwest::get(format!("{base}/models")).await.unwrap();
    assert_eq!(resp.status(), 200);
    let etag = resp
        .headers()
        .get(reqwest::header::ETAG)
        .and_then(|value| value.to_str().ok())
        .expect("an active authoritative scope emits a virtual ETag");
    assert_ne!(
        etag, "\"fixture-models-etag\"",
        "the root scope must never expose one account's native upstream ETag"
    );
    let body: serde_json::Value = resp.json().await.unwrap();

    let slugs: Vec<&str> = body["models"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["slug"].as_str().unwrap())
        .collect();
    // The floor's minimal raw placeholders are omitted from the codex-parseable `models` array.
    assert!(!slugs.contains(&"gpt-5.6-sol"));
    // The stubbed upstream slug's full ModelInfo IS present, verbatim.
    assert!(slugs.contains(&"gpt-5.7-nova"));
    let nova = body["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["slug"] == "gpt-5.7-nova")
        .unwrap();
    assert_eq!(nova["context_window"], 500_000);
    assert_eq!(nova["prefer_websockets"], true);
    // `data` carries the native floor plus live upstream models. Translation aliases stay on the
    // Claude request path and must not pollute the Codex picker.
    let ids: Vec<&str> = body["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["id"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&"gpt-5.6-sol"));
    assert!(ids.contains(&"gpt-5.5"));
    assert!(ids.contains(&"gpt-5.7-nova"));
    assert!(!ids.contains(&"claude-opus-4-1"));
}

/// (b) A floor-only cache (the disabled / no-accounts / fetch-None path, via
/// `floor_only_model_catalog()`) — `GET /models`'s `data` (OpenAI shape) must return EXACTLY
/// today's static floor, never empty, never broken. The codex-shape `models`
/// array is empty here (Task 2: the static floor's raw entries are minimal placeholders with no
/// `supported_reasoning_levels` marker, so they're correctly omitted rather than emitted as
/// partial/invalid `ModelInfo`) — this matches the approved design's acceptance that an
/// unenriched floor just means codex falls back to its own bundled catalog, not an error.
#[tokio::test]
async fn models_endpoint_falls_back_to_static_floor_when_cache_has_no_live_data() {
    let base = spawn_with_catalog(floor_only_model_catalog()).await;
    let resp = reqwest::get(format!("{base}/models")).await.unwrap();
    assert_eq!(resp.status(), 200);
    assert!(
        resp.headers().get(reqwest::header::ETAG).is_none(),
        "an empty active fleet must not reuse an ETag cached for a formerly active account"
    );
    let body: serde_json::Value = resp.json().await.unwrap();

    assert!(
        body["models"].as_array().unwrap().is_empty(),
        "the static floor has no full ModelInfo raw to emit into `models`"
    );

    let ids: Vec<&str> = body["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["id"].as_str().unwrap())
        .collect();
    // The `data` array carries the static floor, so the floor slugs must appear as a prefix, exactly as
    // today's pre-D15 static catalog produced.
    assert_eq!(
        &ids[..5],
        [
            "gpt-5.6-sol",
            "gpt-5.6-terra",
            "gpt-5.6-luna",
            "gpt-5.5",
            "gpt-5.4"
        ],
        "disabled/no-live-data must serve exactly the static floor first, in order"
    );
    assert!(!ids.is_empty(), "never an empty /models catalog");
    // No live-upstream-only slug (e.g. a stubbed `gpt-5.7-nova`) leaked in from a colder run.
    assert!(!ids.contains(&"gpt-5.7-nova"));
    assert!(!ids.iter().any(|id| id.starts_with("claude-")));
}

/// `/v1/models` with `client_version` also reads the live-or-floor cache (not just the bare
/// `/models` path) — a quick parity check that both routes share the same read, including the
/// Task 2 marker-gated `models` array (full-raw live upstream present, minimal-raw floor
/// omitted) while `data` still carries everything.
#[tokio::test]
async fn v1_models_with_client_version_also_serves_merged_catalog() {
    let floor = polyflare_server::catalog::codex_bootstrap_floor();
    let mut upstream = floor.clone();
    upstream.push(UpstreamModel {
        slug: "gpt-5.7-nova".to_string(),
        display_name: "GPT-5.7 Nova".to_string(),
        context_window: None,
        prefer_websockets: None,
        raw: serde_json::json!({
            "slug": "gpt-5.7-nova",
            "display_name": "GPT-5.7 Nova",
            "supported_reasoning_levels": [{"effort": "medium", "description": "x"}],
            "visibility": "list",
            "supported_in_api": true,
            "priority": 1
        }),
    });
    let stub = FixedSource(upstream);
    let cache = ModelCatalogCache::new(Box::new(stub), Duration::from_secs(3600), floor);
    cache.get_or_refresh().await;

    let base = spawn_with_catalog_and_active_account(Arc::new(cache)).await;
    let resp = reqwest::get(format!("{base}/v1/models?client_version=0.144.4"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let slugs: Vec<&str> = body["models"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["slug"].as_str().unwrap())
        .collect();
    assert!(slugs.contains(&"gpt-5.7-nova"));
    assert!(!slugs.contains(&"gpt-5.5"));

    let ids: Vec<&str> = body["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["id"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&"gpt-5.5"));
}

#[tokio::test]
async fn scoped_catalog_intersects_only_the_named_accounts_and_isolates_other_pools() {
    let source = ScopedFixedSource(std::collections::HashMap::from([
        (
            "pool-a-1".to_string(),
            fetched("\"a1\"", &["gpt-common", "gpt-a-only"]),
        ),
        (
            "pool-a-2".to_string(),
            fetched("\"a2\"", &["gpt-common", "gpt-a2-only"]),
        ),
        ("pool-b-1".to_string(), fetched("\"b1\"", &["gpt-b-only"])),
    ]));
    let cache = ModelCatalogCache::new(
        Box::new(source),
        Duration::from_secs(3600),
        polyflare_server::catalog::codex_bootstrap_floor(),
    );

    let pool_a = cache
        .get_or_refresh_scoped(&["pool-a-1".to_string(), "pool-a-2".to_string()])
        .await;
    let pool_b = cache.get_or_refresh_scoped(&["pool-b-1".to_string()]).await;

    assert_eq!(
        pool_a
            .models
            .iter()
            .map(|model| model.slug.as_str())
            .collect::<Vec<_>>(),
        ["gpt-common"],
        "a pool may advertise only the safe intersection of all active member catalogs"
    );
    assert_eq!(
        pool_b
            .models
            .iter()
            .map(|model| model.slug.as_str())
            .collect::<Vec<_>>(),
        ["gpt-b-only"],
        "an out-of-pool account must not influence the scoped result"
    );
    assert_eq!(
        cache.account_supports_model("pool-a-1", "gpt-a-only"),
        Some(true)
    );
    assert_eq!(
        cache.account_supports_model("pool-a-2", "gpt-a-only"),
        Some(false),
        "routing can exclude a member that lacks a directly requested model"
    );
}

#[tokio::test]
async fn scoped_virtual_etag_is_deterministic_and_never_reuses_an_upstream_or_other_pool_etag() {
    let source = ScopedFixedSource(std::collections::HashMap::from([
        (
            "pool-a-1".to_string(),
            fetched("\"shared-upstream\"", &["gpt-common"]),
        ),
        ("pool-a-2".to_string(), fetched("\"a2\"", &["gpt-common"])),
        (
            "pool-b-1".to_string(),
            fetched("\"shared-upstream\"", &["gpt-common"]),
        ),
    ]));
    let cache = ModelCatalogCache::new(
        Box::new(source),
        Duration::from_secs(3600),
        polyflare_server::catalog::codex_bootstrap_floor(),
    );

    let pool_a = cache
        .get_or_refresh_scoped(&["pool-a-2".to_string(), "pool-a-1".to_string()])
        .await;
    let pool_a_reordered = cache
        .get_or_refresh_scoped(&["pool-a-1".to_string(), "pool-a-2".to_string()])
        .await;
    let pool_b = cache.get_or_refresh_scoped(&["pool-b-1".to_string()]).await;

    assert_eq!(pool_a.etag, pool_a_reordered.etag);
    assert_ne!(pool_a.etag, pool_b.etag);
    assert_ne!(pool_a.etag.as_deref(), Some("\"shared-upstream\""));
    assert_ne!(pool_b.etag.as_deref(), Some("\"shared-upstream\""));
}

#[tokio::test]
async fn cold_root_and_pool_etag_resolvers_warm_the_exact_scope_on_demand() {
    let cache = Arc::new(ModelCatalogCache::new(
        Box::new(ScopedFixedSource(std::collections::HashMap::from([
            (
                "acct-a".to_string(),
                fetched("\"native-a\"", &["gpt-common", "gpt-a-only"]),
            ),
            (
                "acct-b".to_string(),
                fetched("\"native-b\"", &["gpt-common", "gpt-b-only"]),
            ),
        ]))),
        Duration::from_secs(3600),
        polyflare_server::catalog::codex_bootstrap_floor(),
    ));
    assert_eq!(
        cache.cached_scoped_etag(&["acct-a".to_string(), "acct-b".to_string()]),
        None,
        "the regression must begin from a genuinely cold root scope"
    );
    let state = test_state(cache.clone()).await;
    let tokens = PlainTokens {
        access_token: "access".to_string(),
        refresh_token: "refresh".to_string(),
        id_token: "id".to_string(),
    };
    for id in ["acct-a", "acct-b"] {
        state
            .store
            .accounts()
            .insert(&account(id), &tokens, &state.cipher)
            .await
            .unwrap();
    }
    state
        .store
        .accounts()
        .replace_pools("acct-a", &["pool-a".to_string()])
        .await
        .unwrap();

    let warmed = polyflare_server::catalog::warm_active_model_scopes(&state).await;
    assert_eq!(
        warmed,
        polyflare_server::catalog::ModelScopeWarmup {
            attempted_scopes: 2,
            authoritative_scopes: 2,
        }
    );
    let root = cache
        .cached_scoped_etag(&["acct-a".to_string(), "acct-b".to_string()])
        .expect("startup warmup must prime the root scope");
    let pool = cache
        .cached_scoped_etag(&["acct-a".to_string()])
        .expect("startup warmup must prime every named pool scope");

    assert_ne!(
        root, pool,
        "different exact scopes need different identities"
    );
    for native in ["\"native-a\"", "\"native-b\""] {
        assert_ne!(root, native);
        assert_ne!(pool, native);
    }
    assert_eq!(
        cache.cached_scoped_etag(&["acct-a".to_string(), "acct-b".to_string()]),
        Some(root)
    );
    assert_eq!(
        cache.cached_scoped_etag(&["acct-a".to_string()]),
        Some(pool)
    );
}

#[tokio::test]
async fn pooled_handler_uses_active_multi_pool_membership_and_keeps_catalogs_isolated() {
    let source = ScopedFixedSource(std::collections::HashMap::from([
        (
            "pool-a-1".to_string(),
            fetched("\"a1\"", &["gpt-common", "gpt-a-only"]),
        ),
        (
            "pool-a-2".to_string(),
            fetched("\"a2\"", &["gpt-common", "gpt-a2-only"]),
        ),
        ("pool-b-1".to_string(), fetched("\"b1\"", &["gpt-b-only"])),
    ]));
    let cache = Arc::new(ModelCatalogCache::new(
        Box::new(source),
        Duration::from_secs(3600),
        polyflare_server::catalog::codex_bootstrap_floor(),
    ));
    let state = test_state(cache).await;
    let tokens = PlainTokens {
        access_token: "access".to_string(),
        refresh_token: "refresh".to_string(),
        id_token: "id".to_string(),
    };
    for id in ["pool-a-1", "pool-a-2", "pool-b-1"] {
        state
            .store
            .accounts()
            .insert(&account(id), &tokens, &state.cipher)
            .await
            .unwrap();
    }
    state
        .store
        .accounts()
        .replace_pools("pool-a-1", &["pool-a".to_string(), "shared".to_string()])
        .await
        .unwrap();
    state
        .store
        .accounts()
        .replace_pools("pool-a-2", &["pool-a".to_string()])
        .await
        .unwrap();
    state
        .store
        .accounts()
        .replace_pools("pool-b-1", &["pool-b".to_string()])
        .await
        .unwrap();

    let app = Router::new()
        .route(
            "/{pool}/models",
            get(polyflare_server::catalog::pooled_codex_models_handler),
        )
        .with_state(state);
    let base = spawn_app(app).await;

    let pool_a = reqwest::get(format!("{base}/pool-a/models")).await.unwrap();
    let pool_a_etag = pool_a
        .headers()
        .get(reqwest::header::ETAG)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let pool_a_body: serde_json::Value = pool_a.json().await.unwrap();
    assert_eq!(
        pool_a_body["models"]
            .as_array()
            .unwrap()
            .iter()
            .map(|model| model["slug"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["gpt-common"]
    );
    assert_eq!(
        pool_a_body["data"]
            .as_array()
            .unwrap()
            .iter()
            .map(|model| model["id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["gpt-common"],
        "pool discovery must contain only models in the pool intersection"
    );

    let pool_b = reqwest::get(format!("{base}/pool-b/models")).await.unwrap();
    let pool_b_etag = pool_b
        .headers()
        .get(reqwest::header::ETAG)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let pool_b_body: serde_json::Value = pool_b.json().await.unwrap();
    assert_eq!(
        pool_b_body["models"]
            .as_array()
            .unwrap()
            .iter()
            .map(|model| model["slug"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["gpt-b-only"]
    );
    assert_ne!(pool_a_etag, pool_b_etag);
    assert_ne!(pool_b_etag, "\"b1\"");

    let shared_body: serde_json::Value = reqwest::get(format!("{base}/shared/models"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        shared_body["models"]
            .as_array()
            .unwrap()
            .iter()
            .any(|model| model["slug"] == "gpt-a-only"),
        "secondary pool membership must be honored, not only accounts.pool"
    );
}
