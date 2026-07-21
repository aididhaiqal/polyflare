//! D15 Task 3 e2e: `/models` serves the live upstream model catalog (merged onto the static floor)
//! off `AppState.model_catalog`, and degrades airtight to exactly the static floor + synthetic
//! aliases when the cache holds no live data (mirrors the disabled/no-accounts/fetch-failure
//! path — see `crate::model_catalog::ModelCatalogCache`'s fallback ladder).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{CapacityWeighted, Continuity};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_server::model_catalog::{
    floor_only_model_catalog, ModelCatalogCache, ModelSource, UpstreamModel,
};
use polyflare_server::runtime_settings::{RuntimeSettings, RuntimeSettingsFields};
use polyflare_store::{Store, TokenCipher};

/// A [`ModelSource`] that always returns a fixed, pre-scripted upstream catalog — the external
/// (integration-test-crate) equivalent of `model_catalog.rs`'s internal `#[cfg(test)] StubSource`.
struct FixedSource(Vec<UpstreamModel>);

#[async_trait]
impl ModelSource for FixedSource {
    async fn fetch(&self) -> Option<Vec<UpstreamModel>> {
        Some(self.0.clone())
    }
}

/// Builds a minimal, real `AppState`/`build_app` server with the given `model_catalog`, seeding no
/// accounts (this endpoint is ungated and doesn't touch the store). Returns the server's base URL.
async fn spawn_with_catalog(model_catalog: Arc<ModelCatalogCache>) -> String {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    std::mem::forget(dir);
    let cipher = TokenCipher::from_key_bytes(&[21u8; 32]).unwrap();
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_secs(30),
    ));
    let state = Arc::new(AppState {
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
        log_bus: polyflare_server::log_bus::LogBus::new(1000),
        failover_metrics: polyflare_server::observability::FailoverMetrics::new(),
        health_tier_metrics: polyflare_server::observability::HealthTierMetrics::new(),
        lease_metrics: polyflare_server::observability::LeaseMetrics::new(),
        upstream_request_metrics: polyflare_server::observability::UpstreamRequestMetrics::new(),
        rate_limit_metrics: polyflare_server::observability::RateLimitMetrics::new(),
        relay_metrics: polyflare_server::observability::RelayMetrics::new(),
        starvation_metrics: polyflare_server::observability::StarvationMetrics::new(),
        model_catalog,
    });
    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

/// (a) The cache holds a merged catalog (floor + a stubbed upstream-only slug) — `GET /models`
/// must include the stub's upstream slug's full `ModelInfo` verbatim in `models` (Task 2: the
/// codex `models` array only carries entries with a `supported_reasoning_levels` marker — the
/// static floor's minimal placeholders are correctly omitted there), while `data` (OpenAI shape)
/// carries the complete merged catalog regardless.
#[tokio::test]
async fn models_endpoint_serves_merged_catalog_with_stub_upstream_slug() {
    let floor = polyflare_server::catalog::codex_bootstrap_floor();
    let stub = FixedSource(vec![UpstreamModel {
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
    }]);
    let cache = ModelCatalogCache::new(Box::new(stub), Duration::from_secs(3600), floor);
    // Warm it BEFORE serving — a fresh cache's sync `cached_or_fallback()` would still be the
    // floor until something calls `get_or_refresh()` at least once.
    cache.get_or_refresh().await;

    let base = spawn_with_catalog(Arc::new(cache)).await;
    let resp = reqwest::get(format!("{base}/models")).await.unwrap();
    assert_eq!(resp.status(), 200);
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
    // `data` (OpenAI shape) still carries the FULL merged catalog: floor + live upstream +
    // synthetic aliases — unaffected by Task 2.
    let ids: Vec<&str> = body["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["id"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&"gpt-5.6-sol"));
    assert!(ids.contains(&"gpt-5.5"));
    assert!(ids.contains(&"gpt-5.7-nova"));
    assert!(ids.contains(&"claude-opus-4-1"));
}

/// (b) A floor-only cache (the disabled / no-accounts / fetch-None path, via
/// `floor_only_model_catalog()`) — `GET /models`'s `data` (OpenAI shape) must return EXACTLY
/// today's static floor + synthetic aliases, never empty, never broken. The codex-shape `models`
/// array is empty here (Task 2: the static floor's raw entries are minimal placeholders with no
/// `supported_reasoning_levels` marker, so they're correctly omitted rather than emitted as
/// partial/invalid `ModelInfo`) — this matches the approved design's acceptance that an
/// unenriched floor just means codex falls back to its own bundled catalog, not an error.
#[tokio::test]
async fn models_endpoint_falls_back_to_static_floor_when_cache_has_no_live_data() {
    let base = spawn_with_catalog(floor_only_model_catalog()).await;
    let resp = reqwest::get(format!("{base}/models")).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();

    assert!(
        body["models"].as_array().unwrap().is_empty(),
        "the static floor + synthetic aliases have no full ModelInfo raw to emit into `models`"
    );

    let ids: Vec<&str> = body["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["id"].as_str().unwrap())
        .collect();
    // The `data` array carries the FULL merged catalog (static floor + synthetic aliases — see
    // `catalog.rs::build_catalog`), so the floor slugs must appear as a prefix, exactly as
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
    assert!(
        ids.contains(&"claude-opus-4-1"),
        "synthetic aliases still merged in"
    );
    assert!(ids.contains(&"claude-sonnet-4-5"));
    assert!(ids.contains(&"claude-haiku-4-5"));
}

/// `/v1/models` with `client_version` also reads the live-or-floor cache (not just the bare
/// `/models` path) — a quick parity check that both routes share the same read, including the
/// Task 2 marker-gated `models` array (full-raw live upstream present, minimal-raw floor
/// omitted) while `data` still carries everything.
#[tokio::test]
async fn v1_models_with_client_version_also_serves_merged_catalog() {
    let floor = polyflare_server::catalog::codex_bootstrap_floor();
    let stub = FixedSource(vec![UpstreamModel {
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
    }]);
    let cache = ModelCatalogCache::new(Box::new(stub), Duration::from_secs(3600), floor);
    cache.get_or_refresh().await;

    let base = spawn_with_catalog(Arc::new(cache)).await;
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
