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
        live_logs: false,
        ws_downstream: false,
        log_bus: polyflare_server::log_bus::LogBus::new(1000),
        max_account_attempts: 3,
        failover_metrics: polyflare_server::observability::FailoverMetrics::new(),
        health_tier_metrics: polyflare_server::observability::HealthTierMetrics::new(),
        starvation_wait_budget: Duration::from_secs(60),
        starvation_heartbeat: Duration::from_secs(10),
        wake_jitter_ms: 0,
        inflight_penalty_pct: 2.5,
        lease_metrics: polyflare_server::observability::LeaseMetrics::new(),
        upstream_request_metrics: polyflare_server::observability::UpstreamRequestMetrics::new(),
        rate_limit_metrics: polyflare_server::observability::RateLimitMetrics::new(),
        relay_metrics: polyflare_server::observability::RelayMetrics::new(),
        starvation_metrics: polyflare_server::observability::StarvationMetrics::new(),
        stream_idle_timeout: Duration::from_secs(300),
        soft_drain_enabled: true,
        request_log_retention_days: 0,
        usage_history_retention_days: 0,
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
/// must include BOTH the floor slugs AND the stub's upstream slug.
#[tokio::test]
async fn models_endpoint_serves_merged_catalog_with_stub_upstream_slug() {
    let floor = polyflare_server::catalog::codex_bootstrap_floor();
    let stub = FixedSource(vec![UpstreamModel {
        slug: "gpt-5.7-nova".to_string(),
        display_name: "GPT-5.7 Nova".to_string(),
        context_window: Some(500_000),
        prefer_websockets: Some(true),
        raw: serde_json::json!({"slug": "gpt-5.7-nova", "display_name": "GPT-5.7 Nova"}),
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
    // Floor slugs still present.
    assert!(slugs.contains(&"gpt-5.6-sol"));
    assert!(slugs.contains(&"gpt-5.5"));
    // The stubbed upstream-only slug is present too.
    assert!(slugs.contains(&"gpt-5.7-nova"));
    // ... and its enrichment rendered.
    let nova = body["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["slug"] == "gpt-5.7-nova")
        .unwrap();
    assert_eq!(nova["context_window"], 500_000);
    assert_eq!(nova["prefer_websockets"], true);
    // Synthetic aliases are still merged in on top.
    let ids: Vec<&str> = body["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["id"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&"claude-opus-4-1"));
}

/// (b) A floor-only cache (the disabled / no-accounts / fetch-None path, via
/// `floor_only_model_catalog()`) — `GET /models` must return EXACTLY today's static floor +
/// synthetic aliases, never empty, never broken.
#[tokio::test]
async fn models_endpoint_falls_back_to_static_floor_when_cache_has_no_live_data() {
    let base = spawn_with_catalog(floor_only_model_catalog()).await;
    let resp = reqwest::get(format!("{base}/models")).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();

    let slugs: Vec<&str> = body["models"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["slug"].as_str().unwrap())
        .collect();
    // The `models`/`data` arrays carry the FULL merged catalog (static floor + synthetic
    // aliases — see `catalog.rs::build_catalog`), so the floor slugs must appear as a prefix,
    // exactly as today's pre-D15 static catalog produced.
    assert_eq!(
        &slugs[..5],
        [
            "gpt-5.6-sol",
            "gpt-5.6-terra",
            "gpt-5.6-luna",
            "gpt-5.5",
            "gpt-5.4"
        ],
        "disabled/no-live-data must serve exactly the static floor first, in order"
    );
    assert!(!slugs.is_empty(), "never an empty /models catalog");
    // No live-upstream-only slug (e.g. a stubbed `gpt-5.7-nova`) leaked in from a colder run.
    assert!(!slugs.contains(&"gpt-5.7-nova"));

    let ids: Vec<&str> = body["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["id"].as_str().unwrap())
        .collect();
    assert!(
        ids.contains(&"claude-opus-4-1"),
        "synthetic aliases still merged in"
    );
    assert!(ids.contains(&"claude-sonnet-4-5"));
    assert!(ids.contains(&"claude-haiku-4-5"));
}

/// `/v1/models` with `client_version` also reads the live-or-floor cache (not just the bare
/// `/models` path) — a quick parity check that both routes share the same read.
#[tokio::test]
async fn v1_models_with_client_version_also_serves_merged_catalog() {
    let floor = polyflare_server::catalog::codex_bootstrap_floor();
    let stub = FixedSource(vec![UpstreamModel {
        slug: "gpt-5.7-nova".to_string(),
        display_name: "GPT-5.7 Nova".to_string(),
        context_window: None,
        prefer_websockets: None,
        raw: serde_json::json!({"slug": "gpt-5.7-nova", "display_name": "GPT-5.7 Nova"}),
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
    assert!(slugs.contains(&"gpt-5.5"));
}
