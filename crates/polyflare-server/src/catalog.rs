//! Model catalog: serves native Codex models plus operator-configured models whose discovery policy
//! includes the requested client surface. Claude translation aliases are routing-only and never
//! leak into the Codex picker or generic OpenAI list.
//!
//! # Two shapes, content-negotiated (matching codex-lb)
//! - `GET /v1/models` with NO `client_version` -> the OpenAI-style `{object:"list", data:[...]}`.
//! - `GET /v1/models?client_version=...`, `GET /models`, `GET /backend-api/codex/models` -> the
//!   Codex `{object:"list", models:[...], data:[...]}` catalog shape. A real Codex CLI that hits
//!   `/v1/models` sends `client_version` and expects the rich Codex catalog (it silently falls back
//!   to stale bundled metadata if handed the thin OpenAI list), so the negotiation is load-bearing.
//!
//! # This increment (D15 Task 3)
//! The Codex side reads `AppState.model_catalog` (see `crate::model_catalog::ModelCatalogCache`):
//! a live-upstream-fetch-merged-onto-the-static-floor catalog, TTL-cached, falling back airtight to
//! the static floor on any failure/disable/no-accounts. `codex_bootstrap_floor()` below IS that
//! static floor (converted from this module's own bootstrap slugs) — the same 5 slugs, never
//! empty. Claude `/v1/messages` translation is handled separately by `crate::alias`; it is not a
//! Codex/OpenAI model-discovery source.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::header::ETAG;
use axum::response::{IntoResponse, Json, Response};
use polyflare_core::Provider;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::alias::synthetic_models;
use crate::app::AppState;
use crate::model_catalog::UpstreamModel;

pub fn model_slug_is_reserved(state: &AppState, slug: &str) -> bool {
    state
        .model_catalog
        .cached_or_fallback()
        .iter()
        .any(|model| model.slug == slug)
        || synthetic_models().iter().any(|model| model.id == slug)
}

#[derive(Clone, Copy)]
enum CatalogSurface {
    Codex,
    OpenAi,
}

/// Accept only typed, non-structural overrides for PolyFlare's generated `ModelInfo` template.
///
/// Raw `ModelInfo` replacement is intentionally not supported: codex-rs evolves that schema and a
/// partial or malformed nested value can make the whole rich catalog unparsable. These fields are
/// leaf values whose accepted JSON types exactly match codex-rs, so applying them cannot remove or
/// invalidate any required template field.
pub(crate) fn safe_codex_model_info_extensions(value: &serde_json::Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    !object.is_empty()
        && object.iter().all(|(key, value)| match key.as_str() {
            "description" => value
                .as_str()
                .is_some_and(|description| description.len() <= 4 * 1024),
            "base_instructions" => value.is_string(),
            "priority" => value
                .as_i64()
                .is_some_and(|priority| i32::try_from(priority).is_ok()),
            _ => false,
        })
}

/// A provider-agnostic catalog row before it's shaped for a response.
struct CatalogModel {
    id: String,
    display_name: String,
    /// Provider owner exposed by the OpenAI-compatible list shape.
    owned_by: &'static str,
    /// Context window size in tokens, when known (carried through from a live-upstream
    /// [`UpstreamModel`]; `None` for static-floor entries that don't declare it).
    /// Rendered in both response shapes when present — see `to_codex_response`/`to_openai_items`.
    context_window: Option<u64>,
    /// Whether this model prefers the WebSocket transport, when known. Same provenance/rendering
    /// as `context_window` above.
    prefer_websockets: Option<bool>,
    /// Extra fields surfaced under `metadata` in the OpenAI shape.
    metadata: serde_json::Map<String, serde_json::Value>,
    /// The full upstream `ModelInfo` entry (verbatim, from [`UpstreamModel::raw`]). Emitted
    /// byte-for-byte into the Codex `models` array by
    /// `to_codex_response` when it's a full `ModelInfo` (carries the `supported_reasoning_levels`
    /// marker); a minimal/partial static-floor value is omitted from `models` but never affects
    /// the OpenAI `data` shape.
    raw: serde_json::Value,
}

/// The bootstrap floor of real Codex model slugs — the static list served when the live upstream
/// fetch is disabled/unavailable/empty (see `crate::model_catalog::ModelCatalogCache`'s fallback
/// ladder). Trimmed from codex-lb's `_BOOTSTRAP_STATIC_MODELS`; these are the alias targets plus a
/// couple of common base models so the catalog is never empty.
fn codex_bootstrap() -> Vec<CatalogModel> {
    const SLUGS: &[(&str, &str)] = &[
        ("gpt-5.6-sol", "GPT-5.6 Sol"),
        ("gpt-5.6-terra", "GPT-5.6 Terra"),
        ("gpt-5.6-luna", "GPT-5.6 Luna"),
        ("gpt-5.5", "GPT-5.5"),
        ("gpt-5.4", "GPT-5.4"),
    ];
    SLUGS
        .iter()
        .map(|(slug, name)| CatalogModel {
            id: (*slug).to_string(),
            display_name: (*name).to_string(),
            owned_by: "openai",
            context_window: None,
            prefer_websockets: None,
            metadata: serde_json::Map::new(),
            raw: serde_json::json!({"slug": *slug, "display_name": *name}),
        })
        .collect()
}

/// The bootstrap floor as [`UpstreamModel`]s — the never-empty `floor` every
/// `ModelCatalogCache::new` call (production AND every test/dev construction site) is built with.
/// Same 5 static slugs as `codex_bootstrap()` above, just reshaped for the cache; the
/// never-empty-floor guarantee this crate's D15 review flagged depends on this NEVER returning an
/// empty `Vec` (see `codex_bootstrap_floor_is_never_empty` below).
pub fn codex_bootstrap_floor() -> Vec<UpstreamModel> {
    codex_bootstrap()
        .into_iter()
        .map(|m| UpstreamModel {
            slug: m.id,
            display_name: m.display_name,
            context_window: m.context_window,
            prefer_websockets: m.prefer_websockets,
            raw: m.raw,
        })
        .collect()
}

fn catalog_model_from_upstream(u: &UpstreamModel) -> CatalogModel {
    CatalogModel {
        id: u.slug.clone(),
        display_name: u.display_name.clone(),
        owned_by: "openai",
        context_window: u.context_window,
        prefer_websockets: u.prefer_websockets,
        metadata: serde_json::Map::new(),
        raw: u.raw.clone(),
    }
}

/// Shape the already-resolved native + visible custom model set. Translation aliases deliberately
/// stay outside discovery; route lookup in `alias.rs` remains independent.
fn build_catalog(live_models: &[UpstreamModel]) -> Vec<CatalogModel> {
    live_models
        .iter()
        .map(catalog_model_from_upstream)
        .collect()
}

async fn root_models_with_custom(
    state: &AppState,
    mut native: Vec<UpstreamModel>,
    surface: CatalogSurface,
) -> Vec<UpstreamModel> {
    let configured = state
        .store
        .providers()
        .list_enabled_models()
        .await
        .unwrap_or_default();
    let mut seen: std::collections::HashSet<String> =
        native.iter().map(|model| model.slug.clone()).collect();
    for (provider, model) in configured.into_iter().filter(|(_, model)| match surface {
        CatalogSurface::Codex => model.visible_in_codex,
        CatalogSurface::OpenAi => model.visible_in_openai,
    }) {
        if !seen.insert(model.public_model.clone()) {
            tracing::warn!(
                provider = %provider.slug,
                model = %model.public_model,
                "custom model collides with built-in catalog; built-in model wins"
            );
            continue;
        }
        let reasoning: Vec<String> =
            serde_json::from_str(&model.reasoning_levels_json).unwrap_or_default();
        let supported_reasoning_levels: Vec<serde_json::Value> = reasoning
            .iter()
            .map(|effort| {
                serde_json::json!({
                    "effort": effort,
                    "description": format!("{effort} reasoning")
                })
            })
            .collect();
        let mut raw = serde_json::json!({
            "slug": model.public_model,
            "display_name": model.display_name,
            "description": format!("{} via {}", model.display_name, provider.display_name),
            "default_reasoning_level": reasoning.first(),
            "supported_reasoning_levels": supported_reasoning_levels,
            "shell_type": "shell_command",
            "visibility": "list",
            "supported_in_api": true,
            "priority": 50,
            "additional_speed_tiers": [],
            "service_tiers": [],
            "default_service_tier": null,
            "availability_nux": null,
            "upgrade": null,
            "base_instructions": "",
            "model_messages": null,
            "include_skills_usage_instructions": false,
            "supports_reasoning_summary_parameter": model.supports_reasoning_summaries,
            "default_reasoning_summary": "none",
            "support_verbosity": false,
            "default_verbosity": null,
            "apply_patch_tool_type": if model.supports_tools { Some("freeform") } else { None },
            "web_search_tool_type": if model.supports_web_search { "text_and_image" } else { "text" },
            "truncation_policy": {"mode": "tokens", "limit": 10000},
            "supports_parallel_tool_calls": model.supports_parallel_tool_calls,
            "supports_image_detail_original": false,
            "context_window": model.context_window,
            "max_context_window": model.context_window,
            "auto_compact_token_limit": null,
            "comp_hash": null,
            "experimental_supported_tools": [],
            "input_modalities": if model.supports_vision { vec!["text", "image"] } else { vec!["text"] },
            "supports_search_tool": model.supports_web_search,
            "use_responses_lite": false,
            "auto_review_model_override": null,
            "tool_mode": null,
            "multi_agent_version": null
        });
        if let Some(object) = raw.as_object_mut() {
            if let Some(extensions) = model
                .model_info_json
                .as_deref()
                .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
                .filter(safe_codex_model_info_extensions)
                .and_then(|value| value.as_object().cloned())
            {
                object.extend(extensions);
            }
            object.insert("slug".into(), model.public_model.clone().into());
            object.insert("display_name".into(), model.display_name.clone().into());
            object.insert("context_window".into(), model.context_window.into());
            object.insert(
                "supported_reasoning_levels".into(),
                serde_json::Value::Array(supported_reasoning_levels),
            );
            object.insert(
                "supports_parallel_tool_calls".into(),
                model.supports_parallel_tool_calls.into(),
            );
            object.insert(
                "supports_search_tool".into(),
                model.supports_web_search.into(),
            );
        }
        native.push(UpstreamModel {
            slug: model.public_model,
            display_name: model.display_name,
            context_window: model
                .context_window
                .and_then(|value| u64::try_from(value).ok()),
            // PolyFlare can still accept downstream WS, but the custom upstream transport is SSE.
            prefer_websockets: Some(false),
            raw,
        });
    }
    native
}

fn custom_catalog_etag(models: &[UpstreamModel]) -> Option<String> {
    if models.is_empty() {
        return None;
    }
    let mut hasher = Sha256::new();
    for model in models {
        hasher.update(model.slug.as_bytes());
        hasher.update([0]);
        hasher.update(model.raw.to_string().as_bytes());
        hasher.update([0xff]);
    }
    Some(format!("\"pf-{}\"", hex::encode(&hasher.finalize()[..16])))
}

fn root_catalog_etag(
    native_etag: Option<String>,
    native_model_count: usize,
    merged: &[UpstreamModel],
) -> Option<String> {
    if merged.len() == native_model_count {
        native_etag
    } else {
        custom_catalog_etag(merged)
    }
}

// --- response shapes ---

/// One model in the OpenAI-style `/v1/models` list.
#[derive(Serialize)]
struct OpenAiModel {
    id: String,
    object: &'static str,
    created: i64,
    owned_by: String,
    #[serde(skip_serializing_if = "serde_json::Map::is_empty")]
    metadata: serde_json::Map<String, serde_json::Value>,
}

#[derive(Serialize)]
struct OpenAiModelList {
    object: &'static str,
    data: Vec<OpenAiModel>,
}

#[derive(Serialize)]
struct CodexModelsResponse {
    object: &'static str,
    /// The real, codex-parseable `ModelInfo` entries — each `CatalogModel.raw` emitted
    /// byte-for-byte (verbatim, no reparse into a lossy struct). A `CatalogModel` whose `raw`
    /// isn't a full `ModelInfo` (missing the `supported_reasoning_levels` marker — the static
    /// floor) is OMITTED here: a partial entry would fail codex's parse. See
    /// `to_codex_response`.
    models: Vec<serde_json::Value>,
    /// OpenAI-item mirror for clients that read `data`. Includes every visible catalog row,
    /// regardless of whether its raw document was complete enough for `models` above.
    data: Vec<OpenAiModel>,
}

fn to_openai_items(models: &[CatalogModel]) -> Vec<OpenAiModel> {
    models
        .iter()
        .map(|m| {
            // Preserve any configured metadata while projecting the typed transport fields into
            // the OpenAI-compatible shape.
            let mut metadata = m.metadata.clone();
            if let Some(cw) = m.context_window {
                metadata.insert("context_window".to_string(), serde_json::Value::from(cw));
            }
            if let Some(pw) = m.prefer_websockets {
                metadata.insert("prefer_websockets".to_string(), serde_json::Value::Bool(pw));
            }
            OpenAiModel {
                id: m.id.clone(),
                object: "model",
                created: 0,
                owned_by: m.owned_by.to_string(),
                metadata,
            }
        })
        .collect()
}

fn to_openai_list(models: &[CatalogModel]) -> OpenAiModelList {
    OpenAiModelList {
        object: "list",
        data: to_openai_items(models),
    }
}

fn to_codex_response(models: &[CatalogModel]) -> CodexModelsResponse {
    let entries: Vec<serde_json::Value> = models
        .iter()
        // The `supported_reasoning_levels` marker distinguishes a full `ModelInfo` from the
        // static floor's minimal placeholders.
        .filter(|m| m.raw.get("supported_reasoning_levels").is_some())
        .map(|m| m.raw.clone())
        .collect();
    CodexModelsResponse {
        object: "list",
        models: entries,
        data: to_openai_items(models),
    }
}

#[derive(Deserialize)]
pub struct ModelsQuery {
    /// A real Codex CLI appends this; its presence selects the Codex catalog shape on `/v1/models`.
    client_version: Option<String>,
}

/// `GET /models` and `GET /backend-api/codex/models` — always the Codex catalog shape. Resolves the
/// exact active root fleet; an empty fleet serves the static floor without reusing a native ETag
/// cached for an account that is no longer active.
pub async fn codex_models_handler(State(state): State<Arc<AppState>>) -> Response {
    let account_ids = active_codex_account_ids(&state).await;
    let scoped = state
        .model_catalog
        .get_or_refresh_scoped(&account_ids)
        .await;
    let native_model_count = scoped.models.len();
    let native_etag = scoped.etag;
    let models = root_models_with_custom(&state, scoped.models, CatalogSurface::Codex).await;
    let etag = root_catalog_etag(native_etag, native_model_count, &models);
    codex_models_response(
        serde_json::to_value(to_codex_response(&build_catalog(&models)))
            .expect("catalog serializes"),
        etag,
    )
}

/// `GET /{pool}/models` — resolves the exact active Codex membership of `pool`, refreshes that
/// account-scoped cache on demand, and advertises only the model intersection supported by every
/// member. Multi-pool membership is read from `account_pool_memberships`, not the legacy primary
/// `accounts.pool` label.
pub async fn pooled_codex_models_handler(
    Path(pool): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Response {
    let account_repo = state.store.accounts();
    let accounts = match account_repo.list().await {
        Ok(accounts) => accounts,
        Err(error) => {
            tracing::warn!(
                pool = %pool,
                error = %error,
                "pooled model catalog: could not list accounts; serving floor fallback"
            );
            Vec::new()
        }
    };
    let mut account_ids = Vec::new();
    let mut membership_failed = false;
    for account in accounts
        .iter()
        .filter(|account| account.status == "active" && account.provider == "codex")
    {
        match account_repo.list_pools(&account.id).await {
            Ok(pools) if pools.iter().any(|membership| membership == &pool) => {
                account_ids.push(account.id.clone());
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(
                    pool = %pool,
                    account_id = %account.id,
                    error = %error,
                    "pooled model catalog: could not resolve account memberships"
                );
                membership_failed = true;
                break;
            }
        }
    }
    if membership_failed {
        account_ids.clear();
    }

    let scoped = state
        .model_catalog
        .get_or_refresh_scoped(&account_ids)
        .await;
    codex_models_response(
        serde_json::to_value(to_codex_response(&build_catalog(&scoped.models)))
            .expect("catalog serializes"),
        scoped.etag,
    )
}

/// Resolve the virtual model ETag for a named pool, warming the exact account scope on demand.
/// Every response transport uses this same resolver so a cold process never leaks one selected
/// account's native upstream ETag as the identity of the whole pool.
pub async fn pooled_models_etag(state: &AppState, pool: &str) -> Option<String> {
    let snapshots = state.account_cache.snapshots(&state.store).await.ok()?;
    let account_ids: Vec<String> =
        crate::snapshot::filter_by_provider_and_pool(&snapshots, Provider::Codex, Some(pool))
            .into_iter()
            .filter(|snapshot| snapshot.status == "active")
            .map(|snapshot| snapshot.id.to_string())
            .collect();
    state
        .model_catalog
        .get_or_refresh_scoped(&account_ids)
        .await
        .etag
}

/// Resolve the virtual model ETag for the active root Codex fleet, warming that exact scope on
/// demand. A failed/cold authoritative fetch returns `None`; callers must still remove any
/// account-native upstream ETag.
pub async fn root_models_etag(state: &AppState) -> Option<String> {
    let account_ids = active_codex_account_ids(state).await;
    let scoped = state
        .model_catalog
        .get_or_refresh_scoped(&account_ids)
        .await;
    let native_model_count = scoped.models.len();
    let native_etag = scoped.etag;
    let models = root_models_with_custom(state, scoped.models, CatalogSurface::Codex).await;
    root_catalog_etag(native_etag, native_model_count, &models)
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ModelScopeWarmup {
    pub attempted_scopes: usize,
    pub authoritative_scopes: usize,
}

/// Warm the exact active root fleet and every named pool before the listener begins accepting
/// Codex traffic. This moves account-authenticated model discovery off the first user request while
/// preserving the request-path on-demand resolver as a race/failure fallback.
pub async fn warm_active_model_scopes(state: &AppState) -> ModelScopeWarmup {
    let snapshots = match state.account_cache.snapshots(&state.store).await {
        Ok(snapshots) => snapshots,
        Err(error) => {
            tracing::warn!(error = %error, "model scope warmup: could not load account snapshots");
            return ModelScopeWarmup::default();
        }
    };
    let active = snapshots
        .iter()
        .filter(|snapshot| snapshot.status == "active" && snapshot.provider == Provider::Codex)
        .cloned()
        .collect::<Vec<_>>();
    if active.is_empty() {
        return ModelScopeWarmup::default();
    }

    let mut scopes = vec![active
        .iter()
        .map(|snapshot| snapshot.id.to_string())
        .collect::<Vec<_>>()];
    let mut pools: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for snapshot in &active {
        for pool in &snapshot.pools {
            pools
                .entry(pool.clone())
                .or_default()
                .push(snapshot.id.to_string());
        }
    }
    scopes.extend(pools.into_values());

    let attempted_scopes = scopes.len();
    let mut authoritative_scopes = 0;
    for account_ids in scopes {
        if state
            .model_catalog
            .get_or_refresh_scoped(&account_ids)
            .await
            .etag
            .is_some()
        {
            authoritative_scopes += 1;
        }
    }
    ModelScopeWarmup {
        attempted_scopes,
        authoritative_scopes,
    }
}

async fn active_codex_account_ids(state: &AppState) -> Vec<String> {
    let snapshots = state
        .account_cache
        .snapshots(&state.store)
        .await
        .unwrap_or_default();
    snapshots
        .iter()
        .filter(|snapshot| snapshot.status == "active" && snapshot.provider == Provider::Codex)
        .map(|snapshot| snapshot.id.to_string())
        .collect()
}

fn codex_models_response(body: serde_json::Value, etag: Option<String>) -> Response {
    let mut response = Json(body).into_response();
    if let Some(etag) = etag.and_then(|value| value.parse().ok()) {
        response.headers_mut().insert(ETAG, etag);
    }
    response
}

/// `GET /v1/models` — Codex catalog shape when `client_version` is present (a real Codex CLI),
/// else the OpenAI-style list. Same live-catalog read as `codex_models_handler` above.
pub async fn v1_models_handler(
    State(state): State<Arc<AppState>>,
    Query(q): Query<ModelsQuery>,
) -> Response {
    let account_ids = active_codex_account_ids(&state).await;
    let scoped = state
        .model_catalog
        .get_or_refresh_scoped(&account_ids)
        .await;
    let native_model_count = scoped.models.len();
    let native_etag = scoped.etag;
    let surface = if q.client_version.is_some() {
        CatalogSurface::Codex
    } else {
        CatalogSurface::OpenAi
    };
    let merged = root_models_with_custom(&state, scoped.models, surface).await;
    let etag = root_catalog_etag(native_etag, native_model_count, &merged);
    let models = build_catalog(&merged);
    if q.client_version.is_some() {
        codex_models_response(
            serde_json::to_value(to_codex_response(&models)).expect("catalog serializes"),
            etag,
        )
    } else {
        Json(to_openai_list(&models)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The pre-D15 test fixture: `build_catalog` fed exactly the static floor, reproducing the
    /// catalog shape callers saw before the live-cache wiring existed.
    fn floor_only_catalog() -> Vec<CatalogModel> {
        build_catalog(&codex_bootstrap_floor())
    }

    // --- carry-forward 1: the floor is never empty (the never-empty guarantee depends on it) ---

    #[test]
    fn codex_bootstrap_floor_is_never_empty() {
        let floor = codex_bootstrap_floor();
        assert!(
            !floor.is_empty(),
            "ModelCatalogCache's floor must never be empty"
        );
        assert_eq!(floor.len(), 5);
        assert!(floor.iter().any(|m| m.slug == "gpt-5.6-sol"));
    }

    #[test]
    fn catalog_contains_native_floor_without_translation_aliases() {
        let cat = floor_only_catalog();
        let ids: Vec<&str> = cat.iter().map(|m| m.id.as_str()).collect();
        assert!(ids.contains(&"gpt-5.6-sol"));
        assert!(ids.contains(&"gpt-5.5"));
        assert!(!ids.iter().any(|id| id.starts_with("claude-")));
        let unique: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(unique.len(), ids.len(), "catalog ids must be unique");
    }

    #[test]
    fn openai_list_shape_is_object_list_with_model_items() {
        let list = to_openai_list(&floor_only_catalog());
        assert_eq!(list.object, "list");
        let v = serde_json::to_value(&list).unwrap();
        assert_eq!(v["object"], "list");
        assert_eq!(v["data"][0]["object"], "model");
        assert!(!v["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["id"] == "claude-opus-4-1"));
    }

    #[test]
    fn codex_shape_has_models_and_data_arrays() {
        let resp = to_codex_response(&floor_only_catalog());
        let v = serde_json::to_value(&resp).unwrap();
        assert_eq!(v["object"], "list");
        assert!(v["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["id"] == "gpt-5.6-sol"));
        // Task 2: the static floor's `raw` entries are minimal placeholders (no
        // `supported_reasoning_levels` marker), so they're correctly OMITTED from the
        // codex-parseable `models` array — a partial `ModelInfo` would break codex's parse.
        // `data` (OpenAI shape) is unaffected and still lists every row.
        assert!(v["models"].as_array().unwrap().is_empty());
    }

    #[test]
    fn build_catalog_shapes_only_the_resolved_input_set() {
        let live = vec![
            UpstreamModel {
                slug: "gpt-5.5".to_string(),
                display_name: "GPT-5.5".to_string(),
                context_window: None,
                prefer_websockets: None,
                raw: serde_json::json!({"slug": "gpt-5.5", "display_name": "GPT-5.5"}),
            },
            // Upstream-only slug the floor doesn't have.
            UpstreamModel {
                slug: "gpt-5.7-nova".to_string(),
                display_name: "GPT-5.7 Nova".to_string(),
                context_window: Some(300_000),
                prefer_websockets: Some(true),
                raw: serde_json::json!({"slug": "gpt-5.7-nova", "display_name": "GPT-5.7 Nova"}),
            },
            // A genuinely upstream-provided slug is preserved even if it resembles a translation
            // alias; build_catalog itself never synthesizes one.
            UpstreamModel {
                slug: "claude-opus-4-1".to_string(),
                display_name: "Real Upstream Wins".to_string(),
                context_window: None,
                prefer_websockets: None,
                raw: serde_json::json!({"slug": "claude-opus-4-1", "display_name": "Real Upstream Wins"}),
            },
        ];
        let cat = build_catalog(&live);
        let ids: Vec<&str> = cat.iter().map(|m| m.id.as_str()).collect();
        assert!(ids.contains(&"gpt-5.5"), "live upstream slug present");
        assert!(
            ids.contains(&"gpt-5.7-nova"),
            "upstream-only live slug present"
        );
        assert!(!ids.contains(&"claude-sonnet-4-5"));
        assert!(!ids.contains(&"claude-haiku-4-5"));
        let unique: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(unique.len(), ids.len(), "catalog ids must stay unique");

        let opus = cat.iter().find(|m| m.id == "claude-opus-4-1").unwrap();
        assert_eq!(
            opus.owned_by, "openai",
            "a real upstream row remains provider-owned"
        );
        assert_eq!(opus.display_name, "Real Upstream Wins");
    }

    #[test]
    fn context_window_and_prefer_websockets_render_in_openai_shape_when_present() {
        // Task 2: the Codex `models` array now emits raw `ModelInfo` verbatim — PolyFlare's own
        // `context_window`/`prefer_websockets` convenience fields aren't synthesized into it
        // (a real `ModelInfo` carries its own equivalent data, if any). The OpenAI `data` shape's
        // rendering is unaffected and still carries them under `metadata`.
        let live = vec![UpstreamModel {
            slug: "gpt-5.7-nova".to_string(),
            display_name: "GPT-5.7 Nova".to_string(),
            context_window: Some(400_000),
            prefer_websockets: Some(true),
            raw: serde_json::json!({"slug": "gpt-5.7-nova", "display_name": "GPT-5.7 Nova"}),
        }];
        let cat = build_catalog(&live);

        let openai_v = serde_json::to_value(to_openai_list(&cat)).unwrap();
        let nova = openai_v["data"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["id"] == "gpt-5.7-nova")
            .unwrap();
        assert_eq!(nova["metadata"]["context_window"], 400_000);
        assert_eq!(nova["metadata"]["prefer_websockets"], true);
    }

    #[test]
    fn context_window_and_prefer_websockets_are_omitted_from_openai_metadata_when_unknown() {
        // The static floor doesn't know either field — both must be ABSENT (not `null`) from
        // `metadata` in the OpenAI shape, matching the `skip_serializing_if` convention.
        let cat = floor_only_catalog();
        let openai_v = serde_json::to_value(to_openai_list(&cat)).unwrap();
        let sol = openai_v["data"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["id"] == "gpt-5.6-sol")
            .unwrap();
        assert!(sol["metadata"].get("context_window").is_none());
        assert!(sol["metadata"].get("prefer_websockets").is_none());
    }

    // --- Task 2: emit raw `ModelInfo` verbatim in the Codex `/models` response ---

    fn full_raw_model_info(slug: &str, display_name: &str, effort: &str) -> serde_json::Value {
        serde_json::json!({
            "slug": slug,
            "display_name": display_name,
            "supported_reasoning_levels": [{"effort": effort, "description": "x"}],
            "visibility": "list",
            "supported_in_api": true,
            "priority": 1,
        })
    }

    #[test]
    fn codex_response_emits_raw_verbatim() {
        let raw = full_raw_model_info("gpt-5.6-sol", "Sol", "low");
        let live = vec![UpstreamModel {
            slug: "gpt-5.6-sol".to_string(),
            display_name: "Sol".to_string(),
            context_window: None,
            prefer_websockets: None,
            raw: raw.clone(),
        }];
        let resp = to_codex_response(&build_catalog(&live));
        let v = serde_json::to_value(resp).unwrap();
        let entry = v["models"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["slug"] == "gpt-5.6-sol")
            .expect("gpt-5.6-sol present in the codex models array");
        assert_eq!(
            entry, &raw,
            "raw ModelInfo must be emitted byte-for-byte, unmodified"
        );
    }

    #[test]
    fn translation_alias_is_not_cloned_into_codex_response() {
        let raw = full_raw_model_info("gpt-5.6-sol", "Sol", "high");
        let live = vec![UpstreamModel {
            slug: "gpt-5.6-sol".to_string(),
            display_name: "Sol".to_string(),
            context_window: None,
            prefer_websockets: None,
            raw: raw.clone(),
        }];
        let resp = to_codex_response(&build_catalog(&live));
        let v = serde_json::to_value(resp).unwrap();
        assert!(
            !v["models"]
                .as_array()
                .unwrap()
                .iter()
                .any(|m| m["slug"] == "claude-opus-4-1"),
            "Claude translation aliases must not appear in the Codex picker"
        );
        assert!(
            !v["data"]
                .as_array()
                .unwrap()
                .iter()
                .any(|m| m["id"] == "claude-opus-4-1"),
            "Claude translation aliases must not appear in the Codex data mirror"
        );
    }
}
