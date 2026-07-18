//! Model catalog: serves the list of available models — real Codex models MERGED with PolyFlare's
//! synthetic alias models (`claude-*` -> Codex targets, from `crate::alias`).
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
//! empty. The Claude side remains synthetic-only by design: Anthropic's `/v1/models` needs an API
//! key (a subscription-OAuth Bearer isn't authorized for it), so the model list is the synthetic
//! aliases.

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::response::{IntoResponse, Json, Response};
use serde::{Deserialize, Serialize};

use crate::alias::synthetic_models;
use crate::app::AppState;
use crate::model_catalog::UpstreamModel;

/// A provider-agnostic catalog row before it's shaped for a response.
struct CatalogModel {
    id: String,
    display_name: String,
    /// `openai` for real Codex models, `polyflare` for synthetic aliases.
    owned_by: &'static str,
    /// Context window size in tokens, when known (carried through from a live-upstream
    /// [`UpstreamModel`]; `None` for the static floor / synthetic aliases, which don't know it).
    /// Rendered in both response shapes when present — see `to_codex_response`/`to_openai_items`.
    context_window: Option<u64>,
    /// Whether this model prefers the WebSocket transport, when known. Same provenance/rendering
    /// as `context_window` above.
    prefer_websockets: Option<bool>,
    /// Extra fields surfaced under `metadata` in the OpenAI shape (e.g. the alias target,
    /// `context_window`, `prefer_websockets` — see `to_openai_items`).
    metadata: serde_json::Map<String, serde_json::Value>,
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
    }
}

/// Build the merged catalog: `live_models` (the codex model set — `AppState.model_catalog`'s
/// `cached_or_fallback()`, which is itself ALREADY the live-upstream-onto-static-floor merge and
/// therefore never empty) first, then synthetic aliases appended and de-duplicated by id (real
/// upstream wins on any id collision — mirrors codex-lb's merge order). Pure function of its input
/// so it's independently unit-testable without a real `AppState`/cache.
fn build_catalog(live_models: &[UpstreamModel]) -> Vec<CatalogModel> {
    let mut models: Vec<CatalogModel> = live_models
        .iter()
        .map(catalog_model_from_upstream)
        .collect();
    let seen: std::collections::HashSet<String> = models.iter().map(|m| m.id.clone()).collect();
    for s in synthetic_models() {
        if seen.contains(s.id) {
            continue;
        }
        let mut metadata = serde_json::Map::new();
        metadata.insert(
            "aliased_to".to_string(),
            serde_json::Value::String(s.alias.target_model.clone()),
        );
        if let Some(effort) = &s.alias.reasoning_effort {
            metadata.insert(
                "reasoning_effort".to_string(),
                serde_json::Value::String(effort.clone()),
            );
        }
        models.push(CatalogModel {
            id: s.id.to_string(),
            display_name: s.display_name.to_string(),
            owned_by: "polyflare",
            context_window: None,
            prefer_websockets: None,
            metadata,
        });
    }
    models
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

/// One model in the Codex `/models` catalog shape.
#[derive(Serialize)]
struct CodexModelEntry {
    slug: String,
    display_name: String,
    /// `list` (advertised) — bootstrap/hidden rows would use `hide`.
    visibility: &'static str,
    /// Context window in tokens, when the live upstream fetch supplied one. Omitted entirely
    /// (rather than rendered as `null`) when unknown — matches `metadata`'s
    /// `skip_serializing_if` convention on the OpenAI shape below.
    #[serde(skip_serializing_if = "Option::is_none")]
    context_window: Option<u64>,
    /// Whether this model prefers the WebSocket transport, when known. Same omit-if-unknown
    /// convention as `context_window` above.
    #[serde(skip_serializing_if = "Option::is_none")]
    prefer_websockets: Option<bool>,
}

#[derive(Serialize)]
struct CodexModelsResponse {
    object: &'static str,
    models: Vec<CodexModelEntry>,
    /// Mirror of `models` in OpenAI-item form, for clients that read `data`.
    data: Vec<OpenAiModel>,
}

fn to_openai_items(models: &[CatalogModel]) -> Vec<OpenAiModel> {
    models
        .iter()
        .map(|m| {
            // `context_window`/`prefer_websockets` join whatever alias metadata is already
            // present (e.g. `aliased_to`/`reasoning_effort`) — real Codex rows normally start
            // from an empty map, synthetic rows don't carry either field, so there's no
            // collision between the two provenances.
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
    let entries = models
        .iter()
        .map(|m| CodexModelEntry {
            slug: m.id.clone(),
            display_name: m.display_name.clone(),
            visibility: "list",
            context_window: m.context_window,
            prefer_websockets: m.prefer_websockets,
        })
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

/// `GET /models` and `GET /backend-api/codex/models` — always the Codex catalog shape. Reads the
/// live-or-floor codex model set off `AppState.model_catalog` (D15 Task 3) instead of only the
/// static bootstrap — `cached_or_fallback()` is sync/zero-I/O and never empty, so this never
/// blocks and never serves a broken/empty catalog.
pub async fn codex_models_handler(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let live = state.model_catalog.cached_or_fallback();
    Json(
        serde_json::to_value(to_codex_response(&build_catalog(&live))).expect("catalog serializes"),
    )
}

/// `GET /v1/models` — Codex catalog shape when `client_version` is present (a real Codex CLI),
/// else the OpenAI-style list. Same live-catalog read as `codex_models_handler` above.
pub async fn v1_models_handler(
    State(state): State<Arc<AppState>>,
    Query(q): Query<ModelsQuery>,
) -> Response {
    let live = state.model_catalog.cached_or_fallback();
    let models = build_catalog(&live);
    if q.client_version.is_some() {
        Json(to_codex_response(&models)).into_response()
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
    fn catalog_merges_bootstrap_and_synthetic_without_collision() {
        let cat = floor_only_catalog();
        let ids: Vec<&str> = cat.iter().map(|m| m.id.as_str()).collect();
        // Real Codex slugs present.
        assert!(ids.contains(&"gpt-5.6-sol"));
        assert!(ids.contains(&"gpt-5.5"));
        // Synthetic Claude aliases present.
        assert!(ids.contains(&"claude-opus-4-1"));
        assert!(ids.contains(&"claude-sonnet-4-5"));
        assert!(ids.contains(&"claude-haiku-4-5"));
        // No duplicate ids.
        let unique: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(unique.len(), ids.len(), "catalog ids must be unique");
    }

    #[test]
    fn synthetic_rows_carry_alias_target_metadata() {
        let cat = floor_only_catalog();
        let opus = cat.iter().find(|m| m.id == "claude-opus-4-1").unwrap();
        assert_eq!(opus.owned_by, "polyflare");
        assert_eq!(opus.metadata["aliased_to"], "gpt-5.6-sol");
        assert_eq!(opus.metadata["reasoning_effort"], "high");
    }

    #[test]
    fn openai_list_shape_is_object_list_with_model_items() {
        let list = to_openai_list(&floor_only_catalog());
        assert_eq!(list.object, "list");
        let v = serde_json::to_value(&list).unwrap();
        assert_eq!(v["object"], "list");
        assert_eq!(v["data"][0]["object"], "model");
        // A synthetic row round-trips its metadata.
        let has_alias =
            v["data"].as_array().unwrap().iter().any(|m| {
                m["id"] == "claude-opus-4-1" && m["metadata"]["aliased_to"] == "gpt-5.6-sol"
            });
        assert!(has_alias, "synthetic alias metadata must serialize: {v}");
    }

    #[test]
    fn codex_shape_has_models_and_data_arrays() {
        let resp = to_codex_response(&floor_only_catalog());
        let v = serde_json::to_value(&resp).unwrap();
        assert_eq!(v["object"], "list");
        assert!(v["models"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["slug"] == "gpt-5.6-sol"));
        assert!(v["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["id"] == "gpt-5.6-sol"));
        assert_eq!(v["models"][0]["visibility"], "list");
    }

    // --- D15 Task 3 (d): the synthetic-alias merge still applies OVER a live/cached upstream set,
    // and real-upstream-wins-over-synthetic-alias is preserved when a live slug collides with an
    // alias id. ---

    #[test]
    fn build_catalog_applies_synthetic_alias_merge_over_live_upstream_set() {
        let live = vec![
            UpstreamModel {
                slug: "gpt-5.5".to_string(),
                display_name: "GPT-5.5".to_string(),
                context_window: None,
                prefer_websockets: None,
            },
            // Upstream-only slug the floor doesn't have.
            UpstreamModel {
                slug: "gpt-5.7-nova".to_string(),
                display_name: "GPT-5.7 Nova".to_string(),
                context_window: Some(300_000),
                prefer_websockets: Some(true),
            },
            // Collides with a synthetic alias id — live/real wins, matching the pre-existing
            // real-wins-over-synthetic-alias contract.
            UpstreamModel {
                slug: "claude-opus-4-1".to_string(),
                display_name: "Real Upstream Wins".to_string(),
                context_window: None,
                prefer_websockets: None,
            },
        ];
        let cat = build_catalog(&live);
        let ids: Vec<&str> = cat.iter().map(|m| m.id.as_str()).collect();
        assert!(ids.contains(&"gpt-5.5"), "live upstream slug present");
        assert!(
            ids.contains(&"gpt-5.7-nova"),
            "upstream-only live slug present"
        );
        // Every synthetic alias EXCEPT the colliding one is still appended.
        assert!(ids.contains(&"claude-sonnet-4-5"));
        assert!(ids.contains(&"claude-haiku-4-5"));
        let unique: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(unique.len(), ids.len(), "catalog ids must stay unique");

        let opus = cat.iter().find(|m| m.id == "claude-opus-4-1").unwrap();
        assert_eq!(
            opus.owned_by, "openai",
            "live upstream wins over the synthetic alias on id collision"
        );
        assert_eq!(opus.display_name, "Real Upstream Wins");
    }

    #[test]
    fn context_window_and_prefer_websockets_render_in_both_response_shapes_when_present() {
        let live = vec![UpstreamModel {
            slug: "gpt-5.7-nova".to_string(),
            display_name: "GPT-5.7 Nova".to_string(),
            context_window: Some(400_000),
            prefer_websockets: Some(true),
        }];
        let cat = build_catalog(&live);

        let codex_v = serde_json::to_value(to_codex_response(&cat)).unwrap();
        let nova = codex_v["models"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["slug"] == "gpt-5.7-nova")
            .unwrap();
        assert_eq!(nova["context_window"], 400_000);
        assert_eq!(nova["prefer_websockets"], true);

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
    fn context_window_and_prefer_websockets_are_omitted_when_unknown() {
        // The static floor doesn't know either field — both must be ABSENT (not `null`) in the
        // Codex shape, matching the `skip_serializing_if` convention.
        let cat = floor_only_catalog();
        let codex_v = serde_json::to_value(to_codex_response(&cat)).unwrap();
        let sol = codex_v["models"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["slug"] == "gpt-5.6-sol")
            .unwrap();
        assert!(sol.get("context_window").is_none());
        assert!(sol.get("prefer_websockets").is_none());
    }
}
