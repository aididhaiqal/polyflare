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
//! # This increment
//! The Codex side is a small hardcoded BOOTSTRAP FLOOR of real slugs; a follow-up fetches the live
//! catalog from `backend-api/codex/models` (subscription-OAuth-reachable) and merges it in. The
//! Claude side is synthetic-only by design: Anthropic's `/v1/models` needs an API key (a
//! subscription-OAuth Bearer isn't authorized for it), so the model list is the synthetic aliases.

use axum::extract::Query;
use axum::response::{IntoResponse, Json, Response};
use serde::{Deserialize, Serialize};

use crate::alias::synthetic_models;

/// A provider-agnostic catalog row before it's shaped for a response.
struct CatalogModel {
    id: String,
    display_name: String,
    /// `openai` for real Codex models, `polyflare` for synthetic aliases.
    owned_by: &'static str,
    /// Extra fields surfaced under `metadata` in the OpenAI shape (e.g. the alias target).
    metadata: serde_json::Map<String, serde_json::Value>,
}

/// The bootstrap floor of real Codex model slugs, served until the live upstream fetch lands (a
/// follow-up). Trimmed from codex-lb's `_BOOTSTRAP_STATIC_MODELS`; these are the alias targets plus
/// a couple of common base models so the catalog is never empty.
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
            metadata: serde_json::Map::new(),
        })
        .collect()
}

/// Build the merged catalog: real Codex models first, then synthetic aliases appended and
/// de-duplicated by id (real upstream wins on any id collision — mirrors codex-lb's merge order).
fn build_catalog() -> Vec<CatalogModel> {
    let mut models = codex_bootstrap();
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

/// One model in the Codex `/models` catalog shape. Minimal for the bootstrap increment; the live
/// fetch will flatten the verbatim upstream `raw` fields in here.
#[derive(Serialize)]
struct CodexModelEntry {
    slug: String,
    display_name: String,
    /// `list` (advertised) — bootstrap/hidden rows would use `hide`.
    visibility: &'static str,
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
        .map(|m| OpenAiModel {
            id: m.id.clone(),
            object: "model",
            created: 0,
            owned_by: m.owned_by.to_string(),
            metadata: m.metadata.clone(),
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

/// `GET /models` and `GET /backend-api/codex/models` — always the Codex catalog shape.
pub async fn codex_models_handler() -> Json<serde_json::Value> {
    Json(serde_json::to_value(to_codex_response(&build_catalog())).expect("catalog serializes"))
}

/// `GET /v1/models` — Codex catalog shape when `client_version` is present (a real Codex CLI),
/// else the OpenAI-style list.
pub async fn v1_models_handler(Query(q): Query<ModelsQuery>) -> Response {
    let models = build_catalog();
    if q.client_version.is_some() {
        Json(to_codex_response(&models)).into_response()
    } else {
        Json(to_openai_list(&models)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_merges_bootstrap_and_synthetic_without_collision() {
        let cat = build_catalog();
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
        let cat = build_catalog();
        let opus = cat.iter().find(|m| m.id == "claude-opus-4-1").unwrap();
        assert_eq!(opus.owned_by, "polyflare");
        assert_eq!(opus.metadata["aliased_to"], "gpt-5.6-sol");
        assert_eq!(opus.metadata["reasoning_effort"], "high");
    }

    #[test]
    fn openai_list_shape_is_object_list_with_model_items() {
        let list = to_openai_list(&build_catalog());
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
        let resp = to_codex_response(&build_catalog());
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
}
