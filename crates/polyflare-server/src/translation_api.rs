//! Authenticated management API for bidirectional protocol translation routes.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Json, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use polyflare_core::Provider;
use polyflare_store::{
    NewTranslationRoute, TranslatedRequestRow, TranslationRoute, TranslationRouteUpdate,
};
use serde::{Deserialize, Serialize};

use crate::app::AppState;

const MAX_NAME_LEN: usize = 128;
const MAX_MODEL_LEN: usize = 192;
const MIN_PRIORITY: i64 = -1_000_000;
const MAX_PRIORITY: i64 = 1_000_000;

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

fn route_id() -> String {
    format!("translation-{:032x}", rand::random::<u128>())
}

fn valid_model(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= MAX_MODEL_LEN && !value.chars().any(char::is_control)
}

fn valid_effort(value: Option<&str>) -> bool {
    value.is_none_or(|value| {
        matches!(
            value,
            "none" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max"
        )
    })
}

fn valid_protocol(value: &str) -> bool {
    matches!(value, "anthropic_messages" | "openai_responses")
}

fn builtin_wire_protocol(provider: &str) -> Option<&'static str> {
    match provider {
        "codex" => Some("openai_responses"),
        "anthropic" => Some("anthropic_messages"),
        _ => None,
    }
}

fn validate_shape(input: &RouteInput) -> Result<(), &'static str> {
    if input.name.trim().is_empty()
        || input.name.len() > MAX_NAME_LEN
        || input.name.chars().any(char::is_control)
    {
        return Err("name must be 1-128 printable characters");
    }
    if !valid_protocol(&input.source_protocol) {
        return Err("unsupported source protocol");
    }
    if !matches!(input.match_kind.as_str(), "exact" | "prefix" | "contains") {
        return Err("match_kind must be exact, prefix, or contains");
    }
    if !valid_model(&input.model_pattern) || !valid_model(&input.target_model) {
        return Err("model pattern and target model must be 1-192 printable characters");
    }
    if !matches!(
        input.target_kind.as_str(),
        "builtin_provider" | "custom_provider"
    ) || input.target_provider_id.trim().is_empty()
    {
        return Err("invalid target provider");
    }
    if !valid_effort(input.reasoning_effort.as_deref()) {
        return Err("unsupported reasoning effort");
    }
    if !(MIN_PRIORITY..=MAX_PRIORITY).contains(&input.priority) {
        return Err("priority is outside the supported range");
    }
    Ok(())
}

async fn validate_target(state: &AppState, input: &RouteInput) -> Result<(), &'static str> {
    let target_protocol = match input.target_kind.as_str() {
        "builtin_provider" => builtin_wire_protocol(&input.target_provider_id)
            .ok_or("unknown built-in target provider")?
            .to_string(),
        "custom_provider" => {
            state
                .store
                .providers()
                .get_provider(&input.target_provider_id)
                .await
                .map_err(|_| "target provider lookup failed")?
                .ok_or("custom target provider does not exist")?
                .wire_api
        }
        _ => return Err("invalid target provider"),
    };
    if input.source_protocol == target_protocol {
        return Err("same-protocol traffic must use normal model routing");
    }
    if input.reasoning_effort.is_some() && target_protocol != "openai_responses" {
        return Err("reasoning effort is only valid for Responses targets");
    }
    Ok(())
}

fn bad_request(message: &'static str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({ "error": message })),
    )
        .into_response()
}

#[derive(Debug, Clone, Deserialize)]
pub struct RouteInput {
    name: String,
    #[serde(default = "default_true")]
    enabled: bool,
    source_protocol: String,
    match_kind: String,
    model_pattern: String,
    target_kind: String,
    target_provider_id: String,
    target_model: String,
    reasoning_effort: Option<String>,
    priority: i64,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize)]
pub struct RouteView {
    id: String,
    name: String,
    enabled: bool,
    source_protocol: String,
    match_kind: String,
    model_pattern: String,
    target_kind: String,
    target_provider_id: String,
    target_model: String,
    reasoning_effort: Option<String>,
    priority: i64,
    created_at: i64,
    updated_at: i64,
}

impl From<TranslationRoute> for RouteView {
    fn from(route: TranslationRoute) -> Self {
        Self {
            id: route.id,
            name: route.name,
            enabled: route.enabled,
            source_protocol: route.source_protocol,
            match_kind: route.match_kind,
            model_pattern: route.model_pattern,
            target_kind: route.target_kind,
            target_provider_id: route.target_provider_id,
            target_model: route.target_model,
            reasoning_effort: route.reasoning_effort,
            priority: route.priority,
            created_at: route.created_at,
            updated_at: route.updated_at,
        }
    }
}

#[derive(Serialize)]
struct TranslatedRequestView {
    requested_at: i64,
    request_id: Option<String>,
    path: String,
    provider: String,
    status: i64,
    model: Option<String>,
    reasoning_effort: Option<String>,
    duration_ms: i64,
}

impl From<TranslatedRequestRow> for TranslatedRequestView {
    fn from(row: TranslatedRequestRow) -> Self {
        Self {
            requested_at: row.requested_at,
            request_id: row.request_id,
            path: row.path,
            provider: row.provider,
            status: row.status,
            model: row.model,
            reasoning_effort: row.reasoning_effort,
            duration_ms: row.duration_ms,
        }
    }
}

/// Whether a route's target can actually serve a TRANSLATED request right now.
///
/// A route can be enabled, well-formed, and matching, and still have nothing able to serve it: a
/// subscription-OAuth grant authorizes one first-party client shape, so those accounts are excluded
/// from translated traffic during selection and such a request fails with "no eligible account".
/// That rule is invisible in the route definition, so the API states it per target.
#[derive(Debug, Clone, Copy, Serialize)]
struct TargetCapacityView {
    /// Accounts (built-in target) or enabled credentials (custom target) able to serve translated
    /// traffic for this target.
    eligible: i64,
    /// Accounts that belong to this target but may serve only native client traffic. Non-zero here
    /// with `eligible == 0` is the trap: the route looks healthy and cannot run.
    barred_subscription: i64,
}

/// Serviceability of one route target, keyed `"{target_kind}:{target_provider_id}"`.
///
/// Returned as a map rather than a field on each route so a target shared by several routes is
/// computed once, and so `RouteView`'s pure `From<TranslationRoute>` conversion stays free of
/// async state lookups.
type TargetCapacityMap = HashMap<String, TargetCapacityView>;

fn target_key(target_kind: &str, target_provider_id: &str) -> String {
    format!("{target_kind}:{target_provider_id}")
}

/// Count what can serve translated traffic for one target.
///
/// Built-in targets resolve to accounts and honor the subscription-OAuth exclusion. Custom targets
/// resolve to provider credentials, which are API keys — never subscription grants — so nothing is
/// ever barred there.
async fn target_capacity(
    state: &AppState,
    target_kind: &str,
    target_provider_id: &str,
) -> TargetCapacityView {
    match target_kind {
        "builtin_provider" => {
            let Ok(provider) = Provider::from_str(target_provider_id) else {
                return TargetCapacityView {
                    eligible: 0,
                    barred_subscription: 0,
                };
            };
            let Ok(snapshots) = state.account_cache.snapshots(&state.store).await else {
                return TargetCapacityView {
                    eligible: 0,
                    barred_subscription: 0,
                };
            };
            let mine = snapshots.iter().filter(|s| s.provider == provider);
            let (mut eligible, mut barred) = (0, 0);
            for snapshot in mine {
                if snapshot.subscription_oauth {
                    barred += 1;
                } else {
                    eligible += 1;
                }
            }
            TargetCapacityView {
                eligible,
                barred_subscription: barred,
            }
        }
        "custom_provider" => {
            let eligible = state
                .store
                .providers()
                .list_credentials(target_provider_id)
                .await
                .map(|credentials| credentials.iter().filter(|c| c.enabled).count() as i64)
                .unwrap_or(0);
            TargetCapacityView {
                eligible,
                barred_subscription: 0,
            }
        }
        _ => TargetCapacityView {
            eligible: 0,
            barred_subscription: 0,
        },
    }
}

/// Capacity for every distinct target among `routes`, computed once per target.
async fn capacity_for_routes(state: &AppState, routes: &[RouteView]) -> TargetCapacityMap {
    let mut out = TargetCapacityMap::new();
    for route in routes {
        let key = target_key(&route.target_kind, &route.target_provider_id);
        if out.contains_key(&key) {
            continue;
        }
        let capacity = target_capacity(state, &route.target_kind, &route.target_provider_id).await;
        out.insert(key, capacity);
    }
    out
}

#[derive(Serialize)]
struct RoutesView {
    routes: Vec<RouteView>,
    recent_requests: Vec<TranslatedRequestView>,
    target_capacity: TargetCapacityMap,
}

pub async fn list(State(state): State<Arc<AppState>>) -> Response {
    let routes: Vec<RouteView> = match state.store.translations().list().await {
        Ok(routes) => routes.into_iter().map(RouteView::from).collect(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let recent_requests = match state.store.request_log().recent_translated(8).await {
        Ok(rows) => rows.into_iter().map(TranslatedRequestView::from).collect(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let target_capacity = capacity_for_routes(&state, &routes).await;
    Json(RoutesView {
        routes,
        recent_requests,
        target_capacity,
    })
    .into_response()
}

pub async fn create(State(state): State<Arc<AppState>>, Json(input): Json<RouteInput>) -> Response {
    if let Err(message) = validate_shape(&input) {
        return bad_request(message);
    }
    if let Err(message) = validate_target(&state, &input).await {
        return bad_request(message);
    }
    let timestamp = now();
    let route = NewTranslationRoute {
        id: route_id(),
        name: input.name.trim().into(),
        enabled: input.enabled,
        source_protocol: input.source_protocol,
        match_kind: input.match_kind,
        model_pattern: input.model_pattern.trim().into(),
        target_kind: input.target_kind,
        target_provider_id: input.target_provider_id,
        target_model: input.target_model.trim().into(),
        reasoning_effort: input.reasoning_effort,
        priority: input.priority,
        created_at: timestamp,
    };
    match state.store.translations().create(&route).await {
        Ok(()) => match state.store.translations().get(&route.id).await {
            Ok(Some(route)) => (StatusCode::CREATED, Json(RouteView::from(route))).into_response(),
            _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        },
        Err(_) => StatusCode::CONFLICT.into_response(),
    }
}

pub async fn update(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(input): Json<RouteInput>,
) -> Response {
    if let Err(message) = validate_shape(&input) {
        return bad_request(message);
    }
    if let Err(message) = validate_target(&state, &input).await {
        return bad_request(message);
    }
    let update = TranslationRouteUpdate {
        name: input.name.trim().into(),
        enabled: input.enabled,
        source_protocol: input.source_protocol,
        match_kind: input.match_kind,
        model_pattern: input.model_pattern.trim().into(),
        target_kind: input.target_kind,
        target_provider_id: input.target_provider_id,
        target_model: input.target_model.trim().into(),
        reasoning_effort: input.reasoning_effort,
        priority: input.priority,
        updated_at: now(),
    };
    match state.store.translations().update(&id, &update).await {
        Ok(true) => match state.store.translations().get(&id).await {
            Ok(Some(route)) => Json(RouteView::from(route)).into_response(),
            _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        },
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::CONFLICT.into_response(),
    }
}

pub async fn delete(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    match state.store.translations().delete(&id).await {
        Ok(true) => Json(serde_json::json!({ "ok": true })).into_response(),
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[derive(Deserialize)]
pub struct TestInput {
    source_protocol: String,
    model: String,
}

#[derive(Serialize)]
struct TestView {
    matched: bool,
    route: Option<RouteView>,
    /// Serviceability of the matched route's target. `None` when nothing matched — in which case
    /// the request is NOT translated at all: a native client request falls through to the
    /// same-protocol path, where a real Claude client is forwarded byte-for-byte.
    target_capacity: Option<TargetCapacityView>,
}

pub async fn test_match(
    State(state): State<Arc<AppState>>,
    Json(input): Json<TestInput>,
) -> Response {
    if !valid_protocol(&input.source_protocol) || !valid_model(&input.model) {
        return bad_request("invalid source protocol or model");
    }
    match state
        .store
        .translations()
        .resolve(&input.source_protocol, input.model.trim())
        .await
    {
        Ok(route) => {
            let route = route.map(RouteView::from);
            let capacity = match route.as_ref() {
                Some(route) => Some(
                    target_capacity(&state, &route.target_kind, &route.target_provider_id).await,
                ),
                None => None,
            };
            Json(TestView {
                matched: route.is_some(),
                route,
                target_capacity: capacity,
            })
            .into_response()
        }
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}
