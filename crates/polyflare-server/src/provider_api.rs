//! Authenticated management API for generic Responses-compatible providers.
//!
//! Credential secrets are write-only: response views contain stable ids, labels, health, and
//! routing policy, never ciphertext or plaintext API keys.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::{Json, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use polyflare_store::{
    CustomProvider, NewCustomProvider, NewProviderModel, ProviderCredential, ProviderModel,
};
use serde::{Deserialize, Serialize};

use crate::app::AppState;

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

fn id(prefix: &str) -> String {
    format!("{prefix}-{:032x}", rand::random::<u128>())
}

fn ok() -> Response {
    Json(serde_json::json!({ "ok": true })).into_response()
}

fn valid_slug(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 96
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn valid_label(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 128
}

fn validate_base_url(base_url: &str, allow_private_hosts: bool) -> bool {
    let Ok(url) = reqwest::Url::parse(base_url) else {
        return false;
    };
    url.username().is_empty()
        && url.password().is_none()
        && url.query().is_none()
        && url.fragment().is_none()
        && (url.scheme() == "https" || (allow_private_hosts && url.scheme() == "http"))
        && url.host_str().is_some()
}

#[derive(Serialize)]
pub struct ProviderView {
    id: String,
    slug: String,
    display_name: String,
    base_url: String,
    wire_api: String,
    enabled: bool,
    stateless_responses: bool,
    allow_private_hosts: bool,
    connect_timeout_ms: i64,
    stream_idle_timeout_ms: i64,
    request_max_retries: i64,
    max_concurrency: Option<i64>,
    credentials: Vec<CredentialView>,
    models: Vec<ModelView>,
}

#[derive(Serialize)]
pub struct CredentialView {
    id: String,
    provider_id: String,
    label: String,
    enabled: bool,
    health_status: String,
    routing_weight: f64,
    max_concurrency: Option<i64>,
    cooldown_until: Option<i64>,
    last_error_at: Option<i64>,
}

#[derive(Serialize)]
pub struct ModelView {
    id: String,
    provider_id: String,
    public_model: String,
    upstream_model: String,
    display_name: String,
    context_window: Option<i64>,
    max_output_tokens: Option<i64>,
    supports_tools: bool,
    supports_vision: bool,
    supports_parallel_tool_calls: bool,
    supports_web_search: bool,
    supports_reasoning_summaries: bool,
    reasoning_levels: Vec<String>,
    input_per_million: Option<f64>,
    cached_input_per_million: Option<f64>,
    output_per_million: Option<f64>,
    visible_in_codex: bool,
    visible_in_openai: bool,
    enabled: bool,
}

impl From<ProviderCredential> for CredentialView {
    fn from(value: ProviderCredential) -> Self {
        Self {
            id: value.id,
            provider_id: value.provider_id,
            label: value.label,
            enabled: value.enabled,
            health_status: value.health_status,
            routing_weight: value.routing_weight,
            max_concurrency: value.max_concurrency,
            cooldown_until: value.cooldown_until,
            last_error_at: value.last_error_at,
        }
    }
}

impl From<ProviderModel> for ModelView {
    fn from(value: ProviderModel) -> Self {
        Self {
            id: value.id,
            provider_id: value.provider_id,
            public_model: value.public_model,
            upstream_model: value.upstream_model,
            display_name: value.display_name,
            context_window: value.context_window,
            max_output_tokens: value.max_output_tokens,
            supports_tools: value.supports_tools,
            supports_vision: value.supports_vision,
            supports_parallel_tool_calls: value.supports_parallel_tool_calls,
            supports_web_search: value.supports_web_search,
            supports_reasoning_summaries: value.supports_reasoning_summaries,
            reasoning_levels: serde_json::from_str(&value.reasoning_levels_json)
                .unwrap_or_default(),
            input_per_million: value.input_per_million,
            cached_input_per_million: value.cached_input_per_million,
            output_per_million: value.output_per_million,
            visible_in_codex: value.visible_in_codex,
            visible_in_openai: value.visible_in_openai,
            enabled: value.enabled,
        }
    }
}

async fn view(state: &AppState, provider: CustomProvider) -> Result<ProviderView, Response> {
    let credentials = state
        .store
        .providers()
        .list_credentials(&provider.id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())?;
    let models = state
        .store
        .providers()
        .list_models(&provider.id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())?;
    Ok(ProviderView {
        id: provider.id,
        slug: provider.slug,
        display_name: provider.display_name,
        base_url: provider.base_url,
        wire_api: provider.wire_api,
        enabled: provider.enabled,
        stateless_responses: provider.stateless_responses,
        allow_private_hosts: provider.allow_private_hosts,
        connect_timeout_ms: provider.connect_timeout_ms,
        stream_idle_timeout_ms: provider.stream_idle_timeout_ms,
        request_max_retries: provider.request_max_retries,
        max_concurrency: provider.max_concurrency,
        credentials: credentials.into_iter().map(Into::into).collect(),
        models: models.into_iter().map(Into::into).collect(),
    })
}

pub async fn list(State(state): State<Arc<AppState>>) -> Response {
    let providers = match state.store.providers().list_providers().await {
        Ok(providers) => providers,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let mut result = Vec::with_capacity(providers.len());
    for provider in providers {
        match view(&state, provider).await {
            Ok(provider) => result.push(provider),
            Err(response) => return response,
        }
    }
    Json(result).into_response()
}

#[derive(Deserialize)]
pub struct CreateProvider {
    slug: String,
    display_name: String,
    base_url: String,
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    stateless_responses: Option<bool>,
    #[serde(default)]
    allow_private_hosts: bool,
    #[serde(default)]
    connect_timeout_ms: Option<i64>,
    #[serde(default)]
    stream_idle_timeout_ms: Option<i64>,
    #[serde(default)]
    request_max_retries: Option<i64>,
    max_concurrency: Option<i64>,
}

pub async fn create(
    State(state): State<Arc<AppState>>,
    Json(input): Json<CreateProvider>,
) -> Response {
    if !valid_slug(&input.slug)
        || !valid_label(&input.display_name)
        || !validate_base_url(&input.base_url, input.allow_private_hosts)
    {
        return (StatusCode::BAD_REQUEST, "invalid provider configuration").into_response();
    }
    let timestamp = now();
    let provider = NewCustomProvider {
        id: id("provider"),
        slug: input.slug,
        display_name: input.display_name.trim().to_string(),
        base_url: input.base_url.trim_end_matches('/').to_string(),
        wire_api: "responses".into(),
        enabled: input.enabled.unwrap_or(true),
        stateless_responses: input.stateless_responses.unwrap_or(true),
        allow_private_hosts: input.allow_private_hosts,
        connect_timeout_ms: input
            .connect_timeout_ms
            .unwrap_or(10_000)
            .clamp(100, 120_000),
        stream_idle_timeout_ms: input
            .stream_idle_timeout_ms
            .unwrap_or(300_000)
            .clamp(1_000, 7_200_000),
        request_max_retries: input.request_max_retries.unwrap_or(0).clamp(0, 10),
        max_concurrency: input.max_concurrency.filter(|value| *value > 0),
        created_at: timestamp,
    };
    if state
        .store
        .providers()
        .create_provider(&provider)
        .await
        .is_err()
    {
        return (StatusCode::CONFLICT, "provider slug already exists").into_response();
    }
    let Some(created) = state
        .store
        .providers()
        .get_provider(&provider.id)
        .await
        .ok()
        .flatten()
    else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    match view(&state, created).await {
        Ok(provider) => (StatusCode::CREATED, Json(provider)).into_response(),
        Err(response) => response,
    }
}

#[derive(Deserialize)]
pub struct EnabledPatch {
    enabled: bool,
}

pub async fn patch_provider(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(input): Json<EnabledPatch>,
) -> Response {
    match state
        .store
        .providers()
        .set_provider_enabled(&id, input.enabled, now())
        .await
    {
        Ok(true) => ok(),
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

pub async fn delete_provider(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    match state.store.providers().delete_provider(&id).await {
        Ok(true) => ok(),
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

pub async fn test_provider(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    let Some(provider) = state
        .store
        .providers()
        .get_provider(&id)
        .await
        .ok()
        .flatten()
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(model) = state
        .store
        .providers()
        .list_models(&id)
        .await
        .ok()
        .and_then(|models| models.into_iter().find(|model| model.enabled))
    else {
        return (StatusCode::BAD_REQUEST, "provider has no enabled model").into_response();
    };
    let body = Bytes::from(
        serde_json::to_vec(&serde_json::json!({
            "model": model.public_model,
            "input": "Respond with OK.",
            "max_output_tokens": 1,
            "stream": true
        }))
        .expect("fixed provider test body serializes"),
    );
    let started = std::time::Instant::now();
    let (response, outcome) = crate::custom_provider::execute(
        &state.store,
        &state.cipher,
        provider,
        model,
        &HeaderMap::new(),
        &body,
    )
    .await;
    let upstream_status = response.status().as_u16();
    drop(response);
    let payload = serde_json::json!({
        "ok": (200..300).contains(&upstream_status),
        "upstream_status": upstream_status,
        "provider": outcome.provider_slug,
        "model": outcome.public_model,
        "credential_id": outcome.credential_id,
        "latency_ms": started.elapsed().as_millis() as u64,
    });
    if (200..300).contains(&upstream_status) {
        Json(payload).into_response()
    } else {
        (StatusCode::BAD_GATEWAY, Json(payload)).into_response()
    }
}

#[derive(Deserialize)]
pub struct CreateCredential {
    label: String,
    api_key: String,
    #[serde(default)]
    routing_weight: Option<f64>,
    max_concurrency: Option<i64>,
}

pub async fn create_credential(
    State(state): State<Arc<AppState>>,
    Path(provider_id): Path<String>,
    Json(input): Json<CreateCredential>,
) -> Response {
    if !valid_label(&input.label)
        || input.api_key.is_empty()
        || input.api_key.len() > 16_384
        || input
            .routing_weight
            .is_some_and(|weight| !weight.is_finite() || weight <= 0.0)
    {
        return (StatusCode::BAD_REQUEST, "invalid credential").into_response();
    }
    if !matches!(
        state.store.providers().get_provider(&provider_id).await,
        Ok(Some(_))
    ) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let credential_id = id("credential");
    match state
        .store
        .providers()
        .create_credential(
            &credential_id,
            &provider_id,
            input.label.trim(),
            &input.api_key,
            input.routing_weight.unwrap_or(1.0),
            input.max_concurrency.filter(|value| *value > 0),
            now(),
            &state.cipher,
        )
        .await
    {
        Ok(()) => {
            let credential = state
                .store
                .providers()
                .list_credentials(&provider_id)
                .await
                .ok()
                .and_then(|rows| rows.into_iter().find(|row| row.id == credential_id));
            match credential {
                Some(credential) => {
                    (StatusCode::CREATED, Json(CredentialView::from(credential))).into_response()
                }
                None => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            }
        }
        Err(_) => (StatusCode::CONFLICT, "credential label already exists").into_response(),
    }
}

pub async fn patch_credential(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(input): Json<EnabledPatch>,
) -> Response {
    match state
        .store
        .providers()
        .set_credential_enabled(&id, input.enabled, now())
        .await
    {
        Ok(true) => ok(),
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

pub async fn delete_credential(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    match state.store.providers().delete_credential(&id).await {
        Ok(true) => ok(),
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[derive(Deserialize)]
pub struct CreateModel {
    public_model: String,
    upstream_model: String,
    display_name: String,
    context_window: Option<i64>,
    max_output_tokens: Option<i64>,
    #[serde(default = "default_true")]
    supports_tools: bool,
    #[serde(default)]
    supports_vision: bool,
    #[serde(default = "default_true")]
    supports_parallel_tool_calls: bool,
    #[serde(default)]
    supports_web_search: bool,
    #[serde(default)]
    supports_reasoning_summaries: bool,
    #[serde(default)]
    reasoning_levels: Vec<String>,
    model_info: Option<serde_json::Value>,
    input_per_million: Option<f64>,
    cached_input_per_million: Option<f64>,
    output_per_million: Option<f64>,
    #[serde(default = "default_true")]
    visible_in_codex: bool,
    #[serde(default = "default_true")]
    visible_in_openai: bool,
}

fn default_true() -> bool {
    true
}

pub async fn create_model(
    State(state): State<Arc<AppState>>,
    Path(provider_id): Path<String>,
    Json(input): Json<CreateModel>,
) -> Response {
    let valid_prices = [
        input.input_per_million,
        input.cached_input_per_million,
        input.output_per_million,
    ]
    .into_iter()
    .flatten()
    .all(|price| price.is_finite() && price >= 0.0);
    if !valid_slug(&input.public_model)
        || !valid_slug(&input.upstream_model)
        || !valid_label(&input.display_name)
        || input.context_window.is_some_and(|value| value <= 0)
        || input.max_output_tokens.is_some_and(|value| value <= 0)
        || !valid_prices
        || input.reasoning_levels.iter().any(|level| {
            !matches!(
                level.as_str(),
                "none" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max"
            )
        })
        || input.model_info.as_ref().is_some_and(|value| {
            value.to_string().len() > 64 * 1024
                || !crate::catalog::safe_codex_model_info_extensions(value)
        })
        || crate::catalog::model_slug_is_reserved(&state, &input.public_model)
    {
        return (StatusCode::BAD_REQUEST, "invalid or reserved model").into_response();
    }
    if !matches!(
        state.store.providers().get_provider(&provider_id).await,
        Ok(Some(_))
    ) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let timestamp = now();
    let model = NewProviderModel {
        id: id("model"),
        provider_id,
        public_model: input.public_model,
        upstream_model: input.upstream_model,
        display_name: input.display_name.trim().to_string(),
        context_window: input.context_window,
        max_output_tokens: input.max_output_tokens,
        supports_tools: input.supports_tools,
        supports_vision: input.supports_vision,
        supports_parallel_tool_calls: input.supports_parallel_tool_calls,
        supports_web_search: input.supports_web_search,
        supports_reasoning_summaries: input.supports_reasoning_summaries,
        reasoning_levels_json: serde_json::to_string(&input.reasoning_levels)
            .unwrap_or_else(|_| "[]".into()),
        model_info_json: input.model_info.map(|value| value.to_string()),
        input_per_million: input.input_per_million,
        cached_input_per_million: input.cached_input_per_million,
        output_per_million: input.output_per_million,
        visible_in_codex: input.visible_in_codex,
        visible_in_openai: input.visible_in_openai,
        enabled: true,
        created_at: timestamp,
    };
    match state.store.providers().create_model(&model).await {
        Ok(()) => {
            let created = state
                .store
                .providers()
                .list_models(&model.provider_id)
                .await
                .ok()
                .and_then(|rows| rows.into_iter().find(|row| row.id == model.id));
            match created {
                Some(model) => (StatusCode::CREATED, Json(ModelView::from(model))).into_response(),
                None => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            }
        }
        Err(_) => (StatusCode::CONFLICT, "model already exists").into_response(),
    }
}

#[derive(Deserialize)]
pub struct ModelPatch {
    enabled: Option<bool>,
    visible_in_codex: Option<bool>,
    visible_in_openai: Option<bool>,
}

pub async fn patch_model(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(input): Json<ModelPatch>,
) -> Response {
    if input.enabled.is_none()
        && input.visible_in_codex.is_none()
        && input.visible_in_openai.is_none()
    {
        return (StatusCode::BAD_REQUEST, "empty model patch").into_response();
    }
    match state
        .store
        .providers()
        .update_model_policy(
            &id,
            input.enabled,
            input.visible_in_codex,
            input.visible_in_openai,
            now(),
        )
        .await
    {
        Ok(true) => ok(),
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

pub async fn delete_model(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    match state.store.providers().delete_model(&id).await {
        Ok(true) => ok(),
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}
