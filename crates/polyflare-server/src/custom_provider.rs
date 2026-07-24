//! Generic OpenAI Responses-compatible custom-provider transport.
//!
//! Custom providers are model-routed and API-key-backed. They deliberately do not participate in
//! subscription-account continuity: stateless providers receive a materialized request with
//! `previous_response_id` removed, while PolyFlare streams their SSE bytes to the existing client.

use std::collections::{HashMap, HashSet};
use std::io;
use std::net::IpAddr;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::{Body, Bytes};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use futures_util::StreamExt;
use polyflare_store::{CustomProvider, ProviderCredential, ProviderModel, Store, TokenCipher};

const MAX_ERROR_BODY_BYTES: usize = 64 * 1024;
const MAX_MODEL_CATALOG_BYTES: usize = 2 * 1024 * 1024;
const MAX_DISCOVERED_MODELS: usize = 1_000;

static HTTP_CLIENTS: LazyLock<Mutex<HashMap<(String, i64), reqwest::Client>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

static IN_FLIGHT: LazyLock<Mutex<HashMap<String, usize>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Debug, Clone)]
pub struct CustomRouteOutcome {
    pub provider_slug: String,
    pub credential_id: Option<String>,
    pub public_model: String,
    pub upstream_model: String,
    pub upstream_transport: String,
    pub input_per_million: Option<f64>,
    pub cached_input_per_million: Option<f64>,
    pub output_per_million: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct DiscoveredProviderModel {
    pub upstream_model: String,
    pub display_name: String,
    pub context_window: Option<i64>,
    pub max_output_tokens: Option<i64>,
    pub supports_tools: bool,
    pub supports_vision: bool,
    pub supports_parallel_tool_calls: bool,
    pub supports_web_search: bool,
    pub supports_reasoning_summaries: bool,
    pub reasoning_levels: Vec<String>,
    pub model_info: Option<serde_json::Value>,
}

struct CredentialLease {
    id: String,
}

impl Drop for CredentialLease {
    fn drop(&mut self) {
        let mut in_flight = IN_FLIGHT.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(count) = in_flight.get_mut(&self.id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                in_flight.remove(&self.id);
            }
        }
    }
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

async fn http_client(
    provider: &CustomProvider,
    endpoint: &reqwest::Url,
) -> Result<reqwest::Client, &'static str> {
    let cache_key = (provider.id.clone(), provider.updated_at);
    if let Some(client) = HTTP_CLIENTS
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(&cache_key)
        .cloned()
    {
        return Ok(client);
    }

    let timeout_ms = u64::try_from(provider.connect_timeout_ms.max(100)).unwrap_or(10_000);
    let mut builder = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_millis(timeout_ms));

    // A hostname that passed the lexical URL check can still resolve to loopback/RFC1918 space.
    // Resolve once, reject the entire set if any address is non-public, and pin the validated
    // address into this cached client so the subsequent TLS request cannot be DNS-rebound.
    if !provider.allow_private_hosts {
        let host = endpoint.host_str().ok_or("provider URL has no host")?;
        if parse_url_host_ip(host).is_none() {
            let port = endpoint
                .port_or_known_default()
                .ok_or("provider URL has no port")?;
            let addresses: Vec<_> = tokio::net::lookup_host((host, port))
                .await
                .map_err(|_| "provider host resolution failed")?
                .collect();
            if addresses.is_empty() || addresses.iter().any(|address| !is_public_ip(address.ip())) {
                return Err("provider host resolved to a private address");
            }
            builder = builder.resolve(host, addresses[0]);
        }
    }

    let client = builder.build().map_err(|_| "provider HTTP client failed")?;
    HTTP_CLIENTS
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .insert(cache_key, client.clone());
    Ok(client)
}

fn validate_provider_url(
    provider: &CustomProvider,
    endpoint_name: &str,
) -> Result<reqwest::Url, &'static str> {
    let mut url = reqwest::Url::parse(&provider.base_url).map_err(|_| "invalid provider URL")?;
    if url.username() != ""
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("invalid provider URL");
    }
    if url.scheme() != "https" && !(provider.allow_private_hosts && url.scheme() == "http") {
        return Err("provider URL must use HTTPS");
    }
    let host = url.host_str().ok_or("provider URL has no host")?;
    let private_host = host.eq_ignore_ascii_case("localhost")
        || host.ends_with(".localhost")
        || parse_url_host_ip(host).is_some_and(|ip| !is_public_ip(ip));
    if private_host && !provider.allow_private_hosts {
        return Err("private provider host is disabled");
    }
    let mut path = url.path().trim_end_matches('/').to_string();
    path.push('/');
    path.push_str(endpoint_name);
    url.set_path(&path);
    Ok(url)
}

fn validate_endpoint(provider: &CustomProvider) -> Result<reqwest::Url, &'static str> {
    validate_provider_url(provider, "responses")
}

fn parse_url_host_ip(host: &str) -> Option<IpAddr> {
    host.strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host)
        .parse()
        .ok()
}

fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            !(ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_broadcast()
                || ip.is_documentation()
                || ip.is_unspecified())
        }
        IpAddr::V6(ip) => {
            if let Some(mapped) = ip.to_ipv4_mapped() {
                return is_public_ip(IpAddr::V4(mapped));
            }
            !(ip.is_loopback()
                || ip.is_unspecified()
                || ip.is_unique_local()
                || ip.is_unicast_link_local())
        }
    }
}

fn acquire_credential(
    provider: &CustomProvider,
    credentials: &[ProviderCredential],
    tried: &HashSet<String>,
    now: i64,
) -> Option<(ProviderCredential, CredentialLease)> {
    let mut in_flight = IN_FLIGHT.lock().unwrap_or_else(|e| e.into_inner());
    let provider_in_flight: usize = credentials
        .iter()
        .map(|credential| in_flight.get(&credential.id).copied().unwrap_or(0))
        .sum();
    if provider
        .max_concurrency
        .is_some_and(|limit| provider_in_flight >= limit as usize)
    {
        return None;
    }

    let candidate = credentials
        .iter()
        .filter(|credential| {
            credential.enabled
                && !tried.contains(&credential.id)
                && (credential.health_status == "healthy"
                    || (credential.health_status == "cooldown"
                        && credential.cooldown_until.is_some_and(|until| until <= now)))
                && !credential.max_concurrency.is_some_and(|limit| {
                    in_flight.get(&credential.id).copied().unwrap_or(0) >= limit as usize
                })
        })
        .min_by(|left, right| {
            let left_score =
                in_flight.get(&left.id).copied().unwrap_or(0) as f64 / left.routing_weight;
            let right_score =
                in_flight.get(&right.id).copied().unwrap_or(0) as f64 / right.routing_weight;
            left_score
                .total_cmp(&right_score)
                .then_with(|| left.id.cmp(&right.id))
        })?
        .clone();

    *in_flight.entry(candidate.id.clone()).or_default() += 1;
    let lease = CredentialLease {
        id: candidate.id.clone(),
    };
    Some((candidate, lease))
}

fn retryable_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

async fn mark_pre_stream_failure(store: &Store, credential_id: &str, status: StatusCode) {
    let now = unix_now();
    let (health, cooldown) =
        if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
            ("reauth_required", None)
        } else if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
            ("cooldown", Some(now + 30))
        } else {
            return;
        };
    let _ = store
        .providers()
        .set_credential_health(credential_id, health, cooldown, now)
        .await;
}

fn valid_discovered_model_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 96
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn reasoning_levels(value: &serde_json::Value) -> Vec<String> {
    let mut seen = HashSet::new();
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|level| {
            level
                .as_str()
                .or_else(|| level.get("effort").and_then(serde_json::Value::as_str))
        })
        .filter(|level| {
            matches!(
                *level,
                "none" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max"
            )
        })
        .filter(|level| seen.insert((*level).to_string()))
        .map(str::to_string)
        .collect()
}

fn known_reasoning_levels(model: &str) -> &'static [&'static str] {
    match model {
        "fugu-ultra" | "fugu-ultra-v1.1" => &["high", "xhigh", "max"],
        "fugu" | "fugu-ultra-v1.0" | "fugu-cyber" | "fugu-cyber-v1.0" => &["high", "xhigh"],
        _ => &[],
    }
}

fn optional_positive_i64(value: Option<&serde_json::Value>) -> Option<i64> {
    value
        .and_then(serde_json::Value::as_i64)
        .filter(|value| *value > 0)
}

fn safe_model_info_extensions(model: &serde_json::Value) -> Option<serde_json::Value> {
    let object = model.as_object()?;
    let mut extensions = serde_json::Map::new();
    if let Some(description) = object
        .get("description")
        .and_then(serde_json::Value::as_str)
        .filter(|description| description.len() <= 4 * 1024)
    {
        extensions.insert("description".into(), description.into());
    }
    if let Some(base_instructions) = object
        .get("base_instructions")
        .and_then(serde_json::Value::as_str)
    {
        extensions.insert("base_instructions".into(), base_instructions.into());
    }
    if let Some(priority) = object
        .get("priority")
        .and_then(serde_json::Value::as_i64)
        .filter(|priority| i32::try_from(*priority).is_ok())
    {
        extensions.insert("priority".into(), priority.into());
    }
    (!extensions.is_empty()).then_some(serde_json::Value::Object(extensions))
}

fn parse_discovered_models(payload: &[u8]) -> Result<Vec<DiscoveredProviderModel>, &'static str> {
    let root: serde_json::Value =
        serde_json::from_slice(payload).map_err(|_| "provider model catalog is invalid JSON")?;
    let (rows, rich) = if let Some(models) = root.get("models").and_then(|value| value.as_array()) {
        (models, true)
    } else if let Some(data) = root.get("data").and_then(|value| value.as_array()) {
        (data, false)
    } else {
        return Err("provider model catalog has an unsupported shape");
    };

    let mut seen = HashSet::new();
    let models = rows
        .iter()
        .take(MAX_DISCOVERED_MODELS)
        .filter_map(|row| {
            let id = if rich { row.get("slug") } else { row.get("id") }
                .and_then(serde_json::Value::as_str)?;
            if !valid_discovered_model_id(id) || !seen.insert(id.to_string()) {
                return None;
            }

            let mut levels = reasoning_levels(
                row.get("supported_reasoning_levels")
                    .unwrap_or(&serde_json::Value::Null),
            );
            if levels.is_empty() {
                levels.extend(
                    known_reasoning_levels(id)
                        .iter()
                        .map(|level| (*level).to_string()),
                );
            }
            let modalities = row
                .get("input_modalities")
                .and_then(serde_json::Value::as_array);
            let supports_vision = modalities
                .is_some_and(|values| values.iter().any(|value| value.as_str() == Some("image")));
            let supports_tools = row
                .get("apply_patch_tool_type")
                .map(|value| !value.is_null())
                .unwrap_or(true);
            let supports_web_search = row
                .get("supports_search_tool")
                .and_then(serde_json::Value::as_bool)
                .or_else(|| {
                    row.get("web_search_tool_type")
                        .map(|value| !value.is_null())
                })
                .unwrap_or(false);
            let supports_reasoning_summaries = row
                .get("supports_reasoning_summaries")
                .or_else(|| row.get("supports_reasoning_summary_parameter"))
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);

            Some(DiscoveredProviderModel {
                upstream_model: id.to_string(),
                display_name: row
                    .get("display_name")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or(id)
                    .to_string(),
                context_window: optional_positive_i64(row.get("context_window")).or_else(|| {
                    optional_positive_i64(
                        row.get("metadata")
                            .and_then(|metadata| metadata.get("context_window")),
                    )
                }),
                max_output_tokens: optional_positive_i64(row.get("max_output_tokens")),
                supports_tools,
                supports_vision,
                supports_parallel_tool_calls: row
                    .get("supports_parallel_tool_calls")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(true),
                supports_web_search,
                supports_reasoning_summaries,
                reasoning_levels: levels,
                model_info: rich.then(|| safe_model_info_extensions(row)).flatten(),
            })
        })
        .collect::<Vec<_>>();
    if models.is_empty() {
        Err("provider model catalog contains no usable models")
    } else {
        Ok(models)
    }
}

pub async fn discover_models(
    store: &Store,
    cipher: &TokenCipher,
    provider: &CustomProvider,
) -> Result<Vec<DiscoveredProviderModel>, &'static str> {
    let endpoint = validate_provider_url(provider, "models")?;
    let client = http_client(provider, &endpoint).await?;
    let credentials = store
        .providers()
        .list_credentials(&provider.id)
        .await
        .map_err(|_| "provider credentials unavailable")?;
    let (credential, lease) =
        acquire_credential(provider, &credentials, &HashSet::new(), unix_now())
            .ok_or("no eligible provider credential")?;
    let (_, secret) = store
        .providers()
        .decrypt_credential(&credential.id, cipher)
        .await
        .map_err(|_| "provider credential unavailable")?
        .ok_or("provider credential unavailable")?;

    let request_timeout = Duration::from_millis(
        u64::try_from(provider.stream_idle_timeout_ms.max(1_000)).unwrap_or(300_000),
    );
    let response = client
        .get(endpoint)
        .bearer_auth(&secret.0)
        .header(header::ACCEPT, "application/json")
        .timeout(request_timeout)
        .send()
        .await
        .map_err(|_| "provider model discovery failed")?;
    let status =
        StatusCode::from_u16(response.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    if !status.is_success() {
        mark_pre_stream_failure(store, &credential.id, status).await;
        drop(lease);
        return Err("provider model discovery returned an error");
    }

    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| "provider model discovery stream failed")?;
        if body.len().saturating_add(chunk.len()) > MAX_MODEL_CATALOG_BYTES {
            drop(lease);
            return Err("provider model catalog is too large");
        }
        body.extend_from_slice(&chunk);
    }
    let _ = store
        .providers()
        .set_credential_health(&credential.id, "healthy", None, unix_now())
        .await;
    drop(lease);
    parse_discovered_models(&body)
}

fn copy_response_headers(
    source: &reqwest::header::HeaderMap,
    target: &mut axum::http::response::Builder,
) {
    for name in [
        header::CONTENT_TYPE,
        header::CACHE_CONTROL,
        header::RETRY_AFTER,
    ] {
        if let Some(value) = source.get(&name) {
            *target = std::mem::take(target).header(name, value);
        }
    }
    for name in ["x-request-id", "openai-processing-ms"] {
        if let Some(value) = source.get(name) {
            *target = std::mem::take(target).header(name, value);
        }
    }
}

pub async fn execute(
    store: &Store,
    cipher: &TokenCipher,
    provider: CustomProvider,
    model: ProviderModel,
    inbound_headers: &HeaderMap,
    raw_body: &Bytes,
) -> (Response, CustomRouteOutcome) {
    let mut outcome = CustomRouteOutcome {
        provider_slug: provider.slug.clone(),
        credential_id: None,
        public_model: model.public_model.clone(),
        upstream_model: model.upstream_model.clone(),
        upstream_transport: "http_sse".into(),
        input_per_million: model.input_per_million,
        cached_input_per_million: model.cached_input_per_million,
        output_per_million: model.output_per_million,
    };
    let endpoint = match validate_endpoint(&provider) {
        Ok(endpoint) => endpoint,
        Err(message) => return ((StatusCode::BAD_GATEWAY, message).into_response(), outcome),
    };
    let mut body: serde_json::Value = match serde_json::from_slice(raw_body) {
        Ok(serde_json::Value::Object(object)) => serde_json::Value::Object(object),
        _ => {
            return (
                (StatusCode::BAD_REQUEST, "invalid JSON body").into_response(),
                outcome,
            )
        }
    };
    let object = body.as_object_mut().expect("validated object");
    object.insert(
        "model".into(),
        serde_json::Value::String(model.upstream_model.clone()),
    );
    if provider.stateless_responses {
        object.remove("previous_response_id");
    }
    let encoded = match serde_json::to_vec(&body) {
        Ok(encoded) => encoded,
        Err(_) => {
            return (
                (StatusCode::BAD_REQUEST, "invalid JSON body").into_response(),
                outcome,
            )
        }
    };

    let credentials = match store.providers().list_credentials(&provider.id).await {
        Ok(credentials) => credentials,
        Err(_) => {
            return (
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
                outcome,
            )
        }
    };
    let client = match http_client(&provider, &endpoint).await {
        Ok(client) => client,
        Err(_) => {
            return (
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
                outcome,
            )
        }
    };
    let mut tried = HashSet::new();
    let max_attempts = usize::try_from(provider.request_max_retries.saturating_add(1))
        .unwrap_or(1)
        .min(credentials.len().max(1));
    let mut last_response: Option<Response> = None;

    for _ in 0..max_attempts {
        let Some((credential, lease)) =
            acquire_credential(&provider, &credentials, &tried, unix_now())
        else {
            break;
        };
        tried.insert(credential.id.clone());
        let (_, secret) = match store
            .providers()
            .decrypt_credential(&credential.id, cipher)
            .await
        {
            Ok(Some(pair)) => pair,
            _ => {
                drop(lease);
                continue;
            }
        };
        outcome.credential_id = Some(credential.id.clone());
        let mut request = client
            .post(endpoint.clone())
            .bearer_auth(&secret.0)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::ACCEPT, "text/event-stream")
            .body(encoded.clone());
        if let Some(value) = inbound_headers.get("openai-beta") {
            request = request.header("openai-beta", value);
        }
        let upstream = match request.send().await {
            Ok(response) => response,
            Err(_) => {
                let _ = store
                    .providers()
                    .set_credential_health(
                        &credential.id,
                        "cooldown",
                        Some(unix_now() + 30),
                        unix_now(),
                    )
                    .await;
                drop(lease);
                continue;
            }
        };
        let status = upstream.status();
        let status = StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
        if !status.is_success() {
            mark_pre_stream_failure(store, &credential.id, status).await;
            let headers = upstream.headers().clone();
            let mut bytes = upstream.bytes().await.unwrap_or_default().to_vec();
            bytes.truncate(MAX_ERROR_BODY_BYTES);
            let mut builder = Response::builder().status(status);
            copy_response_headers(&headers, &mut builder);
            last_response = Some(
                builder
                    .body(Body::from(bytes))
                    .expect("valid custom-provider error response"),
            );
            drop(lease);
            if retryable_status(status) {
                continue;
            }
            return (last_response.expect("set above"), outcome);
        }

        let _ = store
            .providers()
            .set_credential_health(&credential.id, "healthy", None, unix_now())
            .await;
        let headers = upstream.headers().clone();
        let mut builder = Response::builder().status(status);
        copy_response_headers(&headers, &mut builder);
        let idle_timeout = Duration::from_millis(
            u64::try_from(provider.stream_idle_timeout_ms.max(1_000)).unwrap_or(300_000),
        );
        let credential_id = credential.id.clone();
        let store = store.clone();
        let stream = async_stream::stream! {
            let _lease = lease;
            let mut bytes = upstream.bytes_stream();
            loop {
                match tokio::time::timeout(idle_timeout, bytes.next()).await {
                    Ok(Some(Ok(chunk))) => yield Ok::<Bytes, io::Error>(chunk),
                    Ok(Some(Err(error))) => {
                        let _ = store
                            .providers()
                            .set_credential_health(
                                &credential_id,
                                "cooldown",
                                Some(unix_now() + 30),
                                unix_now(),
                            )
                            .await;
                        yield Err(io::Error::other(error));
                        break;
                    }
                    Ok(None) => break,
                    Err(_) => {
                        let _ = store
                            .providers()
                            .set_credential_health(
                                &credential_id,
                                "cooldown",
                                Some(unix_now() + 30),
                                unix_now(),
                            )
                            .await;
                        yield Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "custom provider stream idle timeout",
                        ));
                        break;
                    }
                }
            }
        };
        let response = builder
            .body(Body::from_stream(stream))
            .expect("valid custom-provider streaming response");
        return (response, outcome);
    }

    (
        last_response.unwrap_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "no eligible provider credential",
            )
                .into_response()
        }),
        outcome,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_validation_rejects_private_hosts_by_default() {
        let provider = CustomProvider {
            id: "p".into(),
            slug: "local".into(),
            display_name: "Local".into(),
            base_url: "http://127.0.0.1:9999/v1".into(),
            wire_api: "responses".into(),
            enabled: true,
            stateless_responses: true,
            allow_private_hosts: false,
            connect_timeout_ms: 1000,
            stream_idle_timeout_ms: 1000,
            request_max_retries: 0,
            max_concurrency: None,
            created_at: 0,
            updated_at: 0,
        };
        assert!(validate_endpoint(&provider).is_err());

        let mut mapped_loopback = provider;
        mapped_loopback.base_url = "https://[::ffff:127.0.0.1]/v1".into();
        assert!(validate_endpoint(&mapped_loopback).is_err());
    }

    #[test]
    fn model_discovery_parses_rich_codex_capabilities_and_efforts() {
        let payload = serde_json::to_vec(&serde_json::json!({
            "models": [{
                "slug": "fugu-ultra-v1.1",
                "display_name": "Fugu Ultra v1.1",
                "context_window": 1_000_000,
                "supported_reasoning_levels": [
                    {"effort": "high", "description": "default"},
                    {"effort": "xhigh", "description": "deep"},
                    {"effort": "max", "description": "maximum"}
                ],
                "supports_reasoning_summaries": true,
                "supports_parallel_tool_calls": true,
                "supports_search_tool": true,
                "input_modalities": ["text", "image"],
                "apply_patch_tool_type": "freeform",
                "description": "Operator-safe description",
                "priority": 3
            }]
        }))
        .unwrap();

        let models = parse_discovered_models(&payload).unwrap();
        assert_eq!(models.len(), 1);
        let model = &models[0];
        assert_eq!(model.upstream_model, "fugu-ultra-v1.1");
        assert_eq!(model.context_window, Some(1_000_000));
        assert_eq!(model.reasoning_levels, ["high", "xhigh", "max"]);
        assert!(model.supports_tools);
        assert!(model.supports_vision);
        assert!(model.supports_parallel_tool_calls);
        assert!(model.supports_web_search);
        assert!(model.supports_reasoning_summaries);
        assert_eq!(
            model.model_info.as_ref().unwrap()["description"],
            "Operator-safe description"
        );
        assert_eq!(model.model_info.as_ref().unwrap()["priority"], 3);
    }

    #[test]
    fn model_discovery_url_uses_the_validated_provider_base() {
        let provider = CustomProvider {
            id: "p".into(),
            slug: "sakana".into(),
            display_name: "Sakana".into(),
            base_url: "https://api.sakana.ai/v1/".into(),
            wire_api: "responses".into(),
            enabled: true,
            stateless_responses: true,
            allow_private_hosts: false,
            connect_timeout_ms: 1000,
            stream_idle_timeout_ms: 1000,
            request_max_retries: 0,
            max_concurrency: None,
            created_at: 0,
            updated_at: 0,
        };
        assert_eq!(
            validate_provider_url(&provider, "models").unwrap().as_str(),
            "https://api.sakana.ai/v1/models"
        );
    }

    #[test]
    fn openai_catalog_applies_known_fugu_efforts_without_limiting_other_models() {
        let payload = serde_json::to_vec(&serde_json::json!({
            "data": [
                {"id": "fugu"},
                {"id": "fugu-ultra"},
                {"id": "fugu-ultra-v1.0"},
                {"id": "fugu-ultra-v1.1"},
                {"id": "fugu-cyber"},
                {"id": "fugu-cyber-v1.0"},
                {"id": "another-provider-model"}
            ]
        }))
        .unwrap();

        let models = parse_discovered_models(&payload).unwrap();
        let efforts = |id: &str| {
            models
                .iter()
                .find(|model| model.upstream_model == id)
                .unwrap()
                .reasoning_levels
                .clone()
        };
        assert_eq!(efforts("fugu"), ["high", "xhigh"]);
        assert_eq!(efforts("fugu-ultra"), ["high", "xhigh", "max"]);
        assert_eq!(efforts("fugu-ultra-v1.0"), ["high", "xhigh"]);
        assert_eq!(efforts("fugu-ultra-v1.1"), ["high", "xhigh", "max"]);
        assert_eq!(efforts("fugu-cyber"), ["high", "xhigh"]);
        assert_eq!(efforts("fugu-cyber-v1.0"), ["high", "xhigh"]);
        assert!(efforts("another-provider-model").is_empty());
    }
}
