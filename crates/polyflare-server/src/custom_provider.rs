//! Generic custom-provider transport for OpenAI Responses and Anthropic Messages wire APIs.
//!
//! Custom providers are model-routed and API-key-backed. They deliberately do not participate in
//! subscription-account continuity: stateless providers receive a materialized request with
//! `previous_response_id` removed, while PolyFlare streams their SSE bytes to the existing client.

use std::collections::{HashMap, HashSet};
use std::io;
use std::net::IpAddr;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::{Body, Bytes};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use futures_util::StreamExt;
use polyflare_store::{CustomProvider, ProviderCredential, ProviderModel, Store, TokenCipher};
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};

const MAX_ERROR_BODY_BYTES: usize = 64 * 1024;
const MAX_MODEL_CATALOG_BYTES: usize = 2 * 1024 * 1024;
const MAX_DISCOVERED_MODELS: usize = 1_000;
const PROFILE_SEPARATOR: &str = "\n\n--- PolyFlare model profile ---\n";
const HTTP_CLIENT_DNS_TTL: Duration = Duration::from_secs(300);
const CUSTOM_AFFINITY_TTL: Duration = Duration::from_secs(30 * 60);
const MAX_CUSTOM_AFFINITIES: usize = 10_000;
const CUSTOM_FAIR_ROUTING_TTL: Duration = Duration::from_secs(30 * 60);
const MAX_CUSTOM_FAIR_ROUTING_SCOPES: usize = 10_000;
const MAX_AFFINITY_SSE_LINE_BYTES: usize = 1024 * 1024;

struct CachedHttpClient {
    client: reqwest::Client,
    expires_at: Instant,
}

static HTTP_CLIENTS: LazyLock<Mutex<HashMap<(String, i64), CachedHttpClient>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

static IN_FLIGHT: LazyLock<Mutex<HashMap<String, usize>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Debug, Clone, PartialEq, Eq)]
struct AffinityTarget {
    provider_id: String,
    credential_id: String,
    last_success: Instant,
}

struct AffinityCache {
    entries: HashMap<String, AffinityTarget>,
    ttl: Duration,
    max_entries: usize,
}

impl AffinityCache {
    fn new(ttl: Duration, max_entries: usize) -> Self {
        Self {
            entries: HashMap::new(),
            ttl,
            max_entries,
        }
    }

    fn prune_expired(&mut self, now: Instant) {
        self.entries
            .retain(|_, target| now.saturating_duration_since(target.last_success) < self.ttl);
    }

    fn get(&mut self, key: &str, now: Instant) -> Option<AffinityTarget> {
        self.prune_expired(now);
        self.entries.get(key).cloned()
    }

    fn record(&mut self, key: String, provider_id: String, credential_id: String, now: Instant) {
        self.prune_expired(now);
        if self.max_entries == 0 {
            return;
        }
        if !self.entries.contains_key(&key) && self.entries.len() >= self.max_entries {
            if let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, target)| target.last_success)
                .map(|(key, _)| key.clone())
            {
                self.entries.remove(&oldest);
            }
        }
        self.entries.insert(
            key,
            AffinityTarget {
                provider_id,
                credential_id,
                last_success: now,
            },
        );
    }
}

static CUSTOM_AFFINITY: LazyLock<Mutex<AffinityCache>> = LazyLock::new(|| {
    Mutex::new(AffinityCache::new(
        CUSTOM_AFFINITY_TTL,
        MAX_CUSTOM_AFFINITIES,
    ))
});

#[derive(Debug)]
struct SmoothWeightedState {
    current: HashMap<String, f64>,
    last_used: Instant,
}

struct SmoothWeightedCache {
    entries: HashMap<String, SmoothWeightedState>,
    ttl: Duration,
    max_entries: usize,
}

impl SmoothWeightedCache {
    fn new(ttl: Duration, max_entries: usize) -> Self {
        Self {
            entries: HashMap::new(),
            ttl,
            max_entries,
        }
    }

    fn prune_expired(&mut self, now: Instant) {
        self.entries
            .retain(|_, state| now.saturating_duration_since(state.last_used) < self.ttl);
    }

    fn select(
        &mut self,
        scope: String,
        candidates: &[(String, f64)],
        now: Instant,
    ) -> Option<String> {
        self.prune_expired(now);
        if self.max_entries == 0 || candidates.is_empty() {
            return None;
        }
        if !self.entries.contains_key(&scope) && self.entries.len() >= self.max_entries {
            if let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, state)| state.last_used)
                .map(|(key, _)| key.clone())
            {
                self.entries.remove(&oldest);
            }
        }

        let mut candidates = candidates
            .iter()
            .filter(|(_, weight)| weight.is_finite() && *weight > 0.0)
            .cloned()
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| left.0.cmp(&right.0));
        if candidates.is_empty() {
            return None;
        }
        let candidate_ids = candidates
            .iter()
            .map(|(id, _)| id.as_str())
            .collect::<HashSet<_>>();
        let state = self
            .entries
            .entry(scope)
            .or_insert_with(|| SmoothWeightedState {
                current: HashMap::new(),
                last_used: now,
            });
        state.last_used = now;
        state
            .current
            .retain(|id, _| candidate_ids.contains(id.as_str()));

        let mut total_weight = 0.0;
        let mut selected: Option<(&str, f64)> = None;
        for (id, weight) in &candidates {
            total_weight += *weight;
            let current = state.current.entry(id.clone()).or_default();
            *current += *weight;
            if selected.is_none_or(|(selected_id, selected_current)| {
                current.total_cmp(&selected_current).is_gt()
                    || (current.total_cmp(&selected_current).is_eq() && id.as_str() < selected_id)
            }) {
                selected = Some((id, *current));
            }
        }
        let selected_id = selected?.0.to_string();
        if let Some(current) = state.current.get_mut(&selected_id) {
            *current -= total_weight;
        }
        Some(selected_id)
    }
}

static CUSTOM_FAIR_ROUTING: LazyLock<Mutex<SmoothWeightedCache>> = LazyLock::new(|| {
    Mutex::new(SmoothWeightedCache::new(
        CUSTOM_FAIR_ROUTING_TTL,
        MAX_CUSTOM_FAIR_ROUTING_SCOPES,
    ))
});

pub(crate) fn evict_provider_client(provider_id: &str) {
    HTTP_CLIENTS
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .retain(|(cached_provider_id, _), _| cached_provider_id != provider_id);
}

#[derive(Debug, Clone)]
pub struct CustomRouteOutcome {
    pub provider_slug: String,
    pub credential_id: Option<String>,
    pub public_model: String,
    pub upstream_model: String,
    pub upstream_transport: String,
    /// Effective tier used by the selected target when PolyFlare changed the client's request.
    pub effective_service_tier: Option<String>,
    pub profile_revision: Option<String>,
    pub input_per_million: Option<f64>,
    pub cached_input_per_million: Option<f64>,
    pub output_per_million: Option<f64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct RequestCapabilities {
    priority: bool,
    tools: bool,
    vision: bool,
    parallel_tool_calls: bool,
    web_search: bool,
    reasoning_summaries: bool,
}

#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileRequestOverrides {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<i64>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DiscoveredProviderModel {
    pub upstream_model: String,
    pub display_name: String,
    pub context_window: Option<i64>,
    pub max_output_tokens: Option<i64>,
    pub supports_tools: bool,
    pub supports_vision: bool,
    pub supports_parallel_tool_calls: bool,
    pub supports_web_search: bool,
    pub supports_reasoning: bool,
    pub supports_reasoning_summaries: bool,
    pub reasoning_levels: Vec<String>,
    pub input_per_million: Option<f64>,
    pub cached_input_per_million: Option<f64>,
    pub output_per_million: Option<f64>,
    #[serde(skip_serializing)]
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
    http_client_at(provider, endpoint, Instant::now()).await
}

async fn http_client_at(
    provider: &CustomProvider,
    endpoint: &reqwest::Url,
    now: Instant,
) -> Result<reqwest::Client, &'static str> {
    let cache_key = (provider.id.clone(), provider.updated_at);
    {
        let mut clients = HTTP_CLIENTS
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(cached) = clients.get(&cache_key) {
            if cached.expires_at > now {
                return Ok(cached.client.clone());
            }
        }
        clients.remove(&cache_key);
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
    let mut clients = HTTP_CLIENTS
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    clients.retain(|(provider_id, _), _| provider_id != &provider.id);
    clients.insert(
        cache_key,
        CachedHttpClient {
            client: client.clone(),
            expires_at: now + HTTP_CLIENT_DNS_TTL,
        },
    );
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
    match provider.wire_api.as_str() {
        "responses" => validate_provider_url(provider, "responses"),
        "anthropic_messages" => validate_provider_url(provider, "messages"),
        _ => Err("unsupported provider wire protocol"),
    }
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
            let [a, b, c, d] = ip.octets();
            let shared = a == 100 && b & 0b1100_0000 == 0b0100_0000;
            let protocol_assignment = a == 192 && b == 0 && c == 0 && d != 9 && d != 10;
            let benchmarking = a == 198 && b & 0xfe == 18;
            let reserved = a & 0xf0 == 0xf0 && !ip.is_broadcast();
            !(a == 0
                || ip.is_private()
                || shared
                || ip.is_loopback()
                || ip.is_link_local()
                || protocol_assignment
                || ip.is_documentation()
                || benchmarking
                || reserved
                || ip.is_broadcast()
                || ip.is_multicast())
        }
        IpAddr::V6(ip) => {
            if let Some(mapped) = ip.to_ipv4_mapped() {
                return is_public_ip(IpAddr::V4(mapped));
            }
            let segments = ip.segments();
            let address = u128::from_be_bytes(ip.octets());
            let ietf_protocol_assignment = matches!(
                segments,
                [0x2001, second, _, _, _, _, _, _] if second < 0x200
            ) && !(address
                == 0x2001_0001_0000_0000_0000_0000_0000_0001
                || address == 0x2001_0001_0000_0000_0000_0000_0000_0002
                || matches!(segments, [0x2001, 3, _, _, _, _, _, _])
                || matches!(segments, [0x2001, 4, 0x112, _, _, _, _, _])
                || matches!(
                    segments,
                    [0x2001, second, _, _, _, _, _, _] if (0x20..=0x3f).contains(&second)
                ));
            let documentation = matches!(segments, [0x2001, 0xdb8, _, _, _, _, _, _])
                || matches!(segments, [first, _, _, _, _, _, _, _] if first & 0xfff0 == 0x3ff0);
            !(ip.is_unspecified()
                || ip.is_loopback()
                || matches!(segments, [0x64, 0xff9b, 1, _, _, _, _, _])
                || matches!(segments, [0x100, 0, 0, 0, _, _, _, _])
                || ietf_protocol_assignment
                || matches!(segments, [0x2002, _, _, _, _, _, _, _])
                || documentation
                || matches!(segments, [0x5f00, _, _, _, _, _, _, _])
                || ip.is_unique_local()
                || ip.is_unicast_link_local()
                || ip.is_multicast())
        }
    }
}

fn acquire_credential(
    provider: &CustomProvider,
    credentials: &[ProviderCredential],
    tried: &HashSet<String>,
    preferred_credential_id: Option<&str>,
    now: i64,
) -> Option<(ProviderCredential, CredentialLease)> {
    let mut in_flight = IN_FLIGHT.lock().unwrap_or_else(|e| e.into_inner());
    let candidate = best_credential(
        provider,
        credentials,
        tried,
        preferred_credential_id,
        now,
        &in_flight,
    )?
    .0
    .clone();

    *in_flight.entry(candidate.id.clone()).or_default() += 1;
    let lease = CredentialLease {
        id: candidate.id.clone(),
    };
    Some((candidate, lease))
}

fn best_credential<'a>(
    provider: &CustomProvider,
    credentials: &'a [ProviderCredential],
    tried: &HashSet<String>,
    preferred_credential_id: Option<&str>,
    now: i64,
    in_flight: &HashMap<String, usize>,
) -> Option<(&'a ProviderCredential, f64)> {
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

    let preferred = preferred_credential_id.and_then(|preferred| {
        credentials
            .iter()
            .filter(|credential| credential_is_eligible(credential, tried, now, in_flight))
            .find(|credential| credential.id == preferred)
    });
    preferred
        .or_else(|| {
            credentials
                .iter()
                .filter(|credential| credential_is_eligible(credential, tried, now, in_flight))
                .min_by(|left, right| {
                    credential_pressure(left, in_flight)
                        .total_cmp(&credential_pressure(right, in_flight))
                        .then_with(|| left.id.cmp(&right.id))
                })
        })
        .map(|credential| (credential, credential_pressure(credential, in_flight)))
}

fn credential_is_eligible(
    credential: &ProviderCredential,
    tried: &HashSet<String>,
    now: i64,
    in_flight: &HashMap<String, usize>,
) -> bool {
    credential.enabled
        && !tried.contains(&credential.id)
        && (credential.health_status == "healthy"
            || (credential.health_status == "cooldown"
                && credential.cooldown_until.is_some_and(|until| until <= now)))
        && !credential.max_concurrency.is_some_and(|limit| {
            in_flight.get(&credential.id).copied().unwrap_or(0) >= limit as usize
        })
}

fn credential_pressure(credential: &ProviderCredential, in_flight: &HashMap<String, usize>) -> f64 {
    in_flight.get(&credential.id).copied().unwrap_or(0) as f64 / credential.routing_weight
}

fn retryable_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

fn request_capabilities(raw_body: &Bytes, wire_api: &str) -> RequestCapabilities {
    let Ok(serde_json::Value::Object(object)) =
        serde_json::from_slice::<serde_json::Value>(raw_body)
    else {
        return RequestCapabilities::default();
    };
    let tools = object
        .get("tools")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|tools| !tools.is_empty());
    let web_search = object
        .get("tools")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|tools| {
            tools.iter().any(|tool| {
                tool.get("type")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|kind| kind.starts_with("web_search"))
            })
        });
    let vision = if wire_api == "anthropic_messages" {
        object
            .get("messages")
            .is_some_and(contains_anthropic_message_image)
    } else {
        object.get("input").is_some_and(contains_image_input)
    };
    let reasoning_summaries = object
        .get("reasoning")
        .and_then(|reasoning| reasoning.get("summary"))
        .is_some_and(|summary| !summary.is_null() && summary.as_str() != Some("none"));
    RequestCapabilities {
        priority: object
            .get("service_tier")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|tier| matches!(tier, "priority" | "fast")),
        tools,
        vision,
        parallel_tool_calls: tools
            && object
                .get("parallel_tool_calls")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
        web_search,
        reasoning_summaries,
    }
}

fn without_priority_service_tier(raw_body: &Bytes) -> Option<Bytes> {
    let mut body = serde_json::from_slice::<serde_json::Value>(raw_body).ok()?;
    let object = body.as_object_mut()?;
    if object
        .get("service_tier")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|tier| matches!(tier, "priority" | "fast"))
    {
        object.remove("service_tier");
    }
    serde_json::to_vec(&body).ok().map(Bytes::from)
}

fn contains_image_input(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Array(values) => values.iter().any(contains_image_input),
        serde_json::Value::Object(object) => {
            object
                .get("type")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|kind| matches!(kind, "input_image" | "image" | "image_url"))
                || object.values().any(contains_image_input)
        }
        _ => false,
    }
}

fn contains_anthropic_message_image(messages: &serde_json::Value) -> bool {
    messages.as_array().is_some_and(|messages| {
        messages.iter().any(|message| {
            message
                .get("content")
                .is_some_and(contains_anthropic_content_image)
        })
    })
}

fn contains_anthropic_content_image(content: &serde_json::Value) -> bool {
    content.as_array().is_some_and(|blocks| {
        blocks.iter().any(|block| {
            let Some(kind) = block.get("type").and_then(serde_json::Value::as_str) else {
                return false;
            };
            kind == "image"
                || (kind == "tool_result"
                    && block
                        .get("content")
                        .is_some_and(contains_anthropic_content_image))
        })
    })
}

fn target_supports(
    provider: &CustomProvider,
    model: &ProviderModel,
    wire_api: &str,
    required: RequestCapabilities,
) -> bool {
    provider.wire_api == wire_api
        && (!required.priority
            || crate::catalog::supports_priority_service_tier(model.model_info_json.as_deref()))
        && (!required.tools || model.supports_tools)
        && (!required.vision || model.supports_vision)
        && (!required.parallel_tool_calls || model.supports_parallel_tool_calls)
        && (!required.web_search || model.supports_web_search)
        && (!required.reasoning_summaries || model.supports_reasoning_summaries)
}

struct RankedTarget {
    pressure: f64,
    routing_weight: f64,
    routing_id: String,
    provider: CustomProvider,
    model: ProviderModel,
    preferred_credential_id: Option<String>,
}

fn fair_routing_scope(public_model: &str, wire_api: &str, required: RequestCapabilities) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"polyflare-custom-fair-routing-v1");
    hasher.update([0]);
    hasher.update(public_model.as_bytes());
    hasher.update([0]);
    hasher.update(wire_api.as_bytes());
    hasher.update([0]);
    hasher.update([
        required.priority as u8,
        required.tools as u8,
        required.vision as u8,
        required.parallel_tool_calls as u8,
        required.web_search as u8,
        required.reasoning_summaries as u8,
    ]);
    hex::encode(hasher.finalize())
}

fn weighted_rendezvous_score(selection_key: &str, candidate_id: &str, weight: f64) -> f64 {
    let mut hasher = Sha256::new();
    hasher.update(b"polyflare-custom-weighted-rendezvous-v1");
    hasher.update([0]);
    hasher.update(selection_key.as_bytes());
    hasher.update([0]);
    hasher.update(candidate_id.as_bytes());
    let digest = hasher.finalize();
    let mut prefix = [0u8; 8];
    prefix.copy_from_slice(&digest[..8]);
    let sample = u64::from_be_bytes(prefix);
    let unit = (sample as f64 + 1.0) / (u64::MAX as f64 + 2.0);
    -unit.ln() / weight
}

async fn rank_eligible_targets(
    store: &Store,
    targets: Vec<(CustomProvider, ProviderModel)>,
    wire_api: &str,
    required: RequestCapabilities,
    affinity: Option<&AffinityTarget>,
    selection_key: Option<&str>,
) -> Vec<(CustomProvider, ProviderModel, Option<String>)> {
    let now = unix_now();
    let public_model = targets
        .first()
        .map(|(_, model)| model.public_model.as_str())
        .unwrap_or_default();
    let routing_scope = fair_routing_scope(public_model, wire_api, required);
    let mut candidates = Vec::new();
    for (provider, model) in targets {
        if !target_supports(&provider, &model, wire_api, required) {
            continue;
        }
        let Ok(credentials) = store.providers().list_credentials(&provider.id).await else {
            continue;
        };
        let in_flight = IN_FLIGHT.lock().unwrap_or_else(|error| error.into_inner());
        let preferred_credential_id = affinity
            .filter(|target| target.provider_id == provider.id)
            .map(|target| target.credential_id.as_str());
        let tried = HashSet::new();
        let selected = best_credential(
            &provider,
            &credentials,
            &tried,
            preferred_credential_id,
            now,
            &in_flight,
        )
        .map(|(credential, score)| {
            let routing_weight = credentials
                .iter()
                .filter(|credential| credential_is_eligible(credential, &tried, now, &in_flight))
                .filter(|credential| {
                    credential_pressure(credential, &in_flight)
                        .total_cmp(&score)
                        .is_eq()
                })
                .map(|credential| credential.routing_weight)
                .sum::<f64>();
            (
                score,
                routing_weight,
                (preferred_credential_id == Some(credential.id.as_str()))
                    .then(|| credential.id.clone()),
            )
        });
        drop(in_flight);
        if let Some((pressure, routing_weight, preferred_credential_id)) = selected {
            let routing_id = format!("{}\0{}", provider.id, model.id);
            candidates.push(RankedTarget {
                pressure,
                routing_weight,
                routing_id,
                provider,
                model,
                preferred_credential_id,
            });
        }
    }

    if candidates
        .iter()
        .any(|candidate| candidate.preferred_credential_id.is_some())
    {
        candidates.sort_by(|left, right| {
            left.preferred_credential_id
                .is_none()
                .cmp(&right.preferred_credential_id.is_none())
                .then_with(|| left.pressure.total_cmp(&right.pressure))
                .then_with(|| left.provider.id.cmp(&right.provider.id))
                .then_with(|| left.model.id.cmp(&right.model.id))
        });
    } else if let Some(selection_key) = selection_key {
        candidates.sort_by(|left, right| {
            left.pressure
                .total_cmp(&right.pressure)
                .then_with(|| {
                    weighted_rendezvous_score(selection_key, &left.routing_id, left.routing_weight)
                        .total_cmp(&weighted_rendezvous_score(
                            selection_key,
                            &right.routing_id,
                            right.routing_weight,
                        ))
                })
                .then_with(|| left.provider.id.cmp(&right.provider.id))
                .then_with(|| left.model.id.cmp(&right.model.id))
        });
    } else if let Some(min_pressure) = candidates
        .iter()
        .map(|candidate| candidate.pressure)
        .min_by(f64::total_cmp)
    {
        let fair_candidates = candidates
            .iter()
            .filter(|candidate| candidate.pressure.total_cmp(&min_pressure).is_eq())
            .map(|candidate| (candidate.routing_id.clone(), candidate.routing_weight))
            .collect::<Vec<_>>();
        let selected = CUSTOM_FAIR_ROUTING
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .select(routing_scope, &fair_candidates, Instant::now());
        candidates.sort_by(|left, right| {
            (selected.as_deref() != Some(left.routing_id.as_str()))
                .cmp(&(selected.as_deref() != Some(right.routing_id.as_str())))
                .then_with(|| left.pressure.total_cmp(&right.pressure))
                .then_with(|| left.provider.id.cmp(&right.provider.id))
                .then_with(|| left.model.id.cmp(&right.model.id))
        });
    }

    candidates
        .into_iter()
        .map(|candidate| {
            (
                candidate.provider,
                candidate.model,
                candidate.preferred_credential_id,
            )
        })
        .collect()
}

fn request_affinity_key(
    inbound_headers: &HeaderMap,
    raw_body: &Bytes,
    public_model: &str,
    wire_api: &str,
    identity_override: Option<&str>,
) -> Option<String> {
    let identity = if let Some(identity) = identity_override {
        identity.to_string()
    } else {
        if wire_api != "responses" {
            return None;
        }
        let fields: HashMap<String, &RawValue> = serde_json::from_slice(raw_body).ok()?;
        let prompt_cache_key = fields
            .get("prompt_cache_key")
            .and_then(|value| serde_json::from_str::<String>(value.get()).ok());
        crate::session_key::header_session_key(inbound_headers, prompt_cache_key.as_deref())
            .map(|key| key.value)
            .or(prompt_cache_key)?
    };
    let mut hasher = Sha256::new();
    hasher.update(b"polyflare-custom-affinity-v1");
    hasher.update([0]);
    hasher.update(public_model.as_bytes());
    hasher.update([0]);
    hasher.update(identity.as_bytes());
    Some(hex::encode(hasher.finalize()))
}

fn current_affinity(key: Option<&str>) -> Option<AffinityTarget> {
    let key = key?;
    CUSTOM_AFFINITY
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(key, Instant::now())
}

fn record_affinity(key: Option<String>, provider_id: String, credential_id: String) {
    let Some(key) = key else {
        return;
    };
    CUSTOM_AFFINITY
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .record(key, provider_id, credential_id, Instant::now());
}

struct CompletionObserver {
    wire_api: String,
    event_stream: bool,
    pending: Vec<u8>,
    skipping_oversized_line: bool,
    terminal_success: bool,
}

impl CompletionObserver {
    fn new(wire_api: &str, event_stream: bool) -> Self {
        Self {
            wire_api: wire_api.to_string(),
            event_stream,
            pending: Vec::new(),
            skipping_oversized_line: false,
            terminal_success: false,
        }
    }

    fn observe(&mut self, chunk: &[u8]) {
        if !self.event_stream || self.terminal_success {
            return;
        }
        for byte in chunk {
            if self.skipping_oversized_line {
                if *byte == b'\n' {
                    self.skipping_oversized_line = false;
                }
                continue;
            }
            if *byte == b'\n' {
                let line = std::mem::take(&mut self.pending);
                self.observe_line(&line);
            } else if self.pending.len() < MAX_AFFINITY_SSE_LINE_BYTES {
                self.pending.push(*byte);
            } else {
                self.pending.clear();
                self.skipping_oversized_line = true;
            }
        }
    }

    fn observe_line(&mut self, line: &[u8]) {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let Some(data) = line.strip_prefix(b"data:") else {
            return;
        };
        let Ok(value) =
            serde_json::from_slice::<serde_json::Value>(data.strip_prefix(b" ").unwrap_or(data))
        else {
            return;
        };
        let event_type = value.get("type").and_then(serde_json::Value::as_str);
        self.terminal_success = match self.wire_api.as_str() {
            "responses" => event_type == Some("response.completed"),
            "anthropic_messages" => event_type == Some("message_stop"),
            _ => false,
        };
    }

    fn completed_cleanly(mut self) -> bool {
        if self.event_stream && !self.pending.is_empty() && !self.skipping_oversized_line {
            let line = std::mem::take(&mut self.pending);
            self.observe_line(&line);
        }
        !self.event_stream || self.terminal_success
    }
}

async fn read_bounded_error_body(
    response: reqwest::Response,
    limit: usize,
    idle_timeout: Duration,
) -> Vec<u8> {
    let mut body = Vec::with_capacity(limit);
    let mut stream = response.bytes_stream();
    while body.len() < limit {
        let Ok(Some(Ok(chunk))) = tokio::time::timeout(idle_timeout, stream.next()).await else {
            break;
        };
        let remaining = limit - body.len();
        let take = chunk.len().min(remaining);
        body.extend_from_slice(&chunk[..take]);
        if take == remaining {
            break;
        }
    }
    body
}

async fn send_with_header_timeout(
    request: reqwest::RequestBuilder,
    timeout: Duration,
) -> Result<reqwest::Response, HeaderSendError> {
    match tokio::time::timeout(timeout, request.send()).await {
        Ok(Ok(response)) => Ok(response),
        Ok(Err(error)) if error.is_connect() => Err(HeaderSendError::Connectivity),
        Ok(Err(_)) => Err(HeaderSendError::Other),
        Err(_) => Err(HeaderSendError::Timeout),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HeaderSendError {
    Connectivity,
    Timeout,
    Other,
}

async fn send_with_network_recovery(
    request: reqwest::RequestBuilder,
    timeout: Duration,
    origin: &crate::network_recovery::OriginKey,
    recovery_budget: Duration,
) -> Result<reqwest::Response, HeaderSendError> {
    let deadline = tokio::time::Instant::now() + recovery_budget;
    loop {
        let permit = crate::network_recovery::global_registry()
            .acquire(origin, deadline)
            .await
            .map_err(|_| HeaderSendError::Connectivity)?;
        let Some(attempt) = request.try_clone() else {
            drop(permit);
            return send_with_header_timeout(request, timeout).await;
        };
        match send_with_header_timeout(attempt, timeout).await {
            Ok(response) => {
                permit.success();
                return Ok(response);
            }
            Err(HeaderSendError::Connectivity) => {
                permit.failure();
                if tokio::time::Instant::now() >= deadline {
                    return Err(HeaderSendError::Connectivity);
                }
            }
            Err(error) => {
                // A timeout or non-connect reqwest failure does not prove the origin is offline.
                // Dropping a half-open probe safely reopens it; a normal permit is a no-op.
                drop(permit);
                return Err(error);
            }
        }
    }
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
        && value.len() <= 192
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'/' | b':' | b'~')
        })
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

fn per_token_price_to_per_million(value: Option<&serde_json::Value>) -> Option<f64> {
    let price = value.and_then(|value| {
        value
            .as_f64()
            .or_else(|| value.as_str().and_then(|value| value.parse::<f64>().ok()))
    })?;
    let per_million = price * 1_000_000.0;
    (price.is_finite() && price >= 0.0 && per_million.is_finite()).then_some(per_million)
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
                levels = reasoning_levels(
                    row.get("reasoning")
                        .and_then(|reasoning| reasoning.get("supported_efforts"))
                        .unwrap_or(&serde_json::Value::Null),
                );
            }
            if levels.is_empty() {
                levels.extend(
                    known_reasoning_levels(id)
                        .iter()
                        .map(|level| (*level).to_string()),
                );
            }
            let modalities = row
                .get("input_modalities")
                .or_else(|| {
                    row.get("architecture")
                        .and_then(|architecture| architecture.get("input_modalities"))
                })
                .and_then(serde_json::Value::as_array);
            let supports_vision = modalities
                .is_some_and(|values| values.iter().any(|value| value.as_str() == Some("image")));
            let supported_parameters = row
                .get("supported_parameters")
                .and_then(serde_json::Value::as_array);
            let supports_reasoning =
                supported_parameters.is_some_and(|parameters| {
                    parameters.iter().any(|parameter| {
                        matches!(
                            parameter.as_str(),
                            Some("reasoning" | "reasoning_effort" | "include_reasoning")
                        )
                    })
                }) || row.get("reasoning").is_some_and(|value| !value.is_null());
            let supports_tools = row
                .get("apply_patch_tool_type")
                .map(|value| !value.is_null())
                .or_else(|| {
                    supported_parameters.map(|parameters| {
                        parameters
                            .iter()
                            .any(|parameter| parameter.as_str() == Some("tools"))
                    })
                })
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
                    .or_else(|| row.get("name"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or(id)
                    .to_string(),
                context_window: optional_positive_i64(row.get("context_window"))
                    .or_else(|| optional_positive_i64(row.get("context_length")))
                    .or_else(|| {
                        optional_positive_i64(
                            row.get("metadata")
                                .and_then(|metadata| metadata.get("context_window")),
                        )
                    }),
                max_output_tokens: optional_positive_i64(row.get("max_output_tokens")).or_else(
                    || {
                        optional_positive_i64(
                            row.get("top_provider")
                                .and_then(|provider| provider.get("max_completion_tokens")),
                        )
                    },
                ),
                supports_tools,
                supports_vision,
                supports_parallel_tool_calls: row
                    .get("supports_parallel_tool_calls")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(true),
                supports_web_search,
                supports_reasoning,
                supports_reasoning_summaries,
                reasoning_levels: levels,
                input_per_million: per_token_price_to_per_million(
                    row.get("pricing").and_then(|pricing| pricing.get("prompt")),
                ),
                cached_input_per_million: per_token_price_to_per_million(
                    row.get("pricing")
                        .and_then(|pricing| pricing.get("input_cache_read")),
                ),
                output_per_million: per_token_price_to_per_million(
                    row.get("pricing")
                        .and_then(|pricing| pricing.get("completion")),
                ),
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
        acquire_credential(provider, &credentials, &HashSet::new(), None, unix_now())
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
    let mut request = client
        .get(endpoint)
        .header(header::ACCEPT, "application/json")
        .timeout(request_timeout);
    request = if provider.wire_api == "anthropic_messages" {
        request
            .header("x-api-key", &secret.0)
            .header("anthropic-version", "2023-06-01")
    } else {
        request.bearer_auth(&secret.0)
    };
    let response = request
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

fn profile_revision(model: &ProviderModel) -> Option<String> {
    if model.instruction_mode == "none"
        && model.instruction_text.is_empty()
        && model.request_overrides_json == "{}"
    {
        return None;
    }
    let mut hasher = Sha256::new();
    hasher.update(model.instruction_mode.as_bytes());
    hasher.update([0]);
    hasher.update(model.instruction_text.as_bytes());
    hasher.update([0]);
    hasher.update(model.request_overrides_json.as_bytes());
    Some(hex::encode(&hasher.finalize()[..8]))
}

fn apply_model_profile(
    object: &mut serde_json::Map<String, serde_json::Value>,
    model: &ProviderModel,
) -> Result<Option<String>, (StatusCode, &'static str)> {
    let revision = profile_revision(model);
    if revision.is_none() {
        return Ok(None);
    }

    if object
        .get("instructions")
        .is_some_and(|value| !value.is_null() && !value.is_string())
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "instructions must be a string for this model profile",
        ));
    }

    match model.instruction_mode.as_str() {
        "none" if model.instruction_text.is_empty() => {}
        "append" => {
            let instructions = match object.get("instructions") {
                Some(serde_json::Value::String(value)) => value.as_str(),
                None | Some(serde_json::Value::Null) => "",
                Some(_) => unreachable!("profile instruction shape validated above"),
            };
            let transformed = if instructions.is_empty() {
                model.instruction_text.clone()
            } else {
                format!(
                    "{instructions}{PROFILE_SEPARATOR}{}",
                    model.instruction_text
                )
            };
            object.insert("instructions".into(), transformed.into());
        }
        "replace" => {
            object.insert("instructions".into(), model.instruction_text.clone().into());
        }
        _ => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "invalid provider model profile",
            ));
        }
    }

    let overrides: ProfileRequestOverrides = serde_json::from_str(&model.request_overrides_json)
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "invalid provider model profile",
            )
        })?;
    if let Some(effort) = overrides.reasoning_effort {
        let reasoning = object
            .entry("reasoning")
            .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
        let Some(reasoning) = reasoning.as_object_mut() else {
            return Err((
                StatusCode::BAD_REQUEST,
                "reasoning must be an object for this model profile",
            ));
        };
        reasoning.insert("effort".into(), effort.into());
    }
    if let Some(max_output_tokens) = overrides.max_output_tokens {
        object.insert("max_output_tokens".into(), max_output_tokens.into());
    }
    Ok(revision)
}

fn apply_anthropic_model_profile(
    object: &mut serde_json::Map<String, serde_json::Value>,
    model: &ProviderModel,
) -> Result<Option<String>, (StatusCode, &'static str)> {
    let revision = profile_revision(model);
    if revision.is_none() {
        return Ok(None);
    }
    if object
        .get("system")
        .is_some_and(|value| !value.is_null() && !value.is_string())
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "system must be a string for this model profile",
        ));
    }
    match model.instruction_mode.as_str() {
        "none" if model.instruction_text.is_empty() => {}
        "append" => {
            let system = object
                .get("system")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            let transformed = if system.is_empty() {
                model.instruction_text.clone()
            } else {
                format!("{system}{PROFILE_SEPARATOR}{}", model.instruction_text)
            };
            object.insert("system".into(), transformed.into());
        }
        "replace" => {
            object.insert("system".into(), model.instruction_text.clone().into());
        }
        _ => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "invalid provider model profile",
            ));
        }
    }
    let overrides: ProfileRequestOverrides = serde_json::from_str(&model.request_overrides_json)
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "invalid provider model profile",
            )
        })?;
    if overrides.reasoning_effort.is_some() {
        return Err((
            StatusCode::BAD_REQUEST,
            "reasoning effort is unavailable for Anthropic Messages providers",
        ));
    }
    if let Some(max_output_tokens) = overrides.max_output_tokens {
        object.insert("max_tokens".into(), max_output_tokens.into());
    }
    Ok(revision)
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

fn is_event_stream_content_type(headers: &reqwest::header::HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("text/event-stream"))
}

pub async fn execute(
    store: &Store,
    cipher: &TokenCipher,
    provider: CustomProvider,
    model: ProviderModel,
    inbound_headers: &HeaderMap,
    raw_body: &Bytes,
    recovery_budget: Duration,
) -> (Response, CustomRouteOutcome) {
    let affinity_key = request_affinity_key(
        inbound_headers,
        raw_body,
        &model.public_model,
        &provider.wire_api,
        None,
    );
    let preferred_credential_id = current_affinity(affinity_key.as_deref())
        .filter(|target| target.provider_id == provider.id)
        .map(|target| target.credential_id);
    execute_with_affinity(
        store,
        cipher,
        provider,
        model,
        inbound_headers,
        raw_body,
        affinity_key,
        preferred_credential_id,
        recovery_budget,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn execute_with_affinity(
    store: &Store,
    cipher: &TokenCipher,
    provider: CustomProvider,
    model: ProviderModel,
    inbound_headers: &HeaderMap,
    raw_body: &Bytes,
    affinity_key: Option<String>,
    preferred_credential_id: Option<String>,
    recovery_budget: Duration,
) -> (Response, CustomRouteOutcome) {
    let mut outcome = custom_route_outcome(&provider, &model);
    let endpoint = match validate_endpoint(&provider) {
        Ok(endpoint) => endpoint,
        Err(message) => return ((StatusCode::BAD_GATEWAY, message).into_response(), outcome),
    };
    let origin = match crate::network_recovery::OriginKey::parse(endpoint.as_str()) {
        Ok(origin) => origin,
        Err(_) => {
            return (
                (StatusCode::BAD_GATEWAY, "invalid provider origin").into_response(),
                outcome,
            )
        }
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
    if provider.wire_api == "responses" && provider.stateless_responses {
        object.remove("previous_response_id");
    }
    outcome.profile_revision = match if provider.wire_api == "anthropic_messages" {
        apply_anthropic_model_profile(object, &model)
    } else {
        apply_model_profile(object, &model)
    } {
        Ok(revision) => revision,
        Err((status, message)) => return ((status, message).into_response(), outcome),
    };
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
    let idle_timeout = Duration::from_millis(
        u64::try_from(provider.stream_idle_timeout_ms.max(1_000)).unwrap_or(300_000),
    );
    let mut last_response: Option<Response> = None;

    for _ in 0..max_attempts {
        let Some((credential, lease)) = acquire_credential(
            &provider,
            &credentials,
            &tried,
            preferred_credential_id.as_deref(),
            unix_now(),
        ) else {
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
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::ACCEPT, "text/event-stream")
            .body(encoded.clone());
        if provider.wire_api == "anthropic_messages" {
            request = request.header("x-api-key", &secret.0).header(
                "anthropic-version",
                inbound_headers
                    .get("anthropic-version")
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or("2023-06-01"),
            );
            if let Some(value) = inbound_headers.get("anthropic-beta") {
                request = request.header("anthropic-beta", value);
            }
        } else {
            request = request.bearer_auth(&secret.0);
            if let Some(value) = inbound_headers.get("openai-beta") {
                request = request.header("openai-beta", value);
            }
        }
        let upstream =
            match send_with_network_recovery(request, idle_timeout, &origin, recovery_budget).await
            {
                Ok(response) => response,
                Err(HeaderSendError::Connectivity) => {
                    // Origin-wide transport loss is not credential health. The per-origin circuit
                    // already waited and retried this same credential; do not rotate or cool it down.
                    last_response = Some(
                        (
                            StatusCode::SERVICE_UNAVAILABLE,
                            "upstream network unavailable",
                        )
                            .into_response(),
                    );
                    drop(lease);
                    break;
                }
                Err(HeaderSendError::Timeout | HeaderSendError::Other) => {
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
            let bytes = read_bounded_error_body(upstream, MAX_ERROR_BODY_BYTES, idle_timeout).await;
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

        let headers = upstream.headers().clone();
        let event_stream = is_event_stream_content_type(&headers);
        let mut bytes = upstream.bytes_stream();
        let first_chunk = match tokio::time::timeout(idle_timeout, bytes.next()).await {
            Ok(Some(Ok(chunk))) => chunk,
            Ok(Some(Err(_))) | Ok(None) | Err(_) => {
                let _ = store
                    .providers()
                    .set_credential_health(
                        &credential.id,
                        "cooldown",
                        Some(unix_now() + 30),
                        unix_now(),
                    )
                    .await;
                last_response = Some(
                    (
                        StatusCode::BAD_GATEWAY,
                        "custom provider closed before output",
                    )
                        .into_response(),
                );
                drop(lease);
                continue;
            }
        };
        let _ = store
            .providers()
            .set_credential_health(&credential.id, "healthy", None, unix_now())
            .await;
        let mut builder = Response::builder().status(status);
        copy_response_headers(&headers, &mut builder);
        let credential_id = credential.id.clone();
        let affinity_credential_id = credential.id.clone();
        let affinity_provider_id = provider.id.clone();
        let affinity_key = affinity_key.clone();
        let mut completion = CompletionObserver::new(&provider.wire_api, event_stream);
        completion.observe(&first_chunk);
        let store = store.clone();
        let stream = async_stream::stream! {
            let _lease = lease;
            yield Ok::<Bytes, io::Error>(first_chunk);
            let mut clean_eof = false;
            loop {
                match tokio::time::timeout(idle_timeout, bytes.next()).await {
                    Ok(Some(Ok(chunk))) => {
                        completion.observe(&chunk);
                        yield Ok::<Bytes, io::Error>(chunk);
                    }
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
                    Ok(None) => {
                        clean_eof = true;
                        break;
                    }
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
            if clean_eof && completion.completed_cleanly() {
                record_affinity(
                    affinity_key,
                    affinity_provider_id,
                    affinity_credential_id,
                );
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

fn custom_route_outcome(provider: &CustomProvider, model: &ProviderModel) -> CustomRouteOutcome {
    CustomRouteOutcome {
        provider_slug: provider.slug.clone(),
        credential_id: None,
        public_model: model.public_model.clone(),
        upstream_model: model.upstream_model.clone(),
        upstream_transport: "http_sse".into(),
        effective_service_tier: None,
        profile_revision: profile_revision(model),
        input_per_million: model.input_per_million,
        cached_input_per_million: model.cached_input_per_million,
        output_per_million: model.output_per_million,
    }
}

fn unresolved_custom_outcome(public_model: String) -> CustomRouteOutcome {
    CustomRouteOutcome {
        provider_slug: "custom".into(),
        credential_id: None,
        public_model,
        upstream_model: String::new(),
        upstream_transport: "http_sse".into(),
        effective_service_tier: None,
        profile_revision: None,
        input_per_million: None,
        cached_input_per_million: None,
        output_per_million: None,
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn execute_targets(
    store: &Store,
    cipher: &TokenCipher,
    targets: Vec<(CustomProvider, ProviderModel)>,
    wire_api: &str,
    inbound_headers: &HeaderMap,
    raw_body: &Bytes,
    affinity_identity_override: Option<&str>,
    recovery_budget: Duration,
) -> (Response, CustomRouteOutcome) {
    let Some(public_model) = targets.first().map(|(_, model)| model.public_model.clone()) else {
        return (
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "no compatible provider target",
            )
                .into_response(),
            unresolved_custom_outcome(String::new()),
        );
    };
    let required = request_capabilities(raw_body, wire_api);
    // A public model may have targets on independent origins. In that case a dead origin gets one
    // immediate attempt and the request can fall through to a working origin; the opened circuit
    // still protects subsequent requests and admits one recovery probe. With only one origin,
    // retain the full recovery wait so a brief WAN/provider outage heals in place.
    let has_origin_fallback = targets
        .iter()
        .filter_map(|(provider, _)| {
            crate::network_recovery::OriginKey::parse(&provider.base_url).ok()
        })
        .collect::<HashSet<_>>()
        .len()
        > 1;
    let affinity_key = request_affinity_key(
        inbound_headers,
        raw_body,
        &public_model,
        wire_api,
        affinity_identity_override,
    );
    let affinity = current_affinity(affinity_key.as_deref());
    let mut last_priority = None;
    if required.priority {
        let ranked = rank_eligible_targets(
            store,
            targets.clone(),
            wire_api,
            required,
            affinity.as_ref(),
            affinity_key.as_deref(),
        )
        .await;
        for (index, (provider, model, preferred_credential_id)) in ranked.into_iter().enumerate() {
            let provider_slug = provider.slug.clone();
            let public_model = model.public_model.clone();
            let target_recovery_budget = if has_origin_fallback {
                Duration::ZERO
            } else {
                recovery_budget
            };
            let mut result = execute_with_affinity(
                store,
                cipher,
                provider,
                model,
                inbound_headers,
                raw_body,
                affinity_key.clone(),
                preferred_credential_id,
                target_recovery_budget,
            )
            .await;
            result.1.effective_service_tier = Some("priority".into());
            if result.0.status().is_success() || !retryable_status(result.0.status()) {
                return result;
            }
            tracing::debug!(
                target: "polyflare_server::routing",
                provider = %provider_slug,
                model = %public_model,
                status = result.0.status().as_u16(),
                target_attempt = index + 1,
                requested_tier = "priority",
                "custom Priority target failed before output; trying another Priority target"
            );
            last_priority = Some(result);
        }

        let Some(standard_body) = without_priority_service_tier(raw_body) else {
            return last_priority.unwrap_or_else(|| {
                (
                    (StatusCode::BAD_REQUEST, "invalid Priority fallback request").into_response(),
                    unresolved_custom_outcome(public_model),
                )
            });
        };
        let standard_required = RequestCapabilities {
            priority: false,
            ..required
        };
        let ranked = rank_eligible_targets(
            store,
            targets,
            wire_api,
            standard_required,
            affinity.as_ref(),
            affinity_key.as_deref(),
        )
        .await;
        let mut last_standard = None;
        for (index, (provider, model, preferred_credential_id)) in ranked.into_iter().enumerate() {
            let provider_slug = provider.slug.clone();
            let public_model = model.public_model.clone();
            let target_recovery_budget = if has_origin_fallback {
                Duration::ZERO
            } else {
                recovery_budget
            };
            let mut result = execute_with_affinity(
                store,
                cipher,
                provider,
                model,
                inbound_headers,
                &standard_body,
                affinity_key.clone(),
                preferred_credential_id,
                target_recovery_budget,
            )
            .await;
            result.1.effective_service_tier = Some("standard".into());
            if result.0.status().is_success() || !retryable_status(result.0.status()) {
                tracing::debug!(
                    target: "polyflare_server::routing",
                    provider = %provider_slug,
                    model = %public_model,
                    status = result.0.status().as_u16(),
                    requested_tier = "priority",
                    effective_tier = "standard",
                    "custom Priority request downgraded to a Standard target"
                );
                return result;
            }
            tracing::debug!(
                target: "polyflare_server::routing",
                provider = %provider_slug,
                model = %public_model,
                status = result.0.status().as_u16(),
                target_attempt = index + 1,
                requested_tier = "priority",
                effective_tier = "standard",
                "custom Standard fallback target failed before output; trying another target"
            );
            last_standard = Some(result);
        }
        return last_standard.or(last_priority).unwrap_or_else(|| {
            (
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "no compatible provider target",
                )
                    .into_response(),
                unresolved_custom_outcome(public_model),
            )
        });
    }

    let ranked = rank_eligible_targets(
        store,
        targets,
        wire_api,
        required,
        affinity.as_ref(),
        affinity_key.as_deref(),
    )
    .await;
    let mut last = None;
    for (index, (provider, model, preferred_credential_id)) in ranked.into_iter().enumerate() {
        let provider_slug = provider.slug.clone();
        let public_model = model.public_model.clone();
        let target_recovery_budget = if has_origin_fallback {
            Duration::ZERO
        } else {
            recovery_budget
        };
        let result = execute_with_affinity(
            store,
            cipher,
            provider,
            model,
            inbound_headers,
            raw_body,
            affinity_key.clone(),
            preferred_credential_id,
            target_recovery_budget,
        )
        .await;
        if result.0.status().is_success() || !retryable_status(result.0.status()) {
            return result;
        }
        tracing::debug!(
            target: "polyflare_server::routing",
            provider = %provider_slug,
            model = %public_model,
            status = result.0.status().as_u16(),
            target_attempt = index + 1,
            "custom provider target failed before output; trying another eligible target"
        );
        last = Some(result);
    }
    last.unwrap_or_else(|| {
        (
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "no compatible provider target",
            )
                .into_response(),
            unresolved_custom_outcome(public_model),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn anthropic_provider() -> CustomProvider {
        CustomProvider {
            id: "anthropic-compatible".into(),
            slug: "anthropic-compatible".into(),
            display_name: "Anthropic compatible".into(),
            base_url: "https://example.com/v1".into(),
            wire_api: "anthropic_messages".into(),
            enabled: true,
            stateless_responses: false,
            allow_private_hosts: false,
            connect_timeout_ms: 1_000,
            stream_idle_timeout_ms: 1_000,
            request_max_retries: 0,
            max_concurrency: None,
            created_at: 0,
            updated_at: 0,
        }
    }

    fn credential(id: &str, weight: f64, max_concurrency: Option<i64>) -> ProviderCredential {
        ProviderCredential {
            id: id.into(),
            provider_id: "provider".into(),
            label: id.into(),
            enabled: true,
            health_status: "healthy".into(),
            routing_weight: weight,
            max_concurrency,
            cooldown_until: None,
            last_error_at: None,
            created_at: 0,
            updated_at: 0,
        }
    }

    #[test]
    fn shared_credential_ranker_honors_weight_cooldown_and_concurrency() {
        let mut provider = anthropic_provider();
        provider.id = "provider".into();
        let mut credentials = vec![
            credential("credential-a", 1.0, Some(3)),
            credential("credential-b", 4.0, Some(3)),
        ];
        let tried = HashSet::new();

        let empty = HashMap::new();
        assert_eq!(
            best_credential(&provider, &credentials, &tried, None, 100, &empty)
                .unwrap()
                .0
                .id,
            "credential-a",
            "an idle tie is deterministic"
        );

        let loaded = HashMap::from([
            ("credential-a".to_string(), 1),
            ("credential-b".to_string(), 1),
        ]);
        assert_eq!(
            best_credential(&provider, &credentials, &tried, None, 100, &loaded)
                .unwrap()
                .0
                .id,
            "credential-b",
            "load is normalized by routing weight"
        );

        credentials[1].health_status = "cooldown".into();
        credentials[1].cooldown_until = Some(101);
        assert_eq!(
            best_credential(&provider, &credentials, &tried, None, 100, &loaded)
                .unwrap()
                .0
                .id,
            "credential-a"
        );
        credentials[1].cooldown_until = Some(100);
        assert_eq!(
            best_credential(&provider, &credentials, &tried, None, 100, &loaded)
                .unwrap()
                .0
                .id,
            "credential-b",
            "an expired cooldown re-enters selection"
        );

        credentials[1].max_concurrency = Some(1);
        assert_eq!(
            best_credential(&provider, &credentials, &tried, None, 100, &loaded)
                .unwrap()
                .0
                .id,
            "credential-a",
            "a saturated credential is excluded"
        );
        provider.max_concurrency = Some(2);
        assert!(
            best_credential(&provider, &credentials, &tried, None, 100, &loaded).is_none(),
            "provider-level saturation excludes the whole target"
        );
    }

    #[test]
    fn credential_affinity_is_soft_and_never_overrides_eligibility() {
        let mut provider = anthropic_provider();
        provider.id = "provider".into();
        let mut credentials = vec![
            credential("credential-a", 4.0, Some(3)),
            credential("credential-b", 1.0, Some(3)),
        ];
        let loaded = HashMap::from([
            ("credential-a".to_string(), 0),
            ("credential-b".to_string(), 2),
        ]);
        assert_eq!(
            best_credential(
                &provider,
                &credentials,
                &HashSet::new(),
                Some("credential-b"),
                100,
                &loaded,
            )
            .unwrap()
            .0
            .id,
            "credential-b",
            "a healthy affine credential wins over weighted load"
        );

        credentials[1].health_status = "cooldown".into();
        credentials[1].cooldown_until = Some(101);
        assert_eq!(
            best_credential(
                &provider,
                &credentials,
                &HashSet::new(),
                Some("credential-b"),
                100,
                &loaded,
            )
            .unwrap()
            .0
            .id,
            "credential-a",
            "an unavailable affine credential must fall back normally"
        );
    }

    #[test]
    fn affinity_cache_expires_and_evicts_the_oldest_success() {
        let start = Instant::now();
        let mut cache = AffinityCache::new(Duration::from_secs(10), 2);
        cache.record(
            "a".into(),
            "provider-a".into(),
            "credential-a".into(),
            start,
        );
        cache.record(
            "b".into(),
            "provider-b".into(),
            "credential-b".into(),
            start + Duration::from_secs(1),
        );
        cache.record(
            "c".into(),
            "provider-c".into(),
            "credential-c".into(),
            start + Duration::from_secs(2),
        );
        assert!(cache.get("a", start + Duration::from_secs(2)).is_none());
        assert!(cache.get("b", start + Duration::from_secs(2)).is_some());
        assert!(cache.get("c", start + Duration::from_secs(2)).is_some());
        assert!(
            cache.get("b", start + Duration::from_secs(12)).is_none(),
            "entries expire from their last successful completion"
        );
    }

    #[test]
    fn anthropic_affinity_accepts_only_the_prevalidated_protocol_override() {
        let headers = HeaderMap::new();
        let body = Bytes::from_static(
            br#"{"prompt_cache_key":"codex-shaped","messages":[{"role":"user","content":"hi"}]}"#,
        );
        assert!(
            request_affinity_key(&headers, &body, "shared-model", "anthropic_messages", None,)
                .is_none(),
            "generic Anthropic traffic must not borrow Codex affinity inputs"
        );
        assert!(request_affinity_key(
            &headers,
            &body,
            "shared-model",
            "anthropic_messages",
            Some("validated-session-hash"),
        )
        .is_some());
        assert!(request_affinity_key(&headers, &body, "shared-model", "responses", None).is_some());
    }

    #[test]
    fn anonymous_smooth_weighted_routing_is_exact_and_bounded_by_scope() {
        let now = Instant::now();
        let mut cache = SmoothWeightedCache::new(Duration::from_secs(10), 2);
        let candidates = vec![("a".to_string(), 3.0), ("b".to_string(), 1.0)];
        let selected = (0..8)
            .map(|_| cache.select("scope".into(), &candidates, now).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(selected.iter().filter(|id| id.as_str() == "a").count(), 6);
        assert_eq!(selected.iter().filter(|id| id.as_str() == "b").count(), 2);

        cache
            .select("second".into(), &candidates, now + Duration::from_secs(1))
            .unwrap();
        cache
            .select(
                "third".into(),
                &[("c".to_string(), 1.0)],
                now + Duration::from_secs(2),
            )
            .unwrap();
        assert_eq!(cache.entries.len(), 2);
        assert!(!cache.entries.contains_key("scope"));
    }

    #[test]
    fn affinity_completion_requires_a_protocol_terminal_for_event_streams() {
        let mut responses = CompletionObserver::new("responses", true);
        responses.observe(b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\n");
        assert!(!responses.completed_cleanly());

        let mut completed = CompletionObserver::new("responses", true);
        completed
            .observe(b"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\"}}\n\n");
        assert!(completed.completed_cleanly());

        let mut anthropic = CompletionObserver::new("anthropic_messages", true);
        anthropic.observe(b"data: {\"type\":\"message_stop\"}\n\n");
        assert!(anthropic.completed_cleanly());
        assert!(
            CompletionObserver::new("responses", false).completed_cleanly(),
            "a clean finite JSON response is itself terminal"
        );

        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            reqwest::header::HeaderValue::from_static("Text/Event-Stream; charset=utf-8"),
        );
        assert!(is_event_stream_content_type(&headers));
    }

    #[test]
    fn capability_detection_distinguishes_image_parts_from_structured_tool_output() {
        let tool_output = Bytes::from_static(
            br#"{"input":[{"type":"function_call_output","output":{"image_url":"text"}}]}"#,
        );
        assert!(!request_capabilities(&tool_output, "responses").vision);

        let image = Bytes::from_static(
            br#"{"input":[{"role":"user","content":[{"type":"input_image","image_url":"x"}]}]}"#,
        );
        assert!(request_capabilities(&image, "responses").vision);
    }

    #[test]
    fn anthropic_capability_detection_only_inspects_message_content_blocks() {
        let direct = Bytes::from_static(
            br#"{"messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"opaque"}}]}]}"#,
        );
        assert!(
            request_capabilities(&direct, "anthropic_messages").vision,
            "a direct Anthropic image content block requires vision"
        );

        let nested = Bytes::from_static(
            br#"{"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool-1","content":[{"type":"tool_result","tool_use_id":"tool-2","content":[{"type":"image","source":{"type":"base64","data":"opaque"}}]}]}]}]}"#,
        );
        assert!(
            request_capabilities(&nested, "anthropic_messages").vision,
            "tool_result content may recursively contain image blocks"
        );

        for not_content in [
            br#"{"messages":[{"role":"user","content":[{"type":"text","text":"image","source":{"type":"image"},"data":{"type":"image"}}]}]}"#
                .as_slice(),
            br#"{"messages":[{"role":"user","content":[{"type":"tool_use","name":"inspect","input":{"type":"image"}}]}]}"#
                .as_slice(),
            br#"{"messages":[{"role":"user","content":"plain text"}],"system":[{"type":"image"}]}"#
                .as_slice(),
        ] {
            assert!(
                !request_capabilities(
                    &Bytes::copy_from_slice(not_content),
                    "anthropic_messages"
                )
                .vision,
                "image-shaped values outside Messages content blocks are not vision inputs"
            );
        }
    }

    #[test]
    fn anthropic_provider_uses_messages_endpoint_and_protocol_profile_fields() {
        let provider = anthropic_provider();
        assert_eq!(
            validate_endpoint(&provider).unwrap().as_str(),
            "https://example.com/v1/messages"
        );
        let model = ProviderModel {
            id: "m".into(),
            provider_id: provider.id,
            public_model: "claude-profile".into(),
            upstream_model: "claude-upstream".into(),
            display_name: "Claude".into(),
            context_window: None,
            max_output_tokens: None,
            supports_tools: true,
            supports_vision: true,
            supports_parallel_tool_calls: true,
            supports_web_search: false,
            supports_reasoning_summaries: false,
            reasoning_levels_json: "[]".into(),
            model_info_json: None,
            instruction_mode: "append".into(),
            instruction_text: "Provider guidance".into(),
            request_overrides_json: r#"{"max_output_tokens":123}"#.into(),
            input_per_million: None,
            cached_input_per_million: None,
            output_per_million: None,
            visible_in_codex: true,
            visible_in_openai: true,
            enabled: true,
            created_at: 0,
            updated_at: 0,
        };
        let mut body = serde_json::json!({"system":"Client guidance"})
            .as_object()
            .unwrap()
            .clone();
        assert!(apply_anthropic_model_profile(&mut body, &model)
            .unwrap()
            .is_some());
        assert!(body["system"]
            .as_str()
            .unwrap()
            .contains("Provider guidance"));
        assert_eq!(body["max_tokens"], 123);
        assert!(body.get("instructions").is_none());
        assert!(body.get("max_output_tokens").is_none());
    }

    #[tokio::test]
    async fn error_body_reader_stops_at_limit_without_waiting_for_eof() {
        async fn hanging_error() -> Response {
            let stream = async_stream::stream! {
                yield Ok::<Bytes, std::convert::Infallible>(Bytes::from(vec![
                    b'x';
                    MAX_ERROR_BODY_BYTES + 1
                ]));
                std::future::pending::<()>().await;
            };
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from_stream(stream))
                .unwrap()
        }

        let app = axum::Router::new().route("/error", axum::routing::get(hanging_error));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let response = reqwest::get(format!("http://{address}/error"))
            .await
            .unwrap();

        let body = tokio::time::timeout(
            Duration::from_secs(1),
            read_bounded_error_body(response, MAX_ERROR_BODY_BYTES, Duration::from_millis(100)),
        )
        .await
        .expect("the bounded reader must not wait for the upstream body to finish");
        assert_eq!(body.len(), MAX_ERROR_BODY_BYTES);
    }

    #[tokio::test]
    async fn error_body_reader_honors_idle_timeout_before_first_byte() {
        async fn silent_error() -> Response {
            let stream = async_stream::stream! {
                std::future::pending::<()>().await;
                #[allow(unreachable_code)]
                yield Ok::<Bytes, std::convert::Infallible>(Bytes::new());
            };
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from_stream(stream))
                .unwrap()
        }

        let app = axum::Router::new().route("/error", axum::routing::get(silent_error));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let response = reqwest::get(format!("http://{address}/error"))
            .await
            .unwrap();

        let started = std::time::Instant::now();
        let body =
            read_bounded_error_body(response, MAX_ERROR_BODY_BYTES, Duration::from_millis(50))
                .await;
        assert!(body.is_empty());
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "the configured idle timeout must bound a silent error body"
        );
    }

    #[tokio::test]
    async fn provider_send_is_bounded_while_waiting_for_response_headers() {
        async fn silent_headers() -> Response {
            std::future::pending::<Response>().await
        }

        let app = axum::Router::new().route("/responses", axum::routing::post(silent_headers));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let request = reqwest::Client::new().post(format!("http://{address}/responses"));

        let started = std::time::Instant::now();
        let result = send_with_header_timeout(request, Duration::from_millis(50)).await;
        assert!(result.is_err());
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "the provider stream-idle budget must also bound response headers"
        );
    }

    #[tokio::test]
    async fn provider_http_client_cache_replaces_stale_generations_and_can_be_evicted() {
        let mut provider = CustomProvider {
            id: "cache-replacement-test".into(),
            slug: "cache-test".into(),
            display_name: "Cache test".into(),
            base_url: "http://127.0.0.1:9999/v1".into(),
            wire_api: "responses".into(),
            enabled: true,
            stateless_responses: true,
            allow_private_hosts: true,
            connect_timeout_ms: 1000,
            stream_idle_timeout_ms: 1000,
            request_max_retries: 0,
            max_concurrency: None,
            created_at: 0,
            updated_at: 1,
        };
        let endpoint = validate_endpoint(&provider).unwrap();
        evict_provider_client(&provider.id);
        http_client(&provider, &endpoint).await.unwrap();
        provider.updated_at = 2;
        http_client(&provider, &endpoint).await.unwrap();

        let generations: Vec<_> = HTTP_CLIENTS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .keys()
            .filter(|(provider_id, _)| provider_id == &provider.id)
            .cloned()
            .collect();
        assert_eq!(generations, vec![(provider.id.clone(), 2)]);

        evict_provider_client(&provider.id);
        assert!(HTTP_CLIENTS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .keys()
            .all(|(provider_id, _)| provider_id != &provider.id));
    }

    #[tokio::test]
    async fn provider_http_client_cache_expires_so_dns_can_rotate() {
        let provider = CustomProvider {
            id: "cache-expiry-test".into(),
            slug: "cache-expiry".into(),
            display_name: "Cache expiry".into(),
            base_url: "http://127.0.0.1:9999/v1".into(),
            wire_api: "responses".into(),
            enabled: true,
            stateless_responses: true,
            allow_private_hosts: true,
            connect_timeout_ms: 1000,
            stream_idle_timeout_ms: 1000,
            request_max_retries: 0,
            max_concurrency: None,
            created_at: 0,
            updated_at: 1,
        };
        let endpoint = validate_endpoint(&provider).unwrap();
        let first_now = Instant::now();
        evict_provider_client(&provider.id);
        http_client_at(&provider, &endpoint, first_now)
            .await
            .unwrap();
        let first_expiry = HTTP_CLIENTS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&(provider.id.clone(), provider.updated_at))
            .unwrap()
            .expires_at;

        http_client_at(
            &provider,
            &endpoint,
            first_now + HTTP_CLIENT_DNS_TTL + Duration::from_secs(1),
        )
        .await
        .unwrap();
        let refreshed_expiry = HTTP_CLIENTS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&(provider.id.clone(), provider.updated_at))
            .unwrap()
            .expires_at;
        assert!(
            refreshed_expiry > first_expiry,
            "an expired pinned client must be rebuilt so its hostname is resolved again"
        );
        evict_provider_client(&provider.id);
    }

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
    fn endpoint_validation_rejects_non_global_special_use_hosts() {
        let mut provider = CustomProvider {
            id: "p".into(),
            slug: "special".into(),
            display_name: "Special-use target".into(),
            base_url: String::new(),
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

        for base_url in [
            "https://100.64.0.1/v1",
            "https://198.18.0.1/v1",
            "https://224.0.0.1/v1",
            "https://[ff02::1]/v1",
            "https://[2001:db8::1]/v1",
        ] {
            provider.base_url = base_url.into();
            assert!(
                validate_endpoint(&provider).is_err(),
                "{base_url} must not pass the public-provider SSRF boundary"
            );
        }
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
