//! Stock Codex account-backend gateway.
//!
//! codex-rs intentionally keeps account usage separate from the configured model provider:
//! `account/rateLimits/read` calls `{chatgpt_base_url}/wham/usage`. Pointing that global base at
//! PolyFlare therefore requires two behaviors on one boundary:
//!
//! - synthesize only `wham/usage` from the local aggregate pool quota;
//! - transparently forward every other ChatGPT backend request with the client's own auth.
//!
//! Passthrough never selects an account, decrypts a token, or reads the store. It reuses the
//! process-wide pinned-TLS HTTP client and streams request and response bodies. Completion
//! telemetry records only a normalized route label, method, status, and latency.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderName, Method, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use futures_util::StreamExt;
use polyflare_core::Provider;

use crate::app::AppState;
use crate::ingress::queue_persist_request_log;
use crate::observability::RequestLog;

const WEEKLY_MINUTES: i64 = 10_080;
const FIVE_HOUR_MINUTES: i64 = 300;
const SYNTHETIC_USAGE_LOG_PATH: &str = "chatgpt_backend_synthetic_wham/usage";

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

fn method_label(method: &Method) -> &'static str {
    match *method {
        Method::GET => "GET",
        Method::POST => "POST",
        Method::PUT => "PUT",
        Method::PATCH => "PATCH",
        Method::DELETE => "DELETE",
        Method::HEAD => "HEAD",
        Method::OPTIONS => "OPTIONS",
        _ => "OTHER",
    }
}

fn record_gateway_request(
    state: &AppState,
    method: &'static str,
    path: String,
    status: StatusCode,
    started: Instant,
) {
    let request_id = format!("{:032x}", rand::random::<u128>());
    let log = RequestLog {
        method,
        path,
        provider: Provider::Codex.to_string(),
        aliased: false,
        status,
        duration_ms: started.elapsed().as_millis() as u64,
        account_id: None,
        target_kind: None,
        provider_credential_id: None,
        model: None,
        upstream_model: None,
        upstream_transport: Some("http".to_string()),
        reasoning_effort: None,
        service_tier: None,
        transport: Some("http".to_string()),
        ttft_ms: None,
        total_tokens: None,
        cached_tokens: None,
        subagent: None,
        request_id: Some(request_id),
        session_key: None,
    };
    log.emit();
    state.log_bus.publish(log.to_log_event());
    state
        .upstream_request_metrics
        .record_target("codex", "account", None, status.as_u16());
    queue_persist_request_log(&state.store, log.record(unix_now()));
}

fn error_response(status: StatusCode, code: &'static str) -> Response {
    (
        status,
        [("content-type", "application/json")],
        serde_json::json!({"error": {"code": code}}).to_string(),
    )
        .into_response()
}

fn is_authenticated(headers: &HeaderMap) -> bool {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .is_some_and(|value| !value.as_bytes().is_empty())
}

fn bounded_i32(value: i64) -> i32 {
    value.clamp(0, i64::from(i32::MAX)) as i32
}

fn wham_window(
    window: &crate::pool_quota::SyntheticQuotaWindow,
    reset_at: i64,
    now: i64,
) -> serde_json::Value {
    serde_json::json!({
        "used_percent": window.used_percent.round().clamp(0.0, 100.0) as i32,
        "limit_window_seconds": bounded_i32(window.window_minutes.saturating_mul(60)),
        "reset_after_seconds": bounded_i32(reset_at.saturating_sub(now)),
        "reset_at": bounded_i32(reset_at),
    })
}

fn wham_usage_payload(
    quota: &crate::pool_quota::SyntheticPoolQuota,
    primary_reset_at: Option<i64>,
    secondary_reset_at: i64,
    now: i64,
) -> serde_json::Value {
    let mut rate_limit = serde_json::Map::new();
    let limit_reached = quota.secondary.used_percent >= 100.0
        || quota
            .primary
            .as_ref()
            .is_some_and(|window| window.used_percent >= 100.0);
    rate_limit.insert("allowed".to_string(), serde_json::json!(!limit_reached));
    rate_limit.insert(
        "limit_reached".to_string(),
        serde_json::json!(limit_reached),
    );
    if let (Some(window), Some(reset_at)) = (&quota.primary, primary_reset_at) {
        rate_limit.insert(
            "primary_window".to_string(),
            wham_window(window, reset_at, now),
        );
    }
    rate_limit.insert(
        "secondary_window".to_string(),
        wham_window(&quota.secondary, secondary_reset_at, now),
    );

    serde_json::json!({
        // The field is mandatory in codex-rs's WHAM schema. Aggregation weights still come from
        // every member's real plan/capacity; this value is display/protocol metadata only.
        "plan_type": "pro",
        "rate_limit": rate_limit,
        // Pool quota cannot truthfully promise that an individual account reset credit resets the
        // aggregate. Suppress the reset action instead of exposing a misleading operation.
        "rate_limit_reset_credits": {"available_count": 0},
    })
}

async fn usage_route(state: Arc<AppState>, pool: Option<String>, headers: HeaderMap) -> Response {
    let started = Instant::now();
    let method = "GET";
    let response = if !is_authenticated(&headers) {
        error_response(StatusCode::UNAUTHORIZED, "chatgpt_auth_required")
    } else {
        match state.account_cache.snapshots(&state.store).await {
            Err(_) => error_response(StatusCode::SERVICE_UNAVAILABLE, "pool_usage_unavailable"),
            Ok(snapshots) => {
                let quota = crate::pool_quota::synthesize(
                    &snapshots,
                    Provider::Codex,
                    pool.as_deref(),
                    false,
                );
                let secondary_reset_at = crate::pool_quota::conservative_reset_at(
                    &snapshots,
                    Provider::Codex,
                    pool.as_deref(),
                    false,
                    WEEKLY_MINUTES,
                );
                match (quota, secondary_reset_at) {
                    (Some(quota), Some(secondary_reset_at)) => {
                        let primary_reset_at = quota.primary.as_ref().and_then(|_| {
                            crate::pool_quota::conservative_reset_at(
                                &snapshots,
                                Provider::Codex,
                                pool.as_deref(),
                                false,
                                FIVE_HOUR_MINUTES,
                            )
                        });
                        axum::Json(wham_usage_payload(
                            &quota,
                            primary_reset_at,
                            secondary_reset_at,
                            unix_now(),
                        ))
                        .into_response()
                    }
                    _ => error_response(StatusCode::SERVICE_UNAVAILABLE, "pool_usage_unavailable"),
                }
            }
        }
    };
    record_gateway_request(
        &state,
        method,
        SYNTHETIC_USAGE_LOG_PATH.to_string(),
        response.status(),
        started,
    );
    response
}

pub async fn usage_handler(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    usage_route(state, None, headers).await
}

pub async fn pooled_usage_handler(
    State(state): State<Arc<AppState>>,
    Path(pool): Path<String>,
    headers: HeaderMap,
) -> Response {
    usage_route(state, Some(pool), headers).await
}

fn backend_root(upstream_base_url: &str) -> Result<reqwest::Url, ()> {
    let base = upstream_base_url.trim_end_matches('/');
    const MARKER: &str = "/backend-api";
    let root = match base.find(MARKER) {
        Some(index) => &base[..index + MARKER.len()],
        None => return Err(()),
    };
    reqwest::Url::parse(root).map_err(|_| ())
}

fn passthrough_url(
    upstream_base_url: &str,
    path: &str,
    query: Option<&str>,
) -> Result<reqwest::Url, ()> {
    if path
        .split('/')
        .any(|segment| matches!(segment, "." | "..") || segment.contains('\\'))
    {
        return Err(());
    }
    let mut url = backend_root(upstream_base_url)?;
    let joined = format!(
        "{}/{}",
        url.path().trim_end_matches('/'),
        path.trim_start_matches('/')
    );
    url.set_path(&joined);
    url.set_query(query);
    Ok(url)
}

const HOP_BY_HOP: &[&str] = &[
    "connection",
    "content-length",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

fn end_to_end_headers(headers: &HeaderMap) -> HeaderMap {
    let mut forwarded = headers.clone();
    let connection_named: HashSet<HeaderName> = headers
        .get_all(axum::http::header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|name| name.trim().parse().ok())
        .collect();
    for name in HOP_BY_HOP {
        forwarded.remove(*name);
    }
    for name in connection_named {
        forwarded.remove(name);
    }
    forwarded.remove(axum::http::header::HOST);
    forwarded
}

fn normalized_route(path: &str) -> String {
    let segments: Vec<&str> = path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    let normalized = match segments.as_slice() {
        ["wham", "usage"] => "wham/usage".to_string(),
        ["wham", "rate-limit-reset-credits"] => "wham/rate-limit-reset-credits".to_string(),
        ["wham", "rate-limit-reset-credits", "consume"] => {
            "wham/rate-limit-reset-credits/consume".to_string()
        }
        ["wham", "accounts", "check"] => "wham/accounts/check".to_string(),
        ["wham", "accounts", "send_add_credits_nudge_email"] => {
            "wham/accounts/send_add_credits_nudge_email".to_string()
        }
        ["wham", "profiles", "me"] => "wham/profiles/me".to_string(),
        ["wham", "tasks"] => "wham/tasks".to_string(),
        ["wham", "tasks", "list"] => "wham/tasks/list".to_string(),
        ["wham", "tasks", _] => "wham/tasks/:id".to_string(),
        ["wham", "tasks", _, "turns", _, "sibling_turns"] => {
            "wham/tasks/:id/turns/:id/sibling_turns".to_string()
        }
        ["wham", "config", "bundle"] => "wham/config/bundle".to_string(),
        ["wham", "workspace-messages"] => "wham/workspace-messages".to_string(),
        ["wham", "settings", "user"] => "wham/settings/user".to_string(),
        ["wham", "agent-identities", "jwks"] => "wham/agent-identities/jwks".to_string(),
        ["wham", "environments"] => "wham/environments".to_string(),
        ["wham", "environments", "by-repo", _, _, _] => {
            "wham/environments/by-repo/:host/:owner/:repo".to_string()
        }
        ["wham", "remote", "control", "environments"] => {
            "wham/remote/control/environments".to_string()
        }
        ["wham", "remote", "control", "server"] => "wham/remote/control/server".to_string(),
        ["wham", "remote", "control", "server", "enroll" | "refresh" | "pair"] => {
            format!("wham/remote/control/server/{}", segments[4])
        }
        ["connectors", "directory", "list" | "list_workspace"] => {
            format!("connectors/directory/{}", segments[2])
        }
        ["ps", "apps", "batch"] => "ps/apps/batch".to_string(),
        ["ps", "mcp"] => "ps/mcp".to_string(),
        ["ps", "plugins", "suggested" | "list" | "installed"] => {
            format!("ps/plugins/{}", segments[2])
        }
        ["ps", "plugins", "workspace", "shared"] => "ps/plugins/workspace/shared".to_string(),
        ["ps", "plugins", _, "install" | "uninstall" | "shares"] => {
            format!("ps/plugins/:id/{}", segments[3])
        }
        ["ps", "plugins", _] => "ps/plugins/:id".to_string(),
        ["public", "plugins", "workspace"] => "public/plugins/workspace".to_string(),
        ["public", "plugins", "workspace", "created" | "upload-url"] => {
            format!("public/plugins/workspace/{}", segments[3])
        }
        ["public", "plugins", "workspace", _] => "public/plugins/workspace/:id".to_string(),
        ["hazelnuts"] => "hazelnuts".to_string(),
        ["hazelnuts", _, "export"] => "hazelnuts/:id/export".to_string(),
        ["accounts", _, "settings"] => "accounts/:id/settings".to_string(),
        ["codex", "analytics-events", "events"] => "codex/analytics-events/events".to_string(),
        ["v1", "agent", "register"] => "v1/agent/register".to_string(),
        _ => "unknown".to_string(),
    };
    format!("chatgpt_backend_passthrough_{normalized}")
}

async fn passthrough_route(state: Arc<AppState>, path: String, request: Request<Body>) -> Response {
    let started = Instant::now();
    let method = request.method().clone();
    let method_name = method_label(&method);
    let log_path = normalized_route(&path);
    let query = request.uri().query().map(str::to_string);
    let target = match passthrough_url(&state.upstream_base_url, &path, query.as_deref()) {
        Ok(target) => target,
        Err(_) => {
            let response = error_response(StatusCode::BAD_REQUEST, "invalid_backend_path");
            record_gateway_request(&state, method_name, log_path, response.status(), started);
            return response;
        }
    };
    let (parts, body) = request.into_parts();
    let upstream = state
        .control_client
        .request(method, target)
        .headers(end_to_end_headers(&parts.headers))
        .body(reqwest::Body::wrap_stream(body.into_data_stream()))
        .send()
        .await;
    let response = match upstream {
        Err(_) => error_response(StatusCode::BAD_GATEWAY, "backend_forward_failed"),
        Ok(upstream) => {
            let status = upstream.status();
            let headers = end_to_end_headers(upstream.headers());
            let body = Body::from_stream(
                upstream
                    .bytes_stream()
                    .map(|chunk| chunk.map_err(std::io::Error::other)),
            );
            let mut response = Response::new(body);
            *response.status_mut() = status;
            *response.headers_mut() = headers;
            response
        }
    };
    record_gateway_request(&state, method_name, log_path, response.status(), started);
    response
}

pub async fn passthrough_handler(
    State(state): State<Arc<AppState>>,
    Path(path): Path<String>,
    request: Request<Body>,
) -> Response {
    passthrough_route(state, path, request).await
}

pub async fn pooled_passthrough_handler(
    State(state): State<Arc<AppState>>,
    Path((_pool, path)): Path<(String, String)>,
    request: Request<Body>,
) -> Response {
    // Pool scope affects only the synthesized usage view. Every untouched account-backend call
    // remains tied to the client's own ChatGPT identity and takes the same direct forwarding path.
    passthrough_route(state, path, request).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_normalization_keeps_known_operations_and_drops_identifiers() {
        assert_eq!(
            normalized_route("wham/settings/user"),
            "chatgpt_backend_passthrough_wham/settings/user"
        );
        assert_eq!(
            normalized_route("wham/tasks/task-secret"),
            "chatgpt_backend_passthrough_wham/tasks/:id"
        );
        assert_eq!(
            normalized_route("ps/plugins/plugin-secret/install"),
            "chatgpt_backend_passthrough_ps/plugins/:id/install"
        );
        assert_eq!(
            normalized_route("unrecognized/private-value"),
            "chatgpt_backend_passthrough_unknown"
        );
        assert_eq!(
            normalized_route("wham/environments/by-repo/github/private-owner/private-repo"),
            "chatgpt_backend_passthrough_wham/environments/by-repo/:host/:owner/:repo"
        );
        assert_eq!(
            normalized_route("connectors/directory/list"),
            "chatgpt_backend_passthrough_connectors/directory/list"
        );
        assert_eq!(
            normalized_route("ps/mcp"),
            "chatgpt_backend_passthrough_ps/mcp"
        );
    }

    #[test]
    fn fixed_upstream_url_preserves_query_but_rejects_path_traversal() {
        let url = passthrough_url(
            "https://chatgpt.com/backend-api/codex",
            "wham/settings/user",
            Some("mode=fast"),
        )
        .unwrap();
        assert_eq!(
            url.as_str(),
            "https://chatgpt.com/backend-api/wham/settings/user?mode=fast"
        );
        assert!(passthrough_url(
            "https://chatgpt.com/backend-api/codex",
            "../oauth/token",
            None
        )
        .is_err());
    }
}
