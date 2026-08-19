//! Anthropic backend executor: HTTP `POST /v1/messages`, subscription-OAuth bearer auth, the
//! required `anthropic-version` header, SSE byte-stream pass-through. Mirrors `CodexExecutor`'s
//! M1 shape; byte-parity fingerprinting is M5, not here.

use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use reqwest::header::HeaderMap;

use polyflare_core::{
    Account, ExecError, Executor, PreparedRequest, RequestCtx, ResponseStream, UpstreamHttpError,
};

/// Content-safety cap on how much of a non-2xx error body we read into memory (mirrors the Codex
/// executor's cap). A hostile or merely huge upstream error body must never be read unbounded.
const MAX_ERROR_BODY_BYTES: usize = 64 * 1024;

/// In-place retry budget for a transient `529 overloaded_error` that carries no reset hint. Mirrors
/// better-ccflare's overload retry (≤2 total attempts, base 750ms, 3s cap, FULL jitter). A quick
/// jittered retry on the SAME account usually rides out a momentary overload without benching it —
/// which spreads concurrent spikes out instead of cooling every account at once, and is the only
/// thing that lets a single-account pool survive a 529 (there is no other account to fail over to).
const OVERLOAD_RETRY_MAX_ATTEMPTS: u32 = 2;
const OVERLOAD_RETRY_BASE_MS: u64 = 750;
const OVERLOAD_RETRY_MAX_MS: u64 = 3000;

/// Full-jitter backoff cap for the n-th (1-based) overload retry: `min(base * 2^n, max)`. The actual
/// sleep is a uniform random draw in `[0, cap]` (full jitter), so retries across accounts desync.
fn overload_backoff_cap_ms(attempt: u32) -> u64 {
    OVERLOAD_RETRY_BASE_MS
        .saturating_mul(1u64 << attempt.min(20))
        .min(OVERLOAD_RETRY_MAX_MS)
}

/// A uniform random delay in `[0, cap_ms]` — the full-jitter sleep before an overload retry.
fn jittered_delay_ms(cap_ms: u64) -> u64 {
    if cap_ms == 0 {
        return 0;
    }
    (rand::random::<f64>() * cap_ms as f64) as u64
}

/// Read a non-2xx response body up to [`MAX_ERROR_BODY_BYTES`], truncating past the cap. So the
/// client can be shown the REAL upstream error (a genuine 429 with its message + retry-after) once
/// failover is exhausted, instead of a generic 502.
async fn read_bounded_error_body(resp: reqwest::Response) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut stream = resp.bytes_stream();
    while buf.len() < MAX_ERROR_BODY_BYTES {
        let Some(chunk) = stream.next().await else {
            break;
        };
        let Ok(chunk) = chunk else { break };
        let room = MAX_ERROR_BODY_BYTES - buf.len();
        let take = room.min(chunk.len());
        buf.extend_from_slice(&chunk[..take]);
    }
    buf
}

/// Response headers safe to forward to the client verbatim — hop-by-hop and cookie headers dropped.
/// Keeps `retry-after` and the `anthropic-ratelimit-unified-*` headers so a surfaced 429/529 carries
/// the real reset the client should honor. Mirrors the Codex executor's filter.
fn safe_response_headers(headers: &HeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .filter(|(name, _)| {
            !matches!(
                name.as_str(),
                "connection"
                    | "content-length"
                    | "content-encoding"
                    | "transfer-encoding"
                    | "keep-alive"
                    | "proxy-authenticate"
                    | "proxy-authorization"
                    | "te"
                    | "trailer"
                    | "upgrade"
                    | "set-cookie"
            )
        })
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_string(), value.to_string()))
        })
        .collect()
}

/// The Anthropic Messages API version this executor speaks. Every request must carry this header
/// (doc-verified against the Anthropic TypeScript SDK: `'anthropic-version': '2023-06-01'` is sent
/// on every request in `src/client.ts`).
const ANTHROPIC_VERSION: &str = "2023-06-01";

pub struct AnthropicExecutor {
    client: reqwest::Client,
}

impl AnthropicExecutor {
    pub fn new() -> Result<Self, ExecError> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| ExecError::Upstream(e.to_string()))?;
        Ok(Self { client })
    }
}

fn unix_now() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[async_trait]
impl Executor for AnthropicExecutor {
    async fn execute(
        &self,
        req: PreparedRequest,
        account: &Account,
        _ctx: &RequestCtx,
    ) -> Result<ResponseStream, ExecError> {
        let url = format!("{}/v1/messages", account.base_url.trim_end_matches('/'));

        let mut request = self.client.post(&url);

        // `forward_headers` is the admitted Claude request's allowlisted envelope (built by
        // `claude_wire::outbound_headers`, which already replaced the caller's credential with this
        // account's). When present it is authoritative: applying it first, and only then filling in
        // defaults for what it did NOT set, keeps the client's own protocol envelope byte-faithful
        // instead of overwriting it with ours.
        let has = |name: &str| -> bool {
            req.forward_headers
                .iter()
                .any(|(key, _)| key.eq_ignore_ascii_case(name))
        };
        let forwards_auth = has("authorization");
        let forwards_version = has("anthropic-version");
        let forwards_content_type = has("content-type");
        for (name, value) in &req.forward_headers {
            request = request.header(name, value);
        }
        if !forwards_auth {
            request = request.bearer_auth(&account.bearer_token);
        }
        if !forwards_version {
            request = request.header("anthropic-version", ANTHROPIC_VERSION);
        }

        // Raw bytes forward verbatim — no parse/re-serialize round-trip, so the body the upstream
        // receives is byte-identical to what the client sent (key-order, spacing, and unicode
        // escaping included). Anything that mutated the body sets `body` instead, and the
        // `raw_body.is_none() => body.is_some()` invariant makes one of the two always present.
        request = match req.raw_body.as_ref() {
            Some(raw) => {
                // Only set content-type when the forwarded envelope did not already carry it;
                // setting it twice would emit a duplicate header.
                if !forwards_content_type {
                    request = request.header("content-type", "application/json");
                }
                request.body(raw.clone())
            }
            None => request.json(
                req.body
                    .as_ref()
                    .expect("PreparedRequest: raw_body None ⇒ body Some"),
            ),
        };

        // Send, retrying in place only a transient 529 (overloaded, no reset hint) with full jitter.
        // `try_clone` succeeds for our cloneable bodies (raw bytes / serialized JSON); if a body is
        // ever not cloneable we simply send once without retry.
        let mut attempt: u32 = 0;
        let resp = loop {
            let Some(this_attempt) = request.try_clone() else {
                break request
                    .send()
                    .await
                    .map_err(|e| ExecError::Upstream(e.to_string()))?;
            };
            let resp = this_attempt
                .send()
                .await
                .map_err(|e| ExecError::Upstream(e.to_string()))?;
            if resp.status().as_u16() == 529 && attempt + 1 < OVERLOAD_RETRY_MAX_ATTEMPTS {
                // Only retry when the 529 gives no reset time; a 529 WITH a reset is a real cooldown
                // signal the ingress should honor via failover, not something to hammer in place.
                let signal =
                    crate::rate_limit::failure_signal(529, resp.headers(), unix_now());
                if signal.retry_after.is_none() {
                    attempt += 1;
                    let delay = jittered_delay_ms(overload_backoff_cap_ms(attempt));
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    continue;
                }
            }
            break resp;
        };

        if !resp.status().is_success() {
            // Build the failure signal from the headers (the ladder + out_of_credits/529 logic),
            // capture the forwardable headers, THEN read the bounded body — so that when account
            // failover is exhausted the ingress can surface the REAL upstream response (a genuine
            // 429/529 with its retry-after and message) instead of a generic 502.
            let signal = crate::rate_limit::failure_signal(
                resp.status().as_u16(),
                resp.headers(),
                unix_now(),
            );
            let headers = safe_response_headers(resp.headers());
            let body = read_bounded_error_body(resp).await;
            return Err(ExecError::UpstreamHttp(UpstreamHttpError {
                signal,
                headers,
                body: bytes::Bytes::from(body),
            }));
        }

        let stream = resp
            .bytes_stream()
            .map(|chunk| chunk.map_err(|e| ExecError::Stream(e.to_string())));

        Ok(ResponseStream::new(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overload_backoff_cap_grows_then_saturates() {
        assert_eq!(overload_backoff_cap_ms(1), 1500); // 750 * 2
        assert_eq!(overload_backoff_cap_ms(2), 3000); // 750 * 4, capped at 3000
        assert_eq!(overload_backoff_cap_ms(3), 3000); // saturates
        assert_eq!(overload_backoff_cap_ms(40), 3000); // never overflows the shift
    }

    #[test]
    fn jitter_never_exceeds_the_cap() {
        for _ in 0..1000 {
            assert!(jittered_delay_ms(1500) <= 1500);
        }
        assert_eq!(jittered_delay_ms(0), 0);
    }
}
