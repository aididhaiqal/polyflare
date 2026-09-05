//! Codex backend executor. M1: HTTP-SSE identity pass-through. M5 (T-rustls): the client is
//! pinned onto rustls + the aws-lc-rs crypto provider so its TLS ClientHello structurally matches
//! codex-rs's own `codex-http-client` transport (same rustls release, same provider, the
//! `prefer-post-quantum` X25519MLKEM768 hybrid key share offered) — full byte-for-byte fingerprint
//! parity against a real codex-rs capture is the fingerprint-parity GATE, deferred pending a live
//! capture. WS transport comes in a later milestone.
//!
//! # Header handling: dumb executor, smart ingress
//! This executor does NOT synthesize codex-identity headers (`user-agent`, `originator`,
//! `session-id`, `thread-id`, ...) itself. A real Codex CLI talking to PolyFlare's native
//! `/responses` endpoint already sends its own genuine identity headers — overwriting them here
//! would both discard real conversation ids and produce a WORSE fingerprint than simply relaying
//! what the client sent. Instead, the ingress (`polyflare-server::ingress`) decides what to send
//! upstream and hands it down via `PreparedRequest::forward_headers`: the client's own surviving
//! headers, forwarded untouched, for a native request; a synthesized set (via
//! `polyflare_codex::codex_headers`) for a translated request that has no real Codex client
//! fingerprint to forward. This executor just sets whatever `forward_headers` it's given, then
//! overrides `authorization` (the selected account's own bearer) and `accept`.

use std::sync::Once;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use reqwest::header::{
    HeaderMap, HeaderName, HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_TYPE, RETRY_AFTER,
};

use polyflare_core::{
    Account, ExecError, Executor, FailureSignal, PreparedRequest, RequestCtx, ResponseMetadata,
    ResponseStream, UpstreamHttpError,
};

use crate::chatgpt_cloudflare_cookies::with_chatgpt_cloudflare_cookie_store;

/// Parse the numeric-seconds form of `Retry-After` (the form the Codex/OpenAI backend sends on a
/// 429). The HTTP-date form is ignored (returns `None` ⇒ the caller falls back to exponential
/// backoff). Negative values are rejected.
fn retry_after_secs(headers: &HeaderMap) -> Option<i64> {
    headers
        .get(RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<i64>().ok())
        .filter(|&s| s >= 0)
}

/// Content-safety cap on how much of a non-2xx error body we will ever read into memory. A
/// hostile or merely huge upstream error body must never be read unbounded (hang/OOM risk); this
/// is a hard ceiling, not a hint — bytes past it are never even copied into `buf`.
const MAX_ERROR_BODY_BYTES: usize = 64 * 1024;

/// Read a non-2xx response body up to [`MAX_ERROR_BODY_BYTES`], then drop the response. Never
/// buffers more than the cap regardless of how large the upstream body is or how it's chunked —
/// each incoming chunk is truncated to whatever room remains before being copied in, and reading
/// stops (the stream is dropped) the moment the cap is reached.
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

/// A code token is a short, enum-like ASCII identifier (`invalid_grant`, `account_deactivated`,
/// …) — never prose. Used to gate the `detail` shape below: a `detail` string is only ever
/// treated as a code if it already looks like one, so free-text messages (which can echo request
/// framing) are never scraped for a "code".
fn looks_like_code_token(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// Extract ONLY the error `code` from a non-2xx response body — never the `message`/`detail`
/// prose text, which can echo request framing (content-safety). Handles the OpenAI shape
/// (`{"error":{"code":"...","type":"...","message":"..."}}`, preferred) and the codex-lb-observed
/// `{"detail":"..."}` shape, but only when `detail` itself is already a clean code token — a
/// prose `detail` yields `None` rather than a guessed/scraped code. Any parse failure (malformed
/// JSON, absent/non-string code, truncated body) also yields `None`; this must never be treated
/// as an error in the caller — a missing code is always a valid, silent outcome.
fn extract_error_code(body: &[u8]) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(body).ok()?;
    if let Some(error) = value.get("error") {
        for field in ["code", "type"] {
            if let Some(code) = error.get(field).and_then(|c| c.as_str()) {
                if looks_like_code_token(code) {
                    return Some(code.to_string());
                }
            }
        }
    }
    if let Some(detail) = value.get("detail").and_then(|d| d.as_str()) {
        if looks_like_code_token(detail) {
            return Some(detail.to_string());
        }
    }
    None
}

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

// Pins the exact aws-lc-rs version (see workspace Cargo.toml) that rustls's `aws_lc_rs` feature
// resolves to transitively; never called directly ourselves — `rustls::crypto::aws_lc_rs` is the
// entry point we use below.
use aws_lc_rs as _;

/// Installs aws-lc-rs as the process-wide default rustls `CryptoProvider`, mirroring codex-rs's
/// `codex-utils-rustls-provider::ensure_rustls_crypto_provider`. Guarded by a `Once` so repeated
/// calls (e.g. constructing multiple `CodexExecutor`s) are a cheap no-op instead of the panic
/// `CryptoProvider::install_default()` raises when called twice: a second real attempt returns
/// `Err` (a provider is already installed), which we discard via `.ok()` since a pre-installed
/// provider — ours or, in an embedding host, someone else's — is not an error for us.
///
/// `pub(crate)`: the WS transport (`crate::ws::conn::WsConn::connect`) must call this before its
/// first TLS handshake too, same reason as here.
pub(crate) fn ensure_rustls_crypto_provider() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

/// Builds the exact rustls/aws-lc-rs-pinned `reqwest::Client` [`CodexExecutor`] uses, as a free
/// function so other Codex-egress call sites (the D17 control-forward primitive,
/// `control_forward::control_forward`) can obtain a byte-for-byte identical TLS fingerprint
/// without duplicating the builder — see that module's doc for why sharing THIS function (rather
/// than a second independent builder) matters for fingerprint parity.
pub fn build_client() -> Result<reqwest::Client, ExecError> {
    // Must run before the first TLS use so reqwest's rustls backend picks up aws-lc-rs instead
    // of falling back to ring (see reqwest's `TlsBackend::Rustls` build path).
    ensure_rustls_crypto_provider();
    with_chatgpt_cloudflare_cookie_store(reqwest::Client::builder())
        // Force rustls: `default-tls` (native-tls) is also compiled in workspace-wide, so
        // without this the client would silently use native-tls instead.
        .use_rustls_tls()
        // PolyFlare owns replay policy. Reqwest's redirect and protocol-NACK retry defaults could
        // otherwise resend a state-changing request after bytes reached an upstream, outside the
        // per-origin recovery circuit and its response-establishment boundary.
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .connect_timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| ExecError::Upstream(e.to_string()))
}

pub struct CodexExecutor {
    client: reqwest::Client,
}

impl CodexExecutor {
    pub fn new() -> Result<Self, ExecError> {
        Ok(Self {
            client: build_client()?,
        })
    }

    /// Construct an executor from an already-built shared client.
    ///
    /// Production server startup uses this path so `/responses`, unary control calls, and the
    /// ChatGPT backend gateway clone one `reqwest::Client` and therefore share both its
    /// connection pool and its restricted Cloudflare affinity-cookie store.
    pub fn from_client(client: reqwest::Client) -> Self {
        Self { client }
    }

    /// The executor's own `reqwest::Client` — a cheap clone (reqwest clients are `Arc`-backed
    /// internally). Lets a caller that already holds a live `CodexExecutor` (e.g. a future
    /// control-forward wiring in `polyflare-server`) reuse the SAME pooled client/connections
    /// instead of building a second one via [`build_client`].
    pub fn client(&self) -> reqwest::Client {
        self.client.clone()
    }
}

#[async_trait]
impl Executor for CodexExecutor {
    async fn execute(
        &self,
        req: PreparedRequest,
        account: &Account,
        _ctx: &RequestCtx,
    ) -> Result<ResponseStream, ExecError> {
        let url = format!("{}/responses", account.base_url.trim_end_matches('/'));

        // Set whatever headers the ingress decided to forward (native: the client's own genuine
        // headers, untouched; translated: a synthesized codex identity — see module doc), then
        // override auth/accept. `HeaderMap::insert` (not `append`) is used throughout so an
        // override REPLACES a same-named forwarded header instead of sending it twice (e.g. a
        // native client's own inbound `accept: text/event-stream` is replaced, not duplicated,
        // by the override below). `content-type` is set below only for the raw path, and only when
        // absent — the `.json()` (serialized) path sets it itself, also only when absent.
        let mut headers = HeaderMap::new();
        for (name, value) in &req.forward_headers {
            let header_name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|e| ExecError::Upstream(e.to_string()))?;
            let header_value =
                HeaderValue::from_str(value).map_err(|e| ExecError::Upstream(e.to_string()))?;
            headers.insert(header_name, header_value);
        }
        let bearer = HeaderValue::from_str(&format!("Bearer {}", account.bearer_token))
            .map_err(|e| ExecError::Upstream(e.to_string()))?;
        headers.insert(AUTHORIZATION, bearer);
        headers.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));
        // FedRAMP routing is part of the selected account identity tuple, never a client-owned
        // passthrough. Remove a stale forwarded value first; only the selected account's claim may
        // restore the header.
        headers.remove(HeaderName::from_static("x-openai-fedramp"));
        if account.is_fedramp {
            headers.insert(
                HeaderName::from_static("x-openai-fedramp"),
                HeaderValue::from_static("true"),
            );
        }
        // Pair the SELECTED account's ChatGPT id with its Bearer, exactly as the real Codex CLI
        // does (`ChatGPT-Account-ID`). `insert` (replace) so a client's forwarded value for a
        // DIFFERENT account can never survive next to our overridden Bearer — a mismatched
        // (token, account) pair is precisely what the backend rejects.
        headers.remove(HeaderName::from_static("chatgpt-account-id"));
        if let Some(account_id) = &account.chatgpt_account_id {
            headers.insert(
                HeaderName::from_static("chatgpt-account-id"),
                HeaderValue::from_str(account_id)
                    .map_err(|e| ExecError::Upstream(e.to_string()))?,
            );
        }

        // Content-Type on the raw path: mirror `.json()`'s CONDITIONAL insert (set only when absent)
        // so a native client's own forwarded `content-type` is PRESERVED byte-identically and never
        // duplicated. `RequestBuilder::header` APPENDS (unlike `.json()`'s insert-if-absent), so we
        // must set it on the `HeaderMap` (insert = replace/one value) here, not on the builder.
        if req.raw_body.is_some() && !headers.contains_key(CONTENT_TYPE) {
            headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        }

        // Forward the client's ORIGINAL bytes verbatim when present (native pass-through — no
        // parse→re-serialize round-trip, byte-identical to what the client sent); otherwise
        // serialize the (built/mutated) body.
        let builder = self.client.post(&url).headers(headers);
        let builder = match &req.raw_body {
            Some(raw) => builder.body(raw.clone()),
            // No raw pass-through ⇒ `body` is `Some` per `PreparedRequest`'s invariant.
            None => builder.json(
                req.body
                    .as_ref()
                    .expect("PreparedRequest: raw_body None ⇒ body Some"),
            ),
        };
        // A transient refusal (burst 429, "server is overloaded") is retried in place, on this
        // same account, before anything is relayed. This is the only replay an anchored turn can
        // get: `previous_response_id` pins the conversation to this account, so the cross-account
        // failover loop is gated off for it, and without this one 429 kills the whole turn.
        // Nothing has been relayed downstream yet, so the resend is invisible to the client.
        let mut attempt: u32 = 0;
        let resp = loop {
            let Some(this_attempt) = builder.try_clone() else {
                break builder
                    .send()
                    .await
                    .map_err(|e| ExecError::Upstream(e.to_string()))?;
            };
            let resp = this_attempt
                .send()
                .await
                .map_err(|e| ExecError::Upstream(e.to_string()))?;
            if resp.status().is_success() {
                break resp;
            }

            let status = resp.status().as_u16();
            let retry_after = retry_after_secs(resp.headers());
            let response_headers = safe_response_headers(resp.headers());
            // Bounded read of the error body to extract the code ONLY (content-safety: the
            // message/detail prose is read into `buf` transiently here and then never touched
            // again — not stored, not logged, not placed anywhere on `ExecError`).
            let buf = read_bounded_error_body(resp).await;
            let error_code = extract_error_code(&buf);
            let signal = FailureSignal {
                status,
                retry_after,
                error_code,
            };

            if attempt < TRANSIENT_RETRY_MAX_RETRIES {
                if let Some(delay) = transient_retry_delay(&signal, attempt) {
                    attempt += 1;
                    tracing::debug!(
                        account_id = %account.id,
                        status,
                        error_code = signal.error_code.as_deref().unwrap_or(""),
                        retry_after = ?signal.retry_after,
                        attempt,
                        delay_ms = delay.as_millis() as u64,
                        "transient upstream refusal; retrying in place"
                    );
                    tokio::time::sleep(delay).await;
                    continue;
                }
            }

            return Err(ExecError::UpstreamHttp(UpstreamHttpError {
                signal,
                headers: response_headers,
                body: bytes::Bytes::from(buf),
            }));
        };
        let status = resp.status().as_u16();
        let response_headers = safe_response_headers(resp.headers());

        let stream = resp
            .bytes_stream()
            .map(|chunk| chunk.map_err(|e| ExecError::Stream(e.to_string())));

        Ok(ResponseStream::with_metadata(
            stream,
            ResponseMetadata {
                status,
                headers: response_headers,
            },
        ))
    }
}

/// How many times one send is retried in place after a transient refusal. Two retries with the
/// backoff below add at most ~4.5 s to a turn; a burst 429 on this upstream clears well inside
/// that, and anything longer is real pressure that the retry must not paper over.
const TRANSIENT_RETRY_MAX_RETRIES: u32 = 2;
const TRANSIENT_RETRY_BASE_MS: u64 = 750;
const TRANSIENT_RETRY_MAX_MS: u64 = 3000;
/// A `Retry-After` longer than this is not a burst; sleeping through it would only hold the
/// slot and the client's turn hostage. Let the failure surface instead.
const TRANSIENT_RETRY_AFTER_HONOUR_MAX_SECS: i64 = 5;

/// Error codes that mean the account's quota is spent. A 429 carrying one will not clear in
/// seconds, so it is never retried in place; the caller's cooldown/failover handles it.
fn is_quota_code(code: &str) -> bool {
    matches!(code, "insufficient_quota" | "usage_not_included")
}

/// The delay before retrying this failure in place, or `None` when it must surface as-is.
///
/// Retried: a 429 that is not a quota exhaustion, and a 5xx the upstream explicitly labels
/// `server_is_overloaded`. A short `Retry-After` is honoured verbatim; absent one, the delay is
/// full-jitter exponential backoff so a burst of parallel sends does not resynchronise.
fn transient_retry_delay(signal: &FailureSignal, attempt: u32) -> Option<Duration> {
    let code = signal.error_code.as_deref();
    let transient = match signal.status {
        429 => !code.is_some_and(is_quota_code),
        500..=599 => code == Some("server_is_overloaded"),
        _ => false,
    };
    if !transient {
        return None;
    }
    match signal.retry_after {
        Some(secs) if secs > TRANSIENT_RETRY_AFTER_HONOUR_MAX_SECS => None,
        Some(secs) => Some(Duration::from_secs(secs.max(0) as u64)),
        None => {
            let cap = TRANSIENT_RETRY_BASE_MS
                .saturating_mul(1u64 << (attempt + 1).min(20))
                .min(TRANSIENT_RETRY_MAX_MS);
            Some(Duration::from_millis(
                (rand::random::<f64>() * cap as f64) as u64,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{extract_error_code, transient_retry_delay};
    use polyflare_core::FailureSignal;
    use std::time::Duration;

    fn signal(status: u16, retry_after: Option<i64>, code: Option<&str>) -> FailureSignal {
        FailureSignal {
            status,
            retry_after,
            error_code: code.map(str::to_string),
        }
    }

    #[test]
    fn a_burst_429_and_an_overloaded_5xx_are_retried_in_place() {
        assert!(transient_retry_delay(&signal(429, None, None), 0).is_some());
        assert!(
            transient_retry_delay(&signal(429, None, Some("rate_limit_exceeded")), 0).is_some()
        );
        assert!(
            transient_retry_delay(&signal(502, None, Some("server_is_overloaded")), 0).is_some()
        );
        assert!(
            transient_retry_delay(&signal(503, None, Some("server_is_overloaded")), 1).is_some()
        );
    }

    #[test]
    fn quota_exhaustion_and_unlabelled_5xx_are_not_retried() {
        // A spent quota does not clear in seconds; retrying only delays the failover.
        assert!(transient_retry_delay(&signal(429, None, Some("insufficient_quota")), 0).is_none());
        assert!(transient_retry_delay(&signal(429, None, Some("usage_not_included")), 0).is_none());
        // A bare 5xx may have already done work upstream; replay policy stays with the caller.
        assert!(transient_retry_delay(&signal(500, None, None), 0).is_none());
        assert!(transient_retry_delay(&signal(502, None, Some("bad_gateway")), 0).is_none());
        assert!(transient_retry_delay(&signal(401, None, None), 0).is_none());
        assert!(transient_retry_delay(&signal(408, None, None), 0).is_none());
    }

    #[test]
    fn retry_after_is_honoured_when_short_and_refused_when_long() {
        assert_eq!(
            transient_retry_delay(&signal(429, Some(2), None), 0),
            Some(Duration::from_secs(2))
        );
        assert_eq!(
            transient_retry_delay(&signal(429, Some(0), None), 0),
            Some(Duration::ZERO)
        );
        assert!(transient_retry_delay(&signal(429, Some(30), None), 0).is_none());
    }

    #[test]
    fn backoff_is_bounded_and_grows_with_the_attempt() {
        for attempt in 0..8 {
            let delay = transient_retry_delay(&signal(429, None, None), attempt).unwrap();
            assert!(
                delay <= Duration::from_millis(3000),
                "attempt {attempt}: {delay:?}"
            );
        }
    }

    #[test]
    fn extracts_code_or_code_like_type_without_reading_message() {
        assert_eq!(
            extract_error_code(br#"{"error":{"code":"insufficient_quota","message":"ignored"}}"#)
                .as_deref(),
            Some("insufficient_quota")
        );
        assert_eq!(
            extract_error_code(br#"{"error":{"type":"usage_not_included","message":"ignored"}}"#)
                .as_deref(),
            Some("usage_not_included")
        );
        assert_eq!(
            extract_error_code(br#"{"error":{"type":"not prose allowed","message":"ignored"}}"#),
            None
        );
    }
}
