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
    Account, ExecError, Executor, PreparedRequest, RequestCtx, ResponseMetadata, ResponseStream,
    UpstreamHttpError,
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
        let resp = builder
            .send()
            .await
            .map_err(|e| ExecError::Upstream(e.to_string()))?;
        let status = resp.status().as_u16();
        let response_headers = safe_response_headers(resp.headers());

        if !resp.status().is_success() {
            let retry_after = retry_after_secs(resp.headers());
            // Bounded read of the error body to extract the code ONLY (content-safety: the
            // message/detail prose is read into `buf` transiently here and then never touched
            // again — not stored, not logged, not placed anywhere on `ExecError`).
            let buf = read_bounded_error_body(resp).await;
            let error_code = extract_error_code(&buf);
            return Err(ExecError::UpstreamHttp(UpstreamHttpError {
                signal: polyflare_core::FailureSignal {
                    status,
                    retry_after,
                    error_code,
                },
                headers: response_headers,
                body: bytes::Bytes::from(buf),
            }));
        }

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

#[cfg(test)]
mod tests {
    use super::extract_error_code;

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
