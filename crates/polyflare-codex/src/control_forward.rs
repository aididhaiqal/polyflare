//! D17 Task 1: the generic parameterized-path UNARY control forward primitive.
//!
//! `CodexExecutor::execute` (`executor.rs`) hardcodes `/responses` and returns a live SSE byte
//! stream — correct for the one real-time endpoint codex-rs streams, but codex-rs also calls a
//! handful of small, ordinary (non-streaming) "control" endpoints: `thread/goal/{set,clear,get}`,
//! `agent-identities/jwks` (+ a `wham/`-prefixed variant), `memories/trace_summarize`, and more
//! deferred ones. Those need the SAME account/bearer/header plumbing `CodexExecutor` already has,
//! generalized to an arbitrary path, method, and body, and reading the FULL response into memory
//! instead of streaming it — this module is that primitive. It performs NO SSE parsing, NO
//! `ObservingStream`/watchdog/continuity — a plain unary HTTP round-trip, by design (see the plan's
//! "UNARY, not SSE" global constraint).
//!
//! # URL shape: adapting to PolyFlare's actual `account.base_url`
//! codex-lb's own control-endpoint transport (`core/clients/proxy.py:4165`) builds
//! `{upstream_base}/codex/<path>` (or `{upstream_base}/wham/<path>` for the `wham/`-prefixed
//! paths), where codex-lb's `upstream_base` is the bare `.../backend-api` root (NOT including
//! `/codex`).
//!
//! PolyFlare's `Account::base_url` is **not** that bare root. Every `Account` PolyFlare
//! constructs (`polyflare-server/src/ingress.rs:457`) sets `base_url` to
//! `AppState::upstream_base_url`, whose default (`polyflare-server/src/config.rs`,
//! `DEFAULT_CODEX_UPSTREAM_URL`, overridable via `POLYFLARE_UPSTREAM_URL`) is
//! `https://chatgpt.com/backend-api/codex` — it ALREADY ends in `/codex`, because
//! `CodexExecutor::execute` builds its URL as the simple `{base_url}/responses`
//! (`executor.rs:150`), which only works if `base_url` already carries the `/codex` segment.
//!
//! So [`control_url`] first normalizes `base_url` back to the bare `.../backend-api` root by
//! stripping one trailing `/codex` segment (a no-op if `base_url` is ever configured WITHOUT it),
//! then applies codex-lb's exact join rule on top of that root. Given the real default
//! `base_url = "https://chatgpt.com/backend-api/codex"`, this produces:
//! - `control_url(base_url, "memories/trace_summarize")` →
//!   `"https://chatgpt.com/backend-api/codex/memories/trace_summarize"`
//! - `control_url(base_url, "wham/agent-identities/jwks")` →
//!   `"https://chatgpt.com/backend-api/wham/agent-identities/jwks"` (no `/codex/` segment)
//!
//! — byte-identical to codex-lb's final URL shape.
//!
//! # Content safety
//! The request/response BODY bytes flow through this module purely as opaque `Bytes` — forwarded
//! to the caller, never inspected, never passed to `tracing`/`eprintln!`/any log sink, and never
//! persisted here. (The caller's content-free `request_log` row — status/account/latency only —
//! is Task 3's concern, not this module's.) The bounded read below exists ONLY to cap memory use
//! against a hostile/huge upstream body, not to inspect content.

use bytes::Bytes;
use futures_util::StreamExt;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use reqwest::Method;

use polyflare_core::Account;

/// The response-header allow-set control forwards filter to (lowercase-compared), mirroring
/// codex-lb's `_CODEX_CONTROL_RESPONSE_HEADERS` (`modules/proxy/api.py:473`,
/// `_codex_control_downstream_headers` at `:487`). Any response header from the upstream NOT in
/// this set (e.g. `set-cookie`, an internal `x-internal-secret`) is dropped — the control forward
/// never blindly relays arbitrary upstream headers downstream.
const ALLOWED_RESPONSE_HEADERS: &[&str] = &[
    "cache-control",
    "content-type",
    "etag",
    "last-modified",
    "location",
    "openai-processing-ms",
    "request-id",
    "x-request-id",
];

/// Content-safety cap on how much of a control response body this primitive will ever read into
/// memory. Control responses (JWKS keys, a `thread/goal` payload, a trace-summarize ack) are
/// expected to be small JSON — this is a defensive ceiling against a huge/hostile upstream body,
/// not a hint. Deliberately a DIFFERENT (larger) cap than `executor.rs::MAX_ERROR_BODY_BYTES`
/// (64 KiB, sized only for scraping an error `code` out of a `/responses` failure body): a
/// legitimate control response body — e.g. a JWKS key set — is expected to be actual payload the
/// caller returns to its client, not a discardable error blob, so it gets more headroom.
const MAX_CONTROL_BODY_BYTES: usize = 4 * 1024 * 1024; // 4 MiB

/// The UNARY result of a control forward: status + the filtered response headers + the full body.
#[derive(Debug, Clone)]
pub struct ControlResponse {
    pub status: u16,
    /// Response headers filtered to [`ALLOWED_RESPONSE_HEADERS`], in upstream order. A header
    /// value that isn't valid UTF-8 is dropped (defensive; codex control responses are JSON/ASCII
    /// in practice).
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
}

/// A transport-level failure forwarding a control request (DNS/connect/TLS/timeout/stream
/// error) — the caller (Task 3's handlers) maps this to a 502. Never carries an upstream
/// status/body; those only exist on the `Ok(ControlResponse)` path, however non-2xx that status
/// is — a control forward does not special-case upstream error statuses the way
/// `CodexExecutor::execute` does for `/responses` (there is no retry/failover signal to extract
/// here, just a status to relay).
#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    #[error("control forward transport error: {0}")]
    Transport(String),
    #[error("invalid control forward header: {0}")]
    InvalidHeader(String),
}

/// Builds the final upstream URL for a control-path forward from `account.base_url` and a control
/// `path` (no leading slash expected, but tolerated). See the module doc for why this strips a
/// trailing `/codex` segment before rejoining. `path` starting `wham/` joins directly onto the
/// backend-api root (no `/codex/` inserted); every other path joins under `/codex/`.
pub fn control_url(base_url: &str, path: &str) -> String {
    let trimmed = base_url.trim_end_matches('/');
    let root = trimmed.strip_suffix("/codex").unwrap_or(trimmed);
    let path = path.trim_start_matches('/');
    match path.strip_prefix("wham/") {
        Some(rest) => format!("{root}/wham/{rest}"),
        None => format!("{root}/codex/{path}"),
    }
}

/// Performs one UNARY forward of a codex control request.
///
/// - `client`: the caller's `reqwest::Client` — pass one built via
///   [`crate::executor::build_client`] (or `CodexExecutor::client()`) to match `CodexExecutor`'s
///   own TLS/rustls fingerprint rather than building an independent client.
/// - `path`: a control path with no leading slash, e.g. `"memories/trace_summarize"` or
///   `"wham/agent-identities/jwks"` — see [`control_url`].
/// - `forward_headers`: the client's own control-request headers to relay upstream (the "dumb
///   executor" doctrine — same as `PreparedRequest::forward_headers` on the `/responses` path).
///   `Authorization` and `chatgpt-account-id` are always OVERRIDDEN below regardless of what's in
///   this list, mirroring `CodexExecutor::execute` (`executor.rs:167-181`) exactly, so a forwarded
///   header for the WRONG account can never survive next to the selected account's bearer.
/// - `body`: the request body bytes, forwarded verbatim (no parse/re-serialize) when present.
pub async fn control_forward(
    client: &reqwest::Client,
    account: &Account,
    path: &str,
    method: Method,
    forward_headers: &[(String, String)],
    body: Option<Bytes>,
) -> Result<ControlResponse, ControlError> {
    let url = control_url(&account.base_url, path);

    let mut headers = HeaderMap::new();
    for (name, value) in forward_headers {
        let header_name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|e| ControlError::InvalidHeader(e.to_string()))?;
        let header_value =
            HeaderValue::from_str(value).map_err(|e| ControlError::InvalidHeader(e.to_string()))?;
        // `insert` (not `append`): an override below REPLACES a same-named forwarded header
        // instead of sending it twice — same rule `CodexExecutor::execute` uses.
        headers.insert(header_name, header_value);
    }
    let bearer = HeaderValue::from_str(&format!("Bearer {}", account.bearer_token))
        .map_err(|e| ControlError::InvalidHeader(e.to_string()))?;
    headers.insert(AUTHORIZATION, bearer);
    // Pair the SELECTED account's ChatGPT id with its Bearer — identical rule to
    // `CodexExecutor::execute` (`executor.rs:171-181`): `insert` so a client-forwarded value for a
    // DIFFERENT account can never survive next to the overridden Bearer.
    if let Some(account_id) = &account.chatgpt_account_id {
        headers.insert(
            HeaderName::from_static("chatgpt-account-id"),
            HeaderValue::from_str(account_id)
                .map_err(|e| ControlError::InvalidHeader(e.to_string()))?,
        );
    }
    // Conditional content-type passthrough: only set when a body is present AND the forwarded
    // headers didn't already carry one (preserves a client's own forwarded content-type
    // byte-identically instead of duplicating/overriding it) — mirrors the raw-body branch of
    // `CodexExecutor::execute` (`executor.rs:187-189`).
    if body.is_some() && !headers.contains_key(CONTENT_TYPE) {
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    }

    let mut builder = client.request(method, &url).headers(headers);
    if let Some(bytes) = body {
        builder = builder.body(bytes);
    }
    let resp = builder
        .send()
        .await
        .map_err(|e| ControlError::Transport(e.to_string()))?;

    let status = resp.status().as_u16();
    let filtered_headers: Vec<(String, String)> = resp
        .headers()
        .iter()
        .filter(|(name, _)| ALLOWED_RESPONSE_HEADERS.contains(&name.as_str().to_ascii_lowercase().as_str()))
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|v| (name.as_str().to_string(), v.to_string()))
        })
        .collect();

    let body = read_bounded_body(resp).await?;

    Ok(ControlResponse {
        status,
        headers: filtered_headers,
        body,
    })
}

/// Reads a control response body up to [`MAX_CONTROL_BODY_BYTES`], then stops (the remainder of
/// an oversized body is never read into memory). A mid-stream transport error is surfaced as
/// [`ControlError::Transport`] — unlike `executor.rs::read_bounded_error_body` (which is
/// best-effort since it only feeds a discardable error-code scrape), a control response body IS
/// the payload handed back to the caller, so a read failure must not be silently swallowed into a
/// truncated-but-"successful" body.
async fn read_bounded_body(resp: reqwest::Response) -> Result<Bytes, ControlError> {
    let mut buf = Vec::new();
    let mut stream = resp.bytes_stream();
    while buf.len() < MAX_CONTROL_BODY_BYTES {
        match stream.next().await {
            Some(Ok(chunk)) => {
                let room = MAX_CONTROL_BODY_BYTES - buf.len();
                let take = room.min(chunk.len());
                buf.extend_from_slice(&chunk[..take]);
                if take < chunk.len() {
                    break; // hit the cap mid-chunk; stop reading (stream dropped on return)
                }
            }
            Some(Err(e)) => return Err(ControlError::Transport(e.to_string())),
            None => break,
        }
    }
    Ok(Bytes::from(buf))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_url_joins_non_wham_paths_under_codex() {
        let base = "https://chatgpt.com/backend-api/codex";
        assert_eq!(
            control_url(base, "memories/trace_summarize"),
            "https://chatgpt.com/backend-api/codex/memories/trace_summarize"
        );
    }

    #[test]
    fn control_url_joins_wham_paths_without_a_codex_segment() {
        let base = "https://chatgpt.com/backend-api/codex";
        assert_eq!(
            control_url(base, "wham/agent-identities/jwks"),
            "https://chatgpt.com/backend-api/wham/agent-identities/jwks"
        );
    }

    #[test]
    fn control_url_tolerates_a_bare_backend_api_root() {
        // If base_url were ever configured WITHOUT the `/codex` suffix (unlike PolyFlare's actual
        // default), the join still produces codex-lb's exact shape.
        let base = "https://chatgpt.com/backend-api";
        assert_eq!(
            control_url(base, "memories/trace_summarize"),
            "https://chatgpt.com/backend-api/codex/memories/trace_summarize"
        );
        assert_eq!(
            control_url(base, "wham/agent-identities/jwks"),
            "https://chatgpt.com/backend-api/wham/agent-identities/jwks"
        );
    }

    #[test]
    fn control_url_tolerates_trailing_slash_and_leading_slash_path() {
        let base = "https://chatgpt.com/backend-api/codex/";
        assert_eq!(
            control_url(base, "/memories/trace_summarize"),
            "https://chatgpt.com/backend-api/codex/memories/trace_summarize"
        );
    }
}
