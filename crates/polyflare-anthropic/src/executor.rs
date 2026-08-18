//! Anthropic backend executor: HTTP `POST /v1/messages`, subscription-OAuth bearer auth, the
//! required `anthropic-version` header, SSE byte-stream pass-through. Mirrors `CodexExecutor`'s
//! M1 shape; byte-parity fingerprinting is M5, not here.

use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;

use polyflare_core::{Account, ExecError, Executor, PreparedRequest, RequestCtx, ResponseStream};

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

        let resp = request
            .send()
            .await
            .map_err(|e| ExecError::Upstream(e.to_string()))?;

        if !resp.status().is_success() {
            let signal = crate::rate_limit::failure_signal(
                resp.status().as_u16(),
                resp.headers(),
                unix_now(),
            );
            return Err(ExecError::UpstreamStatus(signal));
        }

        let stream = resp
            .bytes_stream()
            .map(|chunk| chunk.map_err(|e| ExecError::Stream(e.to_string())));

        Ok(ResponseStream::new(stream))
    }
}
