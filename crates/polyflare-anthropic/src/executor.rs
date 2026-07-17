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

#[async_trait]
impl Executor for AnthropicExecutor {
    async fn execute(
        &self,
        req: PreparedRequest,
        account: &Account,
        _ctx: &RequestCtx,
    ) -> Result<ResponseStream, ExecError> {
        let url = format!("{}/v1/messages", account.base_url.trim_end_matches('/'));
        // No Anthropic ingress path forwards raw bytes today (the native + aliased `/v1/messages`
        // paths both build a JSON body), so this serializes `body` via `.json()`. When Anthropic
        // native raw-forwarding is added, add a `raw_body` branch here — mirroring the Codex
        // executor's CONDITIONAL content-type insert to avoid duplicating a forwarded header.
        let resp = self
            .client
            .post(&url)
            .bearer_auth(&account.bearer_token)
            .header("anthropic-version", ANTHROPIC_VERSION)
            // Anthropic paths never set `raw_body`, so `body` is always `Some` here (invariant).
            .json(
                req.body
                    .as_ref()
                    .expect("PreparedRequest: raw_body None ⇒ body Some"),
            )
            .send()
            .await
            .map_err(|e| ExecError::Upstream(e.to_string()))?;

        if !resp.status().is_success() {
            let retry_after = resp
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.trim().parse::<i64>().ok())
                .filter(|&s| s >= 0);
            return Err(ExecError::UpstreamStatus(polyflare_core::FailureSignal {
                status: resp.status().as_u16(),
                retry_after,
                error_code: None,
            }));
        }

        let stream = resp
            .bytes_stream()
            .map(|chunk| chunk.map_err(|e| ExecError::Stream(e.to_string())));

        Ok(Box::pin(stream))
    }
}
