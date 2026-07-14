//! Anthropic backend executor: HTTP `POST /v1/messages`, subscription-OAuth bearer auth, the
//! required `anthropic-version` header, SSE byte-stream pass-through. Mirrors `CodexExecutor`'s
//! M1 shape; byte-parity fingerprinting is M5, not here.

use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;

use polyflare_core::{Account, ExecError, Executor, PreparedRequest, ResponseStream};

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
    ) -> Result<ResponseStream, ExecError> {
        let url = format!("{}/v1/messages", account.base_url.trim_end_matches('/'));
        let resp = self
            .client
            .post(&url)
            .bearer_auth(&account.bearer_token)
            .header("anthropic-version", ANTHROPIC_VERSION)
            .json(&req.body)
            .send()
            .await
            .map_err(|e| ExecError::Upstream(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(ExecError::Upstream(format!("status {}", resp.status())));
        }

        let stream = resp
            .bytes_stream()
            .map(|chunk| chunk.map_err(|e| ExecError::Stream(e.to_string())));

        Ok(Box::pin(stream))
    }
}
