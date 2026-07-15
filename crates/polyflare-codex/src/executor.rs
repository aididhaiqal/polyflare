//! Codex backend executor. M1: HTTP-SSE identity pass-through. M5 (T-rustls): the client is
//! pinned onto rustls + the aws-lc-rs crypto provider so its TLS ClientHello structurally matches
//! codex-rs's own `codex-http-client` transport (same rustls release, same provider, the
//! `prefer-post-quantum` X25519MLKEM768 hybrid key share offered) — full byte-for-byte fingerprint
//! parity against a real codex-rs capture is the fingerprint-parity GATE, deferred pending a live
//! capture. WS transport comes in a later milestone.

use std::sync::Once;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;

use polyflare_core::{Account, ExecError, Executor, PreparedRequest, ResponseStream};

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
fn ensure_rustls_crypto_provider() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

pub struct CodexExecutor {
    client: reqwest::Client,
}

impl CodexExecutor {
    pub fn new() -> Result<Self, ExecError> {
        // Must run before the first TLS use so reqwest's rustls backend picks up aws-lc-rs
        // instead of falling back to ring (see reqwest's `TlsBackend::Rustls` build path).
        ensure_rustls_crypto_provider();
        let client = reqwest::Client::builder()
            // Force rustls: `default-tls` (native-tls) is also compiled in workspace-wide, so
            // without this the client would silently use native-tls instead.
            .use_rustls_tls()
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| ExecError::Upstream(e.to_string()))?;
        Ok(Self { client })
    }
}

#[async_trait]
impl Executor for CodexExecutor {
    async fn execute(
        &self,
        req: PreparedRequest,
        account: &Account,
    ) -> Result<ResponseStream, ExecError> {
        let url = format!("{}/responses", account.base_url.trim_end_matches('/'));
        let resp = self
            .client
            .post(&url)
            .bearer_auth(&account.bearer_token)
            // Minimal M1 laundering; full byte-parity fingerprint is M5.
            .header("user-agent", "codex_cli_rs")
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
