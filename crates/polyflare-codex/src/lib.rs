//! Codex backend: WS/SSE transport, fingerprint laundering, continuity, OAuth. M1 = SSE identity
//! pass-through; M2b adds the `oauth` module (claims decode + refresh).

pub mod executor;
pub mod oauth;

pub use executor::CodexExecutor;
pub use oauth::{
    classify_failure, decode_claims, should_refresh, Claims, FailureClass, OAuthClient, OAuthError,
    Refreshed, RefreshedTokens,
};
