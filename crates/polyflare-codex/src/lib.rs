//! Codex backend: WS/SSE transport, fingerprint laundering, continuity, OAuth. M1 = SSE identity
//! pass-through; M2b adds the `oauth` module (claims decode + refresh).

pub mod codex_headers;
pub mod codex_version;
pub mod control_forward;
pub mod executor;
pub mod login;
pub mod oauth;
pub mod ws;

pub use codex_version::CodexVersionCache;
pub use control_forward::{control_forward, control_url, ControlError, ControlResponse};
pub use executor::{build_client, CodexExecutor};
pub use login::{run_login, LoginError};
pub use oauth::{
    classify_failure, decode_claims, should_refresh, token_exp, Claims, FailureClass, OAuthClient,
    OAuthError, Refreshed, RefreshedTokens,
};
pub use ws::{CodexWsExecutor, WsConn};
