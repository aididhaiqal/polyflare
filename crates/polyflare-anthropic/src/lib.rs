//! Anthropic backend: HTTP executor (M4a), rate-limit/error classification (M4a), OAuth (M4a,
//! VERIFY-gated), the cross-format translator (M4b). Byte-parity fingerprinting is M5.

pub mod claude_wire;
pub mod collect;
pub mod errors;
pub mod executor;
pub mod oauth;
pub mod oauth_contract;
pub mod reverse;
pub mod translate;

pub use claude_wire::{
    admit_native_request, forwarded_client_headers, AdmissionError, ClaudeEnvelope, HeaderSource,
};
pub use collect::MessageCollector;
pub use errors::{
    classify_status, parse_retry_after_secs, AnthropicErrorBody, AnthropicErrorDetail,
    AnthropicErrorType, StatusClass,
};
pub use executor::AnthropicExecutor;
pub use oauth::{
    classify_failure, generate_pkce, generate_state, should_refresh, verify_state,
    AnthropicOAuthClient, FailureClass, OAuthError, OAuthTokens, Pkce, UpstreamIdentity,
};
pub use oauth_contract::{OAuthContract, Provenance, CONTRACT};
pub use reverse::ResponsesToAnthropic;
pub use translate::AnthropicToResponses;
