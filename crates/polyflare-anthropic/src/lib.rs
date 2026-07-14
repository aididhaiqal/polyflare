//! Anthropic backend: HTTP executor (M4a), rate-limit/error classification (M4a), OAuth (M4a,
//! VERIFY-gated), the cross-format translator (M4b). Byte-parity fingerprinting is M5.

pub mod errors;
pub mod executor;
pub mod translate;

pub use errors::{
    classify_status, parse_retry_after_secs, AnthropicErrorBody, AnthropicErrorDetail,
    AnthropicErrorType, StatusClass,
};
pub use executor::AnthropicExecutor;
