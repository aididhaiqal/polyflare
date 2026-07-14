//! Anthropic backend: HTTP executor (M4a), rate-limit/error classification (M4a), OAuth (M4a,
//! VERIFY-gated). Byte-parity fingerprinting + the cross-format translator are M4b/M5.

pub mod errors;
pub mod executor;

pub use errors::{
    classify_status, parse_retry_after_secs, AnthropicErrorBody, AnthropicErrorDetail,
    AnthropicErrorType, StatusClass,
};
pub use executor::AnthropicExecutor;
