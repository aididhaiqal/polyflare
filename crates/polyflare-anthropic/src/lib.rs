//! Anthropic backend: HTTP executor (M4a), rate-limit/error classification (M4a), OAuth (M4a,
//! VERIFY-gated). Byte-parity fingerprinting + the cross-format translator are M4b/M5.

pub mod executor;

pub use executor::AnthropicExecutor;
