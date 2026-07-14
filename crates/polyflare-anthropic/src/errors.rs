//! Anthropic error/rate-limit classification: the doc-verified surface shared by both the
//! platform API-key and Claude subscription-OAuth pools (SPEC-M4 §3.5a). Header-based rate-limit
//! signals (`anthropic-ratelimit-*`) are API-key-only and NOT read here; the ccflare-style
//! subscription signal set is VERIFY-gated (see Task 7 in `docs/PLAN-M4a.md`).

use std::time::Duration;

/// The Anthropic error-response `error.type` vocabulary that is doc-verified and shared by both
/// account surfaces (SPEC-M4 §3.5a). Reused verbatim for mid-stream SSE `error` events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnthropicErrorType {
    InvalidRequest,
    Authentication,
    Permission,
    NotFound,
    RequestTooLarge,
    RateLimit,
    Api,
    Overloaded,
    /// Any `error.type` string not in the doc-verified set above.
    Unknown,
}

impl AnthropicErrorType {
    pub fn from_wire(s: &str) -> Self {
        match s {
            "invalid_request_error" => Self::InvalidRequest,
            "authentication_error" => Self::Authentication,
            "permission_error" => Self::Permission,
            "not_found_error" => Self::NotFound,
            "request_too_large" => Self::RequestTooLarge,
            "rate_limit_error" => Self::RateLimit,
            "api_error" => Self::Api,
            "overloaded_error" => Self::Overloaded,
            _ => Self::Unknown,
        }
    }
}

/// The always-present Anthropic error-response envelope: `{"type":"error","error":{"type",
/// "message"},"request_id"}` (SPEC-M4 §3.5a; doc-verified against the Anthropic TypeScript SDK's
/// error-extraction code, `src/core/error.ts`).
#[derive(Debug, Clone, serde::Deserialize)]
pub struct AnthropicErrorBody {
    pub error: AnthropicErrorDetail,
    pub request_id: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct AnthropicErrorDetail {
    #[serde(rename = "type")]
    pub error_type: String,
    pub message: String,
}

impl AnthropicErrorBody {
    /// The classified `error.type`.
    pub fn classified(&self) -> AnthropicErrorType {
        AnthropicErrorType::from_wire(&self.error.error_type)
    }
}

/// Classify an HTTP status into the doc-verified bucket (SPEC-M4 §3.5a): 429 is rate-limiting,
/// 529 is Anthropic's confirmed-real overload status, 401/403/404/413/500..504 map to their
/// standard meaning. Anything else is `Unclassified`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusClass {
    RateLimited,
    Overloaded,
    Authentication,
    Permission,
    NotFound,
    RequestTooLarge,
    ServerError,
    Unclassified,
}

pub fn classify_status(status: u16) -> StatusClass {
    match status {
        429 => StatusClass::RateLimited,
        529 => StatusClass::Overloaded,
        401 => StatusClass::Authentication,
        403 => StatusClass::Permission,
        404 => StatusClass::NotFound,
        413 => StatusClass::RequestTooLarge,
        500..=504 => StatusClass::ServerError,
        _ => StatusClass::Unclassified,
    }
}

/// Parse the `Retry-After` header as a plain integer number of seconds — the form Anthropic's API
/// sends in practice. The HTTP-date form is valid per RFC 7231 but not implemented here (YAGNI —
/// revisit if a live capture ever shows it).
pub fn parse_retry_after_secs(value: &str) -> Option<Duration> {
    value.trim().parse::<u64>().ok().map(Duration::from_secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_known_error_types() {
        assert_eq!(
            AnthropicErrorType::from_wire("rate_limit_error"),
            AnthropicErrorType::RateLimit
        );
        assert_eq!(
            AnthropicErrorType::from_wire("overloaded_error"),
            AnthropicErrorType::Overloaded
        );
        assert_eq!(
            AnthropicErrorType::from_wire("permission_error"),
            AnthropicErrorType::Permission
        );
        assert_eq!(
            AnthropicErrorType::from_wire("something_new"),
            AnthropicErrorType::Unknown
        );
    }

    #[test]
    fn classifies_known_statuses() {
        assert_eq!(classify_status(429), StatusClass::RateLimited);
        assert_eq!(classify_status(529), StatusClass::Overloaded);
        assert_eq!(classify_status(401), StatusClass::Authentication);
        assert_eq!(classify_status(403), StatusClass::Permission);
        assert_eq!(classify_status(504), StatusClass::ServerError);
        assert_eq!(classify_status(200), StatusClass::Unclassified);
    }

    #[test]
    fn parses_the_doc_verified_error_body_shape() {
        let body: AnthropicErrorBody = serde_json::from_str(
            r#"{"type":"error","error":{"type":"rate_limit_error","message":"slow down"},"request_id":"req_1"}"#,
        )
        .unwrap();
        assert_eq!(body.classified(), AnthropicErrorType::RateLimit);
        assert_eq!(body.error.message, "slow down");
        assert_eq!(body.request_id.as_deref(), Some("req_1"));
    }

    #[test]
    fn retry_after_parses_plain_seconds() {
        assert_eq!(
            parse_retry_after_secs("30"),
            Some(std::time::Duration::from_secs(30))
        );
        assert_eq!(
            parse_retry_after_secs("  7 "),
            Some(std::time::Duration::from_secs(7))
        );
        assert_eq!(parse_retry_after_secs("not-a-number"), None);
    }
}
