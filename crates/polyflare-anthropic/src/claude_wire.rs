//! Admission and egress rules for genuine Claude Code `/v1/messages` traffic.
//!
//! This is the load balancer's core path, and it is deliberately the DUMBEST one: a real Claude
//! Code client already speaks the exact protocol the upstream expects, so PolyFlare's only job is
//! to replace the caller's credential with a selected account's and forward everything else
//! untouched. Nothing here parses, rewrites, or re-serializes the request body.
//!
//! Two rules make that safe:
//!
//! 1. **Admission is positive, not permissive.** A request is forwarded on this path only if it
//!    proves it is Claude-native ([`admit_native_request`]). Anything else — an ordinary Anthropic
//!    SDK call, a translated request, a hand-rolled HTTP client — is rejected here and must take a
//!    different path. This is what keeps subscription-OAuth accounts serving only the client shape
//!    they were authorized for.
//! 2. **Egress is an allowlist, not a denylist.** [`forwarded_client_headers`] names every header that may
//!    reach upstream. A header nobody explicitly allowed is dropped, so a future client header —
//!    or an attacker-supplied one — cannot ride along by default.
//!
//! ## What this is not
//!
//! This is application-protocol fidelity, not client impersonation. PolyFlare's HTTP stack still
//! differs from the real CLI's in header serialization, connection reuse, and TLS fingerprint, and
//! it cannot produce a native attestation. Where the client sent an attestation, it is forwarded
//! as opaque bytes and never generated, decoded, or claimed to still be valid after the account
//! substitution — see [`ClaudeEnvelope::attestation`].

use std::collections::HashSet;

/// Why a request was not admitted to the native pass-through path.
///
/// Carries only the structural reason — never a header value, body fragment, or token.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AdmissionError {
    #[error("user-agent is not a Claude Code CLI agent")]
    NotClaudeCli,
    #[error("missing required header: {0}")]
    MissingHeader(&'static str),
    #[error("header {0} is malformed")]
    MalformedHeader(&'static str),
    #[error("request body is not a native Anthropic Messages body: {0}")]
    NotNativeBody(&'static str),
    #[error("required beta {0} is absent")]
    MissingBeta(&'static str),
}

/// The beta that marks a request as authorized by a subscription OAuth grant rather than an API
/// key. Its presence is what distinguishes Claude Code subscription traffic from generic SDK
/// traffic that merely copied a user agent.
pub const OAUTH_BETA: &str = "oauth-2025-04-20";

/// The Messages API version every Claude client sends.
pub const ANTHROPIC_VERSION: &str = "2023-06-01";

/// A content-free, typed view of an admitted Claude Code request.
///
/// Deliberately holds no message text, tool arguments, or system-prompt content: everything here
/// is safe to log, count, and persist as compatibility telemetry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeEnvelope {
    /// The Claude Code version parsed from the user agent (e.g. `2.1.218`).
    pub cli_version: String,
    /// The client's own session id — forwarded unchanged. NEVER derived from a token or
    /// regenerated: a synthesized session id would silently break the client's own continuity.
    pub session_id: String,
    /// `anthropic-beta` tokens in the order the client sent them, deduplicated. Order is preserved
    /// because it is part of the request the client built and is not ours to normalize.
    pub betas: Vec<String>,
    /// The Stainless SDK package version, when present.
    pub sdk_version: Option<String>,
    /// An opaque native-attestation value, if the client sent one.
    ///
    /// Forwarded verbatim and treated as meaningless input: PolyFlare cannot generate one, cannot
    /// verify one, and does not assume one survives the account substitution. If Anthropic ever
    /// binds attestation to the account or token, this path fails upstream — which is the intended
    /// outcome, not something to paper over with a fabricated value.
    pub attestation: Option<String>,
}

impl ClaudeEnvelope {
    /// A stable, content-free identity for compatibility telemetry: which client shape this was.
    pub fn shape_key(&self) -> String {
        format!(
            "claude-cli/{};sdk/{};betas/{}",
            self.cli_version,
            self.sdk_version.as_deref().unwrap_or("-"),
            self.betas.join(",")
        )
    }
}

/// Parse the Claude Code version out of a CLI user agent.
///
/// Accepts `claude-cli/2.1.218 (external, sdk-ts, ...)` and the bare `claude-cli/2.1.218`. The
/// parenthesised part is informational (the Agent SDK bridge marks itself `external` there) and is
/// not used for admission.
fn parse_cli_version(user_agent: &str) -> Option<String> {
    let rest = user_agent.trim().strip_prefix("claude-cli/")?;
    let version: String = rest
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '.')
        .collect();
    // Require a dotted numeric version, so `claude-cli/x` cannot masquerade as a real client.
    if version.is_empty() || !version.contains('.') {
        return None;
    }
    Some(version)
}

/// Whether `value` is a canonical lowercase hyphenated UUID.
///
/// Claude session and request ids are UUIDs; validating the shape stops an arbitrary caller string
/// from being forwarded into a header we assert is a session identifier.
fn is_uuid(value: &str) -> bool {
    let groups = [8usize, 4, 4, 4, 12];
    let parts: Vec<&str> = value.split('-').collect();
    parts.len() == groups.len()
        && parts
            .iter()
            .zip(groups)
            .all(|(part, len)| part.len() == len && part.bytes().all(|b| b.is_ascii_hexdigit()))
}

/// Split an `anthropic-beta` header into ordered, deduplicated tokens.
pub fn parse_betas(raw: &str) -> Vec<String> {
    let mut seen = HashSet::new();
    raw.split(',')
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .filter(|token| seen.insert(token.to_string()))
        .map(str::to_string)
        .collect()
}

/// A borrowed view of the inbound headers, so this module stays independent of any HTTP type.
pub trait HeaderSource {
    /// The first value for a lowercase header name.
    fn get(&self, name: &str) -> Option<&str>;
    /// Every present header name, lowercased.
    fn names(&self) -> Vec<String>;
}

impl HeaderSource for Vec<(String, String)> {
    fn get(&self, name: &str) -> Option<&str> {
        self.iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    fn names(&self) -> Vec<String> {
        self.iter()
            .map(|(key, _)| key.to_ascii_lowercase())
            .collect()
    }
}

/// Admit a request to the native pass-through path, or explain why it does not qualify.
///
/// Requires, in order: a Claude CLI user agent, the Messages API version, the OAuth beta, a
/// well-formed session id, and a body that is already a native Anthropic Messages body. Each check
/// is structural — none of them inspects message content.
pub fn admit_native_request(
    headers: &impl HeaderSource,
    body: &serde_json::Value,
) -> Result<ClaudeEnvelope, AdmissionError> {
    let user_agent = headers
        .get("user-agent")
        .ok_or(AdmissionError::MissingHeader("user-agent"))?;
    let cli_version = parse_cli_version(user_agent).ok_or(AdmissionError::NotClaudeCli)?;

    let version = headers
        .get("anthropic-version")
        .ok_or(AdmissionError::MissingHeader("anthropic-version"))?;
    if version != ANTHROPIC_VERSION {
        return Err(AdmissionError::MalformedHeader("anthropic-version"));
    }

    let betas = parse_betas(
        headers
            .get("anthropic-beta")
            .ok_or(AdmissionError::MissingHeader("anthropic-beta"))?,
    );
    // Without the OAuth beta this is not subscription-authorized traffic, whatever its user agent
    // claims. Forwarding it on a subscription account would be sending a request shape the grant
    // does not cover.
    if !betas.iter().any(|beta| beta == OAUTH_BETA) {
        return Err(AdmissionError::MissingBeta(OAUTH_BETA));
    }

    let session_id = headers
        .get("x-claude-code-session-id")
        .ok_or(AdmissionError::MissingHeader("x-claude-code-session-id"))?;
    if !is_uuid(session_id) {
        return Err(AdmissionError::MalformedHeader("x-claude-code-session-id"));
    }

    validate_native_body(body)?;

    Ok(ClaudeEnvelope {
        cli_version,
        session_id: session_id.to_string(),
        betas,
        sdk_version: headers
            .get("x-stainless-package-version")
            .map(str::to_string),
        attestation: headers.get(ATTESTATION_HEADER).map(str::to_string),
    })
}

/// Structural check that a body is already a native Anthropic Messages body.
///
/// Shape only: field presence and JSON types. It never reads message text, tool arguments, or
/// system-prompt content, and it does not enforce Claude Code's particular system-block layout —
/// that is a client detail which changes between releases, and pinning it here would reject valid
/// newer clients the pass-through is otherwise happy to forward.
fn validate_native_body(body: &serde_json::Value) -> Result<(), AdmissionError> {
    let object = body
        .as_object()
        .ok_or(AdmissionError::NotNativeBody("body is not a JSON object"))?;
    if !object
        .get("model")
        .is_some_and(serde_json::Value::is_string)
    {
        return Err(AdmissionError::NotNativeBody("model must be a string"));
    }
    let messages = object
        .get("messages")
        .and_then(serde_json::Value::as_array)
        .ok_or(AdmissionError::NotNativeBody("messages must be an array"))?;
    if messages.is_empty() {
        return Err(AdmissionError::NotNativeBody("messages must be non-empty"));
    }
    if !object
        .get("max_tokens")
        .is_some_and(serde_json::Value::is_number)
    {
        return Err(AdmissionError::NotNativeBody("max_tokens must be a number"));
    }
    // A Responses-shaped body reaching this path would mean a translated request was misrouted
    // onto the native pass-through; reject rather than forward something the upstream will not
    // recognize as Messages.
    if object.contains_key("input") || object.contains_key("instructions") {
        return Err(AdmissionError::NotNativeBody(
            "body carries OpenAI-Responses fields",
        ));
    }
    Ok(())
}

/// The opaque native-attestation header, forwarded but never generated or interpreted.
pub const ATTESTATION_HEADER: &str = "x-claude-code-attestation";

/// Headers forwarded verbatim from an admitted Claude request.
///
/// This is the complete allowlist. `authorization` is deliberately absent: it is always REPLACED
/// with the selected account's credential, never forwarded from the caller.
const FORWARDED_HEADERS: &[&str] = &[
    "accept",
    "content-type",
    "anthropic-version",
    "anthropic-beta",
    "anthropic-dangerous-direct-browser-access",
    "user-agent",
    "x-app",
    "x-claude-code-session-id",
    "x-client-app",
    "x-stainless-arch",
    "x-stainless-lang",
    "x-stainless-os",
    "x-stainless-package-version",
    "x-stainless-retry-count",
    "x-stainless-runtime",
    "x-stainless-runtime-version",
    "x-stainless-timeout",
    ATTESTATION_HEADER,
];

/// The allowlisted client headers to forward for an admitted request.
///
/// Everything not named in [`FORWARDED_HEADERS`] is dropped — including the caller's own
/// `authorization` and any cookie, `host`, `content-length`, or hop-by-hop header. Dropping
/// `content-length`/`host` matters beyond hygiene: they describe the ORIGINAL connection and would
/// be wrong for the upstream one.
///
/// No credential is added here, because at ingress there is not yet one to add: the account is
/// chosen later, by selection. The executor attaches the selected account's bearer to whatever this
/// returns. That ordering is also why dropping the caller's `authorization` is not merely tidy —
/// leaving it in would make the executor treat the request as already authorized and skip the
/// substitution entirely, forwarding the caller's own credential upstream.
pub fn forwarded_client_headers(inbound: &impl HeaderSource) -> Vec<(String, String)> {
    let mut out = Vec::with_capacity(FORWARDED_HEADERS.len());
    for name in FORWARDED_HEADERS {
        if let Some(value) = inbound.get(name) {
            out.push(((*name).to_string(), value.to_string()));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Headers captured from a real Claude Code 2.1.218 request (see the Agent SDK bridge POC).
    fn claude_headers() -> Vec<(String, String)> {
        vec![
            ("accept", "application/json"),
            ("content-type", "application/json"),
            ("anthropic-version", "2023-06-01"),
            (
                "anthropic-beta",
                "claude-code-20250219,oauth-2025-04-20,interleaved-thinking-2025-05-14",
            ),
            ("anthropic-dangerous-direct-browser-access", "true"),
            (
                "user-agent",
                "claude-cli/2.1.218 (external, sdk-ts, agent-sdk/0.3.218)",
            ),
            ("x-app", "cli"),
            (
                "x-claude-code-session-id",
                "c38f98c8-7c2a-4e93-aa3d-a79df7a7015f",
            ),
            ("x-stainless-lang", "js"),
            ("x-stainless-package-version", "0.94.0"),
            ("authorization", "Bearer CALLER-SECRET"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
    }

    fn claude_body() -> serde_json::Value {
        json!({
            "model": "claude-sonnet-4-6",
            "max_tokens": 4096,
            "stream": true,
            "system": [{"type": "text", "text": "SYSTEM-PROMPT-MARKER"}],
            "messages": [{"role": "user", "content": "USER-CONTENT-MARKER"}],
        })
    }

    #[test]
    fn a_real_claude_request_is_admitted_with_a_content_free_envelope() {
        let envelope = admit_native_request(&claude_headers(), &claude_body()).unwrap();
        assert_eq!(envelope.cli_version, "2.1.218");
        assert_eq!(envelope.session_id, "c38f98c8-7c2a-4e93-aa3d-a79df7a7015f");
        assert_eq!(
            envelope.betas,
            vec![
                "claude-code-20250219",
                "oauth-2025-04-20",
                "interleaved-thinking-2025-05-14"
            ],
            "beta order is the client's and is preserved"
        );
        assert_eq!(envelope.sdk_version.as_deref(), Some("0.94.0"));
        assert_eq!(envelope.attestation, None);

        // The envelope is telemetry-safe: no message content anywhere in its debug form.
        let rendered = format!("{envelope:?}");
        assert!(!rendered.contains("SYSTEM-PROMPT-MARKER"));
        assert!(!rendered.contains("USER-CONTENT-MARKER"));
    }

    #[test]
    fn non_claude_clients_are_rejected_rather_than_forwarded() {
        let swap = |name: &str, value: Option<&str>| {
            let mut headers: Vec<(String, String)> = claude_headers()
                .into_iter()
                .filter(|(k, _)| k != name)
                .collect();
            if let Some(value) = value {
                headers.push((name.to_string(), value.to_string()));
            }
            headers
        };

        // A generic Anthropic SDK call: right API, wrong client. It is not eligible for a
        // subscription account no matter how well-formed it is.
        assert_eq!(
            admit_native_request(
                &swap("user-agent", Some("anthropic-sdk-python/0.40.0")),
                &claude_body()
            ),
            Err(AdmissionError::NotClaudeCli)
        );
        // A user agent that merely starts with the right prefix but carries no real version.
        assert_eq!(
            admit_native_request(&swap("user-agent", Some("claude-cli/x")), &claude_body()),
            Err(AdmissionError::NotClaudeCli)
        );
        // Claude-shaped, but not subscription-authorized traffic.
        assert_eq!(
            admit_native_request(
                &swap("anthropic-beta", Some("claude-code-20250219")),
                &claude_body()
            ),
            Err(AdmissionError::MissingBeta(OAUTH_BETA))
        );
        // A session id that is not a UUID must not be forwarded as though it were one.
        assert_eq!(
            admit_native_request(
                &swap("x-claude-code-session-id", Some("../../etc/passwd")),
                &claude_body()
            ),
            Err(AdmissionError::MalformedHeader("x-claude-code-session-id"))
        );
        assert_eq!(
            admit_native_request(&swap("x-claude-code-session-id", None), &claude_body()),
            Err(AdmissionError::MissingHeader("x-claude-code-session-id"))
        );
        assert_eq!(
            admit_native_request(
                &swap("anthropic-version", Some("2024-01-01")),
                &claude_body()
            ),
            Err(AdmissionError::MalformedHeader("anthropic-version"))
        );
    }

    #[test]
    fn a_translated_or_non_messages_body_is_never_admitted() {
        // An OpenAI-Responses body misrouted onto the native path.
        let responses_body = json!({
            "model": "gpt-5",
            "max_tokens": 100,
            "messages": [{"role": "user", "content": "hi"}],
            "input": [],
            "instructions": "be brief",
        });
        assert_eq!(
            admit_native_request(&claude_headers(), &responses_body),
            Err(AdmissionError::NotNativeBody(
                "body carries OpenAI-Responses fields"
            ))
        );
        for (body, reason) in [
            (
                json!({"max_tokens": 1, "messages": [{}]}),
                "model must be a string",
            ),
            (
                json!({"model": "m", "max_tokens": 1}),
                "messages must be an array",
            ),
            (
                json!({"model": "m", "max_tokens": 1, "messages": []}),
                "messages must be non-empty",
            ),
            (
                json!({"model": "m", "messages": [{}]}),
                "max_tokens must be a number",
            ),
        ] {
            assert_eq!(
                admit_native_request(&claude_headers(), &body),
                Err(AdmissionError::NotNativeBody(reason))
            );
        }
        assert_eq!(
            admit_native_request(&claude_headers(), &json!("not an object")),
            Err(AdmissionError::NotNativeBody("body is not a JSON object"))
        );
    }

    #[test]
    fn egress_drops_the_caller_credential_and_everything_unlisted() {
        let mut inbound = claude_headers();
        // Headers a caller might add that must NOT reach upstream.
        inbound.push(("cookie".into(), "session=abc".into()));
        inbound.push(("host".into(), "polyflare.internal".into()));
        inbound.push(("content-length".into(), "999".into()));
        inbound.push(("connection".into(), "keep-alive".into()));
        inbound.push(("x-forwarded-for".into(), "10.0.0.1".into()));
        inbound.push(("x-some-future-header".into(), "surprise".into()));

        let out = forwarded_client_headers(&inbound);
        let names: Vec<&str> = out.iter().map(|(k, _)| k.as_str()).collect();

        // No credential at all: the account is not chosen yet, and leaving the caller's own
        // `authorization` here would make the executor skip the substitution and forward it.
        assert!(!names.contains(&"authorization"));
        assert!(!out.iter().any(|(_, v)| v.contains("CALLER-SECRET")));
        for dropped in [
            "cookie",
            "host",
            "content-length",
            "connection",
            "x-forwarded-for",
            "x-some-future-header",
        ] {
            assert!(!names.contains(&dropped), "{dropped} must not be forwarded");
        }
        // The client's own protocol envelope survives untouched.
        for kept in [
            "anthropic-version",
            "anthropic-beta",
            "user-agent",
            "x-app",
            "x-claude-code-session-id",
            "x-stainless-package-version",
        ] {
            assert!(names.contains(&kept), "{kept} must be forwarded");
        }
        assert_eq!(
            out.iter()
                .find(|(k, _)| k == "anthropic-beta")
                .map(|(_, v)| v.as_str()),
            Some("claude-code-20250219,oauth-2025-04-20,interleaved-thinking-2025-05-14"),
            "the beta header is forwarded verbatim, order included"
        );
    }

    #[test]
    fn an_attestation_is_forwarded_opaquely_and_never_invented() {
        let mut inbound = claude_headers();
        inbound.push((ATTESTATION_HEADER.into(), "opaque-blob".into()));
        let envelope = admit_native_request(&inbound, &claude_body()).unwrap();
        assert_eq!(envelope.attestation.as_deref(), Some("opaque-blob"));
        let out = forwarded_client_headers(&inbound);
        assert_eq!(
            out.iter()
                .find(|(k, _)| k == ATTESTATION_HEADER)
                .map(|(_, v)| v.as_str()),
            Some("opaque-blob")
        );

        // When the client sends none, PolyFlare does not fabricate one.
        let without = forwarded_client_headers(&claude_headers());
        assert!(!without.iter().any(|(k, _)| k == ATTESTATION_HEADER));
    }

    #[test]
    fn beta_tokens_are_deduplicated_but_keep_first_seen_order() {
        assert_eq!(
            parse_betas("a, b ,a,, c"),
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
        assert!(parse_betas("").is_empty());
    }

    #[test]
    fn uuid_validation_rejects_near_misses() {
        assert!(is_uuid("c38f98c8-7c2a-4e93-aa3d-a79df7a7015f"));
        assert!(!is_uuid("c38f98c8-7c2a-4e93-aa3d-a79df7a7015"));
        assert!(!is_uuid("c38f98c8_7c2a_4e93_aa3d_a79df7a7015f"));
        assert!(!is_uuid("zzzzzzzz-7c2a-4e93-aa3d-a79df7a7015f"));
        assert!(!is_uuid(""));
    }

    #[test]
    fn shape_key_is_stable_and_content_free() {
        let envelope = admit_native_request(&claude_headers(), &claude_body()).unwrap();
        assert_eq!(
            envelope.shape_key(),
            "claude-cli/2.1.218;sdk/0.94.0;betas/claude-code-20250219,oauth-2025-04-20,\
             interleaved-thinking-2025-05-14"
        );
    }
}
