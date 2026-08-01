//! Operator identity forwarded by an authenticating reverse proxy (Tailscale Serve).
//!
//! # What this solves
//! Every sensitive dashboard operation — exporting an account's credentials, deleting an account,
//! registering or revoking a passkey — is audited, but until now the audit line could only say
//! *that* it happened. The dashboard authenticates with a single shared token or a passkey, and
//! neither carries a person. On a tailnet, `tailscale serve` already knows who the caller is and
//! states it in `Tailscale-User-Login`, so the audit trail can name an actor for free.
//!
//! # Why this is gated on a loopback bind (the whole security argument)
//! These headers are ordinary request headers. Anything that can reach this server directly can
//! set them to whatever it likes, so trusting them is only sound when the ONLY thing that can
//! reach the server is the proxy that sets them. Tailscale's own guidance is to have the backend
//! listen on localhost for exactly this reason.
//!
//! So the header is honoured only when the listener is bound to a loopback address, i.e. reachable
//! solely via a local reverse proxy. On any other bind the header is ignored outright — a remote
//! caller must never be able to forge an audit entry naming someone else.
//!
//! Even on loopback this trusts every LOCAL process, which is the same boundary the tokenless
//! local dashboard bypass already draws. It is an *attribution* aid, never an authorisation one:
//! nothing here grants access, it only labels an action that some other credential already
//! authorised.

use axum::http::HeaderMap;

/// Longest identity string retained. Tailscale logins are email addresses; this is generous for a
/// real one and bounds what an upstream header can push into logs.
const MAX_IDENTITY_LEN: usize = 128;

/// Who a reverse proxy says is making this request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardedIdentity {
    /// `Tailscale-User-Login` — an email address for a real user. Tailscale populates identity
    /// headers only for users, never for tagged devices, so an automated node yields `None` here
    /// rather than a misleading name.
    pub login: String,
    /// `Tailscale-User-Name` — the display name, when present.
    pub display_name: Option<String>,
}

impl ForwardedIdentity {
    /// How this actor should appear in an audit line.
    pub fn label(&self) -> &str {
        &self.login
    }
}

/// Trim, bound, and reject anything that would be unusable or unsafe in a log line.
///
/// Control characters are refused rather than stripped: a header carrying them is not a real
/// Tailscale identity, and silently "cleaning" it would let a caller shape how an audit entry
/// reads. An over-long value is likewise refused, not truncated — a truncated login could collide
/// with a different real one.
fn sanitize(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.len() > MAX_IDENTITY_LEN {
        return None;
    }
    if trimmed.chars().any(|c| c.is_control()) {
        return None;
    }
    Some(trimmed.to_string())
}

/// Read the proxy-forwarded identity, if it may be trusted.
///
/// `trust_forwarded` MUST be the startup-resolved "listener is loopback-bound" decision — see the
/// module docs. When it is false this returns `None` no matter what the caller sent.
pub fn forwarded_identity(headers: &HeaderMap, trust_forwarded: bool) -> Option<ForwardedIdentity> {
    if !trust_forwarded {
        return None;
    }
    let login = headers
        .get("tailscale-user-login")
        .and_then(|value| value.to_str().ok())
        .and_then(sanitize)?;
    let display_name = headers
        .get("tailscale-user-name")
        .and_then(|value| value.to_str().ok())
        .and_then(sanitize);
    Some(ForwardedIdentity {
        login,
        display_name,
    })
}

/// The actor label for an audit line: the forwarded identity, or a truthful placeholder.
///
/// Deliberately never invents a name. "unattributed" states plainly that the credential used
/// carries no identity — which is the honest reading of a shared admin token or a passkey session.
pub fn actor_label(headers: &HeaderMap, trust_forwarded: bool) -> String {
    forwarded_identity(headers, trust_forwarded)
        .map(|identity| identity.login)
        .unwrap_or_else(|| "unattributed".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in pairs {
            headers.insert(
                axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                axum::http::HeaderValue::from_str(value).unwrap(),
            );
        }
        headers
    }

    #[test]
    fn a_forwarded_identity_is_read_only_when_the_bind_permits_trusting_it() {
        let h = headers(&[
            ("tailscale-user-login", "sam@example.test"),
            ("tailscale-user-name", "Sam"),
        ]);
        let identity = forwarded_identity(&h, true).expect("loopback bind trusts the proxy");
        assert_eq!(identity.login, "sam@example.test");
        assert_eq!(identity.display_name.as_deref(), Some("Sam"));
        assert_eq!(identity.label(), "sam@example.test");

        // The security property: on any non-loopback bind the header is forgeable, so it is
        // ignored outright rather than believed.
        assert_eq!(forwarded_identity(&h, false), None);
        assert_eq!(actor_label(&h, false), "unattributed");
    }

    #[test]
    fn an_absent_identity_is_reported_honestly_rather_than_invented() {
        let empty = headers(&[]);
        assert_eq!(forwarded_identity(&empty, true), None);
        assert_eq!(actor_label(&empty, true), "unattributed");
        // A login with no display name is still a usable actor.
        let login_only = headers(&[("tailscale-user-login", "sam@example.test")]);
        let identity = forwarded_identity(&login_only, true).unwrap();
        assert_eq!(identity.display_name, None);
    }

    #[test]
    fn a_header_cannot_shape_or_forge_an_audit_line() {
        // Control characters would let a caller inject newlines into a log record. `HeaderValue`
        // already rejects them, so this can never arrive over real HTTP — asserted against
        // `sanitize` directly because the guard is defence in depth, not the only barrier.
        assert!(axum::http::HeaderValue::from_str("sam@example.test\nWARN forged").is_err());
        assert_eq!(sanitize("sam@example.test\nWARN forged"), None);
        assert_eq!(sanitize("tab\there"), None);

        // Over-long values are refused, not truncated: a truncated login could collide with a
        // different real one and misattribute an action.
        let long = "a".repeat(MAX_IDENTITY_LEN + 1);
        assert_eq!(
            forwarded_identity(&headers(&[("tailscale-user-login", &long)]), true),
            None
        );

        // Blank is absent, not an actor named "".
        assert_eq!(
            forwarded_identity(&headers(&[("tailscale-user-login", "   ")]), true),
            None
        );

        // A bad display name must not discard an otherwise-valid login.
        let mixed = headers(&[
            ("tailscale-user-login", "sam@example.test"),
            ("tailscale-user-name", "  "),
        ]);
        let identity = forwarded_identity(&mixed, true).unwrap();
        assert_eq!(identity.login, "sam@example.test");
        assert_eq!(identity.display_name, None);
    }
}
