//! The reviewed Anthropic subscription-OAuth contract: endpoints, public client id, scopes.
//!
//! Everything an operator-owned OAuth login depends on lives here, in ONE module, so an upstream
//! OAuth change is reviewed as a single diff instead of being chased through scattered literals.
//! This is deliberately separate from the per-request Claude *wire* profile (headers, betas, body
//! shape): the two change on completely different schedules, and a new Claude Code release must not
//! be able to rewrite an account's OAuth contract.
//!
//! # Provenance and verification status
//!
//! The endpoint set below is taken from this repository's reviewed plan
//! (`docs/plans/2026-07-24-202047-01-plan-anthropic-subscription-oauth.md`, Outcome 2), which
//! grounded it in the supplied source plus a loopback capture of Claude Code `2.1.218`.
//!
//! [`CLIENT_ID`] is the one value NOT independently reproducible from any capture held in this
//! repository: a Messages capture proves the request shape, never the OAuth client registration.
//! It is therefore marked [`Provenance::UnverifiedInRepo`] and MUST be confirmed by the Outcome 7
//! static-string probe before the Outcome 8 release gate. [`CONTRACT`] exposes that status so a
//! caller can refuse to start a real login rather than silently attempting one with a guessed
//! registration — see [`OAuthContract::verified_for_production`].

/// How well-grounded a contract value is in evidence available to this repository.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Provenance {
    /// Reproducible from a reviewed fixture or capture committed to this repository.
    CapturedFixture,
    /// Taken from the reviewed plan/source but not reproducible from a committed capture. Requires
    /// the compatibility probe's static-string check before a production login is allowed.
    UnverifiedInRepo,
}

/// A versioned Anthropic OAuth contract profile.
#[derive(Debug, Clone, Copy)]
pub struct OAuthContract {
    /// Stable identifier persisted on the account row (`oauth_contract_version`), so a row records
    /// which contract it was onboarded under.
    pub version: &'static str,
    pub client_id: &'static str,
    pub authorize_url: &'static str,
    pub token_url: &'static str,
    /// The hosted callback used by the manual (paste `code#state`) flow. The automatic flow binds
    /// an OS-assigned loopback port instead and supplies its own redirect URI.
    pub manual_redirect_uri: &'static str,
    /// Minimum scope for serving inference on an operator-owned subscription.
    pub default_scopes: &'static str,
    pub client_id_provenance: Provenance,
}

impl OAuthContract {
    /// Whether this profile may be used for a real login against production Anthropic.
    ///
    /// Fails closed while any value is [`Provenance::UnverifiedInRepo`]. Starting a browser OAuth
    /// flow with a guessed client registration would send an operator's real credentials into an
    /// unverified exchange, so this gate refuses rather than "probably working".
    pub fn verified_for_production(&self) -> bool {
        matches!(self.client_id_provenance, Provenance::CapturedFixture)
    }
}

/// The first production OAuth-contract profile.
///
/// Scope policy: request `user:inference` only. `user:profile` is deliberately NOT requested by
/// default — identity is recovered from the token response's own account fields instead, so an
/// ordinary login never asks for a broader grant than serving inference requires. Claude Code's
/// other permissions (MCP, file upload, API-key creation, session management) are never requested.
pub const CONTRACT: OAuthContract = OAuthContract {
    version: "anthropic-oauth-2026-07",
    // Public, non-secret client registration for the Claude Code CLI — the same class of protocol
    // constant as the Codex CLI's `app_EMoamEEZ73f0CkXaXp7hrann` in `polyflare-codex`.
    // See the module docs: unverified in-repo, gated by `verified_for_production`.
    client_id: "9d1c250a-e61b-44d9-88ed-5944d1962f5e",
    authorize_url: "https://claude.com/cai/oauth/authorize",
    token_url: "https://platform.claude.com/v1/oauth/token",
    manual_redirect_uri: "https://platform.claude.com/oauth/code/callback",
    default_scopes: "user:inference",
    client_id_provenance: Provenance::UnverifiedInRepo,
};

/// Hosts an OAuth request may be sent to in production.
///
/// Endpoint selection is allowlisted rather than free-form so a configuration mistake, or a value
/// that reaches the contract from anywhere but this reviewed module, cannot redirect an
/// authorization code or a refresh token to an attacker-controlled host. Test endpoint injection
/// exists only through [`super::oauth::AnthropicOAuthClient::with_endpoints`], which is
/// `#[cfg(test)]`-gated and unreachable from a production build.
const ALLOWED_HOSTS: &[&str] = &["claude.com", "platform.claude.com"];

/// Whether `url` is an https URL on an allowlisted Anthropic OAuth host.
pub fn is_allowed_endpoint(url: &str) -> bool {
    let Some(rest) = url.strip_prefix("https://") else {
        return false;
    };
    let host = rest.split(['/', '?', '#']).next().unwrap_or("");
    // Compare the full host: a suffix match would accept `claude.com.evil.test`.
    ALLOWED_HOSTS.contains(&host)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_contract_endpoints_are_allowlisted_https() {
        assert!(is_allowed_endpoint(CONTRACT.authorize_url));
        assert!(is_allowed_endpoint(CONTRACT.token_url));
        assert!(is_allowed_endpoint(CONTRACT.manual_redirect_uri));
    }

    #[test]
    fn endpoint_allowlist_rejects_lookalike_and_downgraded_hosts() {
        assert!(!is_allowed_endpoint(
            "https://claude.com.evil.test/oauth/token"
        ));
        assert!(!is_allowed_endpoint("https://evil.test/claude.com"));
        assert!(!is_allowed_endpoint(
            "http://platform.claude.com/v1/oauth/token"
        ));
        assert!(!is_allowed_endpoint("https://notclaude.com/x"));
        // A userinfo segment must not smuggle an allowlisted name past the host check.
        assert!(!is_allowed_endpoint("https://claude.com@evil.test/token"));
    }

    #[test]
    fn contract_is_gated_until_its_client_registration_is_captured() {
        // This assertion is a REMINDER, not a preference: flip the provenance to `CapturedFixture`
        // only once the Outcome 7 probe reproduces the client id, and this test will then fail and
        // force a deliberate update here.
        assert!(!CONTRACT.verified_for_production());
        assert_eq!(CONTRACT.default_scopes, "user:inference");
    }
}
