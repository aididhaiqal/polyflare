//! The dashboard admin token: generation, storage, and the "is one configured" question that the
//! auth gate and the passkey guard both need answered.
//!
//! # Why this exists
//! `POLYFLARE_ADMIN_TOKEN` used to be the only way to configure a token. For a server run by
//! launchd or systemd that means writing the plaintext secret into the service definition — a file
//! that is world-readable by default (`~/Library/LaunchAgents/*.plist` is `0644`). There was no
//! supported way to configure an admin token that did not leave the secret in cleartext on disk.
//!
//! A token set by `polyflare admin-token set` is stored as a SHA-256 hash instead, so the on-disk
//! artefact cannot be replayed. The plaintext is printed exactly once and never persisted — the
//! same reveal-once discipline [`crate::keys`] enforces for client API keys.
//!
//! # Both sources stay valid
//! The environment variable is not deprecated: it is how a container or a CI run injects a token
//! without a writable database, and it is read before the store on every check. A deployment may
//! have either, both, or neither. "Neither" is the posture that keeps the tokenless loopback
//! bypass available (see [`crate::auth`]).
//!
//! # Rotation is live
//! Nothing here is cached in `AppState`: [`configured`] and [`stored_token_is_valid`] read the
//! store per call, so `admin-token set` and `admin-token clear` take effect on the next request
//! without a restart. That is deliberate — a token you cannot rotate without downtime is a token
//! that does not get rotated.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use rand::RngCore;

use polyflare_store::Store;

use crate::keys::sha256_hex;

/// Distinguishes an admin token from a `sk-pf-` client key at a glance. The two authenticate
/// different surfaces — the management API versus the proxy — and pasting one where the other
/// belongs should be obvious from the string itself.
const TOKEN_PREFIX: &str = "pfa_";

/// Characters of the plaintext kept for display. Enough to tell two tokens apart in
/// `admin-token status`; 8 characters of a 256-bit secret is nowhere near enough to use.
const DISPLAY_PREFIX_LEN: usize = TOKEN_PREFIX.len() + 8;

/// A freshly minted admin token. `raw` is the plaintext — revealed once by the CLI, never
/// persisted, never logged.
#[derive(Debug)]
pub struct GeneratedAdminToken {
    pub raw: String,
    pub hash: String,
    pub prefix: String,
}

/// Generate a `pfa_<base64url-nopad(32 CSPRNG bytes)>` token (256 bits of entropy).
///
/// Same RNG as [`crate::keys::generate_key`] — rand 0.9's ChaCha-backed thread generator, seeded
/// from OS entropy, already the workspace's vetted CSPRNG for PKCE verifiers and client keys.
pub fn generate() -> GeneratedAdminToken {
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    let raw = format!("{TOKEN_PREFIX}{}", URL_SAFE_NO_PAD.encode(bytes));
    let hash = sha256_hex(&raw);
    let prefix: String = raw.chars().take(DISPLAY_PREFIX_LEN).collect();
    GeneratedAdminToken { raw, hash, prefix }
}

/// The display prefix for an operator-supplied token, so an imported token is identifiable in
/// `status` exactly like a generated one.
pub fn display_prefix(raw: &str) -> String {
    raw.chars().take(DISPLAY_PREFIX_LEN).collect()
}

/// Whether ANY admin token is configured — the environment variable, or one in the store.
///
/// **Fails closed.** A store error returns `true` ("a token is configured"), because both callers
/// use this to decide whether to *withhold* something: the tokenless loopback bypass, and the
/// refusal to delete a last passkey. Answering "no token" on a database hiccup would open the
/// dashboard to any local process, which is the wrong failure mode for an auth decision.
pub async fn configured(env_token: Option<&str>, store: &Store) -> bool {
    if env_token.is_some() {
        return true;
    }
    store
        .admin_token()
        .get()
        .await
        .map(|row| row.is_some())
        .unwrap_or(true)
}

/// Whether `presented` matches the token stored by `admin-token set`.
///
/// **Fails closed** the other way — a store error returns `false` — because here the question is
/// whether to *admit* a caller. Comparison is against the SHA-256 of the presented token, so the
/// plaintext is never persisted and a database reader cannot replay it.
pub async fn stored_token_is_valid(presented: &str, store: &Store) -> bool {
    let Ok(Some(row)) = store.admin_token().get().await else {
        return false;
    };
    constant_time_eq(sha256_hex(presented).as_bytes(), row.token_hash.as_bytes())
}

/// Constant-time comparison over two hashes.
///
/// SHA-256 digests are not secret and a timing leak here would reveal only the stored hash, which
/// cannot be inverted into a 256-bit random token. Compared in constant time regardless: an auth
/// path that compares credential-derived bytes with `==` is a pattern worth never establishing,
/// and this mirrors the same treatment [`crate::auth`] gives the environment token.
fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let compared_len = left.len().max(right.len());
    let mut difference = left.len() ^ right.len();
    for index in 0..compared_len {
        let a = left.get(index).copied().unwrap_or(0);
        let b = right.get(index).copied().unwrap_or(0);
        difference |= usize::from(a ^ b);
    }
    difference == 0
}

/// What the CLI knows about how this deployment authenticates its dashboard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminTokenStatus {
    /// `POLYFLARE_ADMIN_TOKEN` **as seen by the CLI's own process**. A service started by launchd
    /// or systemd has its own environment, which this cannot observe — see [`render_status`].
    pub env_token_present: bool,
    /// Display prefix and creation time of the stored token, when one is installed.
    pub stored: Option<(String, i64)>,
    pub passkeys_registered: usize,
}

impl AdminTokenStatus {
    /// Whether a sign-in credential exists at all. When false — and the server is on a loopback
    /// bind — the dashboard is open to every local process.
    pub fn any_credential(&self) -> bool {
        self.env_token_present || self.stored.is_some() || self.passkeys_registered > 0
    }
}

/// Gather status from the store (authoritative, shared with the server) and the environment (this
/// process only).
pub async fn status(
    env_token: Option<&str>,
    store: &Store,
) -> Result<AdminTokenStatus, polyflare_store::StoreError> {
    let stored = store
        .admin_token()
        .get()
        .await?
        .map(|row| (row.token_prefix, row.created_at));
    let passkeys_registered = store.passkeys().list().await?.len();
    Ok(AdminTokenStatus {
        env_token_present: env_token.is_some(),
        stored,
        passkeys_registered,
    })
}

/// Render [`AdminTokenStatus`] for a terminal.
///
/// The environment line is explicitly scoped to "this shell". The CLI and the running service are
/// separate processes with separate environments, so a `POLYFLARE_ADMIN_TOKEN` exported in a
/// terminal says nothing about what launchd handed the server — reporting it unqualified would
/// invite exactly the wrong conclusion during a lockout.
pub fn render_status(status: &AdminTokenStatus, format_time: impl Fn(i64) -> String) -> String {
    let mut out = String::new();

    out.push_str("stored token   ");
    match &status.stored {
        Some((prefix, created_at)) => {
            out.push_str(&format!("{prefix}… (set {})\n", format_time(*created_at)));
        }
        None => out.push_str("none\n"),
    }

    out.push_str("env variable   ");
    if status.env_token_present {
        out.push_str("POLYFLARE_ADMIN_TOKEN set in this shell\n");
    } else {
        out.push_str("POLYFLARE_ADMIN_TOKEN unset in this shell\n");
    }
    out.push_str(
        "               (the running service has its own environment; this only reports \
         this shell)\n",
    );

    out.push_str(&format!(
        "passkeys       {} registered\n",
        status.passkeys_registered
    ));

    out.push('\n');
    if status.any_credential() {
        out.push_str("The dashboard requires a credential to sign in.\n");
    } else {
        out.push_str(
            "No credential is configured. On a loopback bind the dashboard API is open to \
             every local process — run `polyflare admin-token set` or register a passkey.\n",
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn store() -> Store {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(&dir.path().join("s.db")).await.unwrap();
        std::mem::forget(dir);
        s
    }

    #[test]
    fn a_generated_token_is_identifiable_high_entropy_and_hashed_not_echoed() {
        let token = generate();
        assert!(
            token.raw.starts_with("pfa_"),
            "an admin token must be distinguishable from a sk-pf- client key"
        );
        // 32 bytes base64url-nopad = 43 chars, plus the 4-char prefix.
        assert_eq!(token.raw.len(), 47);
        assert_eq!(token.hash, sha256_hex(&token.raw));
        assert_eq!(token.hash.len(), 64, "sha256 hex");
        assert!(
            !token.hash.contains(&token.raw),
            "the hash must not embed the plaintext"
        );
        assert_eq!(token.prefix, token.raw.chars().take(12).collect::<String>());
        assert!(
            !token
                .raw
                .starts_with(&format!("{}{}", token.prefix, token.prefix)),
            "sanity: prefix is a slice of the token, not a repeat"
        );

        // Two calls must not collide — a fixed or weakly seeded RNG would be catastrophic here.
        assert_ne!(generate().raw, generate().raw);
    }

    #[tokio::test]
    async fn a_stored_token_validates_and_a_wrong_one_does_not() {
        let s = store().await;
        let token = generate();
        s.admin_token()
            .set(&token.hash, &token.prefix, 100)
            .await
            .unwrap();

        assert!(stored_token_is_valid(&token.raw, &s).await);
        assert!(!stored_token_is_valid("pfa_wrong", &s).await);
        assert!(
            !stored_token_is_valid(&token.hash, &s).await,
            "presenting the HASH must not authenticate — otherwise a database reader could sign in"
        );
    }

    #[tokio::test]
    async fn rotation_invalidates_the_previous_token_immediately() {
        let s = store().await;
        let first = generate();
        s.admin_token()
            .set(&first.hash, &first.prefix, 100)
            .await
            .unwrap();
        let second = generate();
        s.admin_token()
            .set(&second.hash, &second.prefix, 200)
            .await
            .unwrap();

        assert!(stored_token_is_valid(&second.raw, &s).await);
        assert!(
            !stored_token_is_valid(&first.raw, &s).await,
            "the rotated-out token must stop working the moment the new one lands"
        );
    }

    #[tokio::test]
    async fn configured_sees_either_source_and_neither_means_neither() {
        let s = store().await;
        assert!(!configured(None, &s).await, "fresh store, no env token");
        assert!(configured(Some("from-env"), &s).await);

        let token = generate();
        s.admin_token()
            .set(&token.hash, &token.prefix, 100)
            .await
            .unwrap();
        assert!(
            configured(None, &s).await,
            "a stored token counts as configured, with no env var in sight"
        );

        s.admin_token().clear().await.unwrap();
        assert!(!configured(None, &s).await, "clearing removes the posture");
    }

    #[tokio::test]
    async fn a_cleared_token_stops_authenticating() {
        let s = store().await;
        let token = generate();
        s.admin_token()
            .set(&token.hash, &token.prefix, 100)
            .await
            .unwrap();
        s.admin_token().clear().await.unwrap();
        assert!(!stored_token_is_valid(&token.raw, &s).await);
    }

    #[tokio::test]
    async fn status_reports_both_sources_and_never_the_token_itself() {
        let s = store().await;
        let token = generate();
        s.admin_token()
            .set(&token.hash, &token.prefix, 1_700_000_000)
            .await
            .unwrap();

        let st = status(Some("from-env"), &s).await.unwrap();
        assert!(st.env_token_present);
        assert_eq!(
            st.stored,
            Some((token.prefix.clone(), 1_700_000_000)),
            "status carries the display prefix, never the hash"
        );
        assert_eq!(st.passkeys_registered, 0);
        assert!(st.any_credential());

        let rendered = render_status(&st, |_| "2026-08-03".to_string());
        assert!(
            !rendered.contains(&token.raw),
            "rendered status must never contain the plaintext token"
        );
        assert!(
            !rendered.contains(&token.hash),
            "rendered status must never contain the hash either"
        );
        assert!(rendered.contains(&token.prefix), "the prefix identifies it");
        assert!(
            rendered.contains("this shell"),
            "the env line must be scoped to this process, not claimed for the service"
        );
    }

    #[tokio::test]
    async fn status_names_the_open_posture_when_nothing_is_configured() {
        let s = store().await;
        let st = status(None, &s).await.unwrap();
        assert!(!st.any_credential());
        let rendered = render_status(&st, |_| "n/a".to_string());
        assert!(
            rendered.contains("open to every local process"),
            "an unconfigured dashboard must say so plainly: {rendered}"
        );
    }

    #[test]
    fn hash_comparison_requires_exact_bytes_and_length() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abd", b"abc"));
        assert!(!constant_time_eq(b"abc-extra", b"abc"));
        assert!(!constant_time_eq(b"", b"abc"));
    }
}
