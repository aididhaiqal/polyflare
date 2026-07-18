//! D18 Task 2: client API-key generation + the store-backed create/list/revoke logic behind the
//! `polyflare keys` CLI (`main.rs`). This module owns the ONLY place a raw client key is ever
//! produced. The reveal-once discipline (D18 Global Constraint — "store only the hash, reveal the
//! plaintext exactly once at creation") is enforced structurally here:
//!
//! - [`generate_key`] returns the raw key to its caller and nowhere else — it is not stored, not
//!   logged, not retained by this module after the call returns.
//! - [`create_key`] stores `key_hash`/`key_prefix` (via Task 1's [`polyflare_store::ApiKeyRepo`])
//!   and hands the raw key back to ITS caller (the CLI) for a one-time `println!` to stdout — the
//!   only reveal channel. Nothing in this module calls `tracing::`/`eprintln!` with the raw key
//!   (see the `never_logs_the_raw_key` test below, which asserts this mechanically for the pure
//!   and the store-touching path).
//! - [`render_key_row`] formats an already-hash-only [`polyflare_store::ApiKeyRow`] for `keys
//!   list` — there is no raw-key field on that type to accidentally print (Task 1's `ApiKeyRow`
//!   carries no `key_hash`/raw-key field at all), so this function is content-safe by construction.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use rand::RngCore;
use sha2::{Digest, Sha256};

use polyflare_store::{ApiKeyRow, Store, StoreError};

/// Chars of the raw key kept as a display prefix in `keys list` — enough to tell keys apart at a
/// glance, nowhere near enough to reconstruct or use the key (the full key is `sk-pf-` (6 chars) +
/// 43 base64url chars = 49 chars; this prefix is well short of even the encoded-bytes portion).
pub const KEY_PREFIX_LEN: usize = 15;

/// A freshly minted client API key. `raw` is the plaintext — reveal-once, never persisted, never
/// logged, dropped by every caller after the one `println!` at `keys create`.
#[derive(Debug)]
pub struct GeneratedKey {
    pub raw: String,
    pub key_hash: String,
    pub key_prefix: String,
}

/// Generates a `sk-pf-<base64url-nopad(32 CSPRNG bytes)>` key (256 bits of entropy) plus its
/// sha256 hex digest and display prefix.
///
/// RNG choice: `rand::rng()` — rand 0.9's thread-local generator, ChaCha-backed and seeded from OS
/// entropy, i.e. a vetted CSPRNG, not `SmallRng`/`StdRng`-for-simulation or any non-crypto source.
/// This is the SAME generator this codebase already trusts for other security-critical randomness
/// — `polyflare_codex::oauth`'s PKCE `code_verifier`/`state` generation
/// (`crates/polyflare-codex/src/oauth.rs:228,238`) uses `rand::rng().fill_bytes(...)` identically.
/// Reusing it here avoids pulling in a redundant RNG dependency (e.g. a direct `OsRng`/`getrandom`
/// dep) for a workspace that already has one vetted, reviewed source of CSPRNG bytes.
pub fn generate_key() -> GeneratedKey {
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    let raw = format!("sk-pf-{}", URL_SAFE_NO_PAD.encode(bytes));
    let key_hash = sha256_hex(&raw);
    let key_prefix: String = raw.chars().take(KEY_PREFIX_LEN).collect();
    GeneratedKey {
        raw,
        key_hash,
        key_prefix,
    }
}

/// sha256 hex digest of a string. The only hashing primitive this module uses — reuses the `sha2`
/// crate already a dependency of this crate (see `session_key.rs`/`ingress.rs`), no redundant hash
/// dependency added.
pub fn sha256_hex(raw: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    hex::encode(hasher.finalize())
}

/// A random id for a new `api_keys` row. Task 1's `ApiKeyRepo::create` takes the id as a
/// caller-supplied input (mirroring `accounts`' id-is-an-input convention — see
/// `crates/polyflare-store/src/account.rs`), and this workspace has no `uuid` crate. A
/// hex-encoded 128-bit CSPRNG value (same `rand::rng()` source as `generate_key`) is unique and
/// unguessable enough for a primary key that is never parsed, displayed as an external identifier
/// format, or round-tripped through anything that expects RFC 4122 shape.
fn generate_id() -> String {
    let mut bytes = [0u8; 16];
    rand::rng().fill_bytes(&mut bytes);
    format!("key_{}", hex::encode(bytes))
}

/// A newly created + persisted client API key, as returned to the CLI. `raw` must be printed to
/// stdout exactly once by the caller and then dropped — this struct is not retained anywhere.
#[derive(Debug)]
pub struct CreatedKey {
    pub id: String,
    pub raw: String,
    pub key_prefix: String,
}

/// Generates a key and stores its hash + prefix via Task 1's `ApiKeyRepo`, returning the raw key
/// to the caller for a one-time reveal. This is the entire `keys create` logic — `main.rs`'s
/// `keys_create` is a thin wrapper that opens the `Store` and prints [`CreatedKey::raw`].
pub async fn create_key(
    store: &Store,
    label: Option<&str>,
    now: i64,
) -> Result<CreatedKey, StoreError> {
    let generated = generate_key();
    let id = generate_id();
    store
        .api_keys()
        .create(&id, &generated.key_hash, &generated.key_prefix, label, now)
        .await?;
    Ok(CreatedKey {
        id,
        raw: generated.raw,
        key_prefix: generated.key_prefix,
    })
}

/// Formats one `keys list` line. `ApiKeyRow` (Task 1) has no raw-key/hash field at all, so this
/// function is content-safe by construction — there is nothing here it COULD leak even by mistake.
pub fn render_key_row(row: &ApiKeyRow) -> String {
    format!(
        "{}  prefix={}  label={}  enabled={}  created_at={}  last_used_at={}",
        row.id,
        row.key_prefix,
        row.label.as_deref().unwrap_or("-"),
        row.enabled,
        row.created_at,
        row.last_used_at
            .map(|t| t.to_string())
            .unwrap_or_else(|| "-".to_string()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    async fn open_test_store() -> (Store, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        (store, dir)
    }

    #[test]
    fn generate_key_has_the_sk_pf_prefix() {
        let key = generate_key();
        assert!(
            key.raw.starts_with("sk-pf-"),
            "raw key must start with sk-pf-, got {:?}",
            key.raw
        );
    }

    #[test]
    fn generate_key_two_calls_are_distinct() {
        let a = generate_key();
        let b = generate_key();
        assert_ne!(a.raw, b.raw, "CSPRNG-generated keys must not collide");
        assert_ne!(a.key_hash, b.key_hash);
    }

    #[test]
    fn generate_key_hash_matches_sha256_of_raw() {
        let key = generate_key();
        assert_eq!(key.key_hash, sha256_hex(&key.raw));
    }

    #[test]
    fn generate_key_prefix_is_a_strict_shorter_prefix_of_raw() {
        let key = generate_key();
        assert!(
            key.raw.starts_with(&key.key_prefix),
            "key_prefix must be a prefix of raw"
        );
        assert!(
            key.key_prefix.len() < key.raw.len(),
            "key_prefix must be strictly shorter than the full raw key (not the whole key)"
        );
        assert_eq!(key.key_prefix.len(), KEY_PREFIX_LEN);
    }

    #[tokio::test]
    async fn create_key_then_get_by_hash_finds_the_row() {
        let (store, _dir) = open_test_store().await;
        let created = create_key(&store, Some("ci"), 1_000).await.unwrap();

        let hash = sha256_hex(&created.raw);
        let row = store
            .api_keys()
            .get_by_hash(&hash)
            .await
            .unwrap()
            .expect("row must exist under the hash of the revealed raw key");
        assert_eq!(row.id, created.id);
        assert_eq!(row.key_prefix, created.key_prefix);
        assert_eq!(row.label.as_deref(), Some("ci"));
        assert!(row.enabled);
    }

    #[tokio::test]
    async fn two_creates_yield_two_distinct_rows() {
        let (store, _dir) = open_test_store().await;
        let a = create_key(&store, None, 1).await.unwrap();
        let b = create_key(&store, None, 2).await.unwrap();
        assert_ne!(a.id, b.id);
        assert_ne!(a.raw, b.raw);

        let rows = store.api_keys().list().await.unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn list_rendering_contains_prefix_and_label_but_never_the_raw_key() {
        let (store, _dir) = open_test_store().await;
        let created = create_key(&store, Some("laptop"), 42).await.unwrap();

        let rows = store.api_keys().list().await.unwrap();
        let rendered: Vec<String> = rows.iter().map(render_key_row).collect();
        let all = rendered.join("\n");

        assert!(all.contains(&created.key_prefix));
        assert!(all.contains("laptop"));
        assert!(
            !all.contains(&created.raw),
            "the raw key must never appear in list rendering"
        );
        // The raw key is strictly longer than the prefix, so a substring check on the raw value
        // (not just the prefix) is a meaningful, non-tautological assertion.
        assert!(created.raw.len() > created.key_prefix.len());
    }

    #[tokio::test]
    async fn revoke_disables_the_row() {
        let (store, _dir) = open_test_store().await;
        let created = create_key(&store, None, 7).await.unwrap();

        store.api_keys().set_enabled(&created.id, false).await.unwrap();

        let hash = sha256_hex(&created.raw);
        let row = store.api_keys().get_by_hash(&hash).await.unwrap().unwrap();
        assert!(!row.enabled);
    }

    /// Content-safety: capture everything a `tracing` subscriber would see while generating +
    /// storing a key, and assert the raw key (and its hash) never appear in that capture. This
    /// mechanically proves `generate_key`/`create_key` contain no `tracing::`/log call that could
    /// leak the raw key — mirroring the D18 plan's "never log the raw key" constraint. (Combined
    /// with the fact that `keys.rs` contains zero `tracing::`/`eprintln!` calls of any kind — the
    /// only place the raw key is ever printed is `main.rs`'s `keys_create`, to stdout via
    /// `println!`, the intended one-time reveal channel — this closes both the mechanical and the
    /// code-review side of the constraint.)
    #[tokio::test]
    async fn never_logs_the_raw_key() {
        use std::sync::{Arc, Mutex};

        #[derive(Clone, Default)]
        struct BufWriter(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for BufWriter {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BufWriter {
            type Writer = BufWriter;
            fn make_writer(&'a self) -> Self::Writer {
                self.clone()
            }
        }

        let buf = BufWriter::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .with_max_level(tracing::Level::TRACE)
            .finish();

        let (store, _dir) = open_test_store().await;
        // A guard (not `with_default`'s sync closure) so the thread-local default subscriber
        // stays in effect across `create_key`'s `.await` points. `#[tokio::test]` defaults to a
        // current-thread runtime, so the task never migrates to another OS thread mid-await —
        // the thread-local dispatch set here is still current when `create_key` resumes.
        let guard = tracing::subscriber::set_default(subscriber);
        let created = create_key(&store, Some("sentinel"), 99).await.unwrap();
        drop(guard);

        let captured = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        // Not asserting the capture is fully empty: sqlx's own instrumentation may emit benign
        // query-text/timing trace events on this path, and asserting that away is out of this
        // module's scope. The content-safety property this test exists to prove is narrower and
        // load-bearing: whatever WAS captured, the raw key (and its hash) are not in it.
        assert!(
            !captured.contains(&created.raw),
            "the raw key must never reach a tracing sink, got: {captured:?}"
        );
        assert!(
            !captured.contains(&sha256_hex(&created.raw)),
            "the key hash must not appear in tracing output either"
        );
    }
}
