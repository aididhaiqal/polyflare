//! At-rest token crypto: XChaCha20-Poly1305 with a random 24-byte nonce per blob. The key is
//! a raw 32-byte file (chmod 0600 on Unix). Plaintext is never logged.

use std::fs;
use std::io::{Read, Write};
use std::path::Path;

use chacha20poly1305::aead::{Aead, AeadCore, KeyInit, OsRng};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};

use crate::StoreError;

/// Length of the raw key file, in bytes.
const KEY_LEN: usize = 32;
/// Length of the XChaCha20-Poly1305 nonce prepended to each ciphertext, in bytes.
const NONCE_LEN: usize = 24;

/// Encrypts/decrypts short secrets (OAuth tokens) with XChaCha20-Poly1305.
///
/// `Clone`: cheap (the underlying `ChaChaPoly1305` clone is just its 32-byte key), added for D15's
/// `HttpModelSource` which needs its own owned copy of the cipher alongside the account store
/// (mirrors `Store`'s "pool is reference-counted, cloning is cheap" doc).
#[derive(Clone)]
pub struct TokenCipher {
    cipher: XChaCha20Poly1305,
}

impl TokenCipher {
    /// Load a raw 32-byte key from `path`, or generate one and persist it (chmod 0600 on Unix)
    /// if the file does not exist. The parent directory is created if needed.
    pub fn load_or_create(path: &Path) -> Result<Self, StoreError> {
        let key_bytes = loop {
            match read_key_file(path) {
                Ok(bytes) => break bytes,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    if let Some(parent) = path.parent() {
                        if !parent.as_os_str().is_empty() {
                            fs::create_dir_all(parent)?;
                        }
                    }
                    let key = XChaCha20Poly1305::generate_key(&mut OsRng);
                    match write_key_file(path, key.as_slice()) {
                        Ok(()) => break key.to_vec(),
                        // Another process won the create race. Read and validate that key instead
                        // of truncating it or proceeding with a different in-memory key.
                        Err(StoreError::Io(error))
                            if error.kind() == std::io::ErrorKind::AlreadyExists =>
                        {
                            continue;
                        }
                        Err(error) => return Err(error),
                    }
                }
                Err(error) => return Err(error.into()),
            }
        };
        if key_bytes.len() != KEY_LEN {
            return Err(StoreError::Crypto(format!(
                "key file must be {KEY_LEN} raw bytes, found {}",
                key_bytes.len()
            )));
        }
        Self::from_key_bytes(&key_bytes)
    }

    /// Build a cipher from raw key bytes (must be exactly 32).
    pub fn from_key_bytes(key_bytes: &[u8]) -> Result<Self, StoreError> {
        let cipher = XChaCha20Poly1305::new_from_slice(key_bytes)
            .map_err(|_| StoreError::Crypto("key must be 32 bytes".to_string()))?;
        Ok(Self { cipher })
    }

    /// Encrypt `plaintext`, returning `nonce(24) || ciphertext+tag`.
    pub fn encrypt(&self, plaintext: &str) -> Result<Vec<u8>, StoreError> {
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        let ciphertext = self
            .cipher
            .encrypt(&nonce, plaintext.as_bytes())
            .map_err(|_| StoreError::Crypto("encryption failed".to_string()))?;
        let mut out = Vec::with_capacity(NONCE_LEN + ciphertext.len());
        out.extend_from_slice(nonce.as_slice());
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    /// Decrypt a `nonce(24) || ciphertext+tag` blob back to its UTF-8 plaintext.
    pub fn decrypt(&self, blob: &[u8]) -> Result<String, StoreError> {
        if blob.len() < NONCE_LEN {
            return Err(StoreError::Crypto("ciphertext too short".to_string()));
        }
        let (nonce_bytes, ciphertext) = blob.split_at(NONCE_LEN);
        let nonce = XNonce::from_slice(nonce_bytes);
        let plaintext = self
            .cipher
            .decrypt(nonce, ciphertext)
            .map_err(|_| StoreError::Crypto("decryption failed".to_string()))?;
        String::from_utf8(plaintext)
            .map_err(|_| StoreError::Crypto("plaintext is not valid UTF-8".to_string()))
    }
}

#[cfg(unix)]
fn write_key_file(path: &Path, bytes: &[u8]) -> Result<(), StoreError> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn write_key_file(path: &Path, bytes: &[u8]) -> Result<(), StoreError> {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

#[cfg(unix)]
fn read_key_file(path: &Path) -> Result<Vec<u8>, std::io::Error> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    // Keep validation, permission tightening, and reading bound to one descriptor. O_NOFOLLOW
    // rejects a symlink in the final path component, while O_NONBLOCK prevents a malicious FIFO
    // or device path from wedging startup before fstat can reject it.
    let mut file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "key path is not a regular file",
        ));
    }
    if metadata.permissions().mode() & 0o777 != 0o600 {
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    let mut bytes = Vec::with_capacity(KEY_LEN);
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
}

#[cfg(not(unix))]
fn read_key_file(path: &Path) -> Result<Vec<u8>, std::io::Error> {
    let mut file = fs::File::open(path)?;
    if !file.metadata()?.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "key path is not a regular file",
        ));
    }
    let mut bytes = Vec::with_capacity(KEY_LEN);
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cipher() -> TokenCipher {
        TokenCipher::from_key_bytes(&[42u8; KEY_LEN]).unwrap()
    }

    #[test]
    fn round_trip_recovers_plaintext() {
        let c = cipher();
        let blob = c.encrypt("secret-access-token").unwrap();
        assert_eq!(c.decrypt(&blob).unwrap(), "secret-access-token");
    }

    #[test]
    fn ciphertext_does_not_contain_plaintext() {
        let c = cipher();
        let marker = b"plaintext-marker";
        let blob = c.encrypt("plaintext-marker").unwrap();
        assert!(!blob.windows(marker.len()).any(|w| w == marker));
    }

    #[test]
    fn tampered_ciphertext_fails() {
        let c = cipher();
        let mut blob = c.encrypt("secret").unwrap();
        let last = blob.len() - 1;
        blob[last] ^= 0xFF; // flip a bit in the auth tag
        assert!(c.decrypt(&blob).is_err());
    }

    #[test]
    fn nonce_varies_per_call() {
        let c = cipher();
        let a = c.encrypt("same").unwrap();
        let b = c.encrypt("same").unwrap();
        assert_ne!(
            a, b,
            "random nonce ⇒ identical plaintext yields different blobs"
        );
        assert_eq!(c.decrypt(&a).unwrap(), "same");
        assert_eq!(c.decrypt(&b).unwrap(), "same");
    }

    #[test]
    fn load_or_create_persists_reusable_key() {
        let dir = tempfile::tempdir().unwrap();
        let key_path = dir.path().join("key");

        let first = TokenCipher::load_or_create(&key_path).unwrap();
        assert!(key_path.exists());
        let blob = first.encrypt("x").unwrap();

        // A second load reuses the same persisted key.
        let second = TokenCipher::load_or_create(&key_path).unwrap();
        assert_eq!(second.decrypt(&blob).unwrap(), "x");

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&key_path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "key file must be 0600");
        }
    }

    #[cfg(unix)]
    #[test]
    fn load_existing_key_tightens_overly_broad_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let key_path = dir.path().join("key");
        fs::write(&key_path, [7u8; KEY_LEN]).unwrap();
        fs::set_permissions(&key_path, fs::Permissions::from_mode(0o644)).unwrap();

        TokenCipher::load_or_create(&key_path).unwrap();

        let mode = fs::metadata(&key_path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "loaded key file must be 0600");
    }

    #[cfg(unix)]
    #[test]
    fn load_existing_key_rejects_symlinks_without_chmodding_the_target() {
        use std::os::unix::fs::{symlink, PermissionsExt};

        let dir = tempfile::tempdir().unwrap();
        let target_path = dir.path().join("target");
        let key_path = dir.path().join("key");
        fs::write(&target_path, [7u8; KEY_LEN]).unwrap();
        fs::set_permissions(&target_path, fs::Permissions::from_mode(0o644)).unwrap();
        symlink(&target_path, &key_path).unwrap();

        assert!(
            TokenCipher::load_or_create(&key_path).is_err(),
            "the encryption-key path must never follow a symlink"
        );

        let target_mode = fs::metadata(&target_path).unwrap().permissions().mode();
        assert_eq!(
            target_mode & 0o777,
            0o644,
            "rejecting a symlink must not mutate its target"
        );
    }

    #[test]
    fn key_creation_never_overwrites_an_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let key_path = dir.path().join("key");
        let first = [1u8; KEY_LEN];
        let second = [2u8; KEY_LEN];

        write_key_file(&key_path, &first).unwrap();
        assert!(
            write_key_file(&key_path, &second).is_err(),
            "a concurrent creator's key must never be truncated"
        );
        assert_eq!(fs::read(key_path).unwrap(), first);
    }
}
