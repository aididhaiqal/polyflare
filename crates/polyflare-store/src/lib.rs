//! PolyFlare persistence: a SQLite store, at-rest token crypto (XChaCha20-Poly1305, never
//! Fernet), the account repository, and the zero-re-auth codex-lb importer. Token plaintext
//! is never logged.

pub mod account;
pub mod continuity_repo;
pub mod crypto;
pub mod import;
pub mod store;

pub use account::{Account, AccountRepo, EncryptedTokens, PlainTokens, UsageSnapshot, WindowUsage};
pub use continuity_repo::{ContinuityRepo, SessionRow};
pub use crypto::TokenCipher;
pub use import::{import_from_codex_lb, ImportSummary};
pub use store::Store;

/// Errors surfaced by the store, crypto, and importer.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("migration error: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("crypto error: {0}")]
    Crypto(String),
    #[error("import error: {0}")]
    Import(String),
}
