//! PolyFlare persistence: a SQLite store, at-rest token crypto (XChaCha20-Poly1305, never
//! Fernet), the account repository, and the zero-re-auth codex-lb importer. Token plaintext
//! is never logged.

pub mod account;
pub mod api_key_repo;
pub mod continuity_repo;
pub mod crypto;
pub mod import;
pub mod onboarding_repo;
pub mod provider_repo;
pub mod request_log_repo;
pub mod settings_repo;
pub mod store;

pub use account::{
    Account, AccountRepo, AccountSettingsUpdate, EncryptedTokens, PlainTokens, UsageSnapshot,
    WindowUsage,
};
pub use api_key_repo::{ApiKeyRepo, ApiKeyRow};
pub use continuity_repo::{ContinuityRepo, SessionRow};
pub use crypto::TokenCipher;
pub use import::{import_from_codex_lb, ImportSummary};
pub use onboarding_repo::{OnboardingFlow, OnboardingRepo};
pub use provider_repo::{
    CustomProvider, NewCustomProvider, NewProviderModel, ProviderCredential,
    ProviderCredentialSecret, ProviderModel, ProviderModelPatch, ProviderRepo,
};
pub use request_log_repo::{
    RecentErrorRow, ReportBreakdownRow, ReportBucket, ReportMetrics, RequestAggregate,
    RequestBucket, RequestLogRecord, RequestLogRepo, RequestLogRow, RequestProtocolOutcome,
    RequestsFilter,
};
pub use settings_repo::SettingsRepo;
pub use store::{RequestUsageUpdate, Store};

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
    #[error("invalid persisted state: {0}")]
    InvalidState(String),
    #[error("background persistence queue is full")]
    BackgroundQueueFull,
    #[error("background persistence queue is closed")]
    BackgroundQueueClosed,
}
