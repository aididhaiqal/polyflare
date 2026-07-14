//! The SQLite-backed store: a pooled connection with embedded, forward-only migrations.

use std::path::Path;
use std::time::Duration;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions};

use crate::account::AccountRepo;
use crate::StoreError;

/// Owns the SQLite connection pool. The pool is reference-counted, so cloning it is cheap.
pub struct Store {
    pool: SqlitePool,
}

impl Store {
    /// Open the database at `path`, creating it (and its parent directory) if missing,
    /// enabling WAL, and running all embedded migrations. Idempotent across restarts.
    pub async fn open(path: &Path) -> Result<Self, StoreError> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let opts = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(opts)
            .await?;
        sqlx::migrate!("./migrations").run(&pool).await?;
        Ok(Self { pool })
    }

    /// The underlying pool, for callers that run raw queries (e.g. the importer, tests).
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// The account repository over this store's pool.
    pub fn accounts(&self) -> AccountRepo {
        AccountRepo::new(self.pool.clone())
    }
}
