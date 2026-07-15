//! The SQLite-backed store: a pooled connection with embedded, forward-only migrations.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions};

use crate::account::AccountRepo;
use crate::continuity_repo::ContinuityRepo;
use crate::request_log_repo::RequestLogRepo;
use crate::StoreError;

/// Owns the SQLite connection pool. The pool is reference-counted, so cloning it is cheap.
pub struct Store {
    pool: SqlitePool,
    /// In-process account-write generation counter. Every `AccountRepo` write bumps it; a reader
    /// (the server's `AccountCache`) compares it to auto-invalidate its cached account pool on ANY
    /// write, so no caller can forget to invalidate. This is process-local and tracks only writes
    /// made through THIS `Store` instance — correct for PolyFlare's single-process design (a
    /// multi-instance deploy would need a shared/DB counter instead; see `AccountCache` docs).
    account_generation: Arc<AtomicU64>,
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
        Ok(Self {
            pool,
            account_generation: Arc::new(AtomicU64::new(0)),
        })
    }

    /// The underlying pool, for callers that run raw queries (e.g. the importer, tests).
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// The current account-write generation (bumped by every `AccountRepo` write). The
    /// `AccountCache` reads this cheaply (one atomic load) to decide whether its cached pool is
    /// still valid — any write advances it and forces the next read to rebuild.
    pub fn account_generation(&self) -> u64 {
        self.account_generation.load(Ordering::Acquire)
    }

    /// The account repository over this store's pool, wired to bump the write generation.
    pub fn accounts(&self) -> AccountRepo {
        AccountRepo::new(self.pool.clone(), self.account_generation.clone())
    }

    /// The continuity repository over this store's pool.
    pub fn continuity(&self) -> ContinuityRepo {
        ContinuityRepo::new(self.pool.clone())
    }

    /// The request-log repository over this store's pool.
    pub fn request_log(&self) -> RequestLogRepo {
        RequestLogRepo::new(self.pool.clone())
    }
}
