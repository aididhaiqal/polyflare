//! The SQLite-backed store: a pooled connection with embedded, forward-only migrations.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions, SqliteSynchronous,
};

use crate::account::AccountRepo;
use crate::api_key_repo::ApiKeyRepo;
use crate::continuity_repo::ContinuityRepo;
use crate::request_log_repo::RequestLogRepo;
use crate::settings_repo::SettingsRepo;
use crate::StoreError;

/// Owns the SQLite connection pool. The pool is reference-counted, so cloning it is cheap.
#[derive(Clone)]
pub struct Store {
    pool: SqlitePool,
    /// In-process account-write generation counter. Every `AccountRepo` write bumps it; a reader
    /// (the server's `AccountCache`) compares it to auto-invalidate its cached account pool on ANY
    /// write, so no caller can forget to invalidate. This is process-local and tracks only writes
    /// made through THIS `Store` instance — correct for PolyFlare's single-process design (a
    /// multi-instance deploy would need a shared/DB counter instead; see `AccountCache` docs).
    account_generation: Arc<AtomicU64>,
    /// In-process TOKEN/identity-write generation counter. Bumped ONLY by writes that change what
    /// the server's `TokenCache` holds — the tokens + stable identity fields (`insert` and
    /// `update_tokens`). A usage-window / status / pool / routing-policy write does NOT bump this,
    /// so the token cache survives the usage-refresh loop's periodic writes (it only reads
    /// provider/chatgpt_account_id/last_refresh/id + tokens, none of which those writes touch),
    /// while the `AccountCache` still invalidates on them via `account_generation`.
    token_generation: Arc<AtomicU64>,
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
            // `synchronous=NORMAL` is the safe+fast pairing for WAL: fsync only on checkpoint, not on
            // every commit, so a per-request write (the continuity session-state UPSERT) no longer
            // pays an `F_FULLFSYNC` on the hot path — this is the main p99-tail reduction. Under WAL
            // it costs at most the LAST committed transaction on an OS crash (never corruption), and
            // all continuity state is reconstructible, so the durability trade is acceptable here.
            .synchronous(SqliteSynchronous::Normal)
            .foreign_keys(true)
            // Read-side tuning: a larger page cache (16 MB; negative ⇒ KiB), memory-mapped reads
            // (256 MB) to skip the read syscall, and in-memory temp tables — all reduce read-path
            // syscalls + tail jitter.
            .pragma("cache_size", "-16384")
            .pragma("mmap_size", "268435456")
            .pragma("temp_store", "MEMORY")
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            // Keep 2 warm connections so a hot-path acquire never pays a cold open, and skip the
            // per-acquire liveness ping (the pool is process-local and short-lived per query).
            .min_connections(2)
            .test_before_acquire(false)
            .connect_with(opts)
            .await?;
        sqlx::migrate!("./migrations").run(&pool).await?;
        Ok(Self {
            pool,
            account_generation: Arc::new(AtomicU64::new(0)),
            token_generation: Arc::new(AtomicU64::new(0)),
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

    /// The current token/identity-write generation (see `token_generation` field). The server's
    /// `TokenCache` reads this to invalidate ONLY on token/identity writes, not on usage/status
    /// churn.
    pub fn token_generation(&self) -> u64 {
        self.token_generation.load(Ordering::Acquire)
    }

    /// The account repository over this store's pool, wired to bump both write generations.
    pub fn accounts(&self) -> AccountRepo {
        AccountRepo::new(
            self.pool.clone(),
            self.account_generation.clone(),
            self.token_generation.clone(),
        )
    }

    /// The continuity repository over this store's pool.
    pub fn continuity(&self) -> ContinuityRepo {
        ContinuityRepo::new(self.pool.clone())
    }

    /// The request-log repository over this store's pool.
    pub fn request_log(&self) -> RequestLogRepo {
        RequestLogRepo::new(self.pool.clone())
    }

    /// The client API-key repository over this store's pool (D18 Task 1). No generation-bump
    /// wiring: see `ApiKeyRepo`'s doc comment for why — the middleware validates via a fresh
    /// indexed `get_by_hash` per request, not a cached snapshot.
    pub fn api_keys(&self) -> ApiKeyRepo {
        ApiKeyRepo::new(self.pool.clone())
    }

    /// The settings repository over this store's pool (Settings-subsystem Task 3). No
    /// generation-bump wiring: nothing in-process caches `settings` rows today, so there is
    /// nothing here for a generation counter to invalidate.
    pub fn settings(&self) -> SettingsRepo {
        SettingsRepo::new(self.pool.clone())
    }
}
