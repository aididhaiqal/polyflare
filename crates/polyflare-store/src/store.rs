//! The SQLite-backed store: a pooled connection with embedded, forward-only migrations.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions, SqliteSynchronous,
};
use tokio::sync::{mpsc, oneshot};

use crate::account::AccountRepo;
use crate::api_key_repo::ApiKeyRepo;
use crate::continuity_repo::ContinuityRepo;
use crate::onboarding_repo::OnboardingRepo;
use crate::provider_repo::ProviderRepo;
use crate::request_log_repo::{RequestLogRecord, RequestLogRepo, RequestProtocolOutcome};
use crate::settings_repo::SettingsRepo;
use crate::StoreError;

/// Maximum number of non-critical persistence operations waiting behind SQLite. A full queue
/// drops new telemetry/audit updates instead of creating an unbounded number of detached tasks or
/// applying backpressure to the proxied response path.
const BACKGROUND_WRITE_QUEUE_CAPACITY: usize = 1024;

/// The stream-end metrics that are only known after the initial request-log row was queued.
#[derive(Debug, Clone)]
pub struct RequestUsageUpdate {
    pub request_id: String,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cached_input_tokens: Option<i64>,
    pub cache_write_input_tokens: Option<i64>,
    pub reasoning_tokens: Option<i64>,
    pub reported_total_tokens: Option<i64>,
    pub orchestration_input_tokens: Option<i64>,
    pub orchestration_output_tokens: Option<i64>,
    pub orchestration_cached_input_tokens: Option<i64>,
    pub cost_usd: Option<f64>,
    pub latency_first_token_ms: Option<i64>,
    pub duration_ms: Option<i64>,
    pub protocol_outcome: RequestProtocolOutcome,
}

#[derive(Debug)]
enum BackgroundWrite {
    InsertRequestLog(Box<RequestLogRecord>),
    UpdateRequestUsage(RequestUsageUpdate),
    TouchApiKey { id: String, now: i64 },
    Flush(oneshot::Sender<()>),
}

#[derive(Clone)]
struct BackgroundWriter {
    tx: mpsc::Sender<BackgroundWrite>,
}

impl BackgroundWriter {
    fn spawn(pool: SqlitePool) -> Self {
        let (tx, rx) = mpsc::channel(BACKGROUND_WRITE_QUEUE_CAPACITY);
        tokio::spawn(run_background_writer(pool, rx));
        Self { tx }
    }

    fn try_send(&self, write: BackgroundWrite) -> Result<(), StoreError> {
        self.tx.try_send(write).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => StoreError::BackgroundQueueFull,
            mpsc::error::TrySendError::Closed(_) => StoreError::BackgroundQueueClosed,
        })
    }

    async fn flush(&self) -> Result<(), StoreError> {
        let (done_tx, done_rx) = oneshot::channel();
        self.tx
            .send(BackgroundWrite::Flush(done_tx))
            .await
            .map_err(|_| StoreError::BackgroundQueueClosed)?;
        done_rx.await.map_err(|_| StoreError::BackgroundQueueClosed)
    }
}

async fn run_background_writer(pool: SqlitePool, mut rx: mpsc::Receiver<BackgroundWrite>) {
    let request_log = RequestLogRepo::new(pool.clone());
    let api_keys = ApiKeyRepo::new(pool);
    while let Some(write) = rx.recv().await {
        let result = match write {
            BackgroundWrite::InsertRequestLog(record) => request_log.insert(&record).await,
            BackgroundWrite::UpdateRequestUsage(update) => {
                request_log
                    .update_usage(
                        &update.request_id,
                        update.input_tokens,
                        update.output_tokens,
                        update.cached_input_tokens,
                        update.cache_write_input_tokens,
                        update.reasoning_tokens,
                        update.reported_total_tokens,
                        update.orchestration_input_tokens,
                        update.orchestration_output_tokens,
                        update.orchestration_cached_input_tokens,
                        update.cost_usd,
                        update.latency_first_token_ms,
                        update.duration_ms,
                        Some(update.protocol_outcome),
                    )
                    .await
            }
            BackgroundWrite::TouchApiKey { id, now } => api_keys.touch_last_used(&id, now).await,
            BackgroundWrite::Flush(done) => {
                let _ = done.send(());
                continue;
            }
        };
        if let Err(error) = result {
            tracing::warn!(%error, "background persistence failed");
        }
    }
}

/// Owns the SQLite connection pool. The pool is reference-counted, so cloning it is cheap.
#[derive(Clone)]
pub struct Store {
    pool: SqlitePool,
    background_writer: BackgroundWriter,
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
    /// Invalidates the server's custom-provider registry after any provider, credential, or model
    /// mutation. Separate from account/token generations because custom providers are not OAuth
    /// accounts and must not churn those caches.
    provider_generation: Arc<AtomicU64>,
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
        let background_writer = BackgroundWriter::spawn(pool.clone());
        Ok(Self {
            pool,
            background_writer,
            account_generation: Arc::new(AtomicU64::new(0)),
            token_generation: Arc::new(AtomicU64::new(0)),
            provider_generation: Arc::new(AtomicU64::new(0)),
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

    pub fn provider_generation(&self) -> u64 {
        self.provider_generation.load(Ordering::Acquire)
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

    /// Durable state for dashboard OAuth onboarding flows.
    pub fn onboarding(&self) -> OnboardingRepo {
        OnboardingRepo::new(self.pool.clone())
    }

    /// Operator-configured provider, credential, and model persistence.
    pub fn providers(&self) -> ProviderRepo {
        ProviderRepo::new(self.pool.clone(), self.provider_generation.clone())
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

    /// Queue a content-safe request outcome without spawning a task per request.
    pub fn enqueue_request_log(&self, record: RequestLogRecord) -> Result<(), StoreError> {
        self.background_writer
            .try_send(BackgroundWrite::InsertRequestLog(Box::new(record)))
    }

    /// Queue the stream-end usage update after its request-log insert. The single FIFO writer
    /// preserves that ordering, so the UPDATE cannot race ahead of the INSERT for a request.
    pub fn enqueue_request_usage(&self, update: RequestUsageUpdate) -> Result<(), StoreError> {
        self.background_writer
            .try_send(BackgroundWrite::UpdateRequestUsage(update))
    }

    /// Queue the best-effort API-key last-used audit timestamp. Carries only the generated key id,
    /// never the raw presented key or its hash.
    pub fn enqueue_api_key_touch(&self, id: String, now: i64) -> Result<(), StoreError> {
        self.background_writer
            .try_send(BackgroundWrite::TouchApiKey { id, now })
    }

    /// Wait until every background write queued before this call has completed. Used after the
    /// HTTP server finishes graceful shutdown so acknowledged request telemetry is not lost on
    /// process exit.
    pub async fn flush_background_writes(&self) -> Result<(), StoreError> {
        self.background_writer.flush().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_record(request_id: &str) -> RequestLogRecord {
        RequestLogRecord {
            requested_at: 100,
            provider: "codex".into(),
            method: "POST".into(),
            path: "/responses".into(),
            aliased: false,
            status: 200,
            duration_ms: 10,
            account_id: None,
            target_kind: None,
            provider_credential_id: None,
            model: Some("gpt-5.6-sol".into()),
            upstream_model: None,
            upstream_transport: None,
            reasoning_effort: None,
            service_tier: None,
            transport: Some("http".into()),
            ttft_ms: None,
            total_tokens: None,
            cached_tokens: None,
            subagent: None,
            request_id: Some(request_id.into()),
            session_key: None,
            input_tokens: None,
            output_tokens: None,
            cached_input_tokens: None,
            reasoning_tokens: None,
            orchestration_input_tokens: None,
            orchestration_output_tokens: None,
            orchestration_cached_input_tokens: None,
            cost_usd: None,
            latency_first_token_ms: None,
            protocol_outcome: None,
        }
    }

    #[tokio::test]
    async fn flush_drains_insert_then_usage_update_in_fifo_order() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();

        store.enqueue_request_log(sample_record("rq-1")).unwrap();
        store
            .enqueue_request_usage(RequestUsageUpdate {
                request_id: "rq-1".into(),
                input_tokens: Some(10),
                output_tokens: Some(5),
                cached_input_tokens: Some(2),
                cache_write_input_tokens: Some(1),
                reasoning_tokens: Some(1),
                reported_total_tokens: Some(15),
                orchestration_input_tokens: None,
                orchestration_output_tokens: None,
                orchestration_cached_input_tokens: None,
                cost_usd: Some(0.25),
                latency_first_token_ms: Some(7),
                duration_ms: Some(20),
                protocol_outcome: RequestProtocolOutcome::Completed,
            })
            .unwrap();
        store.flush_background_writes().await.unwrap();

        let row = store.request_log().list(1, 0).await.unwrap().remove(0);
        assert_eq!(row.request_id.as_deref(), Some("rq-1"));
        assert_eq!(row.input_tokens, Some(10));
        assert_eq!(row.output_tokens, Some(5));
        assert_eq!(row.cache_write_input_tokens, Some(1));
        assert_eq!(row.reported_total_tokens, Some(15));
        assert_eq!(row.usage_schema.as_deref(), Some("openai_responses_v1"));
        assert_eq!(row.usage_source.as_deref(), Some("upstream_response"));
        assert_eq!(row.usage_status.as_deref(), Some("final"));
        assert_eq!(row.duration_ms, 20);
        assert_eq!(row.protocol_outcome.as_deref(), Some("completed"));
    }

    #[tokio::test]
    async fn flush_drains_api_key_audit_touch() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        store
            .api_keys()
            .create("key-1", "hash", "sk-pf-prefix", None, 1)
            .await
            .unwrap();

        store.enqueue_api_key_touch("key-1".into(), 55).unwrap();
        store.flush_background_writes().await.unwrap();

        let row = store.api_keys().get_by_hash("hash").await.unwrap().unwrap();
        assert_eq!(row.last_used_at, Some(55));
    }
}
