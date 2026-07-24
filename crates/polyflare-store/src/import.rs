//! Zero-re-auth importer: read a codex-lb `store.db` read-only, Fernet-decrypt each account's
//! three tokens, re-encrypt them XChaCha20-Poly1305, and copy accounts + usage_history +
//! request_logs (the chat log) into the PolyFlare schema. Token plaintext is never logged.
//!
//! # request_logs (chat-log) migration — content safety
//! The chat-log copy carries ONLY codex-lb's content-safe `request_logs` columns (ids, model,
//! token counts, cost, latency, outcome/error_code, tiers, plan, timestamps). It deliberately never
//! SELECTs the free-form / PII columns codex-lb also stores — `useragent`, `useragent_group`,
//! `client_ip`, `error_message`, `failure_detail` — so they are never even read, let alone
//! persisted, honoring PolyFlare's stricter no-free-form-request-string rule. codex-lb's TEXT
//! outcome `status` lands in PolyFlare's `outcome` column (its own `status` is the INTEGER HTTP
//! code); `latency_ms` maps onto `duration_ms`. Re-import is idempotent: each row carries its
//! codex-lb `id` as `import_source_id`, and the unique index makes the copy `INSERT OR IGNORE`.
//! Requires a reasonably current codex-lb schema (the fixed SELECT lists columns added across
//! codex-lb's migrations); an older source missing one errors clearly rather than silently skipping.

use std::fs;
use std::path::Path;

use fernet::Fernet;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

use crate::account::{Account, EncryptedTokens, PlainTokens};
use crate::crypto::TokenCipher;
use crate::store::Store;
use crate::StoreError;

/// Counts of what the importer moved.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ImportSummary {
    pub accounts_imported: usize,
    pub usage_rows_imported: usize,
    pub request_logs_imported: usize,
}

/// A codex-lb `accounts` row: durable columns (the intersection with PolyFlare's schema) plus
/// the three Fernet-encrypted token columns. Token columns are read as bytes; the Fernet token
/// is ASCII, so `str::from_utf8` recovers it.
#[derive(sqlx::FromRow)]
struct SrcAccount {
    id: String,
    chatgpt_account_id: Option<String>,
    chatgpt_user_id: Option<String>,
    email: String,
    alias: Option<String>,
    workspace_id: Option<String>,
    workspace_label: Option<String>,
    seat_type: Option<String>,
    plan_type: String,
    routing_policy: String,
    access_token_encrypted: Vec<u8>,
    refresh_token_encrypted: Vec<u8>,
    id_token_encrypted: Vec<u8>,
    // codex-lb stores these as DATETIME (ISO TEXT, e.g. "2026-07-12 06:00:41.345107"), NOT epoch
    // integers — read as text and parse to epoch seconds before persisting (see `parse_epoch`).
    last_refresh: String,
    created_at: String,
    status: String,
    deactivation_reason: Option<String>,
    reset_at: Option<i64>,
    blocked_at: Option<i64>,
    security_work_authorized: bool,
}

/// A codex-lb `usage_history` row (copied by value).
#[derive(sqlx::FromRow)]
struct SrcUsage {
    account_id: String,
    // DATETIME (ISO TEXT) in codex-lb — parsed to epoch seconds on import.
    recorded_at: String,
    // codex-lb's `usage_history.window` is nullable.
    window: Option<String>,
    used_percent: f64,
    input_tokens: Option<i64>,
    output_tokens: Option<i64>,
    reset_at: Option<i64>,
    window_minutes: Option<i64>,
    credits_has: Option<bool>,
    credits_unlimited: Option<bool>,
    credits_balance: Option<f64>,
}

/// A codex-lb `request_logs` (chat-log) row — ONLY the content-safe columns are selected (the
/// free-form/PII columns `useragent`/`client_ip`/`error_message`/`failure_detail` are deliberately
/// never read; see the module doc). Most columns are nullable in codex-lb, so all but the always-
/// present `id`/`requested_at` are `Option`. codex-lb's `status` is the TEXT outcome; `latency_ms`
/// maps onto PolyFlare's `duration_ms`.
#[derive(sqlx::FromRow)]
struct SrcRequestLog {
    /// codex-lb's PK — carried only as `import_source_id` for idempotent re-import.
    id: i64,
    account_id: Option<String>,
    request_id: Option<String>,
    model: Option<String>,
    plan_type: Option<String>,
    source: Option<String>,
    request_kind: Option<String>,
    /// codex-lb outcome: 'success' | 'error' — lands in PolyFlare's `outcome` column.
    status: Option<String>,
    error_code: Option<String>,
    input_tokens: Option<i64>,
    output_tokens: Option<i64>,
    cached_input_tokens: Option<i64>,
    reasoning_tokens: Option<i64>,
    cost_usd: Option<f64>,
    reasoning_effort: Option<String>,
    /// Total latency — maps onto PolyFlare's `duration_ms`.
    latency_ms: Option<i64>,
    latency_first_token_ms: Option<i64>,
    service_tier: Option<String>,
    requested_service_tier: Option<String>,
    actual_service_tier: Option<String>,
    transport: Option<String>,
    /// DATETIME text (NOT NULL) -> epoch seconds.
    requested_at: String,
    /// DATETIME text (nullable) -> epoch seconds.
    deleted_at: Option<String>,
}

/// Import accounts + usage + chat log from the codex-lb `store.db` at `src_db_path`, using the
/// Fernet key file at `fernet_key_path` to decrypt the legacy tokens and `cipher` to re-encrypt
/// them.
///
/// When `dry_run` is true, the import does ALL the work — decrypt, re-encrypt, parse, and stage
/// every row in the transaction, so it genuinely validates the whole migration would succeed — then
/// rolls back instead of committing, persisting nothing. The returned [`ImportSummary`] reports the
/// would-write counts either way, so a dry run is an accurate, side-effect-free preview.
pub async fn import_from_codex_lb(
    store: &Store,
    src_db_path: &Path,
    fernet_key_path: &Path,
    cipher: &TokenCipher,
    dry_run: bool,
    refresh_existing: bool,
) -> Result<ImportSummary, StoreError> {
    // Load the Fernet key (a base64url string, e.g. produced by Fernet.generate_key()).
    let key_text = fs::read_to_string(fernet_key_path)?;
    let fernet = Fernet::new(key_text.trim())
        .ok_or_else(|| StoreError::Import("invalid Fernet key file".to_string()))?;

    // Open the source DB strictly read-only.
    let src_opts = SqliteConnectOptions::new()
        .filename(src_db_path)
        .read_only(true)
        .create_if_missing(false);
    let src_pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(src_opts)
        .await?;

    let mut summary = ImportSummary::default();

    // All destination writes go through a single transaction so a mid-import failure (e.g. a
    // Fernet-decrypt error on a later account) rolls the whole import back — no partial data.
    let mut tx = store.pool().begin().await?;

    // --- accounts (parents first, so usage_history foreign keys resolve) ---
    let src_accounts = sqlx::query_as::<_, SrcAccount>(
        "SELECT id, chatgpt_account_id, chatgpt_user_id, email, alias, \
         workspace_id, workspace_label, seat_type, plan_type, routing_policy, \
         access_token_encrypted, refresh_token_encrypted, id_token_encrypted, \
         last_refresh, created_at, status, deactivation_reason, \
         reset_at, blocked_at, security_work_authorized FROM accounts",
    )
    .fetch_all(&src_pool)
    .await?;

    for src in src_accounts {
        let tokens = PlainTokens {
            access_token: fernet_decrypt(&fernet, &src.access_token_encrypted)?,
            refresh_token: fernet_decrypt(&fernet, &src.refresh_token_encrypted)?,
            id_token: fernet_decrypt(&fernet, &src.id_token_encrypted)?,
        };
        let enc = EncryptedTokens::encrypt(&tokens, cipher)?;
        // codex-lb stores these as ISO DATETIME text; parse to epoch seconds for our schema.
        let last_refresh = parse_epoch(&src.last_refresh)?;
        let created_at = parse_epoch(&src.created_at)?;
        let account = Account {
            id: src.id,
            chatgpt_account_id: src.chatgpt_account_id,
            chatgpt_user_id: src.chatgpt_user_id,
            email: src.email,
            alias: src.alias,
            workspace_id: src.workspace_id,
            workspace_label: src.workspace_label,
            seat_type: src.seat_type,
            plan_type: src.plan_type,
            routing_policy: src.routing_policy,
            last_refresh,
            created_at,
            status: src.status,
            deactivation_reason: src.deactivation_reason,
            reset_at: src.reset_at,
            blocked_at: src.blocked_at,
            security_work_authorized: src.security_work_authorized,
            provider: "codex".to_string(),
            pool: None,
        };
        // Default (`OR IGNORE`) makes the account insert idempotent: re-running after a fix skips
        // ids already present instead of erroring on the `id` PRIMARY KEY.
        //
        // `refresh_existing` (opt-in) instead UPSERTS: for an id already present it refreshes ONLY
        // the token columns (+ `last_refresh`) and clears the not-usable states (`status`->active,
        // `deactivation_reason`/`blocked_at`->NULL) — this is the "pull the latest token from
        // codex-lb, zero re-auth" path for an account whose local token went stale (`reauth_required`
        // etc.). It deliberately does NOT touch pool/alias/routing/security fields, which PolyFlare
        // may have set independently of codex-lb.
        let sql = if refresh_existing {
            "INSERT INTO accounts (\
                id, chatgpt_account_id, chatgpt_user_id, email, alias, \
                workspace_id, workspace_label, seat_type, plan_type, routing_policy, \
                access_token_enc, refresh_token_enc, id_token_enc, \
                last_refresh, created_at, status, deactivation_reason, \
                reset_at, blocked_at, security_work_authorized, provider\
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
            ON CONFLICT(id) DO UPDATE SET \
                access_token_enc = excluded.access_token_enc, \
                refresh_token_enc = excluded.refresh_token_enc, \
                id_token_enc = excluded.id_token_enc, \
                last_refresh = excluded.last_refresh, \
                status = 'active', deactivation_reason = NULL, blocked_at = NULL"
        } else {
            "INSERT OR IGNORE INTO accounts (\
                id, chatgpt_account_id, chatgpt_user_id, email, alias, \
                workspace_id, workspace_label, seat_type, plan_type, routing_policy, \
                access_token_enc, refresh_token_enc, id_token_enc, \
                last_refresh, created_at, status, deactivation_reason, \
                reset_at, blocked_at, security_work_authorized, provider\
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        };
        let result = sqlx::query(sql)
            .bind(account.id.as_str())
            .bind(account.chatgpt_account_id.as_deref())
            .bind(account.chatgpt_user_id.as_deref())
            .bind(account.email.as_str())
            .bind(account.alias.as_deref())
            .bind(account.workspace_id.as_deref())
            .bind(account.workspace_label.as_deref())
            .bind(account.seat_type.as_deref())
            .bind(account.plan_type.as_str())
            .bind(account.routing_policy.as_str())
            .bind(enc.access_token_enc.as_slice())
            .bind(enc.refresh_token_enc.as_slice())
            .bind(enc.id_token_enc.as_slice())
            .bind(account.last_refresh)
            .bind(account.created_at)
            .bind(account.status.as_str())
            .bind(account.deactivation_reason.as_deref())
            .bind(account.reset_at)
            .bind(account.blocked_at)
            .bind(account.security_work_authorized)
            .bind(account.provider.as_str())
            .execute(&mut *tx)
            .await?;
        // `rows_affected()` is 0 when `OR IGNORE` skips an already-present id, 1 when a row is
        // inserted. With `refresh_existing`, an `ON CONFLICT DO UPDATE` also reports 1, so the count
        // covers both newly-inserted and token-refreshed accounts.
        summary.accounts_imported += result.rows_affected() as usize;
    }

    // --- usage_history (copied by value) ---
    // Idempotent via the `idx_usage_history_dedupe` UNIQUE index on
    // (account_id, "window", recorded_at) (migration 0010) + `INSERT OR IGNORE`: re-importing into
    // a populated store skips any snapshot already present (from a prior import OR the live poller)
    // and inserts only genuinely-new rows, so the importer can be re-run to pull a delta. The
    // per-row `rows_affected()` (0 on an ignored collision, 1 on a real insert) makes the reported
    // count reflect true inserts, mirroring the request_log loop below.
    let src_usage = sqlx::query_as::<_, SrcUsage>(
        "SELECT account_id, recorded_at, \"window\", used_percent, input_tokens, \
         output_tokens, reset_at, window_minutes, credits_has, credits_unlimited, \
         credits_balance FROM usage_history",
    )
    .fetch_all(&src_pool)
    .await?;

    for row in src_usage {
        // recorded_at is ISO DATETIME text in codex-lb; parse to epoch seconds. `window` is
        // nullable and preserved as-is (NULL stays NULL).
        let recorded_at = parse_epoch(&row.recorded_at)?;
        let result = sqlx::query(
            "INSERT OR IGNORE INTO usage_history (\
                account_id, recorded_at, \"window\", used_percent, input_tokens, \
                output_tokens, reset_at, window_minutes, credits_has, credits_unlimited, \
                credits_balance\
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(row.account_id.as_str())
        .bind(recorded_at)
        .bind(row.window.as_deref())
        .bind(row.used_percent)
        .bind(row.input_tokens)
        .bind(row.output_tokens)
        .bind(row.reset_at)
        .bind(row.window_minutes)
        .bind(row.credits_has)
        .bind(row.credits_unlimited)
        .bind(row.credits_balance)
        .execute(&mut *tx)
        .await?;
        // `rows_affected()` is 0 when `OR IGNORE` skips an already-present snapshot, 1 on a real
        // insert — so the count reflects true inserts (delta), not the full source size.
        summary.usage_rows_imported += result.rows_affected() as usize;
    }

    // --- request_logs (the chat log) ---
    // Content-safe subset only (see module doc): the free-form/PII columns are never SELECTed.
    // Idempotent via `import_source_id` (codex-lb's row id) + the unique index → `INSERT OR IGNORE`.
    let src_logs = sqlx::query_as::<_, SrcRequestLog>(
        "SELECT id, account_id, request_id, model, plan_type, source, \
         request_kind, status, error_code, input_tokens, output_tokens, cached_input_tokens, \
         reasoning_tokens, cost_usd, reasoning_effort, latency_ms, latency_first_token_ms, \
         service_tier, requested_service_tier, actual_service_tier, transport, \
         requested_at, deleted_at FROM request_logs",
    )
    .fetch_all(&src_pool)
    .await?;

    for src in src_logs {
        let requested_at = parse_epoch(&src.requested_at)?;
        let deleted_at = match &src.deleted_at {
            Some(s) => Some(parse_epoch(s)?),
            None => None,
        };
        // PolyFlare-native NOT NULL columns codex-lb's request_logs has no analog for: all codex-lb
        // request_logs are Codex `/responses` POSTs, so provider/method/path are fixed; `aliased`
        // is unknown historically (false); `status` (HTTP int) was never recorded by codex-lb (0 =
        // "no HTTP status", the outcome/error_code carry the result); `duration_ms` <- `latency_ms`.
        let duration_ms = src.latency_ms.unwrap_or(0);
        let result = sqlx::query(
            "INSERT OR IGNORE INTO request_log (\
                requested_at, provider, method, path, aliased, status, duration_ms, \
                account_id, request_id, model, plan_type, source, request_kind, \
                outcome, error_code, input_tokens, output_tokens, cached_input_tokens, \
                reasoning_tokens, cost_usd, reasoning_effort, latency_first_token_ms, \
                service_tier, requested_service_tier, actual_service_tier, transport, \
                deleted_at, import_source_id\
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \
                      ?, ?, ?)",
        )
        .bind(requested_at)
        .bind("codex")
        .bind("POST")
        .bind("/responses")
        .bind(false)
        .bind(0_i64)
        .bind(duration_ms)
        .bind(src.account_id.as_deref())
        .bind(src.request_id.as_deref())
        .bind(src.model.as_deref())
        .bind(src.plan_type.as_deref())
        .bind(src.source.as_deref())
        .bind(src.request_kind.as_deref())
        .bind(src.status.as_deref())
        .bind(src.error_code.as_deref())
        .bind(src.input_tokens)
        .bind(src.output_tokens)
        .bind(src.cached_input_tokens)
        .bind(src.reasoning_tokens)
        .bind(src.cost_usd)
        .bind(src.reasoning_effort.as_deref())
        .bind(src.latency_first_token_ms)
        .bind(src.service_tier.as_deref())
        .bind(src.requested_service_tier.as_deref())
        .bind(src.actual_service_tier.as_deref())
        .bind(src.transport.as_deref())
        .bind(deleted_at)
        .bind(src.id)
        .execute(&mut *tx)
        .await?;
        // `OR IGNORE` yields 0 rows_affected when a source id is already present (idempotent
        // re-import), 1 when a new row lands — so the count reflects true inserts.
        summary.request_logs_imported += result.rows_affected() as usize;
    }

    // A dry run rolls the whole staged transaction back (persisting nothing) but still reports the
    // would-write counts; a real run commits. Either way any earlier `?` already dropped `tx` for a
    // full rollback on failure.
    if dry_run {
        tx.rollback().await?;
    } else {
        tx.commit().await?;
    }

    Ok(summary)
}

/// Fernet-decrypt one token blob (the bytes of an ASCII Fernet token) to its plaintext string.
fn fernet_decrypt(fernet: &Fernet, token_bytes: &[u8]) -> Result<String, StoreError> {
    let token = std::str::from_utf8(token_bytes)
        .map_err(|_| StoreError::Import("Fernet token is not valid UTF-8".to_string()))?;
    let plaintext = fernet
        .decrypt(token)
        .map_err(|_| StoreError::Import("Fernet decryption failed".to_string()))?;
    String::from_utf8(plaintext)
        .map_err(|_| StoreError::Import("decrypted token is not valid UTF-8".to_string()))
}

/// Parse a codex-lb DATETIME string (ISO `YYYY-MM-DD HH:MM:SS`, with or without fractional
/// seconds) to unix epoch seconds, interpreting the value as UTC. codex-lb persists timestamps
/// as SQLite DATETIME text (e.g. `"2026-07-12 06:00:41.345107"`), so PolyFlare parses them to
/// its own INTEGER-epoch columns on import.
fn parse_epoch(s: &str) -> Result<i64, StoreError> {
    use chrono::NaiveDateTime;
    let dt = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
        .map_err(|e| StoreError::Import(format!("unparseable datetime: {e}")))?;
    Ok(dt.and_utc().timestamp())
}
