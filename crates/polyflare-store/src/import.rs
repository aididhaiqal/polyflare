//! Zero-re-auth importer: read a codex-lb `store.db` read-only, Fernet-decrypt each account's
//! three tokens, re-encrypt them XChaCha20-Poly1305, and copy accounts + usage_history into the
//! PolyFlare schema by column-intersection. Token plaintext is never logged.

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
    last_refresh: i64,
    created_at: i64,
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
    recorded_at: i64,
    window: String,
    used_percent: f64,
    input_tokens: Option<i64>,
    output_tokens: Option<i64>,
    reset_at: Option<i64>,
    window_minutes: Option<i64>,
    credits_has: Option<bool>,
    credits_unlimited: Option<bool>,
    credits_balance: Option<f64>,
}

/// Import accounts + usage from the codex-lb `store.db` at `src_db_path`, using the Fernet key
/// file at `fernet_key_path` to decrypt the legacy tokens and `cipher` to re-encrypt them.
pub async fn import_from_codex_lb(
    store: &Store,
    src_db_path: &Path,
    fernet_key_path: &Path,
    cipher: &TokenCipher,
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

    let repo = store.accounts();
    for src in src_accounts {
        let tokens = PlainTokens {
            access_token: fernet_decrypt(&fernet, &src.access_token_encrypted)?,
            refresh_token: fernet_decrypt(&fernet, &src.refresh_token_encrypted)?,
            id_token: fernet_decrypt(&fernet, &src.id_token_encrypted)?,
        };
        let enc = EncryptedTokens::encrypt(&tokens, cipher)?;
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
            last_refresh: src.last_refresh,
            created_at: src.created_at,
            status: src.status,
            deactivation_reason: src.deactivation_reason,
            reset_at: src.reset_at,
            blocked_at: src.blocked_at,
            security_work_authorized: src.security_work_authorized,
        };
        repo.insert_encrypted(&account, &enc).await?;
        summary.accounts_imported += 1;
    }

    // --- usage_history (copied by value) ---
    let src_usage = sqlx::query_as::<_, SrcUsage>(
        "SELECT account_id, recorded_at, \"window\", used_percent, input_tokens, \
         output_tokens, reset_at, window_minutes, credits_has, credits_unlimited, \
         credits_balance FROM usage_history",
    )
    .fetch_all(&src_pool)
    .await?;

    for row in src_usage {
        sqlx::query(
            "INSERT INTO usage_history (\
                account_id, recorded_at, \"window\", used_percent, input_tokens, \
                output_tokens, reset_at, window_minutes, credits_has, credits_unlimited, \
                credits_balance\
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(row.account_id.as_str())
        .bind(row.recorded_at)
        .bind(row.window.as_str())
        .bind(row.used_percent)
        .bind(row.input_tokens)
        .bind(row.output_tokens)
        .bind(row.reset_at)
        .bind(row.window_minutes)
        .bind(row.credits_has)
        .bind(row.credits_unlimited)
        .bind(row.credits_balance)
        .execute(store.pool())
        .await?;
        summary.usage_rows_imported += 1;
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
