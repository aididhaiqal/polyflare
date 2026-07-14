//! Importer e2e: build a codex-lb-shaped source DB (Fernet-encrypted tokens), import it, and
//! assert the account + usage landed and the tokens decrypt back to plaintext under XChaCha.

use std::path::Path;

use fernet::Fernet;
use polyflare_store::{import_from_codex_lb, Store, StoreError, TokenCipher};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

// codex-lb persists timestamps as SQLite DATETIME text (ISO), NOT epoch integers. The fixture
// models the REAL source schema: TEXT timestamp columns holding these ISO strings, one WITH
// fractional seconds and one WITHOUT (both real formats). The `*_EPOCH` constants are the UTC
// epoch seconds those strings must parse to (computed independently, so the assertions are a
// non-circular check that the importer parses correctly rather than merely that a row landed).
const LAST_REFRESH_ISO: &str = "2026-07-12 06:00:41.345107"; // with fractional seconds
const CREATED_AT_ISO: &str = "2026-07-04 06:00:25"; // no fractional seconds
const RECORDED_AT_ISO: &str = "2026-07-12 06:05:00"; // usage row
const LAST_REFRESH_EPOCH: i64 = 1_783_836_041; // 2026-07-12 06:00:41 UTC (sub-second truncated)
const CREATED_AT_EPOCH: i64 = 1_783_144_825; // 2026-07-04 06:00:25 UTC
const RECORDED_AT_EPOCH: i64 = 1_783_836_300; // 2026-07-12 06:05:00 UTC

/// Create a codex-lb-shaped source DB at `path` with one account (tokens Fernet-encrypted with
/// `fernet_key`) and one usage_history row.
async fn build_source_db(path: &Path, fernet_key: &str) {
    let opts = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(opts)
        .await
        .unwrap();

    sqlx::query(
        "CREATE TABLE accounts (
            id TEXT PRIMARY KEY,
            chatgpt_account_id TEXT,
            chatgpt_user_id TEXT,
            email TEXT NOT NULL,
            alias TEXT,
            workspace_id TEXT,
            workspace_label TEXT,
            seat_type TEXT,
            plan_type TEXT NOT NULL,
            routing_policy TEXT NOT NULL,
            access_token_encrypted BLOB NOT NULL,
            refresh_token_encrypted BLOB NOT NULL,
            id_token_encrypted BLOB NOT NULL,
            last_refresh TEXT NOT NULL,
            created_at TEXT NOT NULL,
            status TEXT NOT NULL,
            deactivation_reason TEXT,
            reset_at INTEGER,
            blocked_at INTEGER,
            security_work_authorized INTEGER NOT NULL
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "CREATE TABLE usage_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT NOT NULL,
            recorded_at TEXT NOT NULL,
            \"window\" TEXT,
            used_percent REAL NOT NULL,
            input_tokens INTEGER,
            output_tokens INTEGER,
            reset_at INTEGER,
            window_minutes INTEGER,
            credits_has INTEGER,
            credits_unlimited INTEGER,
            credits_balance REAL
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    let fernet = Fernet::new(fernet_key).unwrap();
    let access = fernet.encrypt(b"ACCESS-plaintext");
    let refresh = fernet.encrypt(b"REFRESH-plaintext");
    let id = fernet.encrypt(b"IDTOKEN-plaintext");

    sqlx::query(
        "INSERT INTO accounts (
            id, chatgpt_account_id, chatgpt_user_id, email, alias,
            workspace_id, workspace_label, seat_type, plan_type, routing_policy,
            access_token_encrypted, refresh_token_encrypted, id_token_encrypted,
            last_refresh, created_at, status, deactivation_reason,
            reset_at, blocked_at, security_work_authorized
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind("acct-1")
    .bind(Some("ws-acct"))
    .bind(Some("user-1"))
    .bind("user@example.test")
    .bind(Some("primary"))
    .bind(Some("ws-1"))
    .bind(Some("Workspace One"))
    .bind(Some("standard"))
    .bind("pro")
    .bind("normal")
    .bind(access.into_bytes())
    .bind(refresh.into_bytes())
    .bind(id.into_bytes())
    .bind(LAST_REFRESH_ISO) // DATETIME text (with fractional seconds), not an epoch int
    .bind(CREATED_AT_ISO) // DATETIME text (no fractional seconds)
    .bind("active")
    .bind(Option::<String>::None)
    .bind(Option::<i64>::None)
    .bind(Option::<i64>::None)
    .bind(true)
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO usage_history (
            account_id, recorded_at, \"window\", used_percent, input_tokens,
            output_tokens, reset_at, window_minutes, credits_has, credits_unlimited, credits_balance
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind("acct-1")
    .bind(RECORDED_AT_ISO) // DATETIME text, not an epoch int
    .bind(Option::<String>::None) // NULL window — codex-lb leaves this null on some rows
    .bind(42.5_f64)
    .bind(Some(1000_i64))
    .bind(Some(200_i64))
    .bind(Some(1_700_003_600_i64))
    .bind(Some(300_i64))
    .bind(Some(true))
    .bind(Some(false))
    .bind(Some(12.5_f64))
    .execute(&pool)
    .await
    .unwrap();
}

/// Append one more account (id `acct_id`, tokens Fernet-encrypted with `fernet_key`) to an
/// existing codex-lb-shaped source DB. Used to build a mixed-key fixture for the rollback test.
async fn append_account(path: &Path, acct_id: &str, fernet_key: &str) {
    let opts = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(false);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(opts)
        .await
        .unwrap();

    let fernet = Fernet::new(fernet_key).unwrap();
    let access = fernet.encrypt(b"ACCESS-plaintext");
    let refresh = fernet.encrypt(b"REFRESH-plaintext");
    let id = fernet.encrypt(b"IDTOKEN-plaintext");

    sqlx::query(
        "INSERT INTO accounts (
            id, chatgpt_account_id, chatgpt_user_id, email, alias,
            workspace_id, workspace_label, seat_type, plan_type, routing_policy,
            access_token_encrypted, refresh_token_encrypted, id_token_encrypted,
            last_refresh, created_at, status, deactivation_reason,
            reset_at, blocked_at, security_work_authorized
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(acct_id)
    .bind(Option::<String>::None)
    .bind(Option::<String>::None)
    .bind("second@example.test")
    .bind(Option::<String>::None)
    .bind(Option::<String>::None)
    .bind(Option::<String>::None)
    .bind(Option::<String>::None)
    .bind("pro")
    .bind("normal")
    .bind(access.into_bytes())
    .bind(refresh.into_bytes())
    .bind(id.into_bytes())
    .bind(LAST_REFRESH_ISO) // DATETIME text (matches the real source schema)
    .bind(CREATED_AT_ISO)
    .bind("active")
    .bind(Option::<String>::None)
    .bind(Option::<i64>::None)
    .bind(Option::<i64>::None)
    .bind(true)
    .execute(&pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn imports_accounts_usage_and_tokens_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let src_db = dir.path().join("codex-lb-store.db");
    let fernet_key_path = dir.path().join("encryption.key");
    let pf_db = dir.path().join("polyflare-store.db");
    let pf_key = dir.path().join("key");

    // codex-lb Fernet key (a base64url string), written to the key file the importer reads.
    let fernet_key = Fernet::generate_key();
    std::fs::write(&fernet_key_path, &fernet_key).unwrap();
    build_source_db(&src_db, &fernet_key).await;

    let store = Store::open(&pf_db).await.unwrap();
    let cipher = TokenCipher::load_or_create(&pf_key).unwrap();

    let summary = import_from_codex_lb(&store, &src_db, &fernet_key_path, &cipher)
        .await
        .unwrap();
    assert_eq!(summary.accounts_imported, 1);
    assert_eq!(summary.usage_rows_imported, 1);

    // Account metadata landed.
    let account = store.accounts().get("acct-1").await.unwrap().unwrap();
    assert_eq!(account.email, "user@example.test");
    assert_eq!(account.plan_type, "pro");
    assert!(account.security_work_authorized);

    // The ISO DATETIME text columns were parsed to the correct UTC epoch seconds — not merely
    // "a row landed". This is exactly what a real codex-lb store.db would exercise; the old
    // i64 FromRow would have hit a sqlx ColumnDecode error on these TEXT values.
    assert_eq!(
        account.last_refresh, LAST_REFRESH_EPOCH,
        "last_refresh must parse '{LAST_REFRESH_ISO}' (fractional seconds) to epoch"
    );
    assert_eq!(
        account.created_at, CREATED_AT_EPOCH,
        "created_at must parse '{CREATED_AT_ISO}' (no fractional seconds) to epoch"
    );
    // Cross-check the constants against chrono, mirroring the importer's own parse.
    {
        use chrono::NaiveDateTime;
        let via_chrono = NaiveDateTime::parse_from_str(LAST_REFRESH_ISO, "%Y-%m-%d %H:%M:%S%.f")
            .unwrap()
            .and_utc()
            .timestamp();
        assert_eq!(via_chrono, LAST_REFRESH_EPOCH);
    }

    // Tokens re-encrypted under XChaCha decrypt back to the originals.
    let tokens = store
        .accounts()
        .decrypt_tokens("acct-1", &cipher)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(tokens.access_token, "ACCESS-plaintext");
    assert_eq!(tokens.refresh_token, "REFRESH-plaintext");
    assert_eq!(tokens.id_token, "IDTOKEN-plaintext");

    // Usage landed.
    let usage_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM usage_history WHERE account_id = ?")
            .bind("acct-1")
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(usage_count, 1);

    // usage_history.recorded_at parsed from ISO text to the expected epoch, and the NULL source
    // window was preserved as NULL (proving the nullable-window relaxation + Option binding).
    let (recorded_at, window): (i64, Option<String>) =
        sqlx::query_as("SELECT recorded_at, \"window\" FROM usage_history WHERE account_id = ?")
            .bind("acct-1")
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(
        recorded_at, RECORDED_AT_EPOCH,
        "recorded_at must parse to epoch"
    );
    assert_eq!(window, None, "a NULL source window must stay NULL");
}

/// A mid-import Fernet-decrypt failure must fail cleanly (no panic) AND roll the WHOLE import
/// back, leaving the destination store empty. The fixture is built so a real write lands before
/// the failure: `acct-1` (inserted first) is encrypted under the importer's key `key_b` and so
/// decrypts + inserts successfully, then `acct-bad` (inserted second) is encrypted under a
/// different key `key_a` and fails to decrypt. If the writes were not wrapped in one transaction,
/// `acct-1` would survive; asserting the store is empty proves the transaction rolled it back.
#[tokio::test]
async fn mid_import_decrypt_failure_errors_and_rolls_back_leaving_store_empty() {
    let dir = tempfile::tempdir().unwrap();
    let src_db = dir.path().join("codex-lb-store.db");
    let key_path = dir.path().join("import.key");
    let pf_db = dir.path().join("polyflare-store.db");
    let pf_key = dir.path().join("key");

    // `key_b` is the key the importer is given. `acct-1` (first row) is encrypted under it and
    // will insert; `acct-bad` (second row) is encrypted under the different `key_a` and fails.
    let key_a = Fernet::generate_key();
    let key_b = Fernet::generate_key();
    assert_ne!(
        key_a, key_b,
        "keys must differ for this test to be meaningful"
    );
    std::fs::write(&key_path, &key_b).unwrap();
    build_source_db(&src_db, &key_b).await; // inserts acct-1 (decryptable) first
    append_account(&src_db, "acct-bad", &key_a).await; // inserts acct-bad (undecryptable) second

    let store = Store::open(&pf_db).await.unwrap();
    let cipher = TokenCipher::load_or_create(&pf_key).unwrap();

    // (a) It returns Err (no panic) — an import error from the failed decrypt of acct-bad.
    let result = import_from_codex_lb(&store, &src_db, &key_path, &cipher).await;
    assert!(
        matches!(result, Err(StoreError::Import(_))),
        "expected StoreError::Import on the undecryptable account, got {result:?}"
    );

    // (b) The transaction rolled back: even acct-1 (which decrypted fine and was inserted before
    //     the failure) must be gone — the whole import is atomic.
    let accounts = store.accounts().list().await.unwrap();
    assert_eq!(
        accounts.len(),
        0,
        "destination store must be empty after a rolled-back import (acct-1 must not survive)"
    );
}
