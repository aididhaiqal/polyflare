# PolyFlare M2a — Store Foundation + Accounts + Crypto + OAuth Import — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the M1 `polyflare-store` stub into a working SQLite persistence layer — pooled connection with embedded migrations, XChaCha20-Poly1305 token crypto, an account repository, and a zero-re-auth codex-lb OAuth importer — and expose `polyflare serve` / `polyflare accounts import` via a clap CLI.

**Architecture:** `polyflare-store` wraps a `sqlx::SqlitePool` (create-if-missing, WAL) with forward-only embedded migrations (`sqlx::migrate!`). OAuth tokens are encrypted at rest with XChaCha20-Poly1305 (24-byte random nonce prepended to ciphertext) under a raw 32-byte key file; the account repository stores only ciphertext and decrypts on demand. The importer reads a codex-lb `store.db` read-only, Fernet-decrypts the three legacy token blobs once, re-encrypts them XChaCha, and copies `accounts` + `usage_history` by column-intersection. The `polyflare-server` binary gains a clap subcommand layer; the M1 serve path is unchanged.

**Tech Stack:** Rust 2021, tokio, `sqlx` 0.8 (sqlite, runtime-tokio, migrate, macros), `chacha20poly1305` 0.10 (XChaCha20Poly1305 + aead), `fernet` 0.2 (import-only, decrypt legacy), `clap` 4 (derive), `thiserror`, `tempfile` (dev).

**Design references:** `DESIGN-DECISIONS.md` §M2-SCOPE (M2a line), §SE5 + §SE5(a) (SQLite via sqlx; XChaCha20-Poly1305 at-rest, own key file, not Fernet), §TA6 (`security_work_authorized` flag). `POLYFLARE-DESIGN.md` §4.5 (server edge/store), §7 (migration). `reference/codex-lb-port-reference.md` (accounts + usage_history schema, crypto/key-file, importer algorithm — the source of truth for column names).

## Global Constraints

- **Language / runtime:** Rust edition 2021, stable toolchain, `tokio` async runtime. Workspace crates + `.workspace = true` manifest style are established in M1 — follow them.
- **At-rest crypto is XChaCha20-Poly1305, never Fernet.** Fernet appears in exactly one place: the importer, which decrypts legacy codex-lb blobs *once* and immediately re-encrypts XChaCha. No Fernet format is persisted by PolyFlare.
- **Token plaintext is never logged or printed.** Structs carrying plaintext tokens (`PlainTokens`) implement a redacting `Debug`. The importer never prints a token value. `println!` in the CLI prints only counts.
- **Secrets come from env / key-file only.** The at-rest key is a raw 32-byte file (chmod 0600 on Unix); the Fernet import key is a file path passed on the CLI. No secret is a compile-time constant or a log line.
- **The streaming / serve path is unchanged from M1.** `Config::from_env`, `AppState`, `build_app`, `responses_handler`, `CodexExecutor` keep their M1 signatures and behavior. `polyflare serve` runs exactly the M1 server. Existing server integration tests must still pass untouched.
- **sqlx is used in runtime-checked mode, NOT compile-time-macro mode.** Use `sqlx::query(...)`, `sqlx::query_as::<_, T>(...)`, `sqlx::query_scalar(...)` with `#[derive(sqlx::FromRow)]`. Do **not** use the `query!` / `query_as!` compile-time macros. Consequence: **no `DATABASE_URL` and no `.sqlx` offline cache are needed at build time** — CI stays a plain `cargo build`/`cargo test`. The `macros` feature is still enabled (it provides the `FromRow` derive) but no macro that needs a live DB is invoked.
- **Migrations are forward-only.** Plain `<version>_<desc>.sql` files (no `.down.sql`). Embedded via `sqlx::migrate!("./migrations")`. Running migrations is idempotent across restarts.
- **Timestamps are `INTEGER` unix-epoch seconds** (`i64` in Rust). The three token columns are `BLOB` holding `nonce(24) || ciphertext+tag`.
- **CI is strict.** `.github/workflows/ci.yml` runs `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo build --workspace --all-targets`, `cargo test --workspace`. Every task ends fmt-clean, clippy-clean (`-D warnings`), and green.

---

## File structure

```
polyflare/
├── Cargo.toml                                       # MODIFY: +sqlx +chacha20poly1305 +fernet +clap +tempfile workspace deps
├── crates/
│   ├── polyflare-store/
│   │   ├── Cargo.toml                               # MODIFY (each of Tasks 1,2,4): deps + dev-deps
│   │   ├── migrations/
│   │   │   └── 0001_accounts_and_usage.sql          # CREATE (Task 1): accounts + usage_history schema
│   │   ├── src/
│   │   │   ├── lib.rs                               # MODIFY (Tasks 1–4): modules, re-exports, StoreError
│   │   │   ├── store.rs                             # CREATE (Task 1); MODIFY (Task 3): Store::open/pool/accounts
│   │   │   ├── crypto.rs                            # CREATE (Task 2): TokenCipher + inline unit tests
│   │   │   ├── account.rs                           # CREATE (Task 3): Account, PlainTokens, EncryptedTokens, AccountRepo
│   │   │   └── import.rs                            # CREATE (Task 4): import_from_codex_lb + ImportSummary
│   │   └── tests/
│   │       ├── store_roundtrip.rs                  # CREATE (Task 1)
│   │       ├── account_repo.rs                     # CREATE (Task 3)
│   │       └── import_codex_lb.rs                  # CREATE (Task 4)
│   └── polyflare-server/
│       ├── Cargo.toml                               # MODIFY (Task 5): +clap +polyflare-store
│       └── src/
│           ├── config.rs                           # MODIFY (Task 5): data_dir/db_path/key_path helpers (from_env unchanged)
│           └── main.rs                             # MODIFY (Task 5): clap subcommands (serve | accounts import) + parse tests
```

All commands below assume the repo root:
`POLYFLARE=/Users/wmaididhaiqal/Development/Codex-LoadBalancer/polyflare`

---

## Task 1: Workspace deps + `Store` foundation (pool + embedded migrations + schema)

**Files:**
- Modify: `Cargo.toml` (workspace `[workspace.dependencies]`)
- Modify: `crates/polyflare-store/Cargo.toml`
- Create: `crates/polyflare-store/migrations/0001_accounts_and_usage.sql`
- Modify: `crates/polyflare-store/src/lib.rs`
- Create: `crates/polyflare-store/src/store.rs`
- Create: `crates/polyflare-store/tests/store_roundtrip.rs`

**Interfaces:**
- Consumes: nothing (foundational).
- Produces:
  - `struct Store` with `async fn open(path: &Path) -> Result<Store, StoreError>` and `fn pool(&self) -> &sqlx::SqlitePool`.
  - `enum StoreError` (`Db`, `Migrate`, `Io`, `Crypto(String)`, `Import(String)`), `impl std::error::Error`.
  - Schema: tables `accounts` (columns per port reference) and `usage_history` + index.

- [ ] **Step 1: Add the workspace dependencies**

Add to the `[workspace.dependencies]` table in `$POLYFLARE/Cargo.toml` (below the existing `thiserror = "2"` line):
```toml
sqlx = { version = "0.8", default-features = false, features = ["runtime-tokio", "sqlite", "migrate", "macros"] }
chacha20poly1305 = "0.10"
fernet = "0.2"
clap = { version = "4", features = ["derive"] }
tempfile = "3"
```
`default-features = false` on sqlx drops the unused `any`/`json`/TLS defaults; `sqlite` bundles libsqlite3 (no system SQLite needed on CI); `runtime-tokio` needs no TLS for a local file DB.

- [ ] **Step 2: Set the store crate's dependencies**

Replace `$POLYFLARE/crates/polyflare-store/Cargo.toml` with:
```toml
[package]
name = "polyflare-store"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
sqlx = { workspace = true }
thiserror = { workspace = true }

[dev-dependencies]
tokio = { workspace = true }
tempfile = { workspace = true }
```

- [ ] **Step 3: Create the first migration (the schema)**

`$POLYFLARE/crates/polyflare-store/migrations/0001_accounts_and_usage.sql`:
```sql
-- PolyFlare initial schema: accounts + usage_history. Forward-only.
-- Timestamps are INTEGER unix-epoch seconds. The three token columns are
-- XChaCha20-Poly1305 ciphertext (a 24-byte nonce prepended) stored as BLOB.
-- "window" is quoted because WINDOW is a SQLite keyword.

CREATE TABLE IF NOT EXISTS accounts (
    id                        TEXT    PRIMARY KEY,
    chatgpt_account_id        TEXT,
    chatgpt_user_id           TEXT,
    email                     TEXT    NOT NULL,
    alias                     TEXT,
    workspace_id              TEXT,
    workspace_label           TEXT,
    seat_type                 TEXT,
    plan_type                 TEXT    NOT NULL DEFAULT 'plus',
    routing_policy            TEXT    NOT NULL DEFAULT 'normal',
    access_token_enc          BLOB    NOT NULL,
    refresh_token_enc         BLOB    NOT NULL,
    id_token_enc              BLOB    NOT NULL,
    last_refresh              INTEGER NOT NULL DEFAULT 0,
    created_at                INTEGER NOT NULL,
    status                    TEXT    NOT NULL DEFAULT 'active',
    deactivation_reason       TEXT,
    reset_at                  INTEGER,
    blocked_at                INTEGER,
    security_work_authorized  INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS usage_history (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id        TEXT    NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    recorded_at       INTEGER NOT NULL,
    "window"          TEXT    NOT NULL,
    used_percent      REAL    NOT NULL,
    input_tokens      INTEGER,
    output_tokens     INTEGER,
    reset_at          INTEGER,
    window_minutes    INTEGER,
    credits_has       INTEGER,
    credits_unlimited INTEGER,
    credits_balance   REAL
);

CREATE INDEX IF NOT EXISTS idx_usage_history_account_recorded
    ON usage_history (account_id, recorded_at);
```

- [ ] **Step 4: Write the failing round-trip test**

`$POLYFLARE/crates/polyflare-store/tests/store_roundtrip.rs`:
```rust
//! Round-trip: open a temp-file DB, run migrations, assert the schema exists.

use polyflare_store::Store;

#[tokio::test]
async fn open_creates_schema() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("store.db");

    let store = Store::open(&db_path).await.unwrap();

    let names: Vec<String> =
        sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
            .fetch_all(store.pool())
            .await
            .unwrap();

    assert!(names.iter().any(|n| n == "accounts"), "tables: {names:?}");
    assert!(names.iter().any(|n| n == "usage_history"), "tables: {names:?}");
    assert!(db_path.exists(), "the DB file must be created on disk");
}

#[tokio::test]
async fn open_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("store.db");
    // Opening twice must not error: migrations already applied are skipped.
    let _first = Store::open(&db_path).await.unwrap();
    let _second = Store::open(&db_path).await.unwrap();
}
```

- [ ] **Step 5: Run the test to verify it fails**

Run: `cd $POLYFLARE && cargo test -p polyflare-store --test store_roundtrip`
Expected: FAIL — compile error (`unresolved import polyflare_store::Store`).

- [ ] **Step 6: Implement `StoreError` + module wiring in `lib.rs`**

Replace `$POLYFLARE/crates/polyflare-store/src/lib.rs` with:
```rust
//! PolyFlare persistence: a SQLite store, at-rest token crypto (XChaCha20-Poly1305, never
//! Fernet), the account repository, and the zero-re-auth codex-lb importer. Token plaintext
//! is never logged.

pub mod store;

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
```
(`Crypto` and `Import` are constructed in Tasks 2 and 4. A `pub enum`'s variants are public API, so unused-now variants raise no dead-code warning.)

- [ ] **Step 7: Implement `Store`**

`$POLYFLARE/crates/polyflare-store/src/store.rs`:
```rust
//! The SQLite-backed store: a pooled connection with embedded, forward-only migrations.

use std::path::Path;
use std::time::Duration;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions};

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
}
```
`sqlx::migrate!("./migrations")` resolves relative to `CARGO_MANIFEST_DIR` (`crates/polyflare-store/`), so the migration file created in Step 3 is embedded at compile time.

- [ ] **Step 8: Run the test to verify it passes**

Run: `cd $POLYFLARE && cargo test -p polyflare-store --test store_roundtrip`
Expected: PASS (2 tests).

- [ ] **Step 9: Format + lint**

Run: `cd $POLYFLARE && cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings`
Expected: no diff from fmt, no clippy warnings.

- [ ] **Step 10: Commit**

```bash
cd $POLYFLARE
git add Cargo.toml crates/polyflare-store
git commit -m "feat(store): SqlitePool + embedded migrations + accounts/usage schema"
```

---

## Task 2: Token crypto (`TokenCipher`, XChaCha20-Poly1305)

**Files:**
- Modify: `crates/polyflare-store/Cargo.toml` (add `chacha20poly1305`)
- Create: `crates/polyflare-store/src/crypto.rs`
- Modify: `crates/polyflare-store/src/lib.rs`

**Interfaces:**
- Consumes: `crate::StoreError`.
- Produces:
  - `struct TokenCipher` with:
    - `fn load_or_create(path: &Path) -> Result<TokenCipher, StoreError>` — load a raw 32-byte key, or generate + persist one (chmod 0600 on Unix).
    - `fn from_key_bytes(key_bytes: &[u8]) -> Result<TokenCipher, StoreError>`.
    - `fn encrypt(&self, plaintext: &str) -> Result<Vec<u8>, StoreError>` — returns `nonce(24) || ciphertext+tag`.
    - `fn decrypt(&self, blob: &[u8]) -> Result<String, StoreError>`.

- [ ] **Step 1: Add the crypto dependency**

Add to `[dependencies]` in `$POLYFLARE/crates/polyflare-store/Cargo.toml` (after the `sqlx` line):
```toml
chacha20poly1305 = { workspace = true }
```
Default features suffice: they enable `alloc` (allocating `Aead::encrypt`/`decrypt`), `getrandom` (`OsRng`), and `rand_core` (`generate_key` / `generate_nonce`).

- [ ] **Step 2: Write the failing unit tests**

Create `$POLYFLARE/crates/polyflare-store/src/crypto.rs` with ONLY this test module for now (the implementation lands in Step 4, above this block):
```rust
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
        assert_ne!(a, b, "random nonce ⇒ identical plaintext yields different blobs");
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
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cd $POLYFLARE && cargo test -p polyflare-store crypto`
Expected: FAIL — compile errors (`TokenCipher`, `KEY_LEN` not found).

- [ ] **Step 4: Implement `TokenCipher`**

Prepend to `$POLYFLARE/crates/polyflare-store/src/crypto.rs` (above the `#[cfg(test)]` block):
```rust
//! At-rest token crypto: XChaCha20-Poly1305 with a random 24-byte nonce per blob. The key is
//! a raw 32-byte file (chmod 0600 on Unix). Plaintext is never logged.

use std::fs;
use std::io::Write;
use std::path::Path;

use chacha20poly1305::aead::{Aead, AeadCore, KeyInit, OsRng};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};

use crate::StoreError;

/// Length of the raw key file, in bytes.
const KEY_LEN: usize = 32;
/// Length of the XChaCha20-Poly1305 nonce prepended to each ciphertext, in bytes.
const NONCE_LEN: usize = 24;

/// Encrypts/decrypts short secrets (OAuth tokens) with XChaCha20-Poly1305.
pub struct TokenCipher {
    cipher: XChaCha20Poly1305,
}

impl TokenCipher {
    /// Load a raw 32-byte key from `path`, or generate one and persist it (chmod 0600 on Unix)
    /// if the file does not exist. The parent directory is created if needed.
    pub fn load_or_create(path: &Path) -> Result<Self, StoreError> {
        let key_bytes = if path.exists() {
            let bytes = fs::read(path)?;
            if bytes.len() != KEY_LEN {
                return Err(StoreError::Crypto(format!(
                    "key file must be {KEY_LEN} raw bytes, found {}",
                    bytes.len()
                )));
            }
            bytes
        } else {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    fs::create_dir_all(parent)?;
                }
            }
            let key = XChaCha20Poly1305::generate_key(&mut OsRng);
            write_key_file(path, key.as_slice())?;
            key.to_vec()
        };
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
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    Ok(())
}

#[cfg(not(unix))]
fn write_key_file(path: &Path, bytes: &[u8]) -> Result<(), StoreError> {
    fs::write(path, bytes)?;
    Ok(())
}
```

- [ ] **Step 5: Wire `crypto` into `lib.rs`**

Replace `$POLYFLARE/crates/polyflare-store/src/lib.rs` with:
```rust
//! PolyFlare persistence: a SQLite store, at-rest token crypto (XChaCha20-Poly1305, never
//! Fernet), the account repository, and the zero-re-auth codex-lb importer. Token plaintext
//! is never logged.

pub mod crypto;
pub mod store;

pub use crypto::TokenCipher;
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
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd $POLYFLARE && cargo test -p polyflare-store crypto`
Expected: PASS (5 tests).

- [ ] **Step 7: Format + lint**

Run: `cd $POLYFLARE && cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 8: Commit**

```bash
cd $POLYFLARE
git add crates/polyflare-store
git commit -m "feat(store): XChaCha20-Poly1305 TokenCipher with 0600 key file"
```

---

## Task 3: Account model + repository

**Files:**
- Create: `crates/polyflare-store/src/account.rs`
- Modify: `crates/polyflare-store/src/store.rs` (add `Store::accounts`)
- Modify: `crates/polyflare-store/src/lib.rs`
- Create: `crates/polyflare-store/tests/account_repo.rs`

**Interfaces:**
- Consumes: `crate::crypto::TokenCipher`, `crate::StoreError`, `sqlx::SqlitePool`.
- Produces:
  - `struct Account { id, chatgpt_account_id, chatgpt_user_id, email, alias, workspace_id, workspace_label, seat_type, plan_type, routing_policy, last_refresh: i64, created_at: i64, status, deactivation_reason, reset_at: Option<i64>, blocked_at: Option<i64>, security_work_authorized: bool }` (`Debug, Clone, sqlx::FromRow`) — durable metadata, no token fields.
  - `struct PlainTokens { access_token: String, refresh_token: String, id_token: String }` (Clone; redacting Debug).
  - `struct EncryptedTokens { access_token_enc: Vec<u8>, refresh_token_enc: Vec<u8>, id_token_enc: Vec<u8> }` (`Debug, Clone, sqlx::FromRow`) with `fn encrypt(&PlainTokens, &TokenCipher) -> Result<EncryptedTokens, StoreError>`. This is the "encrypted token record" the importer produces.
  - `struct AccountRepo` with `new(SqlitePool)`, `insert`, `insert_encrypted`, `get`, `list`, `update_status`, `update_tokens`, `decrypt_tokens` (exact signatures in Step 4).
  - `Store::accounts(&self) -> AccountRepo`.

- [ ] **Step 1: Write the failing repository integration test**

`$POLYFLARE/crates/polyflare-store/tests/account_repo.rs`:
```rust
//! Account repository integration test against a temp-file DB.

use polyflare_store::{Account, PlainTokens, Store, TokenCipher};

fn sample_account(id: &str) -> Account {
    Account {
        id: id.to_string(),
        chatgpt_account_id: Some("ws-acct".to_string()),
        chatgpt_user_id: Some("user-1".to_string()),
        email: "user@example.test".to_string(),
        alias: Some("main".to_string()),
        workspace_id: Some("ws-1".to_string()),
        workspace_label: Some("Workspace One".to_string()),
        seat_type: Some("standard".to_string()),
        plan_type: "pro".to_string(),
        routing_policy: "normal".to_string(),
        last_refresh: 1_700_000_000,
        created_at: 1_699_000_000,
        status: "active".to_string(),
        deactivation_reason: None,
        reset_at: None,
        blocked_at: None,
        security_work_authorized: true,
    }
}

fn sample_tokens() -> PlainTokens {
    PlainTokens {
        access_token: "access-abc".to_string(),
        refresh_token: "refresh-def".to_string(),
        id_token: "id-ghi".to_string(),
    }
}

#[tokio::test]
async fn insert_get_list_decrypt_and_update() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[9u8; 32]).unwrap();
    let repo = store.accounts();

    // insert
    repo.insert(&sample_account("acct-1"), &sample_tokens(), &cipher)
        .await
        .unwrap();

    // get (present + absent)
    let got = repo.get("acct-1").await.unwrap().unwrap();
    assert_eq!(got.email, "user@example.test");
    assert_eq!(got.plan_type, "pro");
    assert!(got.security_work_authorized);
    assert!(repo.get("missing").await.unwrap().is_none());

    // list (ordered by id)
    repo.insert(&sample_account("acct-2"), &sample_tokens(), &cipher)
        .await
        .unwrap();
    let all = repo.list().await.unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].id, "acct-1");
    assert_eq!(all[1].id, "acct-2");

    // decrypt_tokens == originals
    let toks = repo.decrypt_tokens("acct-1", &cipher).await.unwrap().unwrap();
    assert_eq!(toks.access_token, "access-abc");
    assert_eq!(toks.refresh_token, "refresh-def");
    assert_eq!(toks.id_token, "id-ghi");

    // update_status
    repo.update_status("acct-1", "rate_limited").await.unwrap();
    assert_eq!(
        repo.get("acct-1").await.unwrap().unwrap().status,
        "rate_limited"
    );

    // update_tokens (re-encrypts + stamps last_refresh)
    let new_tokens = PlainTokens {
        access_token: "access-new".to_string(),
        refresh_token: "refresh-new".to_string(),
        id_token: "id-new".to_string(),
    };
    repo.update_tokens("acct-1", &new_tokens, &cipher, 1_700_500_000)
        .await
        .unwrap();
    let toks2 = repo.decrypt_tokens("acct-1", &cipher).await.unwrap().unwrap();
    assert_eq!(toks2.access_token, "access-new");
    assert_eq!(
        repo.get("acct-1").await.unwrap().unwrap().last_refresh,
        1_700_500_000
    );
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd $POLYFLARE && cargo test -p polyflare-store --test account_repo`
Expected: FAIL — compile errors (`Account`, `PlainTokens`, `Store::accounts` not found).

- [ ] **Step 3: Implement the account model + repository**

`$POLYFLARE/crates/polyflare-store/src/account.rs`:
```rust
//! Account model + repository. Durable metadata lives in `Account`; the three OAuth tokens are
//! stored ONLY as XChaCha20-Poly1305 ciphertext and decrypted on demand.

use sqlx::sqlite::SqlitePool;

use crate::crypto::TokenCipher;
use crate::StoreError;

/// Durable, non-secret account columns. The three token columns are intentionally absent —
/// they never leave the store as plaintext except through [`AccountRepo::decrypt_tokens`].
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Account {
    pub id: String,
    pub chatgpt_account_id: Option<String>,
    pub chatgpt_user_id: Option<String>,
    pub email: String,
    pub alias: Option<String>,
    pub workspace_id: Option<String>,
    pub workspace_label: Option<String>,
    pub seat_type: Option<String>,
    pub plan_type: String,
    pub routing_policy: String,
    pub last_refresh: i64,
    pub created_at: i64,
    pub status: String,
    pub deactivation_reason: Option<String>,
    pub reset_at: Option<i64>,
    pub blocked_at: Option<i64>,
    pub security_work_authorized: bool,
}

/// The three OAuth tokens in plaintext. Used as insert/update input and as decrypt output.
/// Never logged: its `Debug` redacts every field.
#[derive(Clone)]
pub struct PlainTokens {
    pub access_token: String,
    pub refresh_token: String,
    pub id_token: String,
}

impl std::fmt::Debug for PlainTokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlainTokens")
            .field("access_token", &"***")
            .field("refresh_token", &"***")
            .field("id_token", &"***")
            .finish()
    }
}

/// The three token columns as stored: XChaCha20-Poly1305 ciphertext (24-byte nonce prepended).
/// This is the "encrypted token record" the importer produces and the repository persists.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct EncryptedTokens {
    pub access_token_enc: Vec<u8>,
    pub refresh_token_enc: Vec<u8>,
    pub id_token_enc: Vec<u8>,
}

impl EncryptedTokens {
    /// Encrypt a [`PlainTokens`] triple under `cipher`.
    pub fn encrypt(tokens: &PlainTokens, cipher: &TokenCipher) -> Result<Self, StoreError> {
        Ok(Self {
            access_token_enc: cipher.encrypt(&tokens.access_token)?,
            refresh_token_enc: cipher.encrypt(&tokens.refresh_token)?,
            id_token_enc: cipher.encrypt(&tokens.id_token)?,
        })
    }
}

/// Full column list for `SELECT`ing an `Account` (must match the `FromRow` field order/names).
const SELECT_ACCOUNT_BY_ID: &str = "SELECT id, chatgpt_account_id, chatgpt_user_id, email, \
    alias, workspace_id, workspace_label, seat_type, plan_type, routing_policy, last_refresh, \
    created_at, status, deactivation_reason, reset_at, blocked_at, security_work_authorized \
    FROM accounts WHERE id = ?";

const SELECT_ALL_ACCOUNTS: &str = "SELECT id, chatgpt_account_id, chatgpt_user_id, email, \
    alias, workspace_id, workspace_label, seat_type, plan_type, routing_policy, last_refresh, \
    created_at, status, deactivation_reason, reset_at, blocked_at, security_work_authorized \
    FROM accounts ORDER BY id";

/// CRUD over the `accounts` table. Cheap to construct (clones the pool handle).
pub struct AccountRepo {
    pool: SqlitePool,
}

impl AccountRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Insert an account, encrypting its tokens on the way in.
    pub async fn insert(
        &self,
        account: &Account,
        tokens: &PlainTokens,
        cipher: &TokenCipher,
    ) -> Result<(), StoreError> {
        let enc = EncryptedTokens::encrypt(tokens, cipher)?;
        self.insert_encrypted(account, &enc).await
    }

    /// Insert an account whose tokens are already XChaCha-encrypted (used by the importer).
    pub async fn insert_encrypted(
        &self,
        account: &Account,
        enc: &EncryptedTokens,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO accounts (\
                id, chatgpt_account_id, chatgpt_user_id, email, alias, \
                workspace_id, workspace_label, seat_type, plan_type, routing_policy, \
                access_token_enc, refresh_token_enc, id_token_enc, \
                last_refresh, created_at, status, deactivation_reason, \
                reset_at, blocked_at, security_work_authorized\
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
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
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Fetch one account's metadata by id.
    pub async fn get(&self, id: &str) -> Result<Option<Account>, StoreError> {
        let account = sqlx::query_as::<_, Account>(SELECT_ACCOUNT_BY_ID)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(account)
    }

    /// List all accounts' metadata, ordered by id.
    pub async fn list(&self) -> Result<Vec<Account>, StoreError> {
        let accounts = sqlx::query_as::<_, Account>(SELECT_ALL_ACCOUNTS)
            .fetch_all(&self.pool)
            .await?;
        Ok(accounts)
    }

    /// Update an account's status string.
    pub async fn update_status(&self, id: &str, status: &str) -> Result<(), StoreError> {
        sqlx::query("UPDATE accounts SET status = ? WHERE id = ?")
            .bind(status)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Replace an account's tokens (re-encrypting) and stamp `last_refresh`.
    pub async fn update_tokens(
        &self,
        id: &str,
        tokens: &PlainTokens,
        cipher: &TokenCipher,
        last_refresh: i64,
    ) -> Result<(), StoreError> {
        let enc = EncryptedTokens::encrypt(tokens, cipher)?;
        sqlx::query(
            "UPDATE accounts SET access_token_enc = ?, refresh_token_enc = ?, \
             id_token_enc = ?, last_refresh = ? WHERE id = ?",
        )
        .bind(enc.access_token_enc.as_slice())
        .bind(enc.refresh_token_enc.as_slice())
        .bind(enc.id_token_enc.as_slice())
        .bind(last_refresh)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Decrypt and return an account's three tokens, or `None` if the account is absent.
    pub async fn decrypt_tokens(
        &self,
        id: &str,
        cipher: &TokenCipher,
    ) -> Result<Option<PlainTokens>, StoreError> {
        let enc = sqlx::query_as::<_, EncryptedTokens>(
            "SELECT access_token_enc, refresh_token_enc, id_token_enc FROM accounts WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        match enc {
            Some(enc) => Ok(Some(PlainTokens {
                access_token: cipher.decrypt(&enc.access_token_enc)?,
                refresh_token: cipher.decrypt(&enc.refresh_token_enc)?,
                id_token: cipher.decrypt(&enc.id_token_enc)?,
            })),
            None => Ok(None),
        }
    }
}
```

- [ ] **Step 4: Add `Store::accounts`**

Replace `$POLYFLARE/crates/polyflare-store/src/store.rs` with (adds the `accounts()` accessor + import):
```rust
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
```

- [ ] **Step 5: Wire `account` into `lib.rs`**

Replace `$POLYFLARE/crates/polyflare-store/src/lib.rs` with:
```rust
//! PolyFlare persistence: a SQLite store, at-rest token crypto (XChaCha20-Poly1305, never
//! Fernet), the account repository, and the zero-re-auth codex-lb importer. Token plaintext
//! is never logged.

pub mod account;
pub mod crypto;
pub mod store;

pub use account::{Account, AccountRepo, EncryptedTokens, PlainTokens};
pub use crypto::TokenCipher;
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
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `cd $POLYFLARE && cargo test -p polyflare-store --test account_repo`
Expected: PASS (1 test).

- [ ] **Step 7: Format + lint**

Run: `cd $POLYFLARE && cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 8: Commit**

```bash
cd $POLYFLARE
git add crates/polyflare-store
git commit -m "feat(store): Account model + repository with encrypt-at-rest tokens"
```

---

## Task 4: OAuth importer (codex-lb `store.db` → PolyFlare)

**Files:**
- Modify: `crates/polyflare-store/Cargo.toml` (add `fernet`)
- Create: `crates/polyflare-store/src/import.rs`
- Modify: `crates/polyflare-store/src/lib.rs`
- Create: `crates/polyflare-store/tests/import_codex_lb.rs`

**Interfaces:**
- Consumes: `crate::{Store, TokenCipher}`, `crate::account::{Account, EncryptedTokens, PlainTokens}`, `fernet::Fernet`, sqlx.
- Produces:
  - `struct ImportSummary { accounts_imported: usize, usage_rows_imported: usize }` (`Debug, Clone, Copy, Default, PartialEq, Eq`).
  - `async fn import_from_codex_lb(store: &Store, src_db_path: &Path, fernet_key_path: &Path, cipher: &TokenCipher) -> Result<ImportSummary, StoreError>`.

- [ ] **Step 1: Add the fernet dependency**

Add to `[dependencies]` in `$POLYFLARE/crates/polyflare-store/Cargo.toml` (after the `chacha20poly1305` line):
```toml
fernet = { workspace = true }
```
Add to `[dev-dependencies]` (the importer test builds a fixture source DB with sqlx and Fernet-encrypts tokens):
```toml
sqlx = { workspace = true }
fernet = { workspace = true }
```
(`sqlx` and `fernet` are already normal deps, but listing them under dev-deps too is harmless and makes the test's direct use explicit. If Cargo warns about a duplicate, drop the dev-dep lines — normal deps are already visible to integration tests, as M1's `polyflare-codex` test uses `futures_util`.)

> **Implementation-time note:** if `cargo` errors on duplicate `sqlx`/`fernet` keys across `[dependencies]` and `[dev-dependencies]`, delete the two dev-dep lines above — the integration test can use the crate's normal deps directly (proven by M1: `crates/polyflare-codex/tests/executor_stream.rs` uses `futures_util`, a `[dependencies]` entry).

- [ ] **Step 2: Write the failing importer test (builds a codex-lb-shaped fixture)**

`$POLYFLARE/crates/polyflare-store/tests/import_codex_lb.rs`:
```rust
//! Importer e2e: build a codex-lb-shaped source DB (Fernet-encrypted tokens), import it, and
//! assert the account + usage landed and the tokens decrypt back to plaintext under XChaCha.

use std::path::Path;

use fernet::Fernet;
use polyflare_store::{import_from_codex_lb, Store, TokenCipher};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

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
            last_refresh INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
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
            recorded_at INTEGER NOT NULL,
            \"window\" TEXT NOT NULL,
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
    .bind(1_700_000_000_i64)
    .bind(1_699_000_000_i64)
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
    .bind(1_700_000_500_i64)
    .bind("secondary")
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
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd $POLYFLARE && cargo test -p polyflare-store --test import_codex_lb`
Expected: FAIL — compile error (`import_from_codex_lb` not found).

- [ ] **Step 4: Implement the importer**

`$POLYFLARE/crates/polyflare-store/src/import.rs`:
```rust
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
```

- [ ] **Step 5: Wire `import` into `lib.rs`**

Replace `$POLYFLARE/crates/polyflare-store/src/lib.rs` with:
```rust
//! PolyFlare persistence: a SQLite store, at-rest token crypto (XChaCha20-Poly1305, never
//! Fernet), the account repository, and the zero-re-auth codex-lb importer. Token plaintext
//! is never logged.

pub mod account;
pub mod crypto;
pub mod import;
pub mod store;

pub use account::{Account, AccountRepo, EncryptedTokens, PlainTokens};
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
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `cd $POLYFLARE && cargo test -p polyflare-store --test import_codex_lb`
Expected: PASS (1 test).

- [ ] **Step 7: Format + lint**

Run: `cd $POLYFLARE && cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 8: Commit**

```bash
cd $POLYFLARE
git add crates/polyflare-store
git commit -m "feat(store): zero-re-auth codex-lb OAuth importer (Fernet→XChaCha)"
```

> **Implementation-time checks (real codex-lb `store.db`, not the fixture):**
> 1. **Column availability / names.** Confirm the real `accounts` table has every column the `SrcAccount` SELECT names — run `sqlite3 <store.db> '.schema accounts'` and reconcile. The port reference (`codex-lb-port-reference.md` §Accounts schema) is the expected set; if an older schema lacks a column (e.g. `routing_policy`, `blocked_at`), `COALESCE(col, default)` it or drop it from the SELECT and default it when building `Account`.
> 2. **Token column storage class.** `SrcAccount` reads the three `*_encrypted` columns as `Vec<u8>` (Python Fernet yields `bytes` → a BLOB value). Verify with `sqlite3 <store.db> "SELECT typeof(access_token_encrypted) FROM accounts LIMIT 1"`; if it reports `text`, change those three `SrcAccount` fields to `String` and simplify `fernet_decrypt` to take `&str` directly (drop the `from_utf8` of the token).
> 3. **WAL + read-only.** A live codex-lb writes its DB in WAL mode; open it read-only for import while codex-lb is stopped (or on a copy), so the `-wal`/`-shm` sidecars are quiescent and the read-only open cannot contend.

---

## Task 5: CLI — `polyflare serve` vs `polyflare accounts import`

**Files:**
- Modify: `crates/polyflare-server/Cargo.toml` (add `clap`, `polyflare-store`)
- Modify: `crates/polyflare-server/src/config.rs` (add data-dir/db/key path helpers; `from_env` unchanged)
- Modify: `crates/polyflare-server/src/main.rs` (clap subcommands + parse unit tests)

**Interfaces:**
- Consumes: `polyflare_server::config::{Config, data_dir_from_env, db_path, key_path}`, `polyflare_codex::CodexExecutor`, `polyflare_server::app::{AppState, build_app}`, `polyflare_store::{Store, TokenCipher, import_from_codex_lb}`.
- Produces: a `polyflare` binary whose CLI is `polyflare serve` | `polyflare accounts import --from <PATH> --fernet-key <PATH>`.

- [ ] **Step 1: Add the server's new dependencies**

Replace the `[dependencies]` block in `$POLYFLARE/crates/polyflare-server/Cargo.toml` with:
```toml
[dependencies]
polyflare-core = { path = "../polyflare-core" }
polyflare-codex = { path = "../polyflare-codex" }
polyflare-store = { path = "../polyflare-store" }
axum = { workspace = true }
tokio = { workspace = true }
serde_json = { workspace = true }
clap = { workspace = true }
```
Leave the existing `[lib]`, `[[bin]]`, and `[dev-dependencies]` sections unchanged.

- [ ] **Step 2: Add path helpers to `config.rs` (leaving `from_env` untouched)**

Replace `$POLYFLARE/crates/polyflare-server/src/config.rs` with:
```rust
//! Process configuration, read from environment. Secrets never logged.

use std::path::{Path, PathBuf};

use polyflare_core::Account;

pub struct Config {
    pub bind_addr: String,
    pub account: Account,
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let bind_addr =
            std::env::var("POLYFLARE_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
        let base_url = std::env::var("POLYFLARE_UPSTREAM_URL")
            .map_err(|_| "POLYFLARE_UPSTREAM_URL not set".to_string())?;
        let bearer_token = std::env::var("POLYFLARE_UPSTREAM_TOKEN")
            .map_err(|_| "POLYFLARE_UPSTREAM_TOKEN not set".to_string())?;
        Ok(Config {
            bind_addr,
            account: Account { id: "default".into(), base_url, bearer_token },
        })
    }
}

/// The PolyFlare data directory: `$POLYFLARE_DATA_DIR`, else `$HOME/.polyflare`.
pub fn data_dir_from_env() -> PathBuf {
    if let Ok(dir) = std::env::var("POLYFLARE_DATA_DIR") {
        return PathBuf::from(dir);
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join(".polyflare")
}

/// The store DB path within a data directory.
pub fn db_path(data_dir: &Path) -> PathBuf {
    data_dir.join("store.db")
}

/// The at-rest key file path within a data directory (raw 32 bytes).
pub fn key_path(data_dir: &Path) -> PathBuf {
    data_dir.join("key")
}
```

- [ ] **Step 3: Write the failing CLI parse tests**

Replace `$POLYFLARE/crates/polyflare-server/src/main.rs` with the file below **in full** in Step 5. For the TDD failing step, first add ONLY the test module by creating `main.rs` containing the current M1 body plus this appended block, then run it — it will fail to compile because `Cli`/`Commands`/`AccountsCommands` don't exist yet. To keep the step atomic, write the complete Step-5 file now and treat Step 4 as the "confirm fail" gate against a deliberately-not-yet-present symbol. The tests to be satisfied:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn parses_serve() {
        let cli = Cli::try_parse_from(["polyflare", "serve"]).unwrap();
        assert!(matches!(cli.command, Commands::Serve));
    }

    #[test]
    fn parses_accounts_import() {
        let cli = Cli::try_parse_from([
            "polyflare",
            "accounts",
            "import",
            "--from",
            "/tmp/store.db",
            "--fernet-key",
            "/tmp/encryption.key",
        ])
        .unwrap();
        match cli.command {
            Commands::Accounts {
                command: AccountsCommands::Import { from, fernet_key },
            } => {
                assert_eq!(from, std::path::PathBuf::from("/tmp/store.db"));
                assert_eq!(fernet_key, std::path::PathBuf::from("/tmp/encryption.key"));
            }
            _ => panic!("expected `accounts import`"),
        }
    }

    #[test]
    fn missing_subcommand_is_an_error() {
        assert!(Cli::try_parse_from(["polyflare"]).is_err());
    }
}
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `cd $POLYFLARE && cargo test -p polyflare-server --bin polyflare`
Expected: FAIL — compile errors (`Cli`, `Commands`, `AccountsCommands` not found) until Step 5 lands.

- [ ] **Step 5: Implement the clap CLI (serve path identical to M1)**

`$POLYFLARE/crates/polyflare-server/src/main.rs`:
```rust
//! PolyFlare binary entrypoint. Subcommands: `serve` (the M1 gateway) and `accounts import`
//! (the zero-re-auth codex-lb importer). Secrets are read from env / files and never logged.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::{Parser, Subcommand};

use polyflare_codex::CodexExecutor;
use polyflare_server::app::{build_app, AppState};
use polyflare_server::config::{self, Config};
use polyflare_store::{import_from_codex_lb, Store, TokenCipher};

#[derive(Parser)]
#[command(name = "polyflare", version, about = "Multi-provider LLM-CLI load balancer")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the gateway server (the M1 serve path).
    Serve,
    /// Account management.
    Accounts {
        #[command(subcommand)]
        command: AccountsCommands,
    },
}

#[derive(Subcommand)]
enum AccountsCommands {
    /// Import accounts + usage from a codex-lb store.db (zero re-auth).
    Import {
        /// Path to the codex-lb store.db (opened read-only).
        #[arg(long = "from", value_name = "PATH")]
        from: PathBuf,
        /// Path to the codex-lb Fernet key file.
        #[arg(long = "fernet-key", value_name = "PATH")]
        fernet_key: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Serve => serve().await,
        Commands::Accounts { command } => match command {
            AccountsCommands::Import { from, fernet_key } => {
                accounts_import(&from, &fernet_key).await
            }
        },
    }
}

/// The M1 server: identical wiring and behavior to the pre-M2a `main`.
async fn serve() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_env()?;
    let executor = Arc::new(CodexExecutor::new()?);
    let state = Arc::new(AppState { executor, account: config.account });
    let app = build_app(state);

    let listener = tokio::net::TcpListener::bind(&config.bind_addr).await?;
    println!("polyflare listening on {}", config.bind_addr);
    axum::serve(listener, app).await?;
    Ok(())
}

/// Run the importer against the configured store + at-rest key, printing only counts.
async fn accounts_import(from: &Path, fernet_key: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = config::data_dir_from_env();
    let db_path = config::db_path(&data_dir);
    let key_path = config::key_path(&data_dir);

    let store = Store::open(&db_path).await?;
    let cipher = TokenCipher::load_or_create(&key_path)?;
    let summary = import_from_codex_lb(&store, from, fernet_key, &cipher).await?;
    println!(
        "imported {} account(s) and {} usage row(s)",
        summary.accounts_imported, summary.usage_rows_imported
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn parses_serve() {
        let cli = Cli::try_parse_from(["polyflare", "serve"]).unwrap();
        assert!(matches!(cli.command, Commands::Serve));
    }

    #[test]
    fn parses_accounts_import() {
        let cli = Cli::try_parse_from([
            "polyflare",
            "accounts",
            "import",
            "--from",
            "/tmp/store.db",
            "--fernet-key",
            "/tmp/encryption.key",
        ])
        .unwrap();
        match cli.command {
            Commands::Accounts {
                command: AccountsCommands::Import { from, fernet_key },
            } => {
                assert_eq!(from, std::path::PathBuf::from("/tmp/store.db"));
                assert_eq!(fernet_key, std::path::PathBuf::from("/tmp/encryption.key"));
            }
            _ => panic!("expected `accounts import`"),
        }
    }

    #[test]
    fn missing_subcommand_is_an_error() {
        assert!(Cli::try_parse_from(["polyflare"]).is_err());
    }
}
```

- [ ] **Step 6: Run the CLI tests to verify they pass**

Run: `cd $POLYFLARE && cargo test -p polyflare-server --bin polyflare`
Expected: PASS (4 tests).

- [ ] **Step 7: Verify the serve path is unchanged (existing server integration tests still green)**

Run: `cd $POLYFLARE && cargo test -p polyflare-server`
Expected: PASS — the M1 integration tests (`e2e_passthrough`, `ingress_relays`, `large_body`) plus the 4 new bin tests.

- [ ] **Step 8: Full workspace test + format + lint**

Run:
```bash
cd $POLYFLARE
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
Expected: fmt clean, no clippy warnings, all tests pass across every crate.

- [ ] **Step 9: Manual CLI smoke (documented; optional to run)**

```bash
cd $POLYFLARE
# Help text lists both subcommands:
cargo run --bin polyflare -- --help
cargo run --bin polyflare -- accounts import --help
# Import against a codex-lb store.db (writes to $POLYFLARE_DATA_DIR or ~/.polyflare):
POLYFLARE_DATA_DIR=/tmp/pf-smoke \
  cargo run --bin polyflare -- accounts import \
    --from /path/to/codex-lb/store.db \
    --fernet-key /path/to/codex-lb/encryption.key
# Serve (unchanged from M1):
POLYFLARE_UPSTREAM_URL="https://<upstream-base>" \
POLYFLARE_UPSTREAM_TOKEN="<oauth-bearer>" \
  cargo run --bin polyflare -- serve
```

- [ ] **Step 10: Commit**

```bash
cd $POLYFLARE
git add crates/polyflare-server
git commit -m "feat(server): clap CLI with serve + accounts import subcommands"
```

---

## Self-review (completed against the spec)

**1. Spec coverage (M2a scope items → tasks):**
- **Workspace deps + `polyflare-store` foundation** (sqlx/chacha20poly1305/fernet/clap; `Store` over `SqlitePool` create-if-missing + WAL; embedded `sqlx::migrate!`; first migration creates `accounts` + `usage_history` with INTEGER epochs + BLOB token columns; temp-file round-trip test asserting tables exist) → **Task 1**. ✅
- **Token crypto** (XChaCha20-Poly1305 `TokenCipher`; load/create raw 32-byte key file chmod 0600; `encrypt(&str)->Vec<u8>` with 24-byte random nonce prepended; `decrypt(&[u8])->Result<String>`; round-trip / tampered-fails / nonce-varies unit tests) → **Task 2**. ✅
- **Account model + repository** (`Account` durable-metadata row struct; `EncryptedTokens` = the tokens-encrypted record; `PlainTokens`; `AccountRepo::{insert, insert_encrypted, get, list, update_status, update_tokens, decrypt_tokens}`; tokens stored encrypted, decrypt-on-demand method; temp-file integration test insert→get/list→decrypt==originals→update_status) → **Task 3**. ✅
- **OAuth importer** (read codex-lb `store.db` read-only; per-`accounts`-row Fernet-decrypt the 3 blobs with the Fernet key file, re-encrypt XChaCha, insert via column-intersection mapping; copy `usage_history` by value; never log tokens; fixture-source test asserting account + usage landed and tokens XChaCha-decrypt to originals) → **Task 4**. ✅
- **CLI** (`clap` derive; `polyflare serve` = M1 path, `polyflare accounts import --from <path> --fernet-key <path>`; serve behavior identical to M1; parse tests) → **Task 5**. ✅
- **Global constraints** (Rust 2021; XChaCha not Fernet at rest; never log tokens; secrets from env/key-file; serve path unchanged; sqlx runtime-checked stated; forward-only migrations) → **Global Constraints** section, enforced per task. ✅
- **Explicitly out of M2a scope (M2b), so no task here:** OAuth *refresh* (JWT decode + `POST /oauth/token` + should_refresh), the `capacity_weighted` selector, pool wiring into ingress. These are named in `M2-SCOPE` as M2b and intentionally omitted. ✅

**2. Placeholder scan:** No `TBD`/`TODO`/"add error handling"/"similar to above". Every code step is complete, compilable Rust. The two "Implementation-time note/checks" blocks (Task 4 dev-dep dedupe; real-DB column/storage/WAL reconciliation) are explicit, bounded verification steps with concrete commands — not hidden gaps, exactly as the spec requested for genuine uncertainties. ✅

**3. Type consistency across tasks:**
- `StoreError` — same 5-variant definition shown verbatim in Tasks 1→2→3→4 lib.rs (grown only by adding module declarations; the enum body is identical each time). ✅
- `Store::open(&Path) -> Result<Store, StoreError>`, `Store::pool() -> &SqlitePool`, `Store::accounts() -> AccountRepo` — `open`/`pool` defined Task 1, `accounts` added Task 3; consumed identically by Tasks 3–5 tests and the importer. ✅
- `TokenCipher::{load_or_create, from_key_bytes, encrypt, decrypt}` — defined Task 2; used unchanged by `AccountRepo`/`EncryptedTokens::encrypt` (Task 3), the importer (Task 4), and the CLI (Task 5). ✅
- `Account` fields (17, no token fields), `PlainTokens{access_token,refresh_token,id_token}`, `EncryptedTokens{access_token_enc,refresh_token_enc,id_token_enc}` — one definition (Task 3), reused by the importer's `Account{..}` construction and `EncryptedTokens::encrypt` (Task 4) and the repo test (Task 3). Field names match the SELECT column lists and the migration DDL column names. ✅
- `import_from_codex_lb(&Store, &Path, &Path, &TokenCipher) -> Result<ImportSummary, StoreError>` and `ImportSummary{accounts_imported,usage_rows_imported}` — defined Task 4; called with the same argument order by the CLI (Task 5) and the Task 4 test. ✅
- CLI `config::{data_dir_from_env, db_path, key_path}` — defined Task 5 config.rs; used by `accounts_import` in the same task. `Config`/`from_env`/`AppState`/`build_app` unchanged from M1 (byte-for-byte in `serve()`), so existing server tests keep compiling. ✅
- SQL column names are consistent everywhere: migration DDL, `SrcAccount`/`Account` fields, INSERT/SELECT column lists, and the importer's source SELECT all use the same names; `"window"` is double-quoted in every statement that references it (migration, importer source SELECT, importer target INSERT, and the fixture DDL/inserts). ✅

**Known API caveats to watch during execution (not blockers):**
- **sqlx bind types:** binds use `.as_str()` / `.as_deref()` / `.as_slice()` for `String`/`Option<String>`/`Vec<u8>` fields and pass `i64`/`Option<i64>`/`bool`/`f64` by value — the forms sqlx's `Encode` impls accept directly (avoids relying on a `&String`/`&Vec<u8>` blanket impl). ✅
- **`sqlx::FromRow` needs the `macros` feature** (enabled) — but no `query!`/`query_as!` compile-time macro is invoked, so no `DATABASE_URL` / `.sqlx` cache is required. ✅
- **`bool` ↔ SQLite `INTEGER`:** sqlx encodes `bool` as 0/1 and decodes non-zero as `true` for `security_work_authorized` and the two `credits_*` columns. ✅
- **`fernet` 0.2 API** (`Fernet::new(&str)->Option`, `.encrypt(&[u8])->String`, `.decrypt(&str)->Result<Vec<u8>,_>`, `Fernet::generate_key()->String`): Context7 had no Rust `fernet` entry, so verify against docs.rs/fernet at build time — the crate is small and stable, and the code above matches its documented surface. ✅
- **Integration tests see normal deps:** `tests/*` use `sqlx`/`fernet` (normal `[dependencies]`) directly, as M1's codex test uses `futures_util`. If any Cargo dedupe warning appears from the optional dev-dep lines in Task 4 Step 1, drop those lines. ✅

---

## Execution handoff

M2a delivers a persistent, encrypted-at-rest account store with a zero-re-auth codex-lb importer and a subcommand CLI — `polyflare accounts import` works and accounts persist + load, with the serve path unchanged from M1. M2b (OAuth refresh + `capacity_weighted` selector + pool wiring into ingress) builds on `polyflare-store` next, on these seams.
