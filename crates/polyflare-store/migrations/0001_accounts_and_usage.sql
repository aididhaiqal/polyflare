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
