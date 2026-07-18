-- D18 Task 1: client API-key auth for the proxy surface. Forward-only. Content-free: this table
-- stores ONLY the sha256 hash of a client-presented key (never the plaintext) plus a short
-- display prefix. The raw key is generated + revealed exactly once by the CLI (Task 2) and is
-- never persisted, never re-derivable from this table. Timestamps are INTEGER unix-epoch seconds,
-- matching every other table in this store.

CREATE TABLE IF NOT EXISTS api_keys (
    id           TEXT    PRIMARY KEY,          -- uuid
    key_hash     TEXT    NOT NULL UNIQUE,       -- sha256 hex of the plaintext key
    key_prefix   TEXT    NOT NULL,              -- first ~15 chars, for display only
    label        TEXT,
    enabled      INTEGER NOT NULL DEFAULT 1,
    created_at   INTEGER NOT NULL,
    last_used_at INTEGER                        -- nullable; set by touch_last_used
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_api_keys_key_hash ON api_keys(key_hash);
