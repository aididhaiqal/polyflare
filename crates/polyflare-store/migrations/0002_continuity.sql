-- PolyFlare continuity state machine (M3). Forward-only. NO conversation content is stored here:
-- only per-session state, the last-observed anchor id, and a response_id -> owning-account map.
-- Timestamps are INTEGER unix-epoch seconds.

CREATE TABLE IF NOT EXISTS continuity_sessions (
    session_key            TEXT    PRIMARY KEY,
    key_strength           TEXT    NOT NULL,              -- 'hard' | 'soft'
    owning_account_id      TEXT        REFERENCES accounts(id) ON DELETE SET NULL,
    anchor_response_id     TEXT,                          -- last response.id we saw complete
    last_input_fingerprint TEXT,                          -- diagnostic sha256 of the input array
    last_input_count       INTEGER,                       -- diagnostic item count of the input array
    reasoning_cache_ref    TEXT,                          -- R3 (M3-followup); NULL in M3-core
    state                  TEXT    NOT NULL,              -- 'fresh'|'anchored'|'reattaching'|'recover'
    created_at             INTEGER NOT NULL,
    updated_at             INTEGER NOT NULL,
    last_activity_at       INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_continuity_sessions_activity
    ON continuity_sessions (last_activity_at);

-- response_id -> owner map: a CLIENT-supplied previous_response_id resolves to its account even
-- when the derived session_key differs (or is soft/absent). The ownership backbone.
CREATE TABLE IF NOT EXISTS continuity_anchors (
    response_id       TEXT    PRIMARY KEY,
    session_key       TEXT    NOT NULL REFERENCES continuity_sessions(session_key) ON DELETE CASCADE,
    owning_account_id TEXT    NOT NULL,
    created_at        INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_continuity_anchors_session
    ON continuity_anchors (session_key);
