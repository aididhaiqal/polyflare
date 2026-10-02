-- Per-message Claude Code usage imported from each machine's local transcripts
-- (~/.claude/projects/**/*.jsonl, the same source ccusage reads), pushed by
-- scripts/claude-usage-push to POST /api/claude/local-usage. One row per (machine, request,
-- message); re-pushes are idempotent through the primary key. Token columns mirror the
-- transcript's `message.usage` (cache writes split by TTL because they are priced differently);
-- `cost_usd` is the API list-price estimate computed at ingest, NULL for an unpriced model.
CREATE TABLE claude_local_usage (
    machine TEXT NOT NULL,
    request_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    session_id TEXT,
    project TEXT,
    model TEXT NOT NULL,
    ts INTEGER NOT NULL,
    input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    cache_write_5m_tokens INTEGER NOT NULL DEFAULT 0,
    cache_write_1h_tokens INTEGER NOT NULL DEFAULT 0,
    cache_read_tokens INTEGER NOT NULL DEFAULT 0,
    service_tier TEXT,
    speed TEXT,
    cost_usd REAL,
    imported_at INTEGER NOT NULL,
    PRIMARY KEY (machine, request_id, message_id)
);
CREATE INDEX idx_claude_local_usage_ts ON claude_local_usage(ts);
CREATE INDEX idx_claude_local_usage_model_ts ON claude_local_usage(model, ts);
