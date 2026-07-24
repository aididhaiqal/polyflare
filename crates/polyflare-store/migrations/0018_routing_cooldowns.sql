CREATE TABLE IF NOT EXISTS routing_cooldowns (
    account_id TEXT PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
    cooldown_until INTEGER NOT NULL,
    reason TEXT NOT NULL CHECK (reason IN ('rate_limit', 'quota')),
    updated_at INTEGER NOT NULL
);
