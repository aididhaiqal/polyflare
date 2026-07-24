CREATE TABLE account_onboarding_flows (
    id TEXT PRIMARY KEY,
    provider TEXT NOT NULL CHECK (provider = 'codex'),
    oauth_state TEXT NOT NULL UNIQUE,
    verifier_enc BLOB NOT NULL,
    initial_pool TEXT,
    status TEXT NOT NULL CHECK (status IN ('pending', 'exchanging', 'completed', 'failed')),
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    finished_at INTEGER,
    account_id TEXT,
    error_code TEXT,
    FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE SET NULL
);

CREATE INDEX account_onboarding_flows_expires_idx
    ON account_onboarding_flows(expires_at);
