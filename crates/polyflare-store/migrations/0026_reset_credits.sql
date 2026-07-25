CREATE TABLE reset_credit_snapshots (
    account_id TEXT PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
    available_count INTEGER NOT NULL,
    fetched_at INTEGER NOT NULL
);

CREATE TABLE reset_credits (
    account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    credit_id TEXT NOT NULL,
    reset_type TEXT,
    status TEXT,
    granted_at INTEGER,
    expires_at INTEGER,
    title TEXT,
    description TEXT,
    redeem_started_at INTEGER,
    redeemed_at INTEGER,
    PRIMARY KEY (account_id, credit_id)
);

CREATE INDEX idx_reset_credits_available_expiry
    ON reset_credits(account_id, status, expires_at);

CREATE TABLE reset_credit_redeem_claims (
    account_id TEXT PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
    holder_id TEXT NOT NULL,
    expires_at INTEGER NOT NULL
);

CREATE TABLE reset_credit_redeem_requests (
    account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    redeem_request_id TEXT NOT NULL,
    credit_id TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    result_code TEXT,
    windows_reset INTEGER,
    redeemed_at INTEGER,
    completed_at INTEGER,
    PRIMARY KEY (account_id, redeem_request_id)
);

CREATE INDEX idx_reset_credit_redeem_requests_created
    ON reset_credit_redeem_requests(created_at);

-- Stock Codex may consume without naming a credit. Pin its caller idempotency key to the first
-- selected fleet account before entering the account-scoped ledger so a retry can never re-rank
-- onto another account after an ambiguous upstream result.
CREATE TABLE reset_credit_native_requests (
    redeem_request_id TEXT PRIMARY KEY,
    -- Intentionally not a foreign key: an ambiguous upstream consume must stay pinned even if
    -- the account is deleted. A retry then fails closed instead of re-ranking onto another account.
    account_id TEXT,
    requested_credit_id TEXT,
    created_at INTEGER NOT NULL,
    result_code TEXT,
    windows_reset INTEGER,
    completed_at INTEGER,
    CHECK (account_id IS NOT NULL OR result_code IS NOT NULL)
);

CREATE INDEX idx_reset_credit_native_requests_created
    ON reset_credit_native_requests(created_at);

-- Fleet retries must preserve the exact reviewed execution plan. Per-account child keys alone do
-- not prevent a caller from appending another account to the same parent request on retry.
CREATE TABLE reset_credit_fleet_requests (
    redeem_request_id TEXT PRIMARY KEY,
    account_ids_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE INDEX idx_reset_credit_fleet_requests_created
    ON reset_credit_fleet_requests(created_at);
