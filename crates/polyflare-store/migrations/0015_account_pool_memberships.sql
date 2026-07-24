-- One account may participate in multiple named routing groups. Keep accounts.pool as the
-- backward-compatible primary label while this normalized table is the routing truth.
CREATE TABLE account_pool_memberships (
    account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    pool TEXT NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    PRIMARY KEY (account_id, pool)
);

INSERT INTO account_pool_memberships (account_id, pool)
SELECT id, pool FROM accounts WHERE pool IS NOT NULL;

CREATE INDEX idx_account_pool_memberships_pool
    ON account_pool_memberships(pool, account_id);
