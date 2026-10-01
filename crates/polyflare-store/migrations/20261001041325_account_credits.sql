-- Per-account purchasable credit balance as the usage poll last saw it (`/wham/usage.credits`):
-- `balance` in USD, `has_credits`/`unlimited` as the upstream reports them. One row per account,
-- replaced on every successful poll; absent until the first poll after onboarding.
CREATE TABLE account_credits (
    account_id TEXT PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
    balance REAL NOT NULL,
    has_credits INTEGER NOT NULL DEFAULT 0,
    unlimited INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL
);
