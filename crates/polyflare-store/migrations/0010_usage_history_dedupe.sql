-- Make usage_history writes idempotent so the codex-lb importer can be RE-RUN to pull only
-- genuinely-new rows instead of duplicating already-imported ones (the importer was originally a
-- one-time into-a-fresh-store tool). The natural key of a usage snapshot is
-- (account_id, "window", recorded_at) — exactly one sample per account, per window, per timestamp.
-- A UNIQUE index on it turns the importer's (and the poller's) `INSERT OR IGNORE` into a
-- content-preserving no-op on collision, deduping against BOTH prior imports and the poller's own
-- rows regardless of origin (no per-row source id needed).
--
-- NULL-safe: the index keys on COALESCE("window", '') rather than the raw column, because SQLite
-- treats NULLs as DISTINCT in a plain UNIQUE index — so two NULL-window snapshots for the same
-- (account, timestamp) would NOT dedupe. `''` is never a real window ('primary'/'secondary' only),
-- so mapping NULL → '' can never collide with a legitimate window value. In practice both the
-- importer and the 600s poller always write a non-null window; this just makes the dedup correct
-- even if a source ever carries a NULL-window row. Verified 0 existing duplicate tuples before
-- adding this index, so creation cannot fail on a populated store.
CREATE UNIQUE INDEX IF NOT EXISTS idx_usage_history_dedupe
    ON usage_history (account_id, COALESCE("window", ''), recorded_at);
