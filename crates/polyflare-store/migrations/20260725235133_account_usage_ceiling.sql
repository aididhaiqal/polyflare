-- Per-account usage ceiling: stop routing to an account once it reaches a configured percentage of
-- its quota, reserving the remainder. NULL is the pre-existing behaviour (uncapped), so every
-- existing row is a no-op. The override lets an operator keep burning a capped account without
-- losing the configured ceiling.
ALTER TABLE accounts ADD COLUMN usage_cap_percent REAL
    CHECK (usage_cap_percent IS NULL OR (usage_cap_percent > 0 AND usage_cap_percent <= 100));

ALTER TABLE accounts ADD COLUMN usage_cap_override INTEGER NOT NULL DEFAULT 0
    CHECK (usage_cap_override IN (0, 1));
