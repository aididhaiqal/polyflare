-- Routing and discovery are separate operator decisions. Existing provider models keep their
-- current catalog behavior; operators may hide either surface without disabling direct routing.
ALTER TABLE provider_models ADD COLUMN visible_in_codex INTEGER NOT NULL DEFAULT 1
    CHECK (visible_in_codex IN (0, 1));
ALTER TABLE provider_models ADD COLUMN visible_in_openai INTEGER NOT NULL DEFAULT 1
    CHECK (visible_in_openai IN (0, 1));
