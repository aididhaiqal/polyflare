-- 0019 and 0023 may already be applied in operator databases. Preserve their columns and
-- constraints as legacy evidence, then add the protocol-capable columns used by current code.
-- Renaming avoids rebuilding tables that own/receive foreign keys on SQLite.
ALTER TABLE custom_providers RENAME COLUMN wire_api TO legacy_wire_api;
ALTER TABLE custom_providers ADD COLUMN wire_api TEXT NOT NULL DEFAULT 'responses'
    CHECK (wire_api IN ('responses', 'anthropic_messages'));

ALTER TABLE translation_routes RENAME COLUMN source_protocol TO legacy_source_protocol;
ALTER TABLE translation_routes ADD COLUMN source_protocol TEXT NOT NULL DEFAULT 'anthropic_messages'
    CHECK (source_protocol IN ('anthropic_messages', 'openai_responses'));

ALTER TABLE translation_routes RENAME COLUMN target_provider TO legacy_target_provider;
ALTER TABLE translation_routes ADD COLUMN target_kind TEXT NOT NULL DEFAULT 'builtin_provider'
    CHECK (target_kind IN ('builtin_provider', 'custom_provider'));
ALTER TABLE translation_routes ADD COLUMN target_provider_id TEXT NOT NULL DEFAULT 'codex';

DROP INDEX idx_translation_routes_resolution;
CREATE INDEX idx_translation_routes_resolution
    ON translation_routes(source_protocol, enabled, priority, id);
