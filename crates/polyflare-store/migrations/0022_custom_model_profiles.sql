-- Multiple public provider models may already share one upstream slug. These fields turn each
-- public mapping into an explicit request profile while keeping every existing row a no-op.
ALTER TABLE provider_models ADD COLUMN instruction_mode TEXT NOT NULL DEFAULT 'none'
    CHECK (instruction_mode IN ('none', 'append', 'replace'));
ALTER TABLE provider_models ADD COLUMN instruction_text TEXT NOT NULL DEFAULT '';
ALTER TABLE provider_models ADD COLUMN request_overrides_json TEXT NOT NULL DEFAULT '{}';

-- Content-free fingerprint of the normalized profile configuration used for a request. The
-- instruction text itself must never enter operational telemetry.
ALTER TABLE request_log ADD COLUMN profile_revision TEXT;
