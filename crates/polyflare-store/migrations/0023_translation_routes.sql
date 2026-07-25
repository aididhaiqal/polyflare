CREATE TABLE translation_routes (
    id              TEXT PRIMARY KEY NOT NULL,
    name            TEXT NOT NULL,
    enabled         INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    source_protocol TEXT NOT NULL CHECK (source_protocol IN ('anthropic_messages')),
    match_kind      TEXT NOT NULL CHECK (match_kind IN ('exact', 'prefix', 'contains')),
    model_pattern   TEXT NOT NULL CHECK (length(model_pattern) > 0),
    target_provider TEXT NOT NULL CHECK (target_provider IN ('codex')),
    target_model    TEXT NOT NULL CHECK (length(target_model) > 0),
    reasoning_effort TEXT CHECK (
        reasoning_effort IS NULL OR reasoning_effort IN
        ('none', 'minimal', 'low', 'medium', 'high', 'xhigh', 'max')
    ),
    priority        INTEGER NOT NULL DEFAULT 100,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);

CREATE INDEX idx_translation_routes_resolution
    ON translation_routes(source_protocol, enabled, priority, id);

-- Preserve the pre-0023 hard-coded behavior. Lower priority values run first.
INSERT INTO translation_routes
    (id, name, enabled, source_protocol, match_kind, model_pattern, target_provider,
     target_model, reasoning_effort, priority, created_at, updated_at)
VALUES
    ('default-anthropic-opus', 'Claude Opus to Codex Sol', 1, 'anthropic_messages',
     'contains', 'opus', 'codex', 'gpt-5.6-sol', 'high', 100, unixepoch(), unixepoch()),
    ('default-anthropic-sonnet', 'Claude Sonnet to Codex Terra', 1, 'anthropic_messages',
     'contains', 'sonnet', 'codex', 'gpt-5.6-terra', 'medium', 200, unixepoch(), unixepoch()),
    ('default-anthropic-haiku', 'Claude Haiku to Codex Luna', 1, 'anthropic_messages',
     'contains', 'haiku', 'codex', 'gpt-5.6-luna', 'low', 300, unixepoch(), unixepoch());
