-- One public model may be backed by more than one provider. Preserve the original table as
-- immutable migration evidence, then copy every row into the current table with uniqueness scoped
-- to `(provider_id, public_model)`. The legacy table deliberately remains available for recovery;
-- runtime code continues to address `provider_models`.
ALTER TABLE provider_models RENAME TO provider_models_global_unique_legacy;

CREATE TABLE provider_models (
    id                              TEXT PRIMARY KEY,
    provider_id                     TEXT NOT NULL REFERENCES custom_providers(id) ON DELETE CASCADE,
    public_model                    TEXT NOT NULL,
    upstream_model                  TEXT NOT NULL,
    display_name                    TEXT NOT NULL,
    context_window                  INTEGER CHECK (context_window IS NULL OR context_window > 0),
    max_output_tokens               INTEGER CHECK (max_output_tokens IS NULL OR max_output_tokens > 0),
    supports_tools                  INTEGER NOT NULL DEFAULT 1 CHECK (supports_tools IN (0, 1)),
    supports_vision                 INTEGER NOT NULL DEFAULT 0 CHECK (supports_vision IN (0, 1)),
    supports_parallel_tool_calls    INTEGER NOT NULL DEFAULT 1 CHECK (supports_parallel_tool_calls IN (0, 1)),
    supports_web_search             INTEGER NOT NULL DEFAULT 0 CHECK (supports_web_search IN (0, 1)),
    supports_reasoning_summaries    INTEGER NOT NULL DEFAULT 0 CHECK (supports_reasoning_summaries IN (0, 1)),
    reasoning_levels_json           TEXT NOT NULL DEFAULT '[]',
    model_info_json                 TEXT,
    input_per_million               REAL CHECK (input_per_million IS NULL OR input_per_million >= 0),
    cached_input_per_million        REAL CHECK (cached_input_per_million IS NULL OR cached_input_per_million >= 0),
    output_per_million              REAL CHECK (output_per_million IS NULL OR output_per_million >= 0),
    enabled                         INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    created_at                      INTEGER NOT NULL,
    updated_at                      INTEGER NOT NULL,
    visible_in_codex                INTEGER NOT NULL DEFAULT 1 CHECK (visible_in_codex IN (0, 1)),
    visible_in_openai               INTEGER NOT NULL DEFAULT 1 CHECK (visible_in_openai IN (0, 1)),
    instruction_mode                TEXT NOT NULL DEFAULT 'none'
                                        CHECK (instruction_mode IN ('none', 'append', 'replace')),
    instruction_text                TEXT NOT NULL DEFAULT '',
    request_overrides_json          TEXT NOT NULL DEFAULT '{}',
    UNIQUE (provider_id, public_model)
);

INSERT INTO provider_models (
    id,
    provider_id,
    public_model,
    upstream_model,
    display_name,
    context_window,
    max_output_tokens,
    supports_tools,
    supports_vision,
    supports_parallel_tool_calls,
    supports_web_search,
    supports_reasoning_summaries,
    reasoning_levels_json,
    model_info_json,
    input_per_million,
    cached_input_per_million,
    output_per_million,
    enabled,
    created_at,
    updated_at,
    visible_in_codex,
    visible_in_openai,
    instruction_mode,
    instruction_text,
    request_overrides_json
)
SELECT
    id,
    provider_id,
    public_model,
    upstream_model,
    display_name,
    context_window,
    max_output_tokens,
    supports_tools,
    supports_vision,
    supports_parallel_tool_calls,
    supports_web_search,
    supports_reasoning_summaries,
    reasoning_levels_json,
    model_info_json,
    input_per_million,
    cached_input_per_million,
    output_per_million,
    enabled,
    created_at,
    updated_at,
    visible_in_codex,
    visible_in_openai,
    instruction_mode,
    instruction_text,
    request_overrides_json
FROM provider_models_global_unique_legacy;

CREATE INDEX idx_provider_models_multi_target_provider
    ON provider_models(provider_id, enabled);
CREATE INDEX idx_provider_models_multi_target_public
    ON provider_models(public_model, enabled, provider_id);
