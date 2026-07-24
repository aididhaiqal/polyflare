-- Operator-configured model providers are deliberately separate from subscription OAuth
-- accounts. A provider owns endpoint/protocol policy, credentials are independently selectable
-- routing targets, and models describe the public catalog/routing surface.
CREATE TABLE custom_providers (
    id                      TEXT PRIMARY KEY,
    slug                    TEXT NOT NULL UNIQUE,
    display_name            TEXT NOT NULL,
    base_url                TEXT NOT NULL,
    wire_api                TEXT NOT NULL DEFAULT 'responses'
                                CHECK (wire_api IN ('responses')),
    enabled                 INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    stateless_responses     INTEGER NOT NULL DEFAULT 0 CHECK (stateless_responses IN (0, 1)),
    allow_private_hosts     INTEGER NOT NULL DEFAULT 0 CHECK (allow_private_hosts IN (0, 1)),
    connect_timeout_ms      INTEGER NOT NULL DEFAULT 10000 CHECK (connect_timeout_ms > 0),
    stream_idle_timeout_ms  INTEGER NOT NULL DEFAULT 300000 CHECK (stream_idle_timeout_ms > 0),
    request_max_retries     INTEGER NOT NULL DEFAULT 0 CHECK (request_max_retries >= 0),
    max_concurrency         INTEGER CHECK (max_concurrency IS NULL OR max_concurrency > 0),
    created_at              INTEGER NOT NULL,
    updated_at              INTEGER NOT NULL
);

CREATE TABLE provider_credentials (
    id                 TEXT PRIMARY KEY,
    provider_id        TEXT NOT NULL REFERENCES custom_providers(id) ON DELETE CASCADE,
    label              TEXT NOT NULL,
    api_key_enc        BLOB NOT NULL,
    enabled            INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    health_status      TEXT NOT NULL DEFAULT 'healthy'
                           CHECK (health_status IN ('healthy', 'cooldown', 'reauth_required', 'disabled')),
    routing_weight     REAL NOT NULL DEFAULT 1.0 CHECK (routing_weight > 0),
    max_concurrency    INTEGER CHECK (max_concurrency IS NULL OR max_concurrency > 0),
    cooldown_until     INTEGER,
    last_error_at      INTEGER,
    created_at         INTEGER NOT NULL,
    updated_at         INTEGER NOT NULL,
    UNIQUE (provider_id, label)
);

CREATE INDEX idx_provider_credentials_provider
    ON provider_credentials(provider_id, enabled, health_status);

CREATE TABLE provider_models (
    id                              TEXT PRIMARY KEY,
    provider_id                     TEXT NOT NULL REFERENCES custom_providers(id) ON DELETE CASCADE,
    public_model                    TEXT NOT NULL UNIQUE,
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
    updated_at                      INTEGER NOT NULL
);

CREATE INDEX idx_provider_models_provider
    ON provider_models(provider_id, enabled);

-- `provider` remains the actual upstream provider dimension. Existing values (`codex`,
-- `anthropic`) remain valid; custom rows store their configured provider slug.
ALTER TABLE request_log ADD COLUMN target_kind TEXT
    CHECK (target_kind IS NULL OR target_kind IN ('account', 'credential'));
ALTER TABLE request_log ADD COLUMN provider_credential_id TEXT;
ALTER TABLE request_log ADD COLUMN upstream_model TEXT;
ALTER TABLE request_log ADD COLUMN upstream_transport TEXT;
ALTER TABLE request_log ADD COLUMN orchestration_input_tokens INTEGER;
ALTER TABLE request_log ADD COLUMN orchestration_output_tokens INTEGER;
ALTER TABLE request_log ADD COLUMN orchestration_cached_input_tokens INTEGER;

CREATE INDEX idx_request_log_provider_credential_time
    ON request_log(provider_credential_id, requested_at DESC);
