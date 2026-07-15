-- Widen request_log to hold the CONTENT-SAFE subset of codex-lb's request_logs columns, so the
-- codex-lb -> PolyFlare cutover can migrate the chat-log history into it (see the request_logs
-- copy path in import.rs). These columns are populated by the importer today and by PolyFlare's
-- own request path as each corresponding feature lands (tokens/cost/session threading, etc.); they
-- are all nullable, so a native request that doesn't yet capture them just leaves them NULL.
--
-- Deliberately NOT carried from codex-lb (content safety / PII — see import.rs): useragent,
-- useragent_group, client_ip, error_message, failure_detail. Bounded metadata only.
--
-- Naming note: codex-lb's `status` is a TEXT outcome ('success'/'error') — it lands in `outcome`
-- here, because PolyFlare's own `status` column (added in 0004) is the INTEGER HTTP status code.
-- codex-lb's `latency_ms` maps onto PolyFlare's existing `duration_ms` (same concept), so it is not
-- re-added.

ALTER TABLE request_log ADD COLUMN account_id             TEXT;
ALTER TABLE request_log ADD COLUMN session_id             TEXT;
ALTER TABLE request_log ADD COLUMN request_id             TEXT;
ALTER TABLE request_log ADD COLUMN model                  TEXT;
ALTER TABLE request_log ADD COLUMN plan_type              TEXT;
ALTER TABLE request_log ADD COLUMN source                 TEXT;
ALTER TABLE request_log ADD COLUMN request_kind           TEXT;
ALTER TABLE request_log ADD COLUMN outcome                TEXT;   -- codex-lb `status`: success|error
ALTER TABLE request_log ADD COLUMN error_code             TEXT;
ALTER TABLE request_log ADD COLUMN input_tokens           INTEGER;
ALTER TABLE request_log ADD COLUMN output_tokens          INTEGER;
ALTER TABLE request_log ADD COLUMN cached_input_tokens    INTEGER;
ALTER TABLE request_log ADD COLUMN reasoning_tokens       INTEGER;
ALTER TABLE request_log ADD COLUMN cost_usd               REAL;
ALTER TABLE request_log ADD COLUMN reasoning_effort       TEXT;
ALTER TABLE request_log ADD COLUMN latency_first_token_ms INTEGER;
ALTER TABLE request_log ADD COLUMN service_tier           TEXT;
ALTER TABLE request_log ADD COLUMN requested_service_tier TEXT;
ALTER TABLE request_log ADD COLUMN actual_service_tier    TEXT;
ALTER TABLE request_log ADD COLUMN transport              TEXT;
ALTER TABLE request_log ADD COLUMN deleted_at             INTEGER; -- soft-delete tombstone (epoch)
-- The source row's codex-lb request_logs.id, kept ONLY as a dedup key so re-importing the same
-- codex-lb DB is idempotent (NULL for PolyFlare-native rows).
ALTER TABLE request_log ADD COLUMN import_source_id       INTEGER;

-- Idempotent re-import: two migrated rows can't share a source id; native rows (NULL) are exempt.
CREATE UNIQUE INDEX IF NOT EXISTS idx_request_log_import_source
    ON request_log (import_source_id) WHERE import_source_id IS NOT NULL;
-- Per-account history (the dashboard's most common filter).
CREATE INDEX IF NOT EXISTS idx_request_log_account_time
    ON request_log (account_id, requested_at);
