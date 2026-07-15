-- PolyFlare request_log: one row per request OUTCOME, written fire-and-forget at completion.
-- This is the persisted, queryable backend for the (post-MVP) dashboard's request-history view.
--
-- It is the FOUNDATION of what grows into codex-lb's full request_log column set: codex-lb reached
-- ~53 columns across ~25 migrations, one column-set per feature as each shipped (quota/pricing,
-- upstream-proxy routing, prewarm, model-sources, dashboard API keys, TTFT phase splits). PolyFlare
-- will have those features too, and each brings its own request_log columns WHEN it lands — never a
-- column ahead of the code that writes it. This first cut carries only the content-safe facts
-- PolyFlare captures at completion today.
--
-- CONTENT SAFETY (mirrors crates/polyflare-server/src/observability.rs): NO conversation content and
-- NO free-form request-derived strings ever. Every column is a PolyFlare-generated bounded value
-- (provider/method/path/status), a number, or a unix-epoch-seconds timestamp. Columns that would
-- carry request-derived free-form text (raw model string, client IP, User-Agent, upstream error
-- text) are deliberately excluded and gated on an explicit content-safety decision before they land
-- (e.g. `model` stored only as the post-alias canonical id, never the raw client string).
CREATE TABLE IF NOT EXISTS request_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    requested_at INTEGER NOT NULL,          -- unix-epoch seconds (request completion time)
    provider     TEXT    NOT NULL,          -- 'codex' | 'anthropic' (bounded discriminator)
    method       TEXT    NOT NULL,          -- HTTP method (e.g. 'POST')
    path         TEXT    NOT NULL,          -- ingress path: '/responses' | '/v1/messages'
    aliased      INTEGER NOT NULL,          -- 0|1: was the client model alias-mapped
    status       INTEGER NOT NULL,          -- client-facing HTTP status code
    duration_ms  INTEGER NOT NULL           -- total request duration
);

-- Newest-first list (the default dashboard query): order by time, tiebreak on the surrogate id.
CREATE INDEX IF NOT EXISTS idx_request_log_requested_at ON request_log (requested_at DESC, id DESC);
-- Per-provider history / breakdowns.
CREATE INDEX IF NOT EXISTS idx_request_log_provider_time ON request_log (provider, requested_at);
