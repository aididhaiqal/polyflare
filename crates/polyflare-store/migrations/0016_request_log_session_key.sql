-- Content-free join from request history/live telemetry to continuity_sessions.
-- The value is PolyFlare's existing one-way SHA-256 SessionKey, never a raw client header.
ALTER TABLE request_log ADD COLUMN session_key TEXT;

-- `session_id` was imported before the privacy-safe continuity hash existed. It cannot be
-- reconstructed into PolyFlare's SessionKey because the original WS key also includes thread and
-- window identity, so legacy rows deliberately remain unlinked and the raw identifier is erased.
UPDATE request_log SET session_id = NULL WHERE session_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_request_log_session_key_requested_at
    ON request_log (session_key, requested_at DESC);
