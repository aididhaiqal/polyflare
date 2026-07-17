-- Content-free per-request metrics for the dashboard's Requests view + Overview KPIs.
-- Every column here is an outcome/metric/identifier — NEVER conversation content.
--
-- NOTE: `account_id`, `model`, `reasoning_effort`, `service_tier`, and `transport` are NOT added
-- here — migration 0005 (`request_log_history`) already added them to this table (they exist in
-- the DB schema today, just not yet on the Rust `RequestLogRecord`/`RequestLogRow` structs; this
-- task's job is wiring those existing columns into the structs/INSERT/SELECT, not re-adding them
-- — a duplicate `ALTER TABLE ADD COLUMN` for an existing column fails migration at startup). Only
-- the three genuinely new columns are added below; note their deliberately distinct names from
-- 0005's near-neighbors (`latency_first_token_ms`, `input_tokens`/`output_tokens`,
-- `cached_input_tokens`) — those are codex-lb-import-shaped columns, these are PolyFlare's own
-- native metric names for the same concepts.
ALTER TABLE request_log ADD COLUMN ttft_ms          INTEGER;
ALTER TABLE request_log ADD COLUMN total_tokens     INTEGER;
ALTER TABLE request_log ADD COLUMN cached_tokens    INTEGER;
