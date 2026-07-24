-- Canonical Responses usage facts. Cached input is a subset of input, and reasoning output is a
-- subset of output; neither may be added to input/output again. `reported_total_tokens` preserves
-- the upstream value independently from PolyFlare's older compatibility `total_tokens` column.
ALTER TABLE request_log ADD COLUMN cache_write_input_tokens INTEGER
    CHECK (cache_write_input_tokens IS NULL OR cache_write_input_tokens >= 0);
ALTER TABLE request_log ADD COLUMN reported_total_tokens INTEGER
    CHECK (reported_total_tokens IS NULL OR reported_total_tokens >= 0);
ALTER TABLE request_log ADD COLUMN usage_schema TEXT
    CHECK (usage_schema IS NULL OR usage_schema IN ('openai_responses_v1', 'legacy_unknown'));
ALTER TABLE request_log ADD COLUMN usage_source TEXT
    CHECK (usage_source IS NULL OR usage_source IN (
        'upstream_response', 'codex_lb_import', 'polyflare_legacy'
    ));
ALTER TABLE request_log ADD COLUMN usage_status TEXT
    CHECK (usage_status IS NULL OR usage_status IN ('final', 'legacy'));

-- Historical rows retain their observed component counts, but we cannot truthfully reconstruct
-- upstream `total_tokens`, cache-write usage, or terminal finality. Classify the evidence instead
-- of fabricating those values.
UPDATE request_log
SET usage_schema = 'legacy_unknown',
    usage_source = CASE
        WHEN import_source_id IS NOT NULL THEN 'codex_lb_import'
        ELSE 'polyflare_legacy'
    END,
    usage_status = 'legacy'
WHERE usage_status IS NULL
  AND (
      input_tokens IS NOT NULL
      OR output_tokens IS NOT NULL
      OR cached_input_tokens IS NOT NULL
      OR reasoning_tokens IS NOT NULL
      OR total_tokens IS NOT NULL
      OR cached_tokens IS NOT NULL
      OR orchestration_input_tokens IS NOT NULL
      OR orchestration_output_tokens IS NOT NULL
      OR orchestration_cached_input_tokens IS NOT NULL
  );
