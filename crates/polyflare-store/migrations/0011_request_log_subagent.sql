-- Sub-agent role label (x-openai-subagent: review/compact/…) — content-free routing metadata.
ALTER TABLE request_log ADD COLUMN subagent TEXT;
