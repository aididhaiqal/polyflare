-- Native Codex HTTP/SSE requests receive an HTTP 200 before their terminal protocol event.
-- Preserve that transport status while recording the bounded terminal result separately so a
-- response.failed, response.incomplete, client cancellation, or lost upstream stream is not
-- reported as a successful request.
ALTER TABLE request_log ADD COLUMN protocol_outcome TEXT
    CHECK (
        protocol_outcome IS NULL OR protocol_outcome IN (
            'completed',
            'failed',
            'incomplete',
            'cancelled',
            'transport_lost'
        )
    );
