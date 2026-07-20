-- request_id correlates the stream-end usage UPDATE to its row (see RequestLogRepo::update_usage).
CREATE INDEX IF NOT EXISTS idx_request_log_request_id ON request_log (request_id);
