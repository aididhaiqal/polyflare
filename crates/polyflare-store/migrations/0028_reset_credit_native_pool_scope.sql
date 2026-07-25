-- Preserve the originating native API scope independently of mutable account membership.
-- Empty string is the root scope, a non-empty value is the named pool slug, and NULL is reserved
-- for requests created before this migration so their existing membership checks remain in force.
ALTER TABLE reset_credit_native_requests ADD COLUMN pool_scope TEXT;
