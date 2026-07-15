-- PolyFlare: named account pools. Forward-only, nullable, no default. An account with
-- `pool = NULL` is UNPOOLED — reachable only via the bare ingress paths (`/responses`,
-- `/v1/messages`), which select over ALL accounts. A non-null `pool` tags the account to a named
-- slug, reachable via the pooled paths (`/{pool}/responses`, `/{pool}/v1/messages`) in addition to
-- the bare paths. Existing rows stay NULL (unpooled), so pre-pool routing is unchanged.

ALTER TABLE accounts ADD COLUMN pool TEXT;
