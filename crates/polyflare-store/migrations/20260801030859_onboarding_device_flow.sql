-- onboarding device flow
--
-- Forward-only. Adds the device-code login method to dashboard onboarding flows: a remote browser
-- can never receive the fixed localhost:1455 OAuth redirect, so the device flow (enter a short
-- code at the auth server's verification page; the SERVER polls for approval) is the only method
-- that works from another machine. `method` defaults to 'browser' so every pre-existing row keeps
-- its meaning.
--
-- `device_auth_id`/`user_code` are stored as plaintext like codex-lb does: together they only
-- allow polling for an approval the account owner must still grant in their own browser, and any
-- reader of this database already holds the (encrypted-at-rest) account tokens themselves. The
-- flow's existing `expires_at` bounds the device code's lifetime.
ALTER TABLE account_onboarding_flows ADD COLUMN method TEXT NOT NULL DEFAULT 'browser';
ALTER TABLE account_onboarding_flows ADD COLUMN device_auth_id TEXT;
ALTER TABLE account_onboarding_flows ADD COLUMN user_code TEXT;
ALTER TABLE account_onboarding_flows ADD COLUMN interval_seconds INTEGER;
