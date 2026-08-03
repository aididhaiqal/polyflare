-- admin token, stored as a hash so it can be configured without an environment variable
--
-- Forward-only. The table starts empty, so every existing deployment keeps its current posture:
-- the admin token remains whatever POLYFLARE_ADMIN_TOKEN says, including "unset".
--
-- WHY THIS TABLE EXISTS: POLYFLARE_ADMIN_TOKEN was the only way to configure the token, which
-- means a launchd/systemd-managed server could only get one by having the plaintext secret written
-- into its service definition — a file that is world-readable by default. There was no supported
-- way to set an admin token that did not leave the secret in cleartext on disk.
--
-- Only the SHA-256 of the token is stored. A database reader cannot recover or replay the token,
-- the same rule `api_keys` and `dashboard_sessions` already follow. `token_prefix` is the first few
-- characters, kept so `polyflare admin-token status` can identify WHICH token is installed without
-- being able to reconstruct it.
--
-- Singleton by construction: `CHECK (id = 1)` means there is one admin token or none, never a set
-- of them. Several named credentials are what `api_keys` (callers) and `dashboard_passkeys`
-- (operators) are for; this is the single shared break-glass token, and a table that could hold
-- many would invite treating it as a general credential store.
CREATE TABLE admin_token (
    id           INTEGER PRIMARY KEY CHECK (id = 1),
    -- sha256 hex of the plaintext token. The plaintext is revealed once, at creation, and is never
    -- persisted anywhere.
    token_hash   TEXT    NOT NULL,
    -- First few characters of the plaintext, for display only — far too short to be usable.
    token_prefix TEXT    NOT NULL,
    created_at   INTEGER NOT NULL
);
