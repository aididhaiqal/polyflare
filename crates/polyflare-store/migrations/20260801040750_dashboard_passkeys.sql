-- dashboard passkeys
--
-- Forward-only. Adds passkey (WebAuthn) sign-in for the dashboard so the management surface can be
-- authenticated without a shared bearer token typed by hand. Both tables start empty, so every
-- existing deployment keeps its current posture until an operator registers a first passkey.
--
-- WHY THIS MATTERS BEYOND CONVENIENCE: with no passkey and no POLYFLARE_ADMIN_TOKEN, a loopback
-- bind opens every /api route unauthenticated, so any local process can reach them — including the
-- credential export. Registering a passkey is what lets that bypass be switched off (see
-- crate::auth), which is why the sign-in path had to exist before the bypass could close.
--
-- `credential_json` holds webauthn-rs's serialized Passkey: the credential id, the COSE PUBLIC key,
-- and the signature counter. It is public-key material — a passkey's private key never leaves the
-- authenticator, so unlike the accounts table there is nothing here to encrypt at rest.
CREATE TABLE dashboard_passkeys (
    id TEXT PRIMARY KEY,
    -- Base64url credential id, unique so the same authenticator cannot register twice.
    credential_id TEXT NOT NULL UNIQUE,
    -- webauthn-rs `Passkey` as JSON (public key + signature counter; no private material).
    credential_json TEXT NOT NULL,
    -- Operator-facing name, e.g. "MacBook Touch ID".
    label TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    last_used_at INTEGER
);

-- Sessions minted after a successful passkey assertion. Only the SHA-256 of the session token is
-- stored: a database reader must not be able to replay a live dashboard session, the same reason
-- api_keys stores hashes rather than raw keys.
CREATE TABLE dashboard_sessions (
    token_hash TEXT PRIMARY KEY,
    passkey_id TEXT,
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    FOREIGN KEY (passkey_id) REFERENCES dashboard_passkeys(id) ON DELETE CASCADE
);

CREATE INDEX dashboard_sessions_expires_idx ON dashboard_sessions(expires_at);
