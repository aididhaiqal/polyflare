-- Provider-neutral upstream identity + explicit upstream-auth mode on accounts.
--
-- Before this migration the only durable upstream identity was `chatgpt_account_id`, which is
-- Codex-shaped. Anthropic subscription-OAuth accounts need the same "which upstream seat is this
-- row" concept without borrowing a ChatGPT column, so re-login targets an existing row instead of
-- inserting a duplicate. `chatgpt_account_id` is preserved untouched as the Codex companion-header
-- value; the neutral column is a COPY, not a replacement.
--
-- `auth_mode` makes the credential contract explicit rather than inferred from `provider`:
-- Anthropic rows may hold either a subscription-OAuth grant (refreshable, Claude-native egress
-- only) or a static API key, and those must never be treated alike by the refresh or egress paths.
ALTER TABLE accounts ADD COLUMN upstream_identity TEXT;

ALTER TABLE accounts ADD COLUMN auth_mode TEXT NOT NULL DEFAULT 'codex_oauth'
    CHECK (auth_mode IN ('codex_oauth', 'anthropic_oauth', 'static_bearer'));

-- Raw access-token expiry (unix seconds) as reported by the token endpoint. Non-secret: it is a
-- timestamp, not a credential. Codex reads expiry from its JWT `exp`, but an Anthropic access token
-- is opaque, so the only truthful expiry is the one the exchange/refresh response carried.
ALTER TABLE accounts ADD COLUMN access_token_expires_at INTEGER
    CHECK (access_token_expires_at IS NULL OR access_token_expires_at > 0);

-- Which reviewed OAuth-contract profile (endpoints, client id, scopes) this account was onboarded
-- under. Deliberately separate from the per-request Claude wire profile: a new Claude Code release
-- changes the wire profile without rewriting account rows.
ALTER TABLE accounts ADD COLUMN oauth_contract_version TEXT;

-- Space-separated scopes actually granted by the authorization server. Refresh sends exactly these
-- back; it never widens the grant.
ALTER TABLE accounts ADD COLUMN granted_scopes TEXT;

-- Backfill: every pre-existing row keeps its current credential behavior. Codex rows are OAuth (the
-- column default); pre-existing Anthropic rows were static bearers, since subscription OAuth did
-- not exist for them.
UPDATE accounts SET auth_mode = 'static_bearer' WHERE provider <> 'codex';

-- Backfill the neutral identity from the Codex-shaped one, but ONLY where it is unambiguous.
-- A duplicate `chatgpt_account_id` (possible historically: no unique constraint ever enforced it)
-- would make the partial unique index below fail and wedge startup for every operator carrying
-- one. Conflicted rows are instead left with a NULL neutral identity: they keep working exactly as
-- before, and re-login simply cannot auto-target them until an operator resolves the duplicate.
-- Silently merging or deleting an account row is never acceptable here.
UPDATE accounts
SET upstream_identity = chatgpt_account_id
WHERE provider = 'codex'
  AND chatgpt_account_id IS NOT NULL
  AND chatgpt_account_id IN (
      SELECT chatgpt_account_id
      FROM accounts
      WHERE provider = 'codex' AND chatgpt_account_id IS NOT NULL
      GROUP BY chatgpt_account_id
      HAVING COUNT(*) = 1
  );

-- `(provider, upstream_identity)` identifies a re-login target. Partial so that legacy rows with no
-- neutral identity (never onboarded through OAuth, or left unset by the duplicate guard above) do
-- not all collide on NULL.
CREATE UNIQUE INDEX accounts_provider_upstream_identity_idx
    ON accounts(provider, upstream_identity)
    WHERE upstream_identity IS NOT NULL;

-- The onboarding-flow table was pinned to Codex by `CHECK (provider = 'codex')`. That constraint
-- cannot be widened in place, and the 0024 rename-and-replace trick does not work here: the old
-- column is NOT NULL, so every future INSERT would still have to supply a value its own CHECK
-- rejects for Anthropic. This needs the full table rebuild.
--
-- Rebuilding is safe for this table specifically: it OWNS a foreign key into `accounts`, but no
-- other table references it, so dropping it leaves no dangling reference. Rows carry forward
-- unchanged; `redirect_uri` is NULL for every existing (Codex) flow, which is correct — Codex uses
-- a fixed registered redirect and never needed to record one.
CREATE TABLE account_onboarding_flows_new (
    id TEXT PRIMARY KEY,
    flow_provider TEXT NOT NULL CHECK (flow_provider IN ('codex', 'anthropic')),
    oauth_state TEXT NOT NULL UNIQUE,
    verifier_enc BLOB NOT NULL,
    initial_pool TEXT,
    status TEXT NOT NULL CHECK (status IN ('pending', 'exchanging', 'completed', 'failed')),
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    finished_at INTEGER,
    account_id TEXT,
    error_code TEXT,
    -- The exact redirect_uri sent on the authorize request. An Anthropic loopback flow binds an
    -- OS-assigned port, so this cannot be reconstructed at exchange time and must be persisted.
    redirect_uri TEXT,
    FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE SET NULL
);

INSERT INTO account_onboarding_flows_new
    (id, flow_provider, oauth_state, verifier_enc, initial_pool, status, created_at, expires_at,
     finished_at, account_id, error_code, redirect_uri)
SELECT id, provider, oauth_state, verifier_enc, initial_pool, status, created_at, expires_at,
       finished_at, account_id, error_code, NULL
FROM account_onboarding_flows;

DROP TABLE account_onboarding_flows;
ALTER TABLE account_onboarding_flows_new RENAME TO account_onboarding_flows;

CREATE INDEX account_onboarding_flows_expires_idx
    ON account_onboarding_flows(expires_at);
