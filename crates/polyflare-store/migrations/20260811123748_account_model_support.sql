-- per-account support for models the upstream /models endpoint does not enumerate
--
-- Forward-only. Starts empty, so every existing deployment behaves exactly as before: selection
-- keeps filtering by the live per-account /models cache alone, and this table contributes nothing
-- until a row is written.
--
-- WHY THIS EXISTS: some models are real and usable on an account but never appear in that account's
-- /models list — hidden previews (e.g. gpt-daybreak-blue-latest) gated to specific seats. The
-- in-memory catalog is built entirely from /models, so it can neither advertise such a model nor
-- know which accounts can serve it. A request for it therefore fails OPEN to every account,
-- including ones that would silently fall back to a different model. This table records, per
-- account, whether a named model is supported — the one fact /models cannot provide.
--
-- `source` distinguishes how the row was established: 'probe' (PolyFlare attempted the model and
-- observed accept/reject) or 'operator' (a human declared it). An operator row is authoritative and
-- must never be overwritten by a later probe — the probe signal is imperfect (an account may accept
-- a request and silently serve a fallback), which is exactly why a human override outranks it.
CREATE TABLE account_model_support (
    account_id TEXT    NOT NULL,
    -- The model string as a client requests it, e.g. 'gpt-daybreak-blue-latest'.
    model      TEXT    NOT NULL,
    -- 1 = this account can serve this model; 0 = it cannot. Both are meaningful: a 0 row lets an
    -- operator explicitly exclude an account the probe wrongly marked supported.
    supported  INTEGER NOT NULL,
    -- 'probe' | 'operator'. See the table comment for the precedence rule.
    source     TEXT    NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (account_id, model)
);

-- Selection asks "which accounts support model X"; the dashboard asks "what does account A support".
-- Index the model direction; the primary key already serves the account direction.
CREATE INDEX account_model_support_by_model ON account_model_support(model);
