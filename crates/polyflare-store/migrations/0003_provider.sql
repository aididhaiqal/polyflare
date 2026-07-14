-- PolyFlare M4a: provider discriminator on accounts. Forward-only; existing (pre-M4a) rows
-- default to 'codex' since every account created before this migration is Codex-shaped. New
-- 'anthropic' rows populate the neutral columns (id, email, plan_type, routing_policy, the three
-- token columns, security_work_authorized) and leave the already-nullable Codex-only columns
-- (chatgpt_account_id, chatgpt_user_id, workspace_id, workspace_label, seat_type) NULL.

ALTER TABLE accounts ADD COLUMN provider TEXT NOT NULL DEFAULT 'codex';
