# codex-lb → PolyFlare Port Reference (M2)

Distilled from a full read of codex-lb (2026-07-14) for the M2 store/selector/OAuth port. File:line refs are into the codex-lb tree at `../codex-lb`.

## Encryption at rest (M2a)
- codex-lb: `cryptography.fernet.Fernet` (AES-128-CBC + HMAC-SHA256, versioned base64url token) — `app/core/crypto.py` `TokenEncryptor.{encrypt,decrypt}`.
- Key file: raw bytes = a base64url 32-byte Fernet key (`Fernet.generate_key()`). Default path `~/.codex-lb/encryption.key` (`$CODEX_LB_DATA_DIR` overrides home). `chmod 0o600`. Store DB next to it at `~/.codex-lb/store.db`.
- Encrypted fields = exactly 3 columns on `accounts`: `access_token_encrypted`, `refresh_token_encrypted`, `id_token_encrypted` (Fernet ciphertext of the UTF-8 token strings).
- **PolyFlare M2a:** encrypt at rest with **XChaCha20-Poly1305** (24-byte random nonce stored alongside ciphertext), own key file (e.g. `~/.polyflare/key`). The importer (below) reads the *Fernet* key to decrypt once, then re-encrypts XChaCha.

## Accounts schema (M2a) — port these durable columns (codex-lb `accounts`, models.py:69-150)
| col | type | note |
|---|---|---|
| id | TEXT PK | `{account_id}_{sha256(workspace|email)[:8]}` |
| chatgpt_account_id | TEXT? | workspace identity (OAuth claim) |
| chatgpt_user_id | TEXT? | per-seat principal (`sub`/claim) |
| email | TEXT | not unique |
| alias | TEXT? | operator label |
| workspace_id / workspace_label / seat_type | TEXT? | |
| plan_type | TEXT | free,plus,pro,prolite,team,business,enterprise,edu |
| routing_policy | TEXT | normal | burn_first | preserve (default normal) |
| access_token_enc / refresh_token_enc / id_token_enc | BLOB | XChaCha ciphertext (+nonce) |
| last_refresh | DateTime (ISO TEXT — parse to epoch on import) | codex-lb stores SQLite DATETIME text, e.g. `2026-07-12 06:00:41.345107` (with or without fractional seconds), interpreted UTC |
| created_at | DateTime (ISO TEXT — parse to epoch on import) | same DATETIME text form |
| status | TEXT | active,rate_limited,quota_exceeded,paused,reauth_required,deactivated |
| deactivation_reason | TEXT? | |
| reset_at | INTEGER? | durable rate-limit/quota reset epoch (selector reads this) |
| blocked_at | INTEGER? | |
| security_work_authorized | INTEGER(bool) default 0 | TA6 hard pre-filter flag |
- **Not columns (runtime-derived, don't persist in M2):** used_percent, secondary_used_percent, error_count, cooldown_until, last_selected_at, health_tier, capacity_credits, in-flight counts. These are assembled per-request from `usage_history` + an in-memory runtime cache. M2b assembles a minimal snapshot; live health-tier/in-flight tracking is a later refinement.

## usage_history schema (M2a) — codex-lb `usage_history` (models.py:153-167)
`id INTEGER PK`, `account_id FK`, `recorded_at DateTime (ISO TEXT — parse to epoch on import)`, `window TEXT?` (nullable — "primary"/"secondary" or NULL), `used_percent REAL`, `input_tokens INT?`, `output_tokens INT?`, `reset_at INT?`, `window_minutes INT?`, `credits_has/credits_unlimited BOOL?`, `credits_balance REAL?`. Index `(account_id, recorded_at)`.

## OAuth (M2b) — `app/core/auth/`
- **Claims (decode-only, NO signature verify):** split id_token JWT on `.`, base64url-decode payload, json parse. Fields: `email, sub, chatgpt_account_id, chatgpt_user_id, chatgpt_plan_type, workspace_id, workspace_label, seat_type, exp`, plus nested `https://api.openai.com/auth` claim with the same auth-scoped values (`chatgpt_user_id` prefers auth-claim > top-level > sub).
- **Refresh:** `POST {auth_base_url}/oauth/token`, default `auth_base_url=https://auth.openai.com`. Body `{grant_type:"refresh_token", client_id:"app_EMoamEEZ73f0CkXaXp7hrann", refresh_token, scope:"openid profile email"}`. Timeout 8s. Response needs access_token+refresh_token+id_token; re-derive claims from new id_token; update last_refresh + identity fields.
- **should_refresh:** `now - last_refresh > 8 days` (`token_refresh_interval_days`, default 8). Dedup concurrent refreshes per account (singleflight).
- **Permanent failure codes** (→ REAUTH_REQUIRED/DEACTIVATED): refresh_token_expired, refresh_token_reused, refresh_token_invalidated, invalid_grant, token_invalidated, token_expired, app_session_terminated, account_session_expired, account_auth_invalidated, account_deactivated, account_suspended, account_deleted.

## OAuth importer (M2a) — from codex-lb `scripts/migrate_oauth_usage.py`
- Read codex-lb `store.db` (read-only, `file:...?mode=ro`). Copy tables parent-first: accounts, usage_history, additional_usage_history (skip dashboard/rollups/request_logs for PolyFlare M2 — accounts+usage is the MVP need).
- **Column intersection**: only copy columns present in both schemas. **Tokens are the exception**: Fernet-decrypt the 3 token blobs with `~/.codex-lb/encryption.key`, re-encrypt XChaCha into PolyFlare's columns.
- DELETE-before-INSERT for singleton/seeded rows; append for history. Never print token values.
- PolyFlare exposes this as `polyflare accounts import --from <codex-lb store.db> --fernet-key <key file>`.

## Selector algorithm (M2b) — codex-lb `app/core/balancer/logic.py`, DEFAULT strategy = `capacity_weighted`
Operates on a pure `AccountState` snapshot (no I/O). Pipeline:
1. **Eligibility hard-filter** (logic.py:431-548): skip reauth/deactivated/paused; auto-recover rate_limited/quota_exceeded if `now >= reset_at` (zero the used%); clear expired cooldown; error backoff `min(300, 30*2^(error_count-3))` for error_count>=3 (skip while within backoff, reset when expired). Empty pool → optional single-candidate backoff fallback → else descriptive error (retry hint capped 300s).
2. **Health-tier pooling** (logic.py:598-601): `pool = healthy or probing or draining or available` (prefer healthy). Tiers: 0 healthy / 1 draining / 2 probing. `should_drain` if used%>=85 OR secondary%>=90 OR (error_count>=2 within 60s).
3. **Burn/normal/preserve waterfall** (logic.py:602-605): `pool = burn_first or normal or preserve or pool` — drain burn_first accounts first, normal next, preserve last-resort.
4. **capacity_weighted pick** (logic.py:~925): weighted-random `choices(pool, weights=[remaining_secondary_credits(s)])`; if all weights 0 → deterministic `min(pool, key=usage_sort_key)`. `remaining_secondary_credits = max(0, capacity*(1 - min(used%,100)/100))`, `capacity = capacity_credits or PLAN_CAPACITY_CREDITS_SECONDARY[plan]`. Deterministic-probe variant: `min` by `(-remaining_secondary_credits, secondary_used%, primary_used%, last_selected_at, account_id)`.
- **Tie-break:** every deterministic sort key ends in `account_id` (lexicographic).
- **Plan capacity (secondary window):** free 1134, plus/business/team/edu 7560, pro/enterprise 50400, prolite 37800 (primary: free 0, plus/... 225, pro/ent 1500, prolite 1125). `app/core/usage/__init__.py:20-44`.
- **Determinism for tests:** the weighted-random draw takes an injectable RNG/seed so parity tests are reproducible.
- Other strategies (usage_weighted, round_robin, relative_availability, fill_first, sequential_drain, reset_drain, single_account) are additive later — M2b ships `capacity_weighted` (the persisted default) only.
- **TA6 pre-filter:** when a request requires `security_work_authorized`, filter candidates to that flag before scoring (a hard filter, above the scoring — see DESIGN TA6).

## Error/failure transitions (M2b/M3) — logic.py:991-1157
- rate_limit → status=RATE_LIMITED, error_count+=1, reset_at from error `resets_at`/`resets_in_seconds` else cooldown `now+max(delay,30)`.
- quota_exceeded → status=QUOTA_EXCEEDED, used%=100, cooldown `now+120`, reset_at from error else `now+3600`.
- permanent → REAUTH_REQUIRED (if code in permanent set) else DEACTIVATED.
- backoff_seconds(attempt) = `200ms * 2^(attempt-1)` jittered ×U(0.9,1.1).
