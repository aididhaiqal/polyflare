# Per-account usage ceiling (stop at N%, admin override)

**Goal:** an operator can say "stop routing to this account once it reaches 50% of its quota" and
have the selector honour it, with an explicit admin override to keep burning it past the ceiling.

**Why:** today an account is only gated when the upstream declares it exhausted (`rate_limited` /
`quota_exceeded` at 100%). There is no way to *reserve* capacity — to keep an account's remaining
quota for later work, or to spread a fleet evenly by capping each member.

**Shape:** one migration, two account columns, one eligibility rule, one API surface. No change to
selection ordering, weighting, or any existing gate.

### Outcome 1: durable per-account configuration

- Migration `20260725235133_account_usage_ceiling`: `accounts.usage_cap_percent REAL NULL` (NULL ⇒ uncapped, the existing
  behaviour) with `CHECK (usage_cap_percent IS NULL OR (usage_cap_percent > 0 AND
  usage_cap_percent <= 100))`, and `accounts.usage_cap_override INTEGER NOT NULL DEFAULT 0
  CHECK (usage_cap_override IN (0,1))`. Additive; every existing row keeps today's behaviour.
- `Account` + `AccountRepo`: read both, plus a setter used by the API.
- Verify: store round-trip test (set, read back, NULL default).

### Outcome 2: the eligibility rule

- `AccountSnapshot` carries `usage_cap_percent: Option<f64>` and `usage_cap_override: bool`;
  `assemble_snapshots` populates them.
- `classify_eligibility` (core `select.rs`), evaluated AFTER the existing recovery arms so it reads
  post-recovery `eff_used` / `eff_secondary_used`: when a cap is set, the override is off, and
  EITHER window is at or above the cap, the account is not selectable. It is reported as
  `InBackoff { recover_at: <that window's reset>, kind: Cooldown }` when a reset epoch is known —
  the same shape `quota_exceeded` uses, so the existing starvation/wait machinery treats a ceiling
  exactly like a lower quota — and `HardBlocked` when no reset is known (nothing to wait for).
- The ceiling is a ROUTING gate only: it never benches the account, never writes health state, and
  a pinned continuation owner that is already mid-conversation is unaffected by design (ownership
  resolution is a separate path — documented, not changed here).
- Verify: unit tests — capped account excluded while an uncapped peer is chosen; override restores
  it; below-cap unaffected; cap compared against BOTH windows; `HardBlocked` without a reset.

### Outcome 3: operator control

- `PATCH /api/accounts/{id}` accepts `usage_cap_percent` (number or null) and
  `usage_cap_override` (bool), validated against the same bounds as the CHECK constraint, admin-
  gated exactly like the existing account mutations.
- Verify: API test for set/clear/override + rejection of out-of-range values.

### Out of scope (follow-ups)

Dashboard controls; a global default ceiling; automatic override expiry. The column is per-account
and NULL-defaulted, so a global default can be layered later without a schema change.

### Gate

`cargo test --workspace` + clippy clean; deploy via the AGENTS.md pipeline (idle-gated restart);
then verify live by capping a real account below its current usage and confirming the selector
stops choosing it while the dashboard still shows it healthy.
