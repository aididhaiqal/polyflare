# C12 — Data-Retention Pruning Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use `- [ ]`.

**Goal:** Stop the SQLite DB growing unbounded on a long-running proxy: a background pruner that age-deletes old
rows from the two append-only, per-event tables — `request_log` (one row per request) and `usage_history` (one row
per usage poll) — in batches, disabled by default, with codex-lb's "protect the latest usage row per account+window"
guard so an idle account never loses the sample the routing gate depends on.

**Architecture:** Two new repo methods (`RequestLogRepo::prune_older_than`, `AccountRepo::prune_usage_history_older_than`)
doing batched `DELETE ... WHERE <ts> < cutoff LIMIT batch` loops, + a `spawn_retention_prune(Arc<AppState>)` background
task mirroring `spawn_usage_refresh` (hourly), + `POLYFLARE_*_RETENTION_DAYS` config (0 = disabled, the default). NO
migration (both timestamp columns exist + are indexed). NEVER touches `accounts`/`api_keys`/`continuity_*`.

**Authority — the C12 scoping study + codex-lb ground truth (this session), file:line cites:**
- Requirement `docs/PORTING-CODEXLB.md:224-230` (C12, LOW/medium): hourly pruner over `request_log` + `usage_history`;
  "always protect the latest row per `(account_id, window)` via `max(recorded_at) GROUP BY`"; batch-delete (10k);
  single-node drops the leader gate. **"codex-lb prunes only these two tables — never a continuity table."**
  codex-lb ref `core/retention/job.py`,`scheduler.py`,`settings.py:236`.
- codex-lb mechanism (verified): age-based, per-table days (`request_log` floor ≥30, `usage_history` ≥45,
  `settings.py:319-320`; disabled by default = 0). `usage_history` guard = protect each identity's latest row per
  `(account_id, window)` via `MAX(recorded_at) GROUP BY`, materialized once per pass, regardless of age
  (`job.py:132-196`). `BATCH_SIZE = 10_000` per DELETE, each its own txn, loop until a batch < 10k (`job.py:21,99-129`).
  Hourly `asyncio.Task` (`RETENTION_INTERVAL_SECONDS=3600`). **VACUUM never called.** The `request_logs` fold-watermark
  guard is codex-lb-rollup-specific → NO PolyFlare analog (PolyFlare has no rollup/fold), so `request_log` is a
  straight age cutoff.
- PolyFlare schema (`crates/polyflare-store/migrations/*.sql`): `request_log` (0004:17-29) — per-request insert
  (`request_log_repo.rs:150`), `requested_at` INTEGER indexed (`idx_request_log_requested_at` 0004:29). `usage_history`
  (0001:31-47) — per-poll insert (`AccountRepo::insert_usage_window` `account.rs:357`), `recorded_at` indexed
  (`idx_usage_history_account_recorded`), columns include `account_id` + a window discriminator (READ 0001:31-44 for
  the exact column names — likely `window`/`limit_window_seconds`; use the real column that distinguishes primary vs
  secondary window). NEVER-PRUNE: `accounts` (0001), `api_keys` (0009) — identity/auth. DEFER: `continuity_sessions`/
  `continuity_anchors` (active-session ownership risk; codex-lb precedent).
- Repo idiom: `struct XRepo { pool: SqlitePool }`, `pub fn new(pool)`, methods `async fn ... -> Result<_, StoreError>`
  over `&self.pool`; `Store::request_log()`/`Store::accounts()` build a fresh repo per call (`store.rs:99-122`). No
  existing delete method anywhere (grep-confirmed) — C12 adds the first.
- Background-task idiom: `spawn_usage_refresh(state: Arc<AppState>)` (`usage_refresh.rs:279`) = `tokio::spawn(async move
  { loop { ...; sleep(REFRESH_INTERVAL).await } })`, reaches store via `state.store`, called once from `main.rs:237`
  fire-and-forget. Config idiom: `xxx_from_env() -> T` reading `POLYFLARE_...`, parse/clamp/default, called ONCE in
  `ServeConfig` ctor, stored as a field (never per-request env read); `0` = disable lever convention.
- Store is WAL + `busy_timeout(5s)` + 5-conn pool (`store.rs:48,62-67`) — concurrent reads safe during a prune; each
  batched DELETE takes the single writer lock briefly (batching keeps it short); the usage_refresh loop already writes
  from a background task to the same pool with no contention issue = pattern proven.

## Global Constraints

- **PRUNE ONLY `request_log` + `usage_history` (inviolable).** NEVER add a delete for `accounts`, `api_keys`,
  `continuity_sessions`, or `continuity_anchors`. Deleting an account/key breaks identity/auth; deleting a continuity
  row breaks in-flight conversation ownership. The pruner MUST be structurally incapable of touching those tables
  (it only calls the two prune methods on the two log tables).
- **The `usage_history` protect-latest-row guard (inviolable correctness).** `prune_usage_history_older_than` MUST
  NEVER delete the most-recent row per `(account_id, window)` even if it's older than the cutoff — the routing gate
  (`derive_gate`/`latest_usage`) and dashboard depend on each account's last-known window sample. Implement via a
  `NOT IN (SELECT ... MAX(recorded_at) GROUP BY account_id, <window>)` (or an anti-join) in the DELETE. A test MUST
  prove an idle account whose only rows are all older than the cutoff KEEPS its latest row per window.
- **Age cutoff + batched, bounded deletes.** Delete `WHERE <ts> < cutoff` in batches (e.g. `LIMIT 10_000`, loop until
  a batch deletes < batch_size), each batch its own statement — never one unbounded DELETE (would hold the SQLite
  writer lock too long). `cutoff = now - retention_days*86400`.
- **Disabled by default; disable lever.** `POLYFLARE_REQUEST_LOG_RETENTION_DAYS` + `POLYFLARE_USAGE_HISTORY_RETENTION_DAYS`,
  both default `0` = disabled (no pruning — today's behavior). A positive value enables. Clamp to a sane range (e.g.
  `[1, 3650]`); malformed ⇒ 0 (disabled, fail-safe — do NOT accidentally enable aggressive pruning on a typo).
- **No migration, no VACUUM.** Both timestamp columns exist + indexed. VACUUM is NOT part of this (codex-lb doesn't;
  SQLite reuses freed pages — sufficient for bounded growth). Document VACUUM as a non-goal.
- **Content-safety:** pruning logs counts only (rows deleted per table) — content-free. NEVER log row contents.
- **Read-only w.r.t. routing/streaming.** The pruner is a background task; it touches no selection/failover/wedge
  code. The 5 wedge/cyber/failover/starvation suites MUST stay green.
- Verify each task: `cargo test --workspace` green + `cargo clippy --workspace --all-targets -- -D warnings` +
  `cargo fmt --all -- --check` clean (run `cargo fmt` before committing — keep the workspace fmt-clean).

---

### Task 1: `RequestLogRepo::prune_older_than` (batched age delete)

**Files:** `crates/polyflare-store/src/request_log_repo.rs` (new method); tests in the same file (mirror the existing
`#[cfg(test)]` seed-and-assert idiom).

**Interfaces — Produces:**
```rust
// on RequestLogRepo:
/// Delete request_log rows with requested_at < cutoff, in batches of `batch_size`, looping until a
/// batch deletes fewer than batch_size. Returns the total rows deleted. Content-free.
pub async fn prune_older_than(&self, cutoff: i64, batch_size: i64) -> Result<u64, StoreError>;
```
SQLite batched delete: `DELETE FROM request_log WHERE requested_at < ?1 AND rowid IN (SELECT rowid FROM request_log
WHERE requested_at < ?1 LIMIT ?2)` in a loop until `rows_affected < batch_size`. (Use `rowid` subselect since SQLite
`DELETE ... LIMIT` needs the `SQLITE_ENABLE_UPDATE_DELETE_LIMIT` compile flag which sqlx's bundled SQLite may lack —
the `rowid IN (SELECT ... LIMIT)` form is portable.)

- [ ] **Step 1:** Failing tests: (a) seed rows at several `requested_at` timestamps; `prune_older_than(cutoff, 100)`
      deletes ONLY rows with `requested_at < cutoff`, leaves `>= cutoff` intact, returns the right count. (b) batching:
      seed > batch_size old rows, `prune_older_than(cutoff, 2)` deletes all of them across multiple internal batches
      (returns total). (c) cutoff in the future ⇒ deletes all; cutoff before all rows ⇒ deletes none, returns 0.
- [ ] **Step 2:** Run — fail. **Step 3:** Implement the batched loop. **Step 4:** Green.
- [ ] **Step 5:** Commit: `feat(store): RequestLogRepo::prune_older_than (batched age delete)`

---

### Task 2: `AccountRepo::prune_usage_history_older_than` + protect-latest-row guard (THE CRUX — adversarial review)

**Files:** `crates/polyflare-store/src/account.rs` (new method, colocated with `insert_usage_window`); tests.

**Read first:** the `usage_history` schema (`migrations/0001_accounts_and_usage.sql:31-47`) — the EXACT columns:
`account_id`, `recorded_at`, and the window discriminator (the column distinguishing primary/secondary window — read
it, e.g. `window` or `limit_window_seconds`). `insert_usage_window`/`latest_usage`/`usage_history_since` (`account.rs`)
show how the table is written/read — mirror the column names EXACTLY.

**Interfaces — Produces:**
```rust
// on AccountRepo:
/// Delete usage_history rows older than cutoff, EXCEPT the latest row per (account_id, <window>) —
/// which is always kept regardless of age (the routing gate + dashboard need each account's last
/// known sample per window). Batched. Returns rows deleted. Content-free.
pub async fn prune_usage_history_older_than(&self, cutoff: i64, batch_size: i64) -> Result<u64, StoreError>;
```
The protect-latest guard: never delete a row whose `recorded_at` is the `MAX(recorded_at)` for its
`(account_id, <window>)` group. SQL shape:
```sql
DELETE FROM usage_history
WHERE recorded_at < ?cutoff
  AND rowid IN (
    SELECT uh.rowid FROM usage_history uh
    WHERE uh.recorded_at < ?cutoff
      AND uh.recorded_at < (SELECT MAX(m.recorded_at) FROM usage_history m
                            WHERE m.account_id = uh.account_id AND m.<window> = uh.<window>)
    LIMIT ?batch)
```
i.e. a row is deletable only if it's below cutoff AND there exists a strictly-newer row in the same
`(account_id, window)` group (so the group's max is never in the deletable set). Verify the `<window>` column matches
the real schema.

- [ ] **Step 1:** Failing tests (the guard is the point): (a) an account+window with 3 rows (old/old/newest, ALL <
      cutoff) ⇒ prune keeps EXACTLY the newest, deletes the 2 older (proves an idle account keeps its latest sample
      even though it's older than the cutoff). (b) two windows for one account (primary+secondary) ⇒ each window's
      latest is protected independently. (c) rows `>= cutoff` are never touched. (d) a group with a single row (all
      < cutoff) ⇒ that row is KEPT (it's the max). (e) batching across many deletable rows returns the right total.
      (f) two accounts ⇒ each account's per-window latest protected independently.
- [ ] **Step 2:** Run — fail. **Step 3:** Implement with the protect-latest anti-join, batched. **Step 4:** Green.
- [ ] **Step 5:** Commit: `feat(store): prune_usage_history_older_than with protect-latest-row-per-window guard`

---

### Task 3: Config + `spawn_retention_prune` background loop + wiring + e2e

**Files:** `crates/polyflare-server/src/config.rs` (`request_log_retention_days` + `usage_history_retention_days`
fields + `_from_env` fns, default 0), a new `crates/polyflare-server/src/retention.rs` (or extend usage_refresh's
module) with `spawn_retention_prune(state: Arc<AppState>)`, `crates/polyflare-server/src/main.rs` (call it next to
`spawn_usage_refresh`), `lib.rs` (module decl if new); tests + e2e.

**Interfaces — Consumes:** Task 1+2's prune methods. **Produces:** an hourly background task that, per tick, if a
table's retention_days > 0, computes `cutoff = now - days*86400` and calls the table's prune method; no-ops when 0.

- `config.rs`: `request_log_retention_days: u32` + `usage_history_retention_days: u32` on `ServeConfig`, from
  `POLYFLARE_REQUEST_LOG_RETENTION_DAYS` / `POLYFLARE_USAGE_HISTORY_RETENTION_DAYS` (unset/malformed ⇒ 0, clamp
  `[0, 3650]`). Thread onto `AppState` (or read from a ServeConfig held on AppState — mirror how
  `starvation_wait_budget`/`stream_idle_timeout` are held).
- `spawn_retention_prune`: `tokio::spawn` loop, interval `const RETENTION_INTERVAL: Duration = from_secs(3600)`. Each
  tick: read the two retention_days off state; for each > 0, `cutoff = unix_now() - days*86400`,
  `state.store.request_log().prune_older_than(cutoff, 10_000)` / `state.store.accounts().prune_usage_history_older_than(
  cutoff, 10_000)`; a content-free `tracing::info!(deleted = n, table = "request_log")` on a non-zero prune; both 0 ⇒
  the whole task can still tick harmlessly (or skip-spawn if both 0 at startup — either; document). On a prune error ⇒
  `tracing::warn!` + continue (a failed prune must never crash the task).
- `main.rs`: call `spawn_retention_prune(state.clone())` next to the existing `spawn_usage_refresh` call.

- [ ] **Step 1:** Failing tests: (a) config parse (unset⇒0, `=30`⇒30, absurd⇒clamp 3650, malformed⇒0). (b) a
      DIRECT test of the one-tick prune logic (factor the per-tick body into a testable `async fn run_retention_pass(
      &AppState)` so you don't need to wait an hour): with retention_days>0 + seeded old rows, one pass deletes the
      old request_log rows + old usage_history rows (respecting the guard); with both = 0, one pass deletes NOTHING
      (no-op disable). (c) an e2e/integration wiring test that `run_retention_pass` reachable through the real store
      prunes end-to-end and the protected latest usage rows survive.
- [ ] **Step 2:** Run — fail. **Step 3:** Implement config + the pass fn + the spawn loop + main.rs wiring. **Step 4:**
      Green; all suites green.
- [ ] **Step 5:** Commit: `feat(server): retention pruning background loop + POLYFLARE_*_RETENTION_DAYS`

---

## Suggested order

1 (request_log prune) → 2 (usage_history prune + protect-latest guard, crux, adversarial review) → 3 (config + loop +
wiring + e2e). After Task 3, a long-running PolyFlare no longer grows its DB unbounded: old request_log + usage_history
rows are age-pruned hourly in batches, disabled by default, with each account's latest per-window usage sample always
protected, and identity/auth/continuity tables never touched. Mark C12 DONE in `PORTING-CODEXLB.md`. Follow-ups (not
this plan): optional VACUUM lever; a conservative continuity-session staleness prune (weeks-scale, its own review);
per-table retention override from the dashboard (codex-lb has this).
