# B8 — Health-Tier Soft-Drain State Machine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use `- [ ]`.

**Goal:** Make the selector *soft-drain* an account that is approaching its limits (usage ≥85%/90%) or flapping
(≥2 recent errors) — prefer healthier accounts BEFORE the draining one hard-fails — via a HEALTHY→DRAINING→PROBING
→HEALTHY state machine. Today the read-side (`should_drain`/`effective_tier`/`health_tier_pool` in `select.rs`) is
already built and tested but never fed: `AccountSnapshot.health_tier` is always 0. B8 supplies the **write side**
(the state machine + its aux state), computed at the runtime write chokepoints and copied onto the snapshot by the
existing overlay. `select.rs` needs **zero changes**.

**Architecture:** A pure transition function `evaluate_health_tier` ported byte-faithfully from codex-lb
(`app/core/balancer/logic.py:1181-1239`), plus two aux fields in `RuntimeState` (`drain_entered_at`,
`probe_success_streak`) and a stored `health_tier`. It is evaluated where each signal lives — **usage-driven**
transitions in the 600s `usage_refresh` loop (it has `used_percent`), **error-driven** DRAINING entry + probe-streak
accounting in the per-request funnel (`record_success`/`record_transient_error`/`record_rate_limit`, pure runtime
state). `overlay` copies `rt.health_tier` onto the snapshot; `select.rs`'s existing `health_tier_pool` bucketing
(healthy→probing→draining) then does the soft-drain preference. A `POLYFLARE_SOFT_DRAIN_ENABLED` flag (default on)
is the disable lever.

**Authority — the B8 scoping study + codex-lb ground truth (this session), file:line cites:**
- Requirement: `docs/PORTING-CODEXLB.md:149-156` (B8). Prereqs A2/A3 + B4/B5 are DONE (`PORTING-CODEXLB.md:39-73`);
  `error_count`/`last_error_at`/`used_percent` are all live-written today, so B8 is unblocked.
- codex-lb algorithm (verified this session): `evaluate_health_tier` `app/core/balancer/logic.py:1181-1239` — a PURE
  fn. Thresholds (`logic.py:84-93`): `HEALTHY=0 DRAINING=1 PROBING=2`; `DRAIN_PRIMARY_THRESHOLD_PCT=85.0`,
  `DRAIN_SECONDARY_THRESHOLD_PCT=90.0`, `DRAIN_ERROR_WINDOW_SECONDS=60.0`, `DRAIN_ERROR_COUNT_THRESHOLD=2`,
  `PROBE_QUIET_SECONDS=60.0`, `PROBE_SUCCESS_STREAK_REQUIRED=3`. Transitions:
  - **frozen** — if `status ∈ {RATE_LIMITED, QUOTA_EXCEEDED, PAUSED, REAUTH_REQUIRED, DEACTIVATED}` ⇒ return the
    stored `health_tier` UNCHANGED (no transition while blocked).
  - `should_drain = used% ≥ 85 OR secondary% ≥ 90 OR (error_count ≥ 2 AND last_error_at set AND now − last_error_at < 60)`.
  - HEALTHY: `should_drain ? DRAINING : HEALTHY`.
  - DRAINING: `should_drain ? DRAINING` ; else if `drain_entered_at set AND now − drain_entered_at ≥ 60 ⇒ PROBING`; else `DRAINING`.
  - PROBING: `should_drain ? DRAINING` ; else if `probe_success_streak ≥ 3 ⇒ HEALTHY`; else `PROBING`.
- codex-lb writeback (`app/modules/proxy/load_balancer.py:2220-2249`): after computing `new_tier`, on the
  **HEALTHY→DRAINING edge** (`new_tier==DRAINING && old != DRAINING`) stamp `drain_entered_at = now`, reset
  `probe_success_streak = 0`; on **→HEALTHY** clear `drain_entered_at = None`, `probe_success_streak = 0`; then store
  `health_tier = new_tier`. Disable path (`soft_drain_enabled=false`, `:2245-2249`): force tier HEALTHY, clear aux.
- codex-lb probe-streak lifecycle (`load_balancer.py:1580-1608`): `record_success` ⇒ if PROBING, `probe_success_streak
  += 1`; the transient-error recorder ⇒ if PROBING, `probe_success_streak = 0`.
- PolyFlare read-side already built + tested: `Candidate::should_drain`/`effective_tier` (`select.rs:93-111`, same
  85/90/2/60s formula), `health_tier_pool` (`select.rs:348-360`, tries `[healthy, probing, draining]`, pigeonhole ⇒
  never empties the pool), `standard_pool` pipeline (`select.rs:438-449`: capability filter → `eligibility()` →
  `health_tier_pool` → `policy_waterfall`). Unit tests `select.rs:820-832,938-956`.
- PolyFlare runtime funnel: `RuntimeState` (`runtime_state.rs:49-54`), `overlay` (`:79-100`, read-only copy),
  `mutate`+`is_neutral` GC (`:104-111,59-61`), `record_rate_limit` (`:117`), `record_transient_error` (`:171`),
  `record_success` (`:183`). AppState holds `runtime: Arc<RuntimeStates>` (`app.rs:81`); `usage_refresh` has
  `Arc<AppState>` (`usage_refresh.rs:19,111,165`) → can reach the runtime map. Config idiom
  `POLYFLARE_STARVATION_*` startup-resolved into `ServeConfig` (`config.rs:54-64,238-300`). Observability
  `FailoverSignal`/`StarvationSignal` content-free + counter (`observability.rs:166-207,263-319`).

## Global Constraints

- **PORT the transition table byte-faithfully.** Thresholds and the exact transition logic MUST match codex-lb's
  `evaluate_health_tier` (values above). No re-invention; a divergence is a defect.
- **Soft-drain is a PREFERENCE among ELIGIBLE accounts, never a hard gate (inviolable).** `health_tier` MUST NOT
  enter `eligibility()` (`select.rs:162-256`) or any hard filter, and MUST NOT touch the capability/security-floor
  filter. `health_tier_pool` can never empty the pool (pigeonhole) — a fully-drained pool still serves its least-bad
  member. Do NOT reorder the `standard_pool` pipeline (capability → eligibility → health_tier → waterfall).
- **Continuity ownership still wins.** `apply_ownership` (`watchdog.rs:122-145`) narrows to the single owner BEFORE
  `pick`; health-tier bucketing is a no-op on a 1-candidate slice. Do NOT add any health-tier check that could
  override an owned pick. The 5 wedge + cyber + failover + starvation suites MUST stay green.
- **`select.rs` is UNCHANGED.** B8 only supplies the write side (RuntimeState + funnel + poller + overlay copy). If
  a task edits `select.rs`, stop — the read side is already correct.
- **Bounded runtime map.** Do NOT cache usage% in `RuntimeState` (would keep every polled account resident). The new
  aux fields default to HEALTHY/None/0 so a healthy account's entry stays neutral and is GC'd by `mutate`.
- **Do NOT wire the persisted tier into `CacheAffinityTier`.** It already folds `should_drain` (from the snapshot's
  usage%) into its own `tier_weight` (`select.rs:565-593`) — a separate, independent consumer. Feeding it the
  persisted tier too would double-count the drain signal. B8 confines the persisted tier to `health_tier_pool`'s
  consumers (the pool-first strategies).
- **Content-safety:** any transition signal carries fixed reason codes + account id + tier numbers only — never a
  body/token.
- **Disable lever:** `POLYFLARE_SOFT_DRAIN_ENABLED=0` ⇒ no evaluation, tier stays 0 everywhere ⇒ `health_tier_pool`
  is a no-op (all one bucket) = today's exact behavior (clean rollback). Default on (codex-lb `soft_drain_enabled`=true).
- Verify each task: `cargo test --workspace` green + `cargo clippy --workspace --all-targets -- -D warnings`.

---

### Task 1: The pure transition function + aux state (THE CRUX — adversarial review)

**Files:** `crates/polyflare-server/src/runtime_state.rs` (add aux fields to `RuntimeState`; add a pure
`evaluate_health_tier` fn + a `HealthInputs` param struct; add a `transition` writeback helper); tests in the same file.

**Interfaces — Produces:**
```rust
// on RuntimeState — new fields (all default to the HEALTHY/neutral values):
//   pub health_tier: u8,              // 0 healthy, 1 draining, 2 probing
//   pub drain_entered_at: Option<i64>,
//   pub probe_success_streak: u32,
// is_neutral() must still return true when these are (0, None, 0) — so a healthy entry is GC'd.

// thresholds as module consts mirroring codex-lb logic.py:84-93:
//   DRAIN_PRIMARY_PCT=85.0, DRAIN_SECONDARY_PCT=90.0, DRAIN_ERROR_WINDOW_SECS=60,
//   DRAIN_ERROR_COUNT=2, PROBE_QUIET_SECS=60, PROBE_SUCCESS_STREAK_REQUIRED=3

/// Pure port of codex-lb evaluate_health_tier. `frozen` = status is a blocked status (caller decides).
/// Returns the NEW tier only; the caller applies the drain_entered_at/probe_streak writeback edges.
pub fn evaluate_health_tier(
    current_tier: u8,
    should_drain: bool,
    drain_entered_at: Option<i64>,
    probe_success_streak: u32,
    frozen: bool,
    now: i64,
) -> u8;

/// Compute should_drain from the three OR'd conditions (usage may be None when unknown).
pub fn compute_should_drain(used_percent: Option<f64>, secondary_percent: Option<f64>,
                            error_count: u32, last_error_at: Option<i64>, now: i64) -> bool;
```
The `transition` writeback (a method on `RuntimeState`, applied under the `mutate` lock by later tasks): given the
new tier, stamp `drain_entered_at`/reset streak on the HEALTHY→DRAINING edge, clear both on →HEALTHY, store the tier.

- [ ] **Step 1:** Failing unit tests, one per transition row of codex-lb's table (assert exact tiers):
      (a) HEALTHY + should_drain ⇒ DRAINING; HEALTHY + !drain ⇒ HEALTHY. (b) DRAINING + should_drain ⇒ DRAINING;
      DRAINING + !drain + `now-drain_entered_at ≥ 60` ⇒ PROBING; DRAINING + !drain + `< 60` ⇒ DRAINING;
      DRAINING + !drain + `drain_entered_at=None` ⇒ DRAINING (no promote without a stamp). (c) PROBING + should_drain
      ⇒ DRAINING; PROBING + streak ≥ 3 ⇒ HEALTHY; PROBING + streak < 3 ⇒ PROBING. (d) **frozen** (blocked status) ⇒
      returns `current_tier` unchanged regardless of should_drain. (e) `compute_should_drain`: each of the three OR
      conditions independently true ⇒ true (used%=85 exactly ⇒ true; secondary=90 ⇒ true; error_count=2 with
      last_error_at `now-59` ⇒ true; error_count=2 with `now-61` ⇒ false; error_count=1 recent ⇒ false; all None/0 ⇒
      false). (f) the `transition` writeback: HEALTHY→DRAINING stamps `drain_entered_at=now` + streak 0; any→HEALTHY
      clears both; DRAINING→PROBING leaves `drain_entered_at` intact (needed if it bounces back). (g) `is_neutral`
      still true for a freshly-defaulted state incl. the new fields.
- [ ] **Step 2:** Run — fail. **Step 3:** Implement the consts + `compute_should_drain` + `evaluate_health_tier` +
      the `transition` helper + the aux fields; keep `is_neutral` == all-default. **Step 4:** Green.
- [ ] **Step 5:** Commit: `feat(server): health-tier transition state machine (pure port of codex-lb evaluate_health_tier)`

---

### Task 2: Error-driven evaluation in the funnel + overlay copy

**Files:** `crates/polyflare-server/src/runtime_state.rs` (`record_success`/`record_transient_error`/
`record_rate_limit` re-evaluate the ERROR branch + probe-streak; `overlay` copies `health_tier`); tests.

**Interfaces — Consumes:** Task 1's `evaluate_health_tier`/`compute_should_drain`/`transition`. **Produces:** the
funnel now maintains `health_tier` from the runtime-only signal (errors) + the probe streak. The usage branch is
Task 3's (the funnel passes `used_percent=None`, so it can only DRIVE the tier via the error condition — it must
never CLEAR a usage-driven drain it can't see: see the constraint below).

- **The care point (correctness):** the funnel has no usage%. When it re-evaluates with `used_percent=None`, an
  account draining purely from USAGE would have `should_drain=false` in the funnel's view and could be wrongly
  promoted toward HEALTHY. PREVENT this: in the funnel, only ADD drain from the error signal — i.e. compute the
  error-only `should_drain`, and (per codex-lb's edges) apply transitions, BUT do not let a `!should_drain` funnel
  evaluation promote DRAINING→PROBING (that promotion is the poller's job, which sees usage). Concretely: in the
  funnel, run the transition ONLY for the HEALTHY→DRAINING (error) edge and the PROBING streak edges; do NOT run the
  DRAINING→PROBING quiet-timer promotion (leave DRAINING as-is when the error signal clears — the poller owns
  demotion because only it knows usage). Document this split explicitly.
- `record_success`: clear error state (as today) AND if currently PROBING, `probe_success_streak += 1`, then apply
  the PROBING streak edge (≥3 ⇒ HEALTHY via `transition`). If currently DRAINING or HEALTHY, no streak change.
- `record_transient_error` / `record_rate_limit`: bump error_count + last_error_at (as today) AND: if currently
  PROBING, reset `probe_success_streak = 0`; then if the error-only `should_drain` is now true and tier is HEALTHY,
  transition HEALTHY→DRAINING (stamp drain_entered_at). (A PROBING account that errors while `should_drain` ⇒
  DRAINING per the table — apply that too.)
- `overlay`: add `snap.health_tier = rt.health_tier;` alongside the existing field copies (absent entry ⇒ stays 0).
- **`is_neutral` interaction:** a DRAINING/PROBING entry is non-neutral ⇒ retained (correct). An entry that returns
  to HEALTHY with cleared aux AND zero error state is neutral ⇒ GC'd (correct — it re-evaluates fresh next signal).

- [ ] **Step 1:** Failing tests: (a) an account with error_count reaching 2 within 60s via
      `record_transient_error` ⇒ `overlay` shows `health_tier == 1 (DRAINING)`. (b) a DRAINING/PROBING account:
      drive it to PROBING (seed tier+drain_entered_at old), then 3× `record_success` ⇒ `health_tier == 0 (HEALTHY)`
      and aux cleared. (c) a PROBING account that gets a `record_transient_error` ⇒ streak reset to 0 (and tier
      DRAINING if error-drain). (d) **the care point:** an account whose tier is DRAINING with `drain_entered_at`
      older than 60s, then `record_success` with NO usage signal ⇒ it does NOT jump to PROBING via the funnel (stays
      DRAINING; only the poller promotes). (e) `overlay` copies health_tier; absent entry ⇒ 0.
- [ ] **Step 2:** Run — fail. **Step 3:** Implement. **Step 4:** Green; the 5 wedge/cyber/failover/starvation suites
      green (funnel additions are additive; `select.rs` untouched).
- [ ] **Step 5:** Commit: `feat(server): error-driven health-tier evaluation in the runtime funnel + overlay copy`

---

### Task 3: Usage-driven evaluation in the poller + config flag

**Files:** `crates/polyflare-server/src/usage_refresh.rs` (`refresh_account` evaluates the FULL tier with usage%),
`crates/polyflare-server/src/runtime_state.rs` (a `evaluate_with_usage(id, used%, secondary%, status_frozen, enabled, now)`
method that does the full evaluate + writeback under the lock), `crates/polyflare-server/src/config.rs`
(`POLYFLARE_SOFT_DRAIN_ENABLED`, default true, startup-resolved into `ServeConfig`/`AppState`); tests.

**Interfaces — Consumes:** Task 1/2. **Produces:** the authoritative periodic (≤600s) evaluation that owns the
usage-driven DRAINING entry AND the DRAINING→PROBING quiet-timer demotion (it has both usage% and, from the runtime
entry, error state — the full `should_drain`). This is where a near-quota account (no errors) drains.

- `RuntimeStates::evaluate_with_usage(...)`: under `mutate`, read the entry, compute full `should_drain`
  (usage OR error), run `evaluate_health_tier` with `frozen = status_is_blocked`, apply the `transition` writeback.
  When `enabled == false` ⇒ force tier HEALTHY + clear aux (codex-lb disable path). Note: `mutate`'s GC will drop the
  entry if it lands neutral — fine (a HEALTHY no-error account needn't persist).
- `usage_refresh::refresh_account`: after it computes `used_percent`/`secondary_used_percent` + the derived status,
  call `state.runtime.evaluate_with_usage(id, used%, secondary%, status_frozen, cfg.soft_drain_enabled, now())`.
  `status_frozen` = the same blocked-status set as codex-lb (rate_limited/quota_exceeded/paused/reauth/deactivated) —
  reuse whatever status enum the refresh already has.
- `config.rs`: `soft_drain_enabled: bool` on `ServeConfig`, resolved from `POLYFLARE_SOFT_DRAIN_ENABLED` (accepts
  `0`/`false` ⇒ off, anything else/unset ⇒ on — mirror the existing bool-env idiom; if none exists, default-true
  parse). Thread onto `AppState` so the poller reads it (startup-resolved, NOT per-request).

- [ ] **Step 1:** Failing tests: (a) `evaluate_with_usage` with `used%=90`, no errors, HEALTHY ⇒ DRAINING +
      `drain_entered_at` stamped. (b) a DRAINING account (drain_entered_at 61s ago) with usage now BELOW threshold +
      no errors ⇒ PROBING (the poller's quiet-timer promotion). (c) `enabled=false` ⇒ forces HEALTHY + clears aux
      even when `used%=99`. (d) frozen status (e.g. rate_limited) + `used%=99` ⇒ tier UNCHANGED (frozen). (e) config:
      `POLYFLARE_SOFT_DRAIN_ENABLED` unset ⇒ true, `=0` ⇒ false, `=false` ⇒ false.
- [ ] **Step 2:** Run — fail. **Step 3:** Implement `evaluate_with_usage` + the poller call + the config field +
      threading. **Step 4:** Green; suites green.
- [ ] **Step 5:** Commit: `feat(server): usage-driven health-tier evaluation in the usage poller + POLYFLARE_SOFT_DRAIN_ENABLED`

---

### Task 4: Content-free transition observability + selection e2e (integration crux)

**Files:** `crates/polyflare-server/src/observability.rs` (a content-free `HealthTierSignal` + a `HealthTierMetrics`
counter, mirroring `StarvationSignal`), wire an emit at the transition edges (in the funnel + poller writeback),
`AppState` counter field; an e2e test (new `tests/health_tier_e2e.rs` or extend an existing selection test).

**Interfaces — Consumes:** Task 1-3. **Produces:** operator-visible, content-free evidence that a drain happened,
and the end-to-end proof that a live-tracked DRAINING account is actually de-preferred in real selection.

- `HealthTierSignal { account_id, from_tier: u8, to_tier: u8, reason: &'static str }` where reason ∈
  {`"usage_drain"`, `"error_drain"`, `"quiet_promote"`, `"probe_promote"`, `"disabled_reset"`} — fixed labels, no
  content. `emit()` (tracing warn on a dedicated target) + `to_log_event()` for the log bus + a `HealthTierMetrics`
  AtomicU64 (transitions counted) on `AppState`. Emit ONLY on an actual tier change (from != to).
- e2e (the real property, not a unit re-test): through the real `build_app` + selector, seed two eligible accounts;
  drive one to DRAINING (via the error funnel, the fast path — 2 `record_transient_error` within 60s, or seed usage
  ≥85% + run one poller tick), then issue a selection and assert the HEALTHY account is chosen over the DRAINING one
  (mirrors the `select.rs` unit test but through the live overlay+snapshot path — proving the write side reaches the
  read side). Then assert with `POLYFLARE_SOFT_DRAIN_ENABLED=0` the DRAINING account is NOT de-preferred (tier stays
  0 end-to-end). Assert the signal/counter fired content-free (no body/token in the log event).

- [ ] **Step 1:** Failing tests: the e2e de-preference (enabled) + the disabled no-op + a content-free assertion on
      the signal. **Step 2:** Run — fail. **Step 3:** Implement the signal/counter + emit at the edges + wire the
      e2e. **Step 4:** Green; ALL suites green; clippy clean.
- [ ] **Step 5:** Commit: `feat(server): content-free health-tier transition signal + soft-drain selection e2e`

---

## Suggested order

1 (pure transition fn, crux, adversarial review) → 2 (funnel/error-driven + overlay) → 3 (poller/usage-driven +
config) → 4 (observability + e2e). After Task 4, B8 is done: a near-limit or flapping account is softly de-preferred
before it hard-fails, recovering through a probing phase, all as a preference among eligible accounts that never
weakens eligibility/security-floor/anti-starvation and never overrides continuity ownership. Mark B8 DONE in
`PORTING-CODEXLB.md`. Follow-ups (not this plan): C9 `in_flight` lease accounting (a separate drain input),
B10 thundering-herd, and deciding whether `CacheAffinityTier` should eventually consume the persisted tier.
