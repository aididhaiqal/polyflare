# B5 — Anti-Starvation (serve-soonest + keepalive recovery-wait) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use `- [ ]`.

**Goal:** When selection/failover finds NO eligible account (all benched), instead of a fast 503: (Layer 1)
serve the soonest-to-recover *error-backoff* account immediately, and (Layer 2) for accounts on a real
rate-limit *cooldown*, hold the client with SSE keepalives and wait out the nearest reset (bounded budget),
then serve it. Completes Phase B — failover that degrades gracefully under full load.

**Architecture:** A three-way `eligibility` verdict (`Eligible | InBackoff{recover_at} | HardBlocked`) surfaces
`recover_at`, contained to `select.rs` (the `Selector::pick` trait is UNCHANGED). A new `soonest_recover`
selector method applies the capability filter then min-reduces `recover_at` over `InBackoff`. Ingress wraps its
empty-pool 503 sites in: Layer 1 (serve-now, guarded) then Layer 2 (a `ResponseStream` combinator that emits
keepalives, waits, re-snapshots + re-selects, and splices the real stream or an in-band error).

**Authority — the B5 scoping study (this session).** codex-lb refs: Layer 1 `logic.py:499-524`, Layer 2
`retry.py:_iter_account_capacity_recovery_wait` (+ `support.py:57-179`), constants MIN=1s / DEFAULT=30s /
MAX=300s / HEARTBEAT=10s, keepalive `": keepalive\n\n"`.

## Global Constraints

- **SECURITY FLOOR (inviolable) — wait/serve only over CAPABLE accounts.** `soonest_recover` applies the
  `require_security_work_authorized` filter BEFORE computing `recover_at` (identical to `select.rs:293/454`), so
  a cyber request only ever waits for / serves the soonest *authorized* account, NEVER a non-authorized one.
  Preserve the flag across the re-select after the wait. A test must assert a cyber request never serves/waits on
  a non-authorized account through the whole B5 path.
- **HARDBLOCKED IS NEVER A WAIT TARGET (inviolable — else wait-forever).** `HardBlocked` = terminal statuses
  (`reauth_required`/`deactivated`/`paused`) AND `rate_limited`/`quota_exceeded` with `reset_at == None` (no known
  recovery time). It carries NO `recover_at` and is excluded from the `min`. An all-HardBlocked (capability-
  filtered) pool ⇒ `soonest_recover` returns `None` ⇒ today's fast 503. Never sleep on a HardBlocked account.
- **BOUNDED BUDGET (inviolable).** Never wait beyond `POLYFLARE_STARVATION_WAIT_BUDGET_SECS` (default e.g. 60,
  clamp to codex-lb's MAX=300). Budget exceeded ⇒ fail (503 if pre-response, in-band SSE error if post-200).
  Never an unbounded wait.
- **RE-SNAPSHOT AFTER THE WAIT (correctness — the scoping's load-bearing gotcha).** `RuntimeStates::overlay`
  (`runtime_state.rs:88-97`) DROPS an elapsed `cooldown_until`. So after sleeping, the retry MUST re-fetch
  snapshots + re-`overlay` with a FRESH `now` — reusing the pre-wait snapshots/now would still see the stale
  cooldown and never recover. Re-run the whole selection.
- **POST-200 COMMIT (correctness — Layer 2, shape ii).** Once the SSE keepalive response has started (HTTP 200
  sent), you CANNOT fall back to an HTTP status. A recovery that still finds nothing, a budget-exceed, or an
  executor failure after the wait MUST surface as an in-band SSE error frame, never a 4xx/5xx. This is the
  riskiest invariant — Task 4 gets adversarial review.
- **The wait is a NEW OUTER step, NOT inside `run_failover_loop`.** The failover loop (B4) fans across distinct
  accounts on upstream failures. B5 triggers on "no eligible account exists at all". Do not conflate them.
- **The wedge fix stays sacred.** B5 wraps selection, not the executor/observe path, but the 5 wedge suites +
  cyber + failover suites MUST stay green at every task.
- Verify each task: `cargo test --workspace` green + `cargo clippy --workspace --all-targets -- -D warnings`.

---

### Task 1: The `eligibility` three-way verdict surfacing `recover_at`

**Files:** `crates/polyflare-core/src/select.rs` (`eligibility` ~126, the enum, `standard_pool` ~291,
`CacheAffinityTier` ~454); tests in the same file.

**Interfaces — Produces** (select.rs-private is fine):
```rust
enum Eligibility<'a> {
    Eligible(Candidate<'a>),
    InBackoff { recover_at: i64, kind: BackoffKind },  // kind distinguishes Layer1 vs Layer2 targets
    HardBlocked,
}
enum BackoffKind { ErrorBackoff, Cooldown }  // ErrorBackoff => Layer-1 serve-now candidate; Cooldown => Layer-2 wait
```
Refactor `eligibility` to return this instead of `Option<Candidate>`, recording the FIRST blocking gate's
`recover_at` + kind. Map per the scoping's gate table: terminal status → HardBlocked; `rate_limited`/`quota`
with `reset_at==Some(r)` and `now<r` → InBackoff{r, Cooldown}; with `reset_at==None` → **HardBlocked**;
`cooldown_until` → InBackoff{cd, Cooldown}; `error_count>=3` backoff → InBackoff{last_error_at+backoff, ErrorBackoff}.
**PRESERVE the recovery-doesn't-admit-early sequencing** (`select.rs:114-118`, tested `:730-749`): a recovered
rate_limited account still falls through the cooldown+backoff gates — don't early-return; thread the `eff_*`
mutations and let the first *remaining* blocking gate decide. `standard_pool`/`CacheAffinityTier` keep returning
only `Eligible` candidates (map the enum → discard non-Eligible, exactly like today's `filter_map`), so callers
are unchanged.

- [ ] **Step 1:** Failing tests: an account past its rate-limit reset but still in cooldown → InBackoff{cooldown, Cooldown}
      (proves fall-through preserved); `reset_at==None` rate_limited → HardBlocked; `error_count>=3` mid-window →
      InBackoff{.., ErrorBackoff}; a clean account → Eligible; a `paused` → HardBlocked. Assert the exact recover_at.
- [ ] **Step 2:** Run — fail. **Step 3:** Implement the enum + refactor; keep `standard_pool`/pick unchanged
      (Eligible-only). **Step 4:** Green — and ALL existing select.rs tests pass (the enum must not change which
      accounts are Eligible).
- [ ] **Step 5:** Commit: `feat(core): three-way eligibility verdict surfacing recover_at`

---

### Task 2: `soonest_recover` selector method (capability-filtered, HardBlocked-excluded)

**Files:** `crates/polyflare-core/src/select.rs` (+ maybe `traits.rs` if a trait method); tests.

**Interfaces — Produces:** a method the ingress calls at empty-pool sites:
`fn soonest_recover(&self, snapshots: &[AccountSnapshot], ctx: &SelectionCtx) -> Option<Recovery>` where
`Recovery { recover_at: i64, account_id: AccountId, kind: BackoffKind }` (the WHICH account + WHEN + why).
It: (1) applies the SAME capability pre-filter as `standard_pool` (`!ctx.require_security_work_authorized ||
s.security_work_authorized`) FIRST, (2) classifies each via `eligibility`, (3) returns the `InBackoff` with the
MIN `recover_at` (HardBlocked + Eligible excluded), or `None` if none. Decide: trait method vs inherent helper
(the scoping suggests a sibling to `standard_pool`); if a trait method, all 6 impls get it (mostly a shared
default). Keep it simple.

- [ ] **Step 1:** Failing tests: pool of {cooldown@100, error-backoff@50, hardblocked} → returns the @50
      error-backoff (min, HardBlocked excluded); **security:** a cyber ctx over {capable-cooldown@200,
      non-authorized-cooldown@50} → returns the @200 CAPABLE one (never the sooner non-authorized); all-HardBlocked
      → None; all-Eligible → None (nothing to wait for).
- [ ] **Step 2:** Run — fail. **Step 3:** Implement. **Step 4:** Green.
- [ ] **Step 5:** Commit: `feat(core): soonest_recover — capability-filtered soonest-to-recover`

---

### Task 3: Layer 1 — serve the soonest error-backoff account immediately (guarded, no wait)

**Files:** `crates/polyflare-server/src/ingress.rs` (the empty-pool sites) + tests.

When selection returns empty AND `soonest_recover` yields an `ErrorBackoff`-kind candidate, serve it IMMEDIATELY
(no wait) — an error-backoff account is *probably* fine; better to try it than 503. Port codex-lb's GUARD
(`logic.py:499-524`): only serve-now when there is >1 backoff account OR (1 backoff + a HardBlocked exists);
a lone backoff account with no hard-blocked peer falls through to Layer 2 / today's path. (This guard avoids
hammering a single flaky account.) Cooldown-kind candidates do NOT serve-now (they'd 429 again) → they go to
Layer 2.

- [ ] **Step 1:** Failing tests: empty eligible pool + 2 error-backoff accounts → serves the soonest one
      (assert the request went to it), no wait. Empty pool + 1 error-backoff + 1 HardBlocked → serves the backoff.
      Empty pool + 1 lone error-backoff (no hardblocked) → does NOT serve-now (falls through). Empty pool + only
      Cooldown accounts → does NOT serve-now (→ Layer 2). Security: a cyber request only serves a capable backoff.
- [ ] **Step 2:** Run — fail. **Step 3:** Implement at the empty-pool sites (reuse the existing resolve+execute
      path — this is just an extra candidate source, not new execution machinery). **Step 4:** Green; wedge/
      failover/cyber suites green.
- [ ] **Step 5:** Commit: `feat(server): B5 Layer 1 — serve soonest error-backoff account (guarded)`

---

### Task 4: Layer 2 — the keepalive recovery-wait combinator (THE CRUX — adversarial review)

**Files:** `crates/polyflare-server/src/ingress.rs` (a new wait-and-retry combinator around selection) + a
keepalive/wait module; tests.

For a `Cooldown`-kind `soonest_recover` result within budget: build a `ResponseStream` combinator that
(a) commits HTTP 200 SSE, (b) emits `": keepalive\n\n"` every HEARTBEAT (default 10s) while waiting until
`recover_at` (bounded by `POLYFLARE_STARVATION_WAIT_BUDGET_SECS`), (c) **re-fetches snapshots + re-overlays with
a fresh `now`** and re-runs selection (Global Constraint: overlay drops elapsed cooldown), (d) on success splices
the real upstream `ResponseStream`; on budget-exceed / still-nothing / executor-error → emits an **in-band SSE
error frame** (NEVER an HTTP status — the 200 is already sent). The async re-selection + executor call run INSIDE
the stream (`async_stream::stream!` or hand-rolled poll), with `Arc`-cloned `AppState`/selector (already
Arc-cloned in the failover loop, `ingress.rs:947-955`). Hand the combinator to the existing `stream_response`
(`ingress.rs:272`) — no new response-builder path.

**Inviolables with a test EACH (reviewer hunts these):**
- **Post-200 in-band error:** a wait that recovers nothing within budget ⇒ the client gets a 200 stream that
  ends in an SSE error frame, NEVER a late 4xx/5xx. Test asserts the response is 200 + an in-band error, no panic.
- **Bounded:** never waits past the budget (assert the wait terminates ≤ budget even if the account never recovers).
- **Re-snapshot:** after the wait, a NOW-recovered account is actually selected (test: account on cooldown until
  T, budget > T, ⇒ after the wait it's served — proving re-overlay saw the recovery, not the stale snapshot).
- **Security floor:** a cyber request waits only for a capable account; never serves/waits a non-authorized one
  (assert via the per-request token recorder across the whole wait+retry).
- **HardBlocked never waited:** all-HardBlocked pool ⇒ no wait, fast 503 (pre-response).
- **Keepalive content-safe:** the keepalive bytes are a fixed `": keepalive\n\n"` (or a content-free status
  frame) — NEVER a body/message/token.

- [ ] **Step 1:** Failing tests for each inviolable above (need a mock account that's cooldown-until-T then
      recovers — extend the testkit minimally: a snapshot whose cooldown elapses during the test, or a
      time-injection). **Step 2:** Run — fail. **Step 3:** Implement the combinator. **Step 4:** Green; 5 wedge +
      cyber + failover suites green.
- [ ] **Step 5:** Commit: `feat(server): B5 Layer 2 — keepalive recovery-wait combinator`

---

### Task 5: Config + observability + e2e

**Files:** `crates/polyflare-server/src/config.rs` (`POLYFLARE_STARVATION_WAIT_BUDGET_SECS` default 60 clamp
[1,300], `POLYFLARE_STARVATION_HEARTBEAT_SECS` default 10; resolved once into `AppState`, NOT per-request),
`observability.rs` (a content-free starvation-wait metric/log — waited-seconds + reason, no content), `tests/` e2e.

- [ ] **Step 1:** Failing tests: config resolves/clamps; content-free starvation signal fires on a wait. E2e
      through the real `build_app`: a pool where all accounts are briefly cooled-down then recover ⇒ the client
      gets keepalives then a clean stream, and a content-free wait signal was emitted (assert no body/token in it).
      A budget=0 (or all-HardBlocked) ⇒ fast 503 (regression / the disable lever).
- [ ] **Step 2:** Run — fail. **Step 3:** Implement. **Step 4:** Green.
- [ ] **Step 5:** Commit: `feat(server): B5 config + content-free starvation observability`

---

## Suggested order

1 (enum) → 2 (soonest_recover) → 3 (Layer 1 serve-now, ships value early) → 4 (Layer 2 keepalive-wait, the
crux, adversarial review) → 5 (config + e2e). After Task 5, B5 completes Phase B's anti-starvation. B8
(health-tier) and B10 (thundering-herd) remain as separate later items.
