# C9 — In-Flight Lease Accounting (soft-penalty, leak-proof) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use `- [ ]`.

**Goal:** Spread concurrent load across accounts by tracking how many requests are in-flight per account and folding
that as a small soft penalty into the capacity weight, so the selector de-prefers an account that's already busy
(before it piles up). This is the orthogonal concurrent-convergence herd dimension B10 explicitly deferred (B10 fixed
*time*-synchronized wake-ups; C9 fixes *many concurrent requests converging on the same best account in one
selection window*). `AccountSnapshot.in_flight: u32` exists but is dead-wired (defaulted 0, never incremented/read);
C9 supplies the write side + the selector penalty.

**Architecture:** A leak-proof `InFlightGuard` (Rust RAII: increments `RuntimeState.in_flight` on acquire, decrements
in `impl Drop`) acquired at selection and **held for the request's true lifetime** by embedding it as a FIELD of
`ObservingStream` — so it releases on EVERY stream exit (drain / client-disconnect / mid-stream error / idle-timeout
/ panic) automatically, with NO change to `ObservingStream`'s poll logic (Rust auto-drops fields). The cross-account
failover loop drops a failed pre-stream attempt's guard before picking the next account. `RuntimeStates::overlay`
copies `in_flight` onto the snapshot (mirroring `health_tier`); `select.rs` folds `in_flight * penalty_pct` into the
account's effective used-percent (mirroring codex-lb exactly), which flows through the existing capacity weighting.
**v1 is SOFT-PENALTY ONLY** — the hard concurrency cap is deferred (see Global Constraints). `pick` stays pure.

**Authority — the C9 scoping study + codex-lb ground truth (this session), file:line cites:**
- Requirement `docs/PORTING-CODEXLB.md:187-194` (C9, Phase C, MEDIUM). codex-lb refs
  `app/modules/proxy/load_balancer.py:201-318,1767-1788,2212-2225`.
- codex-lb mechanism (verified): increment `_acquire_account_lease_locked` (`load_balancer.py:236-263`) at
  selection; decrement `_release_account_lease_locked` (`:281-304`) with `max(0,..)` floor + null-guard; leak
  defense = try/finally per path (`_service/streaming/mixin.py:1026-1029`) PLUS a TTL reclaim sweep
  (`_reclaim_stale_account_leases_locked` `:306-318`, 900s). **Soft penalty** (`:2251-2263`):
  `inflight_pressure_pct = (inflight_response_creates + inflight_streams) * 2.5`; `effective_used_percent =
  min(100, used_percent + pressure_pct)` (+ same for secondary) BEFORE capacity weighting. **Hard cap**
  (`:463-479,661-673`): drops accounts at `response_create_limit(4)`/`stream_limit(8)` — and **can empty the pool
  with an explicit `account_cap_exhausted` failure** (codex-lb has NO pigeonhole fallback for cap exhaustion). In-
  memory only (no DB).
- PolyFlare read-side mirror (B8's exact pattern): `AccountSnapshot.in_flight` (`types.rs:313`, default 0 `:342`,
  dead — grep confirms ZERO reads/writes outside the decl + one test pin `tests/snapshot_assembly.rs:91`).
  `RuntimeStates::overlay` (`runtime_state.rs:315-341`) copies live fields (health_tier at `:338`) — silent on
  in_flight. `RuntimeState` (`:111-126`) + `mutate`/`is_neutral` GC (`:347-355,131-133`). `record_selected`
  (`:569-571`) stamps last_selected_at at selection. select.rs `Candidate`/`eff_used`/`eff_secondary_used` built in
  `eligibility()` (`select.rs:162-176`), `remaining_secondary_credits` (`:78-80`), `weighted_pick` (`:425-430`).
  `apply_ownership` narrows to the pinned owner BEFORE `pick` (`ingress.rs:120-141`).
- **The leak-proof-release crux (scoping §4-5):** `ObservingStream::poll_next` (`watchdog.rs:814-905`) fires
  record_success on clean EOF (`:845`) / record_transient_error on mid-stream Err (`:838`) / idle-timeout (`:872`) —
  but has NO `Drop` impl, so a **client disconnect drops the stream WITHOUT polling to completion → those arms never
  fire.** For health bookkeeping that neutrality is CORRECT (a disconnect isn't an account error). For an in-flight
  lease it is a LEAK (the count MUST decrement on disconnect). Opposite disconnect-correctness → do NOT copy A1's
  "account-neutral" pattern for the release. The only RAII-adjacent primitive is `CommitWitness` (`watchdog.rs:100-118`,
  an AtomicBool marker, NOT a Drop-release). `run_failover_loop` (`ingress.rs:1136-1291`) picks A → execute → on
  `Err(e2)` (`:1283`) picks a new account; a per-attempt guard must release A before B is acquired.
- Config/observability idioms: `POLYFLARE_SOFT_DRAIN_ENABLED`/`POLYFLARE_STARVATION_WAKE_JITTER_MS` startup-resolved
  into `ServeConfig` (`config.rs`); content-free `HealthTierSignal`/metrics (`observability.rs`).

## Global Constraints

- **SOFT PENALTY ONLY for v1 — the hard cap is DEFERRED (inviolable scope boundary).** in_flight folds into the
  effective used-percent as a WEIGHT; it is NEVER a hard eligibility filter. It therefore can NEVER empty the
  eligible pool (a fully-busy pool still selects its least-busy member — same safety class as B8's `should_drain`
  soft nudge). codex-lb's hard cap (which CAN empty the pool with `account_cap_exhausted`) conflicts with PolyFlare's
  established "never let the eligible pool go to zero without a soonest-fallback" principle — porting it needs a new
  pigeonhole-fallback judgment call that is OUT OF SCOPE here. Do NOT add a hard cap / cap filter / eligibility gate
  on in_flight.
- **Leak-proof release by construction (THE crux, inviolable).** The lease MUST be released on EVERY request exit
  path: clean drain, client disconnect, mid-stream error, idle-timeout, pre-relay failure, cross-account failover
  reselect, and panic. The mechanism is a Rust `Drop` guard embedded as a FIELD of `ObservingStream` (Rust auto-
  drops fields on any drop of the stream — covering drain/disconnect/error/timeout/panic uniformly) PLUS explicit
  guard-drop on the failover loop's failed-attempt reselect. A leaked lease permanently poisons an account's
  in_flight and is a Critical defect — a test MUST prove release on client-disconnect (drop the stream mid-flight)
  and on failover-reselect.
- **`ObservingStream`'s poll logic is UNCHANGED (wedge-sacred).** C9 only ADDS a guard field whose own `Drop`
  decrements; it does NOT alter `poll_next`, record_success/record_transient_error, the commit witness, or the
  continuity/observe calls. The 5 wedge + cyber + failover + starvation suites MUST stay green. Do NOT add an
  `impl Drop` to `ObservingStream` itself (unnecessary — the field's Drop suffices and keeps poll logic pristine).
- **Never overrides continuity ownership.** `apply_ownership` narrows to the pinned owner before `pick`, so the
  in_flight penalty (a weight inside the post-ownership pool) structurally cannot move an owned request off its
  owner. Do NOT add any in_flight check that could.
- **`pick` stays pure-sync (M2-GATE1).** in_flight is OVERLAID onto the snapshot (like health_tier/error_count),
  read by `select.rs` — never computed inside `pick`. No clock/rand in the selection path.
- **`is_neutral`/GC must respect in_flight.** An account with `in_flight > 0` is NON-neutral → never GC'd from the
  runtime map mid-flight (would lose the count). An account back at in_flight 0 + no other signal is neutral → GC'd.
- **Disable lever + default.** `POLYFLARE_INFLIGHT_PENALTY_PCT` default 2.5 (codex-lb's value); `=0` ⇒ penalty
  disabled (in_flight still tracked, just not folded into the weight = clean behavioral rollback). Clamp to a sane
  ceiling.
- **Content-safety:** any lease metric/signal is content-free (account id + counts only).
- Verify each task: `cargo test --workspace` green + `cargo clippy --workspace --all-targets -- -D warnings`.

---

### Task 1: The leak-proof `InFlightGuard` + RuntimeState field + overlay (the write-side primitive)

**Files:** `crates/polyflare-server/src/runtime_state.rs` (add `in_flight: u32` to `RuntimeState`; `acquire_in_flight`
returning an `InFlightGuard`; the `InFlightGuard` struct with `impl Drop`; update `is_neutral`; copy in overlay);
tests.

**Interfaces — Produces:**
```rust
// RuntimeState gains: pub in_flight: u32,  (is_neutral() must return false when in_flight > 0)
// A guard that decrements on drop:
#[must_use]
pub struct InFlightGuard { /* Arc<RuntimeStates> (or a Weak/handle) + AccountId, released: bool guard-against-double */ }
impl Drop for InFlightGuard { fn drop(&mut self) { /* mutate: in_flight = in_flight.saturating_sub(1); GC if neutral */ } }
// On RuntimeStates:
pub fn acquire_in_flight(self: &Arc<Self>, id: &AccountId, now: i64) -> InFlightGuard; // mutate: in_flight += 1; returns guard
```
- The guard holds enough to call back into the map on drop (an `Arc<RuntimeStates>` clone, or a `Weak` — decide;
  `Arc` is simplest and the map lives for process life). `acquire_in_flight` must be callable as
  `state.runtime.acquire_in_flight(&id, now)` where `state.runtime: Arc<RuntimeStates>`.
- Guard `Drop` decrements with `saturating_sub(1)` (never underflow) and lets `mutate`'s GC drop a now-neutral entry.
- `overlay`: add `snap.in_flight = rt.in_flight;` (absent entry ⇒ snapshot stays 0).
- `is_neutral`: an entry with `in_flight > 0` is NOT neutral (so it isn't GC'd mid-flight). Confirm the derive/compare
  still treats a defaulted in_flight (0) as neutral.

- [ ] **Step 1:** Failing tests: (a) `acquire_in_flight` increments to 1, a 2nd acquire → 2; dropping one guard → 1,
      dropping the other → 0 and the entry is GC'd (peek == None). (b) overlay copies in_flight onto the snapshot;
      absent entry ⇒ 0. (c) `is_neutral` false while in_flight > 0, true at 0-with-no-other-signal. (d) a guard whose
      `RuntimeStates` still has the entry decrements exactly once even if... (guard is not Copy/Clone; assert single
      decrement). (e) saturating: a stray drop can't underflow below 0.
- [ ] **Step 2:** Run — fail. **Step 3:** Implement the field + guard + acquire + overlay + is_neutral. **Step 4:**
      Green.
- [ ] **Step 5:** Commit: `feat(server): leak-proof InFlightGuard + in_flight runtime field + overlay`

---

### Task 2: Hold the guard for the request's true lifetime (THE CRUX — adversarial review)

**Files:** `crates/polyflare-server/src/watchdog.rs` (embed the guard as a field of `ObservingStream` / thread it
through `wrap_stream`/`execute_*_tracked`), `crates/polyflare-server/src/ingress.rs` (acquire at each selection site;
release-on-reselect in `run_failover_loop`); tests.

**Read fully first:** `ObservingStream` struct + `poll_next` (`watchdog.rs:814-905`) and where it's built
(`wrap_stream`/`execute_recovery_tracked`/`execute_*_tracked`); `run_failover_loop` (`ingress.rs:1136-1291`) and the
~8 `selector.pick` + `record_selected` sites (scoping §4 lists them). Decide the smallest threading that moves the
acquired guard INTO the `ObservingStream` on a successful attempt.

**Implement:**
- Add an `_in_flight: Option<InFlightGuard>` FIELD to `ObservingStream` (name it to signal it's held-for-Drop, not
  read). Do NOT implement `Drop` on `ObservingStream` and do NOT touch `poll_next` — the field's own `Drop` fires
  when the stream is dropped (drain, disconnect, error, timeout, panic), which is exactly the release we want.
- Acquire the guard at the selection point (right after the account is chosen / `resolve_core_account`), thread it
  into the executor call so it lands in the `ObservingStream` that attempt returns. On a SUCCESSFUL attempt the
  guard moves into the returned stream (held until the client's response stream fully drops). On a FAILED pre-stream
  attempt the guard is dropped when that attempt's scope ends (before the next pick).
- `run_failover_loop`: each iteration acquires a guard for the account it's about to try; on `Err(e2)` (reselect),
  that iteration's guard drops (release A) before the loop picks B; on success the guard is inside the returned
  stream. Ensure NO path holds two guards for the same request simultaneously beyond the brief handoff, and NO path
  drops the guard before the stream that needs it.
- The non-streaming / unary control paths (D17) do NOT need a lease (they're not the concurrency-pressure target) —
  scope the guard to the `/responses` + `/v1/messages` streaming selection sites. Document which sites get a guard.

**Tests (the leak-proof proof — this is the crux):**
- **client disconnect releases:** build an `ObservingStream` holding a guard, then DROP it before polling to
  completion (simulate a disconnect) ⇒ the account's in_flight returns to 0. (Use the testkit stream seam; mirror
  the DropSpy pattern at `watchdog.rs:1508` if useful.)
- **clean drain releases:** poll the stream to `Poll::Ready(None)` then drop ⇒ in_flight 0, AND record_success still
  fired (wedge bookkeeping intact).
- **mid-stream error releases:** an Err arm ⇒ stream ends + guard drops ⇒ in_flight 0, record_transient_error fired.
- **failover releases A before B:** drive `run_failover_loop` A(fail)→B(succeed) ⇒ A's in_flight returns to 0, B's
  is 1 while its stream is held, 0 after it drops. No double-count, no leak on A.
- **wedge intact:** the 5 wedge + cyber + failover + starvation suites green; `poll_next`'s success/error/observe
  behavior unchanged (spot test).

- [ ] **Step 1:** Read the stream + failover paths. Write the disconnect-releases + failover-releases failing tests.
      **Step 2:** Run — fail. **Step 3:** Thread the guard (field + acquire + failover release). **Step 4:** Green;
      wedge/cyber/failover/starvation suites green.
- [ ] **Step 5:** Commit: `feat(server): hold InFlightGuard for the stream lifetime + release on failover reselect`

---

### Task 3: The soft penalty in the selector + config

**Files:** `crates/polyflare-core/src/select.rs` (fold `in_flight * penalty_pct` into `eff_used`/`eff_secondary_used`
at Candidate construction), `crates/polyflare-server/src/config.rs` (`POLYFLARE_INFLIGHT_PENALTY_PCT`, default 2.5,
`0`=disable, clamp; threaded into the SelectionCtx / passed to select), tests.

**Read first:** how `eligibility()` builds `eff_used`/`eff_secondary_used` (`select.rs:162-176`) and how B8's
health_tier / the effort penalty are threaded from config into the selection path (is there a penalty/config value
already on `SelectionCtx`? mirror it). The penalty pct must reach `select.rs` the same pure way (on `SelectionCtx`,
startup-resolved — NOT read from env inside pick).

**Implement:**
- `eff_used = min(100.0, used_percent + in_flight * penalty_pct)` and the same for `eff_secondary_used` — mirroring
  codex-lb `:2251-2263` exactly. This flows automatically into `remaining_secondary_credits`/`weighted_pick`. It's a
  scoring-input change, NOT a new filter/pass — a busy account gets less weight but stays eligible.
- **CAUTION (from scoping §7.3):** folding in_flight into `eff_used` means concurrency pressure can push a busy-but-
  healthy account's `eff_used` past B8's `should_drain` threshold (≥85). That is codex-lb's intentional design
  (pressure feeds the same used_percent the drain reads). Confirm this is the desired coupling and TEST it (a heavily
  in-flight account can enter DRAINING via pressure) — or, if the plan/reviewer decides the drain signal should read
  RAW used_percent (not pressure-inflated), keep the penalty confined to the WEIGHT and not the should_drain input.
  **Decide explicitly and document.** (Recommendation: match codex-lb — let pressure feed eff_used uniformly; it's
  the simpler, reference-faithful choice and a busy account SHOULD be drain-preferred-away.)
- Config: `inflight_penalty_pct: f64` on `ServeConfig` from `POLYFLARE_INFLIGHT_PENALTY_PCT` (unset ⇒ 2.5, `0` ⇒
  disabled, malformed ⇒ 2.5, clamp `[0, e.g. 50]`). Thread onto `SelectionCtx` (startup-resolved).

- [ ] **Step 1:** Failing tests: (a) two otherwise-equal accounts, one with in_flight=4 ⇒ the LESS busy one wins the
      weighted pick (over enough seeds / deterministically). (b) penalty=0 ⇒ in_flight has zero effect (rollback).
      (c) a fully-busy pool still selects someone (never empties). (d) config parse (unset⇒2.5, 0⇒0, absurd⇒clamp,
      malformed⇒2.5). (e) the should_drain coupling decision (per above) is asserted whichever way it's decided.
- [ ] **Step 2:** Run — fail. **Step 3:** Implement the fold + config + threading. **Step 4:** Green; the wedge/
      starvation/health suites green (additive scoring input).
- [ ] **Step 5:** Commit: `feat(core): in_flight soft penalty in capacity weighting + POLYFLARE_INFLIGHT_PENALTY_PCT`

---

### Task 4: Content-free lease observability + concurrent-load e2e

**Files:** `crates/polyflare-server/src/observability.rs` (content-free lease metrics — an acquire/release counter +
an in_flight gauge, mirroring the existing metrics idiom), wire the counters at acquire/release, an e2e.

**Implement:**
- Content-free `LeaseMetrics` (AtomicU64 acquired/released counters) on `AppState`, bumped in `acquire_in_flight` /
  the guard `Drop`. Optionally a gauge readout. NO new content — account id + counts only. (Keep it minimal; the
  crux was Tasks 1-2.)
- e2e: through the real selection path, simulate one account holding several in-flight leases (acquire guards) and
  assert a new selection prefers a less-busy account (end-to-end soft-penalty), and that releasing the guards
  restores balance. Assert the leak-proof property once more at the e2e level if cheap (a dropped request's lease is
  reclaimed).

- [ ] **Step 1:** Failing e2e + metric assertion. **Step 2:** Run — fail. **Step 3:** Implement metrics + wire + e2e.
      **Step 4:** Green; ALL suites green; clippy clean.
- [ ] **Step 5:** Commit: `feat(server): content-free in-flight lease metrics + concurrent-load selection e2e`

---

## Suggested order

1 (guard + field + overlay) → 2 (hold-for-lifetime, crux, adversarial review) → 3 (soft penalty + config) → 4
(observability + e2e). After Task 4, C9 (soft-penalty v1) is done: concurrent load spreads across accounts via a
leak-proof in-flight lease that releases on every exit path (disconnect/panic/failover included), never empties the
pool, never overrides ownership, and keeps `pick` pure. Mark C9 DONE in `PORTING-CODEXLB.md` (soft-penalty v1; hard
cap deferred). Follow-ups (not this plan): the hard concurrency cap (needs a pigeonhole/fallback decision to avoid
pool-emptying); a TTL-reclaim sweep as belt-and-suspenders (the Drop guard is leak-proof, so this is optional defense-
in-depth for a leaked-guard wiring bug); C11 Prometheus `account_lease_*` surface.
