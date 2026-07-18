# B10 — Anti-Thundering-Herd: Stagger Layer-2 Waiter Wake-Ups Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use `- [ ]`.

**Goal:** Stop N concurrent clients that are all waiting (B5 Layer-2) on the SAME rate-limited account from waking
in lockstep the instant it recovers and re-selecting onto it simultaneously — a self-inflicted thundering herd that
can immediately re-429 the just-recovered account. Desynchronize the waiters with a small, bounded, per-request
jitter added to each waiter's own wake target, so they re-select spread out over a short window instead of all at once.

**Architecture:** A deterministic-per-request jitter offset `[0, POLYFLARE_STARVATION_WAKE_JITTER_MS]` added to each
waiter's `target_ms` inside `layer2_wait_stream` (`ingress.rs`), capped so it never exceeds the wait budget deadline.
The jitter lives entirely on the ingress/stream side — it does NOT touch `select.rs` (pick stays pure), does NOT
touch the account's stored `recover_at`/`cooldown_until` (so `soonest_recover`'s cross-account ordering — B5's
anti-starvation fairness contract — is unchanged), and does NOT change WHICH account a waiter waits on. Default off
(0) = today's exact B5 behavior; a positive value spreads the wake-ups.

**Authority — the B10 scoping study (this session), file:line cites + the reframe:**
- Requirement `docs/PORTING-CODEXLB.md:158-164` (B10). codex-lb refs `core/utils/retry.py:71-77` (per-request
  same-account retry-delay jitter `200ms·2^(attempt-1)·uniform(0.9,1.1)`), `_service/streaming/retry.py:166`
  (`_iter_account_capacity_recovery_wait` — bounded wait + 10s keepalive), `_service/support.py:43-142`.
- **REFRAME (why the literal port doesn't apply):**
  - **Item (a) [same-account retry jitter] = N/A.** PolyFlare's B4 `run_failover_loop` (`ingress.rs:1037-1200+`)
    does NO same-account retry — on any retryable failure it `tried.insert(failed_id)` and re-picks a DIFFERENT
    account immediately (`ingress.rs:1082-1085`), no inter-attempt sleep, no `watchdog.rs` backoff site. There is
    nowhere to attach codex-lb's inter-attempt jitter; attaching it would require inventing a same-account retry
    loop B4 deliberately replaced with cross-account failover. **Explicitly out of scope — documented as N/A.**
  - **Item (b) [wait-with-heartbeat instead of immediate 503] ≈ already built by B5.** `try_layer2_recovery_wait`
    / `layer2_wait_stream` (`ingress.rs:631-850`) already replaces the empty-pool fail with a bounded `[1s,300s]`
    wait derived from `soonest_recover`'s `recover_at` (`select.rs:277-293`), chunked into keepalive SSE ticks.
  - **The actual herd B10 must damp (scoping §5):** every waiter on the same `wait_target` computes an IDENTICAL
    `target_ms = min(recover_at_ms, budget_deadline_ms)` (`ingress.rs:759-764`) — shared `recover_at`, no per-request
    offset — so all N wake within one heartbeat tick and `snapshots()`+`overlay`+`pick` at nearly the same instant
    (`ingress.rs:797-833`): a synchronized re-select storm on the recovery moment.
- **Purity / ordering constraints (from the scoping):** `pick`/`eligibility`/`soonest_recover` are pure (no clock/
  rand — `select.rs:1-4`, M2-GATE1). Jitter must NOT live there. `soonest_recover`'s `min_by_key(recover_at)`
  (`select.rs:292`) must stay deterministic across ACCOUNTS (fairness). `backoff_secs` (`runtime_state.rs:100-102`)
  must stay STABLE per `(error_count,last_error_at)` — the code comment says so; the write-side cooldown jitter is
  therefore a SEPARATE deferred follow-up, NOT this plan.
- Config/observability idioms: `POLYFLARE_STARVATION_WAIT_BUDGET_SECS`/`_HEARTBEAT_SECS` startup-resolved into
  `ServeConfig` (`config.rs:251-306`); content-free `StarvationSignal` + metrics (`observability.rs`). The B5 stream
  test seam uses a fast clock (`ingress.rs:1361-1366`) — mirror it.

## Global Constraints

- **Jitter lives ONLY on the waiter's own wake schedule (inviolable).** It offsets THIS request's `target_ms` in
  `layer2_wait_stream`. It MUST NOT touch: `select.rs` (pick stays pure), the account's stored
  `recover_at`/`cooldown_until`/`backoff_secs` (stable selector inputs), or WHICH account is waited on
  (`soonest_recover`'s answer is unchanged). A reviewer must be able to confirm `select.rs` and `runtime_state.rs`'s
  backoff are untouched.
- **Bounded + never past budget.** `jittered_target = min(target_ms + jitter, budget_deadline_ms)` where
  `jitter ∈ [0, wake_jitter_ms]`. The wait must still honor the B5 budget ceiling — jitter can only add delay WITHIN
  the budget, never extend past it. A waiter whose `recover_at` already sits at the budget deadline gets ~no jitter
  room (fine — it just waits the full budget).
- **Only spreads LATER, never earlier.** The account isn't recovered before `recover_at`; jitter adds `[0, J]` of
  extra delay beyond it. Never wake a waiter before its computed `target_ms`.
- **Deterministic-per-request (for testability), not process-global rand.** Derive the offset deterministically from
  a per-request identifier already in scope (the session key, or a request id/counter) hashed into `[0, wake_jitter_ms]`
  — so two DIFFERENT waiters on the same account get DIFFERENT offsets (desync), but a given request is reproducible
  in a test. (If no stable per-request id is in scope at the wait site, a single bounded `rand` draw at wait-entry is
  acceptable — but prefer the deterministic hash for a testable seam.)
- **Disable lever = default.** `POLYFLARE_STARVATION_WAKE_JITTER_MS=0` (the DEFAULT) ⇒ zero offset ⇒ byte-for-byte
  today's B5 behavior. A positive value enables spreading. Clamp to a sane ceiling (e.g. ≤ the heartbeat interval, or
  ≤ some absolute like 30_000ms) so a hostile/huge value can't blow the budget.
- **Wedge/starvation intact.** This only changes WHEN a Layer-2 waiter re-selects, not whether/what. The 5 wedge +
  cyber + failover + starvation suites MUST stay green; B5's soonest-account fairness + budget ceiling + keepalive
  content-safety all preserved.
- **Content-safety:** any new signal/log is content-free (account id + counts/durations + fixed labels only).
- Verify each task: `cargo test --workspace` green + `cargo clippy --workspace --all-targets -- -D warnings`.

---

### Task 1: The per-waiter wake jitter + config (THE CRUX — adversarial review)

**Files:** `crates/polyflare-server/src/ingress.rs` (`layer2_wait_stream` / the `target_ms` computation ~759-773 and
wherever the sleep-target is derived), `crates/polyflare-server/src/config.rs`
(`POLYFLARE_STARVATION_WAKE_JITTER_MS`, default 0, clamp, startup-resolved into `ServeConfig` + threaded to
`AppState`), tests.

**Read fully first:** `try_layer2_recovery_wait` + `layer2_wait_stream` (`ingress.rs:631-850`) — how `target_ms` is
computed from `recover_at_ms` and the budget deadline, how the heartbeat-chunked sleep loop works, how it re-selects
after the wait, and the fast-clock test seam (`ingress.rs:1361-1366`). Determine the per-request identifier available
at the wait site (session key? a request id?) for the deterministic offset.

**Implement:**
- A pure helper `wake_jitter_offset_ms(request_key: &str, wake_jitter_ms: u64) -> u64` returning a deterministic
  value in `[0, wake_jitter_ms]` (hash the key, mod (wake_jitter_ms+1)). `wake_jitter_ms == 0 ⇒ always 0`.
- In `layer2_wait_stream`, compute `jitter = wake_jitter_offset_ms(request_key, cfg.wake_jitter_ms)` ONCE at wait
  entry, and set `jittered_target = (target_ms + jitter).min(budget_deadline_ms)`. Use `jittered_target` in place of
  `target_ms` for the sleep-loop deadline. Do NOT re-draw per heartbeat (the offset is per-wait, stable).
- `config.rs`: `wake_jitter_ms: u64` on `ServeConfig` from `POLYFLARE_STARVATION_WAKE_JITTER_MS` — unset/malformed ⇒
  0, clamp to `[0, CEIL]` (pick a sane CEIL, e.g. 30_000). Thread onto `AppState`/`ServeConfig` the SAME way
  `starvation_wait_budget`/`starvation_heartbeat` are (startup-resolved, not per-request).

- [ ] **Step 1:** Failing tests: (a) `wake_jitter_offset_ms`: two DIFFERENT keys with `wake_jitter_ms=1000` produce
      offsets in `[0,1000]` and (for at least one pair) DIFFERENT values (desync); `wake_jitter_ms=0 ⇒ 0` for any
      key; same key ⇒ same offset (deterministic). (b) the target math: `target_ms + jitter` capped at
      `budget_deadline_ms` (a jitter that would exceed the budget is clamped to the budget, never past). (c) config:
      `POLYFLARE_STARVATION_WAKE_JITTER_MS` unset ⇒ 0, `=250` ⇒ 250, absurd `=999999` ⇒ clamped to CEIL, malformed ⇒
      0. (d) a stream-level test (mirror B5 Task 4's fast-clock seam): TWO waiters on the SAME account with
      jitter>0 compute DIFFERENT wake targets (assert the two target deadlines differ), and with jitter=0 compute the
      SAME target (today's behavior). Assert the waited-on ACCOUNT is identical in both cases (jitter never changes
      which account — only when).
- [ ] **Step 2:** Run — fail. **Step 3:** Implement the helper + the target math + config. **Step 4:** Green; the 5
      wedge/cyber/failover/starvation suites green (soonest_recover/select.rs untouched; only the per-waiter deadline
      shifts).
- [ ] **Step 5:** Commit: `feat(server): per-waiter wake jitter to desync Layer-2 recovery waiters (B10)`

---

### Task 2: Content-free observability + concurrent-waiter e2e

**Files:** `crates/polyflare-server/src/observability.rs` (extend the existing `StarvationSignal` with a content-free
jitter field, OR add a tiny counter — smallest thing that lets an operator see spreading is active; mirror the
existing content-free idiom), the emit at the wait site, an e2e/integration test (extend the starvation e2e or a new
one). Keep this SMALL — the crux is Task 1; this proves the end-to-end property + gives one observable.

**Read first:** how `StarvationSignal`/`StarvationMetrics` are shaped + emitted (`observability.rs`,
`tests/starvation_*`), and the existing Layer-2 e2e harness.

**Implement:**
- The minimal content-free observable: e.g. record the applied `wake_jitter_ms` (the configured window, a fixed
  number — NOT any body) on the existing starvation signal, or a `wake_jitter_applied` count. No new content.
- e2e: through the real Layer-2 wait path (fast clock), drive TWO concurrent waiters onto the same recovering account
  with `wake_jitter_ms > 0` and assert they re-select at DIFFERENT (spread) times rather than the same instant — the
  end-to-end herd-damping property — while BOTH still ultimately get served (no waiter is starved or pushed past
  budget). With `wake_jitter_ms = 0`, assert the prior lockstep behavior (baseline unchanged).

- [ ] **Step 1:** Failing e2e + the content-free-signal assertion. **Step 2:** Run — fail. **Step 3:** Implement the
      observable + wire the e2e. **Step 4:** Green; ALL suites green; clippy clean.
- [ ] **Step 5:** Commit: `feat(server): herd-damping observability + concurrent Layer-2 waiter e2e (B10)`

---

## Suggested order

1 (jitter + config, crux, adversarial review) → 2 (observability + e2e). After Task 2, B10 (reframed) is done:
concurrent Layer-2 waiters on the same recovering account wake spread across a bounded window instead of stampeding
it in lockstep, with `pick`/`soonest_recover`/backoff all untouched and a clean disable default. Mark B10 DONE in
`PORTING-CODEXLB.md` **with the reframe noted** (item (a) same-account-retry jitter = N/A under B4's cross-account
failover design; write-side cooldown-desync jitter deferred to keep `backoff_secs` stable). Follow-ups (not this
plan): the write-side cooldown jitter (new knob, must not destabilize the selector's deterministic backoff read);
extending the Layer-2 wait to the Anthropic `/v1/messages` empty-pool sites (a B5-completeness gap, `ingress.rs:1935-1939`,
`2057-2061`); C9 in-flight lease accounting (the orthogonal concurrent-convergence herd dimension).
