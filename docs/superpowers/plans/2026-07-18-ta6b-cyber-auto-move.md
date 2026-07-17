# TA6(b) Cyber Capability Auto-Move Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use `- [ ]`.

**Goal:** A pinned/continuity-owned session that hits a `cyber_policy` rejection its owner can't serve MOVES to
a capability-holding account (re-homing ownership) and becomes sticky-cyber — instead of getting stuck failing
on its owner (the codex-lb bug the user hit). Plus the proactive path: an operator-set capability, a
required-capability resolver, and a dedicated cyber pool.

**Architecture:** Reuse what exists (the `security_work_authorized` account field, the selector's capability
pre-filter, the `record_recovery` re-home machinery). Build the missing links: surface the streamed
`response.failed` `cyber_policy` code into the failure signal, add a reactive move trigger, persist a
sticky-cyber flag, add operator/resolver/pool plumbing.

**Authority — the design + the scoping:** `DESIGN-DECISIONS.md` TA6 / TA6(a) / TA6(b) (the approved design).
The scoping study (this session) established: wire signal is `error.code == "cyber_policy"`
(`codex-rs/codex-api/src/sse/responses.rs:637-639`), NOT message-only; and the build inventory below.

## Global Constraints

- **Wire truth: `error.code == "cyber_policy"`** (codex-rs). NOT codex-lb's `security_work_authorization_required`
  (its own synthetic code). Detect on the CODE, content-safe (never the message) — the existing `extract_error_code`
  shape is correct.
- **Security floor (inviolable):** cyber work is NEVER served on a non-authorized account. No capability-holder
  available ⇒ a clear "no authorized account available" error, NEVER unfiltered failover onto a non-authorized
  account. Moving TO an authorized account is the only permitted routing of a cyber-flagged request.
- **The wedge fix is sacred.** Task 1 touches the streaming relay (`ObservingStream`/watchdog) where
  `Continuity::observe` fires and the wedge fix lives. The 5 wedge suites (`wedge_regression`/`watchdog_race`/
  `no_anchor_failover`/`signal_client`/`failure_routing`) MUST stay green at EVERY task. Task 1 gets adversarial
  review.
- **Content-safety:** the capability flag + the code are safe; never read/store/log a frame's message or body.
- **Reuse, don't reinvent:** the selector filter (`select.rs:294,454`), `record_recovery`
  (`continuity_repo.rs:188`), and the `ResendFull` reselect (`ingress.rs:632-663`) already exist — wire into
  them, don't duplicate.
- Verify each task: `cargo test --workspace` green + `cargo clippy --workspace --all-targets -- -D warnings`.

## Build inventory (from scoping — what exists vs builds)

| Piece | Status |
|---|---|
| Account `security_work_authorized` field | EXISTS (`migrations/0001:28`, `types.rs:303-304`) — but no operator write path |
| Selector capability pre-filter | EXISTS (`select.rs:294,454`, driven by `SelectionCtx.require_security_work_authorized`) |
| `record_recovery` re-home + `ResendFull` reselect | EXISTS (`continuity_repo.rs:188`, `ingress.rs:632-663`) — needs a new trigger |
| Streamed `response.failed` code → failure signal | ABSENT (executor extracts codes only from non-2xx; cyber is a 200-OK stream frame) |
| Reactive move trigger on `cyber_policy` | ABSENT (an `Err` today → 502, no reselect) |
| Session sticky-cyber flag | ABSENT (no column on `continuity_sessions`) |
| Required-cap resolution (header/alias/pool) | ABSENT (`require_security_work_authorized` hardcoded false) |
| Operator capability toggle | ABSENT (not in `AccountPatch`/CLI) |
| Cyber pool (pool-level required_capability) | ABSENT (no pool metadata) |

---

### Task 1: Surface streamed `response.failed` `cyber_policy` into a capability-failure signal (THE CRUX — wedge-adjacent)

**Files:** `crates/polyflare-server/src/watchdog.rs` (the `ObservingStream` that already peeks the stream for
`response.id` and fires `observe` at end); possibly `crates/polyflare-core/src/types.rs`; test
`crates/polyflare-server/tests/` (new).

**The problem:** `cyber_policy` arrives in a `response.failed` SSE frame on a 200-OK stream. The executor returns
`Ok(stream)`, so `extract_error_code` (non-2xx only) never sees it and it streams through to the client. We must
detect the `response.failed` frame's `error.code == "cyber_policy"` DURING relay and surface it as an actionable
capability-rejection signal — WITHOUT breaking the wedge fix, and WITHOUT relaying a broken response to the
client when we intend to reroute.

**Design (mirror M3's peek-before-relay):** The `ObservingStream` already inspects frames (`ResponseIdSniffer`).
Extend the inspection to recognize a terminal `response.failed` carrying `error.code == "cyber_policy"` (code
only — never the message). A policy rejection fails the turn BEFORE producing output, so in the common case the
`response.failed` is the first meaningful frame — detect it before relaying content. Surface a distinct signal
(e.g. a `CapabilityRejection { capability }` variant on the recovery/outcome path, or a
`WatchdogError`/`ExecError` the ingress can branch on) so Task 2's trigger can act. **Do NOT reroute in this
task** — only DETECT + surface. If content was already relayed before the failure (rare), fall back to
today's behavior (pass the failure through) — never double-relay.

**Content-safety:** extract only `error.code`; the `response.failed` message is content-adjacent — never capture it.

- [ ] **Step 1:** Read `watchdog.rs`'s `ObservingStream`/`ResponseIdSniffer` + how `observe`/terminal detection
      works fully. Write a failing test: a mock upstream streams a `response.failed` frame with
      `error.code=="cyber_policy"` on a 200 stream ⇒ the ingress path surfaces a capability-rejection signal
      (not a plain pass-through), AND asserts the frame's `message` never appears in any error/log.
- [ ] **Step 2:** Run — fails (today it passes through). **Step 3:** Implement detect+surface (peek-before-relay
      for the rejection; no reroute yet). **Step 4:** Green — and CRITICALLY the 5 wedge suites all still pass
      (a `response.completed` still fires `observe`; a non-cyber `response.failed` still behaves as before).
- [ ] **Step 5:** Commit: `feat(server): detect streamed cyber_policy rejection (peek-before-relay)`

---

### Task 2: The reactive move trigger — reselect to a cyber-capable account + re-home + security floor

**Files:** `crates/polyflare-server/src/ingress.rs` (the recovery/`Err` handling around the executor call);
reuse `RecoveryPlan::ResendFull`, `execute_recovery`, `record_recovery`. Test in `tests/`.

**Interfaces — Consumes:** Task 1's capability-rejection signal.

On a capability-rejection signal for a session (pinned/owned OR not — a cyber rejection means the current account
can't serve it regardless): build the anchor-stripped full-resend request, re-select with
`SelectionCtx.require_security_work_authorized = true` (the selector filter already exists), `execute_recovery`
on the chosen cyber-capable account, and `record_recovery` to re-home ownership. This reuses the EXACT machinery
`RouteDecision::Recover`/`ResendFull` already uses — the new part is the trigger + threading the capability flag.

**Security floor:** if the capability-filtered re-select yields NO account ⇒ return a clear
"no authorized account available for security work" error to the client. **NEVER** retry unfiltered / on a
non-authorized account.

- [ ] **Step 1:** Failing tests: (a) owner rejects `cyber_policy`, a cyber-capable account EXISTS ⇒ the request
      is re-run on the capable account (assert the second attempt went to a `security_work_authorized` account,
      the client got a clean stream, ownership re-homed via `record_recovery`). (b) NO capable account ⇒ a clear
      error, and assert NO attempt was made on a non-authorized account (the security floor).
- [ ] **Step 2:** Run — fails. **Step 3:** Implement the trigger reusing ResendFull+record_recovery with the cap
      filter. **Step 4:** Green; wedge suites green (this is a NEW trigger, must not alter the existing
      Recover/silence triggers).
- [ ] **Step 5:** Commit: `feat(server): auto-move a cyber-rejected request to a capable account (TA6b core)`

---

### Task 3: Persist the sticky-cyber flag so later turns pre-filter

**Files:** new migration `crates/polyflare-store/migrations/0008_session_capability.sql` (a
`required_capabilities` / `sticky_cyber` column on `continuity_sessions`); `continuity_repo.rs` (accessors);
`crates/polyflare-server/src/continuity.rs` (stamp on move in `observe`/recovery; read in `prepare` to set
`require_security_work_authorized` for the turn).

- [ ] **Step 1:** Failing test: after a cyber move, the session row carries the sticky-cyber flag; a SUBSEQUENT
      turn on that session pre-filters to cyber-capable accounts (assert `require_security_work_authorized` is
      set for turn 2 without a second rejection — i.e. the cost is paid once).
- [ ] **Step 2:** Run — fails. **Step 3:** Implement the column + accessors + stamp-on-move + read-on-prepare.
      Forward-only migration; content-free (a capability enum, not conversation data). **Step 4:** Green.
- [ ] **Step 5:** Commit: `feat(store): persist sticky-cyber on the session so later turns pre-filter`

---

### Task 4: Operator-set capability toggle (dashboard/CLI can mark an account cyber-capable)

**Files:** `crates/polyflare-store/src/account.rs` (`update_security_work_authorized` repo method);
`crates/polyflare-server/src/write_api.rs` (`AccountPatch` gains the field — it's auth-gated already);
`crates/polyflare-server/src/main.rs` (a CLI setter, mirroring `accounts set-pool`).

- [ ] **Step 1:** Failing test: the PATCH (and/or the repo method) flips `security_work_authorized` and it's
      reflected in the next snapshot (generation bump). **Step 2:** Run — fails. **Step 3:** Implement the repo
      method + PATCH field + CLI. **Step 4:** Green.
- [ ] **Step 5:** Commit: `feat(server): operator-settable security_work_authorized (TA6)`

---

### Task 5: Required-capability resolution (proactive: header + cyber pool)

**Files:** `crates/polyflare-server/src/ingress.rs` (resolve `require_security_work_authorized` from a
`X-PolyFlare-Capability: security_work` header AND from a cyber-tagged pool); `crates/polyflare-server/src/config.rs`
(a pool→required-capability map, analogous to `parse_pool_strategies`).

This is TA6(a)'s proactive precedence: a request routed to a cyber-tagged pool (`/cyber/responses`) or carrying
the capability header pre-filters to cyber accounts from turn 1 — zero wasted round-trips. **Self-enforcing:** the
pool tag declares the requirement; the account flag declares who satisfies it.

- [ ] **Step 1:** Failing tests: a request to a cyber-tagged pool resolves `require_security_work_authorized=true`
      (pre-filters from turn 1, no rejection needed); same for the header. A non-cyber pool/request stays false
      (regression). **Step 2:** Run — fails. **Step 3:** Implement the pool-cap config + header parse + ingress
      resolution. **Step 4:** Green.
- [ ] **Step 5:** Commit: `feat(server): resolve required capability from cyber pool + header (TA6a proactive)`

---

## Suggested order

1 → 2 → 3 (the reactive core — fixes the user's actual stuck-session pain; Task 1 is the wedge-adjacent crux and
gets adversarial review) → then 4 → 5 (the proactive pieces). Task 1 and 2 are the highest-risk/highest-value;
3-5 are additive. After 5, TA6(b) is complete: reactive auto-move + sticky + operator toggle + proactive cyber
pool, all on the strict security floor.
