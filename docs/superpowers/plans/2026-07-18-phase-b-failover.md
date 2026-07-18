# Phase B / B4 — Cross-Account Failover Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use `- [ ]`.

**Goal:** When a request fails on its selected account with a *retryable* failure, PolyFlare transparently
retries on the NEXT eligible account (bounded, excluding tried accounts) instead of returning 502 and making
the client retry. This is the porting doc's biggest remaining behavioral gap (`PORTING-CODEXLB.md` §B4).

**Architecture:** Generalize the (battle-tested) `reroute_cyber_rejection` single reselect→`execute_recovery`
→relay step into a bounded (≤3) loop wrapping the executor call in ingress, threading a tried-account exclusion
set through selection, gated by a new retryable-vs-terminal classifier and a committed/downstream-visible flag.
Reuse: `execute_recovery` (the anchor-strip resend), `record_failure` (Phase A health writeback), `record_recovery`
(re-home). Anti-starvation soonest-to-recover (B5) is a SEPARATE follow-on — B4 fails fast on exhaustion.

**Tech Stack:** Rust; reuse Phase A `record_failure`, TA6(b)'s reroute template, `execute_recovery`.

**Authority — the scoping study (this session):** confirmed no N-account loop today (one-shot only); the four
absent pieces (loop, exclusion, classifier, committed-flag); the two riskiest interactions below. codex-lb
reference: `_service/streaming/retry.py` (`_stream_with_retry`), `core/balancer/logic.py:1156` (`failover_decision`),
`service.py:859` (`_STREAM_MAX_ACCOUNT_ATTEMPTS=3`).

## Global Constraints

- **SECURITY FLOOR — HARD, do NOT port codex-lb's degradation (inviolable).** codex-lb's loop, when no
  security-authorized account remains, DROPS `require_security_work_authorized` and continues on a normal
  account (`retry.py:698-717`). **PolyFlare must NOT.** Every failover iteration re-clones the `SelectionCtx`
  with `require_security_work_authorized` INTACT; exhaustion under a cyber requirement returns the distinct
  `no_authorized_account_for_security_work` 503 (`ingress.rs:74-80`), NEVER a relaxed/unfiltered retry. Copying
  the reference loop verbatim would breach the floor TA6(b) proved airtight. A test must assert a cyber request
  exhausts to the security 503 and never touches a non-authorized account across ALL attempts.
- **COMMIT BARRIER — never double-relay (inviolable correctness).** The loop may retry ONLY before the first
  byte reaches the client. Once ANY response byte is relayed downstream ("committed"), a mid-stream failure is
  surfaced in-band (the stream errors to the client) and is NEVER replayed on another account — replaying would
  give the client a second, irreconcilable response. Peek-before-relay (from the cyber work, `watchdog.rs:134-156`)
  already makes first-byte state knowable; B4 surfaces it. This is the same invariant the cyber peek-before-relay
  relied on, generalized.
- **CONTINUITY OWNERSHIP — a live-anchor pinned turn stays fail-closed.** A request with a LIVE anchor
  (`previous_response_id`, owner-pinned) must NOT be blindly resent to a new account — that re-homes ownership
  incorrectly and re-opens the wedge. The loop only iterates when the request is anchorless OR a post-strip
  `ResendFull` (a self-sufficient replay body). Gate on `apply_ownership`'s result + the recovery plan
  (`PORTING-CODEXLB.md:133-135`). A live-anchor turn whose owner fails mid-flight surfaces (today's behavior),
  it does not fan out.
- **The wedge fix is sacred.** The loop wraps the executor/watchdog path where `Continuity::observe` and the
  wedge fix live. The 5 wedge suites (`wedge_regression`/`watchdog_race`/`no_anchor_failover`/`signal_client`/
  `failure_routing`) MUST stay green at EVERY task. The loop task gets adversarial review.
- **Bound = 3 attempts** (codex-lb `_STREAM_MAX_ACCOUNT_ATTEMPTS`, `PORTING-CODEXLB.md:128`). Configurable via
  `POLYFLARE_MAX_ACCOUNT_ATTEMPTS` (default 3); setting 1 = today's one-shot behavior (the degenerate case, a
  clean rollback lever). No thundering-herd beyond the bound (B10 is a later item).
- **Exclusion:** a failed account is excluded from subsequent picks THIS request (never re-picked). Prefer
  pre-filtering the snapshot slice by tried ids before each `pick` (mirrors `apply_ownership`'s narrowing) over a
  `Selector` trait change — cheaper, no ripple.
- **Content-safety:** classification reads status/error-code only; never a body/message. Reuse Phase A's buckets.
- Verify each task: `cargo test --workspace` green + `cargo clippy --workspace --all-targets -- -D warnings`.

---

### Task 1: The failover verdict classifier (retryable-vs-terminal)

**Files:** `crates/polyflare-server/src/` (a new `failover.rs` or in `ingress.rs`); test alongside.

**Interfaces — Produces:** `fn failover_verdict(err: &WatchdogError, attempts_left: bool, committed: bool) -> FailoverVerdict`
where `FailoverVerdict::{Surface, FailoverNext}`. Ports codex-lb `failover_decision` (`logic.py:1156-1168`) over
PolyFlare's Phase A buckets:
- `committed` (a byte already relayed) ⇒ **Surface** (never replay — the commit barrier).
- no attempts left ⇒ **Surface**.
- retryable class — 429 (rate_limit), 5xx/401/403/408 (transient), transport/mid-stream-drop, AND permanent-auth
  codes (account-terminal but REQUEST-retryable — another account can serve) ⇒ **FailoverNext**.
- request-terminal — 400/404/422 (bad request; retrying won't help), and a genuine content/quota terminal ⇒ **Surface**.
- `CapabilityRejection` is NOT handled here (TA6(b) owns its own reroute) — assert it's excluded/unreachable.

Map the classes off the SAME signals `record_failure` (`ingress.rs:110-132`) uses — do NOT invent a second
classification of the same failure (drift risk). Reuse `classify_failure` for the auth codes.

- [ ] **Step 1:** Failing tests, one per class, asserting the verdict: 429→FailoverNext, 5xx→FailoverNext,
      invalid_grant(auth)→FailoverNext (account-terminal, request-retryable), 400→Surface, committed=true→Surface
      (even for a 429), attempts_left=false→Surface. Real values, each fails first.
- [ ] **Step 2:** Run — fail. **Step 3:** Implement the pure function. **Step 4:** Green.
- [ ] **Step 5:** Commit: `feat(server): failover verdict classifier (retryable vs terminal)`

---

### Task 2: Tried-account exclusion in selection

**Files:** `crates/polyflare-server/src/ingress.rs` (the selection call sites) + wherever snapshots are narrowed;
test.

**Interfaces — Produces:** a way to exclude a set of `AccountId` from a `selector.pick`. Prefer: a helper that
filters the `&[AccountSnapshot]` slice removing tried ids before `pick` (mirror `apply_ownership`'s narrowing at
`watchdog.rs:79-83`). No `Selector` trait change if avoidable.

- [ ] **Step 1:** Failing test: given a tried-set containing account A, selection over a pool {A,B} returns B
      (A excluded); tried-set {A,B} over {A,B} returns None (all excluded). Real ids.
- [ ] **Step 2:** Run — fail. **Step 3:** Implement the pre-filter helper. **Step 4:** Green — and confirm an
      empty tried-set reproduces today's selection exactly (no behavior change when not failing over).
- [ ] **Step 5:** Commit: `feat(server): exclude tried accounts from failover reselection`

---

### Task 3: Surface the committed/downstream-visible flag on the failure path

**Files:** `crates/polyflare-server/src/watchdog.rs` (the `WatchdogError` / the executor→ingress error path) — **wedge
territory, trace carefully.**

**The problem:** the loop must know whether the first byte was already relayed (committed) when a failure
occurs — a committed failure CANNOT be retried (commit barrier). Peek-before-relay already distinguishes
Armed-first-frame vs relayed (`watchdog.rs:134-156`), but the failure surfaced to ingress doesn't carry
"was anything relayed?". Surface it: e.g. a `committed: bool` on the relevant `WatchdogError` path, or a distinct
error shape, so ingress's loop can branch. A failure that occurs BEFORE any relay ⇒ `committed=false` (retryable);
a mid-stream failure AFTER a byte ⇒ `committed=true` (surface in-band).

- [ ] **Step 1:** Read the full watchdog error path + how mid-stream errors (`ObservingStream::poll_next`,
      `record_transient_error` at `watchdog.rs:393-403`) surface. Failing test: a pre-relay failure reports
      `committed=false`; a mid-stream failure after ≥1 relayed byte reports `committed=true`.
- [ ] **Step 2:** Run — fail. **Step 3:** Implement (surface the first-byte state onto the error path — do NOT
      alter relay/observe behavior; only ADD the signal). **Step 4:** Green; 5 wedge suites green (a `completed`
      still observes, a non-committed failure still records health).
- [ ] **Step 5:** Commit: `feat(server): surface committed/downstream-visible state on failure`

---

### Task 4: The bounded failover loop (THE CRUX — adversarial review)

**Files:** `crates/polyflare-server/src/ingress.rs` (the `RouteDecision::Route` executor call + the recovery
arms). Consumes Tasks 1-3.

**Interfaces:** wrap the executor call in a bounded loop (≤`POLYFLARE_MAX_ACCOUNT_ATTEMPTS`, default 3). Generalize
`reroute_cyber_rejection`'s body (`ingress.rs:459-526`):
- On `Ok(stream)` → relay (done).
- On `Err(e)`: `record_failure(e)` (health writeback, unchanged), then `failover_verdict(&e, attempts_left, committed)`:
  - **Surface** ⇒ return the appropriate error (today's 502/503/in-band) — the loop ends.
  - **FailoverNext** ⇒ add the failed account to the tried-set, reselect over the pool MINUS tried (Task 2) with
    the SAME `SelectionCtx` (capability flag INTACT — clone-and-preserve), `execute_recovery` (anchor-stripped)
    on the fresh account, loop.
- **Security floor:** the reselect keeps `require_security_work_authorized`; if the filtered reselect returns None
  AND the request required the capability ⇒ the distinct security 503 (NEVER an unfiltered retry). If it required
  no capability and the pool is exhausted ⇒ today's 503/502.
- **Commit barrier:** only enter FailoverNext when `committed=false`. A committed failure ⇒ Surface (in-band).
- **Continuity:** only loop anchorless / post-strip `ResendFull` requests. A live-anchor pinned turn ⇒ do NOT
  fan out (fail-closed / surface), per the Global Constraint.
- **Do not add machinery TA6(b)/the watchdog already own:** the cyber reroute (`CapabilityRejection`) keeps its
  own path; the loop handles the general retryable failures. Ensure they compose, not conflict (a cyber rejection
  inside the loop still routes to the capability-filtered reroute).

- [ ] **Step 1:** Failing tests: (a) account A returns 429, B succeeds ⇒ client gets B's stream, exactly 2 upstream
      attempts, A excluded from attempt 2. (b) A,B,C all 429 with bound=3 ⇒ surface after 3 attempts (assert the
      count, not a loop past the bound). (c) **security floor:** a cyber request, capable account A fails 429, no
      OTHER capable account ⇒ security 503, and assert NO non-authorized account was attempted across the whole
      loop. (d) **commit barrier:** A relays a byte then drops mid-stream ⇒ the error surfaces in-band, NO failover
      to B (assert B never called). (e) **live-anchor pinned** turn fails ⇒ NOT fanned out (today's behavior). (f)
      **terminal:** a 400 ⇒ surface immediately, no failover. (g) regression: bound=1 reproduces today's one-shot.
- [ ] **Step 2:** Run — fail. **Step 3:** Implement the loop reusing `reroute_cyber_rejection`'s structure +
      Tasks 1-3. **Step 4:** Green; 5 wedge suites green.
- [ ] **Step 5:** Commit: `feat(server): bounded cross-account failover loop (B4)`

---

### Task 5: Config flag + observability + e2e

**Files:** `crates/polyflare-server/src/config.rs` (`POLYFLARE_MAX_ACCOUNT_ATTEMPTS`, default 3),
`observability`/logging (a content-free failover-happened metric/log so the rate is visible), `tests/` e2e.

- [ ] **Step 1:** Failing e2e through the REAL ingress stack: a multi-account pool where the first pick 429s and
      the next succeeds ⇒ the client gets a clean stream, and a content-free "failover" signal is emitted (count +
      reason code, NEVER a body). `MAX_ACCOUNT_ATTEMPTS=1` ⇒ one-shot (regression net). **Step 2:** Run — fail.
      **Step 3:** Implement flag + the content-free failover log/metric. **Step 4:** Green.
- [ ] **Step 5:** Commit: `feat(server): POLYFLARE_MAX_ACCOUNT_ATTEMPTS + content-free failover observability`

---

## Suggested order

1 (classifier) → 2 (exclusion) → 3 (committed flag) → 4 (the loop, crux, adversarial review) → 5 (flag + e2e).
Tasks 1-3 are independent primitives (could parallelize, but they're small; sequential is fine and avoids
same-file churn). Task 4 composes them and is the wedge/security-critical crux. After Task 5, B4 is complete;
B5 (anti-starvation soonest-to-recover) and B10 (thundering-herd) are separate follow-ons the doc gates behind B4.
