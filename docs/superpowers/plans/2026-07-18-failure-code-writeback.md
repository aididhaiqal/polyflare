# Failure-Code Enabler + Request-Path Health Writeback (Phase A: A6/A7) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use `- [ ]`.

**Goal:** Carry the upstream error-code string on the request-failure path, then use it to (A7) park
permanent/auth-failed accounts with a durable terminal status and (A6) distinguish quota-exceeded from
rate-limited — closing the two genuine Phase-A gaps the audit found. This is also the shared enabler for
TA6(b) cyber-move (a later milestone).

**Architecture:** Extend `FailureSignal` with an optional error code. Both executors populate it (HTTP reads
the error body; WS already has it in the error envelope). `record_failure` (ingress) routes the code through
the existing `classify_failure` table to a durable status write.

**Tech Stack:** Rust, existing `oauth::classify_failure`, `AccountRepo::update_status`, `runtime_state`.

**Authority — the design:** `docs/PORTING-CODEXLB.md` §"Phase A" audit note (A6 dead-code, A7 absent, shared
root cause = `FailureSignal` carries no code). `oauth::classify_failure` (`polyflare-codex/src/oauth.rs:105-123`)
is the reuse target. TA6 code table in `DESIGN-DECISIONS.md`.

## Global Constraints

- **Content-safety (inviolable):** the upstream error BODY may be read ONLY to extract the error `code` (and
  optionally `type`). NEVER capture, persist, or log the `message`/`detail` text or the raw body — those can
  echo request framing. The code (`invalid_grant`, `account_deactivated`, `usage_limit_reached`, …) is a safe
  enum-like token; the message is not. Bound the body read (e.g. 64 KiB) — never read an unbounded error body.
- **Zero behavior change until wired:** Task 1 adds the field defaulting to `None`; nothing classifies on it
  until Task 4/5. The 5 wedge suites (`wedge_regression`/`watchdog_race`/`no_anchor_failover`/`signal_client`/
  `failure_routing`) MUST stay green at every task.
- **Reuse, don't reinvent:** the permanent/auth code table lives in `oauth::classify_failure` — call it, do not
  copy the code list (drift = a security bug). `record_quota_exceeded`/`update_status` already exist.
- **A durable terminal status leaves `cooldown_until` null** (only re-auth clears reauth_required) — per TA6/A7.
- Verify each task: `cargo test --workspace` green + `cargo clippy --workspace --all-targets -- -D warnings`.

---

### Task 1: Add `error_code` to `FailureSignal` (core type; zero behavior change)

**Files:** `crates/polyflare-core/src/types.rs` (~54-83); every consumer that constructs/matches `FailureSignal`
(both executors, `ingress::record_failure`).

**Interfaces — Produces:**
```rust
#[derive(Debug, Clone, PartialEq, Eq)]   // NOTE: drops Copy (String field)
pub struct FailureSignal {
    pub status: u16,
    pub retry_after: Option<i64>,
    pub error_code: Option<String>,   // upstream error code (e.g. "invalid_grant"); NEVER the message
}
```
`ExecError::failure_signal(&self) -> Option<FailureSignal>` must now return by clone (was `Copy`).

- [ ] **Step 1:** Add the field + drop `Copy` (keep `Clone`). Update the doc comment: "error_code — the
      upstream error code only, never the message/body (content-safety)."
- [ ] **Step 2:** Fix every construction site to pass `error_code: None` and every `Copy`-dependent use
      (`failure_signal` returns `.cloned()`; `record_failure`'s match borrows instead of copies). Compile.
- [ ] **Step 3:** `cargo test --workspace` — all green, zero behavior change (nothing reads error_code yet).
- [ ] **Step 4:** Commit: `refactor(core): FailureSignal carries an optional upstream error code`

---

### Task 2: HTTP executor parses the error code from the response body (content-safe)

**Files:** `crates/polyflare-codex/src/executor.rs` (~150-155); test `crates/polyflare-codex/tests/executor_stream.rs`.

**Interfaces — Consumes:** Task 1's `FailureSignal.error_code`.

The executor currently returns `UpstreamStatus` WITHOUT reading the body (`executor.rs:150`). On `!is_success()`
it must now: read the body (bounded, ≤64 KiB), parse it as JSON, extract `error.code` (OpenAI shape
`{"error":{"code":"...","type":"...","message":"..."}}`) OR a top-level `detail` string's leading code token if
that's the shape (codex-lb sees both) — **extract the CODE only**, and populate `error_code`. On any parse
failure or absent code → `error_code: None` (never fail the error path over a missing code).

- [ ] **Step 1:** Write a failing test: a `MockUpstream` returning `403` with body
      `{"error":{"code":"account_deactivated","message":"..."}}` ⇒ `execute` returns
      `ExecError::UpstreamStatus` with `status==403` AND `error_code==Some("account_deactivated")`, and assert
      the returned error's `Display`/Debug does NOT contain the message text (content-safety).
- [ ] **Step 2:** Run — fails (error_code is None today).
- [ ] **Step 3:** Implement the bounded read + code-only extraction. Reuse `serde_json`; do not store the body.
- [ ] **Step 4:** Test green. Add a test that a body with no parseable code ⇒ `error_code: None`, no panic.
- [ ] **Step 5:** Commit: `feat(codex): parse upstream error code (code only) on the HTTP error path`

---

### Task 3: WS executor carries the error-envelope code into `UpstreamStatus`

**Files:** `crates/polyflare-codex/src/ws/{codec.rs,executor.rs}`; test against `MockWsUpstream`.

**Interfaces — Consumes:** Task 1's field. The WS error envelope already carries `error.code` (codec `classify`
extracts `previous_response_not_found`/`websocket_connection_limit_reached` specifically). For the GENERAL case
(an envelope whose code is neither of those, mapped to `ExecError::UpstreamStatus`), populate `error_code` with
the envelope's `error.code`.

- [ ] **Step 1:** Failing test: `MockWsUpstream` scripts an error envelope `{"type":"error","status":403,
      "error":{"code":"account_deactivated"},...}` ⇒ the executor surfaces `UpstreamStatus` with
      `error_code==Some("account_deactivated")`.
- [ ] **Step 2:** Run — fails. **Step 3:** Implement (thread the envelope code through `classify`'s
      `Error`/`UpstreamStatus` arm). **Step 4:** Green — and the existing anchor-miss/429 tests still pass.
- [ ] **Step 5:** Commit: `feat(codex): WS executor carries the error-envelope code`

---

### Task 4: A7 — route permanent/auth codes to a durable terminal status

**Files:** `crates/polyflare-server/src/ingress.rs` (`record_failure` ~74-88); test
`crates/polyflare-server/tests/failure_routing.rs`.

**Interfaces — Consumes:** `error_code`. Reuse `polyflare_codex::classify_failure(code) -> FailureClass` and
its `.status()` (the reauth/deactivated mapping). `record_failure` gains a branch: if the signal carries a code
AND `classify_failure(code)` is permanent (ReauthRequired/Deactivated) → write the durable status via the store
(`AccountRepo::update_status`, the same call the OAuth-refresh path uses at `ingress.rs:354-361`), and do NOT
also bump transient error_count (a terminal status supersedes health backoff). Leave `cooldown_until` null.

- [ ] **Step 1:** Failing test: an upstream `401` with `error_code=="invalid_grant"` through the real ingress
      stack ⇒ the account's durable status becomes `reauth_required` and it's excluded from the next selection
      (mirror `failure_routing.rs`'s existing 429 test structure). Assert the status write, not just exclusion.
- [ ] **Step 2:** Run — fails (today it's treated as generic transient, self-heals). **Step 3:** Implement the
      branch (async status write — note `record_failure` may need to become async or dispatch the write; follow
      how the OAuth path at `ingress.rs:354-361` does the `update_status` await). **Step 4:** Green; a
      `deactivated` code test too; and a NON-permanent code (e.g. a generic 500) still routes transient.
- [ ] **Step 5:** Commit: `feat(server): park permanent/auth upstream failures with a durable status (A7)`

---

### Task 5: A6 — distinguish quota-exceeded on the request path (or retire the dead writeback)

**Files:** `crates/polyflare-server/src/ingress.rs`; `runtime_state.rs` (`record_quota_exceeded` exists);
`crates/polyflare-server/src/usage_refresh.rs` (the OTHER quota mechanism — read before deciding).

**Decision to make + document in the report:** the audit found `record_quota_exceeded` is dead code, and a
separate `usage_refresh.rs` poller already writes durable `quota_exceeded` status. So either: (a) wire
`record_quota_exceeded` from `record_failure` when the code indicates quota (`usage_limit_reached`/quota codes,
distinct from a plain rate-limit 429), giving an immediate runtime bench instead of waiting ≤600s for the
poller; OR (b) explicitly retire `record_quota_exceeded` as dead code, documenting that `usage_refresh` owns
quota. **Prefer (a)** — an immediate bench on the failing request is strictly better than a ≤600s poller lag,
and the function already exists + is tested. But confirm the quota code strings against `classify_failure` /
codex-lb before wiring; if no reliable request-path quota signal exists, do (b) rather than guess.

- [ ] **Step 1:** If (a): failing test — a 429/403 with a quota code ⇒ `record_quota_exceeded` runtime bench
      (cooldown, NO error_count bump — assert both) via the real ingress path. If (b): a test/assertion + doc
      change proving the retirement is safe (usage_refresh covers it).
- [ ] **Step 2:** Run — fails. **Step 3:** Implement the chosen path. **Step 4:** Green; the plain-429
      (non-quota) rate-limit path still routes to `record_rate_limit` unchanged (no regression).
- [ ] **Step 5:** Commit: `feat(server): distinguish quota-exceeded on the request path (A6)` (or the retire msg)

---

## Suggested order

1 → 2 → 3 → 4 → 5, strictly sequential (each builds on the last; Task 1 is the type change everything needs).
Tasks 2 and 3 both add code-parsing to an executor error path but in DIFFERENT crates' files — still run them
sequentially (both touch the shared `FailureSignal`-consuming logic and it's cheap to serialize). After Task 5,
Phase A is complete and TA6(b) cyber-move becomes a clean follow-on milestone on this enabler.
