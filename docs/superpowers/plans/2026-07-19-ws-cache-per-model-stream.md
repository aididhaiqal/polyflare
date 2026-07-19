# WS cache fix: per-model-stream connection key Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Make PolyFlare's M5a upstream WebSocket path get prompt caching (currently 0%), by keying the WS connection cache per **(session, non-input config)** instead of per session alone — so codex's two interleaved models (`gpt-5.6-luna` + `gpt-5.6-sol`) each get their **own** socket + clean anchor chain, letting `plan_request` emit `Incremental` (which the backend caches).

**Root cause (measured 2026-07-19, WSDBG-instrumented + request_log):** codex runs TWO models per conversation turn. PolyFlare's `CodexWsExecutor` caches ONE socket per `session_key`, storing the anchor/`last_non_input_fingerprint` on it. The luna and sol requests share that one socket and clobber each other's stored fingerprint → every turn fails `plan_request` Gate 3 (`non_input_fingerprint` mismatch) → `RequestPlan::Full` → no anchor → 0% cache. Live-confirmed: `WSDBG: conn REUSED` then `Full gate=3 (non-input fingerprint changed)`; request_log shows `gpt-5.6-luna|medium` and `gpt-5.6-sol|low` interleaved on the same account/session. codex-rs NATIVE WS caches ~72% because each model-stream has its own anchor chain; codex-lb caches because it keys continuity by `(turn_state, api_key_id)` (turn-state separates the streams) and only checks the input prefix.

**Architecture:** The fix is entirely inside `crates/polyflare-codex/src/ws/executor.rs` (WS-executor-local). It does NOT touch `session_key` derivation (`polyflare-server::session_key`), continuity, ownership, or the wedge — so nothing above the seam changes. `delta.rs` is already correct (Gate 3 is a faithful mirror of codex's rule) and is NOT modified. We introduce a `conn_key = session_key + ":" + non_input_fingerprint(body)` used ONLY for the connection cache (get/insert/evict/reconnect); the plain `session_key` is retained for the session-scoped 426 disable + observability.

**Tech Stack:** Rust, the existing `ws::executor` connection cache (`conns: StdMutex<HashMap<String, SharedWsConn>>`), `ws::delta::non_input_fingerprint` (already `pub`).

## Global Constraints

- **The wedge fix is sacred.** Do NOT touch `ObservingStream`/`watchdog`, continuity, `session_key` derivation, or the M3 path. This change is confined to `ws/executor.rs`'s connection-cache keying.
- **`delta.rs` is NOT modified.** Its Gate 3 logic is correct (mirrors codex's rule). The bug is the shared socket, not the delta check.
- **Content-free:** `conn_key` is `sha256(session)` + `":"` + `sha256(non-input fields)` — both content-free hashes. Never log a body/frame; the existing `log_fallback`/`log_wedge_recovery` stay session-key-based (content-free).
- **426 disable stays per-SESSION.** A 426 means the account/session doesn't support WS at all → `is_session_ws_disabled`/`disable_session` keep using `session_key` (the whole session, all model-streams), NOT `conn_key`.
- **No behavior change when `ctx.session_key` is None.** A keyless request still gets a fresh uncached socket + Full (unchanged) — `conn_key` is `None` when `session_key` is `None`.
- **Workspace fmt-clean + clippy clean UNDER `-D warnings`.** Existing WS tests + `wedge_regression` stay green.
- **The fix only matters with `POLYFLARE_WS_UPSTREAM=1`** (default OFF); HTTP already caches ~95% and is untouched.

---

## Task 1: Introduce the per-model-stream `conn_key` in the executor

**Files:**
- Modify: `crates/polyflare-codex/src/ws/executor.rs` (`execute`, `connect_and_cache`, `evict`, `drive_turn`)
- Test: inline `#[cfg(test)] mod tests` in `executor.rs`

**Interfaces:**
- Consumes: `crate::ws::delta::non_input_fingerprint(&Value) -> String` (already `pub`), `materialize_body(&PreparedRequest) -> Result<Value, ExecError>` (existing), the existing `conns` map + `ConnAttempt`.
- Produces: no public API change. Internal: `connect_and_cache` and `evict` take a `conn_key: Option<&str>` (renamed from `session_key`, semantics = the connection-cache key); `drive_turn` takes BOTH `conn_key: Option<&str>` (for `evict`/`connect_and_cache`) AND `session_key: Option<&str>` (for `disable_session` + logging).

- [ ] **Step 1: Write the failing unit test — two models, one session_key, must NOT share a socket**

Add to `executor.rs`'s `#[cfg(test)] mod tests` (study the existing `second_execute_call_same_session_reuses_the_connection_and_sends_a_real_delta` test at ~`executor.rs:656` for the harness — the mock WS upstream, `SessionKey` construction, `prepared(body)` helper, and how it asserts a delta vs full). Write a test that drives, on ONE `CodexExecutor`/`CodexWsExecutor` with a mock upstream and a SINGLE stable `session_key`, two `execute` calls whose bodies differ ONLY in the `model` field (e.g. `gpt-5.6-luna` vs `gpt-5.6-sol`), and asserts they were served on TWO DISTINCT sockets (i.e. `ws_connect_attempts` incremented twice / the mock saw two handshakes), NOT reused as one. Then a THIRD call with the SAME model as the first + a strict-extension input must REUSE the first model's socket and send a real INCREMENTAL delta (the existing single-model delta behavior, preserved). Model the assertions on the existing test's mechanism.

If the existing test harness only supports asserting delta-vs-full (not distinct sockets), assert instead that: same-session + same-model + extension ⇒ Incremental (unchanged), and same-session + DIFFERENT-model ⇒ each is a Full/fresh chain (never an Incremental that would carry the wrong model's anchor). The key property: a request for model B must NEVER be planned as an Incremental anchored on model A's response.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p polyflare-codex --lib ws::executor 2>&1 | tail -20`
Expected: FAIL — today the two different-model calls share one socket (one handshake) and/or model B gets planned against model A's anchor.

- [ ] **Step 3: Implement the `conn_key`**

In `execute` (`executor.rs:499-548`):
- Keep `let session_key = ctx.session_key.as_ref().map(|k| k.value.clone());` and the `is_session_ws_disabled(session_key)` / `is_account_in_cooldown` checks UNCHANGED (they must stay session-scoped).
- MOVE `let body = materialize_body(&req)?;` to BEFORE `connect_and_cache` (it currently sits after, at line 539).
- Compute the connection key:
```rust
// Per-model-stream connection key: codex interleaves multiple models (e.g. gpt-5.6-luna +
// gpt-5.6-sol) on ONE conversation. Keying the socket cache on session_key alone made them
// share a socket and clobber each other's anchor/non-input fingerprint, forcing plan_request
// to Full every turn (0% cache). Folding the non-input fingerprint into the key gives each
// model-stream its OWN socket + clean strict-extension chain -> Incremental -> the backend caches.
// Content-free: session_key and non_input_fingerprint are both sha256 hex digests.
let conn_key = session_key
    .as_ref()
    .map(|sk| format!("{sk}:{}", crate::ws::delta::non_input_fingerprint(&body)));
```
- Pass `conn_key.as_deref()` to `connect_and_cache` (instead of `session_key.as_deref()`).
- Pass BOTH to `drive_turn`: `conn_key.as_deref()` (new param) and `session_key.as_deref()` (existing param).

In `connect_and_cache` (`executor.rs:235-267`): rename the `session_key: Option<&str>` param to `conn_key: Option<&str>` — it is purely the cache key. No logic change (it already just get/insert/removes `self.conns` by that key).

In `evict` (`executor.rs:227-231`): rename `session_key: Option<&str>` → `conn_key: Option<&str>` (purely the cache key).

In `drive_turn` (`executor.rs:304-406`): add a `conn_key: Option<&str>` param alongside the existing `session_key: Option<&str>`. Use `conn_key` for `self.evict(conn_key)` (line 371) and `self.connect_and_cache(account, forward_headers, conn_key)` (line 373). Keep `session_key` for `disable_session(key)` (388-390), `log_wedge_recovery(..., session_key, ...)` (345-350, 365-370), and `log_fallback(..., session_key)` (392-396). Update the caller in `execute` to pass both.

- [ ] **Step 4: Run to verify pass + no regression**

Run: `cargo test -p polyflare-codex --lib ws 2>&1 | tail -20` (the new test + the existing delta/reuse/recovery tests) and `cargo test -p polyflare-codex 2>&1 | tail -10`.
Expected: PASS — the new different-model test passes, and `second_execute_call_same_session_reuses_the_connection_and_sends_a_real_delta` (same model → same conn_key → reuse → delta) still passes. Then `cargo clippy -p polyflare-codex --all-targets -- -D warnings` clean + `cargo fmt --all`.

- [ ] **Step 5: Whole-workspace gate + commit**

Run: `cargo test --workspace 2>&1 | grep -E "test result:" | awk '{p+=$4;f+=$6} END{print p"/"f}'` (expect 0 failed, incl `wedge_regression`), `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --all --check`.
```bash
git add crates/polyflare-codex/src/ws/executor.rs
git commit -m "fix(ws): key WS connection cache per (session, non-input fingerprint) so interleaved models each cache (M5a)"
```

---

## Task 2: Live-verify the fix caches (the whole point — mocks can't prove wire facts)

**Files:** none (operational verification). Requires `POLYFLARE_WS_UPSTREAM=1` + a few pool generations of quota.

This task is REQUIRED — the unit test proves the sockets separate, but only a live run proves the backend actually caches the now-Incremental WS turns. Follow the exact method the diagnosis used.

- [ ] **Step 1: Build + start serve with WS upstream**
```bash
cargo build --release -p polyflare-server
POLYFLARE_WS_UPSTREAM=1 nohup target/release/polyflare serve > /tmp/pf-verify.log 2>&1 &
# confirm listening on 127.0.0.1:8080
```

- [ ] **Step 2: Drive a real codex 2-turn conversation over WS**

Use the `scripts/codex-polyflare` harness with an isolated `POLYFLARE_CODEX_HOME`, `</dev/null` to close stdin, `--skip-git-repo-check`, a ~60-line prefix, `-m gpt-5.6-luna`, then `exec resume --last`. (Same recipe as the diagnosis runs.)

- [ ] **Step 3: Confirm Incremental + caching**

- Read the new session's rollout (`$POLYFLARE_CODEX_HOME/sessions/**/rollout-*.jsonl`): parse `last_token_usage` per request; assert at least one continuation request now reports `cached_input_tokens > 0` (was 0 before the fix).
- (Optional, if temporary instrumentation is re-added and reverted) confirm `plan_request` now returns Incremental for a continuation instead of `Full gate=3`.
- Record the before (0%) / after (>0%) numbers.

- [ ] **Step 4: Clean up**

Kill serve, delete the isolated codex home + scratch, confirm the store's account statuses are unchanged, `git status` clean.

---

## Self-Review

**1. Spec coverage:** the 0%-cache root cause (shared socket across interleaved models) → Task 1 (per-fingerprint conn key). Live proof of caching → Task 2. `delta.rs` untouched (correct). `session_key`/continuity/wedge untouched (426 disable + logging stay session-keyed).

**2. Placeholder scan:** Task 1 Step 1 directs the implementer to the exact existing test (`second_execute_call_..._real_delta`) to copy the harness, with a fallback assertion if distinct-socket asserting isn't supported. No TBD/vague steps.

**3. Type consistency:** `conn_key: Option<&str>` threads through `execute → connect_and_cache/evict/drive_turn`; `session_key: Option<&str>` retained for `is_session_ws_disabled`/`disable_session`/logging. `non_input_fingerprint(&Value) -> String` is already `pub` in `delta.rs`. `materialize_body` moved earlier in `execute`, still `-> Result<Value, ExecError>`.

**Adversarial-review crux (flag for reviewers):**
- **Same-model reuse preserved:** the existing `second_execute_call_..._real_delta` test MUST still pass (same model → identical `non_input_fingerprint` → identical `conn_key` → socket reused → Incremental). If it breaks, the fingerprint isn't stable across a model-stream's turns — investigate before proceeding.
- **426 disable stays whole-session:** `is_session_ws_disabled`/`disable_session` must use `session_key`, NOT `conn_key` — a 426 disables WS for the entire session (every model-stream), not just one fingerprint. Verify both call sites (execute + drive_turn recovery).
- **Content-safety:** `conn_key` is two sha256 digests joined by `:` — content-free. Confirm it's never logged as anything but the existing session-key-based content-free logs.
- **Live-verify is non-negotiable (Task 2):** the unit test proves socket separation; only the live run proves the backend caches the Incremental. Do not claim the fix works on the unit test alone.
