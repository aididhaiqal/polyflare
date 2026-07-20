# WS-Downstream Relay — Phase 2 (transparent reconnect) + Phase 3 (exhaustion-move) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a WS-downstream relay conversation survive upstream drops invisibly on the same account (Phase 2) and move to a fresh account on durable exhaustion (Phase 3), reusing PolyFlare's existing circuit-breaker, selection, and continuity engines — never duplicating them.

**Architecture:** The Phase-1 pump (`ws_relay/pump.rs`) currently tears down both legs on any upstream close/error and pins one account for the connection's life. This plan gives the pump a **re-dialable upstream** and a small decision layer: it classifies each backend signal content-free, and on a *reconnectable* drop (network blip / idle / the 60-min `websocket_connection_limit_reached` cap) it **re-dials the same account in-band** (the client's downstream socket stays open — it never sees the drop and never full-resends); on a *durable* upstream error it **benches the account** (existing `RuntimeStates` cooldown), **re-selects** a new owner (existing `resolve_owner`, which skips the benched account), **re-dials** it, and **re-homes** the ownership map (existing `observe(TurnOutcome::Recovered)`). The now-cross-account anchor yields `previous_response_not_found`, which the relay forwards **verbatim** so the client full-resends — the relay rewrites nothing.

**Tech Stack:** Rust, tokio, axum 0.8 (ws), tokio-tungstenite; crates `polyflare-server` (`ws_relay/*`), `polyflare-codex` (`ws/*`), `polyflare-core` (types), `polyflare-testkit` (`ws_mock`).

## Global Constraints

_Every task's requirements implicitly include this section. Values are binding._

- **Content-free (inviolable):** no conversation content is ever persisted or logged. The ONLY body inspection allowed is (a) `sniff_completed_id` (`type` + `response.id`) and (b) the new upstream-signal classifier reading ONLY `type`, `error.code`, `status`, and the envelope `retry-after` — never a `message`, never any input/output content. No `tracing`/`log`/`println!`/`eprintln!` of any frame body, error message, or header value anywhere in `ws_relay`.
- **Wedge-sacred:** the anchor-wedge fix and the continuity/selection/circuit-breaker engines are REUSED via their existing public APIs, never modified for relay convenience. `crates/polyflare-server/src/watchdog.rs`, `crates/polyflare-core/src/select.rs`, and the HTTP `ObservingStream` stay byte-unchanged. Any refactor of a shared helper (e.g. extracting `record_failure`'s core) MUST be behavior-preserving — the HTTP path's observable behavior is identical afterward.
- **Verbatim fidelity:** relayed frames are forwarded byte-for-byte (raw text, never parse-then-reserialize). The ONLY frames the relay does not forward are ones it deliberately intercepts as control signals (the `websocket_connection_limit_reached` cap frame); every other frame — including `previous_response_not_found` and real upstream errors — is forwarded verbatim.
- **Flag-gated + additive:** all behavior is behind `POLYFLARE_WS_DOWNSTREAM` (default OFF). HTTP-SSE and the Claude→Codex translation paths are byte-unchanged. The relay stays a self-contained `ws_relay` module.
- **codex-rs fidelity:** the relay never *initiates* a WS ping; inbound client Ping is auto-ponged inline (unchanged from Phase 1).
- **Reuse, do not reinvent:** benching = `RuntimeStates::record_rate_limit`/`record_transient_error` + `accounts().update_status` (via the extracted `record_failure` core); re-selection = `resolve_owner` (`control::resolve_owner_affine_account`, which already falls back to the full eligible pool when the pinned owner is benched); re-home = `Continuity::observe(TurnOutcome::Recovered{..})` → `ContinuityRepo::record_recovery`; failure shape = `polyflare_core::FailureSignal { status, retry_after, error_code }`.
- `cargo clippy --workspace --all-targets -- -D warnings` clean; `cargo fmt --all -- --check` clean; full `cargo test -p polyflare-server` green.

## Design Notes (settled decisions — implementers follow these)

1. **Re-dial trigger model = lazy-safe + eager-on-signal.** Upstream death (`recv_text` → `Ok(None)`/`Err`) marks the upstream dead and KEEPS the downstream open (no teardown). Before forwarding a client frame upstream, if the upstream is dead the relay re-dials first. A `ConnectionLimit` signal triggers an *eager* re-dial of the same account (and is NOT forwarded to the client). This needs no separate frame buffer: the client's next frame is the natural flush point.
2. **Reconnect (Phase 2) is same-account and transparent.** Any reconnectable drop → re-dial the SAME pinned account, bounded by `MAX_REDIAL_ATTEMPTS` with `REDIAL_BACKOFF`. If all attempts fail → tear down (close the downstream). The client keeps its socket throughout; because the account is unchanged, the client's `previous_response_id` anchor resumes with no cross-account event → no wedge.
3. **Move (Phase 3) is inherently client-involved.** A durable upstream error → bench + re-select + re-dial the NEW account in-band (downstream socket stays open) + re-home the ownership map. The move-vs-retry decision is NOT a new threshold: after benching via the existing policy, the relay re-runs `resolve_owner`; if it returns a DIFFERENT account the relay moved, if the SAME account it retries in place. The client discovers the cross-account anchor via a forwarded `previous_response_not_found` and full-resends. The raw upstream error frame IS forwarded verbatim (honest; the client reacts immediately) — suppress-and-fast-resend is a deferred refinement to be decided by Phase-3 measurement.
4. **Residual measurement (Phase 3).** Content-free counters distinguish: same-account reconnects, cross-account moves, and *same-account* `previous_response_not_found` (an anchor that failed to resume WITHOUT a move — the ~31% residual wedge). These feed the watchdog decision, which stays deferred (build only if data shows it).
5. **Classifier boundary.** A small relay-local `classify_upstream_signal` reads the error envelope and reuses `FailureSignal` + the shared code-string constants (exposed from `polyflare-codex`). It does NOT pull in the executor's `FrameClass`/turn machinery (different consumer, avoids coupling).

---

## File Structure

- `crates/polyflare-codex/src/ws/mod.rs` — expose the shared marker/code constants (`WS_CONNECTION_LIMIT_CODE`, `WS_ANCHOR_MISS_CODE`) publicly for the relay classifier (currently `pub(crate)` in `ws/turn.rs`).
- `crates/polyflare-server/src/ws_relay/signal.rs` — **new.** Content-free `classify_upstream_signal(text) -> UpstreamSignal`.
- `crates/polyflare-server/src/ws_relay/redial.rs` — **new.** Bounded same-account re-dial helper + the re-dialable-upstream wrapper.
- `crates/polyflare-server/src/ws_relay/pump.rs` — **modify.** The pump gains the re-dial + reconnect + move decision layer.
- `crates/polyflare-server/src/ws_relay/mod.rs` — **modify.** `relay()` passes the pieces the pump now needs (state, headers, session_key, mutable account) and wires the move-time re-home.
- `crates/polyflare-server/src/ingress.rs` — **modify (behavior-preserving).** Extract `record_failure`'s core into a `pub(crate)` helper the relay can call.
- `crates/polyflare-server/src/observability.rs` — **modify.** Add three content-free relay counters (reconnect / move / same-account-anchor-miss).
- `crates/polyflare-server/tests/ws_downstream_relay.rs` — **modify.** Reconnect + move relay-through tests using `MockWsUpstream::scripted`.

---

### Task 1: Content-free upstream-signal classifier

**Files:**
- Create: `crates/polyflare-server/src/ws_relay/signal.rs`
- Modify: `crates/polyflare-codex/src/ws/mod.rs` (expose `WS_CONNECTION_LIMIT_CODE`, `WS_ANCHOR_MISS_CODE`)
- Modify: `crates/polyflare-server/src/ws_relay/mod.rs` (add `mod signal;`)
- Test: inline `#[cfg(test)]` in `signal.rs`

**Interfaces:**
- Consumes: a raw backend WS text frame (`&str`); `polyflare_core::FailureSignal { status: u16, retry_after: Option<i64>, error_code: Option<String> }`.
- Produces:
  ```rust
  pub(crate) enum UpstreamSignal {
      Normal,                    // ordinary response frame — forward verbatim, sniff for completed id
      ConnectionLimit,           // websocket_connection_limit_reached — INTERCEPT, re-dial same account
      AnchorMissing,             // previous_response_not_found — forward verbatim (client resolves)
      Error(FailureSignal),      // any other error envelope — bench + re-select (move-or-retry)
  }
  pub(crate) fn classify_upstream_signal(text: &str) -> UpstreamSignal;
  ```

- [ ] **Step 1: Expose the shared code constants.** In `crates/polyflare-codex/src/ws/turn.rs` find `CONNECTION_LIMIT_MARKER = "websocket_connection_limit_reached"` and `ANCHOR_MISS_MARKER = "previous_response_not_found"`. Re-export them from `crates/polyflare-codex/src/ws/mod.rs` as `pub const WS_CONNECTION_LIMIT_CODE: &str = ...` / `pub const WS_ANCHOR_MISS_CODE: &str = ...` (or `pub use` if already named constants). Run `cargo build -p polyflare-codex`.

- [ ] **Step 2: Write failing tests** in `signal.rs` covering the mock's exact error shapes (module doc `ws_mock.rs:6-8`: `{"type":"error","status":u16,"error":{"code","message",...},"headers":{...}}`):

```rust
#[test]
fn normal_response_frame_is_normal() {
    assert!(matches!(
        classify_upstream_signal(r#"{"type":"response.output_text.delta","delta":"x"}"#),
        UpstreamSignal::Normal
    ));
    assert!(matches!(
        classify_upstream_signal(r#"{"type":"response.completed","response":{"id":"resp_1"}}"#),
        UpstreamSignal::Normal
    ));
}

#[test]
fn connection_limit_is_intercepted() {
    let f = r#"{"type":"error","status":409,"error":{"code":"websocket_connection_limit_reached","message":"the websocket connection limit was reached"},"headers":{}}"#;
    assert!(matches!(classify_upstream_signal(f), UpstreamSignal::ConnectionLimit));
}

#[test]
fn previous_response_not_found_is_anchor_missing() {
    let f = r#"{"type":"error","status":400,"error":{"code":"previous_response_not_found","message":"Previous response with id 'resp_x' not found."},"headers":{}}"#;
    assert!(matches!(classify_upstream_signal(f), UpstreamSignal::AnchorMissing));
}

#[test]
fn rate_limit_carries_status_and_retry_after() {
    let f = r#"{"type":"error","status":429,"error":{"code":"rate_limit_exceeded","message":"rate limit exceeded"},"headers":{"retry-after":"60"}}"#;
    match classify_upstream_signal(f) {
        UpstreamSignal::Error(sig) => {
            assert_eq!(sig.status, 429);
            assert_eq!(sig.retry_after, Some(60));
            assert_eq!(sig.error_code.as_deref(), Some("rate_limit_exceeded"));
        }
        _ => panic!("expected Error"),
    }
}

#[test]
fn malformed_or_non_error_is_normal() {
    assert!(matches!(classify_upstream_signal("not json"), UpstreamSignal::Normal));
    assert!(matches!(classify_upstream_signal(r#"{"type":"error"}"#), UpstreamSignal::Error(_))); // missing code still an error envelope
}
```

- [ ] **Step 3: Run the tests — verify they fail** (`cargo test -p polyflare-server signal::` → compile error / FAIL).

- [ ] **Step 4: Implement `classify_upstream_signal`.** Parse with `serde_json::from_str::<serde_json::Value>(text)`. If it fails, or `type != "error"`, return `Normal`. Otherwise read `error.code` (str), `status` (u16, default 0), and `headers["retry-after"]` (parse to `i64`, tolerate string or number). Map `code == WS_CONNECTION_LIMIT_CODE` → `ConnectionLimit`; `code == WS_ANCHOR_MISS_CODE` → `AnchorMissing`; else `Error(FailureSignal { status, retry_after, error_code: code.map(str::to_string) })`. NEVER read or log `error.message`. Add a module doc noting the content-free contract.

- [ ] **Step 5: Run tests — verify they pass. Clippy + fmt.**

- [ ] **Step 6: Commit** — `feat(ws-relay): content-free upstream-signal classifier (cap/anchor-miss/error)`.

---

### Task 2: Re-dialable upstream + bounded same-account re-dial helper

**Files:**
- Create: `crates/polyflare-server/src/ws_relay/redial.rs`
- Modify: `crates/polyflare-server/src/ws_relay/mod.rs` (add `mod redial;`)
- Test: inline `#[cfg(test)]` in `redial.rs` (uses `MockWsUpstream`)

**Interfaces:**
- Consumes: `dial_owner_upstream(&HeaderMap, &Account) -> Result<WsConn, RelayError>` (Phase-1, `owner.rs`).
- Produces:
  ```rust
  pub(crate) const MAX_REDIAL_ATTEMPTS: u32 = 3;
  pub(crate) const REDIAL_BACKOFF: std::time::Duration = std::time::Duration::from_millis(250);

  /// Re-dial `account`'s upstream WS, up to MAX_REDIAL_ATTEMPTS with REDIAL_BACKOFF between tries.
  /// Content-free: never logs the account, headers, or any error body.
  pub(crate) async fn redial_upstream(
      headers: &HeaderMap,
      account: &Account,
  ) -> Option<WsConn>;
  ```

- [ ] **Step 1: Write failing tests.** (a) `redial_upstream` returns `Some(WsConn)` against a live `MockWsUpstream`; (b) returns `None` after exhausting attempts against an un-dialable base (mirror the Phase-1 `owner.rs` mock scaffolding for a failing dial — e.g. a closed port). Assert it makes at most `MAX_REDIAL_ATTEMPTS` attempts (use a mock that counts handshakes via `MockWsUpstream::handshake_count`).

- [ ] **Step 2: Run — verify fail.**

- [ ] **Step 3: Implement `redial_upstream`.** Loop up to `MAX_REDIAL_ATTEMPTS`; on `Ok(conn)` return `Some(conn)`; on `Err(_)` sleep `REDIAL_BACKOFF` (except after the last attempt) and retry; return `None` if all fail. No logging of the account/headers/error.

- [ ] **Step 4: Run — verify pass. Clippy + fmt.**

- [ ] **Step 5: Commit** — `feat(ws-relay): bounded same-account upstream re-dial helper`.

---

### Task 3: Phase 2 — transparent same-account reconnect in the pump

**Files:**
- Modify: `crates/polyflare-server/src/ws_relay/pump.rs`
- Modify: `crates/polyflare-server/src/ws_relay/mod.rs` (pass `headers` + `account` into the pump so it can re-dial; the ownership callback already has `session_key`/`account_id`)
- Test: `crates/polyflare-server/tests/ws_downstream_relay.rs`

**Interfaces:**
- Consumes: `classify_upstream_signal` (T1), `redial_upstream` (T2), `dial_owner_upstream` (Phase-1), `sniff_completed_id` (Phase-1).
- Produces: the pump signature grows to carry what re-dial needs. New shape:
  ```rust
  pub(crate) async fn run_pump<F, Fut>(
      mut downstream: WebSocket,
      mut upstream: WsConn,
      headers: HeaderMap,            // for re-dial
      account: Account,             // the pinned account (SAME for Phase-2 reconnect)
      on_completed_id: F,
  ) where F: Fn(String) -> Fut, Fut: std::future::Future<Output = ()>;
  ```

**Behavior (replaces the Phase-1 `Ok(None) | Err(_) => break` teardown):**
- **client → backend (`downstream.recv()`):**
  - `Some(Ok(Message::Text(t)))`: if `upstream` is dead, `redial_upstream(&headers, &account)` first (on `None` → break/teardown); then `upstream.send_text(t.to_string())`. On send `Err` → mark upstream dead and retry once via re-dial; if re-dial fails → break.
  - `Some(Ok(Message::Ping(p)))` → auto-pong (unchanged). `Pong`/`Binary` ignored (unchanged).
  - `Some(Ok(Message::Close(_)))` | `None` | `Some(Err(_))` → the CLIENT closed → break (real teardown).
- **backend → client (`upstream.recv_text()`):**
  - `Ok(Some(text))`: `match classify_upstream_signal(&text)`:
    - `Normal` → forward verbatim to downstream FIRST, then `sniff_completed_id` → `on_completed_id`. (Phase-1 behavior.)
    - `ConnectionLimit` → **do NOT forward.** Re-dial the SAME account (`redial_upstream`); on `Some` replace `upstream` and continue; on `None` → break.
    - `AnchorMissing` → forward verbatim (client resolves). (Move handling is Task 4; here treat as forward-only.)
    - `Error(_)` → forward verbatim (Task 4 adds the bench/move; Phase-2 forwards only). 
  - `Ok(None) | Err(_)` → the UPSTREAM dropped (network/idle) → mark upstream dead, DO NOT break; loop continues (the next client frame re-dials). To avoid a busy-loop when the client is also idle, the dead-upstream branch must `select!`-park on `downstream.recv()` only (see Step notes).

> Implementation note for the dead-upstream state: model the upstream as `Option<WsConn>`. When `None`, the `select!` arm for `upstream.recv_text()` is replaced by `std::future::pending()` so the loop waits purely on the client; the next client Text frame re-dials and repopulates `Some(upstream)`. This is the buffer-free flush point from Design Note 1.

- [ ] **Step 1: Write the failing reconnect tests** in `ws_downstream_relay.rs` using `MockWsUpstream::scripted`:
  - `reconnect_on_connection_limit_stays_same_account`: script `[connection_limit_reached(409), normal(vec![])]`. Turn 1 → the mock replies the cap frame; assert the CLIENT does NOT receive an `error`/`websocket_connection_limit_reached` frame (it was intercepted), the downstream stays open, and a subsequent client frame is answered `response.completed` (the relay re-dialed the same account — assert via `mock.handshake_count() >= 2`). Ownership row still the same account.
  - `reconnect_on_upstream_drop_keeps_downstream_open`: script `[close_mid_stream(vec![]), normal(vec![])]`. Turn 1 upstream closes; assert downstream not closed; a second client frame gets a completed reply (re-dialed same account).
- [ ] **Step 2: Run — verify fail.**
- [ ] **Step 3: Implement** the `Option<WsConn>` re-dial state machine + the classify branch above. Keep verbatim + content-free. `mod.rs` passes `headers.clone()` and `account.clone()` into `run_pump`.
- [ ] **Step 4: Run reconnect tests + the full `ws_downstream_relay` suite + Phase-1 tests — verify pass. Clippy + fmt.**
- [ ] **Step 5: Commit** — `feat(ws-relay): transparent same-account reconnect (drop/idle/60-min cap) keeping the client socket`.

---

### Task 4: Phase 3 — exhaustion-move (bench + re-select + re-dial-new + re-home)

**Files:**
- Modify: `crates/polyflare-server/src/ingress.rs` (extract `record_failure` core → `pub(crate)`; behavior-preserving)
- Modify: `crates/polyflare-server/src/ws_relay/pump.rs` (the `Error(sig)` branch triggers the move)
- Modify: `crates/polyflare-server/src/ws_relay/mod.rs` (provide a move callback that benches, re-selects, re-dials, and re-homes)
- Test: `crates/polyflare-server/tests/ws_downstream_relay.rs`

**Interfaces:**
- Extract from `ingress::record_failure` (lines ~140-183) a reusable core:
  ```rust
  // ingress.rs — behavior-preserving extraction; record_failure becomes a thin caller.
  pub(crate) async fn bench_account_for_failure(
      state: &AppState,
      id: &AccountId,
      sig: Option<&FailureSignal>,
      now: i64,
  );
  ```
  (Move the `classify_failure(code).status()` permanent-ban path + the `429 → record_rate_limit` / `5xx|401|403|408 → record_transient_error` / other-4xx no-op / `None → record_transient_error` branching verbatim into this fn, plus the `emit_health_tier_signal` on the returned transition. `record_failure` then calls it.)
- The pump's `Error(sig)` branch calls a move callback provided by `mod.rs`:
  ```rust
  // Returns the (possibly new) upstream to continue on, or None to tear down.
  // move_on_error: Fn(FailureSignal) -> Fut<Output = Option<WsConn>>
  ```
  The callback (in `mod.rs`, capturing `state`, `headers`, `session_key`, and a mutable pinned `account`): (1) `bench_account_for_failure(&state, &current_account_id, Some(&sig), now)`; (2) `resolve_owner(&state, &session_key)` → new `Account`; (3) if the new account id == the current id → retry same: `redial_upstream(&headers, &current_account)`; else → MOVED: `redial_upstream(&headers, &new_account)`, update the pinned account, and `state.continuity.observe(TurnOutcome::Recovered { session_key: Some(session_key.clone()), account: AccountId::from(new_account.id.as_str()), new_response_id: None }, &RequestCtx::default())` to re-home; (4) return the new `WsConn` (or `None` if re-dial failed).

**Behavior:** in the pump's `backend → client` arm, `UpstreamSignal::Error(sig)` now: forward the error frame verbatim to the client FIRST (honest — Design Note 3), THEN call `move_on_error(sig)`; on `Some(new_upstream)` replace `upstream` and continue; on `None` break.

- [ ] **Step 1: Extract `bench_account_for_failure` (behavior-preserving).** Move `record_failure`'s body into the new `pub(crate)` fn; make `record_failure` call it. Run the existing ingress/failure tests to prove behavior unchanged (name them in the commit).
- [ ] **Step 2: Write the failing move test** in `ws_downstream_relay.rs`: spawn a mock with TWO accounts eligible; script `[rate_limited_429(300), normal(vec![])]` so account A's first turn returns a durable 429. Drive: client frame → A returns 429 (forwarded to client) → relay benches A + re-selects → B (assert the re-home wrote `continuity_sessions.owning_account_id == B` via `get_anchor_owner`/`get_session` after a follow-up completed turn on B) → a subsequent client frame is answered by B (`mock.handshake_count` on B's socket ≥ 1). Assert A is now cooled-down in `state.runtime` (skipped by a fresh `resolve_owner`).
- [ ] **Step 3: Run — verify fail.**
- [ ] **Step 4: Implement** the extraction + the `Error(sig)` move branch + the `mod.rs` move callback. Reuse `resolve_owner` (do NOT write new selection). Content-free: the move callback logs nothing but the content-free counters (Task 5). Clippy + fmt.
- [ ] **Step 5: Run the move test + `bench_account_for_failure` behavior-preservation tests + full server suite — verify pass.**
- [ ] **Step 6: Commit** — `feat(ws-relay): exhaustion-move — bench + re-select + re-dial new account + re-home ownership`.

---

### Task 5: Anchor-miss forward-verbatim + content-free residual counters

**Files:**
- Modify: `crates/polyflare-server/src/observability.rs` (three relay counters)
- Modify: `crates/polyflare-server/src/ws_relay/pump.rs` + `mod.rs` (increment counters at the decision points)
- Test: `crates/polyflare-server/tests/ws_downstream_relay.rs` + inline observability test

**Interfaces:**
- Add three content-free counters to the existing metrics registry (mirror the existing `RateLimitMetrics`/`health_tier_metrics` pattern):
  ```rust
  pub struct RelayMetrics {
      pub reconnects_same_account: Counter,      // Phase-2 same-account re-dials
      pub moves_cross_account: Counter,          // Phase-3 account moves
      pub same_account_anchor_miss: Counter,     // residual non-resumption (Design Note 4)
  }
  ```
  Wire `RelayMetrics` onto `AppState` (like `rate_limit_metrics`).

**Behavior:** the pump/mod increment: `reconnects_same_account` on every same-account re-dial (Task 3 + the retry-same branch of Task 4); `moves_cross_account` on every Task-4 move; `same_account_anchor_miss` when an `AnchorMissing` frame arrives while the pinned account has NOT changed since the last completed turn (i.e. the client's anchor failed to resume on the SAME account — the ~31% residual). Track "has the account changed since last completed" with a small in-connection bool the move sets.

- [ ] **Step 1: Write failing tests.** (a) an observability unit test that the three counters exist and increment. (b) extend the Task-3 reconnect test to assert `reconnects_same_account` incremented; extend the Task-4 move test to assert `moves_cross_account` incremented. (c) a `same_account_anchor_miss` test: script `[normal(vec![]), previous_response_not_found("resp_1")]` on a single account (no move) → the second turn's anchor-miss increments `same_account_anchor_miss` (and is forwarded verbatim to the client).
- [ ] **Step 2: Run — verify fail.**
- [ ] **Step 3: Implement** the metrics + increments. Confirm `AnchorMissing` is forwarded verbatim in all cases (it already is from Task 3). Content-free: counters carry no labels beyond the account id (already content-free elsewhere) — prefer NO per-account label to stay minimal; a bare counter is enough for the watchdog decision.
- [ ] **Step 4: Run — verify pass. Clippy + fmt.**
- [ ] **Step 5: Commit** — `feat(ws-relay): content-free reconnect/move/residual-anchor-miss counters`.

---

### Task 6: Live/mock verification (controller-run)

Not a code task. After Tasks 1-5:
- [ ] **Mock-driven end-to-end** (the hard-to-trigger-live scenarios): confirm via the test suite that (a) a 60-min cap frame is intercepted and the conversation continues same-account with the downstream never closing; (b) a durable 429 moves to a second account, re-homes ownership, and the client's `previous_response_not_found` → full-resend path works; (c) counters reflect reality.
- [ ] **Light real probe** (feasible without waiting 60 min): with `POLYFLARE_WS_DOWNSTREAM=1` and temp CONTENT-FREE instrumentation, run a real `codex-polyflare` multi-turn conversation, then kill the upstream socket mid-conversation (or restart the relay's upstream reachability) and confirm the relay re-dials the SAME account and the client continues without a visible drop. Revert instrumentation after.
- [ ] **Content-safety audit** of the relay path (no frame body / error message / header value logged anywhere — grep the server log).
- [ ] **Record results + open items** (the deferred watchdog decision informed by `same_account_anchor_miss`; the suppress-and-fast-resend refinement from Design Note 3).

---

## Self-Review

- **Spec coverage (design §4/§5/§6):** reconnect-same-account incl 60-min cap → Task 3; move-on-durable-exhaustion + client full-resend → Task 4; transient-retry-same → Task 4's same-account branch; forward-verbatim of anchor-miss/errors → Tasks 3-5; content-free response-id sniff + no-body → all tasks; residual measurement / watchdog-deferred → Task 5 + Task 6. ✓
- **Placeholder scan:** the one uncertain client-facing behavior (forward-vs-suppress the raw error, Design Note 3) is an explicit, defaulted decision with a measurement follow-up — not a placeholder. ✓
- **Type consistency:** `UpstreamSignal`/`classify_upstream_signal` (T1), `redial_upstream`/`MAX_REDIAL_ATTEMPTS` (T2), the grown `run_pump` signature (T3), `bench_account_for_failure`/`move_on_error`/`observe(TurnOutcome::Recovered)` (T4), `RelayMetrics` (T5) are referenced consistently. `FailureSignal`/`Account`/`AccountId`/`SessionKey`/`RequestCtx` are the existing `polyflare-core` types. ✓
- **Wedge-sacred:** `watchdog.rs`/`select.rs`/`ObservingStream` untouched; the only shared-code edit is a behavior-preserving `record_failure` extraction, guarded by its existing tests. ✓
