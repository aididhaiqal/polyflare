# WS Fidelity + Robustness (codex-rs parity) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use `- [ ]`.

**Goal:** Close 6 gaps found by studying codex-rs vs PolyFlare's upstream-WS path: 3 fidelity (per-frame `client_metadata` fields real codex sends that PolyFlare omits) + 3 robustness. All latent behind the default-off `POLYFLARE_WS_UPSTREAM`.

**Architecture:** PolyFlare reshapes the client's HTTP `/responses` body into a WS `response.create` envelope (`build_response_create`, preserving the body's `client_metadata` block). The fidelity fixes AUGMENT that preserved `client_metadata` with fields codex adds at WS-send time; the robustness fixes bound the send path, reap the conn cache, and route the read-idle into recovery.

**Tech Stack:** Rust, tokio, tokio-tungstenite.

## Global Constraints
- **Wedge sacred:** do not touch `ObservingStream::poll_next` or continuity/ownership recording. `turn.rs`/`conn.rs` read path is wedge-adjacent — `wedge_regression` (2 tests) must pass; existing frame arms byte-for-byte unchanged unless a task explicitly adds one.
- **Content-free:** `client_metadata` values added here (turn-state token, unix-ms, traceparent, session/window ids) are codex routing/telemetry metadata, never conversation content. Never log a body/frame/token/bearer.
- **Caching not regressed:** the just-merged WS incremental caching (~88%, conn_key = `account:session:fingerprint[:window]`) must not regress. `non_input_fingerprint` MUST NOT change (do not let new `client_metadata` fields enter the fingerprint — verify `NON_INPUT_FIELDS` excludes `client_metadata`; if it includes it, the per-frame timing stamp would break the fingerprint — STOP and flag).
- **Fidelity default:** these make PolyFlare's WS frames MORE codex-identical. Exact codex values (below) are verbatim; do not invent formats.
- Clippy `-D warnings` clean; `cargo fmt --all`.

## Exact codex-rs values (verbatim — from `codex-rs`)
- Timing key: `"x-codex-ws-stream-request-start-ms"` → value = `SystemTime::now().duration_since(UNIX_EPOCH).as_millis()` as i64, `.to_string()` (`core/src/turn_timing.rs:183`, `client.rs:1850`). Stamped just before send.
- Turn-state key: `"x-codex-turn-state"` inside `client_metadata`. Captured from the WS UPGRADE-RESPONSE header `x-codex-turn-state` (`responses_websocket.rs:529-535`) AND from a `response.metadata` frame's `headers` (`sse/responses.rs:203-211`). OnceLock — FIRST value wins. Replayed into EVERY subsequent frame's `client_metadata` (`client.rs:1568-1569`).
- Trace keys: `"ws_request_header_traceparent"` / `"ws_request_header_tracestate"` in `client_metadata` (`common.rs:21-22`), values = the incoming W3C `traceparent`/`tracestate`.

---

### Task 1: Robustness quick-wins — send-path timeout (R1) + read-idle→recovery (R3)

**Files:** Modify `crates/polyflare-codex/src/ws/conn.rs` (`send_frame`), `crates/polyflare-codex/src/ws/executor.rs` (`classify_recovery`). Test: both files' `mod tests`.

**R1 — bound the send.** `send_frame` (conn.rs:325) does `self.socket.send(Message::Text(...)).await` with NO timeout → a hung write stalls a turn forever (codex bounds the send with its idle timeout; PolyFlare already bounds dial + read, not send).

- [ ] **Step 1: Failing test** `send_frame_times_out_on_a_stalled_write` — a `WsConn` whose peer never drains, with a SHORT injected timeout, → `send_frame` returns Err AND `is_closed()` true, within the budget (not a hang). Make the timeout injectable via a `send_frame_with_timeout(envelope, Duration)` internal that `send_frame` calls with a new const `WS_SEND_TIMEOUT: Duration = Duration::from_secs(30)` (mirror `WS_CONNECT_TIMEOUT`). If a stalled-write mock isn't cleanly expressible, assert the timeout wraps (a 0ms budget → immediate Err) and note it.
- [ ] **Step 2: Run → RED.**
- [ ] **Step 3: Implement** — `WS_SEND_TIMEOUT` const; wrap the `self.socket.send(...)` in `tokio::time::timeout(timeout, ...)`; on `Err(_elapsed)` set `self.closed = true` and return `Err(ExecError::Upstream("upstream WS send timed out ...".into()))`. Existing Ok/Err arms unchanged.
- [ ] **Step 4: Run → GREEN.**

**R3 — route read-idle into recovery.** `classify_recovery` (executor.rs:491-497) maps `CONNECTION_LIMIT_MARKER`/`SOCKET_CLOSED_MARKER` → `RecoveryAction::Reconnect` but NOT `WS_READ_IDLE_MARKER` (conn.rs) — so a read-idle-poisoned turn Surfaces/fails-over instead of reconnect+resend. codex retries its idle timeout in place.

- [ ] **Step 5: Failing test** `read_idle_marker_classifies_as_reconnect` — `classify_recovery(&ExecError::Stream(format!("{WS_READ_IDLE_MARKER}: ...")))` == `RecoveryAction::Reconnect`.
- [ ] **Step 6: Run → RED** (currently Surface).
- [ ] **Step 7: Implement** — add `|| msg.contains(crate::ws::conn::WS_READ_IDLE_MARKER)` to the existing `Reconnect` branch (import the marker; it is `pub(crate)`). Document: a 290s read-idle = dead socket → reconnect + full-resend (bounded by the SAME recovery budget as the other reconnect triggers), matching codex retrying its idle timeout. Confirm the recovery budget still bounds it (no infinite loop).
- [ ] **Step 8: Run → GREEN.** Then `cargo test -p polyflare-codex --lib ws`; controller runs `wedge_regression` + `ws_upstream_e2e`. Clippy `-D warnings`, fmt.
- [ ] **Step 9: Commit** — `fix(ws): bound send_frame + route read-idle into reconnect recovery (codex parity)`.

---

### Task 2: Reap the unbounded WS connection cache (R2)

**Files:** Modify `crates/polyflare-codex/src/ws/executor.rs` (`conns` map + `connect_and_cache`/`evict`). Reference: `crates/polyflare-store/src/token_cache.rs` (existing sweep precedent). Test: `executor.rs` `mod tests`.

The `conns: StdMutex<HashMap<String, SharedWsConn>>` (executor.rs:162) has no TTL/reap/cap — abandoned sessions' entries (and possibly-open sockets) live forever, evicted only lazily on next lookup of the same key. `conn_key` is high-cardinality (account:session:fingerprint:window), so this grows unbounded.

- [ ] **Step 1: Failing test** `idle_connections_are_reaped_after_ttl` — insert two cached conns; advance/inject a short TTL; run the reap; assert the idle one is removed AND a still-recent one is kept. Use an injectable clock/`last_used: Instant` per entry + an injectable TTL so no real sleep.
- [ ] **Step 2: Run → RED.**
- [ ] **Step 3: Implement.** Give each cache entry a `last_used: tokio::time::Instant` (a small struct or `(SharedWsConn, Instant)` value). Update `last_used` on every `connect_and_cache` hit. Add a `reap_idle(&self, ttl: Duration)` that removes entries whose `last_used` is older than `ttl` AND drops closed ones. Add `WS_CONN_IDLE_TTL: Duration` (e.g. 15 min — well above a normal inter-turn gap, below indefinite; document the choice vs the 60-min upstream connection limit). Call `reap_idle(WS_CONN_IDLE_TTL)` opportunistically at the top of `connect_and_cache` (cheap, no background task — keeps it self-contained; document that a genuinely-idle process won't reap until the next connect, which is acceptable). Keep the existing `is_closed()`-at-reuse eviction.
- [ ] **Step 4: Run → GREEN.** Confirm existing reuse/caching tests still pass (the `last_used` refresh must not change reuse semantics). `cargo test -p polyflare-codex --lib ws`. Clippy `-D warnings`, fmt.
- [ ] **Step 5: Commit** — `fix(ws): reap idle entries from the WS connection cache (bounded growth)`.

---

### Task 3: Frame client_metadata fidelity — send-timing (F2) + trace relay (F3)

**Files:** Modify `crates/polyflare-codex/src/ws/codec.rs` (`build_response_create`), `crates/polyflare-codex/src/ws/executor.rs` (`execute`, to seed trace into the body's `client_metadata`). Test: `codec.rs` `mod tests`.

**Precondition (verify first):** confirm `delta::NON_INPUT_FIELDS` / `non_input_fingerprint` does NOT include `client_metadata` (grep `NON_INPUT_FIELDS`). If it does, adding a per-frame timing stamp would change the fingerprint every turn → break caching. If so, STOP and flag to the controller — do NOT proceed.

**F2 — send-timing stamp.** codex stamps `x-codex-ws-stream-request-start-ms` = now-unix-ms into `client_metadata` just before every WS send.

- [ ] **Step 1: Failing test** in `codec.rs`: `build_response_create` output's `client_metadata["x-codex-ws-stream-request-start-ms"]` is present and parses as a plausible unix-ms (>= a recent fixed epoch), and is a STRING. Cover both: body already has `client_metadata` (augment) and body has none (create the block).
- [ ] **Step 2: Run → RED.**
- [ ] **Step 3: Implement** in `build_response_create`: get-or-insert a `client_metadata` object on the envelope; insert `"x-codex-ws-stream-request-start-ms"` = `SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis()` clamped to i64, `.to_string()` (a JSON string, matching codex). This is fresh per build (each turn/attempt re-builds → fresh stamp, matching codex).
- [ ] **Step 4: Run → GREEN.**

**F3 — trace relay.** codex adds `ws_request_header_traceparent`/`ws_request_header_tracestate` to `client_metadata` from the request's W3C trace. PolyFlare relays the INCOMING `traceparent`/`tracestate` headers (never generates).

- [ ] **Step 5: Failing test** in `executor.rs` (or codec if cleaner): with a `forward_headers` carrying `traceparent`/`tracestate`, the built envelope's `client_metadata` has `ws_request_header_traceparent`/`ws_request_header_tracestate` == those values; with none present, the keys are absent (no fabricated trace).
- [ ] **Step 6: Run → RED.**
- [ ] **Step 7: Implement** in `execute` (executor.rs), right after `materialize_body`: if `req.forward_headers` contains `traceparent` (case-insensitive), insert `client_metadata["ws_request_header_traceparent"] = <value>` into `body`; same for `tracestate` → `ws_request_header_tracestate`. Then `build_response_create` preserves them (it already preserves `client_metadata`). Do NOT generate a trace when absent.
- [ ] **Step 8: Run → GREEN.** `cargo test -p polyflare-codex --lib ws`. Clippy `-D warnings`, fmt.
- [ ] **Step 9: Commit** — `feat(ws): stamp x-codex-ws-stream-request-start-ms + relay traceparent into WS client_metadata (codex parity)`.

---

### Task 4: Capture + replay the server-issued turn-state (F1)

**Files:** Modify `crates/polyflare-codex/src/ws/conn.rs` (capture from upgrade response; `WsConn` field), `crates/polyflare-codex/src/ws/turn.rs` (capture from `response.metadata` frame), `crates/polyflare-codex/src/ws/executor.rs` (`plan_and_build_locked` — inject into `client_metadata`). Test: all three. **This task gets an adversarial crux review** (fingerprint + sticky-routing critical).

codex captures `x-codex-turn-state` from the WS UPGRADE-RESPONSE header AND from `response.metadata` frames (OnceLock, first wins), and replays it in EVERY subsequent frame's `client_metadata`. PolyFlare currently DISCARDS the upgrade response (`conn.rs:293` `Ok((socket, _response))`) and never replays turn-state on WS — a fingerprint tell AND a sticky-routing/warm-cache signal loss.

**Interfaces:** `WsConn` gains `pub(crate) server_turn_state: Option<String>` (default None = not-yet-seen; set-once semantics like codex's OnceLock).

- [ ] **Step 1: Failing test — capture from upgrade header.** In `conn.rs`, drive `connect_detailed` against a mock WS server whose UPGRADE RESPONSE includes an `x-codex-turn-state: ts-123` header → assert the resulting `WsConn.server_turn_state == Some("ts-123")`. (The conn.rs mock harness can set response headers — see `spawn_*_ws_server`.)
- [ ] **Step 2: Run → RED** (currently the response is discarded).
- [ ] **Step 3: Implement capture.** In `connect_detailed`, stop discarding: `Ok((socket, response))` → extract `response.headers().get("x-codex-turn-state")` (to_str ok) → set `server_turn_state`. Add the field to `WsConn` (default None in every construction site).
- [ ] **Step 4: Failing test — capture from frame.** A `response.metadata` frame carrying a turn-state (in its `headers` object, per codex `sse/responses.rs`) seen by `recv_frame`/turn read → sets `server_turn_state` if not already set (first-wins). Determine the EXACT frame shape codex reads (`response.metadata` event, `headers` field, key `x-codex-turn-state`) and mirror it. If the upgrade-header path already covers the real backend (verify in live-verify), the frame path can be a secondary set-if-none — implement it to match codex, set-once.
- [ ] **Step 5: Run → RED → implement** the frame capture in `turn.rs` (where frames are classified) with set-once semantics; **do NOT alter the existing frame arms' return behavior** — only observe turn-state as a side-effect and continue. Run → GREEN. Confirm `wedge_regression` still passes (turn.rs is wedge-adjacent).
- [ ] **Step 6: Failing test — replay.** Given a `WsConn` with `server_turn_state = Some("ts-9")`, the envelope built by `plan_and_build_locked` has `client_metadata["x-codex-turn-state"] == "ts-9"`; with `None`, the key is absent (never fabricated).
- [ ] **Step 7: Run → RED → implement** in `plan_and_build_locked` (executor.rs): after `build_response_create`, if `conn.server_turn_state` is Some, get-or-insert `client_metadata` on the envelope and set `"x-codex-turn-state"` to it. (This function already reads `conn` state for the anchor, so `conn.server_turn_state` is in scope.)
- [ ] **Step 8: Run → GREEN.** `cargo test -p polyflare-codex --lib ws` + controller runs `wedge_regression` + `ws_upstream_e2e`. Clippy `-D warnings`, fmt.
- [ ] **Step 9: Commit** — `feat(ws): capture + replay server-issued x-codex-turn-state in WS frames (codex parity + sticky routing)`.

---

### Task 5: Live-verify (controller-run)

Not a code task. After Tasks 1-4 merge-ready, the controller:
- [ ] Runs real `codex-polyflare` over `POLYFLARE_WS_UPSTREAM=1` with temp instrumentation logging the OUTBOUND WS frame's `client_metadata` keys (content-free — key names + turn-state token + timing value only), reverted after.
- [ ] Confirms every outbound frame's `client_metadata` now carries `x-codex-ws-stream-request-start-ms` (F2), and — once the backend issues one — `x-codex-turn-state` (F1). Confirms `ws_request_header_traceparent` appears IF the client sent a `traceparent` header (F3).
- [ ] Confirms the backend actually issues `x-codex-turn-state` on the upgrade response and/or a `response.metadata` frame (validates F1's capture source against the real backend — the study inferred it from codex-rs; live is the proof).
- [ ] Confirms NO caching regression (incremental anchor chain + cached_tokens > 0 still hold with the new client_metadata fields — proves the fingerprint didn't change) and turns complete cleanly (R1/R3 didn't break the happy path).
- [ ] Reverts instrumentation; tree clean.

## Self-Review (controller, before Task 1)
- Fingerprint safety: Task 3 precondition (client_metadata NOT in non_input_fingerprint) guards caching. ✓
- Wedge: Tasks 1 & 4 touch conn/turn read path — `wedge_regression` in every gate. ✓
- No fabrication: F1/F3 only emit values the backend/client actually provided (turn-state seen, traceparent relayed) — never invented. ✓
- Set-once turn-state matches codex's OnceLock. ✓
