# WS-to-WS Relay — Phase 0 (gating spike) + Phase 1 (sticky relay MVP) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use `- [ ]`. **Phase 0 is a HARD GATE — if its go/no-go is NO-GO, stop; do not build Phase 1.**

**Goal:** Prove the codex CLI can speak WebSocket *downstream* to PolyFlare (Phase 0), then build a sticky WS-to-WS relay for one conversation on a pinned account with caching confirmed (Phase 1).

**Architecture:** The codex CLI's WS-handshake probe already arrives as `GET /responses` (today answered 426 → HTTP fallback). Behind `POLYFLARE_WS_DOWNSTREAM`, accept that upgrade instead, resolve the conversation's owner account via the existing continuity engine, dial one upstream WS to that account (reusing `ws::conn`), and pump frames both directions verbatim (codex-rs pump pattern), sniffing only `response.completed` ids to feed ownership. HTTP-SSE and translation paths are untouched.

**Tech Stack:** Rust, axum (`axum::extract::ws::WebSocketUpgrade`), tokio, tokio-tungstenite (upstream leg, via existing `ws::conn`).

## Global Constraints (verbatim from the design spec)
- **Content-free:** never persist/log a frame body; only the content-free `response.completed`-id sniff + handshake-header fingerprint normalization. No conversation content stored — ever.
- **Wedge-sacred:** reuse the continuity/ownership engine and its content-free sniff; do not modify `ObservingStream` or continuity recording logic.
- **Additive & flag-gated:** `POLYFLARE_WS_DOWNSTREAM` default OFF (fail-safe convention like `ws_upstream`/`live_logs` — only `Ok("1")|Ok("true")` is ON). HTTP-SSE + translation code paths byte-unchanged.
- **codex-rs fidelity:** relay frames verbatim (the real client built them); the only synthesized bytes are normalized handshake headers (existing fingerprint layer). No client-initiated pings; auto-pong inbound Ping inline; ignore inbound Pong.
- **Sticky per conversation:** one downstream WS = one conversation = one pinned upstream account (Phase 1 pins; moves/reconnect are Phase 2/3).
- Clippy clean under `-D warnings`; `cargo fmt --all`.

## File Structure
- Create: `crates/polyflare-server/src/ws_relay/mod.rs` — the WS-downstream endpoint handler + relay orchestration (accept upgrade, resolve owner, dial upstream, spawn pump, teardown).
- Create: `crates/polyflare-server/src/ws_relay/pump.rs` — the bidirectional pump (verbatim forward each leg; inline pong; close/error teardown).
- Create: `crates/polyflare-server/src/ws_relay/sniff.rs` — content-free `response.completed`-id extraction from a frame's text.
- Modify: `crates/polyflare-server/src/config.rs` — add `pub ws_downstream: bool` (mirror `ws_upstream`).
- Modify: `crates/polyflare-server/src/app.rs:326` — the `/responses` route: when `state.ws_downstream`, route the GET upgrade to `ws_relay`; else keep the existing 426 shim.
- Reuse (no change): `crates/polyflare-codex/src/ws/conn.rs` (`connect_detailed` for the upstream dial), `crates/polyflare-server/src/continuity.rs` (`resolve` → `pin_account`, `observe` → record), the account-selection engine, `crates/polyflare-server/src/session_key.rs` (owner-lookup key).

---

## PHASE 0 — Gating spike (HARD GATE) — ✅ RESULT: **GO** (run 2026-07-20)

**Outcome:** With `-c model_providers.polyflare.supports_websockets=true`, the real codex CLI **attempted a WebSocket-downstream upgrade** to PolyFlare's `GET /responses` (`upgrade=websocket`, `sec-websocket-*` present), then fell back to HTTP on the 426 and completed the turn. So the CLI **will** WS-downstream to a custom provider — the gate is GO.

**Captured handshake headers (content-free — names + identity presence):** `host, connection, upgrade, sec-websocket-version, sec-websocket-key, sec-websocket-extensions, authorization, user-agent, originator, openai-beta, x-codex-turn-metadata, x-codex-beta-features, x-client-request-id, session-id, thread-id, x-codex-window-id`. Present: `session-id`, `thread-id`, `x-codex-window-id`. Absent: `x-codex-turn-state` (server-issued, correct).

**Resolves spec §9:** Task 5's `session_key` = `sha256_hex` of (`session-id` + `thread-id` + `x-codex-window-id`) read from the **WS handshake headers** — no first-frame read needed. (Method used: the throwaway pass-through relay was NOT built; the cheaper "does the CLI attempt WS + what handshake does it send" probe answered both the gate and §9. The remaining Phase-0 residual — full relay round-trip + cache — is low-risk (upstream `ws::conn` is proven; relaying real codex frames is codex-lb's model) and folds into Phase 1 Task 7's live-verify.)

Original spike steps (kept for record; superseded by the probe above):

### Task 1: Prove the codex CLI will WS-downstream to PolyFlare, via a trivial pass-through relay

**This is a spike, not TDD.** Deliverable: a go/no-go answer + a captured real WS handshake+frame trace that pins Phase 1's `session_key` derivation (spec §9). Minimal throwaway scaffolding is fine; do not build production structure yet.

**Files (throwaway/minimal):** a temporary `GET /responses` upgrade handler behind `POLYFLARE_WS_DOWNSTREAM` that accepts the client WS, dials ONE upstream WS to a single hardcoded healthy account (via `ws::conn::connect_detailed`), and blindly forwards text frames both directions (no ownership, no sniff, no pinning logic). Log — CONTENT-FREE — the downstream handshake headers (names only + turn-state/window-id/session-id presence), and per-frame the `type` field only (never the body).

- [ ] **Step 1: Add the flag + accept-upgrade shim.** In `config.rs` add `ws_downstream` (mirror `ws_upstream`). In `app.rs` route the `GET /responses` upgrade to a temporary `ws_relay_spike` handler when `state.ws_downstream`, else the existing 426 path. The handler uses `axum::extract::ws::WebSocketUpgrade` → `.on_upgrade(...)`.
- [ ] **Step 2: Trivial pass-through.** On upgrade: dial upstream via `ws::conn::connect_detailed(&account, &forward_headers)` to ONE hardcoded active account; then two tasks copying text frames verbatim downstream↔upstream; auto-pong Ping inline; tear down both on either close.
- [ ] **Step 3: Configure the real codex CLI for WS-downstream.** In the `scripts/codex-polyflare` isolated `CODEX_HOME`, set the provider `supports_websockets = true` and a `ws://127.0.0.1:8080` (or the `/responses` WS) base — matching how codex gates WS (`codex-rs/core/src/client.rs:930`). Document the exact config used.
- [ ] **Step 4: Run it live.** `POLYFLARE_WS_DOWNSTREAM=1 polyflare serve` + a real `codex-polyflare exec "..."`. Observe whether: (a) the CLI opens a WS to PolyFlare (not HTTP), (b) the relay reaches chatgpt.com, (c) a turn completes, (d) a second turn reuses the socket + caches (cached_tokens > 0).
- [ ] **Step 5: Capture + decide.** Record the downstream handshake headers + frame `type` sequence + which identity fields (`session_id`/`thread_id`/`window_id`/turn-state) appear and where (handshake header vs frame `client_metadata`) — this pins Phase 1's `session_key` derivation. Write a go/no-go: **GO** if the CLI WS-connects and a turn round-trips + caches; **NO-GO** (stop) if the CLI refuses WS to a custom provider or the relay can't round-trip.
- [ ] **Step 6: Revert the spike scaffolding** (keep the `config.rs` flag + the capture notes; remove the throwaway handler). Commit ONLY the flag + a `docs/superpowers/sdd/ws-downstream-phase0-capture.md` findings file.

**GATE: proceed to Phase 1 only on GO.**

---

## PHASE 1 — Sticky relay MVP (only if Phase 0 = GO)

> The exact identity fields used in Task 5's `session_key` derivation are taken from the Phase-0 capture; the tasks below use the codex-rs-documented fields (`session_id`, `thread_id`, `window_id` in frame `client_metadata`; turn-state absent on the handshake) as the concrete default, to be confirmed against that capture before Task 5's test is finalized.

### Task 2: `POLYFLARE_WS_DOWNSTREAM` flag + route the GET `/responses` upgrade to `ws_relay`

**Files:** Modify `config.rs` (keep the Phase-0 flag), `app.rs:326`. Create `ws_relay/mod.rs` (endpoint stub). Test: `app.rs`/`ws_relay` unit test that the GET upgrade routes to the relay when on, 426 when off.
**Interfaces:** Produces `ws_relay::responses_ws_handler(ws: WebSocketUpgrade, State<AppState>, headers) -> Response`.

- [ ] **Step 1: Failing test** — with `ws_downstream=true`, a GET `/responses` with WS-upgrade headers hits `responses_ws_handler` (returns a `101`/upgrade, not `426`); with `false`, still `426`. Use axum's test client.
- [ ] **Step 2: Run → RED.**
- [ ] **Step 3: Implement** — `responses_ws_handler` accepting `WebSocketUpgrade`, `.on_upgrade(handle_relay)` where `handle_relay` is a stub that immediately closes for now. Route it in `app.rs` conditional on `state.ws_downstream`.
- [ ] **Step 4: Run → GREEN.** Clippy `-D warnings`, fmt.
- [ ] **Step 5: Commit** — `feat(ws-relay): accept WS-downstream on /responses behind POLYFLARE_WS_DOWNSTREAM`.

### Task 3: Resolve + pin the owner account (reuse the continuity engine)

**Files:** `ws_relay/mod.rs`. Test: unit test with a mock continuity + selection.
**Interfaces:** Consumes `Continuity::resolve(...) -> directive{pin_account}` and the selection engine (as ingress does). Produces `resolve_owner(state, session_key) -> AccountId` (pins via `resolve`, else selects a healthy account).

- [ ] **Step 1: Failing test** — given a `session_key` whose continuity row has `owning_account_id=A`, `resolve_owner` returns A; given an unknown session, it returns a selected healthy account and the ownership is pinned in memory for the connection.
- [ ] **Step 2: Run → RED.**
- [ ] **Step 3: Implement** `resolve_owner` calling `Continuity::resolve` (mirror how `ingress`/`watchdog` obtain `pin_account` + fall back to selection). No new selection logic — reuse the engine.
- [ ] **Step 4: Run → GREEN.** Clippy, fmt.
- [ ] **Step 5: Commit** — `feat(ws-relay): resolve+pin the conversation owner via the continuity engine`.

### Task 4: Upstream dial to the owner (reuse `ws::conn`)

**Files:** `ws_relay/mod.rs`. Test: unit test against the existing `MockWsUpstream`.
**Interfaces:** Consumes `ws::conn::connect_detailed(&Account, &forward_headers)`; the owner `AccountId` (Task 3). Produces the connected upstream `WsConn` (or a relay error mapped to a downstream close).

- [ ] **Step 1: Failing test** — `handle_relay` dials the owner account's upstream WS via `connect_detailed`; on success it holds an open upstream; on dial failure it closes the downstream with a clean code.
- [ ] **Step 2: Run → RED → implement → GREEN.** Reuse the existing forward-header + auth override from `ws::conn` (do NOT re-synthesize). Clippy, fmt.
- [ ] **Step 3: Commit** — `feat(ws-relay): dial the pinned account's upstream WS`.

### Task 5: `session_key` from the WS handshake (owner lookup key)

**Files:** `ws_relay/mod.rs` (or extend `session_key.rs` with a WS variant). Test: `session_key` unit test.
**Interfaces:** Produces `ws_session_key(handshake_headers, first_frame: Option<&Value>) -> SessionKey` — stable per conversation, content-free (hashed), derived from the identity fields the Phase-0 capture confirmed (default: `session_id`/`thread_id`/`window_id`).

- [ ] **Step 1: Failing test** — two frames of the same conversation (same identity fields) yield the same `SessionKey`; different conversations differ; the value is a sha256 hex (content-free). Use the exact fields from the Phase-0 capture.
- [ ] **Step 2: Run → RED → implement** mirroring `session_key.rs`'s `sha256_hex` derivation. → **GREEN.** Clippy, fmt.
- [ ] **Step 3: Commit** — `feat(ws-relay): derive a content-free session_key from the WS handshake`.

### Task 6: The bidirectional pump + content-free sniff

**Files:** Create `ws_relay/pump.rs`, `ws_relay/sniff.rs`; wire from `mod.rs`. Test: relay-through test (mock downstream + `MockWsUpstream`).
**Interfaces:** Consumes the open downstream `WebSocket` + upstream `WsConn`; `Continuity::observe`. Produces `run_pump(downstream, upstream, on_completed_id: impl Fn(&str))` forwarding both legs verbatim; `sniff_completed_id(text) -> Option<String>`.

- [ ] **Step 1: Failing test (sniff)** — `sniff_completed_id` returns the id from a `response.completed` frame, `None` otherwise; never touches other fields.
- [ ] **Step 2: RED → implement `sniff.rs`** (parse just `type` + `/response/id`) → GREEN.
- [ ] **Step 3: Failing test (pump)** — a text frame sent downstream appears upstream verbatim and vice-versa; a `response.completed` triggers `on_completed_id` with the id; a Ping is auto-ponged; a Close on either leg tears down both. Model the pump on codex-rs `responses_websocket.rs:62-125` (select! over a command channel + `next()`).
- [ ] **Step 4: RED → implement `pump.rs`** → GREEN. Wire `on_completed_id` to `Continuity::observe` (record owner) in `mod.rs`. Clippy, fmt.
- [ ] **Step 5: Commit** — `feat(ws-relay): bidirectional verbatim pump + content-free completed-id sniff feeding ownership`.

### Task 7: Live-verify the MVP (controller-run)

Not a code task. After Tasks 2-6:
- [x] Real `codex-polyflare exec` over `POLYFLARE_WS_DOWNSTREAM=1`, multi-turn, one conversation. Confirm: the relay round-trips; **caching works** (incremental over the relay, cached_tokens > 0 — expect ~72% like direct codex); ownership is recorded (the sniff wrote `response_id → owner`); a second concurrent conversation gets its own pinned account. Temp CONTENT-FREE instrumentation (frame `type` + cached_tokens only), reverted after. **RESULT (2026-07-20, real codex 0.144.4, gpt-5.6-sol, 2 real accounts): round-trip OK; caching ~82% (cached_tokens 6912/8380 on turn 2, beats the ~72% target); ownership rows written for 2 distinct conversations matching the dialed account; content-safety log audit clean (zero content/frame bodies); instrumentation reverted.**
- [x] Confirm no conversation content is logged/persisted anywhere (content-safety audit of the relay path).
- [x] Record results + open items for Phase 2 (reconnect/60-min) and Phase 3 (exhaustion-move/watchdog).

---

## Self-Review
- **Spec coverage:** Phase 0 gate (spec §7), relay loop (§3 → Task 6), pinning+ownership (§4 → Tasks 3/6), session_key (§9 open q → Task 5 pinned by Phase 0), flag+additive (§2/§8 → Task 2), content-free sniff + no-body (§3/§6/§8 → Tasks 6/7). Reconnect/move (§4) + watchdog (§9) are explicitly deferred to Phase 2/3. ✓
- **Placeholder scan:** the one capture-dependent value (session_key identity fields) is a real Task-5-consumes-Phase-0-output dependency with a concrete codex-rs default, not a placeholder. ✓
- **Type consistency:** `resolve_owner`/`ws_session_key`/`run_pump`/`sniff_completed_id`/`responses_ws_handler` used consistently across tasks. ✓
