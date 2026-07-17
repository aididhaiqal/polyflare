# M5a — Upstream WebSocket Transport Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this
> plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** PolyFlare speaks WebSocket to the upstream account, sending a delta + `previous_response_id` on a
continuation turn instead of the full history — while everything above the `Executor` seam stays byte-for-byte
unchanged.

**Architecture:** A second `Executor` impl (`CodexWsExecutor`) in `polyflare-codex`, alongside `CodexExecutor`.
It owns a per-conversation connection cache, speaks the codex WS wire protocol, and **re-serializes received
frames back into SSE bytes** so the existing watchdog / translator / continuity plumbing is untouched. Selected
in `AppState` behind a flag; HTTP-SSE stays the default and the fallback.

**Tech Stack:** Rust, tokio, `tokio-tungstenite` 0.26 (promote from dev-dep), `futures-util`, `rustls` 0.23.36
+ `aws-lc-rs` 1.16.2 (workspace-pinned), `serde_json`.

**Authorities — read before writing code:**
- `docs/SPEC-M5-WEBSOCKET.md` — the design. §1a is the seam contract; §4 the error mapping; §6a the harness.
- `docs/WS-GROUND-TRUTH-CODEX.md` — the wire protocol, every claim cited to real `codex-rs` file:line. **This
  is the only authority for handshake headers, frame shapes, and lifecycle.** Do not infer wire details.
- `crates/polyflare-codex/src/executor.rs` — the `Executor` impl template ("dumb executor, smart ingress").
- `crates/polyflare-server/examples/ws_vs_sse_probe.rs` — the only in-repo *working* WS code against the real
  backend (`ws_body()` is the proven frame shape). Reference, not a test.

## Global Constraints

- **`ResponseStream = Pin<Box<dyn Stream<Item = Result<Bytes, ExecError>> + Send>>`** (`types.rs:87`). The WS
  executor MUST return exactly this, and each `Bytes` MUST be SSE-framed (`data: {json}\n\n`) — the watchdog's
  `ResponseIdSniffer` (`watchdog.rs:329-353`) and `TranslatingStream::feed_line` (`translate_stream.rs:58-79`)
  parse `data:` lines out of it. Raw WS payloads passed through would silently break both.
- **The per-turn stream ends; the socket does not.** Terminate the stream at `response.completed` /
  `response.failed` / `response.incomplete` with `Poll::Ready(None)`, and park the live connection back in the
  cache. `Continuity::observe` (the `response_id → owner` write) fires at stream end (`watchdog.rs:400-417`).
- **Dumb executor, smart ingress** (`executor.rs:8-18`). The executor NEVER synthesizes codex-identity headers;
  it relays `req.forward_headers` and overrides only auth/accept. Same division of labor on WS.
- **Content-safety (inviolable):** PolyFlare persists no conversation content. `PreparedRequest`'s `Debug`
  redacts `body` + `forward_headers` (`types.rs:42-50`) — any new type holding a body or token needs the same,
  plus a test asserting redaction. Never log a frame payload.
- **`PreparedRequest` invariant:** `raw_body.is_none()` ⇒ `body.is_some()` (`types.rs:25`).
- **Never re-parse the body in the executor** to recover the session key — that undoes the native path's
  26–51% parse win (`session_key.rs::parse_inbound`). Thread it (Task 1).
- **No new retry/failover machinery.** M5a swaps the transport under today's behavior. Cross-account failover
  is `PORTING-CODEXLB.md` B4, a separate item.
- Pin `tokio-tungstenite` to the workspace's `rustls = "=0.23.36"` / `aws-lc-rs = "=1.16.2"`; call the existing
  `ensure_rustls_crypto_provider()` (`executor.rs:20-33`) before the first WS TLS handshake.
- Verify each task: `cargo test -p <crate>` green + `cargo clippy --workspace --all-targets -- -D warnings`
  clean. CI has no network — no test may touch the live backend.

---

### Task 1: Thread the session key through the `Executor` seam

The trait carries no session key, but the WS connection cache is keyed by conversation. Ingress already
computes it — pass it down rather than recomputing.

**Files:**
- Modify: `crates/polyflare-core/src/traits.rs:14-21` (the trait)
- Modify: `crates/polyflare-codex/src/executor.rs` (impl), `crates/polyflare-anthropic/src/` (impl)
- Modify: `crates/polyflare-server/src/watchdog.rs` (`execute_with_watchdog`, `execute_recovery` call sites)
- Test: the existing `crates/polyflare-codex/tests/executor_stream.rs` (must still pass, updated for the new arg)

**Interfaces — Produces:**
```rust
#[async_trait]
pub trait Executor: Send + Sync {
    async fn execute(
        &self,
        req: PreparedRequest,
        account: &Account,
        ctx: &RequestCtx,
    ) -> Result<ResponseStream, ExecError>;
}
```

- [ ] **Step 1:** Read `traits.rs`, both `Executor` impls, `RequestCtx` (`polyflare-server/src/session_key.rs`
      — note which crate it lives in; if it is NOT in `polyflare-core`, moving it there is part of this task,
      since `polyflare-core` cannot depend on `polyflare-server`). Decide and document: move `RequestCtx` to
      core, or pass a narrower `&SessionKey`. Prefer the narrowest type that serves the WS cache.
- [ ] **Step 2:** Change the trait; update both existing impls to ignore the new param (`_ctx`).
- [ ] **Step 3:** Update the two call sites in `watchdog.rs` — the `ctx` is already in scope there.
- [ ] **Step 4:** `cargo test -p polyflare-codex -p polyflare-server` — green, no behavior change.
- [ ] **Step 5:** Commit: `refactor(core): thread RequestCtx through the Executor seam`

---

### Task 2: WS mock upstream in the testkit

`MockUpstream` is HTTP-only (`polyflare-testkit/src/lib.rs:36-230`). Everything after this task depends on this.

**Files:** Create `crates/polyflare-testkit/src/ws_mock.rs`; modify `lib.rs`, `Cargo.toml`.

**Interfaces — Produces:** a `MockWsUpstream` mirroring `MockUpstream`'s idiom: `spawn() -> String` (a `ws://`
base URL), scripted responses, and recorders. It MUST be able to script, because Tasks 5–7 test each:
- a normal turn (frames → `response.completed` carrying an id)
- `previous_response_not_found` in a `response.failed`
- the wrapped error envelope `{"type":"error","status":u16,"error":{code,message},"headers":{}}` — including
  `websocket_connection_limit_reached` and a 429 with a `Retry-After` in `headers`
- close mid-stream, before any terminal frame
- stall past the idle timeout
- recorders: `handshake_count()` (proves connection REUSE across turns), `frames()` /
  `last_frame_input_len()` / `last_frame_anchor()` (proves a delta was actually a delta)

- [ ] **Step 1:** Write a failing test that spawns the mock, connects with `tokio-tungstenite`, sends a
      `response.create`, and asserts it receives the scripted `response.completed`.
- [ ] **Step 2:** Run it — expect a compile failure (`ws_mock` doesn't exist).
- [ ] **Step 3:** Implement with `axum::extract::ws::WebSocketUpgrade` (axum 0.8 is already a testkit dep —
      confirm; if not, a raw `tokio-tungstenite` server on an ephemeral port is fine). Mirror `MockUpstream`'s
      `Arc<Mutex<..>>` recorder shape.
- [ ] **Step 4:** Add tests for EACH scripted behavior above. Assert real values.
- [ ] **Step 5:** Commit: `test(testkit): WebSocket mock upstream`

---

### Task 3: WS connection + handshake (fingerprint parity)

**Files:** Create `crates/polyflare-codex/src/ws/conn.rs`; modify `crates/polyflare-codex/Cargo.toml`
(promote `tokio-tungstenite = "0.26"` to `[dependencies]`), `lib.rs`.

**Interfaces — Produces:** `WsConn` — owns the socket + `{last_response_id, last_input_count,
last_input_fingerprint}`. `WsConn::connect(account: &Account, forward_headers: &[(String,String)]) -> Result<WsConn, ExecError>`.

**The handshake is `WS-GROUND-TRUTH-CODEX.md` §1 — follow it exactly:**
- URL = `{account.base_url}/responses` with `https→wss` (§1).
- `Authorization: Bearer {account.bearer_token}` + `chatgpt-account-id` from `account.chatgpt_account_id` —
  same override rules as `executor.rs:104-118`.
- `OpenAI-Beta: responses_websockets=2026-02-06`, **insert (never append)** — §7.2.
- **`x-codex-turn-state` MUST NOT be a handshake header** (§7.1) — it goes only in `client_metadata`, only once
  the server has supplied one. This is the single easiest thing to get wrong; it is the OPPOSITE of the HTTP path.
- Offer `permessage-deflate` (§1). Never send a client-initiated Ping — auto-Pong only (§2/§7.3).
- Call `ensure_rustls_crypto_provider()` first.

- [ ] **Step 1:** Failing test: connect to the Task-2 mock; assert the handshake carried `OpenAI-Beta` exactly
      once with the exact value, carried `Authorization`, and did **NOT** carry `x-codex-turn-state`.
- [ ] **Step 2:** Run it — expect failure.
- [ ] **Step 3:** Implement `WsConn::connect`.
- [ ] **Step 4:** Tests green.
- [ ] **Step 5:** Commit: `feat(codex): WS connection + codex-parity handshake`

---

### Task 4: Frame codec — build `response.create`, re-serialize frames to SSE

**Files:** Create `crates/polyflare-codex/src/ws/codec.rs`.

**Interfaces — Produces:**
- `build_response_create(body: &Value, anchor: Option<&str>, input: &[Value], generate: Option<bool>) -> Value`
  — the exact shape is `WS-GROUND-TRUTH-CODEX.md` §3 (`ResponseCreateWsRequest`) and the PROVEN `ws_body()` in
  `crates/polyflare-server/examples/ws_vs_sse_probe.rs`. Omit `previous_response_id` when `None`; omit
  `generate` unless set.
- `frame_to_sse(frame: &str) -> Option<Bytes>` — re-serialize a received frame into `data: {json}\n\n`.
- `classify(frame: &Value) -> FrameClass` — `Event` / `Terminal` / `Error(ExecError)`, per §3 + §4's table.

- [ ] **Step 1:** Failing tests: an anchorless build omits `previous_response_id` entirely (assert the KEY is
      absent, not null); an anchored build sets it and carries only the delta `input`; `frame_to_sse` produces
      exactly `data: {...}\n\n`; `classify` maps `response.completed`/`response.failed`/`response.incomplete`
      to Terminal, the wrapped error envelope to `ExecError::UpstreamStatus` with the right status, and an
      unknown `type` to Event (ground truth §3: unknown types are ignored, never fatal).
- [ ] **Step 2:** Run — expect failure. **Step 3:** Implement. **Step 4:** Green.
- [ ] **Step 5:** Commit: `feat(codex): WS frame codec`

---

### Task 5: The turn stream — ends while the socket stays open

**Files:** Create `crates/polyflare-codex/src/ws/turn.rs`.

**Interfaces — Produces:** a `Stream<Item = Result<Bytes, ExecError>>` over one turn: read frames → `frame_to_sse`
→ yield; at a Terminal frame, yield it and then `Poll::Ready(None)` **without closing the socket**; record
`last_response_id` from `response.completed`'s `response.id` onto the `WsConn`; park the conn back in the cache.

- [ ] **Step 1:** Failing tests: (a) the stream yields the scripted frames as SSE and ENDS at the terminal
      frame; (b) the socket is still open afterwards and a SECOND turn on it produces
      `mock.handshake_count() == 1` — this is the test that proves reuse, i.e. the whole milestone;
      (c) `last_response_id` is captured from the completed frame.
- [ ] **Step 2:** Run — expect failure. **Step 3:** Implement. **Step 4:** Green.
- [ ] **Step 5:** Commit: `feat(codex): per-turn WS stream with connection parking`

---

### Task 6: The delta decision

**Files:** Create `crates/polyflare-codex/src/ws/delta.rs`.

**Interfaces — Produces:** `fn plan_request(conn: &WsConn, body: &Value) -> RequestPlan` where
`RequestPlan::{Incremental{anchor, suffix}, Full}`. Mirrors codex's own rule (`WS-GROUND-TRUTH-CODEX.md` §3 /
`client.rs:306-359,1222-1253`): incremental ONLY if the socket has a `last_response_id` AND the new input is a
**strict extension** of what this socket last sent AND the non-input fields match (model, instructions, tools,
tool_choice, parallel_tool_calls, reasoning, service_tier, text). Any mismatch ⇒ `Full`. **Do not be cleverer
than codex here.**

- [ ] **Step 1:** Failing tests, each asserting real values: strict extension ⇒ `Incremental` with the suffix
      ONLY; a changed model ⇒ `Full`; a changed earlier item (not an extension) ⇒ `Full`; no `last_response_id`
      ⇒ `Full`; a shorter input ⇒ `Full`.
- [ ] **Step 2:** Run — expect failure. **Step 3:** Implement. **Step 4:** Green.
- [ ] **Step 5:** Commit: `feat(codex): WS incremental-vs-full request planning`

---

### Task 7: `CodexWsExecutor` — cache, recovery, error mapping

**Files:** Create `crates/polyflare-codex/src/ws/executor.rs`; modify `lib.rs`.

**Interfaces — Produces:** `CodexWsExecutor` implementing `Executor`. Holds the connection cache keyed by the
Task-1 session key. Per `SPEC-M5-WEBSOCKET.md` §4:

| Event | Do | Above the seam |
|---|---|---|
| `previous_response_not_found` | strip anchor → full resend on the SAME socket, **bounded attempts** | never surfaces |
| `websocket_connection_limit_reached` / idle timeout / close-before-terminal | reconnect → full resend, bounded | never surfaces |
| handshake 426 | HTTP-SSE for this session (see below) | never surfaces |
| handshake/transport failure | `ExecError::Upstream` (ingress 502s, as today) | unchanged |
| 429 envelope | `ExecError::UpstreamStatus(FailureSignal{status:429, retry_after})` — parse `retry_after` off the envelope's `headers` map the way `retry_after_secs` reads the HTTP header (`executor.rs:34-40`) | unchanged: `record_rate_limit` |
| terminal `response.failed` code | re-frame as SSE, pass through | the error, as today |
| mid-stream failure after first byte | `Poll::Ready(Some(Err(ExecError::Stream(..))))` | unchanged: `record_transient_error` |

**Every recovery path is a BOUNDED retry with an attempt counter — never a loop.** Exhausted ⇒ surface the
error. **A full resend after an anchor miss silently re-bills a whole history: log it content-free (a reason
code + counts, never a payload) so the wedge rate is measurable.** codex-lb wedged ~31% and nobody could see it.

**Fallback scope** — a deliberate divergence from codex (§4): codex flips `disable_websockets` one-way for the
process; PolyFlare scopes fallback per session (426) / per turn (transport) with a cooldown before re-attempting
WS for that account. Document this at the decision site so nobody "fixes" it back.

- [ ] **Step 1:** Failing tests against the Task-2 mock, one per row: anchor-miss recovers and the client sees
      only a clean stream; connection-limit reconnects (assert `handshake_count() == 2`); a 429 envelope yields
      `ExecError::UpstreamStatus` with `status == 429` and the parsed `retry_after`; close-mid-stream after the
      first byte yields `ExecError::Stream`; a bounded recovery gives up rather than looping (assert the
      attempt count).
- [ ] **Step 2:** Run — expect failure. **Step 3:** Implement. **Step 4:** Green.
- [ ] **Step 5:** Commit: `feat(codex): CodexWsExecutor with bounded anchor/reconnect recovery`

---

### Task 8: Wire it in behind a flag + e2e

**Files:** Modify `crates/polyflare-server/src/app.rs` (`AppState`, `executor_for`),
`crates/polyflare-server/src/config.rs`; create `crates/polyflare-server/tests/ws_upstream_e2e.rs`.

**Interfaces — Produces:** `POLYFLARE_WS_UPSTREAM` (default **off**). Off ⇒ `CodexExecutor` exactly as today.

- [ ] **Step 1:** Failing e2e: with the flag on and a WS mock, two sequential turns through the real ingress
      stack produce ONE handshake, the second frame carries an anchor + delta-only input, and the client
      receives well-formed SSE both times. Then: with the flag OFF, the HTTP `MockUpstream` path is used and
      every existing test still passes (the regression net).
- [ ] **Step 2:** Run — expect failure. **Step 3:** Implement. **Step 4:** Green.
- [ ] **Step 5:** `cargo test --workspace` — the full suite, especially `wedge_regression`, `watchdog_race`,
      `no_anchor_failover`, `signal_client`, `failure_routing`. **Any regression here is a blocker, not a nit:
      those tests are the wedge fix.**
- [ ] **Step 6:** Commit: `feat(server): select the WS upstream transport behind POLYFLARE_WS_UPSTREAM`

---

### Task 9: Extend the fingerprint-parity gate to the WS handshake

The WS handshake is a NEW egress surface and gets the same treatment as the HTTP one (E4(a)). Ground truth §1
flags wire header **byte order** as unverified — the capture resolves it.

**Files:** Modify `crates/polyflare-server/src/fingerprint_capture.rs` + the parity gate test.

- [ ] **Step 1:** Read how the HTTP golden is captured/asserted today; mirror it for the WS handshake.
- [ ] **Step 2:** Capture a real handshake via `POLYFLARE_CAPTURE_FINGERPRINT` + `scripts/codex-polyflare`
      (manual, needs a real account — the CAPTURE is manual; the GATE runs in CI off the committed golden).
- [ ] **Step 3:** Assert PolyFlare's synthesized handshake matches the golden.
- [ ] **Step 4:** Commit: `test(server): fingerprint-parity gate covers the WS handshake`

---

### Task 10: Measure the rate-limit question — the milestone's actual premise

`SPEC-M5-WEBSOCKET.md` §8: the 86× is an **upload** measurement. Whether prefilled/cached tokens count against
**rate limits** — the real constraint — is **unverified**. This task answers it. If the answer is "prefilled
tokens still count fully against rate limits," the milestone's quota rationale is wrong and we should say so
loudly rather than ship a claim we can't support.

**Files:** Create `crates/polyflare-server/examples/ws_ratelimit_probe.rs` (an example, like the other probes —
live credentials, never CI).

- [ ] **Step 1:** Read `examples/ws_vs_sse_probe.rs` for the live-account harness idiom.
- [ ] **Step 2:** Probe: run N identical continuation turns over WS (incremental) and over HTTP (full resend)
      on two comparable accounts, reading each account's `/wham/usage` windows before and after (see
      `usage_refresh.rs:48-55` for the URL rule). Compare `used_percent` movement per turn.
- [ ] **Step 3:** Write the result into `docs/TRANSPORT-FINDINGS-2026-07-17.md` as a new measured fact —
      **including if it's negative.** Update `SPEC-M5-WEBSOCKET.md` §8 to a measured statement.
- [ ] **Step 4:** Commit: `docs: measure whether WS prefill reduces rate-limit consumption`

---

## Suggested order

1 → 2 → 3 → 4 → 5 → 6 → 7 → 8, strictly sequential (each builds on the last; Task 2 gates everything).
**Task 10 can run any time after Task 8 and should — it validates the premise.** Task 9 last.
