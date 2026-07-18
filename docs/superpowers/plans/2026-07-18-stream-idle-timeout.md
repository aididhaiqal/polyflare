# Proxy-Wide Response-Stream Idle Timeout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use `- [ ]`.

**Goal:** Bound a stalled upstream response stream. Today `ObservingStream::poll_next` (`watchdog.rs:747`)
returns `Poll::Pending` forever on mid-stream silence — a hung upstream holds the client connection + the task
indefinitely. Add a per-poll idle deadline (reset on each byte) that TERMINATES the stream after N seconds of
silence. This is the tracked codex-lb-parity M2 follow-up (codex's own `stream_idle_timeout` = 300s) and closes
a real reliability gap that matters the moment PolyFlare runs non-locally (post-D18).

**Architecture:** An idle deadline on `ObservingStream`, reset whenever a byte arrives. In the `Streaming`
state, race the inner poll against the deadline; on expiry with no byte, record a transient error and end the
stream with a content-free idle error. TERMINATE, not recover — a mid-stream stall is post-commit (bytes already
relayed), so recovery/failover would double-relay (the commit barrier). This is DISTINCT from the pre-first-byte
Armed wedge timer (`watchdog.rs:243`), which recovers because nothing was relayed yet.

**Authority:** the B5-T1 / cyber-detection reviews (this session) established the gap precisely: no idle/read
timeout anywhere — `reqwest` has only `connect_timeout`, no server `TimeoutLayer`, `ObservingStream` bare
`Poll::Pending`. `DESIGN-DECISIONS.md` M2-follow-ups lists it. codex ref: `stream_idle_timeout` default 300000ms
(`model-provider-info/src/lib.rs:26`), enforced per-`.next()`.

## Global Constraints

- **TERMINATE on mid-stream idle, never recover (the commit-barrier reason).** By the time `ObservingStream` is
  streaming, `commit.mark()` has fired (`watchdog.rs:756`) — bytes reached the client. A failover/recovery here
  would double-relay. So the idle-timeout ENDS the stream (a content-free `ExecError::Stream` idle error, then
  `Poll::Ready(None)`/Done), it does NOT reselect. This composes with — does not replace — the Armed first-chunk
  wedge timer (which recovers pre-relay).
- **Only fire on GENUINE silence.** The deadline resets on EVERY byte (`Poll::Ready(Some(Ok(_)))`). A stream
  producing bytes within the window must NEVER be cut off. A test must prove an actively-streaming (slow but
  alive, bytes within the window) request completes normally with zero spurious timeout.
- **The wedge fix stays sacred.** This edits `ObservingStream::poll_next` — the wedge fix's home. `record_success`
  on clean EOF, `record_transient_error` on mid-stream drop, `Continuity::observe` at true end, and the commit
  witness — all UNCHANGED except adding the idle path. The 5 wedge + cyber + failover + starvation suites MUST
  stay green. The crux task (Task 1) gets adversarial review.
- **Content-safety:** the idle error carries a fixed reason string, never a body/frame/token.
- **Disable lever:** `POLYFLARE_STREAM_IDLE_TIMEOUT_SECS=0` ⇒ no idle timeout (today's behavior — the clean
  rollback). Default matches codex (300s).
- Verify each task: `cargo test --workspace` green + `cargo clippy --workspace --all-targets -- -D warnings`.

---

### Task 1: The mid-stream idle deadline in `ObservingStream` (THE CRUX — adversarial review)

**Files:** `crates/polyflare-server/src/watchdog.rs` (`ObservingStream` struct ~722, `poll_next` ~747,
`wrap_stream` where `ObservingStream` is built); tests.

**Read fully first:** `ObservingStream::poll_next` (747-797) — the `Streaming` state polls `inner`, `mark()`s
commit on a byte, `record_transient_error` on `Err`, `record_success` + `observe` on `None`. The gap is the
`Poll::Pending => return Poll::Pending` at ~786 (no deadline).

**Implement:**
- Add to `ObservingStream` an idle timeout `Duration` (0 / `None` = disabled) + a `Pin<Box<tokio::time::Sleep>>`
  (or reset an `Instant` deadline) representing "no byte since". Initialize when the stream is built (so it also
  bounds the first byte for a Disarmed request that has no pre-relay wedge timer).
- In `Streaming`: on `Poll::Ready(Some(Ok(bytes)))` — RESET the deadline (a byte arrived) — then the existing
  `mark`/`feed`/return. On `Poll::Pending` — poll the sleep deadline: if it's `Ready` (idle elapsed) ⇒
  `record_transient_error(&account, now)` (a stalled upstream is a transient account fault, like a mid-stream
  drop) and END the stream: yield `Poll::Ready(Some(Err(ExecError::Stream("upstream idle timeout"))))` then move
  to `Done` (or go straight to a terminal that the body renders as a clean end). If the sleep is `Pending` too ⇒
  `Poll::Pending` (register BOTH wakers — the inner stream's and the timer's — so a wake from either re-polls).
- **DO NOT recover/reselect** — this is post-commit. And DO NOT alter the `Err`/`None`/`Observing` arms' existing
  behavior. When disabled (timeout 0), behave byte-for-byte as today (no deadline, bare `Poll::Pending`).
- Thread the timeout `Duration` from `wrap_stream`'s caller (Task 2 supplies it from config; for Task 1 use a
  parameter with a test-provided value — do not hardcode).

**Tests (failing first; assert real values — need a mock that sends a byte then STALLS):**
- a stream that relays ≥1 byte then goes SILENT (a `MockUpstream` byte-then-stall — check `stall_after_first` /
  the existing stall modes; extend testkit minimally if needed) ⇒ the ObservingStream ENDS within ~idle-timeout
  (the test completes bounded, NOT hangs), the client got its byte(s) then a terminal, and `record_transient_error`
  fired. Use a SHORT idle timeout in the test (e.g. 200-400ms) so it's fast.
- **no spurious timeout:** a stream producing a byte every (idle/2) that then cleanly EOFs ⇒ completes normally,
  `record_success` fired, NO idle error, `observe` ran (ownership recorded). This proves the deadline resets per byte.
- **disabled (timeout 0):** a byte-then-stall stream ⇒ behaves as TODAY (no idle termination — document that this
  is the pre-fix behavior; the test asserts the disabled path doesn't terminate, bounded by the test's own
  timeout guard so it doesn't actually hang CI).
- wedge intact: `response.completed`→observe, clean-EOF→record_success, mid-stream `Err`→record_transient_error —
  all still fire (existing wedge suites + a spot test).
- **commit barrier:** the idle path fires only AFTER `commit.mark()` (bytes relayed), so it TERMINATES, never
  triggers a reselect — assert no second upstream attempt happens on idle.

- [ ] **Step 1:** Read poll_next fully. Write the failing byte-then-stall test. **Step 2:** Run — it HANGS today
      (guard the test with its own timeout so it fails as "timed out" not hangs forever). **Step 3:** Implement
      the idle deadline. **Step 4:** Green; 5 wedge + cyber + failover + starvation suites green.
- [ ] **Step 5:** Commit: `feat(server): mid-stream response idle timeout (terminate on stall)`

---

### Task 2: Config + wiring + e2e

**Files:** `crates/polyflare-server/src/config.rs` (`POLYFLARE_STREAM_IDLE_TIMEOUT_SECS`, default 300, `0`=disabled,
clamp sane upper e.g. ≤3600; startup-resolved into `AppState`/`ServeConfig`, NOT per-request), `watchdog.rs`/
`app.rs` (thread it to `wrap_stream`), tests.

- [ ] **Step 1:** Failing tests: config `=30`⇒30, unset⇒300, `=0`⇒disabled, malformed⇒default 300, clamp an
      absurd value. e2e through the real `build_app`: a byte-then-stall upstream ⇒ the client gets its bytes then
      a bounded terminal (not a hang), within ~the configured idle (use a short configured value in the test).
      `=0` ⇒ no idle bound (guarded test). **Step 2:** Run — fail. **Step 3:** Implement config + thread it (mirror
      `POLYFLARE_MAX_ACCOUNT_ATTEMPTS`/`STARVATION_*` startup-resolution). **Step 4:** Green.
- [ ] **Step 5:** Commit: `feat(server): POLYFLARE_STREAM_IDLE_TIMEOUT_SECS (default 300, 0=disabled)`

---

## Suggested order

1 (the idle deadline, crux, adversarial review) → 2 (config + wiring + e2e). After Task 2, a stalled upstream
can no longer hang a client indefinitely — bounded proxy-wide (Armed mid-stream + Disarmed all), with the Armed
pre-relay wedge-recover path unchanged. B8 (health-tier) / B10 (thundering-herd) remain separate.
