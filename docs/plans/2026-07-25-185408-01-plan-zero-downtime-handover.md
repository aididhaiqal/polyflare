# Zero-downtime handover restart

**Goal:** a deploy restart never refuses a connection and never severs an in-flight turn. Today a
`kickstart -k` kills the listener, the replacement races the dying process for the port
(`AddrInUse`), and the flat 10-second drain force-closes whatever is still streaming — which is how
2026-07-25's nine deploys destroyed the user's conversation history (the Codex client only persists
a message once its turn completes).

**Shape:** two independent changes to `main.rs`, no new dependency, no protocol change.

### Outcome 1: `SO_REUSEPORT` listener

- Work: bind the serving socket with `SO_REUSEPORT` (plus `SO_REUSEADDR`) via `libc` — already a
  workspace dependency — instead of `TcpListener::bind`. Create → `setsockopt` → `bind` → `listen`
  → `std::net::TcpListener::from_raw_fd` → `set_nonblocking(true)` →
  `tokio::net::TcpListener::from_std`. IPv4 and IPv6 `bind_addr` both supported; any `setsockopt`
  or bind failure returns the same `io::Error` the plain bind would have (no silent fallback that
  would hide a port conflict).
- Why: the replacement process can bind and accept **while the old process is still draining**, so
  there is no window where a client connect is refused, and the `AddrInUse` crash-loop we hit twice
  today becomes impossible.
- Verify: a test binds two listeners to the same ephemeral address and both succeed; the same test
  proves a plain (non-reuseport) second bind fails, so the option is provably in effect.

### Outcome 2: drain until turns are idle, not for a fixed 10s

- Work: replace the fixed `SHUTDOWN_DRAIN_TIMEOUT` sleep with a poll of
  `AppState::lease_metrics.current()` (the process-wide in-flight *turn* count) every 100 ms:
  return `Drained` the instant it reaches zero, `TimedOut` at a configurable ceiling
  (`POLYFLARE_SHUTDOWN_DRAIN_TIMEOUT_SECS`, default 300).
- Why long-lived WS sockets do NOT block this: a relay connection stays open between turns (idle
  budget up to 25 min), so waiting for handlers to end would wait forever. Waiting for zero
  in-flight *turns* is the correct barrier — once no turn is streaming, closing the remaining idle
  sockets is exactly the existing honest-close contract (`honest_close_upstream_drop`): the client
  sees its socket die between turns, reconnects — landing on the NEW process via Outcome 1 — and
  full-resends natively. Nothing is lost because nothing was in flight.
- Verify: unit tests — one in-flight lease keeps the drain waiting and it returns `Drained`
  promptly once released; a lease that never releases returns `TimedOut` at the ceiling.

### Accepted trade-off (documented, not fixed here)

Admission limits (`account_in_flight`, `*_pressure`) are per-process, so while the old process
finishes its last turns the effective concurrency against an account can briefly exceed the
configured cap — bounded by the old process's remaining turns, which are already-admitted work that
would have run anyway. Shared cross-process admission state is out of scope; the exposure window is
seconds and only on deploys.

### Gate

`cargo test --workspace` green; deploy via the AGENTS.md pipeline; then prove it live: start a
long-running turn, `kickstart -k` mid-turn, and confirm (a) the turn completes rather than 502ing,
(b) the log shows no `AddrInUse`, (c) the drain reports `Drained` rather than `timed out`.
