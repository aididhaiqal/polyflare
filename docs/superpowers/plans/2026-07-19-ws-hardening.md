# WS Upstream Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Close three upstream-WS gaps found by comparing PolyFlare's `ws` module against CLIProxyAPI's codex WS executor: (1) HIGH — the connection cache is account-blind, so a failover can serve/bill a turn on the previously-cached account's socket; (2) MED — no bounded WS connect/handshake timeout; (3) MED — a stalled-but-alive socket is never poisoned, so it re-stalls on every reuse.

**Architecture:** All three are localized to `crates/polyflare-codex/src/ws/`. (1) folds `account.id` into `conn_key` (executor.rs) so a different account can never reuse another's socket. (2)+(3) add bounded `tokio::time::timeout`s in `conn.rs` — around the dial (`connect_detailed`) and around each frame read (`recv_frame`, poisoning the conn on idle so it's evicted on next reuse).

**Tech Stack:** Rust, tokio, tokio-tungstenite.

**Context:** upstream WS is behind `POLYFLARE_WS_UPSTREAM` (default OFF), so these are fix-before-enabling-WS hardenings, not a live-prod outage. The WS `conn_key` was recently merged and LIVE-VERIFIED to cache ~88%; do not regress it.

## Global Constraints

- **Wedge sacred:** do NOT touch `ObservingStream::poll_next` (polyflare-server watchdog) or continuity/ownership recording. `recv_frame` (Task 2) is on the wedge-adjacent read path — `wedge_regression` (`crates/polyflare-server/tests/wedge_regression.rs`, 2 tests) MUST still pass.
- **conn_key is transport-only / ownership-blind:** it feeds ONLY the socket cache, never session_key/continuity. Keep it so.
- **Content-free:** `account.id` is a non-secret internal id already stored in `RequestLog.account_id` — folding it into `conn_key` is fine; conn_key is never logged (keep it that way). Never log a body/frame/token/bearer.
- **Caching not regressed:** within a session on ONE account (the common case), `account.id` is stable → `conn_key` stable → socket reused → incremental caching unchanged. Only an account CHANGE (failover) yields a new key (correct: the new account needs its own authenticated socket).
- **Idle read deadline must not kill legit turns:** set it to codex's own stream-idle tolerance (300s — mirrors `polyflare-server`'s `DEFAULT_STREAM_IDLE_TIMEOUT` and codex's `stream_idle_timeout`), so it only fires on a genuinely stalled socket, never a slow-but-alive generation.
- **Clippy clean under `-D warnings`; `cargo fmt --all`.**

## Interfaces

- `conn_key` gains a leading `account.id` component: `"{account_id}:{session}:{fingerprint}[:{window_hash}]"`. The `None`-session-key path is unchanged (`None`).
- `conn.rs` gains two consts: `WS_CONNECT_TIMEOUT: Duration` (30s) and `WS_READ_IDLE_TIMEOUT: Duration` (300s), and `WsConn` poisons itself (`is_closed()` → true) on a read-idle elapse.

---

### Task 1: Account-aware conn_key (HIGH)

**Files:**
- Modify: `crates/polyflare-codex/src/ws/executor.rs` (`execute`, conn_key computation ~555-561; the `# The connection cache` module doc)
- Test: `executor.rs` unit test

**Interfaces:**
- Consumes: `account: &Account` (already a param of `execute`; `Account.id: String` at `polyflare-core/src/types.rs:97`).

**Background:** `connect_and_cache` (executor.rs:262-270) reuses a cached socket by `conn_key` alone, checking only `is_closed()` — never the account. `conn_key` today is `session_key:fingerprint[:window_hash]` with no account. On failover (same session, owner A → account B) the key is identical, so B's turn would be driven over A's still-live authenticated socket → served/billed on A. Folding `account.id` in as the FIRST component makes a different account resolve a different key → its own socket.

- [ ] **Step 1: Write the failing test** in `executor.rs` `mod tests` (model the existing `distinct_window_ids_same_session_and_model_get_distinct_sockets`). `two_accounts_same_session_and_body_get_distinct_sockets`: two `execute` calls with the SAME `session_key`, SAME body (same fingerprint), SAME/absent window-id, but DIFFERENT `account.id` (e.g. "acct-A" vs "acct-B") → must get DISTINCT sockets (2 handshakes); and two calls with the SAME account+session+body REUSE one socket (1 handshake). Construct two `Account`s differing only in `id`.

- [ ] **Step 2: Run to verify fail** — `cargo test -p polyflare-codex --lib ws` → new test FAILs (currently shares one socket → 1 handshake, expected 2).

- [ ] **Step 3: Prepend `account.id`** at the conn_key computation in `execute`:

```rust
        let conn_key = session_key.as_ref().map(|sk| {
            let base = format!("{}:{sk}:{}", account.id, crate::ws::delta::non_input_fingerprint(&body));
            match ctx.conn_discriminator.as_deref() {
                Some(disc) => format!("{base}:{}", crate::ws::delta::sha256_hex(disc.as_bytes())),
                None => base,
            }
        });
```

(`account.id` is a non-secret internal id already in `RequestLog.account_id`; conn_key is never logged. Confirm `account` is in scope here — it is the `execute` param.)

- [ ] **Step 4: Update the `# The connection cache` module doc** to state the key now leads with `account_id` and why: a socket is authenticated per-account at handshake, so a different account MUST get its own socket — a failover can never reuse the prior account's connection.

- [ ] **Step 5: Run tests** — `cargo test -p polyflare-codex --lib ws` (new test + existing `distinct_window_ids…` + `interleaved_models…` + reuse + delta/recovery all PASS), `cargo test -p polyflare-codex --lib` (incl the WS suite). Controller will separately run `wedge_regression` + `ws_upstream_e2e`. Clippy `-D warnings`, fmt.

- [ ] **Step 6: Commit** — `fix(ws): fold account_id into conn_key so a failover never reuses another account's socket`.

---

### Task 2: Bounded WS connect + read timeouts (MED)

**Files:**
- Modify: `crates/polyflare-codex/src/ws/conn.rs` (`connect_detailed` ~209-223; `recv_frame` ~279-... ; `WsConn` closed/poison flag; new consts)
- Test: `conn.rs` unit tests

**Background:** `connect_detailed` (conn.rs:223) awaits `connect_async_with_config` with NO timeout — a hung dial stalls a turn until the OS TCP timeout. `recv_frame` (conn.rs:281) awaits `self.socket.next()` with no deadline — a stalled-but-alive socket (backend silent, TCP open) never poisons, so the same `conn_key` re-stalls every reuse (the watchdog cancels the STREAM at 300s but leaves the socket cached with `is_closed()==false`).

- [ ] **Step 1: Add consts** near the top of `conn.rs`:

```rust
/// Bounded dial/handshake budget — a hung TCP/TLS/WS-upgrade must not stall a turn until the OS
/// TCP timeout. Mirrors CLIProxyAPI's 30s codex WS dial bound.
pub(crate) const WS_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
/// Max silence between received frames before the socket is treated as stalled (poisoned →
/// `is_closed()` → evicted on next reuse). Set to codex's own `stream_idle_timeout` default (300s,
/// = `polyflare-server`'s `DEFAULT_STREAM_IDLE_TIMEOUT`) so it fires ONLY on a genuinely dead socket,
/// never on a slow-but-alive generation.
pub(crate) const WS_READ_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
```

- [ ] **Step 2: Failing test — connect timeout.** In `conn.rs` `mod tests`, add `connect_detailed_times_out_on_a_black_hole` (or the closest expressible form): dial a non-accepting/black-hole address (e.g. a `TcpListener` bound but never `accept`ing, or `10.255.255.1`) with a SHORT injected timeout, assert it returns a handshake/transport error within the budget rather than hanging. If the const can't be overridden per-call, refactor `connect_detailed` to take the timeout (or add a `connect_detailed_with_timeout` used by the test) — do NOT `sleep(300s)` in a test. Run → observe current hang / FAIL.

- [ ] **Step 3: Wrap the dial** in `connect_detailed`:

```rust
    match tokio::time::timeout(WS_CONNECT_TIMEOUT, tokio_tungstenite::connect_async_with_config(request, Some(ws_config()), false)).await {
        Err(_elapsed) => /* map to the same ConnectOutcome variant a dial timeout/transport error uses */,
        Ok(inner) => match inner { /* existing Connected / rejection / error arms unchanged */ },
    }
```

Map the elapsed case to the existing "other handshake/transport failure (…timeout…)" outcome (conn.rs:151) — do not invent a new public variant unless required.

- [ ] **Step 4: Failing test — read idle poison.** Add `recv_frame_poisons_conn_on_read_idle`: drive a `WsConn` whose socket never yields a frame with a SHORT injected read timeout; assert `recv_frame` returns an idle error AND `conn.is_closed()` becomes true afterward. Same rule: make the timeout injectable for the test rather than sleeping 300s. Run → FAIL (currently hangs / never poisons).

- [ ] **Step 5: Wrap the read** in `recv_frame`:

```rust
    match tokio::time::timeout(WS_READ_IDLE_TIMEOUT, self.socket.next()).await {
        Err(_elapsed) => {
            self.closed = true; // poison: is_closed() -> true -> evicted on next connect_and_cache reuse
            Err(ExecError::Stream(format!("{WS_READ_IDLE_MARKER}: no frame within idle timeout")))
        }
        Ok(next) => { /* existing match on next: Some(Ok)/Some(Err)/None unchanged */ }
    }
```

Add a `WS_READ_IDLE_MARKER` const if a marker is useful for the executor/ingress to classify it (mirror the existing `SOCKET_CLOSED_MARKER` pattern in `turn.rs`); otherwise a plain stream error is fine. Set `self.closed = true` using whatever field `is_closed()` already reads (conn.rs:103) — do NOT add a second flag.

- [ ] **Step 6: Run tests** — `cargo test -p polyflare-codex --lib ws` (both new tests + all existing conn/turn/delta/executor tests PASS). Clippy `-D warnings`, fmt. Note any test that needed the timeout made injectable.

- [ ] **Step 7: Commit** — `fix(ws): bound the WS dial + per-read idle timeouts (poison a stalled socket so it is not reused)`.

---

### Task 3: Live-verify (controller-run)

Not a code task. After Tasks 1-2 merge-ready, the controller:
- [ ] Rebuilds release, runs real `codex-polyflare` traffic over `POLYFLARE_WS_UPSTREAM=1` — confirms WS still connects, streams, and CACHES (incremental anchor chain, cached_tokens > 0) — i.e. no regression from the account_id prefix or the timeouts (temp WSDBG conn_key/plan log, reverted after).
- [ ] Confirms (temp log) `conn_key` now leads with the account id (account-aware) for real traffic.
- [ ] Notes: the failover-reuses-wrong-account bug is proven fixed DETERMINISTICALLY by Task 1's unit test (a live mid-session failover over WS is not cheaply reproducible — same limitation as prior WS verifies); live-verify covers the no-regression half.

---

## Self-Review checklist (controller, before Task 1)

- Back-compat: Task 1 keeps single-account sessions stable (account.id stable) → caching preserved; only failover changes the key (correct). ✓
- Wedge: Task 2 touches `recv_frame` — `wedge_regression` in the gate. ✓
- No sleeps in tests: both timeout tests inject a short budget, never sleep the real 30s/300s. ✓
- Idle deadline = 300s = codex tolerance → never kills a legit slow turn. ✓
