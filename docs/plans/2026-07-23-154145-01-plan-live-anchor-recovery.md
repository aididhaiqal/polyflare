# Live WebSocket Anchor Recovery

**Goal:** Recover a generating Codex WebSocket turn when its ephemeral `previous_response_id` disappears, then replace the live binary on its existing listener.
**Why planning is required:** This changes the client-visible continuity contract and includes a live local cutover against the active PolyFlare data store.
**Acceptance:** A same-account `previous_response_not_found` is intercepted once, the complete buffered `response.create` is replayed without only its top-level anchor, the client receives one successful terminal response, non-generating or incomplete frames are never replayed, telemetry distinguishes successful recovery from an unrecoverable miss, and existing cross-account recovery remains intact. Before cutover, record the target state, verify the live store, create a consistent SQLite backup plus a permissions-preserving encryption-key backup, preserve the old binary and URL as rollback, then replace the `8080` process and confirm startup, migration, dashboard health, and successful WebSocket traffic. Abort and restore the prior binary if backup, startup, migration, health, request success, or continuity checks fail.

### Outcome 1: Bounded same-account recovery
- Work: Extend `crates/polyflare-server/src/ws_relay/pump.rs` at the existing anchor-miss decision point. Reuse the in-memory generating-frame validator, remove only top-level `previous_response_id`, replay once on the current account, preserve the active turn clock, and emit fixed-label recovery/miss counters without persisting content.
- Risks/open questions: Anchorless replay is allowed only when the complete generating frame is buffered. A second miss or a frame lacking a removable anchor must remain terminal and visible rather than loop.
- Verify: `cargo test -p polyflare-server --test ws_downstream_relay same_account_anchor`

### Outcome 2: Relay regression safety
- Work: Retain end-to-end coverage for same-account recovery, one-shot failure behavior, non-generating-frame rejection, cross-account recovery, mid-turn reconnect, telemetry cardinality, and verbatim forwarding outside the recovery frame.
- Verify: `cargo test -p polyflare-server --test ws_downstream_relay`

### Outcome 3: Deployable candidate
- Work: Build the isolated candidate from the mirrored live working state, run focused workspace checks, and review the exact continuity diff against the incident and privacy constraints.
- Verify: `cargo test -p polyflare-server --test ws_downstream_relay && cargo clippy -p polyflare-server --all-targets -- -D warnings && cargo build -p polyflare-server`

### Outcome 4: Direct local cutover
- Work: Verify `/Users/wmaididhaiqal/.polyflare/store.db`, create a timestamped consistent SQLite backup and permissions-preserving backup of `/Users/wmaididhaiqal/.polyflare/key`, stop the current `127.0.0.1:8080` process, start the reviewed candidate on the same listener, verify dashboard/API health and real Codex WebSocket traffic, and retain the backup plus old binary as rollback.
- Verify: `curl -fsS http://127.0.0.1:8080/api/capabilities` plus live request-log evidence showing successful WebSocket traffic and no repeated `previous_response_not_found`.
