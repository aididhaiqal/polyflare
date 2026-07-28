# Upstream network recovery

**Status:** Complete in isolated worktree; not committed, merged, deployed, or restarted.

**Goal:** Keep PolyFlare available through temporary upstream network outages without rotating or penalizing healthy accounts, then recover automatically and safely when each upstream origin becomes reachable again.
**Why planning is required:** This changes shared request-routing and retry behavior across built-in HTTP/SSE, custom-provider, and WebSocket transports on a live production gateway.
**Acceptance:** Pre-response DNS/connect/TLS failures open an in-memory circuit per normalized upstream origin, preserve account and credential health, admit one half-open recovery probe, and wait no longer than the existing starvation budget; any received HTTP response or successful WebSocket handshake closes the circuit; working origins remain independent; no request is replayed after client-visible output; content-free telemetry exposes online, degraded, offline, and probing states; focused concurrency and transport regressions plus formatting, Clippy, and the full workspace suite pass. No migration, live-store access, service restart, deployment, commit, merge, or push is performed.

### Outcome 1: Per-origin recovery state machine
- Work: Add a shared in-memory registry keyed by normalized scheme/host/port with closed, open, and single-probe half-open behavior. Use bounded exponential retry delays with jitter, wake blocked callers when connectivity is established, and cap each wait by the caller's existing starvation budget.
- Risks/open questions: The registry must not retain credentials, paths, query strings, raw errors, request bodies, or other content-bearing data.
- Verify: `cargo test -p polyflare-server network_recovery --lib`

### Outcome 2: Consistent transport integration
- Work: Gate built-in HTTP/SSE request setup, custom-provider request setup, and upstream WebSocket dial/redial through the same registry. Treat only failures before an HTTP response or WebSocket handshake as connectivity failures; preserve existing status-based account/credential failover; do not replay established or client-visible streams.
- Risks/open questions: The WebSocket relay pump is intentionally excluded from restructuring; integration stays at its connection ownership and redial boundaries.
- Verify: `cargo test -p polyflare-server failover::tests --lib && cargo test -p polyflare-server custom_provider --lib && cargo test -p polyflare-server ws_relay --lib`

### Outcome 3: Content-free recovery telemetry
- Work: Export per-origin circuit state and counters through the existing metrics/read surfaces and present an aggregate online, degraded, offline, or probing state in the dashboard without mixing request content or raw transport errors into logs or storage.
- Verify: `cargo test -p polyflare-server network_recovery --lib && npm test`

### Outcome 4: Completion evidence
- Work: Review the whole branch diff against account-health isolation, retry-budget bounds, single-probe concurrency, origin independence, client-visible replay safety, custom-provider status behavior, WebSocket handshake recovery, dashboard behavior, and content-safety rules.
- Verify: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && npm run build && git diff --check`

### Completion evidence
- `cargo test -p polyflare-server network_recovery --lib` — 15 passed.
- `cargo test --workspace` — passed after the final notification-race fix.
- `cargo fmt --all -- --check` — passed.
- `cargo clippy --workspace --all-targets -- -D warnings` — passed.
- Dashboard `npm test` — 73 passed.
- Dashboard `npm run build` — passed and regenerated the tracked production assets.
- `git diff --check` — passed.
- Whole-range self-review found and fixed a lost-notification window by registering the owned `Notify` waiter while the circuit lock is still held; no Critical or Important findings remain.
- Unary control/backend forwarding remains deliberately outside replay recovery because its current transport error does not distinguish a pre-response failure from an established response-body failure.
