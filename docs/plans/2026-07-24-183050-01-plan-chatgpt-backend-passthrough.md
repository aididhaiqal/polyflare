# Distinct ChatGPT backend passthrough

**Goal:** Keep ChatGPT backend services functional by default while clearly distinguishing them from model traffic.
**Why planning is required:** This changes an authenticated outbound-routing boundary and the public request telemetry contract.
**Acceptance:** Ordinary `/backend-api/*` passthrough is enabled by default and can be disabled with a persisted live rollback setting. Synthetic usage remains available regardless of the setting. The remote-control WebSocket endpoint upgrades and relays bidirectionally while its enroll/refresh/pair endpoints remain HTTP passthrough. Request logs, filters, details, reports, and dashboard recent-request surfaces visibly distinguish synthetic usage and backend passthrough from model requests without storing request content.

### Outcome 1: Default-on backend boundary with rollback
- Work: Add a live, persisted `chatgpt_backend_passthrough_enabled` setting with a true default; when explicitly false, check it before URL construction or body forwarding and record disabled attempts using normalized, content-safe telemetry.
- Risks/open questions: Authorization headers and request bodies must never be logged or sent while disabled.
- Verify: `cargo test -p polyflare-server --test chatgpt_backend_gateway`

### Outcome 2: Distinct backend telemetry
- Work: Record synthetic usage, disabled attempts, and enabled passthrough under a dedicated `chatgpt_backend` provider identity while retaining database-compatible target metadata.
- Verify: `cargo test -p polyflare-server --test chatgpt_backend_gateway`

### Outcome 3: Live settings contract
- Work: Expose the new boolean in runtime settings, settings GET/PATCH metadata, persistence overlay, and the dashboard Flags section.
- Verify: `cargo test -p polyflare-server --test settings_api && cargo test -p polyflare-server runtime_settings`

### Outcome 4: Dashboard request classification
- Work: Add a shared pure classification helper and use it in provider tags, Requests rows/details/filtering, and Overview recent-request rows so backend operations have meaningful target and operation labels.
- Verify: `npm test && npm run build`

### Outcome 5: Remote-control WebSocket relay
- Work: Route `/backend-api/wham/remote/control/server` WebSocket upgrades through a transparent, bidirectional relay to the fixed ChatGPT backend, preserving the client protocol/authentication headers and recording only content-safe completion telemetry.
- Risks/open questions: Never log remote-control frames, authorization, enrollment tokens, names, or identifiers; do not accept the downstream upgrade unless the upstream handshake succeeds.
- Verify: `cargo test -p polyflare-server --test chatgpt_backend_gateway`

### Outcome 6: Release-quality verification
- Work: Review the final diff against the authenticated-boundary acceptance, then run workspace and generated-dashboard checks and build the release binary without restarting the running process.
- Verify: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && git diff --check && cargo build --release -p polyflare-server`
