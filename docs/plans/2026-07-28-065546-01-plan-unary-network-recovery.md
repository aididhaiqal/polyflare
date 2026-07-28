# Unary backend and control network recovery

**Goal:** Extend PolyFlare's per-origin recovery circuit to Codex unary control requests and replayable ChatGPT backend passthrough requests without duplicating established or non-replayable work.
**Why planning is required:** This changes retry behavior for account-authenticated control operations and client-authenticated backend passthrough on a live gateway.
**Acceptance:** DNS/connect/TLS failures are distinguished from request-stage and response-body failures; replayable control/backend requests retry against the same origin only within the live starvation budget; streamed backend bodies receive circuit protection but are never replayed; any received HTTP response closes the origin circuit; response-body failures surface without replay; account health is not penalized for origin loss; content-free metrics remain the only recovery telemetry. No migration, live-store access, deployment, restart, commit, merge, or push is performed.

### Outcome 1: Preserve response-establishment evidence
- Work: Split Codex unary errors into connect-stage, later request-stage, and established-response body failures while preserving the existing bounded body and header filtering contracts.
- Verify: `cargo test -p polyflare-codex --test control_forward`

### Outcome 2: Recover account-selected unary controls safely
- Work: Route compact, goal, JWKS, search, image, and translated unary control calls through the per-origin circuit. Retry only connect-stage failures on the same account within the starvation budget; preserve reactive 401 refresh and all status-based account-health behavior.
- Risks/open questions: A received response whose body later fails must close the connectivity circuit and surface without replay.
- Verify: `cargo test -p polyflare-server --test control_endpoints_e2e`

### Outcome 3: Protect transparent backend passthrough
- Work: Gate ChatGPT backend HTTP passthrough through the same origin circuit. Buffer only bounded, explicitly sized request bodies so connect-stage failures can be replayed; retain streaming and one-shot behavior for unknown or large bodies. Never replay after a response exists.
- Risks/open questions: The passthrough must preserve client authorization, query, headers, status, response headers, and body bytes without adding content-bearing telemetry.
- Verify: `cargo test -p polyflare-server --test chatgpt_backend_gateway`

### Outcome 4: Completion evidence
- Work: Review the whole diff for replay safety, account-health isolation, retry-budget bounds, origin sharing, reactive-auth ordering, transparent backend fidelity, and content safety.
- Verify: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && git diff --check`

## Completion evidence — 2026-07-28

- `cargo test -p polyflare-codex --test control_forward`: 9 passed.
- `cargo test -p polyflare-server --test control_endpoints_e2e`: 15 passed.
- `cargo test -p polyflare-server --test chatgpt_backend_gateway`: 11 passed.
- `cargo fmt --all -- --check`: passed.
- `cargo clippy --workspace --all-targets -- -D warnings`: passed.
- `cargo test --workspace`: passed, including unit, integration, and doc tests.
- Backend buffering is restricted to a valid, explicit `Content-Length` no greater than 4 MiB.
  Missing, malformed, or larger lengths stay on the one-shot streaming path.
- A bounded body-read failure returns `400 backend_request_body_invalid`; it is never forwarded or
  replayed, and does not incorrectly classify every stream failure as an oversized payload.
- The shared Codex HTTP client disables Reqwest's automatic redirects and protocol-NACK retries,
  keeping every replay decision inside PolyFlare's explicit recovery boundary. A redirect
  regression proves an established `3xx` is returned without contacting its destination.
