# ChatGPT usage gateway

**Status:** Complete (2026-07-24)

**Goal:** Make stock codex-rs account usage reads report PolyFlare's aggregate pool quota while transparently forwarding every unchanged ChatGPT backend request and recording content-safe route telemetry.

**Why planning is required:** This introduces an authenticated reverse-proxy boundary, changes the public `/wham/usage` contract consumed by Codex, and redirects a global `chatgpt_base_url` whose unrelated account, plugin, connector, and cloud calls must remain functional.

**Acceptance:** Root and named-pool `/backend-api/wham/usage` requests return a codex-rs-compatible aggregate `codex` quota using the existing capacity-weighted synthesizer and omit unavailable windows; missing aggregate evidence fails closed without inventing usage; every other `/backend-api/*` request preserves method, query, body, authentication/account headers, status, safe response headers, and streaming response bytes while targeting only the configured ChatGPT backend root; hop-by-hop headers are never replayed; telemetry records method, a bounded content-safe normalized route, status, latency, and whether the request was synthesized or passed through without recording headers, query values, credentials, or bodies; unchanged passthrough performs no account selection, token decryption, quota synthesis, or database reads, and reuses the existing HTTP client.

### Outcome 1: Codex usage response contract

- Work: Add a pure WHAM payload encoder over `SyntheticPoolQuota`, including integer percentages, canonical durations, optional reset timestamps, required plan metadata, no fabricated credits, and a clear unavailable response when fresh fleet-wide weekly evidence is missing. Add root and pool-scoped handlers backed by the existing account snapshot cache and quota synthesizer.
- Risks/open questions: WHAM requires an integer percentage and a plan type even for a heterogeneous pool; round the weighted percentage to the nearest bounded integer and use `pro` only as protocol metadata, never as the aggregation denominator.
- Verify: `cargo test -p polyflare-server chatgpt_backend`

### Outcome 2: Fast fixed-upstream passthrough

- Work: Add a catch-all ChatGPT backend gateway that derives a fixed `/backend-api` root from the configured Codex upstream, forwards the original method/query/body plus audited end-to-end headers, streams the upstream response, and filters transport headers in both directions. The synthetic usage route wins over the catch-all; named-pool passthrough does not select or mutate an account because non-usage ChatGPT backend operations remain tied to the client's own authenticated workspace.
- Risks/open questions: `chatgpt_base_url` is global. Any path not explicitly synthesized must remain a transparent client-authenticated operation, including mutating endpoints such as reset-credit consumption.
- Verify: `cargo test -p polyflare-server --test chatgpt_backend_gateway`

### Outcome 3: Content-safe discovery telemetry

- Work: Record gateway completions through the existing request-log, live-log, and request-metrics pipeline. Normalize dynamic-looking or unbounded path segments before persistence so route discovery cannot turn IDs, user strings, query values, headers, or bodies into logs; distinguish synthesized usage from passthrough in the recorded path.
- Verify: `cargo test -p polyflare-server chatgpt_backend_route`

### Outcome 4: Integration and compatibility gate

- Work: Validate the emitted JSON through the checked-out codex-rs backend parser, exercise a mock upstream passthrough boundary, run PolyFlare formatting/lints/tests, inspect the final security-sensitive diff, and document the required `chatgpt_base_url` setting without changing the user's live configuration during implementation.
- Verify: `cargo test -p codex-backend-client usage_payload_maps_primary_and_additional_rate_limits --lib && cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && git diff --check`

## Completion evidence

- Focused gateway integration: 4 passed.
- PolyFlare workspace: formatting and `git diff --check` clean; Clippy clean with warnings denied;
  full workspace test suite passed.
- Checked-out codex-rs WHAM parser: canonical usage payload parser test passed.
- Final self-review found no blocking correctness, privacy, fixed-upstream, or streaming defects.
  Passthrough completion telemetry is intentionally recorded once upstream response headers arrive,
  so it measures gateway/upstream response latency without delaying or buffering the response body.
