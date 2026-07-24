# Full PolyFlare code audit

**Goal:** Review the complete current PolyFlare codebase for correctness, security, data-integrity, performance, maintainability, and operator-facing integration problems, and repair every supported blocking issue found.

**Why planning is required:** The audit spans request routing, persistent continuity, credential handling, schema migrations, WebSocket recovery, custom providers, telemetry, and the dashboard in a dirty resumable checkout.

**Acceptance:** Every production subsystem and its public or persisted contract has been inspected against its callers, tests, and documented invariants; supported Critical or Important findings are fixed with retained regression coverage; material recommendations are recorded with evidence and trade-offs; prompt and credential content remains excluded from operational telemetry; migration and continuity behavior remain backward-compatible; formatting, lints, focused regressions, workspace tests, dashboard tests/build, and the release build pass from the final source state. Existing unrelated or prerequisite work is preserved, and no commit, push, restart, or external configuration change occurs without separate authorization.

### Outcome 1: Architecture and risk inventory

- Work: Map crates, entry points, trust boundaries, persisted schemas, runtime ownership, protocol paths, generated dashboard assets, and the current dirty change range. Inventory unsafe code, panics, unchecked conversions, unbounded inputs, silent fallbacks, secret-bearing values, blocking operations, detached tasks, and TODO markers.
- Risks/open questions: The checkout contains prerequisite OpenRouter, backend-log, dashboard, and model-profile work that cannot be separated from the current behavior; attribution must use the live diff rather than assume `HEAD` is the audited product.
- Verify: `cargo metadata --no-deps --format-version 1 && git status --short && git diff --check`

### Outcome 2: Request execution and continuity safety

- Work: Trace HTTP/SSE, downstream WebSocket, upstream WebSocket, Messages translation, custom providers, selection, retries, watchdogs, lease ownership, cancellation, and anchor commits. Verify that retries never duplicate visible work, response IDs never cross incompatible accounts or providers, all waits and buffers are bounded, capability floors survive recovery, and no-profile traffic retains a fast path.
- Risks/open questions: Recovery and replay bugs can duplicate costly requests, wedge sessions, or invalidate response anchors.
- Verify: `cargo test -p polyflare-server --test continuity_owner_conflict --test watchdog_race --test wedge_regression --test ws_downstream_relay --test custom_provider`

### Outcome 3: Persistence, authentication, and observability safety

- Work: Audit migrations, repository queries, encryption and token lifecycle, admin/client authentication boundaries, retention, request-log classifications, usage/cost arithmetic, metrics cardinality, and structured/live logs. Verify secrets, prompts, response content, raw session identifiers, and profile instructions never enter operational telemetry or unauthenticated responses.
- Risks/open questions: Schema drift or partial writes can corrupt routing continuity or expose sensitive operator/provider data.
- Verify: `cargo test -p polyflare-store && cargo test -p polyflare-server --test content_safety_lint --test client_key_never_log_e2e --test metrics_endpoint --test read_api --test settings_api`

### Outcome 4: Dashboard and management-contract integrity

- Work: Compare TypeScript API contracts with server responses, inspect authentication and SSE fallback behavior, provider/model/profile onboarding, filtering and pagination, accessibility-critical dialogs, preference persistence, generated asset consistency, and empty/error states. Confirm management operations validate atomically and do not accidentally broaden model imports or provider visibility.
- Risks/open questions: Generated assets are embedded into the release binary, so source/build drift can ship stale behavior even when Rust tests pass.
- Verify: `cd crates/polyflare-server/dashboard && npm test && npm run build && cd ../../.. && cargo test -p polyflare-server --test dashboard --test dashboard_api`

### Outcome 5: Repairs, broad verification, and final review

- Work: Implement minimal root-cause fixes and retained regressions for supported findings, then review the entire final dirty range and surrounding callers for missed integration failures. Separate non-blocking recommendations from defects.
- Verify: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && cargo build --release -p polyflare-server && git diff --check`

## Audit outcome

**Status:** Completed on 2026-07-24.

All supported Critical and Important findings discovered during the review were repaired with
retained regressions. The final source state passed the dashboard test/build gate, focused
protocol/store/API suites, strict workspace Clippy, the complete workspace test suite, and the
optimized server release build.

### Material recommendations

1. **Make initial provider onboarding one server transaction.** The dashboard currently creates
   the provider and credential through separate authenticated requests and compensates by deleting
   the new provider if a later step fails
   (`dashboard/src/lib/queries.ts::useCreateProviderBundle`). This is resumable and the browser
   rollback covers ordinary failures, so it is not a correctness blocker. A dedicated
   server-side onboarding transaction would also cover browser termination and network loss
   between requests. The cost is another public management contract, duplicated validation or a
   validation refactor, and careful handling of an API-key-bearing request body.
2. **Give periodic maintenance tasks explicit cancellation/join ownership.** Version warming,
   catalog warming, usage refresh, and retention are detached Tokio tasks started by `serve`.
   They cannot keep the process alive and the data-critical request writer plus routing cooldowns
   now have explicit shutdown barriers, so this is not a data-integrity blocker. Owning the
   periodic tasks under a cancellation token and join set would make shutdown ordering and task
   failure reporting deterministic, at the cost of additional lifecycle plumbing and bounded
   shutdown policy for potentially active network refreshes.
