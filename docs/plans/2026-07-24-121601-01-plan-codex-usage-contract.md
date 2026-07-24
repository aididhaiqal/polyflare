# Codex-aligned usage data contract

**Status:** Complete — implemented and verified 2026-07-24.

**Goal:** Preserve upstream token usage exactly as Codex receives it, distinguish factual from legacy-derived data, and expose unambiguous derived usage metrics across PolyFlare storage, APIs, reports, and dashboard.

**Why planning is required:** This changes the persistent request-log schema and the meaning of public analytics fields used by routing, cost reporting, imports, and dashboard views.

**Acceptance:** Terminal Responses usage stores input, cached-input, cache-write-input, output, reasoning-output, and upstream-reported total independently; new rows identify the usage schema and finality; existing rows remain readable without fabricated cache-write or reported-total values; API totals never double-count cached or reasoning subsets; Codex effective usage is explicitly `uncached input + output`; cache ratios use input as their denominator; all request/report/dashboard surfaces use the revised contract; HTTP/SSE, WebSocket, and custom-provider paths agree; retained tests and workspace checks pass.

### Outcome 1: Canonical capture and migration

- Work: Extend the content-safe usage parser with upstream `total_tokens` and `cache_write_tokens`. Add nullable request-log columns for reported total, cache-write input, usage schema, and usage status. Mark new terminal observations as final Responses usage while classifying historical token-bearing rows as legacy evidence without backfilling values that were never observed.
- Risks/open questions: Cached input and reasoning output are subsets of input/output and must never be added again. Existing `total_tokens`/`cached_tokens` compatibility columns cannot prove what an old upstream reported.
- Verify: `cargo test -p polyflare-store && cargo test -p polyflare-server usage_capture`

### Outcome 2: One derived-metric contract

- Work: Centralize row-level derivation of API total (`reported total`, then legacy total, then input plus output), uncached input, visible output, Codex effective tokens (`uncached input + output`), and cache-read ratio. Use the same SQL expressions for overview, series, report totals, and breakdowns.
- Risks/open questions: Negative or internally inconsistent provider counts are invalid evidence; clamp subset-derived values at zero and never let malformed counts cancel valid usage.
- Verify: `cargo test -p polyflare-store request_log_repo`

### Outcome 3: End-to-end writers and readers

- Work: Feed the canonical contract from HTTP/SSE, downstream WebSocket, and custom-provider terminal events into persistence. Expose raw and derived dimensions in request rows and report payloads while retaining existing field names as compatibility aliases where needed.
- Verify: `cargo test -p polyflare-server --test custom_provider --test ws_downstream_relay --test read_api`

### Outcome 4: Dashboard semantics

- Work: Update dashboard types and request/report/overview presentation so labels distinguish API total, effective tokens, cached input, cache writes, reasoning output, and visible output. Keep the primary view compact and put detailed dimensions in request details and analytics breakdowns.
- Verify: `npm test && npm run build`

### Outcome 5: Integrated completion gate

- Work: Confirm migration behavior on a pre-change database, compare the final diff to every acceptance item, and run formatting, lint, Rust workspace tests, dashboard tests/build, and diff checks.
- Verify: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && git diff --check`

### Completion evidence

- Migration 0021 upgrades a pre-change schema and classifies historical/imported evidence without
  fabricating reported totals or cache-write counts.
- HTTP/SSE, downstream WebSocket, and custom-provider tests persist the same canonical usage
  fields and provenance.
- Store, request API, overview/report, importer, and dashboard contracts have retained regression
  coverage for raw facts, derived metrics, compatibility fallbacks, and malformed legacy evidence.
- `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo test --workspace`, dashboard `npm test`, dashboard `npm run build`, and
  `git diff --check` pass on the completed change.
