# Cross-provider model balancing

**Goal:** Let one public custom-model id route across multiple compatible providers and credentials without losing Codex conversation context.
**Why planning is required:** This changes the persisted model-routing identity, catalog guarantees, HTTP retry ownership, and wedge-sensitive downstream WebSocket recovery behavior.
**Acceptance:** Existing custom providers remain routable after an additive migration; one public model can have multiple provider targets; selection is health-, weight-, and concurrency-aware; Priority requests prefer and exhaust Priority-capable targets, then may fall back to an eligible Standard target with the Priority marker removed; repeated custom-provider turns softly prefer the last successful provider and credential for the same public model and hashed session or prompt-cache key without overriding hard eligibility or Priority; affinity is bounded and re-homed only after successful completion; pre-output retryable failures may move across targets; visible output is never replayed; anchored WebSocket turns request one bounded full resend from Codex before cross-provider movement; actual provider, credential, effective tier, and bounded failover facts remain content-free; no live deployment or restart occurs.

### Outcome 1: Multi-target model contract
- Work: Replace the globally unique public-model persistence boundary with a migration-safe multi-target representation, preserve existing rows, return all eligible targets for routing, and emit one conservative catalog entry per public model.
- Risks/open questions: Applied migration bytes are immutable; the live service must not run this uncommitted migration.
- Verify: `cargo test -p polyflare-store`; `cargo test -p polyflare-server --test custom_provider`; `cargo test -p polyflare-server --test model_catalog_e2e`

### Outcome 2: Shared capability-aware target selection
- Work: Add one selector over provider/model targets using enabled state, credential availability, health/cooldown, concurrency, operator weight, protocol, and requested Priority capability. Priority-capable targets form the first phase; only after that phase has no eligible route or exhausts retryable pre-output failures may routing use a Standard phase. Remove `service_tier: priority|fast` before contacting a Standard target and record the effective tier as Standard. Keep existing within-provider credential selection as the final lease acquisition step.
- Verify: focused store/server selector regressions covering same-id providers, Priority preference and downgrade, saturation, cooldown, deterministic weighted selection, sanitized Standard requests, and effective-tier logging.

### Outcome 3: Safe pre-output failover and Codex rehydration
- Work: HTTP/SSE full requests may retry another compatible target on transport, 429, and 5xx failures before output. Anchored downstream WebSocket deltas must never be replayed as full requests; when movement is required, use the existing one-shot retryable Codex resend signal, then select the replacement only when the client returns a full anchorless request. Preserve current pump invariants and bounds.
- Risks/open questions: The resend envelope is an internal Codex contract, so retain source-parity and end-to-end regressions.
- Verify: focused HTTP custom-provider failover tests; WebSocket same-model cross-provider resend/reselection test; existing `ws_downstream_relay` suite.

### Outcome 4: Operator controls and observability
- Work: Let the dashboard attach the same public model to multiple providers, show its target set and policy, and record requested/actual tier plus bounded target-attempt/failover facts without content.
- Verify: dashboard tests/build; provider API tests; request-log round-trip and aggregation tests.

### Outcome 5: Soft prompt-cache affinity
- Work: Derive a content-free affinity identity from an existing hashed session key or prompt-cache key, prefer the last successfully completed provider and credential within the selected public model after Priority and all hard capability/health/concurrency gates, and safely fall back when that target is unavailable. Re-home affinity only after a terminal successful completion. Bound entries by TTL and cardinality, keep storage process-local, and apply the same selector semantics to HTTP/SSE and WebSocket without changing native Codex/Anthropic continuity ownership.
- Verify: focused regressions for repeat affinity, unavailable-target fallback, success-only re-homing, Priority precedence, TTL/cardinality bounds, and HTTP/WebSocket parity.

### Outcome 6: Per-model pricing controls
- Work: Let operators create, edit, and clear each target model's standard input, cached-input, and output price per million tokens. Validate finite non-negative values at the API boundary, persist nullable updates, show all configured rates together in the provider dashboard, and keep request cost attribution driven by the selected target's current prices.
- Verify: provider API and store regressions for setting, clearing, and rejecting invalid prices; dashboard tests and production build.

### Outcome 7: Integration safety
- Work: Run formatting, workspace tests, linting, dashboard checks, diff hygiene, migration validation, and final review while leaving the live service untouched.
- Verify: `cargo fmt --all -- --check`; `cargo test --workspace`; `cargo clippy --workspace --all-targets -- -D warnings`; `npm test`; `npm run build`; `git diff --check`
