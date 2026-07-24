# Custom model providers and provider-aware analytics

**Status:** Completed and independently reviewed on 2026-07-24. No Critical or Important findings remain.

**Goal:** Let one main PolyFlare endpoint advertise and route operator-configured models such as `fugu-ultra` through reusable custom-provider credential pools, while making every request, usage, cost, health, and dashboard aggregate identify its real upstream provider and target.

**Why planning is required:** This changes the persistent schema, encrypted credential boundary, public model catalog, HTTP and WebSocket routing, retry and continuity behavior, observability dimensions, and dashboard contracts in a heavily developed shared checkout.

**Acceptance:** Existing Codex and Anthropic routes remain compatible; an administrator can configure a generic OpenAI Responses-compatible provider with multiple encrypted credentials and typed model capabilities; its models appear in the main root catalog and route by requested model over HTTP/SSE and downstream WebSocket; stateless providers never receive `previous_response_id`; pre-stream failures can safely reselect another eligible credential while post-stream failures are not replayed; request rows, live logs, Prometheus metrics, overview, requests, sessions, and reports expose the actual provider and account-or-credential target with correct standard and orchestration token/cost totals; migrations preserve existing rows; retained tests cover the complete paths; workspace formatting, Clippy, tests, dashboard tests/build, and diff checks pass.

### Outcome 1: Provider, credential, model, and analytics persistence

- Work: Add normalized custom-provider, encrypted provider-credential, and provider-model tables under `crates/polyflare-store`; keep the existing built-in account schema intact. Store core model capabilities and pricing as typed columns, with a validated full Codex model-info document only for catalog fields that are genuinely extensible. Extend request history with a bounded target kind, provider credential id, upstream model/transport, and Sakana-style orchestration token details. Treat the existing `provider` column as the actual upstream provider slug (`codex`, `anthropic`, or configured slug), preserving all legacy values and existing provider grouping.
- Risks/open questions: Credentials must remain encrypted and redacted; migrations must be additive and compatible with the current uncommitted migration chain; provider/model slugs and URL/metadata inputs need strict validation; remote base URLs must be checked against the configured private-host policy both at write time and before egress.
- Verify: `cargo test -p polyflare-store`

### Outcome 2: Generic Responses provider runtime

- Work: Add a provider registry/cache and generic HTTP/SSE Responses executor that resolves a requested model to its provider, selects and leases a healthy credential using capacity/concurrency/health inputs, rewrites only declared provider/model fields, preserves safe upstream errors, observes terminal SSE usage including orchestration fields, computes model pricing, and settles leases on every completion, cancellation, and failure path. Retry/reselect only before downstream response bytes have committed unless a provider explicitly declares a stronger idempotency contract.
- Risks/open questions: Never mix subscription-account OAuth continuity with API-key credential health. Avoid logging secrets, bodies, error prose, or arbitrary metadata. Enforce bounded error and SSE buffers, redirect policy, timeouts, idle keepalives, and request-size limits.
- Verify: `cargo test -p polyflare-server custom_provider`

### Outcome 3: Main-catalog HTTP and WebSocket routing

- Work: Merge enabled custom-provider models into the same root Codex catalog as Terra/Luna, with deterministic collision rejection and complete model-info capability projection. Resolve `/responses` by requested model before built-in account selection. Extend downstream WebSocket routing to invoke the same provider runtime while speaking the existing client-facing Responses event protocol. For stateless providers, omit/empty the relayed terminal response id so Codex sends full history on the next round, and strip any inbound `previous_response_id` before upstream egress; no conversation transcript is persisted or reconstructed by PolyFlare.
- Risks/open questions: A model route must remain fixed for a logical turn. WS framing, cancellation, terminal outcomes, and HTTP fallback behavior must match current codex-rs expectations. Pool catalogs must not accidentally advertise global custom models unless their eventual scope contract explicitly says so; the root catalog is the required initial surface.
- Verify: `cargo test -p polyflare-server --test model_catalog_e2e --test ws_downstream_relay`

### Outcome 4: Management and provider-aware operator surfaces

- Work: Add authenticated CRUD/test endpoints and dashboard controls for providers, credentials, and models without ever returning stored secrets. Update live logs, request details/list filters, overview, sessions, reports, and provider/cost/token visualizations so custom-provider traffic is included and distinguishable by provider and account-or-credential target. Preserve existing views for legacy rows and built-in accounts.
- Risks/open questions: Secret replacement must be write-only and explicit; deleting or disabling a provider must not orphan historical request rows; UI must clearly distinguish provider health from subscription quota windows that do not apply.
- Verify: `npm test && npm run build`

### Outcome 5: Integrated compatibility and completion gate

- Work: Run existing protocol, failover, continuity, logging, analytics, migration, dashboard, and content-safety suites against the combined implementation. Compare the complete diff with this plan and the current Codex protocol compatibility contract, then perform an independent material code review and repair every supported blocking finding.
- Verify: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && git diff --check`

### Outcome 6: Catalog-surface policy and translation isolation

- Work: Separate route enablement from model discovery. Add forward-only per-model exposure policy for the rich Codex picker and generic OpenAI model list, preserve existing configured models as visible during migration, and expose both controls through the provider API and dashboard. Claude-to-Codex translation aliases remain routable through `/v1/messages` but are not advertised as Codex or generic OpenAI models unless a future explicit surface supports them.
- Risks/open questions: Catalog visibility must never disable an explicitly addressed route; custom-model ETags must reflect only the models visible to that client surface; alias slugs remain reserved so a configured model cannot ambiguously shadow translation routing.
- Verify: `cargo test -p polyflare-store && cargo test -p polyflare-server --test model_catalog_e2e --test messages_ingress --test custom_provider && npm test && npm run build`
