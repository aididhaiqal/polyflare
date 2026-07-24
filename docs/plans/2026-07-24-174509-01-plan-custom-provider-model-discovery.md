# Custom-provider model discovery

**Goal:** Let operators discover an OpenAI-compatible provider's upstream models, import them into PolyFlare's merged catalog, and accurately configure per-model reasoning effort.

**Why planning is required:** Discovery performs authenticated outbound requests through the custom-provider SSRF boundary, persists new public routing targets, and changes the model catalog consumed by Codex and OpenAI-compatible clients.

**Acceptance:** An authenticated admin can trigger discovery for a configured provider; PolyFlare requests only that provider's validated `{base_url}/models` endpoint using an eligible encrypted credential; OpenAI `data[].id` and rich Codex `models[].slug` responses are bounded and normalized; only missing models are imported while every existing manual model remains unchanged; rich capability and reasoning metadata is retained when available, sparse OpenAI rows use conservative metadata that can be edited afterward; the dashboard exposes discovery plus model reasoning-level and summary configuration; merged `/models` and `/v1/models` responses include imported visible models; errors never expose credentials or upstream bodies.

### Outcome 1: Bounded secure discovery

- Work: Add a custom-provider discovery primitive that reuses endpoint validation, DNS pinning, timeouts, and credential selection; derive `/models` from the provider base URL; reject non-success, oversized, malformed, and empty responses with content-safe errors; parse both OpenAI and rich Codex catalog shapes.
- Risks/open questions: Provider credentials can have different model entitlements. This operator-triggered sync uses one currently eligible credential and reports that bounded snapshot; it does not claim fleet-wide credential intersection.
- Verify: `cargo test -p polyflare-server custom_provider::tests::model_discovery`

### Outcome 2: Import and manual override contract

- Work: Add an authenticated provider sync endpoint that inserts only globally unclaimed model IDs, preserves every existing row, imports safe rich metadata when present, and returns imported/skipped counts without upstream response bodies. Extend model policy updates to edit capabilities, reasoning levels, context, and display metadata with the same validation as creation.
- Verify: `cargo test -p polyflare-server --test custom_provider`

### Outcome 3: Dashboard controls

- Work: Add a Discover/Sync action, surface import results, display configured effort levels, and allow model creation/editing to set reasoning efforts and reasoning-summary support.
- Verify: `npm test && npm run build`

### Outcome 4: Compatibility and security gate

- Work: Verify catalog merging, route resolution, migrations/store behavior, content-safe failure handling, formatting, warnings, all workspace tests, and the final diff.
- Verify: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && git diff --check`
