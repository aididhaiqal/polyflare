# Custom-provider model discovery

**Goal:** Let operators preview an OpenAI-compatible provider's upstream models, selectively import only the models they intend to expose, and accurately configure OpenRouter-scale model metadata and reasoning effort.

**Why planning is required:** Discovery performs authenticated outbound requests through the custom-provider SSRF boundary, persists new public routing targets, and changes the model catalog consumed by Codex and OpenAI-compatible clients.

**Acceptance:** An authenticated admin can preview discovery for a configured provider without changing the database, select an explicit bounded set of discovered upstream model IDs, and import only that set; an empty or omitted selection never imports the whole catalog. PolyFlare requests only that provider's validated `{base_url}/models` endpoint using an eligible encrypted credential; OpenAI `data[].id` and rich Codex `models[].slug` responses are bounded and normalized; OpenRouter identifiers containing provider paths and routing suffixes remain exact upstream IDs while public IDs are collision-safe and editable; existing manual models remain unchanged; OpenRouter name, context, output limit, tools, vision, reasoning-effort, and token-price metadata are retained conservatively; the dashboard provides search, capability filters, selection, and configured/conflict states before import; merged `/models` and `/v1/models` responses include only imported visible models; errors never expose credentials or upstream bodies. Discovery or import never deletes, disables, or silently updates an existing model.

**Status:** Completed and verified on 2026-07-24.

### Outcome 1: Bounded secure preview

- Work: Keep the custom-provider discovery primitive behind endpoint validation, DNS pinning, timeouts, and credential selection; derive `/models` from the provider base URL; reject non-success, oversized, malformed, and empty responses with content-safe errors; parse both OpenAI and rich Codex catalog shapes. Add an authenticated preview endpoint that returns bounded normalized candidates and their configured/reserved state without persistence.
- Risks/open questions: Provider credentials can have different model entitlements. This operator-triggered sync uses one currently eligible credential and reports that bounded snapshot; it does not claim fleet-wide credential intersection.
- Verify: `cargo test -p polyflare-server custom_provider::tests::model_discovery`

### Outcome 2: Explicit selective import and manual override contract

- Work: Replace import-all sync semantics with an authenticated request body containing explicit discovered upstream IDs. Re-discover server-side, intersect with that bounded selection, insert only globally unclaimed selected models, preserve every existing row, and return selected/imported/skipped counts without upstream bodies. Split public-model and upstream-model validation so OpenRouter `/`, `:`, and `~` routing identifiers remain exact without admitting whitespace, query fragments, or control characters. Extend normalization for OpenRouter's nested capabilities, reasoning effort, limits, and per-token prices. Keep model policy updates editable with the same validation as creation.
- Risks/open questions: Public IDs are global routing keys. Default discovered OpenRouter public IDs must be namespaced by the configured provider slug while the exact OpenRouter ID remains the upstream mapping; conflicts require an operator alias rather than destructive normalization.
- Verify: `cargo test -p polyflare-server --test custom_provider`

### Outcome 3: Dashboard selection controls

- Work: Replace the one-click Sync action with a discovery dialog that writes nothing until the operator selects rows and confirms import. Provide search, tools/vision/reasoning/free filters, select-visible behavior, configured/conflict states, and import progress/results. Continue to display and edit configured effort levels and reasoning-summary policy.
- Verify: `npm test && npm run build`

### Outcome 4: Compatibility and security gate

- Work: Verify catalog merging, route resolution, migrations/store behavior, content-safe failure handling, formatting, warnings, all workspace tests, and the final diff.
- Verify: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && git diff --check`
