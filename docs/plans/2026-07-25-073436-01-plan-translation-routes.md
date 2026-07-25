# Bidirectional provider-aware translation routes

**Goal:** Route between Anthropic Messages and OpenAI Responses in either direction, with built-in account fleets and custom providers selected through one persisted, operator-managed protocol-capability model.

**Why planning is required:** This adds forward-only database migrations, changes both public inference routes, and expands the custom-provider execution contract.

**Acceptance:** Existing Opus, Sonnet, and Haiku behavior survives migration as editable seeded routes. Routes may originate from either supported protocol and target a built-in provider or a stable custom-provider id. A target's declared wire protocol determines whether translation is valid; same-protocol routes are rejected because normal model routing owns them. Enabled routes match case-insensitively by exact, prefix, or contains semantics in priority order. An unmatched route retains native routing, while an invalid, missing, or disabled matched target fails clearly without silently selecting another provider. Both request and streaming-response directions preserve text, tools, images, token caps, usage, and terminal status semantics covered by retained golden tests. Pool-scoped built-in targets stay within the requested pool. The authenticated API and responsive dashboard expose source protocol, target kind/provider, model, priority, enablement, and server-backed match validation. Request telemetry records the actual target provider and translated status. No commit, push, restart, or external configuration change occurs.

### Outcome 1: Protocol-capable persistence and management contract

- Work: Extend the applied provider and translation schemas with a new forward-only migration. Custom providers declare `responses` or `anthropic_messages`; routes store source protocol, target kind, stable built-in/custom provider identity, target model, and optional Responses reasoning effort. Preserve seeded defaults and deterministic matching.
- Risks/open questions: Existing databases may already have migration 0023 applied, so the migration must preserve rows without modifying prior migration files. Deleted or disabled custom-provider targets remain observable and non-routable.
- Verify: `cargo test -p polyflare-store --test store_roundtrip`

### Outcome 2: Independent translators for both directions

- Work: Keep `AnthropicToResponses` for Messages-client to Responses-upstream turns and add a separate per-turn `ResponsesToAnthropic` adapter for Responses-client to Messages-upstream turns. Map request instructions/history/text/images/tools/results/token caps and response lifecycle/text/tool deltas/usage/completed/incomplete/error events without sharing state across turns.
- Risks/open questions: The two streaming protocols have different lifecycle and usage timing. Unsupported content must be rejected or intentionally omitted with tests, never mislabeled as another content type.
- Verify: `cargo test -p polyflare-anthropic`

### Outcome 3: Built-in and custom-provider execution

- Work: Resolve translation routes before native routing on both ingress surfaces. Dispatch built-in targets through their account pools and custom targets through credential routing using the target's declared endpoint/auth protocol. Translate the response back to the client protocol and preserve streaming versus buffered response behavior.
- Risks/open questions: A matched route whose target is missing, disabled, incompatible, or lacks an enabled model/credential must fail closed. Translation must not enter Codex continuity ownership or leak protocol-specific headers to the wrong backend.
- Verify: `cargo test -p polyflare-server --test messages_ingress --test custom_provider --test translation_api`

### Outcome 4: Dashboard translation controls

- Work: Update provider onboarding to select a native wire protocol and update the Translations page to configure and explain both directions, built-in/custom targets, target availability, and recent translated traffic.
- Verify: `cd crates/polyflare-server/dashboard && npm test && npm run build`

### Outcome 5: Compatibility gate

- Work: Rebuild embedded dashboard assets, verify formatting and strict lints, run the workspace suite and release build, inspect the final diff against acceptance, and preserve all pre-existing task-owned changes. Existing same-protocol custom-provider and native account paths must remain byte-fast and behaviorally unchanged.
- Verify: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && cargo build --release -p polyflare-server && git diff --check`
