# Custom-provider Priority tier

**Goal:** Let operators mark a custom Responses model as Priority-capable so Codex exposes its Fast-mode selector and PolyFlare forwards and records the selected service tier.
**Why planning is required:** This changes the rich model-catalog contract consumed by Codex and the custom-provider onboarding surface.
**Acceptance:** Priority capability is optional and off by default; enabling it emits canonical `service_tiers` and `additional_speed_tiers` metadata without forcing Priority; a client-selected `service_tier: "priority"` reaches a healthy Priority-capable custom Responses upstream and the request log; if all Priority-capable routes are unavailable or fail retryably before output, PolyFlare may remove the Priority marker and use an eligible Standard target while logging the effective Standard tier; the dashboard can create and edit the capability; Anthropic providers are unaffected; no migration or live deployment occurs.

### Outcome 1: Model capability contract
- Work: Add a validated Priority capability to provider model create, update, and view APIs, persisting it through the existing safe model metadata field without a schema migration.
- Verify: `cargo test -p polyflare-server --test custom_provider`

### Outcome 2: Codex discovery and request behavior
- Work: Emit the canonical Codex Fast/Priority metadata for enabled models while leaving the default tier unset; retain transparent forwarding and effective-tier logging for client-selected Priority requests.
- Verify: `cargo test -p polyflare-server --test model_catalog_e2e`

### Outcome 3: Dashboard onboarding
- Work: Add an optional Priority/Fast capability control to custom model create and edit forms and carry it through typed API requests.
- Verify: `npm test`; `npm run build`

### Outcome 4: Integration safety
- Work: Run formatting, workspace tests, linting, dashboard checks, diff hygiene, and an independent bounded review.
- Verify: `cargo fmt --all -- --check`; `cargo test --workspace`; `cargo clippy --workspace --all-targets -- -D warnings`; `git diff --check`
