# Custom-provider model profiles

**Status:** Completed on 2026-07-24.

**Goal:** Let operators expose multiple PolyFlare model profiles over one custom-provider upstream model, with safe per-profile instruction handling and bounded request overrides.

**Why planning is required:** This changes the persisted provider-model and request-log schemas, rewrites privileged request instructions, and expands the model-management API and dashboard.

**Acceptance:** An authenticated operator can create multiple uniquely named public profiles that map to the same upstream model; each profile can leave instructions unchanged, append a bounded overlay, or explicitly replace instructions, and can carry a bounded allowlisted request override for reasoning effort and maximum output tokens. Append preserves the client instructions byte-for-byte before a deterministic separator; replace is visibly marked advanced; malformed instruction fields or override values fail closed before any upstream request. Existing models migrate to no instruction transformation and no overrides. Every request continues to record its public and upstream model and additionally records a content-free revision hash when a profile changes the request. The dashboard can create a profile from an existing model, edit its behavior, distinguish profiled models, and display the revision on request details. No instruction text enters request logs, live logs, errors, or metrics. Automated tests cover migration defaults, duplicate-upstream profiles, none/append/replace behavior, request overrides, validation, content-safe telemetry, and the dashboard controls.

### Outcome 1: Persisted profile contract

- Work: Add profile instruction mode/text and allowlisted request overrides to provider models, plus a nullable content-free profile revision on request logs. Update store structs, queries, migration coverage, and existing-model defaults without changing public-model uniqueness or upstream-model reuse.
- Risks/open questions: Instruction overlays may contain proprietary operator text; only authenticated provider-management responses may return them, while all operational telemetry carries only a one-way bounded revision.
- Verify: `cargo test -p polyflare-store`

### Outcome 2: Fail-closed request transformation

- Work: Validate profile inputs in the provider API; compute a stable SHA-256 revision from normalized profile configuration; apply none, append, or replace after public-to-upstream model mapping; apply only the supported reasoning-effort and maximum-output overrides; reject malformed client instruction/reasoning shapes before contacting the provider. Preserve the ordinary no-profile fast path.
- Risks/open questions: Replace can remove Codex tool and operating instructions, so it remains an explicit advanced mode rather than a default. Prompt behavior is not a security boundary.
- Verify: `cargo test -p polyflare-server --test custom_provider`

### Outcome 3: Dashboard profile workflow and observability

- Work: Add an Add profile action that clones upstream metadata into a new public alias; expose instruction mode, overlay, and bounded overrides in create/edit forms; label profiles and warn on replace; return and display the content-free profile revision in request details.
- Verify: `cd crates/polyflare-server/dashboard && npm test && npm run build && cd ../../.. && cargo test -p polyflare-server --test dashboard`

### Outcome 4: Compatibility and security gate

- Work: Verify existing imported/discovered models remain no-op profiles, catalog resolution still supports duplicate upstream targets, request logging never contains instruction text, formatting/lints pass, the workspace remains green, and the release binary embeds the rebuilt dashboard.
- Verify: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && cargo build --release -p polyflare-server && git diff --check`
