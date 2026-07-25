# Named pool WHAM usage

**Goal:** Advertise PolyFlare's capacity-weighted aggregate as a named WHAM usage bucket using the same `additional_rate_limits` contract Codex uses for model-specific limits.

**Why planning is required:** This changes the public `/backend-api/wham/usage` contract consumed by stock codex-rs and must preserve the canonical limit used by existing clients, warnings, and status integrations.

**Acceptance:** Root and named-pool usage responses retain the current canonical `rate_limit` unchanged and add exactly one named aggregate entry with a stable scope-specific `metered_feature`, the existing conservative windows and resets, and no credits or fabricated capacity; current authentication, fail-closed behavior, and passthrough isolation remain unchanged.

### Outcome 1: Named aggregate payload
- Work: Extend the WHAM encoder to add `additional_rate_limits` with `PolyFlare overall pool` / `polyflare_pool` for the root scope and a pool-qualified label and identifier for named scopes. Reuse the canonical encoded rate-limit object so both views remain mathematically identical.
- Risks/open questions: Modern clients may render both the canonical compatibility windows and their explicitly named representation; removing the canonical copy would break older or single-limit consumers and Codex's canonical warning path.
- Verify: `cargo test -p polyflare-server --test chatgpt_backend_gateway wham_usage`

### Outcome 2: Codex compatibility and completion gate
- Work: Retain exact root and pool-scoped integration coverage, confirm codex-rs maps the additional entry into a separate named snapshot, and review the final public-contract diff for accidental changes to authentication, reset credits, or aggregation.
- Verify: `cargo test -p codex-backend-client usage_payload_maps_primary_and_additional_rate_limits --lib && cargo fmt --all -- --check && cargo clippy -p polyflare-server --all-targets -- -D warnings && cargo test -p polyflare-server --test chatgpt_backend_gateway && git diff --check`

## Completion evidence

**Status:** Completed on 2026-07-25.

- The root and named-pool gateway regressions failed before implementation because
  `additional_rate_limits` was absent, then passed with the exact labels, identifiers, and
  canonical-window equality asserted.
- The complete ChatGPT backend gateway integration suite passed: 7 tests.
- The checked-out codex-rs `usage_payload_maps_primary_and_additional_rate_limits` parser test
  passed.
- PolyFlare formatting, strict server Clippy, and `git diff --check` passed.
- Final self-review found no unresolved Critical or Important issue. The compatibility duplicate
  is deliberate: the canonical `codex` snapshot continues to drive older consumers and warnings,
  while the additional snapshot supplies the operator-facing pool name.
