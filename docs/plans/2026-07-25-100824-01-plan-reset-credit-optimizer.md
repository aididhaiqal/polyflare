# Reset-credit optimizer and Codex API

**Goal:** Make earned Codex rate-limit reset credits safely usable through PolyFlare from both a polished fleet dashboard and stock codex-rs.

**Why planning is required:** Redemption spends a scarce upstream credit and changes live account capacity, so stale selection, retries, double-clicks, or concurrent callers must not consume the wrong credit or consume twice.

**Acceptance:** PolyFlare discovers credits only for eligible Codex accounts, exposes account-scoped and aggregate recommendations with freshness and explainable capacity value, supports the native Codex reset-credit read/consume contract plus the operator API, serializes and durably pins every consume before the upstream call, refreshes authoritative usage after success, executes fleet selections sequentially with partial results, and never consumes from a stale plan or retries an ambiguous non-idempotent request under a new key.

### Outcome 1: Durable reset-credit state and optimizer
- Work: Add reset-credit snapshots, a per-account leased consume claim, and a 24-hour idempotency ledger to the SQLite store. Add a pure capacity-aware optimizer that excludes ineligible or stale accounts, selects the earliest-expiring credit within an account, discounts recovery near natural weekly reset, and explains expiry urgency and wait recommendations.
- Risks/open questions: Upstream may change the natural reset clock after redemption; optimizer output is an estimate and post-consume usage is always authoritative.
- Verify: `cargo test -p polyflare-store reset_credit && cargo test -p polyflare-core reset_credit`

### Outcome 2: Upstream client, polling, and safe redemption
- Work: Add typed Codex reset-credit fetch/consume calls, periodic jittered refresh beside usage refresh, token/account identity pairing, fresh pre-consume validation, durable per-account serialization, exact request-id replay, cache/snapshot invalidation, and immediate usage refresh after success. Discovery failure must not change routing health, and consume must not be automatically retried.
- Risks/open questions: A lost upstream response remains ambiguous; reusing the same pinned request ID and credit is the only allowed recovery attempt.
- Verify: `cargo test -p polyflare-codex reset_credit && cargo test -p polyflare-server reset_credit`

### Outcome 3: Codex-native and operator APIs
- Work: Intercept the stock codex-rs reset-credit detail and consume paths for root and named-pool scopes, synthesize aggregated details from eligible member accounts, map opaque fleet credit IDs back to their owning accounts, and retain caller idempotency keys. Add admin dashboard endpoints for fleet plans, account redemption, and sequential selected-plan execution with per-account results.
- Risks/open questions: Native Codex consumes one selected opaque credit rather than an optimizer batch; pool scope must never expose or redeem an account outside that pool.
- Verify: `cargo test -p polyflare-server --test reset_credit_api`

### Outcome 4: Dashboard experience
- Work: Add reset-credit summary metrics, account badges/actions, and an optimizer panel showing account identity, pools, credit count/expiry, weekly usage/reset, estimated recovery, recommendation, and reason. Support redeem-best and confirmed sequential multi-select actions, refresh visible state after each result, and clearly separate recommended actions from wait/low-benefit accounts.
- Risks/open questions: “Redeem all” remains an explicit reviewed selection, not a blind parallel action.
- Verify: `cd crates/polyflare-server/dashboard && npm test -- --run && npm run build`

### Outcome 5: Completion gate
- Work: Document the API and safety model, validate migrations and generated dashboard assets, and review the final diff without disturbing the existing usage-meter work.
- Verify: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && git diff --check`

### Outcome 6: Recovery and fleet-freshness hardening
- Work: Refresh account credit evidence with bounded concurrency and a fleet-size-aware freshness budget; preserve terminal redemption results independently of account deletion; make the dashboard retain the exact fleet request ID and account sequence across navigation or reload; and keep the plan's recommended count aligned with every actionable recommendation.
- Risks/open questions: The existing `0026` migration may already be applied to a live local database, so schema hardening must use a forward-only migration. Browser recovery storage contains only opaque operation and internal account identifiers and must fail closed when malformed.
- Verify: `cargo test -p polyflare-store --test reset_credit_repo && cargo test -p polyflare-server --test reset_credit_api && cd crates/polyflare-server/dashboard && npm test -- --run && npm run build`
