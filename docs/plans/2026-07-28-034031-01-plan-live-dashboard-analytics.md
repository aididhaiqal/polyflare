# Live dashboard analytics

**Goal:** Make the Overview surface show trustworthy model traffic and observed API-token activity for the last 60 minutes, with an explainable sudden-traffic signal and cleaner visual hierarchy.
**Why planning is required:** This adds an API query contract and changes shared report filtering semantics on a live production gateway.
**Acceptance:** The dashboard uses persisted content-free request metrics only; model traffic excludes backend-control operations unless Backend is explicitly selected; the current partial minute is not used for anomaly classification; desktop and mobile keep the live evidence readable; focused backend/frontend tests, the dashboard build, workspace tests, formatting, and Clippy pass. No service restart, deployment, live-database access, or migration is performed.

### Outcome 1: Minute-resolution, correctly scoped analytics
- Work: Extend `GET /api/reports` with a `1h` range using 60-second buckets and an optional validated `scope=all|model|backend`. Keep `all` as the API compatibility default, while Overview and Reports explicitly choose model or backend scope. Reuse the existing request-log aggregation and logical backend predicate; do not add schema or storage.
- Risks/open questions: Historical backend rows can carry a model-provider value, so scope must use the normalized backend-path predicate rather than provider equality alone.
- Verify: `cargo test -p polyflare-server --test read_api reports_endpoint`

### Outcome 2: Explainable live-traffic signal
- Work: Add a pure dashboard helper that ignores the in-progress minute, compares the latest five completed minutes with the median of the preceding six five-minute windows, and returns `steady`, `surge`, `new_burst`, or `insufficient_history` with raw counts and ratio. A surge requires at least a 2x rate and an absolute lift of three requests; a near-zero baseline requires at least six recent requests and is labeled a new burst rather than an infinite increase.
- Verify: `npm test -- --test-name-pattern="live activity"`

### Outcome 3: Cleaner Overview hierarchy
- Work: Replace the selected-range request-volume card with one full-width live panel containing visible 60-minute request, observed-token, estimated-cost, and error metrics plus separate request/minute and token/minute charts. Put the selected-range summary below it, show the newest minute as still settling, keep essential values available without hover, and rebalance capacity and weekly pace into a 6/6 row.
- Risks/open questions: Recent request rows can be backfilled with final token usage after insertion, so UI copy must describe observed tokens and the settling current minute rather than presenting a billing ledger.
- Verify: `npm test && npm run build`

### Outcome 4: Completion evidence
- Work: Review the whole branch diff against the request, content-safety rules, report callers, mobile layout, and backend-filter behavior. Preserve the main checkout's unrelated `Cargo.toml` edit.
- Verify: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && git diff --check`
