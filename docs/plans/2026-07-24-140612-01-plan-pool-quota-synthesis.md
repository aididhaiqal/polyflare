# Codex-compatible aggregate pool quota

**Status:** Complete — implemented and verified 2026-07-24.

**Goal:** Make downstream Codex report the capacity-weighted quota of the routable PolyFlare pool while preserving the selected account's real quota as a separate meter and leaving per-turn token usage untouched.

**Why planning is required:** This changes the public HTTP and WebSocket rate-limit contract seen by codex-rs, combines durable usage evidence across accounts, and must remain consistent with routing pool and security-capability boundaries.

**Acceptance:** Successful native Codex responses expose an aggregate `codex` meter scoped to the request's provider, named pool, and security requirement; the selected account remains visible as `polyflare_selected`; five-hour or weekly windows are omitted unless every included account has fresh evidence for that window; terminal/operator-held accounts do not dilute the aggregate while recoverable exhausted accounts remain represented; mismatched member resets do not produce a fabricated pool reset; malformed or incomplete evidence falls back to the untouched upstream meter; HTTP/SSE and downstream WebSocket produce equivalent meter IDs and percentages accepted by the current codex-rs parsers; response-completion token usage and custom providers are unchanged.

### Outcome 1: Fresh quota evidence in routing snapshots

- Work: Carry resolved five-hour and weekly usage evidence, including freshness, duration, and reset metadata, from the existing account cache into `AccountSnapshot` without changing the selector's established percentage fields. Continue resolving windows by duration so a weekly window in the upstream primary slot is not mislabeled as five-hour.
- Verify: `cargo test -p polyflare-server usage_windows snapshot`

### Outcome 2: Pure scoped aggregate

- Work: Add a pure server-side synthesizer that filters Codex snapshots by named pool and security capability, excludes `paused`, `reauth_required`, and `deactivated` accounts, and weights each included account by its capacity override or plan capacity. For each window, calculate `100 × (1 - Σ remaining capacity / Σ full capacity)`, clamp malformed percentages, require fresh evidence from every included account, use the canonical five-hour or weekly duration, and emit a reset only when all member resets agree.
- Risks/open questions: Runtime transport cooldown and health are intentionally not quota—they must not make the displayed entitlement fluctuate. A recoverable `rate_limited` or `quota_exceeded` account remains in the denominator with its observed exhaustion. An absent five-hour window on any included account means the pool is not uniformly governed by that limit, so the aggregate five-hour window is omitted.
- Verify: `cargo test -p polyflare-server pool_quota`

### Outcome 3: HTTP/SSE downstream contract

- Work: On successful native Codex responses, preserve the real upstream account windows under the `polyflare_selected` header family and publish the synthesized pool windows under the canonical `codex` family with friendly HTTP names. Do not attach selected-account credits to the aggregate meter. If synthesis is unavailable or incomplete, leave the upstream headers byte-for-byte unchanged.
- Risks/open questions: Current codex-rs parses credits only from the canonical Codex header family rather than per arbitrary meter, so selected-account credits cannot be represented accurately as a secondary HTTP meter and must not be relabeled as pool credits.
- Verify: `cargo test -p polyflare-server --test e2e_passthrough`

### Outcome 4: WebSocket downstream contract

- Work: Intercept only `codex.rate_limits` upstream text events. When synthesis succeeds, relabel the real event as `polyflare_selected`, inject an equivalent aggregate event as `codex`, and preserve all other WebSocket frames verbatim. Recompute through the existing account cache when a rate-limit event arrives so long-lived sockets do not freeze startup quota.
- Risks/open questions: The current codex-rs WebSocket parser ignores event display names, so `/status` can show only the IDs `codex` and `polyflare_selected` on this transport; values and warning/status-line behavior still use the canonical aggregate `codex` meter.
- Verify: `cargo test -p polyflare-server --test ws_downstream_relay`

### Outcome 5: Codex-rs compatibility and completion gate

- Work: Feed representative transformed headers and WebSocket events into the checked-out current codex-rs rate-limit parser, confirm separate snapshots and aggregate values, then verify the complete PolyFlare workspace and review the final diff against the transport, scope, freshness, and no-token-synthesis constraints.
- Verify: `cargo test -p codex-api rate_limits && cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && git diff --check`

### Completion evidence

- Pure aggregation tests cover mixed plan weights, multi-pool/provider/security scoping, missing five-hour limits, stale or malformed evidence, terminal versus recoverable accounts, clamping, and mismatched resets.
- Live HTTP and WebSocket servers prove the selected account and aggregate pool arrive as separate meters while the pool omits a non-universal five-hour window.
- Exact emitted HTTP and WebSocket fixtures parse into `codex` and `polyflare_selected` snapshots through the checked-out current codex-rs parser; a secondary-only selected HTTP meter carries the zero primary discovery sentinel that parser requires.
- Current codex-rs rate-limit parser tests, PolyFlare formatting, warnings-denied clippy, the complete Rust workspace test suite, and diff checks pass.
