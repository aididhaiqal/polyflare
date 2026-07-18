# C11 — Prometheus `/metrics` Surface Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use `- [ ]`.

**Goal:** Expose PolyFlare's existing content-free metrics in Prometheus text-exposition format at an admin-gated
`GET /metrics`, so an operator can scrape process-wide counters (failover / starvation / health-tier / lease) plus
per-account gauges (in-flight / error_count / health_tier / cooldown / status). This makes the observability built
across this session's features (B4/B5/B8/C9) actually consumable — the codex-lb/ccflare dashboard-parity ability.

**Architecture:** A pure `render_prometheus_text(...) -> String` that reads the four existing `AtomicU64` metric
structs off `AppState` (via their existing `.total()`/`.acquired()`/`.released()` accessors) and a snapshot of the
account pool (via the existing `account_cache.snapshots()` + `RuntimeStates::overlay` read path) and emits
`# HELP`/`# TYPE`/`name{labels} value` lines by hand (no new dependency). One axum `GET /metrics` handler wires it,
gated by `require_admin` (reusing the existing middleware). Per-account labels use the OPAQUE `account_id` only —
never email/token/session content.

**Authority — the C11 scoping study (this session), file:line cites:**
- Requirement `docs/PORTING-CODEXLB.md:196-222` (C11, LOW/medium): emit codex-lb's metric names+label sets as the
  portable contract — `upstream_requests_total{account_id,status}`, `rate_limit_hits_total{type}`,
  `accounts_total{status}`, the `account_lease_*` family (pairs with C9), + `bridge_*`/`continuity_*` (M3 — NO
  backing code yet, DEFER). "No-op when disabled." codex-lb ref `core/metrics/prometheus.py`,`middleware.py` (uses
  `prometheus_client`, label `account_id` never email; served on a SEPARATE port, `metrics_enabled` default False).
- Existing metrics (all `Arc`-on-AppState, all live-incremented — none dead-zero) in
  `crates/polyflare-server/src/observability.rs`: `FailoverMetrics{total}` (`:136`, `.total()`, AppState
  `failover_metrics` `app.rs:103`, bumped `ingress.rs:1284`); `StarvationMetrics{total}` (`:217`, `starvation_metrics`
  `app.rs:139`, bumped `ingress.rs:616`); `HealthTierMetrics{total}` (`:359`, `health_tier_metrics` `app.rs:111`,
  bumped via `emit_health_tier_signal`); `LeaseMetrics{acquired,released}` + `.current()` (`:493`, `lease_metrics`
  `app.rs:189`, bumped `runtime_state.rs:671` acquire / `:363` release). Signals (FailoverSignal etc.) are log-bus
  events, NOT counters — out of scope. Nothing reads these structs outside their own tests yet — C11 is the first
  consumer.
- No existing `/metrics` route or prometheus dep (grep clean). axum `Router` built in `build_app` (`app.rs:241-366`);
  `api` sub-router (`app.rs:245-270`) is `route_layer`'d with `require_admin` (`auth.rs:15-39`, Bearer
  `POLYFLARE_ADMIN_TOKEN`, unset ⇒ 503); ungated top-level routes exist (`/models` `app.rs:349-354`).
- Per-account gauge source: `RuntimeStates::overlay(&mut [AccountSnapshot], now)` (`runtime_state.rs:375-404`, shared
  `RwLock::read`, cheap — the SAME read ingress does per request) patches live `error_count`/`cooldown_until`/
  `health_tier`/`in_flight` onto `account_cache.snapshots()`. `AccountSnapshot` (`types.rs:285-320`) fields:
  `id: AccountId` (opaque, `.as_str()` `types.rs:256`), `status`, `health_tier`, `error_count`, `cooldown_until`,
  `in_flight`, `plan_type`, `provider`, `pool` — **NO email field on this type** (the natural path is already safe).
- Content-safety-of-labels precedent: `AccountId` is the same opaque id `RequestLog.account_id`/`FailoverSignal`
  already treat as loggable (`observability.rs:9-11`). `read_api.rs` account views DO carry email — do NOT reach
  there for a label. Forbidden-substring test pattern: `observability.rs` (the existing content-free assertions).

## Global Constraints

- **CONTENT-FREE (inviolable).** The rendered `/metrics` body contains ONLY: fixed metric names/HELP/TYPE lines,
  integer counter values, and per-account labels drawn from the OPAQUE `account_id` + fixed enums (`status`,
  `provider`, `pool`, `health_tier` number). NEVER email, token, session key, request/response body, or any content.
  Label values must be the `AccountId` string (opaque store row id), NEVER `email`. A test MUST assert the rendered
  body never contains a seeded email/token/SECRET substring.
- **Admin-gated.** `GET /metrics` is behind `require_admin` (same as `/api/*`) — PolyFlare serves client proxy
  traffic on the same port (unlike codex-lb's segregated metrics port), so metrics (ops-sensitive counts) must not
  be open. No `POLYFLARE_ADMIN_TOKEN` ⇒ 503 (inherits `require_admin`'s behavior) = the "no-op when disabled"
  posture. A Prometheus scraper authenticates with `Authorization: Bearer <admin token>` (document this).
- **No new dependency.** Hand-render the text format (a `String` + `push_str`/`write!`). Do NOT add
  `prometheus`/`metrics-exporter-prometheus` — the counter set is tiny and fixed; hand-rendering keeps the
  content-safety surface auditable (no crate label-encoding to trust). Valid Prometheus 0.0.4 text: `# HELP name h`,
  `# TYPE name counter|gauge`, `name{label="v",...} <int>`, content-type `text/plain; version=0.0.4`.
- **First cut = existing counters + per-account gauges ONLY.** Expose: the 4 process counters + per-account
  in_flight/error_count/health_tier/cooldown-active/status gauges + `accounts_total{status}`. DEFER
  `bridge_*`/`continuity_*` (no backing code — would export constant 0, a lie). Use codex-lb's names where a real
  counter backs them; do not emit placeholder names.
- **Read-only, additive.** No selection/failover/health/lease control-flow touched. The overlay read is the existing
  cheap `RwLock::read`. The 5 wedge/cyber/failover/starvation suites MUST stay green.
- Verify each task: `cargo test --workspace` green + `cargo clippy --workspace --all-targets -- -D warnings` +
  `cargo fmt --all -- --check` clean (run `cargo fmt` before committing — the workspace is now fmt-clean, keep it so).

---

### Task 1: The pure Prometheus text renderer (with content-safe labels)

**Files:** `crates/polyflare-server/src/observability.rs` (or a new `crates/polyflare-server/src/metrics.rs` — decide;
a new small module is cleaner) — a pure `render_prometheus_text(...)` fn; tests in the same file.

**Interfaces — Produces:**
```rust
// A pure renderer. Takes plain values (not AppState) so it's unit-testable without a server.
pub struct MetricsSnapshot {
    pub failover_total: u64,
    pub starvation_total: u64,
    pub health_tier_transitions_total: u64,
    pub lease_acquired_total: u64,
    pub lease_released_total: u64,
    pub accounts: Vec<AccountMetric>,   // one per account, from overlaid snapshots
}
pub struct AccountMetric {
    pub account_id: String,   // OPAQUE id — never email
    pub status: String,       // fixed enum string
    pub provider: String,
    pub pool: Option<String>,
    pub in_flight: u32,
    pub error_count: u32,
    pub health_tier: u8,
    pub cooldown_active: bool, // cooldown_until.is_some_and(> now)
}
pub fn render_prometheus_text(s: &MetricsSnapshot) -> String;
```
Render (Prometheus 0.0.4): process counters as `# TYPE ... counter` (e.g. `polyflare_failover_total`,
`polyflare_starvation_total`, `polyflare_health_tier_transitions_total`, `polyflare_lease_acquired_total`,
`polyflare_lease_released_total`, and a derived `polyflare_lease_inflight` gauge = acquired-released saturating);
per-account gauges `# TYPE ... gauge` with labels `{account_id="...",status="...",provider="...",pool="..."}` —
`polyflare_account_inflight`, `polyflare_account_error_count`, `polyflare_account_health_tier`,
`polyflare_account_cooldown_active` (0/1); plus `polyflare_accounts_total{status="..."}` (count per status).
**Label-escape** any label value per Prometheus rules (`\` → `\\`, `"` → `\"`, newline → `\n`) — account_id is
opaque but escape defensively. Each metric gets ONE `# HELP` + `# TYPE` before its samples.

- [ ] **Step 1:** Failing tests: (a) given a `MetricsSnapshot` with known counters + 2 accounts, the output contains
      the exact `# TYPE polyflare_failover_total counter` + `polyflare_failover_total <n>` lines and the per-account
      gauge lines with the right labels+values; (b) `polyflare_lease_inflight` == acquired-released (saturating at 0);
      (c) `polyflare_accounts_total{status="active"} <count>` aggregates correctly; (d) **content-safety:** a snapshot
      whose account_id is a plain id renders NO email — assert the body does not contain an `@` / a seeded
      "SECRET"/email substring (the renderer only ever sees account_id, proving structurally it can't leak email);
      (e) label escaping: an account_id containing `"` or `\` is escaped. (f) empty account list ⇒ valid output (just
      the process counters, no account lines, no panic).
- [ ] **Step 2:** Run — fail. **Step 3:** Implement the structs + renderer. **Step 4:** Green.
- [ ] **Step 5:** Commit: `feat(server): pure Prometheus text renderer for content-free metrics`

---

### Task 2: Wire `GET /metrics` (admin-gated) + integration e2e

**Files:** `crates/polyflare-server/src/observability.rs`/`metrics.rs` (the handler), `crates/polyflare-server/src/app.rs`
(register `/metrics` gated by `require_admin`), tests + e2e.

**Interfaces — Consumes:** Task 1's `render_prometheus_text` + `MetricsSnapshot`/`AccountMetric`. **Produces:**
`GET /metrics` → `200 text/plain; version=0.0.4` with the rendered body (admin-gated).

- Handler `metrics_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse`: read the 4 metric structs off
  `state` via their accessors; build the per-account `Vec<AccountMetric>` by `state.account_cache.snapshots(&state.store)`
  (mirror how `read_api.rs`'s account/overview handlers obtain snapshots) + `state.runtime.overlay(&mut snaps, now)`
  (the existing cheap read) — map each overlaid `AccountSnapshot` to an `AccountMetric` using ONLY `id`/`status`/
  `provider`/`pool`/`in_flight`/`error_count`/`health_tier`/`cooldown_until` (NEVER reach the store row for email);
  `render_prometheus_text(&snapshot)`; return with content-type `text/plain; version=0.0.4`. On a store-read error ⇒
  a generic 500 (like `read_api.rs`), never the error text.
- `app.rs`: register `GET /metrics` gated by `require_admin` — either add it to the `api` sub-router group (yielding
  the gate for free) at the conventional top-level path via a nested route, or apply a `require_admin` `route_layer`
  to a top-level `/metrics` route. Confirm it does NOT shadow `/{pool}/...` or the proxy routes (matchit prefers
  static). The path SHOULD be top-level `/metrics` (Prometheus convention) but admin-gated; if reusing the `api`
  router forces `/api/metrics`, that's an acceptable fallback — document the chosen path.

- [ ] **Step 1:** Failing e2e through the real `build_app`: (a) a `GET /metrics` WITH the admin Bearer ⇒ 200,
      content-type `text/plain; version=0.0.4`, body contains `polyflare_failover_total` + (after seeding an account)
      a `polyflare_account_inflight{account_id="..."}` line; (b) a KEYLESS `GET /metrics` ⇒ rejected (401/403, same
      as `/api/*` — inherits the gate; assert status == a keyless `/api/accounts`); (c) **content-safety e2e:** seed
      an account with a known email + SECRET token, hit `/metrics` with the admin key, assert the body does NOT
      contain the email or the token (proves the account→label mapping never pulls email); (d) counters reflect live
      state (e.g. after acquiring a lease guard, `polyflare_lease_acquired_total` increments / `polyflare_account_inflight`
      shows it).
- [ ] **Step 2:** Run — fail. **Step 3:** Implement the handler + route + gating. **Step 4:** Green; all suites
      green; `/metrics` doesn't shadow other routes.
- [ ] **Step 5:** Commit: `feat(server): admin-gated GET /metrics Prometheus endpoint`

---

## Suggested order

1 (pure renderer + content-safe labels) → 2 (wire + gate + e2e). After Task 2, an operator can scrape `/metrics`
(with the admin Bearer) for failover/starvation/health-tier/lease counters + per-account in-flight/error/health/
cooldown/status gauges — all content-free. Mark C11 DONE in `PORTING-CODEXLB.md` (first cut; `bridge_*`/`continuity_*`
deferred to M3, `upstream_requests_total`/`rate_limit_hits_total` addable later off the request-log if wanted).
Follow-ups (not this plan): `upstream_requests_total{account_id,status}` + `rate_limit_hits_total{type}` derived from
the request-log/runtime; the `bridge_*`/`continuity_*` family once M3 signals exist; a separate metrics port +
`POLYFLARE_METRICS_ENABLED` flag if unauthenticated scraping is ever wanted.
