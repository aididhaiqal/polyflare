# C11b — `upstream_requests_total` + `rate_limit_hits_total` Counters Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use `- [ ]`.

**Goal:** Complete codex-lb's Prometheus metric contract by adding two labeled monotonic counters to the existing
`/metrics` endpoint: `upstream_requests_total{account_id,status}` (one per completed proxied request) and
`rate_limit_hits_total{type}` (one per 429 writeback). In-process AtomicU64/`RwLock<HashMap>` counters incremented at
the request lifecycle — NOT derived from `request_log` (which C12 now prunes; a pruned-derived counter would
decrement, breaking Prometheus counter monotonicity).

**Architecture:** Two new counter-map structs in `observability.rs` (`UpstreamRequestMetrics`,
`RateLimitMetrics`), `Arc`-held on `AppState` (mirroring `FailoverMetrics`/`LeaseMetrics`). `upstream_requests` is
bumped at the 3 request-completion wrapper sites (from the already-built content-free `log`); `rate_limit_hits` is
bumped inside `RuntimeStates::record_rate_limit` (the single 429 chokepoint). `MetricsSnapshot`/`render_prometheus_text`
(metrics.rs, from C11) gains two counter families; the `/metrics` handler reads the two maps. All labels content-free.

**Authority — the C11b scoping study (this session), file:line cites:**
- Existing C11 metrics surface: `crates/polyflare-server/src/metrics.rs` (`render_prometheus_text(&MetricsSnapshot)`,
  `MetricsSnapshot` `:29-36`, `AccountMetric`, `escape_label_value` `:57-68`, `write_account_gauge`/`write_accounts_total`
  `:167-217`, `metrics_handler` `:236-282` reads `state.failover_metrics.total()` etc. `:267-271`). The 4 existing
  metric structs (`FailoverMetrics` `observability.rs:136-157` — `::new()->Arc<Self>`, `record` = `fetch_add`,
  `.total()`; StarvationMetrics/HealthTierMetrics/LeaseMetrics) held on AppState (`app.rs:103,111,139,189`).
- `upstream_requests_total{account_id,status}` chokepoints = 3 request-completion wrappers, each builds ONE
  content-free `RequestLog` and fires `log.emit(); log_bus.publish(log.to_log_event()); spawn_persist_request_log(...)`
  exactly once per client request: `control_route` (`control.rs:232-250`), `responses_route`
  (`ingress.rs:1392-1432`, emit `:1428-1430`), `messages_route` (`ingress.rs:1992-2051`, emit `:2046-2048`). All ~9
  handler paths funnel into these 3. `RequestLog.status: StatusCode` (`.as_u16()`), `RequestLog.account_id:
  Option<String>` (`observability.rs:36,40`; `None` on 503-no-eligible). Failover retries do NOT double-count — the
  `log` reflects only the final attempt (per-retry is `FailoverMetrics`, `ingress.rs:1282`).
- `rate_limit_hits_total{type}` chokepoint = INSIDE `RuntimeStates::record_rate_limit` (`runtime_state.rs:424-451`) —
  the single true site (all ~10 `record_failure` callers funnel through the one `sig.status == 429` branch at
  `ingress.rs:157`). The only real per-request dimension is `retry_after.is_some()` (upstream-provided) vs `.is_none()`
  (computed backoff). `record_quota_exceeded` is dead-from-production (A6 retirement, tests-only) and the durable
  `quota_exceeded` STATUS is set by the usage poller (`usage_refresh.rs:107`), NOT a per-request event — so do NOT
  add a quota `{type}`; it has no request-path source.
- Counter-map precedent: `RuntimeStates` = `RwLock<HashMap<AccountId, RuntimeState>>` (`runtime_state.rs:321-322`)
  with a per-request write-lock `mutate` (`:410-418`) on higher-frequency ops (record_success on every completion) —
  so a `RwLock<HashMap>` write-lock per request for a counter bump is an EXISTING, accepted contention shape, not new.
  No DashMap dep (grep-confirmed zero hits). Cardinality bounded: account_id operator-managed (~tens), status a small
  HTTP-code set, type 2 fixed strings.
- `account_id=None` render convention: `account_id=""` (mirrors the existing `pool: None → pool=""` at
  `metrics.rs:177,184`) — keeps the 503-no-eligible rate visible rather than dropping it.

## Global Constraints

- **In-process MONOTONIC counters (inviolable).** `fetch_add` only — NEVER a decrement/reset/derived-gauge. These are
  Prometheus `counter` type. Do NOT derive from `request_log` (C12 prunes it → non-monotonic). A test asserts values
  only ever increase.
- **Content-free labels (inviolable).** `account_id` = the opaque store-row id (same class already logged). `status` =
  numeric HTTP code. `type` = a FIXED `&'static str` set (`"upstream"`/`"backoff"`) — NEVER an upstream error
  message/body/retry-after value. A test asserts the rendered `/metrics` body never contains an email/token/SECRET.
- **Single-chokepoint-per-metric correctness.** `rate_limit_hits` bumps in exactly one place (`record_rate_limit`).
  `upstream_requests` bumps at each of the 3 wrapper sites — ALL THREE must be wired (missing one undercounts a whole
  traffic class); a test drives each of the 3 traffic classes (control, responses, messages) and asserts each
  increments. No double-count on failover (the `log` is the final outcome).
- **Bounded, no cardinality explosion.** Labels are bounded (accounts × statuses; 2 types). No unbounded label source.
- **Additive, no regression.** Only ADDS structs + increments + render; touches no selection/failover/wedge logic.
  The 5 wedge/cyber/failover/starvation suites MUST stay green. `RuntimeStates` gaining a metrics handle mirrors the
  C9 `LeaseMetrics` coupling precedent (documented exception, content-free counter).
- Verify each task: `cargo test --workspace` green + `cargo clippy --workspace --all-targets -- -D warnings` +
  `cargo fmt --all -- --check` clean (run `cargo fmt` before committing — keep the workspace fmt-clean).

---

### Task 1: The two counter-map structs + AppState fields

**Files:** `crates/polyflare-server/src/observability.rs` (the structs), `crates/polyflare-server/src/app.rs` (AppState
fields), the ~40 mechanical AppState construction sites (tests + main.rs + control.rs) get the two new fields; tests
in observability.rs.

**Interfaces — Produces:**
```rust
// UpstreamRequestMetrics — labeled by (account_id, status). account_id None ⇒ stored as "" key.
pub struct UpstreamRequestMetrics { inner: RwLock<HashMap<(String, u16), u64>> }
impl UpstreamRequestMetrics {
    pub fn new() -> Arc<Self>;
    pub fn record(&self, account_id: Option<&str>, status: u16);          // fetch_add-equivalent: entry += 1
    pub fn snapshot(&self) -> Vec<(String, u16, u64)>;                    // clone-out for rendering
}
// RateLimitMetrics — labeled by a fixed type string.
pub struct RateLimitMetrics { inner: RwLock<HashMap<&'static str, u64>> }
impl RateLimitMetrics {
    pub fn new() -> Arc<Self>;
    pub fn record(&self, kind: &'static str);                            // entry += 1
    pub fn snapshot(&self) -> Vec<(String, u64)>;                        // (type, count)
}
```
- `record` takes a brief write lock, `*entry += 1` (saturating not needed — u64 counter). `None` account_id ⇒ `""` key.
- Hold on AppState: `pub upstream_request_metrics: Arc<UpstreamRequestMetrics>`, `pub rate_limit_metrics: Arc<RateLimitMetrics>`
  (doc-commented content-free, mirror `lease_metrics`'s field doc at `app.rs:189`). Recover a poisoned lock like
  `RuntimeStates::overlay` does (`.unwrap_or_else(|e| e.into_inner())`) rather than panic.

- [ ] **Step 1:** Failing tests: (a) `record(Some("a"), 200)` twice ⇒ snapshot has `("a",200,2)`; different (id,status)
      are distinct keys. (b) `record(None, 503)` ⇒ snapshot has `("",503,1)`. (c) monotonic — repeated record only
      increases. (d) RateLimitMetrics: `record("upstream")` + `record("backoff")` ⇒ 2 distinct entries; repeated
      increments. (e) empty snapshot ⇒ empty Vec.
- [ ] **Step 2:** Run — fail. **Step 3:** Implement the structs + AppState fields (+ the ~40 mechanical
      `upstream_request_metrics: UpstreamRequestMetrics::new(), rate_limit_metrics: RateLimitMetrics::new(),` site
      insertions — mirror how `lease_metrics` was added). **Step 4:** Green; run `cargo fmt`.
- [ ] **Step 5:** Commit: `feat(server): UpstreamRequestMetrics + RateLimitMetrics content-free labeled counters`

---

### Task 2: Wire the increments at the chokepoints

**Files:** `crates/polyflare-server/src/control.rs` (`control_route`), `crates/polyflare-server/src/ingress.rs`
(`responses_route`, `messages_route`), `crates/polyflare-server/src/runtime_state.rs` (`record_rate_limit`); tests.

**Interfaces — Consumes:** Task 1's structs on AppState. **Produces:** live counters.

- **upstream_requests** — at EACH of the 3 wrapper sites, right after `log.emit()` (where the content-free `log` is
  already built), add: `state.upstream_request_metrics.record(log.account_id.as_deref(), log.status.as_u16());`
  (adapt to the exact field names on the `log` struct — read them). All 3 sites: `control.rs:~250`,
  `ingress.rs:~1430` (responses_route), `ingress.rs:~2048` (messages_route).
- **rate_limit_hits** — inside `RuntimeStates::record_rate_limit` (`runtime_state.rs:424`), add a metrics bump keyed
  by `if retry_after.is_some() { "upstream" } else { "backoff" }`. `RuntimeStates` needs a handle to
  `RateLimitMetrics` — thread it the SAME way C9 threaded `LeaseMetrics` into `runtime_state.rs` (a call-site param on
  `record_rate_limit`, OR a field on `RuntimeStates` — check how LeaseMetrics was threaded and mirror it; call-site
  param is lower blast-radius. record_rate_limit's ~10 callers all go through `record_failure` at `ingress.rs:157`,
  so passing `&state.rate_limit_metrics` there is one edit). Content-free: the `&'static str` type only.

- [ ] **Step 1:** Failing tests: (a) an e2e/integration driving a CONTROL request ⇒ `upstream_request_metrics`
      snapshot has an entry for it; a `/responses` request ⇒ an entry; a `/v1/messages` request ⇒ an entry (proves
      all 3 wrappers wired — each traffic class counts). (b) a 429 writeback (drive `record_failure` with a 429
      FailureSignal, or `record_rate_limit` directly) ⇒ `rate_limit_metrics` has `("upstream",1)` when retry_after is
      Some, `("backoff",1)` when None. (c) a 503-no-eligible ⇒ upstream_requests has an `("",503,_)` entry (account
      None). (d) no double-count: a request that fails over once still records exactly ONE upstream_requests entry
      (final outcome).
- [ ] **Step 2:** Run — fail. **Step 3:** Wire the 4 increment points. **Step 4:** Green; wedge/starvation/failover
      suites green; `cargo fmt`.
- [ ] **Step 5:** Commit: `feat(server): wire upstream_requests + rate_limit_hits counters at their chokepoints`

---

### Task 3: Render the two counter families in `/metrics` + e2e

**Files:** `crates/polyflare-server/src/metrics.rs` (extend `MetricsSnapshot` + `render_prometheus_text` + the
handler); tests + e2e.

**Interfaces — Consumes:** Task 1+2. **Produces:** `/metrics` emits the two counter families.

- Extend `MetricsSnapshot` with `pub upstream_requests: Vec<(String, u16, u64)>` + `pub rate_limit_hits: Vec<(String, u64)>`.
- `render_prometheus_text`: two new families, each `# HELP` + `# TYPE ... counter` ONCE, then a sample line per entry:
  `polyflare_upstream_requests_total{account_id="<esc>",status="<n>"} <count>` and
  `polyflare_rate_limit_hits_total{type="<esc>"} <count>`. Reuse `escape_label_value`. `account_id=""` for the None
  key renders `account_id=""` (valid).
- `metrics_handler`: read `state.upstream_request_metrics.snapshot()` + `state.rate_limit_metrics.snapshot()` into the
  `MetricsSnapshot` alongside the existing reads.

- [ ] **Step 1:** Failing tests: (a) `render_prometheus_text` with seeded upstream/rate-limit vecs emits the exact
      `# TYPE polyflare_upstream_requests_total counter` + labeled sample lines + the rate-limit family (HELP/TYPE
      once per family). (b) content-safety: a snapshot renders no `@`/email/SECRET. (c) e2e through the real
      `/metrics` (admin-gated): drive a request + a 429, scrape `/metrics`, assert `polyflare_upstream_requests_total{...}`
      and `polyflare_rate_limit_hits_total{type="..."}` lines appear with the right labels; assert the content-safety
      body has no seeded email/token.
- [ ] **Step 2:** Run — fail. **Step 3:** Extend snapshot + render + handler. **Step 4:** Green; all suites green;
      clippy + fmt clean.
- [ ] **Step 5:** Commit: `feat(server): render upstream_requests_total + rate_limit_hits_total in /metrics`

---

## Suggested order

1 (structs + AppState) → 2 (wire increments at chokepoints) → 3 (render + e2e). After Task 3, `/metrics` exposes
codex-lb's `upstream_requests_total{account_id,status}` + `rate_limit_hits_total{type}` as proper in-process monotonic
counters, content-free, alongside the C11 metrics. Update `PORTING-CODEXLB.md` (C11 note: these two contract counters
now present). Follow-ups (not this plan): `bridge_*`/`continuity_*` (still no M3 backing); per-status/per-provider
splits if wanted.
