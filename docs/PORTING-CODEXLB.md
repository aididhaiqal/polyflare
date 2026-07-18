# Porting codex-lb → PolyFlare: making the routing real

> From a focused deep-study of codex-lb (Python) against PolyFlare's current code. Each item cites the
> codex-lb reference **and** the PolyFlare `file:line` where it lands.

## Thesis

**PolyFlare has ported codex-lb's routing math but runs it on neutral inputs.** The selector
(`crates/polyflare-core/src/select.rs`) already reads `error_count`, `cooldown_until`, `last_error_at`,
`last_selected_at`, `health_tier`, `in_flight` — and already has the eligibility gates and the
`capacity_weighted` waterfall — but **nothing ever writes those fields**, so it picks exactly one
account with no failover on inert data. This is a **write-side + retry-loop** project, not a routing
rewrite.

### Architectural correction (important)

Those runtime fields are **not** DB columns. They are neutral struct defaults in `AccountSnapshot::new`
(`crates/polyflare-core/src/types.rs:263-305`); `assemble_snapshots`
(`crates/polyflare-server/src/snapshot.rs:48-58`) only fills the durable + usage-window fields. The
durable `accounts` table (migration `0001`) carries exactly codex-lb's coarse four:
`status` / `deactivation_reason` / `reset_at` / `blocked_at`.

So mirror codex-lb's split:
- Add an **in-memory `RuntimeState` map** — `RwLock<HashMap<AccountId, RuntimeState>>` — the analog of
  codex-lb's `_runtime`. Have `assemble_snapshots` **merge it into each `AccountSnapshot`**, overriding
  the neutral defaults, so `select.rs` finally sees live state.
- **Persist only the coarse `status`/`reset_at`/`blocked_at`** (already columns) for restart / peer
  recovery. `in_flight` is deliberately never persisted (a DB round-trip per request is why codex-lb
  kept it in memory).
- Bump the `account_cache` generation (`crates/polyflare-server/src/account_cache.rs:41-43`) after each
  mutation so the next snapshot assembly rebuilds.

---

## Phase A — Make the routing see reality (the keystone; do first)

Without this the already-built waterfall and eligibility gates do nothing.

> **STATUS (audited 2026-07-17 against `feat/dashboard-phase1-frontend`; A6/A7 CLOSED 2026-07-18 per
> `docs/superpowers/plans/2026-07-18-failure-code-writeback.md` — Phase A is now complete).**
> - **A1 substrate — DONE.** `RuntimeState`/`RuntimeStates` (`runtime_state.rs:49-69`), overlaid pre-selection,
>   single write funnel `record_failure` (`ingress.rs:85-107`). `FailureSignal` (`polyflare-core/src/types.rs`)
>   now carries an optional upstream `error_code` string (Task 1 of the writeback plan); both executors
>   populate it content-safely (code only, never message/body). Client-disconnect neutrality holds (emergent
>   Stream-drop).
> - **A2 rate-limit — DONE.** `record_rate_limit` (`runtime_state.rs:117-130`): error_count++, last_error_at,
>   Retry-After-else-`backoff_secs`, 30s floor + 24h ceiling, never-shortens. Jitter correctly deferred to B10;
>   `resets_at→reset_at` is N/A per this doc's own cooldown-only simplification. Tested end-to-end (`failure_routing.rs`).
> - **A3 transient/success/decays — DONE.** `record_transient_error` + `record_success` (zeros error_count,
>   nulls last_error_at), both decays in `select.rs::eligibility` (cooldown-elapsed clear + error_count≥3
>   backoff admit), wired at the watchdog success/error paths (`watchdog.rs:381-430`).
> - **A6 quota — RESOLVED as an intentional retirement, not wired.** `record_quota_exceeded`
>   (`runtime_state.rs:135-...`, correct: +120s cooldown, no error_count bump) stays unit-tested,
>   unreachable-from-the-request-path infrastructure — see its doc comment for the full evidence trail.
>   Ground truth (`codex-rs`: `codex-api/src/sse/responses.rs:630,634`, `api_bridge.rs:21-22,112-113`,
>   `core/tests/suite/quota_exceeded.rs`): the real quota wire codes are `insufficient_quota` and
>   `usage_not_included`. Neither can reach `FailureSignal.error_code` on this codebase's actual wire path:
>   `insufficient_quota` arrives only inside a `response.failed` frame that `ws::codec::classify` and the
>   HTTP-SSE relay deliberately reframe as SSE and pass through to the client (never an `ExecError`);
>   `usage_not_included` can arrive as a raw pre-stream 429 but only under the JSON key `error.type`, which
>   `extract_error_code` (which reads only `error.code`) never sees. So a code-keyed quota branch in
>   `record_failure` would be dead code from day one — worse than the status quo. `usage_refresh.rs`'s
>   ≤600s poller (usage-percent-derived, strictly more reliable than scraping an error code) remains the
>   sole, authoritative owner of the durable `quota_exceeded` status. `failure_routing.rs` carries two
>   regression tests proving a quota-shaped code that DOES somehow reach `error_code` still falls through
>   to the ordinary status-keyed bucketing, never to `record_quota_exceeded`.
> - **A7 permanent/auth — DONE.** `record_failure` now has a branch (BEFORE the status-keyed bucketing)
>   that routes any `error_code` through the reused `classify_failure` table (`oauth.rs:105-123`) to a
>   durable terminal status (`reauth_required`/`deactivated`) via `AccountRepo::update_status`, leaving
>   `cooldown_until` null and NOT bumping `error_count` (a terminal status supersedes health backoff).
>   Tested end-to-end in `failure_routing.rs` (401 `invalid_grant`, 403 `account_deactivated`, plus the
>   pre-existing plain-500 regression guard).

### A1. Runtime-state substrate + failure classifier → dispatch funnel  · HIGH · large
The keystone. Build `RuntimeState` (above) + a single write funnel:
`classify_upstream_failure()` bucketing every upstream failure into `{rate_limit, quota, permanent,
transient}` using codex-lb's exact code/status sets, and a dispatcher routing each bucket to a
runtime-mutating handler. Replicate the **account-neutral short-circuit** so a *client* disconnect
writes nothing.
*codex-lb:* `load_balancer.py:114-129,189`, `_service/streaming/helpers.py:753-782`,
`helpers.py:42-64`, `core/balancer/logic.py:16-48`, `core/balancer/types.py:12-21`.

### A2. Rate-limit (429) writeback  · HIGH · medium
`error_count += 1`; `last_error_at = now`; `status = rate_limited`; `cooldown_until = now + delay`,
where `delay = parse-Retry-After` (body "try again in Xs") **else** exponential
`0.2s·2^(n-1)·jitter(0.9-1.1)`. Apply the **30s min-cooldown floor**; carry an upstream `resets_at`
into `reset_at`. PolyFlare persists `cooldown_until` directly instead of codex-lb's
`reset_at`+`blocked_at` reconstruction — strictly simpler. `select.rs:168-188` already reads this.
*codex-lb:* `logic.py:991-1021`, `load_balancer.py:1504-1512,1650-1674`, `core/utils/retry.py:51-77`.

### A3. Transient-error record + success-clear + the two decays  · HIGH · medium
`record_error`: `error_count += 1`, `last_error_at = now`. `record_success`: if `error_count > 0`,
zero it **and** null `last_error_at` — **mandatory**, or errors accumulate forever and every account
eventually looks unhealthy. Plus two lazy self-heal decays `select.rs` applies on read (it already
stubs the shape at `select.rs:144-188`): (a) cooldown elapsed → clear `cooldown_until` + zero
`error_count` + null `last_error_at` **together**; (b) `error_count ≥ 3` backoff window elapsed →
clean-slate admit. Wire `record_success` at the watchdog's success path, `record_error` at its
transient path.
*codex-lb:* `load_balancer.py:1535-1566,1676-1707`, `logic.py:464-483`.

### A6. Quota-exceeded writeback  · HIGH · small
`status = quota_exceeded`; `cooldown_until = now + 120s`; `reset_at = upstream reset else now+3600`;
capacity signal = 100%. **Do NOT bump `error_count`** — quota is a capacity signal, not a health error;
bumping it double-penalizes a merely-full account into the drain tier. `select.rs:155-165` already
excludes `quota_exceeded` until `reset_at` elapses.
*codex-lb:* `logic.py:1024-1053`, `load_balancer.py:1514-1522,2115-2118`.

### A7. Permanent/auth-failure writeback  · HIGH · small
Set a terminal `status` from the code table — `reauth_required` for the reauth codes
(refresh_token_expired/reused/invalidated, invalid_grant, token_invalidated, token_expired,
app_session_terminated, account_session_expired, account_auth_invalidated), else `deactivated`
(account_deactivated/suspended/deleted); set `deactivation_reason`; leave `cooldown_until` null (only
re-auth clears it). `select.rs:112-115` already hard-excludes these. (PolyFlare's `classify_failure`
already has this exact table for the OAuth path — reuse it.)
*codex-lb:* `logic.py:1056-1068`, `load_balancer.py:1524-1533`.

---

## Phase B — Failover & anti-starvation (the biggest behavioral gaps)

### B4. Cross-account failover retry loop  · HIGH · large
**The single biggest missing behavior.** Today every upstream failure is a dead end: `ingress.rs`'s
Route arm (`:449`), ResendFull arm (`:478`), and `RecoveryPlan::None` arm (`:527`) all
`Err(_) => BAD_GATEWAY` with no retry. Build:
- Make `WatchdogError` carry the `failure_class` + a `committed` flag (= codex-lb `downstream_visible`).
  The watchdog already distinguishes Armed peek-before-relay vs Disarmed (`watchdog.rs:44-46`), so
  first-byte-relayed is knowable.
- In `ingress.rs`, wrap `execute_with_watchdog` / `execute_recovery` in a **bounded loop (max 3
  accounts)** threading an `excluded: HashSet<AccountId>` into `selector.pick`, re-picking on a
  `failover_next` decision until the pool empties or one succeeds. Clone `prepared.req` before each
  attempt as the in-memory replay buffer.
- **Hard barrier:** once `committed`/downstream-visible, refuse replay and surface in-band.
- **Constraint:** only `RecoveryPlan::None` and post-strip `ResendFull` branches may iterate the full
  pool; an **owner-pinned request with a live anchor must stay fail-closed** (never enters the loop).
  PolyFlare's `apply_ownership` is already at parity — inherit it as the loop's gate.
*codex-lb:* `_service/streaming/retry.py:218-1389`, `logic.py:1074`, `service.py:1594-2422`.

### B5. Anti-starvation backoff-fallback (serve soonest-to-recover)  · HIGH · medium
Change `eligibility()` (`select.rs:126`) from `Option<Candidate>` to the enum the code's own comment
already proposes at `select.rs:114-125`: `Eligible | InBackoff{recover_at} | HardBlocked`. In
`standard_pool`/`select` (`select.rs:292+`) collect `InBackoff` candidates alongside eligible ones;
when the eligible pool is empty, apply codex-lb's guard (fire only if **>1 backoff account**, OR 1
backoff + ≥1 hard-blocked) and force-admit `min-by recover_at`, where
`recover_at = last_error_at + min(300, 30·2^(error_count-3))`. A **lone** backoff account with nothing
else blocked is NOT force-served. Thread `allow_backoff_fallback = false` on any sticky/pin grace path.
Replace `ingress.rs no_eligible()` (`:64`)'s bare 503 with a soonest-reset retry hint.
*codex-lb:* `logic.py:359-548`, `load_balancer.py:1361,2702`.

### B8. Health-tier computation (soft-drain state machine)  · HIGH · medium
Compute `health_tier` each selection instead of forcing it neutral (`select.rs:100-109` notes this is
deferred). `should_drain = used_percent ≥ 85 OR secondary ≥ 90 OR (error_count ≥ 2 AND last_error_at
within 60s)`, with HEALTHY→DRAINING→PROBING→HEALTHY transitions, frozen on blocked statuses. Store the
two aux inputs (`drain_entered_at`, `probe_success_streak`) in the runtime map. Recompute on read —
**but only after A2/A3 populate the inputs**, else `should_drain` is always false and the
`health_tier_pool` gate never narrows.
*codex-lb:* `logic.py:1099-1157`, `load_balancer.py:2176-2210`.

### B10. Active anti-thundering-herd damping  · MEDIUM · medium
Relevant only once B4 exists. (a) Between same-account transient retries in `watchdog.rs`, sleep
`200ms·2^(attempt-1)·jitter(0.9-1.1)` while not yet committed — the jitter is the de-synchronizer.
(b) In `ingress.rs no_eligible()`, optionally replace the immediate 503 with a bounded [1s, 300s]
capacity-recovery wait derived from the soonest `reset_at` the snapshots already hold, chunked into
~10s keepalive SSE heartbeats so the client holds its connection instead of reconnecting into the herd.
*codex-lb:* `core/utils/retry.py:51-77`, `_service/streaming/retry.py:162`, `_service/support.py:43-142`.

---

## Phase C — Load-shaping & ops

### C9. `in_flight` lease accounting  · MEDIUM · medium
Increment `in_flight` in the runtime map at dispatch (`ingress.rs`, right after
`resolve_core_account`), decrement at completion/error, with a stale-reclaim TTL so a crashed request
doesn't pin the counter. Set `last_selected_at = now` on selection (`round_robin`'s tiebreak reads it
and notes it's always None today). Use `in_flight` two ways: exclude accounts at a per-account
concurrency cap, and fold it into the capacity weight as a soft penalty (~2.5%/lease) so the waterfall
spreads load. Keep in-memory; surface only to metrics.
*codex-lb:* `load_balancer.py:201-318,1767-1788,2212-2225`.

### C11. Prometheus `/metrics` surface  · LOW · medium
Emit codex-lb's metric **names + label sets** as the portable contract (dashboards key off them):
`upstream_requests_total{account_id,status}`, `rate_limit_hits_total{type}`,
`accounts_total{status}`, the `account_lease_*` family (pairs with C9), and the `bridge_*`/
`continuity_*` family that PolyFlare's M3 ownership path should emit under identical names. No-op when
disabled.
*codex-lb:* `core/metrics/prometheus.py`, `core/metrics/middleware.py`.

### C12. Data-retention pruning  · LOW · medium
Hourly pruner over `request_log` + `usage_history`, preserving two guards: never delete above the
rollup watermark (would shrink lifetime totals), and always protect the latest row per
`(account_id, window)` via `max(recorded_at) GROUP BY`. Batch-delete (10k) below a floor. Single-node
PolyFlare drops the leader gate. *(Note: codex-lb prunes only these two tables — never a continuity
table.)*
*codex-lb:* `core/retention/job.py`, `core/retention/scheduler.py`, `core/config/settings.py:236`.

---

## Phase D — Deeper features (large / lower priority)

### D13. Per-account egress HTTP/SOCKS proxy routing  · LOW · large
Four forward-only nullable tables (ProxyEndpoint / ProxyPool / ProxyPoolMember / AccountProxyBinding)
+ a route-resolution step in the Rust upstream client with codex-lb's precedence
(account-binding > default-pool > direct). `reqwest` proxy for http/https, a socks feature for socks5
(mirror the socks5→socks5h rdns rule + ordered fallback for idempotent methods). Matters for
per-account IP diversity / anti-ban.
*codex-lb:* `core/upstream_proxy/resolver.py`, `core/clients/codex.py:150`.

### D14. Tool-call dedupe engine + `/responses/compact`  · LOW · large
Port the dedupe module (pure JSON-in/JSON-out — directly translatable): suppress duplicate
side-effect tool calls (apply_patch/exec_command/write_stdin/spawn_agent/update_plan — the
`_SIDE_EFFECT_TOOL_CALL_NAMES` frozenset is the exact contract) within one stream **and** across
replayed input history, with volatile-arg canonicalizers + a 1024-entry LRU. Wire into the SSE relay
and the replayed-input path so a reconnect never forwards a duplicate destructive call. Separately add
the `/responses/compact` passthrough.
*codex-lb:* `tool_call_dedupe.py`, `_service/compact.py`.

### D15. Live upstream model-catalog fetch/merge  · LOW · medium
PolyFlare serves static golden metadata pinned to one codex version. Port the fetcher
(`GET {base}/codex/models?client_version=`, Bearer + chatgpt-account-id) + a periodic refresh loop
that groups active accounts by plan, fetches with cross-account failover, merges same-plan slugs, and
updates an in-memory registry with the golden set as bootstrap floor. Single-node refresh loop first.
*codex-lb:* `core/clients/model_fetcher.py`, `core/openai/model_refresh_scheduler.py`.

### D16. WeeklyCreditPace + EWMA depletion forecasting  · LOW · medium
Port the EWMA core first (pure math: alpha-0.4 d(used%)/dt with reset-on-drop, burn-rate, projected
exhaustion, safe/warning/danger/critical at 0.60/0.80/0.95). Then the pool-wide WeeklyCreditPace
discrete-event sim. Needs per-account weekly usage rows (capacity/remaining/reset/window) — additive,
grow the usage schema feature-by-feature.
*codex-lb:* `core/usage/depletion.py`, `modules/dashboard/weekly_pace.py`.

### D17. Codex control-endpoint surface  · MEDIUM · medium
PolyFlare serves `/responses` (+ `/v1/messages`, `/models`) and nothing else, so every other endpoint
the Codex CLI can call 404s. codex-lb proxies eleven, each a thin pass-through funnelling into one
helper — `_codex_control_proxy` → `service.codex_control_request(path, …, codex_session_affinity=True)`:
`thread/goal/{set,clear,get}`, `analytics-events/events`, `memories/trace_summarize`, `realtime/calls`,
`safety/arc`, `alpha/search`, `agent-identities/jwks` (GET), `opportunistic/admission` (GET),
`images/{generations,edits}`. (`/responses/compact` is separate and larger — see D14.)

The work is small per endpoint but has two real design points:
- **`codex_session_affinity=True`** — a control call must land on the SAME account as the conversation
  it belongs to, not a freshly-selected one. This is M3 ownership resolution applied to a non-`/responses`
  path, where PolyFlare's owner resolution does not currently reach.
- **Header handling both ways** — requests need the same fingerprint synthesis `/responses` gets;
  responses are filtered downstream through `_codex_control_downstream_headers`.

Which endpoints the CLI actually fires depends on config: the endpoint constants live in
`codex-rs/core/src/client.rs:157-163`, and the user's codex-lb config comments that `name = "openai"`
is what enables remote `/responses/compact` — **the gate for that was not located in the source; verify
before assuming any endpoint is dormant.** PolyFlare's dev harness names the provider
`"PolyFlare (local dev)"`, which is why the gap is currently unhit rather than handled.
*codex-lb:* `modules/proxy/api.py:491-597,1744,1931`.

### D18. Client API-key auth on the proxy surface  · MEDIUM · medium · **DONE** (minimal gate)
`POST /responses` / `/v1/messages` (and their `/{pool}/…` variants) were unauthenticated by
default — safe only while bound to `127.0.0.1`. **The minimal gate is now built and merged**
(`docs/superpowers/plans/2026-07-18-d18-client-auth.md`, Tasks 1–5): an `api_keys` table
(hash-at-rest, reveal-once — `crates/polyflare-store/src/api_key_repo.rs`), a `polyflare keys
create/list/revoke` CLI (`crates/polyflare-server/src/keys.rs`), a `require_client_key`
hash-lookup middleware that never logs the presented key (`crates/polyflare-server/src/auth.rs`),
and a bind-address-aware startup posture (`crates/polyflare-server/src/posture.rs`) wired onto an
extracted `proxy` sub-router in `crate::app::build_app` — any key existing ⇒ enforce; no keys +
loopback bind ⇒ open (unchanged zero-config behavior); no keys + non-loopback bind ⇒ **refuse to
start** unless `POLYFLARE_ALLOW_UNAUTHENTICATED_REMOTE=1`. Note the `/api/*` surface was never in
this gap: `require_admin` already covers it, reads and the account PATCH alike (`app.rs:112-134`)
— this item was about the proxy surface only.
codex-lb's equivalent is `api_key: ApiKeyData | None = Security(validate_proxy_api_key)` on every
proxy route — note the `| None`: it permits unauthenticated calls when no keys are configured, so
the *default posture* matches PolyFlare's; what differed is that codex-lb had the mechanism at all.
D18 ported that mechanism, plus the bind-aware refuse-to-start half codex-lb does NOT have.
Content safety verified end-to-end (`crates/polyflare-server/tests/client_key_never_log_e2e.rs`):
the raw key never appears in the persisted `request_log` row, the `/api/logs/stream` SSE feed, or
any `tracing` output, for a real successful proxied request through the real `build_app` stack.

**Deferred — explicit follow-ons, not part of v1 minimal gate** (still tracked in FEATURE-MAP):
- **Per-key account scoping** — restricting a key to a subset of accounts/pools (codex-lb:
  `api_keys.account_ids`-style association).
- **Per-key source scoping** — restricting a key to specific caller sources/IPs.
- **Enforced model/effort/tier** — a key limited to specific models, reasoning efforts, or service
  tiers (codex-lb's `allowed_models`/tier fields).
- **Per-key usage rollup** — token/request accounting attributed to the calling key (today usage
  rolls up by account only; `last_used_at` is the only per-key signal that exists).
*codex-lb:* `modules/api_keys/{service,repository,api}.py`; `modules/proxy/api.py:495`.

---

## Suggested order

**A1 → A2 → A3 → A6 → A7** turns the routing on (small/medium each after the A1 keystone). **B4 → B5**
add the failover + anti-starvation the comparison flagged as HIGH. **B8** and **C9** sharpen the
waterfall. Everything in Phase C/D is independent and can be picked up opportunistically. The whole of
Phase A is the highest-leverage work in the project right now: it's the difference between a selector
that *looks* sophisticated and one that actually reacts to a failing account.
