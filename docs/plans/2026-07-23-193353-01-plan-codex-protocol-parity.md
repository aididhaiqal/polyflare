# Codex protocol parity and load-balancer hardening

**Goal:** Make PolyFlare faithfully implement the current vendored `codex-rs` core wire contract, then improve account selection using reliable protocol outcomes.
**Why planning is required:** This changes a public streaming protocol, cross-transport continuity ownership, authentication recovery, and failure-driven routing; a partial or incorrect implementation can silently lose conversation context.
**Acceptance:** A real Codex client can move between downstream WebSocket and HTTP fallback, compact, retry failures, use a pool-scoped URL, and call stable auxiliary endpoints without losing identity, changing pool, corrupting continuation state, or losing actionable upstream metadata. Load-balancer health and capacity decisions use completed/failed protocol outcomes rather than transport EOF alone. Existing user changes remain intact.

### Outcome 1: One canonical Codex conversation identity and pool boundary
- Work: Recognize the exact current Codex headers (`session-id`, `thread-id`, `x-codex-window-id`, and turn state), derive one namespaced identity for HTTP, WebSocket, compact, and control traffic, and preserve the selected pool through WebSocket upgrade, owner resolution, redial, and continuity state.
- Risks/open questions: Existing continuity rows use older hashes; compatibility must avoid silently merging unrelated sessions or bypassing a requested pool.
- Verify: `cargo test -p polyflare-server session_key`
- Verify: `cargo test -p polyflare-server --test ws_downstream_relay`
- Verify: `cargo test -p polyflare-server --test pool_routing`

### Outcome 2: Completion-gated continuation and Codex-exact WS reuse
- Work: Commit response IDs, input baselines, ownership, and account success only after `response.completed`; invalidate staged state on failed, incomplete, wrapped-error, or pre-terminal-close paths. Match Codex's exhaustive incremental-request field comparison, active-turn metadata state, client/proxy anchor provenance, and `generate:false` prewarm behavior.
- Risks/open questions: The raw downstream relay and HTTP-to-WS executor have different ownership of full history; only proxy-generated anchors may be stripped locally.
- Verify: `cargo test -p polyflare-codex --lib ws`
- Verify: `cargo test -p polyflare-server --test ws_downstream_relay`
- Verify: `cargo test -p polyflare-server --test wedge_regression`

### Outcome 3: Faithful HTTP fallback and response metadata
- Work: Add bounded zstd request decompression before parsing; forward decompressed or original bytes with correct headers; preserve upstream status, filtered Codex response headers, retry metadata, and opaque bounded error bodies; carry stream headers alongside the body.
- Risks/open questions: Decompression must resist bombs, and response forwarding must not log or persist content-bearing bodies.
- Verify: `cargo test -p polyflare-server --test large_body`
- Verify: `cargo test -p polyflare-server --test large_body`
- Verify: `cargo test -p polyflare-server --test failure_routing`

### Outcome 4: Reactive authentication and auxiliary endpoint parity
- Work: On the first upstream 401, perform one per-account synchronized forced refresh and retry before benching. Make initial WS dial failures observable and health-affecting. Route stable Codex image endpoints with correct body/content-type/headers; cover feature-gated search and realtime endpoints according to the vendored feature contract.
- Risks/open questions: Realtime WebRTC plus sideband WebSocket needs explicit session/account pinning and must not be approximated as an ordinary unary forward.
- Verify: `cargo test -p polyflare-server --test refresh_path`
- Verify: `cargo test -p polyflare-server --test control_endpoints_e2e`

### Outcome 5: Protocol-driven load balancing
- Work: Feed completed, failed, incomplete, 401, 429, quota, overload, and transport-loss outcomes into account health without retrying after client-visible output. Preserve hard owner affinity, use capacity weighting only for new/unowned sessions, refresh usage immediately after capacity failures, and keep pool/capability constraints hard across every transport.
- Risks/open questions: Failure feedback must not double-penalize request-level errors or reroute an anchored partial stream.
- Verify: `cargo test -p polyflare-core select`
- Verify: `cargo test -p polyflare-server --test pool_routing`
- Verify: `cargo test -p polyflare-server --test failure_routing`

### Outcome 6: Pinned compatibility contract and completion gate
- Work: Add fixtures derived from the vendored Codex request/response types for every request-field mutation, terminal event, transport fallback, compact handoff, and supported endpoint. Run a real Codex-through-PolyFlare matrix for WS, forced HTTP fallback, compression, compaction, pool isolation, auth recovery, rate limiting, and image generation.
- Verify: `cargo fmt --all -- --check`
- Verify: `cargo clippy --workspace --all-targets -- -D warnings`
- Verify: `cargo test --workspace`
- Verify: `git diff --check`

### Outcome 7: Selected-account authentication identity tuple
- Work: Derive `chatgpt_account_is_fedramp` from the selected account's already-encrypted,
  durably stored ID token and derive `Authorization`, `ChatGPT-Account-ID`, and
  `X-OpenAI-Fedramp` exclusively from that selected account across Responses HTTP/WS,
  compact/control, model discovery, usage refresh, onboarding/imported credentials, and reactive
  token refresh. A non-FedRAMP selected account must remove any client-forwarded FedRAMP header; a
  FedRAMP account must set exactly `true`.
- Risks/open questions: Do not duplicate the claim into a second account column that can drift from
  rotated ID tokens. Malformed/missing claims must fail closed to non-FedRAMP while retaining the
  selected bearer/account pair; token refresh naturally updates the source ID token.
- Verify: `cargo test -p polyflare-codex oauth`
- Verify: `cargo test -p polyflare-store --test store_roundtrip`
- Verify: `cargo test -p polyflare-server --test codex_fingerprint_parity_gate`
- Verify: `cargo test -p polyflare-server --test control_endpoints_e2e`
- Verify: `cargo test -p polyflare-server --test ws_downstream_relay`

### Outcome 8: Aggregate logical-turn attempt budget
- Work: Extract the stable Codex `turn_id` from the canonical `x-codex-turn-metadata` projection
  on HTTP and `client_metadata` on downstream WebSocket frames, hash it with session/thread/pool
  scope, and enforce one process-wide attempt budget across client retries, PolyFlare account
  failover, same-account recovery, and transparent WebSocket replay. Requests without trustworthy
  turn identity retain the existing per-request bound.
- Risks/open questions: Do not count `generate:false` startup prewarm against a later user turn;
  never log or persist raw metadata; do not retry after client-visible output; and do not return a
  retryable error when the aggregate budget is exhausted or Codex will amplify the loop again.
  Process-local enforcement is only an interim single-instance guarantee; a replicated deployment
  requires shared atomic ownership.
- Verify: `cargo test -p polyflare-server session_key`
- Verify: `cargo test -p polyflare-server turn_attempt`
- Verify: `cargo test -p polyflare-server --test failover_loop`
- Verify: `cargo test -p polyflare-server --test ws_downstream_relay`

### Verification checkpoint — 2026-07-23

- Automated compatibility matrix: complete and green, including pre-upgrade WebSocket readiness,
  same-account reactive authentication, upgrade metadata, HTTP fallback, pool isolation, terminal
  outcomes, and routing-health writeback.
- Workspace gates: `cargo test --workspace --quiet`, `cargo fmt --all -- --check`,
  `cargo clippy --workspace --all-targets -- -D warnings`, and `git diff --check` passed.
- Remaining external evidence: rerun the real Codex-through-PolyFlare smoke matrix against live
  upstream credentials before treating this as a production release sign-off.
- Deliberate exclusion: Realtime call creation plus sideband WebSocket remains unsupported until
  call IDs can be pinned to the same account across both transports.

### Verification checkpoint — transport-neutral routing follow-up

- WebSocket hidden-redial 401s now enter synchronized same-account refresh instead of retrying a
  stale bearer; replay remains forbidden after any client-visible upstream output.
- Each generating WebSocket turn holds one in-flight lease. Completion clears stale health,
  terminal-less upstream loss records one transient failure, and client cancellation or
  request-level/incomplete terminals release without penalizing the account.
- Initial WS selection and durable reselect preserve the HTTP capability floor from sticky session
  state, pool tags, and `x-polyflare-capability`.
- Fresh gates after the final changes: downstream relay 29/29, `cargo test --workspace --quiet`,
  strict workspace clippy, format check, and `git diff --check`.
- The running `127.0.0.1:8080` instance answered the dashboard and showed active successful WS
  traffic, but it predates these final source changes. Restart and rerun the live smoke matrix before
  production sign-off.

### Verification checkpoint — fleet catalog and admission hardening

- Root and named-pool model catalogs now expose only the all-member model intersection. Synthetic
  aliases follow their surviving target, and known per-account model support is a hard request,
  compact, and translated-alias eligibility input.
- Root/pool `/models`, HTTP response metadata, initial WS handshakes, and hidden WS redials share one
  deterministic virtual scope ETag; pooled drift on any member invalidates a stale socket.
- Unary forwarding holds one lease through its same-account 401 retry and writes transport-neutral
  health feedback. Request logs persist bounded protocol outcomes rather than treating every initial
  HTTP 200 as success.
- New HTTP work and new WS handshakes select and reserve pressure atomically. Idle sockets use a
  separate `open_ws` counter; each active turn independently holds `in_flight`.
- Capacity-weighted routing now applies bounded-load weighted rendezvous to the raw Codex
  `session-id` for both HTTP and initial/reselected WS ownership, keeping a main session and its
  subagents together when capacity permits.
- Rate-limit/quota cooldowns are restart-safe, and immediate usage refresh requests are keyed and
  coalesced per account.
- Focused evidence: model catalog 7/7, control/unary 14/14, downstream WS relay 29/29, dashboard
  30/30, restart cooldown regression, and the WS session-family/atomic socket-admission unit
  regressions.
- Live upstream sign-off still requires a coordinated restart of the manually managed port 8080
  process; it was deliberately not interrupted during this source-level pass.

### Verification checkpoint — vendored defaults and catalog readiness

- Re-audited every exported `codex-api` endpoint plus current feature specs. Responses WebSocket
  flags are removed compatibility toggles; provider capability enables WS, the dated beta header
  remains required, and only `426` activates session-lifetime HTTP fallback. Search is off by
  default, images are on, and realtime remains experimental/off.
- Exact root/pool catalogs now warm before listener readiness and periodically at the configured
  TTL. A fresh per-account catalog is reused across overlapping scopes, eliminating repeated
  authenticated fetches while retaining independent exact-scope stale fallback and virtual ETags.
- Empty fleets no longer expose a legacy cached native account ETag.
- The atomic admission implementation preserves `last_selected_at` as selector input. Reservation
  visibility is proved directly through `in_flight` and `open_ws`; it does not distort timestamps
  to force a RoundRobin ordering.
- Exact-scope stale fallback now has a regression proving a failed root refresh retains only the
  prior root projection and cannot borrow a newly refreshed narrower pool projection.
- Fresh post-mutation evidence: the focused stale-scope and atomic-admission regressions pass,
  `model_catalog_e2e` is 7/7, and `cargo fmt --all -- --check`, `git diff --check`,
  `cargo test --workspace --quiet`, and strict all-target workspace clippy all pass.
- Live upstream sign-off still requires a coordinated restart and real Codex-through-PolyFlare
  smoke matrix against the manually managed port 8080 process.

### Verification checkpoint — catalog authentication and failure isolation

- Model discovery now shares the same per-account refresh-token singleflight as responses, unary
  control, and WebSocket paths. An upstream 401 gets one synchronized same-account refresh and
  retry, and the rotated credentials are persisted.
- Partial multi-account discovery retains successful per-account catalogs without publishing an
  incomplete root/pool projection. Failed members receive a short negative-cache window, preventing
  immediate retries once per overlapping scope.
- Startup catalog warming has a two-second readiness budget. Slow work continues detached and can
  populate the shared cache after the listener starts; stale exact-scope/static-floor fallback
  remains available meanwhile.
- Focused regressions cover catalog 401 recovery, shared refresh locks, partial-member reuse,
  negative retry suppression, and detached warmup completion. Full workspace gates are required
  after this checkpoint's final mutation.

### Verification checkpoint — hard admission and owner-safe waiting

- Atomic selection now enforces configurable global and per-account in-flight and open-WebSocket
  caps. Ordinary work preserves one per-account recovery slot by default.
- Pinned HTTP and WebSocket work waits at most the configured admission timeout for its exact
  owner. Capacity pressure never reroutes a continuation token to another account.
- Native and translated Messages ingress now select and reserve atomically, and recovery/failover
  paths cannot bypass ordinary caps with an unconditional lease.
- `generate:false` WebSocket prewarm holds admission capacity without creating a user request-log
  row.
- Session-family weighted rendezvous no longer uses the raw `min_load + 1` spill heuristic; hard
  caps determine availability and live load remains a soft capacity penalty.

### Verification checkpoint — aggregate logical-turn attempt budget

- Canonical HTTP and WebSocket turn metadata now resolve to one session/thread/pool-scoped SHA-256
  key; malformed, empty, oversized, and `generate:false` metadata does not enter the budget.
- A bounded process-local TTL map atomically limits the same logical turn across client retries,
  HTTP failover, watchdog recovery, WebSocket sends, and transparent replay. Missing trustworthy
  metadata retains the existing per-request bounds.
- A pre-output 401 refunds its rejected authentication attempt. All transport, capacity, failover,
  and replay attempts remain charged.
- Exhaustion is surfaced as a content-safe HTTP/WS 400 invalid-request error, never a retryable cap
  signal. Focused regressions cover concurrency, expiry, bypass, refund, upstream non-execution,
  prewarm exclusion, raw-ID safety, and blocked WebSocket replay.
- This remains single-process enforcement. Multi-replica deployment requires shared atomic turn
  ownership before the guarantee can extend across instances.

### Verification checkpoint — request-size-aware admission

- Native Codex HTTP and per-turn WebSocket ingress derive a bounded, content-free token estimate
  from the raw input JSON length and requested output ceiling without materializing the prompt
  tree. Materialized native/translated Messages requests use the same estimator.
- Atomic request admission now enforces both request-count caps and independent weighted-pressure
  caps. One request remains at least one unit; large turns consume several 16K-token units and
  route around an account whose ordinary pressure budget is full even when its request count is
  still low.
- Ordinary work cannot consume either the owner request-count reserve or the owner pressure
  reserve. Pinned continuations may use both reserves but still wait only for their exact owner.
- Every retry, failover, watchdog recovery, translated request, and generating WebSocket turn
  carries the same request pressure through its lease. Idle WebSockets remain governed separately
  by the open-socket bulkhead.
- Authoritative terminal Codex usage updates one fixed-cardinality bounded EWMA ratio. Future
  estimates are calibrated by that ratio; uncached input counts 1x, cached input 1/8x, and
  autoregressive output 4x on the same compute-pressure scale. No model, account, session, or
  request content becomes a calibration key.
- Prometheus exposes per-account live pressure plus global calibration ratio/sample/token totals.
  `/api/overview` and the existing Admission status tile expose aggregate live pressure and
  calibration state without adding dashboard clutter.
- Focused evidence: pressure-estimation, hostile-bound, pressure-spill, owner-reserve, calibration,
  server-lib, metrics-endpoint, read-API, dashboard test, dashboard typecheck, and production-build
  checks pass. Full workspace and strict-clippy gates remain required after final review.

### Architecture boundary — multi-process coordination and realtime

- PolyFlare's current deployment contract is one binary over one local SQLite WAL store. Its
  account-cache generations, request/pressure leases, logical-turn budget, refresh singleflights,
  and WebSocket connection ownership are deliberately process-local.
- Running multiple independent processes against separate SQLite files cannot provide shared
  admission or continuity. Running them against one filesystem path still lacks instance epochs,
  TTL-fenced leases, cross-process cache invalidation, owner forwarding, and scheduler leadership;
  SQLite locking alone is not a coordinator.
- A real replicated mode therefore requires an explicit shared backend and deployment contract
  (for example Postgres/Redis), stable instance identity, lease claim/renew/release fencing,
  cross-instance owner forwarding or deterministic handoff, cache generations, and leader-elected
  background jobs. This is a separate deployment architecture, not a safe incremental toggle for
  the current local service.
- Realtime call creation plus sideband WebSocket remains intentionally disabled, matching the
  vendored experimental-off default. Enabling it requires a durable call-id→account binding so
  both legs use one selected-account identity tuple; ordinary unary forwarding is not sufficient.

### Verification checkpoint — isolated real-client candidate smoke

- A consistent SQLite/key clone of the live store booted the reviewed candidate on an isolated
  loopback listener without disturbing the manually managed `127.0.0.1:8080` process. Catalog
  readiness completed authoritatively for the active root fleet before the listener opened.
- Real Codex `0.144.4` traffic through the candidate completed over both HTTP-SSE and downstream
  WebSocket. A WebSocket conversation resumed from a second Codex process with the same hashed
  session and serving account and correctly recalled prior-turn context.
- A real unary `POST /responses/compact` against the rebuilt candidate returned 200 with the
  upstream compact payload and produced one content-free request-log row linked to its hashed
  session.
- The live smoke exposed one contract violation that mock-only verification had missed: successful
  WS rows retained status 200 but left `protocol_outcome` null. WS terminal classification is now
  included in the initial FIFO request-log insert for completed, failed, incomplete, cancelled,
  and transport-lost turns; HTTP-SSE retains its ordered post-stream usage/outcome update.
- The focused red regression reproduced the null outcome before the fix. Afterward, WS telemetry
  unit tests and the 30-test downstream relay suite passed, as did `cargo test --workspace --quiet`,
  strict all-target workspace clippy, format checking, and `git diff --check`.
- The rebuilt candidate then completed another real WebSocket Codex request. Its newest durable row
  carried `transport=ws`, `status=200`, and `protocol_outcome=completed`; terminal usage updated the
  pressure calibration sample and all account pressure returned to zero.
- Final review also closed two configuration/telemetry edges: buffered non-SSE `/responses`
  bodies now derive their normal-EOF outcome from the final HTTP status instead of being mislabeled
  as transport loss, and an enabled per-account admission cap clamps its owner reserve below the
  cap so ordinary new work always retains at least one slot. The acceptance commands now name the
  consolidated test targets that actually exist.
- This proves the reviewed candidate against real upstream traffic. Replacing the user's manually
  managed `8080` process remains an operational cutover, not evidence that may be inferred from the
  isolated listener.
