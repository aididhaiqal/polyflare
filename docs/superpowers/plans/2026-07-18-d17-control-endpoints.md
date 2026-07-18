# D17 — Codex Control-Endpoint Surface (minimal) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use `- [ ]`.

**Goal:** Stop 404ing the codex control endpoints a real CLI calls. Minimal set = a reusable generic
parameterized-path forward primitive + the 3 endpoints codex-rs actually emits: `thread/goal/{set,clear,get}`,
`agent-identities/jwks` (GET, + `wham/` variant), `memories/trace_summarize`. The primitive makes adding the
deferred rest (realtime/calls, analytics-events, alpha/search, safety/arc, images) trivial later.

**Architecture:** A generic `codex_control_forward(state, path, method, headers, body, session_key) -> Response`
in `polyflare-server`: resolves the account (SOFT session→owner affinity — owner if a session header is present,
else any eligible account), materializes it (`resolve_core_account`), forwards a UNARY (non-SSE) request to
`{base}/codex/<path>` (or `{base}/<path>` for `wham/…`) reusing the `codex_headers` synth + bearer injection,
filters the response headers to codex-lb's allow-set, and writes a content-free `request_log` row. Routes attach
to the D18-gated `proxy` sub-router.

**Authority — the D17 scoping study (this session).** codex-lb refs: `_service/codex_control.py:148`
(orchestration), `core/clients/proxy.py:4165` (transport, URL build, `_build_upstream_headers`),
`modules/proxy/api.py:473,487` (`_CODEX_CONTROL_RESPONSE_HEADERS` 8-name allow-set, `_codex_control_downstream_headers`).
Affinity is SOFT (`affinity.py:196` — owner only if a session/turn-state header is present, else any account).

## Global Constraints

- **FORWARD the body, NEVER store it (content-safety, inviolable).** The control body is proxied upstream and
  discarded. The persisted `request_log` row is content-free: `request_kind = "codex_control_<path>"` + account +
  status + latency + error_code — NEVER the body/message. The transient body decode must not reach `request_log`
  or any log line. codex-lb does exactly this (`codex_control.py:422` passes `model=None`, no body). No schema
  change — reuse `request_log`.
- **SOFT affinity — do NOT over-bind (inviolable for correctness).** `codex_session_affinity` binds to the
  conversation's OWNER account ONLY when the request carries a session header (`x-codex-turn-state` / `session_id`
  / `thread-id` etc. — the SAME headers `session_key::parse_inbound` reads). No session header ⇒ select ANY
  eligible account. This is NOT a `/responses` hard anchor — a no-header control call, or one whose owner is
  unavailable, must fall back to normal selection, NEVER be stranded/failed. Over-binding it into a hard anchor
  is the primary risk the scoping flagged.
- **Behind the D18 gate.** Control routes are proxy surface → they attach to the `proxy` sub-router
  (`app.rs:218-228`) so they inherit `require_client_key` when `enforce_client_keys`. GETs (`jwks`) too — parity
  with codex-lb (not open like `/models`).
- **UNARY, not SSE.** Unlike `/responses`, control endpoints return a normal (JSON/unary) response — the forward
  primitive reads the full response + status + filtered headers, NOT a stream. Do not route through the SSE/
  `ObservingStream`/continuity machinery.
- **Reuse, don't reinvent:** `codex_headers` synth (`polyflare-codex/src/codex_headers.rs`), `resolve_core_account`
  (`ingress.rs:326`, decrypt+refresh), the owner lookup (`continuity_repo.rs:64 get_anchor_owner` + the session-row
  owner in `continuity.rs`), `session_key::parse_inbound`, the reqwest client from `CodexExecutor`. The MISSING
  piece is only the parameterized-path unary forward (`executor.rs:150` hardcodes `/responses`).
- These do NOT touch the `/responses` continuity/streaming/wedge path. The 5 wedge + cyber + failover + starvation
  suites MUST stay green.
- Verify each task: `cargo test --workspace` green + `cargo clippy --workspace --all-targets -- -D warnings`.

---

### Task 1: The generic parameterized-path unary forward primitive

**Files:** a new `crates/polyflare-server/src/control.rs` (or extend `polyflare-codex` with a control-forward fn —
decide: it needs `Account` + reqwest; the executor crate is the natural home for the HTTP, but the account
selection/log live in `polyflare-server`. Recommend the HTTP forward fn in `polyflare-codex` [mirrors
`CodexExecutor`], the orchestration in `polyflare-server`); tests + a mock control endpoint in `polyflare-testkit`.

**Interfaces — Produces:** a fn that, given a materialized `Account`, a `path`, `method`, forwarded client
headers, and an optional body, does a UNARY forward:
- URL = `{account.base_url-trimmed}/codex/<path>` — EXCEPT `<path>` starting `wham/` ⇒ `{base}/<path>` (base
  already ends `/backend-api`; confirm PolyFlare's `base_url` shape vs codex-lb's — check what `account.base_url`
  is and whether it includes `/backend-api/codex`; adapt the join so the final URL matches codex-lb's
  `{upstream_base}/codex/<path>`).
- Headers: reuse `codex_headers` synth + override `Authorization: Bearer {account.bearer_token}` +
  `chatgpt-account-id` (same rules as `CodexExecutor::execute` `executor.rs:167-181`); forward the client's
  control headers per the "dumb executor" doctrine; conditional `content-type` passthrough.
- Send (reqwest, the same client construction as `CodexExecutor`), read the FULL response (unary), and return
  status + BODY BYTES + the response headers FILTERED to the allow-set `{cache-control, content-type, etag,
  last-modified, location, openai-processing-ms, request-id, x-request-id}`.
- On transport error ⇒ a typed error the caller maps to 502.

- [ ] **Step 1:** Add a mock control endpoint to `polyflare-testkit` (an axum route that records the request path/
      headers/body and returns a scripted status+body+headers) — mirror `MockUpstream`'s idiom. Failing test:
      the forward fn sends a POST to `{base}/codex/memories/trace_summarize` with the account bearer + synthesized
      identity headers, and returns the mock's status+body; the response headers are FILTERED (a non-allow-listed
      header the mock sends is dropped). A `wham/…` path ⇒ `{base}/wham/…` (no `/codex/`).
- [ ] **Step 2:** Run — fail. **Step 3:** Implement the forward fn (reuse the executor's client + bearer rules).
- [ ] **Step 4:** Green. Content-safety: assert the fn itself never logs the body.
- [ ] **Step 5:** Commit: `feat(codex): generic parameterized-path unary control forward`

---

### Task 2: Soft session→owner affinity resolution

**Files:** `crates/polyflare-server/src/control.rs` (the orchestration side) — an account-resolution fn for a
control request; tests.

**Interfaces — Produces:** `resolve_control_account(state, headers) -> Result<(Account, AccountId), Response>`:
1. Derive the session key from headers (`session_key::parse_inbound` — the SAME derivation `/responses` uses;
   read how it reads `x-codex-turn-state`/session headers, `session_key.rs:125-177`).
2. If a session key is present AND `continuity` resolves an OWNER for it (`get_anchor_owner` / the session-row
   `owning_account_id`) AND that owner is currently ELIGIBLE ⇒ use the owner (soft affinity).
3. OTHERWISE (no session header, no owner, or owner ineligible) ⇒ select ANY eligible account via the normal
   selector path (snapshots + overlay + `selector.pick`) — the SAME machinery `/responses` selection uses.
4. `resolve_core_account` the chosen id (decrypt + refresh). Return the materialized account.
- **The inviolable:** step 3 must ALWAYS have a fallback — a no-header or owner-unavailable control call is NEVER
  stranded; it falls to normal selection. No-eligible-account ⇒ a clean 503 (like `/responses`).

- [ ] **Step 1:** Failing tests: (a) a control request WITH a session header whose session has a known eligible
      owner ⇒ resolves to that OWNER account. (b) a control request with NO session header ⇒ resolves to a
      selected (any-eligible) account, does NOT fail. (c) a session header whose owner is INELIGIBLE
      (cooldown/benched) ⇒ falls back to another eligible account (NOT stranded). (d) no eligible account at all ⇒
      503.
- [ ] **Step 2:** Run — fail. **Step 3:** Implement (reuse owner lookup + the selector). **Step 4:** Green; wedge
      suites green (owner lookup is read-only).
- [ ] **Step 5:** Commit: `feat(server): soft session-owner affinity for control requests`

---

### Task 3: Wire the endpoints + content-free log + e2e

**Files:** `crates/polyflare-server/src/control.rs` (the handlers), `app.rs` (register on the `proxy` sub-router),
`request_log` write (content-free); tests + e2e.

**Handlers** (each = `resolve_control_account` → `codex_control_forward` → return the filtered unary response;
then a content-free `request_log` row `request_kind="codex_control_<path>"` + account + status, NEVER the body):
- `POST /thread/goal/set`, `POST /thread/goal/clear`, `GET /thread/goal/get` (or match codex-lb's methods — the
  scoping said get=GET, set/clear=POST; confirm and mirror). Treat as generic forwards (pass the body through —
  PolyFlare need not parse the goal, unlike codex-lb's payload rebuild).
- `GET /agent-identities/jwks` + `GET /wham/agent-identities/jwks`.
- `POST /memories/trace_summarize`.
All on the `proxy` sub-router (inherit `require_client_key`). Verify the `{pool}` param doesn't shadow these
static paths (matchit prefers static — `app.rs:216`).

- [ ] **Step 1:** Failing e2e through the real `build_app` + the testkit mock control endpoint: a `POST
      /memories/trace_summarize` with a valid client key (D18 enforced) + a session header ⇒ forwarded to the mock
      on the owner account, the mock's response returned (status+filtered headers+body), AND a content-free
      request_log row written (assert `request_kind` + account present, and assert the request BODY does NOT
      appear in the row — a sentinel-body test). A keyless request (D18 enforced) ⇒ 401 (inherits the gate). A
      `GET /agent-identities/jwks` ⇒ forwarded + returned. `/thread/goal/set` ⇒ forwarded.
- [ ] **Step 2:** Run — fail. **Step 3:** Implement the handlers + routing + the content-free log write. **Step 4:**
      Green; all suites green; the routes don't shadow `/{pool}/responses` or `/responses`.
- [ ] **Step 5:** Commit: `feat(server): D17 control endpoints (thread/goal, jwks, memories) on the gated proxy`

---

## Suggested order

1 (the forward primitive) → 2 (affinity resolution) → 3 (wire + log + e2e). After Task 3, minimal D17 is done:
the 3 real endpoints work, content-free, behind the D18 gate, with soft owner affinity. The deferred endpoints
(realtime/calls, analytics-events, alpha/search, safety/arc, images, opportunistic/admission) become trivial
route registrations on the primitive — build them if/when a real need appears. Update `PORTING-CODEXLB.md` D17
with the done set + the deferred list.
