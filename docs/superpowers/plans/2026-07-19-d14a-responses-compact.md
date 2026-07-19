# D14a — `/responses/compact` unary passthrough Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop PolyFlare 404ing the `POST /responses/compact` endpoint that the real Codex CLI emits (`codex-rs/core/src/client.rs:159`, `compact_conversation_history`) — proxy it upstream as a UNARY forward that lands on the conversation's owner account (warm prompt cache), content-free-logged and client-key-gated, exactly like the D17 control endpoints.

**Architecture:** Compact is a unary (non-streaming) POST whose body is `/responses`-shaped (`model`/`input`/`instructions`/`tools`/`reasoning`/`prompt_cache_key`, `store:false`) and whose response is unary JSON (a compacted transcript). It therefore sidesteps the SSE relay, `ObservingStream`, continuity, and the wedge entirely. It reuses the D17 machinery: the `polyflare_codex::control_forward` unary primitive (whose `control_url` produces the correct `{base}/responses/compact` for path `"responses/compact"`) and D17's soft session→owner affinity (`resolve_control_account`), generalized to take a body-derived `session_key` + a `pool`. The dedupe engine half of D14 is deliberately DEFERRED (see "Scope: dedupe deferred" below).

**Tech Stack:** Rust, axum 0.8, `reqwest` (unary), the existing `polyflare_codex::control_forward` + `crate::session_key::parse_inbound` + `crate::control` D17 code.

## Scope: dedupe DEFERRED (documented, not built)

D14's tool-call dedupe engine is a **B10-style reframe: N/A on PolyFlare's architecture today, so deferred.** codex-lb dedupes duplicate side-effect tool calls because its persistent WS bridge replays accumulated history and can double-send. PolyFlare's `/responses`+`/v1/messages` are stateless single HTTP responses where the CLIENT owns the input and full-resend replays the client's own (anchor-stripped) body — PolyFlare never fabricates a duplicate destructive call. A well-behaved upstream emits each `response.output_item.done` once per response. The genuine analog (an upstream re-emitting a side-effect call across turns on one socket) only exists on the WS bridge, whose client-facing RAM-accumulation half is explicitly M5b. Building a frame-DROPPING stream layer now would (a) solve a non-existent problem and (b) be wedge-adjacent (dropping frames is exactly what `ObservingStream::poll_next` must never do). If ever built, it belongs OUTSIDE `ObservingStream` (mirroring `wrap_translating_stream`), hash-based (`sha256`-digest LRU like `ws/delta.rs`'s `ItemHash`, never retaining arg strings), in the WS layer — a future M5b follow-up. **This plan builds compact only.**

## Global Constraints

- **Wedge sacred.** Compact is UNARY — it must NOT touch `ObservingStream::poll_next`, continuity's `prepare`/`observe`, or the SSE/streaming path. It is a plain HTTP round-trip via `control_forward`. The owner lookup is a READ-ONLY `ContinuityRepo::get_session` (never `Continuity::prepare`, which would mutate the session row) — exactly what `resolve_control_account` already does.
- **Content-free forever.** The request/response BODY flows through as opaque `Bytes` — forwarded verbatim, never logged/persisted. The `request_log` row is content-free: status/account/latency + the `model` string (a model name, already logged for `/responses`, NOT conversation content) + a fixed `path` label. `prompt_cache_key` is used ONLY as an input to the sha256 `session_key` derivation (never logged raw). NEVER log the body, tokens, or emails.
- **Owner affinity is SOFT (never over-binds).** Bind to the conversation's owner account ONLY when a session key resolves an owner AND that owner is currently eligible; otherwise fall through to normal any-eligible selection. A compact with no/unavailable owner is NEVER stranded — mirrors `resolve_control_account`'s Step 3 exactly, and is distinct from `/responses`'s hard `previous_response_id` anchor (compact carries no anchor; `store:false`).
- **Client-key gated.** Compact is a proxy surface → it registers on the D18-gated `proxy` sub-router (`app.rs`), inheriting `require_client_key` exactly like `/responses`.
- **Zero D17/`/responses` behavior change.** Generalizing `resolve_control_account` must leave its control-endpoint behavior byte-identical (control callers pass the header-only key + `pool=None`). Existing D17 + `/responses` tests are the guard.
- **Workspace stays fmt-clean + clippy clean UNDER `-D warnings`** (CI). Avoid raw multi-element tuples in any `query_as`; no `partial_cmp().unwrap()`; keep the existing `#[allow(clippy::too_many_arguments)]` where present.
- **Never log tokens/bearers.**

---

## File Structure

- **Modify** `crates/polyflare-server/src/control.rs` — extract `resolve_control_account`'s soft-affinity core into `resolve_owner_affine_account(state, session_key, pool)` (T1); add `compact_route` glue + `compact_handler`/`pooled_compact_handler` (T2).
- **Modify** `crates/polyflare-server/src/app.rs` — register `POST /responses/compact` + `POST /{pool}/responses/compact` on the D18-gated `proxy` sub-router (T2).
- **Test** `crates/polyflare-server/tests/compact_e2e.rs` — new e2e (T2), modeled on `crates/polyflare-server/tests/control_endpoints_e2e.rs`.

---

## Task 1: Generalize the soft-affinity resolver (session_key + pool params)

**Files:**
- Modify: `crates/polyflare-server/src/control.rs` (`resolve_control_account` ~line 68)
- Test: inline `#[cfg(test)]` in `control.rs` if a test module exists there, else assert via the existing D17 `control_endpoints_e2e.rs` behavior + a new unit test near the fn.

**Interfaces:**
- Consumes: existing `header_session_key`, `filter_by_provider_and_pool`, `state.selector_for`, `ContinuityRepo::get_session`, `resolve_core_account`, `no_eligible`, `SelectionCtx`, `polyflare_core::SessionKey`.
- Produces:
  - `pub(crate) async fn resolve_owner_affine_account(state: &AppState, session_key: Option<&polyflare_core::SessionKey>, pool: Option<&str>) -> Result<(Account, AccountId), Response>` — the extracted soft-affinity core (snapshots → `filter_by_provider_and_pool(Codex, pool)` → overlay → owner lookup via `session_key` → soft-affinity pick → `resolve_core_account`).
  - `resolve_control_account(state, headers)` is REWRITTEN to a thin wrapper: `let sk = header_session_key(headers, None); resolve_owner_affine_account(state, sk.as_ref(), None).await` — behavior byte-identical to before.

- [ ] **Step 1: Capture the green baseline (existing D17 behavior is the guard)**

Run: `cargo test -p polyflare-server --test control_endpoints_e2e 2>&1 | tail -15`
Expected: all green (this proves T1's refactor is behavior-preserving for control endpoints).

- [ ] **Step 2: Write the failing new unit test**

Add near `resolve_control_account` in `control.rs` (adapt to the file's existing test scaffolding — if `control.rs` has no test module, put this in the e2e file instead and note it):

```rust
#[cfg(test)]
mod resolver_tests {
    use super::*;
    // Verify the extracted core honors a body-derived session key + a pool. Build a minimal
    // AppState with two seeded codex accounts (one pooled "p", one unpooled), a continuity session
    // owned by the pooled account keyed K, then assert resolve_owner_affine_account(Some(K), Some("p"))
    // returns the pooled owner, and resolve_owner_affine_account(None, None) returns *some* eligible
    // account (unowned any-pick). Reuse whatever in-memory AppState/store test builder the crate's
    // other server-side unit tests use (grep the tests/ dir + control.rs neighbors for the helper).
    // If no lightweight AppState builder exists, SKIP this unit test and rely on the T2 e2e's
    // owner-affinity assertion instead — say so in the report.
}
```

**Implementer note:** if `control.rs` cannot cheaply construct an `AppState` in a unit test (it may need the full `build_app` harness), do NOT force it — the T2 e2e (`compact_lands_on_the_conversation_owner_account`) is the real owner-affinity guard. In that case, T1's test is just: re-run the D17 e2e (Step 1) after the refactor and confirm still-green. Report which path you took.

- [ ] **Step 3: Extract the core + rewrite the wrapper**

Refactor `resolve_control_account` (`control.rs:68-136`): move its body (from `let now = unix_now();` through `Ok((account, picked))`) into a new `resolve_owner_affine_account(state, session_key: Option<&polyflare_core::SessionKey>, pool: Option<&str>)`, replacing:
- `filter_by_provider_and_pool(&snapshots, Provider::Codex, None)` → `filter_by_provider_and_pool(&snapshots, Provider::Codex, pool)`
- `state.selector_for(None)` → `state.selector_for(pool)`
- the `let session_key = header_session_key(headers, None);` line → DELETE it (the key is now the passed `session_key` param); use `session_key` directly in the `match session_key.as_ref()` owner lookup (adjust: the param is already `Option<&SessionKey>`, so `match session_key { Some(sk) => ... }`).

Then rewrite `resolve_control_account`:
```rust
pub async fn resolve_control_account(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<(Account, AccountId), Response> {
    // Control endpoints have no body ⇒ header-only session key, and are never pool-scoped.
    let session_key = header_session_key(headers, None);
    resolve_owner_affine_account(state, session_key.as_ref(), None).await
}
```

- [ ] **Step 4: Verify no D17 regression + new test**

Run: `cargo test -p polyflare-server --test control_endpoints_e2e 2>&1 | tail -15` (identical green to Step 1) and the new unit test if written. Then `cargo clippy -p polyflare-server --all-targets -- -D warnings` clean + `cargo fmt --all`.

- [ ] **Step 5: Commit**

```bash
git add crates/polyflare-server/src/control.rs
git commit -m "refactor(server): extract resolve_owner_affine_account (session_key + pool params) from resolve_control_account (D14a T1)"
```

---

## Task 2: `/responses/compact` handler + routes + e2e

**Files:**
- Modify: `crates/polyflare-server/src/control.rs` (add `compact_route` glue + `compact_handler`/`pooled_compact_handler`)
- Modify: `crates/polyflare-server/src/app.rs` (register the two routes on the `proxy` sub-router ~line 314)
- Test: create `crates/polyflare-server/tests/compact_e2e.rs`

**Interfaces:**
- Consumes: T1's `resolve_owner_affine_account`, `crate::session_key::parse_inbound` (`-> Option<InboundFacts { model, effort, ctx: RequestCtx { session_key, .. } }>`), `polyflare_codex::control_forward` + `control_response_from` + `forward_headers_from_inbound` (all existing in `control.rs`/`control_forward.rs`), `RequestLog`, `spawn_persist_request_log`, `state.control_client`, `state.upstream_request_metrics`.
- Produces:
  - `pub async fn compact_handler(State<Arc<AppState>>, HeaderMap, Bytes) -> Response` (unpooled)
  - `pub async fn pooled_compact_handler(State<Arc<AppState>>, Path<String>, HeaderMap, Bytes) -> Response` (pooled)
  - `async fn compact_route(state, pool: Option<String>, headers, body: Bytes) -> Response` (shared glue)

**Content-safety (inviolable):** the body flows opaque `Bytes` → `control_forward` → upstream, and is NEVER logged. The `request_log` row carries `model` (from `parse_inbound`) + the fixed label `"responses_compact"` + account/status/latency only. The e2e's sentinel test proves the body reaches the mock upstream but never the persisted row.

- [ ] **Step 1: Write the failing e2e**

Create `crates/polyflare-server/tests/compact_e2e.rs`, modeled on `crates/polyflare-server/tests/control_endpoints_e2e.rs` (read it first for the real `build_app` harness, the mock-upstream spawn, account seeding, admin/client-key helpers, and the sentinel-in-request_log assertion pattern — do NOT invent helper names). Cover:

1. **Forwarding works (404 gone):** seed a codex account, spawn a mock upstream serving `POST /responses/compact` returning a unary JSON body (e.g. `{"output":[...]}`) with 200. Drive `POST /responses/compact` through the real `build_app`. Assert 200 and the mock RECEIVED the request at path `/responses/compact` (i.e. `control_url` produced the right suffix), and the client got the mock's JSON body back.
2. **Content-safety (teeth):** the compact request body carries a sentinel (e.g. `"SENTINEL_COMPACT_BODY_4242"`). Assert the mock upstream RECEIVED the sentinel (forwarding genuinely works) BUT the persisted `request_log` row's `Debug`/fields never contain it, AND a seeded sentinel email/token never appears in the row.
3. **Owner affinity:** seed TWO codex accounts A and B; create a continuity session (via the store's `ContinuityRepo`) owned by A with key K; craft a compact body whose `prompt_cache_key` derives (through `parse_inbound`/`header_session_key`) to session key K (compute K in the test with the same derivation the code uses — `header_session_key(headers, Some(pck))` or `parse_inbound`); assert the served account (the `request_log` row's `account_id`, or the mock's received `chatgpt-account-id`/bearer identity) is A when A is eligible. If pinning the exact derived key is impractical, assert the WEAKER but still-real property: a compact whose session resolves to owner A lands on A (seed the session row keyed by the value `parse_inbound` actually produces for your test body — derive it in the test, then seed with that value).
4. **D18 gate:** with client-key enforcement on, a keyless `POST /responses/compact` ⇒ 401 (inherits the proxy gate); with a valid key ⇒ 200.
5. **(If cheap) pool scoping:** `POST /{pool}/responses/compact` selects only that pool's accounts.

Write these fully against the real harness.

- [ ] **Step 2: Run to verify fail**

Run: `cargo test -p polyflare-server --test compact_e2e 2>&1 | tail -25`
Expected: FAIL — `POST /responses/compact` 404s (route not registered).

- [ ] **Step 3: Implement `compact_route` + handlers**

Add to `control.rs` (mirroring `control_route`, but parse the body for the session key + model):

```rust
/// D14a: the `/responses/compact` glue. UNARY, like `control_route`, but compact carries a
/// `/responses`-shaped BODY, so it derives the owner-affinity session key + the (content-free)
/// `model` from that body via `parse_inbound` — then forwards the SAME bytes verbatim to
/// `{base}/responses/compact` (`control_forward` with path `"responses/compact"`). Sidesteps the
/// SSE relay / `ObservingStream` / continuity entirely (unary round-trip).
async fn compact_route(
    state: Arc<AppState>,
    pool: Option<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let start = Instant::now();
    let log_repo = state.store.request_log();
    let log_bus = state.log_bus.clone();

    // Shallow parse: derive the session key (for soft owner affinity) + the content-free model.
    // None ⇒ malformed body ⇒ 400 (mirrors `/responses`'s malformed-body behavior); still log it.
    let facts = crate::session_key::parse_inbound(&headers, &body);
    let (session_key, model) = match &facts {
        Some(f) => (f.ctx.session_key.clone(), Some(f.model.clone())),
        None => (None, None),
    };

    let forward_headers = forward_headers_from_inbound(&headers);

    let (response, account_id) = if facts.is_none() {
        // Malformed compact body — do not forward garbage upstream.
        (
            (StatusCode::BAD_REQUEST, "malformed compact body").into_response(),
            None,
        )
    } else {
        match resolve_owner_affine_account(&state, session_key.as_ref(), pool.as_deref()).await {
            Err(resp) => (resp, None),
            Ok((account, account_id)) => {
                let outcome = polyflare_codex::control_forward(
                    &state.control_client,
                    &account,
                    "responses/compact",
                    Method::POST,
                    &forward_headers,
                    Some(body),
                )
                .await;
                let resp = match outcome {
                    Ok(cr) => control_response_from(cr),
                    Err(_e) => {
                        (StatusCode::BAD_GATEWAY, "compact upstream forward failed").into_response()
                    }
                };
                (resp, Some(account_id))
            }
        }
    };

    let log = RequestLog {
        method: "POST",
        path: "responses_compact",
        provider: Provider::Codex,
        aliased: false,
        status: response.status(),
        duration_ms: start.elapsed().as_millis() as u64,
        account_id: account_id.map(|id| id.to_string()),
        model,
        reasoning_effort: None,
        service_tier: None,
        transport: Some("http".to_string()),
        ttft_ms: None,
        total_tokens: None,
        cached_tokens: None,
    };
    log.emit();
    log_bus.publish(log.to_log_event());
    state
        .upstream_request_metrics
        .record(log.account_id.as_deref(), log.status.as_u16());
    spawn_persist_request_log(log_repo, log.record(unix_now()));

    response
}

/// `POST /responses/compact` — unpooled.
pub async fn compact_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    compact_route(state, None, headers, body).await
}

/// `POST /{pool}/responses/compact` — pool-scoped.
pub async fn pooled_compact_handler(
    State(state): State<Arc<AppState>>,
    Path(pool): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    compact_route(state, Some(pool), headers, body).await
}
```

**Implementer notes:**
- Add whatever `use` items are missing (`axum::extract::Path`, `bytes::Bytes`, `Method`, `Instant`, `RequestLog`, `spawn_persist_request_log`, `forward_headers_from_inbound`, `control_response_from`, `parse_inbound`) — most are already imported in `control.rs` for `control_route`; verify.
- `parse_inbound`'s `InboundFacts.ctx.session_key` is `Option<SessionKey>` already derived from the body's `prompt_cache_key` + headers (the SAME Hard derivation `/responses` uses) — so a compact resolves the SAME continuity owner row `/responses` would. `.clone()` it (SessionKey is `Clone`).
- Confirm `RequestLog.path` is a `&'static str` — `"responses_compact"` is a fixed label (the row's content-free "kind" discriminator, like `control_route`'s `"codex_control_<path>"`). If the label must be dynamic per pool, keep it `"responses_compact"` regardless of pool (pool is already captured in routing; the label is the kind).

- [ ] **Step 4: Register the routes**

In `app.rs`, on the `proxy` sub-router (~line 314, alongside `/responses`/`/{pool}/responses`), add:
```rust
        .route("/responses/compact", post(crate::control::compact_handler))
        .route("/{pool}/responses/compact", post(crate::control::pooled_compact_handler))
```
Place them so matchit resolves cleanly: `/responses/compact` (static seg 1 = `responses`) never collides with `/{pool}/responses` (param seg 1) — matchit prefers the static segment, and seg 2 (`compact` vs `responses`) differs anyway. Verify the routes compile + the e2e's keyless-401 (D18) still holds (they're POST on the gated sub-router).

- [ ] **Step 5: Run to verify pass**

Run: `cargo test -p polyflare-server --test compact_e2e 2>&1 | tail -25`
Expected: PASS. Then no-regression: `cargo test -p polyflare-server 2>&1 | tail -15`, `cargo clippy -p polyflare-server --all-targets -- -D warnings` clean, `cargo fmt --all`.

- [ ] **Step 6: Commit**

```bash
git add crates/polyflare-server/src/control.rs crates/polyflare-server/src/app.rs crates/polyflare-server/tests/compact_e2e.rs
git commit -m "feat(server): POST /responses/compact unary passthrough with soft owner affinity, content-safe, D18-gated (D14a T2)"
```

---

## Self-Review

**1. Spec coverage:**
- Stop 404ing `/responses/compact` (real CLI endpoint, `client.rs:159`) → **T2** ✓ (routes + handler).
- Unary passthrough, no SSE/wedge → **T2** ✓ (reuses `control_forward`; no `ObservingStream`).
- Owner affinity (warm prompt cache) → **T1** (resolver) + **T2** (body-derived `session_key` via `parse_inbound`) ✓.
- Content-free (opaque body, `model`-only log) → **T2** ✓ + e2e sentinel guard.
- Client-key gated → **T2** ✓ (proxy sub-router).
- Pool scoping → **T2** ✓ (`/{pool}/responses/compact`).
- Dedupe engine → DEFERRED, documented (Scope section) — a B10-style N/A reframe.

**2. Placeholder scan:** the e2e steps direct the implementer to read `control_endpoints_e2e.rs` for the real harness (named file) rather than invent helpers; T1's unit test has an explicit "skip-if-no-cheap-AppState, the T2 e2e is the real guard" fallback. No TBD/vague steps.

**3. Type consistency:** `resolve_owner_affine_account(state, Option<&SessionKey>, Option<&str>)` (T1) is consumed by `compact_route` (T2) and by the rewritten `resolve_control_account` (T1). `parse_inbound -> Option<InboundFacts>` with `ctx.session_key: Option<SessionKey>` + `model: String` (verified `session_key.rs:58`, `types.rs:127`). `control_forward(client, &Account, &str, Method, &[(String,String)], Option<Bytes>) -> Result<ControlResponse, _>` + `control_response_from` (verified `control_forward.rs`, `control.rs:158`). Route handler signatures match axum extractors.

**Adversarial-review crux (flag for reviewers):**
- **T1 no-D17-regression** — `resolve_control_account`'s control behavior must be byte-identical after the extraction (control passes header-only key + `pool=None`); the D17 `control_endpoints_e2e` is the guard.
- **T2 content-safety** — the sentinel body must reach the mock upstream (forwarding works) but never the persisted `request_log` row; `prompt_cache_key` never logged raw (only hashed into `session_key`).
- **T2 wedge/unary** — no `ObservingStream`/continuity `prepare`/SSE touched; the owner lookup is the read-only `get_session`, not `prepare`.
- **T2 owner affinity soft** — a compact with no/ineligible owner falls through to any-eligible, never stranded (inherited from `resolve_owner_affine_account`).
