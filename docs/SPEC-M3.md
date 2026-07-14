# SPEC-M3 — The Continuity Engine (the "wedge fix")

**Status:** Design spec for review. No code. A TDD plan (`PLAN-M3.md`) is built from this.
**Milestone:** M3 — the crown-jewel milestone; the whole reason PolyFlare exists.
**Builds on:** M1 (stateless HTTP-SSE pass-through executor + mock upstream + e2e) and M2 (SQLite store, `capacity_weighted` selector, OAuth refresh, pool wiring).
**Sources of truth:** `docs/reference/codex-lb-continuity-reference.md` (the wedge), `docs/DESIGN-DECISIONS.md` §C1/C1(a)/C1(b)/S3/M2-GATE1, `docs/POLYFLARE-DESIGN.md` §4.1.

---

## 1. Goal

Make conversation continuity an **explicit, persisted, observable state machine with a proactive watchdog**, so that a `store:false` ephemeral anchor that fails to resume is *always detected and recovered* instead of silently hanging (the "wedge"). Concretely, M3 delivers, over PolyFlare's HTTP-SSE architecture:

1. **Continuity-ownership routing** — an anchored conversation returns to the account that created its anchor (a HARD pre-filter above the M2 selector), or safely Recovers. This is the foundational piece PolyFlare lacks today (M2 re-selects per request → instant cross-account wedge).
2. **R1 — never trim a `store:false` full-resend** to a bookkeeping-derived anchor; PolyFlare does not inject anchors either. The always-safe full pass-through stays the default.
3. **R2 — a proactive silence watchdog** on any request carrying an anchor: no first upstream event within `N` seconds → cancel the dead attempt and recover (resend full without the anchor, or signal the client to resend).
4. **R3 — a reasoning-replay cache** so chain-of-thought survives a recover (follow-up).
5. **The wedge-regression e2e** that reproduces silence-after-accept and proves recovery — the guardrail that makes the fix *stick*.

**Non-goals (M3):** proactive anchor injection + token-saving trim (deferred; see Q2), WS transport (M5), the dashboard, Anthropic continuity (`polyflare-anthropic` is a no-op `Continuity`).

---

## 2. Problem recap (the wedge, three sentences)

`store:false` is the operative reality (the real Codex CLI sends it; PolyFlare passes it through), which makes every `previous_response_id` an **ephemeral anchor** — resolvable only while the exact upstream turn-state that produced it is still alive on the account that created it, never durably. When a request carries a dead anchor (wrong account after per-request re-selection, or an expired ephemeral window), the `store:false` upstream **silently accepts the request and never emits `response.created` or any error** — so nothing reactive fires and the client hangs indefinitely. codex-lb worsens this by *trimming a client full-resend down to that dead anchor*, converting a self-healing retry into a guaranteed hang; PolyFlare's fix is to never trim/inject (R1), route anchored turns back to their owner, and put a bounded silence-watchdog on every anchored request (R2) so the invisible stall becomes a bounded, recoverable event.

---

## 3. Architecture

### 3.1 The state machine

Per-conversation, keyed by a derived **session key**, persisted in `polyflare-store`.

```
                         request arrives
                               │
                 derive session_key + look up state
                 (also map any client previous_response_id → owner)
                               │
             ┌─────────────────┴───────────────────┐
        no known owner/anchor              known owner + anchor
             │                                      │
             ▼                                      ▼
          ┌──────┐  observe: 1st response.id     ┌──────────┐
          │Fresh │ ───────────────────────────►  │ Anchored │◄──────────────┐
          └──────┘                               └────┬─────┘                │
             │  (client sent an anchor we can't       │                      │
             │   verify → arm watchdog defensively)   │ next turn carries an │
             │                                        │ anchor (client's, or │
             │                                        │ references our last) │
             │                                        ▼                      │
             │                                 ┌──────────────┐              │
             │             prepare pins to owner│ Reattaching │              │
             │             + ARMS watchdog on   │ (in-flight, │              │
             │             the anchored request │  watchdog    │              │
             │                                  │  running)    │              │
             │                                  └──────┬───────┘             │
             │                                         │                     │
             │                     ┌───────────────────┴──────────────┐      │
             │           first upstream event                 SILENCE  │      │
             │           within N (anchor live)          (no event <N) │      │
             │                     │                                   ▼      │
             │                     │                            ┌─────────────┐│
             │                     └── observe: response.id ──► │  Recover    ││
             │                        (update anchor) ─────────►│ cancel dead ││
             │                                                  │ attempt;    ││
             └── observe: response.id ─────────────────────────►│ strip anchor;│
                                                                │ resend FULL  │
                                                                │ (Strategy A) │
                                                                │  OR signal   │
                                                                │  client      │
                                                                │ (Strategy B) │
                                                                └──────┬───────┘
                                                            success: re-anchored
                                                                     └──────────┘
```

**Owner unavailable at prepare time** (rate-limited / paused / reauth): do **not** hard-fail — this is a Recover edge. If the request is a full-resend → cross-account Recover (strip anchor, resend full on a freshly-selected account, re-home ownership). If the request is a bare tail → Strategy B (signal the client to resend full). Rationale: a full-resend is always safe to run anywhere; hard-failing an anchored conversation because its owner is briefly busy is worse than the wedge we're killing.

### 3.2 Where each piece slots into the existing code

```
axum ingress (polyflare-server/src/ingress.rs)  ── the orchestration changes here ──
  1. decode body → derive session_key + client_previous_response_id + is_full_resend  (NEW: header+body derivation)
  2. Prepared{ req, directive } = continuity.prepare(req, &ctx).await?               (NEW: async Continuity)
  3. candidates = assemble_snapshots(&store)                                          (M2, unchanged; O(N) collapse is a follow-up)
  4. account_id = apply_ownership(&directive, &candidates, &selector, &ctx)           (NEW: pin-or-recover pre-filter)
  5. load + refresh-if-stale + decrypt account                                        (M2, unchanged; singleflight is a follow-up)
  6. stream = execute_with_watchdog(&executor, &continuity, prepared, &account, &directive).await?   (NEW: R2 wrapper)
  7. build text/event-stream response  (Body::from_stream)                            (M1, unchanged)
  8. continuity.observe(outcome, &ctx) fires when the sniffing stream ends            (NEW: state advance)

polyflare-core/src/continuity.rs (NEW module)  — the `Continuity` trait + Codex state machine + types.
polyflare-core/src/traits.rs                   — reshape `Continuity` (async + Result + directive/observe).
polyflare-core/src/types.rs                    — enrich RequestCtx; add SessionKey, ContinuityDirective, TurnOutcome.
polyflare-store/migrations/0002_continuity.sql — continuity_sessions + continuity_anchors (+ reasoning_cache in R3).
polyflare-store/src/continuity_repo.rs (NEW)   — ContinuityRepo over the pool.
polyflare-codex/src/executor.rs                — unchanged transport; the watchdog wraps it, not inside it.
polyflare-testkit/src/lib.rs                   — add MockUpstream "silent-on-anchor" mode + response.id emission.
```

`CodexExecutor::execute` stays a stateless pass-through. The watchdog is a **wrapper around** it (in the server, or a `polyflare-core` helper that takes `&dyn Executor`), so continuity logic never leaks into transport (E4).

### 3.3 The reshaped `Continuity` trait (Q5)

```rust
// polyflare-core/src/traits.rs
#[async_trait]
pub trait Continuity: Send + Sync {
    /// Request-time. Resolve session + ownership, decide routing + watchdog, (R3) re-inject
    /// cached reasoning on a recover. Reads/writes persisted session state; may fail.
    async fn prepare(
        &self,
        req: PreparedRequest,
        ctx: &RequestCtx,
    ) -> Result<Prepared, ContinuityError>;

    /// Response-outcome. Advance the state machine from how the turn resolved.
    async fn observe(
        &self,
        outcome: TurnOutcome,
        ctx: &RequestCtx,
    ) -> Result<(), ContinuityError>;
}
```

```rust
// polyflare-core/src/types.rs (additions)

/// Enriched per-request context. `session_key` + `client_previous_response_id` + `is_full_resend`
/// are derived at ingress from headers + body BEFORE prepare (prepare stays store-focused, not
/// header-parsing). `session_id` (M1) is retained.
pub struct RequestCtx {
    pub session_id: Option<String>,
    pub session_key: Option<SessionKey>,
    pub client_previous_response_id: Option<String>,
    pub is_full_resend: bool,
}

/// A derived conversation key + its strength (hard binds routing; soft is best-effort).
pub struct SessionKey { pub value: String, pub strength: KeyStrength }
pub enum KeyStrength { Hard, Soft }

/// Output of prepare: the (possibly-rewritten) request + how to route & guard it.
pub struct Prepared {
    pub req: PreparedRequest,
    pub directive: ContinuityDirective,
}

pub struct ContinuityDirective {
    /// HARD routing pre-filter. Some ⇒ the request MUST route to this account (or Recover).
    pub pin_account: Option<AccountId>,
    /// Arm the silence watchdog when the outgoing request carries an unguaranteed anchor.
    pub watchdog: WatchdogArm,      // Disarmed | Armed { timeout: Duration }
    /// What to do if the watchdog fires.
    pub recovery: RecoveryPlan,
    /// Threaded back to observe so it knows which session/turn this was.
    pub session_key: Option<SessionKey>,
}

pub enum RecoveryPlan {
    /// The outgoing input is self-sufficient (a full-resend, per the is_full_resend heuristic):
    /// on silence, re-execute this anchorless request (+ replayed reasoning in R3) on the SAME account.
    ResendFull { anchorless_req: PreparedRequest },
    /// The outgoing input is a bare tail (client-trimmed) with no cached history to reconstruct:
    /// on silence, surface `previous_response_not_found` so the client self-heals with a full resend.
    SignalClient,
    /// No anchor present ⇒ nothing to recover.
    None,
}

/// What observe consumes (built by the watchdog wrapper as the stream resolves).
pub enum TurnOutcome {
    /// Upstream produced its first event and we relayed it. `response_id` is sniffed from the
    /// streamed `response.created`/`response.completed`. `reasoning` is None until R3.
    Completed {
        session_key: Option<SessionKey>,
        account: AccountId,
        response_id: Option<String>,
        input_fingerprint: String,
        input_count: u32,
        reasoning: Option<ReasoningItems>,
    },
    /// Watchdog fired; we recovered (Strategy A) or signaled the client (Strategy B).
    Recovered { session_key: Option<SessionKey>, account: AccountId, new_response_id: Option<String> },
    /// A hard upstream error (not silence).
    Failed { session_key: Option<SessionKey> },
}

#[derive(Debug, thiserror::Error)]
pub enum ContinuityError {
    #[error("continuity store error")]           // generic Display — never leaks session content
    Store(#[source] Box<dyn std::error::Error + Send + Sync>),
}
```

The Anthropic backend gets a `NoopContinuity` whose `prepare` returns `Prepared { req, directive: Disarmed/None/no-pin }` and whose `observe` is a no-op — so the ingress path is uniform across backends.

### 3.4 Persistence schema (`0002_continuity.sql`, runtime-checked sqlx, forward-only)

```sql
-- Per-conversation state machine row.
CREATE TABLE IF NOT EXISTS continuity_sessions (
    session_key            TEXT    PRIMARY KEY,
    key_strength           TEXT    NOT NULL,              -- 'hard' | 'soft'
    owning_account_id      TEXT        REFERENCES accounts(id) ON DELETE SET NULL,
    anchor_response_id     TEXT,                          -- last response.id we saw complete (the anchor)
    last_input_fingerprint TEXT,                          -- sha256 of the last observed input prefix
    last_input_count       INTEGER,                       -- item count of the last observed input array
    reasoning_cache_ref    TEXT,                          -- key into reasoning_cache (R3); NULL until R3
    state                  TEXT    NOT NULL,              -- 'fresh'|'anchored'|'reattaching'|'recover'
    created_at             INTEGER NOT NULL,
    updated_at             INTEGER NOT NULL,
    last_activity_at       INTEGER NOT NULL               -- idle-TTL eviction driver
);
CREATE INDEX IF NOT EXISTS idx_continuity_sessions_activity
    ON continuity_sessions (last_activity_at);

-- response_id → owner map, so a CLIENT-supplied previous_response_id resolves to its account even
-- when the derived session_key differs (or is soft/absent). This is the ownership backbone.
CREATE TABLE IF NOT EXISTS continuity_anchors (
    response_id       TEXT    PRIMARY KEY,
    session_key       TEXT    NOT NULL REFERENCES continuity_sessions(session_key) ON DELETE CASCADE,
    owning_account_id TEXT    NOT NULL,
    created_at        INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_continuity_anchors_session
    ON continuity_anchors (session_key);

-- R3 (follow-up migration or same file, gated behind the R3 task). Reasoning content is sensitive:
-- store it encrypted (XChaCha via TokenCipher) OR at minimum never log it; redact in Debug.
CREATE TABLE IF NOT EXISTS reasoning_cache (
    id                 TEXT    PRIMARY KEY,               -- == reasoning_cache_ref
    session_key        TEXT    NOT NULL REFERENCES continuity_sessions(session_key) ON DELETE CASCADE,
    anchor_response_id TEXT    NOT NULL,
    reasoning_enc      BLOB    NOT NULL,                  -- encrypted JSON array of reasoning items
    created_at         INTEGER NOT NULL
);
```

`sqlx` note (per handoff conventions): runtime-checked `query`/`query_as::<_,T>`/`FromRow`, no compile-time macros, no `DATABASE_URL`; `"window"`-style keyword quoting where needed; forward-only.

### 3.5 Transitions: `prepare` vs `observe`

| Transition | Driven by | Reads | Writes |
|---|---|---|---|
| (new) → Fresh | `prepare` | session_key lookup (miss) | insert row `state=fresh` |
| Fresh → Anchored | `observe` (Completed w/ id) | — | `owning_account`, `anchor_response_id`, `state=anchored`, anchors row |
| Anchored → Reattaching | `prepare` (turn carries anchor) | session + anchors | `state=reattaching` (transient) |
| Reattaching → Anchored | `observe` (first event + Completed) | — | update `anchor_response_id`, `state=anchored` |
| Reattaching → Recover | watchdog silence → `observe` (Recovered) | — | `state=recover`→`anchored` on recovery success; clear dead anchor |
| Anchored → Recover (owner busy) | `prepare` | eligibility of owner | `state=recover`; re-home `owning_account` on cross-account resend |

`prepare` = request-time (routing + watchdog arming + reasoning re-inject). `observe` = response-outcome (records the anchor/owner, advances state). The **watchdog** is the mechanism that manufactures the `Recovered`/`Completed`/`Failed` outcome that `observe` persists.

---

## 4. Resolved decisions (Q1–Q8)

### Q1 — Continuity-ownership routing

**Decision.** A request is tied to an owning account by two independent lookups, resolved in `prepare`:
1. **Session key** — derived like codex-lb: `x-codex-turn-state` header ⇒ **hard**; else a session/`prompt_cache_key` header ⇒ **hard**; else a soft key from request-id/content hash ⇒ **soft**. Look up `continuity_sessions[session_key]` → `owning_account_id` + `anchor_response_id`.
2. **Client-supplied `previous_response_id`** — look it up in `continuity_anchors[response_id]` → `owning_account_id`. This is authoritative even when the session key is soft/absent, because the client got that id from a prior PolyFlare turn.

If either resolves an owner, `prepare` returns `pin_account = Some(owner)`.

**Placement (S3).** Ownership is a **hard pre-filter ABOVE availability scoring**, realized by **narrowing the candidate set before `Selector::pick`** — not by changing the `Selector` trait (M2-GATE1: only reshape a seam when its milestone builds on it; Continuity is M3's seam, Selector was M2's). Ingress `apply_ownership`:
```
if let Some(owner) = directive.pin_account {
    let narrowed: Vec<_> = candidates.iter().filter(|s| s.id == owner).cloned().collect();
    match selector.pick(&narrowed, &ctx) {            // reuses select.rs eligibility for free
        Some(id) => id,                                // owner eligible ⇒ pinned (scoring bypassed)
        None     => RECOVER,                           // owner ineligible ⇒ escape hatch, not a hard fail
    }
} else {
    selector.pick(&candidates, &ctx)                   // Fresh / unowned ⇒ normal scoring
}
```
Narrowing to `[owner]` then calling `pick` reuses the M2 eligibility filter (status/reset/cooldown/backoff) unchanged: if the owner is eligible the selector returns it; if not, `pick` returns `None`, which ingress reads as "owner unavailable → Recover."

**Owner unavailable → Recover, never error.** On the RECOVER branch: strip the anchor; if `is_full_resend` → resend full on a freshly-selected account (full pool) and **re-home** `owning_account` in `observe`; if a bare tail → Strategy B (signal client). *Rationale:* continuity ownership is a hard *routing* constraint only while the owner is usable; when it isn't, degrade to a correct full-resend rather than wedge or fail. This is exactly TA6's "pinned requests degrade only along the documented path," composed for free (S3 already makes pinned == continuity-owned win over TA6 retries).

**Trade-off.** A soft-keyed conversation with no client anchor cannot be pinned (it looks Fresh each turn); acceptable — soft keys are best-effort and the watchdog still guards any anchor. **Revisit if** soft-key mis-affiliation is observed harming continuity → strengthen the soft-key derivation.

### Q2 — Does M3 add anchor injection/trimming? **Decision: NO — defer proactive injection + trim; M3 = ownership + watchdog + client-anchor safety + reasoning cache.**

**Rationale (one line).** Under forced `store:false` there is **no safe trim to gain** (the anchor is never guaranteed live), and trimming a full-resend to a dead anchor is the *exact* mechanism that creates the wedge — so the MVP keeps the always-safe full pass-through (R1) and spends M3 on the correctness net, deferring token-optimization trim/inject to an opt-in follow-up gated on proven anchor durability.

Consequences:
- PolyFlare **does not inject** a `previous_response_id` the client didn't send, and **does not trim** the client's `input`. The codex-lb-style wedge (inject-then-trim a full-resend) is therefore *structurally impossible* in PolyFlare.
- The residual wedge vector is the **client-supplied anchor** (`previous_response_id` + tail) meeting the wrong/dead account. Ownership routing (Q1) fixes the common cross-account case; the watchdog (R2) fixes the dead-ephemeral-window case. **Both remain essential**, so M3 is still the crown jewel even without injection.
- The `allow_store_false_trim` per-account seam (C1(a)) stays **OFF** and unimplemented — a documented seam, not code, in M3.

**Revisit-trigger.** An account/provider is *proven* to durably resolve `store:false` anchors (or a `store:true`-keeping client appears). Then add per-account injection+trim **under the watchdog's protection** (the watchdog already makes an injected anchor safe: silence → strip → full-resend), reclaiming the token savings without reintroducing the wedge.

### Q3 — The state machine + schema

Defined in §3.1 (states/transitions), §3.4 (schema), §3.5 (prepare-vs-observe). Key points:
- States: `Fresh → Anchored → Reattaching → {Anchored | Recover}`. `Reattaching` is the transient "anchored request in flight, watchdog armed" state; `Recover` is the cancel-and-resend escape.
- Persisted per-conversation: `session_key, key_strength, owning_account_id, anchor_response_id, last_input_fingerprint, last_input_count, reasoning_cache_ref, state, timestamps` + the `continuity_anchors` response_id→owner map.
- `prepare` drives request-time transitions (route + arm); `observe` drives outcome transitions (record anchor/owner, advance). The `last_input_fingerprint/count` are recorded by `observe` and used only for diagnostics/idempotency in M3-core (NOT to gate a trim — we don't trim).

### Q4 — R2 watchdog mechanism (the hard part)

**Detection.** `reqwest`'s `send().await` resolves on **response headers** (200 OK), not on the first body byte — so silence-after-accept is invisible at `send()` and only visible at the **stream** level. Therefore the watchdog races the **first stream item**:

```
let mut stream = executor.execute(req_with_anchor, account).await?;   // 200 headers back
match tokio::time::timeout(N, stream.next()).await {
    Ok(Some(Ok(first_bytes))) => {            // ALIVE: anchor resolved (or non-anchored stream)
        // Rebuild the full stream = once(first_bytes).chain(rest); wrap in the sniffing adapter; relay.
    }
    Ok(Some(Err(e)))          => { /* Failed: hard upstream error → 502, observe(Failed) */ }
    Ok(None)                  => { /* upstream closed w/ no bytes → treat as Failed/Recover */ }
    Err(_elapsed)             => { /* SILENCE: the wedge → RECOVER */ }
}
```

**Cancel.** On timeout, **drop `stream`**. Dropping the `reqwest` response stream aborts the upstream request and closes the connection — the cancel is implicit and cancel-safe (no partial state escapes).

**Recover.** Per `directive.recovery`:
- `ResendFull { anchorless_req }` — re-execute `anchorless_req` (the same `input`, anchor stripped, + replayed reasoning in R3) on the **same account**. A request with **no anchor cannot be silent** (a fresh turn always gets `response.created`), so the recovery attempt does not need the anchor-watchdog; it still carries a *generous* idle/first-token timeout (the M2-review follow-up) so a pathological upstream still yields a bounded 504 rather than a hang. Relay the recovery stream to the client.
- `SignalClient` — emit a synthetic `previous_response_not_found` error event to the client (the signal it already self-heals from) and stop. Used when the outgoing input is a bare tail with no cached history to resend.

**The safety boundary (critical).** The watchdog may restart **only before any byte has reached the client.** Because we peek the first item *before* writing the response body (step 6 precedes step 7), this invariant holds by construction. After the first client byte we are committed: a mid-stream stall is handled by a weaker net (a bounded idle-timeout that errors the stream — the M2-review follow-up), never a restart. This split is deliberate: the wedge is defined by silence *at the start*, which is exactly the region where restart is safe.

**Not double-charging the client.** The client issues one request and receives exactly one response body (either the alive stream or the recovery stream). The canceled silent attempt produced zero output events (it never emitted `response.created`), so upstream token cost is ~0 and the client is never double-billed. `observe(Recovered)` records that a recovery happened for metrics.

**Cancel-safety on client disconnect.** If the client disconnects during the first-event race, axum drops the handler future → our `execute` future + `stream` drop → upstream is canceled. No task leak, no panic.

**Config.** `N` is fixed + configurable (C1(b)): default ~30s (biased high for Sol's slow first token), injected via `ServeConfig`/`AppState` (e.g. `continuity_watchdog: Duration`). Tests inject a tiny `N` (e.g. 150ms). Documented upgrade path to adaptive `p95_TTFT × factor` once TTFT telemetry exists.

### Q5 — The `Continuity` trait reshape

Signatures + supporting types in §3.3. Summary: `async fn prepare(...) -> Result<Prepared, ContinuityError>` (returns the request + a `ContinuityDirective` carrying the routing pin, watchdog arm, and recovery plan) and `async fn observe(outcome, ctx) -> Result<(), ContinuityError>`. `ContinuityError` has a **generic `Display`** (never leaks session content or reasoning into an error string). The impl (`CodexContinuity`) holds a `ContinuityRepo` store handle. Slots into ingress exactly at steps 2/4/6/8 of §3.2. `NoopContinuity` keeps the Anthropic path uniform.

### Q6 — R3 reasoning-replay cache (new work; follow-up)

- **Cached:** the reasoning-typed output items of a **completed** turn (encrypted content that rides the ephemeral `store:false` response and is otherwise lost when the anchor dies).
- **Keyed:** `session_key` + `anchor_response_id`; the session row's `reasoning_cache_ref` points at the latest.
- **Re-injected:** on Recover/reattach — when the watchdog strips the anchor and resends full, the cached reasoning items are prepended to the resent `input` so chain-of-thought survives. Generalized across source protocols (the `ReasoningItems` type is protocol-neutral; the Codex extractor is the first producer).
- **Stored:** `reasoning_cache` table (§3.4), content **encrypted at rest** (TokenCipher) and redacted from `Debug`/logs (reasoning is sensitive user data — handoff §3).
- **Why follow-up, not core:** extracting reasoning from the streamed response requires the same non-buffering response-sniff plumbing as the response.id sniff but bigger; the wedge is *killed* by R1 + ownership + R2 without it (recover-without-reasoning still completes — it only loses CoT on the rare recover). Ship the wedge fix first; add CoT preservation second.
- **Eviction:** bounded by session idle-TTL + a size cap (revisit trigger already in C1: "reasoning cache footprint forces an eviction policy").

### Q7 — The wedge-regression e2e (the guardrail)

**New mock mode.** `MockUpstream` gains a **silent-on-anchor** variant: on `POST /responses`, if the JSON body contains `previous_response_id`, return **200 headers then stream nothing** — a body that never yields (`futures::stream::pending()`), **no keep-alive** (a keep-alive comment would be "first bytes" and mask silence). If the body has **no** `previous_response_id`, stream a normal `response.created` … `response.completed` with a generated `response.id`. It records every request body (already supported) so the test can assert request count + shapes. It must also be able to **emit a `response.id`** in `response.created`/`response.completed` so ownership + observe are exercised.

**Primary test — R2 Strategy A recovery (proves no hang):**
1. Seed one account; configure a tiny watchdog `N` (e.g. 150ms).
2. POST a request whose `input` is a **full multi-item history** (a full-resend) **and** carries `previous_response_id: "resp_dead"`.
3. Mock sees the anchor → 200 + silence.
4. Assert: within a bound (≪ the old 7200s), PolyFlare cancels, strips the anchor, re-executes.
5. Mock's **second** request has **no** `previous_response_id` and its `input` **equals the client's full input** (R1: full-resend not trimmed → the recovery *has* the history to resend).
6. Client receives the completed stream; total wall-time is bounded; the mock recorded exactly **two** requests (silent + recovery). **No hang.**

**Ownership test — 2nd turn returns to the same account:**
1. Two accounts; bias the selector to prefer B when unpinned.
2. Turn 1 (Fresh, no anchor) → routed to A (force via seed/state); mock returns `response.id = resp_1`; assert `observe` recorded `owner(resp_1)=A`.
3. Turn 2 carries `previous_response_id: resp_1` → assert it routes to **A** (the ownership pin overrides scoring that would pick B).

**R1 assertion (explicit, unit-level too):** a full-resend (multi-item `input`) passed through yields an upstream body whose `input` is byte-equal to the client's `input`, with or without an anchor present — never trimmed to a tail.

**Strategy B test (trimmed dead anchor):** a bare-tail request + `previous_response_id` to the silent-on-anchor mock → assert PolyFlare emits a `previous_response_not_found` to the client within `N` (bounded), not a hang.

**R3 test (follow-up):** after a completed turn with reasoning items, force a recover → assert the recovery request's `input` contains the cached reasoning items.

### Q8 — Scope + split + task breakdown

**M3-core** (must ship together — this is what actually kills the wedge):

| # | Task | Notes |
|---|---|---|
| C0 | **Red wedge test first** — add the silent-on-anchor mock mode; write the failing wedge-regression e2e | TDD: a red wedge test is the clearest spec for R2 (handoff §6) |
| C1 | Reshape `Continuity` trait + add `Prepared`/`ContinuityDirective`/`RecoveryPlan`/`TurnOutcome`/`ContinuityError`/`SessionKey`; enrich `RequestCtx` | seam change; `NoopContinuity` for Anthropic |
| C2 | Migration `0002_continuity.sql` (sessions + anchors) + `ContinuityRepo` (get/upsert session, get/put anchor) | runtime-checked sqlx |
| C3 | Session-key derivation (`x-codex-turn-state` hard / session header hard / soft) + `is_full_resend` heuristic + `client_previous_response_id` extraction, wired at ingress into `RequestCtx` | faithful to codex-lb `helpers.py` key rules |
| C4 | `CodexContinuity::prepare` — resolve owner (session + anchor lookups), set `pin_account`, arm watchdog, build `RecoveryPlan` | writes `state=reattaching`/`fresh` |
| C5 | Ingress `apply_ownership` pin-or-recover pre-filter (narrow candidates → `pick`; `None`→Recover) | no Selector-trait change |
| C6 | `execute_with_watchdog` — first-event race, cancel-on-silence, Strategy A resend / Strategy B signal; configurable `N` | the core of R2 |
| C7 | Non-buffering response sniff → `response.id`; `CodexContinuity::observe` records anchor+owner, advances state | inspect-and-forward, no buffering |
| C8 | Make the wedge-regression + ownership + R1 + Strategy-B tests green | the guardrail |

**M3-followups** (sequence after core; each an independent task/commit):

| # | Task | Why deferred |
|---|---|---|
| F1 | **R3** reasoning-replay cache (table + reasoning extraction + re-inject on recover, encrypted + redacted) | heaviest new plumbing; wedge is killed without it |
| F2 | Per-account refresh **singleflight** + surface `update_tokens` persist failures (M2 `let _ =`) | HIGH: avoids wrongful `reauth_required` on token rotation |
| F3 | Live **error/cooldown tracking** + transient-endpoint backoff (populate `error_count`/`last_error_at`/`cooldown_until` from real outcomes — `select.rs` already reads them) | needs runtime-state plumbing M2 left inert |
| F4 | **O(N) snapshot query collapse** (`GROUP BY account_id,"window"` join; single `(last_refresh, EncryptedTokens)` fetch) | LOW perf; correctness-neutral |
| F5 | `oauth.refresh`: decouple success from `id_token` decodability (best-effort claims) | small correctness fix flagged in M2 review |

**Order:** C0 → C1 → C2 → C3 → C4 → C5 → C6 → C7 → C8, then F2 (HIGH) → F3 → F1 (R3) → F5 → F4. (F2 before F1 because singleflight is a live-correctness HIGH; R3 is a feature, not a correctness gap.)

---

## 5. Testing strategy

- **Wedge-regression e2e is the acceptance gate** (Q7). A green suite that does not reproduce silence-after-accept is *not done* (handoff §7). Build the silent-on-anchor mock + red test first.
- **Unit:** session-key derivation (hard/soft cases), `is_full_resend` heuristic (multi-item / ≥4096-char boundaries, faithful to codex-lb `helpers.py:849-861`), ownership resolution (session hit / anchor hit / miss), `apply_ownership` (owner eligible → pin; owner ineligible → Recover), state-machine transitions, `RecoveryPlan` selection (full-resend → ResendFull; tail → SignalClient).
- **Watchdog unit/integration:** first-event-within-N → relay; silence → cancel + ResendFull; hard error → 502; client-disconnect-mid-race → clean cancel (no leak). Use a tiny injected `N`.
- **Non-buffering guarantee:** assert the response path streams (interleave assertion like the existing `e2e_passthrough` ordering check) and does not accumulate the whole body before first client byte.
- **Security:** redacting `Debug` + a redaction test on any new secret-bearing type (reasoning cache especially); `ContinuityError` `Display` carries no session content; no reasoning/token in logs.
- **Faithfulness:** session-key rules + full-resend heuristic verified against `../codex-lb` (handoff §3), not guessed. Mock fixtures model the real wire shapes (`response.created`/`response.completed` with a real `response.id`).
- **Gates:** `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace` green before each commit; PR CI green before merge.

---

## 6. Open risks / questions for the reviewer

1. **Heartbeat-before-`response.created` (detection fidelity).** M3-core treats *first bytes* as "alive." If a real upstream ever emits an SSE keep-alive/comment before `response.created`, that would mask a dead anchor. OpenAI is not believed to heartbeat pre-`created`, and the silent mock sends zero bytes — but should the watchdog instead wait for the first *typed event* (`response.created`) rather than first bytes? That needs incremental SSE parsing in the race, not just `stream.next()`. **Recommend:** first-bytes for M3-core (simple, correct against real upstream), upgrade to typed-event detection only if a heartbeating upstream is observed. Confirm acceptable.
2. **`store:false` forcing.** M3 assumes `store:false` semantics but does not itself force `store:false` (M1 passes the client's value through; the real CLI sends `false`). If a future fingerprint-parity milestone forces it, the watchdog already covers it. Confirm M3 should *not* add a forcing validator.
3. **History for cross-account / trimmed recover.** M3-core does **not** persist full conversation input (privacy + size). So a trimmed dead-anchor recovers via **Strategy B (signal client)**, not a proxy full-resend. Is signal-client an acceptable MVP for the trimmed case, or do you want PolyFlare to persist enough history for a proxy-side full-resend there too (bigger scope, privacy review)?
4. **Session-key strength vs. real Codex headers.** The exact header set the current Codex CLI sends (`x-codex-turn-state`, session/`prompt_cache_key`) should be re-verified against the live CLI at implementation time (`codex-fingerprint-gaps` notes turn-state identity headers); a wrong key derivation silently weakens ownership. Flagging as a verify-at-build item.
5. **Watchdog `N` default.** ~30s is conservative for Sol's slow first token; too low → false recoveries (wasted resends on slow-but-healthy turns), too high → wedge lingers longer. Confirm ~30s, or set per-deployment via config until TTFT telemetry lands (C1(b)).
6. **`observe` firing point.** `observe` must run when the client stream *ends*, via a sniffing stream adapter that fires on completion/drop. Confirm this (a Drop-guard / stream-end hook) is acceptable vs. a heavier explicit completion channel.
7. **`SignalClient` wire shape.** The exact synthetic error PolyFlare emits to trigger client self-heal (`previous_response_not_found` as an SSE `response.failed`/error event vs. an HTTP error) should be validated against what the real Codex CLI actually recovers from — a build-time capture check.
