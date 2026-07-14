# SPEC-M4 — Anthropic Backend + the Anthropic→Codex Translator + Model-Alias Mapping

**Status: PROPOSED — awaiting user approval.** Depends on M3-core (PR #4) being merged to `main`.

This is a design proposal in the shape of SPEC-M3. Sections 1–3 are the architecture; Section 4 lists decisions I propose to make; **Section 5 lists the questions I need *you* to answer before I write the plan.** Nothing here is implemented yet.

---

## 1. Goal

Make PolyFlare genuinely multi-provider. A Claude client (Claude Code speaks the **Anthropic Messages** API) can route to **either** pool:

- **The Anthropic account pool** — native identity path (Messages in, Messages out).
- **The Codex account pool** — translated to OpenAI-Responses, with **model-alias mapping** so `opus`→`sol`, `sonnet`→`terra`, `haiku`→`luna`, **and the reasoning-effort / tier is preserved end-to-end**.

That second path is the headline feature you proposed: Claude Code natively tiers its own work (opus orchestrator / sonnet subagent / haiku searcher), so mapping the *model name* lets the **client** drive per-role Codex model selection — a clean answer to the Codex "subagents forced to the session model" problem, with no A/B metadata fight.

## 2. Current state vs what M4 adds

Today PolyFlare is Codex-only end to end:

- **One ingress path** — `POST /responses` (OpenAI-Responses shape) in `ingress.rs`.
- **One executor** — `AppState.executor: Arc<dyn Executor>` wired to `CodexExecutor`; no per-provider dispatch.
- **Codex-only store** — `Account` has no `provider` discriminator; the schema is Codex-shaped (`chatgpt_account_id`, `plan_type`, …, `security_work_authorized`).
- **Identity-only translator registry** — `translate.rs`; the one real cross-translator does not exist.
- **Raw `model` passthrough** — `ingress.rs` extracts `body["model"]` into `PreparedRequest.model` once, with **no alias/remap anywhere**.
- **`polyflare-anthropic` is an empty stub** (a single doc-comment line).

M4 adds, in order of dependency: (1) provider modeling in the store; (2) per-provider executor dispatch; (3) an Anthropic-Messages ingress path; (4) the Anthropic HTTP executor + rate-limit semantics + TA6 detection; (5) the stateful Anthropic→OpenAI-Responses streaming translator; (6) the model-alias + payload-override layer.

## 3. Architecture

### 3.1 Request routing (the new dispatch)

```
client request
  → ingress decode path:  /responses (OpenAIResponses)  |  /v1/messages (AnthropicMessages)
  → determine client Format
  → assemble snapshots → Selector.pick  (unchanged; picks an Account)
  → picked Account.provider decides the backend Format + Executor
  → Translator = registry[(clientFormat, backendFormat)]
       · same format  → identity (zero-cost)
       · cross        → the M4 translator, WITH model-alias + payload-override applied to the request
  → Continuity.prepare  (no-op for Anthropic backend; full engine for Codex)
  → Executor.execute
  → translate response events back to the client Format (identity or M4 translator)
  → relay (still non-buffering)
```

### 3.2 Per-provider executor dispatch

`AppState` currently holds one `executor`. M4 replaces it with a small dispatch keyed by provider — e.g. `codex_executor` + `anthropic_executor` fields, or `executors: HashMap<Provider, Arc<dyn Executor>>`. The `Selector` still picks an `Account`; that account's `provider` selects the executor. No change to the `Executor` trait (`async fn execute(&self, req, account) -> Result<ResponseStream, ExecError>`) — the Anthropic executor is a second impl mirroring `CodexExecutor`'s current simple HTTP+bearer+`bytes_stream` shape.

### 3.3 Store: modeling Anthropic accounts *(open — see U3)*

The M2/M3 schema is Codex-only. **Proposed (minimal, additive):** add `provider TEXT NOT NULL DEFAULT 'codex'` to the accounts table (forward-only migration `0003_provider.sql`), make the Codex-specific columns nullable, and reuse the existing XChaCha20 token-encryption machinery for Anthropic OAuth tokens. Anthropic accounts populate the neutral fields (`id`, `base_url`, `bearer_token`, `security_work_authorized`) and leave Codex-only fields NULL. A `Provider` enum (`Codex | Anthropic`) in `polyflare-core`.

### 3.4 The `Translator` trait reshape — M4's crux (the "Q5")

The current seam is insufficient for the real translator:

```rust
// today (translate.rs) — stateless, 1-in-1-out:
pub trait Translator: Send + Sync {
    fn translate_request(&self, body: Value) -> Value;
    fn translate_response_event(&self, event: Value) -> Value;
}
```

An Anthropic SSE stream → OpenAI-Responses SSE stream is **stateful and 1→N**: one `message_start` opens a response and emits `response.created`; a `content_block_start` + several `content_block_delta`s coalesce into `response.output_text.delta`s; `message_stop` emits `response.completed`; a single incoming event can produce **zero, one, or many** outgoing events, and the mapping needs per-turn state (message id, content-block index, accumulated usage). DESIGN-DECISIONS' M2-GATE1 flagged this exact revisit.

**Proposed reshape** — a per-turn stateful translator:

```rust
pub trait Translator: Send + Sync {
    fn translate_request(&mut self, body: Value) -> Value;              // may rewrite model + inject effort
    fn translate_response_event(&mut self, event: SseEvent) -> Vec<SseEvent>;  // 1 → N
}
```

The registry becomes a **factory** (`fn() -> Box<dyn Translator>`) so each turn gets a fresh stateful instance. Identity translators stay trivial (`translate_request` returns input; `translate_response_event` returns `vec![event]`). This is additive to M1's registry shape but touches its storage type — flagged as a risk (§7).

### 3.5 The Anthropic→OpenAI-Responses event map *(VERIFY at impl — live captures)*

Proposed mapping table (to be confirmed against captured fixtures, per DESIGN §10):

| Anthropic Messages SSE | OpenAI-Responses SSE |
|---|---|
| `message_start` | `response.created` |
| `content_block_start` (text) | (open text item; no direct emit) |
| `content_block_delta` (text_delta) | `response.output_text.delta` |
| `content_block_start`/`_delta` (tool_use) | `response.function_call_arguments.*` / function-call item |
| `content_block_stop` | (close item) |
| `message_delta` (stop_reason, usage) | accumulate usage / finalize |
| `message_stop` | `response.completed` |

Golden replay tests assert semantic equivalence over real captured fixtures with **no buffering**.

### 3.6 Model-alias mapping + per-tier payload-override

An alias map applied at the **request-translation seam** (inside `translate_request`, which already owns `body: Value` — the architecturally-consistent hook vs. a special-case in `ingress.rs`). For the Anthropic→Codex direction: `opus→sol`, `sonnet→terra`, `haiku→luna` (config-driven — see U2). Per-tier **payload-override** injects reasoning-effort (and optionally `service_tier`) for the mapped model, so the client's tier survives translation. Bidirectional-*ready*, but M4 ships the **Anthropic→Codex** direction only; the inverse (`Codex→Anthropic`) is the T2-deferred inverse translator → **M4c**.

### 3.7 TA6 — Anthropic capability

The shared eligibility machinery already exists (M2 gave `Account.security_work_authorized` + the Selector hard-filter; M3 the retry orchestration seam). M4's Anthropic-specific job is only: (a) populate the per-account capability flag (Anthropic approved-org / dual-use entitlement — **VERIFY the signal**), and (b) classify an Anthropic provider rejection into the neutral `NeedsCapability(cap)` error the retry loop already understands. Continuity is a no-op for the Anthropic backend (no `previous_response_id`-style anchor), so the wedge machinery simply doesn't arm.

## 4. Resolved decisions (proposed — I'll own these unless you object)

- **Q1 — Two ingress paths, dispatch by picked account's provider.** `/responses` and `/v1/messages`; the Selector picks an account, its `provider` picks the executor + backend Format.
- **Q2 — Reshape `Translator` to stateful 1→N** (per-turn instance via a registry factory). Identity stays trivial.
- **Q3 — Anthropic backend continuity = no-op** (no anchor). Wedge engine unaffected.
- **Q4 — Rate-limit semantics = a typed module in `polyflare-anthropic`** (ccflare `out_of_credits` / `extra_usage` / `529` / 24h-clamp).
- **Q5 — Model-alias applied inside `translate_request`** (not a special-case in ingress), config-driven.
- **Q6 — Split:** **M4a** (store provider column + executor dispatch + Anthropic-Messages ingress + Anthropic HTTP executor + rate-limit + TA6 detection + Anthropic OAuth) → a Claude client reaches the Anthropic pool natively. **M4b** (Translator reshape + Anthropic→OpenAI-Responses translator + golden tests + model-alias + payload-override) → **your headline feature: a Claude client reaches the Codex pool as Sol.** **M4c** (inverse `Codex→Anthropic`) deferred (T2-YAGNI).

## 5. Open questions FOR YOU (needed before I write the plan)

- **U1 — Direction & split.** Confirm the M4a → M4b → (deferred M4c) order above? Or do you want the cross-translator (M4b, the headline) *first*, before the native Anthropic path? My recommendation: M4a first (it's the foundation the translator plugs into), but M4b is the feature you actually want, so if you'd rather see it sooner I can invert with a temporary hardcoded Anthropic executor.
- **U2 — Model-alias config + payload-override scope.** (a) Source: a **static config** (ServeConfig / TOML / env) for M4, DB-backed + dashboard-editable at L5 — OK? (b) How far does payload-override go in M4 — just **reasoning-effort**, or also `service_tier` and others? (c) **The exact pairs + tiers you want:** is it `opus→sol` (which effort?), `sonnet→terra`, `haiku→luna`? Anything else?
- **U3 — Store provider modeling.** Minimal additive (`provider` column + nullable Codex fields) as proposed, or a cleaner separate-table refactor? (I recommend minimal.)
- **U4 — Golden-test fixtures.** Do you have real captured **Anthropic Messages SSE** + **Codex Responses SSE** streams I can use as golden fixtures, or should M4 include a capture step (needs one live Claude request + one live Codex request through a tap)?

## 6. Testing strategy

- **Golden replay** — captured Anthropic SSE → assert semantically-equivalent OpenAI-Responses SSE, asserting **no buffering** (byte-timed).
- **Anthropic executor** — rate-limit classification unit tests (`out_of_credits` / `extra_usage` / `529` / 24h-clamp), non-2xx → `ExecError`.
- **Model-alias** — unit tests: `opus→sol` rewrites `body["model"]` and injects the configured effort; unmapped models pass through.
- **Dispatch** — an Anthropic-provider account routes to the Anthropic executor; a Codex account to Codex.
- **e2e** — a Claude-Messages client request routed to the Codex pool completes as Sol with effort preserved.
- **TA6** — an Anthropic capability rejection on an unpinned request excludes + re-selects + retries (never hard-fails when an eligible account exists).

## 7. Open risks / VERIFY-at-impl

- **The Anthropic SSE ↔ OpenAI-Responses event map (§3.5)** — the single largest technical risk; build from live captures, not from better-ccflare's TS alone.
- **Anthropic error / rate-limit header signals + whether an org-"approved" flag is exposed (TA6)** — VERIFY vs the live API.
- **Exact model strings each CLI sends** — Claude Code likely sends full IDs (`claude-opus-4-…`), not bare `opus`; the alias map keys must match reality. VERIFY.
- **Translator reshape ripple** — moving to `&mut self` + `Vec<SseEvent>` + a registry factory touches every identity translator and the registry's storage type; contain it and keep identity zero-cost.
- **Continuity no-op path** — confirm the Anthropic backend cleanly bypasses the watchdog/ownership (arm-only-on-anchor already guarantees this, but assert it).
