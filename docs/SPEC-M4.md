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

### 3.5 The SSE event map *(event schemas doc-verified 2026-07-14; a short live-capture list remains — see §7)*

> **⚠️ DIRECTION CORRECTION (2026-07-15).** This section was originally written with the RESPONSE event map in the wrong direction (Anthropic→OpenAI). For the headline path — a Claude client (`AnthropicMessages`) routed to a Codex backend (`OpenAIResponses`) — §3.1's rule is `translate_request: client→backend` then "translate response events **back to the client format**" = `backend→client`. So the **request** map is Anthropic→OpenAI (`map_request`, correct as written) but the **streaming response** map is **OpenAI-Responses → Anthropic-Messages** — the INVERSE of the field table below. The implementation (`crates/polyflare-anthropic/src/translate.rs`, `AnthropicToResponses::translate_response_event`) is authoritative for the correct OpenAI→Anthropic response direction; read the table below as the field-level correspondence, inverted. (The Anthropic→OpenAI response direction is the M4c inverse-path translator, deferred.)

Both wire formats are `event: <type>` + `data: <json>` SSE. The Anthropic order is fixed: `message_start` → N×[`content_block_start` → M×`content_block_delta` → `content_block_stop`] → 1+×`message_delta` → `message_stop`, with `ping` (keepalive) and `error` interleavable. The OpenAI-Responses side requires a **global monotonic `sequence_number`** on every event plus three coordinated positional counters (`output_index` for items, `content_index` for parts-within-item, `summary_index` for reasoning-summary parts) — **none of which Anthropic supplies.**

The central fact: **the translator is a stateful assembler.** Anthropic's stream carries none of `response.id`, `item.id`, `call_id`, `output_index`/`content_index`/`summary_index`, `sequence_number`, `logprobs`, nor the terminal accumulated `text`/`arguments` strings — all must be minted and buffered per-turn. This is exactly why the trait must reshape (§3.4).

**Verified mapping** (`[S]` = translator must synthesize the field):

| Anthropic | → OpenAI-Responses | Notes |
|---|---|---|
| `message_start` | `response.created` + `response.in_progress` | `response.id` `[S]`, `sequence_number` `[S]` (own counter), `usage:null` at this stage |
| `content_block_start` (text) | `response.output_item.added` (type `message`) + `response.content_part.added` (`output_text`) | `item.id`, `output_index`, `content_index` `[S]` |
| `content_block_start` (tool_use) | `response.output_item.added` (type `function_call`, `id`/`call_id`/`name`) | `call_id` `[S]` (from Anthropic `tool_use.id`) |
| `content_block_start` (thinking) | `response.output_item.added` (type `reasoning`) | `item.id` `[S]`; Anthropic `signature` has **no** OpenAI field |
| `content_block_delta` `text_delta` | `response.output_text.delta` (1:1) | `logprobs:[]` `[S]` (required; Anthropic never provides) |
| `content_block_delta` `input_json_delta` | `response.function_call_arguments.delta` (1:1) | `partial_json` passes through as-is (compatible partial-JSON) |
| `content_block_delta` `thinking_delta` | `response.reasoning_summary_text.delta` | `summary_index` `[S]`=0; target-event choice (summary vs raw `reasoning_text`) needs live confirm |
| `content_block_delta` `signature_delta` | **∅ (one-to-zero)** | no OpenAI event carries a reasoning signature; drop or stash out-of-band |
| `content_block_stop` | `response.output_text.done` / `.function_call_arguments.done` / `.reasoning_summary_text.done` + `.content_part.done` + `.output_item.done` | full accumulated `text`/`arguments` `[S]` (buffered across deltas) |
| `message_delta` (`stop_reason`, cumulative `usage`) | folds into terminal `response.completed`/`.incomplete` | `stop_reason`→status: `end_turn`→completed, `max_tokens`→incomplete(`max_output_tokens`) |
| `message_stop` | `response.completed` (or `.incomplete`/`.failed`) | full `response` object `[S]`-assembled; `usage` `[S]`-mapped (below) |
| mid-stream `error` event | `error` event | `error.type`→`code` mapping `[S]` (no canonical table) |

**Usage mapping** (all cumulative-at-completion, only on `response.completed`): `input_tokens`→`input_tokens`; `output_tokens`→`output_tokens`; `cache_read_input_tokens`→`input_tokens_details.cached_tokens` (lossy — Anthropic also has `cache_creation_input_tokens` with no OpenAI slot); `thinking_tokens`→`output_tokens_details.reasoning_tokens`; `total_tokens` `[S]` = sum (Anthropic never reports a total).

Golden replay tests assert semantic equivalence over real captured fixtures with **no buffering** (byte-timed).

### 3.5a Anthropic rate-limit / error module *(account model matters — see U5)*

**Account model (resolved by design intent — confirm at U5):** PolyFlare pools **Claude Max/Pro OAuth subscription** accounts (the codex-lb + better-ccflare subscription-pooling lineage — the same model as the Codex pool), **not** platform API-key accounts. This is load-bearing for rate limits, because the two surfaces signal differently:

- **Doc-verified and shared by both surfaces** (safe to build on): HTTP **429** (`rate_limit_error`), **529** (`overloaded_error`, confirmed to exist), 401/403/413/500/504 with their `error.type`s; the error body is always `{"type":"error","error":{"type","message"},"request_id"}`; mid-stream `error` SSE events reuse the same `error.type` vocabulary (the executor must surface them even after a 200).
- **API-key-only** (do *not* assume for the subscription pool): the `anthropic-ratelimit-{requests,tokens,…}-{limit,remaining,reset}` headers documented for the platform API. Subscription OAuth uses the **ccflare-style** signals instead (`out_of_credits` / `extra_usage` / windowed reset / 24h-clamp) — port these from better-ccflare and **confirm the exact OAuth rate-limit signals via live capture** (added to §7). The SSE event mapping (§3.5) is identical across both surfaces, so none of that work is affected.

### 3.6 Model-alias mapping + per-tier payload-override

An alias map applied at the **request-translation seam** (inside `translate_request`, which already owns `body: Value` — the architecturally-consistent hook vs. a special-case in `ingress.rs`). For the Anthropic→Codex direction: `opus→sol`, `sonnet→terra`, `haiku→luna` (config-driven — see U2). Per-tier **payload-override** injects reasoning-effort (and optionally `service_tier`) for the mapped model, so the client's tier survives translation. Bidirectional-*ready*, but M4 ships the **Anthropic→Codex** direction only; the inverse (`Codex→Anthropic`) is the T2-deferred inverse translator → **M4c**.

### 3.7 TA6 — Anthropic capability

The shared eligibility machinery already exists (M2 gave `Account.security_work_authorized` + the Selector hard-filter; M3 the retry orchestration seam). **Research resolved the key uncertainty:** Anthropic exposes **no** documented per-account/org "approved" or entitlement flag in any API response, header, or error — usage tier lives only in the Console, and the only implicit signal is the *presence* of `anthropic-priority-*-tokens-*` headers (Priority Tier). This **confirms the existing design**: the capability flag is **operator-set** (exactly like codex-lb's `security_work_authorized`), never derived from the API. So M4's Anthropic-specific job narrows to just: classify an Anthropic provider rejection (`permission_error` / a policy refusal) into the neutral `NeedsCapability(cap)` error the retry loop already understands — reactive detection is the only path, as TA6 always intended. Continuity is a no-op for the Anthropic backend (no `previous_response_id`-style anchor), so the wedge machinery simply doesn't arm.

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
- **U5 — Anthropic account model.** Confirm the Anthropic pool is **Claude Max/Pro OAuth subscription** accounts (my assumption, from the codex-lb/ccflare subscription-pooling premise — §3.5a), not platform API-key accounts? This picks the OAuth flow and the rate-limit signal set (ccflare-style vs the platform `anthropic-ratelimit-*` headers). *If it's actually API-key accounts, say so — it changes §3.5a and the OAuth task.*

## 6. Testing strategy

- **Golden replay** — captured Anthropic SSE → assert semantically-equivalent OpenAI-Responses SSE, asserting **no buffering** (byte-timed).
- **Anthropic executor** — rate-limit classification unit tests (`out_of_credits` / `extra_usage` / `529` / 24h-clamp), non-2xx → `ExecError`.
- **Model-alias** — unit tests: `opus→sol` rewrites `body["model"]` and injects the configured effort; unmapped models pass through.
- **Dispatch** — an Anthropic-provider account routes to the Anthropic executor; a Codex account to Codex.
- **e2e** — a Claude-Messages client request routed to the Codex pool completes as Sol with effort preserved.
- **TA6** — an Anthropic capability rejection on an unpinned request excludes + re-selects + retries (never hard-fails when an eligible account exists).

## 7. Open risks / VERIFY-at-impl

The event/error/header **schemas are now doc-verified** (§3.5, §3.5a, §3.7) — the SSE map is no longer a blank unknown. What genuinely remains for **live capture** is a short, specific list (a single real Claude request + one Codex request through a tap resolves most of it):

- **Anthropic flat `index` → OpenAI `output_index` vs `content_index` split** — does each Anthropic content block become its own OpenAI output item, or parts within one message item? Confirm from a real 2-block stream.
- **`thinking_delta` target** — `response.reasoning_summary_text.delta` vs raw `response.reasoning_text.delta`; pick whichever the downstream OpenAI-shaped consumer actually reads.
- **`stop_reason` → status and `error.type` → `code` tables** — no canonical mapping exists in either doc; these are our design choices, pin them with a captured example each.
- **cache-token combination** (`cache_read` vs `cache_creation` → single `cached_tokens`) and **`anthropic-fast-*` / Priority-Tier header exact names** — confirm from real response headers.
- **Exact model strings each CLI sends** — Claude Code likely sends full IDs (`claude-opus-4-…`), not bare `opus`; the alias-map keys must match reality. (Feeds U2.)
- **Subscription-OAuth rate-limit signals** (assuming U5 = subscription) — the actual `out_of_credits`/window-reset/quota signals a Claude Max OAuth account returns, vs better-ccflare's port; capture from a real rate-limited OAuth account.

Design/impl risks (not doc-resolvable):

- **Translator reshape ripple** — moving to `&mut self` + `Vec<SseEvent>` + a registry factory touches every identity translator and the registry's storage type; contain it and keep identity zero-cost.
- **Continuity no-op path** — confirm the Anthropic backend cleanly bypasses the watchdog/ownership (arm-only-on-anchor already guarantees this, but assert it).
