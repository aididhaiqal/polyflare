# PolyFlare — Design Spec

**Date:** 2026-07-14
**Status:** Validated design (brainstorming complete) — ready for implementation planning.
**Companion docs:** [`CAPABILITY-MARKUP.md`](./CAPABILITY-MARKUP.md) (source-system inventory), [`DESIGN-DECISIONS.md`](./DESIGN-DECISIONS.md) (per-decision reasoning traces: *Why / Trade-off / Revisit-if*).

---

## 1. What PolyFlare is

PolyFlare is a from-scratch **Rust** multi-provider LLM-CLI load balancer / gateway. It fronts a pool of provider accounts (Codex/ChatGPT and Anthropic to start), presents a client-facing API in multiple wire formats, translates between formats where needed, and routes each request to the best available account while impersonating the native CLI on egress.

It is the successor to two systems and takes the crown jewels of each (see `CAPABILITY-MARKUP.md`):
- **codex-lb** (Python) — deep single-provider Codex pool + fingerprint laundering + quota-shaped routing + continuity.
- **CLIProxyAPI** (Go) — the multi-provider translator-registry + selector abstraction + payload-override patterns.
- **better-ccflare** (TS) — the Anthropic-pool rate-limit intelligence + the dashboard direction (post-MVP).

**Naming:** `poly-` (multi-provider — the differentiator vs the single-provider ancestors) + `-flare` (the ccflare/Cloudflare edge-proxy lineage). Binary `polyflare`; workspace crates `polyflare-core` / `-codex` / `-anthropic` / `-store` / `-server`.

### Why it exists (the two fixes that justify a rebuild, not a refactor)

1. **Continuity done right — kill the wedge.** codex-lb's `previous_response_id` anchoring is implicit inline logic; when it mis-decides trim-vs-resend on a `store:false` full-resend, the conversation *wedges* (~31% of reattaches; ownership guards e0f3da3f/d846fd0a did NOT fix it — proving it's a decision bug, not an ownership bug). PolyFlare makes continuity an **explicit per-conversation state machine with a watchdog**, so the wedge is impossible to miss and always recoverable.
2. **A real egress fingerprint.** codex-lb faked the User-Agent but ran on Python's TLS stack, so its JA3/handshake never matched the real Codex CLI. Rust's TLS control lets PolyFlare present a **byte-identical** fingerprint — a capability the old stack could not have.

---

## 2. Goals / Non-goals

### MVP goals (the lean core, L0–L4)
- Working **dual-pool** gateway: Codex pool + Anthropic pool, single binary.
- Neutral core + **translator registry** with one real cross-translator (`Anthropic → Codex`).
- **Continuity engine** (state machine + watchdog + reasoning-replay) wired into the Codex executor.
- **Selector** with one default strategy behind a trait; correct continuity-aware ordering.
- Byte-identical Codex **egress fingerprint** (WS-first transport + TLS parity).
- **API-key auth**, account **OAuth import** (zero re-auth from codex-lb), embedded **SQLite** store.
- Thin observability + **e2e + latency test harness shipped with the MVP** (first-class, not a follow-up).

### Non-goals (explicitly deferred — additive later, never a rewrite)
- **Dashboard (L5)** — codex-lb remains the operator UI until then; PolyFlare serves the same REST API the dashboard will later read.
- **Inverse translator** (`Codex → Anthropic`) and any Gemini / Chat-Completions formats.
- **The other 7 routing strategies** (force-finish, priority-drain, etc.) — added when live data justifies ("test and see").
- **Distributed coordination** — single-binary; a `Coordinator` trait seam keeps a future adapter additive.
- **Scheduled synthetic account-warming** — parked principle (§9).

---

## 3. Architecture

### Layered model
```
L4  server edge   — axum ingress (2 decode paths), API-key auth, OAuth import, store, thin observ.
L3  ingress/xlate  — decode client Format → neutral core → translator registry → encode
L2  selection      — Selector (pool/policy/health/cooldown), continuity-aware ordering
L1  execution      — Executor per backend: Codex (WS-first + fingerprint + continuity), Anthropic (HTTP)
L0  neutral core   — Format enum + translator registry + the five traits (the spine)
```

### Workspace crates
| Crate | Owns |
|---|---|
| `polyflare-core` | `Format` enum, translator registry, the five traits, continuity state machine, selector logic |
| `polyflare-codex` | Codex `Executor` + WS/SSE transport + fingerprint laundering + Codex `Continuity` impl |
| `polyflare-anthropic` | Anthropic `Executor` + HTTP transport + rate-limit header semantics |
| `polyflare-store` | SQLite (`sqlx`) schema, account/usage/log/continuity persistence, at-rest crypto |
| `polyflare-server` | axum ingress, auth, OAuth import, config, observability, `Coordinator` in-process impl |

### The five traits (the seams that keep everything additive)
- `Translator` — `req: Value→Value`, `resp: {stream, nonstream}`, `tokencount`. Registered per `(Format, Format)`.
- `Executor` — `execute(PreparedReq, Account) → ResponseStream`. One per backend; transport lives below it.
- `Selector` — `pick(pool, ctx) → Account`. One default impl; others additive.
- `Continuity` — `prepare(req, state) → PreparedReq`, `observe(resp) → Transition`. Codex impl carries anchor semantics; Anthropic no-op.
- `Coordinator` — session-ownership + admission. In-process v1; distributed adapter later.

---

## 4. Component designs

### 4.1 Continuity engine (`polyflare-core::continuity`) — **the wedge fix**
State owned per conversation in `polyflare-store`: `{ anchor_response_id, owning_account, last_good_turn, reasoning_cache_ref, state }`.

```
                        new turn
                           │
                    anchor known? ──no──► Fresh ──(1st resp)──┐
                           │yes                                │
                           ▼                                   ▼
   client sent full history?                              Anchored ◄──────┐
        │            │                                        │           │ upstream
     store:true   store:false                     next turn / reattach    │ accepted
     (trim to      (DANGER: upstream may           │                       │ anchor
      anchor —      NOT have persisted) ──────► Reattaching ──watchdog──────┘
      safe)                                         │
                                          no accepted-anchor / first
                                          token within N seconds
                                                    │
                                                    ▼
                                        Recover: resend FULL history,
                                        never trim, re-anchor on same
                                        account (reasoning replayed
                                        from cache) → back to Anchored
```

**Three rules:**
- **R1 — never trim a `store:false` full-resend** to an anchor the upstream isn't guaranteed to hold. Trim only when the client kept `store:true` history. *(Default hard; per-account `allow_store_false_trim` seam OFF until an account is proven to resume reliably.)*
- **R2 — watchdog** on every `Reattaching`: no accepted-anchor / first-token within **N seconds** → cancel, full-resend recovery on the same account. *(MVP: fixed configurable N ~30s, biased high for Sol's slow first-token; upgrade path to adaptive `p95_TTFT × factor` once telemetry exists.)*
- **R3 — reasoning-replay cache for all source protocols** (generalizes CLIProxyAPI's Claude-only replay) — reasoning items survive a reattach.

**Failure mode targeted:** SAME-account `store:false` anchor non-resumption → the 31% wedge. See `DESIGN-DECISIONS.md#C1`.

### 4.2 Translator registry (`polyflare-core::translate`) — **the spine**
`HashMap<(Format, Format), Translator>`; same-format pairs are **identity** (zero cost — the native paths pay nothing).

**MVP registry (3 entries, 1 real):**
| (from, to) | Translator | Serves |
|---|---|---|
| `OpenAIResponses → OpenAIResponses` | identity | real Codex CLI → Codex pool |
| `AnthropicMessages → AnthropicMessages` | identity | Claude Code → Anthropic pool |
| **`AnthropicMessages → OpenAIResponses`** | **the one cross-translator** | Claude-format client / subagent → Codex pool |

**The hard part is streaming:** map Anthropic SSE events (`message_start` / `content_block_delta` / `message_delta`) onto OpenAI-Responses events (`response.output_text.delta` / `response.completed`) **event-by-event, no buffering**. The request direction is mechanical by comparison. This translator is the ~2k-LOC slice scoped from CLIProxyAPI.

### 4.3 Selector / pool / policy (`polyflare-core::select`)
`Selector::pick(pool, ctx) → Account`. MVP impl = codex-lb's **relative-availability + burn/preserve** scoring (`logic.py` is pure math → direct port + parity tests), wrapped by session-affinity.

**The load-bearing ordering rule** (binds selection to continuity):
```
1. Continuity ownership   — anchored conversation MUST return to owning_account (or Recover). HARD pre-filter.
2. Session affinity        — same session → same account while healthy (won't re-pin a rate-limited-but-sessioned acct).
3. Availability scoring     — burn/preserve + relative-availability picks among the eligible.
4. Health / cooldown gate   — per-account-per-model cooldown; excluded accounts actually leave the loop.
```
Continuity ownership is a **hard pre-filter, not a scoring input** — if availability outranked it, an anchored conversation could be routed to a cold account that doesn't hold its anchor, **re-creating the wedge**.

Health/cooldown is per-account-**per-model**: an account rate-limited for `gpt-5.6-sol` can still serve `luna`. The other 7 strategies are additive `Selector` impls, added only when live data justifies.

### 4.4 Executors + transport (`polyflare-codex` / `polyflare-anthropic`)
`Executor::execute(...)` sits above transport; transport (WS / SSE / HTTP) is behind a common streaming interface so continuity and translation are transport-agnostic.

- **`polyflare-codex`** — **WS-first** (`supports_websockets=true`, SSE fallback on 426), `codex_cli_rs` UA + `x-stainless-*` stripping + native-vs-SDK detection keyed only on UA/originator (never replayable turn-state), plugs the Codex `Continuity` impl.
- **`polyflare-anthropic`** — HTTP + Anthropic OAuth + ccflare rate-limit header semantics (`out_of_credits` / `extra_usage` / `529` / 24h-clamp) as a typed module, no-op continuity.

**Fingerprint fidelity target = byte-identical (uTLS-style parity), chased continuously.** Made affordable by the key enabler: the impersonation target is **itself a Rust/reqwest client (`codex-rs`), not a browser** — so byte-parity reduces to **pinning PolyFlare's TLS/HTTP dependency stack to codex-rs's** (same `reqwest` + TLS backend + `h2` versions → matching ClientHello / cipher order / GREASE / HTTP/2 SETTINGS for free), sustained by a **capture-fixture + CI parity-diff gate** (capture the real CLI's egress fingerprint per Codex release; assert PolyFlare byte-matches).
> **MUST verify at implementation time:** codex-rs's actual TLS backend + reqwest/h2 versions (authoritative source per `codex-model-metadata-sources` → client.rs + Cargo.toml). If it uses `native-tls` rather than a stack giving ClientHello control, use BoringSSL (`boring`/`tokio-boring`) + a ClientHello customization layer. The affordability of byte-parity rests on this check.

### 4.5 Server edge (`polyflare-server` + `polyflare-store`)
- **Ingress** — two decode paths (OpenAI-Responses, Anthropic-Messages) → neutral core.
- **Auth** — API-key (admin + client keys), same contract codex-lb exposes.
- **Store** — **SQLite via `sqlx`**, clean schema day one (port codex-lb's *final* tables only; drop all 176 Alembic migrations).
- **At-rest crypto** — **XChaCha20-Poly1305** (RustCrypto `aead`), external key file retained. Chosen over Fernet-compat: zero-re-auth is separable from format-lock — the one-time importer decrypts old Fernet blobs and re-encrypts native, so PolyFlare carries no legacy format. XChaCha's 192-bit nonce is safe with random per-blob nonces. See `DESIGN-DECISIONS.md#SE5`.
- **Observability** — thin: 6-phase latency model + small Prometheus set + request logs; 3–4 decision numbers, not the 53-col/25-field sprawl.
- **Coordinator seam** — session-ownership + admission behind a trait; in-process impl v1.

---

## 5. Request lifecycle (end-to-end)
```
client request (Format A)
  → axum ingress: auth + decode to neutral core
  → Continuity.prepare(req, session_state)         # trim/resend decision, anchor injection
  → Selector.pick(pool, ctx)                        # ownership → affinity → scoring → health
  → Translator[(A, backendFormat)].req              # identity if A == backendFormat
  → Executor.execute(preparedReq, account)          # WS-first (Codex) / HTTP (Anthropic), fingerprinted
  → stream upstream response
  → Translator[(backendFormat, A)].resp.stream      # event-by-event, no buffering
  → Continuity.observe(resp) → Transition           # watchdog resolves; state advances or Recovers
  → usage/quota accounting + request log
  → client (Format A)
```

---

## 6. Testing & latency strategy (first-class MVP deliverable)
Shipped **with** the MVP, not after:
1. **e2e harness** with scriptable **mock upstreams** (fake OpenAI-Responses + fake Anthropic) that emit exact SSE sequences, 429s, mid-stream stalls, and `store:false` non-resumption.
2. **Golden replay tests** for the `Anthropic → Codex` streaming translator — captured real event fixtures → assert semantic equivalence, no buffering.
3. **Continuity wedge-regression e2e** — reproduce the `store:false` reattach; assert R1 (never-trim), R2 (watchdog recovers), R3 (reasoning replayed). This is the guardrail that makes the wedge fix *stick*.
4. **Latency-regression gate** in CI — assert PolyFlare's *own* added latency (`total − upstream`) stays under a small threshold; plus time-to-first-translated-event. Port codex-lb's 6-phase latency model as the phase breakdown. (codex-lb's own gate overhead was measured at ~2ms/23k — PolyFlare must prove parity continuously, not by vibe.)
5. **Fingerprint parity-diff gate** — assert egress ClientHello/headers byte-match the captured Codex-release fixture.

---

## 7. Migration / cutover
- **Accounts** — import from codex-lb with **zero re-auth**: the importer (extending the existing `migrate_oauth_usage.py`) reads Python Fernet-encrypted OAuth blobs with the shared `~/.codex-lb/encryption.key`, decrypts, and re-encrypts under PolyFlare's XChaCha20-Poly1305 key.
- **Usage/history** — column-intersection copy into the clean PolyFlare schema (as the codex-lb cutover already did).
- codex-lb stays live as the operator UI until the L5 dashboard lands; PolyFlare can run alongside on a different port during bring-up.

---

## 8. Decisions summary (traces in `DESIGN-DECISIONS.md`)
| # | Decision | Verdict |
|---|---|---|
| Name | Product name | **PolyFlare** |
| Q1 | Center of gravity | **B** — provider-neutral dual-upstream core; translator registry = spine |
| Q2 | Deployment | **A+** — single-binary with `Coordinator` trait seam |
| Q3 | MVP line | **(i)** — lean core L0–L4, no dashboard |
| C1 | Continuity | Explicit state machine + watchdog; R1 hard-default / R2 fixed-N ~30s / R3 all-protocol replay |
| T2 | Translator registry | 3 entries; only `Anthropic → Codex` real |
| S3 | Selector | One default behind trait; continuity-ownership-first ordering; expansion = test-and-see |
| E4 | Fingerprint fidelity | **(b)** byte-identical parity; affordable via codex-rs dep-pin + CI parity gate |
| SE5 | At-rest crypto | Rust-native **XChaCha20-Poly1305**, not Fernet-compat |
| X1 | Testing | e2e + golden + wedge-regression + latency gate, all first-class |

---

## 9. Deferred / parked
- **Dashboard (L5)** — keep the React 19 + Vite + TanStack + Tailwind + shadcn + Recharts stack; embed the Vite build in the Rust binary via `rust-embed`/`include_dir`; axum catch-all; SSE from Rust. Do NOT rewrite in Leptos/Dioxus. Resolve the parked B-vs-C dashboard ambiguity first (`dashboard-redesign-parked`).
- **Inverse + more formats** — `Codex → Anthropic`, Gemini, Chat-Completions.
- **More selector strategies** — force-finish, priority-drain, the rest of codex-lb's 8.
- **Distributed `Coordinator`** — only if HA/multi-machine becomes a real need.
- **Account warming** — parked principle: *"traffic to an account happens only because a real user request needs it."* KEEP OAuth refresh + one-time onboarding probe; REDESIGN prewarm cheap/optional; scheduled synthetic warming OFF by default (it fights impersonation and spends quota).

## 10. Open items to verify during implementation
- codex-rs's actual TLS backend + `reqwest`/`h2` versions (gates the byte-parity affordability argument — §4.4).
- Exact SSE event-mapping table for `AnthropicMessages ↔ OpenAIResponses` (build from live captures).
- Final SQLite schema (the minimal set of codex-lb tables actually needed).
