# PolyFlare vs codex-lb vs better-ccflare vs CLIProxyAPI

A capability comparison of PolyFlare against the three systems it draws from, and an honest ledger
of what PolyFlare **has**, **partially has**, and **does not have yet**.

- **[codex-lb](https://github.com/aididhaiqal/codex-lb)** (Python) — PolyFlare's predecessor. A deep
  single-provider Codex pool + faithful `codex_cli_rs` impersonator. The reference for Codex depth.
- **[better-ccflare](https://github.com/tombii/better-ccflare)** (TypeScript/Bun) — an Anthropic-first
  pool with a polished analytics dashboard, prompt-cache machinery, and rate-limit intelligence.
- **[CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI)** (Go) — a universal N×M protocol
  translator + multi-provider router. The reference for translation architecture.

> Generated from a multi-agent capability audit (reading the codex-lb and better-ccflare source
> locally, CLIProxyAPI from its docs) cross-checked against `docs/CAPABILITY-MARKUP.md`. PolyFlare's
> column is grounded strictly in what is **actually built and wired** in this repo — designed-but-unbuilt
> items are marked 🟡 or ❌ with a note, never credited as ✅.

**Legend:** ✅ built · 🟡 partial / scaffolded · ❌ not yet · — not applicable

## Headline

PolyFlare has already matched the *hard* parts its rivals lack — real rustls/JA3 fingerprint control,
a watchdog state machine that structurally kills the anchor **wedge**, and a faithful
`capacity_weighted` port — but it is honestly **early**. Its biggest holes are all things every
reference system ships: **inert failure-driven health/cooldown tracking** (the routing runs on neutral
data), **zero admin auth**, **no retry/failover or anti-starvation**, **no prompt-cache stickiness**,
and **Anthropic-side OAuth**.

## The matrix

Columns: **PF** = PolyFlare · **CLB** = codex-lb · **CC** = better-ccflare · **CP** = CLIProxyAPI.

### Providers & Protocols
| Feature | PF | CLB | CC | CP | Note |
|---|:--:|:--:|:--:|:--:|---|
| OpenAI Responses API (Codex/ChatGPT) ingress + backend | ✅ | ✅ | ✅ | ✅ | PF does byte-verbatim pass-through over pinned TLS. |
| Anthropic Messages API (`/v1/messages`) ingress + backend | ✅ | ❌ | ✅ | ✅ | codex-lb is Codex-only; ccflare is Anthropic-native. |
| OpenAI Chat Completions ingress | ❌ | ✅ | 🟡 | ✅ | PF `Format` enum has only OpenAIResponses + AnthropicMessages. |
| Gemini wire format | ❌ | ❌ | ❌ | ✅ | Only CLIProxyAPI accepts native Gemini inbound. |
| WebSocket transport for Codex | ❌ | ✅ | ❌ | ✅ | PF is HTTP-SSE only; WS is a later milestone. |
| Broad multi-provider deep pooling (3+ providers) | ❌ | ❌ | ✅ | ✅ | PF has 2 backends; ccflare/cliproxy span many vendors. |
| Local/self-hosted providers (Ollama etc.) | ❌ | 🟡 | ✅ | ✅ | codex-lb `openai_compatible` is a thin BYO relay. |

### Protocol Translation
| Feature | PF | CLB | CC | CP | Note |
|---|:--:|:--:|:--:|:--:|---|
| Cross-provider request + response-SSE translation (event-by-event) | ✅ | ✅ | ✅ | ✅ | PF Anthropic→Codex request + Codex→Anthropic SSE, stateful, unit-tested. |
| Model-alias-driven cross-provider routing (Claude client → Codex pool) | ✅ | — | ✅ | ✅ | codex-lb is single-provider. |
| Bidirectional / inverse translation | ❌ | ✅ | ✅ | ✅ | PF is Anthropic→Codex only; inverse (M4c) deferred YAGNI. |
| Universal N×M translator registry (live dispatch) | 🟡 | ❌ | ❌ | ✅ | PF has a registry scaffold but ingress builds the translator directly. CLIProxyAPI's `(from,to)` map is its crown jewel. |
| Confirmed production alias pairs + `reasoning.effort` wire shape | 🟡 | ✅ | 🟡 | ❌ | PF's `gpt-5.6-sol/terra/luna` + `{reasoning:{effort}}` are speculative placeholders (U2/U4), unverified vs a live backend. |

### Routing & Selection
| Feature | PF | CLB | CC | CP | Note |
|---|:--:|:--:|:--:|:--:|---|
| Multiple config-selectable selection strategies | ✅ | ✅ | 🟡 | 🟡 | PF 6 strategies; codex-lb 8; ccflare deliberately 1–2 (anti-ban). |
| Capacity/quota-shaped routing (burn/preserve waterfall) | 🟡 | ✅ | 🟡 | ❌ | PF ports the logic faithfully, **but** health/error inputs are inert defaults → runs on neutral data. |
| Live health tiers / cooldown driven by request failures | ❌ | ✅ | ✅ | ✅ | **PF's `health_tier`/`error_count`/`cooldown_until` are ALWAYS neutral — nothing writes them on upstream failure.** |
| Session affinity / sticky routing | 🟡 | ✅ | ✅ | ✅ | PF has a content-free ownership pin for Codex continuity, but no general `prompt_cache_key` stickiness. |
| Anti-starvation backoff-fallback (serve soonest-to-recover) | ❌ | ✅ | ✅ | ✅ | PF: empty eligible pool → 503. codex-lb `logic.py:485-548` fallback not ported. |
| Retry / failover across accounts on request failure | 🟡 | ✅ | ✅ | ✅ | PF only fails over on the no-anchor recovery path; no general request-retry. |
| Cross-provider fallback chains (combos) | ❌ | ❌ | ✅ | 🟡 | ccflare named ordered chains with per-family activation. |
| Novel tier-aware cache-affinity strategy | ✅ | ❌ | ❌ | ❌ | **PF-original:** pack haiku onto near-limit accounts, preserve fresh for opus. |

### Continuity & Wedge
| Feature | PF | CLB | CC | CP | Note |
|---|:--:|:--:|:--:|:--:|---|
| `previous_response_id` continuity for Codex | ✅ | ✅ | — | 🟡 | cliproxy has continuity but with orphan-reasoning-id + cross-account bugs. |
| **Silence watchdog state machine** (arm-on-anchor, peek-before-relay) | ✅ | ❌ | ❌ | ❌ | **PF's headline differentiator — the thing PolyFlare exists to fix.** |
| **`store:false` anchor wedge structurally fixed** | ✅ | ❌ | — | ❌ | codex-lb carries the known wedge defect; cliproxy has related chaining bugs. |
| Ownership routing + content-free anchor persistence | ✅ | ✅ | — | 🟡 | PF anchor map persists **zero** conversation content. |
| Request-body buffering / replay on failover | 🟡 | ✅ | ✅ | ✅ | PF does anchor-stripped full-resend for recovery but no general small-body replay. |
| Durable bridge ownership across replicas | ❌ | ✅ | ❌ | ❌ | PF is in-process single-binary by design. |
| Reasoning-replay cache (R3) | ❌ | ❌ | — | ❌ | PF schema column exists but inert; no reference ships a real one either. |
| Tool-call dedupe / compaction | ❌ | ✅ | ❌ | 🟡 | codex-lb has an 809-LOC dedupe + `/responses/compact`. |

### Egress Fingerprint & Impersonation
| Feature | PF | CLB | CC | CP | Note |
|---|:--:|:--:|:--:|:--:|---|
| **Real TLS / JA3 ClientHello control** | ✅ | ❌ | ❌ | 🟡 | **PF pins rustls+aws-lc-rs with an X25519MLKEM768 PQ share, proven on the wire.** codex-lb fakes only the app layer. |
| Byte-exact TLS parity (ext order / GREASE / cipher list) | 🟡 | ❌ | ❌ | ❌ | PF proves structural parity; byte-exact gate deferred pending a live capture. |
| Codex UA synthesis from live version | ✅ | ✅ | 🟡 | 🟡 | PF capture-verified vs codex-cli 0.144.4, GitHub→npm version resolution. |
| Native-vs-SDK detection + `x-stainless-*` stripping | 🟡 | ✅ | ❌ | 🟡 | PF uses a small conservative drop-list, not codex-lb's full normalizer. |
| `chatgpt-account-id` paired with the selected bearer | ✅ | ✅ | 🟡 | ✅ | PF forces (token, account) consistency from the selected row. |
| HTTP header-order wire fidelity | ❌ | ❌ | ❌ | ❌ | PF axum `HeaderMap` loses receipt order; documented gap. **No system nails this.** |
| Fingerprint capture + parity CI gate | ✅ | ❌ | ❌ | ❌ | **PF-unique guardrail test.** |

### OAuth & Login
| Feature | PF | CLB | CC | CP | Note |
|---|:--:|:--:|:--:|:--:|---|
| Codex OAuth refresh (refresh_token grant) + PKCE login | ✅ | ✅ | ✅ | ✅ | PF now exp-driven refresh + loopback:1455 PKCE login. |
| Per-account refresh singleflight + failure classification | ✅ | ✅ | ✅ | ✅ | PF `RefreshLocks` + 12 permanent-code classification. |
| Anthropic subscription OAuth refresh/login | ❌ | — | ✅ | ✅ | PF Anthropic accounts use their stored access_token as-is (Task 7, no confirmed endpoint). |
| Device-code login flow | ❌ | ✅ | ❌ | ❌ | codex-lb supports headless enrollment. |
| Multi-provider OAuth (Gemini/Grok/Vertex/…) | ❌ | ❌ | ✅ | ✅ | Scope difference. |
| Token encryption at rest | ✅ | ✅ | ❌ | 🟡 | PF XChaCha20-Poly1305 + ZeroizeOnDrop; **ccflare stores tokens plaintext** (its top security gap). |

### Multi-pool & Accounts
| Feature | PF | CLB | CC | CP | Note |
|---|:--:|:--:|:--:|:--:|---|
| Store-backed multi-account selection | ✅ | ✅ | ✅ | ✅ | PF snapshots + account/token caches off the hot path. |
| Named account pools (first-class routable) | ✅ | 🟡 | ✅ | 🟡 | PF pooled ingress paths + per-pool selectors; codex-lb has a single global pool. |
| Provider partitioning of pools | ✅ | 🟡 | ✅ | ✅ | PF `Provider` enum ensures `/responses` picks only Codex accounts. |
| Per-API-key account scoping (multi-tenant) | ❌ | ✅ | 🟡 | 🟡 | codex-lb `ApiKeyAccountAssignment`. |

### Quota / Usage / Rate-limit
| Feature | PF | CLB | CC | CP | Note |
|---|:--:|:--:|:--:|:--:|---|
| Runtime usage-window tracking (5h + weekly) | ✅ | ✅ | ✅ | ❌ | PF polls `/wham/usage` every 600s + duration-aware classification. |
| Live error/cooldown state written from failures | ❌ | ✅ | ✅ | ✅ | PF's cooldown/error fields are never written; only OAuth-permanent + usage-status changes are live. |
| Depletion / pace / burn-rate forecasting | ❌ | ✅ | 🟡 | ❌ | codex-lb `WeeklyCreditPace`; ccflare server-computed exhaustion prediction. |
| Per-request token/cost accounting (native path) | ❌ | ✅ | ✅ | ❌ | PF `request_log` has the columns but the native path fills only method/path/provider/status/duration. |
| Reset-time epochs + dashboard countdowns | ✅ | ✅ | ✅ | ❌ | PF live 1s-ticking countdown pills. |
| Adaptive 429/529 backoff + force-reset | ❌ | ✅ | ✅ | ✅ | ccflare is the specialist (unified rate-limit header semantics). |
| Per-API-key usage limits | ❌ | ✅ | 🟡 | ❌ | codex-lb `LimitRule` across token/cost/credit windows. |

### Observability & Metrics
| Feature | PF | CLB | CC | CP | Note |
|---|:--:|:--:|:--:|:--:|---|
| Content-safe per-request completion logging | ✅ | ✅ | ✅ | ✅ | PF emits exactly one leak-tested tracing event per request. |
| Persisted queryable request-log history | ✅ | ✅ | ✅ | 🟡 | PF content-safe subset of codex-lb's 53 cols. |
| Latency percentiles (p95) / phase breakdown | ❌ | ✅ | ✅ | ❌ | PF has a latency-regression test but no p95 analytics surface. |
| Prometheus / external metrics endpoint | ❌ | ✅ | ❌ | ❌ | codex-lb exposes ~40 metrics. |
| Live log streaming (SSE) | ❌ | ❌ | ✅ | ❌ | ccflare `/api/logs/stream`. |
| Audit log / conversation archive | ❌ | ✅ | 🟡 | ❌ | PF persists no conversation content by design. |

### Dashboard & UI
| Feature | PF | CLB | CC | CP | Note |
|---|:--:|:--:|:--:|:--:|---|
| Embedded SPA served from the binary | ✅ | ✅ | ✅ | ✅ | PF rust-embed React/Vite at `/dashboard`. |
| Rich analytics dashboard (charts, p95, cost splits) | 🟡 | ✅ | ✅ | ❌ | PF has account/pool/request views + countdowns but no charting dataset. **ccflare/codex-lb dashboards are their crown jewels.** |
| Inline account settings edits from dashboard | ✅ | ✅ | ✅ | ✅ | PF `PATCH /api/accounts/{id}` pool/policy/pause. |
| Internationalization | ❌ | ✅ | ❌ | ❌ | codex-lb EN + zh-CN. |

### Admin / Auth / Security
| Feature | PF | CLB | CC | CP | Note |
|---|:--:|:--:|:--:|:--:|---|
| **Admin / API-key auth on management + proxy endpoints** | ❌ | ✅ | ✅ | ✅ | **PF: ALL endpoints unauthenticated (network-boundary only).** Planned `POLYFLARE_ADMIN_TOKEN`. |
| 2FA / TOTP | ❌ | ✅ | ❌ | ❌ | codex-lb dashboard TOTP. |
| IP allowlist / firewall | ❌ | ✅ | 🟡 | ❌ | codex-lb `ApiFirewallAllowlist`. |
| TLS serving of API/dashboard | ❌ | 🟡 | ✅ | ✅ | ccflare `SSL_KEY/CERT`; cliproxy `tls.enable`. |
| RBAC / roles | ❌ | 🟡 | ❌ | ❌ | codex-lb has a guest role. |

### Model Catalog
| Feature | PF | CLB | CC | CP | Note |
|---|:--:|:--:|:--:|:--:|---|
| Model catalog endpoints (content-negotiated shapes) | ✅ | ✅ | ✅ | ✅ | PF `/models`, `/v1/models`, `/backend-api/codex/models`. |
| Live upstream catalog fetch/merge | 🟡 | ✅ | ✅ | ✅ | PF Codex side is a hardcoded 5-slug bootstrap floor. |
| Bidirectional model aliasing (tier → model + effort) | ✅ | ✅ | ✅ | ✅ | PF `synthetic_models` single source of truth. |
| Reasoning-effort levels + ultra→max wire alias | 🟡 | ✅ | 🟡 | ❌ | PF effort shape speculative (U4); codex-lb mirrors codex-rs ultra→max. |
| Pricing data + cost accounting | ❌ | ✅ | ✅ | ❌ | PF computes no cost on the native path. |

### Coordination & Clustering
| Feature | PF | CLB | CC | CP | Note |
|---|:--:|:--:|:--:|:--:|---|
| In-process concurrency coordination | ✅ | ✅ | ✅ | ✅ | PF `RefreshLocks` + cache-generation atomics + observe-before-end ordering. |
| Multi-instance / distributed coordination | ❌ | 🟡 | 🟡 | 🟡 | PF single-binary by target; others bolt on via shared DB. |
| Leader election for schedulers | ❌ | 🟡 | ❌ | ❌ | codex-lb has it but disabled on default SQLite. |

### Retention & Automations
| Feature | PF | CLB | CC | CP | Note |
|---|:--:|:--:|:--:|:--:|---|
| Background automation loops | ✅ | ✅ | ✅ | ✅ | PF usage-refresh + codex-version loops. |
| Data retention / pruning / soft-delete | ❌ | ✅ | ✅ | 🟡 | **PF anchors/sessions/usage/request_log all grow unbounded**; `deleted_at` is importer-set only. |
| Scheduled keep-warm / limit-warmup automations | ❌ | ✅ | 🟡 | ❌ | PF synthetic warming deliberately OFF. |

### Upstream Proxy & Config
| Feature | PF | CLB | CC | CP | Note |
|---|:--:|:--:|:--:|:--:|---|
| Per-provider upstream base-URL override | ✅ | ✅ | ✅ | ✅ | PF env-driven Codex/Anthropic/auth retarget. |
| Per-account egress HTTP/SOCKS proxy routing | ❌ | ✅ | ❌ | ✅ | codex-lb `ProxyPool/AccountProxyBinding`. Matters for per-account IP diversity / anti-ban. |
| Config file (TOML/YAML/JSON) + hot reload | ❌ | 🟡 | ✅ | ✅ | PF is env-only, no file, no hot reload. |
| Declarative payload-override engine (JSON-path mutation) | ❌ | ❌ | ❌ | ✅ | CLIProxyAPI default/override/filter with predicates. |
| PostgreSQL storage option | ❌ | ✅ | ✅ | ✅ | PF SQLite/WAL only (single-binary target). |

### Extensibility & Store
| Feature | PF | CLB | CC | CP | Note |
|---|:--:|:--:|:--:|:--:|---|
| SQLite store + at-rest token crypto | ✅ | ✅ | ✅ | 🟡 | PF sqlx WAL + XChaCha20-Poly1305. |
| Zero-re-auth importer from predecessor pool | ✅ | — | — | — | **PF Fernet→XChaCha codex-lb importer: ~148k rows + 5 accounts, zero re-auth.** |
| Reusable SDK — custom executors + translators | 🟡 | ❌ | ❌ | ✅ | PF has internal trait seams but no public embeddable SDK; cliproxy ships one. |
| Runtime plugin system | ❌ | ❌ | ❌ | ✅ | cliproxy plugin host (also a maintenance/security surface). |

## What PolyFlare already has that the others don't

- **The watchdog + structurally-fixed `store:false` wedge** — the entire reason the rebuild exists.
  codex-lb still carries the wedge defect; nobody else has the state machine.
- **Real TLS/JA3 control** with a post-quantum key share, capture-verified against a real Codex CLI,
  guarded by a **fingerprint-parity CI gate** — no reference system controls the TLS layer this way.
- The **novel `cache_affinity_tier` strategy** (pack cheap tiers onto near-limit accounts, preserve
  fresh capacity for expensive ones) — PolyFlare-original.
- **At-rest token encryption + a zero-re-auth importer** from codex-lb (ccflare stores tokens in
  plaintext).
- A single self-contained **tokio binary** — no leader-election / cache-poller / bridge-ring
  coordination debt.

## What PolyFlare does NOT have yet — prioritized gaps

**HIGH — make the already-built routing actually work / basic hardening**
1. **Live failure-driven health/cooldown tracking.** `health_tier`/`error_count`/`cooldown_until`/
   `last_error_at` exist as columns but nothing writes them on an upstream failure, so
   `capacity_weighted` runs on neutral data and `round_robin` degenerates to an id tiebreak.
   *(codex-lb is the same-domain reference for the port; a focused codex-lb deep-study feeds the
   detailed porting recipes.)*
2. **Admin / API-key auth.** Every endpoint (ingress, `/api/*`, dashboard) is unauthenticated,
   relying only on the network boundary. Planned `POLYFLARE_ADMIN_TOKEN`. **Headline security caveat.**
3. **Anti-starvation fallback + general retry/failover across accounts.** PolyFlare 503s an empty
   eligible pool and only fails over on the no-anchor recovery path.
4. **Verify the speculative alias pairs + `reasoning.effort` wire shape (U2/U4).** The flagship
   Claude→Codex translation targets (`gpt-5.6-sol/terra/luna`, `{reasoning:{effort}}`) are unverified
   placeholders; if wrong, the marquee feature is broken.

**MEDIUM**
5. **Prompt-cache stickiness** — derive a `prompt_cache_key` on the translated path (see the adoption
   brief below). High value / low effort; PolyFlare already has the routing-affinity half.
6. **Anthropic-side OAuth refresh + login** — Anthropic accounts silently expire today.
7. **Per-request token/cost accounting** on the native path (columns exist, unfilled).
8. **Data-retention pruning** — several tables grow unbounded.
9. **Live upstream model-catalog fetch/merge** (Codex side is a hardcoded floor).
10. **Prometheus metrics endpoint** and **per-account egress proxy routing**.
11. **Rich analytics dashboard** (charts, p95, cost/token splits).

**LOW**
12. Config file + hot reload · N×M translator registry as the *live* path · Codex WebSocket transport ·
    depletion/pace forecasting · declarative payload-override engine.

---

## Adoption brief: prompt-cache (`prompt_cache_key`) — from better-ccflare

better-ccflare runs **two** independent cache subsystems. Only the first is on PolyFlare's critical
path today; the rest are Anthropic-`/v1/messages`-only and lower priority.

### 1. `prompt_cache_key` derivation — build this first

OpenAI's Responses backend routes a prompt to a cache machine by hashing *(prompt-prefix +
`prompt_cache_key`)*. A cache hit makes `cache_read` input tokens **~10× cheaper** and skips
re-processing the large prefix (**lower TTFB**). The key exists purely to make the *same conversation*
land on the *same warm cache machine* deterministically.

**The load-bearing insight — anti-thrash keying** (`packages/providers/src/providers/codex/provider.ts`
`derivePromptCacheKey()` :975-1010): a Claude Code session multiplexes its main loop **and every
subagent** over one `session_id`. Keying the cache on `session_id` alone funnels a 170-conversation,
5-minute subagent fan-out onto one cache machine, blows past OpenAI's ~15-req/min-per-key guidance,
and degrades caching to cold-start. ccflare partitions **per conversation** instead:

- default: `"ccflare-convo-" + sha256(sessionId \0 instructions \0 JSON(input[0]))[:48]`
- session mode (`CCFLARE_CODEX_CACHE_KEY_MODE=session`, or when `input` is empty):
  `"ccflare-session-" + sha256(sessionId)[:48]`

Reimplementation checklist for PolyFlare:
- Session identity comes from `body.metadata.user_id` (Claude Code's convention; validate a UUID).
- **Gate on host** — only attach it for `chatgpt.com` / `api.openai.com` (a custom endpoint may 400 on
  the unknown field).
- Truncate to 48 hex (< OpenAI's 64-char bound); raw session/prompt content never leaks into the key.
- Request shape is always `stream:true, store:false` — caching is prompt-*prefix* based, which is
  exactly why full-resend + a stable key is sufficient (this matches PolyFlare's existing design).

**Where PolyFlare stands:** on the **native** `/responses` path the real Codex client already sends its
own `prompt_cache_key`, and PolyFlare forwards the bytes verbatim — so that path is covered. The gap is
the **translated alias path** (`messages_handler_codex_aliased`): it builds a fresh Codex body, sets
neither a `prompt_cache_key` **nor** a session/ownership pin (`NoopContinuity`). So aliased Claude Code
traffic gets no cache affinity today. That path is where the derivation + a session pin should land.

### 2. Cache ↔ routing coupling — ship *with* #1

A derived key is wasted if the next turn lands on a different account and misses that account's warm
cache. ccflare pins `clientId → account` stickily (`load-balancer/strategies/session-affinity.ts`) and —
crucially — **does not delete the mapping on a 429**; it fails over temporarily and snaps back on
recovery, because the prompt-cache window outlives the rate-limit window. PolyFlare already has the
Codex ownership pin (M3); the work is aligning the *translated* path with it and keying it on the same
session identity as the cache key.

### 3. The Anthropic cache triad — measure-then-maybe (defer)

`cacheBodyStore` + `CacheKeepaliveScheduler` + `injectSystemCacheTtl` keep Anthropic's ~5-min ephemeral
`cache_control` blocks warm (a `max_tokens=1` self-loop replay just before expiry; a 5min→1h TTL
upgrade). These are Anthropic-`/v1/messages`-specific — OpenAI's cache TTL is provider-managed and not
client-settable — so they have no Codex equivalent and should wait until PolyFlare measures OpenAI's
real retention window. **The keepalive needs strict loop-prevention discipline** (its replays must skip
staging *and* rate-limit cooldown) before it can be safely enabled.

### Also worth stealing from ccflare (provider-agnostic)

- **Rate-limit taxonomy** (`anthropic/provider.ts` `parseRateLimit` :336-464): distinguishes
  `out_of_credits` vs `extra_usage` vs `429/529`, and **clamps every reset to now+24h** to defuse a
  pathological `Retry-After`. Directly applicable to Codex's 5h/weekly windows.
- **Pace-based proactive throttling** (`usage-throttling.ts`): benches an account *before* it 429s when
  consumption outruns the linear pace of its window.
- **Session-storm governor**: a per-client volume circuit breaker.

### Translation reference (for M4b hardening)

`codex/provider.ts` `convertToCodexFormat()` (:1012) and `handleCodexEvent()` (:1620), plus the
`openai-responses-adapter` package, are the most transferable files for PolyFlare's Anthropic↔Codex
translation. ccflare also confirms a PolyFlare design assumption: over HTTP, `previous_response_id` is
ignored, `store:false`, stateless full-history.
