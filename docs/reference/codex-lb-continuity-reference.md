# codex-lb Continuity / Anchor / Wedge — Reference for PolyFlare M3

Distilled from a full read of codex-lb (2026-07-14) for the M3 continuity engine. File:line refs are into the codex-lb tree at `../codex-lb` (HEAD `e41a381f`, branch `local`).

## The one fact that explains everything: `store:false` is forced, always
codex-lb **never** sends `store:true` upstream — a validator forces `store=false` on every Responses request (`app/core/openai/requests.py:665-668`, `:743,772-775`; `v1_requests.py:47-50`). No branch in the continuity code inspects `store` at all (0 grep hits). Yet it still uses `previous_response_id` for continuity and **trims history down to a bare anchor as if the anchored response were durably retrievable.**

It only works empirically because a `store:false` `previous_response_id` is an **ephemeral anchor** — resolvable *only while the specific upstream connection/turn-state that produced it is still alive* on OpenAI's side, not for the account's lifetime, not durably (`openspec/specs/responses-api-compat/context.md:34`). codex-lb's DB record of `latest_response_id` is **bookkeeping about what it believes it last sent — NOT proof upstream can still resume it.** Trimming based on that record is unsafe once the anchor is no longer live. **This is the root of the wedge.**

## Anchor injection (3 sites) + trim
All in `app/modules/proxy/_service/http_bridge/streaming.py` (`_stream_via_http_bridge`, 617-1852):
- **(A) Durable "fresh-reattach" injection** (`798-852`): fires when a durable DB record exists but no live local session / no active remote owner, client sent no `previous_response_id`, key strength `hard`. Injects `durable_lookup.latest_response_id`. **If the payload looks like a full-resend, injection is gated on a fingerprint match against codex-lb's own (stale) bookkeeping — NOT anchor liveness.** *This site is the wedge path and is NOT touched by the owner-guard.*
- **(B) Session-level injection** (`1331-1393`): for a live in-memory session that completed a turn. *This is the only site the owner-guard patches.*
- **(C) Native-WebSocket equivalent** (`websocket/helpers.py:409-432`): parallel `_WebSocketContinuityState`, **no account-owner field at all.**

**Store-context trim** (`streaming.py:1394-1434`): when an anchor is present and the incoming input's first `stored_count` items fingerprint-match the stored prefix, it sends **only `input[stored_count:]`** (the tail) + the anchor. On fingerprint MISmatch it logs `store_context_input_trim_skipped_prefix_mismatch` and skips the trim **but keeps the anchor attached** (sends untrimmed full payload + anchor). "Full resend" heuristic (`helpers.py:849-861`): **any multi-item input array counts as a full resend** (or a string ≥4096 chars, or 1 item ≥4096 chars serialized).

## The wedge, step by step
1. Client sends a full-resend (its own local state was lost → it re-sends the ENTIRE history, no `previous_response_id`) on a session with no live local bridge but a surviving durable row (after process restart / idle-TTL eviction / ring rebalance).
2. Site (A) injects `durable_lookup.latest_response_id` (a possibly-dead anchor) — gated only on a fingerprint match against stale bookkeeping.
3. The durable values are copied onto the (new) session; the store-context trim strips the client's full history down to just the new tail + the dead anchor.
4. Upstream is `store:false`, so the anchored response may be long gone → upstream **silently accepts `response.create` but never emits `response.created` OR any error** → none of codex-lb's reactive stale-anchor recovery fires (it's all triggered by explicit errors/closes).
5. The per-bridge `response_create_gate` (`asyncio.Semaphore(1)`) stays acquired; the holder's client eventually times out ("idle timeout waiting for SSE"); queued requests fail as "temporarily overloaded". The proxy's own idle timeout is **7200s** (`settings.py:166`) — useless.
6. The durable row is unchanged → the client's retry (another full-resend) walks the identical path → **wedges again** (recurring).

**Why a full-resend is special:** it is *evidence the client itself no longer trusts continuity* (it re-sent everything). Silently re-imposing the proxy's stale anchor and discarding the client's redundant-but-safe full context converts a self-healing retry into a guaranteed-unresolvable request.

## Why the anchor-owner guard doesn't fix it
Commit `2bd4ff82` (branch `origin/pr/anchor-owner-guard`, unmerged — NOT `e0f3da3f`/`d846fd0a`, which don't exist) adds ONE thing: an `account_id` equality check before the **session-level** injection (site B only). It guards *cross-account* identity, not *anchor liveness*, and never touches site (A) (the fresh-reattach/full-resend path) or the native-WS path. A **same-account** fresh-reattach passes the identity check trivially yet the anchor can be just as dead → same silent hang. The PR's own `tasks.md` lists the real fixes as deferred follow-ups: *"guard the durable direct-anchor injection that runs before account binding"*, *"proactive `response.created` watchdog that replays stored full-history payload on stall"*, *"audit the WebSocket-transport anchor path"*.

## Reasoning items: neither preserved nor replayed (M3 must ADD this)
codex-lb has **no local reasoning cache**. Reasoning rides inside the ephemeral `store:false` upstream response; codex-lb only *strips* reasoning-shaped items it thinks the anchor already carries (`helpers.py:460-495`, `requests.py:424-456`). It even detects & discards a client's fake "Local compact fallback preserved the latest encrypted reasoning state" placeholder (`requests.py:793-856`) — confirming it has NO server-side way to reconstruct reasoning when the anchor dies. When the anchor goes stale, reasoning context is unrecoverably lost. **M3's reasoning-replay cache is new work, not a port.**

## Watchdog: none (M3 must ADD this)
No proactive watchdog on the anchor/response-create step. Only: (1) a 7200s client-facing stream-idle timeout (fires long after the client gave up); (2) a *reactive* "stuck-gate retirement" that needs a SECOND request to contend on the gate (≥300s stuck) to evict the session for FUTURE requests — it never unsticks the wedged holder, and never fires without contention. Extensive reactive recovery exists for *explicit* upstream errors (`previous_response_not_found`, WS closes) — but the wedge is defined by upstream sending NEITHER. **M3 needs a proactive watchdog: on submitting a proxy-injected anchor, start a bounded timer (seconds, not 7200s); on silence → treat the anchor as dead, release the gate-equivalent, and replay the full history WITHOUT the anchor.**

## Session identity + anchor state (primitives to model)
- Session key: `x-codex-turn-state` header → `hard`; else session header → `hard` (+ `prompt_cache_key` isolates threads); else soft `prompt_cache`/`request_id` (`helpers.py:988-1064`).
- In-proc anchor state: `last_completed_response_id` / `last_completed_input_count` / `last_completed_input_prefix_fingerprint` (`support.py:523-525`).
- Durable cross-process: `HttpBridgeSessionRecord.latest_response_id / latest_input_item_count / latest_input_full_fingerprint / account_id` (`db/models.py:1436-1496`) — **records what codex-lb sent, NOT upstream resolvability.**
- Transport: downstream is SSE; upstream is a persistent WS session per bridge (multiplexing turns) — plus a separate native-WS surface. PolyFlare M1/M2 is HTTP-SSE pass-through only (no WS bridge yet), so PolyFlare introduces continuity fresh over its simpler architecture.

## What M3's fix must do (the design — see DESIGN-DECISIONS C1 / POLYFLARE-DESIGN §4.1)
1. **R1 — never trim a `store:false` full-resend** to a DB/bookkeeping-derived anchor. Only trim when there's a live guarantee the anchor resolves (or don't trim at all — full-resend is always safe, just costs tokens). A full-resend must be sent in full.
2. **R2 — proactive watchdog** on any request carrying a proxy-injected anchor: bounded timer; on no `response.created`/first-token within N seconds → anchor is dead → release + fall back to full-history resend without the anchor. (The trigger codex-lb lacks: *silence*, not just errors.)
3. **R3 — reasoning-replay cache**: cache reasoning items so a reattach/recover can replay them instead of losing them (codex-lb has nothing here).
4. Model it as an **explicit per-conversation state machine** (Fresh → Anchored → Reattaching → {Anchored | Recover}) with the anchor state persisted, so a non-resuming anchor is always detected and recovered rather than silently hanging.
