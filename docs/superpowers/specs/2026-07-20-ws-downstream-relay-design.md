# WS-to-WS Relay (WS-downstream) — Design

**Status:** Design approved 2026-07-20. Next: implementation plan (writing-plans) → Phase-0 gating spike.

**One-liner:** Add a native WS-downstream transport where the codex CLI speaks WebSocket to PolyFlare and PolyFlare **relays** frames to an upstream WebSocket on the conversation's pinned account — replacing the M5a HTTP→WS *transport reshape* for native codex clients, and structurally avoiding the anchor "wedge" by pinning per conversation and only ever moving accounts on durable exhaustion (where the client itself resolves the anchor).

---

## 1. Motivation & the two "reshapes"

PolyFlare's current WS-upstream (M5a, behind `POLYFLARE_WS_UPSTREAM`, default off) is an awkward middle-ground: the client speaks **HTTP-SSE** to PolyFlare, so to use WS upstream PolyFlare must **reshape** the HTTP body into a codex WS `response.create` frame and **synthesize** `client_metadata` (timing, turn-state, traceparent) that a real codex WS client would have produced. That synthesis is the entire class of fidelity work in the `codex-fingerprint-gaps` line.

codex-lb (and real codex end-to-end) avoid this by **relaying**: the codex CLI holds the WebSocket and builds its own frames; the proxy forwards them verbatim (normalizing only handshake headers). Relayed frames carry all `client_metadata` **for free** — zero synthesis, zero fingerprint gap.

**Two distinct "reshapes" — only one is retired:**

| Reshape | What it is | Fate |
|---|---|---|
| **Translation** (Claude → Codex) | `/v1/messages` → `/responses` body; the claudex bridge (`derive_alias_prompt_cache_key`, `synthesize_codex_forward_headers`, translator) | **Stays, untouched** — core feature, unrelated to transport |
| **Transport** (codex-HTTP-body → codex-WS-frame) | M5a's `build_response_create` + `client_metadata` synthesis | **Retired for native WS clients** (relay replaces it); becomes **vestigial** — only reachable if we ever wanted *aliased* requests on WS-upstream, which we don't. Do not delete now; retire after the relay proves out. |

A Claude client is inherently HTTP-downstream (`/v1/messages`), so it can **never** use the relay (no client WS to relay) — the aliased path is always translate → **HTTP-upstream** (already caches ~95% via the alias key).

### Transport matrix (the whole surface)

| Client | Downstream | Path | Upstream |
|---|---|---|---|
| native codex (WS-capable) | **WS** | **WS relay (NEW)** — pin account via ownership, reconnect same-account | WS |
| native codex | HTTP-SSE | existing default (unchanged) | HTTP-SSE |
| Claude (aliased) | HTTP `/v1/messages` | translation reshape (unchanged) | HTTP-SSE |
| — | — | M5a transport reshape | vestigial, retire later |

The relay is **strictly additive**: HTTP-SSE and translation code paths are not modified.

---

## 2. Architecture & boundaries

- A new ingress endpoint: a WebSocket upgrade at the codex `/responses` WS path. Gated by `POLYFLARE_WS_DOWNSTREAM` (default off; fail-safe convention like `ws_upstream`/`live_logs`).
- Enabled client-side by the codex CLI's own provider config (`supports_websockets = true` + a `ws://`/`wss://` base — codex gates WS on `provider.info().supports_websockets`, `codex-rs/core/src/client.rs:930`).
- On connect: accept the client WS → derive `session_key` from handshake/first-frame identity → resolve/create the conversation's **owner account** via the existing M3 ownership + selection engine → open **one** upstream WS to that account → pump frames both ways verbatim.
- Only handshake **headers** are touched (existing fingerprint-normalization + auth/account override, mirroring `ws::conn`); frame **bodies** are never modified or persisted.

**Isolation:** the relay is its own module (`ws_relay`), with clear seams to (a) the selection/ownership engine, (b) the WS dial/handshake (reuse `ws::conn` for the upstream leg), (c) a content-free response-id sniff. HTTP path, translation, and the M5a executor are untouched.

---

## 3. The relay loop

Modeled on codex-rs's own WS pump (`codex-rs/codex-api/src/endpoint/responses_websocket.rs:62-125`): a per-socket task with `tokio::select!` over an outbound **command channel** and `inner.next()` inbound reads, fanning received frames into a bounded **mpsc** (codex uses 1600 for events, 32 for commands).

- **downstream→upstream:** forward the client's `response.create` frames **verbatim** (handshake already normalized; bodies never touched).
- **upstream→downstream:** forward backend frames **verbatim**.
- **Ping** auto-ponged inline on each leg (codex's inline pong; ignore inbound Pong). No client-initiated pings (codex-rs fidelity).
- **Close/error** on either leg tears down both — after Section 4's reconnect logic decides whether to transparently re-dial the upstream first.
- **Content-free sniff only:** parse just enough to read `response.completed`'s `response.id` (feed the ownership map) — the same content-free sniff the HTTP watchdog already does (`ResponseIdSniffer`). No other body parsing.
- **Backpressure:** bounded channels; a brief client-frame buffer covers an in-flight upstream re-dial.

---

## 4. Account-pinning, reconnect-vs-move, and the 60-min cap (wedge avoidance)

**Pinning.** One downstream WS = one conversation. The upstream account is chosen once (healthy-account selection) and **pinned** in memory for the connection's life; the sniffed `response_id → owner` writes let a *future* reconnected/HTTP turn find the same owner. Stickiness is to the **account**, not the socket.

**Reconnect = same account (transparent).** The upstream WS is PolyFlare's to manage; the downstream stays open. On any **non-exhaustion** upstream drop — network blip, idle, or the **60-minute connection-limit frame** (`websocket_connection_limit_reached`, `responses_websocket.rs:158-159`) — PolyFlare **re-dials the same owner account**, buffering any pending client frame, and the conversation continues. The client never sees the drop; the 60-min cap becomes invisible. Same account → the client's `previous_response_id` anchor resumes → **no cross-account event → no wedge.**

**Move = new account (durable exhaustion only).** Only when the owner is genuinely exhausted (a 429/quota the existing cooldown/circuit-breaker classifies as not-soon-clearing) does PolyFlare re-select a new owner, update the ownership map, and re-dial upstream. The anchor is now cross-account → backend returns `previous_response_not_found` → PolyFlare **forwards that frame to the client** → codex-rs drops the anchor and **full-resends** (uncached that turn, correct). PolyFlare rewrites nothing; the wedge is the client's to resolve over the relay. Transient 429s **retry the same account** (no move).

**Residual wedge = the rare same-account full-resend that doesn't resume** (~31% of reattaches in the old `sol-anchor-wedge-rootcause` note) → the **watchdog**, **deferred** (measure on our accounts in Phase 3, add only if the data shows it).

**Net wedge surface:** never on reconnect; client-owned on exhaustion-move; watchdog on the rare residual.

---

## 5. What to adopt from codex-rs (and what "WS v2" actually is)

- **"WS v2" is a red herring for responses:** `RESPONSES_WEBSOCKETS_V2_BETA_HEADER_VALUE` = `responses_websockets=2026-02-06` (the exact value PolyFlare sends) — a "v2"-named constant, unchanged value. The real `protocol_v2`/`methods_v2` are the **realtime/voice** WS, a different endpoint, irrelevant here. Nothing to adopt from a "v2 protocol."
- **Adopt the pump architecture** (§3) — proven bidirectional shape.
- **Adopt explicit 60-min-cap handling** → transparent same-account re-dial (§4).
- **Adopt `connection_reused` / `warmup` telemetry** (`responses_websocket.rs:175-176`, 267-268) — closes the "no `connection_reused` telemetry" gap the lifecycle study flagged (content-free counters, not values).

---

## 6. Failure modes & edge cases

- **Handshake (no conversation yet):** auth failure / account-deactivated / circuit-open → pick another healthy account before pinning. Upstream `426` → surface (not expected from chatgpt.com; no downstream-transport downgrade in the experiment).
- **Mid-stream (reuse existing 429/error classification):** network/idle/60-min → reconnect same; transient 429 → retry same; durable 429 → move; `previous_response_not_found` and any other backend error frame → forward verbatim.
- **Downstream disconnect:** client closes → tear down upstream, release the account lease, finalize ownership (symmetric to HTTP stream-end).
- **Content-safety (inviolable):** never log/persist a frame body; only the content-free response-id sniff + handshake-header normalization. No new content-safety surface, no conversation content stored (PolyFlare's permanent limit).
- **One in-flight turn per socket** (codex's model) — the pump serializes; the owned upstream lock mirrors the M5a concurrency guarantee.

---

## 7. Testing & experiment phases

All behind `POLYFLARE_WS_DOWNSTREAM` (default off); HTTP-SSE + translation untouched.

- **Phase 0 — gating spike (first, cheap):** can the real codex CLI be pointed at PolyFlare over WS-downstream (`supports_websockets=true` + `ws://` base) and complete one turn through a trivial pass-through relay? The single biggest unknown — if the CLI won't WS to a custom provider, the whole idea is blocked. Prove it before building.
- **Phase 1 — sticky relay MVP:** one conversation, one pinned account, bidirectional pump, ownership sniff, content-safe; **confirm caching** (native incremental over the relay, expect ~72% like direct codex).
- **Phase 2 — transparent same-account reconnect**, including a forced 60-min-cap re-dial.
- **Phase 3 — exhaustion-move** (client full-resend relayed) + **measure the residual** same-account non-resumption → decide the watchdog.
- **Testing approach:** mock-WS-downstream + mock-WS-upstream relay-through tests (pump, pinning, reconnect, error classification); live-verify with the real CLI against real chatgpt.com at each phase (the "distrust green-but-vacuous tests / live-probe" discipline).

---

## 8. Global constraints

- Content-free: no conversation content ever persisted/logged (frame bodies never touched beyond the response-id sniff).
- Wedge-sacred: the ownership/continuity engine and the HTTP `ObservingStream` are reused, not modified; the relay feeds ownership via the same content-free sniff.
- Flag-gated (`POLYFLARE_WS_DOWNSTREAM`, default off); additive; HTTP-SSE and translation paths byte-unchanged.
- codex-rs fidelity: relayed frames are the real client's, so fidelity is inherited; the only synthesized bytes are the normalized handshake headers (existing fingerprint layer). No client-initiated pings (default codex-rs behavior).

## 9. Open questions / deferred

- **Watchdog for the residual same-account non-resumption** — deferred to Phase-3 measurement.
- **Exact `session_key` derivation from a WS handshake** (which headers/first-frame fields identify the conversation) — pin in the plan against a real Phase-0 capture.
- **Retiring the M5a transport reshape** — after the relay proves out; not in scope now.
