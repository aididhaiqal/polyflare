# SPEC-M5 — WebSocket Transport

**Goal.** Ride WebSocket to the upstream account so a continuation turn uploads a delta instead of the
whole history, and the history is billed once instead of every turn — without inheriting codex-lb's wedge.

**Why now.** Measured: 582 B vs 50,308 B per continuation turn (**86×**), history prefilled once
(`TRANSPORT-FINDINGS-2026-07-17.md` §2). With accounts limit-constrained, this is the largest single lever
PolyFlare has. The risky unknown is already retired — the probes proved the wire protocol live, and a dead
anchor turned out to be a fast catchable 400, not a silent hang.

**Inputs.** `WS-GROUND-TRUTH-CODEX.md` (what real codex-rs does, cited to `codex-rs` file:line — the
authority for every wire detail below), `TRANSPORT-FINDINGS-2026-07-17.md` (D1–D5, the live measurements),
`DESIGN-DECISIONS.md` E4 (transport sits *below* the `Executor` trait), `SPEC-M3.md` (ownership + continuity,
already built).

**Status.** Design approved; not built. Two milestones, sequenced deliberately (§6).

---

## 1. Shape

```
codex  --WS-->  PolyFlare  --WS-->  account        (M5b)
codex --HTTP-->  PolyFlare  --WS-->  account        (M5a)
```

The two sides are **separable**, and that is the spec's central structural claim. The upstream side carries
the entire token win. The client-facing side only removes a *localhost* hop — but it is what forces
PolyFlare to hold conversation state (§5). So they ship in that order.

Transport lives below `Executor` (E4), in `polyflare-codex`. Continuity (M3) and translation stay above it
and stay transport-agnostic.

## 2. The delta decision (M5a)

Over HTTP the client hands PolyFlare the **full history every turn**. That is what makes M5a free of state:
PolyFlare already holds everything needed to send either shape.

Per turn, on the owner's live socket:

| Condition | Sent upstream |
|---|---|
| socket alive **and** new input is a strict extension of what this socket last sent **and** non-input fields match | **suffix items + `previous_response_id`** |
| anything else | **full input, no anchor** |

This mirrors codex's own rule — `get_incremental_items` + `responses_request_properties_match`
(`client.rs:306-359,1222-1253`). Non-input fields (model, instructions, tools, tool_choice, reasoning,
service_tier, text, …) differing ⇒ full request; do not try to be cleverer than codex here.

**The anchor is a property of the live socket, never of the database.** Ground truth §7.5: a reconnect zeroes
incremental state and the next request carries no anchor. `WsConn` owns `{last_response_id, last_input_count,
last_input_fingerprint}`; dropping the connection drops them. M3's durable `continuity_sessions` row keeps its
existing job (ownership routing — D3) and must **not** be read back as a live WS anchor.

## 3. Handshake and fingerprint

Byte-parity with codex-rs is a project-level commitment (E4(a)), and the WS handshake is a *new* egress
surface — it gets the same treatment as the HTTP one. `WS-GROUND-TRUTH-CODEX.md` §1 is the contract:

- URL = `{base}/responses` with scheme `https→wss`.
- Header set + insertion order per §1, ending with `Authorization: Bearer <account token>` and
  `ChatGPT-Account-ID` (the same values `ingress.rs` already synthesizes for HTTP).
- `OpenAI-Beta: responses_websockets=2026-02-06`, `.insert()` — exactly once.
- **`x-codex-turn-state` MUST NOT appear as a handshake header** (§7.1) — WS carries it only inside
  `client_metadata`, and only after the server has supplied one. This is the exact opposite of the HTTP path
  and is the single easiest thing to get wrong.
- Offer `permessage-deflate` (§1). Never send a client-initiated Ping; auto-Pong only (§2).
- Dial with the pinned `rustls` stack, not a second TLS configuration.

**Gate:** extend the existing fingerprint capture/parity CI gate to cover the WS handshake, captured from the
real CLI the same way the HTTP golden is. Wire header *byte order* is flagged unverified in ground truth §1 —
the capture resolves it; do not guess.

## 4. Failure and recovery (M5a)

PolyFlare owns recovery entirely. Ground truth §5 is unambiguous: `previous_response_not_found` has **zero**
occurrences in codex-rs — the client has no handling, so a forwarded anchor error burns its retry budget and
can silently disable WS for the whole conversation.

| Upstream event | PolyFlare does | Client sees |
|---|---|---|
| `previous_response_not_found` | strip anchor → **full resend on the same socket** | nothing |
| 429 | record rate-limit cooldown (existing `runtime_state`) → **failover**: next account, full resend | nothing |
| `websocket_connection_limit_reached` (60-min cap) | reconnect → full resend | nothing |
| idle timeout (300 s, per-event) | reconnect → full resend | nothing |
| `Close` before `response.completed` | reconnect → full resend | nothing |
| handshake 426 | HTTP-SSE for this session | nothing |
| handshake/transport failure | HTTP-SSE for this turn | nothing |
| `response.failed` with a terminal code (quota, context-window, cyber-policy, invalid-request) | pass through | the error |

Two invariants:

- **Every recovery path terminates.** Each is a *bounded* retry with an attempt counter, not a loop. A resend
  that fails again escalates (next account → HTTP → error), never re-attempts indefinitely.
- **Recovery never re-bills silently without a record.** A full resend after an anchor miss costs a full
  history re-prefill; log it (content-free: counts + reason code) so the wedge rate is measurable. codex-lb
  wedged ~31% and nobody could see it.

**Fallback policy — deliberate divergence from codex.** codex flips `disable_websockets` one-way for the
session (§5). PolyFlare is a long-lived multi-tenant server, so a permanent process-wide switch is wrong:
fallback is scoped **per session** (426) or **per turn** (transport), with a cooldown before re-attempting WS
for that account. Diverging here is a decision, not an oversight.

## 5. Client-facing WS (M5b) — and the state it forces

A WS client sends **deltas**, so PolyFlare cannot reconstruct history from the request alone. To fail over to
another account it must hold the conversation itself. This is the constraint that produced codex-lb's
in-memory bridge — and its wedge.

**Design:**

- Terminate the upgrade on `/responses` (replacing the 426 shim, `ingress.rs::websocket_fallback_handler`).
- Per client connection: `WsSession { items: Vec<Item>, owner, upstream: Option<WsConn>, … }`. Each inbound
  `response.create` delta is appended → `items` is the full history.
- **The client's `previous_response_id` is ignored** (sanity-checked, not translated). PolyFlare reconstructs
  from its own `items`, so it never needs to map a client-visible id onto a per-account upstream id. This is
  what deletes codex-lb's `should_rewrite`/trim machinery rather than reimplementing it: the id-rewriting
  problem only exists if you refuse to hold the history.
- Downstream, every turn is then §2's two-case decision. Failover is invisible to the client.
- **No unbounded gate.** One in-flight turn per socket is codex's own model (§4) and PolyFlare needs the same
  serialization — but codex-lb's `response_create_gate = Semaphore(1)` is precisely its wedge amplifier: a
  wedged holder blocks every queued turn. Ours is a **bounded wait + the M3 silence watchdog**; a stalled turn
  is cancelled, never left holding the gate.

**Content fence (hard).** `WsSession.items` is the only place PolyFlare holds conversation content. It is
**RAM-only**: never persisted, never logged, never surfaced through `/api/*` or the log bus, dropped on
session close. It gets a redacting `Debug` and a test asserting content cannot reach a log line — the same
treatment secrets get. This is a narrow, fenced exception to "PolyFlare persists no conversation content", and
it is why M5b is sequenced second.

**Bounded memory.** `items` grows with the conversation. Cap per session and in aggregate; on exceed, the
session degrades to **owner-pinned, no-failover** (and says so in a metric) rather than silently evicting the
history that failover depends on. An unbounded `Vec` per client connection is a memory-exhaustion surface.

## 6. Milestones

**M5a — upstream WS.** Client stays HTTP (`supports_websockets = false`). Delivers the whole token win, needs
zero conversation retention, and cannot wedge — there is nothing to hold. Flag-gated
(`POLYFLARE_WS_UPSTREAM`), default off, HTTP-SSE remains the fallback on every path.

**M5b — client-facing WS.** The upgrade handshake, `WsSession` accumulation, invisible failover, the content
fence. Flag-gated separately.

Sequenced this way because M5a banks the value with no new state; if M5b's accumulation proves uglier than it
looks here, the win is already in hand.

## 7. Non-goals

WS for the Anthropic `/v1/messages` path. Multiplexing concurrent turns on one socket (codex is strictly
one-at-a-time, §4 — do not invent a protocol the server has never seen). Predictive standby prewarm via
`generate:false` — proven cheap (~0.55 s) and reserved for a failure-signal trigger (D5), a follow-up, not
unconditional 2× cost. Removing the HTTP path — it stays the fallback and the wedge-immune floor.

## 8. Risks

- **The 86× is an upload measurement.** Billing follows `previous_response_id` prefill-once, which the probe
  observed but did not price. Whether cached/prefilled tokens count against *rate* limits the way they count
  for billing is **unverified** — and rate limits are the actual constraint. Measure this in M5a before
  claiming a quota win.
- **Version coupling.** Wire constants are pinned to codex 0.144.x. A CLI bump can move the `OpenAI-Beta`
  value or the header set; the parity gate catches it, that's its job.
- **M5b is where wedges live.** Every mitigation in §5 exists because codex-lb has the scars. If the wedge
  rate is not measurable from day one (§4), it is not being managed.
