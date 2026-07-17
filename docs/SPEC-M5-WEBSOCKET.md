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

### 1a. What the seam actually is (surveyed, not assumed)

The `Executor` trait is one method (`polyflare-core/src/traits.rs:14-21`):

```rust
#[async_trait]
pub trait Executor: Send + Sync {
    async fn execute(&self, req: PreparedRequest, account: &Account) -> Result<ResponseStream, ExecError>;
}
```

`ResponseStream = Pin<Box<dyn Stream<Item = Result<Bytes, ExecError>> + Send>>` (`types.rs:87`). A WS executor
is a second `Executor` impl in `polyflare-codex` alongside `CodexExecutor`, wired via `AppState`
(`app.rs:31,85-90`, `state.executor_for(provider)`). Three consequences fall out, and each is load-bearing:

**(a) The stream must be SSE bytes, not WS frames.** Everything downstream parses `data:`-prefixed lines out
of the raw `Bytes` — `ResponseIdSniffer::extract_response_id` (`watchdog.rs:329-353`) and
`TranslatingStream::feed_line` (`translate_stream.rs:58-79`). So the WS executor must **re-serialize each
received JSON frame back into SSE framing** (`format!("data: {json}\n\n")`) inside its own `execute`. Handing
raw WS payloads through would silently break the watchdog's id-sniffing and the Anthropic translator.

**(b) The per-turn stream must END while the socket stays OPEN.** `Continuity::observe` — which writes the
`response_id → owner` anchor map — fires from `ObservingStream` at true stream end (`watchdog.rs:400-417`), and
`record_success`/`record_transient_error` writeback keys off the same terminal (`watchdog.rs:393-403`). A WS
connection deliberately outlives the turn (ground truth §2). So the executor must terminate its
`ResponseStream` at the terminal frame (`response.completed` / `response.failed` / `response.incomplete`) with
`Poll::Ready(None)` **without closing the socket**, and park the live connection back in its cache. Get this
wrong and continuity's turn-boundary model breaks — ownership stops being recorded, and every turn re-anchors.

**(c) The trait carries no session key.** `execute` receives only `PreparedRequest`
(`body: Option<Value>`, `model`, `forward_headers`, `raw_body: Option<Bytes>`) and `Account` — no `RequestCtx`,
no session key. But WS needs one to key its connection cache. Do **not** re-parse the body in the executor to
recover it: the native path is `raw_body: Some(_) / body: None` precisely so the big `input` tree is never
materialized (the 26–51% parse win, `session_key.rs::parse_inbound`). **Extend the seam** — the session key is
already computed in ingress; thread it down rather than recomputing. This is a trait change, so it lands as its
own task, first.

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

**Compare per-item HASHES, never items — this is what keeps M5a content-free.** "Strict extension" needs the
prior turn's items, and holding them would mean retaining conversation content in RAM on a long-lived
connection — precisely the fenced M5b exception that M5a is sequenced to avoid (§6). Instead `WsConn` carries
the per-item hash vector of what it last sent: extension ⇔ the stored hashes are a **prefix** of the new
input's hashes AND the new input is strictly longer; the suffix is the items past that prefix. Equivalent to
codex's item-by-item comparison modulo collision, at ~32 bytes/item and zero content. PolyFlare already
fingerprints input content-free — M3 persists `input_fingerprint` durably
(`polyflare-core/src/types.rs:217`) — so this is the established convention, not a new one.

**The silent failure mode:** whoever sends a turn MUST set those hashes. If they stay `None`, every turn plans
`Full`, the milestone's entire benefit evaporates, and *nothing errors* — it just quietly behaves like HTTP at
HTTP's cost. Test for the delta actually being a delta (assert the sent `input` length + the anchor), never
merely for the absence of failure.

**The anchor is a property of the live socket, never of the database.** Ground truth §7.5: a reconnect zeroes
incremental state and the next request carries no anchor. `WsConn` owns `{last_response_id, last_input_count,
last_item_hashes, last_input_fingerprint}` — note the last of those covers the **non-input** fields despite its
name (it has already misled one reader). Dropping the connection drops them all. M3's durable `continuity_sessions` row keeps its
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

**Scope correction (surveyed).** PolyFlare has **no generic N-account retry loop** today. Ingress does:
ownership pre-filter → one-shot recovery (`RecoveryPlan::{ResendFull, SignalClient, None}`, `watchdog.rs:63-86`)
→ on `Err`, `record_failure` (`ingress.rs:74-88`) writes account health and returns **502; the client retries**.
So "429 → transparently failover to the next account" is **not** M5a — that is `PORTING-CODEXLB.md` **B4
(cross-account failover retry loop, HIGH, large)**, a separate item. M5a keeps today's behavior exactly and
changes only the transport underneath it.

Within the executor (M5a):

**Classify on `error.code`, not `status`.** A dead anchor and a genuine bad-request both arrive as the same
wrapped envelope with `status: 400` (ground truth §5, live-measured). Only `code` separates "strip the anchor
and resend" from "surface the error" — key on status alone and you either swallow real 400s or never recover
the wedge.

| Upstream event | WS executor does | Above the seam |
|---|---|---|
| error envelope, `status:400`, `code:"previous_response_not_found"` | strip anchor → **full resend on the same socket**, bounded attempts | never surfaces |
| error envelope, `status:400`, any other `code` | re-frame as SSE, pass through | the error, as today |
| `websocket_connection_limit_reached` (60-min cap) | reconnect → full resend | never surfaces |
| idle timeout (300 s, per-event) | reconnect → full resend | never surfaces |
| `Close` before a terminal frame | reconnect → full resend | never surfaces |
| handshake 426 | HTTP-SSE for this session | never surfaces |
| handshake / transport failure | HTTP-SSE for this turn | never surfaces |
| 429 (wrapped error envelope) | `ExecError::UpstreamStatus(FailureSignal{status, retry_after})` | **unchanged**: `record_rate_limit` → 502 → client retries |
| `response.failed`, terminal code (quota, context-window, cyber-policy, invalid-request) | re-frame as SSE, pass through | the error, as today |
| mid-stream failure after first byte | `Poll::Ready(Some(Err(ExecError::Stream(..))))` | **unchanged**: `record_transient_error` (`watchdog.rs:393-399`) |

The two `ExecError` rows matter: they are how a WS transport keeps the existing health/routing writeback
working. `retry_after` must be parsed off the WS error envelope's `headers` map the way
`retry_secs` reads the HTTP `Retry-After` (`executor.rs:34-40`) — same semantics, different carrier.

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

## 6a. Test harness — net-new, and on the critical path

`polyflare-testkit::MockUpstream` is **axum/HTTP-only** and does not speak WebSocket
(`polyflare-testkit/src/lib.rs:36-230`). Every existing ingress test builds `AppState` with
`codex_executor: Arc::new(CodexExecutor::new().unwrap())` against it (e.g. `tests/no_anchor_failover.rs:63-102`).

So a WS `MockUpstream` equivalent is a **prerequisite task, not an afterthought**: same
"scriptable mock + `spawn() -> base URL`" idiom, plus WS-specific scripting the existing modes can't express —
kill the socket mid-stream, return the `websocket_connection_limit_reached` envelope, answer an anchor with
`previous_response_not_found`, stall past the idle timeout, and record whether a frame carried an anchor and
what its `input` length was (that last one is what proves the delta path actually sent a delta).

The live probes (`examples/ws_vs_sse_probe.rs`, `ws_wedge_demo.rs`) are the only in-repo evidence of the real
wire shape and are the reference for framing — but they hit the live backend with real credentials from
`~/.polyflare/store.db`, so **CI can never depend on them**.

`tests/websocket_fallback.rs` asserts the current 426-on-GET shim. When M5b lands, that test changes rather
than disappears — the shim's own doc comment (`ingress.rs:429-431`) says replace, not delete.

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
