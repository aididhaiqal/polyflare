# Codex WebSocket Transport — Ground Truth (from `openai/codex` source)

What the **real Codex CLI** does on WS, read out of the cloned `openai/codex` tree. Every claim carries
a `file:line` into `codex-rs/`. This is the authoritative input to `SPEC-M5-WEBSOCKET.md`; it records
*observed source behavior*, not our design.

**Scope note.** This describes codex-rs as a **client**. PolyFlare plays two roles — a WS *server* to
the Codex CLI and a WS *client* to upstream — so most of this binds our upstream side, and §5 binds our
server side (it is what makes a client fall back).

**Two kinds of evidence — do not conflate them.** Almost everything here is a *source fact*: what the codex
client's code does, cited to `codex-rs` file:line. A source fact tells you nothing about what the **server**
sends: the client having no handling for something does not mean the server never emits it (see §5's
`previous_response_not_found` — the exact trap, which this document previously fell into). Where a claim is
instead a *live-measured fact* about real backend behavior, it is labelled as such and cites
`TRANSPORT-FINDINGS-2026-07-17.md` / the probes in `crates/polyflare-server/examples/`. **Live measurement
outranks source inference about server behavior.**

**Stability caveat.** Read at codex 0.144.x. The wire constants below are version-coupled by design
(see `DESIGN-DECISIONS.md` E4(a)); re-verify on a version bump rather than assuming.

---

## 1. Handshake

- **URL**: `Provider::websocket_url_for_path` (`codex-api/src/provider.rs:92-103`) takes
  `url_for_path("responses")` and rewrites the scheme `http→ws` / `https→wss`. Wire URL =
  `{base_url}/responses` with the scheme swapped, e.g. `wss://chatgpt.com/backend-api/codex/responses`.
- **Transport is NOT the reqwest client.** The socket is dialed by `codex-rs/websocket-client/src/dialer.rs`
  over raw `tokio-tungstenite`/`rustls`; `WebSocketConnector::new` (`websocket-client/src/lib.rs:38-47`)
  builds its own `rustls::ClientConfig` via the same custom-CA helper the HTTP client uses
  (`http-client/src/custom_ca.rs:206`). CA logic shared; TCP/TLS connection and pooling independent.
  Proxy routing goes through the same `HttpClientFactory`/`OutboundProxyRoute` abstraction
  (`websocket-client/src/lib.rs:56-58`, `http-client/src/outbound_proxy.rs:154-160`).
- **Headers** — built by `build_websocket_headers` (`core/src/client.rs:1072-1103`), in insertion order:
  1. `x-codex-beta-features` (only if non-empty) — `client.rs:1867-1885`
  2. `x-client-request-id` = `responses_metadata.thread_id` — `client.rs:1081-1083`
  3. `session-id` / `thread-id` — `codex-api/src/requests/headers.rs:5-14`, values at `client.rs:1084-1087`
  4. compatibility headers (`client.rs:740-755`, called at `:1088`): `x-codex-window-id` (always),
     `x-codex-turn-metadata` / `x-codex-parent-thread-id` / `x-openai-subagent` (conditional),
     `x-openai-memgen-request: true` for memory-consolidation sessions (`responses_metadata.rs:222-260`)
  5. `x-oai-attestation` (if enabled and provided) — `client.rs:1089-1091`, name at `core/src/attestation.rs:7`
  6. `OpenAI-Beta: responses_websockets=2026-02-06` — **always**, `.insert()` (`client.rs:1092-1095`,
     constants `client.rs:140,154`)
  7. `x-responsesapi-include-timing-metrics: true` (if enabled) — `client.rs:1096-1101`

  Then `merge_request_headers` (`codex-api/src/endpoint/responses_websocket.rs:471-484`): provider headers,
  then the above (overwrite), then `default_headers()` **only for vacant slots** (`Entry::Vacant`, `:479`)
  — `login/src/auth/default_client.rs:334-350` unconditionally sets `originator` + `User-Agent`. Finally
  `auth.add_auth_headers` (`responses_websocket.rs:391`) adds `Authorization: Bearer <token>` and
  optionally `ChatGPT-Account-ID` / `X-OpenAI-Fedramp` (`model-provider/src/bearer_auth_provider.rs:32-46`).
- **No subprotocol, no `Origin`.** Never set anywhere in the tree. The request comes from
  `into_client_request()` (`responses_websocket.rs:494-497`), so only the standard `Sec-WebSocket-Key` /
  `-Version: 13` / `Connection: Upgrade` / `Upgrade: websocket` appear.
- **`permessage-deflate` IS offered** — `websocket_config()` (`responses_websocket.rs:546-553`).
  Omitting the offer is a detectable handshake difference.
- Test-asserted present on the handshake: `OpenAI-Beta`, `x-client-request-id`, `session-id`, `thread-id`,
  `User-Agent` (`core/tests/suite/client_websockets.rs:178-198`).

**Unverified:** on-the-wire header *byte order*. Insertion order above is verified; whether `http`/
`tungstenite` serialize in that order was not confirmed. Treat name/value/presence as verified, order as not.

## 2. Session lifecycle

- **Connection outlives the turn.** `ModelClientState` (`client.rs:198-216`) holds
  `cached_websocket_session: StdMutex<WebsocketSession>` and is `Arc`-shared → conversation-scoped.
  `new_session()` (`client.rs:479-485`) `mem::take`s the cached session; `impl Drop for ModelClientSession`
  (`client.rs:1106-1112`) puts it back. Proven: two turns, `handshakes().len() == 1`, `connection.len() == 2`
  (`client_websockets.rs:322-391`).
- **No cache key.** Reuse is "whatever was last stored", gated only on liveness (`conn.is_closed()`,
  `client.rs:1322-1325`) — not on provider/auth/model identity. (Separately, *incremental* reuse is gated by
  `responses_request_properties_match`, `client.rs:306-359`.)
- **Lazy connect**: on first `stream()` for a turn, or explicit `preconnect_websocket`/`prewarm_websocket`
  (`client.rs:1258-1296`, `1713-1763`).
- **Idle timeout = `stream_idle_timeout`, default 300_000 ms** (`model-provider-info/src/lib.rs:26,316-320`),
  applied per `.next()` (`responses_websocket.rs:688-690`), NOT as one overall deadline. Expiry →
  `ApiError::Stream("idle timeout waiting for websocket")` (retryable).
- **Server 60-min connection cap** surfaces as `websocket_connection_limit_reached`
  (`responses_websocket.rs:158-159,616-627`) → `ApiError::Retryable` → ordinary reconnect, **no HTTP
  fallback** (`client_websockets.rs:1637-1685`).
- **Keepalive: the client NEVER initiates a Ping.** It only auto-Pongs (`responses_websocket.rs:90-96`).
  Unsolicited pings would be a fingerprint mismatch.
- **Reconnect** is synchronous-on-demand inside `websocket_connection()` when the cached conn is closed
  (`client.rs:1322-1353`) — never background/timer-driven.

## 3. Framing

- **Request**: `ResponsesWsRequest` (`codex-api/src/common.rs:317-323`) — `#[serde(tag="type")]`, single
  variant serialized `"type":"response.create"`. Body `ResponseCreateWsRequest` (`common.rs:265-293`):
  `model`, `instructions` (omit if empty), `previous_response_id` (omit if None), `input`, `tools`,
  `tool_choice`, `parallel_tool_calls`, `reasoning`, `store`, `stream`, `stream_options`, `include`,
  `service_tier`, `prompt_cache_key`, `text`, `generate` (omit unless set), `client_metadata`.
- **`previous_response_id` source**: `prepare_websocket_request` (`client.rs:1222-1253`) — if
  `get_last_response()` (`client.rs:1212-1220`, **non-blocking `try_recv()`**) yields a `LastResponse`
  AND the new request is a strict extension with matching non-input fields
  (`responses_request_properties_match`), set `previous_response_id` and truncate `input` to the new
  suffix only. `LastResponse.response_id` comes from the `response.id` of the most recent
  `response.completed` on **that connection** (`client.rs:1998-2018`).
- **Received frame types** — `process_responses_event` (`codex-api/src/sse/responses.rs:327-471`), shared by
  HTTP-SSE and WS: `response.created`, `response.output_item.added`, `response.output_item.done`,
  `response.output_text.delta`, `response.custom_tool_call_input.delta`, `response.reasoning_summary_text.delta`,
  `.done`, `response.reasoning_text.delta`, `response.reasoning_summary_part.added`, `response.metadata`
  (gates turn-state extraction, exact string match, `:203-234`), terminal `response.completed` (`:434-451`),
  terminal `response.failed` (`:387-421`, code → ContextWindowExceeded / QuotaExceeded / UsageNotIncluded /
  CyberPolicy / InvalidRequest / ServerOverloaded / else Retryable), terminal `response.incomplete`
  (`:423-432`). Unknown types are ignored (`:467-469`).
- **WS-only frames** (`responses_websocket.rs:709-806`): a wrapped error envelope
  `{"type":"error","status":u16,"error":{code,message},"headers":{}}` checked *before* generic parsing
  (`:597-640`); `codex.rate_limits` → `ResponseEvent::RateLimits` (`:739-744`); `Binary` → error (`:797-799`);
  `Close` → `"websocket closed by server before response.completed"` with **no close-code inspection**
  (`:800-804`).

## 4. Concurrency

**Strictly one in-flight turn per socket.** `stream_request` holds `stream.lock().await` for the entire
lifetime of the response stream (`responses_websocket.rs:274-311`). There is **no request-id in the wire
protocol** — correlation is implicit: the single in-flight request owns the whole event stream until
`response.completed` / `response.failed` / close. A second concurrent request just blocks on the mutex.

## 5. Fallback — binds PolyFlare's SERVER side

- **`FallbackToHttp` has exactly ONE trigger**: `websocket_connection()` returning HTTP
  **426 Upgrade Required** at handshake time (`client.rs:1596-1600`). Checked before any frame is sent.
  **No other status falls back** — a 404/405/500 is a hard error.
- Effect: `force_http_fallback` (`client.rs:508-527`) sets `disable_websockets: AtomicBool` and clears the
  cached session. **One-way for the session** — no reset path exists in the tree (only
  `AtomicBool::new(false)` at construction, `client.rs:451`).
- **Second path**: retry-budget exhaustion. `handle_retryable_response_stream_error`
  (`core/src/responses_retry.rs:22-79`) calls `try_switch_fallback_transport` once a *retryable* error
  exceeds `stream_max_retries` (default 5, `model-provider-info/src/lib.rs:27,309-311`). Same permanent effect.
- **Retryable** (`protocol/src/error.rs:176-214`): Stream, Timeout, RequestTimeout, UnexpectedStatus,
  ResponseStreamFailed, ConnectionFailed, InternalServerError, InternalAgentDied, Io, Json, TokioJoin.
  **Not retryable → immediate error, no fallback**: ContextWindowExceeded, QuotaExceeded, UsageNotIncluded,
  InvalidRequest, CyberPolicy, ServerOverloaded, UsageLimitReached, RetryLimit, RefreshTokenFailed.
- **429 does NOT trigger fallback** — it maps to `RetryLimit`/`UsageLimitReached` (`api_bridge.rs:85-121`)
  and is surfaced immediately (`turn.rs:1209-1211`).
- **401** is handled inside the stream loop by `handle_unauthorized` (`client.rs:1601-1614,2164-2279`),
  bounded by the recovery state machine rather than the retry budget; does not itself cause fallback.

### `previous_response_not_found`: emitted by the SERVER, unhandled by the CLIENT

Two separate facts, from two different kinds of evidence. Keep them apart — conflating them produces wrong code.

**Source fact (this document's kind of evidence):** grepped the whole `codex-rs` tree, source + tests —
**zero occurrences** of `previous_response_not_found` / `response_not_found`. The client has no special case.
Absence from the client is NOT absence from the wire; it means only that codex never learned to handle it.

**Live-measured fact (`TRANSPORT-FINDINGS-2026-07-17.md` §3, `examples/ws_wedge_demo.rs` against the real
backend):** the server absolutely does emit it, in 0.5–2 s, and it arrives as the **wrapped error envelope with
`status: 400`** — NOT as a `response.failed`:

```json
{"type":"error","error":{"type":"invalid_request_error","code":"previous_response_not_found",
 "message":"Previous response with id 'resp_...' not found.","param":"previous_response_id"},"status":400}
```

Confirmed for both the cross-account case and the same-account fresh-reattach case. So it takes the §3
wrapped-envelope path (`responses_websocket.rs:597-640`), which is checked BEFORE generic frame parsing —
and since its `code` isn't `websocket_connection_limit_reached`, codex maps it to
`ApiError::Transport(Http{status:400})`, i.e. an ordinary hard error.

**Consequences for us:**
1. Anchor recovery is entirely PolyFlare's. There is no client-side partner logic, and letting the error reach
   the client wastes its retry budget and can silently disable WS for the rest of that conversation.
2. **A classifier must inspect the envelope's `error.code` BEFORE mapping `status` to an outcome.** An anchor
   miss and a genuine bad-request are both `status: 400` on the same envelope shape; only `code`
   distinguishes "strip the anchor and resend" from "surface the error". Keying purely on status would either
   swallow real 400s or fail to recover the wedge.

*(An earlier revision of this section asserted a dead anchor arrives as a generic `response.failed` — that was
an inference about client handling, contradicted by our own live probe. Corrected 2026-07-17.)*

## 6. `generate: false`

Sent **only** on prewarm — `stream_responses_websocket` sets `generate = Some(false)` when `warmup`
(`client.rs:1578-1580`), from `prewarm_websocket()` (`client.rs:1713-1763`); omitted for normal turns
(`common.rs:259,290`). Purpose per its own doc comment (`client.rs:15-16`): *"a v2-only `response.create`
with `generate=false`; it waits for completion so the next request can reuse the same connection and
`previous_response_id`"*. Telemetry is explicitly suppressed for it (`client.rs:1559-1564,1623`).

**Client-observed contract**: it still returns a `response.completed` carrying a usable `response.id`,
which seeds the first real turn's anchor (`client_websockets.rs:520,537`). Server internals are a black
box — treat that as the observed contract, not a guarantee.

## 7. Gotchas for a faithful reimplementation

1. **`x-codex-turn-state` must be ABSENT from the WS handshake headers.** `build_websocket_headers` passes
   `turn_state = None` (`client.rs:1078`), unlike the HTTP path which sends it as a real header
   (`client.rs:1146`). On WS it travels only as `client_metadata["x-codex-turn-state"]` inside each
   `response.create`, and only after a value has been received from the server (`client.rs:1568-1570`) —
   never invented client-side.
2. `OpenAI-Beta` is `.insert()`'d — exactly `responses_websockets=2026-02-06`, never appended to.
3. No client-initiated Ping (§2).
4. Idle timeout is per-event, not per-connection (§2).
5. **Reconnect resets incremental state completely** — `needs_new` zeroes `last_request` /
   `last_response_rx` / `last_response_from_untraced_warmup` (`client.rs:1328-1330`); the next
   `response.create` is a full request with **no** anchor (`client_websockets.rs:2104-2135`). Never carry
   delta state across a reconnect.
6. Anchor reuse is opportunistic (`try_recv()`, `client.rs:1212-1220`) — on a miss it silently sends a full
   request rather than waiting.
7. Fallback is a one-way session-lifetime switch (§5) — "retry WS later in the same process" would diverge.
8. 429 does not fall back (§5).
9. Close code is not inspected mid-stream — don't over-engineer close-code semantics (§3).
10. `response.metadata` must match that exact string (`sse/responses.rs:203-234`).

**`client_metadata` keys populated per turn** (`responses_metadata.rs:207-227`, `client.rs:757-770`,
`common.rs:295-315`, `client.rs:1850-1859`): `x-codex-installation-id`, `session_id`, `thread_id`,
`x-codex-window-id`, and conditionally `turn_id`, `x-openai-subagent`, `x-codex-parent-thread-id`,
`x-codex-turn-metadata`, `ws_request_header_x_openai_internal_codex_responses_lite`,
`x-codex-turn-state`, `ws_request_header_traceparent` / `ws_request_header_tracestate`, plus
`x-codex-ws-stream-request-start-ms` stamped immediately before every send (`client.rs:1627`).
