# Codex-rs compatibility contract

This document pins PolyFlare's supported wire contract to the vendored `../codex/codex-rs`
checkout at `37eef7baccebaeb00e42a88b323054cdbfe418c5` (audited on 2026-07-24). It is an
implementation and regression index, not a claim that experimental Codex features are enabled.

## Endpoint matrix

| Codex client surface | Vendored path | PolyFlare status | Compatibility evidence |
|---|---|---|---|
| Responses HTTP/SSE | `POST /responses` | Supported, including pooled paths | `large_body`, `failure_routing`, `refresh_path`, `codex_fingerprint_parity_gate` |
| Responses WebSocket | `GET /responses` upgrade | Supported by default, including pooled paths; upstream is ready before `101` | `ws_downstream_relay`, `wedge_regression`, `polyflare-codex` WS unit tests |
| Compact | `POST /responses/compact` | Supported, including pooled paths and response turn state | `compact_e2e` |
| Models | `GET /models?client_version=…` | Supported at root and `/{pool}/models`; fleet scopes expose the safe account intersection and a deterministic virtual ETag | `model_catalog_e2e`, `model_catalog` unit tests |
| Memories | `POST /memories/trace_summarize` | Supported at root and `/{pool}/memories/trace_summarize` | `control_endpoints_e2e` |
| Images | `POST /images/generations`, `POST /images/edits` | Supported, including pooled paths and larger bounded responses | `control_endpoints_e2e`, `control_forward` |
| Search | `POST /alpha/search` | Routed when the client explicitly enables the experimental feature | `control_endpoints_e2e` |
| Realtime call + sideband WS | `POST /realtime/calls`, then account-matched sideband WS | Intentionally not exposed | Requires call-ID-to-account pinning and a second WS relay; unary forwarding would be unsafe |

Control endpoints already used by the current client (`thread/goal/*` and agent-identity JWKS)
share the same account-aware unary forwarder and reactive one-shot authentication refresh.

## Vendored feature defaults

- Responses WebSocket feature toggles are removed compatibility flags. Current Codex enables the
  transport from the provider's `supports_websockets` capability, sends
  `OpenAI-Beta: responses_websockets=2026-02-06`, and makes an upstream `426` the only automatic
  session-lifetime HTTP fallback signal.
- `WebSearchRequest` and `WebSearchCached` are deprecated and default off; cached search takes
  precedence when explicitly configured. Standalone `/alpha/search` is also opt-in.
- `ImageGeneration` is stable and defaults on.
- `RealtimeConversation` remains under development and defaults off. PolyFlare must not expose it
  until call creation and the returned call ID can be pinned to the same account used by the
  sideband WebSocket.

## Invariants that must not drift

1. Durable conversation identity is session + thread + pool. `x-codex-window-id` separates live
   sockets but does not create a second durable owner.
2. A prior response becomes a continuation anchor only after `response.completed`. Failed,
   incomplete, wrapped-error, and pre-terminal EOF paths never commit an ID or input baseline.
3. WebSocket incremental reuse requires a strict input extension and equality of all twelve
   fields in vendored `responses_request_properties_match`: `model`, `instructions`, `tools`,
   `tool_choice`, `parallel_tool_calls`, `reasoning`, `store`, `stream`, `include`,
   `service_tier`, `prompt_cache_key`, and `text`. `stream_options` and `client_metadata` are the
   only deliberate exclusions.
4. A client-provided continuation anchor is never stripped and replayed locally. Only a
   PolyFlare-generated anchor may use local full-resend recovery.
5. No failover is allowed after any client-visible response byte.
6. Existing owner, pool, provider, and capability filters are hard boundaries. Capacity weighting
   runs only after those filters and therefore chooses only for new/unowned work.
7. Quota exhaustion is capacity feedback, not account sickness. It starts a capacity cooldown and
   queues an immediate authoritative usage refresh without incrementing transient health errors.
8. Request-level failures such as context length, invalid prompt, or policy rejection neither
   reward nor penalize an account. Rate limits, overload, auth failures, and transport loss do.
9. `/models` `ETag` and response-stream `X-Models-Etag` are one protocol. Dropping or inventing
   either value can cause stale catalogs or repeated refetches.
10. Pool scope and selected account survive HTTP, WebSocket upgrade/redial, compact, images,
    search, and reactive authentication refresh.
11. A downstream WebSocket `101` is returned only after owner resolution and a successful upstream
    upgrade. Initial `401` gets one synchronized same-account refresh; upstream `426` remains the
    Codex HTTP-fallback signal; `x-codex-turn-state`, `x-models-etag`, `x-reasoning-included`, and
    `openai-model` are the only upstream upgrade headers copied downstream.
12. A transparent upstream WebSocket redial is allowed only while `x-models-etag`,
    `openai-model`, and the presence of `x-reasoning-included` match the original downstream `101`.
    If any changes, the downstream socket closes so Codex can reconnect and receive a fresh
    handshake. `x-codex-turn-state` is per-turn state and is deliberately excluded from this
    comparison.
13. An established WebSocket 401 receives one synchronized same-account refresh and replay only
    before any upstream frame for that turn has reached the client. After visible progress, the 401
    is surfaced and the turn is never replayed.
14. Unary response caps fail explicitly; an oversized compact, image, or control payload is never
    returned as a truncated body with the upstream success status.
15. A hidden WebSocket redial handshake 401 is not retried with the same bearer. It enters the same
    synchronized, one-shot account refresh path as initial and established-socket authentication
    failures, then redials the same account with the refreshed token.
16. Routing feedback is transport-neutral. Every WebSocket `response.create`, including
    `generate:false` prewarm, holds one in-flight lease (the long-lived socket itself holds none);
    prewarm is excluded from user request history. `response.completed` clears stale account
    errors, and a terminal-less upstream/reconnect loss records one transient error. Downstream
    client cancellation and request-level terminal failures only release the lease. Initial and
    reselected WS owners use the same sticky, pool, and request-header capability floor as HTTP.
17. Fleet-scoped native model catalogs are intersections, not unions. Root/pool `/models`
    advertises only native models supported by every eligible account in that scope. Configured
    custom models use their explicit Codex/OpenAI discovery policy, while Claude translation
    aliases are never advertised in either catalog. An alias target still participates in
    translated-request eligibility, and known per-account model incompatibility is a hard
    eligibility filter for responses, compact, and translated aliases.
18. Root/pool model ETags are deterministic virtual scope identities. The same virtual value is
    emitted by `/models`, HTTP response metadata, the initial WS handshake, and hidden WS redial
    validation. Drift on any scoped member closes a stale pooled socket even if its current
    account's raw upstream ETag did not change.
19. New-session admission is atomic: routing pressure is overlaid, the account is selected, and
    the request or open-socket pressure guard is reserved under one runtime lock. Idle WebSockets
    count as `open_ws`, while only an active generating turn counts as `in_flight`.
20. Capacity-weighted routing uses deterministic weighted rendezvous on raw `session-id` so a main
    Codex session and its subagents prefer the same account. Availability is governed by explicit
    hard admission caps; raw lease count remains only a soft weight penalty and cannot force a
    high-capacity account onto a Free account. Durable continuation ownership remains harder than
    this preference.
21. Rate-limit/quota cooldowns survive process restart. Capacity failures trigger a keyed,
    coalesced immediate usage refresh, so repeated failures do not create an unbounded refresh
    stampede.
22. Request telemetry records a bounded protocol outcome (`completed`, `failed`, `incomplete`,
    `cancelled`, or `transport_lost`). Dashboard success/error calculations prefer that terminal
    protocol truth over an initial HTTP 200. No response content is persisted.
23. Compact, image, search, memory, and control forwarding holds one lease across the initial call
    and its same-account 401 refresh retry. Success clears stale health; rate limit, quota, 5xx, and
    transport failures update shared routing health; ordinary request-level 4xx stays neutral and
    exact `Retry-After` is preserved.
24. Startup readiness and the periodic catalog warmer resolve the exact active root fleet plus
    every named pool before request traffic pays for discovery. Fresh authoritative per-account
    catalogs are reused across overlapping scopes, while each exact scope retains its own stale
    fallback and virtual ETag. Successful members survive a partial refresh; failed members have a
    short retry backoff so overlapping scopes do not hammer them. Startup waits at most two seconds
    before serving with stale/floor fallback while the detached warmup continues. An empty active
    fleet serves the static floor with no ETag.
25. Model discovery uses the same per-account refresh-token singleflight as response, control, and
    WebSocket traffic. Its first upstream 401 gets one same-account OAuth refresh and retry; it
    never rotates a shared refresh token independently or changes account/pool.
26. New work is bounded by configurable global and per-account request-count, weighted-pressure,
    and open-WebSocket caps. Pressure is estimated from bounded shallow request facts and
    calibrated from content-free terminal usage. Ordinary selection cannot consume the reserved
    owner-recovery request or pressure slot. A pinned continuation waits up to the configured
    admission deadline for its exact account and returns retryable capacity exhaustion on timeout;
    it never reroutes a response ID or turn-state token.
27. Bearer token, `chatgpt-account-id`, and `x-openai-fedramp` form one selected-account identity
    tuple on every Codex egress path. PolyFlare removes a client-forwarded FedRAMP header and emits
    exactly `true` only when the selected account's current ID-token claim says it is FedRAMP.
    OAuth rotation updates the tuple atomically for the same account; malformed or missing claims
    fail closed to non-FedRAMP.
28. One hashed logical-turn attempt budget spans HTTP client retries, account failover, watchdog
    full-resend recovery, WebSocket client retries, and transparent WebSocket replay. The key is
    derived only from canonical bounded turn metadata plus session/thread/pool scope; raw turn IDs
    are never retained or logged. Per-request WebSocket `client_metadata` takes precedence over a
    reused connection's potentially stale compatibility header. `generate:false` prewarm is
    excluded. A pre-output 401 is refunded because model sampling could not start; every
    transport/capacity/replay attempt remains charged.
    Exhaustion returns a non-retryable invalid-request error instead of another retry signal.

## Drift review procedure

When updating vendored Codex, inspect every module in `codex-api/src/endpoint`, the exhaustive
`ResponsesApiRequest` destructuring in `core/src/client.rs`, SSE and WebSocket terminal parsing,
authentication retry behavior in `endpoint/session.rs`, and feature defaults for search/images/
realtime. Update the table-driven reuse fixture and endpoint matrix before enabling new behavior.
Run:

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
git diff --check
```
