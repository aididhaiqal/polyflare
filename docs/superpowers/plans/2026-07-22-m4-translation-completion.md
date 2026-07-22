# M4 Translation Completion (correctness wins + non-streaming buffering)

**Goal:** Make the aliased `/v1/messages` (Anthropic→Codex) path faithful to the Anthropic Messages API — correct `stop_reason`, forwarded `tool_choice`, and full support for **non-streaming** clients — building on the P0 contract fix (`cabf1fa`) that already made the streaming path work.

**Why planning is required:** High-risk per the adaptive router — this changes a **public contract** (the `/v1/messages` request/response format) and is **cross-system** (Anthropic client ↔ Codex backend). Non-streaming buffering changes the endpoint's response *shape* (SSE → single JSON) for a whole class of clients.

**Acceptance (observable, live-verified against the real Codex backend):**
- A `stream:true` `/v1/messages` request still returns Anthropic SSE, unchanged from today (no regression).
- A `stream:false`/`stream`-absent request returns a single `application/json` Anthropic `Message` (not SSE) with assembled `content[]`, correct `stop_reason`, and `usage`. **Scoped limitation (documented in code + follow-up):** this holds on the immediate-pick success path; the pool-STARVATION recovery-wait fallback still returns SSE for a non-streaming client (buffering it needs a refactor of the shared `try_layer1/2` helpers used by `/responses` too) — a rare edge, deferred.
- A turn that calls a tool reports `stop_reason: "tool_use"` (not `end_turn`), in both streaming (`message_delta`) and non-streaming (final `Message`) forms.
- `tool_choice` is forwarded and honored (or, if Codex rejects it like `max_output_tokens`, deliberately dropped with a recorded reason — decided by live probe, not assumption).
- All touched crates: `cargo test` green, `clippy -D warnings --all-targets` clean, `fmt` clean. Wedge-sacred files untouched.

---

### Outcome 1: `stop_reason: "tool_use"` when the turn emitted a tool call
- Work: In `polyflare-anthropic/translate.rs`'s stateful response translator, track whether the turn emitted a `tool_use` content block; when it did, the terminal `message_delta` maps `stop_reason` to `tool_use` instead of the current `completed→end_turn` default. Keep `incomplete→max_tokens` and the `completed→end_turn` (no-tool) mappings. The existing doc note flagging this as "awaiting U4 live-capture" is now resolved by live captures.
- Risks/open questions: confirm the tool-call signal is available from the OpenAI-Responses events the translator already consumes (function_call item), not requiring a new upstream field.
- Verify: `cargo test -p polyflare-anthropic` (add a unit test: a stream containing a function_call yields `stop_reason: "tool_use"`); later confirmed live in Outcome 4.

### Outcome 2: forward `tool_choice` (and settle `stop_sequences`) per Codex's real contract
- Work: In `map_request`, map Anthropic `tool_choice` to the Responses shape and include it. Live-probe whether Codex `/responses` accepts `tool_choice` and `stop_sequences` (it hard-rejected `max_output_tokens`, so neither is assumed): forward what it accepts; for anything it rejects, drop it with a one-line comment citing the exact 400, exactly as the P0 fix documented `store`/`stream`/`max_output_tokens`.
- Risks/open questions: Codex may reject `tool_choice` and/or `stop_sequences`; the probe decides. No guessing.
- Verify: `cargo test -p polyflare-anthropic` (unit: `tool_choice` present in the mapped body when accepted); live in Outcome 4.

### Outcome 3: non-streaming client support (buffer → single `Message` JSON)
- Work: Parse the client's `stream` field in the aliased `/v1/messages` handler (`messages_handler_codex_aliased`). When `true`, the current SSE path (`stream_response`) is unchanged. When `false`/absent (Anthropic's default), consume the wrapped-translating Anthropic SSE event stream through a new **collector** (lives in `polyflare-anthropic` — it folds Anthropic events into Anthropic shape): accumulate ordered `content[]` (text from `text_delta`, `tool_use.input` from `input_json_delta`, `thinking` from `thinking_delta`), and take `id`/`model`/`role` from `message_start` and `stop_reason`/`stop_sequence`/`usage` from `message_delta`; emit one `application/json` `Message`. A mid-stream upstream error returns a single Anthropic error JSON, never a partial `Message`.
- Risks/open questions: content-free (the collector holds one in-flight response's assembled text only long enough to serialize it — it is conversation content in transit, never logged/persisted, same boundary as the streaming path already crosses); must not regress the streaming path; usage-capture instrumentation must still fire.
- Verify: `cargo test -p polyflare-anthropic` (collector unit tests: text-only, tool_use, error-mid-stream) + `cargo test -p polyflare-server` (handler picks JSON vs SSE by client `stream`); live in Outcome 4.

### Outcome 4: live-verify the completed path (the standing U4 gate)
- Work: Against a copy of the real store (loopback, enforcement off), verify end-to-end: streaming still works (all 3 tiers, no regression); non-streaming returns a single JSON `Message`; a tool call reports `stop_reason: "tool_use"` in both forms; `tool_choice` behaves as Outcome 2 settled. Record the evidence.
- Risks/open questions: account-pool availability in this env (transient 503s under rapid probing — pace the probes); tokens must authenticate.
- Verify: controller-run curl probes + `request_log` inspection; wedge-sacred `git diff --name-only` clean; latency gate green.
