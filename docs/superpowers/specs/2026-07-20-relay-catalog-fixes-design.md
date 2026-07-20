# Relay + Catalog Fixes — Design

**Status:** Approved 2026-07-20. Three independent fixes surfaced by the PolyFlare-vs-direct A/B (see memory `polyflare-vs-direct-ab`). Next: implementation plan → SDD.

## Motivation

The A/B measured the WS relay as cache- and speed-neutral vs going direct, but surfaced three fixable issues:
1. **`/models` schema gap** — codex 0.144.4 can't parse PolyFlare's `/models` (missing ~15 required `ModelInfo` fields) → logs `failed to refresh available models` every turn and falls back to its bundled catalog (no dynamic model discovery). Non-fatal, but real.
2. **Transient-429 causes a needless account move** — the approved WS-relay design says *"transient 429s retry the same account (no move)"*, but the code benches on every 429 (≥30s cooldown clamp) → always moves → discards the conversation's prompt cache (cold full-resend on the new account).
3. **Mid-turn 60-min cap → ~290s stall** — the pump intercepts the cap and re-dials the same account, but does not replay the in-flight turn; if the cap fires mid-turn the client's socket stays open and its own resend never fires, so it waits out its ~290s read-idle.

## Fix 1 — `/models` raw pass-through

**Decision: preserve and forward the raw upstream `ModelInfo` entries verbatim** rather than lossy-parsing into a 4-field subset. PolyFlare already fetches the full upstream `/models` (chatgpt.com returns complete `ModelInfo` entries — the same shape as codex's bundled `models.json`); it just discards all but `slug`/`display_name`/`context_window`/`prefer_websockets` at parse time.

- `UpstreamModel` gains `raw: serde_json::Value` — the full upstream entry. `parse_one_model` keeps it (the existing convenience fields stay, derived from `raw`, for the OpenAI-shape rendering + merge/floor logic).
- The Codex `/models` response (`to_codex_response`) emits the raw `ModelInfo` entries **verbatim** in the `models` array — already valid, so codex parses them with no error.
- **Synthetic aliases** (`claude-opus-4-1` → `gpt-5.6-sol`, etc.): build a full `ModelInfo` by **cloning the aliased-to model's raw entry** and overriding `slug`/`display_name` (+ the alias's `reasoning_effort` metadata). If the aliased-to model isn't in the live set (fetch unavailable), the alias is surfaced only in the OpenAI `data` array (unchanged) and omitted from the Codex `models` array — codex never errors on a missing alias, it just won't list it.
- **Floor** (compiled-in fallback when the live fetch is unavailable): carry one or two full `ModelInfo` entries (template from codex's `models.json` `gpt-5.6-sol`) so the offline `/models` is still parseable; if we choose not to, codex falls back to its own bundle — acceptable, no error surfaced beyond the fetch-unavailable case.
- Reference: codex-lb `app/modules/model_sources/catalog.py` (`_to_upstream_model` builds the full shape incl. `supported_reasoning_levels`).
- Authoritative schema: `codex-rs/protocol/src/openai_models.rs` `ModelInfo` (required fields: `slug`, `display_name`, `supported_reasoning_levels`, `shell_type`, `visibility`, `supported_in_api`, `priority`, `base_instructions`, `truncation_policy`, `supports_parallel_tool_calls`, `support_verbosity`, `experimental_supported_tools`, `availability_nux`, `upgrade`, `default_verbosity`, `apply_patch_tool_type` — the rest have serde defaults).

**Content-safety:** model metadata is not conversation content; the raw entries are model descriptions from the upstream, safe to forward/serve. No tokens/bearers involved.

## Fix 2 — Transient-429 retry-same-account

**Decision: on a transient 429, wait it out on the same account instead of benching + moving.** In `ws_relay::mod`'s `on_upstream_error`, before the `bench_account_for_failure` call:

- If `sig.status == 429` AND `sig.retry_after` is `Some(n)` with `n <= TRANSIENT_RETRY_MAX_SECS` (**30**, the existing `RATE_LIMITED_MIN_COOLDOWN_SECS` — at/under the clamp = transient): **do not bench.** `tokio::time::sleep(n)`, then `redial_upstream(&headers, &current)` (the SAME account), and return `Some((current, new_upstream))` — a retry-in-place that preserves the conversation's cache. Counts as `reconnect_same_account`.
- Otherwise (no `retry_after`, `retry_after` > 30, or a durable/permanent code): the existing path — bench → `resolve_owner` (skips the now-benched account) → re-dial → move.
- Bound the wait so a malformed huge `retry_after` (already excluded by the ≤30 gate) can't stall; the existing `MAX_RECONNECTS_WITHOUT_PROGRESS` still bounds repeated transient retries with no completed turn.

**Reuse:** `FailureSignal.retry_after` (already extracted by `classify_upstream_signal`); `redial_upstream`; the existing bench/move path untouched for the durable branch.

## Fix 3 — Mid-turn-cap replay (in-flight frame buffer)

**Decision: buffer the in-flight client `response.create` frame and replay it on the fresh socket after a same-account re-dial.**

- The pump tracks `in_flight: Option<String>` — set to the raw client `Text` frame when it is forwarded upstream (`send_client_text` success), cleared when a `response.completed` for it is sniffed (the turn finished). (One in-flight turn per socket — codex's model.)
- On a `ConnectionLimit` cap intercept **or** a mid-turn upstream drop (`recv_text` → `Ok(None)`/`Err` while `in_flight.is_some()`), after the same-account re-dial: if `in_flight` is `Some`, **replay it** on the fresh upstream (`send_text`), so the interrupted turn resumes. Same account → the `previous_response_id` anchor resumes → the turn completes; the client sees only its normal response, no ~290s stall.
- Replay is **verbatim** (the buffered raw frame, no reparse) and bounded by `MAX_RECONNECTS_WITHOUT_PROGRESS` (a turn that never completes across N replays tears down rather than looping).
- Only same-account replay (Fix 3 does not replay across a cross-account move — there the anchor is intentionally cross-account and the client full-resends, unchanged from Phase 3).

**Wedge-sacred:** replay re-sends the client's own frame verbatim on the same account; it never rewrites the anchor or touches the continuity/ObservingStream engine.

## Global constraints

- **Content-free:** no conversation content logged/persisted. The in-flight buffer holds the raw `response.create` frame **in memory only** for replay — never logged, never persisted, dropped on turn completion or teardown (same content-safety posture as the verbatim relay: the frame transits memory but is never surfaced).
- **Wedge-sacred:** continuity/selection/circuit-breaker engines reused via existing APIs; `watchdog.rs`/`select.rs`/`continuity.rs`/`ObservingStream` byte-unchanged.
- **Flag-gated:** all pump changes behind `POLYFLARE_WS_DOWNSTREAM` (default off). The `/models` fix is on the always-on catalog path (not relay-gated) but is additive/behavior-preserving for existing clients (richer response, same or superset of fields).
- **Verbatim:** relayed/replayed frames byte-for-byte; raw upstream `ModelInfo` forwarded unmodified.
- Clippy `-D warnings`, fmt, full `cargo test -p polyflare-server` green.

## Testing

- **Fix 1:** unit — `parse_models` preserves `raw`; `to_codex_response` emits full `ModelInfo` that round-trips through codex's `ModelInfo` deserialize (import the shape or assert the required fields present). Live — codex over PolyFlare no longer logs `failed to refresh available models`; `/models` lists the models.
- **Fix 2:** relay-through — a scripted transient 429 (`rate_limited_429(5)`) → the relay waits ~5s and retries the SAME account (no move; ownership unchanged; `reconnect_same_account` bumped, `move_cross_account` not). A durable 429 (`rate_limited_429(300)` or no retry-after) still moves.
- **Fix 3:** relay-through — script a turn that gets a `connection_limit_reached` mid-turn → assert the buffered frame is replayed on the re-dialed socket and the turn completes (client receives the `response.completed`), same account, no teardown.
- Live-verify Fix 1 (real codex — the per-turn error disappears) + a mock-driven Fix 3 (cap mid-turn); Fix 2 mock-driven (deterministic retry-after).
