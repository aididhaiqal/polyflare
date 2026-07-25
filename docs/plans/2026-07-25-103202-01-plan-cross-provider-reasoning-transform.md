# Cross-provider reasoning transform (phase 1: reactive strip-and-summarize)

**Goal:** A thread that switches provider platforms (e.g. fugu → codex) stops hard-failing with
`invalid_encrypted_content`. The relay reacts to that exact upstream rejection by resending the
turn once with foreign reasoning envelopes removed and their plaintext summaries preserved as
ordinary assistant text.

**Why reactive, not proactive:** reasoning items carry no provenance — the only party who can
prove an envelope is foreign is the upstream that fails to decrypt it. Reacting to the explicit
`invalid_encrypted_content` error code means healthy same-platform traffic stays byte-identical
(prompt-cache preserved) and the transform fires exactly when the content is proven undecryptable.

**Scope:** WS relay only (the live failing path). HTTP ingress is a follow-up if ever observed
there. Server-side compaction (phase 2) is explicitly out of scope.

**Content rule:** the transform parses and rewrites the in-flight frame in memory only. No
summary text, item id, or frame fragment is ever logged or persisted. Metrics count events only.

### Outcome 1: the transform primitive

- `ws_relay/reasoning_transform.rs`: `strip_unverifiable_reasoning(&str) -> Option<String>`.
  Parses a `response.create` frame; removes every `input` item of type `reasoning`; each removed
  item with a non-empty `summary` is replaced in place by an assistant `message` item carrying the
  summary text (marked as prior-model reasoning). Returns `None` when there is nothing to do
  (no reasoning items, not a generating frame, malformed JSON) so the caller never retries
  uselessly. All other fields (including `previous_response_id`) pass through untouched.
- Unit tests: summary preserved + envelope gone; summary-less item dropped cleanly; no-op cases
  return `None`; non-reasoning items byte-preserved.

### Outcome 2: pump interception

- In `UpstreamSignal::Error(sig)` arm, before the generic forward-and-bench path, mirroring the
  401 reactive block: when `sig.error_code == "invalid_encrypted_content"`, the turn has produced
  no client-visible output, `in_flight` is buffered, and the per-turn `reasoning_transform_attempted`
  flag is clear → transform the buffered frame; on `Some`, redial the same account
  (`redial_for_scope`), spend a budget attempt (refund the failed one, mirroring 401), send the
  transformed frame, store it as the new `in_flight`, record `reasoning_transform_replay`.
  Any failure (redial, send, budget, `None` transform) falls through to today's behavior.
- Flag resets at `start_turn` alongside `reactive_auth_attempted` — once per turn, so a client
  full-resend after an anchored-frame interaction gets its own attempt.

### Outcome 3: proof

- Testkit: `ScriptedTurn::invalid_encrypted_content()` wrapped-error constructor.
- Integration (`tests/ws_downstream_relay.rs`, mirroring `mid_turn_cap_replays_inflight_frame`):
  connection 1 rejects the reasoning-bearing turn with the wrapped 400; connection 2 must receive
  the transformed frame (no `reasoning` items, summary text present, same model/fields) and serves
  `response.completed`; the client sees only the clean completion. A second test: same-platform
  traffic (no reasoning items) never triggers a redial.
- Gate: `cargo test --workspace`, deploy via the standard pipeline (commit → fresh build →
  quiescence-gated kickstart), then live-verify by switching the tether thread fugu → sol.
