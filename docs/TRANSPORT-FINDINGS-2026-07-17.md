# Transport & Wedge — Live Findings and Decisions (2026-07-17)

Empirical results from probing the **live** Codex backend with real accounts, and the design
decisions they force. Supersedes assumptions baked into `SPEC-M3.md §2` (the silent-hang premise).

**Harnesses** (`crates/polyflare-server/examples/`, run against `~/.polyflare/store.db`):
- `handover_probe.rs` — HTTP-SSE: store/anchor support matrix + cross-account prefill cost.
- `ws_vs_sse_probe.rs` — WebSocket incremental vs HTTP-SSE full resend.
- `ws_wedge_demo.rs` — dead-anchor behavior (cross-account + same-account reattach) + recovery.
- `ws_ratelimit_probe.rs` — **measured-by** for the open question below: does WS incremental
  continuation reduce `/wham/usage` `used_percent` movement (rate-limit consumption), not just
  upload bytes? Built, not yet run — see "Rate-limit consumption (UNRUN)" below.
- `cache_billing_probe.rs` — **measured-by** for the "Cache-affinity billing (PENDING
  MEASUREMENT)" section below: does a same-account continuation report `cached_tokens > 0`, and
  ≈0 cross-account, for the same `prompt_cache_key`? Built, not yet run.

## What was measured (facts)

1. **HTTP transport has no server-side anchor.** The backend *enforces* `store:false`
   (`store:true` → `{"detail":"Store must be set to false"}`) and rejects `previous_response_id`
   with `store:false` (`{"detail":"Unsupported parameter: previous_response_id"}`), for every model
   tried. Confirmed in `openai/codex` `core/src/client.rs`: the HTTP `ResponsesApiRequest` has no
   `previous_response_id` field; `store = provider.is_azure_responses_endpoint()` (false for
   ChatGPT). → **HTTP continuation is always stateless full-history resend.**

2. **The `previous_response_id` anchor is WebSocket-only.** Set solely in
   `prepare_websocket_request` from a cached WS session's `last_response`, sending incremental items
   over the same live connection. **WS handshake works live** (HTTP 101), incremental continuation
   works (582 B upload vs 50 KB full resend = **86× smaller**, deterministic), and `generate:false`
   is a **warmup** (prefill-only, no generation, ~0.55 s) — a cheap standby-prewarm primitive.

3. **A dead anchor is a FAST, EXPLICIT ERROR — not a silent hang.** Firing a dead anchor on a fresh
   WS connection returns, in 0.5–2 s:
   ```json
   {"type":"error","error":{"type":"invalid_request_error","code":"previous_response_not_found",
    "message":"Previous response with id 'resp_...' not found.","param":"previous_response_id"},"status":400}
   ```
   Confirmed for **both** the cross-account case AND the same-account fresh-reattach case (the
   scenario the root-cause notes call the recurring wedge). Controls pass: own-anchor on its live
   socket completes; **full resend (no anchor) recovers cleanly**.

4. **HTTP cross-account handover is ~free.** Since every HTTP turn is already a full resend, moving a
   conversation to a fresh account is a normal turn; the only extra cost is the org-scoped
   prompt-cache miss (~0.3–1.5 s cold prefill @128k tok, noisy). No "long reprocessing."

5. **codex-lb already handles `previous_response_not_found`** — across nine files
   (`is_previous_response_not_found_error`, `previous_response_id_from_not_found_message`,
   `should_rewrite`, WS + HTTP-bridge variants) — and still wedges ~31% of reattaches. The silent
   hang is therefore **self-inflicted** by codex-lb's rewrite/trim/bridge machinery, not the backend.

## Rate-limit consumption (UNRUN / result pending)

The `used_percent` movement question — the milestone's actual premise (SPEC-M5-WEBSOCKET.md §8)
— is **measured-by:** `crates/polyflare-server/examples/ws_ratelimit_probe.rs`.

**Status: UNRUN.** The probe is built, compiles clean (`cargo build --examples -p
polyflare-server`, `cargo clippy --workspace --all-targets -- -D warnings`), and is ready to run,
but it has **not been executed against live accounts** — running it spends the exact resource it
measures (rate-limit quota), and one account is already exhausted. This section intentionally
carries **no measured result** — do not treat the absence of a number here as "WS doesn't help" or
as "WS helps"; it means nobody has run the probe yet.

Everything above this section (the 86× upload figure, the anchor/handover facts) was measured and
holds regardless of this open question. What is NOT yet established: whether prefilled/cached
tokens that make the 86× upload win possible are billed fully against `/wham/usage` windows
anyway, in which case the upload win would not translate into a quota win.

**When someone runs it** (`cargo run -p polyflare-server --example ws_ratelimit_probe --release
-- --live`, only with real headroom on two accounts), replace this paragraph with the measured
`used_percent`-per-turn delta for WS vs HTTP and the probe's printed verdict line, and update
`SPEC-M5-WEBSOCKET.md` §8 from "measure this" to the actual measured statement — including if the
answer is negative (prefill still billed fully against rate limits). Do not fabricate a number in
either document before that run happens.

## Cache-affinity billing (PENDING MEASUREMENT)

**The question:** a user observed that when their session stayed pinned to ONE account, that
account's quota lasted noticeably longer. Hypothesis: same-account continuation with a stable
`prompt_cache_key` keeps the prompt-prefill cache WARM (the re-sent history is billed at the
cheaper cached-token rate on every turn), while bouncing between accounts is a cold cache every
turn (the same history re-billed at full rate) — i.e. this is the token-billing sibling of D4
above, at the granularity of actual `usage.input_tokens_details.cached_tokens` counts rather than
`used_percent` window movement.

This is **measured-by:** `crates/polyflare-server/examples/cache_billing_probe.rs`. It sends a
short two-turn conversation (tiny literal prompts, no context padding) on one ACTIVE account with
a stable `prompt_cache_key`, reads `cached_tokens` off turn 2's `response.completed` `usage`, and
— if a second ACTIVE account has headroom — replays the SAME turn-2 continuation with the SAME
`prompt_cache_key` on that different account to confirm the cache is per-account/org-scoped
(`cached_tokens ≈ 0` there). It reports the measured cache-hit fraction and a one-line "cache
affinity saves ~X% of per-turn input billing on a warm session" verdict.

**Status: UNRUN.** The probe is built, compiles clean (`cargo build --examples -p
polyflare-server`, `cargo clippy --workspace --all-targets -- -D warnings`), and is intentionally
the cheapest probe in this directory (2–3 short generations total, no `/wham/usage` reads) — but
it has **not been executed against live accounts**. This section carries **no measured result** —
do not treat its absence as confirming or refuting the user's field observation; it means nobody
has run the probe yet.

**When someone runs it** (`cargo run -p polyflare-server --example cache_billing_probe --release
-- --live`, only with real headroom — 1 account minimum, 2 for the cross-account contrast),
replace this paragraph with the measured cached/input token counts, the cache-hit percentage, the
cross-account contrast (or "skipped, no 2nd headroom account"), and the probe's printed verdict
line. Do not fabricate a number in this document before that run happens.

## Decisions forced

- **D1 — Hybrid transport is the target.** Ride **WS on the owner** (86× less upload, history
  prefilled once — kills HTTP's per-turn history re-bill) and **fall back to HTTP full-resend on
  handover/failover** (clean, wedge-immune). codex itself does WS→HTTP `FallbackToHttp`.

- **D2 — Continuity recovery is simpler than SPEC-M3 assumed.** The primary recovery is **catch
  `previous_response_not_found` → strip the anchor → full resend** (which R1 no-trim makes free —
  we always still have the full input). The R2 silence-watchdog is demoted from centerpiece to a
  bounded **backstop** (for a genuine hang, or if backend behavior regresses). This collapses
  codex-lb's nine-file labyrinth into a catch-and-resend the controls prove is sufficient.

- **D3 — Ownership routing keeps its value** — not to prevent a hang, but to avoid paying the
  rejected round-trip at all (route an anchored turn to its owner so the anchor is live).

- **D4 — `prompt_cache_key` affinity is a real, addressable cost.** HTTP re-bills the full history
  every turn (at the cached rate); maximizing cache hits (stable key + owner affinity) is the lever.
  The alias/translated path currently sets no key → cache-misses every turn → **fix it** (low
  effort, high value, needs no WS).

- **D5 — Predictive standby prewarm becomes cheap** via `generate:false`. Reserve it for a
  failure-signal trigger (owner near its limit / elevated errors), not unconditional 2× cost.

## Impact on SPEC-M3

- §2 premise ("silently accepts … hangs indefinitely") is **not current raw-backend behavior** —
  a dead anchor is a fast `previous_response_not_found` 400. Revise the framing.
- R2 (silence watchdog): **backstop, not primary.** Primary = error-code recovery (new).
- R1 (no-trim) and ownership routing: **unchanged and validated.**
- WS transport (was M5, deferred): **de-risked and proven.** The efficiency win (86×) is large and
  the wedge on WS is a fast catchable error, so building WS is both worthwhile and lower-risk than
  the silent-hang model implied.

Full context: memory `handover-prefill-experiment`.
