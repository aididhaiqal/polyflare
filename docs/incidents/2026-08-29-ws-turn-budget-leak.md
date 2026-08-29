# 2026-08-29 — WS turns leak their logical-turn attempt budget

## Symptom

A long research turn dies with HTTP 400 `logical_turn_attempts_exhausted`
("This logical turn exhausted its upstream attempt budget") after N successful tool rounds,
where N = `max_account_attempts`. **No round failed.** The user's report was exactly right:
"there isn't any exhaustion failure, so why did we get hit by this budget".

## Root cause

The WS pump CONSUMES and CLEARS the turn budget through two different keys, and the clear is
gated on a condition the consume is not.

- **Consume** (`ws_relay/pump.rs`, via `try_consume_active_turn_attempt`) uses
  `turn_telemetry.logical_turn_key()` — refreshed every round, so it always charges.
- **Clear** (`ws_relay/pump.rs:1403`) uses a *separate* `active_turn_key` variable, and only runs
  inside `if let Some(id) = sniff_completed_id(&text)`.

`sniff_completed_id` (`ws_relay/sniff.rs:19`) requires a FULL `serde_json` parse of the frame plus
a present `response.id`. When that returns `None` the turn completed but nothing clears, so the
charge is permanent. `active_turn_key` is also set to `None` immediately after each clear, so a
later completion clears with no key at all — a silent no-op.

The HTTP/SSE path does not share the bug: `watchdog.rs` clears from the same `ctx.logical_turn_key`
it consumed with, at terminal sighting, needing no id.

## Evidence (instrumented, 2026-08-29)

Two turns interleaved in one mixed WS+SSE run:

```
ad434b30e513:  consume=1 -> clear removed=true   (x8, every round)   <- HTTP/SSE, correct
883d9beb0ee0:  consume=1,2,3,4,5,6,7,8,9         never cleared        <- WS, leaking
               ...with a single "clear NO-KEY" in the middle
```

With `supports_websockets = false` (HTTP only) the leak does not reproduce: 7/7 rounds paired
consume->clear cleanly.

## Two wrong turns taken while diagnosing (recorded so they are not retried)

1. **"The 503 storm poisoned the turns."** Disproved by the timeline: the server restart clears the
   in-memory registry, and exhaustion recurred afterwards with a clean run of successes in between.
2. **"The 64 KiB oversized-SSE-line cap eats the terminal."** A patch was written and reverted:
   instrumentation showed that branch firing ZERO times, and observed `max_payload` was ~35 KB.

A third trap: `total_tokens IS NULL` was used as a proxy for "terminal not seen". It is not —
it measures `usage_capture`, a DIFFERENT observer. 100% of SSE rows show NULL usage while those
same streams reach `terminal=Completed`. That proxy produced a confident and wrong localization.

## Fix direction (not yet implemented)

Clear from the same key the consume used, and stop gating progress-detection on id extraction:

- clear with `turn_telemetry.logical_turn_key()` rather than the stale `active_turn_key`, and
- treat a terminal `response.completed` as progress whether or not `response.id` parses.

Keep the failure-terminal exclusion intact — only a COMPLETED generation may clear, since that is
the amplification bound the budget exists for.

## Mitigation in place

`max_account_attempts` raised 8 -> 64 on the laptop replica (settings row). This is headroom, not a
cure: a long enough turn still dies, and a genuinely amplifying turn now gets 64 upstream attempts
instead of 8.

## Separate, still-open issue

`usage_capture` records NO usage for any non-WS row (160/160 observed), so `total_tokens` is NULL
for them while WS rows are populated. Unrelated to the budget leak; worth its own investigation.
