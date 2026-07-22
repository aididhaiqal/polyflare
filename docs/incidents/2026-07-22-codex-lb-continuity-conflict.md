# 2026-07-22 — codex-lb continuity-owner conflict (field incident; design directives for PolyFlare)

Status: directives bound into PolyFlare — see `docs/superpowers/plans/2026-07-22-continuity-owner-conflict-hardening.md`
and the `### S3(a)` addendum in `docs/DESIGN-DECISIONS.md`. Regression fixture:
`crates/polyflare-server/tests/continuity_owner_conflict.rs`.

## What happened (field incident, production trace available)

After pulling codex-lb upstream (#1430 "restore architecture ratchets" + new
`app/modules/proxy/continuity.py`), a long-running Codex conversation started failing
permanently with:

    503 "Account-owned continuity sources conflict; retry the logical turn"
    (error_code=continuity_owner_conflict, POST /backend-api/codex/responses)

Mechanism, confirmed from logs + DB:

1. A conversation's turns were served on account A (`previous_response_id` chain owned by A;
   owner resolvable from the request-log index).
2. The same client session's **sticky-affinity row** (kind=codex_session, keyed by session
   header / turn-state) pointed at account B — because capacity-weighted routing had sent other
   first-turns to B, and codex-lb's codex_session sticky rows have **no TTL** (rows from 9 days
   earlier were still authoritative).
3. The conversation's HTTP bridge sat idle → `evict_idle` destroyed the in-memory pin → the next
   follow-up re-resolved ownership from persistent sources → request-log owner (A) vs sticky row
   (B) disagreed → upstream's new check **fails closed with a terminal 503 and no recovery path**.
4. The Codex client blind-retries the same logical turn with the same `previous_response_id`
   → every retry hits the same persistent conflict → 120+ consecutive 503s; the conversation is
   permanently wedged. There is no config toggle. The only remedy was manually deleting all
   codex_session sticky rows from the live SQLite DB and restarting — twice, because the first
   date-filtered wipe missed older rows (no TTL).

## Why this matters for PolyFlare

The failure is not the conflict *detection* — it's that (a) affinity state was allowed to
masquerade as ownership evidence, and (b) the resolution was a terminal error the client's
natural retry behavior can never escape. Both are already prohibited by PolyFlare's recorded
decisions; this incident is field proof they're load-bearing.

## Directives (bound to existing decisions; tests added)

1. **Affinity is never ownership.** S3's ordering (continuity ownership → session affinity →
   availability → health) already implies this: when the resolved owner and a session-affinity
   hint disagree, that is NOT a conflict — ownership wins, the turn routes to the owner, and the
   stale affinity entry is overwritten. Assert in code/review that no affinity source can ever
   veto or contradict a resolved `owning_account`.
2. **No terminal errors for resolvable disagreements.** If two *hard* ownership sources ever
   truly disagree (split-brain: store vs runtime), do not return a client-visible error loop.
   Enter Recover: strip the anchor → reselect → full resend → `record_recovery` re-homes the
   session (same path as the TA6 revision; transport findings showed HTTP-SSE full resend is
   cheap and wedge-immune). A client-visible error is acceptable only if the client's natural
   retry converges — codex-lb's "retry the logical turn" failed this test because Codex CLI
   retries the same turn verbatim.
3. **All affinity state gets a TTL.** codex_session-style rows without expiry become false
   ownership evidence over time. Every affinity entry must have a bounded lifetime and be
   safely deletable at any moment without breaking conversations (ownership must survive an
   affinity wipe — verify with a test that clears the affinity store mid-conversation).
4. **Ownership-decision observability.** One log line per selection: which sources resolved
   (owner index / affinity / durable session), what each said, which won, and why. codex-lb's
   `continuity_owner_resolution` line is what made the incident diagnosable in minutes — keep an
   equivalent even in "thin observability."
5. **Regression fixture from the real trace** (added to the wedge-regression suite):
   - conversation anchored on account A (owner index says A),
   - session-affinity entry for the same session key points at B,
   - the in-memory/bridge pin is evicted (idle),
   - follow-up arrives carrying the A-owned `previous_response_id`.
   Expected: routes to A (or enters Recover and re-homes); the stale affinity entry is
   corrected; a repeated identical follow-up MUST succeed. Explicitly assert it does NOT return
   the same error twice in a row (that's the codex-lb failure signature).

## How each directive lands in PolyFlare

| # | Directive | Where |
|---|-----------|-------|
| 1 | Affinity never ownership | `CodexContinuity::prepare` resolves anchor-map-first; the session row only fills in on a miss (`owner.is_none()`), and `record_completion`/`record_recovery` overwrite the stale row. Locked by unit tests in `crates/polyflare-server/src/continuity.rs`. |
| 2 | No terminal error loops | `apply_ownership` (`crates/polyflare-server/src/watchdog.rs`): pinned-but-ineligible owner ⇒ `RouteDecision::Recover`, never an error. Convergence locked by the e2e fixture (repeated identical follow-up always succeeds). |
| 3 | Affinity TTL | `run_retention_pass` prunes `continuity_sessions` (`last_activity_at`) and `continuity_anchors` (`created_at`) at a fixed 30-day TTL, always on. Wipe-safety locked by the mid-conversation `DELETE FROM continuity_sessions` e2e scenario. |
| 4 | Resolution observability | `continuity_owner_resolution` tracing line in `CodexContinuity::prepare`: anchor owner, session owner, which won, `stale_affinity` flag. Content-free (ids + hashed session key only). |
| 5 | Regression fixture | `crates/polyflare-server/tests/continuity_owner_conflict.rs` (PolyFlare has no in-memory bridge pin — resolution is store-backed each turn, so the "pin evicted" precondition is inherently true). |
