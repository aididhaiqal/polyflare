# Continuity owner-conflict hardening (codex-lb 2026-07-22 incident directives)

**Goal:** Bind the five directives from the codex-lb `continuity_owner_conflict` field incident
(2026-07-22) into PolyFlare as code, tests, and versioned docs — proving affinity can never
masquerade as ownership, disagreements never become terminal client-visible error loops, all
affinity state is bounded-lifetime, and the exact incident state-sequence is a permanent
regression fixture.

**Why planning is required:** High-risk tier — production-data deletion (retention pruning of two
new tables on the live DB) on the wedge-sacred continuity path (S3/C1 core).

**Acceptance:**
- The incident is versioned at `docs/incidents/2026-07-22-codex-lb-continuity-conflict.md`, and
  DESIGN-DECISIONS.md carries an S3 addendum citing it as field proof.
- Unit + e2e tests encode: (1) anchor-map owner beats a stale session-row owner with NO error;
  (2) the stale session row is corrected by the next completion; (3) a repeated identical
  follow-up NEVER yields the same client-visible error twice (the codex-lb failure signature);
  (4) wiping `continuity_sessions` mid-conversation does not break the conversation.
- `run_retention_pass` prunes `continuity_sessions` (by `last_activity_at`) and
  `continuity_anchors` (by `created_at`) with a fixed 30-day TTL — always on, batched,
  failure-isolated like the existing prunes.
- `CodexContinuity::prepare` emits one content-free `continuity_owner_resolution` tracing line
  per resolution: what the anchor map said, what the session row said, which won, and whether a
  stale-affinity disagreement was detected.
- Production-data hygiene: prune targets are exactly the two continuity tables; batch size reuses
  `PRUNE_BATCH_SIZE`; per-table failure is logged and isolated (never crashes the pass); deletes
  are age-gated strictly `< cutoff`; repo tests prove rows at/after the cutoff survive. No
  backup step — the tables are rebuildable affinity/diagnostic state, and safe deletability at
  ANY moment is exactly what the wipe test proves.
- Verification: full workspace `cargo test`, `cargo fmt --all -- --check`, `cargo clippy`.

### Outcome 1: Incident doc + design-decision addendum
- Work: create `docs/incidents/2026-07-22-codex-lb-continuity-conflict.md` (mechanism, why it
  matters, the five directives). Append an `### S3(a)` addendum to `docs/DESIGN-DECISIONS.md`
  binding the directives to S3's ordering / TA6(b)'s recover path and linking the incident doc.
- Verify: files render; references resolve.

### Outcome 2: Directive 1+5 unit tests (resolution layer)
- Work: in `crates/polyflare-server/src/continuity.rs` tests — (a) seed anchor `resp_1→A` AND a
  session row whose `owning_account_id='B'`; `prepare` with the anchor must pin A, no error;
  (b) after `observe(Completed on A)`, the session row's owner reads A (stale affinity
  corrected). Requires a repo seam or direct SQL to force the B row (the stale-state injection).
- Verify: `cargo test -p polyflare-server --lib continuity`

### Outcome 3: e2e regression fixture from the real trace
- Work: new `crates/polyflare-server/tests/continuity_owner_conflict.rs` with a two-account
  harness (A, B; MockUpstream distinguishing accounts by bearer token) and a kept store-pool
  handle. Scenarios: (a) anchored-on-A + stale-B session row → follow-up carrying the A-owned
  anchor routes to A, 200, session row corrected to A, and the SAME follow-up repeated succeeds
  again (assert both 200 — strictly stronger than "not the same error twice"); (b) same state
  but A hard-blocked (`paused`) → Recover: anchor stripped, full resend lands on B,
  `record_recovery` re-homes to B, and a client blind-retry with the ORIGINAL anchor still
  succeeds (convergence — never two identical terminal errors); (c) mid-conversation
  `DELETE FROM continuity_sessions` → follow-up with the anchor still routes to A via the anchor
  map, 200 (ownership survives an affinity wipe).
- Risks/open questions: PolyFlare has no in-memory bridge pin (resolution is store-backed every
  turn), so the incident's "pin evicted" precondition is inherently true — note this in the
  test doc rather than simulating an eviction.
- Verify: `cargo test -p polyflare-server --test continuity_owner_conflict`

### Outcome 4: ownership-decision observability
- Work: one `tracing::info!(target: "continuity_owner_resolution", ...)` in
  `CodexContinuity::prepare` with content-free fields: `session` (already a sha256 key),
  `anchor_present`, `anchor_owner`, `session_owner`, `resolved`, `source`
  (anchor_map|session_row|none), `stale_affinity` (both sources present and disagreeing).
  Must pass `content_safety_lint` (no forbidden idents Debug-captured).
- Verify: `cargo test -p polyflare-server --test content_safety_lint` + lib tests still green.

### Outcome 5: affinity TTL — retention pruning for the two continuity tables
- Work: `ContinuityRepo::prune_sessions_older_than(cutoff, batch)` (on `last_activity_at`) and
  `prune_anchors_older_than(cutoff, batch)` (on `created_at`), same batched-subselect +
  `batch_size <= 0` no-op guard as `RequestLogRepo::prune_older_than`. Extend
  `run_retention_pass` with a fixed `CONTINUITY_TTL_DAYS = 30` (always on — a disabled-by-default
  knob would not satisfy "ALL affinity state gets a TTL"; deliberately NOT a runtime setting:
  adding an 11th `RuntimeSettingsFields` field churns every test-harness literal for a value
  that is uncritical precisely because deletion is safe at any moment — promote to a setting
  only if a real need appears). Each prune independently failure-isolated (warn + continue),
  mirroring the existing pass. Active conversations are untouched: every completed turn bumps
  `last_activity_at` and inserts a fresh anchor row, so only ≥30-day-idle state ages out; a
  resumed >30-day conversation degrades to an unowned pick + the existing armed-watchdog
  recovery, never a wedge.
- Verify: `cargo test -p polyflare-store continuity` (cutoff-boundary repo tests) +
  `cargo test -p polyflare-server retention`

### Outcome 6: whole-change verification
- Work: run the full gates; inspect the diff.
- Verify: `cargo test --workspace`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets`
