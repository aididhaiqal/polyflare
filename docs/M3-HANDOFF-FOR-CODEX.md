# M3 Handoff — for Codex (guided by Claude)

**You (Codex) are implementing Milestone 3: the Continuity Engine — the "wedge fix" that is the entire reason PolyFlare exists.** Claude has built and merged M1 + M2 and will *guide and review* your work at each gate (design → plan → per-task → whole-branch). This doc is your entry point. Read it fully, then the linked references, then propose a design before writing code.

---

## 0. Working agreement (how this goes)
1. **Design first.** Do NOT start coding. Read everything in §2, then write an M3 design/spec (`docs/SPEC-M3.md`) and get Claude's sign-off. The design space here is subtle — a wrong shape re-creates the wedge.
2. **Then a plan**, then TDD implementation (`docs/PLAN-M3.md`), one bite-sized task at a time.
3. **Branch + PR + green CI before merge.** Work on a `feat/m3-*` branch off `main`; open a PR; CI (`fmt --check` + `clippy -D warnings` + `build` + `test`) must be green before merge.
4. **Claude reviews** the design, the plan, and every task/whole-branch diff. Expect faithfulness challenges — this is the crown jewel.

---

## 1. What PolyFlare is + current state (M1 + M2, on `main`)
PolyFlare is a from-scratch **Rust** multi-provider LLM-CLI load balancer (successor to codex-lb). Merged so far:
- **M1** — cargo workspace (6 crates); provider-neutral core (`Format` + identity translator registry + 5 trait seams); a Codex **HTTP-SSE pass-through executor** (non-buffering, `reqwest` `bytes_stream`); axum ingress relaying as `text/event-stream`; scriptable mock upstream (`polyflare-testkit`); binary + full e2e.
- **M2** — SQLite (`sqlx`) store + accounts/usage schema; **XChaCha20-Poly1305** at-rest token crypto; account repo; **zero-re-auth codex-lb importer** (Fernet→XChaCha); `capacity_weighted` **selector** (pure, seed-deterministic, verified faithful to codex-lb `logic.py`); **OAuth refresh** (decode-only claims + `POST /oauth/token`); **pool wiring** into ingress. Per request: `assemble_snapshots → selector.pick(rng_seed=None) → get account → refresh-if-stale (persist re-encrypted / mark reauth on failure) → decrypt → execute → relay`; no-eligible → 503, upstream → generic 502.

**Crates:** `polyflare-core` (Format, translator registry, the five traits, `AccountSnapshot`/`SelectionCtx`/`AccountId`, `CapacityWeighted` selector, `PreparedRequest`/`ResponseStream`/`ExecError`), `polyflare-codex` (`CodexExecutor`, `oauth`), `polyflare-store` (`Store`/`AccountRepo`/`TokenCipher`/`import`), `polyflare-anthropic` (stub, M4), `polyflare-testkit` (`MockUpstream`/`MockOAuth`), `polyflare-server` (axum ingress/config/`AppState`/binary, `snapshot::assemble_snapshots`).

**IMPORTANT architectural note:** M1/M2's Codex executor is a **stateless HTTP-SSE pass-through** — it does NOT inject or trim `previous_response_id` at all today. codex-lb's continuity lives in a WebSocket session-bridge with a durable session store, `response_create_gate`, etc. — **none of which PolyFlare has**. So **M3 introduces continuity into PolyFlare for the first time** — you are building the *fixed* design from day one, not porting codex-lb's buggy machinery. (WS transport itself is deferred to M5-ish; continuity over HTTP-SSE is fine — the `store:false` ephemeral-anchor problem is transport-independent.)

## 2. Read these (in order)
1. **`docs/POLYFLARE-DESIGN.md` §4.1** — the Continuity engine design (state machine, R1/R2/R3).
2. **`docs/DESIGN-DECISIONS.md`** — **C1** (continuity = explicit state machine + watchdog; R1 never-trim-store:false-full-resend / R2 fixed-N ~30s watchdog / R3 reasoning-replay), **C1(a)** (trim-policy: hard never-trim default + per-account opt-in seam), **C1(b)** (watchdog N: fixed ~30s, path to adaptive), **S3** (selection ordering — continuity ownership is the top hard pre-filter), **M2-GATE1** (the `Continuity` trait is PROVISIONAL — reshape it now), **TA6/TA6(a)** (capability routing, related).
3. **`docs/reference/codex-lb-continuity-reference.md`** — the faithful codex-lb anchoring mechanism + the exact wedge (7 steps) + why the owner-guard doesn't fix it + why reasoning/watchdog are new work. **This is your source of truth for what codex-lb does and precisely what the fix must do.**
4. **`docs/reference/codex-lb-port-reference.md`** — the broader codex-lb port reference (schema, selector, OAuth, error transitions).
5. The M1/M2 code you'll build on: `polyflare-core/src/{traits.rs,types.rs}`, `polyflare-server/src/{ingress.rs,app.rs}`, `polyflare-store/src/{store.rs,account.rs}`.

## 3. Standing conventions (Claude enforces these — follow them)
- **TDD**: failing test → confirm it fails → minimal impl → confirm pass → `fmt`/`clippy` → commit. One bite-sized task per commit.
- **Faithful to codex-lb where porting behavior**; verify against the real source (`../codex-lb`), don't guess. Where M3 is NEW (reasoning cache, watchdog), design it cleanly.
- **Any secret-bearing type needs a redacting `Debug` + a test asserting redaction.** Never log/print a token value. (Reasoning content may be sensitive user data — treat it carefully; don't log it.)
- **Test fixtures must model the REAL wire shapes** (a fixture that diverges from reality hides bugs — this exact class of bug bit M2a).
- **Runtime-checked sqlx** (`query`/`query_as::<_,T>`/`FromRow`, no compile-time macros, no `DATABASE_URL`); `"window"` and other SQLite keywords double-quoted; forward-only migrations.
- **Streaming stays non-buffering** on the response path.
- Gates: `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace` all clean before each commit; PR CI green before merge.

## 4. M3 scope — the Continuity engine (the wedge fix)
Build the design from `DESIGN-DECISIONS.md#C1`, adapted to PolyFlare's HTTP-SSE architecture:

1. **Reshape the `Continuity` trait** (it's provisional per M2-GATE1). Expected shape: `async fn prepare(&self, req: PreparedRequest, ctx: &RequestCtx) -> Result<PreparedRequest, ContinuityError>` (reads/writes persisted session state; the impl holds a store handle) + an `observe`/`record` path to advance the state machine on the response outcome. Define `ContinuityError`. Decide the exact `observe` signature from the state-machine's needs.
2. **Per-conversation state machine** (`Fresh → Anchored → Reattaching → {Anchored | Recover}`) with anchor state persisted in the store: `{ session_key, anchor_response_id, owning_account, last_input_fingerprint/count, reasoning_cache_ref, state }`. A new `sqlx` migration + a store repo for it. Session-key derivation like codex-lb (`x-codex-turn-state` header → hard; session header → hard; else soft).
3. **R1 — never trim a `store:false` full-resend** to a bookkeeping-derived anchor. This is the core fix: a full-resend (client re-sent everything) is *always* sent in full; do NOT strip it to a possibly-dead anchor. (Trimming a `store:true` client-kept history is the only safe trim; PolyFlare forces `store:false` like codex-lb, so effectively: don't trim full-resends. The per-account `allow_store_false_trim` seam from C1(a) stays OFF.)
4. **R2 — proactive watchdog** on any request carrying a proxy-injected anchor: a bounded timer (fixed ~30s per C1(b), configurable); on **silence** (no `response.created`/first-token within N) → treat the anchor as dead → cancel + **replay the full history WITHOUT the anchor** (recover), and advance the state machine. Silence is the trigger codex-lb lacks — it only reacts to explicit errors. This is what actually kills the wedge.
5. **R3 — reasoning-replay cache**: cache the reasoning items from a completed turn so a reattach/recover can re-inject them (codex-lb loses them entirely). Generalize beyond one protocol.
6. **Selection ordering**: continuity ownership is a HARD pre-filter above the selector (S3) — an anchored conversation must return to its owning account (or Recover). In M2 the selector's continuity-ownership hook is a documented no-op; M3 makes it real.
7. **Wedge-regression e2e** (REQUIRED — this is what makes the fix stick): reproduce the store:false fresh-reattach + full-resend scenario against a mock upstream that **silently accepts `response.create` but never emits `response.created`**, and assert PolyFlare does NOT wedge — the watchdog fires, it recovers with a full resend, and the request completes. Also assert R1 (full-resend not trimmed) and R3 (reasoning replayed).

## 5. Also in M3 scope — follow-ups the M2 whole-branch reviews surfaced
These fit M3 because they need the live runtime-state tracking M2 deliberately left inert (`health_tier`/`error_count`/`last_error_at`/`cooldown_until` all default to 0/None in M2's snapshot assembly):
- **Per-account refresh singleflight** (HIGH): today concurrent requests on a just-stale account fire N parallel OAuth refreshes; if OpenAI rotates the refresh token, the losers replay a spent token → `invalid_grant`/`refresh_token_reused` → an otherwise-healthy account is wrongly marked `reauth_required`. Add a per-account refresh lock/singleflight. Pair it with surfacing `update_tokens` persist failures (M2 swallows them via `let _ =`).
- **Live error/cooldown tracking + transient-endpoint backoff**: populate `error_count`/`last_error_at`/`cooldown_until` from real request outcomes (the selector already reads them — see `select.rs` eligibility). A persistently-5xx token endpoint currently re-refreshes a stale account every request with no backoff; a persisted `cooldown_until` fixes it.
- **O(N) snapshot query collapse** (LOW): `assemble_snapshots` does `list()` + 2 `latest_window_usage` per account + a `get()` + `decrypt_tokens()` per request. Collapse into fewer queries (a `GROUP BY account_id, window` join; a single `(last_refresh, EncryptedTokens)` fetch).
- **oauth.refresh: decouple success from id_token decodability** — a valid access+refresh token with a malformed `id_token` currently discards good tokens (`MalformedJwt`). Make claims-decode best-effort.

These are secondary to the continuity engine — sequence them after (or alongside) the core wedge fix as separate tasks.

## 6. Suggested approach (yours to refine in the design pass)
- M3 is large; consider splitting: **M3-core** (Continuity trait reshape + state machine + session store + R1 + R2 watchdog + wedge-regression e2e) then **M3-follow-ups** (reasoning cache R3, singleflight, live error tracking/backoff, query collapse). Propose your split in the design.
- The hard part is R2's watchdog interacting with the streaming executor: you need to detect "no first upstream event within N" while relaying SSE, and be able to cancel + restart with the full payload. Study how `CodexExecutor` streams (`polyflare-codex/src/executor.rs`) and where continuity `prepare`/`observe` slot into the ingress flow (`polyflare-server/src/ingress.rs`).
- Reproduce the wedge in a test FIRST (a mock upstream that accepts-then-goes-silent) — a red wedge test is the clearest spec for R2.

## 7. What Claude will check in review
- **Does it actually kill the wedge?** The wedge-regression e2e must reproduce silence-after-accept and prove recovery. A green suite that doesn't reproduce the wedge is not done.
- **R1 faithfulness**: a full-resend is never trimmed to a bookkeeping anchor. Verify against `codex-lb-continuity-reference.md`.
- **Watchdog correctness**: bounded N, fires on silence, cancels cleanly, recovers with full resend, no leak/panic, cancel-safe on client disconnect.
- **Security**: no token/reasoning-content leak to logs/errors; redacting Debug + tests on any new secret-bearing type.
- **Seam quality**: the reshaped `Continuity` trait composes cleanly with the ingress + selector + store; ownership pre-filter is a real hard filter now.
- The standing gates (fmt/clippy/CI green, real-schema fixtures, faithful ports).

Start by reading §2, then propose `docs/SPEC-M3.md`. Claude will review it before you plan or code.
