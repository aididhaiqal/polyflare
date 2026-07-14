# PolyFlare Roadmap

**Overall goal.** A single-binary Rust load balancer that fronts multiple provider accounts (Codex + Anthropic), speaks multiple client wire formats, translates between them where needed, and routes each request to the best available account while presenting the native CLI's fingerprint on egress — with **continuity done right** (no `previous_response_id` "wedge") and a **byte-identical egress fingerprint** that the old Python stack could not achieve.

**Definition of done (the MVP).** The lean core (L0–L4) running as a daily driver: a dual-pool gateway with the continuity state machine and byte-parity fingerprint, and the four first-class test gates green — **e2e**, **latency-regression**, **wedge-regression**, and **fingerprint-parity**. The dashboard (L5) is the additive follow-on, not part of the MVP bar.

Design: [docs/POLYFLARE-DESIGN.md](docs/POLYFLARE-DESIGN.md) · Decisions & rationale (with *Why / Trade-off / Revisit-if*): [docs/DESIGN-DECISIONS.md](docs/DESIGN-DECISIONS.md)

## Milestones

- [x] **M1 — Skeleton + Codex identity pass-through** — *merged*
  - Cargo workspace (6 crates); provider-neutral core (`Format` + identity translator registry + the five trait seams); Codex SSE pass-through executor (non-buffering); axum ingress relaying as `text/event-stream`; scriptable mock-upstream testkit; binary + full e2e. 11 tests; CI (fmt/clippy/build/test). Whole-branch-review fixes applied (100 MiB body limit; `Account` Debug token redaction).
- [x] **M2 — Store + accounts + selector + OAuth import** — *merged*
  - **M2a ✅:** SQLite (`sqlx`) store + accounts/usage schema; XChaCha20-Poly1305 at-rest token crypto (redacting Debug); account repository; transactional + idempotent zero-re-auth codex-lb importer (Fernet→XChaCha, parses real DATETIME-text timestamps); `polyflare serve | accounts import` CLI.
  - **M2b ✅:** OAuth refresh (decode-only JWT claims + `POST /oauth/token` + 8-day `should_refresh`) + the ported default `capacity_weighted` `Selector` (pure scoring, seed-deterministic, verified faithful to codex-lb `logic.py`, TA6 pre-filter) + usage-snapshot assembly + **pool wiring into ingress** — per request: assemble → select (`rng_seed=None`) → refresh-if-stale (persist re-encrypted / mark-on-failure) → decrypt → execute → relay; no-eligible→503, upstream→generic 502. PolyFlare is now a working store-backed multi-account load balancer.
- [ ] **M3 — Continuity engine (the wedge fix)** — *next; implemented by Codex, guided by Claude (see `docs/M3-HANDOFF-FOR-CODEX.md`)*
  - Explicit per-conversation state machine + watchdog + reasoning-replay cache; **wedge-regression e2e**; session capability sticky-flag; stream idle/read-timeout watchdog. Plus M2-surfaced follow-ups now in scope: per-account refresh **singleflight**, transient-endpoint **backoff/cooldown** + live error-tracking (populate the health_tier/error_count/cooldown fields M2 left inert), O(N) snapshot-query collapse. Reshape the provisional `Continuity` trait to `async fn → Result` with a session-state handle (M2-GATE1 note).
- [ ] **M4 — Anthropic executor + `Anthropic → Codex` translator**
  - Anthropic HTTP executor + rate-limit semantics + account capability flag; the one real cross-translator (event-by-event streaming SSE mapping) + golden replay tests; verify Anthropic's trusted-access signal (TA6).
- [ ] **M5 — Byte-identical fingerprint + CI gates + observability**
  - rustls/BoringSSL ClientHello parity pinned to `codex-rs` + capture-fixture **parity gate**; **latency-regression gate**; thin metrics + request logs. (First verify codex-rs's actual TLS backend — DESIGN §10 / E4(a).)
- [ ] **L5 (post-MVP) — Dashboard**
  - Embed the React/Vite build in the binary (`rust-embed`); resolve the parked dashboard redesign; port from codex-lb + better-ccflare.

## Cross-cutting deferred / parked
- **Account warming:** traffic to an account only on real demand — KEEP OAuth refresh + one-time onboarding probe; scheduled synthetic warming OFF by default (fights impersonation, spends quota). See DESIGN-DECISIONS → "Parked principle".
- **Additive later, when data justifies:** inverse `Codex → Anthropic` translator; Gemini / Chat-Completions formats; the other 7 selector strategies (force-finish, priority-drain, …); distributed `Coordinator` (single-binary is the target; the trait seam keeps it additive).

## Progress
- **M1: complete** (merged). **M2: complete** (merged — store/crypto/accounts/import + `capacity_weighted` selector + OAuth refresh + pool wiring; PolyFlare load-balances a store-backed account pool). Next: **M3 — continuity engine (the wedge fix)**, to be implemented by Codex with Claude guiding/reviewing (`docs/M3-HANDOFF-FOR-CODEX.md`).
