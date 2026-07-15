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
- [x] **M3 — Continuity engine (the wedge fix)** — *merged (PR #4, merge `4d29d68`)*
  - **M3-core ✅:** explicit per-conversation state machine + silence watchdog (arm-only-on-anchor, peek-before-relay, cancel-safe recover) + ownership routing + content-free anchor-map persistence + R1 (never-trim-full-resend). Reshaped the `Continuity` trait to `async prepare/observe`. **wedge-regression e2e GREEN** (the guardrail). Per-task + whole-branch (opus) reviewed; no-anchor failover + `PreparedRequest` Debug redaction fixes applied. The `store:false` anchor wedge is structurally eliminated.
  - **M3-followups (post-merge track):** F2 per-account refresh **singleflight** (HIGH — rotation race can wrongly deactivate accounts) + surface `update_tokens` persist failures; F3 live error/cooldown tracking + transient-endpoint backoff (populate the health_tier/error_count/cooldown fields M2 left inert); F1 R3 reasoning-replay cache; F5 oauth id_token decouple; F4 O(N) snapshot-query collapse; anchor/session retention prune.
- [ ] **M4 — Anthropic executor + `Anthropic → Codex` translator** — *designed (`docs/SPEC-M4.md`); impl blocked on user answers (U1–U5, esp. the model-alias pairs)*
  - Anthropic HTTP executor + rate-limit semantics + account capability flag; the one real cross-translator (event-by-event streaming SSE mapping — doc-verified) + golden replay tests + **bidirectional model-alias mapping** + per-tier payload-override; verify Anthropic's trusted-access signal (TA6). Crux: reshape `Translator` to stateful 1→N. Split M4a (native path) / M4b (cross-translator + mapping) / M4c (inverse, deferred).
- [ ] **M5 — Fingerprint parity + CI gates + observability** — *designed (`docs/SPEC-M5.md`); E4 verified — byte-parity premise revised*
  - **E4 finding:** `codex-rs` default uses native-tls (OS TLS), not rustls → byte-identical ClientHello is unachievable (default path) and unnecessary (fleet is TLS-heterogeneous). Reframed: exact **HTTP-layer parity** (headers/order/version) + plausible-real pinned-rustls TLS. **latency-regression gate ✅ built** (`feat/m5-fingerprint`). fingerprint-parity gate + HTTP-layer parity need M5-Q1/Q2 + runtime captures. Thin metrics + request logs (content-safe).
- [ ] **L5 (post-MVP) — Dashboard**
  - Embed the React/Vite build in the binary (`rust-embed`); resolve the parked dashboard redesign; port from codex-lb + better-ccflare.

## Cross-cutting deferred / parked
- **Account warming:** traffic to an account only on real demand — KEEP OAuth refresh + one-time onboarding probe; scheduled synthetic warming OFF by default (fights impersonation, spends quota). See DESIGN-DECISIONS → "Parked principle".
- **Additive later, when data justifies:** inverse `Codex → Anthropic` translator; Gemini / Chat-Completions formats; the other 7 selector strategies (force-finish, priority-drain, …); distributed `Coordinator` (single-binary is the target; the trait seam keeps it additive).

## Progress
- **M1 + M2 + M3: complete** (merged to main). PolyFlare is a store-backed multi-account Codex load balancer with the continuity engine that structurally kills the `store:false` anchor wedge. MVP CI gates on main: **e2e ✓ · wedge-regression ✓** (latency-regression ✓ built on `feat/m5-fingerprint`; fingerprint-parity pending).
- **In progress:** M3-followups (F2 refresh singleflight first). **M4 + M5 designed** (`SPEC-M4.md` / `SPEC-M5.md`); implementation blocked on user input — M4 needs the model-alias pairs (U2); M5 needs the TLS-target decision + runtime captures.
