# PolyFlare Roadmap

**Overall goal.** A single-binary Rust load balancer that fronts multiple provider accounts (Codex + Anthropic), speaks multiple client wire formats, translates between them where needed, and routes each request to the best available account while presenting the native CLI's fingerprint on egress — with **continuity done right** (no `previous_response_id` "wedge") and a **byte-identical egress fingerprint** that the old Python stack could not achieve.

**Definition of done (the MVP).** The lean core (L0–L4) running as a daily driver: a dual-pool gateway with the continuity state machine and byte-parity fingerprint, and the four first-class test gates green — **e2e**, **latency-regression**, **wedge-regression**, and **fingerprint-parity**. The dashboard (L5) is the additive follow-on, not part of the MVP bar.

Design: [docs/POLYFLARE-DESIGN.md](docs/POLYFLARE-DESIGN.md) · Decisions & rationale (with *Why / Trade-off / Revisit-if*): [docs/DESIGN-DECISIONS.md](docs/DESIGN-DECISIONS.md)

## Milestones

- [x] **M1 — Skeleton + Codex identity pass-through** — *merged*
  - Cargo workspace (6 crates); provider-neutral core (`Format` + identity translator registry + the five trait seams); Codex SSE pass-through executor (non-buffering); axum ingress relaying as `text/event-stream`; scriptable mock-upstream testkit; binary + full e2e. 11 tests; CI (fmt/clippy/build/test). Whole-branch-review fixes applied (100 MiB body limit; `Account` Debug token redaction).
- [ ] **M2 — Store + accounts + selector + OAuth import**
  - SQLite (`sqlx`); XChaCha20-Poly1305 at-rest crypto; zero-re-auth importer from codex-lb; one default `Selector` (relative-availability + burn/preserve) with the **continuity-ownership-first** ordering; per-API-key capability config groundwork (TA6 / TA6(a)).
  - **Kickoff gate — M2-GATE1:** reshape the `Selector` / `Continuity` / `Coordinator` trait seams (async + `Result` + richer inputs) *before* building on them. See DESIGN-DECISIONS → M2-GATE1.
- [ ] **M3 — Continuity engine (the wedge fix)**
  - Explicit per-conversation state machine + watchdog + reasoning-replay cache; **wedge-regression e2e**; session capability sticky-flag; stream idle/read-timeout watchdog.
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
- **M1: complete** (merged to `main`). Next: **M2**, opening with the M2-GATE1 seam reshape.
