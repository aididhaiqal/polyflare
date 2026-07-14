# SPEC-M5 — Egress Fingerprint Parity + CI Gates + Observability

**Status: PROPOSED — awaiting user approval.** Independent of M3/M4 (touches the Codex egress path from M1, not continuity or Anthropic). Can branch off `main`.

**This spec revises a *locked* design decision.** E4 in DESIGN-DECISIONS chose "(b) byte-identical parity, affordable because the target is Rust `codex-rs`." Ground-truth research (2026-07-14, actual `openai/codex` clone + `cargo tree -p codex-cli`) shows that premise does **not** hold for codex-rs's default traffic path. §2 explains; §3 reframes the goal.

---

## 1. Goal

PolyFlare's Codex egress should be **indistinguishable from a real `codex-rs` CLI at the layers OpenAI's edge actually keys on**, so that multi-account pooling can't be detected via fingerprint. Plus the two MVP CI gates that live in M5 — **fingerprint-parity** and **latency-regression** — and thin observability (metrics + request logs, counts only, never conversation content).

## 2. The E4 finding — byte-identical-to-one-canonical-ClientHello is neither achievable nor necessary

Verified against `openai/codex` @ `5bed644` via a real clone + `cargo tree -p codex-cli -e features` (ground-truth resolved features for the shipped binary, not just Cargo.toml declarations):

- **Default install uses native-tls, not rustls.** `reqwest 0.12.28` compiles with *both* `default-tls` (native-tls) and rustls features; reqwest's `TlsBackend::default()` picks **native-tls** when both are present. codex only calls `.use_rustls_tls()` when `CODEX_CA_CERTIFICATE` / `SSL_CERT_FILE` is set. So the default path is **OS-native TLS** — Secure Transport (macOS), SChannel (Windows), OpenSSL (Linux) — whose ClientHello bytes are OS/library internals, **not controllable by pinning Rust crates.**
- **The default path likely speaks HTTP/1.1, not HTTP/2.** native-tls only offers ALPN when the opt-in `native-tls-alpn` feature is on; codex-rs never enables it → no ALPN → no h2 negotiation on the default path. (Confirm via capture — §5.)
- **The rustls path (the only one crate-pinning can match) is a minority config:** it runs only under the CA-override env vars, or for WebSocket traffic (`tokio-tungstenite` pinned with rustls). On that path, codex installs **aws-lc-rs** as the crypto provider and rustls resolves with `prefer-post-quantum` active → a distinctive **X25519MLKEM768** hybrid key share.
- **Therefore:** the real codex fleet presents a **heterogeneous** TLS fingerprint (one per OS/TLS-lib/config). There is no single canonical codex ClientHello. Byte-identical parity is (a) **unachievable** for the default native-tls path and (b) **unnecessary** — the bar is *blending into the heterogeneous real-codex population*, not matching one exact byte sequence.

**Ground-truth pinned versions** (if we do target the rustls path): `reqwest 0.12.28`, `rustls 0.23.36`, `hyper 1.8.1`, `h2 0.4.13`, `tokio 1.52.3`, `aws-lc-rs 1.16.2`.

## 3. Reframed approach — parity where it counts, at the layer we control

Split the fingerprint into two layers with different strategies:

### 3.1 HTTP-layer parity — the priority (fully controllable, highest-signal identity)

The header set + value formats + **order** + HTTP version is what identifies a client *above* TLS, and it's 100% under our control. Match `codex-rs` exactly:

- **User-Agent**: `{originator}/{version} ({os_type} {os_version}; {arch}) {terminal_token}` (originator default `codex_cli_rs`).
- The per-request identity headers: `x-codex-turn-state`, `x-codex-window-id`, `x-codex-turn-metadata` (JSON: turn_id, turn_started_at_unix_ms, sandbox, workspace/git…), `session-id`/`thread-id`, `x-codex-beta-features`, `accept: text/event-stream`, `content-type` only-if-absent, `authorization: Bearer` applied **last**.
- Header **order** is built by a specific chained-insert sequence (login → core → codex-api); byte-parity requires replaying that order, not just the name set.

This is exactly the surface the `codex-fingerprint-gaps` memory already enumerated (10 gaps, 5 HIGH turn-state identity headers). **Those gaps become the concrete M5 HTTP-parity tasks.**

### 3.2 TLS-layer plausibility — choose a real-codex-consistent config, don't chase byte-identity

Pick a target (see M5-Q1):

- **T-rustls (recommended for CI-checkability):** pin `rustls 0.23.36` + aws-lc-rs + `prefer-post-quantum`. Produces a *deterministic, Rust-controlled, CI-assertable* ClientHello that matches a **real** codex config (the CA-override/WebSocket path). Trade-off: it matches a *minority* codex config, not the default OS-native one — a sophisticated observer could note that PolyFlare always presents the "CA-override" ClientHello. Given the fleet is heterogeneous anyway, this is a plausible-real fingerprint, not a fabricated one.
- **T-native (blend with the default population):** use native-tls on the same OS as the codex users we impersonate → ClientHello matches the *default* codex population on that OS. Trade-off: OS-dependent, not byte-deterministic, not fully CI-checkable, and PolyFlare would run native-tls (giving up rustls's control).

### 3.3 CI gates (MVP deliverables)

- **fingerprint-parity gate:** capture PolyFlare's egress against a captured `codex-rs` golden fixture and assert equality at the **HTTP layer** (headers/order/version — fully automatable). For T-rustls, extend to a ClientHello assertion via a TLS-capturing test harness (M5-Q2).
- **latency-regression gate:** assert the proxy's added latency (ingress→egress overhead, excluding upstream generation) stays within a fixed budget; fail CI on regression.

### 3.4 Observability

Thin metrics (request counts, per-account routing, error/cooldown transitions) + structured request logs — **counts and ids-of-our-own only, never session/account/api-key ids or any conversation content** (standing content-safety constraint). This also lands the "add a lint/grep gate against `?prepared`/`?req`/`?body`" follow-up the M3 review recommended for when logging arrives.

## 4. Decisions for the user

- **M5-Q1 — TLS target:** T-rustls (deterministic, CI-checkable, matches a real-but-minority codex config) vs T-native (blends with the default OS-native population, but OS-dependent + not CI-deterministic)? *My rec: T-rustls* — the fleet is heterogeneous so a pinned-rustls ClientHello is a plausible-real codex fingerprint, and it's the only option that's deterministic and gate-able in CI.
- **M5-Q2 — parity-gate depth:** HTTP-layer only (fully automatable, ships now) vs also ClientHello (needs a TLS-capturing harness)? *My rec: HTTP-layer as the blocking MVP gate; ClientHello as an additive check once T-rustls is chosen.*

## 5. Needs runtime capture (can't be sourced)

A single real `codex` run (no CA env vars) through a tap resolves most of this:

- Real release `CARGO_PKG_VERSION` baked into shipped binaries (source shows a `"0.0.0"` placeholder).
- Actual ClientHello bytes per OS for the native-tls default path (only if T-native).
- **HTTP version on the live Responses call (1.1 vs 2)** — load-bearing for the HTTP-parity gate.
- `x-codex-installation-id` header-vs-body placement and whether `OpenAI-Beta` rides the HTTP SSE path (vs only the WebSocket path).
- The exact header **order** on the wire (source shows the assembly sequence; a capture confirms it).

## 6. Relationship to prior work

- **`codex-fingerprint-gaps`** memory — its 10 enumerated HTTP-layer gaps (5 HIGH) are the M5 §3.1 task list.
- **M1's executor** already sets `user-agent: codex_cli_rs` as "minimal M1 laundering"; M5 §3.1 makes the whole header set + order exact.
- **Scope note:** M5 targets the Codex egress. The Anthropic egress (M4) has its own native fingerprint; a parallel Anthropic-parity pass is out of MVP scope (additive later).
