# PolyFlare

A multi-provider LLM-CLI load balancer, in Rust.

PolyFlare fronts a pool of provider accounts (OpenAI Codex / ChatGPT and Anthropic to start), speaks multiple client wire formats, translates between them where needed, and routes each request to the best available account — presenting the native CLI's fingerprint on egress. It is the Rust successor to [codex-lb](https://github.com/Soju06/codex-lb), adopting the multi-provider translation model from [CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI) and the Anthropic-pool + dashboard ideas from [better-ccflare](https://github.com/tombii/better-ccflare).

> **Status: functional, still early.** The core is built and green — Codex-native `/responses`
> pass-through over pinned TLS, the continuity watchdog + `store:false` wedge fix, `Anthropic → Codex`
> translation, multi-account/multi-pool selection, OAuth (exp-driven refresh + PKCE login), a
> zero-re-auth importer from codex-lb, and an embedded dashboard — all under e2e / wedge-regression /
> latency / fingerprint-parity CI gates. It runs against the real backends today, but it is **not yet a
> daily driver**: notably there is **no admin auth**, and failure-driven health/cooldown routing,
> retry/failover, and prompt-cache stickiness are **not built yet**. See
> [`docs/COMPARISON.md`](docs/COMPARISON.md) for the full have / don't-have ledger.

## Why a rebuild

Two problems in the existing tools motivate building from scratch rather than refactoring:

1. **Continuity done right.** Cross-account `previous_response_id` anchoring can *wedge* a conversation when a `store:false` full-resend is trimmed to an anchor the upstream never persisted. PolyFlare models continuity as an explicit per-conversation **state machine with a watchdog**, so a non-resuming anchor is always detected and recovered instead of stalling.
2. **A real egress fingerprint.** Faking a `User-Agent` on a Python TLS stack still ships a mismatched TLS handshake. Rust's TLS control lets PolyFlare match the native CLI's fingerprint, not just its headers.

Full design: [`docs/POLYFLARE-DESIGN.md`](docs/POLYFLARE-DESIGN.md). The reasoning behind every decision (with trade-offs and revisit-triggers): [`docs/DESIGN-DECISIONS.md`](docs/DESIGN-DECISIONS.md).

## Architecture

A single [tokio](https://tokio.rs) binary. A provider-neutral core — a `Format` enum plus a translator registry — is the spine; Codex and Anthropic are both first-class backends behind five traits: `Translator`, `Executor`, `Selector`, `Continuity`, `Coordinator`. Adding a provider means registering a translator and an executor, never a rewrite.

| Crate | Responsibility |
|---|---|
| `polyflare-core` | `Format` enum, translator registry, core types, the five traits |
| `polyflare-codex` | Codex backend: SSE transport, fingerprint laundering, continuity, OAuth |
| `polyflare-anthropic` | Anthropic backend: HTTP transport + `Anthropic → Codex` translator |
| `polyflare-store` | SQLite persistence + at-rest crypto (XChaCha20-Poly1305) |
| `polyflare-testkit` | Scriptable mock upstreams for e2e tests |
| `polyflare-server` | axum ingress, auth, config, wiring — and the `polyflare` binary |

## Build & test

```sh
cargo build
cargo test
```

Run the proxy:

```sh
cargo run --bin polyflare -- serve
```

The shared upstream base URLs default to production (`https://chatgpt.com/backend-api/codex`,
`https://api.anthropic.com`, `https://auth.openai.com`), so no configuration is required to run
against the real backends. Override any of them for a mock/staging/self-hosted-proxy upstream:

```sh
POLYFLARE_UPSTREAM_URL="https://<codex-upstream-base>" \
POLYFLARE_ANTHROPIC_UPSTREAM_URL="https://<anthropic-upstream-base>" \
POLYFLARE_BIND="127.0.0.1:8080" \
cargo run --bin polyflare -- serve
```

Per-account bearer tokens live encrypted in the store (added via `accounts login` / `accounts
import`), not in the environment, and are never logged.

To capture a real Codex CLI request's content-safe structural HTTP fingerprint (the golden fixture
the M5 fingerprint-parity gate diffs against — method, path, header names, and redacted/structural
value descriptors only; never a token, id, or body), set `POLYFLARE_CAPTURE_FINGERPRINT` to a file
path before routing one real `codex-rs` request through PolyFlare:

```sh
POLYFLARE_CAPTURE_FINGERPRINT="./fingerprint_golden.jsonl" \
POLYFLARE_UPSTREAM_URL="https://<upstream-base>" \
cargo run --bin polyflare -- serve
```

Each request appends one JSON line to the golden path (JSON Lines); see
`crates/polyflare-server/src/fingerprint_capture.rs` for the exact redaction rules.

### Local dev: `codex-polyflare`

`scripts/codex-polyflare` runs the **real Codex CLI against your local PolyFlare server**, so you
can exercise PolyFlare with genuine `codex-rs` traffic without touching your normal `codex`
(your usual `~/.codex` → OpenAI/codex-lb keeps working — the wrapper uses a separate `CODEX_HOME`):

```bash
polyflare serve                       # terminal 1 (default 127.0.0.1:8080)
scripts/codex-polyflare "hello"       # terminal 2 — codex, routed to PolyFlare
```

It writes an isolated `~/.codex-polyflare/config.toml` defining a `polyflare` model provider
(`base_url` → your PolyFlare, `wire_api = "responses"`, a placeholder bearer PolyFlare ignores —
no `codex login` needed). Override the target with `POLYFLARE_URL=...`. This is also how you grab
the fingerprint golden — start PolyFlare with `POLYFLARE_CAPTURE_FINGERPRINT` set, then send one
`codex-polyflare` request.

## Roadmap

Core milestones — **built and CI-gated:**

- **M1** — Skeleton + Codex identity pass-through ✅
- **M2** — Store + accounts + selector + zero-re-auth OAuth import ✅
- **M3** — Continuity engine (the wedge fix) ✅
- **M4** — Anthropic executor + `Anthropic → Codex` translator ✅
- **M5** — Fingerprint parity + latency/wedge CI gates + content-safe observability ✅

Next up, in rough priority (full ledger in [`docs/COMPARISON.md`](docs/COMPARISON.md)):

1. **Failure-driven health/cooldown tracking** — make the already-built `capacity_weighted` routing
   run on real data instead of inert neutral defaults.
2. **Admin / API-key auth** (`POLYFLARE_ADMIN_TOKEN`) on management + proxy endpoints.
3. **Retry/failover across accounts** + anti-starvation "serve soonest-to-recover" fallback.
4. **Prompt-cache stickiness** — derive a `prompt_cache_key` on the translated path for ~10× cheaper
   cached input tokens (design in `docs/COMPARISON.md`).
5. Verify the speculative Codex alias/`reasoning.effort` wire shape against a live backend.

## Responsible use

PolyFlare load-balances provider accounts **you own and are authorized to use**. You are responsible for complying with the terms of service of every provider you connect it to. Don't use it to share, pool, or resell access you don't have.

## License

[MIT](LICENSE) © 2026 aididhaiqal
