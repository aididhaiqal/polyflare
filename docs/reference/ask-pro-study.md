# ask-pro Study — reaching "sol pro" as a custom/aliased PolyFlare model

**Date:** 2026-07-14
**Trigger:** Codex's subscription backend doesn't expose the "Pro"-intelligence tier of the internal `GPT-5.6 Sol` model ("sol pro"). Question: can [`Pimpmuckl/ask-pro`](https://github.com/Pimpmuckl/ask-pro) (fork of `steipete/oracle`) be wired into PolyFlare as a custom/aliased model that reaches it?
**Verdict up front:** ask-pro is real, working code, but it's a **browser-automation CLI**, not an API client — its interface shape is fundamentally incompatible with PolyFlare's `Executor` trait as specified, and its operational model (visible Chrome, human-gated login, no headless/CI mode, no true streaming, unknown/likely-long latency) is a poor match for an always-on server backend. Treat this as a low-priority, explicitly-opt-in escalation path post-M4, not a pool member — see §5.

---

## 1. What ask-pro is

Source: GitHub repo `Pimpmuckl/ask-pro` (fork of `steipete/oracle`), fetched 2026-07-14 via the repo page, README, package.json, and `src/` listing. Read directly — no invented details; anything not confirmed is flagged as unknown in §4.

- **What it is:** a Codex CLI plugin + standalone CLI that lets a coding agent (e.g. Codex) request a "focused second opinion" from ChatGPT Pro on hard problems — architecture decisions, production-risk reviews, migrations, debugging strategy. Per the README: *"`ask-pro` is a Codex plugin and CLI that lets coding agents ask ChatGPT Pro for a focused second opinion through a human-controlled browser session."* The calling agent keeps ownership of the work; ask-pro "never applies generated code automatically."
- **How it reaches "Pro" — confirms this is exactly the same tier the user calls "sol pro":** it does **not** call any OpenAI API. It drives a real Chrome browser against the consumer `chatgpt.com` web UI via Chrome DevTools Protocol. Per the README: *"The browser flow selects `GPT-5.6 Sol`, then `Pro` intelligence automatically."* This is UI automation clicking the model/intelligence selectors a human would click — not a `gpt-5-pro`/`o1-pro` API model name.
- **Auth:** manual, human-in-the-loop, cookie/session-based via a **persistent Chrome profile** — explicitly not API keys, and the tool never touches secrets itself: *"Authentication is manual. `ask-pro` never asks for, types, reads, or logs passwords, MFA codes, recovery codes, session cookies, or raw auth tokens."* Each new profile needs one human login; after that the profile's session persists. Requires a real ChatGPT Pro subscription on the logged-in account.
- **Interface — CLI only, no server, no API surface:** invoked as `ask-pro [options] [question...]` (bin entry `ask-pro` → `dist/bin/ask-pro-cli.js`). Also ships as a Codex plugin (`.codex-plugin/plugin.json`, installed via `codex plugin marketplace add`) and a packaged Skill (`skills/ask-pro/SKILL.md`). **There is no HTTP endpoint, no OpenAI-compatible Chat Completions/Responses shape, nothing to translate against on the wire** — the "protocol" is CLI args in, files on disk out.
- **Execution model — async session + poll/harvest, not a blocking call:**
  - Starting a consult begins a session persisted at `.ask-pro/sessions/<session-id>/`.
  - `--status [session-id]` — print compact session state.
  - `--resume [session-id]` — resume a prepared/waiting/auth-gated session.
  - `--harvest [session-id]` — print `ANSWER.md` for answer-bearing sessions.
  - Output is **markdown** (`ANSWER.md`), not JSON. Generated-file artifacts are opt-in only (`--artifacts` / `--response-zip` → `ask-pro-response.zip`), explicitly not auto-executed: *"Never execute generated zip contents automatically."* A convention flags incomplete answers (`INCOMPLETE_ANSWER` / `preamble_without_artifacts`).
  - **No token-by-token streaming was found anywhere in the README or usage flow** — the model is "kick off, poll, harvest a finished document," matching a browser session that itself has to finish before the page content can be scraped.
- **Headless/CI fit:** nothing in the README describes a headless or container/CI mode. Requirements explicitly list "Chrome" alongside Node 24+ and pnpm 10+, and several documented run paths (login, resume/recovery, stale-auth, debug) keep or restore a **visible** browser window for human action on Windows specifically. This points at "run on your dev machine, human present," not "run unattended on a server."
- **Stack:** TypeScript (96.4%) CLI. Chrome automation via **`chrome-launcher` + `chrome-remote-interface`** (raw Chrome DevTools Protocol) plus `@steipete/sweet-cookie` — notably **not** Playwright/Puppeteer. CLI parsing via `commander`. Build/lint via `esbuild`/`oxlint`/`oxfmt`, tests via `vitest`.
- **Maturity/risk signals:** MIT-licensed, 8 GitHub stars, 5 forks, no published releases/packages, high raw commit count (largely inherited from the `steipete/oracle` fork history). Reads as an actively-developed but small, single/few-maintainer niche tool, not a hardened production integration.
- **Explicit unknowns (not documented anywhere I could find, don't invent):**
  - No published rate limit, cooldown, or "how many Pro queries per period" figure — whatever chatgpt.com's own web-UI throttling does applies, unquantified.
  - No ToS discussion in the repo at all. Automating the consumer ChatGPT web UI (versus the official API) to reach a subscription-gated tier is inherently a **ToS gray area** for OpenAI's consumer product; the repo doesn't address this, and this study is not a legal opinion — flagging it as an open risk, not a determination.
  - Real end-to-end latency for a Pro-tier answer (browser automation + a reasoning-heavy "Pro" pass, which the ChatGPT product line is known for being slow) is not documented; expect it to be materially slower than any of PolyFlare's current backends (see §5).
  - Whether one Chrome profile can serve concurrent sessions, or must be serialized to one-at-a-time, is not documented.

## 2. PolyFlare integration assessment

Ground truth used: `docs/POLYFLARE-DESIGN.md` §3 (architecture), §4.2 (translator registry), §4.4 (executors), §4.5 (server edge); `docs/DESIGN-DECISIONS.md` Q1/T2/E4/M2-GATE1.

### 2.1 Does ask-pro fit the `Executor` seam directly?

**No — not as specified.** `Executor::execute(PreparedRequest, Account) → ResponseStream` (§4.4) assumes a **network** transport underneath (WS/SSE/HTTP) that PolyFlare's continuity and translation layers can stream through uniformly. ask-pro is a **subprocess CLI with a poll/harvest lifecycle**, not a request/response or streaming network client:

- The "request" isn't a JSON payload over a socket — it's a CLI invocation (`ask-pro <question> [--files ...]`) that returns a session id, not a response.
- The "response" isn't a stream of deltas — it's a **completed markdown document** retrieved later via `--harvest`, after however long the underlying browser/Pro-tier turn takes.
- There is no native concept of `PreparedRequest` (OpenAI-Responses-shaped, with prior turns/anchors) — ask-pro has no equivalent of Codex's `previous_response_id` anchoring; its closest analog is `--resume <session-id>`, which resumes a *browser session*, not a model conversation state PolyFlare's continuity engine understands.

A working integration therefore needs an ask-pro-specific **Executor implementation that does its own internal translation**, not a registry `Translator` entry (§4.2's registry is keyed on `(Format,Format)` pairs where both sides are wire formats other backends also speak — OpenAI-Responses, Anthropic-Messages. ask-pro's CLI-args/markdown shape isn't a `Format` any other component needs, so it doesn't belong in the shared registry). Concretely, the executor would need to, internally:
1. Marshal the incoming neutral request (messages + any prior turns, since ask-pro has no native multi-turn) into a single synthesized question string (+ optional file bundle via `--files`/`--prompt-file`), because ask-pro treats each consult as stateless context that must be given explicitly (its own docs stress that ChatGPT Pro has "no inherent knowledge of the repo or prior context").
2. Spawn/poll the CLI (start session → poll `--status` → `--harvest` on completion) instead of opening a stream.
3. Wrap the harvested `ANSWER.md` text into a single OpenAI-Responses-shaped completed chunk (or, if the client wants incremental output, a synthetic word-chunked "stream" — not real token streaming, since ask-pro never exposes intermediate tokens).
4. Supply a **no-op `Continuity` impl** (like the Anthropic backend today) — there's nothing to anchor; each ask-pro session is closer to a fire-and-forget consult than a resumable conversation, aside from ask-pro's own session-resume, which is a different, lower-level concept.

This is buildable — PolyFlare's "transport lives below the executor behind a common streaming interface" language (§4.4) is generic enough that a subprocess-based transport is architecturally additive, not a violation of the seam — but it is real new work, not a drop-in.

### 2.2 Auth/config

- ask-pro's "credential" is **not a token PolyFlare's account store models today**. It's a stateful, host-local Chrome profile directory that a human has already logged into. It doesn't fit `polyflare-store`'s encrypted-OAuth-token account rows (§4.5, `codex-lb-port-reference.md` accounts schema) — there's no access/refresh/id token to encrypt, just a profile path plus "has a human logged in recently."
- Practically, config would be host-level, not per-request: a path to the `ask-pro` binary/plugin, a path to its persistent Chrome profile dir, and an executor-specific timeout/watchdog value (see §2.3). This ties the feature to whichever machine actually runs the logged-in Chrome profile — it cannot be load-balanced across "accounts" the way Codex/Anthropic pools are, and it doesn't survive a clean multi-machine/distributed deployment (§Q2 in `DESIGN-DECISIONS.md` — single-binary is already PolyFlare's posture, so this is a soft constraint, not a hard blocker, but it does mean "the ask-pro executor only works on whichever box has the logged-in Chrome").
- **On-demand trigger — this is the good-fit part.** The user's framing ("only when we need it") maps exactly onto PolyFlare's already-planned **model-alias / rewrite** mechanism (rewrite an incoming `model` field to a pool model, referenced in the M2 memory context and structurally the same shape as codex-lb's abandoned "aliased provider models" / Fugu idea). Concretely: register an alias like `sol-pro` that, when a client names it, **short-circuits before `Selector.pick()` runs at all** and routes straight to a dedicated ask-pro `Executor` instead of the Codex account pool. No pool, no selection scoring needed — there's exactly one "account" (the human's logged-in profile), so this is closer to a special-cased static route than a poolable backend. This keeps the blast radius small: normal `gpt-5.6-sol` traffic is entirely unaffected, and the feature only activates when a client explicitly asks for `sol-pro`.

### 2.3 Which milestone

**Post-M4, additive-only — not in MVP (L0–L4), not the Anthropic executor (M4), not the model-alias base work itself.** It's the same conceptual slot as codex-lb's shelved "aliased provider models" (Fugu) idea: a virtual model name routing to an **external, non-pool** provider endpoint. Land it only after:
- the model-alias/rewrite mechanism exists (a prerequisite, not part of this work),
- M4's Anthropic executor has proven the "second `Executor` impl" pattern works end-to-end,
so ask-pro becomes the **third** executor and the first with a genuinely non-network transport.

**Effort estimate:** medium, driven by the transport/latency mismatch rather than code volume — roughly a handful of days once scheduled:
- New `Executor` impl: subprocess spawn + poll loop + harvest parsing → neutral-response marshaling (1–2 days).
- No-op `Continuity` impl + alias short-circuit wiring ahead of `Selector` (half a day, mostly config/routing glue).
- Config/host-binding for the Chrome profile path + executor-specific long timeout, distinct from the ~30s Codex watchdog (half a day).
- Tests: since there's no mock-upstream-friendly network boundary, this needs its own harness (fake CLI binary emitting scripted session/status/harvest files) rather than reusing the existing e2e mock-upstream fixtures (1 day).
- Operator-facing documentation of the caveats in §3 so "sol pro" isn't silently expected to behave like a pooled model.

### 3. Risks / recommendation

- **Reliability.** Browser automation against a live consumer web UI has no stability contract — any OpenAI UI change can silently break ask-pro's selectors, with no upstream deprecation notice the way an API would give. It's a small (8-star), few-maintainer fork; treat it as fragile.
- **ToS / gray area.** Automating the consumer ChatGPT web product to reach a subscription tier, rather than an official API, is an unresolved gray area the repo itself doesn't address. Running this unattended inside a server-hosted load balancer is a materially different (larger) exposure than the tool's evident intended use ("a human runs this locally when they want a second opinion"). This alone is reason to keep it opt-in/manual-feeling rather than transparently automatic.
- **Operational fit.** No documented headless/CI mode; several flows (login, stale-auth, recovery, debug) expect a visible window and a human. That's the opposite of "always-on backend," and it means the feature can silently stop working (stale auth) until a human re-visits the Chrome profile — worth an explicit health check / alert, not silent failure.
- **Latency and streaming.** Expect multi-minute turnaround (browser automation + a reasoning-heavy Pro pass) with no true token streaming — only a completed-document harvest. This breaks the assumptions the rest of PolyFlare is built around (§4.1 continuity watchdog ~30s N, §6 latency-regression CI gate, "stream upstream response" in the request lifecycle). The ask-pro path needs its own generous, separately-configured timeout and probably a long-poll/async client contract (return a handle immediately, let the client poll) rather than blocking a request thread for the duration.
- **Not poolable.** One human-authenticated Chrome profile ≈ one identity; no rate-limit numbers are published, and concurrent-session behavior on one profile is undocumented — assume it should be serialized (one in-flight ask-pro consult at a time) until proven otherwise.

**Recommendation:** worth building as a small, explicitly-labeled, opt-in escalation path once the model-alias mechanism exists and M4 has proven the second-executor pattern — but do **not** build it as a first-class pooled backend, and do not promise it Codex-pool-grade reliability or latency. Gate it behind an explicit config flag, document the human-in-the-loop/staleness caveat prominently to the operator, and treat every `sol-pro` call as "may take minutes and may occasionally need a human to re-touch the Chrome profile," not "just another model." If OpenAI ships a real Pro-tier API model reachable through the Codex subscription (making this whole shim unnecessary) that remains the better long-term outcome — this is a stopgap, not a target architecture.
