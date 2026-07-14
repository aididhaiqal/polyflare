# Rust LB — Capability Markup (foundation for the design)

Source: parallel inventory of codex-lb (Python), CLIProxyAPI (Go), better-ccflare (TS/Bun), + dashboard synthesis. 2026-07-14.

## The three systems at a glance

| System | Essence | Crown jewel | What it's WEAK at |
|---|---|---|---|
| **codex-lb** (Py) | Deep single-provider Codex pool + impersonator | Fingerprint laundering (codex_cli_rs UA + x-stainless stripping + native-vs-SDK detection) · quota-shaped routing (8 strategies, burn/preserve, relative-availability) · previous_response_id continuity · 53-col observability · WeeklyCreditPace forecasting | anchor **wedge** bug · Python single-instance ceiling · 45k-LOC accreted proxy · single-provider only |
| **CLIProxyAPI** (Go) | Universal N×M translator + multi-provider router | **Translator registry** (Format enum + (from,to)→transform map) · **selector abstraction** (RR/FillFirst/SessionAffinity + priority buckets + model-level cooldown) · **payload-override engine** (predicate-scoped JSON-path mutation) · oauth-model-alias | thin Codex talker (deletes previous_response_id, no ultra→max, lighter fingerprint) · removed first-party analytics · huge 6-vendor surface · C-ABI plugin security surface |
| **better-ccflare** (TS) | Anthropic pool + polished analytics dashboard | Anthropic rate-limit intelligence (out_of_credits vs extra_usage vs 529, 24h-clamp) · anti-ban session-stickiness (reset aligned to real window) · **the dashboard** (consolidated analytics SQL, live SSE feeds, usage-exhaustion prediction, conversation viewer) · session-governor circuit breaker | NO egress fingerprint synthesis (relies on client) · Bun-coupled · only 1 strategy active · debug/tech-debt |

## The steal list (crown jewels to port into the Rust rebuild)

**From codex-lb (the Codex depth):**
- Fingerprint laundering contract — native-vs-SDK detection keyed only on UA/originator (never on replayable turn-state); x-stainless-* stripping; codex_cli_rs UA from live version. **Rust's rustls gives REAL TLS/JA3 control the Python stack fakes.**
- The 8 routing strategies + burn/preserve + relative-availability scoring + health tiers (`logic.py` is pure → direct port + parity tests).
- Cap-partitioning share-growth hysteresis (verbatim math).
- 53-col request_logs + 6-phase latency model + Prometheus set.
- OpenAI-status advisory feature (clean, self-contained).

**From CLIProxyAPI (the architecture patterns):**
- **Translator registry** — `HashMap<(Format,Format), Translator{req, resp{stream,nonstream,tokencount}}>` + pass-through fallback. This is how you generalize beyond Codex.
- **Selector state machine** — RR/FillFirst/SessionAffinity-wrapper + priority buckets + per-auth-per-model cooldown (NextRetryAfter/Quota.NextRecoverAt/Exceeded/blockReason).
- 7-source session-id ladder (Claude-Code user_id parse → headers → content-hash w/ first-turn inheritance).
- **Payload-override engine** (protocol+model+header predicate → JSON-path mutate) — declarative service_tier/effort injection.
- oauth-model-alias + fork + force-mapping · identity-confuse (UUIDv5 per-account remap).

**From better-ccflare (the Anthropic half + dashboard):**
- Claude-pool selection: session stickiness aligned to real rate_limit_reset · priority(0-100)+utilization tiebreak · won't re-pin a rate-limited-but-sessioned account · auto-fallback re-promotion.
- Anthropic rate-limit header semantics as a typed module (out_of_credits/extra_usage/529/24h-clamp).
- **The dashboard**: consolidated analytics SQL (plan-vs-api cost split, burn rate, p95 window-fn, tokens/sec, cache-hit) · live SSE request/log/alert feeds · per-account usage-exhaustion prediction · conversation/thinking/tool-use viewer.
- Session-governor circuit breaker (runaway subagent fan-out). · AsyncDbWriter batched writes.

## The two big FIXES the rebuild exists to make
1. **Continuity done right:** keep codex-lb's `previous_response_id` anchor BUT design store:false full-resend + reattach as an explicit **state machine with a watchdog** from day one → kills the wedge. Combine with a reasoning-replay cache generalized to all source protocols (CLIProxyAPI only does it for Claude source).
2. **Coordination without the sprawl:** Rust single-binary (tokio) removes codex-lb's leader-election + cache-invalidation-poller + bridge-ring + shared-Fernet coordination debt. Pick single-binary-first (or a deliberate cluster primitive), not per-process-semaphore + DB-hacks.

## Dashboard direction (from the dash agent)
- **Serve model:** keep SPA-as-static-assets; embed the Vite build in the Rust binary via `rust-embed`/`include_dir`; axum catch-all (immutable hashed assets, no-cache index.html fallback); SSE from Rust for live tiles. **Do NOT rewrite UI in Leptos/Dioxus.**
- **Keep the React 19 + Vite + TanStack + Tailwind + shadcn + Recharts stack** — port, don't rewrite.
- **Redesign:** resolve the parked B-vs-C ambiguity FIRST; then Dashboard = a *glance* (one quota+pace pane, compact error strip, ONE trend chart, logs collapsed); push deep cost analysis to Reports; unify the two account renderings; split the 13-section Settings into tabbed sub-routes.
- **Steal from better-ccflare:** pooled 5h/7d quota tiles w/ account-breakdown popover, grouped-error UI (1h/24h/7d/all), URL+localStorage view-state persistence.

## Drop / defer
- 176 Alembic migrations → port final tables, clean schema. · quota-phase-planner (experimental) → defer. · C-ABI plugin host → compile-time traits or WASM. · 6-vendor sprawl → adopt the pattern, add providers incrementally. · ~25-field WeeklyCreditPace payload → expose 3-4 decision numbers.
