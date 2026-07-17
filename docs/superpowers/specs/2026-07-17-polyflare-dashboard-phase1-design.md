# PolyFlare Dashboard — Phase 1 Design Spec

**Status:** Design spec for review (no code yet). A TDD implementation plan is built from this via `superpowers:writing-plans`.
**Date:** 2026-07-17
**Depends on:** the existing `/dashboard` SPA (React 18 + Vite + bun + `rust_embed`), `read_api.rs` (`/api/pools`, `/api/accounts`, `/api/accounts/{id}`, `/api/requests`), the `request_log` table + `observability::RequestLog` chokepoint, `runtime_state` (Phase A health), and the `accounts` + `usage_history` store.
**References:** codex-lb + better-ccflare dashboards (inventoried 2026-07-16/17). Palette/components modeled on ccflare; feature breadth on codex-lb.

---

## 1. Goal

A full-fledged operator dashboard for PolyFlare, modeled on codex-lb and ccflare, letting the operator monitor accounts, quota, routing health, pools, and a **live log stream** while the product's features expand. It is delivered in **phases**; this spec covers **Phase 1 only**.

## 2. Scope & phasing

Full parity is a multi-phase project. Each later phase gets its own spec.

- **Phase 1 (this spec) — Foundation + Observe + Live Logs.** Auth gate + app shell + design system, and the read-only observability surface: Overview, Accounts (list/cards + detail + master-detail quick-switch), Pools, Requests, and the live-logs SSE console. **No state mutations.**
- **Phase 2 — Analytics + Usage.** Reports/Analytics page (cost/tokens/TTFT/TPS trends, breakdowns, cumulative view), usage-history with pace/depletion forecasting beyond the Overview pace card.
- **Phase 3 — Account & pool admin.** The mutating actions designed but disabled in Phase 1: pause/resume, force probe, re-authenticate, routing policy, trusted-access flag, warm-up, reset-credit redeem, delete, alias edit, and pool management.
- **Phase 4 — Config, keys, automations.** Settings (routing strategy, retention, watchdog), proxy-access API-key management, scheduled automations.

### Phase 1 non-goals
Any mutation/admin action; the Analytics/Reports page; usage prediction beyond the single pace card; automations; API-key management; settings editing; proxy-pool binding; per-model quota data; the reset-credit inventory; WebSocket transport (the Requests "Transport" column shows `http` until WS ships).

## 3. Architecture

### 3.1 Frontend
Extend the existing `crates/polyflare-server/dashboard/` app (Vite + bun, embedded via `rust_embed`, served at `/dashboard`). Upgrade the bare React-18 setup to the parity stack:
- **React Router** — routes: `/login`, `/` (Overview), `/accounts`, `/accounts/:id`, `/pools`, `/requests`, `/logs`.
- **TanStack Query** — all data fetching + interval polling (30s default; the Overview/accounts/requests poll, the log console streams).
- **Recharts** — sparklines, area charts (request volume, account trend).
- **Tailwind + a small set of shadcn-style Radix components** (Card, Select, Switch, Popover, Dialog, Tabs) + **lucide-style line icons** (inline SVG). **No emoji.**

### 3.2 Backend
Extend `read_api.rs`; add `sse.rs` (the log stream) and an axum auth-middleware layer. New/extended endpoints in §6. One content-free `request_log` schema extension (§5).

### 3.3 Auth (single-operator, remote)
- **`POLYFLARE_ADMIN_TOKEN`** (env). An axum middleware requires `Authorization: Bearer <token>` on every `/api/*` request. When the token is unset, the dashboard + `/api` are disabled (bind localhost only).
- `/login` takes the token, verifies via `GET /api/whoami`, stores it in `localStorage`, and injects it on every request; any `401` clears it and returns to `/login`.
- HTTPS is the operator's responsibility (reverse proxy / tunnel); the server speaks HTTP. Documented, since access is remote.

### 3.4 Live logs (SSE)
- Gated by **`POLYFLARE_LIVE_LOGS`** (default off). When off, `GET /api/logs/stream` returns `404` and the SPA hides the Live Logs nav item (via `/api/capabilities`).
- The `observability::RequestLog` chokepoint — already the single point every request outcome flows through — additionally publishes a **content-free `LogEvent`** to an in-process `tokio::sync::broadcast` channel. Routing/lifecycle events (select, failover, cooldown, ownership pin, watchdog, token refresh) publish to the same channel.
- `GET /api/logs/stream` (auth-gated) subscribes to the channel and emits `text/event-stream` — one JSON `data:` frame per event, a heartbeat comment every 15s. A small in-memory ring buffer (last ~1000 events) lets a fresh client backfill before the live tail.
- Client: an `EventSource` with backoff reconnect, keeping the last 1000 lines; pause/resume (close/reopen), clear (client buffer), auto-scroll, level filter, text filter.

### 3.5 Multi-provider
Provider (`codex` | `anthropic`/claude) is first-class throughout: a provider filter on Overview/Accounts/Requests; provider tags on every account/row; and **per-provider, adaptive quota windows** — each provider renders only the windows it has (Codex: 5-hour + weekly; Claude: weekly + session, no 5-hour). Today the pool is Codex-only; the UI already accommodates more.

## 4. Design system

- **Palette (ccflare-derived).** Dark blue-grey surfaces — bg `hsl(220 13% 8%)`, card `hsl(220 13% 12%)`, muted `hsl(220 13% 18%)`, border `hsl(220 13% 20%)`, foreground `hsl(0 0% 95%)`. Warm **orange accent `hsl(24 89% 56%)` (#ee7f2e)**. Provider hues: codex = accent orange, claude = purple `#b57cff`. Semantic: success `#37b24d`, warn `#e8a23d`, error `#f0473e` — distinct from the accent. Radius `0.375rem`. `tabular-nums` on all numeric columns.
- **Theming.** Token-based light + dark; both first-class (dark is the primary shown). Tokens as CSS custom properties, redefined per theme.
- **12-column grid.** Every card declares a whole-column span (3/4/6/12) with constant gutters; rows align on the same gridlines — no ragged half-column widths. A panel needing an odd width snaps to the nearest sensible span (4/8), never a loose flex width.
- **Icons.** lucide-style inline SVG line icons only. Action buttons are text-only. No emoji anywhere.
- **Components.** MetricCard (faint oversized icon + trend badge + big value + inline sparkline); adaptive quota bars (per-provider windows); weekly-pace forecast (actual vs expected marker + projection); account card; dense tables with status pills + provider tags + inline mini-bars; provider filter segment; status/level pills.

## 5. Data model

Phase-1 aggregates come from the existing `accounts` + `usage_history` + `request_log` tables. One **content-free** extension is required.

- **`request_log` extension (migration).** Today: `requested_at, provider, method, path, aliased, status, duration_ms`. Add content-free columns to back the detailed Requests view + Overview KPIs: `request_id`, `account_id`, `model`, `reasoning_effort`, `service_tier`, `transport` (`http`|`ws`), `ttft_ms`, `total_tokens`, `cached_tokens`. All counts/metadata — no bodies. Populated at the `RequestLog` chokepoint (the sole content-safety gate). `tps` is derived (tokens ÷ generation time), not stored.
- **Account trend** (`GET /api/accounts/{id}/trends`): derived from `usage_history` time-series (per-window % over ~7 days). The dashed "plan" projection line is **⚑ deferred** (no scheduled series today) — Phase 1 renders the two usage series; the plan line lands with Phase 2.
- **Deferred (⚑ new data, not Phase 1):** per-model `additionalQuotas`, the reset-credit inventory + expiry, per-token expiry/state (Token Status shows what `last_refresh` + JWT `exp` give), the `limit_warmup` flag, and proxy-pool binding.

## 6. Endpoints (Phase 1, all auth-gated)

| Endpoint | Purpose |
|---|---|
| `GET /api/whoami` | Auth check (200 with valid token). |
| `GET /api/capabilities` | Feature flags (e.g. `live_logs`) so the SPA adapts its nav. |
| `GET /api/overview` | KPIs (requests, success %, error %, avg latency, tokens) + trend deltas + quota aggregates (per-provider windows) + weekly-pace forecast + pools summary + accounts-available count + recent errors. |
| `GET /api/accounts` *(extend)* | Per-account metadata + usage % (per window) + reset + token health + provider + pool. |
| `GET /api/accounts/{id}` *(extend)* | Detail: identity, status, per-window quota + reset, token status, routing policy, security flag, request totals. |
| `GET /api/accounts/{id}/trends` *(new)* | 7-day per-window % series (from `usage_history`). |
| `GET /api/pools` *(extend)* | Pools with account count, available count, aggregate usage, configured strategy. |
| `GET /api/requests` *(extend)* | Filters (time / account / provider / status / model / transport) + pagination; content-free rows incl. model, transport, ttft, tps (derived), tokens, cached. |
| `GET /api/logs/stream` *(new, flagged)* | SSE of content-free `LogEvent`s; `404` when `POLYFLARE_LIVE_LOGS` is off. |

## 7. Pages

**Login** — token field, verify via `whoami`, store, redirect. `401` anywhere returns here.

**Overview** (`/`) — 12-col grid: KPI cards with sparklines (row of 4×3); a combined **Quota / runway** card (per-provider adaptive bars) + **Weekly pace** forecast + **request-volume** area chart; an **account-health table** (provider, pool, status, 5h/weekly mini-bars, 24h reqs) + a **Pools** summary panel; a slim **recent-errors** strip. Header: provider filter, accounts-available + pools counts. Polls 30s.

**Accounts** (`/accounts`) — **Cards ⇄ List** toggle; provider + pool filters; status summary. Card view: rich per-account cards (identity, provider, status, plan, pool, usage bars, token health, 24h reqs). List view: dense table (same fields).

**Account detail** (`/accounts/:id`) — **master-detail**: a searchable **account rail** (grouped by pool, usage bar per account, selected flagged) for one-click switching, plus header ‹ › to cycle. Panels (codex-lb order): identity header (alias edit — Phase 3), usage/quota per window + resets, **7-day trend chart** (5h + weekly areas; plan line deferred), token status, per-model quotas (⚑), and an **Actions** panel tagged **admin · Phase 3** — routing policy, trusted-access, warm-up, pause/probe/re-authenticate/export, **rate-limit resets shown with per-credit expiry** (soonest consumed first), delete. In Phase 1 the Actions controls render disabled (design visible, mutations off).

**Pools** (`/pools`) — pools list: name, accounts (available/total), aggregate usage, routing strategy. Read-only in Phase 1.

**Requests** (`/requests`) — the deep view (the Overview only summarizes). Detailed filterable/paginated table with a **Live · SSE** feed badge; columns: time, account, provider, model (+effort/tier), **transport** (`http`|`ws`), status pill, TTFT, TPS, tokens (+cached). Expandable **content-free** detail row: request id, api key, path, downstream/upstream transport, model/effort/tier, timing, retry-after, upstream error code, routing trail — with a privacy note where bodies would be.

**Live Logs** (`/logs`, flagged) — SSE console: `Live · SSE` badge, level filter / pause / clear / auto-scroll / search, monospace rows (timestamp + colored level + content-free operational message), last-1000-line buffer, auto-reconnect. Footer states the `POLYFLARE_LIVE_LOGS` flag behavior.

## 8. Content-safety invariant

Every dashboard surface is **content-free**: request/response bodies and any conversation content are never stored, streamed, or displayed. `RequestLog` remains the single chokepoint; the SSE `LogEvent`, the `request_log` columns, and every API response carry only outcomes, timings, counts, and routing metadata. This is surfaced to the operator (the Requests detail privacy note) as a deliberate differentiator.

## 9. Error handling

- Missing/invalid token → `401` → login screen.
- `POLYFLARE_LIVE_LOGS` off → `/api/logs/stream` 404 + nav item hidden.
- SSE drop → client auto-reconnects with backoff; a "reconnecting" state in the console.
- Empty states for every page (no accounts, no requests, no trend data).
- API errors surface as inline non-blocking banners; polling continues.

## 10. Testing

- **Rust:** auth middleware (401 without token / 200 with); `/api/logs/stream` emits a frame when a `RequestLog` records and 404s when the flag is off; overview-aggregate correctness; request filters + pagination; capabilities reflect flags; the `request_log` migration round-trips the new content-free columns. On the existing mock-upstream + temp-store harness.
- **Frontend:** build + a render smoke test (matching the repo's current light FE-test posture).

## 11. Deferred / open (⚑)

Per-model quota data; reset-credit inventory + expiry (the Actions "resets with expiry" UI is designed, backed by new data in Phase 3); the scheduled-plan trend line; per-token expiry/state detail; `limit_warmup` flag; proxy-pool binding; WS transport (Transport column reads `http` until it ships). All are out of Phase 1 and land in the noted later phases.
