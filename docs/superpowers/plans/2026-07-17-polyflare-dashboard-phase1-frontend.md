# PolyFlare Dashboard — Phase 1 Frontend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Phase-1 dashboard SPA against the now-live backend endpoints — the ccflare-skin design system, the app shell + auth, and the six read-only pages (Login, Overview, Accounts + detail, Pools, Requests, Live Logs) — replacing the bare React-18 single-file app.

**Architecture:** Upgrade `crates/polyflare-server/dashboard/` (Vite + bun, embedded via `rust_embed`, served at `/dashboard`) to the parity stack: React Router + TanStack Query + Recharts + Tailwind + a small set of Radix primitives + lucide-react icons. Data comes from the auth-gated `/api/*` endpoints (a single `Authorization: Bearer <admin token>` held in `localStorage`); the live logs + (optionally) the request feed stream over SSE. Every page is read-only; the account-detail Actions panel is rendered visibly but disabled (Phase-3 mutations).

**Tech Stack:** React 18, TypeScript, Vite, bun, `react-router-dom` v6, `@tanstack/react-query` v5, `recharts`, `tailwindcss` + `postcss` + `autoprefixer`, `lucide-react`, `clsx`, a few `@radix-ui/react-*` primitives (Select, Switch, Popover, Tabs, Dialog).

## Global Constraints

- **Base path:** the app is served under **`/dashboard`** (not root). Configure Vite `base: "/dashboard/"` and React Router `basename="/dashboard"`. All `/api/*` fetches are absolute-from-origin (`/api/...`), NOT under `/dashboard`.
- **Auth:** every `/api/*` request carries `Authorization: Bearer <token>` where the token is read from `localStorage["polyflare_admin_token"]`. A `401` from any request clears the token and routes to `/login`. There is no login *credential* — the operator pastes the `POLYFLARE_ADMIN_TOKEN` value; the app verifies it via `GET /api/whoami`.
- **Capabilities gate:** `GET /api/capabilities` returns `{live_logs: bool}`; when `live_logs` is false, hide the **Live Logs** nav item and route `/logs` to a "disabled" notice.
- **Content-free:** the UI only ever renders what the endpoints return (outcomes/metrics/identifiers/timings). There is no request/response body anywhere — the Requests detail shows a privacy note where bodies would be.
- **Design system (ccflare skin):** dark blue-grey surfaces + warm orange accent. Tokens (exact): bg `hsl(220 13% 8%)`, card `hsl(220 13% 12%)`, muted `hsl(220 13% 18%)`, border `hsl(220 13% 20%)`, fg `hsl(0 0% 95%)`, accent `#ee7f2e` (`hsl(24 89% 56%)`), provider codex = accent orange, claude = `#b57cff`, success `#37b24d`, warn `#e8a23d`, error `#f0473e`. Radius `0.375rem` (6px). `font-variant-numeric: tabular-nums` on all numeric columns. Light + dark themes via CSS custom properties toggled by `data-theme` on `<html>`; dark is default.
- **12-column grid:** page bodies use a 12-col CSS grid with constant gutters; cards declare a whole-column span (3/4/6/8/12). No ragged half-column widths.
- **Icons:** lucide-react line icons ONLY. **No emoji anywhere.** Action buttons are text (+ optional leading icon).
- **Visual spec = the mockups.** The approved per-page mockups live at `crates/polyflare-server/../../.superpowers/brainstorm/23587-1784245350/content/` (repo-root `.superpowers/brainstorm/.../content/`, git-ignored but on disk). Read the relevant mockup for each page's exact layout, spacing, columns, and colors, and match it. Mockup → page map: `overview-ccflare-v2.html` → Overview; `accounts-page.html` → Accounts list/cards; `accounts-master-detail-v2.html` + `accounts-detail-v2.html` → Account detail; `live-logs.html` → Live Logs; `requests-page-v2.html` → Requests. (Pools has no dedicated mockup — model it on the Overview "Pools" panel, expanded to a page.)
- **Verification per task:** `cd crates/polyflare-server/dashboard && bun install && bun run build` (this runs `tsc -b && vite build`) must succeed with ZERO TypeScript errors. Where a task adds a pure function (formatters, the query key builders, the SSE line parser), add a lightweight test if a test runner exists; otherwise a render smoke via a temporary mount is acceptable (the repo's posture is light FE testing). Do NOT introduce a heavy test framework.
- **Endpoint contracts (from the shipped backend — do not re-derive):**
  - `GET /api/whoami` → `{ok:true}` (200) / 401.
  - `GET /api/capabilities` → `{live_logs:bool}`.
  - `GET /api/overview` → `{kpis:{requests,success_rate,errors,avg_latency_ms,total_tokens}, quota:[{provider,windows:[{window,used_percent,reset_at}]}], pools:[{pool,accounts,available,usage_percent}], accounts_available, recent_errors:[{status,account_id,error_code,requested_at}]}` (shape approximate — read the actual `read_api.rs` `OverviewView`/serde field names and match them exactly).
  - `GET /api/accounts` → `[{id,email,alias,provider,pool,status,plan_type,reset_at,usage:[{window,used_percent,reset_at}],token_health:{access_state,access_expires_at},request_count_24h}]`.
  - `GET /api/accounts/{id}` → detail (`AccountDetailView`: identity, status, quota_windows, token_status, routing_policy, security_work_authorized, request_totals).
  - `GET /api/accounts/{id}/trends` → `{account_id, primary:[{t,v}], secondary:[{t,v}]}`.
  - `GET /api/pools` → `[{pool,accounts,active,available,usage_percent,strategy}]`.
  - `GET /api/requests?limit&offset&account&provider&status_class&model&transport&since_ts` → `{total, rows:[{requested_at,account_id,provider,model,reasoning_effort,service_tier,transport,path,status,duration_ms,ttft_ms,total_tokens,cached_tokens,tps}]}`.
  - `GET /api/logs/stream` → SSE `data: <LogEvent json>` where `LogEvent = {ts_ms,level,provider?,account?,model?,status?,latency_ms?,kind,message}`; 404 when the flag is off.
  - **Every implementer MUST open `crates/polyflare-server/src/read_api.rs`, `auth.rs`, `sse.rs`, `log_bus.rs` and use the EXACT serde field names** — the shapes above are a guide, the source is authoritative.

---

## File Structure

Under `crates/polyflare-server/dashboard/`:
- `package.json`, `vite.config.ts`, `tailwind.config.ts`, `postcss.config.js`, `tsconfig.json` — stack config (Task 1).
- `src/index.css` — Tailwind directives + the CSS-variable token theme (Task 1).
- `src/lib/api.ts` — typed fetch client (auth header, 401 handling) + response types (Task 2).
- `src/lib/queries.ts` — TanStack Query hooks (`useOverview`, `useAccounts`, `useAccount`, `useAccountTrends`, `usePools`, `useRequests`) (Task 2).
- `src/lib/useLogStream.ts` — the SSE `EventSource` hook (Task 2).
- `src/lib/format.ts` — pure formatters (countdown, pct, bytes, relative time, tps) (Task 2).
- `src/auth/AuthProvider.tsx`, `src/pages/Login.tsx` — auth context + login (Task 3).
- `src/App.tsx`, `src/main.tsx` — Router + QueryClient + the auth gate (Task 3).
- `src/shell/Shell.tsx`, `src/shell/Sidebar.tsx`, `src/shell/ThemeToggle.tsx` — app shell (Task 4).
- `src/ui/` — design-system atoms: `Card.tsx`, `Grid.tsx`, `MetricCard.tsx`, `StatusPill.tsx`, `ProviderTag.tsx`, `QuotaBars.tsx`, `Sparkline.tsx`, `icons.ts` (lucide re-exports) (Task 4).
- `src/pages/Overview.tsx` (Task 5), `src/pages/Accounts.tsx` (Task 6), `src/pages/AccountDetail.tsx` (Task 7), `src/pages/Pools.tsx` (Task 8), `src/pages/Requests.tsx` (Task 9), `src/pages/LiveLogs.tsx` (Task 10).

Delete the old single-file `src/App.tsx` body / `src/api.ts` / `src/styles.css` as they're superseded (Task 1 scaffolds, later tasks replace).

---

## Task 1: Stack + Tailwind + token theme + build

**Files:** `package.json`, `vite.config.ts`, `tailwind.config.ts`, `postcss.config.js`, `src/index.css`, `src/main.tsx`, `src/App.tsx` (temporary placeholder). **Verify:** `bun run build`.

**Interfaces — Produces:** the buildable stack + the CSS-variable theme other tasks style against; `main.tsx` mounts `<App/>`.

- [ ] **Step 1: Add dependencies** to `package.json` (`react-router-dom@6`, `@tanstack/react-query@5`, `recharts`, `lucide-react`, `clsx`, `@radix-ui/react-select`, `@radix-ui/react-switch`, `@radix-ui/react-popover`, `@radix-ui/react-tabs`; dev: `tailwindcss`, `postcss`, `autoprefixer`). Run `bun install`.
- [ ] **Step 2: `tailwind.config.ts`** — `content: ["./index.html","./src/**/*.{ts,tsx}"]`, `darkMode: ["class", '[data-theme="dark"]']`, and extend `theme.colors` to reference the CSS variables (`bg: "hsl(var(--bg))"`, `card`, `muted`, `border`, `fg`, `accent`, `codex`, `claude`, `success`, `warn`, `error`), `borderRadius.DEFAULT: "0.375rem"`. `postcss.config.js` with tailwind + autoprefixer.
- [ ] **Step 3: `src/index.css`** — `@tailwind base/components/utilities;` then `:root{ --bg:220 13% 8%; --card:220 13% 12%; --muted:220 13% 18%; --border:220 13% 20%; --fg:0 0% 95%; --accent:24 89% 56%; --codex:24 89% 56%; --claude:267 100% 74%; --success:130 53% 46%; --warn:36 79% 58%; --error:3 85% 60%; }` (values as HSL components so Tailwind's `hsl(var(--x))` works). Add a `:root[data-theme="light"]{ ... }` override (bg `0 0% 100%`, card `0 0% 100%`, fg `240 10% 4%`, border `240 6% 90%`, accent unchanged). Set `body{ @apply bg-bg text-fg; font-variant-numeric: tabular-nums; }` and a monospace utility.
- [ ] **Step 4: `vite.config.ts`** — `base: "/dashboard/"`, `@vitejs/plugin-react`.
- [ ] **Step 5: Temporary `App.tsx`** rendering a single styled Card ("PolyFlare dashboard — scaffolding") to prove the theme compiles; `main.tsx` mounts it.
- [ ] **Step 6: Build** — `bun run build`; expect success, `dist/` written. **Commit** `feat(dashboard): parity stack + ccflare token theme`.

---

## Task 2: API client, query hooks, SSE hook, formatters

**Files:** `src/lib/api.ts`, `src/lib/queries.ts`, `src/lib/useLogStream.ts`, `src/lib/format.ts`. **Verify:** `bun run build` + (if a runner exists) unit tests for `format.ts` + the SSE line parser.

**Interfaces — Produces:** `api.get<T>(path)` (injects Bearer, throws `ApiError{status}` on non-2xx, triggers the 401 handler); typed response interfaces mirroring the backend serde shapes (read `read_api.rs`); `useOverview/useAccounts/useAccount(id)/useAccountTrends(id)/usePools/useRequests(params)` (TanStack Query, `refetchInterval: 30_000` for lists/overview, `staleTime` sensible); `useLogStream({enabled})` → `{lines, connected, pause, resume, clear}` over `EventSource("/api/logs/stream")`; pure `format.ts` helpers.

- [ ] **Step 1: `api.ts`** — a `fetchJson<T>(path, init?)` that sets `Authorization: Bearer ${token}` from `localStorage`, `Accept: application/json`; on `401` calls a registered `onUnauthorized()` and throws `ApiError`; on other non-2xx throws `ApiError{status, body}`; else returns parsed JSON. Export `setUnauthorizedHandler(fn)`. Define the response TS interfaces by reading the exact serde field names in `read_api.rs`/`auth.rs`.
- [ ] **Step 2 (test-first where possible): `format.ts`** — `countdown(resetAtSecs, nowMs)`, `pct(n)`, `relTime(unixSecs)`, `compactNum(n)` (12.4k / 4.1M), `latency(ms)`, `tps(n)`. If a test runner is available, write unit tests (e.g. `countdown(now+3660, now*1000) === "1h 1m"`) FIRST, watch fail, implement, pass. If no runner, implement + a `// @check` inline example and rely on `tsc`.
- [ ] **Step 3: `queries.ts`** — the six hooks wrapping `api` + `useQuery`, with a `queryKeys` object. `useRequests(params)` passes the filter/pagination query string.
- [ ] **Step 4: `useLogStream.ts`** — open an `EventSource` when `enabled`, parse each `event.data` as a `LogEvent`, keep the last 1000 in a ref-backed state, expose pause (close ES) / resume (reopen) / clear; auto-reconnect with backoff on `onerror`. A pure `parseLogEvent(data: string): LogEvent | null` is unit-testable — test it if a runner exists.
- [ ] **Step 5: Build** (+ tests). **Commit** `feat(dashboard): typed API client, query hooks, SSE log hook, formatters`.

---

## Task 3: Auth provider, Login page, app gate, Router + QueryClient

**Files:** `src/auth/AuthProvider.tsx`, `src/pages/Login.tsx`, `src/App.tsx`, `src/main.tsx`. **Verify:** `bun run build`; manual smoke (login stores token, 401 bounces).

**Interfaces — Consumes:** `api.setUnauthorizedHandler`, `GET /api/whoami`, `GET /api/capabilities`. **Produces:** `useAuth() → {token, setToken, clear}`; a `<RequireAuth>` wrapper; `App` = `<QueryClientProvider><BrowserRouter basename="/dashboard"><AuthProvider>…routes…`.

- [ ] **Step 1: `AuthProvider`** — holds the token (from `localStorage`), exposes `setToken`/`clear`; registers `setUnauthorizedHandler(() => { clear(); navigate("/login") })`. A `<RequireAuth>` renders children only when a token is present, else `<Navigate to="/login"/>`.
- [ ] **Step 2: `Login.tsx`** — a single centered Card: a password-type input for the admin token + a "Connect" button. On submit, `setToken(value)` then `api.get("/api/whoami")`; on success `navigate("/")`, on `401` show an inline "Invalid token" error and clear. No account creation, no other fields. Match the ccflare card styling.
- [ ] **Step 3: `App.tsx`** — `QueryClient` (defaults: `retry: false`, `refetchOnWindowFocus: true`), `BrowserRouter basename="/dashboard"`, `AuthProvider`, and the `<Routes>`: `/login` → `Login`; everything else wrapped in `<RequireAuth><Shell/></RequireAuth>` with nested routes for `/`, `/accounts`, `/accounts/:id`, `/pools`, `/requests`, `/logs`. (Shell + pages are stubs until their tasks — use placeholder components now so routing compiles.)
- [ ] **Step 4: Capabilities** — fetch `/api/capabilities` once authenticated; expose `live_logs` via context so the Shell hides the Logs nav + `/logs` shows a disabled notice when false.
- [ ] **Step 5: Build.** **Commit** `feat(dashboard): auth provider + login + router + capabilities gate`.

---

## Task 4: App shell + design-system atoms

**Files:** `src/shell/{Shell,Sidebar,ThemeToggle}.tsx`, `src/ui/{Card,Grid,MetricCard,StatusPill,ProviderTag,QuotaBars,Sparkline,icons}.tsx`. **Verify:** `bun run build`; visual smoke vs mockups.

**Interfaces — Produces:** `<Shell>` (sidebar + `<Outlet/>`), and the reusable atoms every page composes. `Grid`/`Col` for the 12-col layout; `MetricCard`, `QuotaBars` (adaptive per-provider windows), `StatusPill`, `ProviderTag`, `Sparkline` (recharts) matching the mockup styling.

- [ ] **Step 1: `icons.ts`** — re-export the lucide icons the nav + UI use (LayoutGrid, Users, Boxes/Layers, List, Terminal/Activity, BarChart3, Settings, Search, Pause, Trash2, ArrowDown, ChevronLeft/Right, Pencil, RotateCcw, ShieldCheck, Route, Lock). No emoji.
- [ ] **Step 2: `Sidebar.tsx`** — the left nav (logo "Poly**Flare**" with orange accent span; nav items Overview/Accounts/Pools/Requests/Live Logs with lucide icons + `NavLink` active state using the accent-tint background; a divider; greyed Analytics/Settings; footer with `ThemeToggle` + Log Out). Hide Live Logs when `!live_logs`. Match `overview-ccflare-v2.html`'s sidebar.
- [ ] **Step 3: `ThemeToggle.tsx`** — toggles `document.documentElement.dataset.theme` between `dark`/`light`, persisted in `localStorage`.
- [ ] **Step 4: UI atoms** — `Card` (bg-card, border, radius, padding), `Grid`/`Col` (CSS grid 12 cols, gap; `Col span={3|4|6|8|12}`), `MetricCard` (faint oversized icon top-left, trend badge top-right, title, big 2xl value, optional inline `Sparkline`), `QuotaBars` (grouped by provider, one bar row per window, adaptive), `StatusPill` (active/cooldown/reauth colors), `ProviderTag` (codex orange / claude purple). Style strictly from the tokens; match the mockups.
- [ ] **Step 5: `Shell.tsx`** — flex: `<Sidebar/>` + a main region rendering `<Outlet/>` with page padding. **Commit** `feat(dashboard): app shell + ccflare design-system atoms`.

---

## Task 5: Overview page

**Files:** `src/pages/Overview.tsx`. **Visual spec:** `overview-ccflare-v2.html`. **Consumes:** `useOverview()`. **Verify:** `bun run build`; visual smoke.

- [ ] **Step 1:** header (title + `4 of N accounts available · M pools · updated …` + a provider filter segment All/Codex/Claude that scopes the view). 
- [ ] **Step 2:** 12-col grid — KPI row: 4 `MetricCard`s (Requests, Success rate, Avg latency, Tokens) each with trend + a `Sparkline` (span 3 each). 
- [ ] **Step 3:** row — combined **Quota/runway** `QuotaBars` card grouped by provider (adaptive windows) + **Weekly pace** forecast card (actual-vs-expected bar with marker + projected EOW + headroom note) + **request-volume** recharts area chart. 
- [ ] **Step 4:** row — **Account health** table (account · provider · pool · status · 5h/weekly mini-bars · reqs, span 8) + **Pools** summary panel (span 4). 
- [ ] **Step 5:** slim full-width **Recent errors** strip (429/5xx chips + "View all in Requests →"). Handle loading (skeleton) + error (inline banner) states. **Commit** `feat(dashboard): Overview page`.

---

## Task 6: Accounts page (Cards ⇄ List)

**Files:** `src/pages/Accounts.tsx`. **Visual spec:** `accounts-page.html`. **Consumes:** `useAccounts()`. **Verify:** build + smoke.

- [ ] **Step 1:** header — status summary (`N accounts · X active · Y reauth · Z pools`), provider filter, pool filter (Radix Select), and a **Cards ⇄ List** toggle (Radix Tabs / segmented), the choice persisted in URL search params.
- [ ] **Step 2: Card view** — a responsive grid of account cards: status dot + name(alias||id) + `ProviderTag` + `StatusPill`; `email · plan · pool`; usage bars (5h/weekly with reset); footer token-health + 24h req count. Clicking a card → `/accounts/:id`.
- [ ] **Step 3: List view** — a dense table of the same fields.
- [ ] **Step 4:** provider/pool filters actually filter the rendered set; loading/error states. **Commit** `feat(dashboard): Accounts page (cards/list)`.

---

## Task 7: Account detail + master-detail quick-switch

**Files:** `src/pages/AccountDetail.tsx`. **Visual spec:** `accounts-master-detail-v2.html` + `accounts-detail-v2.html`. **Consumes:** `useAccounts()` (for the rail), `useAccount(id)`, `useAccountTrends(id)`. **Verify:** build + smoke.

- [ ] **Step 1:** layout = an **account rail** (searchable, grouped by pool, one compact row per account with a usage bar, selected row flagged with the orange left-border) + the detail region; `‹ ›` header controls cycle prev/next account. Clicking a rail row navigates to `/accounts/:otherId` (detail updates in place via the query).
- [ ] **Step 2: detail header** — status dot + name + (disabled) edit-alias pencil + `ProviderTag` + `StatusPill` + the meta line.
- [ ] **Step 3: panels** — Usage/quota per window (bars + reset + request totals); the **7-day trend** recharts area chart (5h + weekly series from `/trends`, orange + purple, fixed 0–100% axis — no plan line); Token status (access/refresh/id); Per-model quotas (render only if present).
- [ ] **Step 4: Actions panel** — the three groups (Configuration: routing-policy select + trusted-access + warm-up toggles; Rate-limit resets with per-credit expiry; Operations: Pause/Force probe/Re-authenticate/Export/Reset/Delete) exactly as `accounts-detail-v2.html`, tagged **admin · phase 3** and **rendered DISABLED** (no handlers — Phase 3). 
- [ ] **Step 5:** loading/error/empty states. **Commit** `feat(dashboard): Account detail + master-detail switch`.

---

## Task 8: Pools page

**Files:** `src/pages/Pools.tsx`. **Visual spec:** the Overview "Pools" panel, expanded. **Consumes:** `usePools()`. **Verify:** build + smoke.

- [ ] **Step 1:** a table/cards of pools: name, accounts (available/total), aggregate `usage_percent` bar, routing `strategy`. Read-only. Loading/error states. **Commit** `feat(dashboard): Pools page`.

---

## Task 9: Requests page

**Files:** `src/pages/Requests.tsx`. **Visual spec:** `requests-page-v2.html`. **Consumes:** `useRequests(params)`. **Verify:** build + smoke.

- [ ] **Step 1:** header — count + a **Live · SSE** badge (a pulsing dot; respect `prefers-reduced-motion`).
- [ ] **Step 2:** filter bar — time range (1h/24h/7d → `since_ts`), account, provider, status, model, transport (Radix Selects), request-id search; filters drive `useRequests` params + are URL-synced.
- [ ] **Step 3:** dense table — time, account, `ProviderTag`, model (+effort/tier), transport pill (http/ws), status pill, TTFT, TPS, tokens(+cached). Server pagination controls (`Showing 1–50 of N`, Prev/Next via offset).
- [ ] **Step 4:** expandable row → content-free detail (request id, path, transport, model/effort/tier, timing, retry-after, error code, routing trail if present) + the **privacy note** (no bodies stored). **Commit** `feat(dashboard): Requests page`.

---

## Task 10: Live Logs console

**Files:** `src/pages/LiveLogs.tsx`. **Visual spec:** `live-logs.html`. **Consumes:** `useLogStream({enabled: live_logs})`. **Verify:** build + smoke.

- [ ] **Step 1:** header — title + a **Live · SSE** badge. When `!capabilities.live_logs`, render a disabled notice ("Live logs disabled — set `POLYFLARE_LIVE_LOGS=1`") instead of the console.
- [ ] **Step 2:** controls — level filter (All/Info/Warn/Error), Pause/Resume (drives `useLogStream.pause/resume`), Clear (client buffer), Auto-scroll toggle, text filter.
- [ ] **Step 3:** the console — a monospace, auto-scrolling list of the last 1000 events: `ts` (muted) + level tag (colored) + message, with account/provider colored where present. Level filter + text filter applied client-side. Footer notes the 1000-line buffer + auto-reconnect. **Commit** `feat(dashboard): Live Logs SSE console`.

---

## Self-Review

**Spec coverage:** every SPEC §7 page → a task (Login T3, Overview T5, Accounts T6, Account detail T7, Pools T8, Requests T9, Live Logs T10); the design system §4 → T1+T4; auth §3.3 + capabilities gate → T2+T3; SSE consumption §3.4 → T2+T10; the endpoint list §6 → the hooks in T2 consumed per page. The read-only/Phase-3 boundary → T7 Actions rendered disabled. No mutation UI anywhere.

**Placeholder scan:** No "TBD". Two intentional references: (a) the exact serde field names — each data task MUST read `read_api.rs`/`auth.rs`/`sse.rs` and match them (the plan's JSON shapes are a guide, the source is authoritative); (b) the per-page pixel layout defers to the named mockup file. These are deliberate (avoids transcribing 1000 lines of already-approved CSS), not gaps — the source + mockup are concrete and on disk.

**Type consistency:** the response interfaces are defined once in `api.ts` (T2) and consumed by name in every hook/page; the hook names (`useOverview/useAccounts/useAccount/useAccountTrends/usePools/useRequests/useLogStream`) are fixed in T2 and referenced verbatim in T5–T10; the UI atom names (`Card/Grid/Col/MetricCard/QuotaBars/StatusPill/ProviderTag/Sparkline`) are fixed in T4 and used by name thereafter.

---

## Execution Handoff

Execute with superpowers:subagent-driven-development (recommended). Each task's verification is `bun run build` (zero TS errors) plus a visual/render smoke against the named mockup; the task review checks the page matches its mockup + consumes the correct endpoint with the correct (source-verified) field names + stays read-only. Final whole-branch review before merge.
