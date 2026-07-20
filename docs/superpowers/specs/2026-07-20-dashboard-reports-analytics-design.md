# Dashboard Reports / Analytics — Design

**Status:** Approved 2026-07-20. Phase 2 of the dashboard build-out (the roadmap's designated analytics phase; Phase 3 account-admin shipped first). Next: implementation plan → SDD.

## Motivation

PolyFlare captures rich per-request telemetry but only summarizes it on the Overview. A dedicated **Reports** page turns the `request_log` history into decision-grade views: **cost** (spend by account/model over time), **usage** (token throughput, cache-hit rate, request volume), and **performance** (duration, TTFT, error rate). This is the "cost visibility across accounts" feature that makes the dashboard feel complete versus codex-lb / ccflare.

**Data is now live (prerequisite shipped).** Sub-project 1 (live usage + cost capture, merged) means `request_log` now carries tokens + a cent-accurate `cost_usd` + TTFT on **live** PolyFlare traffic — not just the ~185k imported codex-lb rows. So Reports reflects ongoing activity, not a frozen snapshot. Note the two token-column generations: the imported rows populate the `0005` family (`input_tokens`/`output_tokens`/`cost_usd`/`latency_first_token_ms`), live rows populate the same `0005` family via `update_usage`; the legacy `0007` `total_tokens` is NULL everywhere, so **all token aggregation uses the `total_tokens` ELSE `input_tokens + output_tokens` fallback** (never `+ reasoning_tokens` — reasoning ⊆ output). Cost cross-checks to the cent against the ported pricing table. Caveat: `latency_first_token_ms`/`duration_ms` are now consistent-origin on live rows (the TPS-basis fix), so TTFT/TPS are meaningful; `service_tier` is not yet detected per-account, so non-default-tier cost is approximate until a later slice.

## Scope

One read-only `/reports` page: a single scrollable page with a **shared control bar** driving three co-equal stacked sections — **Cost**, **Usage**, **Performance** — all rendered from a single server-aggregated dataset. No mutations. Aggregation happens entirely in SQL (185k rows are never shipped raw).

**Explicitly out of scope (this slice):** CSV/data export; custom date ranges (presets only); rebuilding depletion/pace forecasting (already on the Overview pace card + per-account trends — Reports links to those); per-model quota inventory; anything mutating.

## Backend — one composite endpoint

`GET /api/reports?range=<24h|7d|30d>&dimension=<account|model|provider>&provider=<optional>`

- **Admin-gated** by the existing `require_admin` route layer (all `/api/*`). **Content-free:** the response carries only counts, sums, averages, model slugs, account ids, and provider — never a body, prompt, or token/bearer value (same content-safety class as the existing read APIs).
- **Params:** `range` selects `since_ts` + bucket size (24h → hourly buckets, 7d/30d → daily buckets); `dimension` selects the `GROUP BY` key for the breakdown; `provider` optionally filters. Unknown/absent `range`/`dimension` fall back to sane defaults (`7d`, `model`); an invalid explicit value → 400.
- **Two indexed SQL sweeps** over `request_log WHERE requested_at >= since_ts [AND provider = ?]` (uses the existing `idx_request_log_requested_at` / `idx_request_log_provider_time`):
  1. **`time_series`** — one row per time bucket, **zero-filled** across the whole range in the handler (mirrors `/api/overview/series`'s zero-fill so there are no gaps). Each bucket carries the full metric set (below).
  2. **`breakdown`** — one row per distinct value of `dimension`, same metric set + a display `label` (for `account`: the account's alias-or-id/email; for `model`/`provider`: the slug itself).
- **Metric set** (per bucket and per breakdown row): `requests`, `errors`, `cost_usd` (SUM, skips the ~2.4% NULLs), `input_tokens`, `output_tokens`, `cached_tokens`, `reasoning_tokens` (SUMs), `avg_duration_ms` (AVG over `duration_ms`), `avg_ttft_ms` (AVG over `latency_first_token_ms` — SQL `AVG` naturally skips the ~65% NULLs), and **`ttft_sample_count`** (COUNT of non-NULL `latency_first_token_ms`, so the UI can caveat a partial average).
- **`totals`** — the range rollup: `requests`, `errors`, `error_rate`, `cost_usd`, total `tokens`, `cached_tokens`, `cache_hit_rate` (cached / input), `avg_duration_ms`, `avg_ttft_ms`, `ttft_sample_count`.
- **Store layer:** one `RequestLogRepo` aggregation method pair (bucketed series + dimension breakdown), mirroring the existing `series_since`. Switching `dimension` refetches the whole (few-KB) payload; the time_series/totals recompute is cheap on indexed columns.

Errors: DB error → the existing generic 500 (`Response::error()`), never a partial/leaky body.

## Frontend

- New `crates/polyflare-server/dashboard/src/pages/Reports.tsx`; a Sidebar nav entry (add a `BarChart3`-family icon via the `ui/icons.ts` barrel — reuse one already exported if it fits); a route in `App.tsx` (`/reports`, inside the authed Shell).
- **Data:** `useReports(params)` React-Query hook + `api.reports(qs)` in `lib/api.ts` + typed `ReportsView` mirror of the Rust serde struct (field-for-field, like every other interface in that file). 60s refetch / staleTime (reports drift slowly, unlike the 30s list views).
- **Control bar** (top, shared): range preset (a segmented control: 24h / 7d / 30d), dimension select (account / model / provider), optional provider filter. Control state lives in the page (URL search-params optional, not required this slice); it is the React-Query key, so changing it refetches.
- **Three sections**, each = KPI cards (from `totals`) + a recharts chart (from `time_series`) + a breakdown table or horizontal bars (from `breakdown`), reusing existing ui atoms (`Card`, `Grid`, `MetricCard`, `Sparkline`, `StatusPill`, `ProviderTag`) and recharts (already a dep, used by Overview/trends):
  - **Cost** — spend-over-time area; per-dimension `$` bars/table; headline total `$` + cached-token savings estimate.
  - **Usage** — stacked token area (input/output/cached/reasoning) + request-volume; per-dimension token table; cache-hit rate.
  - **Performance** — avg-duration + avg-TTFT lines (TTFT plotted only where `ttft_sample_count > 0`, with a footnote) + error-rate; per-dimension latency/error table.
- **Skin:** ccflare tokens (dark default), **no emoji**, `tabular-nums`, `codex`=orange / `claude`=purple via the existing `ProviderTag`/`providerBrandKey()` mapping. Follow the Overview page's sectioned layout and the 12-col `Grid`.

## Data flow

control-bar state → `useReports(range, dimension, provider)` → `GET /api/reports?…` → one composite payload → the three sections each render a different slice (different fields of the same `time_series`/`breakdown`, different KPIs from `totals`). One query, one refetch per control change.

## Error / empty handling

- **Empty range** (no rows) → zero-filled `time_series` + empty `breakdown` + zeroed `totals` → each section shows a "no data in this range" empty state, never a crash or a misleading chart.
- **Partial TTFT** → the TTFT line renders only where `ttft_sample_count > 0`; a footnote states it covers ~a fraction of requests. A zero-filled bucket's `avg_*` is 0/absent and must not read as a real 0ms.
- **Null cost** (~2.4%) → excluded from `SUM` (treated as absent, not 0-cost); a small "cost is best-effort" note.
- **401** → the existing `fetchJson` unauthorized handler (redirect to login).

## Testing

- **Backend (store + endpoint, TDD):** seed `request_log` rows spanning multiple buckets, dimensions, and providers, then assert: bucketed sums/counts are correct; buckets are zero-filled across the full range (no gaps); the breakdown groups by the selected dimension with correct labels; the `provider` filter narrows correctly; `avg_ttft_ms` skips NULLs and `ttft_sample_count` counts only non-NULLs; `cost_usd` SUM skips NULLs; an invalid `range`/`dimension` → 400; keyless → 401 (router layer). Reuse the existing `read_api.rs` / `write_api.rs` test harness + the `RequestLogRecord` seed helper.
- **Frontend:** `bun run build` (tsc strict + vite build) clean — no frontend test runner, so typecheck+build is the gate; then a **live click-through** against the real 185k-row store (each range preset, each dimension, provider filter, empty-range state).
- **Content-safety:** grep the new endpoint's outputs/logs — only numeric aggregates + slugs/ids; no body/prompt/token surface widened.

## Global constraints

- **Content-free** (no conversation content; no token/bearer value logged or returned); **admin-gated** (existing `require_admin`).
- **Additive / backward-compatible:** a new endpoint + a new page + a nav entry + a new `RequestLogRepo` method; nothing existing changes behavior.
- **No emoji in UI; ccflare-skin consistency; reuse existing components/atoms + recharts + the `ui/icons.ts` barrel.**
- **Wedge-sacred:** touches none of the streaming/continuity core (`ObservingStream`, `watchdog.rs`, `continuity.rs`, `select.rs`).
- Clippy `-D warnings` (`--all-targets`), `cargo fmt`, full `cargo test -p polyflare-server` + `-p polyflare-store` green; dashboard `tsc` + build clean; the tracked `dist/` bundle rebuilt and committed with each frontend commit.

## Out of scope (explicit)

CSV/data export; custom date ranges; depletion/pace forecasting (links to the existing Overview pace card + account trends instead); per-model quota inventory; any mutation; the other Phase-4 subsystems (Settings, API-keys, Automations) — each its own spec→plan→ship cycle.
