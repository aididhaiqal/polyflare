# Dashboard Reports / Analytics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** A read-only `/reports` page with a shared control bar driving three stacked sections (Cost, Usage, Performance), all from one server-aggregated `/api/reports` endpoint over `request_log` (now live, post-SP1).

**Architecture:** One composite `GET /api/reports?range&dimension&provider` returns `{ time_series, breakdown, totals }` computed by SQL aggregation (185k+ rows never shipped raw). The React page renders slices of that one payload into three sections, reusing recharts (as `AccountDetail.tsx` does) + the ccflare skin.

**Tech Stack:** Rust (`polyflare-store` SQL aggregation, `polyflare-server` read_api handler), React 18 + TS + @tanstack/react-query v5 + recharts, bun.

## Global Constraints
- **Content-free:** only counts, sums, averages, model slugs, account ids, provider — never a body/prompt/token. Admin-gated by the existing `require_admin`.
- **Token totals use the fallback:** `total_tokens` ELSE `input_tokens + output_tokens` (NEVER `+ reasoning_tokens` — reasoning ⊆ output). Mirror `aggregate_since`'s exact `COALESCE(SUM(COALESCE(total_tokens, COALESCE(input_tokens,0)+COALESCE(output_tokens,0))),0)`.
- **TTFT is partially populated:** `AVG(latency_first_token_ms)` skips NULLs; carry a `ttft_sample_count` so the UI caveats it.
- **No emoji; ccflare skin; reuse existing ui atoms + recharts + the `ui/icons.ts` barrel; tabular-nums; codex=orange/claude=purple.**
- **Wedge-sacred:** touch none of `watchdog.rs`/`continuity.rs`/`select.rs`.
- **Additive:** new endpoint + new page + nav entry; nothing existing changes. Clippy `--all-targets -D warnings`, fmt, `cargo test -p polyflare-server -p polyflare-store` green; dashboard `bun run build` clean; the tracked `dist/` rebuilt+committed per frontend commit.

---

### Task 1: Store — reports aggregation (totals + breakdown + series)

**Files:** Modify `crates/polyflare-store/src/request_log_repo.rs` (model on `aggregate_since` ~L339 / `series_since` ~L381 / `RequestAggregate` ~L143 / `RequestBucket` ~L156). Test: inline `#[cfg(test)]`.

**Interfaces produced:**
- `pub struct ReportMetrics { pub requests: i64, pub errors: i64, pub cost_usd: f64, pub tokens: i64, pub cached_tokens: i64, pub reasoning_tokens: i64, pub avg_duration_ms: f64, pub avg_ttft_ms: f64, pub ttft_sample_count: i64 }` — the shared metric set.
- `pub struct ReportBucket { pub ts: i64, pub metrics: ReportMetrics }`
- `pub struct ReportBreakdownRow { pub key: String, pub metrics: ReportMetrics }` (`key` = the dimension value; NULL account/model/provider → `""` via `COALESCE(col,'')`).
- `pub async fn reports_totals(&self, since_ts: i64, provider: Option<&str>) -> Result<ReportMetrics, StoreError>`
- `pub async fn reports_series(&self, since_ts: i64, bucket_secs: i64, provider: Option<&str>) -> Result<Vec<ReportBucket>, StoreError>` — one row per bucket that HAS rows (zero-fill is the handler's job, per `series_since`).
- `pub async fn reports_breakdown(&self, since_ts: i64, dimension: &str, provider: Option<&str>) -> Result<Vec<ReportBreakdownRow>, StoreError>` — `dimension` ∈ `{"account","model","provider"}` maps to the GROUP BY column `{account_id, model, provider}`; reject others by defaulting to `model` (the handler validates first, so this is defense-in-depth).

The shared metric SELECT list (reuse verbatim in all three, with the WHERE/GROUP BY differing):
```sql
COUNT(*),
COALESCE(SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END), 0),
COALESCE(SUM(cost_usd), 0.0),
COALESCE(SUM(COALESCE(total_tokens, COALESCE(input_tokens,0)+COALESCE(output_tokens,0))), 0),
COALESCE(SUM(COALESCE(cached_tokens, cached_input_tokens, 0)), 0),
COALESCE(SUM(COALESCE(reasoning_tokens, 0)), 0),
COALESCE(AVG(duration_ms), 0.0),
COALESCE(AVG(latency_first_token_ms), 0.0),
COUNT(latency_first_token_ms)
```
`provider` filter: append `AND provider = ?` when `Some`. `series` GROUP BY `(requested_at / bucket_secs) * bucket_secs` ascending (clamp `bucket_secs` to ≥1). `breakdown` GROUP BY the dimension column, ORDER BY cost_usd DESC.

- [ ] **Step 1: Failing tests** — seed request_log rows across 2 buckets, 2 models, 2 providers, with `total_tokens=None` + `input/output` set (imported-shaped) + some `cost_usd`, some `latency_first_token_ms` NULL. Assert: `reports_totals` sums correct (cost, tokens via fallback, errors by status≥400, ttft_sample_count counts only non-NULL ttft, avg_ttft over non-NULL only); `reports_breakdown("model")` groups by model with right per-model metrics + labels; `reports_series` buckets ascending with per-bucket metrics; provider filter narrows. (Reuse the file's existing seed helpers.)
- [ ] **Step 2: RED** — `cargo test -p polyflare-store request_log`.
- [ ] **Step 3: Implement** the three methods + structs, reusing the shared SELECT list. A private helper building the metric tuple → `ReportMetrics` avoids duplication across the three.
- [ ] **Step 4: GREEN** — `cargo test -p polyflare-store`; clippy `--all-targets` + fmt clean.
- [ ] **Step 5: Commit** — `feat(store): reports aggregation (totals/breakdown/series with token+cost)`.

---

### Task 2: `GET /api/reports` endpoint

**Files:** Modify `crates/polyflare-server/src/read_api.rs` (+ route in `crates/polyflare-server/src/app.rs`). Test: `crates/polyflare-server/tests/read_api.rs`.

**Interfaces:**
- Consumes Task 1's `reports_totals`/`reports_series`/`reports_breakdown`.
- Produces `GET /api/reports?range=<24h|7d|30d>&dimension=<account|model|provider>&provider=<opt>` → JSON `ReportsView { time_series: Vec<ReportBucketView>, breakdown: Vec<ReportBreakdownView>, totals: ReportTotalsView }` where the metric fields are serialized flat (mirror `SeriesBucketView`'s style). `range`→(since_ts, bucket_secs): `24h`→(now-86400, 3600), `7d`→(now-604800, 86400), `30d`→(now-2592000, 86400). Absent/unknown `range`→`7d`, absent/unknown `dimension`→`model` (a KNOWN-invalid explicit value like `range=99h`/`dimension=foo` → 400). The `time_series` is ZERO-FILLED across `[since_ts, now]` at `bucket_secs` (mirror `overview_series_handler`'s zero-fill), so gaps become zeroed buckets.
- `totals` adds derived `error_rate` (errors/requests, 0 when requests=0), `cache_hit_rate` (cached/tokens... use cached/(input) — since tokens is the fallback total, use `cached_tokens as f64 / tokens as f64` guarded for 0).

- [ ] **Step 1: Failing test** — admin `GET /api/reports?range=7d&dimension=model` → 200 with `time_series` (zero-filled, ascending), `breakdown` (per-model), `totals` (requests/cost/tokens/error_rate); `?range=bogus` → 400; keyless → 401. Seed rows via the existing harness.
- [ ] **Step 2: RED** — `cargo test -p polyflare-server --test read_api`.
- [ ] **Step 3: Implement** the handler (parse+validate params → call the three store methods → zero-fill series → assemble `ReportsView`) + register the route `/api/reports` inside the admin-gated `api` router in `app.rs`.
- [ ] **Step 4: GREEN** — full `cargo test -p polyflare-server`; clippy `--all-targets` + fmt clean.
- [ ] **Step 5: Commit** — `feat(read-api): GET /api/reports composite analytics endpoint`.

---

### Task 3: Frontend data layer + nav + route + page stub

**Files:** Modify `crates/polyflare-server/dashboard/src/lib/api.ts` (types + `api.reports`), `src/lib/queries.ts` (`useReports`), `src/App.tsx` (route), `src/shell/Sidebar.tsx` (nav entry). Create `src/pages/Reports.tsx` (stub).

**Interfaces:** `ReportsView` TS interface mirroring the Rust serde (field-for-field, like every interface in api.ts); `api.reports(qs: string)`; `useReports(params: {range, dimension, provider?})` (React-Query, 60s staleTime — reports drift slowly; key includes params). `Reports.tsx` stub: a control bar (range segmented 24h/7d/30d + dimension select account/model/provider + optional provider filter) whose state feeds `useReports`, and render `totals` as KPI cards (`MetricCard`) — no charts yet. Add a Sidebar nav entry to `/reports` (use an existing `BarChart3`/`Activity` icon from `ui/icons.ts`). Add the `<Route path="reports" element={<Reports/>}>` inside the authed Shell in App.tsx.

- [ ] **Step 1** — add the `ReportsView`/`ReportBucketView`/`ReportBreakdownView`/`ReportTotalsView` interfaces + `api.reports` to api.ts; `useReports` to queries.ts.
- [ ] **Step 2** — build the `Reports.tsx` stub (control bar + `useReports` + totals KPI cards via `MetricCard`; loading/error/empty states). Add nav + route.
- [ ] **Step 3 — Verify** — `bun run build` clean; `git add -A` (incl. rebuilt `dist/`); commit.
- [ ] **Step 4: Commit** — `feat(dashboard): Reports page data layer + nav + totals stub`.

---

### Task 4: `ReportSection` scaffold + Cost section

**Files:** Modify `crates/polyflare-server/dashboard/src/pages/Reports.tsx`; optionally create `src/ui/ReportSection.tsx` (a reusable section = title + KPI row + a recharts chart + a breakdown table).

**Interfaces consumed:** `useReports` data (Task 3). Model the recharts chart on `AccountDetail.tsx`'s existing chart (imports, `ResponsiveContainer`, axes, ccflare colors). Build `ReportSection` parameterized by: title, the KPI cards, a chart render (given `time_series`), and a breakdown table (given `breakdown` + which metric columns to show).

- [ ] **Step 1** — implement `ReportSection` (KPI cards + `ResponsiveContainer` chart + a `<table>` breakdown styled like the Requests/Accounts tables, tabular-nums, no emoji).
- [ ] **Step 2** — wire the **Cost** section: spend-over-time area chart from `time_series[].cost_usd`; per-dimension `$` breakdown table (cost, requests, tokens); KPI cards = total `$`, cached-token savings estimate, requests. Format `$` with the app's number format helper (`lib/format.ts`).
- [ ] **Step 3 — Verify** — `bun run build` clean; `git add -A` (dist); commit.
- [ ] **Step 4: Commit** — `feat(dashboard): Reports Cost section + reusable ReportSection`.

---

### Task 5: Usage + Performance sections

**Files:** Modify `crates/polyflare-server/dashboard/src/pages/Reports.tsx` (reuse `ReportSection`).

- [ ] **Step 1** — **Usage** section: stacked token area (input/output... use `tokens` + `cached_tokens` + `reasoning_tokens` from `time_series`) + request-volume; per-dimension token breakdown table; cache-hit-rate KPI.
- [ ] **Step 2** — **Performance** section: avg-duration + avg-TTFT lines from `time_series` (render TTFT only where `ttft_sample_count > 0`, with a footnote that it covers a fraction of requests) + error-rate; per-dimension latency/error breakdown table.
- [ ] **Step 3 — Verify** — `bun run build` clean; `git add -A` (dist); commit.
- [ ] **Step 4: Commit** — `feat(dashboard): Reports Usage + Performance sections`.

---

### Task 6: Live verification (controller-run)

- [ ] **Step 1** — Build + run the server against a store clone (valid tokens); issue a few real requests via `scripts/codex-polyflare` so live rows exist.
- [ ] **Step 2** — Load `/reports` (browser if the Chrome extension is available, else curl `/api/reports` for each range/dimension + assert the payload): confirm each range preset (24h/7d/30d), each dimension (account/model/provider), and the provider filter return sane aggregates; empty-range shows the empty state; cost totals cross-check against a direct `SUM(cost_usd)` query; the token totals are nonzero (fallback working).
- [ ] **Step 3** — Content-safety: grep the server log — only numeric aggregates + slugs, no content. Confirm wedge-clean + latency gate green.

---

## Self-Review
- Spec coverage: Cost/Usage/Performance sections → T4/T5; composite endpoint → T1/T2; control bar + shared payload → T3; zero-fill + TTFT caveat + token fallback → T1/T2; live-verify → T6. Covered.
- Types: `ReportMetrics`/`ReportsView` field names consistent T1→T2→T3. Token fallback identical to `aggregate_since`. No placeholders.
