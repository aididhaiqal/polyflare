// The dashboard's landing page. Built from FOUR real endpoints:
//   - `useOverview()`      GET /api/overview         — content-free 24h aggregates (KPIs, per-
//                                                       provider quota headroom, per-pool counts,
//                                                       recent errors).
//   - `useOverviewSeries()` GET /api/overview/series  — 24h of hourly request-volume buckets,
//                                                       zero-filled by the handler (Task 5a).
//   - `useAccounts()`      GET /api/accounts          — the live per-account list (status, usage
//                                                       windows, token health, 24h request count).
//   - `usePace()`          GET /api/pace              — the pool-wide weekly credit pace forecast
//                                                       (admin-gated; D16 T6).
//
// Task 5 shipped only the first of these (no time series / no per-account list existed yet) and
// documented three deferred mockup rows in task-5-report.md: the request-volume chart, the
// account-health table, and a weekly-pace forecast. Task 5a added the series endpoint; task 5b
// restored the first two using ONLY real, derived-from-real-fields data (see task-5b-report.md) and
// stood the weekly-pace card up as a legitimate client-side per-provider linear-extrapolation
// derivation. The Weekly Pace card now sources GET /api/pace (backend EWMA burn-rate + pool-drain
// simulation), replacing that earlier client-side per-provider linear-extrapolation estimate; see
// task-6-report.md (D16) for the field mapping.
import { useEffect, useState, type ReactNode } from "react";
import { Link } from "react-router-dom";
import clsx from "clsx";

import type { AccountView, PaceStatus, RecentErrorView, WeeklyCreditPaceReport } from "../lib/api";
import { compactNum, latency, pct, relTime } from "../lib/format";
import { useAccounts, useOverview, useOverviewSeries, usePace } from "../lib/queries";
import { Card } from "../ui/Card";
import { Col, Grid } from "../ui/Grid";
import { Activity, AlertTriangle, CheckCircle2, ChevronRight, Clock, Coins, Lock } from "../ui/icons";
import { MetricCard } from "../ui/MetricCard";
import { providerBrandKey, ProviderTag } from "../ui/ProviderTag";
import { QuotaBars, type QuotaProviderGroup } from "../ui/QuotaBars";
import { Sparkline } from "../ui/Sparkline";
import { StatusPill, statusTone } from "../ui/StatusPill";

type ProviderFilter = "all" | "codex" | "claude";

const PROVIDER_FILTERS: Array<{ value: ProviderFilter; label: string }> = [
  { value: "all", label: "All" },
  { value: "codex", label: "Codex" },
  { value: "claude", label: "Claude" },
];

function matchesFilter(provider: string, filter: ProviderFilter): boolean {
  return filter === "all" || providerBrandKey(provider) === filter;
}

export function Overview() {
  const { data, isLoading, isError, error, refetch, dataUpdatedAt } = useOverview();
  const seriesQuery = useOverviewSeries();
  const accountsQuery = useAccounts();
  const paceQuery = usePace();
  const [providerFilter, setProviderFilter] = useState<ProviderFilter>("all");

  // Ticks the header's "updated Xs ago" text (and the weekly-pace elapsed-fraction math below)
  // between refetches (useOverview() polls every 30s; without this the label/pace would only ever
  // update once per poll instead of counting up smoothly).
  const [nowMs, setNowMs] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNowMs(Date.now()), 5000);
    return () => clearInterval(id);
  }, []);

  if (isLoading) {
    return <OverviewSkeleton />;
  }

  if (isError) {
    return (
      <div className="flex flex-col gap-3">
        <PageHeader />
        <Card>
          <div className="flex flex-wrap items-center justify-between gap-3">
            <span className="flex items-center gap-2 text-[12px] text-error">
              <AlertTriangle className="h-4 w-4 shrink-0" strokeWidth={1.9} />
              Couldn&apos;t load the overview
              {error instanceof Error ? `: ${error.message}` : "."}
            </span>
            <button
              type="button"
              onClick={() => refetch()}
              className="shrink-0 rounded border border-border px-2.5 py-1 text-[11px] text-fg opacity-80 hover:opacity-100"
            >
              Retry
            </button>
          </div>
        </Card>
      </div>
    );
  }

  // Unreachable in practice once isLoading/isError are handled (retry:false means a settled query
  // is either success-with-data or error) — a defensive guard, not a real empty state, so TS can
  // narrow `data` below without a non-null assertion.
  if (!data) return null;

  const totalAccounts = data.pools.reduce((sum, p) => sum + p.accounts, 0);
  const hasRequests = data.kpis.requests > 0;

  const filteredQuota =
    providerFilter === "all"
      ? data.quota
      : data.quota.filter((q) => providerBrandKey(q.provider) === providerFilter);
  const quotaGroups: QuotaProviderGroup[] = filteredQuota.map((q) => ({
    provider: q.provider,
    windows: [
      { window: "five_hour", usedPercent: 100 - q.five_hour, meta: `${pct(q.five_hour)} left` },
      { window: "weekly", usedPercent: 100 - q.weekly, meta: `${pct(q.weekly)} left` },
    ],
  }));

  const accounts = accountsQuery.data ?? [];

  return (
    <div className="flex flex-col gap-3">
      <PageHeader
        subtitle={
          <>
            last 24h ·{" "}
            <span className="font-semibold text-success">
              {data.accounts_available} of {totalAccounts} accounts available
            </span>{" "}
            · {data.pools.length} {data.pools.length === 1 ? "pool" : "pools"} · updated{" "}
            {dataUpdatedAt ? relTime(Math.floor(dataUpdatedAt / 1000), nowMs) : "—"}
          </>
        }
        actions={
          <div className="flex shrink-0 overflow-hidden rounded border border-border bg-card text-[10.5px]">
            {PROVIDER_FILTERS.map((f) => (
              <button
                key={f.value}
                type="button"
                onClick={() => setProviderFilter(f.value)}
                className={clsx(
                  "px-2.5 py-1",
                  providerFilter === f.value
                    ? "bg-accent/[0.12] font-medium text-accent"
                    : "text-fg opacity-60 hover:opacity-100",
                )}
              >
                {f.label}
              </button>
            ))}
          </div>
        }
      />

      <Grid>
        <Col span={3}>
          <MetricCard icon={Activity} title="Requests" value={compactNum(data.kpis.requests)} />
        </Col>
        <Col span={3}>
          <MetricCard
            icon={CheckCircle2}
            title="Success rate"
            value={hasRequests ? pct(data.kpis.success_rate * 100) : "—"}
          />
        </Col>
        <Col span={3}>
          <MetricCard
            icon={Clock}
            title="Avg latency"
            value={hasRequests ? latency(data.kpis.avg_latency_ms) : "—"}
          />
        </Col>
        <Col span={3}>
          <MetricCard icon={Coins} title="Tokens" value={compactNum(data.kpis.total_tokens)} />
        </Col>

        <Col span={5}>
          <RequestVolumeCard
            isLoading={seriesQuery.isLoading}
            isError={seriesQuery.isError}
            error={seriesQuery.error}
            onRetry={() => seriesQuery.refetch()}
            buckets={seriesQuery.data?.buckets ?? []}
          />
        </Col>

        <Col span={3}>
          <Card>
            <div className="text-[10px] uppercase tracking-wide text-fg opacity-60">Quota</div>
            {quotaGroups.length === 0 ? (
              <p className="mt-2 text-[11px] text-fg opacity-50">
                {data.quota.length === 0
                  ? "No accounts configured yet."
                  : "No accounts for this provider."}
              </p>
            ) : (
              <QuotaBars groups={quotaGroups} className="mt-1.5" />
            )}
          </Card>
        </Col>

        <Col span={4}>
          <PaceCard
            isLoading={paceQuery.isLoading}
            isError={paceQuery.isError}
            error={paceQuery.error}
            onRetry={() => paceQuery.refetch()}
            pace={paceQuery.data?.pace ?? null}
          />
        </Col>

        <Col span={8}>
          <AccountHealthCard
            isLoading={accountsQuery.isLoading}
            isError={accountsQuery.isError}
            error={accountsQuery.error}
            onRetry={() => accountsQuery.refetch()}
            accounts={accounts}
            providerFilter={providerFilter}
          />
        </Col>

        <Col span={4}>
          <Card>
            <div className="flex items-center justify-between text-[10px] uppercase tracking-wide text-fg opacity-60">
              <span>Pools</span>
              <span className="normal-case tracking-normal opacity-70">
                {data.accounts_available}/{totalAccounts} available
              </span>
            </div>
            {data.pools.length === 0 ? (
              <p className="mt-2 text-[11px] text-fg opacity-50">No pools configured yet.</p>
            ) : (
              <div className="mt-1.5 flex flex-col">
                {data.pools.map((p) => {
                  const label = p.pool ?? "unpooled";
                  const availPct = p.accounts > 0 ? (p.available / p.accounts) * 100 : 0;
                  return (
                    <div
                      key={label}
                      className="flex items-center gap-2 border-b border-border py-1.5 text-[10.5px] last:border-0"
                    >
                      <span className="w-16 shrink-0 truncate font-semibold text-fg">{label}</span>
                      <span className="w-[78px] shrink-0 text-fg opacity-60">
                        {p.accounts} {p.accounts === 1 ? "acct" : "accts"} · {p.available} avail
                      </span>
                      <div className="h-1.5 flex-1 overflow-hidden rounded-full bg-muted">
                        <div
                          className="h-full rounded-full bg-accent"
                          style={{ width: `${Math.max(0, Math.min(100, availPct))}%` }}
                        />
                      </div>
                      <span className="w-9 shrink-0 text-right text-fg opacity-70">{pct(availPct)}</span>
                    </div>
                  );
                })}
              </div>
            )}
            <p className="mt-auto border-t border-border pt-1.5 text-[9.5px] text-fg opacity-50">
              Pools route independently — pick a strategy per pool in Settings.
            </p>
          </Card>
        </Col>

        <Col span={12}>
          <RecentErrorsStrip errors={data.recent_errors} />
        </Col>
      </Grid>
    </div>
  );
}

/** Title + optional subtitle/actions row. Rendered in every query state (loading gets a skeleton
 * variant, error/success get the real title) so the page never loses its heading. */
function PageHeader({
  subtitle,
  actions,
}: {
  subtitle?: ReactNode;
  actions?: ReactNode;
}) {
  return (
    <div className="flex items-center justify-between gap-3">
      <div>
        <h1 className="text-lg font-semibold text-fg">Overview</h1>
        {subtitle && <p className="mt-0.5 text-[11px] text-fg opacity-60">{subtitle}</p>}
      </div>
      {actions}
    </div>
  );
}

/** Shared shape for the three new cards' independent loading/error handling — each of `useAccounts`
 * / `useOverviewSeries` resolves on its own schedule, so a slow/failed one degrades only its own
 * card rather than blocking the whole page (the page-level `isError` above only covers
 * `useOverview`). */
interface AsyncCardState {
  isLoading: boolean;
  isError: boolean;
  error: unknown;
  onRetry: () => void;
}

function AsyncCardStatus({ title, state }: { title: string; state: AsyncCardState }) {
  if (state.isLoading) {
    return (
      <Card>
        <div className="text-[10px] uppercase tracking-wide text-fg opacity-60">{title}</div>
        <div className="mt-2 h-24 animate-pulse rounded bg-muted" />
      </Card>
    );
  }
  if (state.isError) {
    return (
      <Card>
        <div className="text-[10px] uppercase tracking-wide text-fg opacity-60">{title}</div>
        <div className="mt-2 flex flex-1 flex-col items-start justify-center gap-1.5 text-[11px] text-error">
          <span className="flex items-center gap-1.5">
            <AlertTriangle className="h-3.5 w-3.5 shrink-0" strokeWidth={1.9} />
            Couldn&apos;t load {state.error instanceof Error ? `: ${state.error.message}` : "."}
          </span>
          <button
            type="button"
            onClick={state.onRetry}
            className="rounded border border-border px-2 py-0.5 text-[10.5px] text-fg opacity-80 hover:opacity-100"
          >
            Retry
          </button>
        </div>
      </Card>
    );
  }
  return null;
}

// ---------------------------------------------------------------------------------------------
// Request-volume chart — `GET /api/overview/series` (24h, hourly, zero-filled by the handler).
// ---------------------------------------------------------------------------------------------

interface SeriesBucketLike {
  ts: number;
  requests: number;
  errors: number;
  avg_latency_ms: number;
  total_tokens: number;
}

function RequestVolumeCard(
  props: AsyncCardState & { buckets: SeriesBucketLike[] },
) {
  const status = AsyncCardStatus({ title: "Request volume · 24h", state: props });
  if (status) return status;

  const { buckets } = props;
  const totalRequests = buckets.reduce((sum, b) => sum + b.requests, 0);
  const peak = buckets.reduce((max, b) => Math.max(max, b.requests), 0);
  // IMPORTANT: only `requests` (a real count, where 0 genuinely means "no traffic that hour") is
  // ever plotted here. `avg_latency_ms` is deliberately NEVER charted per-bucket: a zero-filled
  // no-traffic bucket carries `avg_latency_ms: 0.0` on the wire, indistinguishable from a real 0ms
  // average, and plotting it would render every quiet hour as a (fake) latency improvement. See
  // task-5b-report.md.
  const values = buckets.map((b) => b.requests);

  return (
    <Card>
      <div className="flex items-center justify-between text-[10px] uppercase tracking-wide text-fg opacity-60">
        <span>Request volume · 24h</span>
        <span className="normal-case tracking-normal opacity-70">peak {compactNum(peak)}/hr</span>
      </div>
      {buckets.length === 0 ? (
        <p className="mt-2 text-[11px] text-fg opacity-50">No data yet.</p>
      ) : (
        <>
          <Sparkline data={values} area height={132} className="mt-2" />
          {totalRequests === 0 && (
            <p className="mt-1 text-[10px] text-fg opacity-50">No requests in the last 24h.</p>
          )}
        </>
      )}
    </Card>
  );
}

// ---------------------------------------------------------------------------------------------
// Account-health table — `GET /api/accounts` (no backend work needed for this task).
// ---------------------------------------------------------------------------------------------

const TONE_BAR_CLASS: Record<"ok" | "warn" | "error", string> = {
  ok: "bg-success",
  warn: "bg-warn",
  error: "bg-error",
};

/** Usage-risk thresholds for the health table's 5-hour/weekly mini bars: how close to exhausted a
 * window is, not which provider it belongs to (unlike the pixel mockup, which colors one example
 * row by provider brand — see task-5b-report.md for why this page uses a single, consistent
 * risk-based scale across every row instead). */
function usageRiskTone(usedPercent: number): "ok" | "warn" | "error" {
  if (usedPercent >= 90) return "error";
  if (usedPercent >= 70) return "warn";
  return "ok";
}

function UsageMiniBar({ usedPercent }: { usedPercent: number | null }) {
  if (usedPercent === null) {
    return <span className="text-fg opacity-40">—</span>;
  }
  const clamped = Math.max(0, Math.min(100, usedPercent));
  return (
    <div className="flex items-center gap-1.5">
      <div className="h-[5px] w-[50px] shrink-0 overflow-hidden rounded-full bg-muted">
        <div
          className={clsx("h-full rounded-full", TONE_BAR_CLASS[usageRiskTone(clamped)])}
          style={{ width: `${clamped}%` }}
        />
      </div>
      <span className="text-fg opacity-70">{pct(clamped)}</span>
    </div>
  );
}

function AccountHealthCard(
  props: AsyncCardState & { accounts: AccountView[]; providerFilter: ProviderFilter },
) {
  const status = AsyncCardStatus({ title: "Account health", state: props });
  if (status) return status;

  const { accounts, providerFilter } = props;
  const filtered = accounts.filter((a) => matchesFilter(a.provider, providerFilter));

  return (
    <Card>
      <div className="text-[10px] uppercase tracking-wide text-fg opacity-60">Account health</div>
      {accounts.length === 0 ? (
        <p className="mt-2 text-[11px] text-fg opacity-50">No accounts configured yet.</p>
      ) : filtered.length === 0 ? (
        <p className="mt-2 text-[11px] text-fg opacity-50">No accounts for this provider.</p>
      ) : (
        <div className="mt-1.5 max-h-64 overflow-x-auto overflow-y-auto">
          <table className="w-full min-w-[520px] border-collapse text-[10.5px]">
            <thead>
              <tr>
                <th className="sticky top-0 bg-card px-1.5 py-1 text-left text-[9px] font-medium uppercase tracking-wide text-fg opacity-60">
                  Account
                </th>
                <th className="sticky top-0 bg-card px-1.5 py-1 text-left text-[9px] font-medium uppercase tracking-wide text-fg opacity-60">
                  Provider
                </th>
                <th className="sticky top-0 bg-card px-1.5 py-1 text-left text-[9px] font-medium uppercase tracking-wide text-fg opacity-60">
                  Pool
                </th>
                <th className="sticky top-0 bg-card px-1.5 py-1 text-left text-[9px] font-medium uppercase tracking-wide text-fg opacity-60">
                  Status
                </th>
                <th className="sticky top-0 bg-card px-1.5 py-1 text-left text-[9px] font-medium uppercase tracking-wide text-fg opacity-60">
                  5-hour
                </th>
                <th className="sticky top-0 bg-card px-1.5 py-1 text-left text-[9px] font-medium uppercase tracking-wide text-fg opacity-60">
                  Weekly
                </th>
                <th className="sticky top-0 bg-card px-1.5 py-1 text-right text-[9px] font-medium uppercase tracking-wide text-fg opacity-60">
                  Reqs (24h)
                </th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((a) => (
                <tr key={a.id} className="border-b border-border/55 last:border-0">
                  <td className="whitespace-nowrap px-1.5 py-1.5">
                    <span
                      className={clsx(
                        "mr-1.5 inline-block h-[7px] w-[7px] rounded-full",
                        TONE_BAR_CLASS[statusTone(a.status)],
                      )}
                    />
                    {a.id}
                  </td>
                  <td className="px-1.5 py-1.5">
                    <ProviderTag provider={a.provider} />
                  </td>
                  <td className="px-1.5 py-1.5 text-fg opacity-60">{a.pool ?? "unpooled"}</td>
                  <td className="px-1.5 py-1.5">
                    <StatusPill status={a.status} />
                  </td>
                  <td className="px-1.5 py-1.5">
                    <UsageMiniBar usedPercent={a.five_hour?.used_percent ?? null} />
                  </td>
                  <td className="px-1.5 py-1.5">
                    <UsageMiniBar usedPercent={a.weekly?.used_percent ?? null} />
                  </td>
                  <td className="px-1.5 py-1.5 text-right tabular-nums text-fg opacity-80">
                    {compactNum(a.request_count_24h)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Card>
  );
}

// ---------------------------------------------------------------------------------------------
// Weekly credit pace — `GET /api/pace` (D16 T6): the backend's pool-wide, EWMA-burn-rate +
// discrete-event pool-drain-simulation forecast (see `polyflare_core::weekly_pace`). Admin-gated,
// content-free (credits/percentages/hours/counts + status/confidence enums only — see
// `read_api.rs::pace_handler`'s doc comment). This replaces the earlier per-provider client-side
// linear-extrapolation estimate (task-5b's `computeWeeklyPace`), a legitimate stand-in derivation
// now superseded by this backend forecast. Unlike that estimate, the report is a single pool-wide
// number aggregated across every eligible account regardless of provider — it doesn't respond to
// the page's provider filter (the backend has no per-provider breakdown to filter).
// ---------------------------------------------------------------------------------------------

const PACE_STATUS_LABEL: Record<PaceStatus, string> = {
  on_track: "on track",
  ahead: "ahead",
  behind: "behind",
  danger: "at risk",
};

/** on_track = muted (neither over nor under budget), ahead = warn/gold (consuming FASTER than the
 * linear schedule — delta > +5%, caution), behind = success/green (consuming SLOWER than the
 * linear schedule — delta < -5%, more headroom), danger = flare-amber accent (the pool-drain sim
 * projects running dry before enough resets refill it) — the brief's "amber/critical" tone. */
const PACE_STATUS_CLASS: Record<PaceStatus, string> = {
  on_track: "bg-muted text-fg opacity-70",
  ahead: "bg-warn/15 text-warn",
  behind: "bg-success/15 text-success",
  danger: "bg-accent/15 text-accent",
};

function PaceStatusPill({ status }: { status: PaceStatus }) {
  return (
    <span className={clsx("rounded px-1.5 py-0.5 text-[9px] font-bold", PACE_STATUS_CLASS[status])}>
      {PACE_STATUS_LABEL[status]}
    </span>
  );
}

const PACE_CONFIDENCE_CLASS: Record<WeeklyCreditPaceReport["confidence"], string> = {
  high: "text-success",
  medium: "text-fg opacity-80",
  low: "text-warn",
};

/** Formats an hours-from-now duration (`projected_depletion_hours` is already relative, not an
 * epoch second the way `format.ts::countdown`'s input is) as `"Nd Nh"` / `"Nh Nm"` / `"Nm"`. */
function formatHoursFromNow(hours: number): string {
  if (!Number.isFinite(hours) || hours < 0) return "—";
  const totalMinutes = Math.round(hours * 60);
  const days = Math.floor(totalMinutes / 1440);
  const hrs = Math.floor((totalMinutes % 1440) / 60);
  const mins = totalMinutes % 60;
  if (days >= 1) return `${days}d ${hrs}h`;
  if (hrs >= 1) return `${hrs}h ${mins}m`;
  return `${mins}m`;
}

function PaceCard(props: AsyncCardState & { pace: WeeklyCreditPaceReport | null }) {
  const status = AsyncCardStatus({ title: "Weekly pace", state: props });
  if (status) return status;

  const { pace } = props;

  return (
    <Card>
      <div className="flex items-center justify-between text-[10px] uppercase tracking-wide text-fg opacity-60">
        <span>Weekly pace</span>
        {pace && <PaceStatusPill status={pace.status} />}
      </div>
      {!pace ? (
        <p className="mt-2 text-[11px] text-fg opacity-50">
          No eligible accounts to project a pace for yet.
        </p>
      ) : (
        <div className="mt-1.5 flex flex-col gap-2">
          <div className="relative h-[9px] rounded-full bg-muted">
            <div
              className={clsx(
                "h-full rounded-full",
                pace.status === "danger"
                  ? "bg-accent"
                  : pace.status === "ahead"
                    ? "bg-warn"
                    : "bg-success",
              )}
              style={{ width: `${Math.max(0, Math.min(100, pace.actual_used_percent))}%` }}
            />
            <div
              className="absolute -top-[3px] -bottom-[3px] w-[2px] rounded-sm bg-fg"
              style={{ left: `${Math.max(0, Math.min(100, pace.scheduled_used_percent))}%` }}
            />
          </div>

          <div className="flex justify-between text-[10px]">
            <span className="text-fg opacity-60">Used vs scheduled</span>
            <span className="text-fg">
              {pct(pace.actual_used_percent)} / {pct(pace.scheduled_used_percent)}{" "}
              <span className={pace.delta_percent > 0 ? "text-warn" : "text-success"}>
                {pace.delta_percent > 0 ? "+" : ""}
                {Math.round(pace.delta_percent)}%
              </span>
            </span>
          </div>
          <div className="flex justify-between text-[10px]">
            <span className="text-fg opacity-60">Capacity</span>
            <span className="font-semibold text-fg">
              {compactNum(pace.total_full_credits)} credits
            </span>
          </div>
          <div className="flex justify-between text-[10px]">
            <span className="text-fg opacity-60">Depletion</span>
            <span className="font-semibold text-fg">
              {pace.projected_depletion_hours === null
                ? "no shortfall"
                : `in ${formatHoursFromNow(pace.projected_depletion_hours)}`}
            </span>
          </div>
          <div className="flex justify-between text-[10px]">
            <span className="text-fg opacity-60">Confidence</span>
            <span
              className={clsx(
                "font-semibold capitalize",
                PACE_CONFIDENCE_CLASS[pace.confidence],
              )}
            >
              {pace.confidence}
            </span>
          </div>

          <div className="mt-0.5 border-t border-border pt-1.5 text-[9.5px] text-fg opacity-55">
            {pace.account_count} paced · {pace.stale_account_count} stale ·{" "}
            {pace.inactive_account_count} inactive
          </div>
          <div className="flex items-center gap-1.5 text-[9px] text-fg opacity-45">
            <Lock className="h-2.5 w-2.5 shrink-0" strokeWidth={1.9} />
            Credits and percentages only — no account identity or conversation content.
          </div>
        </div>
      )}
    </Card>
  );
}

// ---------------------------------------------------------------------------------------------
// Recent errors strip
// ---------------------------------------------------------------------------------------------

/** Groups content-free error rows by `(status, error_code)` so the strip shows one chip per
 * distinct failure kind rather than one per request. `recent_errors` is only the most recent
 * `RECENT_ERRORS_LIMIT` (10) rows (see read_api.rs), so counts here describe that recent window,
 * not a full historical tally. */
interface ErrorGroup {
  status: number;
  errorCode: string | null;
  count: number;
  accountIds: Set<string>;
}

function groupRecentErrors(rows: RecentErrorView[]): ErrorGroup[] {
  const byKey = new Map<string, ErrorGroup>();
  for (const row of rows) {
    const key = `${row.status}:${row.error_code ?? ""}`;
    let group = byKey.get(key);
    if (!group) {
      group = { status: row.status, errorCode: row.error_code, count: 0, accountIds: new Set() };
      byKey.set(key, group);
    }
    group.count += 1;
    if (row.account_id) group.accountIds.add(row.account_id);
  }
  return [...byKey.values()].sort((a, b) => b.count - a.count);
}

function RecentErrorsStrip({ errors }: { errors: RecentErrorView[] }) {
  if (errors.length === 0) {
    return (
      <Card>
        <div className="flex items-center gap-2 text-[11px] text-fg opacity-60">
          <CheckCircle2 className="h-4 w-4 shrink-0 text-success" strokeWidth={1.75} />
          No errors recorded.
        </div>
      </Card>
    );
  }

  const groups = groupRecentErrors(errors);
  const oldest = errors.reduce((min, r) => Math.min(min, r.requested_at), errors[0].requested_at);

  return (
    <Card>
      <div className="flex flex-wrap items-center gap-3">
        <span className="flex shrink-0 items-center gap-1.5 whitespace-nowrap text-[11px] font-semibold text-error">
          <AlertTriangle className="h-3.5 w-3.5" strokeWidth={1.9} />
          {errors.length} recent {errors.length === 1 ? "error" : "errors"} · since {relTime(oldest)}
        </span>
        {groups.map((g) => (
          <span
            key={`${g.status}:${g.errorCode ?? ""}`}
            className="whitespace-nowrap rounded bg-muted px-2 py-0.5 text-[10.5px] text-fg opacity-80"
          >
            <b className={clsx("font-bold", g.status >= 500 ? "text-error" : "text-warn")}>
              {g.status}
            </b>{" "}
            {g.errorCode ?? "error"} ×{g.count}
            {g.accountIds.size > 0 &&
              ` · ${g.accountIds.size === 1 ? [...g.accountIds][0] : `${g.accountIds.size} accounts`}`}
          </span>
        ))}
        <Link
          to="/requests"
          className="ml-auto shrink-0 whitespace-nowrap text-[10.5px] font-medium text-accent no-underline hover:underline"
        >
          View all in Requests
          <ChevronRight className="ml-0.5 inline h-3 w-3" strokeWidth={2} />
        </Link>
      </div>
    </Card>
  );
}

/** Loading placeholder — mirrors the real layout's grid spans exactly (4×span3, 5+3+4, 8+4,
 * span12) so data arriving doesn't reflow the page. */
function OverviewSkeleton() {
  return (
    <div className="flex flex-col gap-3">
      <div className="flex items-center justify-between gap-3">
        <div>
          <div className="h-[22px] w-24 animate-pulse rounded bg-muted" />
          <div className="mt-1.5 h-3 w-72 animate-pulse rounded bg-muted" />
        </div>
        <div className="h-7 w-40 animate-pulse rounded bg-muted" />
      </div>
      <Grid>
        {[0, 1, 2, 3].map((i) => (
          <Col key={i} span={3}>
            <Card>
              <div className="h-[74px] animate-pulse rounded bg-muted" />
            </Card>
          </Col>
        ))}
        <Col span={5}>
          <Card>
            <div className="h-40 animate-pulse rounded bg-muted" />
          </Card>
        </Col>
        <Col span={3}>
          <Card>
            <div className="h-40 animate-pulse rounded bg-muted" />
          </Card>
        </Col>
        <Col span={4}>
          <Card>
            <div className="h-40 animate-pulse rounded bg-muted" />
          </Card>
        </Col>
        <Col span={8}>
          <Card>
            <div className="h-40 animate-pulse rounded bg-muted" />
          </Card>
        </Col>
        <Col span={4}>
          <Card>
            <div className="h-40 animate-pulse rounded bg-muted" />
          </Card>
        </Col>
        <Col span={12}>
          <Card>
            <div className="h-6 animate-pulse rounded bg-muted" />
          </Card>
        </Col>
      </Grid>
    </div>
  );
}
