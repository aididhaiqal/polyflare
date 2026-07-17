// The dashboard's landing page. Built from THREE real endpoints:
//   - `useOverview()`      GET /api/overview         — content-free 24h aggregates (KPIs, per-
//                                                       provider quota headroom, per-pool counts,
//                                                       recent errors).
//   - `useOverviewSeries()` GET /api/overview/series  — 24h of hourly request-volume buckets,
//                                                       zero-filled by the handler (Task 5a).
//   - `useAccounts()`      GET /api/accounts          — the live per-account list (status, usage
//                                                       windows, token health, 24h request count).
//
// Task 5 shipped only the first of these (no time series / no per-account list existed yet) and
// documented three deferred mockup rows in task-5-report.md: the request-volume chart, the
// account-health table, and a weekly-pace forecast. Task 5a added the series endpoint; this task
// (5b) restores all three using ONLY real, derived-from-real-fields data — see task-5b-report.md
// for the field-by-field mapping and the reasoning behind every derived number.
import { useEffect, useState, type ReactNode } from "react";
import { Link } from "react-router-dom";
import clsx from "clsx";

import type { AccountView, RecentErrorView } from "../lib/api";
import { compactNum, latency, pct, relTime } from "../lib/format";
import { useAccounts, useOverview, useOverviewSeries } from "../lib/queries";
import { Card } from "../ui/Card";
import { Col, Grid } from "../ui/Grid";
import { Activity, AlertTriangle, CheckCircle2, ChevronRight, Clock, Coins } from "../ui/icons";
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
          <WeeklyPaceCard
            isLoading={accountsQuery.isLoading}
            isError={accountsQuery.isError}
            error={accountsQuery.error}
            onRetry={() => accountsQuery.refetch()}
            accounts={accounts}
            providerFilter={providerFilter}
            nowMs={nowMs}
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
        <div className="mt-1.5 max-h-64 overflow-y-auto">
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
// Weekly-pace forecast — derived from `GET /api/accounts`'s `weekly` window (`used_percent` +
// `reset_at`). `GET /api/overview`'s own `quota` aggregate does NOT carry `reset_at` (see
// read_api.rs::ProviderQuotaView), so this card intentionally reads `useAccounts()`, not
// `useOverview()`. See task-5b-report.md for the full derivation and its assumptions.
// ---------------------------------------------------------------------------------------------

/** The real weekly window duration (`usage_windows.rs`: "real windows are 300 min (5h) and 10080
 * min (weekly)" — 10080 min = 7 days). Used to turn an absolute `reset_at` into "how far into the
 * current weekly period are we", not an invented constant. */
const WEEKLY_PERIOD_SECS = 7 * 24 * 3600;

/** Below this fraction of the weekly period elapsed (~3.4h), a linear projection to end-of-week is
 * too noisy to report honestly (a tiny denominator blows up the extrapolation) — shown as "not
 * enough data yet" instead of a wild number. */
const PACE_MIN_ELAPSED_FRACTION = 0.02;

interface PaceGroup {
  provider: string;
  usedPercent: number;
  expectedPercent: number;
  /** Linear extrapolation of `usedPercent` to 100% of the period, given how much of the period has
   * elapsed. `null` when too little of the period has elapsed to project responsibly. */
  projectedEow: number | null;
}

function computeWeeklyPace(accounts: AccountView[], nowSecs: number): PaceGroup[] {
  const byProvider = new Map<string, AccountView[]>();
  for (const a of accounts) {
    // A stale window (upstream stopped refreshing it) is last-known, not live — using it to
    // project a live pace would be misleading, so those accounts are excluded from this
    // calculation (they still appear in the Quota card / health table as-is).
    if (!a.weekly || a.weekly.reset_at === null || a.weekly.stale) continue;
    const list = byProvider.get(a.provider) ?? [];
    list.push(a);
    byProvider.set(a.provider, list);
  }

  const groups: PaceGroup[] = [];
  for (const [provider, list] of byProvider) {
    // Worst case (highest used_percent) across the provider's accounts — same convention
    // `overview_handler` uses for the Quota card's per-provider aggregation.
    let worst: AccountView | null = null;
    for (const a of list) {
      if (!worst || a.weekly!.used_percent > worst.weekly!.used_percent) worst = a;
    }
    if (!worst?.weekly || worst.weekly.reset_at === null) continue;

    const resetAt = worst.weekly.reset_at;
    const remainingSecs = resetAt - nowSecs;
    const elapsedSecs = WEEKLY_PERIOD_SECS - remainingSecs;
    const elapsedFraction = Math.max(0, Math.min(1, elapsedSecs / WEEKLY_PERIOD_SECS));
    const usedPercent = worst.weekly.used_percent;
    const projectedEow =
      elapsedFraction >= PACE_MIN_ELAPSED_FRACTION
        ? Math.min(usedPercent / elapsedFraction, 999)
        : null;

    groups.push({
      provider,
      usedPercent,
      expectedPercent: elapsedFraction * 100,
      projectedEow,
    });
  }
  return groups.sort((a, b) => a.provider.localeCompare(b.provider));
}

function WeeklyPaceCard(
  props: AsyncCardState & {
    accounts: AccountView[];
    providerFilter: ProviderFilter;
    nowMs: number;
  },
) {
  const status = AsyncCardStatus({ title: "Weekly pace", state: props });
  if (status) return status;

  const { accounts, providerFilter, nowMs } = props;
  const groups = computeWeeklyPace(accounts, Math.floor(nowMs / 1000)).filter((g) =>
    matchesFilter(g.provider, providerFilter),
  );

  return (
    <Card>
      <div className="text-[10px] uppercase tracking-wide text-fg opacity-60">Weekly pace</div>
      {accounts.length === 0 ? (
        <p className="mt-2 text-[11px] text-fg opacity-50">No accounts configured yet.</p>
      ) : groups.length === 0 ? (
        <p className="mt-2 text-[11px] text-fg opacity-50">
          No weekly-quota data available yet for this provider.
        </p>
      ) : (
        <div className="mt-1.5 flex flex-col gap-3">
          {groups.map((g) => (
            <WeeklyPaceRow key={g.provider} group={g} />
          ))}
        </div>
      )}
    </Card>
  );
}

function WeeklyPaceRow({ group }: { group: PaceGroup }) {
  const { provider, usedPercent, expectedPercent, projectedEow } = group;
  const onTrack = projectedEow !== null && projectedEow <= 100;
  const delta = usedPercent - expectedPercent;

  return (
    <div>
      <div className="flex items-center justify-between">
        <ProviderTag provider={provider} />
        {projectedEow !== null && (
          <span
            className={clsx(
              "rounded px-1.5 py-0.5 text-[9px] font-bold",
              onTrack ? "bg-success/15 text-success" : "bg-warn/15 text-warn",
            )}
          >
            {onTrack ? "on track" : "at risk"}
          </span>
        )}
      </div>
      <div className="relative my-2 h-[9px] rounded-full bg-muted">
        <div
          className="h-full rounded-full bg-success"
          style={{ width: `${Math.max(0, Math.min(100, usedPercent))}%` }}
        />
        <div
          className="absolute -top-[3px] -bottom-[3px] w-[2px] rounded-sm bg-fg"
          style={{ left: `${Math.max(0, Math.min(100, expectedPercent))}%` }}
        />
      </div>
      <div className="flex justify-between text-[10px]">
        <span className="text-fg opacity-60">Used</span>
        <span className="font-semibold text-fg">{pct(usedPercent)}</span>
      </div>
      <div className="flex justify-between text-[10px]">
        <span className="text-fg opacity-60">Expected by now</span>
        <span className="text-fg">
          {pct(expectedPercent)}{" "}
          <span className={delta > 0 ? "text-warn" : "text-success"}>
            {delta > 0 ? "+" : ""}
            {Math.round(delta)}%
          </span>
        </span>
      </div>
      <div className="flex justify-between text-[10px]">
        <span className="text-fg opacity-60">Projected EOW</span>
        <span className="font-semibold text-fg">
          {projectedEow === null ? "—" : pct(projectedEow)}
        </span>
      </div>
      <p className="mt-1.5 text-[9.5px] text-fg opacity-60">
        {projectedEow === null
          ? "Not enough data yet this period to project a pace."
          : onTrack
            ? `At current pace, projects to ${pct(projectedEow)} of the weekly quota by reset.`
            : `At current pace, this provider projects to exceed the weekly quota before reset (${pct(projectedEow)} projected).`}
      </p>
    </div>
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
