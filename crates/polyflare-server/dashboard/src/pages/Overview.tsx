// The dashboard's landing page. Built from seven real endpoint-backed queries:
//   - `useOverview()`      GET /api/overview         — availability, pools, and recent errors.
//   - `useReports()`       GET /api/reports          — time-scoped KPIs and zero-filled trends.
//   - `useAccounts()`      GET /api/accounts          — the live per-account list (status, usage
//                                                       windows, token health, 24h request count).
//   - `usePace()`          GET /api/pace              — the pool-wide weekly credit pace forecast
//                                                       (admin-gated; D16 T6).
//   - `useRequests()`      GET /api/requests          — a bounded, content-free recent-routing
//                                                       ledger shared with the Requests page.
//   - `usePools()`         GET /api/pools             — routing strategies and pool health.
//   - `useSettings()`      GET /api/settings          — recovery and retention posture.
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
import { Link, useSearchParams } from "react-router-dom";
import clsx from "clsx";
import {
  Area,
  AreaChart,
  CartesianGrid,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { useAuth } from "../auth/AuthProvider";
import { useCapabilityFlags } from "../capabilities/CapabilitiesProvider";
import {
  routePseudonym,
  ShieldedAccount,
  useScreenShield,
} from "../privacy/ScreenShield";
import type {
  AccountView,
  AdmissionOverviewView,
  CustomProviderView,
  PaceStatus,
  PoolOverviewView,
  PoolView,
  RecentErrorView,
  ReportBreakdownView,
  ReportBucketView,
  ReportsView,
  RequestRowView,
  SettingsView,
  WeeklyCreditPaceReport,
} from "../lib/api";
import { buildAccountHealth, type AccountHealthLevel } from "../lib/accountHealth";
import { accountDisplayLabel } from "../lib/accountDisplay";
import { capacityMapAccounts } from "../lib/capacityMap";
import { buildFleetBalance, fleetBalanceFallbackState } from "../lib/fleetBalance";
import { compactNum, countdown, latency, pct, ratePct, relTime, tpsFmt } from "../lib/format";
import {
  quotaDisplayLabel,
  quotaDisplayPercent,
  quotaWindowIsPresent,
} from "../lib/quotaDisplay";
import {
  requestBucketErrorRate,
  summarizeRequestVolume,
} from "../lib/requestVolume";
import { useQuotaDisplayPreference } from "../preferences/QuotaDisplayPreference";
import {
  requestOutcomeIsFailure,
  requestOutcomeIsSuccess,
  requestOutcomeLabel,
  requestOutcomeSource,
} from "../lib/requestOutcome";
import {
  buildRoutingHeartbeat,
  classifyRoutingAge,
  type RoutingHeartbeatState,
} from "../lib/routingHeartbeat";
import {
  useAccounts,
  useOverview,
  usePace,
  usePools,
  usePatchAccount,
  useProviders,
  useReports,
  useRequests,
  useSettings,
} from "../lib/queries";
import { Card } from "../ui/Card";
import { ActionMenu } from "../ui/ActionMenu";
import { Col, Grid } from "../ui/Grid";
import {
  Activity,
  AlertTriangle,
  ArrowDown,
  BarChart3,
  CheckCircle2,
  ChevronRight,
  Clock,
  Coins,
  List,
  Lock,
  Pause,
  Play,
  RotateCcw,
  Route,
  ShieldCheck,
  Zap,
  type LucideIcon,
} from "../ui/icons";
import type { MetricTrend } from "../ui/MetricCard";
import { providerBrandKey, ProviderTag } from "../ui/ProviderTag";
import { RequestDetailsDialog } from "../ui/RequestDetails";
import { ServiceTierBadge } from "../ui/ServiceTierBadge";
import { StatusPill, statusTone } from "../ui/StatusPill";
import { TransportPill } from "../ui/TransportPill";

type ProviderFilter = string;
type OverviewRange = "24h" | "7d" | "30d";

const BUILT_IN_PROVIDER_FILTERS: Array<{ value: ProviderFilter; label: string }> = [
  { value: "all", label: "All" },
  { value: "codex", label: "Codex" },
  { value: "claude", label: "Claude" },
];

const OVERVIEW_RANGES: Array<{ value: OverviewRange; label: string }> = [
  { value: "24h", label: "24h" },
  { value: "7d", label: "7d" },
  { value: "30d", label: "30d" },
];

const RANGE_LABEL: Record<OverviewRange, string> = {
  "24h": "last 24 hours",
  "7d": "last 7 days",
  "30d": "last 30 days",
};

function matchesFilter(provider: string, filter: ProviderFilter): boolean {
  return filter === "all" || providerBrandKey(provider) === filter;
}

function parseOverviewRange(value: string | null): OverviewRange {
  return value === "24h" || value === "30d" ? value : "7d";
}

function parseProviderFilter(value: string | null): ProviderFilter {
  return value?.trim() || "all";
}

function SegmentedControl<T extends string>({
  items,
  value,
  onChange,
  label,
}: {
  items: Array<{ value: T; label: string }>;
  value: T;
  onChange: (value: T) => void;
  label: string;
}) {
  return (
    <div
      role="group"
      aria-label={label}
      className="flex shrink-0 overflow-hidden rounded-lg border border-border bg-card text-[10.5px] shadow-sm"
    >
      {items.map((item) => (
        <button
          key={item.value}
          type="button"
          aria-pressed={value === item.value}
          onClick={() => onChange(item.value)}
          className={clsx(
            "px-3 py-1.5 transition-colors",
            value === item.value
              ? "bg-accent/[0.12] font-medium text-accent"
              : "text-fg opacity-60 hover:opacity-100",
          )}
        >
          {item.label}
        </button>
      ))}
    </div>
  );
}

interface PeriodMetrics {
  requests: number;
  errors: number;
  duration: number;
  cost: number;
  tokens: number;
}

function summarizeBuckets(buckets: ReportBucketView[]): PeriodMetrics {
  const requests = buckets.reduce((sum, bucket) => sum + bucket.requests, 0);
  return {
    requests,
    errors: buckets.reduce((sum, bucket) => sum + bucket.errors, 0),
    duration:
      requests > 0
        ? buckets.reduce((sum, bucket) => sum + bucket.avg_duration_ms * bucket.requests, 0) /
          requests
        : 0,
    cost: buckets.reduce((sum, bucket) => sum + bucket.cost_usd, 0),
    tokens: buckets.reduce(
      (sum, bucket) => sum + bucket.tokens + bucket.orchestration_tokens,
      0,
    ),
  };
}

function comparisonTrend(
  current: number,
  previous: number,
  increaseIsPositive: boolean,
): MetricTrend | undefined {
  if (!Number.isFinite(current) || !Number.isFinite(previous) || current === previous) return undefined;
  const direction = current > previous ? "up" : "down";
  if (previous === 0) {
    return current > 0 ? { label: "new", direction: "up", positive: increaseIsPositive } : undefined;
  }
  const change = Math.abs(((current - previous) / previous) * 100);
  if (change < 0.5) return undefined;
  return {
    label: `${Math.round(change)}%`,
    direction,
    positive: direction === "up" ? increaseIsPositive : !increaseIsPositive,
  };
}

function buildReportTrends(report: ReportsView): {
  requests?: MetricTrend;
  reliability?: MetricTrend;
  duration?: MetricTrend;
  cost?: MetricTrend;
  tokens?: MetricTrend;
} {
  const split = Math.floor(report.time_series.length / 2);
  if (split === 0) return {};
  const previous = summarizeBuckets(report.time_series.slice(0, split));
  const current = summarizeBuckets(report.time_series.slice(split));
  const previousReliability =
    previous.requests > 0 ? 1 - previous.errors / previous.requests : 0;
  const currentReliability = current.requests > 0 ? 1 - current.errors / current.requests : 0;
  return {
    requests: comparisonTrend(current.requests, previous.requests, true),
    reliability: comparisonTrend(currentReliability, previousReliability, true),
    duration: comparisonTrend(current.duration, previous.duration, false),
    cost: comparisonTrend(current.cost, previous.cost, false),
    tokens: comparisonTrend(current.tokens, previous.tokens, true),
  };
}

export function Overview() {
  const { data, isLoading, isError, error, refetch, dataUpdatedAt } = useOverview();
  const accountsQuery = useAccounts();
  const paceQuery = usePace();
  const poolsQuery = usePools();
  const providersQuery = useProviders();
  const settingsQuery = useSettings();
  const { liveLogs } = useCapabilityFlags();
  const { localAccess, token } = useAuth();
  const [searchParams, setSearchParams] = useSearchParams();
  const providerFilter = parseProviderFilter(searchParams.get("provider"));
  const overviewRange = parseOverviewRange(searchParams.get("range"));
  const providerParam =
    providerFilter === "all"
      ? undefined
      : providerFilter === "claude"
        ? "anthropic"
        : providerFilter;
  const reportsQuery = useReports({
    range: overviewRange,
    dimension: "provider",
    provider: providerParam,
  });
  // Always keep an unfiltered provider breakdown available for the provider pulse. When the
  // overview itself is unfiltered this shares the exact TanStack query key with `reportsQuery`;
  // selecting Codex/Claude adds only the all-provider comparison query.
  const providerPulseQuery = useReports({
    range: overviewRange,
    dimension: "provider",
  });
  const modelDriversQuery = useReports({
    range: overviewRange,
    dimension: "model",
    provider: providerParam,
  });
  const accountDriversQuery = useReports({
    range: overviewRange,
    dimension: "account",
    provider: providerParam,
  });
  const requestsQuery = useRequests({
    limit: 12,
    provider: providerParam,
  });

  const updateOverviewFilter = (key: "range" | "provider", value: string) => {
    setSearchParams(
      (current) => {
        const next = new URLSearchParams(current);
        const isDefault = (key === "range" && value === "7d") || (key === "provider" && value === "all");
        if (isDefault) next.delete(key);
        else next.set(key, value);
        return next;
      },
      { replace: true },
    );
  };

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
  const accounts = accountsQuery.data ?? [];
  const providerFilters = [
    ...BUILT_IN_PROVIDER_FILTERS,
    ...(providersQuery.data ?? [])
      .filter((provider) => provider.enabled)
      .filter(
        (provider) =>
          !BUILT_IN_PROVIDER_FILTERS.some(
            (option) => option.value === providerBrandKey(provider.slug),
          ),
      )
      .map((provider) => ({ value: provider.slug, label: provider.display_name })),
  ];
  const report = reportsQuery.data;
  const hasReportRequests = (report?.totals.requests ?? 0) > 0;
  const reportTrends = report ? buildReportTrends(report) : null;
  const analyticsParams = new URLSearchParams();
  if (overviewRange !== "7d") analyticsParams.set("range", overviewRange);
  if (providerParam) analyticsParams.set("provider", providerParam);
  const analyticsHref = `/reports${analyticsParams.size > 0 ? `?${analyticsParams.toString()}` : ""}`;
  const refreshAll = () => {
    const reportRefreshes =
      providerFilter === "all"
        ? [reportsQuery.refetch()]
        : [reportsQuery.refetch(), providerPulseQuery.refetch()];
    void Promise.all([
      refetch(),
      ...reportRefreshes,
      modelDriversQuery.refetch(),
      accountDriversQuery.refetch(),
      accountsQuery.refetch(),
      paceQuery.refetch(),
      requestsQuery.refetch(),
      poolsQuery.refetch(),
      providersQuery.refetch(),
      settingsQuery.refetch(),
    ]);
  };

  return (
    <div className="flex flex-col gap-5">
      <PageHeader
        subtitle={
          <>
            {RANGE_LABEL[overviewRange]} ·{" "}
            <span className="font-semibold text-success">
              {data.accounts_available} of {totalAccounts} accounts available
            </span>{" "}
            · {data.pools.length} {data.pools.length === 1 ? "pool" : "pools"} · checked{" "}
            {dataUpdatedAt ? relTime(Math.floor(dataUpdatedAt / 1000), nowMs) : "—"}
          </>
        }
        actions={
          <div className="flex w-full flex-wrap items-center justify-start gap-2 sm:w-auto sm:justify-end">
            <SegmentedControl
              items={OVERVIEW_RANGES}
              value={overviewRange}
              onChange={(value) => updateOverviewFilter("range", value)}
              label="Overview timeframe"
            />
            <SegmentedControl
              items={providerFilters}
              value={providerFilter}
              onChange={(value) => updateOverviewFilter("provider", value)}
              label="Provider"
            />
            <button
              type="button"
              onClick={refreshAll}
              aria-label="Refresh overview"
              className="flex h-[30px] w-[30px] items-center justify-center rounded-lg border border-border bg-card text-fg opacity-65 shadow-sm transition-colors hover:border-accent hover:text-accent hover:opacity-100"
            >
              <RotateCcw
                className={clsx(
                  "h-3.5 w-3.5",
                  (reportsQuery.isFetching ||
                    modelDriversQuery.isFetching ||
                    accountDriversQuery.isFetching ||
                    accountsQuery.isFetching ||
                    requestsQuery.isFetching) &&
                    "animate-spin motion-reduce:animate-none",
                )}
                strokeWidth={1.9}
              />
            </button>
          </div>
        }
      />

      <CommandMetricsStrip
        report={report}
        hasReportRequests={hasReportRequests}
        reportTrends={reportTrends}
        range={overviewRange}
      />

      <RoutingHeartbeatStrip
        isLoading={requestsQuery.isLoading}
        isError={requestsQuery.isError}
        rows={requestsQuery.data?.rows ?? []}
        accounts={accounts}
        providerFilter={providerFilter}
        range={overviewRange}
        nowMs={nowMs}
        statusRail={
          <RoutingStatusRail
            pools={poolsQuery.data ?? null}
            poolsError={poolsQuery.isError}
            settings={settingsQuery.data ?? null}
            settingsError={settingsQuery.isError}
            liveLogs={liveLogs}
            localAccess={localAccess}
            hasToken={token !== null}
            availableAccounts={data.accounts_available}
            totalAccounts={totalAccounts}
            admission={data.admission}
          />
        }
      />

      <OperatorDispatchBrief
        isLoading={accountsQuery.isLoading}
        isError={accountsQuery.isError}
        error={accountsQuery.error}
        onRetry={() => accountsQuery.refetch()}
        accounts={accounts}
        customProviders={providersQuery.data ?? []}
        errors={data.recent_errors}
        providerFilter={providerFilter}
        range={overviewRange}
        nowMs={nowMs}
      />

      <section className="flex flex-col gap-3" aria-labelledby="traffic-runway-heading">
        <div className="flex flex-wrap items-end gap-3">
          <div>
            <h2
              id="traffic-runway-heading"
              className="text-[10px] font-bold uppercase tracking-[0.15em] text-fg opacity-55"
            >
              Traffic &amp; runway
            </h2>
            <p className="mt-0.5 text-[9.5px] text-fg opacity-40">
              Demand, weekly account headroom, and the fleet-wide pool forecast
            </p>
          </div>
          <div className="mb-1 h-px min-w-10 flex-1 bg-border" />
          <Link
            to={analyticsHref}
            className="mb-0.5 shrink-0 text-[9.5px] font-semibold text-accent no-underline hover:underline"
          >
            Open analytics
            <ChevronRight className="ml-0.5 inline h-3 w-3" strokeWidth={2} />
          </Link>
        </div>

        <Grid>
          <Col span={7} fill>
            <RequestVolumeCard
              isLoading={reportsQuery.isLoading}
              isError={reportsQuery.isError}
              error={reportsQuery.error}
              onRetry={() => reportsQuery.refetch()}
              buckets={report?.time_series ?? []}
              range={overviewRange}
            />
          </Col>

          <Col span={5} fill>
            <Card className="!block !p-0">
              <div className="grid min-h-full divide-y divide-border lg:grid-cols-2 lg:divide-x lg:divide-y-0">
                <CapacityMapCard
                  embedded
                  isLoading={accountsQuery.isLoading}
                  isError={accountsQuery.isError}
                  error={accountsQuery.error}
                  onRetry={() => accountsQuery.refetch()}
                  accounts={accounts}
                  providerFilter={providerFilter}
                  nowMs={nowMs}
                />
                <PaceCard
                  embedded
                  isLoading={paceQuery.isLoading}
                  isError={paceQuery.isError}
                  error={paceQuery.error}
                  onRetry={() => paceQuery.refetch()}
                  pace={paceQuery.data?.pace ?? null}
                  accounts={accounts}
                  accountsLoading={accountsQuery.isLoading}
                  accountsIsError={accountsQuery.isError}
                  accountsError={accountsQuery.error}
                  onRetryAccounts={() => accountsQuery.refetch()}
                  nowMs={nowMs}
                />
              </div>
            </Card>
          </Col>
        </Grid>
      </section>

      <ProviderPulse
        isLoading={
          providerPulseQuery.isLoading || accountsQuery.isLoading || providersQuery.isLoading
        }
        isError={
          providerPulseQuery.isError || accountsQuery.isError || providersQuery.isError
        }
        error={providerPulseQuery.error ?? accountsQuery.error ?? providersQuery.error}
        onRetry={() =>
          void Promise.all([
            providerPulseQuery.refetch(),
            accountsQuery.refetch(),
            providersQuery.refetch(),
          ])
        }
        breakdown={providerPulseQuery.data?.breakdown ?? []}
        accounts={accounts}
        customProviders={providersQuery.data ?? []}
        range={overviewRange}
      />

      {data.recent_errors.length > 0 && (
        <RecentErrorsStrip
          errors={data.recent_errors}
          accounts={accounts}
          customProviders={providersQuery.data ?? []}
        />
      )}

      <LoadDriversCard
        models={modelDriversQuery.data ?? null}
        modelsLoading={modelDriversQuery.isLoading}
        modelsError={modelDriversQuery.isError ? modelDriversQuery.error : null}
        onRetryModels={() => modelDriversQuery.refetch()}
        accountDrivers={accountDriversQuery.data ?? null}
        accountsLoading={accountDriversQuery.isLoading}
        accountsError={accountDriversQuery.isError ? accountDriversQuery.error : null}
        onRetryAccounts={() => accountDriversQuery.refetch()}
        accounts={accounts}
        customProviders={providersQuery.data ?? []}
        providerFilter={providerFilter}
        range={overviewRange}
      />

      <Grid>
        <Col span={12}>
          <AccountHealthCard
            isLoading={accountsQuery.isLoading}
            isError={accountsQuery.isError}
            error={accountsQuery.error}
            onRetry={() => accountsQuery.refetch()}
            accounts={accounts}
            providerFilter={providerFilter}
            nowMs={nowMs}
          />
        </Col>

        <Col span={12}>
          <PoolsOverviewCard
            pools={data.pools}
            availableAccounts={data.accounts_available}
            totalAccounts={totalAccounts}
          />
        </Col>

        <Col span={12}>
          <RecentRequestsCard
            isLoading={requestsQuery.isLoading}
            isError={requestsQuery.isError}
            error={requestsQuery.error}
            onRetry={() => requestsQuery.refetch()}
            rows={requestsQuery.data?.rows ?? []}
            accounts={accounts}
            providerFilter={providerFilter}
            range={overviewRange}
            nowMs={nowMs}
          />
        </Col>

      </Grid>
    </div>
  );
}

type ReportTrends = ReturnType<typeof buildReportTrends>;

function CommandMetric({
  icon: Icon,
  title,
  value,
  meta,
  trend,
  className,
}: {
  icon: LucideIcon;
  title: string;
  value: string;
  meta: string;
  trend?: MetricTrend;
  className?: string;
}) {
  const positive = trend ? (trend.positive ?? trend.direction === "up") : false;
  return (
    <div className={clsx("min-w-0 bg-card/95 px-3.5 py-3", className)}>
      <div className="flex items-center justify-between gap-2">
        <div className="flex min-w-0 items-center gap-1.5 text-[8.5px] font-bold uppercase tracking-[0.13em] text-fg opacity-50">
          <Icon className="h-3 w-3 shrink-0 text-signal" strokeWidth={1.9} />
          <span className="truncate">{title}</span>
        </div>
        {trend && (
          <span
            title="Compared with the earlier half of the selected range"
            className={clsx(
              "flex shrink-0 items-center gap-0.5 text-[9px] font-semibold",
              positive ? "text-success" : "text-error",
            )}
          >
            <ArrowDown className={clsx("h-2.5 w-2.5", trend.direction === "up" && "rotate-180")} />
            {trend.label}
          </span>
        )}
      </div>
      <div className="mt-1.5 text-[1.35rem] font-semibold leading-none tracking-[-0.035em] tabular-nums text-fg">
        {value}
      </div>
      <p className="mt-1 truncate text-[8.5px] text-fg opacity-45">{meta}</p>
    </div>
  );
}

function CommandMetricsStrip({
  report,
  hasReportRequests,
  reportTrends,
  range,
}: {
  report: ReportsView | undefined;
  hasReportRequests: boolean;
  reportTrends: ReportTrends | null;
  range: OverviewRange;
}) {
  return (
    <section aria-label="Command metrics">
      <Card className="!block !bg-border/70 !p-0">
        <div className="grid grid-cols-2 gap-px overflow-hidden sm:grid-cols-3 xl:grid-cols-5">
          <CommandMetric
            icon={Activity}
            title={`Requests · ${range}`}
            value={report ? compactNum(report.totals.requests) : "—"}
            meta={report ? `${compactNum(report.totals.errors)} errors in range` : "Loading analytics"}
            trend={reportTrends?.requests}
          />
          <CommandMetric
            icon={CheckCircle2}
            title="Reliability"
            value={
              hasReportRequests
                ? ratePct((1 - (report?.totals.error_rate ?? 0)) * 100)
                : "—"
            }
            meta={
              !report
                ? "Loading analytics"
                : hasReportRequests
                  ? `${ratePct(report.totals.error_rate * 100)} error rate`
                  : "No traffic in range"
            }
            trend={reportTrends?.reliability}
          />
          <CommandMetric
            icon={Clock}
            title="Avg duration"
            value={hasReportRequests ? latency(report?.totals.avg_duration_ms) : "—"}
            meta={
              !report
                ? "Loading analytics"
                : hasReportRequests
                  ? `${latency(report.totals.avg_ttft_ms)} avg TTFT`
                  : "No latency samples"
            }
            trend={reportTrends?.duration}
          />
          <CommandMetric
            icon={BarChart3}
            title="Estimated cost"
            value={report ? `$${report.totals.cost_usd.toFixed(2)}` : "—"}
            meta={
              !report
                ? "Loading analytics"
                : hasReportRequests
                  ? `$${(report.totals.cost_usd / report.totals.requests).toFixed(4)} per request`
                  : "No billable traffic"
            }
            trend={reportTrends?.cost}
          />
          <CommandMetric
            icon={Coins}
            title="Tokens"
            value={
              report
                ? compactNum(report.totals.tokens + report.totals.orchestration_tokens)
                : "—"
            }
            meta={
              report
                ? `${compactNum(report.totals.tokens)} model · ${compactNum(report.totals.orchestration_tokens)} orchestration`
                : "Loading analytics"
            }
            trend={reportTrends?.tokens}
            className="col-span-2 sm:col-span-1"
          />
        </div>
      </Card>
    </section>
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
    <div className="flex w-full flex-col items-start justify-between gap-3 sm:flex-row sm:items-end">
      <div className="min-w-0 w-full sm:w-auto">
        <p className="mb-1 text-[9px] font-bold uppercase tracking-[0.2em] text-signal opacity-75">
          Routing observatory
        </p>
        <h1 className="text-[1.7rem] font-semibold leading-none tracking-[-0.04em] text-fg">Overview</h1>
        {subtitle && <p className="mt-2 text-[11px] text-fg opacity-55">{subtitle}</p>}
      </div>
      {actions && <div className="w-full sm:w-auto">{actions}</div>}
    </div>
  );
}

const HEARTBEAT_TONE: Record<
  RoutingHeartbeatState,
  { dot: string; text: string; wash: string }
> = {
  live: { dot: "bg-success", text: "text-success", wash: "bg-success/[0.035]" },
  quiet: { dot: "bg-signal", text: "text-signal", wash: "bg-signal/[0.035]" },
  idle: { dot: "bg-warn", text: "text-warn", wash: "bg-warn/[0.035]" },
  historical: { dot: "bg-warn", text: "text-warn", wash: "bg-warn/[0.035]" },
  unobserved: { dot: "bg-fg/30", text: "text-fg", wash: "bg-muted/20" },
};

function RoutingHeartbeatStrip({
  isLoading,
  isError,
  rows,
  accounts,
  providerFilter,
  range,
  nowMs,
  statusRail,
}: {
  isLoading: boolean;
  isError: boolean;
  rows: RequestRowView[];
  accounts: AccountView[];
  providerFilter: ProviderFilter;
  range: OverviewRange;
  nowMs: number;
  statusRail?: ReactNode;
}) {
  const heartbeat = buildRoutingHeartbeat(
    rows.map((row) => ({
      id: row.id,
      requestedAt: row.requested_at,
      accountId: row.account_id,
      provider: row.provider,
      model: row.model,
      transport: row.transport,
      outcomeLabel: requestOutcomeLabel(row),
      failure: requestOutcomeIsFailure(row),
      durationMs: row.duration_ms,
      ttftMs: row.ttft_ms,
      tps: row.tps,
      serviceTier: row.service_tier,
    })),
    Math.floor(nowMs / 1000),
  );
  const tone = HEARTBEAT_TONE[heartbeat.state];
  const explorerParams = new URLSearchParams();
  if (range !== "24h") explorerParams.set("range", range);
  if (providerFilter !== "all") explorerParams.set("provider", providerFilter);
  const explorerQuery = explorerParams.toString();
  const explorerHref = explorerQuery ? `/requests?${explorerQuery}` : "/requests";

  if (isLoading) {
    return (
      <Card className="!block !p-0">
        <div className="flex items-center gap-3 px-4 py-3 text-[10px] text-fg opacity-50">
          <span className="h-2 w-2 animate-pulse rounded-full bg-signal motion-reduce:animate-none" />
          Reading the newest routing observation…
        </div>
        {statusRail && <div className="border-t border-border">{statusRail}</div>}
      </Card>
    );
  }

  if (isError) {
    return (
      <Card className="!block !border-warn/25 !bg-warn/[0.035] !p-0">
        <div className="flex items-center gap-3 px-4 py-3 text-[10px] text-warn">
          <AlertTriangle className="h-3.5 w-3.5" />
          Routing heartbeat unavailable; the request ledger could not be read.
        </div>
        {statusRail && <div className="border-t border-warn/20">{statusRail}</div>}
      </Card>
    );
  }

  const latest = heartbeat.latest;
  const latestAccount = latest?.accountId
    ? accounts.find((account) => account.id === latest.accountId)
    : undefined;
  return (
    <Card className="!block !overflow-hidden !p-0">
      <div className="grid lg:grid-cols-[minmax(240px,0.9fr)_minmax(0,2fr)_auto]">
        <section className={clsx("relative border-b border-border px-4 py-3 lg:border-b-0 lg:border-r", tone.wash)}>
          <div className={clsx("absolute inset-y-0 left-0 w-0.5", tone.dot)} />
          <div className={clsx("flex items-center gap-2 text-[9px] font-bold uppercase tracking-[0.14em]", tone.text)}>
            <span className="relative flex h-2 w-2">
              {heartbeat.state === "live" && (
                <span className={clsx("absolute inline-flex h-full w-full animate-ping rounded-full opacity-45 motion-reduce:animate-none", tone.dot)} />
              )}
              <span className={clsx("relative inline-flex h-2 w-2 rounded-full", tone.dot)} />
            </span>
            Routing heartbeat
          </div>
          <div className={clsx("mt-2 text-[15px] font-semibold tracking-[-0.025em]", tone.text)}>
            {heartbeat.label}
          </div>
          <div className="mt-0.5 text-[10px] font-medium text-fg">
            {latest ? `Last routed ${relTime(latest.requestedAt, nowMs)}` : "Waiting for the first route"}
          </div>
          <p className="mt-1 text-[9.5px] leading-relaxed text-fg opacity-55">{heartbeat.guidance}</p>
        </section>

        <section className="grid grid-cols-2 divide-x divide-y divide-border/60 sm:grid-cols-4 sm:divide-y-0">
          <div className="px-3 py-3">
            <div className="text-[8.5px] font-semibold uppercase tracking-[0.1em] text-fg opacity-50">Latest result</div>
            <div className={clsx("mt-1 text-[11px] font-semibold", latest?.failure ? "text-error" : latest ? "text-success" : "text-fg opacity-45")}>
              {latest?.outcomeLabel ?? "—"}
            </div>
            <div className="mt-0.5 truncate text-[9px] text-fg opacity-50">{latest?.transport?.replace(/_/g, " ") ?? "no transport evidence"}</div>
          </div>
          <div className="px-3 py-3">
            <div className="text-[8.5px] font-semibold uppercase tracking-[0.1em] text-fg opacity-50">Route</div>
            <div className="mt-1 truncate text-[10px] font-semibold text-fg">
              {latest?.accountId ? (
                <ShieldedAccount
                  id={latest.accountId}
                  label={accountDisplayLabel(latestAccount, latest.accountId)}
                />
              ) : "Unassigned"}
            </div>
            <div className="mt-0.5 flex min-w-0 items-center gap-1">
              {latest && <ProviderTag provider={latest.provider} />}
              {latest && <ServiceTierBadge tier={latest.serviceTier} />}
              <span className="truncate text-[9px] text-fg opacity-50">{latest?.model ?? "model unavailable"}</span>
            </div>
          </div>
          <div className="px-3 py-3">
            <div className="text-[8.5px] font-semibold uppercase tracking-[0.1em] text-fg opacity-50">Latest sample</div>
            <div className="mt-1 text-[10px] font-semibold tabular-nums text-fg">{latest ? latency(latest.durationMs) : "—"}</div>
            <div className="mt-0.5 text-[9px] tabular-nums text-fg opacity-50">
              {latest ? `TTFT ${latency(latest.ttftMs)} · ${tpsFmt(latest.tps)}` : "no timing sample"}
            </div>
          </div>
          <div className="px-3 py-3">
            <div className="text-[8.5px] font-semibold uppercase tracking-[0.1em] text-fg opacity-50">Last 5 minutes</div>
            <div className="mt-1 text-[10px] font-semibold tabular-nums text-fg">{heartbeat.windowCount} routes</div>
            <div className={clsx("mt-0.5 text-[9px]", heartbeat.windowFailures > 0 ? "text-error" : "text-fg opacity-50")}>
              {heartbeat.windowFailures} failed · {heartbeat.windowAccounts} accounts
            </div>
          </div>
        </section>

        <Link
          to={explorerHref}
          aria-label="Open request explorer from routing heartbeat"
          className="flex items-center justify-center gap-1 border-t border-border px-4 py-2 text-[9px] font-semibold text-accent no-underline hover:bg-muted/35 lg:border-l lg:border-t-0"
        >
          Inspect
          <ChevronRight className="h-3 w-3" strokeWidth={2} />
        </Link>
      </div>
      {statusRail && <div className="border-t border-border">{statusRail}</div>}
    </Card>
  );
}

type RailTone = "ok" | "warn" | "neutral";

const RAIL_TONE_CLASS: Record<RailTone, string> = {
  ok: "bg-success",
  warn: "bg-warn",
  neutral: "bg-signal",
};

function settingValue(settings: SettingsView | null, key: string): string | null {
  const field = settings?.fields.find((candidate) => candidate.key === key);
  return field?.value ?? field?.default ?? null;
}

function StatusRailItem({
  icon: Icon,
  label,
  value,
  meta,
  tone,
  to,
}: {
  icon: LucideIcon;
  label: string;
  value: string;
  meta: string;
  tone: RailTone;
  to: string;
}) {
  return (
    <Link
      to={to}
      className="group min-w-0 bg-card/95 px-3 py-2.5 text-fg no-underline transition-colors hover:bg-muted/65"
    >
      <div className="flex items-center gap-1.5 text-[9px] font-bold uppercase tracking-[0.1em] text-fg opacity-55">
        <Icon className="h-3 w-3" strokeWidth={1.8} />
        {label}
      </div>
      <div className="mt-1 flex items-center gap-1.5">
        <span className={clsx("h-1.5 w-1.5 shrink-0 rounded-full", RAIL_TONE_CLASS[tone])} />
        <span className="truncate text-[11px] font-semibold group-hover:text-accent">{value}</span>
      </div>
      <div className="mt-0.5 truncate pl-3 text-[9.5px] text-fg opacity-50">{meta}</div>
    </Link>
  );
}

function RoutingStatusRail({
  pools,
  poolsError,
  settings,
  settingsError,
  liveLogs,
  localAccess,
  hasToken,
  availableAccounts,
  totalAccounts,
  admission,
}: {
  pools: PoolView[] | null;
  poolsError: boolean;
  settings: SettingsView | null;
  settingsError: boolean;
  liveLogs: boolean;
  localAccess: boolean;
  hasToken: boolean;
  availableAccounts: number;
  totalAccounts: number;
  admission: AdmissionOverviewView;
}) {
  const strategies = [...new Set((pools ?? []).map((pool) => pool.strategy))];
  const routingValue =
    poolsError
      ? "unavailable"
      : pools === null
        ? "loading"
        : strategies.length === 0
          ? "not configured"
          : strategies.length === 1
            ? strategies[0]
            : "mixed";
  const attempts = settingValue(settings, "max_account_attempts");
  const retentionDays = settingValue(settings, "request_log_retention_days");
  const accessValue = localAccess ? "loopback trusted" : hasToken ? "admin token" : "checking";
  const admissionValue =
    admission.waiters > 0
      ? `${admission.waiters} waiting`
      : admission.timeouts_total > 0
        ? "capacity clear"
        : "no queue";
  const admissionMeta =
    admission.waits_total > 0
      ? `${latency(admission.avg_wait_ms)} avg wait · ${admission.timeouts_total} timed out`
      : admission.in_flight_pressure > 0
        ? `${admission.in_flight_pressure} pressure units · ${admission.calibration_ratio.toFixed(2)}× calibrated`
        : `${attempts ?? "—"} max account attempts`;

  return (
    <div className="bg-border/70">
      <div className="grid grid-cols-2 gap-px overflow-hidden sm:grid-cols-3 xl:grid-cols-5">
        <StatusRailItem
          icon={Activity}
          label="Fleet"
          value={`${availableAccounts}/${totalAccounts} ready`}
          meta={availableAccounts === totalAccounts ? "all accounts available" : `${totalAccounts - availableAccounts} needs attention`}
          tone={availableAccounts === totalAccounts ? "ok" : "warn"}
          to="/accounts"
        />
        <StatusRailItem
          icon={Route}
          label="Routing"
          value={routingValue.replace(/_/g, " ")}
          meta={
            pools
              ? `${pools.length} ${pools.length === 1 ? "routing group" : "routing groups"}`
              : poolsError
                ? "pool state could not load"
                : "reading pool state"
          }
          tone={poolsError || pools === null ? "neutral" : strategies.length > 1 ? "warn" : "ok"}
          to="/pools"
        />
        <StatusRailItem
          icon={Zap}
          label="Admission"
          value={admissionValue}
          meta={admissionMeta}
          tone={admission.waiters > 0 ? "warn" : "ok"}
          to="/settings"
        />
        <StatusRailItem
          icon={List}
          label="Observability"
          value={liveLogs ? "live stream on" : "request ledger"}
          meta={
            settingsError
              ? "retention unavailable"
              : retentionDays === "0"
                ? "history pruning off"
              : retentionDays
                ? `${retentionDays}d request retention`
                : "retention loading"
          }
          tone={liveLogs ? "ok" : "neutral"}
          to={liveLogs ? "/logs" : "/requests"}
        />
        <StatusRailItem
          icon={ShieldCheck}
          label="Access"
          value={accessValue}
          meta={localAccess ? "tokenless on this device" : hasToken ? "bearer-protected dashboard" : "verifying access mode"}
          tone={localAccess || hasToken ? "ok" : "neutral"}
          to="/settings"
        />
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------------------------
// Operator dispatch brief — turns the same local fleet evidence used elsewhere on Overview into
// a ranked first-action queue. This is deliberately not a global provider-status claim: every
// signal comes from this PolyFlare instance's account, quota, token-health, or request outcomes.
// ---------------------------------------------------------------------------------------------

type DispatchTone = "critical" | "warn" | "watch";

interface DispatchIssue {
  key: string;
  accountId?: string;
  score: number;
  title: string;
  meta: string;
  to: string;
  tone: DispatchTone;
}

const DISPATCH_TONE_CLASS: Record<DispatchTone, { dot: string; text: string; border: string }> = {
  critical: { dot: "bg-error", text: "text-error", border: "border-error/25" },
  warn: { dot: "bg-warn", text: "text-warn", border: "border-warn/25" },
  watch: { dot: "bg-signal", text: "text-signal", border: "border-signal/25" },
};

function dispatchAccountIssue(account: AccountView, nowMs: number): DispatchIssue | null {
  const signals: string[] = [];
  let score = 0;

  if (account.token_health.access_state === "expired") {
    signals.push("token expired");
    score = Math.max(score, 100);
  } else if (account.token_health.access_state === "missing") {
    signals.push("token missing");
    score = Math.max(score, 88);
  }

  if (account.status !== "active") {
    signals.push(account.status.replace(/_/g, " "));
    score = Math.max(score, account.status === "paused" ? 72 : 94);
  }

  const weeklyUsed = account.weekly?.used_percent;
  if (weeklyUsed !== undefined && weeklyUsed !== null && weeklyUsed >= 90) {
    signals.push(
      account.weekly?.stale
        ? `weekly reading stale (${pct(100 - weeklyUsed)} left)`
        : `${pct(100 - weeklyUsed)} weekly left`,
    );
    score = Math.max(
      score,
      account.weekly?.stale ? 74 : weeklyUsed >= 100 ? 96 : 84 + weeklyUsed / 10,
    );
  }

  const fiveHourUsed = account.five_hour?.used_percent;
  if (fiveHourUsed !== undefined && fiveHourUsed !== null && fiveHourUsed >= 90) {
    signals.push(
      account.five_hour?.stale
        ? `5-hour reading stale (${pct(100 - fiveHourUsed)} left)`
        : `${pct(100 - fiveHourUsed)} 5-hour left`,
    );
    score = Math.max(
      score,
      account.five_hour?.stale ? 70 : fiveHourUsed >= 100 ? 92 : 78 + fiveHourUsed / 10,
    );
  }

  if (signals.length === 0) return null;

  const resetAt = account.reset_at ?? account.weekly?.reset_at ?? account.five_hour?.reset_at;
  const reset = resetAt ? ` · reset ${countdown(resetAt, nowMs)}` : "";
  return {
    key: `account:${account.id}`,
    accountId: account.id,
    score,
    title: account.alias ?? account.email,
    meta: `${signals.join(" · ")}${reset}`,
    to: `/accounts/${encodeURIComponent(account.id)}`,
    tone: score >= 94 ? "critical" : score >= 80 ? "warn" : "watch",
  };
}

function dispatchRequestHref(range: OverviewRange, providerFilter: ProviderFilter): string {
  const params = new URLSearchParams();
  if (range !== "24h") params.set("range", range);
  if (providerFilter !== "all") params.set("provider", providerFilter);
  params.set("status", "error");
  const query = params.toString();
  return `/requests?${query}`;
}

function dispatchRangeSeconds(range: OverviewRange): number {
  if (range === "24h") return 24 * 60 * 60;
  if (range === "7d") return 7 * 24 * 60 * 60;
  return 30 * 24 * 60 * 60;
}

function dispatchErrorIssue(
  errors: RecentErrorView[],
  range: OverviewRange,
  providerFilter: ProviderFilter,
  nowMs: number,
): DispatchIssue | null {
  if (errors.length === 0) return null;
  const groups = groupRecentErrors(errors);
  const primary = groups.find((group) => group.status >= 500) ?? groups[0];
  const serverFailure = groups.some((group) => group.status >= 500);
  const latestAt = errors.reduce((latest, error) => Math.max(latest, error.requested_at), 0);
  const evidenceState = classifyRoutingAge(
    Math.max(0, Math.floor(nowMs / 1000) - latestAt),
  );
  const historical = evidenceState === "historical";
  const idle = evidenceState === "idle";
  const affected = new Set(
    errors.flatMap((error) => {
      const target = error.account_id ?? error.provider_credential_id;
      return target ? [target] : [];
    }),
  ).size;
  return {
    key: "requests:recent-errors",
    score: historical ? 58 : idle ? 74 : serverFailure ? 98 : 82,
    title: `${errors.length} ${historical ? "historical" : idle ? "earlier" : "recent"} routing ${errors.length === 1 ? "failure" : "failures"}`,
    meta: `${errorGroupStatusLabel(primary.status)} ${primary.errorCode ?? "error"} ×${primary.count}${
      affected > 0 ? ` · ${affected} ${affected === 1 ? "target" : "targets"}` : ""
    } · last seen ${relTime(latestAt, nowMs)}`,
    to: dispatchRequestHref(range, providerFilter),
    tone: historical || idle ? "watch" : serverFailure ? "critical" : "warn",
  };
}

function OperatorDispatchBrief(
  props: AsyncCardState & {
    accounts: AccountView[];
    customProviders: CustomProviderView[];
    errors: RecentErrorView[];
    providerFilter: ProviderFilter;
    range: OverviewRange;
    nowMs: number;
  },
) {
  const status = AsyncCardStatus({ title: "Operator dispatch", state: props });
  if (status) return status;

  const { accounts, customProviders, errors, providerFilter, range, nowMs } = props;
  const filteredAccounts = accounts.filter((account) => matchesFilter(account.provider, providerFilter));
  const filteredCustomProviders = customProviders.filter((provider) =>
    matchesFilter(provider.slug, providerFilter),
  );
  const configuredTargets =
    filteredAccounts.length +
    filteredCustomProviders.reduce(
      (total, provider) =>
        total + provider.credentials.filter((credential) => credential.enabled).length,
      0,
    );
  const scopedErrors =
    providerFilter === "all"
      ? errors
      : errors.filter((error) => matchesFilter(error.provider, providerFilter));
  const rangeStart = Math.floor(nowMs / 1000) - dispatchRangeSeconds(range);
  const rangeErrors = scopedErrors.filter((error) => error.requested_at >= rangeStart);
  const configurationIssue: DispatchIssue | null =
    configuredTargets === 0
      ? {
          key: "accounts:not-configured",
          score: 99,
          title: `No ${providerFilter === "all" ? "routing" : providerFilter} targets configured`,
          meta: "Add an account or provider credential before this scope can accept traffic",
          to: filteredCustomProviders.length > 0 ? "/providers" : "/accounts",
          tone: "critical",
        }
      : null;
  const issues = [
    configurationIssue,
    ...filteredAccounts.map((account) => dispatchAccountIssue(account, nowMs)).filter(Boolean),
    dispatchErrorIssue(rangeErrors, range, providerFilter, nowMs),
  ]
    .filter((issue): issue is DispatchIssue => issue !== null)
    .sort((left, right) => right.score - left.score);
  const visibleIssues = issues.slice(0, 3);
  const eligibleAccounts = filteredAccounts
    .filter(
      (account) =>
        account.status === "active" && account.token_health.access_state === "valid",
    )
    .sort((left, right) => {
      const weeklyDelta =
        (left.weekly && !left.weekly.stale ? left.weekly.used_percent : 101) -
        (right.weekly && !right.weekly.stale ? right.weekly.used_percent : 101);
      if (weeklyDelta !== 0) return weeklyDelta;
      return (
        (left.five_hour && !left.five_hour.stale ? left.five_hour.used_percent : 101) -
        (right.five_hour && !right.five_hour.stale ? right.five_hour.used_percent : 101)
      );
    });
  const reserve = eligibleAccounts[0];
  const criticalCount = issues.filter((issue) => issue.tone === "critical").length;
  const posture =
    filteredAccounts.length === 0
      ? { label: "No route configured", tone: "critical" as const }
      : criticalCount > 0
      ? { label: "Intervene now", tone: "critical" as const }
      : issues.length > 0
        ? issues.every((issue) => issue.score < 70)
          ? { label: "Review history", tone: "watch" as const }
          : { label: "Watch the queue", tone: "warn" as const }
        : { label: "Fleet clear", tone: "watch" as const };
  const postureTone = DISPATCH_TONE_CLASS[posture.tone];

  return (
    <Card className="!block !overflow-hidden !p-0">
      <div className="grid lg:grid-cols-[minmax(0,0.9fr)_minmax(0,1.75fr)_minmax(0,0.9fr)]">
        <section className="relative border-b border-border bg-muted/20 p-4 lg:border-b-0 lg:border-r">
          <div className="flex items-center gap-2 text-[9px] font-bold uppercase tracking-[0.14em] text-fg opacity-55">
            <span className={clsx("h-1.5 w-1.5 rounded-full", postureTone.dot)} />
            Dispatch brief
          </div>
          <div className={clsx("mt-4 text-[21px] font-semibold leading-none tracking-[-0.035em]", postureTone.text)}>
            {posture.label}
          </div>
          <p className="mt-2 max-w-[28rem] text-[10px] leading-relaxed text-fg opacity-50">
            {issues.length === 0
              ? "No blocking account, quota, token, or recent request signal in this scope."
              : `${issues.length} ranked ${issues.length === 1 ? "signal" : "signals"} from local routing evidence.`}
          </p>
          <div className="mt-4 flex items-center gap-2 text-[9px] text-fg opacity-45">
            <span className="rounded border border-border px-1.5 py-0.5 font-mono uppercase">
              {providerFilter === "all" ? "all routes" : providerFilter}
            </span>
            <span>{range} window</span>
          </div>
        </section>

        <section className="border-b border-border p-4 lg:border-b-0 lg:border-r">
          <div className="flex items-center justify-between gap-3">
            <div className="text-[9px] font-bold uppercase tracking-[0.14em] text-fg opacity-55">
              Priority sequence
            </div>
            {issues.length > visibleIssues.length && (
              <span className="text-[9px] text-fg opacity-40">+{issues.length - visibleIssues.length} below</span>
            )}
          </div>
          {visibleIssues.length === 0 ? (
            <div className="mt-3 flex min-h-[82px] items-center gap-3 rounded-lg border border-dashed border-success/25 bg-success/[0.035] px-3 py-3">
              <CheckCircle2 className="h-4 w-4 shrink-0 text-success" strokeWidth={1.9} />
              <div>
                <div className="text-[11px] font-semibold text-fg">No intervention queued</div>
                <div className="mt-0.5 text-[9.5px] text-fg opacity-45">Provider pulse and request evidence remain available below.</div>
              </div>
            </div>
          ) : (
            <div className="mt-2 divide-y divide-border/65">
              {visibleIssues.map((issue, index) => {
                const tone = DISPATCH_TONE_CLASS[issue.tone];
                return (
                  <Link
                    key={issue.key}
                    to={issue.to}
                    className="group grid grid-cols-[24px_minmax(0,1fr)_auto] items-center gap-2 py-2 text-fg no-underline first:pt-1 last:pb-0"
                  >
                    <span className={clsx("flex h-5 w-5 items-center justify-center rounded border bg-card font-mono text-[8.5px] font-bold", tone.border, tone.text)}>
                      {String(index + 1).padStart(2, "0")}
                    </span>
                    <span className="min-w-0">
                      {issue.accountId ? (
                        <ShieldedAccount
                          id={issue.accountId}
                          label={issue.title}
                          className="block truncate text-[10.5px] font-semibold group-hover:text-accent"
                        />
                      ) : (
                        <span className="block truncate text-[10.5px] font-semibold group-hover:text-accent">
                          {issue.title}
                        </span>
                      )}
                      <span className="mt-0.5 block truncate text-[9px] text-fg opacity-45">{issue.meta}</span>
                    </span>
                    <ChevronRight className="h-3.5 w-3.5 text-fg opacity-35 transition-transform group-hover:translate-x-0.5 group-hover:text-accent group-hover:opacity-100" strokeWidth={1.9} />
                  </Link>
                );
              })}
            </div>
          )}
        </section>

        <section className="p-4">
          <div className="text-[8.5px] font-bold uppercase tracking-[0.16em] text-fg opacity-45">
            Best configured reserve
          </div>
          {reserve ? (
            <Link
              to={`/accounts/${encodeURIComponent(reserve.id)}`}
              className="group mt-4 block text-fg no-underline"
            >
              <ShieldedAccount
                id={reserve.id}
                label={reserve.alias ?? reserve.email}
                className="block truncate text-[12px] font-semibold group-hover:text-accent"
              />
              <div className="mt-2 flex items-end justify-between gap-3">
                <div>
                  <div className="text-[22px] font-semibold leading-none tracking-[-0.035em] text-success">
                    {reserve.weekly && !reserve.weekly.stale
                      ? pct(100 - reserve.weekly.used_percent)
                      : "—"}
                  </div>
                  <div className="mt-1 text-[8.5px] uppercase tracking-wide text-fg opacity-40">
                    weekly headroom
                  </div>
                </div>
                <ChevronRight className="mb-1 h-4 w-4 text-fg opacity-30 transition-transform group-hover:translate-x-0.5 group-hover:text-accent group-hover:opacity-100" strokeWidth={1.9} />
              </div>
              <div className="mt-3 h-1.5 overflow-hidden rounded-full bg-muted">
                <div
                  className="h-full rounded-full bg-success"
                  style={{
                    width: `${Math.max(
                      0,
                      Math.min(
                        100,
                        reserve.weekly && !reserve.weekly.stale
                          ? 100 - reserve.weekly.used_percent
                          : 0,
                      ),
                    )}%`,
                  }}
                />
              </div>
              <div className="mt-2 text-[8.5px] leading-relaxed text-fg opacity-35">
                Active status and token are valid. Runtime cooldown can still bench this route.
              </div>
            </Link>
          ) : (
            <div className="mt-4 rounded-lg border border-dashed border-border px-3 py-3 text-[10px] text-fg opacity-50">
              No active account with a valid token is configured in this scope.
            </div>
          )}
        </section>
      </div>
    </Card>
  );
}

/** Shared shape for the three new cards' independent loading/error handling — each of `useAccounts`
 * / `useReports` resolves on its own schedule, so a slow/failed one degrades only its own
 * card rather than blocking the whole page (the page-level `isError` above only covers
 * `useOverview`). */
interface AsyncCardState {
  isLoading: boolean;
  isError: boolean;
  error: unknown;
  onRetry: () => void;
  embedded?: boolean;
}

function OverviewPanelBody({
  embedded,
  children,
}: {
  embedded?: boolean;
  children: ReactNode;
}) {
  return embedded ? (
    <section className="min-w-0 p-4">{children}</section>
  ) : (
    <Card>{children}</Card>
  );
}

function AsyncCardStatus({ title, state }: { title: string; state: AsyncCardState }) {
  if (state.isLoading) {
    return (
      <OverviewPanelBody embedded={state.embedded}>
        <div className="text-[10px] uppercase tracking-wide text-fg opacity-60">{title}</div>
        <div className="mt-2 h-24 animate-pulse rounded bg-muted" />
      </OverviewPanelBody>
    );
  }
  if (state.isError) {
    return (
      <OverviewPanelBody embedded={state.embedded}>
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
      </OverviewPanelBody>
    );
  }
  return null;
}

// ---------------------------------------------------------------------------------------------
// Provider pulse — observed provider outcomes, not a third-party global status assertion.
// ---------------------------------------------------------------------------------------------

type PulseTone = "ok" | "warn" | "error" | "neutral";

const BUILT_IN_PULSE_PROVIDERS: Array<{ key: string; apiKey: string }> = [
  { key: "codex", apiKey: "codex" },
  { key: "claude", apiKey: "anthropic" },
];

const PULSE_TONE_CLASS: Record<PulseTone, string> = {
  ok: "border-success/25 bg-success/[0.055] text-success",
  warn: "border-warn/25 bg-warn/[0.055] text-warn",
  error: "border-error/25 bg-error/[0.055] text-error",
  neutral: "border-border bg-muted/20 text-fg",
};

function providerPulseState(
  metrics: ReportBreakdownView | undefined,
  configured: number,
  ready: number,
): { label: string; tone: PulseTone } {
  if (configured === 0) return { label: "not configured", tone: "neutral" };
  if (ready === 0) return { label: "no target ready", tone: "error" };
  if (!metrics || metrics.requests === 0) return { label: "no observed traffic", tone: "neutral" };
  const errorRate = metrics.errors / metrics.requests;
  if (errorRate >= 0.05) return { label: "degraded outcomes", tone: "error" };
  if (metrics.errors > 0) return { label: "errors observed", tone: "warn" };
  if (ready < configured) return { label: "capacity constrained", tone: "warn" };
  return { label: "clean outcomes", tone: "ok" };
}

function ProviderPulse(
  props: AsyncCardState & {
    breakdown: ReportBreakdownView[];
    accounts: AccountView[];
    customProviders: CustomProviderView[];
    range: OverviewRange;
  },
) {
  const status = AsyncCardStatus({ title: "Provider pulse", state: props });
  if (status) return status;

  return (
    <Card className="!block !p-0">
      <div className="flex flex-wrap items-center justify-between gap-2 border-b border-border px-3.5 py-2.5">
        <div>
          <div className="text-[9px] font-bold uppercase tracking-[0.14em] text-fg opacity-55">
            Provider pulse
          </div>
          <p className="mt-0.5 text-[9.5px] text-fg opacity-40">
            Outcomes observed by this PolyFlare instance · {props.range}
          </p>
        </div>
        <p className="text-[9px] text-fg opacity-40">
          Local evidence, not a global status-page claim
        </p>
      </div>

      <div className="grid divide-y divide-border md:grid-cols-2 md:divide-x md:divide-y-0">
        {[
          ...BUILT_IN_PULSE_PROVIDERS,
          ...props.customProviders
            .filter(
              (provider) =>
                !BUILT_IN_PULSE_PROVIDERS.some((builtIn) => builtIn.apiKey === provider.slug),
            )
            .map((provider) => ({ key: provider.slug, apiKey: provider.slug })),
        ].map((provider) => {
          const providerAccounts = props.accounts.filter(
            (account) => providerBrandKey(account.provider) === provider.key,
          );
          const customProvider = props.customProviders.find(
            (configured) => configured.slug === provider.apiKey,
          );
          const customCredentials = customProvider?.credentials.filter(
            (credential) => credential.enabled,
          );
          const configured = customCredentials?.length ?? providerAccounts.length;
          const ready =
            customCredentials?.filter((credential) => credential.health_status === "healthy")
              .length ??
            providerAccounts.filter((account) => statusTone(account.status) === "ok").length;
          const metrics = props.breakdown.find(
            (row) => providerBrandKey(row.key) === provider.key,
          );
          const pulse = providerPulseState(metrics, configured, ready);
          const hasTraffic = (metrics?.requests ?? 0) > 0;
          const reliability = hasTraffic
            ? (1 - (metrics?.errors ?? 0) / (metrics?.requests ?? 1)) * 100
            : null;

          return (
            <Link
              key={provider.key}
              to={`/requests?range=${props.range}&provider=${provider.key}`}
              className="group grid min-w-0 grid-cols-[minmax(0,1fr)_auto] gap-x-4 gap-y-2 px-3.5 py-3 text-fg no-underline transition-colors hover:bg-muted/35"
            >
              <div className="flex min-w-0 items-center gap-2">
                <ProviderTag provider={provider.apiKey} />
                <span
                  className={clsx(
                    "truncate rounded-full border px-2 py-0.5 text-[9px] font-semibold",
                    PULSE_TONE_CLASS[pulse.tone],
                  )}
                >
                  {pulse.label}
                </span>
              </div>
              <ChevronRight
                className="h-3.5 w-3.5 self-center opacity-35 transition-transform group-hover:translate-x-0.5 group-hover:text-accent group-hover:opacity-100 motion-reduce:transform-none"
                strokeWidth={1.8}
              />

              <div className="col-span-2 grid grid-cols-3 gap-3">
                <div>
                  <div className="text-[8.5px] uppercase tracking-wide opacity-40">Traffic</div>
                  <div className="mt-0.5 text-[12px] font-semibold tabular-nums">
                    {hasTraffic ? compactNum(metrics?.requests ?? 0) : "—"}
                  </div>
                  <div className="text-[8.5px] opacity-40">requests</div>
                </div>
                <div>
                  <div className="text-[8.5px] uppercase tracking-wide opacity-40">Reliability</div>
                  <div className="mt-0.5 text-[12px] font-semibold tabular-nums">
                    {reliability === null ? "—" : ratePct(reliability)}
                  </div>
                  <div className="text-[8.5px] opacity-40">
                    {hasTraffic ? `${compactNum(metrics?.errors ?? 0)} errors` : "no sample"}
                  </div>
                </div>
                <div>
                  <div className="text-[8.5px] uppercase tracking-wide opacity-40">Readiness</div>
                  <div className="mt-0.5 text-[12px] font-semibold tabular-nums">
                    {ready}/{providerAccounts.length}
                  </div>
                  <div className="text-[8.5px] opacity-40">
                    {hasTraffic ? `${latency(metrics?.avg_duration_ms)} avg` : "accounts"}
                  </div>
                </div>
              </div>
            </Link>
          );
        })}
      </div>
    </Card>
  );
}

// ---------------------------------------------------------------------------------------------
// Cost and load drivers — two views of the same report window, ranked by backend-estimated spend.
// This is intentionally an attribution surface rather than another aggregate chart: it answers
// which model and account are actually carrying the selected provider/timeframe and links that
// evidence directly into the content-free request explorer.
// ---------------------------------------------------------------------------------------------

function driverExplorerHref(
  range: OverviewRange,
  providerFilter: ProviderFilter,
  dimension: "model" | "account",
  value: string,
): string {
  const params = new URLSearchParams();
  if (range !== "24h") params.set("range", range);
  if (providerFilter !== "all") params.set("provider", providerFilter);
  params.set(dimension, value);
  return `/requests?${params.toString()}`;
}

function driverShare(
  row: ReportBreakdownView,
  totalCost: number,
  totalRequests: number,
): { value: number; label: "spend" | "traffic" } {
  if (totalCost > 0) {
    return { value: (row.cost_usd / totalCost) * 100, label: "spend" };
  }
  return {
    value: totalRequests > 0 ? (row.requests / totalRequests) * 100 : 0,
    label: "traffic",
  };
}

function driverCost(cost: number): string {
  if (cost >= 1_000) return `$${trimDriverNumber(cost / 1_000)}k`;
  if (cost >= 10) return `$${cost.toFixed(2)}`;
  if (cost > 0 && cost < 0.01) return "<$0.01";
  return `$${cost.toFixed(2)}`;
}

function rankDriverRows(report: ReportsView): ReportBreakdownView[] {
  return [...report.breakdown].sort((left, right) =>
    report.totals.cost_usd > 0
      ? right.cost_usd - left.cost_usd
      : right.requests - left.requests,
  );
}

function trimDriverNumber(value: number): string {
  return value.toFixed(1).replace(/\.0$/, "");
}

function DriverColumn({
  title,
  emptyLabel,
  rows,
  report,
  accounts,
  customProviders,
  providerFilter,
  range,
  dimension,
}: {
  title: string;
  emptyLabel: string;
  rows: ReportBreakdownView[];
  report: ReportsView;
  accounts: AccountView[];
  customProviders: CustomProviderView[];
  providerFilter: ProviderFilter;
  range: OverviewRange;
  dimension: "model" | "account";
}) {
  const accountById = new Map(accounts.map((account) => [account.id, account]));
  const credentialLabelById = new Map(
    customProviders.flatMap((provider) =>
      provider.credentials.map((credential) => [credential.id, credential.label] as const),
    ),
  );
  const visibleRows = rankDriverRows({ ...report, breakdown: rows }).slice(0, 4);
  const rankLabel = report.totals.cost_usd > 0 ? "estimated spend" : "traffic";

  return (
    <div className="min-w-0 px-3.5 py-3">
      <div className="flex items-center justify-between gap-3">
        <div className="text-[9px] font-bold uppercase tracking-[0.13em] text-fg opacity-50">
          {title}
        </div>
        <div className="text-[8.5px] text-fg opacity-35">ranked by {rankLabel}</div>
      </div>

      {visibleRows.length === 0 ? (
        <p className="mt-3 text-[10.5px] text-fg opacity-45">{emptyLabel}</p>
      ) : (
        <div className="mt-2 divide-y divide-border/55">
          {visibleRows.map((row, index) => {
            const share = driverShare(row, report.totals.cost_usd, report.totals.requests);
            const reliability = row.requests > 0 ? (1 - row.errors / row.requests) * 100 : null;
            const account = dimension === "account" && row.key ? accountById.get(row.key) : undefined;
            const label =
              dimension === "account"
                ? (account?.alias ??
                    account?.email ??
                    credentialLabelById.get(row.key) ??
                    row.key) ||
                  "Unattributed target"
                : row.key || "Unattributed model";
            const href = row.key
              ? driverExplorerHref(range, providerFilter, dimension, row.key)
              : null;
            const content = (
              <>
                <span className="w-5 shrink-0 font-mono text-[8.5px] text-fg opacity-30">
                  {String(index + 1).padStart(2, "0")}
                </span>
                <span className="min-w-0 flex-1">
                  {dimension === "account" && row.key ? (
                    <ShieldedAccount
                      id={row.key}
                      label={label}
                      className="block truncate text-[10.5px] font-semibold text-fg group-hover:text-accent"
                    />
                  ) : (
                    <span className="block truncate text-[10.5px] font-semibold text-fg group-hover:text-accent">
                      {label}
                    </span>
                  )}
                  <span className="mt-0.5 block truncate text-[8.5px] text-fg opacity-40">
                    {compactNum(row.requests)} requests · {reliability === null ? "—" : ratePct(reliability)} reliable
                  </span>
                  <span className="mt-1 block h-1 overflow-hidden rounded-full bg-muted">
                    <span
                      className="block h-full rounded-full bg-signal"
                      style={{
                        width: `${share.value > 0 ? Math.max(2, Math.min(100, share.value)) : 0}%`,
                      }}
                    />
                  </span>
                </span>
                <span className="shrink-0 text-right">
                  <span className="block text-[10.5px] font-semibold tabular-nums text-fg">
                    {driverCost(row.cost_usd)}
                  </span>
                  <span className="block text-[8.5px] tabular-nums text-fg opacity-40">
                    {pct(share.value)} {share.label}
                  </span>
                </span>
                {href && (
                  <ChevronRight
                    className="h-3 w-3 shrink-0 text-fg opacity-25 transition-transform group-hover:translate-x-0.5 group-hover:text-accent group-hover:opacity-100 motion-reduce:transform-none"
                    strokeWidth={1.8}
                  />
                )}
              </>
            );

            return href ? (
              <Link
                key={row.key || `unattributed-${index}`}
                to={href}
                className="group flex min-w-0 items-center gap-2 py-2 text-fg no-underline"
              >
                {content}
              </Link>
            ) : (
              <div
                key={row.key || `unattributed-${index}`}
                className="group flex min-w-0 items-center gap-2 py-2"
              >
                {content}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

function DriverColumnStatus({
  title,
  isLoading,
  error,
  onRetry,
}: {
  title: string;
  isLoading: boolean;
  error: unknown | null;
  onRetry: () => void;
}) {
  return (
    <div className="min-h-44 px-3.5 py-3">
      <div className="text-[9px] font-bold uppercase tracking-[0.13em] text-fg opacity-50">
        {title}
      </div>
      {isLoading ? (
        <div className="mt-2 h-32 animate-pulse rounded-lg bg-muted" />
      ) : (
        <div className="mt-5 flex flex-col items-start gap-2 text-[10.5px] text-error">
          <span className="flex items-center gap-1.5">
            <AlertTriangle className="h-3.5 w-3.5 shrink-0" strokeWidth={1.9} />
            Attribution unavailable
            {error instanceof Error ? `: ${error.message}` : "."}
          </span>
          <button
            type="button"
            onClick={onRetry}
            className="rounded border border-border px-2 py-0.5 text-[10px] text-fg opacity-80 hover:opacity-100"
          >
            Retry {title.toLowerCase()}
          </button>
        </div>
      )}
    </div>
  );
}

function LoadDriversCard(props: {
  models: ReportsView | null;
  modelsLoading: boolean;
  modelsError: unknown | null;
  onRetryModels: () => void;
  accountDrivers: ReportsView | null;
  accountsLoading: boolean;
  accountsError: unknown | null;
  onRetryAccounts: () => void;
  accounts: AccountView[];
  customProviders: CustomProviderView[];
  providerFilter: ProviderFilter;
  range: OverviewRange;
}) {
  const modelsReady = !props.modelsLoading && props.modelsError === null && props.models !== null;
  const accountsReady =
    !props.accountsLoading && props.accountsError === null && props.accountDrivers !== null;

  const topModel = modelsReady && props.models ? rankDriverRows(props.models)[0] : undefined;
  const topAccount =
    accountsReady && props.accountDrivers ? rankDriverRows(props.accountDrivers)[0] : undefined;
  const modelShare = topModel && props.models
    ? driverShare(topModel, props.models.totals.cost_usd, props.models.totals.requests)
    : null;
  const accountShare = topAccount && props.accountDrivers
    ? driverShare(topAccount, props.accountDrivers.totals.cost_usd, props.accountDrivers.totals.requests)
    : null;

  return (
    <Card className="!block !p-0">
      <div className="flex flex-wrap items-center justify-between gap-2 border-b border-border px-3.5 py-2.5">
        <div>
          <div className="text-[9px] font-bold uppercase tracking-[0.14em] text-fg opacity-55">
            Cost and load drivers
          </div>
          <p className="mt-0.5 text-[9.5px] text-fg opacity-40">
            Attribution from this PolyFlare instance · {props.range}
          </p>
        </div>
        <div className="flex flex-wrap gap-x-3 gap-y-1 text-[9px] text-fg opacity-45">
          <span>
            Top model {modelShare ? `${pct(modelShare.value)} ${modelShare.label}` : "—"}
          </span>
          <span>
            Top target {accountShare ? `${pct(accountShare.value)} ${accountShare.label}` : "—"}
          </span>
        </div>
      </div>
      <div className="grid divide-y divide-border md:grid-cols-2 md:divide-x md:divide-y-0">
        {modelsReady && props.models ? (
          <DriverColumn
            title="Model drivers"
            emptyLabel="No model-attributed traffic in this range."
            rows={props.models.breakdown}
            report={props.models}
            accounts={props.accounts}
            customProviders={props.customProviders}
            providerFilter={props.providerFilter}
            range={props.range}
            dimension="model"
          />
        ) : (
          <DriverColumnStatus
            title="Model drivers"
            isLoading={props.modelsLoading}
            error={props.modelsError}
            onRetry={props.onRetryModels}
          />
        )}
        {accountsReady && props.accountDrivers ? (
          <DriverColumn
            title="Target drivers"
            emptyLabel="No target-attributed traffic in this range."
            rows={props.accountDrivers.breakdown}
            report={props.accountDrivers}
            accounts={props.accounts}
            customProviders={props.customProviders}
            providerFilter={props.providerFilter}
            range={props.range}
            dimension="account"
          />
        ) : (
          <DriverColumnStatus
            title="Target drivers"
            isLoading={props.accountsLoading}
            error={props.accountsError}
            onRetry={props.onRetryAccounts}
          />
        )}
      </div>
    </Card>
  );
}

// ---------------------------------------------------------------------------------------------
// Request-volume chart — `GET /api/reports` (24h hourly; 7d/30d daily; zero-filled by handler).
// ---------------------------------------------------------------------------------------------

function requestVolumeTick(ts: number, range: OverviewRange): string {
  const date = new Date(ts * 1000);
  if (range === "24h") {
    return date.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" });
  }
  if (range === "7d") {
    return date.toLocaleDateString(undefined, { weekday: "short" });
  }
  return date.toLocaleDateString(undefined, { month: "short", day: "numeric" });
}

function RequestVolumeTooltip({
  active,
  payload,
  range,
}: {
  active?: boolean;
  payload?: Array<{ payload?: ReportBucketView }>;
  range: OverviewRange;
}) {
  const bucket = payload?.[0]?.payload;
  if (!active || !bucket) return null;
  const errorRate = requestBucketErrorRate(bucket);
  return (
    <div className="pointer-events-none rounded-lg border border-border bg-card px-2.5 py-2 text-[9.5px] text-fg shadow-lg">
      <div className="font-semibold">
        {new Date(bucket.ts * 1000).toLocaleString(undefined, {
          month: "short",
          day: "numeric",
          hour: range === "24h" ? "2-digit" : undefined,
          minute: range === "24h" ? "2-digit" : undefined,
        })}
      </div>
      <div className="mt-1 grid grid-cols-[auto_auto] gap-x-4 gap-y-0.5 tabular-nums">
        <span className="opacity-55">Requests</span>
        <b className="text-right font-semibold">{bucket.requests.toLocaleString()}</b>
        <span className="opacity-55">Errors</span>
        <b className={clsx("text-right font-semibold", bucket.errors > 0 && "text-error")}>
          {bucket.errors.toLocaleString()} · {ratePct(errorRate * 100)}
        </b>
      </div>
    </div>
  );
}

function RequestVolumeCard(
  props: AsyncCardState & { buckets: ReportBucketView[]; range: OverviewRange },
) {
  const status = AsyncCardStatus({ title: `Request volume · ${props.range}`, state: props });
  if (status) return status;

  const { buckets } = props;
  const summary = summarizeRequestVolume(buckets);
  const unit = props.range === "24h" ? "hour" : "day";

  return (
    <Card className="!block overflow-hidden !p-0">
      <div className="flex flex-wrap items-start justify-between gap-2 border-b border-border px-3.5 py-2.5">
        <div>
          <div className="text-[10px] font-bold uppercase tracking-[0.13em] text-fg opacity-65">
            Request volume · {props.range}
          </div>
          <p className="mt-0.5 text-[9.5px] text-fg opacity-40">
            Routed demand by {unit}, with error incidence
          </p>
        </div>
        <div className="flex items-center gap-3 text-[8.5px] text-fg">
          <span className="inline-flex items-center gap-1 opacity-60">
            <i className="h-1.5 w-3 rounded-full bg-accent" />
            requests
          </span>
          <span className="inline-flex items-center gap-1 opacity-60">
            <i className="h-px w-3 border-t border-dashed border-warn" />
            average
          </span>
          <span className="inline-flex items-center gap-1 text-error">
            <i className="h-1.5 w-3 rounded-full bg-error" />
            errors
          </span>
        </div>
      </div>
      {buckets.length === 0 ? (
        <p className="px-3.5 py-4 text-[11px] text-fg opacity-50">No data yet.</p>
      ) : (
        <>
          <div className="grid grid-cols-2 border-b border-border sm:grid-cols-4">
            {[
              [`Latest / ${unit}`, compactNum(summary.latest)],
              [`Average / ${unit}`, compactNum(Math.round(summary.average))],
              ["Peak", compactNum(summary.peak)],
              ["Error rate", ratePct(summary.errorRate * 100)],
            ].map(([label, value], index) => (
              <div
                key={label}
                className={clsx(
                  "px-3.5 py-2",
                  index % 2 !== 0 && "border-l border-border",
                  index >= 2 && "border-t border-border sm:border-t-0",
                  index === 2 && "sm:border-l",
                )}
              >
                <div className="text-[8px] font-semibold uppercase tracking-[0.12em] text-fg opacity-40">
                  {label}
                </div>
                <div
                  className={clsx(
                    "mt-0.5 text-[13px] font-semibold tabular-nums text-fg",
                    label === "Error rate" && summary.errors > 0 && "text-error",
                  )}
                >
                  {value}
                </div>
              </div>
            ))}
          </div>

          <div
            className="px-2.5 pb-2 pt-3"
            role="img"
            aria-label={`${summary.total.toLocaleString()} requests over the ${RANGE_LABEL[props.range]}; peak ${summary.peak.toLocaleString()} per ${unit}; ${summary.errors.toLocaleString()} errors`}
          >
            <div className="h-[168px]">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={buckets} margin={{ top: 8, right: 8, bottom: 0, left: -6 }}>
                  <defs>
                    <linearGradient id="overview-request-volume" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor="hsl(var(--accent))" stopOpacity={0.32} />
                      <stop offset="100%" stopColor="hsl(var(--accent))" stopOpacity={0.01} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid
                    vertical={false}
                    stroke="hsl(var(--border))"
                    strokeDasharray="3 4"
                  />
                  <XAxis
                    dataKey="ts"
                    type="number"
                    domain={["dataMin", "dataMax"]}
                    tickCount={props.range === "7d" ? 7 : 5}
                    minTickGap={24}
                    axisLine={false}
                    tickLine={false}
                    tick={{
                      fontSize: 8.5,
                      fill: "hsl(var(--fg))",
                      fillOpacity: 0.45,
                    }}
                    tickFormatter={(value: number) => requestVolumeTick(value, props.range)}
                  />
                  <YAxis
                    width={42}
                    allowDecimals={false}
                    axisLine={false}
                    tickLine={false}
                    tick={{
                      fontSize: 8.5,
                      fill: "hsl(var(--fg))",
                      fillOpacity: 0.5,
                    }}
                    tickFormatter={(value: number) => compactNum(value)}
                  />
                  {summary.average > 0 && (
                    <ReferenceLine
                      y={summary.average}
                      stroke="hsl(var(--warn))"
                      strokeOpacity={0.7}
                      strokeDasharray="4 4"
                    />
                  )}
                  <Tooltip
                    cursor={{ stroke: "hsl(var(--fg))", strokeOpacity: 0.2 }}
                    content={<RequestVolumeTooltip range={props.range} />}
                  />
                  <Area
                    type="monotone"
                    dataKey="requests"
                    stroke="hsl(var(--accent))"
                    strokeWidth={2}
                    fill="url(#overview-request-volume)"
                    isAnimationActive={false}
                    dot={false}
                    activeDot={{ r: 3, fill: "hsl(var(--accent))", strokeWidth: 0 }}
                  />
                </AreaChart>
              </ResponsiveContainer>
            </div>

            <div className="mt-1 flex items-center gap-2">
              <span className="shrink-0 text-[8px] font-semibold uppercase tracking-[0.1em] text-fg opacity-35">
                Errors
              </span>
              <div
                className="grid h-1.5 min-w-0 flex-1 gap-px overflow-hidden rounded-full bg-muted"
                style={{ gridTemplateColumns: `repeat(${buckets.length}, minmax(0, 1fr))` }}
                aria-label="Error rate by request bucket"
              >
                {buckets.map((bucket) => {
                  const bucketRate = requestBucketErrorRate(bucket);
                  return (
                    <i
                      key={bucket.ts}
                      title={`${requestVolumeTick(bucket.ts, props.range)}: ${bucket.errors.toLocaleString()} errors · ${ratePct(bucketRate * 100)}`}
                      className={bucket.errors > 0 ? "bg-error" : "bg-muted"}
                      style={{ opacity: bucket.errors > 0 ? 0.25 + bucketRate * 0.75 : 0.35 }}
                    />
                  );
                })}
              </div>
              <span className="shrink-0 text-[8.5px] tabular-nums text-fg opacity-45">
                {compactNum(summary.errors)} total
              </span>
            </div>
          </div>

          {summary.total === 0 && (
            <p className="border-t border-border px-3.5 py-2 text-[10px] text-fg opacity-50">
              No requests in the {RANGE_LABEL[props.range]}.
            </p>
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
  const { mode } = useQuotaDisplayPreference();
  if (usedPercent === null) {
    return <span className="text-fg opacity-40">—</span>;
  }
  const clamped = Math.max(0, Math.min(100, usedPercent));
  const displayed = quotaDisplayPercent(clamped, mode);
  return (
    <div
      className="flex items-center gap-1.5"
      title={`${pct(displayed)} ${quotaDisplayLabel(mode)}`}
    >
      <div className="h-[5px] w-[50px] shrink-0 overflow-hidden rounded-full bg-muted">
        <div
          className={clsx("h-full rounded-full", TONE_BAR_CLASS[usageRiskTone(clamped)])}
          style={{ width: `${displayed}%` }}
        />
      </div>
      <span className="text-fg opacity-70">{pct(displayed)}</span>
    </div>
  );
}

function CapacityMapCard(
  props: AsyncCardState & {
    accounts: AccountView[];
    providerFilter: ProviderFilter;
    nowMs: number;
    embedded?: boolean;
  },
) {
  const { active } = useScreenShield();
  const status = AsyncCardStatus({ title: "Capacity map", state: props });
  if (status) return status;

  const { accounts, providerFilter, nowMs } = props;
  const filtered = accounts.filter((account) => matchesFilter(account.provider, providerFilter));
  const tracked = capacityMapAccounts(filtered);
  const remaining = tracked
    .map((account) => Math.max(0, 100 - (account.weekly?.used_percent ?? 0)))
    .sort((a, b) => a - b);
  const medianRemaining =
    remaining.length === 0
      ? null
      : remaining.length % 2 === 1
        ? remaining[Math.floor(remaining.length / 2)]
        : (remaining[remaining.length / 2 - 1] + remaining[remaining.length / 2]) / 2;
  const constrained = tracked.filter((account) => (account.weekly?.used_percent ?? 0) >= 80).length;

  return (
    <OverviewPanelBody embedded={props.embedded}>
      <div className="flex items-start justify-between gap-2">
        <div>
          <div className="text-[10px] uppercase tracking-wide text-fg opacity-60">Capacity map</div>
          <p className="mt-1 text-[9.5px] text-fg opacity-45">Weekly headroom by account</p>
        </div>
        {medianRemaining !== null && (
          <div className="text-right">
            <div
              className={clsx(
                "text-lg font-semibold leading-none",
                constrained > 0 ? "text-warn" : "text-fg",
              )}
            >
              {constrained > 0 ? constrained : pct(medianRemaining)}
            </div>
            <div className="mt-1 text-[8.5px] uppercase tracking-wide text-fg opacity-40">
              {constrained > 0 ? "at risk" : "median left"}
            </div>
          </div>
        )}
      </div>

      {filtered.length === 0 ? (
        <p className="mt-3 text-[11px] text-fg opacity-50">
          {accounts.length === 0 ? "No accounts configured yet." : "No accounts for this provider."}
        </p>
      ) : tracked.length === 0 ? (
        <p className="mt-3 text-[11px] text-fg opacity-50">
          Weekly quota observations have not arrived yet.
        </p>
      ) : (
        <>
          <div className="mt-3 flex h-2 gap-1" aria-label="Weekly capacity distribution">
            {tracked.map((account) => {
              const used = account.weekly?.used_percent ?? 0;
              return (
                <div
                  key={account.id}
                  title={`${active ? routePseudonym(account.id) : account.alias ?? account.email}: ${pct(100 - used)} left`}
                  className={clsx("min-w-1 flex-1 rounded-full", TONE_BAR_CLASS[usageRiskTone(used)])}
                />
              );
            })}
          </div>
          <div className="mt-2 flex items-center justify-between text-[9px] text-fg opacity-50">
            <span>
              {tracked.length} tracked · {pct(medianRemaining)} median left
            </span>
            <span className={constrained > 0 ? "text-warn opacity-100" : undefined}>
              {constrained} constrained
            </span>
          </div>

          <div className="mt-2 flex flex-col border-t border-border pt-1">
            {tracked.map((account) => {
              const used = account.weekly?.used_percent ?? 0;
              return (
                <Link
                  key={account.id}
                  to={`/accounts/${encodeURIComponent(account.id)}`}
                  className="flex items-center gap-2 border-b border-border/55 py-1.5 text-fg no-underline last:border-0 hover:text-accent"
                >
                  <span
                    className={clsx(
                      "h-1.5 w-1.5 shrink-0 rounded-full",
                      TONE_BAR_CLASS[usageRiskTone(used)],
                    )}
                  />
                  <ShieldedAccount
                    id={account.id}
                    label={account.alias ?? account.email}
                    className="min-w-0 flex-1 truncate text-[10px] font-semibold"
                  />
                  <span className="shrink-0 text-right text-[9px]">
                    <b className="font-semibold">{pct(100 - used)} left</b>
                    <span className="ml-1 opacity-45">· {countdown(account.weekly?.reset_at, nowMs)}</span>
                  </span>
                </Link>
              );
            })}
          </div>
        </>
      )}
    </OverviewPanelBody>
  );
}

type AccountPatchMutation = ReturnType<typeof usePatchAccount>;

function AccountQuickActions({
  account,
  patchAccount,
}: {
  account: AccountView;
  patchAccount: AccountPatchMutation;
}) {
  const { active } = useScreenShield();
  return (
    <ActionMenu
      label={`Quick actions for ${active ? routePseudonym(account.id) : account.alias ?? account.email}`}
    >
      <ActionMenu.Item
        icon={account.status === "paused" ? Play : Pause}
        disabled={patchAccount.isPending}
        onSelect={() =>
          patchAccount.mutate({
            id: account.id,
            body: { status: account.status === "paused" ? "active" : "paused" },
          })
        }
      >
        {account.status === "paused" ? "Resume account" : "Pause account"}
      </ActionMenu.Item>
      <ActionMenu.Separator />
      <ActionMenu.Label>Routing policy</ActionMenu.Label>
      {(["normal", "burn_first", "preserve"] as const).map((policy) => (
        <ActionMenu.CheckItem
          key={policy}
          checked={account.routing_policy === policy}
          onSelect={() => patchAccount.mutate({ id: account.id, body: { routing_policy: policy } })}
        >
          {policy.replace(/_/g, " ")}
        </ActionMenu.CheckItem>
      ))}
    </ActionMenu>
  );
}

function AccountHealthCard(
  props: AsyncCardState & {
    accounts: AccountView[];
    providerFilter: ProviderFilter;
    nowMs: number;
  },
) {
  const patchAccount = usePatchAccount();
  const { active } = useScreenShield();
  const status = AsyncCardStatus({ title: "Account health", state: props });
  if (status) return status;

  const { accounts, providerFilter, nowMs } = props;
  const filtered = accounts.filter((account) => matchesFilter(account.provider, providerFilter));
  const byId = new Map(filtered.map((account) => [account.id, account]));
  const health = buildAccountHealth(filtered);
  const focusHealth = health.accounts.find((account) => account.level !== "ready");
  const focus = focusHealth ? byId.get(focusHealth.id) : undefined;
  const roster = health.accounts
    .filter((account) => account.id !== focusHealth?.id)
    .map((account) => ({ account: byId.get(account.id), health: account }))
    .filter((entry): entry is { account: AccountView; health: (typeof health.accounts)[number] } => Boolean(entry.account));

  const summaryItems: Array<{ level: AccountHealthLevel | "observed"; label: string; value: number }> = [
    { level: "action", label: "Action", value: health.summary.action },
    { level: "watch", label: "Watch", value: health.summary.watch },
    { level: "ready", label: "Ready", value: health.summary.ready },
    { level: "observed", label: "Observed", value: health.summary.observed },
  ];

  const levelClasses: Record<AccountHealthLevel, { dot: string; text: string; chip: string }> = {
    action: { dot: "bg-error", text: "text-error", chip: "border-error/25 bg-error/[0.07] text-error" },
    watch: { dot: "bg-warn", text: "text-warn", chip: "border-warn/25 bg-warn/[0.07] text-warn" },
    ready: { dot: "bg-success", text: "text-success", chip: "border-success/25 bg-success/[0.07] text-success" },
  };

  return (
    <Card className="!block !overflow-hidden !p-0">
      <div className="flex flex-col gap-3 border-b border-border px-4 py-3 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <div className="text-[10px] font-semibold uppercase tracking-[0.12em] text-fg opacity-60">Account health</div>
          <div className="mt-0.5 text-[9.5px] text-fg opacity-40">Exceptions first · local route evidence</div>
        </div>
        {filtered.length > 0 && (
          <div className="grid grid-cols-4 overflow-hidden rounded-lg border border-border bg-muted/15">
            {summaryItems.map((item) => (
              <div key={item.level} className="min-w-[58px] border-r border-border px-2.5 py-1.5 text-center last:border-r-0">
                <div className={clsx("text-[13px] font-semibold tabular-nums", item.level === "observed" ? "text-fg" : levelClasses[item.level].text)}>
                  {item.value}
                </div>
                <div className="text-[7.5px] font-semibold uppercase tracking-[0.12em] text-fg opacity-40">{item.label}</div>
              </div>
            ))}
          </div>
        )}
      </div>
      {accounts.length === 0 ? (
        <p className="px-4 py-5 text-[11px] text-fg opacity-50">No accounts configured yet.</p>
      ) : filtered.length === 0 ? (
        <p className="px-4 py-5 text-[11px] text-fg opacity-50">No accounts for this provider.</p>
      ) : (
        <div>
          {focus && focusHealth ? (
            <section className={clsx("relative border-b border-border px-4 py-4", focusHealth.level === "action" ? "bg-error/[0.035]" : "bg-warn/[0.035]")}>
              <div className={clsx("absolute inset-y-0 left-0 w-1", levelClasses[focusHealth.level].dot)} />
              <div className="grid gap-4 lg:grid-cols-[minmax(0,1.2fr)_minmax(240px,0.8fr)] lg:items-center">
                <div className="min-w-0">
                  <div className={clsx("flex items-center gap-2 text-[8.5px] font-bold uppercase tracking-[0.15em]", levelClasses[focusHealth.level].text)}>
                    <span className={clsx("h-1.5 w-1.5 rounded-full", levelClasses[focusHealth.level].dot)} />
                    Exception focus · {focusHealth.level === "action" ? "action required" : "watch closely"}
                  </div>
                  <div className="mt-2 flex min-w-0 flex-wrap items-center gap-2">
                    <ShieldedAccount id={focus.id} label={focus.alias ?? focus.email} className="truncate text-[17px] font-semibold tracking-[-0.025em] text-fg" />
                    <ProviderTag provider={focus.provider} />
                    <StatusPill status={focus.status} />
                  </div>
                  <div className="mt-1 text-[9.5px] text-fg opacity-45">
                    {active ? "identity shielded" : focus.alias ? focus.email : focus.id} · {focus.pools.length > 0 ? focus.pools.join(", ") : "unpooled"} · reset {countdown(focus.reset_at ?? focus.weekly?.reset_at, nowMs)}
                  </div>
                  <div className="mt-3 flex flex-wrap gap-1.5" aria-label="Why this route needs attention">
                    {focusHealth.reasons.map((reason) => (
                      <span key={reason.key} className={clsx("rounded-md border px-2 py-1 text-[9px] font-semibold", levelClasses[reason.level].chip)}>
                        {reason.label}
                      </span>
                    ))}
                  </div>
                </div>
                <div className="rounded-xl border border-border bg-card/80 p-3 shadow-sm">
                  <div className="text-[8px] font-semibold uppercase tracking-[0.14em] text-fg opacity-40">Next move</div>
                  <div className="mt-1.5 text-[12px] font-semibold leading-snug text-fg">{focusHealth.nextActionLabel}</div>
                  <div className="mt-2 grid grid-cols-2 gap-3 border-t border-border/60 pt-2">
                    <div><div className="text-[8px] text-fg opacity-40">Weekly</div><div className="mt-1"><UsageMiniBar usedPercent={focus.weekly?.used_percent ?? null} /></div></div>
                    <div><div className="text-[8px] text-fg opacity-40">Activity · 24h</div><div className="mt-1 text-[11px] font-semibold tabular-nums text-fg">{compactNum(focus.request_count_24h)} requests</div></div>
                  </div>
                  <div className="mt-3 flex items-center justify-between gap-2">
                    <Link to={`/accounts/${encodeURIComponent(focus.id)}`} className="inline-flex items-center rounded-md bg-accent px-2.5 py-1.5 text-[9.5px] font-semibold text-white no-underline hover:opacity-90">
                      Inspect route <ChevronRight className="ml-0.5 h-3 w-3" strokeWidth={2} />
                    </Link>
                    <AccountQuickActions account={focus} patchAccount={patchAccount} />
                  </div>
                </div>
              </div>
            </section>
          ) : (
            <section className="flex items-center gap-3 border-b border-border bg-success/[0.035] px-4 py-3">
              <CheckCircle2 className="h-4 w-4 text-success" />
              <div><div className="text-[11px] font-semibold text-fg">Fleet clear</div><div className="text-[9px] text-fg opacity-45">No route needs intervention in this scope.</div></div>
            </section>
          )}

          {roster.length > 0 && (
            <section className="px-4 py-3">
              <div className="mb-1.5 flex items-center justify-between text-[8px] font-semibold uppercase tracking-[0.13em] text-fg opacity-40">
                <span>Fleet roster</span><span>{roster.length} remaining</span>
              </div>
              <div className="divide-y divide-border/60">
                {roster.map(({ account, health: item }) => (
                  <div key={account.id} className="grid gap-2 py-2.5 sm:grid-cols-[minmax(0,1.2fr)_minmax(170px,0.8fr)_auto] sm:items-center">
                    <div className="flex min-w-0 items-start gap-2">
                      <span className={clsx("mt-1.5 h-1.5 w-1.5 shrink-0 rounded-full", levelClasses[item.level].dot)} />
                      <div className="min-w-0">
                        <div className="flex min-w-0 items-center gap-1.5">
                          <ShieldedAccount id={account.id} label={account.alias ?? account.email} className="truncate text-[10.5px] font-semibold text-fg" />
                          <ProviderTag provider={account.provider} />
                        </div>
                        <div className="mt-0.5 truncate text-[8.5px] text-fg opacity-40">
                          {item.reasons[0]?.label ?? "No intervention needed"} · {account.pools.length > 0 ? account.pools.join(", ") : "unpooled"}
                        </div>
                      </div>
                    </div>
                    <div className="grid grid-cols-[42px_1fr] items-center gap-x-1 gap-y-0.5 pl-3 sm:pl-0">
                      {quotaWindowIsPresent(account.five_hour) && (
                        <>
                          <span className="text-[8px] text-fg opacity-40">5-hour</span>
                          <UsageMiniBar usedPercent={account.five_hour.used_percent} />
                        </>
                      )}
                      <span className="text-[8px] text-fg opacity-40">Weekly</span><UsageMiniBar usedPercent={account.weekly?.used_percent ?? null} />
                    </div>
                    <div className="flex items-center justify-between gap-2 pl-3 sm:justify-end sm:pl-0">
                      <span className="text-[8.5px] text-fg opacity-45"><b className="font-semibold text-fg opacity-100">{compactNum(account.request_count_24h)}</b> · 24h</span>
                      <Link to={`/accounts/${encodeURIComponent(account.id)}`} aria-label={`Inspect ${active ? routePseudonym(account.id) : account.alias ?? account.email}`} className="rounded-md border border-border p-1 text-fg no-underline hover:border-accent hover:text-accent">
                        <ChevronRight className="h-3 w-3" strokeWidth={2} />
                      </Link>
                      <AccountQuickActions account={account} patchAccount={patchAccount} />
                    </div>
                  </div>
                ))}
              </div>
            </section>
          )}
        </div>
      )}
    </Card>
  );
}

function PoolsOverviewCard({
  pools,
  availableAccounts,
  totalAccounts,
}: {
  pools: PoolOverviewView[];
  availableAccounts: number;
  totalAccounts: number;
}) {
  return (
    <Card className="!block !p-0">
      <div className="flex flex-col gap-1 border-b border-border px-4 py-3 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <div className="text-[10px] font-semibold uppercase tracking-[0.12em] text-fg opacity-60">
            Pool posture
          </div>
          <div className="mt-0.5 text-[9.5px] text-fg opacity-40">
            Independent routing groups and their immediately available accounts
          </div>
        </div>
        <div className="flex items-center gap-3">
          <span className="text-[10px] tabular-nums text-fg opacity-55">
            {availableAccounts}/{totalAccounts} fleet ready
          </span>
          <Link
            to="/pools"
            className="inline-flex items-center text-[9.5px] font-semibold text-accent no-underline hover:underline"
          >
            Open pools
            <ChevronRight className="ml-0.5 h-3 w-3" strokeWidth={2} />
          </Link>
        </div>
      </div>
      {pools.length === 0 ? (
        <p className="px-4 py-4 text-[11px] text-fg opacity-50">No pools configured yet.</p>
      ) : (
        <div className="flex flex-wrap gap-px bg-border/60">
          {pools.map((pool) => {
            const label = pool.pool ?? "unpooled";
            const availablePercent = pool.accounts > 0 ? (pool.available / pool.accounts) * 100 : 0;
            return (
              <div key={label} className="min-w-[240px] flex-1 bg-card px-4 py-3">
                <div className="flex items-center justify-between gap-3">
                  <span className="truncate text-[10.5px] font-semibold text-fg">{label}</span>
                  <span className="shrink-0 text-[9.5px] tabular-nums text-fg opacity-55">
                    {pool.available}/{pool.accounts} ready
                  </span>
                </div>
                <div className="mt-2 flex items-center gap-3">
                  <div className="h-1.5 flex-1 overflow-hidden rounded-full bg-muted">
                    <div
                      className="h-full rounded-full bg-accent"
                      style={{ width: `${Math.max(0, Math.min(100, availablePercent))}%` }}
                    />
                  </div>
                  <span className="w-9 shrink-0 text-right text-[10px] tabular-nums text-fg opacity-65">
                    {pct(availablePercent)}
                  </span>
                </div>
              </div>
            );
          })}
        </div>
      )}
      <p className="border-t border-border px-4 py-2 text-[9.5px] text-fg opacity-45">
        Pools route independently — choose each routing strategy in Settings.
      </p>
    </Card>
  );
}

// ---------------------------------------------------------------------------------------------
// Recent request activity — the same content-free GET /api/requests rows as the full Requests
// page, reduced to the operator's first-glance fields. The provider filter is sent to the API and
// checked client-side as a defensive wire-contract guard.
// ---------------------------------------------------------------------------------------------

function requestStatusClass(row: RequestRowView): string {
  if (requestOutcomeIsFailure(row)) return "bg-error/15 text-error";
  if (requestOutcomeIsSuccess(row)) return "bg-success/15 text-success";
  if (row.status >= 300) return "bg-warn/15 text-warn";
  return "bg-muted text-fg";
}

function requestInvestigationHref(
  row: RequestRowView,
  range: OverviewRange,
  providerFilter: ProviderFilter,
): string {
  const params = new URLSearchParams();
  if (range !== "24h") params.set("range", range);
  params.set(
    "provider",
    providerFilter === "all" ? providerBrandKey(row.provider) : providerFilter,
  );
  if (row.account_id) params.set("account", row.account_id);
  if (row.model) params.set("model", row.model);
  return `/requests?${params.toString()}`;
}

function RecentRequestsCard(
  props: AsyncCardState & {
    rows: RequestRowView[];
    accounts: AccountView[];
    providerFilter: ProviderFilter;
    range: OverviewRange;
    nowMs: number;
  },
) {
  const [selectedRequest, setSelectedRequest] = useState<RequestRowView | null>(null);
  const status = AsyncCardStatus({ title: "Recent requests", state: props });
  if (status) return status;

  const { rows, accounts, providerFilter, range, nowMs } = props;
  const accountById = new Map(accounts.map((account) => [account.id, account]));
  const filteredRows = rows.filter((row) => matchesFilter(row.provider, providerFilter));
  const visibleRows = filteredRows.slice(0, 6);
  const latestRow = filteredRows.reduce<RequestRowView | null>(
    (latest, row) => latest === null || row.requested_at > latest.requested_at ? row : latest,
    null,
  );
  const slowestRow = filteredRows.reduce<RequestRowView | null>(
    (slowest, row) => slowest === null || row.duration_ms > slowest.duration_ms ? row : slowest,
    null,
  );
  const failureCount = filteredRows.filter(requestOutcomeIsFailure).length;
  const routedAccounts = new Set(
    filteredRows.flatMap((row) => (row.account_id ? [row.account_id] : [])),
  ).size;
  const explorerParams = new URLSearchParams();
  if (range !== "24h") explorerParams.set("range", range);
  if (providerFilter !== "all") explorerParams.set("provider", providerFilter);
  const explorerQuery = explorerParams.toString();
  const explorerHref = explorerQuery ? `/requests?${explorerQuery}` : "/requests";
  const failureExplorerParams = new URLSearchParams(explorerParams);
  failureExplorerParams.set("status", "error");
  const failureExplorerHref = `/requests?${failureExplorerParams.toString()}`;
  const selectedExplorerHref = selectedRequest
    ? requestInvestigationHref(selectedRequest, range, providerFilter)
    : explorerHref;

  return (
    <Card>
      <div className="flex items-center justify-between gap-3">
        <div>
          <div className="text-[10px] uppercase tracking-wide text-fg opacity-60">
            Recent requests
          </div>
          <p className="mt-1 text-[9.5px] text-fg opacity-45">
            Latest routing outcomes. Prompts and responses are never stored.
          </p>
        </div>
        <Link
          to={explorerHref}
          className="shrink-0 text-[10.5px] font-semibold text-accent no-underline hover:underline"
        >
          Open request explorer
          <ChevronRight className="ml-0.5 inline h-3 w-3" strokeWidth={2} />
        </Link>
      </div>

      {rows.length === 0 ? (
        <div className="mt-3 rounded-lg border border-dashed border-border bg-muted/25 px-4 py-5 text-center">
          <p className="text-[11px] font-semibold text-fg">
            {providerFilter === "all"
              ? "No requests recorded yet"
              : "No recent requests for this provider"}
          </p>
          <p className="mt-1 text-[10px] text-fg opacity-50">
            {providerFilter === "all"
              ? "Activity appears here after PolyFlare routes its first request."
              : "Choose All to see recent activity from other providers."}
          </p>
        </div>
      ) : visibleRows.length === 0 ? (
        <p className="mt-3 text-[11px] text-fg opacity-50">No recent requests for this provider.</p>
      ) : (
        <>
          <div className="mt-3 grid grid-cols-2 overflow-hidden rounded-lg border border-border bg-muted/15 sm:grid-cols-4">
            <RequestEvidence
              label="Latest route"
              value={latestRow ? relTime(latestRow.requested_at, nowMs) : "—"}
              onClick={latestRow ? () => setSelectedRequest(latestRow) : undefined}
              actionLabel={latestRow ? `Inspect latest request ${latestRow.id}` : undefined}
              className="border-b border-r sm:border-b-0"
            />
            <RequestEvidence
              label="Slowest sample"
              value={slowestRow ? latency(slowestRow.duration_ms) : "—"}
              onClick={slowestRow ? () => setSelectedRequest(slowestRow) : undefined}
              actionLabel={slowestRow ? `Inspect slowest request ${slowestRow.id}` : undefined}
              className="border-b sm:border-b-0 sm:border-r"
            />
            <RequestEvidence
              label={`Failures · latest ${filteredRows.length}`}
              value={String(failureCount)}
              tone={failureCount > 0 ? "error" : "success"}
              to={failureExplorerHref}
              actionLabel="Open failed requests"
              className="border-r"
            />
            <RequestEvidence
              label="Routes involved"
              value={`${routedAccounts} ${routedAccounts === 1 ? "route" : "routes"}`}
              to="/accounts"
              actionLabel="Open account fleet"
            />
          </div>
          <div className="mt-2 divide-y divide-border/55 sm:hidden">
            {visibleRows.map((row) => {
              return (
                <article key={row.id} className="py-3 first:pt-1 last:pb-0">
                  <div className="flex min-w-0 items-start justify-between gap-3">
                    <div className="min-w-0">
                      <div className="flex min-w-0 items-center gap-1.5">
                        <ProviderTag provider={row.provider} />
                        <ServiceTierBadge tier={row.service_tier} />
                        <TransportPill transport={row.transport} />
                        <span className="truncate text-[11px] font-semibold text-fg">
                          {row.model ?? row.path}
                        </span>
                      </div>
                      <div className="mt-1 truncate font-mono text-[9px] text-fg opacity-45">
                        {row.method} {row.path}
                      </div>
                    </div>
                    <button
                      type="button"
                      onClick={() => setSelectedRequest(row)}
                      aria-label={`Inspect request ${row.id}`}
                      title={
                        requestOutcomeSource(row) === "imported"
                          ? `Imported codex-lb outcome; HTTP status unavailable${row.error_code ? ` · ${row.error_code}` : ""}`
                          : "Open routing evidence"
                      }
                      className={clsx(
                        "inline-flex min-w-10 shrink-0 items-center justify-center gap-0.5 rounded px-1.5 py-0.5 text-[9.5px] font-bold transition-opacity hover:opacity-80",
                        requestStatusClass(row),
                      )}
                    >
                      {requestOutcomeLabel(row)}
                      <ChevronRight className="h-2.5 w-2.5" strokeWidth={2} />
                    </button>
                  </div>

                  <div className="mt-2 flex min-w-0 items-center justify-between gap-3">
                    <div className="min-w-0">
                      {row.account_id ? (
                        <Link
                          to={`/accounts/${encodeURIComponent(row.account_id)}`}
                          className="block truncate text-[10px] font-semibold text-fg no-underline hover:text-accent"
                        >
                          <ShieldedAccount
                            id={row.account_id}
                            label={accountDisplayLabel(accountById.get(row.account_id), row.account_id)}
                          />
                        </Link>
                      ) : (
                        <span className="block truncate text-[10px] text-fg opacity-50">
                          Unassigned
                        </span>
                      )}
                      <div className="mt-0.5 font-mono text-[9px] text-fg opacity-45">
                        {relTime(row.requested_at, nowMs)}
                      </div>
                    </div>
                    <div className="grid shrink-0 grid-cols-2 gap-x-3 gap-y-1 text-right tabular-nums">
                      <RequestRowMetric label="TTFT" value={latency(row.ttft_ms)} />
                      <RequestRowMetric label="Latency" value={latency(row.duration_ms)} />
                      <RequestRowMetric label="Throughput" value={tpsFmt(row.tps)} />
                      <RequestRowMetric
                        label={
                          row.cached_tokens !== null && row.cached_tokens > 0
                            ? `${compactNum(row.cached_tokens)} cached`
                            : "Tokens"
                        }
                        value={row.total_tokens === null ? "—" : compactNum(row.total_tokens)}
                        positive={row.cached_tokens !== null && row.cached_tokens > 0}
                      />
                    </div>
                  </div>
                </article>
              );
            })}
          </div>
          <div className="mt-2 hidden overflow-x-auto sm:block">
            <table className="w-full min-w-[840px] border-collapse text-[10.5px]">
            <thead>
              <tr className="border-b border-border">
                <th className="px-2 py-1.5 text-left text-[9px] font-medium uppercase tracking-wide text-fg opacity-60">
                  When
                </th>
                <th className="px-2 py-1.5 text-left text-[9px] font-medium uppercase tracking-wide text-fg opacity-60">
                  Account
                </th>
                <th className="px-2 py-1.5 text-left text-[9px] font-medium uppercase tracking-wide text-fg opacity-60">
                  Route
                </th>
                <th className="px-2 py-1.5 text-left text-[9px] font-medium uppercase tracking-wide text-fg opacity-60">
                  Status
                </th>
                <th className="px-2 py-1.5 text-left text-[9px] font-medium uppercase tracking-wide text-fg opacity-60">
                  Transport
                </th>
                <th className="px-2 py-1.5 text-right text-[9px] font-medium uppercase tracking-wide text-fg opacity-60">
                  TTFT
                </th>
                <th className="px-2 py-1.5 text-right text-[9px] font-medium uppercase tracking-wide text-fg opacity-60">
                  Latency
                </th>
                <th className="px-2 py-1.5 text-right text-[9px] font-medium uppercase tracking-wide text-fg opacity-60">
                  Throughput
                </th>
                <th className="px-2 py-1.5 text-right text-[9px] font-medium uppercase tracking-wide text-fg opacity-60">
                  Tokens
                </th>
              </tr>
            </thead>
            <tbody>
              {visibleRows.map((row) => {
                return (
                  <tr key={row.id} className="border-b border-border/55 last:border-0 hover:bg-muted/35">
                    <td className="whitespace-nowrap px-2 py-2 font-mono text-fg opacity-65">
                      {relTime(row.requested_at, nowMs)}
                    </td>
                    <td className="max-w-[190px] px-2 py-2">
                      {row.account_id ? (
                        <Link
                          to={`/accounts/${encodeURIComponent(row.account_id)}`}
                          className="block truncate font-semibold text-fg no-underline hover:text-accent"
                        >
                          <ShieldedAccount
                            id={row.account_id}
                            label={accountDisplayLabel(accountById.get(row.account_id), row.account_id)}
                          />
                        </Link>
                      ) : (
                        <span className="text-fg opacity-50">Unassigned</span>
                      )}
                    </td>
                    <td className="max-w-[240px] px-2 py-2">
                      <div className="flex items-center gap-1.5">
                        <ProviderTag provider={row.provider} />
                        <ServiceTierBadge tier={row.service_tier} />
                        <span className="truncate font-medium text-fg">{row.model ?? row.path}</span>
                      </div>
                      <div className="mt-0.5 truncate font-mono text-[9px] text-fg opacity-40">
                        {row.method} {row.path}
                      </div>
                    </td>
                    <td className="px-2 py-2">
                      <button
                        type="button"
                        onClick={() => setSelectedRequest(row)}
                        aria-label={`Inspect request ${row.id}`}
                        className={clsx(
                          "inline-flex min-w-10 items-center justify-center gap-0.5 rounded px-1.5 py-0.5 text-[9.5px] font-bold transition-opacity hover:opacity-80",
                          requestStatusClass(row),
                        )}
                        title={
                          requestOutcomeSource(row) === "imported"
                            ? `Imported codex-lb outcome; HTTP status unavailable${row.error_code ? ` · ${row.error_code}` : ""}`
                            : "Open routing evidence"
                        }
                      >
                        {requestOutcomeLabel(row)}
                        <ChevronRight className="h-2.5 w-2.5" strokeWidth={2} />
                      </button>
                    </td>
                    <td className="px-2 py-2">
                      <TransportPill transport={row.transport} />
                    </td>
                    <td className="whitespace-nowrap px-2 py-2 text-right tabular-nums text-fg opacity-75">
                      {latency(row.ttft_ms)}
                    </td>
                    <td className="whitespace-nowrap px-2 py-2 text-right tabular-nums text-fg opacity-75">
                      {latency(row.duration_ms)}
                    </td>
                    <td className="whitespace-nowrap px-2 py-2 text-right tabular-nums text-fg opacity-75">
                      {tpsFmt(row.tps)}
                    </td>
                    <td className="whitespace-nowrap px-2 py-2 text-right tabular-nums text-fg">
                      {row.total_tokens === null ? "—" : compactNum(row.total_tokens)}
                      {row.cached_tokens !== null && row.cached_tokens > 0 && (
                        <div className="text-[9px] text-success opacity-75">
                          {compactNum(row.cached_tokens)} cached
                        </div>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
            </table>
          </div>
        </>
      )}
      <RequestDetailsDialog
        row={selectedRequest}
        accountLabel={
          selectedRequest?.account_id
            ? accountDisplayLabel(
                accountById.get(selectedRequest.account_id),
                selectedRequest.account_id,
              )
            : undefined
        }
        explorerHref={selectedExplorerHref}
        onClose={() => setSelectedRequest(null)}
      />
    </Card>
  );
}

function RequestRowMetric({
  label,
  value,
  positive = false,
}: {
  label: string;
  value: string;
  positive?: boolean;
}) {
  return (
    <div className="min-w-0">
      <div className={clsx("text-[10px] font-semibold text-fg", positive && "text-success")}>
        {value}
      </div>
      <div className="max-w-[4.5rem] truncate text-[8px] uppercase tracking-wide text-fg opacity-40">
        {label}
      </div>
    </div>
  );
}

function RequestEvidence({
  label,
  value,
  tone,
  className,
  onClick,
  to,
  actionLabel,
}: {
  label: string;
  value: string;
  tone?: "success" | "error";
  className?: string;
  onClick?: () => void;
  to?: string;
  actionLabel?: string;
}) {
  const content = (
    <>
      <div className="truncate text-[8.5px] font-medium uppercase tracking-wide text-fg opacity-40">
        {label}
      </div>
      <div className="mt-0.5 flex min-w-0 items-center gap-1">
        <span
          className={clsx(
            "truncate text-[11px] font-semibold tabular-nums text-fg",
            tone === "success" && "text-success",
            tone === "error" && "text-error",
          )}
        >
          {value}
        </span>
        {(onClick || to) && (
          <ChevronRight className="h-3 w-3 shrink-0 text-accent opacity-40 transition-opacity group-focus-visible:opacity-100 sm:opacity-0 sm:group-hover:opacity-100" strokeWidth={2} />
        )}
      </div>
    </>
  );

  const classes = clsx(
    "group min-w-0 border-border px-3 py-2 text-left no-underline outline-none transition-colors",
    (onClick || to) && "cursor-pointer hover:bg-muted/40 focus-visible:ring-1 focus-visible:ring-inset focus-visible:ring-accent",
    className,
  );
  if (onClick) {
    return <button type="button" onClick={onClick} aria-label={actionLabel} className={classes}>{content}</button>;
  }
  if (to) {
    return <Link to={to} aria-label={actionLabel} className={classes}>{content}</Link>;
  }
  return <div className={classes}>{content}</div>;
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

function paceRecommendations(
  pace: WeeklyCreditPaceReport,
): Array<{ label: string; value: string }> {
  const recommendations: Array<{ label: string; value: string }> = [];
  if (pace.pause_for_break_even_hours !== null) {
    recommendations.push({
      label: "Pause",
      value: `${formatHoursFromNow(pace.pause_for_break_even_hours)} to break even`,
    });
  }
  if (pace.reduce_by_percent !== null && pace.throttle_to_percent !== null) {
    recommendations.push({
      label: "Throttle",
      value: `reduce ${pct(pace.reduce_by_percent)} · run at ${pct(pace.throttle_to_percent)}`,
    });
  }
  if (
    pace.pro_accounts_to_cover_over_plan !== null &&
    pace.pro_accounts_to_cover_over_plan > 0
  ) {
    recommendations.push({
      label: "Add capacity",
      value: `${pace.pro_accounts_to_cover_over_plan} Pro ${pace.pro_accounts_to_cover_over_plan === 1 ? "account" : "accounts"}`,
    });
  }
  if (
    recommendations.length === 0 &&
    (pace.status === "danger" || pace.projected_shortfall_credits > 0)
  ) {
    recommendations.push({
      label: "Protect capacity",
      value: "hold discretionary traffic until the next reset",
    });
  }
  return recommendations;
}

function PaceCard(
  props: AsyncCardState & {
    pace: WeeklyCreditPaceReport | null;
    accounts: AccountView[];
    accountsLoading: boolean;
    accountsIsError: boolean;
    accountsError: unknown;
    onRetryAccounts: () => void;
    nowMs: number;
    embedded?: boolean;
  },
) {
  const status = AsyncCardStatus({ title: "Weekly pace", state: props });
  if (status) return status;

  const { pace } = props;
  const fallbackState = fleetBalanceFallbackState(
    pace !== null,
    props.accountsLoading,
    props.accountsIsError,
  );
  if (fallbackState === "loading" || fallbackState === "error") {
    return AsyncCardStatus({
      title: "Weekly balance",
      state: {
        isLoading: fallbackState === "loading",
        isError: fallbackState === "error",
        error: props.accountsError,
        onRetry: props.onRetryAccounts,
        embedded: props.embedded,
      },
    });
  }
  const recommendations = pace ? paceRecommendations(pace) : [];
  const paceDelta = pace?.smoothed_delta_percent ?? pace?.delta_percent ?? 0;

  return (
    <OverviewPanelBody embedded={props.embedded}>
      <div className="flex items-center justify-between text-[10px] uppercase tracking-wide text-fg opacity-60">
        <span>Weekly pace</span>
        {pace && <PaceStatusPill status={pace.status} />}
      </div>
      {!pace ? (
        <FleetBalanceFallback accounts={props.accounts} nowMs={props.nowMs} />
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
              <span className={paceDelta > 0 ? "text-warn" : "text-success"}>
                {paceDelta > 0 ? "+" : ""}
                {Math.round(paceDelta)}%
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

          <div
            className={clsx(
              "rounded-lg border px-2.5 py-2 text-[9.5px]",
              recommendations.length > 0
                ? "border-warn/25 bg-warn/[0.06]"
                : "border-success/20 bg-success/[0.05]",
            )}
          >
            <div className="mb-1.5 text-[8.5px] font-bold uppercase tracking-wide text-fg opacity-55">
              Operator guidance
            </div>
            {recommendations.length === 0 ? (
              <div className="font-medium text-success">No intervention needed at current pace.</div>
            ) : (
              <div className="flex flex-col gap-1">
                {recommendations.map((recommendation) => (
                  <div key={recommendation.label} className="flex justify-between gap-3">
                    <span className="text-fg opacity-55">{recommendation.label}</span>
                    <span className="text-right font-semibold text-fg">{recommendation.value}</span>
                  </div>
                ))}
              </div>
            )}
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
    </OverviewPanelBody>
  );
}

function FleetBalanceFallback({ accounts, nowMs }: { accounts: AccountView[]; nowMs: number }) {
  const balance = buildFleetBalance(accounts);
  const byId = new Map(accounts.map((account) => [account.id, account]));
  const coolestAccount = balance.coolest ? byId.get(balance.coolest.id) : undefined;
  const hottestAccount = balance.hottest ? byId.get(balance.hottest.id) : undefined;

  if (!balance.coolest || !balance.hottest || balance.medianUsedPercent === null) {
    return (
      <div className="mt-3 rounded-lg border border-dashed border-border bg-muted/20 px-3 py-3">
        <div className="flex items-center justify-between gap-3">
          <p className="text-[10.5px] font-semibold text-fg">Forecast building</p>
          <span className="rounded bg-muted px-1.5 py-0.5 text-[8.5px] font-bold uppercase tracking-wide text-fg opacity-65">
            snapshot unavailable
          </span>
        </div>
        <p className="mt-1 text-[9.5px] text-fg opacity-50">
          Restore a fresh, active, token-valid weekly route to unlock current load guidance.
        </p>
        <p className="mt-2 border-t border-border pt-1.5 text-[9px] text-fg opacity-45">
          {balance.trackedCount} tracked · {balance.staleCount} stale · 0 routable
        </p>
      </div>
    );
  }

  const toneClass =
    balance.tone === "constrained"
      ? "bg-error/15 text-error"
      : balance.tone === "uneven"
        ? "bg-warn/15 text-warn"
        : "bg-success/15 text-success";
  const guidance =
    balance.action === "hold"
      ? `Hold discretionary traffic; the only routable weekly route is already ${pct(balance.hottest.usedPercent)} used.`
      : balance.action === "protect"
        ? `Keep new work on the coolest route while ${balance.constrainedCount} constrained ${balance.constrainedCount === 1 ? "route recovers" : "routes recover"}.`
        : balance.action === "rebalance"
          ? `Favor the coolest route until the ${Math.round(balance.spreadPoints ?? 0)}-point load spread narrows.`
          : `No rebalance needed; routable lanes are within ${Math.round(balance.spreadPoints ?? 0)} points.`;

  return (
    <div className="mt-2 flex flex-col gap-2">
      <div className="flex items-center justify-between gap-3">
        <div>
          <p className="text-[10.5px] font-semibold text-fg">Current fleet balance</p>
          <p className="mt-0.5 text-[9px] text-fg opacity-45">Forecast history is still building.</p>
        </div>
        <span
          className={clsx(
            "rounded px-1.5 py-0.5 text-[8.5px] font-bold uppercase tracking-wide",
            toneClass,
          )}
        >
          live snapshot
        </span>
      </div>

      <div className="grid grid-cols-3 overflow-hidden rounded-lg border border-border bg-muted/15">
        <BalanceMetric label="Median used" value={pct(balance.medianUsedPercent)} />
        <BalanceMetric label="Load spread" value={`${Math.round(balance.spreadPoints ?? 0)} pts`} />
        <BalanceMetric label="Routable" value={`${balance.eligibleCount}/${balance.trackedCount}`} />
      </div>

      <div className="rounded-lg border border-border bg-bg/35 px-2.5 py-2">
        <div
          className="relative h-1.5 rounded-full bg-muted"
          aria-label="Current weekly load spread"
        >
          <div
            className={clsx(
              "absolute h-full rounded-full",
              balance.tone === "balanced" ? "bg-success/55" : "bg-warn/55",
            )}
            style={{
              left: `${balance.coolest.usedPercent}%`,
              width: `${Math.max(1, balance.hottest.usedPercent - balance.coolest.usedPercent)}%`,
            }}
          />
          <span
            className="absolute top-1/2 h-2.5 w-2.5 -translate-x-1/2 -translate-y-1/2 rounded-full border-2 border-bg bg-success"
            style={{ left: `${balance.coolest.usedPercent}%` }}
          />
          <span
            className="absolute top-1/2 h-2.5 w-2.5 -translate-x-1/2 -translate-y-1/2 rounded-full border-2 border-bg bg-warn"
            style={{ left: `${balance.hottest.usedPercent}%` }}
          />
        </div>
        <div className="mt-2 grid grid-cols-2 gap-3 text-[9px] text-fg">
          <BalanceRoute
            label="Coolest"
            account={coolestAccount}
            id={balance.coolest.id}
            usedPercent={balance.coolest.usedPercent}
          />
          <BalanceRoute
            label="Hottest"
            account={hottestAccount}
            id={balance.hottest.id}
            usedPercent={balance.hottest.usedPercent}
            align="right"
          />
        </div>
      </div>

      <div
        className={clsx(
          "rounded-lg border px-2.5 py-2 text-[9.5px]",
          balance.tone === "balanced"
            ? "border-success/20 bg-success/[0.05]"
            : "border-warn/25 bg-warn/[0.06]",
        )}
      >
        <div className="mb-1 text-[8px] font-bold uppercase tracking-wide text-fg opacity-50">
          Operator guidance
        </div>
        <p className="font-medium text-fg">{guidance}</p>
        {balance.hottest.resetAt !== null && (
          <p className="mt-1 text-[9px] text-fg opacity-45">
            Hottest route resets {countdown(balance.hottest.resetAt, nowMs)}.
          </p>
        )}
      </div>

      <div className="flex items-center gap-1.5 text-[9px] text-fg opacity-45">
        <Lock className="h-2.5 w-2.5 shrink-0" strokeWidth={1.9} />
        Fresh, active, token-valid weekly snapshots only · {balance.staleCount} stale excluded.
      </div>
    </div>
  );
}

function BalanceMetric({ label, value }: { label: string; value: string }) {
  return (
    <div className="border-r border-border px-2 py-1.5 last:border-r-0">
      <div className="text-[8px] uppercase tracking-wide text-fg opacity-40">{label}</div>
      <div className="mt-0.5 text-[11px] font-semibold tabular-nums text-fg">{value}</div>
    </div>
  );
}

function BalanceRoute({
  label,
  account,
  id,
  usedPercent,
  align = "left",
}: {
  label: string;
  account: AccountView | undefined;
  id: string;
  usedPercent: number;
  align?: "left" | "right";
}) {
  return (
    <div className={align === "right" ? "text-right" : undefined}>
      <div className="uppercase tracking-wide opacity-40">{label} · {pct(usedPercent)}</div>
      <Link
        to={`/accounts/${encodeURIComponent(id)}`}
        className="mt-0.5 block truncate font-semibold text-fg no-underline hover:text-accent"
      >
        <ShieldedAccount id={id} label={account?.alias ?? account?.email ?? id} />
      </Link>
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
  provider: string;
  errorCode: string | null;
  count: number;
  targetIds: Set<string>;
}

function groupRecentErrors(rows: RecentErrorView[]): ErrorGroup[] {
  const byKey = new Map<string, ErrorGroup>();
  for (const row of rows) {
    const key = `${row.provider}:${row.status}:${row.error_code ?? ""}`;
    let group = byKey.get(key);
    if (!group) {
      group = {
        status: row.status,
        provider: row.provider,
        errorCode: row.error_code,
        count: 0,
        targetIds: new Set(),
      };
      byKey.set(key, group);
    }
    group.count += 1;
    const targetId = row.account_id ?? row.provider_credential_id;
    if (targetId) group.targetIds.add(targetId);
  }
  return [...byKey.values()].sort((a, b) => b.count - a.count);
}

function errorGroupStatusLabel(status: number): string {
  return status === 0 ? "imported" : String(status);
}

function RecentErrorsStrip({
  errors,
  accounts,
  customProviders,
}: {
  errors: RecentErrorView[];
  accounts: AccountView[];
  customProviders: CustomProviderView[];
}) {
  if (errors.length === 0) return null;

  const groups = groupRecentErrors(errors);
  const accountById = new Map(accounts.map((account) => [account.id, account]));
  const credentialLabelById = new Map(
    customProviders.flatMap((provider) =>
      provider.credentials.map((credential) => [credential.id, credential.label] as const),
    ),
  );
  const oldest = errors.reduce((min, r) => Math.min(min, r.requested_at), errors[0].requested_at);

  return (
    <Card>
      <div className="flex flex-wrap items-center gap-3">
        <span className="flex shrink-0 items-center gap-1.5 whitespace-nowrap text-[11px] font-semibold text-error">
          <AlertTriangle className="h-3.5 w-3.5" strokeWidth={1.9} />
          Fleet-wide · {errors.length} recent {errors.length === 1 ? "error" : "errors"} · since{" "}
          {relTime(oldest)}
        </span>
        {groups.map((g) => (
          <span
            key={`${g.provider}:${g.status}:${g.errorCode ?? ""}`}
            className="whitespace-nowrap rounded bg-muted px-2 py-0.5 text-[10.5px] text-fg opacity-80"
          >
            <ProviderTag provider={g.provider} className="mr-1" />
            <b
              className={clsx(
                "font-bold",
                g.status === 0 || g.status >= 500 ? "text-error" : "text-warn",
              )}
            >
              {errorGroupStatusLabel(g.status)}
            </b>{" "}
            {g.errorCode ?? "error"} ×{g.count}
            {g.targetIds.size > 0 && (
              <>
                {" · "}
                {g.targetIds.size === 1 ? (
                  <ShieldedAccount
                    id={[...g.targetIds][0]}
                    label={
                      credentialLabelById.get([...g.targetIds][0]) ??
                      accountDisplayLabel(
                        accountById.get([...g.targetIds][0]),
                        [...g.targetIds][0],
                      )
                    }
                  />
                ) : (
                  `${g.targetIds.size} targets`
                )}
              </>
            )}
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

/** Loading placeholder — mirrors the real layout's grid spans exactly so data arriving doesn't
 * reflow the page. The incident strip is absent while loading because its healthy state is also
 * intentionally silent. */
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
      <div className="grid grid-cols-2 gap-4 xl:grid-cols-5">
        {[0, 1, 2, 3, 4].map((i) => (
          <Card key={i} className={i === 4 ? "col-span-2 xl:col-span-1" : undefined}>
            <div className="h-[98px] animate-pulse rounded bg-muted" />
          </Card>
        ))}
      </div>
      <Card>
        <div className="h-[92px] animate-pulse rounded bg-muted" />
      </Card>
      <Grid>
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
        <Col span={12}>
          <Card>
            <div className="h-40 animate-pulse rounded bg-muted" />
          </Card>
        </Col>
        <Col span={12}>
          <Card>
            <div className="h-24 animate-pulse rounded bg-muted" />
          </Card>
        </Col>
        <Col span={12}>
          <Card>
            <div className="h-44 animate-pulse rounded bg-muted" />
          </Card>
        </Col>
      </Grid>
    </div>
  );
}
