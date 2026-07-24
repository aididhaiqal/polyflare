// The Reports page: the dashboard's analytics view — `GET /api/reports` (`useReports(params)`) is
// the ONLY endpoint this page consumes. Task 3 shipped the frontend data layer (typed client +
// hook), the nav entry (promoted from Sidebar.tsx's disabled "Analytics" placeholder), the
// `/reports` route, and a control-bar + totals-KPI stub — see task-3-brief.md. Task 4 rendered the
// first real section — Cost — via the reusable `ReportSection` (a trend `AreaChart` over
// `time_series` + a per-dimension breakdown table). Task 5 (this pass) reuses `ReportSection` for
// the Usage (token throughput) and Performance (latency/error) sections, completing the page.
//
// CONTENT-SAFETY: `ReportsView` is sourced from the same content-free `request_log` aggregates the
// rest of the dashboard already exposes (counts, cost, token COUNTS, timing) — never a body,
// prompt, response, or key.
import { useSearchParams } from "react-router-dom";
import clsx from "clsx";
import {
  Area,
  AreaChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  XAxis,
  YAxis,
} from "recharts";

import type { ReportBreakdownView, ReportBucketView, ReportsView } from "../lib/api";
import { compactNum, latency, pct, ratePct } from "../lib/format";
import { useProviders, useReports, type ReportsParams } from "../lib/queries";
import { Card } from "../ui/Card";
import { Col, Grid } from "../ui/Grid";
import { Activity, AlertTriangle, BarChart3, Clock, Coins, Layers, Zap } from "../ui/icons";
import { MetricCard } from "../ui/MetricCard";
import { ReportSection, type ReportSectionColumn } from "../ui/ReportSection";

type RangeKey = "24h" | "7d" | "30d";
const RANGE_OPTIONS: Array<{ value: RangeKey; label: string }> = [
  { value: "24h", label: "24h" },
  { value: "7d", label: "7d" },
  { value: "30d", label: "30d" },
];

type DimensionKey = "account" | "model" | "provider";
const DIMENSION_OPTIONS: Array<{ value: DimensionKey; label: string }> = [
  { value: "account", label: "Target" },
  { value: "model", label: "Model" },
  { value: "provider", label: "Provider" },
];

/** Backend wire values for `provider` are "codex"/"anthropic" (never "claude") — same mapping
 * `Requests.tsx`'s provider filter uses. `ALL` means "omit the param entirely", not a literal
 * `provider=all` sent to the backend. */
const ALL = "all";
const BUILT_IN_PROVIDER_OPTIONS: Array<{ value: string; label: string }> = [
  { value: ALL, label: "all providers" },
  { value: "codex", label: "codex" },
  { value: "anthropic", label: "claude" },
];

const SELECT_CLASS =
  "shrink-0 rounded border border-border bg-card px-2.5 py-1 text-[10.5px] text-fg opacity-80 outline-none hover:opacity-100 focus:opacity-100";

function parseRange(value: string | null): RangeKey {
  return value === "24h" || value === "30d" ? value : "7d";
}

function parseDimension(value: string | null): DimensionKey {
  return value === "account" || value === "provider" ? value : "model";
}

function parseProvider(value: string | null): string {
  if (value === "claude") return "anthropic";
  return value?.trim() || ALL;
}

export function Reports() {
  const [searchParams, setSearchParams] = useSearchParams();
  const range = parseRange(searchParams.get("range"));
  const dimension = parseDimension(searchParams.get("dimension"));
  const provider = parseProvider(searchParams.get("provider"));
  const providersQuery = useProviders();
  const providerOptions = [
    ...BUILT_IN_PROVIDER_OPTIONS,
    ...(providersQuery.data ?? [])
      .filter(
        (configured) =>
          !BUILT_IN_PROVIDER_OPTIONS.some((option) => option.value === configured.slug),
      )
      .map((configured) => ({ value: configured.slug, label: configured.display_name })),
  ];

  function setReportParam(key: "range" | "dimension" | "provider", value: string) {
    const defaults = { range: "7d", dimension: "model", provider: ALL };
    setSearchParams(
      (current) => {
        const next = new URLSearchParams(current);
        if (value === defaults[key]) next.delete(key);
        else next.set(key, value);
        return next;
      },
      { replace: true },
    );
  }

  const params: ReportsParams = {
    range,
    dimension,
    provider: provider !== ALL ? provider : undefined,
  };

  const { data, isLoading, isFetching, isError, error, refetch } = useReports(params);

  // Computed once here (not re-derived per section) — every `ReportSection` below shares this
  // same dimension-column label ("Account"/"Model"/"Provider").
  const dimensionLabel = DIMENSION_OPTIONS.find((o) => o.value === dimension)?.label ?? "Dimension";

  return (
    <div className="flex flex-col gap-3">
      <PageHeader />

      <div className="flex flex-wrap items-center gap-2">
        <div className="flex shrink-0 overflow-hidden rounded border border-border bg-card text-[10.5px]">
          {RANGE_OPTIONS.map((o) => (
            <button
              key={o.value}
              type="button"
              onClick={() => setReportParam("range", o.value)}
              className={clsx(
                "px-2.5 py-1",
                range === o.value
                  ? "bg-accent/[0.12] font-medium text-accent"
                  : "text-fg opacity-60 hover:opacity-100",
              )}
            >
              {o.label}
            </button>
          ))}
        </div>

        <select
          value={dimension}
          onChange={(e) => setReportParam("dimension", e.target.value)}
          className={SELECT_CLASS}
          aria-label="Breakdown dimension"
        >
          {DIMENSION_OPTIONS.map((o) => (
            <option key={o.value} value={o.value}>
              {o.label}
            </option>
          ))}
        </select>

        <select
          value={provider}
          onChange={(e) => setReportParam("provider", e.target.value)}
          className={SELECT_CLASS}
          aria-label="Provider filter"
        >
          {providerOptions.map((o) => (
            <option key={o.value} value={o.value}>
              {o.label}
            </option>
          ))}
        </select>

        {isFetching && !isLoading && (
          <span className="text-[10.5px] text-fg opacity-50">refreshing…</span>
        )}
      </div>

      {isLoading ? (
        <ReportsSkeleton />
      ) : isError ? (
        <Card>
          <div className="flex flex-wrap items-center justify-between gap-3">
            <span className="flex items-center gap-2 text-[12px] text-error">
              <AlertTriangle className="h-4 w-4 shrink-0" strokeWidth={1.9} />
              Couldn&apos;t load reports
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
      ) : !data ? null : data.totals.requests === 0 ? (
        <Card>
          <p className="text-[11px] text-fg opacity-50">
            No requests in this window — totals will appear once PolyFlare serves traffic.
          </p>
        </Card>
      ) : (
        <>
          <CostSection data={data} dimensionLabel={dimensionLabel} />
          <UsageSection data={data} dimensionLabel={dimensionLabel} />
          <PerformanceSection data={data} dimensionLabel={dimensionLabel} />
        </>
      )}
    </div>
  );
}

function PageHeader() {
  return (
    <div>
      <h1 className="text-lg font-semibold text-fg">Reports</h1>
      <p className="mt-0.5 text-[11px] text-fg opacity-60">
        Cost, usage, and performance trends with a per-dimension breakdown over the selected
        window.
      </p>
    </div>
  );
}

// ---------------------------------------------------------------------------------------------
// Cost section — the first `ReportSection` consumer. `dimensionLabel` is passed down from the
// page's own `DIMENSION_OPTIONS` labels ("Account"/"Model"/"Provider") so the breakdown table's
// first column always names the currently-selected dimension, not a hardcoded one.
// ---------------------------------------------------------------------------------------------

const COST_COLUMNS: ReportSectionColumn[] = [
  {
    header: "Cost",
    align: "right",
    render: (row: ReportBreakdownView) => `$${row.cost_usd.toFixed(2)}`,
  },
  {
    header: "Requests",
    align: "right",
    render: (row: ReportBreakdownView) => compactNum(row.requests),
  },
  {
    header: "Tokens",
    align: "right",
    render: (row: ReportBreakdownView) => compactNum(row.tokens),
  },
];

function CostSection({ data, dimensionLabel }: { data: ReportsView; dimensionLabel: string }) {
  return (
    <ReportSection
      title="Cost"
      kpis={
        <>
          <Col span={4}>
            <MetricCard
              icon={Coins}
              title="Total cost"
              value={`$${data.totals.cost_usd.toFixed(2)}`}
            />
          </Col>
          <Col span={4}>
            <MetricCard
              icon={Activity}
              title="Cache-hit rate"
              value={pct(data.totals.cache_hit_rate * 100)}
            />
          </Col>
          <Col span={4}>
            <MetricCard icon={BarChart3} title="Requests" value={compactNum(data.totals.requests)} />
          </Col>
        </>
      }
      chart={
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={data.time_series} margin={{ top: 4, right: 6, bottom: 0, left: -6 }}>
            <defs>
              <linearGradient id="cost-trend" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="hsl(var(--codex))" stopOpacity={0.32} />
                <stop offset="100%" stopColor="hsl(var(--codex))" stopOpacity={0} />
              </linearGradient>
            </defs>
            <CartesianGrid vertical={false} stroke="hsl(var(--border))" strokeDasharray="3 3" />
            <XAxis dataKey="ts" type="number" domain={["dataMin", "dataMax"]} hide />
            <YAxis
              width={44}
              tick={{ fontSize: 8.5, fill: "hsl(var(--fg))", fillOpacity: 0.6 }}
              axisLine={false}
              tickLine={false}
              tickFormatter={(v: number) => `$${v.toFixed(2)}`}
            />
            <Area
              type="monotone"
              dataKey="cost_usd"
              stroke="hsl(var(--codex))"
              strokeWidth={1.7}
              fill="url(#cost-trend)"
              isAnimationActive={false}
              dot={false}
            />
          </AreaChart>
        </ResponsiveContainer>
      }
      breakdown={data.breakdown}
      columns={COST_COLUMNS}
      dimensionLabel={dimensionLabel}
    />
  );
}

// ---------------------------------------------------------------------------------------------
// Usage section — token throughput. Plots `tokens` ALONE: `cached_tokens` is a subset of input
// tokens and `reasoning_tokens` is a subset of output tokens, both already folded into `tokens`,
// so stacking all three would double-count. Cached/requests still appear per-dimension in the
// breakdown table, just not as extra chart series.
// ---------------------------------------------------------------------------------------------

const USAGE_COLUMNS: ReportSectionColumn[] = [
  {
    header: "Tokens",
    align: "right",
    render: (row: ReportBreakdownView) => compactNum(row.tokens),
  },
  {
    header: "Cached",
    align: "right",
    render: (row: ReportBreakdownView) => compactNum(row.cached_tokens),
  },
  {
    header: "Orchestration",
    align: "right",
    render: (row: ReportBreakdownView) => compactNum(row.orchestration_tokens),
  },
  {
    header: "Requests",
    align: "right",
    render: (row: ReportBreakdownView) => compactNum(row.requests),
  },
];

function UsageSection({ data, dimensionLabel }: { data: ReportsView; dimensionLabel: string }) {
  return (
    <ReportSection
      title="Usage"
      kpis={
        <>
          <Col span={3}>
            <MetricCard icon={Layers} title="Total tokens" value={compactNum(data.totals.tokens)} />
          </Col>
          <Col span={3}>
            <MetricCard
              icon={Zap}
              title="Orchestration"
              value={compactNum(data.totals.orchestration_tokens)}
              meta={`${compactNum(data.totals.orchestration_cached_tokens)} cached`}
            />
          </Col>
          <Col span={3}>
            <MetricCard
              icon={Activity}
              title="Cache-hit rate"
              value={pct(data.totals.cache_hit_rate * 100)}
            />
          </Col>
          <Col span={3}>
            <MetricCard icon={BarChart3} title="Requests" value={compactNum(data.totals.requests)} />
          </Col>
        </>
      }
      chart={
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={data.time_series} margin={{ top: 4, right: 6, bottom: 0, left: -6 }}>
            <defs>
              <linearGradient id="tokens-trend" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="hsl(var(--claude))" stopOpacity={0.32} />
                <stop offset="100%" stopColor="hsl(var(--claude))" stopOpacity={0} />
              </linearGradient>
              <linearGradient id="orchestration-trend" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="hsl(var(--accent))" stopOpacity={0.24} />
                <stop offset="100%" stopColor="hsl(var(--accent))" stopOpacity={0} />
              </linearGradient>
            </defs>
            <CartesianGrid vertical={false} stroke="hsl(var(--border))" strokeDasharray="3 3" />
            <XAxis dataKey="ts" type="number" domain={["dataMin", "dataMax"]} hide />
            <YAxis
              width={40}
              tick={{ fontSize: 8.5, fill: "hsl(var(--fg))", fillOpacity: 0.6 }}
              axisLine={false}
              tickLine={false}
              tickFormatter={(v: number) => compactNum(v)}
            />
            <Area
              type="monotone"
              dataKey="tokens"
              stroke="hsl(var(--claude))"
              strokeWidth={1.7}
              fill="url(#tokens-trend)"
              isAnimationActive={false}
              dot={false}
            />
            <Area
              type="monotone"
              dataKey="orchestration_tokens"
              stroke="hsl(var(--accent))"
              strokeWidth={1.5}
              fill="url(#orchestration-trend)"
              isAnimationActive={false}
              dot={false}
            />
          </AreaChart>
        </ResponsiveContainer>
      }
      breakdown={data.breakdown}
      columns={USAGE_COLUMNS}
      dimensionLabel={dimensionLabel}
    />
  );
}

// ---------------------------------------------------------------------------------------------
// Performance section — latency + error rate. `PerfPoint`/`toPerfSeries` implement the CRITICAL
// zero-fill gotcha from the task brief: the backend zero-fills empty time buckets, so an empty
// bucket's `avg_duration_ms`/`avg_ttft_ms` is `0` — indistinguishable from a real 0ms sample and
// would plot as a false latency dip. Buckets with `requests === 0` map to `null` here so recharts
// (with `connectNulls={false}` on both `<Line>`s below) renders a genuine gap instead of a dip.
// ---------------------------------------------------------------------------------------------

interface PerfPoint {
  ts: number;
  duration: number | null;
  ttft: number | null;
}

function toPerfSeries(buckets: ReportBucketView[]): PerfPoint[] {
  return buckets.map((b) => ({
    ts: b.ts,
    duration: b.requests > 0 ? b.avg_duration_ms : null,
    ttft: b.requests > 0 ? b.avg_ttft_ms : null,
  }));
}

const PERFORMANCE_COLUMNS: ReportSectionColumn[] = [
  {
    header: "Avg duration",
    align: "right",
    render: (row: ReportBreakdownView) => latency(row.avg_duration_ms),
  },
  {
    header: "Avg TTFT",
    align: "right",
    render: (row: ReportBreakdownView) => latency(row.avg_ttft_ms),
  },
  {
    header: "Errors",
    align: "right",
    render: (row: ReportBreakdownView) => compactNum(row.errors),
  },
  {
    header: "Requests",
    align: "right",
    render: (row: ReportBreakdownView) => compactNum(row.requests),
  },
];

function PerformanceSection({
  data,
  dimensionLabel,
}: {
  data: ReportsView;
  dimensionLabel: string;
}) {
  const perfSeries = toPerfSeries(data.time_series);
  const ttftIsPartial = data.totals.ttft_sample_count < data.totals.requests;

  return (
    <ReportSection
      title="Performance"
      kpis={
        <>
          <Col span={4}>
            <MetricCard
              icon={Clock}
              title="Avg duration"
              value={latency(data.totals.avg_duration_ms)}
            />
          </Col>
          <Col span={4}>
            <MetricCard icon={Zap} title="Avg TTFT" value={latency(data.totals.avg_ttft_ms)} />
          </Col>
          <Col span={4}>
            <MetricCard
              icon={AlertTriangle}
              title="Error rate"
              value={ratePct(data.totals.error_rate * 100)}
            />
          </Col>
        </>
      }
      chart={
        <div className="flex h-full flex-col">
          <div className="flex items-center justify-end gap-3 pb-1 text-[9px] text-fg opacity-70">
            <LegendSwatch colorClass="bg-codex" label="Duration" />
            <LegendSwatch colorClass="bg-claude" label="TTFT" />
          </div>
          <div className="min-h-0 flex-1">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={perfSeries} margin={{ top: 4, right: 6, bottom: 0, left: -6 }}>
                <CartesianGrid vertical={false} stroke="hsl(var(--border))" strokeDasharray="3 3" />
                <XAxis dataKey="ts" type="number" domain={["dataMin", "dataMax"]} hide />
                <YAxis
                  width={40}
                  tick={{ fontSize: 8.5, fill: "hsl(var(--fg))", fillOpacity: 0.6 }}
                  axisLine={false}
                  tickLine={false}
                  tickFormatter={(v: number) => latency(v)}
                />
                <Line
                  type="monotone"
                  dataKey="duration"
                  stroke="hsl(var(--codex))"
                  strokeWidth={1.7}
                  dot={false}
                  isAnimationActive={false}
                  connectNulls={false}
                />
                <Line
                  type="monotone"
                  dataKey="ttft"
                  stroke="hsl(var(--claude))"
                  strokeWidth={1.7}
                  dot={false}
                  isAnimationActive={false}
                  connectNulls={false}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      }
      breakdown={data.breakdown}
      columns={PERFORMANCE_COLUMNS}
      dimensionLabel={dimensionLabel}
      note={
        ttftIsPartial
          ? `TTFT covers ${data.totals.ttft_sample_count} of ${data.totals.requests} requests`
          : undefined
      }
    />
  );
}

function LegendSwatch({ colorClass, label }: { colorClass: string; label: string }) {
  return (
    <span className="flex items-center gap-1">
      <span className={clsx("inline-block h-[3px] w-[9px] rounded-sm", colorClass)} />
      {label}
    </span>
  );
}

/** Loading placeholder — mirrors the real header + control bar + 4-card KPI row so data arriving
 * doesn't reflow the page. */
function ReportsSkeleton() {
  return (
    <Grid>
      {[0, 1, 2, 3].map((i) => (
        <Col key={i} span={3}>
          <Card>
            <div className="h-[74px] animate-pulse rounded bg-muted" />
          </Card>
        </Col>
      ))}
    </Grid>
  );
}
