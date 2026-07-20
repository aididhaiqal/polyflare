// The Reports page: the dashboard's analytics view — `GET /api/reports` (`useReports(params)`) is
// the ONLY endpoint this page consumes. Task 3 shipped the frontend data layer (typed client +
// hook), the nav entry (promoted from Sidebar.tsx's disabled "Analytics" placeholder), the
// `/reports` route, and a control-bar + totals-KPI stub — see task-3-brief.md. Task 4 (this pass)
// renders the first real section — Cost — via the reusable `ReportSection` (a trend `AreaChart`
// over `time_series` + a per-dimension breakdown table). Task 5 reuses `ReportSection` for the
// Usage and Performance sections.
//
// CONTENT-SAFETY: `ReportsView` is sourced from the same content-free `request_log` aggregates the
// rest of the dashboard already exposes (counts, cost, token COUNTS, timing) — never a body,
// prompt, response, or key.
import { useState } from "react";
import clsx from "clsx";
import { Area, AreaChart, CartesianGrid, ResponsiveContainer, XAxis, YAxis } from "recharts";

import type { ReportBreakdownView, ReportsView } from "../lib/api";
import { compactNum, pct } from "../lib/format";
import { useReports, type ReportsParams } from "../lib/queries";
import { Card } from "../ui/Card";
import { Col, Grid } from "../ui/Grid";
import { Activity, AlertTriangle, BarChart3, Coins } from "../ui/icons";
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
  { value: "account", label: "Account" },
  { value: "model", label: "Model" },
  { value: "provider", label: "Provider" },
];

/** Backend wire values for `provider` are "codex"/"anthropic" (never "claude") — same mapping
 * `Requests.tsx`'s provider filter uses. `ALL` means "omit the param entirely", not a literal
 * `provider=all` sent to the backend. */
const ALL = "all";
const PROVIDER_OPTIONS: Array<{ value: string; label: string }> = [
  { value: ALL, label: "all providers" },
  { value: "codex", label: "codex" },
  { value: "anthropic", label: "claude" },
];

const SELECT_CLASS =
  "shrink-0 rounded border border-border bg-card px-2.5 py-1 text-[10.5px] text-fg opacity-80 outline-none hover:opacity-100 focus:opacity-100";

export function Reports() {
  const [range, setRange] = useState<RangeKey>("7d");
  const [dimension, setDimension] = useState<DimensionKey>("model");
  const [provider, setProvider] = useState<string>(ALL);

  const params: ReportsParams = {
    range,
    dimension,
    provider: provider !== ALL ? provider : undefined,
  };

  const { data, isLoading, isFetching, isError, error, refetch } = useReports(params);

  return (
    <div className="flex flex-col gap-3">
      <PageHeader />

      <div className="flex flex-wrap items-center gap-2">
        <div className="flex shrink-0 overflow-hidden rounded border border-border bg-card text-[10.5px]">
          {RANGE_OPTIONS.map((o) => (
            <button
              key={o.value}
              type="button"
              onClick={() => setRange(o.value)}
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
          onChange={(e) => setDimension(e.target.value as DimensionKey)}
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
          onChange={(e) => setProvider(e.target.value)}
          className={SELECT_CLASS}
          aria-label="Provider filter"
        >
          {PROVIDER_OPTIONS.map((o) => (
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
        <CostSection
          data={data}
          dimensionLabel={DIMENSION_OPTIONS.find((o) => o.value === dimension)?.label ?? "Dimension"}
        />
      )}
    </div>
  );
}

function PageHeader() {
  return (
    <div>
      <h1 className="text-lg font-semibold text-fg">Reports</h1>
      <p className="mt-0.5 text-[11px] text-fg opacity-60">
        Cost trends and a per-dimension breakdown over the selected window — the Usage and
        Performance sections land in a later pass.
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
