// The Reports page: the dashboard's analytics view — `GET /api/reports` (`useReports(params)`) is
// the ONLY endpoint this page consumes. Task 3 ships the frontend data layer (typed client + hook),
// the nav entry (promoted from Sidebar.tsx's disabled "Analytics" placeholder), the `/reports`
// route, and a control-bar + totals-KPI stub — see task-3-brief.md. `ReportsView.time_series` /
// `.breakdown` are already fetched here but deliberately UNRENDERED: the trend chart and per-
// dimension breakdown table are Task 4/5's job, not this task's.
//
// CONTENT-SAFETY: `ReportsView` is sourced from the same content-free `request_log` aggregates the
// rest of the dashboard already exposes (counts, cost, token COUNTS, timing) — never a body,
// prompt, response, or key. This page renders ONLY `totals`' real fields.
import { useState } from "react";
import clsx from "clsx";

import { compactNum, pct } from "../lib/format";
import { useReports, type ReportsParams } from "../lib/queries";
import { Card } from "../ui/Card";
import { Col, Grid } from "../ui/Grid";
import { AlertTriangle, BarChart3, Coins, Layers } from "../ui/icons";
import { MetricCard } from "../ui/MetricCard";

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
        <Grid>
          <Col span={3}>
            <MetricCard
              icon={Coins}
              title="Total cost"
              value={`$${data.totals.cost_usd.toFixed(2)}`}
            />
          </Col>
          <Col span={3}>
            <MetricCard icon={BarChart3} title="Requests" value={compactNum(data.totals.requests)} />
          </Col>
          <Col span={3}>
            <MetricCard icon={Layers} title="Tokens" value={compactNum(data.totals.tokens)} />
          </Col>
          <Col span={3}>
            <MetricCard
              icon={AlertTriangle}
              title="Error rate"
              // `ReportTotalsView.error_rate` is a 0-1 fraction (errors/requests, see read_api.rs),
              // same convention as `KpisView.success_rate` — `pct()` expects a 0-100 scale (see
              // format.ts), so it's scaled here exactly like Overview.tsx's
              // `pct(data.kpis.success_rate * 100)` does for that analogous ratio.
              value={pct(data.totals.error_rate * 100)}
            />
          </Col>
        </Grid>
      )}
    </div>
  );
}

function PageHeader() {
  return (
    <div>
      <h1 className="text-lg font-semibold text-fg">Reports</h1>
      <p className="mt-0.5 text-[11px] text-fg opacity-60">
        Cost, request, and token totals over a selected window — trend charts and the per-dimension
        breakdown land in a later pass.
      </p>
    </div>
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
