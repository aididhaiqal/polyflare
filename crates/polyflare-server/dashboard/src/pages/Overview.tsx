// The dashboard's landing page. Consumes ONLY `useOverview()` (GET /api/overview —
// read_api.rs::OverviewView) — no other query hook. That endpoint is a set of content-free
// AGGREGATES (rolling-24h KPI totals, per-provider quota headroom, per-pool account/availability
// counts, a global available-account count, and the most recent error rows) — it carries no
// per-account list and no historical/bucketed time series. Several regions of the authoritative
// mockup (overview-ccflare-v2.html) assume richer data than this endpoint provides (a request-volume
// time series, a weekly-pace forecast needing an expected-vs-actual trend, a per-account health
// table); rather than fabricate placeholder numbers for those, this page adapts the layout to what
// `OverviewView` actually reports. See task-5-report.md for the full fidelity-deviation rationale.
import { useEffect, useState, type ReactNode } from "react";
import { Link } from "react-router-dom";
import clsx from "clsx";

import type { RecentErrorView } from "../lib/api";
import { compactNum, latency, pct, relTime } from "../lib/format";
import { useOverview } from "../lib/queries";
import { Card } from "../ui/Card";
import { Col, Grid } from "../ui/Grid";
import { Activity, AlertTriangle, CheckCircle2, ChevronRight, Clock, Coins } from "../ui/icons";
import { MetricCard } from "../ui/MetricCard";
import { providerBrandKey } from "../ui/ProviderTag";
import { QuotaBars, type QuotaProviderGroup } from "../ui/QuotaBars";

type ProviderFilter = "all" | "codex" | "claude";

const PROVIDER_FILTERS: Array<{ value: ProviderFilter; label: string }> = [
  { value: "all", label: "All" },
  { value: "codex", label: "Codex" },
  { value: "claude", label: "Claude" },
];

export function Overview() {
  const { data, isLoading, isError, error, refetch, dataUpdatedAt } = useOverview();
  const [providerFilter, setProviderFilter] = useState<ProviderFilter>("all");

  // Ticks the header's "updated Xs ago" text between refetches (useOverview() polls every 30s;
  // without this the label would only ever update once per poll instead of counting up smoothly).
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

        <Col span={6}>
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

        <Col span={6}>
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
                      className="flex items-center gap-2.5 border-b border-border py-1.5 text-[10.5px] last:border-0"
                    >
                      <span className="w-16 shrink-0 truncate font-semibold text-fg">{label}</span>
                      <span className="w-24 shrink-0 text-fg opacity-60">
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

/** Loading placeholder — mirrors the real layout's grid spans exactly (4×span3, 6+6, span12) so
 * data arriving doesn't reflow the page. */
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
        <Col span={6}>
          <Card>
            <div className="h-40 animate-pulse rounded bg-muted" />
          </Card>
        </Col>
        <Col span={6}>
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
