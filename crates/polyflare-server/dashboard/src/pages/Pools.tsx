// The Pools page: `GET /api/pools` (`usePools()`) is the ONLY endpoint this page consumes. It is
// the expanded form of Overview's compact "Pools" panel (`overview-ccflare-v2.html`'s `.card.c4`
// mini-list) — no dedicated pools mockup exists (checked `.superpowers/brainstorm/23587-1784245350/
// content/`: 18 files, none named for a pools page), so this page follows that panel's visual
// language (name / account counts / usage bar / — this task adds the `strategy` column the compact
// panel didn't have room for) rather than inventing a new one.
//
// Field mapping (see read_api.rs::PoolView / src/lib/api.ts's mirror):
//   pool            <- p.pool (`null` = the unpooled group — a REAL, reachable state: those
//                       accounts are only selectable via the bare `/responses` route, not
//                       `/{pool}/responses`. Rendered as an honest "unpooled" row with an inline
//                       caption explaining the routing difference, never hidden or asterisked away.)
//   accounts column <- p.available / p.accounts (brief's literal "available/total"), with p.active
//                       surfaced as a supplementary caption — PoolView carries `active` too and
//                       Task 8's brief is explicit that every real field here is real data, not a
//                       placeholder, so it isn't dropped just because the brief's own column list
//                       didn't spell out a 5th column for it.
//   usage bar       <- p.usage_percent (mean primary-window used_percent across the pool's
//                       accounts), risk-toned (ok/warn/error by value) — NOT provider-branded, same
//                       deliberate deviation Task 5b/6 already established (a pool mixes providers,
//                       so there's no single brand color that would even apply).
//   strategy        <- p.strategy (`AppState::selector_for(pool).name()` — a real configured
//                       routing-selector name, per the brief's note that `Selector::name()` was
//                       added specifically so this could be surfaced here).
//
// Phase 1 is READ-ONLY: pool (re)assignment is CLI-only (`accounts set-pool`) plus the auth-gated
// `PATCH /api/accounts/{id}` — neither is wired here, and no disabled affordance is rendered either
// (omitted per the brief's explicit either/or), matching Accounts.tsx's precedent of not surfacing
// any mutation UI at all in Phase 1.
import { useEffect, useState, type ReactNode } from "react";
import clsx from "clsx";

import type { PoolView } from "../lib/api";
import { pct, relTime } from "../lib/format";
import { quotaDisplayLabel, quotaDisplayPercent } from "../lib/quotaDisplay";
import { useAccounts, usePools } from "../lib/queries";
import { useQuotaDisplayPreference } from "../preferences/QuotaDisplayPreference";
import { Card } from "../ui/Card";
import { Col, Grid } from "../ui/Grid";
import { AlertTriangle, Plus } from "../ui/icons";
import { CreatePoolDialog } from "../ui/CreatePoolDialog";
import type { StatusTone } from "../ui/StatusPill";

/** Risk-tone thresholds for the aggregate usage bar. Duplicated (not shared) per the convention
 * Accounts.tsx already established for this exact 3-line pure function — see that page's own
 * comment on why a shared atom isn't worth it here. */
function usageRiskTone(usedPercent: number): StatusTone {
  if (usedPercent >= 90) return "error";
  if (usedPercent >= 70) return "warn";
  return "ok";
}

const TONE_BAR_CLASS: Record<StatusTone, string> = {
  ok: "bg-success",
  warn: "bg-warn",
  error: "bg-error",
};

const TABLE_HEAD_CLASS =
  "px-2 py-1.5 text-left text-[9px] font-medium uppercase tracking-wide text-fg opacity-60";

export function Pools() {
  const { data, isLoading, isError, error, refetch, dataUpdatedAt } = usePools();
  const { data: accounts = [] } = useAccounts();
  const [createOpen, setCreateOpen] = useState(false);

  // Ticks the header's "updated Xs ago" text between usePools()'s 30s polls — same pattern
  // Overview.tsx/Accounts.tsx already use for their own header/countdown text.
  const [nowMs, setNowMs] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNowMs(Date.now()), 5000);
    return () => clearInterval(id);
  }, []);

  if (isLoading) return <PoolsSkeleton />;

  if (isError) {
    return (
      <div className="flex flex-col gap-3">
        <PageHeader />
        <Card>
          <div className="flex flex-wrap items-center justify-between gap-3">
            <span className="flex items-center gap-2 text-[12px] text-error">
              <AlertTriangle className="h-4 w-4 shrink-0" strokeWidth={1.9} />
              Couldn&apos;t load pools
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
  // is either success-with-data or error) — narrows `data` for TS below without a non-null assert.
  if (!data) return null;

  const pools = data;
  const namedPools = pools.filter((p) => p.pool !== null);
  const unpooledRow = pools.find((p) => p.pool === null) ?? null;
  const totalAccounts = pools.reduce((sum, p) => sum + p.accounts, 0);
  const totalAvailable = pools.reduce((sum, p) => sum + p.available, 0);

  return (
    <div className="flex flex-col gap-3">
      <PageHeader
        subtitle={
          pools.length > 0 ? (
            <>
              {namedPools.length} named {namedPools.length === 1 ? "pool" : "pools"}
              {unpooledRow && (
                <>
                  {" "}
                  · <span className="tabular-nums">{unpooledRow.accounts}</span> unpooled
                </>
              )}
              {" · "}
              <span className="font-semibold text-success">
                {totalAvailable} of {totalAccounts} accounts available
              </span>
              {" · updated "}
              {dataUpdatedAt ? relTime(Math.floor(dataUpdatedAt / 1000), nowMs) : "—"}
            </>
          ) : undefined
        }
        actions={<button type="button" disabled={accounts.length === 0} onClick={() => setCreateOpen(true)} className="flex items-center gap-1.5 rounded bg-accent px-3 py-1.5 text-[11px] font-semibold text-white disabled:cursor-not-allowed disabled:opacity-40"><Plus className="h-3.5 w-3.5" />Create routing group</button>}
      />

      {pools.length === 0 ? (
        <Card>
          <p className="text-[11px] text-fg opacity-50">
            No routing groups yet. Create one by assigning at least one existing account.
          </p>
        </Card>
      ) : (
        <Grid>
          <Col span={12}>
            <PoolsTable pools={pools} />
          </Col>
        </Grid>
      )}
      <CreatePoolDialog open={createOpen} onOpenChange={setCreateOpen} accounts={accounts} />
    </div>
  );
}

function PageHeader({ subtitle, actions }: { subtitle?: ReactNode; actions?: ReactNode }) {
  return (
    <div className="flex flex-wrap items-center justify-between gap-3">
      <div><h1 className="text-lg font-semibold text-fg">Pools</h1>
      {subtitle && <p className="mt-0.5 text-[11px] text-fg opacity-60">{subtitle}</p>}</div>
      {actions}
    </div>
  );
}

function PoolsTable({ pools }: { pools: PoolView[] }) {
  const { mode } = useQuotaDisplayPreference();
  return (
    <Card>
      <div className="overflow-x-auto">
        <table className="w-full min-w-[620px] border-collapse text-[10.5px]">
          <thead>
            <tr className="border-b border-border">
              <th className={TABLE_HEAD_CLASS}>Pool</th>
              <th className={TABLE_HEAD_CLASS}>Accounts</th>
              <th className={TABLE_HEAD_CLASS}>Quota {quotaDisplayLabel(mode)}</th>
              <th className={TABLE_HEAD_CLASS}>Strategy</th>
            </tr>
          </thead>
          <tbody>
            {pools.map((p) => (
              <PoolRow key={p.pool ?? "__unpooled__"} pool={p} />
            ))}
          </tbody>
        </table>
      </div>
    </Card>
  );
}

function PoolRow({ pool: p }: { pool: PoolView }) {
  const { mode } = useQuotaDisplayPreference();
  const isUnpooled = p.pool === null;
  const label = p.pool ?? "unpooled";
  const clampedUsage = Math.max(0, Math.min(100, p.usage_percent));
  const displayedUsage = quotaDisplayPercent(clampedUsage, mode);
  const tone = usageRiskTone(clampedUsage);

  return (
    <tr className="border-b border-border/55 last:border-0">
      <td className="px-2 py-2 align-top">
        <span className="font-semibold text-fg">{label}</span>
        {isUnpooled && (
          <div className="mt-0.5 max-w-[220px] text-[9px] leading-snug text-fg opacity-50">
            Available to the global router via the bare{" "}
            <code className="rounded bg-muted px-1 py-0.5 font-mono">/responses</code> route; assign
            a routing group to also enable a scoped path.
          </div>
        )}
      </td>
      <td className="px-2 py-2 align-top">
        <div className="tabular-nums text-fg">
          <span className="font-semibold">{p.available}</span>
          <span className="opacity-60">/{p.accounts} available</span>
        </div>
        <div className="mt-0.5 tabular-nums text-[9.5px] text-fg opacity-50">
          {p.active} active
        </div>
      </td>
      <td className="px-2 py-2 align-top">
        <div className="flex items-center gap-2">
          <div className="h-1.5 w-24 shrink-0 overflow-hidden rounded-full bg-muted">
            <div
              className={clsx("h-full rounded-full", TONE_BAR_CLASS[tone])}
              style={{ width: `${displayedUsage}%` }}
            />
          </div>
          <span className="tabular-nums text-fg opacity-70">{pct(displayedUsage)}</span>
        </div>
      </td>
      <td className="px-2 py-2 align-top">
        <span className="inline-block whitespace-nowrap rounded bg-muted px-2 py-0.5 text-[10px] font-semibold text-fg opacity-80">
          {p.strategy.replace(/_/g, " ")}
        </span>
      </td>
    </tr>
  );
}

// ---------------------------------------------------------------------------------------------
// Loading skeleton — mirrors the real header + the single full-width table panel so data arriving
// doesn't reflow.
// ---------------------------------------------------------------------------------------------

function PoolsSkeleton() {
  return (
    <div className="flex flex-col gap-3">
      <div>
        <div className="h-[22px] w-16 animate-pulse rounded bg-muted" />
        <div className="mt-1.5 h-3 w-72 animate-pulse rounded bg-muted" />
      </div>
      <Grid>
        <Col span={12}>
          <Card>
            <div className="flex flex-col gap-2">
              {[0, 1, 2].map((i) => (
                <div key={i} className="h-9 animate-pulse rounded bg-muted" />
              ))}
            </div>
          </Card>
        </Col>
      </Grid>
    </div>
  );
}
