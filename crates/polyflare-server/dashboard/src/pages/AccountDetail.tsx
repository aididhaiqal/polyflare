// The Account detail page (`/accounts/:id`): a master-detail layout — a searchable, pool-grouped
// account rail (`useAccounts()`) on the left for quick account switching, and the selected
// account's full detail (`useAccount(id)` + `useAccountTrends(id)`) on the right. Mockups:
// `accounts-master-detail-v2.html` (rail + overall structure) and `accounts-detail-v2.html`
// (the reworked 3-column Actions panel — used verbatim per the task brief, which names it
// explicitly over the master-detail mockup's flatter single-row Actions).
//
// PHASE 3 (this task) ENABLES the in-scope Actions-panel controls (routing policy, trusted access,
// pool, alias, pause/resume, delete) via the shared `useAccountActions()` hook (Task 7,
// `../lib/useAccountActions`) — each wired control calls `actions.patch.mutate({id, body})` directly
// or opens one of the hook's confirmation dialogs. Controls with no backing field in this MVP schema
// (limit warm-up, rate-limit reset credits, force probe, re-authenticate, export auth) remain plain
// disabled placeholders — see task-7-report.md/task-8-report.md for the full mapping and reasoning.
//
// Field mapping (see read_api.rs::AccountDetailView / src/lib/api.ts's mirror):
//   rail rows                 <- useAccounts() (AccountView[] — DOES have an `alias` field; Task 4b
//                                 put it on the backend wire, Task 7 added it to the TS interface;
//                                 see task-7-report.md). This page's rail still borrows the
//                                 currently-open row's alias from the detail query (identity.alias)
//                                 rather than reading it off the list row directly — a harmless
//                                 pre-existing pattern, left as-is since this task's scope is the
//                                 Actions panel, not the rail.
//   header dot/name/pv/status  <- detail.status (via statusTone/StatusPill), detail.identity.alias
//                                 ?? identity.id, identity.provider
//   meta line                  <- identity.email/workspace_label/plan_type/pool/seat_type
//   Usage / quota              <- detail.quota_windows (adaptive UsageWindowView[] — a window not
//                                 reported is simply absent, never a fabricated dash row here,
//                                 unlike the Accounts list page's fixed five_hour/weekly pair)
//   all-time totals footer     <- detail.request_totals {request_count, total_tokens}
//   Token status                <- detail.token_status {access_state, access_expires_at} — access
//                                 only; there is no refresh/id-token field on the backend, so the
//                                 mockup's "Refresh: Stored"/"ID token: Parsed" rows are omitted
//                                 entirely rather than invented.
//   7-day trend                 <- useAccountTrends(id): primary (5h, orange) / secondary (weekly,
//                                 purple) point series — confirmed in polyflare-store's account.rs
//                                 that `usage_history`'s "primary"/"secondary" window labels ARE the
//                                 5h/weekly windows respectively. Fixed 0-100 y-axis, no plan line.
//   Actions / Configuration      <- routing policy: detail.routing_policy, EDITABLE (all 3 options,
//                                 PATCHes via actions.patch); trusted access: detail.
//                                 security_work_authorized, EDITABLE (toggle switch); pool:
//                                 identity.pool, EDITABLE (opens actions.openSetPool); limit
//                                 warm-up: NO backend field — rendered as a static "not tracked"
//                                 chip instead of a fabricated switch state.
//                                 Rate-limit resets: NO backend concept at all (grepped the store —
//                                 no reset-credit table/field exists) — rendered as an explanatory
//                                 placeholder, no invented counts/expiries.
//                                 Operations: Pause/Resume and Delete are EDITABLE (wired to
//                                 actions.patch / actions.openDelete); Force probe/Re-authenticate/
//                                 Export auth remain plain disabled buttons — no backend for them.
import { useEffect, useMemo, useState, type ReactNode } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  XAxis,
  YAxis,
} from "recharts";
import clsx from "clsx";

import {
  ApiError,
  type AccountDetailView,
  type AccountView,
  type DepletionForecast,
  type Point,
  type RiskLevel,
  type TokenHealthView,
  type UsageWindowView,
} from "../lib/api";
import { compactNum, countdown, pct } from "../lib/format";
import {
  quotaDisplayLabel,
  quotaDisplayPercent,
  quotaWindowIsPresent,
} from "../lib/quotaDisplay";
import { useAccount, useAccountTrends, useAccounts } from "../lib/queries";
import { useAccountActions, type AccountActionsApi } from "../lib/useAccountActions";
import { useQuotaDisplayPreference } from "../preferences/QuotaDisplayPreference";
import {
  routePseudonym,
  ShieldedAccount,
  useScreenShield,
} from "../privacy/ScreenShield";
import { Card } from "../ui/Card";
import { CodexOnboardingDialog } from "../ui/CodexOnboardingDialog";
import { Col, Grid } from "../ui/Grid";
import {
  AlertTriangle,
  ChevronLeft,
  ChevronRight,
  Flame,
  Key,
  Layers,
  LogIn,
  Pause,
  Pencil,
  Play,
  Route,
  Search,
  ShieldCheck,
  Trash2,
  type LucideIcon,
} from "../ui/icons";
import { providerBrandKey, ProviderTag } from "../ui/ProviderTag";
import { StatusPill, statusTone, type StatusTone } from "../ui/StatusPill";

const DOT_CLASS: Record<StatusTone, string> = {
  ok: "bg-success",
  warn: "bg-warn",
  error: "bg-error",
};

const TEXT_TONE_CLASS: Record<StatusTone, string> = {
  ok: "text-success",
  warn: "text-warn",
  error: "text-error",
};

/** Usage-risk thresholds — same convention as Overview.tsx/Accounts.tsx's own per-page helper of
 * the same name (duplicated on purpose, per the established precedent, rather than a shared atom
 * for a 3-line pure function). */
function usageRiskTone(usedPercent: number): StatusTone {
  if (usedPercent >= 90) return "error";
  if (usedPercent >= 70) return "warn";
  return "ok";
}

// ---------------------------------------------------------------------------------------------
// Account ordering / grouping shared by the rail and the header's ‹ › cycle control. Sorted by
// pool (named pools alphabetically, an "unpooled" bucket last — an assumption, since no mockup
// scenario has both named and unpooled accounts to disambiguate), then by id within a pool, so the
// rail's visual grouping and the cycle control's "N of M" always agree on the same order.
// ---------------------------------------------------------------------------------------------

function poolSortKey(pool: string | null): string {
  return pool ?? "￿";
}

function orderAccounts(accounts: AccountView[]): AccountView[] {
  return [...accounts].sort((a, b) => {
    const byPool = poolSortKey(a.pool).localeCompare(poolSortKey(b.pool));
    if (byPool !== 0) return byPool;
    return a.id.localeCompare(b.id);
  });
}

interface RailGroup {
  key: string;
  label: string;
  accounts: AccountView[];
}

/** Groups an already-`orderAccounts`-sorted list into consecutive per-pool buckets. */
function groupByPool(ordered: AccountView[]): RailGroup[] {
  const groups: RailGroup[] = [];
  for (const a of ordered) {
    const key = a.pool ?? "__unpooled__";
    const last = groups[groups.length - 1];
    if (last && last.key === key) {
      last.accounts.push(a);
    } else {
      groups.push({ key, label: a.pool ?? "unpooled", accounts: [a] });
    }
  }
  return groups;
}

function worstUsedPercent(a: AccountView): number | null {
  const vals = [a.five_hour?.used_percent, a.weekly?.used_percent].filter(
    (v): v is number => v !== undefined && v !== null,
  );
  if (vals.length === 0) return null;
  return Math.max(...vals);
}

// ---------------------------------------------------------------------------------------------
// Page root
// ---------------------------------------------------------------------------------------------

export function AccountDetail() {
  const { id: rawId } = useParams<{ id: string }>();
  const id = rawId ?? "";
  const navigate = useNavigate();

  const accountsQuery = useAccounts();
  const detailQuery = useAccount(id);
  const trendsQuery = useAccountTrends(id);

  const [search, setSearch] = useState("");

  // Ticks countdowns (quota-window resets, token-expiry countdown) — same pattern
  // Overview.tsx/Accounts.tsx use for their own countdown math between poll intervals.
  const [nowMs, setNowMs] = useState(() => Date.now());
  useEffect(() => {
    const t = setInterval(() => setNowMs(Date.now()), 5000);
    return () => clearInterval(t);
  }, []);

  const accounts = accountsQuery.data ?? [];
  const ordered = useMemo(() => orderAccounts(accounts), [accounts]);
  const currentIndex = ordered.findIndex((a) => a.id === id);
  const canCycle = ordered.length > 1 && currentIndex !== -1;

  function goPrev() {
    if (!canCycle) return;
    const target = ordered[(currentIndex - 1 + ordered.length) % ordered.length];
    navigate(`/accounts/${encodeURIComponent(target.id)}`);
  }
  function goNext() {
    if (!canCycle) return;
    const target = ordered[(currentIndex + 1) % ordered.length];
    navigate(`/accounts/${encodeURIComponent(target.id)}`);
  }

  const notFound =
    detailQuery.isError && detailQuery.error instanceof ApiError && detailQuery.error.status === 404;
  const detail = detailQuery.data;

  return (
    <div className="flex flex-1 flex-col items-stretch gap-4 lg:flex-row lg:items-start">
      <AccountRail
        accounts={accounts}
        isLoading={accountsQuery.isLoading}
        isError={accountsQuery.isError}
        onRetry={() => accountsQuery.refetch()}
        currentId={id}
        currentAlias={detail?.identity.alias ?? null}
        search={search}
        onSearchChange={setSearch}
      />

      <div className="min-w-0 flex-1">
        {notFound ? (
          <NotFoundPanel id={id} />
        ) : detailQuery.isError ? (
          <ErrorPanel
            message={`Couldn't load this account${detailQuery.error instanceof Error ? `: ${detailQuery.error.message}` : "."}`}
            onRetry={() => detailQuery.refetch()}
          />
        ) : detailQuery.isLoading ? (
          <DetailSkeleton />
        ) : detail ? (
          <DetailContent
            detail={detail}
            nowMs={nowMs}
            cycleLabel={canCycle ? `${currentIndex + 1} of ${ordered.length}` : null}
            onPrev={goPrev}
            onNext={goNext}
            canCycle={canCycle}
            trendsLoading={trendsQuery.isLoading}
            trendsError={trendsQuery.isError}
            onTrendsRetry={() => trendsQuery.refetch()}
            primary={trendsQuery.data?.primary ?? []}
            secondary={trendsQuery.data?.secondary ?? []}
            forecast={trendsQuery.data?.forecast ?? null}
          />
        ) : (
          // Defensive fallback (e.g. a route somehow reached with no :id) — never a blank page.
          <NotFoundPanel id={id} />
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------------------------
// Rail
// ---------------------------------------------------------------------------------------------

interface AccountRailProps {
  accounts: AccountView[];
  isLoading: boolean;
  isError: boolean;
  onRetry: () => void;
  currentId: string;
  currentAlias: string | null;
  search: string;
  onSearchChange: (v: string) => void;
}

function AccountRail({
  accounts,
  isLoading,
  isError,
  onRetry,
  currentId,
  currentAlias,
  search,
  onSearchChange,
}: AccountRailProps) {
  const needle = search.trim().toLowerCase();
  const filtered =
    needle.length === 0 ? accounts : accounts.filter((a) => a.id.toLowerCase().includes(needle));
  const groups = groupByPool(orderAccounts(filtered));

  return (
    <div className="flex w-full shrink-0 flex-col gap-2 self-start rounded-xl border border-border/80 bg-card/90 p-3 shadow-[0_12px_32px_hsl(var(--surface-shadow)/0.12)] lg:w-[232px]">
      <Link
        to="/accounts"
        className="px-0.5 text-[11.5px] font-semibold text-fg no-underline hover:text-accent"
      >
        Accounts <span className="font-normal text-fg opacity-50">{accounts.length}</span>
      </Link>

      <div className="flex items-center gap-1.5 rounded border border-border bg-bg px-2 py-1">
        <Search className="h-3 w-3 shrink-0 text-fg opacity-50" strokeWidth={2} />
        <input
          value={search}
          onChange={(e) => onSearchChange(e.target.value)}
          placeholder="Search accounts…"
          className="w-full bg-transparent text-[10.5px] text-fg outline-none placeholder:text-fg placeholder:opacity-40"
        />
      </div>

      <div className="flex max-h-[220px] flex-col gap-0.5 overflow-y-auto lg:max-h-[560px]">
        {isLoading ? (
          <RailSkeleton />
        ) : isError ? (
          <div className="flex flex-col items-start gap-1.5 px-1 py-2 text-[10.5px] text-error">
            <span className="flex items-center gap-1.5">
              <AlertTriangle className="h-3.5 w-3.5 shrink-0" strokeWidth={1.9} />
              Couldn&apos;t load accounts.
            </span>
            <button
              type="button"
              onClick={onRetry}
              className="rounded border border-border px-2 py-0.5 text-[10px] text-fg opacity-80 hover:opacity-100"
            >
              Retry
            </button>
          </div>
        ) : accounts.length === 0 ? (
          <p className="px-1 py-2 text-[10.5px] text-fg opacity-50">No accounts configured yet.</p>
        ) : groups.length === 0 ? (
          <p className="px-1 py-2 text-[10.5px] text-fg opacity-50">No matches.</p>
        ) : (
          groups.map((group) => (
            <div key={group.key}>
              <div className="px-1 pb-1 pt-2 text-[8.5px] font-medium uppercase tracking-wide text-fg opacity-50 first:pt-0.5">
                {group.label} · {group.accounts.length}
              </div>
              {group.accounts.map((a) => (
                <RailRow
                  key={a.id}
                  account={a}
                  isSelected={a.id === currentId}
                  displayName={a.id === currentId && currentAlias ? currentAlias : a.id}
                />
              ))}
            </div>
          ))
        )}
      </div>
    </div>
  );
}

function RailRow({
  account,
  isSelected,
  displayName,
}: {
  account: AccountView;
  isSelected: boolean;
  displayName: string;
}) {
  const tone = statusTone(account.status);
  const worst = worstUsedPercent(account);
  const { active } = useScreenShield();
  const { mode: quotaMode } = useQuotaDisplayPreference();
  // The mockup's rail meta line shows "id · provider" for the one row whose name is an alias (so
  // the real id is still visible somewhere), and "provider · status" for every plain-id row (no
  // point repeating the id right below itself).
  const isAliasName = displayName !== account.id;
  const meta = active
    ? `${providerBrandKey(account.provider)} · identity shielded`
    : isAliasName
      ? `${account.id} · ${providerBrandKey(account.provider)}`
      : `${providerBrandKey(account.provider)} · ${account.status.replace(/_/g, " ")}`;

  return (
    <Link
      to={`/accounts/${encodeURIComponent(account.id)}`}
      className={clsx(
        "relative flex items-center gap-2 rounded py-1.5 pl-2.5 pr-2 no-underline",
        isSelected ? "bg-accent/[0.1]" : "hover:bg-muted/60",
      )}
    >
      {isSelected && <span className="absolute inset-y-1 left-0 w-[3px] rounded-full bg-accent" />}
      <span className={clsx("h-[7px] w-[7px] shrink-0 rounded-full", DOT_CLASS[tone])} />
      <span className="min-w-0 flex-1">
        <ShieldedAccount
          id={account.id}
          label={displayName}
          className="block truncate text-[11px] font-medium text-fg"
        />
        <span className="block truncate text-[9px] text-fg opacity-55">{meta}</span>
      </span>
      <div className="h-[4px] w-8 shrink-0 overflow-hidden rounded-full bg-muted">
        {worst !== null && (
          <div
            className={clsx("h-full rounded-full", DOT_CLASS[usageRiskTone(worst)])}
            style={{ width: `${quotaDisplayPercent(worst, quotaMode)}%` }}
          />
        )}
      </div>
    </Link>
  );
}

function RailSkeleton() {
  return (
    <div className="flex flex-col gap-1.5 px-1 py-1">
      {[0, 1, 2, 3, 4].map((i) => (
        <div key={i} className="h-8 animate-pulse rounded bg-muted" />
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------------------------
// Not-found / error panels
// ---------------------------------------------------------------------------------------------

function NotFoundPanel({ id }: { id: string }) {
  return (
    <Card>
      <div className="flex flex-col items-start gap-2 text-[12px]">
        <span className="flex items-center gap-2 text-fg opacity-80">
          <AlertTriangle className="h-4 w-4 shrink-0 text-warn" strokeWidth={1.9} />
          {id ? (
            <>
              No account named{" "}
              <b className="font-mono">
                <ShieldedAccount id={id} label={id} />
              </b>{" "}
              was found.
            </>
          ) : (
            "No account selected."
          )}
        </span>
        <Link
          to="/accounts"
          className="text-[11px] font-medium text-accent no-underline hover:underline"
        >
          Back to Accounts
        </Link>
      </div>
    </Card>
  );
}

function ErrorPanel({ message, onRetry }: { message: string; onRetry: () => void }) {
  return (
    <Card>
      <div className="flex flex-wrap items-center justify-between gap-3">
        <span className="flex items-center gap-2 text-[12px] text-error">
          <AlertTriangle className="h-4 w-4 shrink-0" strokeWidth={1.9} />
          {message}
        </span>
        <button
          type="button"
          onClick={onRetry}
          className="shrink-0 rounded border border-border px-2.5 py-1 text-[11px] text-fg opacity-80 hover:opacity-100"
        >
          Retry
        </button>
      </div>
    </Card>
  );
}

function DetailSkeleton() {
  return (
    <div className="flex flex-col gap-3">
      <div className="flex items-center gap-2">
        <div className="h-[9px] w-[9px] animate-pulse rounded-full bg-muted" />
        <div className="h-4 w-32 animate-pulse rounded bg-muted" />
        <div className="h-4 w-14 animate-pulse rounded bg-muted" />
        <div className="h-4 w-16 animate-pulse rounded bg-muted" />
      </div>
      <div className="h-3 w-64 animate-pulse rounded bg-muted" />
      <Grid>
        <Col span={5}>
          <Card>
            <div className="h-52 animate-pulse rounded bg-muted" />
          </Card>
        </Col>
        <Col span={7}>
          <Card>
            <div className="h-52 animate-pulse rounded bg-muted" />
          </Card>
        </Col>
        <Col span={12}>
          <Card>
            <div className="h-32 animate-pulse rounded bg-muted" />
          </Card>
        </Col>
      </Grid>
    </div>
  );
}

// ---------------------------------------------------------------------------------------------
// Detail content
// ---------------------------------------------------------------------------------------------

const WINDOW_ORDER: Record<string, number> = { five_hour: 0, weekly: 1 };
const WINDOW_LABEL: Record<string, string> = { five_hour: "5-hour", weekly: "Weekly" };

function labelForWindow(window: string): string {
  return WINDOW_LABEL[window] ?? window.replace(/_/g, " ");
}

function tokenStatusText(
  th: TokenHealthView,
  nowMs: number,
): { text: string; className: string } {
  if (th.access_state === "expired") return { text: "Expired", className: "text-error" };
  if (th.access_state === "missing") return { text: "Missing", className: "text-warn" };
  return { text: `Valid · ${countdown(th.access_expires_at, nowMs)}`, className: "text-success" };
}

interface DetailContentProps {
  detail: AccountDetailView;
  nowMs: number;
  cycleLabel: string | null;
  onPrev: () => void;
  onNext: () => void;
  canCycle: boolean;
  trendsLoading: boolean;
  trendsError: boolean;
  onTrendsRetry: () => void;
  primary: Point[];
  secondary: Point[];
  forecast: DepletionForecast | null;
}

function DetailContent({
  detail,
  nowMs,
  cycleLabel,
  onPrev,
  onNext,
  canCycle,
  trendsLoading,
  trendsError,
  onTrendsRetry,
  primary,
  secondary,
  forecast,
}: DetailContentProps) {
  const { identity } = detail;
  const displayName = identity.alias ?? identity.id;
  const tone = statusTone(detail.status);
  const token = tokenStatusText(detail.token_status, nowMs);
  const quotaRows = detail.quota_windows
    .filter((row) => row.window !== "five_hour" || quotaWindowIsPresent(row))
    .sort((a, b) => (WINDOW_ORDER[a.window] ?? 99) - (WINDOW_ORDER[b.window] ?? 99));
  const hasFiveHourWindow = quotaRows.some((row) => row.window === "five_hour");
  const actions = useAccountActions();
  const navigate = useNavigate();
  const { active } = useScreenShield();
  const { mode: quotaMode } = useQuotaDisplayPreference();
  const [reauthOpen, setReauthOpen] = useState(false);
  const weekly = quotaRows.find((row) => row.window === "weekly") ?? quotaRows[0] ?? null;
  const weeklyDisplay = weekly ? quotaDisplayPercent(weekly.used_percent, quotaMode) : null;

  return (
    <div className="flex flex-col gap-4">
      <Card className="gap-4 bg-gradient-to-br from-card via-card to-accent/[0.035]">
        <div className="flex flex-wrap items-start gap-3">
          <div className="flex min-w-0 flex-1 items-start gap-3">
            <span
              className={clsx(
                "mt-1.5 h-2.5 w-2.5 shrink-0 rounded-full ring-4 ring-muted",
                DOT_CLASS[tone],
              )}
            />
            <div className="min-w-0">
              <div className="flex flex-wrap items-center gap-2">
                <ShieldedAccount
                  id={identity.id}
                  label={displayName}
                  className="truncate text-[17px] font-bold tracking-tight text-fg"
                />
                <button
                  type="button"
                  title={active ? "Show identities to edit alias" : "Edit alias"}
                  disabled={active}
                  onClick={() => actions.openRename({ id: identity.id, alias: identity.alias })}
                  className="inline-flex h-7 w-7 shrink-0 items-center justify-center rounded-md border border-transparent text-fg opacity-55 hover:border-border hover:bg-muted hover:opacity-100 disabled:cursor-not-allowed disabled:opacity-25"
                >
                  <Pencil className="h-3.5 w-3.5" strokeWidth={1.8} />
                </button>
                <ProviderTag provider={identity.provider} />
                <StatusPill status={detail.status} />
              </div>
              <p className="mt-1.5 text-[11px] leading-relaxed text-fg opacity-60">
                {active ? (
                  "Identity and workspace shielded"
                ) : (
                  <>
                    {identity.email}
                    {identity.workspace_label && <> · {identity.workspace_label}</>}
                  </>
                )}
                {" · "}
                <span className="font-semibold text-fg opacity-90">{identity.plan_type}</span> plan
                {" · "}
                <span className="font-semibold text-fg opacity-90">
                  {identity.pools.length > 0 ? identity.pools.join(", ") : "unpooled"}
                </span>
                {identity.seat_type && <> · {identity.seat_type} seat</>}
              </p>
            </div>
          </div>

          {cycleLabel && (
            <div className="flex shrink-0 items-center gap-1.5 rounded-lg border border-border bg-bg/55 p-1 text-[10.5px] text-fg">
              <span className="px-1.5 opacity-55">{cycleLabel}</span>
              <button type="button" onClick={onPrev} disabled={!canCycle} aria-label="Previous account" className="flex h-7 w-7 items-center justify-center rounded-md text-fg opacity-65 hover:bg-muted hover:opacity-100 disabled:opacity-30">
                <ChevronLeft className="h-3.5 w-3.5" strokeWidth={2} />
              </button>
              <button type="button" onClick={onNext} disabled={!canCycle} aria-label="Next account" className="flex h-7 w-7 items-center justify-center rounded-md text-fg opacity-65 hover:bg-muted hover:opacity-100 disabled:opacity-30">
                <ChevronRight className="h-3.5 w-3.5" strokeWidth={2} />
              </button>
            </div>
          )}
        </div>

        <div className="grid grid-cols-2 gap-2 border-t border-border/70 pt-3 xl:grid-cols-4">
          <HealthMetric label={`Weekly ${quotaDisplayLabel(quotaMode)}`} value={weeklyDisplay === null ? "—" : pct(weeklyDisplay)} hint={weekly ? `resets ${countdown(weekly.reset_at, nowMs)}` : "no quota reported"} tone={weekly ? usageRiskTone(weekly.used_percent) : undefined} />
          <HealthMetric label="Token" value={token.text} hint="access credential" tone={detail.token_status.access_state === "valid" ? "ok" : detail.token_status.access_state === "expired" ? "error" : "warn"} />
          <HealthMetric label="Requests" value={compactNum(detail.request_totals.request_count)} hint="all time" />
          <HealthMetric label="Tokens routed" value={compactNum(detail.request_totals.total_tokens)} hint="all time" />
        </div>
      </Card>

      <Grid>
        <Col span={5}>
          <Card>
            <div className="text-[10px] uppercase tracking-wide text-fg opacity-60">
              Usage / quota
            </div>
            {quotaRows.length === 0 ? (
              <p className="mt-2 text-[11px] text-fg opacity-50">No quota windows reported yet.</p>
            ) : (
              <div className="mt-1.5">
                {quotaRows.map((row) => (
                  <QuotaRow key={row.window} row={row} nowMs={nowMs} />
                ))}
              </div>
            )}

            <div className="mt-2.5 border-t border-border pt-2 text-[9.5px] text-fg opacity-55">
              {compactNum(detail.request_totals.total_tokens)} tok ·{" "}
              {compactNum(detail.request_totals.request_count)} req{" "}
              <span className="opacity-70">(all-time)</span>
            </div>

            <div className="mt-3 text-[10px] uppercase tracking-wide text-fg opacity-60">
              Token status
            </div>
            <div className="mt-1.5 flex items-center justify-between text-[11px]">
              <span className="flex items-center gap-1.5 text-fg opacity-70">
                <Key className="h-3 w-3 shrink-0" strokeWidth={2} />
                Access token
              </span>
              <b className={token.className}>{token.text}</b>
            </div>
          </Card>
        </Col>

        <Col span={7}>
          <TrendCard
            isLoading={trendsLoading}
            isError={trendsError}
            onRetry={onTrendsRetry}
            primary={primary}
            secondary={secondary}
            forecast={forecast}
            hasFiveHourWindow={hasFiveHourWindow}
          />
        </Col>

        <Col span={12}>
          <ActionsCard
            id={identity.id}
            alias={identity.alias}
            status={detail.status}
            routingPolicy={detail.routing_policy}
            trustedAccess={detail.security_work_authorized}
            pools={identity.pools}
            actions={actions}
            onReauthenticate={() => setReauthOpen(true)}
            onDeleted={() => navigate("/accounts")}
          />
        </Col>
      </Grid>

      {actions.dialogs}
      <CodexOnboardingDialog open={reauthOpen} onOpenChange={setReauthOpen} />
    </div>
  );
}

function HealthMetric({
  label,
  value,
  hint,
  tone,
}: {
  label: string;
  value: string;
  hint: string;
  tone?: StatusTone;
}) {
  return (
    <div className="rounded-lg border border-border/70 bg-bg/45 px-3 py-2.5">
      <div className="text-[9px] font-semibold uppercase tracking-[0.08em] text-fg opacity-45">
        {label}
      </div>
      <div className={clsx("mt-1 truncate text-[13px] font-bold text-fg", tone && TEXT_TONE_CLASS[tone])}>
        {value}
      </div>
      <div className="mt-0.5 text-[9.5px] text-fg opacity-45">{hint}</div>
    </div>
  );
}

function QuotaRow({ row, nowMs }: { row: UsageWindowView; nowMs: number }) {
  const clamped = Math.max(0, Math.min(100, row.used_percent));
  const tone = usageRiskTone(clamped);
  const { mode } = useQuotaDisplayPreference();
  const displayed = quotaDisplayPercent(clamped, mode);
  return (
    <div className="mt-2 first:mt-0">
      <div className="flex items-center justify-between text-[10.5px]">
        <span className="text-fg opacity-70">
          {labelForWindow(row.window)} {quotaDisplayLabel(mode)}
        </span>
        <b className={TEXT_TONE_CLASS[tone]}>{pct(displayed)}</b>
      </div>
      <div className="mt-1 h-[6px] overflow-hidden rounded-full bg-muted">
        <div
          className={clsx("h-full rounded-full", DOT_CLASS[tone])}
          style={{ width: `${displayed}%` }}
        />
      </div>
      <div className="mt-0.5 text-[9px] text-fg opacity-50">Reset {countdown(row.reset_at, nowMs)}</div>
    </div>
  );
}

// ---------------------------------------------------------------------------------------------
// 7-day trend chart — GET /api/accounts/{id}/trends. `primary` is the 5-hour window, `secondary`
// the weekly window (confirmed against polyflare-store's account.rs — the same "primary"/
// "secondary" labels usage_history stores duration-resolved windows under). Built with recharts
// directly rather than the shared `Sparkline` atom, which only supports a single series — same
// "build a bespoke chart when the shared atom doesn't fit" precedent Accounts.tsx already
// established for its usage bars.
// ---------------------------------------------------------------------------------------------

interface TrendRow {
  t: number;
  primary: number | null;
  secondary: number | null;
}

/** Merges two independently-sampled `{t, v}` series (5h samples land far more often than weekly
 * ones) onto one shared, sorted timestamp axis, forward-filling each series' last-known value at
 * every timestamp the OTHER series contributed. Both inputs are ordered oldest-first (see
 * read_api.rs's `account_trends_handler` docs), so a single forward pass suffices. A series has no
 * value (`null`) only before its own first sample — never fabricated backward. */
function mergeTrend(primary: Point[], secondary: Point[]): TrendRow[] {
  const timestamps = Array.from(
    new Set<number>([...primary.map((p) => p.t), ...secondary.map((p) => p.t)]),
  ).sort((a, b) => a - b);

  let pi = 0;
  let si = 0;
  let plast: number | null = null;
  let slast: number | null = null;
  const rows: TrendRow[] = [];
  for (const t of timestamps) {
    while (pi < primary.length && primary[pi].t <= t) {
      plast = primary[pi].v;
      pi += 1;
    }
    while (si < secondary.length && secondary[si].t <= t) {
      slast = secondary[si].v;
      si += 1;
    }
    rows.push({ t, primary: plast, secondary: slast });
  }
  return rows;
}

// ---------------------------------------------------------------------------------------------
// Depletion-risk badge — `trends.forecast?.risk_level` (D16 T6). The secondary/weekly-window EWMA
// depletion forecast rebuilt server-side from the same `usage_history` this trend chart already
// plots (see `read_api.rs::account_trends_handler` / `polyflare_core::depletion`). `null` (fewer
// than 2 secondary samples, the rate never establishing, or the window having already reset) hides
// the badge entirely rather than rendering a misleading "safe" default.
// ---------------------------------------------------------------------------------------------

/** safe = muted, warning = warn/gold, danger = flare-amber accent, critical = red — an escalating
 * ramp one step past `PACE_STATUS_CLASS` (Overview.tsx), consistent with this file's `RiskLevel`. */
const RISK_CLASS: Record<RiskLevel, string> = {
  safe: "bg-muted text-fg opacity-70",
  warning: "bg-warn/15 text-warn",
  danger: "bg-accent/15 text-accent",
  critical: "bg-error/15 text-error",
};

function DepletionRiskBadge({ forecast }: { forecast: DepletionForecast }) {
  return (
    <span
      className={clsx(
        "inline-block whitespace-nowrap rounded px-1.5 py-0.5 text-[9px] font-bold normal-case tracking-normal",
        RISK_CLASS[forecast.risk_level],
      )}
      title={`Projected to reach ${pct(Math.min(100, forecast.risk * 100))} of the weekly window by reset`}
    >
      depletion · {forecast.risk_level}
    </span>
  );
}

function TrendCard({
  isLoading,
  isError,
  onRetry,
  primary,
  secondary,
  forecast,
  hasFiveHourWindow,
}: {
  isLoading: boolean;
  isError: boolean;
  onRetry: () => void;
  primary: Point[];
  secondary: Point[];
  forecast: DepletionForecast | null;
  hasFiveHourWindow: boolean;
}) {
  const { mode } = useQuotaDisplayPreference();
  const trendLabel = `7-day ${quotaDisplayLabel(mode)} trend`;
  if (isLoading) {
    return (
      <Card>
        <div className="text-[10px] uppercase tracking-wide text-fg opacity-60">
          {trendLabel}
        </div>
        <div className="mt-2 h-52 animate-pulse rounded bg-muted" />
      </Card>
    );
  }
  if (isError) {
    return (
      <Card>
        <div className="text-[10px] uppercase tracking-wide text-fg opacity-60">
          {trendLabel}
        </div>
        <div className="mt-2 flex flex-1 flex-col items-start justify-center gap-1.5 text-[11px] text-error">
          <span className="flex items-center gap-1.5">
            <AlertTriangle className="h-3.5 w-3.5 shrink-0" strokeWidth={1.9} />
            Couldn&apos;t load trend data.
          </span>
          <button
            type="button"
            onClick={onRetry}
            className="rounded border border-border px-2 py-0.5 text-[10.5px] text-fg opacity-80 hover:opacity-100"
          >
            Retry
          </button>
        </div>
      </Card>
    );
  }

  const visiblePrimary = hasFiveHourWindow ? primary : [];
  const hasData = visiblePrimary.length > 0 || secondary.length > 0;
  const merged = mergeTrend(visiblePrimary, secondary).map((row) => ({
    ...row,
    primary: row.primary === null ? null : quotaDisplayPercent(row.primary, mode),
    secondary: row.secondary === null ? null : quotaDisplayPercent(row.secondary, mode),
  }));

  return (
    <Card>
      <div className="flex items-center justify-between text-[10px] uppercase tracking-wide text-fg opacity-60">
        <span>{trendLabel}</span>
        <span className="flex items-center gap-2">
          {forecast && <DepletionRiskBadge forecast={forecast} />}
          <span className="flex items-center gap-3 normal-case tracking-normal text-[9px] opacity-80">
            {hasFiveHourWindow && <LegendSwatch colorClass="bg-codex" label="5h" />}
            <LegendSwatch colorClass="bg-claude" label="Weekly" />
          </span>
        </span>
      </div>
      {!hasData ? (
        <p className="mt-2 text-[11px] text-fg opacity-50">No trend data yet.</p>
      ) : (
        <div className="mt-2" style={{ height: 190 }}>
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={merged} margin={{ top: 4, right: 6, bottom: 0, left: -18 }}>
              <defs>
                {hasFiveHourWindow && (
                  <linearGradient id="trend-5h" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor="hsl(var(--codex))" stopOpacity={0.32} />
                    <stop offset="100%" stopColor="hsl(var(--codex))" stopOpacity={0} />
                  </linearGradient>
                )}
                <linearGradient id="trend-weekly" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="hsl(var(--claude))" stopOpacity={0.22} />
                  <stop offset="100%" stopColor="hsl(var(--claude))" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid vertical={false} stroke="hsl(var(--border))" strokeDasharray="3 3" />
              <XAxis dataKey="t" type="number" domain={["dataMin", "dataMax"]} hide />
              <YAxis
                domain={[0, 100]}
                ticks={[0, 50, 100]}
                width={26}
                tick={{ fontSize: 8.5, fill: "hsl(var(--fg))", fillOpacity: 0.6 }}
                axisLine={false}
                tickLine={false}
              />
              <Area
                type="monotone"
                dataKey="secondary"
                stroke="hsl(var(--claude))"
                strokeWidth={1.4}
                fill="url(#trend-weekly)"
                isAnimationActive={false}
                dot={false}
              />
              {hasFiveHourWindow && (
                <Area
                  type="monotone"
                  dataKey="primary"
                  stroke="hsl(var(--codex))"
                  strokeWidth={1.7}
                  fill="url(#trend-5h)"
                  isAnimationActive={false}
                  dot={false}
                />
              )}
            </AreaChart>
          </ResponsiveContainer>
        </div>
      )}
    </Card>
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

// ---------------------------------------------------------------------------------------------
// Actions panel — Phase 3: reproduces `accounts-detail-v2.html`'s reworked 3-column Actions layout
// (per the task brief, which names that mockup specifically for this panel over the master-detail
// mockup's flatter version), now with the in-scope controls (routing policy, trusted access, pool,
// pause/resume, delete) live and wired to the shared `useAccountActions()` hook. Controls with no
// backend field (limit warm-up, rate-limit resets, force probe, re-authenticate, export auth) stay
// plain disabled placeholders — no event handler, real HTML `disabled` attribute.
// ---------------------------------------------------------------------------------------------

function ActionsCard({
  id,
  alias,
  status,
  routingPolicy,
  trustedAccess,
  pools,
  actions,
  onReauthenticate,
  onDeleted,
}: {
  id: string;
  alias: string | null;
  status: string;
  routingPolicy: string;
  trustedAccess: boolean;
  pools: string[];
  actions: AccountActionsApi;
  onReauthenticate: () => void;
  onDeleted: () => void;
}) {
  const paused = status === "paused";
  const { active } = useScreenShield();
  const displayLabel = active ? routePseudonym(id) : alias ?? id;
  return (
    <Card>
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div>
          <div className="text-[10px] font-semibold uppercase tracking-[0.08em] text-fg opacity-65">
            Account controls
          </div>
          <p className="mt-1 text-[10px] text-fg opacity-45">
            Routing changes apply to the next eligible selection.
          </p>
        </div>
        <span className={clsx("rounded-full px-2 py-1 text-[9px] font-semibold", paused ? "bg-warn/10 text-warn" : "bg-success/10 text-success")}>
          {paused ? "Manually paused" : "Ready for routing"}
        </span>
      </div>

      <div className="mt-4 grid grid-cols-1 gap-5 lg:grid-cols-2">
        {/* Configuration */}
        <div className="flex flex-col gap-3 rounded-lg border border-border/70 bg-bg/35 p-3">
          <div className="text-[9px] font-semibold uppercase tracking-wide text-fg opacity-50">
            Routing &amp; access
          </div>

          <ConfigRow icon={Route} label="Routing policy">
            <select
              value={routingPolicy}
              onChange={(e) =>
                actions.patch.mutate({ id, body: { routing_policy: e.target.value } })
              }
              className="rounded border border-border bg-bg px-2 py-1 text-[10.5px] text-fg hover:border-accent"
            >
              <option value="normal">normal</option>
              <option value="burn_first">burn_first</option>
              <option value="preserve">preserve</option>
            </select>
          </ConfigRow>

          <ConfigRow icon={ShieldCheck} label="Trusted access" hint="cyber">
            <button
              type="button"
              role="switch"
              aria-checked={trustedAccess}
              onClick={() =>
                actions.patch.mutate({
                  id,
                  body: { security_work_authorized: !trustedAccess },
                })
              }
              className={clsx(
                "relative h-[18px] w-[34px] shrink-0 rounded-full",
                trustedAccess ? "bg-success/35" : "bg-muted",
              )}
            >
              <span
                className={clsx(
                  "absolute top-[2px] h-[14px] w-[14px] rounded-full",
                  trustedAccess ? "right-[2px] bg-success" : "left-[2px] bg-fg opacity-50",
                )}
              />
            </button>
          </ConfigRow>

          <ConfigRow icon={Layers} label="Routing groups">
            <button
              type="button"
              onClick={() => actions.openSetPool({ id, pools })}
              className="max-w-[220px] truncate rounded border border-border bg-bg px-2 py-1 text-[10.5px] text-fg hover:border-accent"
            >
              {pools.length > 0 ? pools.join(", ") : "unpooled"}
            </button>
          </ConfigRow>

          <ConfigRow icon={Flame} label="Warm-up policy">
            <span className="rounded bg-muted px-2 py-1 text-[9px] text-fg opacity-50">
              server default
            </span>
          </ConfigRow>
        </div>

        {/* Operations + Danger */}
        <div className="flex flex-col gap-3 rounded-lg border border-border/70 bg-bg/35 p-3">
          <div className="text-[9px] font-semibold uppercase tracking-wide text-fg opacity-50">
            Lifecycle
          </div>
          <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
            <OpButton
              icon={paused ? Play : Pause}
              label={paused ? "Resume" : "Pause"}
              onClick={() =>
                actions.patch.mutate({ id, body: { status: paused ? "active" : "paused" } })
              }
            />
            <OpButton icon={LogIn} label="Re-authenticate" onClick={onReauthenticate} />
          </div>
          <div className="rounded-md border border-border/60 bg-muted/35 px-3 py-2 text-[9.5px] leading-relaxed text-fg opacity-50">
            Force probe and credential export stay unavailable until PolyFlare has explicit,
            audited backend operations for them.
          </div>
          <div className="flex flex-wrap items-center justify-between gap-2 border-t border-border/70 pt-3">
            <span className="text-[9px] font-semibold uppercase tracking-wide text-error opacity-75">
              Danger zone
            </span>
            <button
              type="button"
              onClick={() => actions.openDelete({ id, label: displayLabel, onDeleted })}
              className="flex items-center gap-1.5 rounded-md border border-error/35 bg-error/10 px-3 py-1.5 text-[10.5px] font-semibold text-error hover:bg-error/15"
            >
              <Trash2 className="h-3.5 w-3.5" />
              Delete account
            </button>
          </div>
        </div>
      </div>
    </Card>
  );
}

function ConfigRow({
  icon: Icon,
  label,
  hint,
  children,
}: {
  icon: LucideIcon;
  label: string;
  hint?: string;
  children: ReactNode;
}) {
  return (
    <div className="flex items-center justify-between gap-2">
      <span className="flex items-center gap-1.5 text-[11px] text-fg opacity-80">
        <Icon className="h-3.5 w-3.5 shrink-0 opacity-60" strokeWidth={1.8} />
        {label}
        {hint && <span className="text-[9px] text-fg opacity-40">· {hint}</span>}
      </span>
      {children}
    </div>
  );
}

function OpButton({
  icon: Icon,
  label,
  danger,
  onClick,
  disabled = !onClick,
}: {
  icon: LucideIcon;
  label: string;
  danger?: boolean;
  onClick?: () => void;
  disabled?: boolean;
}) {
  return (
    <button
      type="button"
      disabled={disabled}
      onClick={onClick}
      className={clsx(
        "flex items-center justify-center gap-1.5 rounded border px-2.5 py-1.5 text-[10.5px]",
        disabled ? "cursor-not-allowed" : "hover:opacity-100 hover:border-accent",
        danger
          ? clsx("border-error/30 bg-error/10 text-error", disabled ? "opacity-60" : "opacity-90")
          : clsx("border-border bg-muted text-fg", disabled ? "opacity-50" : "opacity-80"),
      )}
    >
      <Icon className="h-3 w-3 shrink-0" strokeWidth={1.9} />
      {label}
    </button>
  );
}
