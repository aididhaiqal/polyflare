// The Accounts page: every account's live status/quota/token-health, in a Cards ⇄ List toggle —
// `GET /api/accounts` (`useAccounts()`) is the ONLY read endpoint this page consumes. Task 7 turns
// it from read-only into a control surface: a per-row kebab (⋯) menu (pause/resume, routing
// policy, security toggle, rename, set pool, delete) backed by the shared `useAccountActions` hook
// (`../lib/useAccountActions`) — one instance per page, reused verbatim by Task 8's AccountDetail.
//
// Field mapping (see read_api.rs::AccountView / src/lib/api.ts's mirror):
//   status dot + StatusPill  <- a.status (via ui/StatusPill's statusTone)
//   name                     <- a.alias ?? a.id (Task 4b put `alias` on AccountView's wire; a.id
//                                stays visible as a secondary muted detail alongside it)
//   provider chip            <- a.provider (via ui/ProviderTag)
//   email / plan / pool      <- a.email, a.plan_type, a.pool (null -> "unpooled")
//   5-hour / weekly bars     <- a.five_hour / a.weekly (WindowView | null — Claude accounts have no
//                                five_hour window; rendered as a dash row, not omitted, so the card
//                                shape stays a stable 2-row grid across accounts)
//   token-health footer      <- a.token_health {access_state, access_expires_at}
//   24h request count        <- a.request_count_24h
//   kebab menu               <- AccountRowMenu (pause/resume, routing_policy, security_work_
//                                authorized, rename, set pool, delete — all via AccountActionsApi)
//
// Usage-bar coloring is RISK-based (ok/warn/error by used_percent), not provider-brand-colored —
// same deliberate deviation from the mockup that task-5b's Overview account-health table already
// made (see that task's report): the mockup colors one example row by provider brand
// inconsistently with its own risk-coloring of every other row, so this page uses a single
// consistent scale instead.
import { useEffect, useState, type ReactNode } from "react";
import { Link, useNavigate, useSearchParams } from "react-router-dom";
import * as Select from "@radix-ui/react-select";
import * as Tabs from "@radix-ui/react-tabs";
import clsx from "clsx";

import type { AccountView, TokenHealthView, WindowView } from "../lib/api";
import { compactNum, countdown, pct } from "../lib/format";
import { useAccounts } from "../lib/queries";
import { useAccountActions, type AccountActionsApi } from "../lib/useAccountActions";
import { ActionMenu } from "../ui/ActionMenu";
import { Card } from "../ui/Card";
import {
  AlertTriangle,
  ChevronDown,
  Key,
  Layers,
  LayoutGrid,
  List as ListIcon,
  Pause,
  Pencil,
  Play,
  ShieldCheck,
  Trash2,
} from "../ui/icons";
import { providerBrandKey, ProviderTag } from "../ui/ProviderTag";
import { StatusPill, statusTone, type StatusTone } from "../ui/StatusPill";

type ViewMode = "cards" | "list";
type ProviderFilter = "all" | "codex" | "claude";

const PROVIDER_FILTERS: Array<{ value: ProviderFilter; label: string }> = [
  { value: "all", label: "All" },
  { value: "codex", label: "Codex" },
  { value: "claude", label: "Claude" },
];

function matchesProvider(provider: string, filter: ProviderFilter): boolean {
  return filter === "all" || providerBrandKey(provider) === filter;
}

/** Sentinel `Select.Item` values — Radix Select forbids an empty-string value (reserved for
 * clearing), so the "all pools" / "no pool" choices need non-empty placeholders. */
const ALL_POOLS = "__all_pools__";
const UNPOOLED = "__unpooled__";

function matchesPool(pool: string | null, filter: string): boolean {
  if (filter === ALL_POOLS) return true;
  if (filter === UNPOOLED) return pool === null;
  return pool === filter;
}

/** Shared tone->fill-color map for every usage bar on this page (status dot, 5-hour/weekly bars,
 * list-view mini bars) — same values as `ui/StatusPill.tsx`'s tone classes, kept local since this
 * page derives tones for bars (a per-window risk level), not just account status. */
const TONE_BAR_CLASS: Record<StatusTone, string> = {
  ok: "bg-success",
  warn: "bg-warn",
  error: "bg-error",
};

/** Usage-risk thresholds for the 5-hour/weekly bars: how close to exhausted a window is. Mirrors
 * `Overview.tsx`'s `usageRiskTone` (same reasoning, duplicated per that page's own established
 * per-page-helper convention rather than a shared atom for a 3-line pure function). */
function usageRiskTone(usedPercent: number): StatusTone {
  if (usedPercent >= 90) return "error";
  if (usedPercent >= 70) return "warn";
  return "ok";
}

function tokenHealthLabel(
  th: TokenHealthView,
  nowMs: number,
): { text: string; className: string } {
  if (th.access_state === "expired") return { text: "token expired", className: "text-error" };
  if (th.access_state === "missing") return { text: "token missing", className: "text-warn" };
  return {
    text: `token ok · ${countdown(th.access_expires_at, nowMs)}`,
    className: "text-fg opacity-70",
  };
}

export function Accounts() {
  const { data, isLoading, isError, error, refetch } = useAccounts();
  // One shared instance for the whole list — both the card grid and the table pass this down to
  // their row-level `AccountRowMenu`, and its dialogs render exactly once below.
  const actions = useAccountActions();
  const [searchParams, setSearchParams] = useSearchParams();
  const view: ViewMode = searchParams.get("view") === "list" ? "list" : "cards";
  const [providerFilter, setProviderFilter] = useState<ProviderFilter>("all");
  const [poolFilter, setPoolFilter] = useState<string>(ALL_POOLS);

  // Ticks countdowns (quota-window resets, token-expiry countdowns) between the 30s useAccounts()
  // poll — same pattern Overview.tsx uses for its own countdown/relative-time math.
  const [nowMs, setNowMs] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNowMs(Date.now()), 5000);
    return () => clearInterval(id);
  }, []);

  function setView(next: ViewMode) {
    const params = new URLSearchParams(searchParams);
    if (next === "cards") {
      params.delete("view");
    } else {
      params.set("view", next);
    }
    setSearchParams(params, { replace: true });
  }

  if (isLoading) return <AccountsSkeleton />;

  if (isError) {
    return (
      <div className="flex flex-col gap-3">
        <PageHeader />
        <Card>
          <div className="flex flex-wrap items-center justify-between gap-3">
            <span className="flex items-center gap-2 text-[12px] text-error">
              <AlertTriangle className="h-4 w-4 shrink-0" strokeWidth={1.9} />
              Couldn&apos;t load accounts
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

  const accounts = data;
  const totalAccounts = accounts.length;
  const activeCount = accounts.filter((a) => statusTone(a.status) === "ok").length;
  const reauthCount = accounts.filter((a) => statusTone(a.status) === "error").length;

  const namedPools = Array.from(
    new Set(accounts.filter((a) => a.pool !== null).map((a) => a.pool as string)),
  ).sort();
  const hasUnpooled = accounts.some((a) => a.pool === null);
  const poolCount = namedPools.length + (hasUnpooled ? 1 : 0);

  const filtered = accounts.filter(
    (a) => matchesProvider(a.provider, providerFilter) && matchesPool(a.pool, poolFilter),
  );

  return (
    <div className="flex flex-col gap-3">
      <PageHeader
        subtitle={`${totalAccounts} ${totalAccounts === 1 ? "account" : "accounts"} · ${activeCount} active · ${reauthCount} reauth · ${poolCount} ${poolCount === 1 ? "pool" : "pools"}`}
        actions={
          <div className="flex flex-wrap items-center gap-2">
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

            <PoolSelect
              value={poolFilter}
              onChange={setPoolFilter}
              namedPools={namedPools}
              hasUnpooled={hasUnpooled}
            />

            <Tabs.Root value={view} onValueChange={(v) => setView(v as ViewMode)}>
              <Tabs.List className="flex shrink-0 overflow-hidden rounded border border-border bg-card text-[10.5px]">
                <Tabs.Trigger
                  value="cards"
                  className={clsx(
                    "flex items-center gap-1 px-2.5 py-1",
                    view === "cards"
                      ? "bg-accent/[0.12] font-medium text-accent"
                      : "text-fg opacity-60 hover:opacity-100",
                  )}
                >
                  <LayoutGrid className="h-3 w-3" strokeWidth={2} />
                  Cards
                </Tabs.Trigger>
                <Tabs.Trigger
                  value="list"
                  className={clsx(
                    "flex items-center gap-1 px-2.5 py-1",
                    view === "list"
                      ? "bg-accent/[0.12] font-medium text-accent"
                      : "text-fg opacity-60 hover:opacity-100",
                  )}
                >
                  <ListIcon className="h-3 w-3" strokeWidth={2} />
                  List
                </Tabs.Trigger>
              </Tabs.List>
            </Tabs.Root>
          </div>
        }
      />

      {accounts.length === 0 ? (
        <Card>
          <p className="text-[11px] text-fg opacity-50">No accounts configured yet.</p>
        </Card>
      ) : filtered.length === 0 ? (
        <Card>
          <p className="text-[11px] text-fg opacity-50">No accounts match the current filters.</p>
        </Card>
      ) : view === "cards" ? (
        <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-3">
          {filtered.map((a) => (
            <AccountCard key={a.id} account={a} nowMs={nowMs} actions={actions} />
          ))}
        </div>
      ) : (
        <AccountsTable accounts={filtered} nowMs={nowMs} actions={actions} />
      )}

      {actions.dialogs}
    </div>
  );
}

// ---------------------------------------------------------------------------------------------
// Row kebab menu — shared by both the card and table views (Task 7). Discrete actions (pause/
// resume, routing-policy pick, security toggle) fire the shared patch mutation directly; rename/
// set-pool/delete open one of `useAccountActions`'s confirmation dialogs instead.
// ---------------------------------------------------------------------------------------------

function AccountRowMenu({ account: a, actions }: { account: AccountView; actions: AccountActionsApi }) {
  const paused = a.status === "paused";
  return (
    <ActionMenu label={`Actions for ${a.alias ?? a.id}`}>
      <ActionMenu.Item
        icon={paused ? Play : Pause}
        onSelect={() => actions.patch.mutate({ id: a.id, body: { status: paused ? "active" : "paused" } })}
      >
        {paused ? "Resume" : "Pause"}
      </ActionMenu.Item>
      <ActionMenu.Separator />
      <ActionMenu.Label>Routing policy</ActionMenu.Label>
      {(["normal", "burn_first", "preserve"] as const).map((p) => (
        <ActionMenu.CheckItem
          key={p}
          checked={a.routing_policy === p}
          onSelect={() => actions.patch.mutate({ id: a.id, body: { routing_policy: p } })}
        >
          {p}
        </ActionMenu.CheckItem>
      ))}
      <ActionMenu.Separator />
      <ActionMenu.Item
        icon={ShieldCheck}
        onSelect={() =>
          actions.patch.mutate({ id: a.id, body: { security_work_authorized: !a.security_work_authorized } })
        }
      >
        {a.security_work_authorized ? "Revoke trusted access" : "Grant trusted access"}
      </ActionMenu.Item>
      <ActionMenu.Item icon={Pencil} onSelect={() => actions.openRename({ id: a.id, alias: a.alias })}>
        Rename…
      </ActionMenu.Item>
      <ActionMenu.Item icon={Layers} onSelect={() => actions.openSetPool({ id: a.id, pool: a.pool })}>
        Set pool…
      </ActionMenu.Item>
      <ActionMenu.Separator />
      <ActionMenu.Item
        icon={Trash2}
        danger
        onSelect={() => actions.openDelete({ id: a.id, label: a.alias ?? a.id })}
      >
        Delete…
      </ActionMenu.Item>
    </ActionMenu>
  );
}

function PageHeader({ subtitle, actions }: { subtitle?: string; actions?: ReactNode }) {
  return (
    <div className="flex flex-wrap items-center justify-between gap-3">
      <div>
        <h1 className="text-lg font-semibold text-fg">Accounts</h1>
        {subtitle && <p className="mt-0.5 text-[11px] text-fg opacity-60">{subtitle}</p>}
      </div>
      {actions}
    </div>
  );
}

function PoolSelect({
  value,
  onChange,
  namedPools,
  hasUnpooled,
}: {
  value: string;
  onChange: (v: string) => void;
  namedPools: string[];
  hasUnpooled: boolean;
}) {
  const itemClass =
    "cursor-pointer select-none rounded px-2.5 py-1 text-fg opacity-80 outline-none data-[highlighted]:bg-muted data-[highlighted]:opacity-100";
  return (
    <Select.Root value={value} onValueChange={onChange}>
      <Select.Trigger className="flex shrink-0 items-center gap-1.5 rounded border border-border bg-card px-2.5 py-1 text-[10.5px] text-fg opacity-80 outline-none hover:opacity-100 focus:opacity-100">
        <span className="opacity-60">Pool:</span>
        <Select.Value />
        <Select.Icon>
          <ChevronDown className="h-3 w-3" strokeWidth={2} />
        </Select.Icon>
      </Select.Trigger>
      <Select.Portal>
        <Select.Content
          position="popper"
          sideOffset={4}
          className="z-50 overflow-hidden rounded border border-border bg-card text-[10.5px] shadow-lg"
        >
          <Select.Viewport className="p-1">
            <Select.Item value={ALL_POOLS} className={itemClass}>
              <Select.ItemText>all</Select.ItemText>
            </Select.Item>
            {namedPools.map((p) => (
              <Select.Item key={p} value={p} className={itemClass}>
                <Select.ItemText>{p}</Select.ItemText>
              </Select.Item>
            ))}
            {hasUnpooled && (
              <Select.Item value={UNPOOLED} className={itemClass}>
                <Select.ItemText>unpooled</Select.ItemText>
              </Select.Item>
            )}
          </Select.Viewport>
        </Select.Content>
      </Select.Portal>
    </Select.Root>
  );
}

// ---------------------------------------------------------------------------------------------
// Card view
// ---------------------------------------------------------------------------------------------

/** One 5-hour/weekly row inside an account card: label, risk-toned bar, `used%  ·  countdown`. A
 * `null` window (upstream isn't reporting it — e.g. Claude has no five_hour window) renders as a
 * dash row rather than being omitted, so every card keeps the same 2-row shape. */
function CardUsageRow({
  label,
  window,
  nowMs,
}: {
  label: string;
  window: WindowView | null;
  nowMs: number;
}) {
  if (!window) {
    return (
      <div className="flex items-center gap-2 text-[9.5px]">
        <span className="w-11 shrink-0 text-fg opacity-60">{label}</span>
        <div className="h-1.5 flex-1 rounded-full bg-muted" />
        <span className="shrink-0 text-fg opacity-40">—</span>
      </div>
    );
  }
  const clamped = Math.max(0, Math.min(100, window.used_percent));
  return (
    <div className="flex items-center gap-2 text-[9.5px]">
      <span className="w-11 shrink-0 text-fg opacity-60">{label}</span>
      <div className="h-1.5 flex-1 overflow-hidden rounded-full bg-muted">
        <div
          className={clsx("h-full rounded-full", TONE_BAR_CLASS[usageRiskTone(clamped)])}
          style={{ width: `${clamped}%` }}
        />
      </div>
      <span className="shrink-0 whitespace-nowrap text-right text-fg opacity-70">
        {pct(clamped)} · {countdown(window.reset_at, nowMs)}
        {window.stale && <span className="text-warn"> · stale</span>}
      </span>
    </div>
  );
}

function AccountCard({
  account: a,
  nowMs,
  actions,
}: {
  account: AccountView;
  nowMs: number;
  actions: AccountActionsApi;
}) {
  const tone = statusTone(a.status);
  const token = tokenHealthLabel(a.token_health, nowMs);

  return (
    // The kebab is a SIBLING overlay, not nested inside the `<Link>` (an interactive control can't
    // nest inside an `<a>`, and it would fight the card's own click-through navigation). `h-full` on
    // both this wrapper and the `Link` keeps the stretch-to-row-height behavior the grid relied on
    // before this wrapper existed.
    <div className="relative h-full">
      <Link to={`/accounts/${encodeURIComponent(a.id)}`} className="block h-full no-underline">
        <Card className="h-full gap-2 transition-colors hover:border-accent">
          {/* pr-7 keeps the StatusPill clear of the kebab overlaid at the card's top-right corner */}
          <div className="flex items-center gap-1.5 pr-7">
            <span className={clsx("h-[7px] w-[7px] shrink-0 rounded-full", TONE_BAR_CLASS[tone])} />
            <span className="truncate text-[12.5px] font-semibold text-fg">{a.alias ?? a.id}</span>
            <ProviderTag provider={a.provider} />
            <StatusPill status={a.status} className="ml-auto shrink-0" />
          </div>

          <div className="truncate text-[10px] text-fg opacity-60">
            {a.alias && <span className="opacity-60">{a.id} · </span>}
            {a.email} · <span className="font-medium text-fg opacity-90">{a.plan_type}</span> · pool{" "}
            <span className="font-medium text-fg opacity-90">{a.pool ?? "unpooled"}</span>
          </div>

          <div className="flex flex-col gap-1">
            <CardUsageRow label="5-hour" window={a.five_hour} nowMs={nowMs} />
            <CardUsageRow label="Weekly" window={a.weekly} nowMs={nowMs} />
          </div>

          <div className="mt-auto flex items-center justify-between gap-2 border-t border-border pt-2 text-[9.5px]">
            <span className={clsx("flex items-center gap-1 whitespace-nowrap", token.className)}>
              <Key className="h-3 w-3 shrink-0" strokeWidth={2} />
              {token.text}
            </span>
            <span className="whitespace-nowrap text-fg opacity-60">
              <span className="font-semibold text-fg opacity-100">
                {compactNum(a.request_count_24h)}
              </span>{" "}
              reqs 24h
            </span>
          </div>
        </Card>
      </Link>
      <div className="absolute right-2 top-2 z-10">
        <AccountRowMenu account={a} actions={actions} />
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------------------------
// List view
// ---------------------------------------------------------------------------------------------

function ListUsageCell({ window }: { window: WindowView | null }) {
  if (!window) return <span className="text-fg opacity-40">—</span>;
  const clamped = Math.max(0, Math.min(100, window.used_percent));
  return (
    <div className="flex items-center gap-1.5">
      <div className="h-[5px] w-[46px] shrink-0 overflow-hidden rounded-full bg-muted">
        <div
          className={clsx("h-full rounded-full", TONE_BAR_CLASS[usageRiskTone(clamped)])}
          style={{ width: `${clamped}%` }}
        />
      </div>
      <span className="text-fg opacity-70">{pct(clamped)}</span>
    </div>
  );
}

const TABLE_HEAD_CLASS =
  "px-2 py-1.5 text-left text-[9px] font-medium uppercase tracking-wide text-fg opacity-60";

function AccountsTable({
  accounts,
  nowMs,
  actions,
}: {
  accounts: AccountView[];
  nowMs: number;
  actions: AccountActionsApi;
}) {
  const navigate = useNavigate();
  return (
    <Card>
      <div className="overflow-x-auto">
        <table className="w-full min-w-[760px] border-collapse text-[10.5px]">
          <thead>
            <tr className="border-b border-border">
              <th className={TABLE_HEAD_CLASS}>Account</th>
              <th className={TABLE_HEAD_CLASS}>Provider</th>
              <th className={TABLE_HEAD_CLASS}>Pool</th>
              <th className={TABLE_HEAD_CLASS}>Plan</th>
              <th className={TABLE_HEAD_CLASS}>Status</th>
              <th className={TABLE_HEAD_CLASS}>5-hour</th>
              <th className={TABLE_HEAD_CLASS}>Weekly</th>
              <th className={TABLE_HEAD_CLASS}>Token</th>
              <th className={clsx(TABLE_HEAD_CLASS, "text-right")}>Reqs 24h</th>
              <th className={TABLE_HEAD_CLASS}>
                <span className="sr-only">Actions</span>
              </th>
            </tr>
          </thead>
          <tbody>
            {accounts.map((a) => {
              const tone = statusTone(a.status);
              const token = tokenHealthLabel(a.token_health, nowMs);
              const go = () => navigate(`/accounts/${encodeURIComponent(a.id)}`);
              return (
                <tr
                  key={a.id}
                  role="button"
                  tabIndex={0}
                  onClick={go}
                  onKeyDown={(e) => {
                    if (e.key === "Enter" || e.key === " ") {
                      e.preventDefault();
                      go();
                    }
                  }}
                  className="cursor-pointer border-b border-border/55 last:border-0 hover:bg-muted/60"
                >
                  <td className="whitespace-nowrap px-2 py-1.5">
                    <span
                      className={clsx(
                        "mr-1.5 inline-block h-[7px] w-[7px] rounded-full",
                        TONE_BAR_CLASS[tone],
                      )}
                    />
                    {a.alias ?? a.id}
                    {a.alias && <span className="ml-1.5 text-fg opacity-40">({a.id})</span>}
                  </td>
                  <td className="px-2 py-1.5">
                    <ProviderTag provider={a.provider} />
                  </td>
                  <td className="px-2 py-1.5 text-fg opacity-60">{a.pool ?? "unpooled"}</td>
                  <td className="px-2 py-1.5 text-fg opacity-80">{a.plan_type}</td>
                  <td className="px-2 py-1.5">
                    <StatusPill status={a.status} />
                  </td>
                  <td className="px-2 py-1.5">
                    <ListUsageCell window={a.five_hour} />
                  </td>
                  <td className="px-2 py-1.5">
                    <ListUsageCell window={a.weekly} />
                  </td>
                  <td className={clsx("whitespace-nowrap px-2 py-1.5", token.className)}>
                    {token.text}
                  </td>
                  <td className="px-2 py-1.5 text-right tabular-nums text-fg opacity-80">
                    {compactNum(a.request_count_24h)}
                  </td>
                  <td className="px-2 py-1.5 text-right">
                    <AccountRowMenu account={a} actions={actions} />
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </Card>
  );
}

// ---------------------------------------------------------------------------------------------
// Loading skeleton — mirrors the real header + a 3-col card grid so data arriving doesn't reflow.
// ---------------------------------------------------------------------------------------------

function AccountsSkeleton() {
  return (
    <div className="flex flex-col gap-3">
      <div className="flex items-center justify-between gap-3">
        <div>
          <div className="h-[22px] w-24 animate-pulse rounded bg-muted" />
          <div className="mt-1.5 h-3 w-64 animate-pulse rounded bg-muted" />
        </div>
        <div className="h-7 w-64 animate-pulse rounded bg-muted" />
      </div>
      <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-3">
        {[0, 1, 2, 3, 4, 5].map((i) => (
          <Card key={i}>
            <div className="h-[150px] animate-pulse rounded bg-muted" />
          </Card>
        ))}
      </div>
    </div>
  );
}
