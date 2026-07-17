// The Requests page: the dashboard's deepest view into request-log rows — `GET /api/requests`
// (`useRequests(params)`) is the ONLY endpoint this page consumes (plus `useAccounts()`, read-only,
// solely to populate the Account filter's option list with every real account id, not just ids that
// happen to appear on the currently loaded page). The Overview page only summarizes; this page is
// where "was this http or ws" / "what was TTFT/TPS" actually gets answered — see the explicit user
// ask captured in task-9-brief.md: the Transport column is the point of this page.
//
// CONTENT-SAFETY: `request_log` is content-free by construction (see
// `polyflare_store::request_log_repo`'s own module doc) — counts, ids, statuses, timing, token
// COUNTS, never a body/prompt/response/key. This page renders ONLY the real fields on
// `RequestRowView` (read_api.rs:490-508): id, requested_at, provider, method, path, aliased, status,
// duration_ms, account_id, model, reasoning_effort, service_tier, transport, ttft_ms, total_tokens,
// cached_tokens, tps (server-derived). Nothing else exists to render, so nothing else is rendered —
// see the detail-row section below for the specific fields the mockup shows that are NOT real and
// are therefore omitted rather than fabricated.
//
// Field mapping (table columns, in order):
//   Time        <- r.requested_at (unix secs -> local HH:MM:SS; a plain wall-clock display, not
//                   `format.ts::relTime`, which is relative-only — no absolute-clock formatter
//                   exists yet, so a tiny local `formatClock` is added here, same precedent as this
//                   page's other page-local helpers)
//   Account     <- r.account_id (nullable — a request can fail before an account is chosen)
//   Provider    <- r.provider via `ProviderTag` (wire value "codex"/"anthropic", never "claude")
//   Model       <- r.model (+ r.reasoning_effort / r.service_tier as small chips, all nullable)
//   Transport   <- r.transport ("http" today; "ws" once the WS milestone lands — see
//                   observability.rs's own doc comment on `RequestLog.transport`). Historical rows
//                   from before this field existed carry `null`, rendered as a dash, never a
//                   fabricated pill.
//   Status      <- r.status (HTTP code); toned ok/warn(429)/error(>=400), NOT the account-status
//                   `ui/StatusPill` (that component tones backend STATUS STRINGS like "active" /
//                   "cooldown" — a completely different domain from an HTTP status code) — a small
//                   local `HttpStatusPill` instead.
//   TTFT        <- `format.ts::latency(r.ttft_ms)` — null (never backfilled pre-migration-0007 rows,
//                   or a non-streaming/failed request) renders "—", never "0ms".
//   TPS         <- `format.ts::tpsFmt(r.tps)` — server-derived (read_api.rs::derive_tps); null
//                   renders "—".
//   Tokens      <- r.total_tokens (compactNum; null -> "—") + r.cached_tokens as a small "· N cch"
//                   caption, shown only when cached_tokens is NOT null (a real 0 is shown as "0 cch";
//                   a missing/unreported value is omitted entirely, never rendered as "0 cch" — the
//                   brief's absent-vs-0 rule).
//
// Expandable detail row: request id, path (method+path), transport, model/effort/tier, requested-at
// (full date+time), duration, TTFT, TPS, tokens(+cached), aliased — every one a real
// `RequestRowView` field. The mockup's detail row ALSO shows an api key, a pool, a
// downstream/upstream transport split, retry-after, an upstream error code, and a routing trail —
// NONE of those exist on `RequestRowView` (no pool field; `transport` is a single value, not a
// downstream/upstream pair; `error_code`/retry-after/routing trail exist nowhere on this endpoint —
// `error_code` is only on the separate, narrower `RecentErrorRow` used by `/api/overview`, per
// read_api.rs/request_log_repo.rs). Per this page's binding content-safety constraint (never render
// a key) and the general no-fabrication rule, all of those are omitted rather than invented. See
// task-9-report.md for the full reasoning.
//
// Filters drive REAL `RequestsQuery` params only (read_api.rs:527-536): account, provider,
// status_class ("success"|"error" — the backend has no per-code filter), model, transport, since_ts
// (derived from the 1h/24h/7d range picker), limit/offset (server pagination). The mockup's
// request-id search has NO backend equivalent (no id/`q` param on `RequestsQuery` at all) — sending
// one would be silently ignored by the handler (extra query fields aren't rejected, just unused),
// which is exactly the "filter the backend can't honor" case the brief warns against. It is
// implemented instead as a client-side quick-filter over the CURRENTLY LOADED page only (labeled
// "this page" in the placeholder text) — it never changes `total`/pagination and is intentionally
// NOT URL-synced (it isn't a real query param).
import { useEffect, useState, type ReactNode } from "react";
import { useSearchParams } from "react-router-dom";
import * as Select from "@radix-ui/react-select";
import clsx from "clsx";

import type { RequestRowView, RequestsQueryParams } from "../lib/api";
import { compactNum, latency, relTime, tpsFmt } from "../lib/format";
import { useAccounts, useRequests } from "../lib/queries";
import { Card } from "../ui/Card";
import { Col, Grid } from "../ui/Grid";
import { AlertTriangle, ChevronDown, ChevronLeft, ChevronRight, Lock, Search } from "../ui/icons";
import { ProviderTag } from "../ui/ProviderTag";

// Mirrors `lib/queries.ts`'s internal (non-exported) `LIST_REFETCH_MS` — kept in sync manually
// since that constant isn't exported; used only for the live badge's honest "polling Ns" label.
const POLL_SECS = 30;

const ALL = "all";
const PAGE_SIZE = 50;

type RangeKey = "1h" | "24h" | "7d";
const RANGE_SECS: Record<RangeKey, number> = { "1h": 3600, "24h": 86400, "7d": 604800 };
const RANGE_OPTIONS: Array<{ value: RangeKey; label: string }> = [
  { value: "1h", label: "1h" },
  { value: "24h", label: "24h" },
  { value: "7d", label: "7d" },
];

function isRangeKey(v: string | null): v is RangeKey {
  return v === "1h" || v === "24h" || v === "7d";
}

/** Local wall-clock formatters — `format.ts` only has relative-time (`relTime`); an absolute
 * HH:MM:SS / date+time display is a distinct concern this page needs for the Time column and the
 * detail row's full timestamp, so small pure helpers live here rather than in the shared module. */
function formatClock(unixSecs: number): string {
  return new Date(unixSecs * 1000).toLocaleTimeString(undefined, { hour12: false });
}
function formatFullDateTime(unixSecs: number): string {
  const d = new Date(unixSecs * 1000);
  return `${d.toLocaleDateString(undefined)} ${formatClock(unixSecs)}`;
}

// ---------------------------------------------------------------------------------------------
// HTTP status tone — distinct from `ui/StatusPill` (which tones ACCOUNT status strings like
// "active"/"cooldown", a different domain from an HTTP response code).
// ---------------------------------------------------------------------------------------------

type HttpTone = "ok" | "warn" | "error";

function httpStatusTone(status: number): HttpTone {
  if (status === 429) return "warn";
  if (status >= 400) return "error";
  if (status >= 300) return "warn";
  return "ok";
}

const HTTP_TONE_CLASS: Record<HttpTone, string> = {
  ok: "bg-success/15 text-success",
  warn: "bg-warn/15 text-warn",
  error: "bg-error/15 text-error",
};

function HttpStatusPill({ status }: { status: number }) {
  return (
    <span
      className={clsx(
        "inline-block whitespace-nowrap rounded px-2 py-0.5 text-[10px] font-semibold leading-none tabular-nums",
        HTTP_TONE_CLASS[httpStatusTone(status)],
      )}
    >
      {status}
    </span>
  );
}

function TransportPill({ transport }: { transport: string | null }) {
  if (!transport) return <span className="text-fg opacity-40">—</span>;
  const isWs = transport === "ws";
  return (
    <span
      className={clsx(
        "inline-block whitespace-nowrap rounded px-1.5 py-0.5 text-[9px] font-bold lowercase leading-none",
        isWs ? "bg-accent/15 text-accent" : "bg-muted text-fg opacity-70",
      )}
    >
      {transport}
    </span>
  );
}

function ModelCell({
  model,
  effort,
  tier,
}: {
  model: string | null;
  effort: string | null;
  tier: string | null;
}) {
  if (!model) return <span className="text-fg opacity-40">—</span>;
  return (
    <span className="whitespace-nowrap">
      {model}
      {effort && (
        <span className="ml-1 rounded bg-muted px-1 py-0 text-[8.5px] text-fg opacity-60">
          {effort}
        </span>
      )}
      {tier && (
        <span className="ml-1 rounded bg-muted px-1 py-0 text-[8.5px] text-fg opacity-60">
          {tier}
        </span>
      )}
    </span>
  );
}

function TokensCell({ total, cached }: { total: number | null; cached: number | null }) {
  if (total === null) return <span className="text-fg opacity-40">—</span>;
  return (
    <span className="whitespace-nowrap tabular-nums">
      {compactNum(total)}
      {cached !== null && (
        <span className="ml-1 text-[9px] text-fg opacity-50">· {compactNum(cached)} cch</span>
      )}
    </span>
  );
}

// ---------------------------------------------------------------------------------------------
// Generic Radix Select filter — reused for account/provider/status/model/transport (the brief's
// explicit choice of control for these 5; time range uses segmented buttons instead, matching the
// mockup's `.seg` vs `.drop` distinction).
// ---------------------------------------------------------------------------------------------

function FilterSelect({
  label,
  value,
  onChange,
  options,
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  options: Array<{ value: string; label: string }>;
}) {
  const itemClass =
    "cursor-pointer select-none rounded px-2.5 py-1 text-fg opacity-80 outline-none data-[highlighted]:bg-muted data-[highlighted]:opacity-100";
  return (
    <Select.Root value={value} onValueChange={onChange}>
      <Select.Trigger className="flex shrink-0 items-center gap-1.5 rounded border border-border bg-card px-2.5 py-1 text-[10.5px] text-fg opacity-80 outline-none hover:opacity-100 focus:opacity-100">
        <span className="opacity-60">{label}:</span>
        <Select.Value />
        <Select.Icon>
          <ChevronDown className="h-3 w-3" strokeWidth={2} />
        </Select.Icon>
      </Select.Trigger>
      <Select.Portal>
        <Select.Content
          position="popper"
          sideOffset={4}
          className="z-50 max-h-64 overflow-auto rounded border border-border bg-card text-[10.5px] shadow-lg"
        >
          <Select.Viewport className="p-1">
            {options.map((o) => (
              <Select.Item key={o.value} value={o.value} className={itemClass}>
                <Select.ItemText>{o.label}</Select.ItemText>
              </Select.Item>
            ))}
          </Select.Viewport>
        </Select.Content>
      </Select.Portal>
    </Select.Root>
  );
}

// ---------------------------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------------------------

export function Requests() {
  const [searchParams, setSearchParams] = useSearchParams();

  const range: RangeKey = isRangeKey(searchParams.get("range")) ? (searchParams.get("range") as RangeKey) : "24h";
  const accountFilter = searchParams.get("account") ?? ALL;
  const providerFilter = searchParams.get("provider") ?? ALL;
  const statusFilter = searchParams.get("status") ?? ALL;
  const modelFilter = searchParams.get("model") ?? ALL;
  const transportFilter = searchParams.get("transport") ?? ALL;
  const offset = Math.max(0, Number(searchParams.get("offset") ?? "0") || 0);

  // Client-only quick filter over the currently loaded page (see the file-header note — no
  // server-side id search exists). Deliberately not URL-synced.
  const [searchId, setSearchId] = useState("");

  // Ticks the header's "updated Xs ago" text and re-derives `since_ts` from the selected range —
  // same 5s-tick pattern Overview/Accounts/Pools already use. `since_ts` is bucketed to the minute
  // (see below) so this tick doesn't create a new query key every 5s.
  const [nowMs, setNowMs] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNowMs(Date.now()), 5000);
    return () => clearInterval(id);
  }, []);
  const bucketedNowSecs = Math.floor(Math.floor(nowMs / 1000) / 60) * 60;
  const sinceTs = bucketedNowSecs - RANGE_SECS[range];

  /** Per-key default that's omitted from the URL entirely (keeps the URL clean when a filter is at
   * its default) — `ALL` for every dropdown filter, `"24h"` for the range picker, `"0"` for offset. */
  const PARAM_DEFAULT: Record<string, string> = {
    range: "24h",
    account: ALL,
    provider: ALL,
    status: ALL,
    model: ALL,
    transport: ALL,
    offset: "0",
  };

  function setParam(key: string, value: string, opts: { resetOffset?: boolean } = {}) {
    const params = new URLSearchParams(searchParams);
    if (value === PARAM_DEFAULT[key]) {
      params.delete(key);
    } else {
      params.set(key, value);
    }
    if (opts.resetOffset !== false) params.delete("offset");
    setSearchParams(params, { replace: true });
  }

  const accountsQuery = useAccounts();
  const accounts = accountsQuery.data ?? [];

  const queryParams: RequestsQueryParams = {
    limit: PAGE_SIZE,
    offset,
    account: accountFilter !== ALL ? accountFilter : undefined,
    provider:
      providerFilter !== ALL ? (providerFilter === "claude" ? "anthropic" : providerFilter) : undefined,
    status_class: statusFilter !== ALL ? statusFilter : undefined,
    model: modelFilter !== ALL ? modelFilter : undefined,
    transport: transportFilter !== ALL ? transportFilter : undefined,
    since_ts: sinceTs,
  };

  const { data, isLoading, isFetching, isError, error, refetch, dataUpdatedAt } =
    useRequests(queryParams);

  const [expandedId, setExpandedId] = useState<number | null>(null);

  if (isLoading) return <RequestsSkeleton />;

  if (isError) {
    return (
      <div className="flex flex-col gap-3">
        <PageHeader />
        <Card>
          <div className="flex flex-wrap items-center justify-between gap-3">
            <span className="flex items-center gap-2 text-[12px] text-error">
              <AlertTriangle className="h-4 w-4 shrink-0" strokeWidth={1.9} />
              Couldn&apos;t load requests
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

  const filtersActive =
    accountFilter !== ALL ||
    providerFilter !== ALL ||
    statusFilter !== ALL ||
    modelFilter !== ALL ||
    transportFilter !== ALL;

  const rows = data.rows;
  const visibleRows =
    searchId.trim() === ""
      ? rows
      : rows.filter((r) => String(r.id).includes(searchId.trim()));

  // Model filter's options: the backend has no distinct-values endpoint, so options are derived
  // from the currently loaded page — plus the current selection (kept visible even if it later
  // drops out of view, so choosing a value never makes it disappear from its own dropdown).
  const modelValues = new Set<string>();
  for (const r of rows) if (r.model) modelValues.add(r.model);
  if (modelFilter !== ALL) modelValues.add(modelFilter);
  const modelOptions = [
    { value: ALL, label: "all" },
    ...Array.from(modelValues)
      .sort()
      .map((m) => ({ value: m, label: m })),
  ];

  const accountOptions = [
    { value: ALL, label: "all" },
    ...accounts.map((a) => ({ value: a.id, label: a.id })),
  ];

  const pageStart = data.total > 0 ? offset + 1 : 0;
  const pageEnd = offset + rows.length;
  const hasPrev = offset > 0;
  const hasNext = offset + rows.length < data.total;

  return (
    <div className="flex flex-col gap-3">
      <PageHeader
        subtitle={
          <>
            {data.total.toLocaleString()} {data.total === 1 ? "request" : "requests"} · last{" "}
            {range}
            {dataUpdatedAt ? <> · updated {relTime(Math.floor(dataUpdatedAt / 1000), nowMs)}</> : null}
          </>
        }
        actions={<LiveBadge isFetching={isFetching} />}
      />

      <div className="flex flex-wrap items-center gap-2">
        <div className="flex shrink-0 overflow-hidden rounded border border-border bg-card text-[10.5px]">
          {RANGE_OPTIONS.map((o) => (
            <button
              key={o.value}
              type="button"
              onClick={() => setParam("range", o.value)}
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

        <FilterSelect
          label="Account"
          value={accountFilter}
          onChange={(v) => setParam("account", v)}
          options={accountOptions}
        />
        <FilterSelect
          label="Provider"
          value={providerFilter}
          onChange={(v) => setParam("provider", v)}
          options={[
            { value: ALL, label: "all" },
            { value: "codex", label: "codex" },
            { value: "claude", label: "claude" },
          ]}
        />
        <FilterSelect
          label="Status"
          value={statusFilter}
          onChange={(v) => setParam("status", v)}
          options={[
            { value: ALL, label: "all" },
            { value: "success", label: "success" },
            { value: "error", label: "error" },
          ]}
        />
        <FilterSelect
          label="Model"
          value={modelFilter}
          onChange={(v) => setParam("model", v)}
          options={modelOptions}
        />
        <FilterSelect
          label="Transport"
          value={transportFilter}
          onChange={(v) => setParam("transport", v)}
          options={[
            { value: ALL, label: "all" },
            { value: "http", label: "http" },
            { value: "ws", label: "ws" },
          ]}
        />

        <div className="ml-auto flex shrink-0 items-center gap-1.5 rounded border border-border bg-card px-2.5 py-1 text-[10.5px] text-fg opacity-80">
          <Search className="h-3 w-3 shrink-0 opacity-60" strokeWidth={2} />
          <input
            value={searchId}
            onChange={(e) => setSearchId(e.target.value)}
            placeholder="Search request id (this page)…"
            className="w-[168px] bg-transparent text-fg outline-none placeholder:opacity-50"
          />
        </div>
      </div>

      {rows.length === 0 ? (
        <Card>
          <p className="text-[11px] text-fg opacity-50">
            {filtersActive
              ? "No requests match the current filters."
              : "No requests logged yet — they'll appear here as soon as PolyFlare serves one."}
          </p>
        </Card>
      ) : (
        <Grid>
          <Col span={12}>
            <RequestsTable
              rows={visibleRows}
              hiddenBySearch={rows.length - visibleRows.length}
              expandedId={expandedId}
              onToggle={(id) => setExpandedId((cur) => (cur === id ? null : id))}
            />
          </Col>
        </Grid>
      )}

      {rows.length > 0 && (
        <div className="flex items-center justify-between text-[10.5px] text-fg opacity-70">
          <span>
            Showing {pageStart}–{pageEnd} of {data.total.toLocaleString()} · newest first
          </span>
          <div className="flex gap-1.5">
            <button
              type="button"
              disabled={!hasPrev}
              onClick={() => setParam("offset", String(Math.max(0, offset - PAGE_SIZE)), { resetOffset: false })}
              className="flex items-center gap-1 rounded border border-border bg-card px-2.5 py-1 text-fg disabled:opacity-40"
            >
              <ChevronLeft className="h-3 w-3" strokeWidth={2} />
              Prev
            </button>
            <button
              type="button"
              disabled={!hasNext}
              onClick={() => setParam("offset", String(offset + PAGE_SIZE), { resetOffset: false })}
              className="flex items-center gap-1 rounded border border-border bg-card px-2.5 py-1 text-fg disabled:opacity-40"
            >
              Next
              <ChevronRight className="h-3 w-3" strokeWidth={2} />
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

function PageHeader({ subtitle, actions }: { subtitle?: ReactNode; actions?: ReactNode }) {
  return (
    <div className="flex flex-wrap items-center justify-between gap-3">
      <div>
        <h1 className="text-lg font-semibold text-fg">Requests</h1>
        {subtitle && <p className="mt-0.5 text-[11px] text-fg opacity-60">{subtitle}</p>}
      </div>
      {actions}
    </div>
  );
}

/** The header's "live" indicator. Reflects something REAL: `useRequests`'s own 30s auto-refresh —
 * never a decorative badge implying an SSE stream this page doesn't have (live-logs SSE is Task
 * 10's `useLogStream`, a completely different endpoint). The dot only pulses while `isFetching` is
 * true (an actual network request in flight, including the background poll), and the label switches
 * to "refreshing…" for that same real reason — otherwise it reads as the honest steady state
 * ("polling 30s"). `motion-reduce:hidden` on the ping ring respects `prefers-reduced-motion`. */
function LiveBadge({ isFetching }: { isFetching: boolean }) {
  return (
    <span className="flex shrink-0 items-center gap-1.5 whitespace-nowrap rounded border border-success/30 bg-success/[0.08] px-2.5 py-1 text-[10.5px] text-success">
      <span className="relative flex h-[7px] w-[7px] shrink-0">
        {isFetching && (
          <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-success opacity-60 motion-reduce:hidden" />
        )}
        <span className="relative inline-flex h-[7px] w-[7px] rounded-full bg-success" />
      </span>
      {isFetching ? "Live · refreshing…" : `Live · polling ${POLL_SECS}s`}
    </span>
  );
}

// ---------------------------------------------------------------------------------------------
// Table
// ---------------------------------------------------------------------------------------------

const TABLE_HEAD_CLASS =
  "px-2 py-1.5 text-left text-[9px] font-medium uppercase tracking-wide text-fg opacity-60";

function RequestsTable({
  rows,
  hiddenBySearch,
  expandedId,
  onToggle,
}: {
  rows: RequestRowView[];
  hiddenBySearch: number;
  expandedId: number | null;
  onToggle: (id: number) => void;
}) {
  return (
    <Card>
      {hiddenBySearch > 0 && (
        <p className="mb-1.5 text-[10px] text-fg opacity-50">
          {rows.length} of {rows.length + hiddenBySearch} rows on this page match your search.
        </p>
      )}
      <div className="overflow-x-auto">
        <table className="w-full min-w-[860px] border-collapse text-[10.5px]">
          <thead>
            <tr className="border-b border-border">
              <th className={TABLE_HEAD_CLASS}>Time</th>
              <th className={TABLE_HEAD_CLASS}>Account</th>
              <th className={TABLE_HEAD_CLASS}>Provider</th>
              <th className={TABLE_HEAD_CLASS}>Model</th>
              <th className={TABLE_HEAD_CLASS}>Transport</th>
              <th className={TABLE_HEAD_CLASS}>Status</th>
              <th className={clsx(TABLE_HEAD_CLASS, "text-right")}>TTFT</th>
              <th className={clsx(TABLE_HEAD_CLASS, "text-right")}>TPS</th>
              <th className={clsx(TABLE_HEAD_CLASS, "text-right")}>Tokens</th>
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 ? (
              <tr>
                <td colSpan={9} className="px-2 py-3 text-center text-fg opacity-50">
                  No rows on this page match your search.
                </td>
              </tr>
            ) : (
              rows.map((r) => (
                <RequestRow
                  key={r.id}
                  row={r}
                  expanded={expandedId === r.id}
                  onToggle={() => onToggle(r.id)}
                />
              ))
            )}
          </tbody>
        </table>
      </div>
    </Card>
  );
}

function RequestRow({
  row: r,
  expanded,
  onToggle,
}: {
  row: RequestRowView;
  expanded: boolean;
  onToggle: () => void;
}) {
  return (
    <>
      <tr
        role="button"
        tabIndex={0}
        onClick={onToggle}
        onKeyDown={(e) => {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            onToggle();
          }
        }}
        className={clsx(
          "cursor-pointer border-b border-border/55 hover:bg-muted/60",
          expanded && "bg-muted/60",
          !expanded && "last:border-0",
        )}
      >
        <td className="whitespace-nowrap px-2 py-1.5 font-mono tabular-nums text-fg opacity-90">
          <ChevronRight
            className={clsx("mr-1 inline h-3 w-3 opacity-50 transition-transform", expanded && "rotate-90")}
            strokeWidth={2}
          />
          {formatClock(r.requested_at)}
        </td>
        <td className="whitespace-nowrap px-2 py-1.5 text-fg opacity-90">{r.account_id ?? "—"}</td>
        <td className="px-2 py-1.5">
          <ProviderTag provider={r.provider} />
        </td>
        <td className="px-2 py-1.5">
          <ModelCell model={r.model} effort={r.reasoning_effort} tier={r.service_tier} />
        </td>
        <td className="px-2 py-1.5">
          <TransportPill transport={r.transport} />
        </td>
        <td className="px-2 py-1.5">
          <HttpStatusPill status={r.status} />
        </td>
        <td className="whitespace-nowrap px-2 py-1.5 text-right tabular-nums text-fg opacity-80">
          {latency(r.ttft_ms)}
        </td>
        <td className="whitespace-nowrap px-2 py-1.5 text-right tabular-nums text-fg opacity-80">
          {tpsFmt(r.tps)}
        </td>
        <td className="whitespace-nowrap px-2 py-1.5 text-right text-fg opacity-90">
          <TokensCell total={r.total_tokens} cached={r.cached_tokens} />
        </td>
      </tr>
      {expanded && (
        <tr className="border-b border-border/55 bg-muted/30 last:border-0">
          <td colSpan={9} className="p-0">
            <RequestDetail row={r} />
          </td>
        </tr>
      )}
    </>
  );
}

/** Content-free detail row. Every value here is a real `RequestRowView` field — see the file-header
 * comment for the mockup fields (api key, pool, downstream/upstream split, retry-after, upstream
 * error code, routing trail) that are NOT real and are therefore omitted, not fabricated. */
function RequestDetail({ row: r }: { row: RequestRowView }) {
  return (
    <div className="flex flex-col gap-2 px-4 py-3 text-[10.5px]">
      <div className="flex flex-wrap gap-x-5 gap-y-1">
        <DetailField label="request id" value={`#${r.id}`} mono />
        <DetailField label="status" value={String(r.status)} />
        <DetailField label="account" value={r.account_id ? `${r.account_id} (${r.provider})` : "—"} />
        <DetailField label="aliased" value={r.aliased ? "yes" : "no"} />
      </div>
      <div className="flex flex-wrap gap-x-5 gap-y-1">
        <DetailField label="path" value={`${r.method} ${r.path}`} mono />
        <DetailField label="transport" value={r.transport ?? "—"} />
        <DetailField
          label="model"
          value={[r.model ?? "—", r.reasoning_effort, r.service_tier].filter(Boolean).join(" · ")}
        />
      </div>
      <div className="flex flex-wrap gap-x-5 gap-y-1">
        <DetailField label="requested at" value={formatFullDateTime(r.requested_at)} />
        <DetailField label="duration" value={latency(r.duration_ms)} />
        <DetailField label="TTFT" value={latency(r.ttft_ms)} />
        <DetailField label="TPS" value={tpsFmt(r.tps)} />
        <DetailField
          label="tokens"
          value={
            r.total_tokens === null
              ? "—"
              : `${compactNum(r.total_tokens)}${r.cached_tokens !== null ? ` (${compactNum(r.cached_tokens)} cached)` : ""}`
          }
        />
      </div>
      <div className="flex items-center gap-1.5 rounded border border-dashed border-border bg-card px-2.5 py-1.5 text-[9.5px] text-fg opacity-60">
        <Lock className="h-3 w-3 shrink-0" strokeWidth={1.9} />
        No request or response bodies are stored — PolyFlare logs outcomes only (status, timing,
        token counts, routing).
      </div>
    </div>
  );
}

function DetailField({ label, value, mono }: { label: string; value: string; mono?: boolean }) {
  return (
    <div className="text-fg opacity-60">
      {label}{" "}
      <b className={clsx("font-medium text-fg opacity-100", mono && "font-mono")}>{value}</b>
    </div>
  );
}

// ---------------------------------------------------------------------------------------------
// Loading skeleton — mirrors the real header + filter bar + table shape so data arriving doesn't
// reflow the page.
// ---------------------------------------------------------------------------------------------

function RequestsSkeleton() {
  return (
    <div className="flex flex-col gap-3">
      <div className="flex items-center justify-between gap-3">
        <div>
          <div className="h-[22px] w-24 animate-pulse rounded bg-muted" />
          <div className="mt-1.5 h-3 w-64 animate-pulse rounded bg-muted" />
        </div>
        <div className="h-7 w-40 animate-pulse rounded bg-muted" />
      </div>
      <div className="h-7 w-full animate-pulse rounded bg-muted" />
      <Grid>
        <Col span={12}>
          <Card>
            <div className="flex flex-col gap-2">
              {[0, 1, 2, 3, 4, 5, 6, 7].map((i) => (
                <div key={i} className="h-6 animate-pulse rounded bg-muted" />
              ))}
            </div>
          </Card>
        </Col>
      </Grid>
    </div>
  );
}
