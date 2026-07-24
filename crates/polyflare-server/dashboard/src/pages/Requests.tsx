// The Requests page: the dashboard's deepest view into request-log rows. `GET /api/requests`
// supplies the durable filtered page; the content-free `/api/logs/stream` SSE channel invalidates
// it on request-completion events, with 30-second polling retained only while SSE is unavailable.
// `useAccounts()` resolves account ids to nicknames/emails and populates the Account filter.
//
// CONTENT-SAFETY: `request_log` is content-free by construction (see
// `polyflare_store::request_log_repo`'s own module doc) — counts, ids, statuses, timing, token
// COUNTS, never a body/prompt/response/key. This page renders ONLY the real fields on
// `RequestRowView` (read_api.rs): id, request_id, session_key, requested_at, provider, method, path, aliased, status,
// duration_ms, account_id, model, reasoning_effort, service_tier, transport, ttft_ms, total_tokens,
// cached_tokens, tps (server-derived), subagent, outcome, error_code. `outcome`/`error_code` are
// bounded legacy codex-lb evidence used only when its imported row has no HTTP status. `subagent`
// is the codex sub-agent role slug from
// `x-openai-subagent` (`review`/`compact`/`memory_consolidation`/`collab_spawn`), or `null` for the
// main agent — a bounded role slug, same content-safety class as `model`, never conversation
// content. Nothing else exists to render, so nothing else is rendered — see the detail-row section
// below for the specific fields the mockup shows that are NOT real and are therefore omitted rather
// than fabricated.
//
// Field mapping (table columns, in order):
//   Time        <- r.requested_at (unix secs -> local HH:MM:SS; a plain wall-clock display, not
//                   `format.ts::relTime`, which is relative-only — no absolute-clock formatter
//                   exists yet, so a tiny local `formatClock` is added here, same precedent as this
//                   page's other page-local helpers)
//   Account     <- r.account_id (nullable — a request can fail before an account is chosen)
//   Provider    <- r.provider via `ProviderTag` (wire value "codex"/"anthropic", never "claude")
//   Model       <- r.model (+ r.reasoning_effort / r.service_tier as small chips, all nullable) +
//                   r.subagent as a small tag ("main" when null — the main agent, not missing data)
//   Transport   <- r.transport ("sse" for streamed HTTP, "http" for buffered HTTP, "ws" for
//                   WebSocket — see observability.rs's `RequestLog.transport`). Historical rows
//                   from before this field existed carry `null`, rendered as a dash, never a
//                   fabricated pill.
//   Status      <- native r.status (HTTP code), or imported r.outcome when status=0 means the
//                   codex-lb source had no HTTP status; toned ok/warn(429)/error, NOT account-status
//                   `ui/StatusPill` (that component tones backend STATUS STRINGS like "active" /
//                   "cooldown" — a completely different domain from an HTTP status code) — a small
//                   local `HttpStatusPill` instead.
//   TTFT        <- `format.ts::latency(r.ttft_ms)` — the API's effective native/imported first-token
//                   timing; missing/non-streaming/failed requests render "—", never "0ms".
//   Latency     <- `format.ts::latency(r.duration_ms)` — total end-to-end request duration.
//   Throughput  <- `format.ts::tpsFmt(r.tps)` — server-derived output tokens divided by the
//                   post-TTFT generation window; missing source evidence renders "—".
//   Tokens      <- r.total_tokens (compactNum; null -> "—") + r.cached_tokens as a small "· N cch"
//                   caption, shown only when cached_tokens is NOT null (a real 0 is shown as "0 cch";
//                   a missing/unreported value is omitted entirely, never rendered as "0 cch" — the
//                   brief's absent-vs-0 rule).
//
// Expandable detail row: request id, path (method+path), transport, model/effort/tier, subagent
// ("main" when null), requested-at (full date+time), duration, TTFT, throughput, tokens(+cached), aliased
// — every one a real `RequestRowView` field. The mockup's detail row ALSO shows an api key, a pool, a
// downstream/upstream transport split, retry-after, and a routing trail —
// NONE of those exist on `RequestRowView` (no pool field; `transport` is a single value, not a
// downstream/upstream pair; retry-after/routing trail exist nowhere on this endpoint). Per this
// page's binding content-safety constraint (never render
// a key) and the general no-fabrication rule, all of those are omitted rather than invented. See
// task-9-report.md for the full reasoning.
//
// Filters drive REAL `RequestsQuery` params only (read_api.rs:527-536): account, provider,
// status_class ("success"|"error" — the backend has no per-code filter), model, transport, since_ts
// (derived from the 1h/24h/7d/30d range picker), limit/offset (server pagination). The mockup's
// Correlation-id lookup uses the backend's exact `request_id` filter, so links from Live Logs
// resolve the durable row even when it is far outside the current page. The input also retains a
// local substring match for the numeric SQLite row id while no exact correlation filter is active.
import { useEffect, useState, type ReactNode } from "react";
import { Link, useSearchParams } from "react-router-dom";
import * as Select from "@radix-ui/react-select";
import clsx from "clsx";

import type { AccountView, RequestRowView, RequestsQueryParams } from "../lib/api";
import { accountDisplayLabel } from "../lib/accountDisplay";
import { compactNum, latency, relTime, tpsFmt } from "../lib/format";
import { paginationWindow } from "../lib/pagination";
import { useAccounts, useProviders, useRequests } from "../lib/queries";
import {
  latestRequestEventKey,
  requestLiveLabel,
} from "../lib/requestLive";
import { useLogStream } from "../lib/useLogStream";
import { useCapabilityFlags } from "../capabilities/CapabilitiesProvider";
import {
  requestOutcomeIsFailure,
  requestOutcomeIsSuccess,
  requestOutcomeLabel,
  requestOutcomeSource,
} from "../lib/requestOutcome";
import {
  routePseudonym,
  ShieldedAccount,
  useScreenShield,
} from "../privacy/ScreenShield";
import { Card } from "../ui/Card";
import { Col, Grid } from "../ui/Grid";
import { AlertTriangle, ChevronDown, ChevronLeft, ChevronRight, Search } from "../ui/icons";
import { ProviderTag } from "../ui/ProviderTag";
import { RequestDetailPanel } from "../ui/RequestDetails";
import { ServiceTierBadge } from "../ui/ServiceTierBadge";
import { TransportPill } from "../ui/TransportPill";

const ALL = "all";
const PAGE_SIZE = 20;

type RangeKey = "1h" | "24h" | "7d" | "30d";
const RANGE_SECS: Record<RangeKey, number> = {
  "1h": 3600,
  "24h": 86400,
  "7d": 604800,
  "30d": 2592000,
};
const RANGE_OPTIONS: Array<{ value: RangeKey; label: string }> = [
  { value: "1h", label: "1h" },
  { value: "24h", label: "24h" },
  { value: "7d", label: "7d" },
  { value: "30d", label: "30d" },
];

function isRangeKey(v: string | null): v is RangeKey {
  return v === "1h" || v === "24h" || v === "7d" || v === "30d";
}

/** Local wall-clock formatters — `format.ts` only has relative-time (`relTime`); an absolute
 * HH:MM:SS / date+time display is a distinct concern this page needs for the Time column and the
 * detail row's full timestamp, so small pure helpers live here rather than in the shared module. */
function formatClock(unixSecs: number): string {
  return new Date(unixSecs * 1000).toLocaleTimeString(undefined, { hour12: false });
}
// ---------------------------------------------------------------------------------------------
// HTTP status tone — distinct from `ui/StatusPill` (which tones ACCOUNT status strings like
// "active"/"cooldown", a different domain from an HTTP response code).
// ---------------------------------------------------------------------------------------------

type HttpTone = "ok" | "warn" | "error" | "neutral";

function httpStatusTone(row: RequestRowView): HttpTone {
  if (row.status === 429) return "warn";
  if (requestOutcomeIsFailure(row)) return "error";
  if (requestOutcomeIsSuccess(row)) return "ok";
  if (row.status >= 300) return "warn";
  return "neutral";
}

const HTTP_TONE_CLASS: Record<HttpTone, string> = {
  ok: "bg-success/15 text-success",
  warn: "bg-warn/15 text-warn",
  error: "bg-error/15 text-error",
  neutral: "bg-muted text-fg opacity-65",
};

function HttpStatusPill({ row }: { row: RequestRowView }) {
  const source = requestOutcomeSource(row);
  const title =
    source === "protocol"
      ? `Codex stream ${row.protocol_outcome}; initial HTTP status ${row.status}`
      : source === "imported"
      ? `Imported codex-lb outcome; HTTP status unavailable${row.error_code ? ` · ${row.error_code}` : ""}`
      : source === "unknown"
        ? "HTTP status and imported outcome unavailable"
        : `HTTP ${row.status}`;
  return (
    <span
      title={title}
      className={clsx(
        "inline-block whitespace-nowrap rounded px-2 py-0.5 text-[10px] font-semibold leading-none tabular-nums",
        HTTP_TONE_CLASS[httpStatusTone(row)],
      )}
    >
      {requestOutcomeLabel(row)}
    </span>
  );
}

/** The sub-agent role tag (`x-openai-subagent`: "review"/"compact"/"memory_consolidation"/
 * "collab_spawn"), rendered next to the model. `null` means the main agent — a real, determined
 * state (not missing data), so it renders the word "main" rather than the em-dash this page uses
 * elsewhere for genuinely absent values. Styled like `TransportPill`'s "on" state (small
 * accent-tinted pill) when a sub-agent role is present, and like the `effort`/`tier` chips in
 * `ModelCell` (muted, low-emphasis) for the "main" fallback. */
function SubagentTag({ subagent }: { subagent: string | null }) {
  if (!subagent) {
    return (
      <span className="ml-1 rounded bg-muted px-1 py-0 text-[8.5px] text-fg opacity-50">main</span>
    );
  }
  return (
    <span className="ml-1 inline-block whitespace-nowrap rounded bg-accent/15 px-1 py-0 text-[8.5px] font-bold lowercase leading-none text-accent">
      {subagent}
    </span>
  );
}

function ModelCell({
  model,
  effort,
  tier,
  subagent,
}: {
  model: string | null;
  effort: string | null;
  tier: string | null;
  subagent: string | null;
}) {
  return (
    <span className="whitespace-nowrap">
      {model ?? <span className="text-fg opacity-40">—</span>}
      {model && effort && (
        <span className="ml-1 rounded bg-muted px-1 py-0 text-[8.5px] text-fg opacity-60">
          {effort}
        </span>
      )}
      <ServiceTierBadge tier={tier} className="ml-1" />
      <SubagentTag subagent={subagent} />
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
  const { active: screenShieldActive } = useScreenShield();
  const { liveLogs } = useCapabilityFlags();
  const requestStream = useLogStream({ enabled: liveLogs });

  const range: RangeKey = isRangeKey(searchParams.get("range")) ? (searchParams.get("range") as RangeKey) : "24h";
  const accountFilter = searchParams.get("account") ?? ALL;
  const providerFilter = searchParams.get("provider") ?? ALL;
  const statusFilter = searchParams.get("status") ?? ALL;
  const modelFilter = searchParams.get("model") ?? ALL;
  const transportFilter = searchParams.get("transport") ?? ALL;
  const requestIdFilter = searchParams.get("request_id") ?? "";
  const offset = Math.max(0, Number(searchParams.get("offset") ?? "0") || 0);

  const [searchId, setSearchId] = useState(requestIdFilter);
  useEffect(() => setSearchId(requestIdFilter), [requestIdFilter]);

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
    request_id: "",
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
  const providersQuery = useProviders();
  const accounts = accountsQuery.data ?? [];
  const accountById = new Map(accounts.map((account) => [account.id, account]));
  const credentialLabelById = new Map(
    (providersQuery.data ?? []).flatMap((provider) =>
      provider.credentials.map((credential) => [credential.id, credential.label] as const),
    ),
  );

  const queryParams: RequestsQueryParams = {
    limit: PAGE_SIZE,
    offset,
    request_id: requestIdFilter || undefined,
    account: accountFilter !== ALL ? accountFilter : undefined,
    provider:
      providerFilter !== ALL ? (providerFilter === "claude" ? "anthropic" : providerFilter) : undefined,
    status_class: statusFilter !== ALL ? statusFilter : undefined,
    model: modelFilter !== ALL ? modelFilter : undefined,
    transport: transportFilter !== ALL ? transportFilter : undefined,
    // An exact correlation-id lookup is intentionally all-time; the operator is following one
    // known request, so the normal dashboard time window must not hide an older matching row.
    since_ts: requestIdFilter ? undefined : sinceTs,
  };

  const { data, isLoading, isFetching, isError, error, refetch, dataUpdatedAt } =
    useRequests(queryParams, { sseConnected: requestStream.connected });

  const latestRequestKey = latestRequestEventKey(requestStream.lines);
  useEffect(() => {
    if (!requestStream.connected || latestRequestKey === null) return;
    // Reconnect backfill can deliver many events in one burst. Coalesce that burst into one
    // durable-page refresh; a real live completion still appears within a fraction of a second.
    const timer = window.setTimeout(() => {
      void refetch();
    }, 150);
    return () => window.clearTimeout(timer);
  }, [latestRequestKey, refetch, requestStream.connected]);

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
    transportFilter !== ALL ||
    requestIdFilter !== "";

  const rows = data.rows;
  const visibleRows =
    searchId.trim() === ""
      ? rows
      : rows.filter(
          (r) =>
            String(r.id).includes(searchId.trim()) ||
            r.request_id?.toLowerCase().includes(searchId.trim().toLowerCase()),
        );

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

  const targetOptions = [
    { value: ALL, label: "all" },
    ...accounts.map((a) => ({
      value: a.id,
      label: screenShieldActive ? routePseudonym(a.id) : accountDisplayLabel(a, a.id),
    })),
    ...(providersQuery.data ?? []).flatMap((provider) =>
      provider.credentials.map((credential) => ({
        value: credential.id,
        label: screenShieldActive
          ? routePseudonym(credential.id)
          : `${credential.label} · ${provider.display_name}`,
      })),
    ),
  ];
  const providerOptions = [
    { value: ALL, label: "all" },
    { value: "codex", label: "codex" },
    { value: "claude", label: "claude" },
    ...(providersQuery.data ?? [])
      .filter((provider) => provider.slug !== "codex" && provider.slug !== "anthropic")
      .map((provider) => ({ value: provider.slug, label: provider.display_name })),
  ];

  const pageStart = data.total > 0 ? offset + 1 : 0;
  const pageEnd = offset + rows.length;
  const hasPrev = offset > 0;
  const hasNext = offset + rows.length < data.total;
  const totalPages = Math.max(1, Math.ceil(data.total / PAGE_SIZE));
  const currentPage = Math.min(totalPages, Math.floor(offset / PAGE_SIZE) + 1);
  const pageNumbers = paginationWindow(currentPage, totalPages);

  function goToPage(page: number) {
    const boundedPage = Math.min(totalPages, Math.max(1, page));
    setParam("offset", String((boundedPage - 1) * PAGE_SIZE), { resetOffset: false });
    setExpandedId(null);
  }

  return (
    <div className="flex flex-col gap-3">
      <PageHeader
        subtitle={
          <>
            {data.total.toLocaleString()} {data.total === 1 ? "request" : "requests"} ·{" "}
            {requestIdFilter ? "exact correlation lookup" : <>last {range}</>}
            {dataUpdatedAt ? <> · updated {relTime(Math.floor(dataUpdatedAt / 1000), nowMs)}</> : null}
          </>
        }
        actions={
          <LiveBadge
            isFetching={isFetching}
            sseConnected={requestStream.connected}
          />
        }
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
          label="Target"
          value={accountFilter}
          onChange={(v) => setParam("account", v)}
          options={targetOptions}
        />
        <FilterSelect
          label="Provider"
          value={providerFilter}
          onChange={(v) => setParam("provider", v)}
          options={providerOptions}
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
            { value: "http", label: "HTTP" },
            { value: "sse", label: "SSE" },
            { value: "ws", label: "WS" },
          ]}
        />

        <form
          className="ml-auto flex shrink-0 items-center gap-1.5 rounded border border-border bg-card px-2 py-1 text-[10.5px] text-fg"
          onSubmit={(event) => {
            event.preventDefault();
            setParam("request_id", searchId.trim());
          }}
        >
          <Search className="h-3 w-3 shrink-0 opacity-60" strokeWidth={2} />
          <input
            value={searchId}
            onChange={(e) => setSearchId(e.target.value)}
            placeholder="Correlation ID…"
            aria-label="Find request by correlation ID"
            className="w-[184px] bg-transparent text-fg outline-none placeholder:opacity-50"
          />
          <button
            type="submit"
            className="rounded bg-muted px-2 py-0.5 text-[9.5px] font-semibold opacity-70 hover:opacity-100"
          >
            Find
          </button>
        </form>
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
              accountById={accountById}
              credentialLabelById={credentialLabelById}
              hiddenBySearch={rows.length - visibleRows.length}
              expandedId={expandedId}
              onToggle={(id) => setExpandedId((cur) => (cur === id ? null : id))}
            />
          </Col>
        </Grid>
      )}

      {rows.length > 0 && (
        <div className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-border bg-card px-3 py-2.5 text-[10.5px] text-fg">
          <div>
            <div className="font-semibold tabular-nums">
              {pageStart}–{pageEnd} of {data.total.toLocaleString()}
            </div>
            <div className="mt-0.5 text-[9px] opacity-50">
              Page {currentPage.toLocaleString()} of {totalPages.toLocaleString()} · newest first
            </div>
          </div>
          <nav aria-label="Request pages" className="flex flex-wrap items-center justify-end gap-1">
            <button
              type="button"
              disabled={!hasPrev}
              onClick={() => goToPage(1)}
              className="rounded border border-border px-2 py-1.5 font-medium opacity-70 hover:bg-muted hover:opacity-100 disabled:pointer-events-none disabled:opacity-30"
            >
              First
            </button>
            <button
              type="button"
              disabled={!hasPrev}
              onClick={() => goToPage(currentPage - 1)}
              className="flex items-center gap-1 rounded border border-border px-2 py-1.5 font-medium opacity-70 hover:bg-muted hover:opacity-100 disabled:pointer-events-none disabled:opacity-30"
            >
              <ChevronLeft className="h-3 w-3" strokeWidth={2} />
              Prev
            </button>
            {pageNumbers.map((page) => (
              <button
                key={page}
                type="button"
                aria-current={page === currentPage ? "page" : undefined}
                aria-label={`Page ${page}`}
                onClick={() => goToPage(page)}
                className={clsx(
                  "min-w-7 rounded border px-2 py-1.5 font-semibold tabular-nums",
                  page === currentPage
                    ? "border-accent/40 bg-accent/15 text-accent"
                    : "border-border opacity-65 hover:bg-muted hover:opacity-100",
                )}
              >
                {page}
              </button>
            ))}
            <button
              type="button"
              disabled={!hasNext}
              onClick={() => goToPage(currentPage + 1)}
              className="flex items-center gap-1 rounded border border-border px-2 py-1.5 font-medium opacity-70 hover:bg-muted hover:opacity-100 disabled:pointer-events-none disabled:opacity-30"
            >
              Next
              <ChevronRight className="h-3 w-3" strokeWidth={2} />
            </button>
            <button
              type="button"
              disabled={!hasNext}
              onClick={() => goToPage(totalPages)}
              className="rounded border border-border px-2 py-1.5 font-medium opacity-70 hover:bg-muted hover:opacity-100 disabled:pointer-events-none disabled:opacity-30"
            >
              Last
            </button>
          </nav>
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

/** Honest transport state: SSE while the authenticated stream is open, 30-second polling only
 * while disconnected/disabled. `isFetching` reflects the durable `/api/requests` refresh. */
function LiveBadge({
  isFetching,
  sseConnected,
}: {
  isFetching: boolean;
  sseConnected: boolean;
}) {
  return (
    <span
      className={clsx(
        "flex shrink-0 items-center gap-1.5 whitespace-nowrap rounded border px-2.5 py-1 text-[10.5px]",
        sseConnected
          ? "border-success/30 bg-success/[0.08] text-success"
          : "border-warn/30 bg-warn/[0.08] text-warn",
      )}
    >
      <span className="relative flex h-[7px] w-[7px] shrink-0">
        {isFetching && (
          <span
            className={clsx(
              "absolute inline-flex h-full w-full animate-ping rounded-full opacity-60 motion-reduce:hidden",
              sseConnected ? "bg-success" : "bg-warn",
            )}
          />
        )}
        <span
          className={clsx(
            "relative inline-flex h-[7px] w-[7px] rounded-full",
            sseConnected ? "bg-success" : "bg-warn",
          )}
        />
      </span>
      {requestLiveLabel({ sseConnected, isFetching })}
    </span>
  );
}

// ---------------------------------------------------------------------------------------------
// Table
// ---------------------------------------------------------------------------------------------

const TABLE_HEAD_CLASS =
  "px-2.5 py-2 text-left text-[9px] font-medium uppercase tracking-wide text-fg opacity-60";

function RequestsTable({
  rows,
  accountById,
  credentialLabelById,
  hiddenBySearch,
  expandedId,
  onToggle,
}: {
  rows: RequestRowView[];
  accountById: Map<string, AccountView>;
  credentialLabelById: Map<string, string>;
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
        <table className="w-full min-w-[940px] border-collapse text-[11px]">
          <thead>
            <tr className="border-b border-border">
              <th className={TABLE_HEAD_CLASS}>Time</th>
              <th className={TABLE_HEAD_CLASS}>Target</th>
              <th className={TABLE_HEAD_CLASS}>Provider</th>
              <th className={TABLE_HEAD_CLASS}>Model</th>
              <th className={TABLE_HEAD_CLASS}>Transport</th>
              <th className={TABLE_HEAD_CLASS}>Status</th>
              <th className={clsx(TABLE_HEAD_CLASS, "text-right")}>TTFT</th>
              <th className={clsx(TABLE_HEAD_CLASS, "text-right")}>Latency</th>
              <th className={clsx(TABLE_HEAD_CLASS, "text-right")}>Throughput</th>
              <th className={clsx(TABLE_HEAD_CLASS, "text-right")}>Tokens</th>
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 ? (
              <tr>
                <td colSpan={10} className="px-2 py-3 text-center text-fg opacity-50">
                  No rows on this page match your search.
                </td>
              </tr>
            ) : (
              rows.map((r) => (
                <RequestRow
                  key={r.id}
                  row={r}
                  accountLabel={
                    r.target_kind === "credential" && r.provider_credential_id
                      ? credentialLabelById.get(r.provider_credential_id) ??
                        r.provider_credential_id
                      : r.account_id
                      ? accountDisplayLabel(accountById.get(r.account_id), r.account_id)
                      : undefined
                  }
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
  accountLabel,
  expanded,
  onToggle,
}: {
  row: RequestRowView;
  accountLabel?: string;
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
        <td className="whitespace-nowrap px-2.5 py-2.5 font-mono tabular-nums text-fg opacity-90">
          <div>
            <ChevronRight
              className={clsx("mr-1 inline h-3 w-3 opacity-50 transition-transform", expanded && "rotate-90")}
              strokeWidth={2}
            />
            {formatClock(r.requested_at)}
          </div>
          {r.request_id && (
            <div title={r.request_id} className="ml-4 mt-0.5 text-[8.5px] text-accent opacity-70">
              req {r.request_id.slice(0, 8)}
            </div>
          )}
          {r.session_key && (
            <Link
              to={`/sessions?session_key=${encodeURIComponent(r.session_key)}`}
              title={`Open session ${r.session_key}`}
              onClick={(event) => event.stopPropagation()}
              className="ml-4 mt-0.5 block text-[8.5px] text-fg opacity-50 hover:text-accent hover:opacity-100"
            >
              session {r.session_key.slice(0, 8)}
            </Link>
          )}
        </td>
        <td className="whitespace-nowrap px-2.5 py-2.5 text-fg opacity-90">
          {r.target_kind === "credential" && r.provider_credential_id ? (
            <ShieldedAccount
              id={r.provider_credential_id}
              label={accountLabel ?? r.provider_credential_id}
            />
          ) : r.account_id ? (
            <ShieldedAccount
              id={r.account_id}
              label={accountLabel ?? accountDisplayLabel(undefined, r.account_id)}
            />
          ) : (
            "—"
          )}
        </td>
        <td className="px-2.5 py-2.5">
          <ProviderTag provider={r.provider} />
        </td>
        <td className="px-2.5 py-2.5">
          <ModelCell
            model={r.model}
            effort={r.reasoning_effort}
            tier={r.service_tier}
            subagent={r.subagent}
          />
        </td>
        <td className="px-2.5 py-2.5">
          <TransportPill transport={r.transport} />
        </td>
        <td className="px-2.5 py-2.5">
          <HttpStatusPill row={r} />
        </td>
        <td className="whitespace-nowrap px-2.5 py-2.5 text-right tabular-nums text-fg opacity-80">
          {latency(r.ttft_ms)}
        </td>
        <td className="whitespace-nowrap px-2.5 py-2.5 text-right tabular-nums text-fg opacity-80">
          {latency(r.duration_ms)}
        </td>
        <td className="whitespace-nowrap px-2.5 py-2.5 text-right tabular-nums text-fg opacity-80">
          {tpsFmt(r.tps)}
        </td>
        <td className="whitespace-nowrap px-2.5 py-2.5 text-right text-fg opacity-90">
          <TokensCell total={r.total_tokens} cached={r.cached_tokens} />
        </td>
      </tr>
      {expanded && (
        <tr className="border-b border-border/55 bg-muted/30 last:border-0">
          <td colSpan={10} className="p-0">
            <RequestDetailPanel row={r} accountLabel={accountLabel} />
          </td>
        </tr>
      )}
    </>
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
