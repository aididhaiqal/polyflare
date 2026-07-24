// The Sessions page: `GET /api/sessions` (`useSessions(params)`) is the ONLY endpoint this page
// consumes. It answers the operator's "which account is this conversation sticky to?" question —
// session→account affinity + the stickiness state machine, made visible and read-only.
//
// CONTENT-SAFETY: `continuity_sessions` is content-free by construction. `session_key` is a sha256
// hash (one-way — never a raw header, prompt, or body; see read_api.rs' module doc), and the view
// carries NOTHING else that could leak content — only the owner id/email, the state, the
// content-free `required_capabilities` tag set, and timestamps. This page renders ONLY the real
// fields on `SessionRowView` (api.ts mirror of read_api.rs::SessionRowView); the inline notice at
// the bottom of the table states that plainly.
//
// Field mapping (table columns, in order):
//   Session       <- s.session_key, truncated to the first 12 chars + monospace. Truncation is
//                    display-only (the hash is long and opaque); the full value is title-attr'd for
//                    copy/inspect. `key_strength` ("hard"/"soft") rides along as a small caption.
//   Owner         <- s.owner_email (the joined account email); a NULL owner (a `fresh` session that
//                    never completed a turn, or an account deleted → SET NULL — LEFT JOIN keeps the
//                    row) renders as a muted "unowned", never dropped.
//   State         <- s.state via `SessionStatePill` (anchored→success, reattaching/recover→warn,
//                    fresh→muted, anything else→neutral default). This is the continuity state
//                    machine, a different domain from the account-status `ui/StatusPill`, so a small
//                    local pill is used (same precedent as Requests.tsx's local HttpStatusPill).
//   Capabilities  <- s.required_capabilities (the content-free sticky-cyber tag set) or "—".
//   Last activity <- `format.ts::relTime(s.last_activity_at)` — relative time; rows idle past a
//                    threshold are dimmed (a client-side "stale" hint, mirroring WindowView.stale).
import { useEffect, useState, type ReactNode } from "react";
import { useSearchParams } from "react-router-dom";
import clsx from "clsx";

import type { SessionRowView, SessionsQueryParams } from "../lib/api";
import { relTime } from "../lib/format";
import { useSessions } from "../lib/queries";
import { ShieldedAccount } from "../privacy/ScreenShield";
import { Card } from "../ui/Card";
import { Col, Grid } from "../ui/Grid";
import { AlertTriangle, ChevronLeft, ChevronRight, Lock } from "../ui/icons";
import { ProviderTag } from "../ui/ProviderTag";

// Mirrors `lib/queries.ts`'s internal (non-exported) `LIST_REFETCH_MS` — kept in sync manually
// since that constant isn't exported; used only for the live badge's honest "polling Ns" label.
const POLL_SECS = 30;

const PAGE_SIZE = 50;

/** A session with no activity for this long is dimmed as a client-side "stale" hint — purely a
 * display affordance (mirrors `WindowView.stale`'s precedent), it never changes what's fetched. */
const STALE_AFTER_SECS = 3600;

// ---------------------------------------------------------------------------------------------
// Session-state tone — the continuity state machine ("fresh" | "anchored" | "reattaching" |
// "recover"), a distinct domain from `ui/StatusPill` (which tones ACCOUNT status strings). A future
// state falls into the neutral default rather than silently reading as healthy.
// ---------------------------------------------------------------------------------------------

type StateTone = "anchored" | "transient" | "fresh" | "default";

function sessionStateTone(state: string): StateTone {
  if (state === "anchored") return "anchored";
  if (state === "reattaching" || state === "recover") return "transient";
  if (state === "fresh") return "fresh";
  return "default";
}

const STATE_TONE_CLASS: Record<StateTone, string> = {
  anchored: "bg-success/15 text-success",
  transient: "bg-warn/15 text-warn",
  fresh: "bg-muted text-fg opacity-60",
  default: "bg-muted text-fg opacity-80",
};

function SessionStatePill({ state }: { state: string }) {
  return (
    <span
      className={clsx(
        "inline-block whitespace-nowrap rounded px-2 py-0.5 text-[10px] font-semibold leading-none",
        STATE_TONE_CLASS[sessionStateTone(state)],
      )}
    >
      {state}
    </span>
  );
}

// ---------------------------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------------------------

export function Sessions() {
  const [searchParams, setSearchParams] = useSearchParams();
  const offset = Math.max(0, Number(searchParams.get("offset") ?? "0") || 0);
  const sessionKey = searchParams.get("session_key") ?? "";

  // Ticks the header's "updated Xs ago" text + the per-row relative-time / stale derivation between
  // useSessions()'s 30s polls — same 5s-tick pattern Overview/Pools/Requests already use.
  const [nowMs, setNowMs] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNowMs(Date.now()), 5000);
    return () => clearInterval(id);
  }, []);

  function setOffset(next: number) {
    const params = new URLSearchParams(searchParams);
    if (next <= 0) params.delete("offset");
    else params.set("offset", String(next));
    setSearchParams(params, { replace: true });
  }

  const queryParams: SessionsQueryParams = {
    limit: PAGE_SIZE,
    offset: sessionKey ? 0 : offset,
    session_key: sessionKey || undefined,
  };
  const { data, isLoading, isFetching, isError, error, refetch, dataUpdatedAt } =
    useSessions(queryParams);

  if (isLoading) return <SessionsSkeleton />;

  if (isError) {
    return (
      <div className="flex flex-col gap-3">
        <PageHeader />
        <Card>
          <div className="flex flex-wrap items-center justify-between gap-3">
            <span className="flex items-center gap-2 text-[12px] text-error">
              <AlertTriangle className="h-4 w-4 shrink-0" strokeWidth={1.9} />
              Couldn&apos;t load sessions
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

  const rows = data.rows;
  const pageStart = data.total > 0 ? (sessionKey ? 1 : offset + 1) : 0;
  const pageEnd = (sessionKey ? 0 : offset) + rows.length;
  const hasPrev = offset > 0;
  const hasNext = offset + rows.length < data.total;

  return (
    <div className="flex flex-col gap-3">
      <PageHeader
        subtitle={
          <>
            {data.total.toLocaleString()} {data.total === 1 ? "session" : "sessions"} · which account
            each conversation is pinned to
            {dataUpdatedAt ? <> · updated {relTime(Math.floor(dataUpdatedAt / 1000), nowMs)}</> : null}
          </>
        }
        actions={
          <div className="flex items-center gap-2">
            {sessionKey && (
              <button
                type="button"
                onClick={() => setSearchParams({}, { replace: true })}
                className="rounded border border-border bg-card px-2.5 py-1 text-[10.5px] text-fg opacity-75 hover:opacity-100"
              >
                Show all sessions
              </button>
            )}
            <LiveBadge isFetching={isFetching} />
          </div>
        }
      />

      {sessionKey && (
        <div className="rounded border border-accent/25 bg-accent/[0.06] px-3 py-2 text-[10.5px] text-fg">
          Request session <span className="font-mono text-accent">{sessionKey.slice(0, 12)}</span>
        </div>
      )}

      {rows.length === 0 ? (
        <Card>
          <p className="text-[11px] text-fg opacity-50">
            {sessionKey
              ? "That request session is no longer present in continuity history."
              : "No sessions tracked yet — they’ll appear here as soon as PolyFlare anchors a conversation to an account."}
          </p>
        </Card>
      ) : (
        <Grid>
          <Col span={12}>
            <SessionsTable rows={rows} nowMs={nowMs} />
          </Col>
        </Grid>
      )}

      {rows.length > 0 && !sessionKey && (
        <div className="flex items-center justify-between text-[10.5px] text-fg opacity-70">
          <span>
            Showing {pageStart}–{pageEnd} of {data.total.toLocaleString()} · most recent first
          </span>
          <div className="flex gap-1.5">
            <button
              type="button"
              disabled={!hasPrev}
              onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}
              className="flex items-center gap-1 rounded border border-border bg-card px-2.5 py-1 text-fg disabled:opacity-40"
            >
              <ChevronLeft className="h-3 w-3" strokeWidth={2} />
              Prev
            </button>
            <button
              type="button"
              disabled={!hasNext}
              onClick={() => setOffset(offset + PAGE_SIZE)}
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
        <h1 className="text-lg font-semibold text-fg">Sessions</h1>
        {subtitle && <p className="mt-0.5 text-[11px] text-fg opacity-60">{subtitle}</p>}
      </div>
      {actions}
    </div>
  );
}

/** The header's "live" indicator — reflects `useSessions`'s real 30s auto-refresh (the dot pulses
 * only while `isFetching`, an actual network request in flight). Same honest badge as Requests.tsx. */
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

function SessionsTable({ rows, nowMs }: { rows: SessionRowView[]; nowMs: number }) {
  return (
    <Card>
      <div className="overflow-x-auto">
        <table className="w-full min-w-[900px] border-collapse text-[10.5px]">
          <thead>
            <tr className="border-b border-border">
              <th className={TABLE_HEAD_CLASS}>Session</th>
              <th className={TABLE_HEAD_CLASS}>Provider</th>
              <th className={TABLE_HEAD_CLASS}>Target</th>
              <th className={TABLE_HEAD_CLASS}>Model</th>
              <th className={TABLE_HEAD_CLASS}>State</th>
              <th className={TABLE_HEAD_CLASS}>Capabilities</th>
              <th className={clsx(TABLE_HEAD_CLASS, "text-right")}>Requests</th>
              <th className={clsx(TABLE_HEAD_CLASS, "text-right")}>Last activity</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((s) => (
              <SessionRow key={s.session_key} row={s} nowMs={nowMs} />
            ))}
          </tbody>
        </table>
      </div>

      <div className="mt-2 flex items-center gap-1.5 rounded border border-dashed border-border bg-card px-2.5 py-1.5 text-[9.5px] text-fg opacity-60">
        <Lock className="h-3 w-3 shrink-0" strokeWidth={1.9} />
        Session keys are one-way hashes — no conversation content is stored. PolyFlare tracks only
        provider and account-or-credential target served it, its state, and timing.
      </div>
    </Card>
  );
}

function SessionRow({ row: s, nowMs }: { row: SessionRowView; nowMs: number }) {
  const idleSecs = Math.floor(nowMs / 1000) - s.last_activity_at;
  const stale = idleSecs >= STALE_AFTER_SECS;

  return (
    <tr className={clsx("border-b border-border/55 last:border-0 hover:bg-muted/60", stale && "opacity-60")}>
      <td className="px-2 py-1.5 align-top" title={s.session_key}>
        <span className="font-mono text-fg opacity-90">{s.session_key.slice(0, 12)}</span>
        <div className="mt-0.5 text-[9px] text-fg opacity-45">{s.key_strength}</div>
      </td>
      <td className="whitespace-nowrap px-2 py-1.5 align-top">
        <ProviderTag provider={s.provider} />
      </td>
      <td className="whitespace-nowrap px-2 py-1.5 align-top">
        {s.target_label ? (
          <ShieldedAccount
            id={s.target_id ?? s.target_label}
            label={s.target_label}
            className="text-fg opacity-90"
          />
        ) : (
          <span className="text-fg opacity-40">unowned</span>
        )}
        <div className="mt-0.5 text-[9px] text-fg opacity-45">{s.target_kind}</div>
      </td>
      <td className="max-w-[180px] truncate px-2 py-1.5 align-top font-mono text-[9.5px] text-fg opacity-75">
        {s.model ?? "—"}
      </td>
      <td className="px-2 py-1.5 align-top">
        <SessionStatePill state={s.state} />
      </td>
      <td className="whitespace-nowrap px-2 py-1.5 text-right align-top tabular-nums text-fg opacity-80">
        {s.request_count}
      </td>
      <td className="px-2 py-1.5 align-top">
        {s.required_capabilities ? (
          <span className="inline-block whitespace-nowrap rounded bg-muted px-2 py-0.5 text-[10px] font-medium text-fg opacity-80">
            {s.required_capabilities}
          </span>
        ) : (
          <span className="text-fg opacity-40">—</span>
        )}
      </td>
      <td className="whitespace-nowrap px-2 py-1.5 text-right align-top tabular-nums text-fg opacity-80">
        {relTime(s.last_activity_at, nowMs)}
      </td>
    </tr>
  );
}

// ---------------------------------------------------------------------------------------------
// Loading skeleton — mirrors the real header + the single full-width table panel so data arriving
// doesn't reflow.
// ---------------------------------------------------------------------------------------------

function SessionsSkeleton() {
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
        <Col span={12}>
          <Card>
            <div className="flex flex-col gap-2">
              {[0, 1, 2, 3, 4, 5].map((i) => (
                <div key={i} className="h-8 animate-pulse rounded bg-muted" />
              ))}
            </div>
          </Card>
        </Col>
      </Grid>
    </div>
  );
}
