// The Live Logs console: `GET /api/logs/stream` (`crates/polyflare-server/src/sse.rs`'s
// `logs_stream_handler`) via `useLogStream` (Task 2, `lib/useLogStream.ts` — fetch+ReadableStream,
// NOT EventSource, because EventSource can't send the `Authorization: Bearer` header this endpoint
// requires; see that file's own header comment). This is the page the user asked for by name — "it
// should be able to see live logs if the feature flag is enabled" — so it gets the most direct
// possible rendering of what the backend actually streams.
//
// CONTENT-SAFETY: `crates/polyflare-server/src/log_bus.rs`'s `LogEvent` is content-free BY
// CONSTRUCTION (see that module's own doc comment) — it is built exclusively from
// `RequestLog::to_log_event` (`observability.rs`), which draws from the same audited field set as
// the persisted `request_log` row: counts, ids, statuses, timings, never a body/prompt/response/
// key. This page renders ONLY the 10 real fields on the wire `LogEvent` type (`lib/api.ts`):
// ts_ms, level, provider, account, model, status, latency_ms, subagent, kind, message. `subagent`
// is the codex sub-agent role label from `x-openai-subagent` (`"review"` / `"compact"` /
// `"memory_consolidation"` / `"collab_spawn"`) — a bounded role slug, i.e. content-free routing
// metadata, NOT conversation content; same content-safety class as `model`/`provider`. It is
// `#[serde(skip_serializing_if = "Option::is_none")]` on the wire like `provider`/`account`/`model`,
// so it is simply absent (not `null`) for the main agent and for non-request events — this page
// therefore only renders the tag when the field is actually present, exactly like the other
// optional fields below, never fabricating a "main" label for events that carry no such signal. No
// tooltip, no "expand for detail" affordance, no field beyond this set is added anywhere below —
// there is nothing else in the event to show, so nothing else is shown. `message` itself is the
// backend's own pre-built content-free string (e.g. `"req 200 · codex · 707ms"`, see
// `RequestLog::to_log_event`) and is rendered verbatim, never parsed/re-colorized by guessing at
// substrings (the mockup's per-token rainbow message is its own illustrative embellishment, not a
// real wire shape — reproducing it would require inventing structure the backend doesn't send).
//
// Flag gating (must AGREE with the Sidebar's nav-item hiding, not contradict it): primarily gated
// on `useCapabilityFlags().liveLogs` (same source Sidebar.tsx already uses to hide the nav item —
// false while `/api/capabilities` is loading, per `CapabilitiesProvider`'s own doc comment, same
// transient-false-on-load behavior every other capability consumer in this app already accepts).
// `useLogStream` is only ever invoked with `enabled: liveLogs`, so with the flag off no connection
// is attempted at all. As a second, independent guard for the direct-URL case the brief calls out
// (capabilities said "on" but the live server 404s anyway — e.g. a stale/cached capabilities
// response, or the flag flipped between page loads), the hook's own `disabled` (set exactly when
// the stream response is a real 404) ALSO forces the disabled notice. Either signal is sufficient;
// neither can be overridden by the other into showing a console that isn't actually streaming.
//
// Known backend ledger issue (progress.md): `observability.rs::to_log_event`'s status→level
// mapping is inverted (429/5xx → Warn, other 4xx → Error) — a recorded existing bug, out of scope
// for this frontend task. This page does NOT re-derive `level` from `status` anywhere; it renders
// whatever `level` the wire event actually carries and filters on that same value, so nothing here
// depends on the mapping being correct — if/when the backend bug is fixed, this page's behavior is
// unchanged.
import { useEffect, useRef, useState, type ReactNode } from "react";

import type { LogEvent, LogLevel } from "../lib/api";
import { useCapabilityFlags } from "../capabilities/CapabilitiesProvider";
import { useLogStream } from "../lib/useLogStream";
import { Card } from "../ui/Card";
import { Col, Grid } from "../ui/Grid";
import {
  ArrowDownToLine,
  Clock as ClockIcon,
  EyeOff,
  Pause as PauseIcon,
  Play as PlayIcon,
  Search,
  Trash2,
} from "../ui/icons";
import { providerBrandKey } from "../ui/ProviderTag";

import clsx from "clsx";

type LevelFilter = "all" | "info" | "warn" | "error";

const LEVEL_TABS: Array<{ value: LevelFilter; label: string }> = [
  { value: "all", label: "All" },
  { value: "info", label: "Info" },
  { value: "warn", label: "Warn" },
  { value: "error", label: "Error" },
];

/** Per-level tab highlight — distinct tint per level (mirrors `live-logs.html`'s `.seg span.on` /
 * `.warn.on` / `.err.on`), not a single accent color for every tab. */
const LEVEL_TAB_ON_CLASS: Record<LevelFilter, string> = {
  all: "bg-accent/[0.14] text-accent",
  info: "bg-success/15 text-success",
  warn: "bg-warn/15 text-warn",
  error: "bg-error/15 text-error",
};

/** Per-line level tag color (mockup's `.lvl.i/.w/.e/.d`). `debug` only ever appears under the "All"
 * filter tab (there is no dedicated Debug tab, matching both the brief and the mockup). */
const LEVEL_TEXT_CLASS: Record<LogLevel, string> = {
  info: "text-success",
  warn: "text-warn",
  error: "text-error",
  debug: "text-fg opacity-45",
};

/** Colors an account id by its provider's brand token (mockup's `.acc`/`.cl` spans) — same brand
 * mapping `ProviderTag` uses (`providerBrandKey`), applied to plain text instead of a chip so it
 * reads as part of the log line rather than a separate pill. Falls back to a neutral tone when
 * `provider` is absent (a request can fail before an account/provider is resolved). */
function accountTextClass(provider: string | undefined): string {
  if (provider === undefined) return "text-fg opacity-80";
  return providerBrandKey(provider) === "claude" ? "text-claude" : "text-codex";
}

/** HTTP status → text color, matching `Requests.tsx`'s `httpStatusTone` convention exactly (429 is
 * `warn`, not `error` — same domain, same tones, so a status code reads identically on both pages)
 * rather than inventing a second, slightly different rule here. */
function httpStatusTextClass(status: number): string {
  if (status === 429) return "text-warn";
  if (status >= 400) return "text-error";
  if (status >= 300) return "text-warn";
  return "text-success";
}

function formatLogTs(tsMs: number): string {
  const d = new Date(tsMs);
  const pad = (n: number, len = 2) => String(n).padStart(len, "0");
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`;
}

/** Mitigates a duplicate-on-reconnect gap in `useLogStream` (Task 2 — not modified here per the
 * brief's "use it, don't rewrite it"): `sse.rs::logs_stream_handler` sends the *entire current ring
 * buffer* as backfill on every fresh subscription, and `useLogStream.connect()` calls
 * `LogBus::subscribe()` again on EVERY reconnect (an explicit `resume()` after `pause()`, or its own
 * network-drop auto-reconnect) — but only ever appends to `lines`, never resets it first. Live-
 * verified: 3 events → Pause → 2 more published server-side while paused → Resume → the buffer held
 * 8 lines (the original 3 duplicated, plus the 5 real ones from the fresh backfill). This is a
 * genuine correctness gap in the hook, not a quirk of this page, but the brief scopes
 * `useLogStream.ts` out of this task — so instead of touching it, every render here first collapses
 * `stream.lines` down to one entry per distinct event. The key is every real content-free field
 * (never anything invented): two entries are "the same event" only if ts_ms/level/kind/message/
 * status/latency_ms/account/provider/model/subagent all match, which is true for a byte-for-byte
 * repeat backfill replay and false for any two genuinely different events (even ones a millisecond
 * apart). First occurrence wins, so chronological order is preserved. */
function logEventKey(ev: LogEvent): string {
  return [
    ev.ts_ms,
    ev.level,
    ev.kind,
    ev.message,
    ev.status,
    ev.latency_ms,
    ev.account,
    ev.provider,
    ev.model,
    ev.subagent,
  ].join("|");
}

function dedupeLogEvents(lines: LogEvent[]): LogEvent[] {
  const seen = new Set<string>();
  const out: LogEvent[] = [];
  for (const ev of lines) {
    const key = logEventKey(ev);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(ev);
  }
  return out;
}

/** Case-insensitive substring match over every real, renderable text-ish field on the event — the
 * page's one client-side "search" affordance. Not URL-synced (this is a live in-memory buffer, not
 * a server query — there is nothing to deep-link to). */
function matchesTextFilter(ev: LogEvent, needle: string): boolean {
  if (needle === "") return true;
  const haystack = [
    ev.kind,
    ev.message,
    ev.account,
    ev.provider,
    ev.model,
    ev.subagent,
    ev.status?.toString(),
  ]
    .filter((v): v is string => v !== undefined && v !== null)
    .join(" ")
    .toLowerCase();
  return haystack.includes(needle);
}

export function LiveLogs() {
  const { liveLogs } = useCapabilityFlags();
  const stream = useLogStream({ enabled: liveLogs });

  const [levelFilter, setLevelFilter] = useState<LevelFilter>("all");
  const [textFilter, setTextFilter] = useState("");
  const [autoScroll, setAutoScroll] = useState(true);
  /** Tracks the user's own Pause/Resume choice — distinct from `stream.connected` (a paused stream
   * is also disconnected, but "paused" and "dropped mid-stream" must read differently to the user;
   * see `LiveIndicator` below). */
  const [paused, setPaused] = useState(false);

  const consoleRef = useRef<HTMLDivElement>(null);

  // Disabled state must agree in both directions — see the file-header comment.
  const disabled = !liveLogs || stream.disabled;

  // De-duplicate first (see `dedupeLogEvents`'s doc comment — a `useLogStream` reconnect gap, not
  // this page's own bug), THEN apply level/text filters on top of the deduplicated set.
  const dedupedLines = dedupeLogEvents(stream.lines);
  const visibleLines = dedupedLines.filter(
    (ev) => (levelFilter === "all" || ev.level === levelFilter) && matchesTextFilter(ev, textFilter),
  );

  // Auto-scroll-to-bottom: only runs while the toggle is on, and only reacts to the FILTERED count
  // (so filtering to zero rows, or narrowing the view, doesn't itself yank the scroll position).
  useEffect(() => {
    if (!autoScroll) return;
    const el = consoleRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [autoScroll, visibleLines.length]);

  function togglePause() {
    if (paused) {
      setPaused(false);
      stream.resume();
    } else {
      setPaused(true);
      stream.pause();
    }
  }

  if (disabled) {
    return (
      <div className="flex flex-col gap-3">
        <PageHeader />
        <DisabledNotice />
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-3">
      <PageHeader actions={<LiveIndicator connected={stream.connected} paused={paused} />} />

      <div className="flex flex-wrap items-center gap-2">
        <div className="flex shrink-0 overflow-hidden rounded border border-border bg-card text-[10.5px]">
          {LEVEL_TABS.map((t) => (
            <button
              key={t.value}
              type="button"
              onClick={() => setLevelFilter(t.value)}
              className={clsx(
                "px-2.5 py-1",
                levelFilter === t.value
                  ? clsx("font-medium", LEVEL_TAB_ON_CLASS[t.value])
                  : "text-fg opacity-60 hover:opacity-100",
              )}
            >
              {t.label}
            </button>
          ))}
        </div>

        <button
          type="button"
          onClick={togglePause}
          className={clsx(
            "flex shrink-0 items-center gap-1.5 rounded border px-2.5 py-1 text-[10.5px]",
            paused
              ? "border-accent/40 bg-accent/[0.1] text-accent"
              : "border-border bg-card text-fg opacity-80 hover:opacity-100",
          )}
        >
          {paused ? (
            <PlayIcon className="h-3 w-3" strokeWidth={2} />
          ) : (
            <PauseIcon className="h-3 w-3" strokeWidth={2} />
          )}
          {paused ? "Resume" : "Pause"}
        </button>

        <button
          type="button"
          onClick={() => stream.clear()}
          className="flex shrink-0 items-center gap-1.5 rounded border border-border bg-card px-2.5 py-1 text-[10.5px] text-fg opacity-80 hover:opacity-100"
        >
          <Trash2 className="h-3 w-3" strokeWidth={2} />
          Clear
        </button>

        <button
          type="button"
          onClick={() => setAutoScroll((v) => !v)}
          className={clsx(
            "flex shrink-0 items-center gap-1.5 rounded border px-2.5 py-1 text-[10.5px]",
            autoScroll
              ? "border-accent/40 bg-accent/[0.1] text-accent"
              : "border-border bg-card text-fg opacity-80 hover:opacity-100",
          )}
        >
          <ArrowDownToLine className="h-3 w-3" strokeWidth={2} />
          Auto-scroll
        </button>

        <div className="ml-auto flex shrink-0 items-center gap-1.5 rounded border border-border bg-card px-2.5 py-1 text-[10.5px] text-fg opacity-80">
          <Search className="h-3 w-3 shrink-0 opacity-60" strokeWidth={2} />
          <input
            value={textFilter}
            onChange={(e) => setTextFilter(e.target.value.toLowerCase())}
            placeholder="Filter…"
            className="w-[168px] bg-transparent text-fg outline-none placeholder:opacity-50"
          />
        </div>
      </div>

      <Grid>
        <Col span={12}>
          <Card className="h-[560px]">
            <div
              ref={consoleRef}
              className="-mx-3.5 -my-3 flex-1 overflow-y-auto bg-bg py-1.5 font-mono text-[11px] leading-[1.55]"
            >
              {visibleLines.length === 0 ? (
                <p className="px-3.5 py-2 text-fg opacity-50">
                  {dedupedLines.length === 0
                    ? "Waiting for log events…"
                    : "No lines match the current filters."}
                </p>
              ) : (
                visibleLines.map((ev) => <LogLine key={logEventKey(ev)} ev={ev} />)
              )}
            </div>
          </Card>
        </Col>
      </Grid>

      <Footer shown={visibleLines.length} total={dedupedLines.length} />
    </div>
  );
}

// ---------------------------------------------------------------------------------------------
// Header
// ---------------------------------------------------------------------------------------------

function PageHeader({ actions }: { actions?: ReactNode }) {
  return (
    <div className="flex flex-wrap items-center justify-between gap-3">
      <div>
        <h1 className="text-lg font-semibold text-fg">Live Logs</h1>
        <p className="mt-0.5 text-[11px] text-fg opacity-60">operational events · content-free</p>
      </div>
      {actions}
    </div>
  );
}

/** The header's Live·SSE badge. Reflects two REAL states, not a decorative always-green dot:
 * `paused` (the user's own Pause click — the stream is intentionally closed) vs `connected`
 * (`useLogStream`'s own open-2xx-stream flag). Anything else (enabled, not paused, not yet
 * connected/reconnecting) reads as "Reconnecting…", which is honest — `useLogStream` retries with
 * backoff on its own; this page adds no separate retry logic. */
function LiveIndicator({ connected, paused }: { connected: boolean; paused: boolean }) {
  if (paused) {
    return (
      <span className="flex shrink-0 items-center gap-1.5 whitespace-nowrap rounded border border-border bg-card px-2.5 py-1 text-[10.5px] text-fg opacity-70">
        <PauseIcon className="h-3 w-3" strokeWidth={2} />
        Paused
      </span>
    );
  }
  if (connected) {
    return (
      <span className="flex shrink-0 items-center gap-1.5 whitespace-nowrap rounded border border-success/30 bg-success/[0.08] px-2.5 py-1 text-[10.5px] text-success">
        <span className="relative flex h-[7px] w-[7px] shrink-0">
          <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-success opacity-60 motion-reduce:hidden" />
          <span className="relative inline-flex h-[7px] w-[7px] rounded-full bg-success" />
        </span>
        Live · SSE
      </span>
    );
  }
  return (
    <span className="flex shrink-0 items-center gap-1.5 whitespace-nowrap rounded border border-warn/30 bg-warn/[0.08] px-2.5 py-1 text-[10.5px] text-warn">
      <ClockIcon className="h-3 w-3" strokeWidth={2} />
      Reconnecting…
    </span>
  );
}

// ---------------------------------------------------------------------------------------------
// Disabled notice — the flag-off path. Distinct from a loading spinner or an error card: this is
// an expected, discoverable server configuration state (per `sse.rs`'s own doc comment: gated so
// the capability is "discoverable via /api/capabilities rather than silently absent").
// ---------------------------------------------------------------------------------------------

function DisabledNotice() {
  return (
    <Card>
      <div className="flex flex-col items-center gap-2 py-10 text-center">
        <EyeOff className="h-6 w-6 text-fg opacity-40" strokeWidth={1.7} />
        <p className="text-[13px] font-medium text-fg opacity-80">Live logs disabled</p>
        <p className="max-w-md text-[11px] text-fg opacity-55">
          Set <code className="rounded bg-muted px-1 py-0.5 font-mono">POLYFLARE_LIVE_LOGS=1</code> and
          restart the server to enable this page. While off,{" "}
          <code className="rounded bg-muted px-1 py-0.5 font-mono">GET /api/logs/stream</code> returns
          404 and the Live Logs nav item stays hidden.
        </p>
      </div>
    </Card>
  );
}

// ---------------------------------------------------------------------------------------------
// Console line — every span below is a real `LogEvent` field. See the file-header comment.
// ---------------------------------------------------------------------------------------------

function LogLine({ ev }: { ev: LogEvent }) {
  return (
    <div className="flex flex-wrap items-baseline gap-x-2.5 gap-y-0 px-3.5 py-0.5 hover:bg-muted/50">
      <span className="shrink-0 tabular-nums text-fg opacity-45">{formatLogTs(ev.ts_ms)}</span>
      <span className={clsx("w-[38px] shrink-0 text-[9px] font-bold uppercase tracking-wide", LEVEL_TEXT_CLASS[ev.level])}>
        {ev.level}
      </span>
      <span className="shrink-0 text-fg opacity-40">{ev.kind}</span>
      {ev.account !== undefined && (
        <span className={clsx("shrink-0 font-medium", accountTextClass(ev.provider))}>{ev.account}</span>
      )}
      {ev.provider !== undefined && (
        <span className="shrink-0 text-fg opacity-45">{providerBrandKey(ev.provider)}</span>
      )}
      {ev.model !== undefined && <span className="shrink-0 text-fg opacity-45">{ev.model}</span>}
      {ev.subagent != null && (
        <span className="shrink-0 whitespace-nowrap rounded bg-accent/15 px-1 py-0 text-[9px] font-bold lowercase leading-none text-accent">
          {ev.subagent}
        </span>
      )}
      <span className="min-w-0 flex-1 break-words text-fg opacity-90">{ev.message}</span>
      {ev.status !== undefined && (
        <span className={clsx("shrink-0 tabular-nums", httpStatusTextClass(ev.status))}>{ev.status}</span>
      )}
      {ev.latency_ms !== undefined && (
        <span className="shrink-0 tabular-nums text-fg opacity-45">{ev.latency_ms}ms</span>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------------------------
// Footer — notes the real 1000-line client-side cap (`useLogStream`'s own `MAX_LINES`, not
// duplicated here) and the real auto-reconnect behavior (also entirely `useLogStream`'s own, not
// re-implemented on this page).
// ---------------------------------------------------------------------------------------------

function Footer({ shown, total }: { shown: number; total: number }) {
  return (
    <div className="flex flex-wrap items-center justify-between gap-2 text-[9.5px] text-fg opacity-50">
      <span>
        Showing {shown.toLocaleString()} of {total.toLocaleString()} buffered lines · buffer holds the
        last 1000 · oldest evicted first
      </span>
      <span>Drops reconnect automatically with backoff · Clear only resets this browser's buffer</span>
    </div>
  );
}
