// Claude usage — what Claude Code's `/usage` prints, for every Claude seat in the pool. Reads
// `GET /api/claude/usage` (read_api.rs::claude_usage_handler): per seat the 5-hour session, the
// all-models week, each per-model week the upstream reports, and the extra-usage state. The
// numbers come from PolyFlare's own usage poll of `/api/oauth/usage`, not from relayed traffic,
// so they are right even when Claude Code talks to Anthropic directly.
import { useEffect, useState } from "react";
import clsx from "clsx";

import type { ClaudeSeatUsageView, ClaudeUsageLine } from "../lib/api";
import { usageLabel, usageSummary, usageTone } from "../lib/claudeUsage";
import { planLabel, relTime } from "../lib/format";
import { useClaudeUsage } from "../lib/queries";
import { Card } from "../ui/Card";
import { StatusPill } from "../ui/StatusPill";

const FILL_BY_TONE = {
  ok: "bg-accent",
  warn: "bg-warn",
  error: "bg-warn/80",
} as const;

export function ClaudeUsage() {
  const { data, isLoading, isError } = useClaudeUsage();
  const [nowMs, setNowMs] = useState(() => Date.now());
  useEffect(() => {
    const id = window.setInterval(() => setNowMs(Date.now()), 30_000);
    return () => window.clearInterval(id);
  }, []);

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-[15px] font-semibold text-fg">Claude usage</h1>
        <p className="mt-1 text-[12px] text-fg opacity-60">
          What <code className="rounded bg-muted px-1">/usage</code> shows in Claude Code, for every Claude seat — from
          PolyFlare's own poll of the seat, so it is right whether or not the traffic went through here.
        </p>
      </div>
      {isLoading && <p className="text-[12px] text-fg opacity-60">Loading…</p>}
      {isError && <p className="text-[12px] text-warn">Could not load Claude usage.</p>}
      {data && data.length === 0 && (
        <p className="text-[12px] text-fg opacity-60">No Claude seats are onboarded.</p>
      )}
      <div className="grid grid-cols-1 gap-3 xl:grid-cols-2">
        {data?.map((seat) => <SeatCard key={seat.id} seat={seat} nowMs={nowMs} />)}
      </div>
    </div>
  );
}

function SeatCard({ seat, nowMs }: { seat: ClaudeSeatUsageView; nowMs: number }) {
  const lines: ClaudeUsageLine[] = [
    ...(seat.session ? [seat.session] : []),
    ...(seat.weekly_all ? [seat.weekly_all] : []),
    ...seat.per_model,
  ];
  return (
    <Card>
      <div className="flex flex-wrap items-center gap-2">
        <span className="truncate text-[13px] font-semibold text-fg">{seat.alias ?? seat.email}</span>
        <span className="text-[11px] text-fg opacity-60">{planLabel(seat.plan_type, "anthropic")}</span>
        <StatusPill status={seat.status} className="ml-auto" />
      </div>
      {seat.alias && <div className="mt-0.5 truncate text-[11px] text-fg opacity-50">{seat.email}</div>}

      <div className="mt-3 space-y-3">
        {lines.length === 0 && (
          <p className="text-[12px] text-fg opacity-55">No usage reported for this seat yet.</p>
        )}
        {lines.map((line) => (
          <UsageRow key={`${line.kind}:${line.model ?? ""}`} line={line} nowMs={nowMs} />
        ))}
      </div>

      <div className="mt-3 flex flex-wrap items-center gap-x-3 gap-y-1 border-t border-border/70 pt-2.5 text-[11px] text-fg opacity-60">
        <span>
          Extra usage:{" "}
          {seat.extra_usage_enabled === null
            ? "unknown"
            : seat.extra_usage_enabled
              ? "enabled"
              : `disabled${seat.extra_usage_disabled_reason ? ` (${seat.extra_usage_disabled_reason.replace(/_/g, " ")})` : ""}`}
        </span>
        <span className="ml-auto">
          {seat.source === "poll" && seat.polled_at !== null
            ? `polled ${relTime(seat.polled_at, nowMs)}`
            : "from stored windows · per-model detail arrives with the next poll"}
        </span>
      </div>
    </Card>
  );
}

function UsageRow({ line, nowMs }: { line: ClaudeUsageLine; nowMs: number }) {
  const tone = usageTone(line);
  const width = Math.max(0, Math.min(100, line.percent));
  return (
    <div>
      <div className="flex items-baseline justify-between gap-2 text-[12px]">
        <span className="font-medium text-fg">{usageLabel(line)}</span>
        <span className={clsx("tabular-nums", tone === "ok" ? "text-fg opacity-70" : "text-warn")}>
          {usageSummary(line, nowMs)}
        </span>
      </div>
      <div className="mt-1 h-1.5 w-full overflow-hidden rounded-full bg-muted">
        <div className={clsx("h-full rounded-full", FILL_BY_TONE[tone])} style={{ width: `${width}%` }} />
      </div>
    </div>
  );
}
