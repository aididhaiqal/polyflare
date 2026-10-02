// One Claude seat's `/usage`-style card — shared by the Analytics page's "Claude usage" section
// and the standalone /claude page. Pure presentation over `ClaudeSeatUsageView`.
import clsx from "clsx";

import type { ClaudeSeatUsageView, ClaudeUsageLine } from "../lib/api";
import { usageLabel, usageSummary, usageTone } from "../lib/claudeUsage";
import { planLabel, relTime } from "../lib/format";
import { Card } from "./Card";
import { StatusPill } from "./StatusPill";

const FILL_BY_TONE = {
  ok: "bg-accent",
  warn: "bg-warn",
  error: "bg-warn/80",
} as const;

export function ClaudeSeatCard({ seat, nowMs }: { seat: ClaudeSeatUsageView; nowMs: number }) {
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
