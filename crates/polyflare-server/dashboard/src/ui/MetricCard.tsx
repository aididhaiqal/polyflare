import clsx from "clsx";

import { Card } from "./Card";
import { ArrowDown, type LucideIcon } from "./icons";
import { Sparkline } from "./Sparkline";

export interface MetricTrend {
  /** Already-formatted delta text, e.g. `"8%"` — no sign/arrow, the icon carries direction. */
  label: string;
  /** Numeric direction of the underlying value's change — controls the arrow's rotation. There is
   * deliberately only one arrow icon in the budget (`icons.ts`'s `ArrowDown`); "up" renders it
   * rotated 180°, "down" renders it as-is. */
  direction: "up" | "down";
  /** Whether this change is *good* news for this particular metric — controls color
   * (`text-success`/`text-error`). Defaults to `direction === "up"`, but callers should override it
   * for metrics where a decrease is the good outcome — e.g. avg latency going down is still
   * `text-success` in `overview-ccflare-v2.html`'s KPI row, even though its arrow points down. */
  positive?: boolean;
}

export interface MetricCardProps {
  icon: LucideIcon;
  title: string;
  /** The big headline value, e.g. `"12.4k"` or `"98.2"`. */
  value: string;
  /** Optional small suffix rendered after `value` at a smaller size, e.g. `"s"` in `"1.9s"` or
   * `"M"` in `"4.1M"` (mockup's `.kpi-val small`). */
  unit?: string;
  trend?: MetricTrend;
  /** Inline trend series (oldest first) rendered under the value via `<Sparkline>`. */
  sparkline?: number[];
  /** Sparkline stroke color; defaults to the accent token. Mockup KPI cards intentionally vary this
   * per metric (brand-accent for raw counts, success-green for rate-style metrics) — a page-level
   * choice, so it's a prop rather than something this atom infers. */
  sparklineColor?: string;
  className?: string;
}

/** KPI tile: faint oversized icon top-left, trend badge top-right, title, big value (+ optional
 * unit), optional inline sparkline pinned to the bottom. Matches `overview-ccflare-v2.html`'s
 * `.card.c3` KPI cards. */
export function MetricCard({
  icon: Icon,
  title,
  value,
  unit,
  trend,
  sparkline,
  sparklineColor,
  className,
}: MetricCardProps) {
  const positive = trend ? (trend.positive ?? trend.direction === "up") : false;

  return (
    <Card className={className}>
      <div className="flex items-start justify-between">
        <Icon className="h-6 w-6 text-fg opacity-20" strokeWidth={1.75} />
        {trend && (
          <span
            className={clsx(
              "flex items-center gap-0.5 text-[11.5px] font-semibold",
              positive ? "text-success" : "text-error",
            )}
          >
            <ArrowDown className={clsx("h-3 w-3", trend.direction === "up" && "rotate-180")} />
            {trend.label}
          </span>
        )}
      </div>
      <div className="mt-2 text-[11.5px] text-fg opacity-60">{title}</div>
      <div className="text-2xl font-bold leading-tight tabular-nums text-fg">
        {value}
        {unit && <span className="ml-0.5 text-xs font-semibold opacity-60">{unit}</span>}
      </div>
      {sparkline && sparkline.length > 0 && (
        <Sparkline data={sparkline} color={sparklineColor} className="mt-auto pt-1.5" />
      )}
    </Card>
  );
}
