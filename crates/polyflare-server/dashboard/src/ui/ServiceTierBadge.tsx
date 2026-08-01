import clsx from "clsx";

import { serviceTierDisplay } from "../lib/serviceTier";

const TIER_CLASS = {
  priority: "border-accent/30 bg-accent/[0.12] text-accent",
  flex: "border-warn/30 bg-warn/[0.1] text-warn",
  default: "border-border bg-muted text-fg opacity-55",
  other: "border-border bg-muted text-fg opacity-70",
} as const;

export function ServiceTierBadge({
  tier,
  downgraded = false,
  className,
}: {
  tier: string | null | undefined;
  /** PolyFlare asked for priority and the upstream reported serving something else. */
  downgraded?: boolean;
  className?: string;
}) {
  const display = serviceTierDisplay(tier);
  if (downgraded) {
    return (
      <span
        title="Priority was requested but the upstream served this turn at a lower tier, so it is billed at the standard rate"
        className={clsx(
          "inline-flex shrink-0 items-center whitespace-nowrap rounded border px-1.5 py-0.5 text-[8.5px] font-bold leading-none",
          "border-warn/40 bg-warn/[0.12] text-warn",
          className,
        )}
      >
        PRIO DECLINED
      </span>
    );
  }
  return (
    <span
      title={
        display.recordedValue
          ? `Recorded service tier: ${display.recordedValue}`
          : "No explicit service tier recorded"
      }
      className={clsx(
        "inline-flex shrink-0 items-center whitespace-nowrap rounded border px-1.5 py-0.5 text-[8.5px] font-bold leading-none",
        TIER_CLASS[display.kind],
        className,
      )}
    >
      {display.label}
    </span>
  );
}
