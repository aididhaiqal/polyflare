import type { ReactNode } from "react";
import clsx from "clsx";

/** 12-column CSS grid matching the mockups' `.grid{grid-template-columns:repeat(12,1fr)}` layout.
 * Every row of `<Col>` children must sum to 12 — never leave a row visually stranded mid-grid (see
 * the brief's grid-discipline rule). */
export function Grid({ children, className }: { children: ReactNode; className?: string }) {
  return <div className={clsx("grid grid-cols-12 gap-3", className)}>{children}</div>;
}

// Static class map (not string interpolation) so Tailwind's content scanner can see every
// col-span-* class literally in this file.
const SPAN_CLASS = {
  3: "col-span-3",
  4: "col-span-4",
  5: "col-span-5",
  6: "col-span-6",
  8: "col-span-8",
  12: "col-span-12",
} as const;

export type ColSpan = keyof typeof SPAN_CLASS;

/** One cell of a `<Grid>` row. `span=5` is not in the brief's literal `{3|4|6|8|12}` list but IS
 * required to reproduce the authoritative mockup's 5+3+4 KPI/quota/pace row
 * (overview-ccflare-v2.html's `.c5`) — see task-4-report.md for the fidelity note. */
export function Col({
  span,
  children,
  className,
}: {
  span: ColSpan;
  children: ReactNode;
  className?: string;
}) {
  return <div className={clsx(SPAN_CLASS[span], "min-w-0", className)}>{children}</div>;
}
