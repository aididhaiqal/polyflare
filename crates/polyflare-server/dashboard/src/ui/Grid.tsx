import type { ReactNode } from "react";
import clsx from "clsx";

/** 12-column CSS grid matching the mockups' `.grid{grid-template-columns:repeat(12,1fr)}` layout.
 * Every row of `<Col>` children must sum to 12 — never leave a row visually stranded mid-grid (see
 * the brief's grid-discipline rule). */
export function Grid({ children, className }: { children: ReactNode; className?: string }) {
  return <div className={clsx("grid grid-cols-12 gap-4", className)}>{children}</div>;
}

// Static class map (not string interpolation) so Tailwind's content scanner can see every
// col-span-* class literally in this file.
const SPAN_CLASS = {
  3: "col-span-6 lg:col-span-3",
  4: "col-span-12 sm:col-span-6 lg:col-span-4",
  5: "col-span-12 lg:col-span-5",
  6: "col-span-12 md:col-span-6",
  7: "col-span-12 lg:col-span-7",
  8: "col-span-12 lg:col-span-8",
  12: "col-span-12",
} as const;

export type ColSpan = keyof typeof SPAN_CLASS;

/** One cell of a `<Grid>` row. `span=5` is not in the brief's literal `{3|4|6|8|12}` list but IS
 * required to reproduce the authoritative mockup's 5+3+4 KPI/quota/pace row
 * (overview-ccflare-v2.html's `.c5`) — see task-4-report.md for the fidelity note. `span=7` is the
 * same kind of addition for Task 7's account-detail page, whose master-detail mockup
 * (`accounts-master-detail-v2.html`) pairs a `.c5` quota/token card with a `.c7` trend chart —
 * see task-7-report.md. */
export function Col({
  span,
  children,
  className,
  fill = false,
}: {
  span: ColSpan;
  children: ReactNode;
  className?: string;
  /** Stretch the cell's direct child to the row height for deliberately paired card rows. */
  fill?: boolean;
}) {
  return (
    <div className={clsx(SPAN_CLASS[span], "min-w-0", fill && "[&>*]:h-full", className)}>
      {children}
    </div>
  );
}
