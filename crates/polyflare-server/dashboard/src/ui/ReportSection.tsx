// Reusable Reports-page section: a `Card` with a title, a caller-built KPI row, a caller-built
// recharts chart (rendered inside a fixed-height box), and a per-dimension breakdown `<table>`
// driven by a `columns` prop. Task 4 wires this up for the Cost section; Task 5 reuses it verbatim
// for the Usage and Performance sections — the `columns`/`dimensionLabel` props exist specifically
// so those sections don't need their own table markup.
//
// CONTENT-SAFETY: this component only ever renders what its caller passes in — `ReportBreakdownView`
// rows (counts/cost/tokens/timing, see lib/api.ts), never a body/prompt/response/key.
import type { ReactNode } from "react";
import clsx from "clsx";

import type { ReportBreakdownView } from "../lib/api";
import { Card } from "./Card";
import { Grid } from "./Grid";

/** One column of the breakdown table, beyond the always-present first `dimensionLabel`/`key`
 * column. `render` receives the full row so callers can format multiple fields (e.g. combining
 * `cost_usd` with a formatter) without this component knowing about specific metric fields. */
export interface ReportSectionColumn {
  header: string;
  render: (row: ReportBreakdownView) => ReactNode;
  align?: "left" | "right";
}

export interface ReportSectionProps {
  title: string;
  /** A row of `<MetricCard>`s wrapped in `<Col>`s (caller builds, span sum = 12) — rendered inside
   * this component's own `<Grid>`. */
  kpis: ReactNode;
  /** A recharts chart (caller builds, including its own `<ResponsiveContainer width="100%"
   * height="100%">`) — rendered inside this component's fixed-height `h-[180px]` box. */
  chart: ReactNode;
  /** The per-dimension rows (`ReportsView.breakdown`) driving the table body. */
  breakdown: ReportBreakdownView[];
  columns: ReportSectionColumn[];
  /** The first column's header, e.g. `"Model"` — describes what `row.key` is for this dimension. */
  dimensionLabel: string;
  /** Optional footnote rendered under the table, e.g. a sampling caveat. */
  note?: ReactNode;
}

const TABLE_HEAD_CLASS =
  "px-2 py-1.5 text-left text-[9px] font-medium uppercase tracking-wide text-fg opacity-60";

export function ReportSection({
  title,
  kpis,
  chart,
  breakdown,
  columns,
  dimensionLabel,
  note,
}: ReportSectionProps) {
  return (
    <Card className="gap-3">
      <div className="text-[13px] font-semibold uppercase tracking-wide text-fg opacity-70">
        {title}
      </div>

      <Grid>{kpis}</Grid>

      <div className="h-[180px]">{chart}</div>

      <div className="overflow-x-auto">
        <table className="w-full min-w-[480px] border-collapse text-[10.5px]">
          <thead>
            <tr className="border-b border-border">
              <th className={TABLE_HEAD_CLASS}>{dimensionLabel}</th>
              {columns.map((col) => (
                <th
                  key={col.header}
                  className={clsx(TABLE_HEAD_CLASS, col.align === "right" && "text-right")}
                >
                  {col.header}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {breakdown.length === 0 ? (
              <tr>
                <td
                  colSpan={columns.length + 1}
                  className="px-2 py-3 text-center text-[10.5px] text-fg opacity-50"
                >
                  No data for this window.
                </td>
              </tr>
            ) : (
              breakdown.map((row) => (
                <tr key={row.key} className="border-b border-border/55 last:border-0">
                  <td className="whitespace-nowrap px-2 py-1.5 tabular-nums text-fg opacity-90">
                    {row.key}
                  </td>
                  {columns.map((col) => (
                    <td
                      key={col.header}
                      className={clsx(
                        "whitespace-nowrap px-2 py-1.5 tabular-nums text-fg opacity-80",
                        col.align === "right" && "text-right",
                      )}
                    >
                      {col.render(row)}
                    </td>
                  ))}
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {note && <p className="text-[10px] text-fg opacity-50">{note}</p>}
    </Card>
  );
}
