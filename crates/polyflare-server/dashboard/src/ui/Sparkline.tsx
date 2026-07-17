import { Area, AreaChart, Line, LineChart, ResponsiveContainer } from "recharts";

export interface SparklineProps {
  /** Plain y-values, oldest first. Callers extract these from whatever series shape their query
   * returns (e.g. `TrendsView.primary.map((p) => p.v)`) — this atom has no notion of `{t, v}`
   * points or timestamps, only the values to draw. */
  data: number[];
  /** Stroke (and, when `area`, gradient fill) color. Any valid CSS color string — defaults to the
   * accent token via its CSS variable so it tracks the current theme without a re-render. */
  color?: string;
  /** Pixel height of the chart; width always fills the parent. */
  height?: number;
  /** Renders a soft gradient fill under the line (for the bigger trend charts elsewhere) instead of
   * a bare line (the default, used inline in `MetricCard`). */
  area?: boolean;
  className?: string;
}

/** Minimal inline trend chart: no axes, no grid, no tooltip, no legend — matches the mockups'
 * `.spark` KPI sparklines (and, with `area`, the bigger gradient-filled trend charts). */
export function Sparkline({
  data,
  color = "hsl(var(--accent))",
  height = 28,
  area = false,
  className,
}: SparklineProps) {
  if (data.length === 0) return <div className={className} style={{ height }} />;

  const points = data.map((v, i) => ({ i, v }));
  const gradientId = `sparkline-gradient-${color.replace(/[^a-zA-Z0-9]/g, "")}`;

  return (
    <div className={className} style={{ height }}>
      <ResponsiveContainer width="100%" height="100%">
        {area ? (
          <AreaChart data={points} margin={{ top: 2, right: 0, bottom: 0, left: 0 }}>
            <defs>
              <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor={color} stopOpacity={0.35} />
                <stop offset="100%" stopColor={color} stopOpacity={0} />
              </linearGradient>
            </defs>
            <Area
              type="monotone"
              dataKey="v"
              stroke={color}
              strokeWidth={1.6}
              fill={`url(#${gradientId})`}
              isAnimationActive={false}
              dot={false}
            />
          </AreaChart>
        ) : (
          <LineChart data={points} margin={{ top: 2, right: 0, bottom: 0, left: 0 }}>
            <Line
              type="monotone"
              dataKey="v"
              stroke={color}
              strokeWidth={1.6}
              dot={false}
              isAnimationActive={false}
            />
          </LineChart>
        )}
      </ResponsiveContainer>
    </div>
  );
}
