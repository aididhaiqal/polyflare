// Pure helpers for the Claude usage page: the text Claude Code's `/usage` prints for one window,
// derived from `ClaudeUsageLine`. No React, no Date.now() — every "now" is a parameter.
import type { ClaudeUsageLine } from "./api";
import { countdown } from "./format.ts";

export type UsageTone = "ok" | "warn" | "error";

/** Tone for a window: exhausted or `limit_reached` is an error, 80%+ or `warning` is a warning. */
export function usageTone(line: ClaudeUsageLine): UsageTone {
  if (line.percent >= 100 || line.severity === "limit_reached") return "error";
  if (line.percent >= 80 || line.severity === "warning") return "warn";
  return "ok";
}

/** The label Claude Code uses: "Current session", "Current week (all models)", "Current week (Fable)". */
export function usageLabel(line: ClaudeUsageLine): string {
  if (line.kind === "session") return "Current session";
  if (line.kind === "weekly_all") return "Current week (all models)";
  if (line.kind === "weekly_scoped") return `Current week (${line.model ?? "model"})`;
  return line.model ? `${line.kind} (${line.model})` : line.kind;
}

/** "43% used · resets in 2h 10m" — or "no reset time reported" when the upstream sent none. */
export function usageSummary(line: ClaudeUsageLine, nowMs: number): string {
  const used = `${Math.round(line.percent)}% used`;
  if (line.resets_at === null) return `${used} · no reset time reported`;
  const left = countdown(line.resets_at, nowMs);
  return left === "due" ? `${used} · reset due` : `${used} · resets in ${left}`;
}
