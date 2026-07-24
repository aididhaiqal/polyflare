export type QuotaDisplayMode = "remaining" | "used";

export const DEFAULT_QUOTA_DISPLAY_MODE: QuotaDisplayMode = "used";

export function clampQuotaPercent(value: number): number {
  return Math.max(0, Math.min(100, value));
}

export function quotaDisplayPercent(
  usedPercent: number,
  mode: QuotaDisplayMode,
): number {
  const used = clampQuotaPercent(usedPercent);
  return mode === "remaining" ? 100 - used : used;
}

export function quotaDisplayLabel(mode: QuotaDisplayMode): string {
  return mode === "remaining" ? "remaining" : "used";
}

export function quotaWindowIsPresent<T extends { stale?: boolean }>(
  window: T | null | undefined,
): window is T {
  return window !== null && window !== undefined && window.stale !== true;
}
