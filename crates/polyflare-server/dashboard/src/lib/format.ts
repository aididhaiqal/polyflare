// Pure formatting helpers for the dashboard. No React, no fetch, no Date.now() as a hidden
// default anywhere it would break determinism — every "now" is an explicit parameter. Keep this
// file free of side effects so a later task can unit-test it directly (parseLogEvent in
// useLogStream.ts follows the same discipline).

/** Countdown from an absolute unix-epoch-seconds deadline to `nowMs` (unix epoch milliseconds).
 * Examples: `countdown(now+3660, now*1000) === "1h 1m"`; 4+ days shows `"4d 3h"`; a passed deadline
 * is `"due"`; a missing deadline (`null`/`undefined` — upstream isn't reporting this window) is
 * `"—"`. */
export function countdown(resetAtSecs: number | null | undefined, nowMs: number): string {
  if (resetAtSecs === null || resetAtSecs === undefined) return "—";
  const remainingSecs = resetAtSecs - Math.floor(nowMs / 1000);
  if (remainingSecs <= 0) return "due";

  const days = Math.floor(remainingSecs / 86400);
  const hours = Math.floor((remainingSecs % 86400) / 3600);
  const minutes = Math.floor((remainingSecs % 3600) / 60);

  if (days >= 1) return `${days}d ${hours}h`;
  if (hours >= 1) return `${hours}h ${minutes}m`;
  if (minutes >= 1) return `${minutes}m`;
  return "<1m";
}
// @check countdown(1_700_003_660, 1_700_000_000_000) === "1h 1m"
// @check countdown(1_700_356_400, 1_700_000_000_000) === "4d 3h"
// @check countdown(1_699_999_000, 1_700_000_000_000) === "due"
// @check countdown(null, 1_700_000_000_000) === "—"

/** Formats a backend `used_percent`/quota value (already on a 0-100 scale, see
 * `read_api.rs::WindowView`/`ProviderQuotaView` — never a 0-1 fraction) as a rounded percentage
 * string, e.g. `pct(63.7) === "64%"`. Non-finite input (missing/NaN) renders as `"—"`. */
export function pct(n: number | null | undefined): string {
  if (n === null || n === undefined || !Number.isFinite(n)) return "—";
  return `${Math.round(n)}%`;
}

/** Relative-time string for an absolute unix-epoch-seconds timestamp, e.g. `"3m ago"`. `nowMs`
 * defaults to `Date.now()` for call-site convenience but can be overridden for deterministic
 * testing. */
export function relTime(unixSecs: number, nowMs: number = Date.now()): string {
  const diffSecs = Math.floor(nowMs / 1000) - unixSecs;
  if (diffSecs < 5) return "just now";
  if (diffSecs < 60) return `${diffSecs}s ago`;
  const minutes = Math.floor(diffSecs / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}
// @check relTime(1_699_999_820, 1_700_000_000_000) === "3m ago"

/** Compact-notation formatter for large counts, e.g. `compactNum(12400) === "12.4k"`,
 * `compactNum(4_100_000) === "4.1M"`. Values under 1000 render exactly; one decimal place is
 * dropped when it would be a trailing `.0` (e.g. `2000` -> `"2k"`, not `"2.0k"`), and dropped
 * entirely (rounded to an integer) once the scaled value reaches 100+ (e.g. `123_000` ->
 * `"123k"`). */
export function compactNum(n: number): string {
  if (!Number.isFinite(n)) return "0";
  const sign = n < 0 ? "-" : "";
  const abs = Math.abs(n);
  if (abs < 1000) return `${sign}${Math.round(abs)}`;

  const units: Array<[number, string]> = [
    [1_000_000_000, "B"],
    [1_000_000, "M"],
    [1_000, "k"],
  ];
  for (const [threshold, suffix] of units) {
    if (abs >= threshold) {
      const scaled = abs / threshold;
      const formatted = scaled >= 100 ? `${Math.round(scaled)}` : trimTrailingZero(scaled);
      return `${sign}${formatted}${suffix}`;
    }
  }
  return `${sign}${Math.round(abs)}`;
}
// @check compactNum(12400) === "12.4k"
// @check compactNum(4_100_000) === "4.1M"

function trimTrailingZero(n: number): string {
  return n.toFixed(1).replace(/\.0$/, "");
}

/** Formats a duration in milliseconds as a human latency string: sub-second values as whole
 * milliseconds (`"420ms"`), one-second-and-up as seconds to one decimal place (`"1.9s"`). */
export function latency(ms: number | null | undefined): string {
  if (ms === null || ms === undefined || !Number.isFinite(ms)) return "—";
  if (ms < 1000) return `${Math.round(ms)}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}
// @check latency(420) === "420ms"
// @check latency(1900) === "1.9s"

/** Formats a tokens/sec rate (see `read_api.rs::RequestRowView.tps`, derived server-side) to one
 * decimal place with a unit suffix, e.g. `tpsFmt(42.37) === "42.4 tok/s"`. Missing/non-finite input
 * (the window wasn't derivable — see `read_api.rs::derive_tps`) renders as `"—"`. */
export function tpsFmt(n: number | null | undefined): string {
  if (n === null || n === undefined || !Number.isFinite(n)) return "—";
  return `${n.toFixed(1)} tok/s`;
}

/** Alias for `tpsFmt` — kept for compatibility with the `tps(n)` naming used in the task-2 SDD
 * brief; prefer `tpsFmt` at new call sites since it disambiguates from the `tps` data field. */
export const tps = tpsFmt;
