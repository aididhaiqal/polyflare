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

/** Outcome/reliability percentage with enough precision to avoid contradicting non-zero counts.
 * Quota displays should keep using `pct`; this is for rates where 0.5% must not render as 0% and
 * 99.5% must not render as 100%. */
export function ratePct(n: number | null | undefined): string {
  if (n === null || n === undefined || !Number.isFinite(n)) return "—";
  const absolute = Math.abs(n);
  if (absolute > 0 && absolute < 0.1) return `${n < 0 ? "-" : ""}<0.1%`;
  if (n > 99 && n < 100) {
    const rounded = n.toFixed(1);
    return Number(rounded) >= 100 ? "<100%" : `${rounded}%`;
  }
  if (absolute > 0 && absolute < 10) {
    return `${n.toFixed(1)}%`;
  }
  return `${Math.round(n)}%`;
}
// @check ratePct(0.493) === "0.5%"
// @check ratePct(99.507) === "99.5%"
// @check ratePct(99.95) === "<100%"
// @check ratePct(99.99) === "<100%"
// @check ratePct(0) === "0%"

/** A percentage kept to one decimal ACROSS THE WHOLE RANGE, for a rate that sits in the middle of
 * it and moves only slightly.
 *
 * Neither formatter above fits such a metric. `pct` rounds to whole numbers; `ratePct` adds
 * precision only below 10% and between 99% and 100%, falling through to the same whole-number
 * rounding in between — it was written for error and reliability rates, which live at the
 * extremes. The cache-hit rate does not: it holds around 95-96% and varies by roughly two points
 * across a day. Rounded, it renders as one unchanging number and reads as a stuck gauge rather
 * than a live measurement, which is exactly how it was first reported as a bug. */
export function pctTenths(n: number | null | undefined): string {
  if (n === null || n === undefined || !Number.isFinite(n)) return "—";
  return `${n.toFixed(1)}%`;
}
// @check pctTenths(95.78) === "95.8%"
// @check pctTenths(96) === "96.0%"
// @check pctTenths(0) === "0.0%"

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

/** Human label for an account `plan_type`. Codex tiers arrive as the ChatGPT plan slug from the
 * ID-token claim and the usage endpoint: `prolite` / `pro` / `promax` are the three Pro tiers that
 * ChatGPT sells as Pro 100 / Pro 200 / Pro 500 (the names codex-rs shows since 2026-09-28), plus
 * `plus` / `team` / `free`. Anthropic tiers are `max_20x` / `max_5x` / `max` / `pro` (from the OAuth
 * profile's `rate_limit_tier`, mapped server-side in `anthropic_usage::plan_slug_from_tier`) — the
 * `pro` slug is shared, so a Claude Pro seat also reads "Pro 200" here; pass the provider to
 * disambiguate. An unrecognized value is title-cased so a new tier still reads sensibly. */
export function planLabel(plan: string | null | undefined, provider?: string | null): string {
  if (!plan) return "—";
  const slug = plan.trim().toLowerCase();
  if (provider === "anthropic") {
    const claude: Record<string, string> = { max_20x: "Max 20×", max_5x: "Max 5×", max: "Max", pro: "Pro" };
    if (slug in claude) return claude[slug];
  }
  const known: Record<string, string> = {
    prolite: "Pro 100",
    pro: "Pro 200",
    promax: "Pro 500",
    max_20x: "Max 20×",
    max_5x: "Max 5×",
    max: "Max",
    plus: "Plus",
    team: "Team",
    free: "Free",
    unknown: "Unknown",
  };
  return known[slug] ?? slug.replace(/(^|[_\s])([a-z])/g, (_, sep, c) => (sep ? " " : "") + c.toUpperCase());
}
// @check planLabel("max_20x") === "Max 20×"
// @check planLabel("promax") === "Pro 500"
// @check planLabel("pro", "anthropic") === "Pro"

/** Calendar date for a unix-seconds timestamp, e.g. `"29 Oct"`, with the year appended when it is
 * not the year of `nowMs` (`"8 Aug 2025"`). UTC-stable so tests are deterministic. */
export function shortDate(unixSecs: number, nowMs: number): string {
  const d = new Date(unixSecs * 1000);
  const day = d.getUTCDate();
  const month = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"][d.getUTCMonth()];
  const year = d.getUTCFullYear();
  return year === new Date(nowMs).getUTCFullYear() ? `${day} ${month}` : `${day} ${month} ${year}`;
}

/** The billing card on the account profile, from `AccountDetailView.subscription`. The claims come
 * from the seat's last FULL login and are never refreshed by the refresh grant, so the hint always
 * carries the as-of date; a period that has already ended is labelled stale rather than shown as
 * a fact, because the seat is plainly still serving. Returns `null` when there is nothing to show. */
export function billingLabel(
  sub: { active_until: number | null; last_checked: number | null } | null | undefined,
  nowMs: number,
): { value: string; hint: string; stale: boolean } | null {
  if (!sub || sub.active_until === null) return null;
  const asOf = sub.last_checked === null ? "" : ` · as of ${shortDate(sub.last_checked, nowMs)}`;
  const stale = sub.active_until * 1000 < nowMs;
  if (stale) {
    return {
      value: `Ended ${shortDate(sub.active_until, nowMs)}`,
      hint: `stale claim${asOf} · re-login refreshes`,
      stale,
    };
  }
  return { value: `Renews ${shortDate(sub.active_until, nowMs)}`, hint: `in ${countdown(sub.active_until, nowMs)}${asOf}`, stale };
}
// @check billingLabel({ active_until: 1_793_243_736, last_checked: 1_790_651_761 }, 1_790_800_000_000)?.value === "Renews 29 Oct"
// @check billingLabel(null, 1_790_800_000_000) === null

/** A plan-credit balance for the accounts list. The usage endpoint's `credits.balance` is in the
 * plan's own credit unit (a Pro 200 week starts at 62,500; an Ultrafast turn burns thousands),
 * NOT dollars — so no currency sign, whole credits with separators, decimals only below 100
 * (`credits(62500) === "62,500"`, `credits(12.5) === "12.5"`, `credits(0) === "0"`). */
export function credits(amount: number): string {
  if (!Number.isFinite(amount)) return "—";
  if (Math.abs(amount) >= 100) return Math.round(amount).toLocaleString("en-US");
  return Number(amount.toFixed(1)).toString();
}
// @check credits(62500) === "62,500"
// @check credits(12.5) === "12.5"
