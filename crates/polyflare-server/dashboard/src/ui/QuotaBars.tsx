import clsx from "clsx";

import { ProviderTag, providerBrandKey } from "./ProviderTag";

export interface QuotaWindowRow {
  /** Raw window key, e.g. `"five_hour" | "weekly" | "session"` (see `UsageWindowView.window` /
   * `ProviderQuotaView`'s fixed `five_hour`/`weekly` fields) — the display label is derived from
   * this via a small lookup, falling back to the raw key for any window PolyFlare adds later. */
  window: string;
  /** 0-100 scale, or `null` when this window isn't reported for this provider/account (e.g. Claude
   * has no five-hour window) — the row is omitted entirely rather than drawn as an empty/0% bar. */
  usedPercent: number | null;
  /** Right-aligned meta text, e.g. a countdown (`"2h11m"`) or reset day (`"Sun"`). Callers compute
   * this with `format.ts`'s `countdown`/`relTime` — QuotaBars has no notion of "now". */
  meta?: string;
}

export interface QuotaProviderGroup {
  provider: string;
  windows: QuotaWindowRow[];
}

const WINDOW_LABELS: Record<string, string> = {
  five_hour: "5-hour",
  weekly: "Weekly",
  session: "Session",
};

function labelForWindow(window: string): string {
  return WINDOW_LABELS[window] ?? window.replace(/_/g, " ");
}

const PROVIDER_BAR_CLASS: Record<string, string> = {
  codex: "bg-codex",
  claude: "bg-claude",
};

/** Grouped, per-provider quota bars — one row per reported window, adaptive to however many/which
 * windows a provider actually reports. Matches `overview-ccflare-v2.html`'s Quota card / the "A ·
 * Bars" variant chosen in `quota-style.html`. */
export function QuotaBars({
  groups,
  className,
}: {
  groups: QuotaProviderGroup[];
  className?: string;
}) {
  return (
    <div className={className}>
      {groups.map((group) => {
        const rows = group.windows.filter((w) => w.usedPercent !== null);
        return (
          <div key={group.provider} className="mt-2.5 first:mt-0">
            <ProviderTag provider={group.provider} />
            {rows.length === 0 ? (
              <div className="mt-1 text-[10px] text-fg opacity-40">no reported windows</div>
            ) : (
              rows.map((w) => (
                <div key={w.window} className="mt-1 flex items-center gap-2 text-[10px]">
                  <span className="w-11 shrink-0 text-fg opacity-60">{labelForWindow(w.window)}</span>
                  <div className="h-1.5 flex-1 overflow-hidden rounded-full bg-muted">
                    <div
                      className={clsx(
                        "h-full rounded-full",
                        PROVIDER_BAR_CLASS[providerBrandKey(group.provider)] ?? "bg-accent",
                      )}
                      style={{ width: `${Math.max(0, Math.min(100, w.usedPercent as number))}%` }}
                    />
                  </div>
                  {w.meta && <span className="w-10 shrink-0 text-right text-fg opacity-70">{w.meta}</span>}
                </div>
              ))
            )}
          </div>
        );
      })}
    </div>
  );
}
