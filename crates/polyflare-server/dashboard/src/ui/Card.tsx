import type { ReactNode } from "react";
import clsx from "clsx";

/** Base surface for every panel/tile in the dashboard: card background, hairline border, the
 * theme's default radius, standard padding, and a column flex layout (so a sparkline/footer row
 * can be pinned to the bottom via `mt-auto`, per the mockups' `.card`). `min-w-0` keeps a card from
 * blowing out its grid column when it contains a table or long unbreakable text. */
export function Card({ children, className }: { children: ReactNode; className?: string }) {
  return (
    <div
      className={clsx(
        "flex min-w-0 flex-col overflow-hidden rounded-xl border border-border/80 bg-card/90 px-4 py-4 shadow-[0_12px_32px_hsl(var(--surface-shadow)/0.14)]",
        className,
      )}
    >
      {children}
    </div>
  );
}
