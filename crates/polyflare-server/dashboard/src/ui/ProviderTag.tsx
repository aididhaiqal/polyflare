import clsx from "clsx";

/** Brand-colored provider chips (mockups' `.pv.codex`/`.pv.claude`). Any provider string beyond
 * these two (see `read_api.rs`'s `AccountView.provider` — a plain `String`, forward-compatible with
 * providers PolyFlare doesn't support yet) falls back to a neutral muted chip rather than defaulting
 * to one of the two brand colors it doesn't actually match. */
const PROVIDER_CLASSES: Record<string, string> = {
  codex: "bg-codex/15 text-codex",
  claude: "bg-claude/15 text-claude",
};

export function ProviderTag({ provider, className }: { provider: string; className?: string }) {
  return (
    <span
      className={clsx(
        "inline-block whitespace-nowrap rounded px-1.5 py-0.5 text-[9px] font-bold lowercase leading-none",
        PROVIDER_CLASSES[provider] ?? "bg-muted text-fg opacity-70",
        className,
      )}
    >
      {provider}
    </span>
  );
}
