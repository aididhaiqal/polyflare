import clsx from "clsx";

/** Brand-colored provider chips (mockups' `.pv.codex`/`.pv.claude`). Any provider string beyond
 * these two (see `read_api.rs`'s `AccountView.provider` — a plain `String`, forward-compatible with
 * providers PolyFlare doesn't support yet) falls back to a neutral muted chip rather than defaulting
 * to one of the two brand colors it doesn't actually match. */
const PROVIDER_CLASSES: Record<string, string> = {
  codex: "bg-codex/15 text-codex",
  claude: "bg-claude/15 text-claude",
};

/** Canonical brand key for provider styling/labels. The backend's WIRE value for the Anthropic
 * backend is `"anthropic"` (see `polyflare_core::Provider`'s `Display`/`FromStr` — every
 * `provider` field in `read_api.rs`/`api.ts` carries this literal string), but the dashboard's
 * brand tokens/mockups use the consumer-facing name `"claude"` (`--claude` in index.css,
 * `bg-claude`/`text-claude` in tailwind.config.ts). This is the one place that mapping happens, so
 * every provider-branded surface (this chip, `QuotaBars`, page-level provider filters) renders the
 * Claude brand color/label instead of silently falling into the neutral "unknown provider" style —
 * import this rather than re-deriving the mapping at each call site. */
export function providerBrandKey(provider: string): string {
  return provider === "anthropic" ? "claude" : provider;
}

export function ProviderTag({ provider, className }: { provider: string; className?: string }) {
  const key = providerBrandKey(provider);
  return (
    <span
      className={clsx(
        "inline-block whitespace-nowrap rounded px-1.5 py-0.5 text-[9px] font-bold lowercase leading-none",
        PROVIDER_CLASSES[key] ?? "bg-muted text-fg opacity-70",
        className,
      )}
    >
      {key}
    </span>
  );
}
