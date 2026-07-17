import clsx from "clsx";

type StatusTone = "ok" | "warn" | "error";

/** Buckets every status string the backend can report (`read_api.rs`'s `AccountView.status` /
 * `AccountDetailView.status` — a plain `String`, not a closed Rust enum) into the three tones the
 * mockups use (`accounts-page.html`'s `.st.a`/`.st.c`/`.st.r`). An unrecognized/future status falls
 * into "warn" — visibly not-quite-right — rather than silently rendering as healthy. */
const TONE_BY_STATUS: Record<string, StatusTone> = {
  active: "ok",
  cooldown: "warn",
  rate_limited: "warn",
  quota_exceeded: "warn",
  paused: "warn",
  reauth_required: "error",
  reauth: "error",
  deactivated: "error",
};

/** Shortens a few verbose backend status strings to the mockups' labels (e.g. `reauth_required` ->
 * `reauth`); anything else falls back to the raw status with underscores turned to spaces. */
const LABEL_BY_STATUS: Record<string, string> = {
  reauth_required: "reauth",
  rate_limited: "rate limited",
  quota_exceeded: "quota exceeded",
};

const TONE_CLASSES: Record<StatusTone, string> = {
  ok: "bg-success/15 text-success",
  warn: "bg-warn/15 text-warn",
  error: "bg-error/15 text-error",
};

/** Small rounded status badge (mockups' `.st`). */
export function StatusPill({ status, className }: { status: string; className?: string }) {
  const tone = TONE_BY_STATUS[status] ?? "warn";
  const label = LABEL_BY_STATUS[status] ?? status.replace(/_/g, " ");
  return (
    <span
      className={clsx(
        "inline-block whitespace-nowrap rounded px-2 py-0.5 text-[10px] font-semibold capitalize leading-none",
        TONE_CLASSES[tone],
        className,
      )}
    >
      {label}
    </span>
  );
}
