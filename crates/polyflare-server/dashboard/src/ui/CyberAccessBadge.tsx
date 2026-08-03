import clsx from "clsx";

import { ShieldCheck } from "./icons";

/** Marks an account authorized for cyber/security work (`security_work_authorized`).
 *
 * Rendered wherever an account's identity appears, so the capability is visible at a glance
 * instead of only inside the row menu. Deliberately NOT hidden by the screen shield: this is a
 * routing capability, not identity data — it reveals nothing about who the account belongs to. */
export function CyberAccessBadge({ className }: { className?: string }) {
  return (
    <span
      title="Authorized for cyber/security work"
      aria-label="Authorized for cyber/security work"
      className={clsx("inline-flex shrink-0 items-center text-signal", className)}
    >
      <ShieldCheck className="h-3 w-3" strokeWidth={2.2} />
    </span>
  );
}
