// Hand-built controlled modal — this app ships radix popover/select/switch/tabs but no
// radix-dialog dependency, so the overlay/centering/focus/Escape behavior a dialog needs is
// implemented directly here. Used for destructive confirmations (e.g. deleting an account) by the
// Accounts-list kebab (Task 7) and the AccountDetail action panel (Task 8) — no consumer lives in
// this task; it's exercised only by the build.
import { useRef, type ReactNode } from "react";
import clsx from "clsx";

import { AlertTriangle } from "./icons";
import { useDialogA11y } from "./useDialogA11y";

export interface ConfirmDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: string;
  description?: ReactNode;
  /** @default "Confirm" */
  confirmLabel?: string;
  /** @default "Cancel" */
  cancelLabel?: string;
  /** Red confirm button + a small warning glyph next to the title, for destructive actions. */
  danger?: boolean;
  /** Disables both buttons and reflects a pending mutation; caller flips this during the async call. */
  busy?: boolean;
  /** Caller runs the mutation. Does NOT auto-close — the caller decides when to close (typically
   * via `onOpenChange(false)` in the mutation's `onSuccess`), so it can keep the dialog open with
   * `busy` set while the request is in flight. */
  onConfirm: () => void;
  /** Extra body content rendered below the description (e.g. a delete_history checkbox). */
  children?: ReactNode;
}

/** Controlled confirmation modal. Renders nothing when `!open`. Closes on overlay click, Escape,
 * or Cancel — never on Confirm (that's left entirely to the caller). Never wraps native
 * `alert`/`confirm`/`prompt`. */
export function ConfirmDialog({
  open,
  onOpenChange,
  title,
  description,
  confirmLabel = "Confirm",
  cancelLabel = "Cancel",
  danger,
  busy,
  onConfirm,
  children,
}: ConfirmDialogProps) {
  const dialogRef = useRef<HTMLDivElement>(null);
  const cancelRef = useRef<HTMLButtonElement>(null);

  // Escape-to-close + Tab focus-trap (shared with the API-key show-once modal); initial focus on
  // Cancel — the safer default for a destructive dialog.
  useDialogA11y(open, () => onOpenChange(false), dialogRef, cancelRef);

  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/50"
      onClick={() => onOpenChange(false)}
    >
      <div
        ref={dialogRef}
        tabIndex={-1}
        role="dialog"
        aria-modal="true"
        aria-label={title}
        onClick={(e) => e.stopPropagation()}
        className="w-full max-w-sm rounded-lg border border-border bg-card p-4 text-fg shadow-xl outline-none"
      >
        <div className="flex items-center gap-2 text-sm font-semibold">
          {danger && <AlertTriangle className="h-4 w-4 shrink-0 text-error" />}
          <span>{title}</span>
        </div>
        {description && <div className="mt-1 text-[12px] text-fg opacity-70">{description}</div>}
        {children && <div className="mt-2">{children}</div>}
        <div className="mt-4 flex justify-end gap-2">
          <button
            ref={cancelRef}
            type="button"
            disabled={busy}
            onClick={() => onOpenChange(false)}
            className="rounded border border-border bg-muted px-3 py-1.5 text-[12px] text-fg disabled:cursor-not-allowed disabled:opacity-40"
          >
            {cancelLabel}
          </button>
          <button
            type="button"
            disabled={busy}
            onClick={onConfirm}
            className={clsx(
              "rounded px-3 py-1.5 text-[12px] text-white disabled:cursor-not-allowed disabled:opacity-40",
              danger ? "bg-error" : "bg-accent",
            )}
          >
            {confirmLabel}
          </button>
        </div>
      </div>
    </div>
  );
}
