import { useEffect, useRef, type RefObject } from "react";

// Shared modal-dialog keyboard/focus behavior for the app's hand-rolled dialogs (ConfirmDialog,
// the API-key show-once modal) — the app ships no radix-dialog, so this centralizes what a modal
// needs: on open, focus a sensible initial control; while open, Escape closes and Tab / Shift+Tab
// is TRAPPED inside the dialog (focus can't wander to the page behind the overlay). `onClose` is
// held in a ref and read at event time, so the effect subscribes once per open (deps `[active]`)
// instead of re-subscribing on every parent render — which is why callers no longer need an
// inline-callback eslint-disable.
//
// A focus trap adds no *secret* protection for the show-once key (the key is fully on screen while
// the modal is open by design); it is a keyboard-a11y correctness fix so the overlay behaves like a
// real modal.
const FOCUSABLE =
  'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';

export function useDialogA11y(
  active: boolean,
  onClose: () => void,
  /** The dialog container to trap focus within (attach to the `role="dialog"` element). */
  dialogRef: RefObject<HTMLElement | null>,
  /** Control to focus on open; falls back to the first focusable, then the dialog itself. */
  initialFocusRef?: RefObject<HTMLElement | null>,
) {
  const onCloseRef = useRef(onClose);
  onCloseRef.current = onClose;

  useEffect(() => {
    if (!active) return;
    const opener = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    const dialog = dialogRef.current;
    const focusables = () =>
      dialog ? Array.from(dialog.querySelectorAll<HTMLElement>(FOCUSABLE)) : [];

    (initialFocusRef?.current ?? focusables()[0] ?? dialog)?.focus();

    function onKeyDown(e: KeyboardEvent) {
      if (e.key === "Escape") {
        onCloseRef.current();
        return;
      }
      if (e.key !== "Tab" || !dialog) return;
      const items = focusables();
      if (items.length === 0) {
        // Nothing focusable inside — keep focus on the dialog rather than let Tab escape.
        e.preventDefault();
        dialog.focus();
        return;
      }
      const first = items[0];
      const last = items[items.length - 1];
      const current = document.activeElement;
      const outside = !dialog.contains(current);
      if (e.shiftKey && (current === first || outside)) {
        e.preventDefault();
        last.focus();
      } else if (!e.shiftKey && (current === last || outside)) {
        e.preventDefault();
        first.focus();
      }
    }

    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("keydown", onKeyDown);
      if (opener?.isConnected) opener.focus();
    };
    // onClose is read via onCloseRef; dialogRef/initialFocusRef are stable ref objects.
  }, [active, dialogRef, initialFocusRef]);
}
