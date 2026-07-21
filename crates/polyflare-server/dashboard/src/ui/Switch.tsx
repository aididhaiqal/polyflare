import * as SwitchPrimitive from "@radix-ui/react-switch";
import clsx from "clsx";

/** Small pill toggle wrapping Radix's `Switch` primitive — real `role="switch"` semantics,
 * keyboard support, `aria-checked`, and (via `disabled`) a properly non-interactive read-only
 * state, rather than a plain styled `<button>`. Styled to match the bespoke toggle
 * `AccountDetail.tsx`'s "Trusted access" row already established (`h-[18px] w-[34px]` track,
 * `success`-tinted thumb when on) so both toggles read as the same control. Used by the Settings
 * page for every `kind: "bool"` field — live (editable) and restart-only/fixed (via `disabled`). */
export function Switch({
  checked,
  onCheckedChange,
  disabled,
  ariaLabel,
}: {
  checked: boolean;
  onCheckedChange: (checked: boolean) => void;
  disabled?: boolean;
  ariaLabel?: string;
}) {
  return (
    <SwitchPrimitive.Root
      checked={checked}
      onCheckedChange={onCheckedChange}
      disabled={disabled}
      aria-label={ariaLabel}
      className={clsx(
        "relative h-[18px] w-[34px] shrink-0 rounded-full outline-none transition-colors",
        checked ? "bg-success/35" : "bg-muted",
        disabled ? "cursor-not-allowed opacity-50" : "cursor-pointer",
      )}
    >
      <SwitchPrimitive.Thumb
        className={clsx(
          "block h-[14px] w-[14px] rounded-full transition-transform",
          checked ? "translate-x-[18px] bg-success" : "translate-x-[2px] bg-fg opacity-50",
        )}
      />
    </SwitchPrimitive.Root>
  );
}
