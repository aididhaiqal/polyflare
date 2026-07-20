// Reusable kebab (⋯) popover menu, built on radix's `@radix-ui/react-popover` — this app
// deliberately ships popover/select/switch/tabs but NOT a dropdown-menu primitive, so the menu
// behavior (click-outside/Escape dismiss, positioning, portal) comes from Popover while the
// menu-item semantics (close-on-select, danger styling, checked group) are hand-rolled here.
// Consumed by the Accounts-list row kebab (Task 7) and the AccountDetail action panel (Task 8) —
// no consumer lives in this task; it's exercised only by the build.
import { createContext, useContext, useState, type ReactNode } from "react";
import * as Popover from "@radix-ui/react-popover";
import clsx from "clsx";

import { Check, MoreVertical, type LucideIcon } from "./icons";

interface ActionMenuContextValue {
  /** Closes the menu. Radix dropdown-menu auto-closes on item click; a plain Popover doesn't, so
   * `ActionMenu` owns `open` state and threads `close()` through context for its Item/CheckItem
   * subcomponents to call right after firing `onSelect`. */
  close: () => void;
}

const ActionMenuContext = createContext<ActionMenuContextValue | null>(null);

function useActionMenuContext(): ActionMenuContextValue {
  const ctx = useContext(ActionMenuContext);
  if (!ctx) {
    throw new Error("ActionMenu.Item/.CheckItem/.Label/.Separator must be rendered inside <ActionMenu>");
  }
  return ctx;
}

export interface ActionMenuProps {
  /** Accessible label for the trigger button, e.g. `Actions for ${email}`. */
  label: string;
  /** Menu body — compose from ActionMenu.Item / .CheckItem / .Label / .Separator. */
  children: ReactNode;
  /** Which side the popover content aligns to relative to the trigger. Defaults to "end". */
  align?: "start" | "end";
}

/** Kebab popover menu. Always self-managed/uncontrolled (open state lives inside the component,
 * exposed to children only via `close()`) — there is no external open/onOpenChange prop. */
export function ActionMenu({ label, children, align }: ActionMenuProps) {
  const [open, setOpen] = useState(false);
  const close = () => setOpen(false);

  return (
    <Popover.Root open={open} onOpenChange={setOpen}>
      <Popover.Trigger asChild>
        <button
          type="button"
          aria-label={label}
          onClick={(e) => e.stopPropagation()}
          // Also stop keydown bubbling so Enter/Space on a focused kebab inside a keyboard-
          // navigable row (the table view's `<tr role="button" onKeyDown>`) opens the menu WITHOUT
          // also triggering the row's navigate-to-detail. Radix's own toggle handler is on this same
          // button, so stopping propagation to ancestors doesn't suppress opening the popover.
          onKeyDown={(e) => e.stopPropagation()}
          className="flex h-6 w-6 shrink-0 items-center justify-center rounded border border-border bg-card text-fg hover:border-accent focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent"
        >
          <MoreVertical className="h-4 w-4" />
        </button>
      </Popover.Trigger>
      <Popover.Portal>
        <Popover.Content
          align={align ?? "end"}
          sideOffset={4}
          onClick={(e) => e.stopPropagation()}
          className="z-50 min-w-[180px] rounded-md border border-border bg-card p-1 text-[12px] text-fg shadow-lg"
        >
          <ActionMenuContext.Provider value={{ close }}>{children}</ActionMenuContext.Provider>
        </Popover.Content>
      </Popover.Portal>
    </Popover.Root>
  );
}

export interface ActionMenuItemProps {
  /** Optional leading glyph, e.g. `Pencil` or `Trash2` from `ui/icons`. */
  icon?: LucideIcon;
  children: ReactNode;
  onSelect: () => void;
  /** Red text + red-tinted hover, for destructive actions (e.g. Delete). */
  danger?: boolean;
  disabled?: boolean;
}

/** A single actionable row. Fires `onSelect`, then closes the menu via context. */
function ActionMenuItem({ icon: Icon, children, onSelect, danger, disabled }: ActionMenuItemProps) {
  const { close } = useActionMenuContext();
  return (
    <button
      type="button"
      disabled={disabled}
      onClick={() => {
        onSelect();
        close();
      }}
      className={clsx(
        "flex w-full items-center gap-2 rounded px-2 py-1.5 text-left hover:bg-muted disabled:cursor-not-allowed disabled:opacity-40",
        danger && "text-error hover:bg-error/10",
      )}
    >
      {Icon && <Icon className="h-3.5 w-3.5 shrink-0" />}
      <span className="min-w-0 flex-1">{children}</span>
    </button>
  );
}

export interface ActionMenuCheckItemProps {
  checked: boolean;
  children: ReactNode;
  onSelect: () => void;
}

/** Like `.Item`, but shows a checkmark when `checked` and reserves the left gutter when it isn't,
 * so a group of check items (e.g. the routing-policy choices) keeps its labels aligned. */
function ActionMenuCheckItem({ checked, children, onSelect }: ActionMenuCheckItemProps) {
  const { close } = useActionMenuContext();
  return (
    <button
      type="button"
      onClick={() => {
        onSelect();
        close();
      }}
      className="flex w-full items-center gap-2 rounded px-2 py-1.5 text-left hover:bg-muted"
    >
      <span className="flex h-3.5 w-3.5 shrink-0 items-center justify-center">
        {checked && <Check className="h-3.5 w-3.5" />}
      </span>
      <span className="min-w-0 flex-1">{children}</span>
    </button>
  );
}

/** Small uppercase section heading inside the menu (e.g. above a routing-policy check group). */
function ActionMenuLabel({ children }: { children: ReactNode }) {
  return (
    <div className="px-2 py-1 text-[10px] uppercase tracking-wide text-fg opacity-50">{children}</div>
  );
}

/** Hairline divider between menu groups. */
function ActionMenuSeparator() {
  return <div className="my-1 h-px bg-border" />;
}

ActionMenu.Item = ActionMenuItem;
ActionMenu.CheckItem = ActionMenuCheckItem;
ActionMenu.Label = ActionMenuLabel;
ActionMenu.Separator = ActionMenuSeparator;
