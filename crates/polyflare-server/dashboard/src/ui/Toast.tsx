// In-house toast provider — dependency-free (React context + a self-rendered fixed viewport) so
// mutation hooks (queries.ts's usePatchAccount/useDeleteAccount) can surface success/error
// feedback without pulling in an external toast library. See task-5-brief.md Step 2.
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";

import clsx from "clsx";

import { Check, CircleAlert, X } from "./icons";

/** "success" (default) or "error" — the only two tones the mutation hooks ever raise. */
export type ToastVariant = "success" | "error";

export interface ToastOptions {
  title: string;
  description?: string;
  variant?: ToastVariant;
}

interface ToastItem {
  id: number;
  title: string;
  description?: string;
  variant: ToastVariant;
}

interface ToastContextValue {
  toast: (opts: ToastOptions) => void;
}

const ToastContext = createContext<ToastContextValue | null>(null);

/** How long a toast stays visible before auto-dismissing. */
const AUTO_DISMISS_MS = 4000;

const VARIANT_ICON = { success: Check, error: CircleAlert } as const;
const VARIANT_ACCENT: Record<ToastVariant, string> = {
  success: "text-success",
  error: "text-error",
};

let nextId = 0;

/** Holds the active toast list and renders its own fixed bottom-right viewport, so mounting
 * `<ToastProvider>` once in App.tsx is the only wiring any page needs. */
export function ToastProvider({ children }: { children: ReactNode }) {
  const [toasts, setToasts] = useState<ToastItem[]>([]);
  const timers = useRef(new Map<number, ReturnType<typeof setTimeout>>());

  const dismiss = useCallback((id: number) => {
    setToasts((prev) => prev.filter((t) => t.id !== id));
    const timer = timers.current.get(id);
    if (timer !== undefined) {
      clearTimeout(timer);
      timers.current.delete(id);
    }
  }, []);

  const toast = useCallback(
    (opts: ToastOptions) => {
      const id = nextId++;
      setToasts((prev) => [
        ...prev,
        { id, title: opts.title, description: opts.description, variant: opts.variant ?? "success" },
      ]);
      timers.current.set(
        id,
        setTimeout(() => dismiss(id), AUTO_DISMISS_MS),
      );
    },
    [dismiss],
  );

  // Clear any pending auto-dismiss timers on unmount so none fire (and setState) after teardown.
  useEffect(() => {
    const timersMap = timers.current;
    return () => {
      for (const timer of timersMap.values()) clearTimeout(timer);
      timersMap.clear();
    };
  }, []);

  const value = useMemo<ToastContextValue>(() => ({ toast }), [toast]);

  return (
    <ToastContext.Provider value={value}>
      {children}
      <div className="fixed bottom-4 right-4 z-50 flex flex-col gap-2">
        {toasts.map((t) => {
          const Icon = VARIANT_ICON[t.variant];
          return (
            <div
              key={t.id}
              role="status"
              className="flex items-start gap-2 rounded-md border border-border bg-card px-4 py-3 text-sm text-fg shadow"
            >
              <Icon className={clsx("mt-0.5 h-4 w-4 shrink-0", VARIANT_ACCENT[t.variant])} />
              <div className="min-w-0 flex-1">
                <div className="font-medium">{t.title}</div>
                {t.description && <div className="mt-0.5 text-xs text-fg opacity-60">{t.description}</div>}
              </div>
              <button
                type="button"
                onClick={() => dismiss(t.id)}
                aria-label="Dismiss"
                className="shrink-0 text-fg opacity-50 hover:opacity-100"
              >
                <X className="h-3.5 w-3.5" />
              </button>
            </div>
          );
        })}
      </div>
    </ToastContext.Provider>
  );
}

/** Reads the toast context. Throws if called outside `<ToastProvider>` — a standard context guard
 * so a missing provider fails loudly at the call site instead of silently no-op-ing. */
export function useToast(): ToastContextValue {
  const ctx = useContext(ToastContext);
  if (!ctx) throw new Error("useToast must be used within a ToastProvider");
  return ctx;
}
