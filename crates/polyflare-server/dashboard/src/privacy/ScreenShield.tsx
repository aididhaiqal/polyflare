import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";

const STORAGE_KEY = "polyflare-screen-shield";

interface ScreenShieldState {
  active: boolean;
  toggle: () => void;
}

const ScreenShieldContext = createContext<ScreenShieldState | null>(null);

function readStored(): boolean {
  try {
    return window.localStorage.getItem(STORAGE_KEY) === "1";
  } catch {
    return false;
  }
}

export function ScreenShieldProvider({ children }: { children: ReactNode }) {
  const [active, setActive] = useState(readStored);

  useEffect(() => {
    try {
      window.localStorage.setItem(STORAGE_KEY, active ? "1" : "0");
    } catch {
      // Storage can be unavailable in hardened/private contexts; the in-memory control still works.
    }
  }, [active]);

  const value = useMemo(
    () => ({ active, toggle: () => setActive((current) => !current) }),
    [active],
  );
  return <ScreenShieldContext.Provider value={value}>{children}</ScreenShieldContext.Provider>;
}

export function useScreenShield(): ScreenShieldState {
  const value = useContext(ScreenShieldContext);
  if (!value) throw new Error("useScreenShield must be used inside ScreenShieldProvider");
  return value;
}

/** Stable non-identifying label that preserves account differentiation during screen sharing. */
export function routePseudonym(seed: string): string {
  let hash = 0x811c9dc5;
  for (let index = 0; index < seed.length; index += 1) {
    hash ^= seed.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193);
  }
  const code = (hash >>> 0).toString(16).padStart(8, "0").toUpperCase();
  return `Route · ${code.slice(0, 4)}-${code.slice(4)}`;
}

export function ShieldedAccount({
  id,
  label,
  className,
}: {
  id: string;
  label: string;
  className?: string;
}) {
  const { active } = useScreenShield();
  return (
    <span className={className} data-screen-shielded={active ? "true" : undefined}>
      {active ? routePseudonym(id) : label}
    </span>
  );
}
