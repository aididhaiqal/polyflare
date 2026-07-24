import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";

import {
  DEFAULT_QUOTA_DISPLAY_MODE,
  type QuotaDisplayMode,
} from "../lib/quotaDisplay";

const STORAGE_KEY = "polyflare-quota-display";

interface QuotaDisplayPreferenceState {
  mode: QuotaDisplayMode;
  setMode: (mode: QuotaDisplayMode) => void;
}

const QuotaDisplayPreferenceContext = createContext<QuotaDisplayPreferenceState | null>(null);

function readStoredMode(): QuotaDisplayMode {
  try {
    const stored = window.localStorage.getItem(STORAGE_KEY);
    return stored === "remaining" || stored === "used"
      ? stored
      : DEFAULT_QUOTA_DISPLAY_MODE;
  } catch {
    return DEFAULT_QUOTA_DISPLAY_MODE;
  }
}

export function QuotaDisplayPreferenceProvider({ children }: { children: ReactNode }) {
  const [mode, setMode] = useState<QuotaDisplayMode>(readStoredMode);

  useEffect(() => {
    try {
      window.localStorage.setItem(STORAGE_KEY, mode);
    } catch {
      // The preference still works for this session when browser storage is unavailable.
    }
  }, [mode]);

  const value = useMemo(() => ({ mode, setMode }), [mode]);
  return (
    <QuotaDisplayPreferenceContext.Provider value={value}>
      {children}
    </QuotaDisplayPreferenceContext.Provider>
  );
}

export function useQuotaDisplayPreference(): QuotaDisplayPreferenceState {
  const value = useContext(QuotaDisplayPreferenceContext);
  if (!value) {
    throw new Error(
      "useQuotaDisplayPreference must be used inside QuotaDisplayPreferenceProvider",
    );
  }
  return value;
}
