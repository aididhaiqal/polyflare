// Exposes /api/capabilities (fetched once authenticated) as context. Consumers — the Shell's nav
// (Task 4) and the /logs route — read `liveLogs` to hide the Live Logs nav item / show a disabled
// notice instead of rendering the stream when the server was started without that feature.
import { createContext, useContext, useMemo, type ReactNode } from "react";

import { useCapabilities } from "../lib/queries";

interface CapabilitiesContextValue {
  /** Mirrors CapabilitiesView.live_logs. False (not "loading") until the fetch resolves, so
   * gated UI stays hidden rather than flashing in before capabilities are known. */
  liveLogs: boolean;
}

const defaultValue: CapabilitiesContextValue = { liveLogs: false };

const CapabilitiesContext = createContext<CapabilitiesContextValue>(defaultValue);

export function CapabilitiesProvider({ children }: { children: ReactNode }) {
  const { data } = useCapabilities();
  const value = useMemo<CapabilitiesContextValue>(
    () => ({ liveLogs: data?.live_logs ?? false }),
    [data?.live_logs],
  );
  return <CapabilitiesContext.Provider value={value}>{children}</CapabilitiesContext.Provider>;
}

export function useCapabilityFlags(): CapabilitiesContextValue {
  return useContext(CapabilitiesContext);
}
