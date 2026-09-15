// Exposes /api/capabilities (fetched once authenticated) as context. Consumers — the Shell's nav
// (Task 4) and the /logs route — read `liveLogs` to hide the Live Logs nav item / show a disabled
// notice instead of rendering the stream when the server was started without that feature.
import { createContext, useContext, useMemo, type ReactNode } from "react";

import { useCapabilities } from "../lib/queries";

interface CapabilitiesContextValue {
  /** Mirrors CapabilitiesView.live_logs. False (not "loading") until the fetch resolves, so
   * gated UI stays hidden rather than flashing in before capabilities are known. */
  liveLogs: boolean;
  /** Which node serves this dashboard, or null until capabilities resolve — the badge stays
   * absent rather than claiming "main" before the answer is known. */
  nodeRole: "main" | "replica" | null;
  /** Operator-set friendly name for this node, when configured. */
  nodeLabel: string | null;
}

const defaultValue: CapabilitiesContextValue = {
  liveLogs: false,
  nodeRole: null,
  nodeLabel: null,
};

const CapabilitiesContext = createContext<CapabilitiesContextValue>(defaultValue);

export function CapabilitiesProvider({ children }: { children: ReactNode }) {
  const { data } = useCapabilities();
  const value = useMemo<CapabilitiesContextValue>(
    () => ({
      liveLogs: data?.live_logs ?? false,
      nodeRole: data?.node_role ?? null,
      nodeLabel: data?.node_label ?? null,
    }),
    [data?.live_logs, data?.node_role, data?.node_label],
  );
  return <CapabilitiesContext.Provider value={value}>{children}</CapabilitiesContext.Provider>;
}

export function useCapabilityFlags(): CapabilitiesContextValue {
  return useContext(CapabilitiesContext);
}
