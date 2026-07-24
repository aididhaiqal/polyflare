export type ServiceTierKind = "priority" | "flex" | "default" | "other";

export interface ServiceTierDisplay {
  kind: ServiceTierKind;
  label: string;
  recordedValue: string | null;
}

/** Normalize the recorded request tier without claiming an upstream tier PolyFlare did not observe. */
export function serviceTierDisplay(tier: string | null | undefined): ServiceTierDisplay {
  const recordedValue = tier?.trim() || null;
  const normalized = recordedValue?.toLowerCase() ?? null;

  if (normalized === "priority" || normalized === "fast") {
    return { kind: "priority", label: "Priority", recordedValue };
  }
  if (normalized === "flex") {
    return { kind: "flex", label: "Flex", recordedValue };
  }
  if (normalized === null || normalized === "auto" || normalized === "default") {
    return { kind: "default", label: "Default", recordedValue };
  }
  return { kind: "other", label: recordedValue ?? "Default", recordedValue };
}
