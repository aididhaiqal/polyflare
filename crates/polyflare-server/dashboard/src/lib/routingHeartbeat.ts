export type RoutingHeartbeatState = "live" | "quiet" | "idle" | "historical" | "unobserved";

export interface RoutingObservation {
  id: string | number;
  requestedAt: number;
  accountId: string | null;
  provider: string;
  model: string | null;
  transport: string | null;
  outcomeLabel: string;
  failure: boolean;
  durationMs: number;
  ttftMs: number | null;
  tps: number | null;
  serviceTier: string | null;
}

export interface RoutingHeartbeat {
  state: RoutingHeartbeatState;
  label: string;
  guidance: string;
  latest: RoutingObservation | null;
  ageSeconds: number | null;
  windowCount: number;
  windowFailures: number;
  windowAccounts: number;
}

const LIVE_SECONDS = 2 * 60;
const QUIET_SECONDS = 15 * 60;
const IDLE_SECONDS = 6 * 60 * 60;
const WINDOW_SECONDS = 5 * 60;

export function classifyRoutingAge(ageSeconds: number | null): RoutingHeartbeatState {
  if (ageSeconds === null) return "unobserved";
  if (ageSeconds <= LIVE_SECONDS) return "live";
  if (ageSeconds <= QUIET_SECONDS) return "quiet";
  if (ageSeconds <= IDLE_SECONDS) return "idle";
  return "historical";
}

/**
 * Classifies the age of the newest routing observation independently from query-fetch freshness.
 * The dashboard can therefore be freshly fetched while honestly describing its traffic evidence
 * as quiet, idle, or historical.
 */
export function buildRoutingHeartbeat(
  observations: RoutingObservation[],
  nowSeconds: number,
): RoutingHeartbeat {
  const latest = observations.reduce<RoutingObservation | null>(
    (current, observation) =>
      current === null || observation.requestedAt > current.requestedAt ? observation : current,
    null,
  );

  if (latest === null) {
    return {
      state: "unobserved",
      label: "No traffic observed",
      guidance: "Dashboard checked now; routing evidence will appear after the first request.",
      latest: null,
      ageSeconds: null,
      windowCount: 0,
      windowFailures: 0,
      windowAccounts: 0,
    };
  }

  const ageSeconds = Math.max(0, nowSeconds - latest.requestedAt);
  const windowStart = nowSeconds - WINDOW_SECONDS;
  const inWindow = observations.filter((observation) => observation.requestedAt >= windowStart);
  const windowAccounts = new Set(
    inWindow.flatMap((observation) => (observation.accountId ? [observation.accountId] : [])),
  ).size;

  const state = classifyRoutingAge(ageSeconds);
  let label: string;
  let guidance: string;
  if (state === "live") {
    label = "Traffic live";
    guidance = `${inWindow.length} ${inWindow.length === 1 ? "route" : "routes"} observed in the last 5 minutes.`;
  } else if (state === "quiet") {
    label = "Traffic quiet";
    guidance = "Dashboard checked now; no route has completed in the last 2 minutes.";
  } else if (state === "idle") {
    label = "Routing idle";
    guidance = "Dashboard checked now; no route has completed in the last 15 minutes.";
  } else {
    label = "Historical evidence";
    guidance = "Dashboard checked now; newest routing evidence is historical.";
  }

  return {
    state,
    label,
    guidance,
    latest,
    ageSeconds,
    windowCount: inWindow.length,
    windowFailures: inWindow.filter((observation) => observation.failure).length,
    windowAccounts,
  };
}
