export interface FleetBalanceRoute {
  id: string;
  status: string;
  token_health: { access_state: "missing" | "expired" | "valid" };
  weekly: {
    used_percent: number;
    reset_at: number | null;
    stale: boolean;
  } | null;
}

export interface FleetBalanceEndpoint {
  id: string;
  usedPercent: number;
  resetAt: number | null;
}

export interface FleetBalance {
  trackedCount: number;
  eligibleCount: number;
  staleCount: number;
  constrainedCount: number;
  medianUsedPercent: number | null;
  spreadPoints: number | null;
  coolest: FleetBalanceEndpoint | null;
  hottest: FleetBalanceEndpoint | null;
  tone: "balanced" | "uneven" | "constrained" | "unavailable";
  action: "steady" | "rebalance" | "protect" | "hold" | "restore";
}

const clampPercent = (value: number) => Math.max(0, Math.min(100, value));

/**
 * Builds a current weekly-load snapshot for the period before the history-backed pace forecast is
 * available. Only fresh routes that could accept work now participate in balance metrics; paused,
 * token-invalid, and stale routes remain visible in the evidence counts without distorting the
 * recommendation.
 */
export function buildFleetBalance(routes: FleetBalanceRoute[]): FleetBalance {
  const tracked = routes.filter((route) => route.weekly !== null);
  const staleCount = tracked.filter((route) => route.weekly?.stale).length;
  const eligible = tracked
    .filter(
      (route) =>
        route.status === "active" &&
        route.token_health.access_state === "valid" &&
        route.weekly?.stale === false,
    )
    .map((route) => ({
      id: route.id,
      usedPercent: clampPercent(route.weekly?.used_percent ?? 0),
      resetAt: route.weekly?.reset_at ?? null,
    }))
    .sort((left, right) => left.usedPercent - right.usedPercent || left.id.localeCompare(right.id));

  if (eligible.length === 0) {
    return {
      trackedCount: tracked.length,
      eligibleCount: 0,
      staleCount,
      constrainedCount: 0,
      medianUsedPercent: null,
      spreadPoints: null,
      coolest: null,
      hottest: null,
      tone: "unavailable",
      action: "restore",
    };
  }

  const middle = Math.floor(eligible.length / 2);
  const medianUsedPercent =
    eligible.length % 2 === 1
      ? eligible[middle].usedPercent
      : (eligible[middle - 1].usedPercent + eligible[middle].usedPercent) / 2;
  const coolest = eligible[0];
  const hottest = eligible[eligible.length - 1];
  const spreadPoints = hottest.usedPercent - coolest.usedPercent;
  const constrainedCount = eligible.filter((route) => route.usedPercent >= 80).length;
  const tone = constrainedCount > 0 ? "constrained" : spreadPoints >= 25 ? "uneven" : "balanced";
  const action =
    constrainedCount > 0
      ? eligible.length === 1
        ? "hold"
        : "protect"
      : spreadPoints >= 25
        ? "rebalance"
        : "steady";

  return {
    trackedCount: tracked.length,
    eligibleCount: eligible.length,
    staleCount,
    constrainedCount,
    medianUsedPercent,
    spreadPoints,
    coolest,
    hottest,
    tone,
    action,
  };
}

export function fleetBalanceFallbackState(
  hasPace: boolean,
  accountsLoading: boolean,
  accountsError: boolean,
): "pace" | "loading" | "error" | "snapshot" {
  if (hasPace) return "pace";
  if (accountsLoading) return "loading";
  if (accountsError) return "error";
  return "snapshot";
}
