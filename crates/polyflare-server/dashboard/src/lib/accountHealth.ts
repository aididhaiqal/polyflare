export type AccountHealthLevel = "action" | "watch" | "ready";

export type AccountHealthNextAction =
  | "reauthenticate"
  | "wait_for_reset"
  | "resume_or_exclude"
  | "reduce_traffic"
  | "refresh_evidence"
  | "inspect_status"
  | "none";

export interface AccountHealthInput {
  id: string;
  status: string;
  token_health: { access_state: "missing" | "expired" | "valid" };
  weekly: { used_percent: number; stale: boolean } | null;
  five_hour: { used_percent: number; stale: boolean } | null;
  request_count_24h: number;
}

export interface AccountHealthReason {
  key: string;
  label: string;
  level: Exclude<AccountHealthLevel, "ready">;
  weight: number;
}

export interface AccountHealthAccount {
  id: string;
  level: AccountHealthLevel;
  reasons: AccountHealthReason[];
  nextAction: AccountHealthNextAction;
  nextActionLabel: string;
  quotaRisk: number;
  activity: number;
}

export interface AccountHealthModel {
  accounts: AccountHealthAccount[];
  summary: Record<AccountHealthLevel, number> & { observed: number };
}

const LEVEL_RANK: Record<AccountHealthLevel, number> = { action: 2, watch: 1, ready: 0 };

function reason(
  key: string,
  label: string,
  level: Exclude<AccountHealthLevel, "ready">,
  weight: number,
): AccountHealthReason {
  return { key, label, level, weight };
}

function classifyAccount(account: AccountHealthInput): AccountHealthAccount {
  const reasons: AccountHealthReason[] = [];
  const tokenState = account.token_health.access_state;

  if (tokenState === "expired") {
    reasons.push(reason("token_expired", "Token expired", "action", 100));
  } else if (tokenState === "missing") {
    reasons.push(reason("token_missing", "Token missing", "action", 98));
  }

  if (account.status === "reauth_required" || account.status === "reauth") {
    reasons.push(reason("reauth_required", "Reauthentication required", "action", 97));
  } else if (account.status === "deactivated") {
    reasons.push(reason("deactivated", "Route deactivated", "action", 96));
  } else if (account.status === "quota_exceeded") {
    reasons.push(reason("quota_exhausted", "Quota exhausted", "action", 94));
  } else if (account.status === "rate_limited") {
    reasons.push(reason("rate_limited", "Rate limited", "watch", 78));
  } else if (account.status === "paused") {
    reasons.push(reason("paused", "Route paused", "watch", 74));
  } else if (account.status === "cooldown") {
    reasons.push(reason("cooldown", "Route cooling down", "watch", 70));
  } else if (account.status !== "active") {
    reasons.push(reason("unknown_status", account.status.replace(/_/g, " "), "watch", 60));
  }

  const weeklyUsed = account.weekly?.used_percent;
  if (weeklyUsed !== undefined && weeklyUsed !== null) {
    if (weeklyUsed >= 100) {
      reasons.push(reason("weekly_exhausted", "Weekly quota exhausted", "action", 92));
    } else if (weeklyUsed >= 80) {
      reasons.push(reason("weekly_constrained", "Weekly quota constrained", "watch", 80 + weeklyUsed / 10));
    }
    if (account.weekly?.stale) {
      reasons.push(reason("weekly_stale", "Weekly evidence stale", "watch", 68));
    }
  }

  const fiveHourUsed = account.five_hour?.used_percent;
  if (fiveHourUsed !== undefined && fiveHourUsed !== null && !account.five_hour?.stale && fiveHourUsed >= 90) {
    reasons.push(reason("five_hour_constrained", "5-hour quota constrained", "watch", 76 + fiveHourUsed / 10));
  }

  reasons.sort((left, right) => right.weight - left.weight || left.key.localeCompare(right.key));
  const level: AccountHealthLevel = reasons.some((item) => item.level === "action")
    ? "action"
    : reasons.length > 0
      ? "watch"
      : "ready";

  let nextAction: AccountHealthNextAction = "none";
  let nextActionLabel = "No intervention needed";
  const keys = new Set(reasons.map((item) => item.key));
  if (keys.has("token_expired") || keys.has("token_missing") || keys.has("reauth_required")) {
    nextAction = "reauthenticate";
    nextActionLabel = "Reauthenticate this route";
  } else if (keys.has("quota_exhausted") || keys.has("weekly_exhausted") || keys.has("rate_limited") || keys.has("cooldown")) {
    nextAction = "wait_for_reset";
    nextActionLabel = "Keep traffic away until reset";
  } else if (keys.has("deactivated") || keys.has("paused")) {
    nextAction = "resume_or_exclude";
    nextActionLabel = "Resume it or keep it out of rotation";
  } else if (keys.has("weekly_constrained") || keys.has("five_hour_constrained")) {
    nextAction = "reduce_traffic";
    nextActionLabel = "Shift new traffic to cooler routes";
  } else if (keys.has("weekly_stale")) {
    nextAction = "refresh_evidence";
    nextActionLabel = "Refresh quota evidence before routing";
  } else if (keys.has("unknown_status")) {
    nextAction = "inspect_status";
    nextActionLabel = "Inspect the route status";
  }

  return {
    id: account.id,
    level,
    reasons: reasons.slice(0, 4),
    nextAction,
    nextActionLabel,
    quotaRisk: Math.max(weeklyUsed ?? -1, fiveHourUsed ?? -1),
    activity: account.request_count_24h,
  };
}

/** Turns raw route observations into a deterministic exception-first operator queue. */
export function buildAccountHealth(accounts: AccountHealthInput[]): AccountHealthModel {
  const classified = accounts.map(classifyAccount).sort((left, right) => {
    const levelDelta = LEVEL_RANK[right.level] - LEVEL_RANK[left.level];
    if (levelDelta !== 0) return levelDelta;
    const quotaDelta = right.quotaRisk - left.quotaRisk;
    if (quotaDelta !== 0) return quotaDelta;
    const activityDelta = right.activity - left.activity;
    if (activityDelta !== 0) return activityDelta;
    return left.id.localeCompare(right.id);
  });

  return {
    accounts: classified,
    summary: {
      action: classified.filter((account) => account.level === "action").length,
      watch: classified.filter((account) => account.level === "watch").length,
      ready: classified.filter((account) => account.level === "ready").length,
      observed: classified.length,
    },
  };
}
