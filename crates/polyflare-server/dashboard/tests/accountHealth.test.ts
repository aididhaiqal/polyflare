import assert from "node:assert/strict";
import test from "node:test";

import { buildAccountHealth } from "../src/lib/accountHealth.ts";

const route = (
  id: string,
  options: {
    status?: string;
    token?: "missing" | "expired" | "valid";
    weeklyUsed?: number | null;
    weeklyStale?: boolean;
    requests?: number;
  } = {},
) => ({
  id,
  status: options.status ?? "active",
  token_health: { access_state: options.token ?? "valid" },
  weekly:
    options.weeklyUsed === null
      ? null
      : {
          used_percent: options.weeklyUsed ?? 20,
          stale: options.weeklyStale ?? false,
        },
  five_hour: null,
  request_count_24h: options.requests ?? 0,
});

test("buildAccountHealth promotes exhausted and expired routes with explicit recovery evidence", () => {
  const health = buildAccountHealth([
    route("ready", { requests: 50 }),
    route("exception", {
      status: "quota_exceeded",
      token: "expired",
      weeklyUsed: 100,
      weeklyStale: true,
      requests: 4,
    }),
  ]);

  assert.deepEqual(health.summary, { action: 1, watch: 0, ready: 1, observed: 2 });
  assert.equal(health.accounts[0]?.id, "exception");
  assert.equal(health.accounts[0]?.level, "action");
  assert.equal(health.accounts[0]?.nextAction, "reauthenticate");
  assert.equal(health.accounts[0]?.nextActionLabel, "Reauthenticate this route");
  assert.deepEqual(
    health.accounts[0]?.reasons.map((reason) => reason.label),
    ["Token expired", "Quota exhausted", "Weekly quota exhausted", "Weekly evidence stale"],
  );
});

test("buildAccountHealth separates watch signals from routes that are ready", () => {
  const health = buildAccountHealth([
    route("ready-high-activity", { requests: 120 }),
    route("stale", { weeklyUsed: 40, weeklyStale: true, requests: 5 }),
    route("constrained", { weeklyUsed: 88, requests: 20 }),
  ]);

  assert.deepEqual(health.summary, { action: 0, watch: 2, ready: 1, observed: 3 });
  assert.equal(health.accounts[0]?.id, "constrained");
  assert.equal(health.accounts[0]?.nextAction, "reduce_traffic");
  assert.equal(health.accounts[1]?.id, "stale");
  assert.equal(health.accounts[1]?.nextAction, "refresh_evidence");
  assert.equal(health.accounts[2]?.id, "ready-high-activity");
  assert.equal(health.accounts[2]?.reasons.length, 0);
});

test("buildAccountHealth uses deterministic severity, quota, activity, and id ordering", () => {
  const health = buildAccountHealth([
    route("lower-use", { weeklyUsed: 82, requests: 90 }),
    route("higher-use", { weeklyUsed: 94, requests: 1 }),
    route("same-b", { weeklyUsed: 82, requests: 40 }),
    route("same-a", { weeklyUsed: 82, requests: 40 }),
  ]);

  assert.deepEqual(
    health.accounts.map((account) => account.id),
    ["higher-use", "lower-use", "same-a", "same-b"],
  );
});
