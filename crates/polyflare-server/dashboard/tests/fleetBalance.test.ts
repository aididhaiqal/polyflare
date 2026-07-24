import assert from "node:assert/strict";
import test from "node:test";

import {
  buildFleetBalance,
  fleetBalanceFallbackState,
} from "../src/lib/fleetBalance.ts";

const route = (
  id: string,
  usedPercent: number | null,
  options: {
    status?: string;
    token?: "missing" | "expired" | "valid";
    stale?: boolean;
    resetAt?: number | null;
  } = {},
) => ({
  id,
  status: options.status ?? "active",
  token_health: { access_state: options.token ?? "valid" },
  weekly:
    usedPercent === null
      ? null
      : {
          used_percent: usedPercent,
          reset_at: options.resetAt ?? null,
          stale: options.stale ?? false,
        },
});

test("buildFleetBalance measures only fresh, routable weekly snapshots", () => {
  const balance = buildFleetBalance([
    route("cool", 8, { resetAt: 1_800_000_000 }),
    route("middle-a", 32),
    route("middle-b", 48),
    route("hot", 88, { resetAt: 1_800_003_600 }),
    route("paused", 100, { status: "paused" }),
    route("expired", 90, { token: "expired" }),
    route("stale", 95, { stale: true }),
    route("unobserved", null),
  ]);

  assert.equal(balance.trackedCount, 7);
  assert.equal(balance.eligibleCount, 4);
  assert.equal(balance.staleCount, 1);
  assert.equal(balance.medianUsedPercent, 40);
  assert.equal(balance.spreadPoints, 80);
  assert.equal(balance.constrainedCount, 1);
  assert.equal(balance.tone, "constrained");
  assert.equal(balance.action, "protect");
  assert.deepEqual(balance.coolest, {
    id: "cool",
    usedPercent: 8,
    resetAt: 1_800_000_000,
  });
  assert.deepEqual(balance.hottest, {
    id: "hot",
    usedPercent: 88,
    resetAt: 1_800_003_600,
  });
});

test("buildFleetBalance distinguishes a balanced fleet from an uneven one", () => {
  const balanced = buildFleetBalance([route("a", 20), route("b", 36)]);
  assert.equal(balanced.tone, "balanced");
  assert.equal(balanced.spreadPoints, 16);

  const uneven = buildFleetBalance([route("a", 10), route("b", 40)]);
  assert.equal(uneven.tone, "uneven");
  assert.equal(uneven.spreadPoints, 30);
});

test("buildFleetBalance reports missing fresh routing evidence without inventing metrics", () => {
  const balance = buildFleetBalance([
    route("paused", 100, { status: "paused" }),
    route("stale", 75, { stale: true }),
  ]);

  assert.equal(balance.trackedCount, 2);
  assert.equal(balance.eligibleCount, 0);
  assert.equal(balance.staleCount, 1);
  assert.equal(balance.medianUsedPercent, null);
  assert.equal(balance.coolest, null);
  assert.equal(balance.hottest, null);
  assert.equal(balance.tone, "unavailable");
  assert.equal(balance.action, "restore");
});

test("a single constrained route is held rather than recommended as an alternate", () => {
  const balance = buildFleetBalance([route("only-route", 88)]);

  assert.equal(balance.eligibleCount, 1);
  assert.equal(balance.tone, "constrained");
  assert.equal(balance.action, "hold");
});

test("fallback waits for account evidence and surfaces account failures", () => {
  assert.equal(fleetBalanceFallbackState(false, true, false), "loading");
  assert.equal(fleetBalanceFallbackState(false, false, true), "error");
  assert.equal(fleetBalanceFallbackState(false, false, false), "snapshot");
  assert.equal(fleetBalanceFallbackState(true, true, true), "pace");
});
