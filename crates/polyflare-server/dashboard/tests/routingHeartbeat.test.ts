import assert from "node:assert/strict";
import test from "node:test";

import { buildRoutingHeartbeat, classifyRoutingAge } from "../src/lib/routingHeartbeat.ts";

const NOW = 1_800_000_000;

const observation = (
  id: string,
  ageSeconds: number,
  options: { failure?: boolean; accountId?: string | null; tps?: number | null } = {},
) => ({
  id,
  requestedAt: NOW - ageSeconds,
  accountId: options.accountId ?? id,
  provider: "codex",
  model: "gpt-5.6-sol",
  transport: "websocket",
  outcomeLabel: options.failure ? "592" : "200",
  failure: options.failure ?? false,
  durationMs: 1_250,
  ttftMs: 350,
  tps: options.tps ?? 42,
  serviceTier: "priority",
});

test("buildRoutingHeartbeat distinguishes live traffic and summarizes the live window", () => {
  const heartbeat = buildRoutingHeartbeat(
    [
      observation("older", 80, { accountId: "route-a" }),
      observation("latest", 12, { accountId: "route-b", tps: 88 }),
      observation("failed", 45, { accountId: "route-b", failure: true }),
    ],
    NOW,
  );

  assert.equal(heartbeat.state, "live");
  assert.equal(heartbeat.latest?.id, "latest");
  assert.equal(heartbeat.latest?.ttftMs, 350);
  assert.equal(heartbeat.latest?.serviceTier, "priority");
  assert.equal(heartbeat.ageSeconds, 12);
  assert.equal(heartbeat.windowCount, 3);
  assert.equal(heartbeat.windowFailures, 1);
  assert.equal(heartbeat.windowAccounts, 2);
});

test("buildRoutingHeartbeat makes historical evidence explicit instead of calling the fetch fresh", () => {
  const heartbeat = buildRoutingHeartbeat([observation("old", 2 * 24 * 60 * 60)], NOW);

  assert.equal(heartbeat.state, "historical");
  assert.equal(heartbeat.label, "Historical evidence");
  assert.equal(heartbeat.ageSeconds, 172_800);
  assert.equal(heartbeat.windowCount, 0);
  assert.equal(heartbeat.guidance, "Dashboard checked now; newest routing evidence is historical.");
});

test("buildRoutingHeartbeat reports empty evidence without invented recency", () => {
  const heartbeat = buildRoutingHeartbeat([], NOW);

  assert.equal(heartbeat.state, "unobserved");
  assert.equal(heartbeat.latest, null);
  assert.equal(heartbeat.ageSeconds, null);
  assert.equal(heartbeat.windowCount, 0);
  assert.equal(heartbeat.windowAccounts, 0);
});

test("buildRoutingHeartbeat keeps future clock skew bounded at zero age", () => {
  const future = { ...observation("future", 0), requestedAt: NOW + 30 };
  const heartbeat = buildRoutingHeartbeat([future], NOW);

  assert.equal(heartbeat.state, "live");
  assert.equal(heartbeat.ageSeconds, 0);
});

test("classifyRoutingAge exposes the operator-relevant recency boundaries", () => {
  assert.equal(classifyRoutingAge(null), "unobserved");
  assert.equal(classifyRoutingAge(120), "live");
  assert.equal(classifyRoutingAge(121), "quiet");
  assert.equal(classifyRoutingAge(900), "quiet");
  assert.equal(classifyRoutingAge(901), "idle");
  assert.equal(classifyRoutingAge(21_600), "idle");
  assert.equal(classifyRoutingAge(21_601), "historical");
});
