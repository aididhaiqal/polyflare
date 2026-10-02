import assert from "node:assert/strict";
import test from "node:test";

import { usageLabel, usageSummary, usageTone } from "../src/lib/claudeUsage.ts";

const now = 1_790_000_000_000;

test("usage lines read like Claude Code's /usage", () => {
  assert.equal(
    usageSummary({ kind: "session", model: null, percent: 43.4, resets_at: 1_790_003_600 + 600, severity: null }, now),
    "43% used · resets in 1h 10m",
  );
  assert.equal(
    usageSummary({ kind: "weekly_all", model: null, percent: 12, resets_at: null, severity: null }, now),
    "12% used · no reset time reported",
  );
  assert.equal(
    usageSummary({ kind: "weekly_scoped", model: "Fable", percent: 100, resets_at: 1_789_999_000, severity: "limit_reached" }, now),
    "100% used · reset due",
  );
  assert.equal(usageLabel({ kind: "session", model: null, percent: 0, resets_at: null, severity: null }), "Current session");
  assert.equal(usageLabel({ kind: "weekly_all", model: null, percent: 0, resets_at: null, severity: null }), "Current week (all models)");
  assert.equal(usageLabel({ kind: "weekly_scoped", model: "Fable", percent: 0, resets_at: null, severity: null }), "Current week (Fable)");
});

test("usage tone follows the percentage and the upstream severity", () => {
  const line = (percent: number, severity: string | null) => ({ kind: "session", model: null, percent, resets_at: null, severity });
  assert.equal(usageTone(line(10, null)), "ok");
  assert.equal(usageTone(line(85, null)), "warn");
  assert.equal(usageTone(line(10, "warning")), "warn");
  assert.equal(usageTone(line(100, null)), "error");
  assert.equal(usageTone(line(50, "limit_reached")), "error");
});
