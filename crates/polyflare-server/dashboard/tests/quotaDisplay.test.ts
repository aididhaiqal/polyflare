import assert from "node:assert/strict";
import test from "node:test";

import {
  clampQuotaPercent,
  quotaDisplayLabel,
  quotaDisplayPercent,
  quotaWindowIsPresent,
} from "../src/lib/quotaDisplay.ts";

test("quotaDisplayPercent switches between consumed and available capacity", () => {
  assert.equal(quotaDisplayPercent(74, "used"), 74);
  assert.equal(quotaDisplayPercent(74, "remaining"), 26);
  assert.equal(quotaDisplayLabel("used"), "used");
  assert.equal(quotaDisplayLabel("remaining"), "remaining");
});

test("quota display clamps malformed upstream percentages before inversion", () => {
  assert.equal(clampQuotaPercent(-4), 0);
  assert.equal(clampQuotaPercent(104), 100);
  assert.equal(quotaDisplayPercent(-4, "remaining"), 100);
  assert.equal(quotaDisplayPercent(104, "remaining"), 0);
});

test("quotaWindowIsPresent hides limits the provider does not report", () => {
  assert.equal(quotaWindowIsPresent(null), false);
  assert.equal(quotaWindowIsPresent(undefined), false);
  assert.equal(quotaWindowIsPresent({ used_percent: 12, stale: true }), false);
  assert.equal(quotaWindowIsPresent({ used_percent: 0 }), true);
});
