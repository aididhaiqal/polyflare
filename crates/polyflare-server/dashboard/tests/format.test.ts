import assert from "node:assert/strict";
import test from "node:test";

import { billingLabel, planLabel, ratePct, shortDate } from "../src/lib/format.ts";

test("ratePct never presents a non-perfect rate as 100%", () => {
  assert.equal(ratePct(99.95), "<100%");
  assert.equal(ratePct(99.99), "<100%");
  assert.equal(ratePct(100), "100%");
});

test("billingLabel shows the renewal with its as-of date, and calls an elapsed period stale", () => {
  const now = Date.UTC(2026, 8, 30, 0, 0, 0); // 2026-09-30
  const fresh = billingLabel({ active_until: 1_793_243_736, last_checked: 1_790_651_761 }, now);
  assert.equal(fresh?.value, "Renews 29 Oct");
  assert.match(fresh?.hint ?? "", /^in 29d \d+h · as of 29 Sep$/);
  assert.equal(fresh?.stale, false);
  // A token whose claims say the period ended in August is stale, not a fact about the seat.
  const stale = billingLabel({ active_until: Date.UTC(2026, 7, 8) / 1000, last_checked: Date.UTC(2026, 7, 3) / 1000 }, now);
  assert.equal(stale?.value, "Ended 8 Aug");
  assert.equal(stale?.hint, "stale claim · as of 3 Aug · re-login refreshes");
  assert.equal(stale?.stale, true);
  assert.equal(billingLabel(null, now), null);
  assert.equal(billingLabel({ active_until: null, last_checked: 1 }, now), null);
  assert.equal(shortDate(Date.UTC(2025, 7, 8) / 1000, now), "8 Aug 2025");
});
