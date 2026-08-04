import { expect, test } from "bun:test";

import { pct, pctTenths, ratePct } from "../src/lib/format";

/**
 * Why the cache-hit card needed its own formatter.
 *
 * The rate is genuinely stable — 95.78% token-weighted across 1.29 billion input tokens — but
 * whole-number rounding flattened the residual movement too, so the card showed one unchanging
 * number and read as a stuck gauge. `ratePct` looked like the fix and is not: it adds precision
 * only below 10% and between 99% and 100%, and falls through to the same rounding in between.
 */
test("neither existing formatter shows a mid-range rate moving", () => {
  // The real hourly figures from 2026-08-04.
  const hourly = [96.6, 94.5, 93.6, 95.9, 96.1, 94.2, 95.3, 96.0];

  expect(pct(95.78)).toBe("96%");
  expect(ratePct(95.78)).toBe("96%"); // identical — the trap this test exists to document
  expect(pctTenths(95.78)).toBe("95.8%");

  // Every distinct hourly reading must stay distinct once rendered.
  expect(new Set(hourly.map((n) => pctTenths(n))).size).toBe(new Set(hourly).size);
});

test("pctTenths keeps one decimal across the whole range", () => {
  expect(pctTenths(0)).toBe("0.0%");
  expect(pctTenths(96)).toBe("96.0%");
  expect(pctTenths(99.94)).toBe("99.9%");
  expect(pctTenths(null)).toBe("—");
  expect(pctTenths(Number.NaN)).toBe("—");
});

/** `ratePct` keeps its own job: rates at the extremes, where a rounded 100% would be a lie. */
test("ratePct still guards the extremes it was written for", () => {
  expect(ratePct(99.6)).toBe("99.6%");
  expect(ratePct(0.493)).toBe("0.5%");
  expect(pct(99.6)).toBe("100%");
});
