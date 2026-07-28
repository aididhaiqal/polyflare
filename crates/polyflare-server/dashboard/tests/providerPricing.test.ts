import test from "node:test";
import assert from "node:assert/strict";

import {
  formatProviderPrice,
  providerPricingSummary,
} from "../src/lib/providerPricing.ts";

test("provider pricing presents normal cached and output rates per million tokens", () => {
  assert.equal(
    providerPricingSummary({
      input_per_million: 2.5,
      cached_input_per_million: 0.25,
      output_per_million: 12,
    }),
    "Input $2.50 · Cache $0.25 · Output $12.00",
  );
});

test("provider pricing keeps unknown and malformed rates explicit", () => {
  assert.equal(formatProviderPrice(null), "—");
  assert.equal(formatProviderPrice(Number.NaN), "—");
  assert.equal(formatProviderPrice(-1), "—");
});
