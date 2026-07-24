import assert from "node:assert/strict";
import test from "node:test";

import { ratePct } from "../src/lib/format.ts";

test("ratePct never presents a non-perfect rate as 100%", () => {
  assert.equal(ratePct(99.95), "<100%");
  assert.equal(ratePct(99.99), "<100%");
  assert.equal(ratePct(100), "100%");
});
