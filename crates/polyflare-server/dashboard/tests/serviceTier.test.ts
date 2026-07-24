import assert from "node:assert/strict";
import test from "node:test";

import { serviceTierDisplay } from "../src/lib/serviceTier.ts";

test("serviceTierDisplay identifies priority aliases without overstating missing evidence", () => {
  assert.deepEqual(serviceTierDisplay("priority"), {
    kind: "priority",
    label: "Priority",
    recordedValue: "priority",
  });
  assert.deepEqual(serviceTierDisplay(" FAST "), {
    kind: "priority",
    label: "Priority",
    recordedValue: "FAST",
  });
  assert.deepEqual(serviceTierDisplay(null), {
    kind: "default",
    label: "Default",
    recordedValue: null,
  });
});

test("serviceTierDisplay preserves flex and unfamiliar recorded tiers", () => {
  assert.equal(serviceTierDisplay("flex").label, "Flex");
  assert.deepEqual(serviceTierDisplay("batch"), {
    kind: "other",
    label: "batch",
    recordedValue: "batch",
  });
});
