import assert from "node:assert/strict";
import test from "node:test";

import { capacityMapAccounts } from "../src/lib/capacityMap.ts";

const account = (id: string, usedPercent: number | null) => ({
  id,
  weekly: usedPercent === null ? null : { used_percent: usedPercent },
});

test("capacityMapAccounts returns every weekly-observed account in constrained-first order", () => {
  const visible = capacityMapAccounts([
    account("coolest", 0),
    account("hot", 97),
    account("cool", 1),
    account("middle", 19),
    account("warm", 4),
    account("unobserved", null),
  ]);

  assert.deepEqual(
    visible.map((route) => route.id),
    ["hot", "middle", "warm", "cool", "coolest"],
  );
});
