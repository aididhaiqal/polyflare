import assert from "node:assert/strict";
import test from "node:test";

import {
  requestBucketErrorRate,
  summarizeRequestVolume,
} from "../src/lib/requestVolume.ts";

test("summarizeRequestVolume derives visible operational context from report buckets", () => {
  assert.deepEqual(
    summarizeRequestVolume([
      { ts: 1, requests: 10, errors: 1 },
      { ts: 2, requests: 30, errors: 2 },
      { ts: 3, requests: 20, errors: 0 },
    ]),
    {
      total: 60,
      errors: 3,
      average: 20,
      peak: 30,
      latest: 20,
      errorRate: 0.05,
    },
  );
});

test("requestBucketErrorRate handles empty and malformed buckets without invalid intensity", () => {
  assert.equal(requestBucketErrorRate({ ts: 1, requests: 0, errors: 2 }), 0);
  assert.equal(requestBucketErrorRate({ ts: 1, requests: 10, errors: -1 }), 0);
  assert.equal(requestBucketErrorRate({ ts: 1, requests: 10, errors: 20 }), 1);
});
