import assert from "node:assert/strict";
import test from "node:test";

import {
  analyzeTrafficSignal,
  type LiveActivityBucket,
} from "../src/lib/liveActivity.ts";

const NOW_TS = 10_000;
const CURRENT_MINUTE_TS = Math.floor(NOW_TS / 60) * 60;

function completedMinutes(requests: number[]): LiveActivityBucket[] {
  const firstTs = CURRENT_MINUTE_TS - requests.length * 60;
  return requests.map((value, index) => ({
    ts: firstTs + index * 60,
    requests: value,
  }));
}

test("live activity reports insufficient history instead of inventing a baseline", () => {
  const signal = analyzeTrafficSignal(completedMinutes(Array(20).fill(1)), CURRENT_MINUTE_TS);
  assert.equal(signal.state, "insufficient_history");
  assert.equal(signal.ratio, null);
});

test("live activity keeps ordinary variation steady", () => {
  const signal = analyzeTrafficSignal(
    completedMinutes([
      ...Array(30).fill(2),
      2,
      2,
      3,
      3,
      3,
    ]),
    CURRENT_MINUTE_TS,
  );
  assert.equal(signal.state, "steady");
  assert.equal(signal.baselineRequests, 10);
  assert.equal(signal.recentRequests, 13);
  assert.equal(signal.ratio, 1.3);
});

test("live activity flags a material two-times traffic surge", () => {
  const signal = analyzeTrafficSignal(
    completedMinutes([
      ...Array(30).fill(1),
      2,
      2,
      2,
      2,
      2,
    ]),
    CURRENT_MINUTE_TS,
  );
  assert.equal(signal.state, "surge");
  assert.equal(signal.baselineRequests, 5);
  assert.equal(signal.recentRequests, 10);
  assert.equal(signal.ratio, 2);
});

test("live activity names a burst when the prior baseline was zero", () => {
  const signal = analyzeTrafficSignal(
    completedMinutes([
      ...Array(30).fill(0),
      2,
      1,
      1,
      1,
      1,
    ]),
    CURRENT_MINUTE_TS,
  );
  assert.equal(signal.state, "new_burst");
  assert.equal(signal.baselineRequests, 0);
  assert.equal(signal.recentRequests, 6);
  assert.equal(signal.ratio, null);
});

test("live activity ignores the in-progress minute even when it is large", () => {
  const signal = analyzeTrafficSignal(
    [
      ...completedMinutes(Array(35).fill(1)),
      { ts: CURRENT_MINUTE_TS, requests: 1_000 },
    ],
    CURRENT_MINUTE_TS,
  );
  assert.equal(signal.state, "steady");
  assert.equal(signal.recentRequests, 5);
  assert.equal(signal.latestCompletedTs, CURRENT_MINUTE_TS - 60);
});

test("live activity keeps the response's settling bucket excluded after the wall clock rolls over", () => {
  const responseBuckets = [
    ...completedMinutes(Array(35).fill(1)),
    { ts: CURRENT_MINUTE_TS, requests: 1_000 },
  ];
  const signal = analyzeTrafficSignal(responseBuckets, CURRENT_MINUTE_TS);
  assert.equal(signal.state, "steady");
  assert.equal(signal.recentRequests, 5);
  assert.equal(signal.latestCompletedTs, CURRENT_MINUTE_TS - 60);
});

test("live activity rejects a gapped baseline rather than comparing unequal windows", () => {
  const buckets = completedMinutes(Array(35).fill(1));
  buckets.splice(10, 1);
  const signal = analyzeTrafficSignal(buckets, CURRENT_MINUTE_TS);
  assert.equal(signal.state, "insufficient_history");
});
