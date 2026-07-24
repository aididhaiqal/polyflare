import assert from "node:assert/strict";
import test from "node:test";

import {
  latestRequestEventKey,
  requestLiveLabel,
  requestRefreshInterval,
} from "../src/lib/requestLive.ts";

test("request refresh uses SSE while connected and polling only as fallback", () => {
  assert.equal(requestRefreshInterval(true), false);
  assert.equal(requestRefreshInterval(false), 30_000);
  assert.equal(requestLiveLabel({ sseConnected: true, isFetching: false }), "Live · SSE");
  assert.equal(
    requestLiveLabel({ sseConnected: false, isFetching: false }),
    "Fallback · polling 30s",
  );
});

test("latest request event ignores unrelated operational log events", () => {
  const key = latestRequestEventKey([
    { ts_ms: 10, level: "info", kind: "request", message: "done", request_id: "req-a" },
    { ts_ms: 11, level: "info", kind: "health", message: "healthy" },
  ]);

  assert.equal(key, "req-a||10|||");
  assert.equal(
    latestRequestEventKey([{ ts_ms: 11, level: "info", kind: "health", message: "healthy" }]),
    null,
  );
});
