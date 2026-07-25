import assert from "node:assert/strict";
import test from "node:test";

import type { LogEvent } from "../src/lib/api.ts";
import {
  appendUniqueLogEvent,
  logEventMatchesProviders,
} from "../src/lib/liveLogFiltering.ts";

function event(provider?: string): LogEvent {
  return {
    ts_ms: 1,
    level: "info",
    kind: "request",
    message: "request completed",
    ...(provider === undefined ? {} : { provider }),
  };
}

test("model selections exclude ChatGPT backend events", () => {
  const selected = ["codex", "anthropic"];
  assert.equal(logEventMatchesProviders(event("codex"), selected), true);
  assert.equal(logEventMatchesProviders(event("anthropic"), selected), true);
  assert.equal(logEventMatchesProviders(event("chatgpt_backend"), selected), false);
});

test("backend can be combined with model providers", () => {
  const selected = ["codex", "chatgpt_backend"];
  assert.equal(logEventMatchesProviders(event("codex"), selected), true);
  assert.equal(logEventMatchesProviders(event("chatgpt_backend"), selected), true);
  assert.equal(logEventMatchesProviders(event("anthropic"), selected), false);
});

test("provider-less operational events stay visible", () => {
  assert.equal(logEventMatchesProviders(event(), []), true);
});

test("replayed SSE backfill events are not appended twice", () => {
  const first = {
    ...event("codex"),
    ts_ms: 10,
    request_id: "req-1",
    session_key: "session-1",
    status: 200,
  };
  const second = {
    ...event("codex"),
    ts_ms: 11,
    request_id: "req-2",
    session_key: "session-1",
    status: 200,
  };

  const once = appendUniqueLogEvent([], first, 1000);
  const replayed = appendUniqueLogEvent(once, first, 1000);
  const withNext = appendUniqueLogEvent(replayed, second, 1000);

  assert.deepEqual(replayed, [first]);
  assert.deepEqual(withNext, [first, second]);
});

test("same-looking request events remain distinct when correlation ids differ", () => {
  const first = { ...event("codex"), ts_ms: 10, request_id: "req-1" };
  const second = { ...event("codex"), ts_ms: 10, request_id: "req-2" };

  assert.deepEqual(appendUniqueLogEvent([first], second, 1000), [first, second]);
});

test("unique SSE buffer remains bounded after de-duplication", () => {
  const old = { ...event("codex"), ts_ms: 1, request_id: "old" };
  const middle = { ...event("codex"), ts_ms: 2, request_id: "middle" };
  const newest = { ...event("codex"), ts_ms: 3, request_id: "newest" };

  assert.deepEqual(appendUniqueLogEvent([old, middle], newest, 2), [middle, newest]);
});
