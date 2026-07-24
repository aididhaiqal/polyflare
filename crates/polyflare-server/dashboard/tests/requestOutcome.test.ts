import assert from "node:assert/strict";
import test from "node:test";

import type { RequestRowView } from "../src/lib/api.ts";
import {
  requestOutcomeIsFailure,
  requestOutcomeIsSuccess,
  requestOutcomeLabel,
  requestOutcomeSource,
} from "../src/lib/requestOutcome.ts";

function row(
  protocol_outcome: RequestRowView["protocol_outcome"],
  status = 200,
): RequestRowView {
  return {
    id: 1,
    request_id: "request-1",
    session_key: "session-1",
    requested_at: 1,
    provider: "codex",
    method: "POST",
    path: "/responses",
    aliased: false,
    status,
    duration_ms: 10,
    account_id: "account-1",
    model: "gpt-5.6-sol",
    reasoning_effort: null,
    service_tier: null,
    transport: "sse",
    ttft_ms: null,
    total_tokens: null,
    cached_tokens: null,
    tps: null,
    outcome: null,
    protocol_outcome,
    error_code: null,
    subagent: null,
  };
}

test("native protocol failures override their initial HTTP 200", () => {
  for (const outcome of [
    "failed",
    "incomplete",
    "cancelled",
    "transport_lost",
  ] as const) {
    const request = row(outcome);
    assert.equal(requestOutcomeIsFailure(request), true);
    assert.equal(requestOutcomeIsSuccess(request), false);
    assert.equal(requestOutcomeSource(request), "protocol");
  }
});

test("completed remains successful and exposes the bounded terminal label", () => {
  const request = row("completed");
  assert.equal(requestOutcomeIsSuccess(request), true);
  assert.equal(requestOutcomeIsFailure(request), false);
  assert.equal(requestOutcomeLabel(request), "completed");
});
