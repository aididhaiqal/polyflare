import assert from "node:assert/strict";
import test from "node:test";

import { backendRequestDisplay } from "../src/lib/requestClassification.ts";

test("classifies synthetic pool usage separately from backend passthrough", () => {
  assert.deepEqual(
    backendRequestDisplay({
      provider: "chatgpt_backend",
      path: "chatgpt_backend_synthetic_wham/usage",
    }),
    {
      kind: "synthetic_usage",
      targetLabel: "ChatGPT backend",
      operationLabel: "Synthetic usage",
    },
  );
  assert.deepEqual(
    backendRequestDisplay({
      provider: "chatgpt_backend",
      path: "chatgpt_backend_passthrough_wham/settings/user",
    }),
    {
      kind: "passthrough",
      targetLabel: "ChatGPT backend",
      operationLabel: "Backend passthrough",
    },
  );
});

test("does not reclassify ordinary model providers", () => {
  assert.equal(
    backendRequestDisplay({
      provider: "codex",
      path: "/responses",
    }),
    null,
  );
});

test("classifies legacy gateway rows by their normalized path", () => {
  assert.deepEqual(
    backendRequestDisplay({
      provider: "codex",
      path: "chatgpt_backend_passthrough_wham/settings/user",
    }),
    {
      kind: "passthrough",
      targetLabel: "ChatGPT backend",
      operationLabel: "Backend passthrough",
    },
  );
});
