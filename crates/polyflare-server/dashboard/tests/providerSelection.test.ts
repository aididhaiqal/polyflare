import assert from "node:assert/strict";
import test from "node:test";

import {
  parseProviderSelection,
  providerSelectionLabel,
  resolveProviderSelection,
  serializeProviderSelection,
} from "../src/lib/providerSelection.ts";

const models = ["codex", "anthropic", "sakana"];
const all = [...models, "chatgpt_backend"];

test("provider selection defaults to every model provider but not backend", () => {
  const selected = resolveProviderSelection(null, models, all);
  assert.deepEqual(selected, models);
  assert.equal(providerSelectionLabel(selected, models, all), "Models");
  assert.equal(serializeProviderSelection(selected, models, all), null);
});

test("provider selection supports multiple explicit providers including backend", () => {
  assert.deepEqual(
    resolveProviderSelection("codex,chatgpt_backend", models, all),
    ["codex", "chatgpt_backend"],
  );
  assert.equal(
    serializeProviderSelection(["codex", "chatgpt_backend"], models, all),
    "codex,chatgpt_backend",
  );
});

test("legacy claude URL values normalize to the anthropic provider", () => {
  assert.deepEqual(parseProviderSelection("claude,chatgpt_backend"), [
    "anthropic",
    "chatgpt_backend",
  ]);
});
