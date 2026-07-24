import assert from "node:assert/strict";
import test from "node:test";

import type { ProviderModelView } from "../src/lib/api.ts";
import {
  isProviderModelProfile,
  providerProfileTemplate,
} from "../src/lib/providerProfiles.ts";

function model(overrides: Partial<ProviderModelView> = {}): ProviderModelView {
  return {
    id: "model-1",
    provider_id: "provider-1",
    public_model: "openrouter/vendor/model",
    upstream_model: "vendor/model",
    display_name: "Vendor Model",
    context_window: 100_000,
    max_output_tokens: 10_000,
    supports_tools: true,
    supports_vision: false,
    supports_parallel_tool_calls: true,
    supports_web_search: false,
    supports_reasoning_summaries: false,
    reasoning_levels: ["medium", "high"],
    instruction_mode: "none",
    instruction_text: "",
    request_overrides: {},
    input_per_million: 1,
    cached_input_per_million: 0.1,
    output_per_million: 4,
    visible_in_codex: true,
    visible_in_openai: true,
    enabled: true,
    ...overrides,
  };
}

test("ordinary imported models remain no-op profiles", () => {
  assert.equal(isProviderModelProfile(model()), false);
});

test("instruction and request override variants are classified as profiles", () => {
  assert.equal(
    isProviderModelProfile(
      model({ instruction_mode: "append", instruction_text: "Review carefully." }),
    ),
    true,
  );
  assert.equal(
    isProviderModelProfile(model({ request_overrides: { reasoning_effort: "high" } })),
    true,
  );
});

test("profile template keeps the upstream mapping behind a new editable public alias", () => {
  assert.deepEqual(providerProfileTemplate(model()), {
    publicModel: "openrouter/vendor/model~profile",
    displayName: "Vendor Model · Profile",
    instructionMode: "append",
  });
});
