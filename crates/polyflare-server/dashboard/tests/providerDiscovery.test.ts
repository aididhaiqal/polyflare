import assert from "node:assert/strict";
import test from "node:test";

import {
  filterDiscoveredModels,
  selectableDiscoveredModelIds,
  type DiscoveryCapabilityFilter,
} from "../src/lib/providerDiscovery.ts";
import type { ProviderDiscoveredModelView } from "../src/lib/api.ts";

function model(
  upstreamModel: string,
  overrides: Partial<ProviderDiscoveredModelView> = {},
): ProviderDiscoveredModelView {
  return {
    upstream_model: upstreamModel,
    suggested_public_model: `openrouter/${upstreamModel}`,
    display_name: upstreamModel,
    context_window: null,
    max_output_tokens: null,
    supports_tools: false,
    supports_vision: false,
    supports_parallel_tool_calls: false,
    supports_web_search: false,
    supports_reasoning: false,
    supports_reasoning_summaries: false,
    reasoning_levels: [],
    input_per_million: null,
    cached_input_per_million: null,
    output_per_million: null,
    state: "available",
    ...overrides,
  };
}

const models = [
  model("anthropic/claude-sonnet-5", {
    display_name: "Claude Sonnet 5",
    supports_tools: true,
    supports_vision: true,
    supports_reasoning: true,
    reasoning_levels: ["high", "medium"],
  }),
  model("deepseek/deepseek-r1:free", {
    supports_tools: true,
    supports_reasoning: true,
  }),
  model("openai/gpt-5.4", { state: "configured", supports_tools: true }),
  model("vendor/conflicted", { state: "conflict" }),
];

test("discovery filtering combines search and capability filters", () => {
  const cases: Array<[DiscoveryCapabilityFilter, string[]]> = [
    ["all", models.map((entry) => entry.upstream_model)],
    ["tools", [
      "anthropic/claude-sonnet-5",
      "deepseek/deepseek-r1:free",
      "openai/gpt-5.4",
    ]],
    ["vision", ["anthropic/claude-sonnet-5"]],
    ["reasoning", ["anthropic/claude-sonnet-5", "deepseek/deepseek-r1:free"]],
    ["free", ["deepseek/deepseek-r1:free"]],
  ];
  for (const [filter, expected] of cases) {
    assert.deepEqual(
      filterDiscoveredModels(models, "", filter).map((entry) => entry.upstream_model),
      expected,
    );
  }
  assert.deepEqual(
    filterDiscoveredModels(models, "sonnet", "tools").map((entry) => entry.upstream_model),
    ["anthropic/claude-sonnet-5"],
  );
});

test("bulk selection never includes configured or conflicting candidates", () => {
  assert.deepEqual(selectableDiscoveredModelIds(models), [
    "anthropic/claude-sonnet-5",
    "deepseek/deepseek-r1:free",
  ]);
});
