import assert from "node:assert/strict";
import test from "node:test";

import type {
  CustomProviderView,
  ProviderPerformanceRowView,
} from "../src/lib/api.ts";
import {
  buildProviderModelPerformanceComparisons,
  performanceUplift,
} from "../src/lib/providerPerformance.ts";

function provider(
  id: string,
  slug: string,
  name: string,
  supportsPriority: boolean,
): CustomProviderView {
  return {
    id,
    slug,
    display_name: name,
    base_url: "https://example.com/v1",
    wire_api: "responses",
    enabled: true,
    stateless_responses: true,
    allow_private_hosts: false,
    connect_timeout_ms: 1_000,
    stream_idle_timeout_ms: 10_000,
    request_max_retries: 0,
    max_concurrency: null,
    credentials: [],
    models: [
      {
        id: `${id}-model`,
        provider_id: id,
        public_model: "kimi-k3",
        upstream_model: "accounts/example/models/kimi-k3",
        display_name: "Kimi K3",
        context_window: 262_144,
        max_output_tokens: 131_072,
        supports_tools: true,
        supports_vision: false,
        supports_parallel_tool_calls: true,
        supports_web_search: false,
        supports_reasoning_summaries: false,
        supports_priority_service_tier: supportsPriority,
        reasoning_levels: [],
        instruction_mode: "none",
        instruction_text: "",
        request_overrides: {},
        input_per_million: null,
        cached_input_per_million: null,
        output_per_million: null,
        visible_in_codex: true,
        visible_in_openai: true,
        enabled: true,
      },
    ],
  };
}

function sample(
  providerSlug: string,
  tier: "standard" | "priority",
  tps: number,
): ProviderPerformanceRowView {
  return {
    provider: providerSlug,
    model: "kimi-k3",
    tier,
    requests: 2,
    avg_ttft_ms: tier === "priority" ? 120 : 240,
    ttft_sample_count: 2,
    output_tokens: 200,
    generation_ms: 1_000,
    tps_sample_count: 2,
    tps,
    p50_ttft_ms: tier === "priority" ? 110 : 220,
    p95_ttft_ms: tier === "priority" ? 250 : 480,
    p50_tps: tps * 0.9,
    p95_tps: tps * 1.2,
    successes: 2,
    errors: 0,
    rate_limited: 0,
  };
}

test("performance comparisons keep the same model separate across providers and tiers", () => {
  const rows = buildProviderModelPerformanceComparisons(
    [
      provider("provider-a", "fireworks", "Fireworks", true),
      provider("provider-b", "openrouter", "OpenRouter", false),
    ],
    [
      sample("fireworks", "standard", 100),
      sample("fireworks", "priority", 180),
      sample("openrouter", "standard", 90),
    ],
  );

  assert.equal(rows.length, 2);
  assert.equal(rows[0].model, "kimi-k3");
  assert.notEqual(rows[0].providerId, rows[1].providerId);
  assert.equal(rows.find((row) => row.providerSlug === "fireworks")?.priority?.tps, 180);
  assert.equal(rows.find((row) => row.providerSlug === "fireworks")?.hasPriorityHistory, true);
  assert.equal(rows.find((row) => row.providerSlug === "openrouter")?.priority, null);
  assert.equal(rows.find((row) => row.providerSlug === "openrouter")?.supportsPriority, false);
  assert.equal(rows.find((row) => row.providerSlug === "openrouter")?.hasPriorityHistory, false);
});

test("recorded priority traffic keeps its lane visible after capability is disabled", () => {
  const rows = buildProviderModelPerformanceComparisons(
    [provider("provider-a", "fireworks", "Fireworks", false)],
    [sample("fireworks", "priority", 170)],
  );

  assert.equal(rows[0].supportsPriority, false);
  assert.equal(rows[0].hasPriorityHistory, true);
  assert.equal(rows[0].priority?.tps, 170);
  assert.equal(rows[0].standard, null);
});

test("performance uplift compares priority against standard without inventing missing evidence", () => {
  const standard = sample("fireworks", "standard", 100);
  const priority = sample("fireworks", "priority", 150);

  assert.deepEqual(performanceUplift(standard, priority), {
    tps_percent: 50,
    ttft_percent: 50,
  });
  assert.deepEqual(performanceUplift(null, priority), {
    tps_percent: null,
    ttft_percent: null,
  });
});
