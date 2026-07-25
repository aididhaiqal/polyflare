import assert from "node:assert/strict";
import test from "node:test";

import { targetCapacityKey, type TranslationRouteView } from "../src/lib/api.ts";
import {
  duplicateTranslationRoute,
  emptyTranslationRoute,
  nextTranslationPriority,
  routeInputFrom,
  unservableReason,
} from "../src/lib/translationRoutes.ts";

const route: TranslationRouteView = {
  id: "route-1",
  name: "Opus",
  enabled: true,
  source_protocol: "anthropic_messages",
  match_kind: "contains",
  model_pattern: "opus",
  target_kind: "builtin_provider",
  target_provider_id: "codex",
  target_model: "gpt-5.6-sol",
  reasoning_effort: "high",
  priority: 20,
  created_at: 1,
  updated_at: 2,
};

test("route input strips server-owned fields", () => {
  assert.deepEqual(routeInputFrom(route), {
    name: "Opus",
    enabled: true,
    source_protocol: "anthropic_messages",
    match_kind: "contains",
    model_pattern: "opus",
    target_kind: "builtin_provider",
    target_provider_id: "codex",
    target_model: "gpt-5.6-sol",
    reasoning_effort: "high",
    priority: 20,
  });
});

test("duplicate remains enabled but moves behind its source", () => {
  const copy = duplicateTranslationRoute(route);
  assert.equal(copy.name, "Opus copy");
  assert.equal(copy.priority, 30);
  assert.equal(copy.target_model, route.target_model);
});

test("new routes follow the current priority range", () => {
  assert.equal(nextTranslationPriority([]), 100);
  assert.equal(nextTranslationPriority([route, { ...route, priority: 300 }]), 400);
  assert.equal(emptyTranslationRoute(400).priority, 400);
});

test("an unservable target names the cause, not just the count", () => {
  // The trap this exists for: the route looks healthy and cannot run, because a subscription grant
  // serves only native client traffic.
  assert.equal(
    unservableReason({ eligible: 0, barred_subscription: 2 }),
    "2 subscription accounts on this target can serve only native client traffic.",
  );
  assert.equal(
    unservableReason({ eligible: 0, barred_subscription: 1 }),
    "1 subscription account on this target can serve only native client traffic.",
  );
  // An empty target is a different problem and must not be blamed on subscriptions.
  assert.equal(
    unservableReason({ eligible: 0, barred_subscription: 0 }),
    "No account or credential on this target can serve it.",
  );
});

test("capacity keys match the server's target map", () => {
  assert.equal(
    targetCapacityKey({ target_kind: "builtin_provider", target_provider_id: "anthropic" }),
    "builtin_provider:anthropic",
  );
  assert.equal(
    targetCapacityKey({ target_kind: "custom_provider", target_provider_id: "prov-1" }),
    "custom_provider:prov-1",
  );
});
