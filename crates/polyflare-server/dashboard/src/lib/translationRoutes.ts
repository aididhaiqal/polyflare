import type {
  TargetCapacityView,
  TranslationRouteInput,
  TranslationRouteView,
} from "./api";

/**
 * Why a route target cannot serve a translated request, phrased for the operator.
 *
 * A route can be enabled, well-formed and matching and still be unservable: a subscription grant
 * authorizes one first-party client shape, so selection excludes those accounts from translated
 * traffic. Saying only "0 accounts" would leave the operator hunting; name the cause.
 */
export function unservableReason(capacity: TargetCapacityView): string {
  if (capacity.barred_subscription > 0) {
    const n = capacity.barred_subscription;
    return `${n} subscription account${n === 1 ? "" : "s"} on this target can serve only native client traffic.`;
  }
  return "No account or credential on this target can serve it.";
}

export function emptyTranslationRoute(priority = 100): TranslationRouteInput {
  return {
    name: "",
    enabled: true,
    source_protocol: "anthropic_messages",
    match_kind: "contains",
    model_pattern: "",
    target_kind: "builtin_provider",
    target_provider_id: "codex",
    target_model: "",
    reasoning_effort: null,
    priority,
  };
}

export function routeInputFrom(route: TranslationRouteView): TranslationRouteInput {
  return {
    name: route.name,
    enabled: route.enabled,
    source_protocol: route.source_protocol,
    match_kind: route.match_kind,
    model_pattern: route.model_pattern,
    target_kind: route.target_kind,
    target_provider_id: route.target_provider_id,
    target_model: route.target_model,
    reasoning_effort: route.reasoning_effort,
    priority: route.priority,
  };
}

export function duplicateTranslationRoute(route: TranslationRouteView): TranslationRouteInput {
  return {
    ...routeInputFrom(route),
    name: `${route.name} copy`,
    priority: route.priority + 10,
  };
}

export function nextTranslationPriority(routes: TranslationRouteView[]): number {
  return routes.length ? Math.max(...routes.map((route) => route.priority)) + 100 : 100;
}
