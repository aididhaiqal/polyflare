import type { TranslationRouteInput, TranslationRouteView } from "./api";

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
