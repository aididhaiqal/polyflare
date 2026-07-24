import type { ProviderDiscoveredModelView } from "./api";

export type DiscoveryCapabilityFilter = "all" | "tools" | "vision" | "reasoning" | "free";

export function filterDiscoveredModels(
  models: ProviderDiscoveredModelView[],
  search: string,
  filter: DiscoveryCapabilityFilter,
): ProviderDiscoveredModelView[] {
  const needle = search.trim().toLowerCase();
  return models.filter((model) => {
    const matchesSearch =
      needle === "" ||
      model.upstream_model.toLowerCase().includes(needle) ||
      model.display_name.toLowerCase().includes(needle) ||
      model.suggested_public_model.toLowerCase().includes(needle);
    if (!matchesSearch) return false;
    if (filter === "tools") return model.supports_tools;
    if (filter === "vision") return model.supports_vision;
    if (filter === "reasoning") return model.supports_reasoning;
    if (filter === "free") return model.upstream_model.includes(":free");
    return true;
  });
}

export function selectableDiscoveredModelIds(
  models: ProviderDiscoveredModelView[],
): string[] {
  return models
    .filter((model) => model.state === "available")
    .map((model) => model.upstream_model);
}
