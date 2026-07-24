export const MODEL_TRAFFIC_PROVIDER = "model";
export const BACKEND_TRAFFIC_PROVIDER = "chatgpt_backend";
export const NO_PROVIDER = "none";

function normalizeProvider(provider: string): string {
  return provider === "claude" ? "anthropic" : provider;
}

export function parseProviderSelection(raw: string | null): string[] {
  if (raw === null || raw.trim() === "") return [MODEL_TRAFFIC_PROVIDER];
  const values = raw
    .split(",")
    .map((value) => normalizeProvider(value.trim()))
    .filter(Boolean);
  return [...new Set(values.length > 0 ? values : [MODEL_TRAFFIC_PROVIDER])];
}

export function resolveProviderSelection(
  raw: string | null,
  modelProviders: string[],
  allProviders: string[],
): string[] {
  const parsed = parseProviderSelection(raw);
  if (parsed.includes("all")) return [...allProviders];
  if (parsed.includes(NO_PROVIDER)) return [];

  const resolved = new Set<string>();
  if (parsed.includes(MODEL_TRAFFIC_PROVIDER)) {
    for (const provider of modelProviders) resolved.add(provider);
  }
  for (const provider of parsed) {
    if (provider !== MODEL_TRAFFIC_PROVIDER) resolved.add(provider);
  }
  return allProviders.filter((provider) => resolved.has(provider));
}

export function serializeProviderSelection(
  selected: string[],
  modelProviders: string[],
  allProviders: string[],
): string | null {
  const chosen = new Set(selected);
  const isDefault =
    chosen.size === modelProviders.length &&
    modelProviders.every((provider) => chosen.has(provider));
  if (isDefault) return null;
  if (chosen.size === 0) return NO_PROVIDER;

  const ordered = allProviders.filter((provider) => chosen.has(provider));
  return ordered.join(",");
}

export function providerSelectionLabel(
  selected: string[],
  modelProviders: string[],
  allProviders: string[],
): string {
  if (selected.length === 0) return "None";
  if (selected.length === allProviders.length) return "All";
  if (
    selected.length === modelProviders.length &&
    modelProviders.every((provider) => selected.includes(provider))
  ) {
    return "Models";
  }
  if (selected.length === 1) return selected[0] === BACKEND_TRAFFIC_PROVIDER ? "Backend" : selected[0];
  return `${selected.length} selected`;
}
