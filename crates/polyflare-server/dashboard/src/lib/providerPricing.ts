export interface ProviderPricing {
  input_per_million: number | null;
  cached_input_per_million: number | null;
  output_per_million: number | null;
}

export function formatProviderPrice(value: number | null): string {
  if (value === null || !Number.isFinite(value) || value < 0) return "—";
  return `$${value.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 4,
  })}`;
}

export function providerPricingSummary(pricing: ProviderPricing): string {
  return [
    `Input ${formatProviderPrice(pricing.input_per_million)}`,
    `Cache ${formatProviderPrice(pricing.cached_input_per_million)}`,
    `Output ${formatProviderPrice(pricing.output_per_million)}`,
  ].join(" · ");
}
