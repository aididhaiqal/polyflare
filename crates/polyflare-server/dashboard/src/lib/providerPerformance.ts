import type {
  CustomProviderView,
  ProviderPerformanceRowView,
} from "./api";

export interface ProviderModelPerformanceComparison {
  key: string;
  providerId: string;
  providerSlug: string;
  providerName: string;
  model: string;
  modelName: string;
  supportsPriority: boolean;
  hasPriorityHistory: boolean;
  standard: ProviderPerformanceRowView | null;
  priority: ProviderPerformanceRowView | null;
}

export interface PerformanceUplift {
  /** Positive means Priority generated more tokens per second. */
  tps_percent: number | null;
  /** Positive means Priority reached the first token sooner. */
  ttft_percent: number | null;
}

function percentChange(baseline: number | null, comparison: number | null): number | null {
  if (baseline === null || comparison === null || baseline <= 0) return null;
  return ((comparison - baseline) / baseline) * 100;
}

export function performanceUplift(
  standard: ProviderPerformanceRowView | null,
  priority: ProviderPerformanceRowView | null,
): PerformanceUplift {
  const standardTtft = standard?.avg_ttft_ms ?? null;
  const priorityTtft = priority?.avg_ttft_ms ?? null;
  return {
    tps_percent: percentChange(standard?.tps ?? null, priority?.tps ?? null),
    ttft_percent:
      standardTtft !== null && priorityTtft !== null && standardTtft > 0
        ? ((standardTtft - priorityTtft) / standardTtft) * 100
        : null,
  };
}

/** Join content-free performance samples onto configured custom models. Provider identity remains
 * part of the key so the same model served by multiple providers stays directly comparable rather
 * than collapsing into one misleading fleet average. */
export function buildProviderModelPerformanceComparisons(
  providers: CustomProviderView[],
  samples: ProviderPerformanceRowView[],
): ProviderModelPerformanceComparison[] {
  const byLane = new Map(
    samples.map((sample) => [
      `${sample.provider}\u0000${sample.model}\u0000${sample.tier}`,
      sample,
    ]),
  );

  return providers
    .flatMap((provider) =>
      provider.models.map((model) => {
        const standard =
          byLane.get(`${provider.slug}\u0000${model.public_model}\u0000standard`) ?? null;
        const priority =
          byLane.get(`${provider.slug}\u0000${model.public_model}\u0000priority`) ?? null;
        return {
          key: `${provider.id}:${model.id}`,
          providerId: provider.id,
          providerSlug: provider.slug,
          providerName: provider.display_name,
          model: model.public_model,
          modelName: model.display_name,
          supportsPriority: model.supports_priority_service_tier,
          hasPriorityHistory: priority !== null,
          standard,
          priority,
        };
      }),
    )
    .sort(
      (left, right) =>
        left.model.localeCompare(right.model) ||
        left.providerName.localeCompare(right.providerName),
    );
}
