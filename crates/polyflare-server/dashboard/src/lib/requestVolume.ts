export interface RequestVolumeBucket {
  ts: number;
  requests: number;
  errors: number;
}

export interface RequestVolumeSummary {
  total: number;
  errors: number;
  average: number;
  peak: number;
  latest: number;
  errorRate: number;
}

export function summarizeRequestVolume(buckets: RequestVolumeBucket[]): RequestVolumeSummary {
  const total = buckets.reduce((sum, bucket) => sum + bucket.requests, 0);
  const errors = buckets.reduce((sum, bucket) => sum + bucket.errors, 0);
  return {
    total,
    errors,
    average: buckets.length === 0 ? 0 : total / buckets.length,
    peak: buckets.reduce((max, bucket) => Math.max(max, bucket.requests), 0),
    latest: buckets.length === 0 ? 0 : buckets[buckets.length - 1].requests,
    errorRate: total === 0 ? 0 : errors / total,
  };
}

export function requestBucketErrorRate(bucket: RequestVolumeBucket): number {
  if (bucket.requests <= 0) return 0;
  return Math.max(0, Math.min(1, bucket.errors / bucket.requests));
}
