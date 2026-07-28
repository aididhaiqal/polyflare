export interface LiveActivityBucket {
  ts: number;
  requests: number;
}

export type TrafficSignalState =
  | "steady"
  | "surge"
  | "new_burst"
  | "insufficient_history";

export interface TrafficSignal {
  state: TrafficSignalState;
  recentRequests: number;
  baselineRequests: number;
  ratio: number | null;
  latestCompletedTs: number | null;
}

const BUCKET_SECONDS = 60;
const RECENT_BUCKETS = 5;
const BASELINE_WINDOWS = 6;
const BUCKETS_PER_BASELINE_WINDOW = 5;
const REQUIRED_COMPLETED_BUCKETS =
  RECENT_BUCKETS + BASELINE_WINDOWS * BUCKETS_PER_BASELINE_WINDOW;

function median(values: number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0
    ? (sorted[middle - 1] + sorted[middle]) / 2
    : sorted[middle];
}

/**
 * Compares the latest five completed minutes with a robust 30-minute baseline. The current minute
 * from the fetched response is always excluded because request rows can arrive before their
 * terminal usage update. The explicit boundary stays correct if the browser clock crosses into a
 * new minute before the next report refresh.
 */
export function analyzeTrafficSignal(
  buckets: LiveActivityBucket[],
  settlingBucketTs: number,
): TrafficSignal {
  const completed = [...buckets]
    .filter((bucket) => bucket.ts < settlingBucketTs)
    .sort((a, b) => a.ts - b.ts)
    .slice(-REQUIRED_COMPLETED_BUCKETS);
  const latestCompletedTs =
    completed.length > 0 ? completed[completed.length - 1].ts : null;
  const recentRequests = completed
    .slice(-RECENT_BUCKETS)
    .reduce((sum, bucket) => sum + Math.max(0, bucket.requests), 0);

  const contiguous =
    completed.length === REQUIRED_COMPLETED_BUCKETS &&
    completed.every(
      (bucket, index) =>
        index === 0 || bucket.ts - completed[index - 1].ts === BUCKET_SECONDS,
    );
  if (!contiguous) {
    return {
      state: "insufficient_history",
      recentRequests,
      baselineRequests: 0,
      ratio: null,
      latestCompletedTs,
    };
  }

  const baselineBuckets = completed.slice(0, -RECENT_BUCKETS);
  const baselineWindows = Array.from({ length: BASELINE_WINDOWS }, (_, index) =>
    baselineBuckets
      .slice(
        index * BUCKETS_PER_BASELINE_WINDOW,
        (index + 1) * BUCKETS_PER_BASELINE_WINDOW,
      )
      .reduce((sum, bucket) => sum + Math.max(0, bucket.requests), 0),
  );
  const baselineRequests = median(baselineWindows);
  const ratio = baselineRequests > 0 ? recentRequests / baselineRequests : null;

  if (baselineRequests < 1) {
    return {
      state: recentRequests >= 6 ? "new_burst" : "steady",
      recentRequests,
      baselineRequests,
      ratio,
      latestCompletedTs,
    };
  }

  return {
    state:
      ratio !== null && ratio >= 2 && recentRequests - baselineRequests >= 3
        ? "surge"
        : "steady",
    recentRequests,
    baselineRequests,
    ratio,
    latestCompletedTs,
  };
}
