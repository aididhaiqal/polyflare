import type { LogEvent } from "./api";

export const REQUEST_POLL_MS = 30_000;

export function requestRefreshInterval(sseConnected: boolean): number | false {
  return sseConnected ? false : REQUEST_POLL_MS;
}

export function requestLiveLabel({
  sseConnected,
  isFetching,
}: {
  sseConnected: boolean;
  isFetching: boolean;
}): string {
  if (sseConnected) return isFetching ? "Live · SSE updating…" : "Live · SSE";
  return isFetching ? "Fallback · refreshing…" : `Fallback · polling ${REQUEST_POLL_MS / 1000}s`;
}

/** Stable identity for the newest request-completion event in an SSE buffer. Non-request
 * operational events must not refresh the Requests query. */
export function latestRequestEventKey(lines: LogEvent[]): string | null {
  for (let i = lines.length - 1; i >= 0; i -= 1) {
    const event = lines[i];
    if (event.kind !== "request" && event.kind !== "request_finalized") continue;
    return [
      event.request_id ?? "",
      event.session_key ?? "",
      event.ts_ms,
      event.status ?? "",
      event.account ?? "",
      event.provider ?? "",
    ].join("|");
  }
  return null;
}
