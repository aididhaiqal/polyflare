import type { LogEvent } from "./api";

const BACKEND_TRAFFIC_PROVIDER = "chatgpt_backend";

function normalizedProvider(provider: string): string {
  return provider === "claude" ? "anthropic" : provider;
}

export function logEventKey(ev: LogEvent): string {
  return [
    ev.ts_ms,
    ev.level,
    ev.kind,
    ev.message,
    ev.status,
    ev.latency_ms,
    ev.account,
    ev.target_kind,
    ev.target_id,
    ev.provider,
    ev.model,
    ev.subagent,
    ev.request_id,
    ev.session_key,
  ].join("|");
}

/**
 * Adds one event to the client ring while suppressing exact replays from the server's reconnect
 * backfill. Request events include PolyFlare's unique correlation id, so distinct completions
 * remain distinct even when all their other visible fields and timestamps happen to match.
 */
export function appendUniqueLogEvent(
  previous: LogEvent[],
  event: LogEvent,
  capacity: number,
): LogEvent[] {
  if (capacity <= 0) return [];
  const eventKey = logEventKey(event);
  if (previous.some((candidate) => logEventKey(candidate) === eventKey)) return previous;
  const keep = Math.max(0, capacity - 1);
  const next = previous.length > keep ? previous.slice(previous.length - keep) : previous.slice();
  next.push(event);
  return next;
}

/**
 * Provider-less operational events remain visible regardless of the traffic filter. Request
 * events use the provider identity emitted by the current server, where ChatGPT backend traffic
 * is explicitly tagged `chatgpt_backend`.
 */
export function logEventMatchesProviders(ev: LogEvent, selectedProviders: string[]): boolean {
  if (ev.provider === undefined) return true;
  const provider = normalizedProvider(ev.provider);
  if (provider === BACKEND_TRAFFIC_PROVIDER) {
    return selectedProviders.includes(BACKEND_TRAFFIC_PROVIDER);
  }
  return selectedProviders.includes(provider);
}
