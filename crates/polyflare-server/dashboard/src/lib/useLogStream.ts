// SSE client for GET /api/logs/stream (crates/polyflare-server/src/sse.rs): one JSON `LogEvent`
// (crates/polyflare-server/src/log_bus.rs) per `data:` line, backfill first then live.
//
// *** CROSS-TASK ISSUE — read before wiring this into the Live Logs page ***
// `/api/logs/stream` sits behind `require_admin` (Bearer-token middleware — see
// crates/polyflare-server/src/auth.rs's `require_admin` + app.rs's `route_layer`), the same as
// every other `/api/*` route. But the browser `EventSource` API has NO mechanism to attach a
// custom `Authorization` header (it only ever sends cookies/credentials, per the WHATWG spec) —
// so a bare `new EventSource("/api/logs/stream")`, as built below, WILL receive a 401 from the
// real server whenever `POLYFLARE_ADMIN_TOKEN` is set (which is required for the dashboard API to
// be enabled at all — see `require_admin`'s 503-when-unset branch). `EventSource.onerror` fires
// for that 401 with no status code exposed to JS, indistinguishable here from a network blip, so
// this hook will just keep retrying with backoff forever rather than surfacing "unauthorized".
// This task deliberately does NOT change backend code to work around it. Options for the Live Logs
// page task to pick from: (a) add a short-lived query-string token this one route accepts
// (`?token=...`) alongside the Bearer header, (b) replace `EventSource` with `fetch()` +
// `ReadableStream` parsing (supports headers; loses native auto-reconnect, must reimplement), or
// (c) a server-set session cookie from a login endpoint that `EventSource`'s cookie jar would
// carry automatically. Flagged, not fixed, here.

import { useCallback, useEffect, useRef, useState } from "react";

import { type LogEvent } from "./api";

/** Ring-buffer cap on the client side, matching the brief's "last 1000" requirement (independent
 * of the server's own ring-buffer cap in log_bus.rs — the two need not match). */
const MAX_LINES = 1000;

/** Reconnect backoff bounds: start at 1s, double each consecutive failure, cap at 30s. */
const RECONNECT_BASE_MS = 1000;
const RECONNECT_MAX_MS = 30_000;

/** Pure parser for one SSE `data:` line: `JSON.parse` in a try/catch, `null` on anything that
 * isn't valid JSON. Deliberately does NOT validate the parsed shape beyond "is an object with a
 * `kind` string" — good enough to reject garbage/heartbeat comments without over-fitting to every
 * field `LogEvent` happens to carry today. */
export function parseLogEvent(data: string): LogEvent | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(data);
  } catch {
    return null;
  }
  if (
    typeof parsed === "object" &&
    parsed !== null &&
    typeof (parsed as { kind?: unknown }).kind === "string" &&
    typeof (parsed as { message?: unknown }).message === "string"
  ) {
    return parsed as LogEvent;
  }
  return null;
}

export interface UseLogStreamOptions {
  /** Whether the stream should be connected. Flipping to `false` closes the connection (without
   * clearing already-received `lines`); flipping back to `true` reopens it. */
  enabled: boolean;
}

export interface UseLogStreamResult {
  /** Up to the last 1000 received log events, oldest first. */
  lines: LogEvent[];
  /** True while the underlying EventSource is open and has fired `onopen`. */
  connected: boolean;
  /** Closes the connection and stops auto-reconnecting until `resume()` is called. */
  pause: () => void;
  /** Reopens the connection (a no-op if already open). */
  resume: () => void;
  /** Clears the accumulated `lines` buffer without affecting the connection. */
  clear: () => void;
}

/** Streams `/api/logs/stream` into a capped in-memory buffer. See the module-doc comment above for
 * the EventSource-can't-send-Authorization limitation this hook does not attempt to work around. */
export function useLogStream({ enabled }: UseLogStreamOptions): UseLogStreamResult {
  const [lines, setLines] = useState<LogEvent[]>([]);
  const [connected, setConnected] = useState(false);

  const esRef = useRef<EventSource | null>(null);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const reconnectDelayRef = useRef(RECONNECT_BASE_MS);
  const pausedRef = useRef(false);

  const clearReconnectTimer = useCallback(() => {
    if (reconnectTimerRef.current !== null) {
      clearTimeout(reconnectTimerRef.current);
      reconnectTimerRef.current = null;
    }
  }, []);

  const closeSource = useCallback(() => {
    clearReconnectTimer();
    esRef.current?.close();
    esRef.current = null;
    setConnected(false);
  }, [clearReconnectTimer]);

  const openSource = useCallback(() => {
    if (pausedRef.current) return;
    clearReconnectTimer();

    const es = new EventSource("/api/logs/stream");
    esRef.current = es;

    es.onopen = () => {
      reconnectDelayRef.current = RECONNECT_BASE_MS;
      setConnected(true);
    };

    es.onmessage = (ev: MessageEvent<string>) => {
      const parsed = parseLogEvent(ev.data);
      if (parsed === null) return;
      setLines((prev) => {
        const next = prev.length >= MAX_LINES ? prev.slice(prev.length - MAX_LINES + 1) : prev.slice();
        next.push(parsed);
        return next;
      });
    };

    // The native EventSource auto-reconnect exists, but we manage it ourselves so we can apply
    // exponential backoff (the browser default is a fixed, short retry interval that would hammer
    // the server if it's down, or — see the module-doc limitation above — perpetually 401ing).
    es.onerror = () => {
      setConnected(false);
      es.close();
      if (esRef.current === es) esRef.current = null;
      if (pausedRef.current) return;

      const delay = reconnectDelayRef.current;
      reconnectDelayRef.current = Math.min(delay * 2, RECONNECT_MAX_MS);
      reconnectTimerRef.current = setTimeout(() => {
        openSource();
      }, delay);
    };
  }, [clearReconnectTimer]);

  const pause = useCallback(() => {
    pausedRef.current = true;
    closeSource();
  }, [closeSource]);

  const resume = useCallback(() => {
    pausedRef.current = false;
    reconnectDelayRef.current = RECONNECT_BASE_MS;
    closeSource();
    openSource();
  }, [closeSource, openSource]);

  const clear = useCallback(() => {
    setLines([]);
  }, []);

  useEffect(() => {
    if (!enabled) {
      closeSource();
      return;
    }
    pausedRef.current = false;
    reconnectDelayRef.current = RECONNECT_BASE_MS;
    openSource();
    return () => {
      closeSource();
    };
    // Intentionally re-run only when `enabled` changes: `openSource`/`closeSource` are stable
    // (useCallback with stable deps), and re-running on every render would thrash the connection.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [enabled]);

  return { lines, connected, pause, resume, clear };
}
