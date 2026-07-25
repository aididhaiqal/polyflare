// SSE client for GET /api/logs/stream (crates/polyflare-server/src/sse.rs): one JSON `LogEvent`
// (crates/polyflare-server/src/log_bus.rs) per `data:` line, backfill first then live.
//
// *** Why this is fetch()+ReadableStream and not EventSource ***
// `/api/logs/stream` sits behind `require_admin` (Bearer-token middleware — see
// crates/polyflare-server/src/auth.rs's `require_admin` + app.rs's `route_layer`), the same as
// every other `/api/*` route. The browser `EventSource` API has NO mechanism to attach a custom
// `Authorization` header (it only ever sends cookies/credentials, per the WHATWG spec) — so a bare
// `new EventSource("/api/logs/stream")` would receive a 401 from the real server whenever
// `POLYFLARE_ADMIN_TOKEN` is set (which is required for the dashboard API to be enabled at all —
// see `require_admin`'s 503-when-unset branch).
//
// Decision: reimplement the stream on `fetch()` + `Response.body`'s `ReadableStream`, with manual
// SSE-frame parsing below, because `fetch` — unlike `EventSource` — CAN set the `Authorization`
// header, so the existing Bearer-token auth stays intact and unchanged. This trades away
// `EventSource`'s native auto-reconnect, which we reimplement here with exponential backoff.
//
// Explicitly REJECTED: a short-lived query-string token (`/api/logs/stream?token=...`) accepted
// alongside the Bearer header. That would leak the admin token into the URL, browser history,
// server access logs, and any `Referer` header on outgoing requests from that page — a real
// secret-exposure regression — so it was not implemented even though `EventSource` could otherwise
// carry it.

import { useCallback, useEffect, useRef, useState } from "react";

import { getToken, notifyUnauthorized, type LogEvent } from "./api";
import { appendUniqueLogEvent } from "./liveLogFiltering";

/** Ring-buffer cap on the client side, matching the brief's "last 1000" requirement (independent
 * of the server's own ring-buffer cap in log_bus.rs — the two need not match). */
const MAX_LINES = 1000;

/** Reconnect backoff bounds: start at 1s, double each consecutive failure, cap at 30s. Only used
 * for network-level failures — never for 401 (unauthorized) or 404 (feature disabled). */
const RECONNECT_BASE_MS = 1000;
const RECONNECT_MAX_MS = 30_000;

/** Pure parser for one SSE `data:` line (or multiple `data:` lines of the same frame, already
 * joined by the caller): `JSON.parse` in a try/catch, `null` on anything that isn't valid JSON.
 * Deliberately does NOT validate the parsed shape beyond "is an object with a `kind` string" —
 * good enough to reject garbage/heartbeat comments without over-fitting to every field `LogEvent`
 * happens to carry today. */
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

/** Splits one `\n`-terminated SSE frame (everything between two `\n\n` frame separators) into the
 * concatenated payload of its `data:` line(s), per the SSE spec (multiple `data:` lines in one
 * frame are joined with `\n`). Lines starting with `:` are comments — the backend's 15s keep-alive
 * — and are ignored, as are any other SSE fields (`event:`, `id:`, `retry:`); `sse.rs` never emits
 * them today, but ignoring them defensively costs nothing. Returns `null` for a frame with no
 * `data:` line at all (e.g. a bare heartbeat comment). */
function extractFrameData(frame: string): string | null {
  const dataLines: string[] = [];
  for (const line of frame.split("\n")) {
    if (line.length === 0 || line.startsWith(":")) continue;
    if (line.startsWith("data:")) {
      dataLines.push(line.slice(5).replace(/^ /, ""));
    }
  }
  return dataLines.length > 0 ? dataLines.join("\n") : null;
}

export interface UseLogStreamOptions {
  /** Whether the stream should be connected. Flipping to `false` closes the connection (without
   * clearing already-received `lines`); flipping back to `true` reopens it. */
  enabled: boolean;
}

export interface UseLogStreamResult {
  /** Up to the last 1000 received log events, oldest first. */
  lines: LogEvent[];
  /** True while the underlying fetch stream is open and headers have been received (2xx). */
  connected: boolean;
  /** True once the server has responded 404 (live logs disabled server-side — see
   * `CapabilitiesView.live_logs` in api.ts). No further reconnect attempts are made while this is
   * set; the Live Logs page should show a "disabled" notice instead of a loading/retry spinner. */
  disabled: boolean;
  /** Closes the connection and stops auto-reconnecting until `resume()` is called. */
  pause: () => void;
  /** Reopens the connection (aborts any in-flight fetch first, then starts a fresh one). */
  resume: () => void;
  /** Clears the accumulated `lines` buffer without affecting the connection. */
  clear: () => void;
}

/** Streams `/api/logs/stream` into a capped in-memory buffer via `fetch` + `ReadableStream` (see
 * the module-doc comment above for why, in place of `EventSource`). */
export function useLogStream({ enabled }: UseLogStreamOptions): UseLogStreamResult {
  const [lines, setLines] = useState<LogEvent[]>([]);
  const [connected, setConnected] = useState(false);
  const [disabled, setDisabled] = useState(false);

  const abortControllerRef = useRef<AbortController | null>(null);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const reconnectDelayRef = useRef(RECONNECT_BASE_MS);
  /** True whenever we should NOT be connected/reconnecting: explicit `pause()`, unmount, a 401, or
   * a 404. `resume()` and the `enabled` effect are what clear it. */
  const pausedRef = useRef(false);

  const clearReconnectTimer = useCallback(() => {
    if (reconnectTimerRef.current !== null) {
      clearTimeout(reconnectTimerRef.current);
      reconnectTimerRef.current = null;
    }
  }, []);

  const abortActive = useCallback(() => {
    abortControllerRef.current?.abort();
    abortControllerRef.current = null;
  }, []);

  const appendEvent = useCallback((event: LogEvent) => {
    setLines((prev) => appendUniqueLogEvent(prev, event, MAX_LINES));
  }, []);

  const scheduleReconnect = useCallback(
    (reconnect: () => void) => {
      const delay = reconnectDelayRef.current;
      reconnectDelayRef.current = Math.min(delay * 2, RECONNECT_MAX_MS);
      clearReconnectTimer();
      reconnectTimerRef.current = setTimeout(reconnect, delay);
    },
    [clearReconnectTimer],
  );

  const connect = useCallback(async () => {
    if (pausedRef.current) return;
    clearReconnectTimer();

    const controller = new AbortController();
    abortControllerRef.current = controller;

    const headers = new Headers({ Accept: "text/event-stream" });
    const token = getToken();
    if (token) headers.set("Authorization", `Bearer ${token}`);

    let shouldReconnect = false;
    try {
      const res = await fetch("/api/logs/stream", { headers, signal: controller.signal });

      if (res.status === 401) {
        // Auth is broken (missing/expired token) — surface it via the same path fetchJson uses,
        // and stop retrying: retrying would just 401 forever.
        notifyUnauthorized();
        pausedRef.current = true;
        setConnected(false);
        return;
      }
      if (res.status === 404) {
        // Live logs are disabled server-side (see CapabilitiesView.live_logs) — not a transient
        // failure, so don't retry; let the page show a distinct "disabled" notice instead.
        pausedRef.current = true;
        setConnected(false);
        setDisabled(true);
        return;
      }
      if (!res.ok || !res.body) {
        throw new Error(`log stream request failed with status ${res.status}`);
      }

      setDisabled(false);
      setConnected(true);
      reconnectDelayRef.current = RECONNECT_BASE_MS;

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";

      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });

        let sepIndex: number;
        while ((sepIndex = buffer.indexOf("\n\n")) !== -1) {
          const frame = buffer.slice(0, sepIndex);
          buffer = buffer.slice(sepIndex + 2);
          const data = extractFrameData(frame);
          if (data === null) continue; // comment/heartbeat-only frame
          const parsed = parseLogEvent(data);
          if (parsed !== null) appendEvent(parsed);
        }
      }

      // Server closed the stream (e.g. process restart) without us aborting — treat like a
      // network drop and reconnect with backoff.
      setConnected(false);
      shouldReconnect = !pausedRef.current;
    } catch {
      if (controller.signal.aborted) {
        // Intentional abort via pause()/resume()/unmount — do not reconnect.
        return;
      }
      setConnected(false);
      shouldReconnect = !pausedRef.current;
    } finally {
      if (abortControllerRef.current === controller) abortControllerRef.current = null;
    }

    if (shouldReconnect) {
      scheduleReconnect(() => {
        void connect();
      });
    }
  }, [appendEvent, clearReconnectTimer, scheduleReconnect]);

  const pause = useCallback(() => {
    pausedRef.current = true;
    clearReconnectTimer();
    abortActive();
    setConnected(false);
  }, [abortActive, clearReconnectTimer]);

  const resume = useCallback(() => {
    pausedRef.current = false;
    reconnectDelayRef.current = RECONNECT_BASE_MS;
    setDisabled(false);
    clearReconnectTimer();
    abortActive();
    void connect();
  }, [abortActive, clearReconnectTimer, connect]);

  const clear = useCallback(() => {
    setLines([]);
  }, []);

  useEffect(() => {
    if (!enabled) {
      pausedRef.current = true;
      clearReconnectTimer();
      abortActive();
      setConnected(false);
      return;
    }
    pausedRef.current = false;
    reconnectDelayRef.current = RECONNECT_BASE_MS;
    setDisabled(false);
    void connect();
    return () => {
      pausedRef.current = true;
      clearReconnectTimer();
      abortActive();
    };
    // Intentionally re-run only when `enabled` changes: `connect`/`abortActive`/`clearReconnectTimer`
    // are stable (useCallback with stable deps), and re-running on every render would thrash the
    // connection.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [enabled]);

  return { lines, connected, disabled, pause, resume, clear };
}
