// The Transport page: per-thread WebSocket->HTTP-SSE pins (`GET/POST/DELETE /api/ws/sse-pins`).
//
// `GET /responses` either upgrades to a WebSocket or answers `426 Upgrade Required`, and a 426 is
// codex-rs's SOLE WS->HTTP fallback trigger. A pin makes that decision per THREAD, so one stuck
// conversation can be moved to HTTP-SSE without downgrading the whole proxy.
//
// Two things this page has to say out loud, because both cost real debugging time when they were
// only implicit:
//
//   1. The field is the THREAD id, never the session id. Codex sends `session-id` and `thread-id`
//      as separate headers and one session can carry several threads; matching on the session id
//      diverted every thread underneath it. The gate compared the wrong one until 2026-07-27, so a
//      pin entered here did nothing at all. A UI labelled "session" would reintroduce that at the
//      human layer, which is why the label, the placeholder, and the help text all say thread.
//
//   2. A pin takes effect at the NEXT handshake, not on the current turn. Codex picks its transport
//      when it opens a connection, so a thread already streaming keeps its current transport until
//      it reconnects. Measured live: an unpin returned 200 immediately and the thread stayed on SSE
//      for a further ~90s and six turns before flipping to WebSocket. Without this stated, the
//      first person to use it reasonably concludes it is broken.
import { useState } from "react";

import { useAddSsePin, useRemoveSsePin, useSsePins } from "../lib/queries";
import { Card } from "../ui/Card";
import { Col, Grid } from "../ui/Grid";
import { AlertTriangle, Plus, X } from "../ui/icons";

/** Mirrors `sse_pins::is_valid_thread_id` so the obvious mistakes are caught before a round-trip.
 * The server remains the authority — this only spares the user a 400 for an empty or clearly
 * malformed value. */
function looksLikeThreadId(id: string): boolean {
  return id.length > 0 && id.length <= 128 && /^[A-Za-z0-9_-]+$/.test(id);
}

export function Transport() {
  const pins = useSsePins();
  const addPin = useAddSsePin();
  const removePin = useRemoveSsePin();
  const [draft, setDraft] = useState("");

  const trimmed = draft.trim();
  const valid = looksLikeThreadId(trimmed);
  const serverError =
    addPin.error instanceof Error ? addPin.error.message : null;

  function submit(event: React.FormEvent) {
    event.preventDefault();
    if (!valid || addPin.isPending) return;
    addPin.mutate(trimmed, { onSuccess: () => setDraft("") });
  }

  const pinned = pins.data?.pinned_threads ?? [];

  return (
    <Grid>
      <Col span={12}>
        <Card>
          <div className="mb-3">
            <h2 className="text-[12px] font-medium text-fg">HTTP-SSE pins</h2>
            <p className="text-[10px] text-fg opacity-60">
              Force one conversation onto HTTP-SSE; every other thread keeps WebSocket
            </p>
          </div>
          <p className="mb-3 text-[11px] leading-relaxed text-fg opacity-70">
            A pinned thread is answered <code className="font-mono">426</code> at the WebSocket
            handshake, which is the only signal codex-rs treats as a WebSocket-to-HTTP fallback. Use
            this to route one thread around a transport-specific failure without downgrading the
            proxy for everything else.
          </p>

          <form onSubmit={submit} className="flex flex-wrap items-start gap-2">
            <label className="flex-1 min-w-[18rem]">
              <span className="mb-1 block text-[9px] font-medium uppercase tracking-wide text-fg opacity-60">
                Thread id
              </span>
              <input
                value={draft}
                onChange={(event) => setDraft(event.target.value)}
                placeholder="019f96f4-d4e8-7751-87c9-beba24bb3330"
                spellCheck={false}
                autoComplete="off"
                className="w-full rounded border border-border bg-bg px-2 py-1.5 font-mono text-[11px] text-fg outline-none focus:border-accent"
              />
              <span className="mt-1 block text-[10px] text-fg opacity-60">
                The thread id, not the session id — one session can hold several threads, and
                pinning a session id would move all of them.
              </span>
            </label>
            <button
              type="submit"
              disabled={!valid || addPin.isPending}
              className="mt-[1.15rem] inline-flex items-center gap-1 rounded border border-border px-2 py-1.5 text-[11px] text-fg transition hover:border-accent disabled:cursor-not-allowed disabled:opacity-40"
            >
              <Plus className="h-3 w-3" />
              {addPin.isPending ? "Pinning…" : "Pin to SSE"}
            </button>
          </form>

          {trimmed.length > 0 && !valid ? (
            <p className="mt-2 text-[10px] text-error">
              A thread id is 1–128 characters of letters, digits, <code>-</code> or <code>_</code>.
            </p>
          ) : null}
          {serverError ? (
            <p className="mt-2 text-[10px] text-error">{serverError}</p>
          ) : null}

          <div className="mt-3 flex items-start gap-1.5 rounded border border-border bg-bg px-2 py-1.5">
            <AlertTriangle className="mt-[1px] h-3 w-3 shrink-0 text-warn" />
            <p className="text-[10px] leading-relaxed text-fg opacity-70">
              Takes effect at the thread's <strong>next connection</strong>, not on the turn in
              flight. Codex chooses its transport when it opens a connection, so a thread that is
              mid-conversation keeps its current transport until it reconnects — pinning and
              unpinning both look like nothing happened for a minute or two.
            </p>
          </div>
        </Card>
      </Col>

      <Col span={12}>
        <Card>
          <div className="mb-3 flex items-baseline justify-between gap-2">
            <h2 className="text-[12px] font-medium text-fg">Pinned threads</h2>
            <span className="text-[10px] text-fg opacity-60">{pinned.length} pinned</span>
          </div>
          {pins.isLoading ? (
            <p className="text-[11px] text-fg opacity-60">Loading…</p>
          ) : pinned.length === 0 ? (
            <p className="text-[11px] text-fg opacity-60">
              No pinned threads — every conversation is free to use WebSocket.
            </p>
          ) : (
            <ul className="divide-y divide-border">
              {pinned.map((threadId) => (
                <li
                  key={threadId}
                  className="flex items-center justify-between gap-2 py-1.5"
                >
                  <code className="truncate font-mono text-[11px] text-fg">{threadId}</code>
                  <button
                    type="button"
                    onClick={() => removePin.mutate(threadId)}
                    disabled={removePin.isPending}
                    title="Return this thread to WebSocket"
                    className="inline-flex items-center gap-1 rounded border border-border px-1.5 py-1 text-[10px] text-fg transition hover:border-accent disabled:cursor-not-allowed disabled:opacity-40"
                  >
                    <X className="h-3 w-3" />
                    Unpin
                  </button>
                </li>
              ))}
            </ul>
          )}
        </Card>
      </Col>
    </Grid>
  );
}
