// Claude usage — what Claude Code's `/usage` prints, for every Claude seat in the pool. The same
// cards appear as a section on the Analytics page; this route keeps a full-width view of them.
// Data: `GET /api/claude/usage` (read_api.rs::claude_usage_handler), from PolyFlare's own poll of
// each seat, so it is right even though Claude Code talks to Anthropic directly.
import { useEffect, useState } from "react";

import { useClaudeUsage } from "../lib/queries";
import { ClaudeSeatCard } from "../ui/ClaudeSeatCard";

export function ClaudeUsage() {
  const { data, isLoading, isError } = useClaudeUsage();
  const [nowMs, setNowMs] = useState(() => Date.now());
  useEffect(() => {
    const id = window.setInterval(() => setNowMs(Date.now()), 30_000);
    return () => window.clearInterval(id);
  }, []);

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-[15px] font-semibold text-fg">Claude usage</h1>
        <p className="mt-1 text-[12px] text-fg opacity-60">
          What <code className="rounded bg-muted px-1">/usage</code> shows in Claude Code, for every Claude seat — from
          PolyFlare's own poll of the seat, so it is right whether or not the traffic went through here.
        </p>
      </div>
      {isLoading && <p className="text-[12px] text-fg opacity-60">Loading…</p>}
      {isError && <p className="text-[12px] text-warn">Could not load Claude usage.</p>}
      {data && data.length === 0 && (
        <p className="text-[12px] text-fg opacity-60">No Claude seats are onboarded.</p>
      )}
      <div className="grid grid-cols-1 gap-3 xl:grid-cols-2">
        {data?.map((seat) => <ClaudeSeatCard key={seat.id} seat={seat} nowMs={nowMs} />)}
      </div>
    </div>
  );
}
