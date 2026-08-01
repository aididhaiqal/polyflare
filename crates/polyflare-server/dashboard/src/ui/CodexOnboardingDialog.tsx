import { useRef, useState } from "react";

import { ApiError } from "../lib/api";
import { useCompleteCodexOnboarding, useStartCodexOnboarding } from "../lib/queries";
import { Copy, LogIn, X } from "./icons";
import { useDialogA11y } from "./useDialogA11y";

/** Targets the flow at one existing account: the server refuses any other seat at completion. */
export interface ReauthTarget {
  accountId: string;
  label: string;
  email?: string | null;
}

function completeErrorText(error: unknown, reauth?: ReauthTarget): string {
  const code =
    error instanceof ApiError && typeof error.body === "object" && error.body !== null
      ? (error.body as { error?: unknown }).error
      : undefined;
  if (code === "seat_mismatch" && reauth) {
    return `That sign-in used a different ChatGPT account than ${reauth.label}${
      reauth.email ? ` (${reauth.email})` : ""
    }. Nothing was changed — start over and sign in with the matching account.`;
  }
  if (code === "intended_account_missing") {
    return "The account this flow was repairing no longer exists. Close this dialog and re-check the accounts list.";
  }
  return "That callback could not be completed. For security, start a new sign-in before retrying if the exchange was already attempted.";
}

export function CodexOnboardingDialog({
  open,
  onOpenChange,
  reauth,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  reauth?: ReauthTarget;
}) {
  const dialogRef = useRef<HTMLDivElement>(null);
  const closeRef = useRef<HTMLButtonElement>(null);
  const [pool, setPool] = useState("");
  const [callbackUrl, setCallbackUrl] = useState("");
  const start = useStartCodexOnboarding();
  const complete = useCompleteCodexOnboarding();
  useDialogA11y(open, () => onOpenChange(false), dialogRef, closeRef);

  if (!open) return null;
  const flow = start.data;
  const busy = start.isPending || complete.isPending;
  const title = reauth ? "Re-authenticate account" : "Add Codex account";
  const close = () => {
    if (!busy) onOpenChange(false);
  };
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/55 p-4" onClick={close}>
      <div ref={dialogRef} tabIndex={-1} role="dialog" aria-modal="true" aria-label={title} onClick={(e) => e.stopPropagation()} className="w-full max-w-lg rounded-lg border border-border bg-card p-4 text-fg shadow-xl outline-none">
        <div className="flex items-start justify-between gap-3">
          <div><h2 className="text-sm font-semibold">{title}</h2><p className="mt-1 text-[11px] opacity-60">Credentials stay encrypted on this PolyFlare server.</p></div>
          <button ref={closeRef} type="button" onClick={close} disabled={busy} aria-label="Close" className="rounded p-1 opacity-55 hover:bg-muted hover:opacity-100"><X className="h-4 w-4" /></button>
        </div>
        {!flow ? (
          <div className="mt-4 space-y-4">
            {reauth ? (
              <p className="rounded border border-warn/40 bg-warn/10 p-3 text-[11px] leading-relaxed">
                This repairs <b>{reauth.label}</b>
                {reauth.email && (
                  <>
                    {" "}
                    (<span className="font-mono">{reauth.email}</span>)
                  </>
                )}
                . Sign in with that exact ChatGPT account — a different account will be refused and
                nothing will change.
              </p>
            ) : (
              <label className="block text-[11px] font-medium">Initial routing group <span className="font-normal opacity-50">(optional)</span><input value={pool} onChange={(e) => setPool(e.target.value)} placeholder="team-a" className="mt-1.5 w-full rounded border border-border bg-bg px-3 py-2 font-mono text-[12px] outline-none focus:border-accent" /></label>
            )}
            <p className="rounded border border-border bg-muted/45 p-3 text-[11px] leading-relaxed opacity-75">OpenAI redirects to <code className="font-mono">localhost:1455</code>. If no local listener is running, copy the final URL from your browser address bar and paste it here.</p>
            {start.isError && <p className="text-[11px] text-error">{reauth ? "Could not begin OAuth. The account may no longer exist — refresh and try again." : "Could not begin OAuth. Check the routing-group slug and try again."}</p>}
            <div className="flex justify-end"><button type="button" disabled={busy} onClick={() => start.mutate(reauth ? { accountId: reauth.accountId } : { initialPool: pool.trim() || undefined })} className="flex items-center gap-1.5 rounded bg-accent px-3 py-2 text-[12px] font-semibold text-white disabled:opacity-45"><LogIn className="h-3.5 w-3.5" />Begin secure sign-in</button></div>
          </div>
        ) : complete.isSuccess ? (
          <div className="mt-5 rounded border border-success/35 bg-success/10 p-4"><p className="text-sm font-semibold text-success">{reauth ? "Account re-authenticated" : "Account connected"}</p><p className="mt-1 text-[11px] opacity-65">{reauth ? "Fresh credentials are stored and the account is back in rotation." : "The account is ready for routing."}</p><button type="button" onClick={() => onOpenChange(false)} className="mt-4 rounded bg-accent px-3 py-1.5 text-[12px] font-semibold text-white">Done</button></div>
        ) : (
          <div className="mt-4 space-y-4">
            <div className="rounded border border-border bg-muted/40 p-3"><p className="text-[11px] font-semibold">1. Sign in with OpenAI{reauth?.email && <span className="font-normal opacity-60"> — as {reauth.email}</span>}</p><div className="mt-2 flex flex-wrap gap-2"><a href={flow.authorize_url} target="_blank" rel="noreferrer" className="rounded bg-accent px-3 py-1.5 text-[12px] font-semibold text-white">Open sign-in page</a><button type="button" onClick={() => navigator.clipboard.writeText(flow.authorize_url)} className="flex items-center gap-1.5 rounded border border-border px-3 py-1.5 text-[12px]"><Copy className="h-3.5 w-3.5" />Copy URL</button></div></div>
            <label className="block text-[11px] font-semibold">2. Paste the final redirect URL<textarea rows={3} value={callbackUrl} onChange={(e) => setCallbackUrl(e.target.value)} placeholder="http://localhost:1455/auth/callback?code=...&state=..." className="mt-2 w-full resize-y rounded border border-border bg-bg px-3 py-2 font-mono text-[11px] outline-none focus:border-accent" /></label>
            {complete.isError && <p className="text-[11px] text-error">{completeErrorText(complete.error, reauth)}</p>}
            <div className="flex justify-end gap-2"><button type="button" disabled={busy} onClick={() => { start.reset(); complete.reset(); setCallbackUrl(""); }} className="rounded border border-border px-3 py-1.5 text-[12px]">Start over</button><button type="button" disabled={busy || !callbackUrl.trim()} onClick={() => complete.mutate({ flowId: flow.flow_id, callbackUrl: callbackUrl.trim() })} className="rounded bg-accent px-3 py-1.5 text-[12px] font-semibold text-white disabled:opacity-45">{complete.isPending ? "Connecting…" : "Complete connection"}</button></div>
          </div>
        )}
      </div>
    </div>
  );
}
