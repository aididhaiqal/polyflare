import { useMemo, useRef, useState } from "react";
import clsx from "clsx";

import { ApiError } from "../lib/api";
import {
  useCompleteCodexOnboarding,
  useOnboardingFlowStatus,
  useStartCodexDeviceOnboarding,
  useStartCodexOnboarding,
} from "../lib/queries";
import { Copy, LogIn, X } from "./icons";
import { useDialogA11y } from "./useDialogA11y";

/** Targets the flow at one existing account: the server refuses any other seat at completion. */
export interface ReauthTarget {
  accountId: string;
  label: string;
  email?: string | null;
}

type Method = "browser" | "device";

/** The browser method's loopback redirect only reaches this server when the browser runs on the
 * SAME machine. A dashboard opened remotely should use the device code instead. */
function isSameMachineAsServer(): boolean {
  return ["localhost", "127.0.0.1", "[::1]"].includes(window.location.hostname);
}

function flowErrorText(code: string | undefined, reauth?: ReauthTarget): string {
  switch (code) {
    case "seat_mismatch":
      return reauth
        ? `That sign-in used a different ChatGPT account than ${reauth.label}${
            reauth.email ? ` (${reauth.email})` : ""
          }. Nothing was changed — start over and sign in with the matching account.`
        : "That sign-in used an unexpected ChatGPT account. Nothing was changed.";
    case "intended_account_missing":
      return "The account this flow was repairing no longer exists. Close this dialog and re-check the accounts list.";
    case "device_auth_unavailable":
      return "This auth server does not offer device-code login. Use the browser sign-in instead.";
    case "device_auth_denied":
      return "The device sign-in was refused or cancelled at the verification page.";
    case "authorize_denied":
      return "The authorization was cancelled or refused in the browser.";
    case "flow_expired":
      return "This sign-in expired. Start a new one.";
    default:
      return "The sign-in could not be completed. Start a new one.";
  }
}

function completeErrorText(error: unknown, reauth?: ReauthTarget): string {
  const code =
    error instanceof ApiError && typeof error.body === "object" && error.body !== null
      ? (error.body as { error?: unknown }).error
      : undefined;
  if (typeof code === "string" && code !== "exchange_failed") return flowErrorText(code, reauth);
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
  const sameMachine = useMemo(isSameMachineAsServer, []);
  const [method, setMethod] = useState<Method>(sameMachine ? "browser" : "device");
  // Anthropic has no device endpoint and no loopback redirect: its reviewed contract redirects to
  // a page on claude.com that DISPLAYS a code. So choosing Claude forces the browser+paste path
  // rather than offering options that cannot work.
  const [provider, setProvider] = useState<"codex" | "anthropic">("codex");
  const isClaude = provider === "anthropic";
  const start = useStartCodexOnboarding();
  const startDevice = useStartCodexDeviceOnboarding();
  const complete = useCompleteCodexOnboarding();
  const browserFlow = start.data ?? null;
  const deviceFlow = startDevice.data ?? null;
  const activeFlowId = isClaude || method === "browser" ? browserFlow?.flow_id : deviceFlow?.flow_id;
  const flowStatus = useOnboardingFlowStatus(activeFlowId ?? null);
  useDialogA11y(open, () => onOpenChange(false), dialogRef, closeRef);

  if (!open) return null;
  const busy = start.isPending || startDevice.isPending || complete.isPending;
  const title = reauth
    ? "Re-authenticate account"
    : isClaude
      ? "Add Claude account"
      : "Add Codex account";
  const succeeded = complete.isSuccess || flowStatus.data?.status === "completed";
  const flowFailed =
    !succeeded &&
    (flowStatus.data?.status === "failed" || flowStatus.data?.status === "expired");
  const started = isClaude || method === "browser" ? browserFlow !== null : deviceFlow !== null;
  const close = () => {
    if (!busy) onOpenChange(false);
  };
  const resetAll = () => {
    start.reset();
    startDevice.reset();
    complete.reset();
    setCallbackUrl("");
  };
  const startFlow = () => {
    const opts = reauth
      ? { accountId: reauth.accountId }
      : { initialPool: pool.trim() || undefined };
    if (isClaude) start.mutate({ ...opts, provider: "anthropic" });
    else if (method === "browser") start.mutate(opts);
    else startDevice.mutate(opts);
  };
  const methodOption = (value: Method, label: string, hint: string, recommended: boolean) => (
    <button
      type="button"
      onClick={() => setMethod(value)}
      className={clsx(
        "flex-1 rounded-lg border p-3 text-left",
        method === value ? "border-accent bg-accent/[0.07]" : "border-border hover:border-accent/50",
      )}
    >
      <span className="flex items-center gap-2 text-[12px] font-semibold text-fg">
        {label}
        {recommended && (
          <span className="rounded-full bg-accent/[0.12] px-1.5 py-0.5 text-[9.5px] font-semibold text-accent">
            Recommended
          </span>
        )}
      </span>
      <span className="mt-1 block text-[11px] leading-relaxed text-fg opacity-60">{hint}</span>
    </button>
  );

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/55 p-4" onClick={close}>
      <div ref={dialogRef} tabIndex={-1} role="dialog" aria-modal="true" aria-label={title} onClick={(e) => e.stopPropagation()} className="w-full max-w-lg rounded-lg border border-border bg-card p-4 text-fg shadow-xl outline-none">
        <div className="flex items-start justify-between gap-3">
          <div><h2 className="text-sm font-semibold">{title}</h2><p className="mt-1 text-[11px] opacity-60">Credentials stay encrypted on this PolyFlare server.</p></div>
          <button ref={closeRef} type="button" onClick={close} disabled={busy} aria-label="Close" className="rounded p-1 opacity-55 hover:bg-muted hover:opacity-100"><X className="h-4 w-4" /></button>
        </div>
        {succeeded ? (
          <div className="mt-5 rounded border border-success/35 bg-success/10 p-4"><p className="text-sm font-semibold text-success">{reauth ? "Account re-authenticated" : "Account connected"}</p><p className="mt-1 text-[11px] opacity-65">{reauth ? "Fresh credentials are stored and the account is back in rotation." : "The account is ready for routing."}</p><button type="button" onClick={() => onOpenChange(false)} className="mt-4 rounded bg-accent px-3 py-1.5 text-[12px] font-semibold text-white">Done</button></div>
        ) : !started ? (
          <div className="mt-4 space-y-4">
            {!reauth && (
              <div className="flex gap-2">
                {(["codex", "anthropic"] as const).map((value) => (
                  <button
                    key={value}
                    type="button"
                    onClick={() => setProvider(value)}
                    className={clsx(
                      "flex-1 rounded-lg border p-3 text-left",
                      provider === value
                        ? "border-accent bg-accent/[0.07]"
                        : "border-border hover:border-accent/50",
                    )}
                  >
                    <div className="text-[12px] font-semibold">
                      {value === "codex" ? "Codex" : "Claude"}
                    </div>
                    <div className="mt-0.5 text-[10.5px] opacity-60">
                      {value === "codex"
                        ? "ChatGPT / Codex subscription seat."
                        : "Claude subscription seat. Sign in, then paste the code shown."}
                    </div>
                  </button>
                ))}
              </div>
            )}
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
            <div className={clsx("flex gap-2", isClaude && "hidden")}>
              {methodOption(
                "browser",
                "Browser sign-in",
                sameMachine
                  ? "You're on the same machine as PolyFlare — the sign-in completes automatically."
                  : "Only works when the browser runs on the PolyFlare machine itself.",
                sameMachine,
              )}
              {methodOption(
                "device",
                "Device code",
                sameMachine
                  ? "Enter a short code at OpenAI's verification page — no redirect needed."
                  : "You're viewing this dashboard remotely — enter a short code from any device.",
                !sameMachine,
              )}
            </div>
            {(start.isError || startDevice.isError) && (
              <p className="text-[11px] text-error">
                {startDevice.isError
                  ? completeErrorText(startDevice.error, reauth)
                  : reauth
                    ? "Could not begin OAuth. The account may no longer exist — refresh and try again."
                    : "Could not begin OAuth. Check the routing-group slug and try again."}
              </p>
            )}
            <div className="flex justify-end"><button type="button" disabled={busy} onClick={startFlow} className="flex items-center gap-1.5 rounded bg-accent px-3 py-2 text-[12px] font-semibold text-white disabled:opacity-45"><LogIn className="h-3.5 w-3.5" />Begin secure sign-in</button></div>
          </div>
        ) : method === "device" && deviceFlow ? (
          <div className="mt-4 space-y-4">
            <div className="rounded border border-border bg-muted/40 p-4 text-center">
              <p className="text-[11px] font-semibold opacity-70">Enter this code at OpenAI's verification page</p>
              <p className="mt-2 font-mono text-2xl font-bold tracking-[0.2em] text-fg">{deviceFlow.user_code}</p>
              <div className="mt-3 flex flex-wrap justify-center gap-2">
                <a href={deviceFlow.verification_url} target="_blank" rel="noreferrer" className="rounded bg-accent px-3 py-1.5 text-[12px] font-semibold text-white">Open verification page</a>
                <button type="button" onClick={() => navigator.clipboard.writeText(deviceFlow.user_code)} className="flex items-center gap-1.5 rounded border border-border px-3 py-1.5 text-[12px]"><Copy className="h-3.5 w-3.5" />Copy code</button>
              </div>
              <p className="mt-3 text-[10.5px] opacity-55">Works from any device — this dialog updates automatically once approved.</p>
            </div>
            {flowFailed && <p className="text-[11px] text-error">{flowErrorText(flowStatus.data?.error_code, reauth)}</p>}
            <div className="flex justify-end"><button type="button" disabled={busy} onClick={resetAll} className="rounded border border-border px-3 py-1.5 text-[12px]">Start over</button></div>
          </div>
        ) : browserFlow ? (
          <div className="mt-4 space-y-4">
            <div className="rounded border border-border bg-muted/40 p-3"><p className="text-[11px] font-semibold">1. Sign in with OpenAI{reauth?.email && <span className="font-normal opacity-60"> — as {reauth.email}</span>}</p><div className="mt-2 flex flex-wrap gap-2"><a href={browserFlow.authorize_url} target="_blank" rel="noreferrer" className="rounded bg-accent px-3 py-1.5 text-[12px] font-semibold text-white">Open sign-in page</a><button type="button" onClick={() => navigator.clipboard.writeText(browserFlow.authorize_url)} className="flex items-center gap-1.5 rounded border border-border px-3 py-1.5 text-[12px]"><Copy className="h-3.5 w-3.5" />Copy URL</button></div>
              {sameMachine && <p className="mt-2 text-[10.5px] opacity-55">This dialog completes automatically after you sign in. If the redirect tab shows a connection error, paste its URL below.</p>}
            </div>
            <label className="block text-[11px] font-semibold">
              {isClaude
                ? "2. Paste the code Claude shows you"
                : `2. ${sameMachine ? "Only if it didn't complete automatically: paste" : "Paste"} the final redirect URL`}
              <textarea
                rows={3}
                value={callbackUrl}
                onChange={(e) => setCallbackUrl(e.target.value)}
                placeholder={
                  isClaude
                    ? "abc123…#state — or the full callback URL"
                    : "http://localhost:1455/auth/callback?code=...&state=..."
                }
                className="mt-2 w-full resize-y rounded border border-border bg-bg px-3 py-2 font-mono text-[11px] outline-none focus:border-accent"
              />
            </label>
            {isClaude && browserFlow?.client_id_verified === false && (
              <p className="rounded border border-warn/40 bg-warn/10 p-2.5 text-[10.5px] leading-relaxed">
                Claude&apos;s OAuth client registration is not verified against a capture held in
                this repository. If Anthropic rejects the sign-in, that is the likely cause rather
                than anything wrong with your account.
              </p>
            )}
            {complete.isError && <p className="text-[11px] text-error">{completeErrorText(complete.error, reauth)}</p>}
            {flowFailed && !complete.isError && <p className="text-[11px] text-error">{flowErrorText(flowStatus.data?.error_code, reauth)}</p>}
            <div className="flex justify-end gap-2"><button type="button" disabled={busy} onClick={resetAll} className="rounded border border-border px-3 py-1.5 text-[12px]">Start over</button><button type="button" disabled={busy || !callbackUrl.trim()} onClick={() => complete.mutate({ flowId: browserFlow.flow_id, callbackUrl: callbackUrl.trim() })} className="rounded bg-accent px-3 py-1.5 text-[12px] font-semibold text-white disabled:opacity-45">{complete.isPending ? "Connecting…" : "Complete connection"}</button></div>
          </div>
        ) : null}
      </div>
    </div>
  );
}
