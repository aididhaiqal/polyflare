// Credential export: a two-step, reveal-on-purpose dialog for the one dashboard action that hands
// live account credentials to the operator.
//
// The credential is fetched ONLY after an explicit confirm (so merely opening the dialog never
// pulls a secret out of the server), is held in component state that unmounts with the dialog, and
// is masked until the operator asks to see it. Every export is audit-logged server-side.
import { useRef, useState } from "react";

import type { ExportedAuthJson } from "../lib/api";
import { useExportAccountAuth } from "../lib/queries";
import { AlertTriangle, Copy, Download, X } from "./icons";
import { useDialogA11y } from "./useDialogA11y";

export function ExportAuthDialog({
  open,
  onOpenChange,
  accountId,
  label,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  accountId: string;
  label: string;
}) {
  const dialogRef = useRef<HTMLDivElement>(null);
  const closeRef = useRef<HTMLButtonElement>(null);
  const [revealed, setRevealed] = useState(false);
  const [copied, setCopied] = useState(false);
  const exportAuth = useExportAccountAuth();
  useDialogA11y(open, () => onOpenChange(false), dialogRef, closeRef);

  if (!open) return null;
  const credential: ExportedAuthJson | undefined = exportAuth.data;
  const json = credential ? JSON.stringify(credential, null, 2) : "";
  const close = () => {
    if (exportAuth.isPending) return;
    // Drop the credential from memory the moment the dialog closes.
    exportAuth.reset();
    setRevealed(false);
    setCopied(false);
    onOpenChange(false);
  };
  const download = () => {
    const url = URL.createObjectURL(new Blob([json], { type: "application/json" }));
    const link = document.createElement("a");
    link.href = url;
    link.download = "auth.json";
    link.click();
    URL.revokeObjectURL(url);
  };
  const copy = async () => {
    await navigator.clipboard.writeText(json);
    setCopied(true);
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/55 p-4" onClick={close}>
      <div
        ref={dialogRef}
        tabIndex={-1}
        role="dialog"
        aria-modal="true"
        aria-label="Export account credentials"
        onClick={(e) => e.stopPropagation()}
        className="w-full max-w-xl rounded-lg border border-border bg-card p-4 text-fg shadow-xl outline-none"
      >
        <div className="flex items-start justify-between gap-3">
          <div>
            <h2 className="flex items-center gap-1.5 text-sm font-semibold">
              <AlertTriangle className="h-4 w-4 text-warn" strokeWidth={2} />
              Export credentials
            </h2>
            <p className="mt-1 text-[11px] opacity-60">
              Codex CLI <code className="font-mono">auth.json</code> for <b>{label}</b>
            </p>
          </div>
          <button ref={closeRef} type="button" onClick={close} disabled={exportAuth.isPending} aria-label="Close" className="rounded p-1 opacity-55 hover:bg-muted hover:opacity-100">
            <X className="h-4 w-4" />
          </button>
        </div>

        {!credential ? (
          <div className="mt-4 space-y-4">
            <div className="rounded border border-warn/40 bg-warn/10 p-3 text-[11px] leading-relaxed">
              This reveals a live <b>refresh token</b>. Anyone who obtains it can use this ChatGPT
              account until you sign the account out — expiry alone will not revoke it. Export only
              to a machine you control, and re-authenticate the account if the file ever leaks.
            </div>
            <p className="text-[11px] leading-relaxed opacity-70">
              The export is recorded in PolyFlare's live logs. Routing is unaffected — this copies
              the credential, it does not move or invalidate it.
            </p>
            <div className="flex justify-end gap-2">
              <button type="button" onClick={close} className="rounded border border-border px-3 py-1.5 text-[12px]">
                Cancel
              </button>
              <button
                type="button"
                disabled={exportAuth.isPending}
                onClick={() => exportAuth.mutate(accountId)}
                className="rounded bg-warn px-3 py-1.5 text-[12px] font-semibold text-white disabled:opacity-45"
              >
                {exportAuth.isPending ? "Exporting…" : "Reveal credentials"}
              </button>
            </div>
          </div>
        ) : (
          <div className="mt-4 space-y-3">
            <div className="relative">
              <pre className="max-h-64 overflow-auto rounded border border-border bg-bg p-3 font-mono text-[10.5px] leading-relaxed">
                {revealed ? json : json.replace(/("(?:id_token|access_token|refresh_token)":\s*")[^"]*(")/g, "$1••••••••••••••••$2")}
              </pre>
              {!revealed && (
                <button
                  type="button"
                  onClick={() => setRevealed(true)}
                  className="absolute right-2 top-2 rounded border border-border bg-card px-2 py-1 text-[10.5px] font-semibold"
                >
                  Show tokens
                </button>
              )}
            </div>
            <p className="text-[10.5px] leading-relaxed opacity-55">
              Save as <code className="font-mono">~/.codex/auth.json</code> to use this account
              directly in the Codex CLI. Closing this dialog discards the copy held here.
            </p>
            <div className="flex justify-end gap-2">
              <button type="button" onClick={copy} className="flex items-center gap-1.5 rounded border border-border px-3 py-1.5 text-[12px]">
                <Copy className="h-3.5 w-3.5" />
                {copied ? "Copied" : "Copy JSON"}
              </button>
              <button type="button" onClick={download} className="flex items-center gap-1.5 rounded bg-accent px-3 py-1.5 text-[12px] font-semibold text-white">
                <Download className="h-3.5 w-3.5" />
                Download auth.json
              </button>
              <button type="button" onClick={close} className="rounded border border-border px-3 py-1.5 text-[12px]">
                Done
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
