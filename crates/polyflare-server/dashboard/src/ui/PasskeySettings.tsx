// Passkey management: register a new authenticator, review the ones already trusted, revoke one.
//
// The copy here carries a load the UI would otherwise hide: registering the FIRST passkey changes
// the server's posture, closing the tokenless local bypass that currently lets any process on this
// machine reach every admin route. That is the point of the feature, and the operator should be
// told before they click, not after.
import { useEffect, useState } from "react";

import { ApiError } from "../lib/api";
import {
  deletePasskey,
  getAuthStatus,
  listPasskeys,
  passkeysAvailable,
  registerPasskey,
  type PasskeyView,
} from "../lib/passkey";
import { Card } from "./Card";
import { AlertTriangle, KeyRound, Trash2 } from "./icons";
import { useToast } from "./Toast";

function when(seconds: number | null): string {
  if (!seconds) return "never";
  return new Date(seconds * 1000).toLocaleString();
}

export function PasskeySettings() {
  const { toast } = useToast();
  const [passkeys, setPasskeys] = useState<PasskeyView[] | null>(null);
  const [supported, setSupported] = useState(true);
  const [busy, setBusy] = useState(false);
  const [label, setLabel] = useState("");

  const refresh = () => {
    listPasskeys()
      .then(setPasskeys)
      .catch(() => setPasskeys([]));
    getAuthStatus()
      .then((status) => setSupported(status.passkey_supported))
      .catch(() => undefined);
  };
  useEffect(refresh, []);

  const register = async () => {
    if (busy) return;
    setBusy(true);
    try {
      await registerPasskey(label.trim() || "Passkey");
      setLabel("");
      refresh();
      toast({ title: "Passkey registered", variant: "success" });
    } catch (err) {
      const aborted =
        err instanceof DOMException && (err.name === "NotAllowedError" || err.name === "AbortError");
      if (!aborted) {
        toast({
          title: "Could not register passkey",
          description:
            err instanceof ApiError && err.status === 400
              ? "This origin cannot host passkeys — open the dashboard at http://localhost:8080."
              : "The authenticator did not complete registration.",
          variant: "error",
        });
      }
    } finally {
      setBusy(false);
    }
  };

  const revoke = async (entry: PasskeyView) => {
    try {
      await deletePasskey(entry.id);
      refresh();
      toast({ title: `Removed ${entry.label}`, variant: "success" });
    } catch (err) {
      toast({
        title: "Could not remove passkey",
        description:
          err instanceof ApiError && err.status === 409
            ? "This is the last passkey. Set POLYFLARE_ADMIN_TOKEN first so you keep a way in."
            : "Try again.",
        variant: "error",
      });
    }
  };

  const browserCapable = passkeysAvailable();
  const none = (passkeys ?? []).length === 0;

  return (
    <Card className="gap-3">
      <div>
        <div className="text-[10px] uppercase tracking-wide text-fg opacity-60">Passkeys</div>
        <p className="mt-1 text-[11px] leading-relaxed text-fg opacity-55">
          Sign in to this dashboard with Touch ID, Windows Hello, or a security key instead of a
          shared token.
        </p>
      </div>

      {!browserCapable ? (
        <p className="rounded border border-border bg-muted/40 p-3 text-[11px] opacity-70">
          This browser does not support passkeys.
        </p>
      ) : !supported ? (
        <p className="flex items-start gap-2 rounded border border-warn/40 bg-warn/10 p-3 text-[11px] leading-relaxed">
          <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-warn" strokeWidth={2} />
          <span>
            Passkeys need a hostname, and this page was opened on an IP address. Reopen the
            dashboard at <code className="font-mono">http://localhost:8080</code> to register one.
          </span>
        </p>
      ) : (
        <>
          {none && (
            <p className="rounded border border-warn/40 bg-warn/10 p-3 text-[11px] leading-relaxed">
              No passkey yet, so every <code className="font-mono">/api</code> route on this machine
              is reachable without authentication — including credential export. Registering one
              closes that, and this browser becomes a way back in.
            </p>
          )}
          <div className="flex flex-wrap items-end gap-2">
            <label className="flex-1 text-[11px] font-medium">
              Name <span className="font-normal opacity-50">(optional)</span>
              <input
                value={label}
                onChange={(e) => setLabel(e.target.value)}
                placeholder="MacBook Touch ID"
                className="mt-1.5 w-full rounded border border-border bg-bg px-3 py-2 text-[12px] outline-none focus:border-accent"
              />
            </label>
            <button
              type="button"
              onClick={register}
              disabled={busy}
              className="flex items-center gap-1.5 rounded bg-accent px-3 py-2 text-[12px] font-semibold text-white disabled:opacity-45"
            >
              <KeyRound className="h-3.5 w-3.5" />
              {busy ? "Waiting…" : "Register passkey"}
            </button>
          </div>
        </>
      )}

      {passkeys && passkeys.length > 0 && (
        <div className="mt-1 divide-y divide-border/60 border-t border-border/60">
          {passkeys.map((entry) => (
            <div key={entry.id} className="flex items-center justify-between gap-3 py-2">
              <div className="min-w-0">
                <div className="truncate text-[12px] font-semibold text-fg">{entry.label}</div>
                <div className="text-[10px] text-fg opacity-50">
                  added {when(entry.created_at)} · last used {when(entry.last_used_at)}
                </div>
              </div>
              <button
                type="button"
                onClick={() => revoke(entry)}
                aria-label={`Remove ${entry.label}`}
                className="flex shrink-0 items-center gap-1.5 rounded border border-error/35 px-2.5 py-1.5 text-[11px] text-error hover:bg-error/10"
              >
                <Trash2 className="h-3.5 w-3.5" />
                Remove
              </button>
            </div>
          ))}
        </div>
      )}
    </Card>
  );
}
