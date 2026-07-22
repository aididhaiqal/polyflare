// The API Keys page (`/keys`): manage client proxy API keys — list (redacted), create
// (plaintext shown exactly once), and per-row enable/disable. Backend contract (Outcome 1,
// shipped): `GET /api/keys` -> `{ keys: ApiKeyView[] }` (never a hash or raw key), `POST /api/keys`
// -> `{ id, key_prefix, key }` (the ONLY response that ever carries the raw plaintext), `PATCH
// /api/keys/{id}` -> `{ ok: true }` (404 on an unknown id). See `../lib/api.ts`'s mirrored types.
//
// CONTENT-SAFETY (inviolable): the raw `key` returned by `useCreateKey` lives ONLY in this page's
// own `rawKey` React state, for exactly as long as the show-once modal is open. It is never
// console-logged, never written into the `["keys"]` query cache (that cache only ever holds
// `useKeys()`'s refetched, redacted `ApiKeyView[]` data — the create mutation's `onSuccess` here
// does not touch it), and is discarded (`setRawKey(null)`) the moment the modal closes, by any
// path (Done button, overlay click, Escape).
import { useEffect, useRef, useState } from "react";
import clsx from "clsx";

import type { ApiKeyView, CreatedApiKey } from "../lib/api";
import { relTime } from "../lib/format";
import { useCreateKey, useKeys, useUpdateKey } from "../lib/queries";
import { Card } from "../ui/Card";
import {
  AlertTriangle,
  Check,
  Copy,
  KeyRound,
  Plus,
  type LucideIcon,
} from "../ui/icons";
import { Switch } from "../ui/Switch";
import { useToast } from "../ui/Toast";

export function Keys() {
  const { data, isLoading, isError, error, refetch } = useKeys();
  const createKey = useCreateKey();
  const updateKey = useUpdateKey();

  // The raw plaintext of a just-created key, for the show-once modal only — see the file-level
  // content-safety doc above. `null` whenever the modal isn't open.
  const [rawKey, setRawKey] = useState<CreatedApiKey | null>(null);
  // Which row's enable/disable Switch has a PATCH in flight, so only THAT row disables itself
  // (mirrors Settings.tsx's `pendingKey` pattern) even though `updateKey` is one shared mutation.
  const [pendingId, setPendingId] = useState<string | null>(null);

  function handleCreate(label?: string) {
    createKey.mutate(label, {
      onSuccess: (created) => setRawKey(created),
    });
  }

  function handleToggle(id: string, enabled: boolean) {
    setPendingId(id);
    updateKey.mutate({ id, enabled }, { onSettled: () => setPendingId(null) });
  }

  const keys = data?.keys ?? [];

  return (
    <div className="flex flex-col gap-3">
      <PageHeader
        subtitle={
          data
            ? `${keys.length} ${keys.length === 1 ? "key" : "keys"} · ${keys.filter((k) => k.enabled).length} enabled`
            : undefined
        }
      />

      <CreateKeyBar onCreate={handleCreate} pending={createKey.isPending} />

      {isLoading ? (
        <KeysSkeleton />
      ) : isError ? (
        <Card>
          <div className="flex flex-wrap items-center justify-between gap-3">
            <span className="flex items-center gap-2 text-[12px] text-error">
              <AlertTriangle className="h-4 w-4 shrink-0" strokeWidth={1.9} />
              Couldn&apos;t load API keys
              {error instanceof Error ? `: ${error.message}` : "."}
            </span>
            <button
              type="button"
              onClick={() => refetch()}
              className="shrink-0 rounded border border-border px-2.5 py-1 text-[11px] text-fg opacity-80 hover:opacity-100"
            >
              Retry
            </button>
          </div>
        </Card>
      ) : keys.length === 0 ? (
        <EmptyState />
      ) : (
        <KeysTable keys={keys} onToggle={handleToggle} pendingId={pendingId} />
      )}

      <ShowOnceKeyModal created={rawKey} onClose={() => setRawKey(null)} />
    </div>
  );
}

function PageHeader({ subtitle }: { subtitle?: string }) {
  return (
    <div>
      <h1 className="text-lg font-semibold text-fg">API Keys</h1>
      <p className="mt-0.5 text-[11px] text-fg opacity-60">
        {subtitle ?? "Client proxy keys — create, list, and enable/disable."}
      </p>
    </div>
  );
}

// ---------------------------------------------------------------------------------------------
// Create bar — an inline optional-label input + "Create key" button (Card-wrapped, same visual
// weight as a form row elsewhere in the app). No separate confirmation step: creating a key is
// non-destructive and its raw plaintext is surfaced immediately via the show-once modal, so there
// is nothing a confirm dialog would usefully gate here.
// ---------------------------------------------------------------------------------------------

function CreateKeyBar({
  onCreate,
  pending,
}: {
  onCreate: (label?: string) => void;
  pending: boolean;
}) {
  const [label, setLabel] = useState("");

  function submit() {
    if (pending) return;
    const trimmed = label.trim();
    onCreate(trimmed.length > 0 ? trimmed : undefined);
    setLabel("");
  }

  return (
    <Card>
      <div className="flex flex-wrap items-center gap-2">
        <span className="shrink-0 text-[10px] uppercase tracking-wide text-fg opacity-60">
          New key
        </span>
        <input
          value={label}
          onChange={(e) => setLabel(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") submit();
          }}
          placeholder="Label (optional) — e.g. laptop, ci"
          maxLength={64}
          className="min-w-0 flex-1 rounded border border-border bg-bg px-2.5 py-1 text-[11px] text-fg outline-none placeholder:text-fg placeholder:opacity-40 hover:border-accent focus:border-accent"
        />
        <button
          type="button"
          onClick={submit}
          disabled={pending}
          className="flex shrink-0 items-center gap-1.5 rounded border border-accent bg-accent/[0.12] px-2.5 py-1 text-[11px] font-medium text-accent hover:bg-accent/[0.2] disabled:cursor-not-allowed disabled:opacity-50"
        >
          <Plus className="h-3.5 w-3.5" strokeWidth={2} />
          {pending ? "Creating…" : "Create key"}
        </button>
      </div>
    </Card>
  );
}

// ---------------------------------------------------------------------------------------------
// Empty state — per the task brief, notes that creating the first key flips the proxy from open
// (loopback-only) to key-enforced (see `posture.rs`'s `has_keys` gate).
// ---------------------------------------------------------------------------------------------

function EmptyState() {
  return (
    <Card>
      <div className="flex flex-col items-start gap-1.5 py-2 text-[11px]">
        <span className="flex items-center gap-2 text-fg opacity-80">
          <KeyRound className="h-4 w-4 shrink-0 opacity-60" strokeWidth={1.8} />
          No API keys yet.
        </span>
        <p className="text-fg opacity-55">
          The proxy currently accepts requests from loopback with no key required. Creating your
          first key switches enforcement on after you restart the proxy — every subsequent request
          must then present a valid, enabled key.
        </p>
      </div>
    </Card>
  );
}

// ---------------------------------------------------------------------------------------------
// Table — key prefix (monospace), label, created/last-used (relative), enabled Switch.
// ---------------------------------------------------------------------------------------------

const TABLE_HEAD_CLASS =
  "px-2 py-1.5 text-left text-[9px] font-medium uppercase tracking-wide text-fg opacity-60";

function KeysTable({
  keys,
  onToggle,
  pendingId,
}: {
  keys: ApiKeyView[];
  onToggle: (id: string, enabled: boolean) => void;
  pendingId: string | null;
}) {
  const [nowMs, setNowMs] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNowMs(Date.now()), 30_000);
    return () => clearInterval(id);
  }, []);

  return (
    <Card>
      <div className="overflow-x-auto">
        <table className="w-full min-w-[640px] border-collapse text-[10.5px]">
          <thead>
            <tr className="border-b border-border">
              <th className={TABLE_HEAD_CLASS}>Key</th>
              <th className={TABLE_HEAD_CLASS}>Label</th>
              <th className={TABLE_HEAD_CLASS}>Created</th>
              <th className={TABLE_HEAD_CLASS}>Last used</th>
              <th className={clsx(TABLE_HEAD_CLASS, "text-right")}>Enabled</th>
            </tr>
          </thead>
          <tbody>
            {keys.map((k) => (
              <tr key={k.id} className="border-b border-border/55 last:border-0">
                <td className="whitespace-nowrap px-2 py-1.5 font-mono text-fg opacity-90">
                  {k.key_prefix}&hellip;
                </td>
                <td className="px-2 py-1.5 text-fg opacity-80">
                  {k.label ?? <span className="text-fg opacity-40">&mdash;</span>}
                </td>
                <td className="whitespace-nowrap px-2 py-1.5 tabular-nums text-fg opacity-60">
                  {relTime(k.created_at, nowMs)}
                </td>
                <td className="whitespace-nowrap px-2 py-1.5 tabular-nums text-fg opacity-60">
                  {k.last_used_at !== null ? relTime(k.last_used_at, nowMs) : "never"}
                </td>
                <td className="px-2 py-1.5 text-right">
                  <Switch
                    checked={k.enabled}
                    onCheckedChange={(v) => onToggle(k.id, v)}
                    disabled={pendingId === k.id}
                    ariaLabel={`${k.enabled ? "Disable" : "Enable"} key ${k.label ?? k.key_prefix}`}
                  />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Card>
  );
}

function KeysSkeleton() {
  return (
    <Card>
      <div className="flex flex-col gap-2">
        {[0, 1, 2].map((i) => (
          <div key={i} className="h-6 animate-pulse rounded bg-muted" />
        ))}
      </div>
    </Card>
  );
}

// ---------------------------------------------------------------------------------------------
// Show-once modal — displays the raw key returned by `POST /api/keys` exactly once. Hand-rolled
// overlay/centering/focus/Escape (same approach `ui/ConfirmDialog.tsx` documents its own reasoning
// for — no radix-dialog dependency in this app), but not built on `ConfirmDialog` itself: this
// isn't a confirm/cancel shape, it's a single acknowledgment ("Done") plus a copy affordance.
// Closing by ANY path (Done, overlay click, Escape) calls `onClose`, which the page wires to
// `setRawKey(null)` — the only place the raw value is discarded from React state.
// ---------------------------------------------------------------------------------------------

function ShowOnceKeyModal({
  created,
  onClose,
}: {
  created: CreatedApiKey | null;
  onClose: () => void;
}) {
  const { toast } = useToast();
  const doneRef = useRef<HTMLButtonElement>(null);
  const [copied, setCopied] = useState(false);

  useEffect(() => {
    if (!created) return;
    setCopied(false);
    doneRef.current?.focus();

    function onKeyDown(e: KeyboardEvent) {
      if (e.key === "Escape") onClose();
    }
    document.addEventListener("keydown", onKeyDown);
    return () => document.removeEventListener("keydown", onKeyDown);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [created]);

  if (!created) return null;

  async function copyKey() {
    try {
      await navigator.clipboard.writeText(created!.key);
      setCopied(true);
      toast({ title: "Copied to clipboard", variant: "success" });
    } catch {
      toast({
        title: "Copy failed",
        description: "Select the key text and copy it manually.",
        variant: "error",
      });
    }
  }

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/50"
      onClick={onClose}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-label="API key created"
        onClick={(e) => e.stopPropagation()}
        className="w-full max-w-md rounded-lg border border-border bg-card p-4 text-fg shadow-xl"
      >
        <div className="flex items-center gap-2 text-sm font-semibold">
          <KeyRound className="h-4 w-4 shrink-0 text-accent" strokeWidth={1.9} />
          <span>API key created</span>
        </div>

        <div className="mt-2 flex items-start gap-2 rounded border border-warn/30 bg-warn/10 px-2.5 py-2 text-[11px] text-warn">
          <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" strokeWidth={1.9} />
          <span>This is the only time you&apos;ll see this key &mdash; save it now. It cannot be shown again.</span>
        </div>

        <div className="mt-3 flex items-center gap-2">
          <code className="min-w-0 flex-1 select-all overflow-x-auto whitespace-nowrap rounded border border-border bg-bg px-2.5 py-1.5 font-mono text-[11px] text-fg">
            {created.key}
          </code>
          <CopyButton icon={copied ? Check : Copy} onClick={copyKey} copied={copied} />
        </div>

        <p className="mt-2 text-[10.5px] text-fg opacity-50">
          Prefix <span className="font-mono">{created.key_prefix}&hellip;</span> — use this to
          recognize the key later in the list (the list never shows the full key again).
        </p>

        <div className="mt-4 flex justify-end">
          <button
            ref={doneRef}
            type="button"
            onClick={onClose}
            className="rounded bg-accent px-3 py-1.5 text-[12px] text-white"
          >
            Done, I&apos;ve saved it
          </button>
        </div>
      </div>
    </div>
  );
}

function CopyButton({
  icon: Icon,
  onClick,
  copied,
}: {
  icon: LucideIcon;
  onClick: () => void;
  copied: boolean;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-label="Copy key to clipboard"
      className={clsx(
        "flex h-8 w-8 shrink-0 items-center justify-center rounded border",
        copied ? "border-success text-success" : "border-border text-fg opacity-80 hover:border-accent hover:opacity-100",
      )}
    >
      <Icon className="h-3.5 w-3.5" strokeWidth={2} />
    </button>
  );
}
