import { useRef, useState } from "react";

import type { AccountView } from "../lib/api";
import { useCreatePool } from "../lib/queries";
import { Layers, X } from "./icons";
import { useDialogA11y } from "./useDialogA11y";

export function CreatePoolDialog({ open, onOpenChange, accounts }: { open: boolean; onOpenChange: (open: boolean) => void; accounts: AccountView[] }) {
  const dialogRef = useRef<HTMLDivElement>(null);
  const closeRef = useRef<HTMLButtonElement>(null);
  const [slug, setSlug] = useState("");
  const [selected, setSelected] = useState<string[]>([]);
  const create = useCreatePool();
  useDialogA11y(open, () => onOpenChange(false), dialogRef, closeRef);
  if (!open) return null;
  const toggle = (id: string) => setSelected((ids) => ids.includes(id) ? ids.filter((value) => value !== id) : [...ids, id]);
  const submit = () => create.mutate({ slug: slug.trim(), accountIds: selected }, { onSuccess: () => { setSlug(""); setSelected([]); onOpenChange(false); } });
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/55 p-4" onClick={() => !create.isPending && onOpenChange(false)}>
      <div ref={dialogRef} tabIndex={-1} role="dialog" aria-modal="true" aria-label="Create routing group" onClick={(e) => e.stopPropagation()} className="w-full max-w-md rounded-lg border border-border bg-card p-4 text-fg shadow-xl outline-none">
        <div className="flex items-start justify-between gap-3"><div><h2 className="flex items-center gap-2 text-sm font-semibold"><Layers className="h-4 w-4 text-accent" />Create routing group</h2><p className="mt-1 text-[11px] opacity-60">Assign at least one account to a new routing tag.</p></div><button ref={closeRef} type="button" disabled={create.isPending} onClick={() => onOpenChange(false)} aria-label="Close" className="rounded p-1 opacity-55 hover:bg-muted hover:opacity-100"><X className="h-4 w-4" /></button></div>
        <label className="mt-4 block text-[11px] font-semibold">Group slug<input autoFocus value={slug} onChange={(e) => setSlug(e.target.value.toLowerCase())} placeholder="team-a" maxLength={48} className="mt-1.5 w-full rounded border border-border bg-bg px-3 py-2 font-mono text-[12px] outline-none focus:border-accent" /><span className="mt-1 block font-normal opacity-50">Lowercase letters, numbers, underscore, or hyphen; must start with a letter or number.</span></label>
        <fieldset className="mt-4"><legend className="text-[11px] font-semibold">Initial accounts</legend><p className="mt-1 text-[10px] opacity-50">Selected accounts join this group without leaving their existing routing groups.</p><div className="mt-2 max-h-56 space-y-1 overflow-y-auto rounded border border-border p-1.5">{accounts.map((account) => <label key={account.id} className="flex cursor-pointer items-center gap-2 rounded px-2 py-2 text-[11px] hover:bg-muted"><input type="checkbox" checked={selected.includes(account.id)} onChange={() => toggle(account.id)} className="accent-accent" /><span className="min-w-0 flex-1 truncate font-medium">{account.alias ?? account.id}</span><span className="hidden max-w-36 shrink-0 truncate opacity-50 sm:inline">{account.pools.length > 0 ? account.pools.join(", ") : "unpooled"}</span></label>)}</div></fieldset>
        {create.isError && <p className="mt-3 text-[11px] text-error">Could not create this routing group. Check the slug and selected accounts.</p>}
        <div className="mt-4 flex justify-end gap-2"><button type="button" disabled={create.isPending} onClick={() => onOpenChange(false)} className="rounded border border-border px-3 py-1.5 text-[12px]">Cancel</button><button type="button" disabled={create.isPending || !slug.trim() || selected.length === 0} onClick={submit} className="rounded bg-accent px-3 py-1.5 text-[12px] font-semibold text-white disabled:opacity-45">{create.isPending ? "Creating…" : "Create routing group"}</button></div>
      </div>
    </div>
  );
}
