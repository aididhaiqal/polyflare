// Shared account-action dialogs (rename / set-pool / delete) + the mutation handles the kebab
// menus fire discrete actions through directly. Built once here (Task 7's Accounts-list kebab) and
// reused as-is by Task 8's AccountDetail action panel — the `AccountActionsApi` surface below is
// the stable contract between the two call sites; keep it stable.
//
// Discrete actions (pause/resume, routing-policy pick, security toggle) don't open a dialog — a
// consumer calls `actions.patch.mutate({ id, body })` directly. Only rename/set-pool/delete need a
// confirmation surface, so those three are the only dialogs this hook owns.
import { useState, type ReactNode } from "react";

import { usePatchAccount, useDeleteAccount } from "./queries";
import { ConfirmDialog } from "../ui/ConfirmDialog";

export interface AccountActionsApi {
  /** The shared patch mutation — call `.mutate({ id, body })` for the discrete actions
   *  (pause/resume, routing-policy, security toggle) directly from a surface. */
  patch: ReturnType<typeof usePatchAccount>;
  openRename: (a: { id: string; alias: string | null }) => void;
  openSetPool: (a: { id: string; pool: string | null }) => void;
  /** `onDeleted` runs after a successful delete (list: omit; detail: navigate away). */
  openDelete: (a: { id: string; label: string; onDeleted?: () => void }) => void;
  /** Render exactly once in the consuming page. */
  dialogs: ReactNode;
}

type Dialog =
  | { kind: "rename"; id: string; draft: string }
  | { kind: "pool"; id: string; draft: string }
  | { kind: "delete"; id: string; label: string; deleteHistory: boolean; onDeleted?: () => void }
  | null;

const FIELD_CLASS = "w-full rounded border border-border bg-bg px-2 py-1 text-fg";
const HINT_CLASS = "mt-1 text-[10.5px] text-fg opacity-50";

export function useAccountActions(): AccountActionsApi {
  const patch = usePatchAccount();
  const del = useDeleteAccount();
  const [dialog, setDialog] = useState<Dialog>(null);

  function openRename(a: { id: string; alias: string | null }) {
    setDialog({ kind: "rename", id: a.id, draft: a.alias ?? "" });
  }
  function openSetPool(a: { id: string; pool: string | null }) {
    setDialog({ kind: "pool", id: a.id, draft: a.pool ?? "" });
  }
  function openDelete(a: { id: string; label: string; onDeleted?: () => void }) {
    setDialog({ kind: "delete", id: a.id, label: a.label, deleteHistory: false, onDeleted: a.onDeleted });
  }
  function closeDialog(open: boolean) {
    if (!open) setDialog(null);
  }

  // Narrowed views of `dialog`, one per kind — lets each ConfirmDialog's `children`/`onConfirm`
  // reference a precisely-typed local instead of re-checking `dialog?.kind` at every use site.
  const renameDialog = dialog?.kind === "rename" ? dialog : null;
  const poolDialog = dialog?.kind === "pool" ? dialog : null;
  const deleteDialog = dialog?.kind === "delete" ? dialog : null;

  const dialogs = (
    <>
      <ConfirmDialog
        open={renameDialog !== null}
        onOpenChange={closeDialog}
        title="Rename account"
        confirmLabel="Save"
        busy={patch.isPending}
        onConfirm={() => {
          if (!renameDialog) return;
          patch.mutate(
            { id: renameDialog.id, body: { alias: renameDialog.draft.trim() || null } },
            { onSuccess: () => setDialog(null) },
          );
        }}
      >
        {renameDialog && (
          <div>
            <input
              autoFocus
              maxLength={64}
              placeholder="alias"
              value={renameDialog.draft}
              onChange={(e) => setDialog({ ...renameDialog, draft: e.target.value })}
              className={FIELD_CLASS}
            />
            <p className={HINT_CLASS}>Empty clears the alias.</p>
          </div>
        )}
      </ConfirmDialog>

      <ConfirmDialog
        open={poolDialog !== null}
        onOpenChange={closeDialog}
        title="Set pool"
        confirmLabel="Save"
        busy={patch.isPending}
        onConfirm={() => {
          if (!poolDialog) return;
          patch.mutate(
            { id: poolDialog.id, body: { pool: poolDialog.draft.trim() || null } },
            { onSuccess: () => setDialog(null) },
          );
        }}
      >
        {poolDialog && (
          <div>
            <input
              autoFocus
              placeholder="pool slug"
              value={poolDialog.draft}
              onChange={(e) => setDialog({ ...poolDialog, draft: e.target.value })}
              className={FIELD_CLASS}
            />
            <p className={HINT_CLASS}>Empty = unpooled.</p>
          </div>
        )}
      </ConfirmDialog>

      <ConfirmDialog
        open={deleteDialog !== null}
        onOpenChange={closeDialog}
        title="Delete account"
        description={
          deleteDialog
            ? `This removes ${deleteDialog.label} and detaches its request-log history.`
            : undefined
        }
        confirmLabel="Delete"
        danger
        busy={del.isPending}
        onConfirm={() => {
          if (!deleteDialog) return;
          const { id, deleteHistory, onDeleted } = deleteDialog;
          del.mutate(
            { id, deleteHistory },
            {
              onSuccess: () => {
                setDialog(null);
                onDeleted?.();
              },
            },
          );
        }}
      >
        {deleteDialog && (
          <label className="flex items-center gap-2 text-[11px] text-fg opacity-80">
            <input
              type="checkbox"
              checked={deleteDialog.deleteHistory}
              onChange={(e) => setDialog({ ...deleteDialog, deleteHistory: e.target.checked })}
            />
            Also purge this account's request-log history
          </label>
        )}
      </ConfirmDialog>
    </>
  );

  return { patch, openRename, openSetPool, openDelete, dialogs };
}
