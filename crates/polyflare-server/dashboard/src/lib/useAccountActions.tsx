// Shared account-action dialogs (rename / set-pool / delete) + the mutation handles the kebab
// menus fire discrete actions through directly. Built once here (Task 7's Accounts-list kebab) and
// reused as-is by Task 8's AccountDetail action panel — the `AccountActionsApi` surface below is
// the stable contract between the two call sites; keep it stable.
//
// Discrete actions (pause/resume, routing-policy pick, security toggle) don't open a dialog — a
// consumer calls `actions.patch.mutate({ id, body })` directly. Only rename/set-pool/delete need a
// confirmation surface, so those three are the only dialogs this hook owns.
import { useState, type ReactNode } from "react";

import { usePatchAccount, useDeleteAccount, usePools } from "./queries";
import { ConfirmDialog } from "../ui/ConfirmDialog";

export interface AccountActionsApi {
  /** The shared patch mutation — call `.mutate({ id, body })` for the discrete actions
   *  (pause/resume, routing-policy, security toggle) directly from a surface. */
  patch: ReturnType<typeof usePatchAccount>;
  openRename: (a: { id: string; alias: string | null }) => void;
  openSetPool: (a: { id: string; pools: string[] }) => void;
  /** `onDeleted` runs after a successful delete (list: omit; detail: navigate away). */
  openDelete: (a: { id: string; label: string; onDeleted?: () => void }) => void;
  /** Render exactly once in the consuming page. */
  dialogs: ReactNode;
}

type Dialog =
  | { kind: "rename"; id: string; draft: string }
  | { kind: "pool"; id: string; selected: string[]; draft: string }
  | { kind: "delete"; id: string; label: string; deleteHistory: boolean; onDeleted?: () => void }
  | null;

const FIELD_CLASS = "w-full rounded border border-border bg-bg px-2 py-1 text-fg";
const HINT_CLASS = "mt-1 text-[10.5px] text-fg opacity-50";

export function useAccountActions(): AccountActionsApi {
  const patch = usePatchAccount();
  const del = useDeleteAccount();
  const poolsQuery = usePools();
  const [dialog, setDialog] = useState<Dialog>(null);

  function openRename(a: { id: string; alias: string | null }) {
    setDialog({ kind: "rename", id: a.id, draft: a.alias ?? "" });
  }
  function openSetPool(a: { id: string; pools: string[] }) {
    setDialog({ kind: "pool", id: a.id, selected: [...a.pools], draft: "" });
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
        title="Manage routing groups"
        description="An account can serve requests from multiple named pools."
        confirmLabel="Save memberships"
        busy={patch.isPending}
        onConfirm={() => {
          if (!poolDialog) return;
          const added = poolDialog.draft
            .toLowerCase()
            .split(/[\s,]+/)
            .map((value) => value.trim())
            .filter(Boolean);
          const pools = Array.from(new Set([...poolDialog.selected, ...added])).sort();
          patch.mutate(
            { id: poolDialog.id, body: { pools } },
            { onSuccess: () => setDialog(null) },
          );
        }}
      >
        {poolDialog && (
          <div className="space-y-3">
            <fieldset>
              <legend className="text-[10.5px] font-semibold text-fg">Existing groups</legend>
              <div className="mt-1.5 max-h-44 space-y-1 overflow-y-auto rounded border border-border bg-bg p-1.5">
                {(poolsQuery.data ?? []).filter((pool) => pool.pool !== null).length === 0 ? (
                  <p className="px-2 py-1.5 text-[10.5px] text-fg opacity-45">
                    No named routing groups yet.
                  </p>
                ) : (
                  (poolsQuery.data ?? [])
                    .filter((pool): pool is typeof pool & { pool: string } => pool.pool !== null)
                    .map((pool) => (
                      <label
                        key={pool.pool}
                        className="flex cursor-pointer items-center gap-2 rounded px-2 py-1.5 text-[11px] text-fg hover:bg-muted"
                      >
                        <input
                          type="checkbox"
                          checked={poolDialog.selected.includes(pool.pool)}
                          onChange={() =>
                            setDialog({
                              ...poolDialog,
                              selected: poolDialog.selected.includes(pool.pool)
                                ? poolDialog.selected.filter((value) => value !== pool.pool)
                                : [...poolDialog.selected, pool.pool],
                            })
                          }
                          className="accent-accent"
                        />
                        <span className="min-w-0 flex-1 truncate font-medium">{pool.pool}</span>
                        <span className="text-[9.5px] opacity-45">{pool.accounts} accounts</span>
                      </label>
                    ))
                )}
              </div>
            </fieldset>
            <div>
              <label className="text-[10.5px] font-semibold text-fg" htmlFor="additional-pools">
                Add new group slugs
              </label>
            <input
              id="additional-pools"
              placeholder="team-a, overflow"
              value={poolDialog.draft}
              onChange={(e) => setDialog({ ...poolDialog, draft: e.target.value.toLowerCase() })}
              className={FIELD_CLASS}
            />
              <p className={HINT_CLASS}>Comma or space separated. Clear all checks for unpooled.</p>
            </div>
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
