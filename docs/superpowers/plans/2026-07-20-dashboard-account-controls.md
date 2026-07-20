# Dashboard Account Control Actions — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Turn PolyFlare's read-only Accounts UI into a control plane — pause/resume, routing-policy, set-pool, security-work toggle (backend PATCH already supports these), plus rename/alias and delete — via a reusable frontend mutation layer.

**Architecture:** Backend adds two small store methods (`update_alias`, `delete`) and extends the existing admin-gated `PATCH /api/accounts/{id}` (+ a new `DELETE`). Frontend adds a React-Query mutation layer + three small `ui/` primitives (action menu on radix-popover, a minimal confirm modal, a minimal toast) and wires action controls into `Accounts` (per-row kebab) and `AccountDetail` (action bar).

**Tech Stack:** Rust (axum, sqlx) — `polyflare-server` (`write_api.rs`), `polyflare-store` (`account.rs`). Frontend — React 18 + TypeScript, `@tanstack/react-query` v5, `react-router-dom` v6, Radix (`react-popover`/`react-select`/`react-switch`), Vite/bun. No frontend test runner exists (scripts: dev/build/preview).

## Global Constraints

- **Content-free / secret-free:** endpoints mutate account *metadata/status* only; NEVER return or accept a token value. All write routes are admin-gated by the existing `require_admin` (`POLYFLARE_ADMIN_TOKEN`) — do not add auth, it's already applied at the router in `app.rs`.
- **No emoji in any UI text.** Match the existing ccflare-skinned dark theme; reuse `ui/` primitives (`Card`, `StatusPill`, `icons`).
- **No new npm dependency** — build the menu on the existing `@radix-ui/react-popover`, the confirm modal and toast in-house. Routing-policy uses `@radix-ui/react-select`, the security toggle uses `@radix-ui/react-switch` (both already deps).
- **Additive + backward-compatible:** the `PATCH` body gains one new optional field (`alias`); existing read APIs and pages are unchanged except for the added controls.
- Backend: `cargo clippy --workspace --all-targets -- -D warnings` clean; `cargo fmt --all -- --check` clean; full `cargo test -p polyflare-server -p polyflare-store` green.
- Frontend: `npx tsc -b` (type-check) clean and `bun run build` (or `npm run build`) succeeds; no runtime console errors on the live click-through (Task 9).
- **Reuse, don't reinvent:** mirror `AccountRepo::update_pool` (bump generation) for new store methods; mirror the `double_option` + validate-then-apply pattern in `write_api.rs` for `alias`.

## File Structure

- `crates/polyflare-store/src/account.rs` — add `update_alias`, `delete`.
- `crates/polyflare-store/tests/…` (or inline `#[cfg(test)]`) — store tests.
- `crates/polyflare-server/src/write_api.rs` — extend `AccountPatch`/`patch_account_handler` with `alias`; add `delete_account_handler`.
- `crates/polyflare-server/src/app.rs:310-313` — add `.delete(...)` to the `/api/accounts/{id}` route.
- `crates/polyflare-server/tests/write_api*.rs` (or existing write-api test file) — endpoint tests.
- `dashboard/src/lib/mutations.ts` — **new** typed mutation client + React-Query hooks.
- `dashboard/src/ui/Toast.tsx` — **new** minimal toast (context + Toaster + `useToast`).
- `dashboard/src/ui/ActionMenu.tsx` — **new** kebab menu (radix-popover).
- `dashboard/src/ui/ConfirmDialog.tsx` — **new** minimal confirm modal.
- `dashboard/src/pages/Accounts.tsx` — add a per-row actions cell/kebab.
- `dashboard/src/pages/AccountDetail.tsx` — add an action bar.
- `dashboard/src/App.tsx` (or shell) — mount `<Toaster/>` once at the app root.

---

### Task 1: `AccountRepo::update_alias` (store)

**Files:** Modify `crates/polyflare-store/src/account.rs`. Test: inline `#[cfg(test)]` (mirror the existing account-repo tests in that file).

**Interfaces:**
- Produces: `pub async fn update_alias(&self, id: &str, alias: Option<&str>) -> Result<(), StoreError>` — `Some(name)` sets `alias`, `None` clears it (stores SQL `NULL`). Bumps the account generation.

- [ ] **Step 1: Failing test** — mirror the pattern of the existing `update_pool` test in `account.rs`: insert an account, `update_alias(id, Some("prod-1"))`, assert `get(id).alias == Some("prod-1")`; then `update_alias(id, None)`, assert `alias == None`; assert `account_generation()` increased across a call.
- [ ] **Step 2: RED** — `cargo test -p polyflare-store account::` (fails: method missing).
- [ ] **Step 3: Implement** (mirror `update_pool` exactly):
```rust
    /// Set (or clear) an account's human-readable alias. `Some(name)` sets it; `None` clears it to
    /// SQL NULL. Bumps the generation so the account cache re-reads on the next selection.
    pub async fn update_alias(&self, id: &str, alias: Option<&str>) -> Result<(), StoreError> {
        sqlx::query("UPDATE accounts SET alias = ? WHERE id = ?")
            .bind(alias)
            .bind(id)
            .execute(&self.pool)
            .await?;
        self.bump_generation();
        Ok(())
    }
```
- [ ] **Step 4: GREEN** — `cargo test -p polyflare-store account::`; clippy + fmt.
- [ ] **Step 5: Commit** — `feat(store): AccountRepo::update_alias (set/clear account alias)`.

---

### Task 2: Extend `PATCH /api/accounts/{id}` with `alias`

**Files:** Modify `crates/polyflare-server/src/write_api.rs`. Test: the existing write-api test file (`git ls-files crates/polyflare-server/tests | grep -i write` — reuse it; else `tests/write_api.rs`).

**Interfaces:**
- Consumes: `AccountRepo::update_alias` (Task 1).
- Produces: `AccountPatch` gains `alias: Option<Option<String>>` via the existing `double_option` deserializer (absent = unchanged; `null` or empty/whitespace = clear; non-empty ≤64 = set).

- [ ] **Step 1: Failing test** — an admin-authed `PATCH /api/accounts/{id}` with `{"alias":"prod-1"}` → 200 and `get(id).alias == Some("prod-1")`; `{"alias":null}` → clears; `{"alias":"   "}` (whitespace) → clears; `{"alias":"<65 chars>"}` → 400 `alias must be 1..=64 characters`; absent `alias` leaves it unchanged. (Model the harness on the existing patch tests for pool/routing.)
- [ ] **Step 2: RED.**
- [ ] **Step 3: Implement** — add the field to `AccountPatch`:
```rust
    #[serde(default, deserialize_with = "double_option")]
    alias: Option<Option<String>>,
```
In `patch_account_handler`, in the VALIDATE block (before any apply), add:
```rust
    // `alias`: present means set/clear. A non-empty trimmed value must be 1..=64 chars; an
    // empty/whitespace value clears (normalized to None below).
    if let Some(Some(a)) = &patch.alias {
        let t = a.trim();
        if !t.is_empty() && t.chars().count() > 64 {
            return bad_request("alias must be 1..=64 characters");
        }
    }
```
In the APPLY block (after the `security_work_authorized` apply), add:
```rust
    if let Some(alias) = &patch.alias {
        // present: set a trimmed non-empty value, else clear (empty/whitespace/null -> None).
        let normalized = alias.as_deref().map(str::trim).filter(|t| !t.is_empty());
        if repo.update_alias(&id, normalized).await.is_err() {
            return internal_error();
        }
    }
```
- [ ] **Step 4: GREEN** — run the patch tests; clippy + fmt; full `cargo test -p polyflare-server`.
- [ ] **Step 5: Commit** — `feat(write-api): PATCH account alias (set/clear, 1..=64 chars)`.

---

### Task 3: `AccountRepo::delete` (store)

**Files:** Modify `crates/polyflare-store/src/account.rs`. Test: inline `#[cfg(test)]`.

**Interfaces:**
- Produces: `pub async fn delete(&self, id: &str, delete_history: bool) -> Result<bool, StoreError>` — deletes the account row (returns `false` if no such id, `true` if deleted). When `delete_history` is true, also purges the account's dependent rows (`usage_history`, `request_log`, `continuity_sessions`, `continuity_anchors`) in the SAME transaction. Bumps the generation.

- [ ] **Step 1: Failing test** — insert an account + a `usage_history` row + a `request_log` row for it (mirror how the existing tests seed those). `delete(id, false)` → returns `true`, account gone, and the `request_log` row survives with `account_id = NULL` (FK `ON DELETE SET NULL`). Separately: seed again, `delete(id, true)` → account AND its `usage_history`/`request_log` rows all gone. `delete("nope", false)` → `Ok(false)`. Assert generation bumped.
- [ ] **Step 2: RED.**
- [ ] **Step 3: Implement:**
```rust
    /// Delete an account. Returns `true` if a row was removed, `false` if `id` was absent. With
    /// `delete_history`, also purges the account's dependent rows (usage/request/continuity) in one
    /// transaction; without it, only the account row goes (its `request_log` FK is `ON DELETE SET
    /// NULL`, so history is retained but detached). Bumps the generation.
    pub async fn delete(&self, id: &str, delete_history: bool) -> Result<bool, StoreError> {
        let mut tx = self.pool.begin().await?;
        if delete_history {
            for table in [
                "usage_history",
                "request_log",
                "continuity_anchors",
                "continuity_sessions",
            ] {
                sqlx::query(&format!("DELETE FROM {table} WHERE account_id = ?"))
                    .bind(id)
                    .execute(&mut *tx)
                    .await?;
            }
        }
        let res = sqlx::query("DELETE FROM accounts WHERE id = ?")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        let removed = res.rows_affected() > 0;
        if removed {
            self.bump_generation();
        }
        Ok(removed)
    }
```
> Note: verify the dependent-table column names against the actual schema (`.schema usage_history` etc.) before finalizing — use the real `account_id`/`session_key`→account linkage. `continuity_*` rows are keyed by `session_key`/`response_id`, NOT `account_id`; if there is no direct `account_id` column on a continuity table, DROP that table from the purge loop rather than writing an invalid query (leave a one-line comment saying why). Keep only tables that genuinely have an `account_id`/owner column.
- [ ] **Step 4: GREEN** — `cargo test -p polyflare-store account::`; clippy + fmt.
- [ ] **Step 5: Commit** — `feat(store): AccountRepo::delete (row-only or purge-history)`.

---

### Task 4: `DELETE /api/accounts/{id}` handler + route

**Files:** Modify `crates/polyflare-server/src/write_api.rs`, `crates/polyflare-server/src/app.rs`. Test: the write-api test file.

**Interfaces:**
- Consumes: `AccountRepo::delete` (Task 3).
- Produces: `pub async fn delete_account_handler(State<Arc<AppState>>, Path<String>, Query<DeleteQuery>) -> Response` where `DeleteQuery { #[serde(default)] delete_history: bool }`. 200 `{ok:true}` on delete, 404 on unknown id. Admin-gated by the existing router layer.

- [ ] **Step 1: Failing test** — admin `DELETE /api/accounts/{id}` (no query) → 200 `{ok:true}`, account gone; `DELETE /api/accounts/{id}?delete_history=true` → 200, account + history gone; `DELETE /api/accounts/nope` → 404; `DELETE` without the admin token → 401 (the router layer). Reuse the write-api harness.
- [ ] **Step 2: RED.**
- [ ] **Step 3: Implement** — in `write_api.rs`:
```rust
#[derive(Deserialize)]
pub struct DeleteQuery {
    #[serde(default)]
    delete_history: bool,
}

/// `DELETE /api/accounts/{id}` — remove an account. `?delete_history=true` also purges its
/// usage/request history; otherwise the row goes and history is retained but detached (FK SET NULL).
pub async fn delete_account_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    axum::extract::Query(q): axum::extract::Query<DeleteQuery>,
) -> Response {
    match state.store.accounts().delete(&id, q.delete_history).await {
        Ok(true) => (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response(),
        Ok(false) => (StatusCode::NOT_FOUND, "no such account").into_response(),
        Err(_) => internal_error(),
    }
}
```
In `app.rs`, extend the route (currently `get(...).patch(...)`):
```rust
            "/api/accounts/{id}",
            get(crate::read_api::account_detail_handler)
                .patch(crate::write_api::patch_account_handler)
                .delete(crate::write_api::delete_account_handler),
```
- [ ] **Step 4: GREEN** — endpoint tests; clippy + fmt; full `cargo test -p polyflare-server`.
- [ ] **Step 5: Commit** — `feat(write-api): DELETE /api/accounts/{id} (?delete_history)`.

---

### Task 5: Frontend mutation client + hooks + Toaster

**Files:** Create `dashboard/src/lib/mutations.ts`, `dashboard/src/ui/Toast.tsx`; modify `dashboard/src/App.tsx` (mount `<Toaster/>`). No test runner — verify via `tsc` + build.

**Interfaces:**
- Consumes: `fetchJson` from `lib/api.ts` (already supports any method via `init`); the app's `QueryClient` (already configured — `useAccounts` proves React Query is set up).
- Produces:
  - `useToast(): { toast: (t: {kind:"success"|"error"; message:string}) => void }` + `<Toaster/>` component.
  - `usePatchAccount()` → mutation over `PATCH /api/accounts/{id}` body `AccountPatchBody`; on success invalidates `["accounts"]` + `["account", id]` and toasts success; on error toasts `error.message`.
  - `useDeleteAccount()` → mutation over `DELETE /api/accounts/{id}?delete_history=` ; same invalidate + toast.
  - `type AccountPatchBody = { pool?: string | null; routing_policy?: "normal"|"burn_first"|"preserve"; status?: "active"|"paused"; security_work_authorized?: boolean; alias?: string | null }`.

- [ ] **Step 1: Minimal Toast** (`ui/Toast.tsx`): a React context holding a list of `{id, kind, message}`; `<Toaster/>` renders them fixed bottom-right as small `Card`-styled divs (success = accent/green border, error = red border — reuse the theme's `--ok`/`--err` tokens from `index.css`; NO emoji), auto-dismiss after ~4s; `useToast()` returns `{toast}`. Keep it ~60 lines, self-contained.
- [ ] **Step 2: Mutations** (`lib/mutations.ts`): the `AccountPatchBody` type; a `patchAccount(id, body)` = `fetchJson<{ok:boolean}>(\`/api/accounts/${id}\`, {method:"PATCH", headers:{"Content-Type":"application/json"}, body: JSON.stringify(body)})`; `deleteAccount(id, deleteHistory)` = `fetchJson(\`/api/accounts/${id}${deleteHistory ? "?delete_history=true" : ""}\`, {method:"DELETE"})`. Then `usePatchAccount()`/`useDeleteAccount()` via `useMutation` from `@tanstack/react-query`, calling `queryClient.invalidateQueries({queryKey:["accounts"]})` and `["account", vars.id]` in `onSuccess`, `useToast().toast(...)` in `onSuccess`/`onError`. (Match the exact query keys `queries.ts` uses for `useAccounts`/account-detail — read them first and reuse verbatim.)
- [ ] **Step 3: Mount** `<Toaster/>` once at the app root (in `App.tsx`, inside the same provider tree as the router/query client).
- [ ] **Step 4: Verify** — `npx tsc -b` clean; `bun run build` succeeds. (No unit runner; the live click-through in Task 9 exercises it.)
- [ ] **Step 5: Commit** — `feat(dashboard): account mutation hooks + minimal toast`.

---

### Task 6: `ui/ActionMenu` (kebab) + `ui/ConfirmDialog` (modal)

**Files:** Create `dashboard/src/ui/ActionMenu.tsx`, `dashboard/src/ui/ConfirmDialog.tsx`. Verify via `tsc` + build.

**Interfaces:**
- Produces:
  - `<ActionMenu trigger?={ReactNode} items={Array<{label:string; onSelect:()=>void; danger?:boolean; disabled?:boolean} | {separator:true} | {custom:ReactNode}}>` — a kebab (⋮ from `ui/icons`, NOT an emoji) button opening a `@radix-ui/react-popover` panel of themed menu rows; `danger` rows use the red token; closes on select.
  - `<ConfirmDialog open onOpenChange title body confirmLabel danger? onConfirm children?>` — a minimal modal: a fixed full-screen overlay (`bg-black/50`) + a centered `Card` with title/body/`children` (for the delete-history checkbox) + Cancel/Confirm buttons (Confirm red when `danger`). Close on overlay click / Escape / Cancel. No focus-trap library needed.

- [ ] **Step 1: ActionMenu** on `@radix-ui/react-popover` — trigger = a small icon button; content = the item rows styled to the theme; support `separator` and `custom` items (so a submenu control like routing-policy `Select` can be embedded).
- [ ] **Step 2: ConfirmDialog** — the minimal overlay+Card modal described above.
- [ ] **Step 3: Verify** — `npx tsc -b` clean; `bun run build` succeeds; import both into a scratch usage in `Accounts.tsx` temporarily to confirm they render (remove before commit, or leave the real wiring for Task 7).
- [ ] **Step 4: Commit** — `feat(dashboard): ActionMenu (popover kebab) + ConfirmDialog (modal)`.

---

### Task 7: Wire account actions into the Accounts page

**Files:** Modify `dashboard/src/pages/Accounts.tsx`. Verify via `tsc` + build + live (Task 9).

**Interfaces:** Consumes `usePatchAccount`/`useDeleteAccount` (Task 5), `ActionMenu`/`ConfirmDialog` (Task 6), `@radix-ui/react-select` (routing-policy), `@radix-ui/react-switch` (security toggle).

- [ ] **Step 1:** Add an **actions cell** to each account row (table view) and an actions affordance to the card view: an `<ActionMenu>` with items:
  - **Pause** / **Resume** (label from `a.status === "paused" ? "Resume" : "Pause"`) → `patchAccount(a.id, {status: a.status === "paused" ? "active" : "paused"})`.
  - **Routing policy** → a `custom` item embedding a small radix `Select` (normal/burn_first/preserve, current selected) → `patchAccount(a.id, {routing_policy: value})`.
  - **Set pool** → a `custom` item with a text input + "Clear" → `patchAccount(a.id, {pool: value || null})`.
  - **Trusted Access** (security-work) → a `custom` item with a radix `Switch` bound to `a.security_work_authorized` → `patchAccount(a.id, {security_work_authorized: next})`.
  - **Rename** → opens a small inline dialog (reuse `ConfirmDialog` with a text input as `children`, Confirm = save) → `patchAccount(a.id, {alias: value})` (empty clears).
  - separator, then **Delete** (danger) → opens `ConfirmDialog` naming the account + a "also delete history" checkbox → `deleteAccount(a.id, checked)`.
- [ ] **Step 2:** Disable the menu / show a spinner while a mutation for that row is `isPending`; rows reconcile automatically via the `["accounts"]` invalidation.
- [ ] **Step 3: Verify** — `npx tsc -b` clean; `bun run build` succeeds.
- [ ] **Step 4: Commit** — `feat(dashboard): per-account action menu on the Accounts page`.

---

### Task 8: Wire the action bar into AccountDetail

**Files:** Modify `dashboard/src/pages/AccountDetail.tsx`. Verify via `tsc` + build + live.

**Interfaces:** Same hooks/components as Task 7.

- [ ] **Step 1:** Add a prominent **action bar** near the account identity header: the same actions as Task 7 (Pause/Resume button, Routing-policy `Select`, Set-pool control, Trusted-Access `Switch`, Rename, Delete-with-confirm). Prefer inline controls here (not hidden in a kebab) since this is the dedicated detail surface. On success the detail refetches (`["account", id]` invalidation). Delete navigates back to `/dashboard/accounts` after success.
- [ ] **Step 2: Verify** — `npx tsc -b` clean; `bun run build` succeeds.
- [ ] **Step 3: Commit** — `feat(dashboard): account action bar on AccountDetail`.

---

### Task 9: Live verification (controller-run)

Not a code task. After Tasks 1-8, with a built dashboard + a running server (`POLYFLARE_ADMIN_TOKEN` set), log into the dashboard and exercise each control against a **safe target** (prefer a benched/quota-exceeded account, never a healthy one you need):
- [ ] Pause an account → it flips to `paused`, a success toast shows, the row/detail reflects it, and `GET /api/accounts` shows `status:"paused"` (and selection now skips it — confirm via the DB/behavior).
- [ ] Resume it → back to `active`.
- [ ] Change routing-policy → persists (re-read `/api/accounts`); set + clear pool → persists; toggle Trusted Access → persists; rename + clear alias → persists.
- [ ] Open the Delete confirm modal → **verify the modal + checkbox render and Cancel is safe**; only actually confirm-delete against a genuinely disposable account (or skip the destructive confirm and assert the endpoint via `curl` on a throwaway). Never delete a real working account as part of the smoke test.
- [ ] Confirm no console errors; a failed mutation (e.g. patch an invalid routing value via curl) surfaces the 400 message as an error toast.
- [ ] Record results; revert any test mutations (un-pause, restore pool/alias) on real accounts.

---

## Self-Review

- **Spec coverage:** pause/resume + routing-policy + set-pool + security-toggle (Tasks 2 already-supported fields wired in 7/8) ✓; alias (Tasks 1-2, 7-8) ✓; delete + delete_history (Tasks 3-4, 7-8) ✓; reusable mutation layer + toast (Task 5) ✓; kebab on Accounts (Task 7) + action bar on AccountDetail (Task 8) ✓; confirm modal for destructive (Task 6-8) ✓; admin-gate reused (all backend tasks) ✓; content-free/no-token (backend tasks) ✓; no-emoji/ccflare-skin/no-new-dep (Global Constraints, Tasks 5-8) ✓; deferred controls explicitly out of scope ✓.
- **Placeholder scan:** the one investigate-first note (continuity table columns in Task 3) is a concrete "verify the schema, drop invalid tables" instruction, not a TODO. No "add error handling"/"TBD" left.
- **Type consistency:** `AccountPatchBody`/`AccountPatch` fields (`pool`/`routing_policy`/`status`/`security_work_authorized`/`alias`), `update_alias`/`delete`/`delete_account_handler`/`DeleteQuery`, `usePatchAccount`/`useDeleteAccount`/`useToast` used consistently across tasks; the query keys are read-and-reused verbatim from `queries.ts` (Task 5 note).
