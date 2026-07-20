# Dashboard Account Control Actions — Design

**Status:** Approved 2026-07-20. First slice of the dashboard build-out (turning the read-only observability dashboard into a control plane). Next: implementation plan → SDD.

## Motivation

PolyFlare's dashboard is a well-built **read-only** observability tool — it has *zero* mutations anywhere, while codex-lb is a full control plane. The highest-value, most foundational slice is **account control actions**: operate on accounts (pause, re-route, re-pool, rename, authorize, delete) from the UI instead of the CLI/DB. This slice also establishes the **reusable frontend mutation pattern** (POST/PATCH/DELETE + optimistic refetch + toasts) that every later dashboard subsystem (Settings, API-keys, Automations) will reuse.

## Scope — the control set (first cut)

**Backend ALREADY supports (just wire the UI):** the existing `PATCH /api/accounts/{id}` (admin-gated, `write_api.rs`) already accepts and validates `pool`, `routing_policy` (normal|burn_first|preserve), `status` (active|paused), and `security_work_authorized`, and each store helper bumps the account-cache generation so it takes effect on the next selection.
- **Pause / Resume** — `status` active↔paused.
- **Routing-policy** — normal / burn_first / preserve.
- **Set pool** — assign a pool slug or clear to unpooled.
- **Security-work ("Trusted Access") toggle** — `security_work_authorized`.

**Small backend add (this slice):**
- **Rename / alias** — new `AccountRepo::update_alias(id, Option<&str>)` (bumps account generation) + add `alias` to the `PATCH` body.
- **Delete account** — new `AccountRepo::delete(id, delete_history: bool)` + new `DELETE /api/accounts/{id}?delete_history=<bool>` handler (admin-gated). `delete_history=false` (default) removes the account row (FK `request_logs.account_id` is `ON DELETE SET NULL`); `delete_history=true` also purges its `usage_history`/`request_log`/`continuity` rows. Bumps the account generation.

**Deferred to later cycles** (need real backend machinery, out of scope here): reset-credit (upstream reset-credit consumption), force-probe (on-demand health check), warm-up toggle, OAuth add / import / export flows, per-account upstream-proxy binding.

## Backend

- **Extend `PATCH /api/accounts/{id}`:** add an optional `alias` field, mirroring the existing `pool` clearing convention — **absent** = leave unchanged; **present** = set it, where a trimmed non-empty value (≤64 chars) sets the alias and an empty/whitespace/`null` value **clears** it (stores `NULL`). Everything else (pool/routing/status/security) is already handled. Keep the "validate ALL fields before applying ANY" ordering already in the handler.
- **Add `DELETE /api/accounts/{id}`:** admin-gated; `delete_history` query flag; returns `{ok:true}` or 404 if absent. New `AccountRepo::delete` runs in one transaction.
- **Cache correctness (already handled for this path):** every `AccountRepo` write bumps `Store::account_generation`, and the account cache re-reads when the generation differs (5s TTL backstop). Because the dashboard write path is **in-process** (the server itself handles the PATCH/DELETE), the bump reaches the running cache immediately — so a pause/re-route/delete takes effect on the next selection with no restart. (The CLI's separate-process bump is what did NOT propagate live during earlier testing; the dashboard path is unaffected.)
- **Content-free:** these endpoints mutate account *metadata/status* only — never conversation content, never a token value in a response body (the token columns are never returned). Admin-gated by the existing `require_admin` (`POLYFLARE_ADMIN_TOKEN`).

## Frontend

- **Mutation client (the reusable foundation):** the dashboard has no mutations today. Add a small typed mutation layer in `dashboard/src/lib/api.ts` (`patchAccount(id, body)`, `deleteAccount(id, opts)`) using the same bearer-auth + 401→login behavior as the existing read client, wrapped in a `useMutation`-style hook (or the app's existing data layer) that **refetches the affected read query on success** (`/api/accounts`, `/api/accounts/{id}`) and surfaces a **toast** on success/failure. This hook is what Settings/API-keys/Automations reuse later.
- **Accounts page:** a per-row **kebab (⋯) actions menu** → Pause/Resume (label reflects current status), Routing-policy submenu (3 options, current checked), Set pool (input/select + clear), Security-work toggle, Rename (inline or small dialog), Delete (opens confirm). Row optimistically reflects the change, then reconciles on refetch.
- **AccountDetail page:** a prominent **action bar** with the same actions (this is the primary control surface; the kebab is the quick path).
- **Safety:** destructive actions (**Delete**, and **Deactivate** if we surface it) go through a **confirm modal** naming the account + consequence; Delete's modal has the `delete_history` checkbox. Non-destructive actions apply directly.
- **Design consistency:** reuse existing primitives (`StatusPill`, `Card`, `Grid`, menus in `ui/`); match the ccflare-skinned dark theme; **no emoji** (project UI rule). No new charting.

## Testing

- **Backend:** endpoint tests for the extended `PATCH` (alias set/clear/validation; each field mutates + bumps generation; invalid enum → 400; unknown id → 404) and `DELETE` (removes the row; `delete_history=true` purges dependent rows; `delete_history=false` leaves request_logs with NULL account_id; unknown id → 404; admin-gate → 401 without token). Reuse the existing write_api test patterns.
- **Store:** `update_alias` + `delete` unit tests (mutation + generation bump; delete cascade behavior).
- **Frontend:** the mutation client refetches on success and shows a toast; the confirm modal gates Delete. (Match the dashboard's existing test approach — if none, keep the mutation layer thin + typed and lean on the backend tests.)

## Global constraints

- **Content-free:** no conversation content; token values never returned. Admin-gated (existing `require_admin`).
- **Additive:** read APIs + existing pages unchanged except for the added controls; the `PATCH` extension is backward-compatible (new optional field).
- **No emoji in UI; ccflare-skin consistency; reuse existing components.**
- Clippy `-D warnings`, fmt, full `cargo test -p polyflare-server` green; dashboard `tsc` + build clean.

## Out of scope (explicit)

Reset-credit, force-probe, warm-up, OAuth/import/export, proxy-binding (deferred controls); and the other dashboard subsystems (Reports/cost analytics, Settings page, API-keys, Automations) — each is its own spec→plan→ship cycle later.
