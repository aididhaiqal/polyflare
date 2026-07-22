# Dashboard API-Keys Subsystem

**Goal:** Manage client proxy API keys from the dashboard — list, create (plaintext shown once), and enable/disable — reusing the existing `keys`/`ApiKeyRepo` primitives that today are CLI-only.

**Why planning is required:** High-risk per the adaptive router — **security-sensitive** (these keys gate proxy access; creating the first key flips the proxy from open-on-loopback to key-enforced — see `posture.rs`), and it exposes a secret (the raw key) exactly once.

**Acceptance (observable, live-verified):**
- `GET /api/keys` (admin-gated) lists every key **redacted** — `id, key_prefix, label, enabled, created_at, last_used_at` — and NEVER the `key_hash` or a raw key.
- `POST /api/keys` (admin-gated, optional `label`) creates a key and returns its **raw plaintext exactly once** (plus id + prefix); the raw value appears in no log and is never retrievable again.
- `PATCH /api/keys/{id}` (admin-gated) enables/disables a key (`set_enabled`); a disabled key is rejected by the proxy's `require_client_key`.
- The `/keys` page lists keys, creates one via a **show-once modal** (copy-to-clipboard, "you won't see this again"), and toggles enable/disable per row.
- Live: a key created via the API authenticates a real proxy request; after disabling it, the same request is rejected (401). Content-safe: no raw key / hash in any log. All touched crates green + clippy/fmt clean; wedge-sacred untouched.

---

### Outcome 1: HTTP endpoints `GET`/`POST /api/keys` + `PATCH /api/keys/{id}`
- Work: Add handlers (GET in `read_api.rs`, POST + PATCH in `write_api.rs`, matching the Settings/account-controls conventions) registered inside the `require_admin` router in `app.rs`. GET → `Vec<ApiKeyView { id, key_prefix, label, enabled, created_at, last_used_at }>` from `store.api_keys().list()` (NEVER emit `key_hash`). POST → `keys::create_key(&store, label, now)`, respond `{ id, key_prefix, key }` where `key` is the raw plaintext (the ONLY place it is ever returned); do not log it. PATCH `{ "enabled": bool }` → `store.api_keys().set_enabled(id, enabled)`; unknown id → 404. Admin-gated; content-safe (raw key only in the POST body, hash never leaves the store).
- Risks/open questions: content-safety of the POST response — the raw key must be in the response body only, never in a `tracing` line or `RequestLog`. Confirm `RequestLog` (already content-free) carries none of it.
- Verify: `cargo test -p polyflare-server` — integration tests: POST returns a raw key + persists a row (hash != raw); GET lists it redacted (no hash, no raw); PATCH disables it; keyless → 401; unknown-id PATCH → 404.

### Outcome 2: `/keys` frontend page
- Work: `api.ts` (`ApiKeyView` type + `api.keys()`/`createKey(label)`/`patchKey(id, {enabled})`), `queries.ts` (`useKeys` + `useCreateKey`/`useUpdateKey` on the account-controls mutation foundation — Toast + invalidate `["keys"]` + `mutationErrorText`), `Sidebar.tsx` (promote "API Keys" from `SOON_ITEMS`), `App.tsx` (route), new `pages/Keys.tsx`. The page: a table (prefix, label, enabled badge, created, last-used), a "Create key" action opening a **show-once modal** that displays the raw `key` returned by POST with copy-to-clipboard and a clear "this is the only time you'll see it" warning, and a per-row enable/disable toggle (Radix `Switch` or an `ActionMenu` "Disable/Enable"). A small note when the list is empty that creating the first key enables client-key enforcement. ccflare skin, no emoji, tabular-nums; rebuild + commit `dist/`.
- Risks/open questions: the raw key must live only in React state for the modal's lifetime — never persisted, never in a query cache that refetches. The `useKeys` list query must never carry the raw value.
- Verify: `bunx tsc -b` exit 0; `bun run build` clean; `dist/` rebuilt+committed (git show --stat).

### Outcome 3: live-verify (verification-before-completion)
- Work: Against a copy of the real store (loopback), with an admin token: `POST /api/keys` → capture the raw key; use it as the client bearer on a real proxy request (`/v1/messages` aliased or `/responses`) → expect success (the key authenticates once enforcement is on); `PATCH` disable it → the same request now returns 401; `GET /api/keys` shows it redacted + disabled. Confirm the server log contains no raw key / hash. Confirm wedge-clean (`git diff --name-only`) + latency gate green.
- Risks/open questions: creating a key flips posture to enforced — the probe must send the key thereafter; account-pool availability for the proxy call (pace probes).
- Verify: controller-run curl; grep the log for the raw key (expect zero); `git diff --name-only` shows no `watchdog.rs`/`continuity.rs`/`select.rs`.
