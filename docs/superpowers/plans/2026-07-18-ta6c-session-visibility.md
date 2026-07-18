# TA6(c) — Session→Account Affinity Visibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use `- [ ]`.

**Goal:** Give the operator a read-only view of which account each known conversation session is currently
sticky/owned to — a `GET /api/sessions` endpoint + a dashboard "Sessions" page — so session→account affinity and
stickiness flow are visible. This is the observability half the user asked for ("to see which session account
being sticky to"). CONTENT-FREE: session-key hash + owner id/email + state + capabilities + timestamps only.

**Architecture:** Three thin layers, each mirroring an EXISTING pattern exactly. (1) one new `ContinuityRepo`
list query joining `continuity_sessions` LEFT JOIN `accounts`; (2) one `GET /api/sessions` read handler on the
auth-gated `/api/*` router mirroring `requests_handler` (limit/offset clamp, `{total, rows}` envelope,
`Response::ok`→200 / err→generic 500); (3) one `Sessions.tsx` dashboard page mirroring `Requests.tsx` (table,
StatusPill, 30s poll) + route + sidebar entry + `api.ts` interface + `queries.ts` hook + a rebuilt committed `dist/`.

**Authority — the TA6(c) scoping study (this session), file:line cites:**
- Data model: `continuity_sessions` (PK `session_key TEXT`) — `crates/polyflare-store/migrations/0002_continuity.sql:5-33`;
  `required_capabilities TEXT` added by `0008_session_capability.sql:18`. Columns: `session_key`, `key_strength`
  (`'hard'|'soft'`), `owning_account_id` (nullable FK→`accounts(id)` ON DELETE SET NULL), `anchor_response_id`,
  `last_input_fingerprint`, `last_input_count`, `reasoning_cache_ref`, `state`
  (`'fresh'|'anchored'|'reattaching'|'recover'`), `created_at`, `updated_at`, `last_activity_at` (INTEGER unix secs),
  `required_capabilities`. Index `idx_continuity_sessions_activity ON (last_activity_at)` (0002:19-20) — already
  backs `ORDER BY last_activity_at DESC` pagination; NO new index/migration needed.
- Row type: `SessionRow` `crates/polyflare-store/src/continuity_repo.rs:9-27` (+ `has_capability()` 30-36).
- Session key = ALWAYS `sha256_hex(...)` (`session_key.rs:131-149,168-174`) — one-way hash, never raw
  header/content ⇒ content-safe to surface as-is.
- Existing repo methods = point lookups only (`get_session` 55-61, `get_anchor_owner` 64-72); NO list query exists.
- Read-API idiom: `crates/polyflare-server/src/read_api.rs` — `requests_handler` + `RequestsQuery`
  (limit `clamp(1,1000)` default 100, offset `max(0)`, lines 526-556) + `RequestsView { total, rows }` (541-546) +
  `Response::ok/error` (896-920, err→generic 500). Route lines in `app.rs:195-214` (auth-gated sub-router).
- Content-safety test: `tests/read_api.rs:397-416` loops `["/api/accounts","/api/pools","/api/requests"]` asserting
  the seeded `"SECRET"` marker never appears — ADD `/api/sessions` to that loop.
- Dashboard: routes `App.tsx:9-15,44-49`; typed client `src/lib/api.ts` (interfaces mirror view structs exactly,
  1-11); hooks `src/lib/queries.ts` (`LIST_REFETCH_MS = 30_000`, list views poll, detail don't, 23,65-91); page
  pattern `Pools.tsx`/`Requests.tsx` (loading skeleton → error card → `PageHeader` → `Card`+`<table>` w/
  `TABLE_HEAD_CLASS`, tone pills; dark-ops tokens `bg-card`/`border-border`/`text-fg`/`text-accent`; NO emoji, icons
  from `ui/icons.ts`); content-safety notice precedent `Requests.tsx:714-718`. `dist/` IS committed
  (`dashboard.rs:11-14` rust-embed at compile time) → MUST `bun run build` + commit `dist/` diff.

## Global Constraints

- **CONTENT-FREE, inviolable.** Surface ONLY: `session_key` (the sha256 hash), `key_strength`, `owning_account_id`,
  `owner_email` (nullable), `state`, `required_capabilities` (content-free tag set), `created_at`, `updated_at`,
  `last_activity_at`. Do NOT surface `last_input_fingerprint`, `last_input_count`, `reasoning_cache_ref`, or
  `anchor_response_id` — content-free but out of scope (minimal view, no scope creep). NEVER a token/body.
- **LEFT JOIN, not INNER.** A session may have `owning_account_id IS NULL` (a `'fresh'` session that never
  completed a turn, or an account deleted → SET NULL). Those rows MUST still appear (owner_email = null). An INNER
  JOIN silently drops them — a correctness bug.
- **Behind the existing `/api/*` admin gate.** `/api/sessions` attaches to the SAME auth-gated sub-router as
  `/api/accounts` (`app.rs` `require_admin`) — it is operator observability, not proxy surface. Not open.
- **Read-only. No write path, no continuity/wedge/selection code touched.** The 5 wedge + cyber + failover +
  starvation suites MUST stay green (this only adds a SELECT + a handler + a page).
- **`{total, rows}` + pagination** (copy `/api/requests`, NOT `/api/pools`'s bare array) — sessions can grow large.
- **Dashboard `dist/` rebuild is a REQUIRED step**, not optional polish — Rust-only CI embeds the committed bundle;
  a stale `dist/` ships stale UI. Rebuild + commit `dist/` in the same task as the source change.
- Verify each task: `cargo test --workspace` green + `cargo clippy --workspace --all-targets -- -D warnings`.

---

### Task 1: Store — list sessions with owner email

**Files:** `crates/polyflare-store/src/continuity_repo.rs` (new method + a row struct); its existing `#[cfg(test)]`
module (mirror the seed-and-assert idiom already there).

**Interfaces — Produces:** a new row struct + method on `ContinuityRepo`:
```rust
#[derive(Debug, Clone)]
pub struct SessionWithOwner {
    pub session_key: String,
    pub key_strength: String,
    pub owning_account_id: Option<String>,
    pub owner_email: Option<String>,
    pub state: String,
    pub required_capabilities: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub last_activity_at: i64,
}
// on ContinuityRepo:
pub async fn list_sessions_with_owner(&self, limit: i64, offset: i64) -> Result<Vec<SessionWithOwner>, sqlx::Error>;
pub async fn count_sessions(&self) -> Result<i64, sqlx::Error>;
```
SQL for the list (LEFT JOIN so NULL-owner rows survive):
```sql
SELECT s.session_key, s.key_strength, s.owning_account_id, a.email AS owner_email,
       s.state, s.required_capabilities, s.created_at, s.updated_at, s.last_activity_at
FROM continuity_sessions s
LEFT JOIN accounts a ON a.id = s.owning_account_id
ORDER BY s.last_activity_at DESC
LIMIT ? OFFSET ?
```
`count_sessions` = `SELECT COUNT(*) FROM continuity_sessions`.

- [ ] **Step 1:** Failing tests in the repo's test module: seed 2 accounts + 3 sessions via the existing write
      methods (`ensure_session` etc.) — one session owned by acct A, one by acct B, one with NO owner (owning_account_id
      NULL, e.g. a fresh `ensure_session` that sets no owner — check what the write methods do; if none leave owner
      NULL, insert a raw fresh row or use the lowest-level path). Assert: (a) `list_sessions_with_owner(10,0)` returns
      all 3 ordered by `last_activity_at DESC`; (b) the acct-A session's `owner_email` == A's email, acct-B's ==
      B's; (c) the NO-owner session appears with `owner_email == None` and `owning_account_id == None` (proves LEFT
      not INNER); (d) `count_sessions()` == 3; (e) LIMIT/OFFSET paginates (limit 2 → 2 rows, offset 2 → the 3rd).
- [ ] **Step 2:** Run — fail. **Step 3:** Implement the struct + two methods (sqlx `query_as`/`FromRow` or manual
      row mapping — match how `SessionRow`/`get_session` maps at continuity_repo.rs:39-61). **Step 4:** Green.
- [ ] **Step 5:** Commit: `feat(store): list continuity sessions with owner email (LEFT JOIN accounts)`

---

### Task 2: Read endpoint — `GET /api/sessions`

**Files:** `crates/polyflare-server/src/read_api.rs` (a `SessionRowView` struct + `SessionsQuery` + `sessions_handler`);
`crates/polyflare-server/src/app.rs` (one route line on the auth-gated `/api/*` sub-router); `tests/read_api.rs`
(a shape test + ADD `/api/sessions` to the existing content-safety loop).

**Interfaces — Consumes:** Task 1's `ContinuityRepo::list_sessions_with_owner` + `count_sessions` (reach the repo
the same way other handlers reach their repos via `AppState` — check how `requests_handler` gets its
`request_log` repo and mirror it; the continuity repo is on the store, see how continuity is accessed elsewhere in
the server, e.g. `ingress.rs`). **Produces:** `GET /api/sessions?limit=&offset=` → `200 {total, rows:[SessionRowView]}`.

```rust
#[derive(serde::Serialize)]
struct SessionRowView {
    session_key: String,          // sha256 hash — opaque, content-free (see module doc)
    key_strength: String,
    owning_account_id: Option<String>,
    owner_email: Option<String>,
    state: String,
    required_capabilities: Option<String>,
    created_at: i64,
    updated_at: i64,
    last_activity_at: i64,
}
#[derive(serde::Serialize)]
struct SessionsView { total: i64, rows: Vec<SessionRowView> }
```
Handler mirrors `requests_handler`: `Query(q): Query<SessionsQuery>` with `limit = q.limit.unwrap_or(100).clamp(1,1000)`,
`offset = q.offset.unwrap_or(0).max(0)`; call the two repo methods; `Response::ok(SessionsView{..})`; any store err →
`Response::error()` (generic 500, never the store text). Add a one-line note to the read_api.rs module doc banner
(1-7) that `session_key` is a sha256 hash (content-free, not raw content — so a reviewer neither over- nor
under-redacts it).

- [ ] **Step 1:** Failing integration tests in `tests/read_api.rs` mirroring `accounts_endpoint_*`/`requests_endpoint_*`:
      (a) seed sessions+accounts through the real store, hit `GET /api/sessions` via the built app (through the
      admin gate like the other `/api/*` tests), assert 200 + the JSON has `total` + a `rows` array whose first row
      carries `session_key`, `state`, and the joined `owner_email`; (b) assert a NULL-owner session serializes
      `owner_email: null` (not dropped, not "null" string); (c) assert `limit`/`offset` are honored + clamped
      (limit 0 or 5000 → clamped). **Also ADD `"/api/sessions"` to the `no_secret_token_is_ever_present_in_a_read_response`
      loop at tests/read_api.rs:401** so the seeded SECRET marker is asserted absent from this endpoint too.
- [ ] **Step 2:** Run — fail. **Step 3:** Implement the view+query+handler + the `app.rs` route line (auth-gated
      router — confirm it lands INSIDE the `require_admin` group, not the open one). **Step 4:** Green; the 5 wedge/
      cyber/failover/starvation suites green (read-only change); `/api/sessions` doesn't shadow another route.
- [ ] **Step 5:** Commit: `feat(server): GET /api/sessions read endpoint (session→account affinity, content-free)`

---

### Task 3: Dashboard — Sessions page

**Files:** `crates/polyflare-server/dashboard/src/lib/api.ts` (a `SessionRowView`/`SessionsView` interface +
fetcher), `src/lib/queries.ts` (`useSessions()` hook, 30s poll like the other list views), a new
`src/pages/Sessions.tsx` (or wherever pages live — mirror `Pools.tsx`/`Requests.tsx` location), `src/App.tsx`
(route), the sidebar component (nav entry), then REBUILD + COMMIT `dist/`.

**Interfaces — Consumes:** `GET /api/sessions` from Task 2. **Produces:** a `/dashboard/sessions` page.

- TS interfaces mirror the Rust view field names/casing EXACTLY (api.ts convention, 1-11): `session_key`,
  `key_strength`, `owning_account_id`, `owner_email`, `state`, `required_capabilities`, `created_at`, `updated_at`,
  `last_activity_at`; `SessionsView { total: number; rows: SessionRowView[] }`.
- `useSessions()` mirrors `useRequests` (list view → `refetchInterval: LIST_REFETCH_MS`, a `queryKeys.sessions` key).
- `Sessions.tsx` mirrors `Requests.tsx`: loading skeleton → error card w/ retry → `PageHeader` (title "Sessions",
  subtitle e.g. "Which account each conversation is pinned to") → `Card`-wrapped `<table>` with `TABLE_HEAD_CLASS`
  columns: Session (truncated `session_key`, e.g. first 12 chars + monospace — it's a hash, truncation is display-
  only), Owner (`owner_email` or a muted "unowned" when null), State (a tone pill: anchored=success,
  reattaching/recover=warn, fresh=muted), Capabilities (`required_capabilities` or "—"), Last activity (relTime
  from `last_activity_at`). Include the content-safety inline notice mirroring `Requests.tsx:714-718` ("Session keys
  are one-way hashes; no conversation content is stored"). NO emoji; icons from `ui/icons.ts`.
- Derive "stale" client-side from `last_activity_at` if useful (a muted row/label past a threshold), mirroring the
  `WindowView.stale` precedent — optional, keep minimal.

- [ ] **Step 1:** Add the `api.ts` interface + fetcher and the `queries.ts` hook. **Step 2:** Build `Sessions.tsx`
      mirroring `Requests.tsx` structure + the route in `App.tsx` + the sidebar nav entry. **Step 3:** `cd
      crates/polyflare-server/dashboard && bun run build` — verify it emits a new `dist/assets/index-*.{js,css}` +
      `dist/index.html`. **Step 4:** `git add` the `src/**` changes AND the rebuilt `dist/**` (so Rust-embed CI
      serves the new bundle). Sanity: `cargo build -p polyflare-server` compiles (rust-embed picks up dist). The
      workspace test suite stays green (no Rust logic changed beyond Task 2).
- [ ] **Step 5:** Commit: `feat(dashboard): Sessions page — session→account affinity view (+ rebuilt dist)`

---

## Suggested order

1 (store list query) → 2 (read endpoint) → 3 (dashboard page + dist rebuild). After Task 3, an operator can open
`/dashboard/sessions` and see every known session, which account it's pinned to (or that it's unowned), its state,
any required capabilities (the TA6(b) sticky-cyber tag), and last activity — the session-stickiness visibility the
user asked for, content-free and behind the admin gate. Update `PORTING-CODEXLB.md` (mark TA6(c) done).
Follow-ups (not this plan): a session DETAIL view (anchor history), "active only" server-side filter, live SSE
session updates.
