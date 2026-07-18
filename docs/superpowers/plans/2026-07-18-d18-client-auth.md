# D18 — Client API-Key Auth on the Proxy Surface Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use `- [ ]`.

**Goal:** Authenticate the CALLER on PolyFlare's proxy surface (`/responses`, `/v1/messages`, `/{pool}/…`) with
client API keys, so PolyFlare can run non-locally without leaving an anonymous quota-drain open. Minimal D18 =
"is this caller allowed at all" — a boolean gate. Per-key scoping/model-enforcement/usage-rollup are follow-ons.

**Architecture:** Mirror the existing `require_admin` pattern (`auth.rs:15-39`, wired `app.rs:187-190`): an
`AppState`-held store, a `from_fn_with_state` middleware, a `route_layer` on an EXTRACTED proxy sub-router. New:
an `api_keys` table + repo, a `require_client_key` middleware that sha256-hashes the presented Bearer and does an
indexed hash-lookup, a bind-address-aware default posture, and a `keys` CLI.

**Authority — the D18 scoping study (this session).** codex-lb refs: `modules/api_keys/service.py` (sha256
`:1273`, gen `:1269`, `validate_key` `:768`, reveal-once `:161`), `core/auth/dependencies.py:61-108` (validate +
the remote-refuse-when-unauthenticated `:76-79`), setting default False `db/models.py:699`.

## Global Constraints

- **STORE ONLY THE HASH — never the plaintext (inviolable).** Persist `sha256(raw_key)` hex + a display
  `key_prefix` (first ~15 chars). The raw key is revealed EXACTLY ONCE, at creation, printed to stdout by the
  CLI; it is never stored, never re-derivable, never in a list/get. sha256-without-salt is correct here BECAUSE
  the input is a 256-bit CSPRNG token (no rainbow/brute risk) — do NOT reach for bcrypt/argon2.
- **NEVER LOG THE CLIENT KEY (inviolable).** The presented `Authorization` value must never reach
  `request_log_repo`, the SSE live-log bus, or any `tracing` line. Audit/log by `key_id`/`key_prefix` only. A
  test must assert the raw key never appears in a log/request-log path.
- **HASH-LOOKUP VALIDATION, not plaintext `==`.** `require_client_key` hashes the presented token and does a
  repo `get_by_hash` (indexed) — do NOT copy `require_admin`'s plain-`==` compare onto raw client keys. The
  hash-lookup removes the timing side-channel on the token; constant-time compare is not additionally required
  (codex-lb doesn't use it either — same reasoning).
- **BIND-ADDRESS-AWARE DEFAULT POSTURE (the security-critical crux).** (1) any key exists ⇒ ENFORCE on the
  proxy surface. (2) no keys + LOOPBACK bind ⇒ open (today's zero-config local trust, preserved). (3) no keys +
  NON-LOOPBACK bind ⇒ REFUSE (startup error / per-request 401) UNLESS `POLYFLARE_ALLOW_UNAUTHENTICATED_REMOTE=1`,
  and emit a LOUD startup warning when that override is used. This closes "0.0.0.0 + no keys = anonymous." Do NOT
  port only codex-lb's permissive half — it ALSO refuses remote-when-unauthenticated. Document the fronting-proxy
  caveat (a reverse proxy makes the socket peer loopback while real callers are remote → the operator must opt
  into key enforcement explicitly).
- **The layer must NOT cover `/dashboard` static assets or the `/api/*` sub-router** (already gated by
  `require_admin`). The **GET-426 WS-fallback shim** (`websocket_fallback_handler`, app.rs:202,211) must stay
  reachable KEYLESS — a keyless WS probe must get 426 (handshake-degrade), NOT 401. Exempt it.
- The client key authenticates only the CALLER — it is DISTINCT from the upstream account bearer (PolyFlare
  injects the selected account token regardless), consumed at the layer, never forwarded upstream.
- The 5 wedge + cyber + failover + both starvation suites + all existing MUST stay green.
- Verify each task: `cargo test --workspace` green + `cargo clippy --workspace --all-targets -- -D warnings`.

---

### Task 1: `api_keys` table + store repo

**Files:** new migration `crates/polyflare-store/migrations/0009_api_keys.sql`; new
`crates/polyflare-store/src/api_key_repo.rs` (+ `lib.rs` export); tests.

**Schema** (forward-only; content-free — a key hash is not a secret-at-rest the way the token cipher is, but it
IS auth material, so treat the plaintext as never-persisted):
```sql
CREATE TABLE api_keys (
  id           TEXT PRIMARY KEY,          -- uuid
  key_hash     TEXT NOT NULL UNIQUE,      -- sha256 hex of the plaintext
  key_prefix   TEXT NOT NULL,             -- first ~15 chars, for display
  label        TEXT,
  enabled      INTEGER NOT NULL DEFAULT 1,
  created_at   INTEGER NOT NULL,
  last_used_at INTEGER                    -- nullable
);
CREATE UNIQUE INDEX idx_api_keys_key_hash ON api_keys(key_hash);
```

**Interfaces — Produces:** `ApiKeyRepo` with `create(id, key_hash, key_prefix, label, now)`,
`get_by_hash(hash) -> Option<ApiKeyRow>` (the hot validation path — indexed), `list() -> Vec<ApiKeyRow>` (no
hash exposed beyond prefix), `set_enabled(id, bool)` (revoke = set false), `touch_last_used(id, now)`. The
`ApiKeyRow` exposed to callers carries id/key_prefix/label/enabled/timestamps — NOT the raw key (never stored).

- [ ] **Step 1:** Failing tests: create + `get_by_hash` round-trips (found by hash, returns the row); a wrong
      hash ⇒ None; `set_enabled(id,false)` then `get_by_hash` still returns the row but `enabled==false` (the
      middleware, not the repo, enforces enabled); `list` shows prefix not a full key; `touch_last_used` updates.
- [ ] **Step 2:** Run — fail. **Step 3:** Implement migration + repo (mirror `account.rs`/`continuity_repo.rs`
      conventions — sqlx, the store's error type). **Step 4:** Green.
- [ ] **Step 5:** Commit: `feat(store): api_keys table + repo (hash-at-rest, reveal-once)`

---

### Task 2: Key generation + `keys` CLI (create/list/revoke, reveal-once)

**Files:** a key-gen helper (`crates/polyflare-server/src/` or `polyflare-store`); `crates/polyflare-server/src/main.rs`
(a `Keys { KeysCommands }` subcommand sibling to `Accounts`, main.rs:29-40); tests.

**Key format:** `sk-pf-<base64url(32 random bytes)>` — a 256-bit CSPRNG token (use a vetted RNG, e.g. `rand`'s
`OsRng` / `getrandom`). `key_hash = sha256_hex(raw)`; `key_prefix = raw[..15]`.

**CLI:**
- `polyflare keys create --label <s>` ⇒ generate, store (hash+prefix+label), **print the RAW key ONCE to stdout**
  with a "store this now, it won't be shown again" notice. The raw key must NOT be logged (only printed to
  stdout for the operator) — no `tracing`/eprintln of the raw value.
- `polyflare keys list` ⇒ id / prefix / label / enabled / created / last_used (never a raw key).
- `polyflare keys revoke --id <id>` ⇒ `set_enabled(id,false)`.

- [ ] **Step 1:** Failing tests: `create` generates a 256-bit key with the `sk-pf-` prefix, stores `sha256(raw)`
      + `raw[..15]`, and the returned/printed raw hashes to the stored hash; two creates yield DISTINCT keys;
      `list` never contains a full key; `revoke` disables. (Test the gen+store logic directly; a CLI-integration
      test if the harness supports it, else test the underlying functions + note it.)
- [ ] **Step 2:** Run — fail. **Step 3:** Implement gen + the three subcommands (mirror `AccountsCommands`
      `main.rs:40+`). **Step 4:** Green.
- [ ] **Step 5:** Commit: `feat(server): keys create/list/revoke CLI (reveal-once)`

---

### Task 3: `require_client_key` middleware (hash-lookup, enabled-check, never-log)

**Files:** `crates/polyflare-server/src/auth.rs` (add `require_client_key`) + `AppState` (the repo is already on
`state.store`); tests.

**Interfaces — Produces:** `pub async fn require_client_key(State<Arc<AppState>>, HeaderMap, Request, Next) -> Response`
mirroring `require_admin`'s signature. It: extracts `Authorization: Bearer <raw>`; `sha256_hex(raw)`;
`state.store.api_keys().get_by_hash(hash).await`; if a row exists AND `enabled` ⇒ `touch_last_used` (fire-and-
forget or awaited — decide, avoid blocking the hot path) and `next.run(req)`; else 401. **The posture decision
(Task 4) is passed IN** — this middleware is the "a key was required and here's whether it's valid" half; whether
a key is required at all is Task 4's bind-aware gate. Structure so Task 4 composes them (e.g. this middleware is
only installed when enforcement is on, OR it takes an "enforce" flag). **NEVER log the raw key** — not on the 401
path, not anywhere.

- [ ] **Step 1:** Failing tests: a valid enabled key ⇒ pass (and `last_used_at` updated); an unknown key ⇒ 401;
      a REVOKED (enabled=false) key ⇒ 401; a missing/malformed Authorization ⇒ 401; **content-safety:** the raw
      key never appears in any log/error (assert via a captured log or the error body — mirror the cyber
      message-leak test pattern).
- [ ] **Step 2:** Run — fail. **Step 3:** Implement (hash-lookup, NOT `==`). **Step 4:** Green; existing suites green.
- [ ] **Step 5:** Commit: `feat(server): require_client_key middleware (hash-lookup, never-log)`

---

### Task 4: Bind-address-aware posture + wire the proxy sub-router (THE CRUX — adversarial review)

**Files:** `crates/polyflare-server/src/app.rs` (extract the proxy routes into a sub-router + conditionally
layer), `config.rs` (`POLYFLARE_ALLOW_UNAUTHENTICATED_REMOTE`, the bind-addr already on `ServeConfig`),
`main.rs`/startup (the warning + the refuse); tests.

**The posture (Global Constraint, restated as the gate):** at startup, resolve `enforce_client_keys`:
- any key exists in the store ⇒ `enforce = true`.
- no keys + loopback bind (`127.0.0.1`/`::1`) ⇒ `enforce = false` (open, zero-config preserved).
- no keys + non-loopback bind ⇒ if `POLYFLARE_ALLOW_UNAUTHENTICATED_REMOTE=1` ⇒ `enforce = false` + LOUD startup
  warning ("proxy is UNAUTHENTICATED and bound non-locally"); else ⇒ REFUSE TO START (clear error: "bind is
  non-local but no client key exists; run `polyflare keys create` or set POLYFLARE_ALLOW_UNAUTHENTICATED_REMOTE=1").

**Wiring:** extract `/responses`, `/v1/messages`, `/{pool}/responses`, `/{pool}/v1/messages` (POST handlers) into
a `proxy` sub-router. When `enforce`, `.route_layer(require_client_key)`. **Exempt:** the GET-426 shims (a keyless
WS probe must still get 426, not 401 — put the GET shim on a route WITHOUT the layer, or exempt GET in the
middleware), `/dashboard` static assets, and `/api/*` (its own `require_admin`). `/models` GETs — decide: they're
low-risk metadata; recommend leaving them open (they leak no quota) but document it.

- [ ] **Step 1:** Failing tests (build `AppState`/router per the existing test pattern): (a) a key exists ⇒ a
      keyless `POST /responses` ⇒ 401, a valid-key one ⇒ reaches the handler. (b) no keys + loopback ⇒ keyless
      `POST /responses` ⇒ reaches the handler (open). (c) no keys + non-loopback + no override ⇒ startup refuses
      (assert the startup/config resolution errors). (d) no keys + non-loopback + override ⇒ open + (assert the
      warning is emitted). (e) **the 426 exemption:** a keyless GET `/responses` still returns 426 even when
      enforcement is on (not 401). (f) `/dashboard` + `/api/*` unaffected by the proxy layer.
- [ ] **Step 2:** Run — fail. **Step 3:** Implement the extraction + conditional layer + the startup posture.
      **Step 4:** Green; ALL suites green (the router refactor must not break existing proxy/e2e tests).
- [ ] **Step 5:** Commit: `feat(server): bind-aware client-key enforcement on the proxy surface (D18)`

---

### Task 5: Never-log verification + e2e + docs

**Files:** a content-safety test over `request_log_repo`/the SSE log path; `tests/` e2e; update
`docs/PORTING-CODEXLB.md` D18 (mark done, list the deferred per-key features), a short operator note (how to
create a key + run non-locally).

- [ ] **Step 1:** Failing tests: **content-safety** — drive a request WITH an `Authorization: Bearer sk-pf-SENTINEL`
      through the real stack and assert the SENTINEL never appears in the request log row / the SSE log event /
      any captured tracing (the request log must record the request content-free as always, and MUST NOT capture
      the client key). E2e: key-enforced pool ⇒ a valid key gets a clean proxied response, an invalid key gets
      401, both through the real `build_app`.
- [ ] **Step 2:** Run — fail. **Step 3:** Implement/confirm (if the request log already omits Authorization, the
      test just proves it; if it captures it, redact). Update the docs. **Step 4:** Green.
- [ ] **Step 5:** Commit: `test(server): D18 never-logs-the-key + e2e; docs`

---

## Suggested order

1 (table+repo) → 2 (gen+CLI) → 3 (middleware) → 4 (posture+wiring, the crux, adversarial review) → 5 (never-log
e2e + docs). After Task 5, minimal D18 is complete: the proxy surface can be key-authenticated, safe by default
on any bind. The per-key features (account/source scoping, enforced model/effort/tier, per-key usage rollup) are
explicit follow-ons, not in this milestone.
