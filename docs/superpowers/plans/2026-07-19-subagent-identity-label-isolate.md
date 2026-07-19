# Sub-agent Identity: Label + Socket-Isolate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Parse codex's sub-agent identity (`x-openai-subagent`) as a content-free label, surface it across request_log + live logs + dashboard, and fold the thread-unique `x-codex-window-id` into the WS `conn_key` so no sub-agent (or interleaved thread) can ever share a socket regardless of which `session_key` branch fired.

**Architecture:** Ingress already forwards codex's real inbound identity headers untouched on the native path (`ingress.rs:227 forward_headers_from_inbound`). We extract two content-free facts at parse time — `subagent` (the `x-openai-subagent` role slug) and `window_id` (`x-codex-window-id`, `{thread_id}:0`) — thread them through `RequestCtx`, use `window_id` as an extra `conn_key` component (transport-only, ownership-blind), and carry `subagent` into `RequestLog` → store row + log bus + read API + dashboard.

**Tech Stack:** Rust (axum, sqlx/sqlite), React+TypeScript dashboard (`crates/polyflare-server/dashboard`).

## Grounding (live capture 2026-07-19, `codex exec review --uncommitted`)

Real `review` sub-agent requests carried: `x-openai-subagent="review"`, `x-codex-window-id="019f7a3b-…:0"`, `x-codex-parent-thread-id="019f7a3b-…"`, `x-codex-turn-state` ABSENT, body `prompt_cache_key="019f7a3b-…"` (= the review thread's own id). The main agent sends NO `x-openai-subagent`. So a sub-agent is its own thread with its own `prompt_cache_key` → already a distinct `session_key` → already an isolated socket. The `window_id` fold is defense-in-depth that closes the one branch where `session_key` drops `prompt_cache_key` (the `x-codex-turn-state` branch, `session_key.rs:135-139`) — socket isolation then never depends on which branch fired. The `x-openai-subagent` value is a bounded role slug (`review` | `compact` | `memory_consolidation` | `collab_spawn`, or a rare free-form `Other(label)`) — content-free routing metadata, same tier as `model`/`path`.

## Global Constraints

- **Content-free:** never persist or log conversation content. `subagent` (a bounded role slug) and `window_id` (a `{uuid}:0` thread id) are routing metadata — storable like `model`/`path`. Any value used in a KEY is hashed (`sha256_hex`), never stored raw in the key. Never log a request body, frame, token, or bearer.
- **Wedge sacred:** do not touch `ObservingStream::poll_next`, continuity/ownership recording, or `delta.rs`. `conn_key` is transport-only and ownership-blind (proven in the WS-cache branch's whole-branch review) — keep it that way.
- **conn_key back-compat:** when `window_id` is absent (non-codex client, translated/alias path), the `conn_key` MUST be byte-identical to today's `session_key:fingerprint` form — the M5a WS caching that was just live-verified must not regress.
- **No emoji** anywhere in UI or logs.
- **Clippy clean under `-D warnings`; `cargo fmt --all`.** Tell every implementer clippy runs with `-D warnings` in CI.
- **Dashboard:** match the existing ccflare-skin styling; no new heavy deps.

## Interfaces produced (consumed by later tasks)

- `polyflare_core::RequestCtx` gains: `pub subagent: Option<String>` (role slug label; `None` = main agent) and `pub conn_discriminator: Option<String>` (the raw `x-codex-window-id`, or `None`).
- `polyflare_server::session_key::parse_inbound` populates both from the `HeaderMap`.
- `polyflare_server::observability::RequestLog` gains `pub subagent: Option<String>`; `RequestLogRecord` + `LogEvent` gain `subagent: Option<String>`.
- `request_log` table gains a nullable `subagent TEXT` column (migration `0011`).

---

### Task 1: Parse sub-agent identity into RequestCtx

**Files:**
- Modify: `crates/polyflare-core/src/types.rs` (RequestCtx struct, ~125-134)
- Modify: `crates/polyflare-server/src/session_key.rs` (`parse_inbound`, ~183-215)
- Test: unit tests in `session_key.rs`

**Interfaces:**
- Produces: `RequestCtx.subagent: Option<String>`, `RequestCtx.conn_discriminator: Option<String>`.

- [ ] **Step 1: Write failing tests** in `session_key.rs` `mod tests`:

```rust
#[test]
fn subagent_and_window_id_are_extracted() {
    let ctx = ctx_of(
        &hdr(&[("x-openai-subagent", "review"), ("x-codex-window-id", "tid-1:0")]),
        serde_json::json!({"input": "hi"}),
    );
    assert_eq!(ctx.subagent.as_deref(), Some("review"));
    assert_eq!(ctx.conn_discriminator.as_deref(), Some("tid-1:0"));
}

#[test]
fn main_agent_has_no_subagent_label() {
    // No x-openai-subagent header (Cli/Exec main agent) => None.
    let ctx = ctx_of(&hdr(&[("x-codex-window-id", "tid-1:0")]), serde_json::json!({"input": "hi"}));
    assert_eq!(ctx.subagent, None);
    assert_eq!(ctx.conn_discriminator.as_deref(), Some("tid-1:0"));
}
```

- [ ] **Step 2: Run to verify fail** — `cargo test -p polyflare-server session_key` → FAIL (unknown field `subagent`).

- [ ] **Step 3: Add fields to `RequestCtx`** (`polyflare-core/src/types.rs`), documented as content-free routing metadata:

```rust
    pub input_count: u32,
    /// The codex sub-agent role slug from `x-openai-subagent` (`review`/`compact`/…), or `None`
    /// for the main agent. Content-free routing metadata (a bounded role label), never conversation
    /// content — used for observability labeling only.
    pub subagent: Option<String>,
    /// The thread-unique `x-codex-window-id` (`{thread_id}:0`), or `None`. Content-free. Folded into
    /// the WS connection key so each codex thread gets its own socket regardless of which
    /// `session_key` branch fired; NEVER used for ownership/continuity.
    pub conn_discriminator: Option<String>,
```

- [ ] **Step 4: Populate in `parse_inbound`** (`session_key.rs`), reusing the existing `header_str`:

```rust
    let ctx = RequestCtx {
        session_id,
        session_key: Some(session_key),
        client_previous_response_id: raw_as_str(field("previous_response_id")),
        is_full_resend,
        input_count,
        subagent: header_str(headers, "x-openai-subagent"),
        conn_discriminator: header_str(headers, "x-codex-window-id"),
    };
```

Update EVERY other `RequestCtx { .. }` literal in the workspace to set the two new fields (grep `RequestCtx {`); prefer `..Default::default()` only if `RequestCtx` already derives `Default` — it likely does NOT, so set them explicitly to `None`.

- [ ] **Step 5: Run tests** — `cargo test -p polyflare-server session_key` and `cargo test -p polyflare-core` → PASS. `cargo build --workspace` (find all RequestCtx literals). Clippy `-D warnings`, fmt.

- [ ] **Step 6: Commit** — `feat(ingress): parse x-openai-subagent + x-codex-window-id into RequestCtx (content-free)`.

---

### Task 2: Fold the thread discriminator into the WS conn_key

**Files:**
- Modify: `crates/polyflare-codex/src/ws/executor.rs` (`execute`, conn_key computation ~533-541; and the module doc `# The connection cache`)
- Test: `executor.rs` unit test

**Interfaces:**
- Consumes: `RequestCtx.conn_discriminator` (Task 1). Access via the same `ctx` the executor already reads `session_key` from.

**Context:** Today `conn_key = session_key.value + ":" + non_input_fingerprint(body)` (just merged, live-verified). This task appends the hashed `conn_discriminator`: `conn_key = "{session}:{fingerprint}:{disc_hash}"`. When `conn_discriminator` is `None`, append NOTHING (byte-identical to today — back-compat constraint).

- [ ] **Step 1: Write failing test** in `executor.rs` `mod tests` (model the existing `interleaved_models_same_session_get_distinct_sockets_and_each_still_caches` harness). Two requests with the SAME `session_key` AND SAME model/body (same `non_input_fingerprint`) but DIFFERENT `x-codex-window-id` (→ different `conn_discriminator`) must get DISTINCT sockets (2 handshakes); and two requests identical in all three (same session, fingerprint, window-id) must REUSE one socket (1 handshake). Name it `distinct_window_ids_same_session_and_model_get_distinct_sockets`.

- [ ] **Step 2: Run to verify fail** — `cargo test -p polyflare-codex --lib ws` → the new test FAILs (currently shares one socket → 1 handshake, expected 2).

- [ ] **Step 3: Fold the discriminator in `execute`.** Where `conn_key` is built:

```rust
    let conn_key = session_key.as_ref().map(|sk| {
        let base = format!("{sk}:{}", crate::ws::delta::non_input_fingerprint(&body));
        match ctx.conn_discriminator.as_deref() {
            Some(disc) => format!("{base}:{}", crate::ws::delta::sha256_hex_str(disc)),
            None => base, // back-compat: no discriminator => byte-identical to the pre-task key
        }
    });
```

Use the existing content-free hash helper (`non_input_fingerprint` uses one — reuse the same `sha256` hex helper it calls; do NOT log `disc` raw). If no shared hex helper is exported from `delta.rs`, hash via the already-used `sha2` path rather than adding a dep. `ctx.conn_discriminator` must be in scope where `session_key`/`body` already are.

- [ ] **Step 4: Update the `# The connection cache` module doc** to state the key is `session:fingerprint:window_id` and why (thread isolation independent of the `session_key` branch).

- [ ] **Step 5: Run tests** — `cargo test -p polyflare-codex --lib ws` (new test + existing `interleaved_models…` + reuse + delta/recovery all PASS), `cargo test -p polyflare-codex --lib` incl `wedge_regression`. Clippy `-D warnings`, fmt.

- [ ] **Step 6: Commit** — `feat(ws): fold x-codex-window-id into conn_key so each codex thread gets its own socket`.

---

### Task 3: Persist + stream the sub-agent label

**Files:**
- Create: `crates/polyflare-store/migrations/0011_request_log_subagent.sql`
- Modify: `crates/polyflare-store/src/request_log_repo.rs` (RequestLogRecord ~33 + read row ~76 + INSERT + SELECT SQL)
- Modify: `crates/polyflare-server/src/observability.rs` (RequestLog ~32, `record()` ~84, `to_log_event()`)
- Modify: `crates/polyflare-server/src/log_bus.rs` (LogEvent ~45)
- Modify: `crates/polyflare-server/src/ingress.rs` (both `RequestLog { .. }` set-sites: ~1502, ~2142)
- Modify: `crates/polyflare-server/src/read_api.rs` (request_log read row ~604 + SELECT mapping ~687)
- Test: store round-trip test + observability content-safety test

**Interfaces:**
- Consumes: `RequestCtx.subagent` (Task 1).
- Produces: `subagent` on `RequestLogRecord`, `LogEvent`, and the read API row.

- [ ] **Step 1: Migration** `0011_request_log_subagent.sql`:

```sql
-- Sub-agent role label (x-openai-subagent: review/compact/…) — content-free routing metadata.
ALTER TABLE request_log ADD COLUMN subagent TEXT;
```

- [ ] **Step 2: Failing store test** in `request_log_repo.rs`: insert a `RequestLogRecord` with `subagent: Some("review".into())`, read it back, assert the field round-trips. Run → FAIL (no such column / field).

- [ ] **Step 3: Add `subagent: Option<String>`** to `RequestLogRecord` (both the write struct ~33 and the read struct ~76), the INSERT column list + bind, and the SELECT column list + `FromRow` mapping. Follow the EXACT pattern of the adjacent `transport` column (it is the nearest precedent for a nullable TEXT request-metadata field).

- [ ] **Step 4: Thread through observability + ingress + log bus + read API:**
  - `observability.rs`: add `pub subagent: Option<String>` to `RequestLog`; set it in `record()` and `to_log_event()` (mirror `transport`/`model`). Re-affirm the content-safety doc-comment note: `subagent` is a bounded role slug, not conversation content.
  - `log_bus.rs`: add `pub subagent: Option<String>` to `LogEvent`.
  - `ingress.rs`: at BOTH set-sites, capture `let subagent = ctx.subagent.clone();` (or `header_str(&headers, "x-openai-subagent")` BEFORE `headers` is moved into `responses_handler_impl`) and set `subagent` on the `RequestLog`. Prefer carrying it on the existing `outcome` struct if that is how `model`/`account_id` already arrive at the set-site; otherwise extract from `&headers` pre-move. Do NOT clone the whole body/headers.
  - `read_api.rs`: add `subagent` to the request_log read row struct + SELECT mapping so `GET /api/requests` returns it.

- [ ] **Step 5: Content-safety test** in `observability.rs`: assert `RequestLog::record()`/`to_log_event()` carry ONLY the audited field set + the new `subagent`, and that a raw body/token never appears (extend the existing content-safety test if present).

- [ ] **Step 6: Run** — `cargo test -p polyflare-store request_log`, `cargo test -p polyflare-server observability read_api`, `cargo build --workspace`. Clippy `-D warnings`, fmt. Confirm the migration applies on a fresh DB (an existing store test harness runs migrations).

- [ ] **Step 7: Commit** — `feat(obs): persist + stream x-openai-subagent label (migration 0011 + read API)`.

---

### Task 4: Surface the sub-agent label in the dashboard

**Files:**
- Modify: `crates/polyflare-server/dashboard/src/lib/api.ts` (request-row + log-event types: add `subagent?: string | null`)
- Modify: `crates/polyflare-server/dashboard/src/pages/Requests.tsx` (request_log table: show a sub-agent badge/column)
- Modify: `crates/polyflare-server/dashboard/src/pages/LiveLogs.tsx` (live stream row: show the sub-agent tag)
- Build: `crates/polyflare-server/dashboard` (`npm run build` → `dist/`)

**Interfaces:**
- Consumes: the read API `subagent` field (Task 3) + the live-log `subagent` field.

- [ ] **Step 1: Types** — add `subagent?: string | null` to the request-row and log-event TS types in `api.ts`.

- [ ] **Step 2: Requests table** — in `Requests.tsx`, render `subagent` as a small text badge next to `model` (fall back to `main` or an em-dash when null). Match the existing `ProviderTag`/`StatusPill` styling; NO emoji.

- [ ] **Step 3: Live logs** — in `LiveLogs.tsx`, show the sub-agent tag on each row where present (same styling).

- [ ] **Step 4: Build** — `cd crates/polyflare-server/dashboard && npm run build`; confirm `dist/` rebuilt with no type errors. If the workspace embeds `dist/` via `rust-embed`/`include_dir`, ensure the rebuilt assets are the ones served.

- [ ] **Step 5: Commit** — `feat(dashboard): show x-openai-subagent label in Requests + LiveLogs`.

---

### Task 5: Live-verify (controller-run, operational)

Not a code task — the controller runs it after Tasks 1-4 merge-ready. Success criteria:

- [ ] Start `polyflare serve` (HTTP path is enough for the label + session/conn keying; also spot-check with `POLYFLARE_WS_UPSTREAM=1`). Run a normal `codex exec "…"` (main agent) AND a `codex exec review --uncommitted` (review sub-agent) through the harness.
- [ ] Confirm from `GET /api/requests` (or request_log): the review requests are labeled `subagent="review"`; the main-agent requests have `subagent=null`.
- [ ] Confirm (temporary WSDBG-style `conn_key` log, reverted after) that the review sub-agent's requests resolve a DIFFERENT `conn_key` / socket than the main agent's — i.e., separate sockets, no shared anchor chain.
- [ ] Confirm the dashboard Requests + LiveLogs views show the `review` badge.
- [ ] Revert any temporary instrumentation; tree clean.

---

## Self-Review checklist (controller, before Task 1)

- Spec coverage: parse (T1) → isolate (T2) → persist/stream (T3) → dashboard (T4) → verify (T5). ✓
- Back-compat: T2 Step 3 `None` branch keeps `conn_key` byte-identical — guards the just-merged WS caching. ✓
- Content-safety: only bounded role slug + `{uuid}:0` stored; keys hashed. ✓
- Type consistency: `subagent: Option<String>` / `conn_discriminator: Option<String>` names identical across core → server → store → TS (`subagent?: string | null`). ✓
