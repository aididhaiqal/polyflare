# Relay + Catalog Fixes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Fix three issues the PolyFlare-vs-direct A/B surfaced — `/models` schema (raw pass-through), transient-429 retry-same-account, and mid-turn-cap replay.

**Architecture:** Fixes 1 (catalog) is on the always-on `/models` path; Fixes 2–3 are in the WS-downstream relay (behind `POLYFLARE_WS_DOWNSTREAM`, default off). All reuse existing engines; no new selection/continuity logic.

**Tech Stack:** Rust, tokio, axum, serde_json; `polyflare-server` (`catalog.rs`, `model_catalog.rs`, `ws_relay/{mod,pump}.rs`), `polyflare-testkit` (`ws_mock`).

## Global Constraints

- **Content-free (inviolable):** no conversation content logged/persisted. The Fix-3 in-flight buffer holds the raw `response.create` frame **in memory only** for replay — never logged, never persisted, dropped on turn completion/teardown. Model metadata (`/models`) is not conversation content. No `tracing`/`log`/`println!`/`eprintln!` of any frame body or the buffered frame anywhere in `ws_relay`.
- **Wedge-sacred:** `watchdog.rs`/`select.rs`/`continuity.rs`/`ObservingStream` byte-unchanged; continuity/selection/circuit-breaker reused via existing APIs.
- **Verbatim:** relayed/replayed frames byte-for-byte (no reparse); raw upstream `ModelInfo` forwarded unmodified.
- **Flag-gated:** all `ws_relay` pump changes behind `POLYFLARE_WS_DOWNSTREAM` (default off); HTTP-SSE + translation byte-unchanged. The `/models` change is additive (richer/superset response).
- `cargo clippy --workspace --all-targets -- -D warnings` clean; `cargo fmt --all -- --check` clean; full `cargo test -p polyflare-server` green.
- Authoritative `/models` schema: `codex-rs/protocol/src/openai_models.rs` `ModelInfo`. Reference impl: codex-lb `app/modules/model_sources/catalog.py`.

## File Structure

- `crates/polyflare-server/src/model_catalog.rs` — `UpstreamModel` gains `raw`; `parse_one_model` preserves the full entry.
- `crates/polyflare-server/src/catalog.rs` — Codex `/models` response emits raw `ModelInfo` verbatim; synthetic aliases cloned from the aliased-to raw entry.
- `crates/polyflare-server/src/ws_relay/mod.rs` — `on_upstream_error` transient-429 branch.
- `crates/polyflare-server/src/ws_relay/pump.rs` — in-flight buffer + replay.
- `crates/polyflare-server/tests/ws_downstream_relay.rs` — relay-through tests (Fixes 2, 3).

---

### Task 1: Preserve the raw upstream `ModelInfo` in `UpstreamModel`

**Files:** Modify `crates/polyflare-server/src/model_catalog.rs`. Test: inline `#[cfg(test)]`.

**Interfaces:**
- `UpstreamModel` gains `pub raw: serde_json::Value` — the full upstream entry as received. Existing fields (`slug`, `display_name`, `context_window`, `prefer_websockets`) stay (derived from `raw`, used by the OpenAI shape + merge/floor).
- `parse_one_model` sets `raw: entry.clone()` alongside the existing extracted fields.
- The compiled-in `floor` (search construction sites) sets `raw` — for floor entries with no real upstream JSON, `raw` is a minimal object `{"slug": <slug>, "display_name": <name>}` (NOT a full ModelInfo — the floor is only reached when the live fetch is unavailable, and codex falls back to its own bundle then; the floor must still construct without panicking).

- [ ] **Step 1: Failing test** — `parse_models` on a `{"models":[{"slug":"gpt-5.6-sol","display_name":"Sol","supported_reasoning_levels":[],"visibility":"list",...}]}` preserves the full entry: `parse_models(&j)[0].raw == j["models"][0]`.
- [ ] **Step 2: RED** (`cargo test -p polyflare-server model_catalog::`).
- [ ] **Step 3: Implement** — add `raw` field; `parse_one_model` clones the entry into `raw`; update every `UpstreamModel { .. }` construction site (floor, tests) to set `raw`.
- [ ] **Step 4: GREEN + clippy + fmt.**
- [ ] **Step 5: Commit** — `feat(catalog): preserve raw upstream ModelInfo entries in UpstreamModel`.

---

### Task 2: Emit raw `ModelInfo` verbatim in the Codex `/models` response

**Files:** Modify `crates/polyflare-server/src/catalog.rs`. Test: inline.

**Interfaces:**
- `CatalogModel` gains `raw: serde_json::Value` (carried from `UpstreamModel.raw`; for synthetic aliases, the constructed full entry). `catalog_model_from_upstream` copies `raw`.
- `to_codex_response`: the `models` array becomes `Vec<serde_json::Value>` (the raw `ModelInfo` entries) instead of the lossy `CodexModelEntry`. Emit each `CatalogModel.raw` verbatim. (Keep the OpenAI-shape `data` array as-is via `to_openai_items`.)
- **Synthetic alias raw construction** (`build_catalog`): when appending a synthetic alias whose `alias.target_model` matches a live model's slug, **clone that live model's `raw`**, then override in the clone: `slug` = alias id, `display_name` = alias display_name, and merge the alias metadata (`aliased_to`, `reasoning_effort`) into a top-level `metadata` object (or the existing convention). If the target model is NOT in the live set, set the alias's `raw` to a minimal object AND mark it so `to_codex_response` OMITS it from the `models` array (codex-parseable requires a full entry; a partial alias entry would break the parse — so omit rather than emit an invalid one). The alias still appears in the OpenAI `data` array (unchanged).

- [ ] **Step 1: Failing test** — `to_codex_response` over a live `gpt-5.6-sol` (full raw) + the `claude-opus-4-1` alias: (a) the `models` array's `gpt-5.6-sol` entry equals the raw upstream entry byte-for-byte; (b) the `claude-opus-4-1` entry is present with `slug=="claude-opus-4-1"`, `display_name` overridden, and the SAME `supported_reasoning_levels`/`shell_type`/etc. as `gpt-5.6-sol` (proving the clone); (c) an alias whose target is absent from the live set is OMITTED from `models` but present in `data`.
- [ ] **Step 2: RED.**
- [ ] **Step 3: Implement** the raw pass-through + alias clone/omit.
- [ ] **Step 4: GREEN + clippy + fmt.** Confirm the emitted `models` entries would deserialize as codex `ModelInfo` (assert the required-field set is present on the alias entry; a full raw upstream entry is valid by construction).
- [ ] **Step 5: Commit** — `feat(catalog): serve raw ModelInfo in /models so codex parses it; aliases cloned from target`.

---

### Task 3: Transient-429 retries the same account (no move)

**Files:** Modify `crates/polyflare-server/src/ws_relay/mod.rs`. Test: `tests/ws_downstream_relay.rs`.

**Interfaces:**
- Add `const TRANSIENT_RETRY_MAX_SECS: i64 = 30;` (the `RATE_LIMITED_MIN_COOLDOWN_SECS` boundary).
- In `on_upstream_error`, BEFORE `bench_account_for_failure`:
  ```rust
  if sig.status == 429 {
      if let Some(n) = sig.retry_after {
          if n <= TRANSIENT_RETRY_MAX_SECS {
              // Transient: wait it out on the SAME account, keep the conversation's cache.
              tokio::time::sleep(std::time::Duration::from_secs(n.max(0) as u64)).await;
              let up = redial_upstream(&headers, &current).await?;
              return Some((current, up)); // same account -> pump counts reconnect_same_account
          }
      }
  }
  // durable / no-retry-after / long: existing bench -> resolve_owner -> move path (unchanged)
  ```
- The pump's move/reconnect counter logic (Phase-3 Task 5) already records `reconnect_same_account` when the returned account id == current (retry-in-place) — so a transient retry is correctly counted, and `move_cross_account` is NOT bumped. No pump change needed.

- [ ] **Step 1: Failing test** `transient_429_retries_same_account_no_move`: two eligible accounts + a shared mock scripted `[rate_limited_429(5), normal(vec![])]`. Drive: client frame → 429 (retry_after 5) forwarded → assert the relay does NOT bench (both accounts still un-cooled in `state.runtime`), waits ~5s, and the follow-up completed turn's owner is the SAME account (no move); `reconnect_same_account >= 1`, `move_cross_account == 0`.
- [ ] **Step 2: RED.**
- [ ] **Step 3: Implement** the transient branch. Keep the durable path (existing `durable_error_moves_to_a_second_account` test) passing.
- [ ] **Step 4: GREEN** (both the new transient test AND the existing durable-move test) + clippy + fmt.
- [ ] **Step 5: Commit** — `feat(ws-relay): transient 429 retries the same account (waits retry-after, keeps cache) instead of moving`.

---

### Task 4: Mid-turn-cap replay of the in-flight frame

**Files:** Modify `crates/polyflare-server/src/ws_relay/pump.rs`. Test: `tests/ws_downstream_relay.rs`.

**Interfaces:**
- Add `let mut in_flight: Option<String> = None;` to `run_pump`.
- **Set** it in the client→backend arm: after `send_client_text` succeeds for a `Text` frame, `in_flight = Some(t.to_string())` (the raw frame just forwarded). (One in-flight turn per socket.)
- **Clear** it in the backend→client `Normal` arm when `sniff_completed_id` returns `Some` (the turn finished): `in_flight = None`.
- **Replay** after a same-account re-dial when a turn was in flight:
  - `ConnectionLimit` arm: after the eager `redial_upstream` succeeds, `if let Some(frame) = in_flight.clone() { if new_upstream.send_text(frame).await.is_err() { break; } }` — replay the buffered frame on the fresh socket. (Restructure so the re-dialed `WsConn` is in scope to send.)
  - Upstream-drop arm (`Ok(None)/Err`): today sets `upstream=None` and relies on the next client frame to re-dial. Extend: if `in_flight.is_some()`, eagerly `redial_upstream(same account)` and replay the buffered frame (the client isn't going to resend — it's waiting). On re-dial failure → break.
- Replay is bounded by the existing `MAX_RECONNECTS_WITHOUT_PROGRESS` (each replay is a re-dial → increments `reconnects_since_progress`; a turn that never completes tears down). Do NOT replay on a cross-account move (Fix unchanged: there the client full-resends).
- **Content-free:** `in_flight` is never logged; it's the raw client frame held in memory only, dropped on completion/teardown.

- [ ] **Step 1: Failing test** `mid_turn_cap_replays_inflight_frame`: mock scripted so the FIRST client frame gets a `connection_limit_reached` mid-turn (before any `response.completed`), then the re-dialed socket serves `normal`. Assert: the relay re-dials the same account AND **replays the client's original frame** (the mock's `raw_frames()` on the second socket shows the SAME frame bytes), and the client receives the `response.completed` — the turn completes with NO client resend and NO teardown. `reconnect_same_account >= 1`.
- [ ] **Step 2: RED.**
- [ ] **Step 3: Implement** the `in_flight` set/clear/replay. Keep all existing relay tests (reconnect, move, anchor-miss, counters) passing.
- [ ] **Step 4: GREEN + clippy + fmt** (full `cargo test -p polyflare-server`).
- [ ] **Step 5: Commit** — `feat(ws-relay): replay the buffered in-flight frame on mid-turn cap/drop (no ~290s stall)`.

---

### Task 5: Live/mock verification (controller-run)

Not a code task. After Tasks 1–4:
- [ ] **Fix 1 live:** real codex over PolyFlare — confirm codex NO LONGER logs `failed to refresh available models` and `/models` lists the models (curl `/models` → valid `ModelInfo` entries; codex `--json` stderr clean of the models error).
- [ ] **Fix 3 mock:** the mid-turn-cap replay test proves the buffered-frame replay; confirm via the suite.
- [ ] **Fix 2 mock:** the transient-429 test proves retry-same; confirm.
- [ ] **Content-safety audit** of the relay path (no frame/buffer content logged).
- [ ] Record results.

---

## Self-Review

- **Spec coverage:** Fix 1 (raw pass-through + alias clone/omit + floor) → T1/T2; Fix 2 (transient-429 wait+retry-same, ≤30s) → T3; Fix 3 (in-flight buffer + replay on cap/mid-drop, same-account only) → T4; verify → T5. ✓
- **Placeholders:** the alias-omit-when-target-absent and floor-minimal-raw are explicit decisions, not gaps. ✓
- **Type consistency:** `UpstreamModel.raw`/`CatalogModel.raw` (`serde_json::Value`), `TRANSIENT_RETRY_MAX_SECS`, `in_flight: Option<String>` used consistently; `FailureSignal`/`Account`/`redial_upstream` are existing. ✓
- **Wedge-sacred:** only `catalog.rs`/`model_catalog.rs` (Fix 1, non-relay) + `ws_relay/{mod,pump}.rs` (Fixes 2/3) change; engines reused. ✓
