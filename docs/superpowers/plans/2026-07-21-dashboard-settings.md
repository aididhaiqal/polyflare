# Live-Editable Settings Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** A `/settings` page that live-edits 10 curated config tunables (persisted + applied to the running proxy on the next request/job, no restart), with all other config shown read-only.

**Architecture:** A `RuntimeSettings` holder of atomics on `AppState` backs the 10 live fields; their `state.<field>` reads are re-routed to it in modifiable files (never the wedge-sacred watchdog/continuity/select). A new `settings` table persists overrides; `from_env` clamps are factored into pure `clamp_<field>` fns shared by boot and the PATCH handler. `GET`/`PATCH /api/settings` expose it.

**Tech Stack:** Rust (`polyflare-server` config/AppState/endpoints, `polyflare-store` settings repo; `std::sync::atomic`), React 18 + TS + react-query, bun.

## Global Constraints
- **Wedge-sacred:** NEVER read-for-logic-change or modify `crates/polyflare-server/src/watchdog.rs`, `crates/polyflare-core/src/continuity.rs`, or any `select.rs`. The 10 live fields are read only in modifiable files (verified); `stream_idle_timeout` is re-routed at its ingress caller.
- **Content-free; admin-gated** (existing `require_admin`). `admin_token` is NEVER returned as a value.
- **Behavior-identical at boot** when the `settings` table is empty (persisted > env only when a row exists; each overlaid value re-clamped).
- **The 10 live fields + atomics:** `max_account_attempts`(u32/AtomicU32), `starvation_wait_budget`(secs/AtomicU64), `starvation_heartbeat`(secs/AtomicU64, ≤budget), `wake_jitter_ms`(u64/AtomicU64), `stream_idle_timeout`(secs/AtomicU64), `inflight_penalty_pct`(f64 via `to_bits`/AtomicU64), `soft_drain_enabled`(bool/AtomicBool), `request_log_retention_days`(u32/AtomicU32), `usage_history_retention_days`(u32/AtomicU32), `live_logs`(bool/AtomicBool). `Relaxed` ordering.
- Clippy `--all-targets -D warnings`, fmt, `cargo test` green across touched crates; dashboard `bun run build` clean; tracked `dist/` rebuilt+committed per frontend commit.

---

### Task 1: Factor pure `clamp_<field>` fns out of `config.rs`

**Files:** Modify `crates/polyflare-server/src/config.rs` (the `*_from_env` helpers for the 10 fields). Test: inline `#[cfg(test)]`.

**Interfaces produced:** pure fns, each taking an already-parsed raw value and applying the SAME bounds the `*_from_env` helper currently applies inline:
`clamp_max_account_attempts(u32)->u32`, `clamp_starvation_wait_budget_secs(u64)->u64`, `clamp_starvation_heartbeat_secs(raw:u64, budget_secs:u64)->u64`, `clamp_wake_jitter_ms(u64)->u64`, `clamp_stream_idle_timeout_secs(u64)->u64`, `clamp_inflight_penalty_pct(f64)->f64`, `clamp_request_log_retention_days(u32)->u32`, `clamp_usage_history_retention_days(u32)->u32`. (`soft_drain_enabled`/`live_logs` are bools — no clamp.)

- [ ] **Step 1: Failing tests** — assert each clamp reproduces the existing bound. Copy the exact bounds FROM the current `*_from_env` body (e.g. `clamp_max_account_attempts(0)==1`, `clamp_max_account_attempts(7)==7`; `clamp_inflight_penalty_pct(-1.0)==0.0`, `clamp_inflight_penalty_pct(99.0)==50.0`, `clamp_inflight_penalty_pct(f64::NAN)==` the helper's NAN fallback `2.5`; `clamp_starvation_heartbeat_secs(999, 60)==60`; and the analogous edges for budget/jitter/idle/retention using whatever bounds those helpers currently hardcode — READ each helper first and mirror it).
- [ ] **Step 2: RED** — `cargo test -p polyflare-server config`.
- [ ] **Step 3: Implement** — for each of the 8 numeric helpers, MOVE the inline clamp/normalize logic into `clamp_<field>(raw)` (verbatim: the same constants, the same `0→1` / `clamp(lo,hi)` / NAN-fallback rules), and rewrite the `*_from_env` helper to `= clamp_<field>(parsed_or_default)`. The env-parse + default-on-error stays in `*_from_env`; only the bound logic moves. Behavior of `*_from_env` must be UNCHANGED (existing config tests still pass).
- [ ] **Step 4: GREEN** — `cargo test -p polyflare-server config` + full `cargo test -p polyflare-server`; clippy `--all-targets` + fmt clean.
- [ ] **Step 5: Commit** — `refactor(config): extract pure clamp_<field> fns (shared by boot + settings PATCH)`.

---

### Task 2: `RuntimeSettings` holder

**Files:** Create `crates/polyflare-server/src/runtime_settings.rs`; declare `pub mod runtime_settings;` in `crates/polyflare-server/src/lib.rs`. Test: inline.

**Interfaces:**
- Consumes: the `clamp_<field>` fns (Task 1).
- Produces: `pub struct RuntimeSettings { /* 10 atomics */ }` with:
  - a constructor `RuntimeSettings::new(cfg: &ServeConfig) -> Self` seeding each atomic from the (already-clamped) config value (durations → `.as_secs()`, f64 → `to_bits`).
  - a typed getter per field returning the live value: `max_account_attempts()->u32`, `starvation_wait_budget()->Duration`, `starvation_heartbeat()->Duration`, `wake_jitter_ms()->u64`, `stream_idle_timeout()->Duration`, `inflight_penalty_pct()->f64` (`f64::from_bits(load)`), `soft_drain_enabled()->bool`, `request_log_retention_days()->u32`, `usage_history_retention_days()->u32`, `live_logs()->bool`. All `Relaxed`.
  - `pub fn set(&self, key: &str, raw: SettingValue) -> Result<(), SettingsError>` where `SettingValue` is a small enum (`U64(u64)`/`F64(f64)`/`Bool(bool)`); it clamps via `clamp_<field>` (heartbeat uses the CURRENT budget), enforces the field↔kind match (wrong kind → `SettingsError::WrongKind`), rejects an unknown/non-live key (`SettingsError::UnknownKey`), and stores the clamped value. Returns the clamped value applied (so callers can persist exactly what was stored) — make `set` return `Result<u64-or-typed, _>` or expose a `get_raw(key)->String` for the persist step; simplest: `set` returns `Result<String, SettingsError>` = the stored value stringified.

- [ ] **Step 1: Failing tests** — `RuntimeSettings::new` from a config seeds getters; `set("max_account_attempts", U64(0))` clamps to 1 and `max_account_attempts()==1`; `set("inflight_penalty_pct", F64(99.0))` → `inflight_penalty_pct()==50.0`; `set("starvation_heartbeat", U64(9999))` clamps to the current budget; `set("live_logs", Bool(true))` flips the flag; `set("unknown", ..)` → `UnknownKey`; `set("live_logs", U64(1))` → `WrongKind`; the `f64` round-trips through bits.
- [ ] **Step 2: RED** — `cargo test -p polyflare-server runtime_settings`.
- [ ] **Step 3: Implement** the struct + getters + `set` (a `match key { ... }` mapping each live key to its atomic + `clamp_<field>` + kind check). Cross-field: `starvation_heartbeat` clamps against `self.starvation_wait_budget().as_secs()`.
- [ ] **Step 4: GREEN** — `cargo test -p polyflare-server runtime_settings` + full crate; clippy `--all-targets` + fmt.
- [ ] **Step 5: Commit** — `feat(settings): RuntimeSettings atomic holder for the 10 live fields`.

---

### Task 3: `settings` table + `SettingsRepo`

**Files:** Create `crates/polyflare-store/migrations/0013_settings.sql`, `crates/polyflare-store/src/settings_repo.rs`; wire into `crates/polyflare-store/src/store.rs` (`pub fn settings(&self) -> SettingsRepo`) + `lib.rs` (`pub use`).

**Interfaces:** `pub struct SettingsRepo`; `pub async fn get_all(&self) -> Result<HashMap<String,String>, StoreError>`; `pub async fn set(&self, key: &str, value: &str, now: i64) -> Result<(), StoreError>` (UPSERT `ON CONFLICT(key) DO UPDATE SET value=?, updated_at=?`).

- [ ] **Step 1: Failing test** — `set("live_logs","true", 100)` then `get_all()["live_logs"]=="true"`; a second `set(...,"false", 200)` overwrites; `get_all()` on an empty store is empty. Mirror the `api_key_repo`/`request_log_repo` test harness (tempdir `Store::open`).
- [ ] **Step 2: RED** — `cargo test -p polyflare-store settings`.
- [ ] **Step 3: Implement** the migration (`CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at INTEGER NOT NULL);` + a one-line content-free header comment) + the repo.
- [ ] **Step 4: GREEN** — `cargo test -p polyflare-store` (migrations run on open); clippy `--all-targets` + fmt.
- [ ] **Step 5: Commit** — `feat(store): settings table + SettingsRepo (persist config overrides)`.

---

### Task 4: Wire `RuntimeSettings` into `AppState` + startup overlay + re-route the 10 reads

**Files:** Modify `crates/polyflare-server/src/app.rs` (`AppState` struct + `build_app`), `crates/polyflare-server/src/main.rs` (`serve` — build `RuntimeSettings`, overlay persisted), and the read sites: `ingress.rs`, `retention.rs`, `usage_refresh.rs`, `sse.rs`, `auth.rs`, `control.rs`. **Do NOT touch `watchdog.rs`/`continuity.rs`/`select.rs`.**

**Interfaces:** Consumes `RuntimeSettings` (Task 2), `SettingsRepo` (Task 3). Produces `AppState.runtime_settings: Arc<RuntimeSettings>`; the 10 former `AppState` fields are REMOVED (their value now comes from the holder).

- [ ] **Step 1: Write/extend a test** — an AppState/serve-level test (reuse the crate's test harness / `responses_handler_impl_for_test`) asserting that after building state with a persisted `settings` row (e.g. `max_account_attempts=7`), `state.runtime_settings.max_account_attempts()==7` (overlay beats the env default); and with no row, it equals the env/clamped default. If a full serve test is heavy, unit-test the small overlay helper (below) directly.
- [ ] **Step 2: RED**.
- [ ] **Step 3: Implement:**
  - `AppState`: add `pub runtime_settings: Arc<RuntimeSettings>`; REMOVE the 10 now-holder-backed fields.
  - In `main.rs::serve`: after `ServeConfig::from_env()` and opening the store, build `let runtime_settings = Arc::new(RuntimeSettings::new(&config));` then `let overlay = store.settings().get_all().await?;` and for each key in `overlay`, `let _ = runtime_settings.set(key, parse_setting_value(key, &val));` (ignore unknown/invalid persisted rows — log content-free). Put `overlay_persisted_settings(&runtime_settings, &overlay)` as a small pure helper (testable). Thread `runtime_settings` into `AppState`.
  - Re-route each of the 10 reads (from the feasibility map): e.g. `ingress.rs:1661` `state.max_account_attempts` → `state.runtime_settings.max_account_attempts()`; `ingress.rs:1662/2384/2564` starvation budget; `1663/2385/2565` heartbeat; `986` wake_jitter; the `stream_idle_timeout` reads at `ingress.rs:677,1090,1207,1438,1888,2015,2088,2414,2596` (still passed as the arg into `watchdog::wrap_stream` — the CALL happens in ingress, unchanged watchdog); `inflight_penalty_pct` at `ingress.rs:1832,2341,2520`+`control.rs:91` (into `SelectionCtx`); `soft_drain_enabled` at `usage_refresh.rs:302,321`; retention at `retention.rs:53,71`; `live_logs` at `sse.rs:24`,`auth.rs:50`,`control.rs`. The compiler will flag every removed-field use — fix each to the getter. Behavior is identical (getters return the same seeded values).
- [ ] **Step 4: GREEN** — full `cargo test -p polyflare-server` + `-p polyflare-store`; clippy `--all-targets -D warnings`; fmt; then the latency gate `cargo test -p polyflare-server --test latency_regression` (atomic loads are negligible). Confirm `git diff --name-only` touches NO wedge-sacred file.
- [ ] **Step 5: Commit** — `feat(settings): RuntimeSettings on AppState + startup overlay + re-routed live reads`.

---

### Task 5: `GET` + `PATCH /api/settings`

**Files:** Modify `crates/polyflare-server/src/read_api.rs` (or a new `settings_api.rs` module — match where handlers live) + `crates/polyflare-server/src/app.rs` (route). Test: `crates/polyflare-server/tests/`.

**Interfaces:** Consumes `RuntimeSettings` + `SettingsRepo`. Produces:
- `GET /api/settings` → `SettingsView { fields: Vec<SettingFieldView> }` where `SettingFieldView { key, value: Option<String>, default: String, class: "live"|"restart-only"|"fixed", kind: "u32"|"secs"|"bool"|"f64"|"string", min: Option<f64>, max: Option<f64> }`. The 10 live fields carry their current holder value + bounds; the restart-only/fixed fields are informational (from the frozen config / a static table); `admin_token` → `value: None` (presence only).
- `PATCH /api/settings` with a JSON object `{ <key>: <value> }` (values typed per the field). For each key: must be one of the 10 live keys (else 400); coerce to the field's kind (wrong type → 400); call `runtime_settings.set(key, val)` (clamps + cross-field); on success persist `store.settings().set(key, stored_value, now)`. Any invalid key/type → 400 with a message; on success return the `GET`-shaped view (or `{ok:true}`). Admin-gated (route inside the `require_admin` router). Content-free.

- [ ] **Step 1: Failing tests** — admin `PATCH {"max_account_attempts": 7}` → 200, then `GET` shows `max_account_attempts` value `7` AND `store.settings().get_all()` has it; `PATCH {"inflight_penalty_pct": 99}` → the clamped `50` persists + is live; `PATCH {"starvation_heartbeat": 99999}` → clamped to budget; `PATCH {"bind_addr": "x"}` (non-live) → 400; `PATCH {"live_logs": "notabool"}` → 400; keyless → 401; `GET` returns all fields with correct `class`.
- [ ] **Step 2: RED** — `cargo test -p polyflare-server` (new test).
- [ ] **Step 3: Implement** the two handlers + the static field-metadata table (key→class/kind/bounds/default) + register `/api/settings` (`.get(...).patch(...)`) inside the admin-gated `api` router in `app.rs`.
- [ ] **Step 4: GREEN** — full `cargo test -p polyflare-server`; clippy `--all-targets` + fmt.
- [ ] **Step 5: Commit** — `feat(settings-api): GET + PATCH /api/settings (validate+clamp+persist+live)`.

---

### Task 6: Settings page (frontend)

**Files:** Modify `crates/polyflare-server/dashboard/src/lib/api.ts` (types + `api.settings` + `patchSettings`), `src/lib/queries.ts` (`useSettings`/`useUpdateSettings`), `src/App.tsx` (route), `src/shell/Sidebar.tsx` (promote "Settings" from `SOON_ITEMS`). Create `src/pages/Settings.tsx`.

**Interfaces:** Consumes `GET/PATCH /api/settings` (Task 5). Reuse the account-controls mutation foundation (`usePatchAccount` pattern → a `useUpdateSettings` using the shared Toast + query invalidation). `SettingsView`/`SettingFieldView` TS interfaces mirror the Rust serde.

- [ ] **Step 1** — api.ts: `SettingFieldView`/`SettingsView` interfaces + `api.settings()` + `patchSettings(body)`. queries.ts: `useSettings()` (60s) + `useUpdateSettings()` (mutation → invalidate `["settings"]` + Toast; error surfaces the backend clamp/validation message via `mutationErrorText`).
- [ ] **Step 2** — Sidebar: move "Settings" from `SOON_ITEMS` into `NAV_ITEMS` as `{ to: "/settings", label: "Settings", icon: Settings }`. App.tsx: add `<Route path="settings" element={<Settings />} />` in the authed Shell.
- [ ] **Step 3** — `Settings.tsx`: group the `fields` by area; for each `class==="live"` field render a labeled control (number `<input min max>` for numeric/secs, a Radix `Switch` for bool) pre-filled with `value`, a small "live" badge, and a Save that PATCHes just that field (or a per-section Save). Restart-only/fixed fields render disabled with a `restart-only`/`fixed` badge. Loading/error/empty states. ccflare skin, no emoji, tabular-nums; reuse `Card`/`Grid`.
- [ ] **Step 4 — Verify** — `bun run build` clean; `git add -A` (dist); commit.
- [ ] **Step 5: Commit** — `feat(dashboard): Settings page (live-edit 10 tunables + read-only config)`.

---

### Task 7: Live verification (controller-run)

- [ ] **Step 1** — Build + run the server against a store clone (valid tokens). `GET /api/settings` → confirm the field list + classes + current values.
- [ ] **Step 2** — `PATCH /api/settings {"max_account_attempts": 9}` → 200; re-`GET` shows 9; query the clone's `settings` table → row present. `PATCH {"inflight_penalty_pct": 999}` → persists the clamped 50. `PATCH {"bind_addr":"x"}` → 400.
- [ ] **Step 3** — ★ Prove LIVE (no restart): `PATCH {"live_logs": true}` then confirm `GET /api/logs/stream` is now enabled (was flag-driven), OR `PATCH` a numeric and confirm the running server's behavior reflects it on the next request. Then RESTART the server (same clone) and `GET /api/settings` → the persisted value survives (overlay > env).
- [ ] **Step 4** — Content-safety: server log has no token/secret; `admin_token` never returned as a value. Confirm wedge-clean (`git diff` touched no sacred file) + latency gate green.

---

## Self-Review
- Spec coverage: 10 live fields + atomics → T2; clamp reuse → T1; table+overlay → T3/T4; re-routed reads (no sacred) → T4; GET/PATCH validate+clamp+cross-field+persist+live → T5; page → T6; live-verify + persistence-across-restart → T7. Covered.
- Types: `RuntimeSettings` getters/`set`, `SettingsRepo` get_all/set, `SettingFieldView` consistent across tasks. Clamp fns named `clamp_<field>` throughout. No placeholders (clamp bounds factored verbatim from the source).
