# Dashboard Settings (Live-Editable) — Design

**Status:** Approved 2026-07-21. Phase-4 subsystem (config as UI). Next: implementation plan → SDD.

## Motivation

PolyFlare's tunables are env-var-only and frozen in `AppState` at startup — changing a retention window, a timeout, or a feature flag means editing the environment and restarting the proxy. A **Settings** page surfaces the full running config and makes a curated set of safe tunables **live-editable**: an edit persists and takes effect on the *next* request/job without a restart. This turns the dashboard into a config control plane and closes the last read-only gap.

## Scope — the 10 live-editable settings

A feasibility pass mapped every `ServeConfig` field's read sites. Exactly **10 fields** are read per-request/per-job off `state.<field>` in MODIFIABLE files (never in the wedge-sacred `watchdog.rs`/`continuity.rs`/`select.rs`) and can be flipped live with a mutable value — no rebuilds, no sacred edits:

| Setting | Type | Atomic | Notes |
|---|---|---|---|
| `max_account_attempts` | u32 | `AtomicU32` | failover cap, read per-request in ingress |
| `starvation_wait_budget` | Duration(secs) | `AtomicU64` | per-request |
| `starvation_heartbeat` | Duration(secs) | `AtomicU64` | per-request; **must stay ≤ budget** (cross-field) |
| `wake_jitter_ms` | u64 | `AtomicU64` | per-request |
| `stream_idle_timeout` | Duration(secs) | `AtomicU64` | read in ingress, passed as an **arg** into `watchdog::wrap_stream` — re-routed at the ingress caller, watchdog untouched |
| `inflight_penalty_pct` | f64 | `AtomicU64` (`to_bits`) | read in ingress/control into a per-request `SelectionCtx`; `select.rs` reads the *ctx field*, not config |
| `soft_drain_enabled` | bool | `AtomicBool` | read per usage-refresh tick |
| `request_log_retention_days` | u32 | `AtomicU32` | read per retention tick (job re-reads per-tick by design) |
| `usage_history_retention_days` | u32 | `AtomicU32` | read per retention tick |
| `live_logs` | bool | `AtomicBool` | read per SSE/log event |

**Deferred / read-only (shown with a badge, NOT editable this slice):**
- **`restart-only`** — `routing_strategy`/`pool_strategies` (frozen into the `Selector` `Arc`; a later slice can hot-swap it via `ArcSwap`), `model_catalog_ttl_secs`/`model_catalog_enabled` (cache/lifecycle), `ws_downstream`/`ws_upstream`/`ws_client_ping` (router/executor reshaping), `continuity_watchdog` (its value feeds a `WatchdogArm` consumed by the sacred `watchdog.rs` — the wedge trap).
- **`fixed`** — `bind_addr`, `db_path`, `key_path`, `upstream_base_url`, `anthropic_upstream_base_url`, `auth_base_url`, `admin_token` (never shown as a value — presence only), `capture_fingerprint_path`, `allow_unauthenticated_remote` (a security gate — never live-edited).

## Architecture

- **`RuntimeSettings`** (new, `polyflare-server`): a struct of the 10 atomics (`AtomicU32`/`AtomicU64`/`AtomicBool`; the `f64` via `f64::to_bits`/`from_bits`), with a getter per field returning the typed value (`Relaxed` ordering — each field independent, per-request eventual consistency) and a validated setter. `AppState` gains `pub runtime_settings: Arc<RuntimeSettings>` alongside the existing destructured fields.
- **Re-route the 10 reads:** each `state.<field>` read for the 10 becomes `state.runtime_settings.<field>()`, in the modifiable files that read them (`ingress.rs`, `retention.rs`, `usage_refresh.rs`, `sse.rs`, `auth.rs`, `control.rs`). The existing frozen `AppState` fields for these 10 are removed (their value now lives in `RuntimeSettings`); fields NOT made live keep their frozen `AppState` field. **No sacred file is read-for-logic-change or modified.**
- **Clamp reuse:** the existing `*_from_env` helpers in `config.rs` both parse env AND clamp. Factor out pure `clamp_<field>(raw) -> value` functions so the boot path AND the PATCH path share identical bounds (single source of truth for the valid ranges). `config.rs` is modifiable.
- **Persistence:** a new `settings` table (`key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at INTEGER`) + `SettingsRepo` (`get_all() -> HashMap<String,String>`, `set(key, value, now)`). Content-free (config values only — never a token/secret; `admin_token` is never persisted here).
- **Startup precedence:** `ServeConfig::from_env()` resolves env as today; then `SettingsRepo::get_all()` overlays any persisted rows (persisted > env at boot, each re-clamped); `RuntimeSettings` is built from the overlaid values before `AppState`.

## Backend endpoints (admin-gated, content-free)

- **`GET /api/settings`** → a list of all config fields with `{ key, value (string; omitted/masked for `admin_token`), default, class: "live"|"restart-only"|"fixed", kind: "u32"|"secs"|"bool"|"f64"|"string", min?, max? }`. The 10 live fields carry their current `RuntimeSettings` value + bounds; the rest are informational.
- **`PATCH /api/settings`** with `{ <key>: <value> }` (one or more of the 10 live keys). For each: reject an unknown/non-live key (400), coerce+`clamp_<field>`, enforce cross-field (`starvation_heartbeat ≤ starvation_wait_budget` using the *incoming-or-current* budget), then in one handler persist to the `settings` table AND update the `RuntimeSettings` atomic. Returns the updated GET-shaped view or `{ok:true}`. Out-of-range/invalid → 400 with a message; unknown/non-live key → 400.

## Frontend

- A `/settings` page (promote the existing disabled "Settings" entry in `Sidebar.tsx`'s `SOON_ITEMS` to `NAV_ITEMS`; add the route in `App.tsx`).
- Grouped sections (e.g. **Reliability & routing** — max attempts, starvation budget/heartbeat, wake jitter, inflight penalty; **Streaming** — stream idle timeout, soft drain; **Retention** — request-log / usage-history days; **Flags** — live logs; and a **read-only** section for the restart-only/fixed fields with a badge).
- Each live setting: a labeled control (number input with min/max, or a Radix switch for bools), current value pre-filled, a small "live" badge; on save, `PATCH /api/settings` via a `useUpdateSettings` mutation built on the account-controls foundation (Toast success/error surfacing the backend clamp/validation message; refetch `GET /api/settings` on success). Restart-only/fixed fields render disabled with their badge. ccflare skin, no emoji, tabular-nums.

## Testing

- **`RuntimeSettings` (unit):** atomic get/set round-trips per field (incl. the `f64` bit-cast); each `clamp_<field>` clamps to its range; the setter enforces the heartbeat≤budget invariant.
- **`SettingsRepo` (unit):** `set` then `get_all` round-trips; overwrite updates `updated_at`.
- **Endpoint (integration):** `PATCH` clamps out-of-range → the clamped value persists + the atomic updates; an unknown/non-live key → 400; a heartbeat > budget → rejected/clamped; `GET` returns all fields with correct classes; keyless → 401. Startup overlay: a persisted row wins over the env default.
- **Live-verify (controller):** run the server; `PATCH /api/settings` a live field (e.g. `max_account_attempts` or `live_logs`); confirm the running proxy uses the new value on the next request/behavior (and that it persisted across a restart via the overlay). Content-safety: only numeric/bool/enum config values in logs/responses — never a token.

## Global constraints

- **Wedge-sacred:** the 10 live fields have no read sites in `watchdog.rs`/`continuity.rs`/`select.rs` (verified); `stream_idle_timeout` is re-routed at the ingress caller. Touch none of those files.
- **Content-free; admin-gated** (existing `require_admin`). `admin_token` is never returned as a value.
- **Additive/backward-compatible:** new table + new `RuntimeSettings` holder + re-pointed reads (behavior-identical at boot when no persisted overrides exist) + new endpoints + new page. Existing behavior unchanged when the `settings` table is empty.
- Clippy `-D warnings` (`--all-targets`), fmt, `cargo test` green across touched crates; dashboard `tsc` + build clean; tracked `dist/` rebuilt+committed per frontend commit.

## Out of scope (explicit)

Live-editing the second-tier (`routing_strategy` hot-swap, model-catalog, WS transport) and restart-only/fixed fields; per-pool strategy editing; any change to `allow_unauthenticated_remote` or credentials; export/import of settings. Each is a later slice.
