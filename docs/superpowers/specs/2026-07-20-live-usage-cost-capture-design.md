# Live Per-Request Usage + Cost Capture — Design

**Status:** Approved 2026-07-20. Sub-project 1 of 2 for the Analytics/Reports build-out (the backend prerequisite; Sub-project 2 = the Reports page, its own spec). Next: implementation plan → SDD.

## Motivation

PolyFlare's own traffic writes `request_log` rows with **no token counts, no TTFT, no cost** — the ingress log-write hard-codes `ttft_ms/total_tokens/cached_tokens = None` with a `TODO(follow-up): populate ttft/tokens from the stream observer` (`ingress.rs:1548-1551`, twin at `2187-2190`). All 185k rows that *do* carry cost/tokens are **imported codex-lb history**, frozen at import. Without live capture, any analytics page is a museum. This sub-project makes PolyFlare record per-request **token usage, TTFT, and derived cost** on its own traffic, so Reports (and the existing Overview KPIs) reflect live activity.

## Feasibility (verified)

Capturing usage is **feasible without touching any wedge-sacred file**. `ObservingStream` (`watchdog.rs`, SACRED) forwards every upstream chunk byte-for-byte unchanged (`watchdog.rs:845`), so an **ingress-owned outer stream wrapper** — layered at the `stream_response(...)` sites exactly like the existing `wrap_translating_stream`/`wrap_recovered` layers — sees the `response.completed` frame (which carries `response.usage` in the same frame the watchdog sniffs for `response.id`). The alternative of threading usage through `TurnOutcome::Completed`/`observe` is **blocked** (its constructor + the id-sniffer live in `watchdog.rs`, SACRED). So the wrapper is the mechanism.

## Scope

Four parts, all in modifiable files (`polyflare-core`/`polyflare-server`/`polyflare-store`), none touching `watchdog.rs`/`continuity.rs`/`select.rs`:

1. **Pricing module** — port codex-lb's per-model rate table + cost computation.
2. **Usage extraction** — parse `response.usage` from the completed frame.
3. **Ingress stream wrapper** — non-sacred outer wrapper: record TTFT + accumulate/parse the completed frame, compute cost, persist at stream-end.
4. **Persistence + schema** — a `request_id`-correlated `update_usage`; standardize analytics on the `0005` column family.

**Out of scope (this sub-project):** the Reports UI (Sub-project 2); the WS-relay path capture (`ws_relay/sniff.rs` is modifiable and already sniffs the completed frame — a clean follow-up, but `POLYFLARE_WS_DOWNSTREAM` is off by default, so it's deferred); config-driven runtime rate overrides (rates ship as a static table this slice; a config override hook is a later nicety); per-account subscription-tier detection (cost uses the tier PolyFlare knows, default otherwise).

## 1. Pricing module

Port codex-lb's `app/core/usage/pricing.py` `DEFAULT_PRICING_MODELS` + cost logic into a new PolyFlare module (`polyflare-core::pricing`). Faithful port so live cost shares the historical basis:

- **`ModelPrice`** per model: `input_per_1m`, `output_per_1m`, `cached_input_per_1m`, service-tier variants (`priority_*`, `flex_*`), and long-context tiering (`long_context_threshold_tokens` + `long_context_{input,cached_input,output}_per_1m`). Models: `gpt-5.6-sol`/`terra`/`luna`, `gpt-5.5`, `gpt-5.4`/`-mini`, `gpt-5.3-codex`/`-spark`, `gpt-5.2` (copy the exact rates from codex-lb; they are the source of truth for the imported `cost_usd`).
- **Cost formula** (mirrors codex-lb): pick the rate set by `service_tier` (default / priority / flex); if `input_tokens ≥ long_context_threshold`, use the long-context rates; `cost = (input−cached)/1e6·input_rate + cached/1e6·cached_rate + output/1e6·output_rate`. `cached_input` is clamped to `[0, input]`. Unknown model → `None` cost (not a guessed 0), logged content-free (model slug only) so gaps are visible.
- **Lookup** supports the `fnmatch`-style family globs codex-lb uses (`gpt-5.6-*`) so codename variants resolve. Pure function, unit-testable in isolation, no I/O.

## 2. Usage extraction

The `response.completed` frame's `usage` object (shape confirmed from codex-lb's `ResponseUsage`): `input_tokens`, `output_tokens`, `input_tokens_details.cached_tokens`, `output_tokens_details.reasoning_tokens`. A small content-free parser extracts these four counts (each `Option<i64>`); it reads only the numeric usage object, never any content/text field. Verify field names against one live `response.completed` frame before finalizing the parser (a one-time capture; codex-lb's model gives the expected shape).

## 3. Ingress stream wrapper (non-sacred)

An ingress-owned `Stream` adapter wrapping the `ResponseStream` at the `stream_response(...)` call sites inside `responses_handler_impl_with_max_attempts` (main route `ingress.rs:1817`, failover `1443`, layer2-wait `814`, native-messages `2324`; for the aliased `/v1/messages` path wrap the **inner** pre-translation stream at `2521` so it sees Codex `response.completed`, not translated frames). The wrapper:
- forwards every chunk unchanged (passthrough — must not alter client bytes or fingerprint);
- records **TTFT** = elapsed to the first yielded chunk;
- scans yielded frames for `response.completed`, parses its `usage`;
- on stream end (or drop), computes cost via the pricing module and fires a **fire-and-forget** `update_usage` (never blocks or fails the client stream — an error is logged content-free and dropped).

Handles the disconnect case: if the client drops mid-stream, the wrapper's `Drop`/end still persists whatever usage was seen (possibly none) — the row is never left half-updated in a way that breaks Reports (usage columns simply stay `NULL`, same as an errored request).

## 4. Persistence + schema

- **Correlation via `request_id`** (avoids the write-ordering problem — the wrapper is created *before* today's synchronous INSERT): generate a `request_id` (uuid) once, early in the request, thread the same value into (a) the wrapper and (b) the existing synchronous `RequestLog` write. The sync INSERT stays exactly as today (row appears immediately; live-logs + `duration_ms` semantics unchanged), now carrying `request_id`. The wrapper's stream-end `update_usage` does `UPDATE request_log SET <usage cols> WHERE request_id = ?`.
- **`RequestLogRecord`/`RequestLogRow` + `insert`** widen to carry `request_id` and the analytics columns; a new **`RequestLogRepo::update_usage(request_id, input_tokens, output_tokens, cached_input_tokens, reasoning_tokens, cost_usd, latency_first_token_ms)`** performs the keyed update. Add an index on `request_id` for the update.
- **Canonical analytics columns = the `0005` family** (`input_tokens`, `output_tokens`, `cached_input_tokens`, `reasoning_tokens`, `cost_usd`, `latency_first_token_ms`), because **all 185k historical rows already populate it** — so live + historical align with no backfill, and it carries the full breakdown + cost (the `0007` `total_tokens/cached_tokens/ttft_ms` set cannot express breakdown or cost). Native ingress now writes the `0005` family.
- **Existing-KPI migration:** the Overview's token KPI currently sums `total_tokens` (`read_api.rs:325`), which is `NULL` on all imported rows (a latent bug — history contributes 0). Migrate that read (and `derive_tps`) to the `0005` family via `COALESCE(total_tokens, input_tokens + output_tokens + reasoning_tokens)` (or read `0005` directly), so the existing Overview also benefits from historical + live data. The `0007` columns become legacy (left in place, no longer the analytics source).

## Content-safety / wedge-sacred / performance

- **Content-free:** the wrapper reads only the numeric `usage` object and never persists or logs frame text/content; cost is a number; the only string that could be logged is the model slug on an unknown-model miss. Same content-safety class as the existing read/log surfaces.
- **Wedge-sacred:** the wrapper and persistence live entirely in `ingress.rs`/`polyflare-store`/`polyflare-core`; `ObservingStream`/`watchdog.rs`, `continuity.rs`, `select.rs` are neither read for logic changes nor modified. The wrapper forwards bytes unchanged, preserving the client-facing stream and fingerprint.
- **Latency:** the per-chunk passthrough adds a trivial `poll_next` hop; the repo's `latency_regression.rs` gate must stay green (verify).

## Testing

- **Pricing module (unit):** per-model cost against codex-lb's own numbers (default/priority/flex tiers; a long-context case above the threshold; cached clamping; unknown model → `None`); family-glob resolution.
- **Usage parser (unit):** extracts the four counts from a representative `response.completed` `usage` JSON; missing/partial fields → `None` not panic; ignores content fields.
- **Store (TDD):** `update_usage` sets the `0005` columns on the row matching `request_id`, leaves others untouched, no-ops on an unknown `request_id`; `insert` round-trips `request_id`.
- **Ingress (integration):** a mock upstream stream ending in a `response.completed` with a known `usage` → the persisted row carries the expected tokens/TTFT/cost; a stream with **no** completed frame (error/disconnect) → usage stays `NULL`, request still logged; bytes forwarded to the client are byte-identical to upstream (passthrough proof).
- **Content-safety:** grep the new code — no frame text/content logged or persisted; only numeric usage + model slug.
- **Live verify:** run against a real account, issue a real request, confirm the persisted row now carries live tokens/TTFT/cost (this is the whole point — it must work end-to-end on live traffic, not just mocks).

## Global constraints

- Content-free; wedge-sacred (no `watchdog.rs`/`continuity.rs`/`select.rs` edits); additive/backward-compatible (new columns already exist in schema; new record fields + a new store method + a non-sacred wrapper — no existing behavior changes except the KPI COALESCE migration, which only *adds* previously-missing data).
- Clippy `-D warnings` (`--all-targets`), `cargo fmt`, full `cargo test -p polyflare-server` + `-p polyflare-store` + `-p polyflare-core` green; the `latency_regression` gate green.
- Rates ported verbatim from codex-lb (the source of truth for historical cost); document the port + date so drift is auditable.

## Out of scope (explicit)

The Reports UI (Sub-project 2); WS-relay usage capture (deferred follow-up, flag-off by default); runtime/config rate overrides; per-account subscription-tier detection; dropping/rewriting the legacy `0007` columns.
