# Live Per-Request Usage + Cost Capture Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Capture per-request token usage, TTFT, and derived cost on PolyFlare's own traffic and persist it to `request_log`, so analytics reflect live activity (not just imported history).

**Architecture:** A pure `pricing` module (ported from codex-lb) computes `cost_usd` from token counts + model + service tier. A non-sacred, ingress-owned stream wrapper forwards upstream bytes unchanged while sniffing the `response.completed` frame's `usage` object and timing TTFT; on stream end it computes cost and fire-and-forget updates the already-inserted `request_log` row, correlated by a generated `request_id`. Analytics standardize on the `0005` token/cost columns (already populated on all historical rows).

**Tech Stack:** Rust (workspace crates `polyflare-core`, `polyflare-store`, `polyflare-server`); SQLite via `sqlx`; `futures`/`tokio` streams; `serde_json`; `uuid`.

## Global Constraints

- **Wedge-sacred:** NEVER read-for-logic-change or modify `crates/polyflare-server/src/watchdog.rs` (`ObservingStream`), `crates/polyflare-core/src/continuity.rs`, or any `select.rs`. The stream wrapper lives in `ingress.rs`/a new server module and forwards bytes byte-for-byte unchanged.
- **Content-free:** never log or persist request/response content or a token/bearer value. Capture reads ONLY the numeric `usage` object; the only string ever logged is a model slug on an unknown-model cost miss.
- **Ported rates are the source of truth:** port codex-lb `app/core/usage/pricing.py`'s `DEFAULT_PRICING_MODELS` (55 entries) + cost logic VERBATIM (it produced the historical `cost_usd`). Document the port + date.
- **Canonical analytics columns = the `0005` family:** `input_tokens`, `output_tokens`, `cached_input_tokens`, `reasoning_tokens`, `cost_usd`, `latency_first_token_ms`. (All 185k historical rows already populate these; the `0007` `total_tokens/cached_tokens/ttft_ms` set is currently NULL on every row and becomes legacy.)
- **Non-blocking capture:** an `update_usage` failure or a missing completed frame must never fail or stall the client stream — log content-free and drop; the row simply keeps NULL usage.
- Clippy `--all-targets -D warnings`, `cargo fmt --all --check`, full `cargo test -p polyflare-core -p polyflare-store -p polyflare-server` green, and the `latency_regression` gate green.

## File Structure

- **Create** `crates/polyflare-core/src/pricing.rs` — `ModelPrice`, `DEFAULT_PRICING_MODELS`, `pricing_for_model`, `effective_rates`, `cost_usd`. Pure, no I/O. (+ `pub mod pricing;` in `crates/polyflare-core/src/lib.rs`.)
- **Create** `crates/polyflare-server/src/usage_capture.rs` — `ResponseUsage` + `parse_response_usage` (frame parser) and `UsageCapturingStream` (the passthrough wrapper). (+ `mod usage_capture;` in `main.rs`/`lib.rs` as the crate does for other modules.)
- **Create** `crates/polyflare-store/migrations/0012_request_log_request_id_index.sql` — index on `request_log(request_id)`.
- **Modify** `crates/polyflare-store/src/request_log_repo.rs` — widen `RequestLogRecord`/`RequestLogRow` + `insert`; add `update_usage`.
- **Modify** `crates/polyflare-server/src/ingress.rs` — generate + thread `request_id`; wrap the response stream; call `update_usage`.
- **Modify** `crates/polyflare-server/src/read_api.rs` — migrate the token/TPS reads to the `0005` family (COALESCE).

---

### Task 1: Pricing module (`polyflare-core::pricing`)

**Files:**
- Create: `crates/polyflare-core/src/pricing.rs`
- Modify: `crates/polyflare-core/src/lib.rs` (add `pub mod pricing;`)
- Reference (port source, read-only): `/Users/wmaididhaiqal/Development/Codex-LoadBalancer/codex-lb/app/core/usage/pricing.py`

**Interfaces:**
- Produces:
  - `pub struct ModelPrice { pub input_per_1m: f64, pub output_per_1m: f64, pub cached_input_per_1m: Option<f64>, pub priority_multiplier: Option<f64>, pub priority_input_per_1m: Option<f64>, pub priority_output_per_1m: Option<f64>, pub priority_cached_input_per_1m: Option<f64>, pub flex_input_per_1m: Option<f64>, pub flex_output_per_1m: Option<f64>, pub flex_cached_input_per_1m: Option<f64>, pub long_context_threshold_tokens: Option<f64>, pub long_context_input_per_1m: Option<f64>, pub long_context_output_per_1m: Option<f64>, pub long_context_cached_input_per_1m: Option<f64> }` (mirrors codex-lb `ModelPrice`, `pricing.py:13-27`).
  - `pub fn pricing_for_model(model: &str) -> Option<&'static ModelPrice>` — case-insensitive exact match against `DEFAULT_PRICING_MODELS`, then the alias map (port `get_pricing_for_model` + `resolve_model_alias` + `DEFAULT_MODEL_ALIASES`, `pricing.py:357-392`).
  - `pub fn cost_usd(model_price: &ModelPrice, input_tokens: i64, output_tokens: i64, cached_input_tokens: i64, service_tier: Option<&str>) -> f64` — the ported cost (see Step 3).

- [ ] **Step 1: Write the failing tests** — in `pricing.rs` under `#[cfg(test)] mod tests`. Use codex-lb's own numbers (compute by hand from `pricing.py` rates for `gpt-5.6-sol`: input 5.0, cached 0.5, output 30.0 per 1M):
```rust
#[test]
fn cost_default_tier_gpt56_sol() {
    let p = pricing_for_model("gpt-5.6-sol").unwrap();
    // 100_000 input (20_000 cached), 10_000 output, default tier.
    // billable_input = 80_000 → 80_000/1e6*5.0 = 0.40; cached 20_000/1e6*0.5 = 0.01; output 10_000/1e6*30.0 = 0.30
    let c = cost_usd(p, 100_000, 10_000, 20_000, None);
    assert!((c - 0.71).abs() < 1e-9, "got {c}");
}
#[test]
fn cost_priority_tier_uses_priority_rates() {
    let p = pricing_for_model("gpt-5.6-sol").unwrap(); // priority in 10, cached 1, out 60
    // 100_000 input (20_000 cached), 10_000 output → 80_000/1e6*10 + 20_000/1e6*1 + 10_000/1e6*60 = 0.80+0.02+0.60
    let c = cost_usd(p, 100_000, 10_000, 20_000, Some("priority"));
    assert!((c - 1.42).abs() < 1e-9, "got {c}");
}
#[test]
fn cost_long_context_above_threshold() {
    let p = pricing_for_model("gpt-5.6-sol").unwrap(); // long_context in 10, cached 1, out 45, threshold 272_000
    // 300_000 input (0 cached), 1_000 output → 300_000/1e6*10 + 0 + 1_000/1e6*45 = 3.0 + 0.045
    let c = cost_usd(p, 300_000, 1_000, 0, None);
    assert!((c - 3.045).abs() < 1e-9, "got {c}");
}
#[test]
fn cached_clamped_to_input() {
    let p = pricing_for_model("gpt-5.6-sol").unwrap();
    // cached (999_999) clamped to input (100_000): billable 0, cached 100_000/1e6*0.5 = 0.05, out 0
    let c = cost_usd(p, 100_000, 0, 999_999, None);
    assert!((c - 0.05).abs() < 1e-9, "got {c}");
}
#[test]
fn unknown_model_has_no_price() {
    assert!(pricing_for_model("totally-made-up").is_none());
}
```
- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p polyflare-core pricing`
Expected: FAIL (module `pricing` does not exist / `cost_usd` not found).

- [ ] **Step 3: Implement** — port the table + logic. Copy all 55 `ModelPrice` entries VERBATIM from `pricing.py:90-355`'s `DEFAULT_PRICING_MODELS` into a Rust `static`/`Lazy<HashMap<&'static str, ModelPrice>>` (use `once_cell::sync::Lazy` — already a workspace dep; verify in `Cargo.toml`). Port `DEFAULT_MODEL_ALIASES` + `resolve_model_alias` (`pricing.py:357-368`). Implement `cost_usd` mirroring `_effective_rates` (`pricing.py:415-465`) + `calculate_cost_breakdown_from_usage` (`pricing.py:479-517`) EXACTLY:
```rust
pub fn cost_usd(price: &ModelPrice, input_tokens: i64, output_tokens: i64, cached_input_tokens: i64, service_tier: Option<&str>) -> f64 {
    let input = input_tokens.max(0) as f64;
    let output = output_tokens.max(0) as f64;
    let cached = (cached_input_tokens.max(0) as f64).min(input); // clamp to [0, input]
    let (input_rate, cached_rate, output_rate) = effective_rates(price, input, service_tier);
    let billable_input = (input - cached).max(0.0);
    (billable_input / 1_000_000.0) * input_rate
        + (cached / 1_000_000.0) * cached_rate
        + (output / 1_000_000.0) * output_rate
}
```
`effective_rates` must reproduce, in order: (a) `is_long_context = threshold set && input > threshold && long_context_input/output set`; (b) if priority tier (`service_tier` normalized ∈ {"priority","fast"}): use `priority_*` rates (cached = `priority_cached` else `priority_input`) if `priority_input`+`priority_output` set, else `priority_multiplier` × base; (c) if flex tier (== "flex") and `flex_input`+`flex_output` set: use `flex_*` (cached = `flex_cached` else flex input), and if `is_long_context` multiply input×2, cached×2, output×1.5; (d) else if `is_long_context`: use `long_context_*` (cached = `long_context_cached` else long input); (e) else base rates (cached = `cached_input_per_1m` else input). Service tier normalize = trim+lowercase, empty→None.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p polyflare-core pricing` — Expected: PASS (5/5). Then `cargo clippy -p polyflare-core --all-targets -- -D warnings` + `cargo fmt --all -- --check` clean.

- [ ] **Step 5: Commit** — `git add crates/polyflare-core/src/pricing.rs crates/polyflare-core/src/lib.rs && git commit -m "feat(pricing): port codex-lb per-model rate table + cost computation"`

---

### Task 2: Usage frame parser (`usage_capture::parse_response_usage`)

**Files:**
- Create: `crates/polyflare-server/src/usage_capture.rs` (parser half; the stream wrapper is Task 5)
- Modify: `crates/polyflare-server/src/main.rs` (or `lib.rs` — wherever sibling modules like `read_api`/`write_api` are declared) to add `mod usage_capture;`

**Interfaces:**
- Produces:
  - `pub struct ResponseUsage { pub input_tokens: Option<i64>, pub output_tokens: Option<i64>, pub cached_input_tokens: Option<i64>, pub reasoning_tokens: Option<i64> }`
  - `pub fn parse_response_usage(frame_json: &str) -> Option<ResponseUsage>` — returns `Some` only for a `response.completed` frame carrying a `usage` object; `None` otherwise. Reads ONLY numeric usage fields (content-free). JSON shape (from codex-lb `_normalize_usage`, `pricing.py:58-89`): `usage.input_tokens`, `usage.output_tokens`, `usage.input_tokens_details.cached_tokens`, `usage.output_tokens_details.reasoning_tokens`, under the completed frame's `response` object.

- [ ] **Step 1: Write the failing tests**
```rust
#[test]
fn parses_usage_from_completed_frame() {
    let f = r#"{"type":"response.completed","response":{"id":"resp_1","usage":{"input_tokens":8380,"output_tokens":120,"input_tokens_details":{"cached_tokens":6912},"output_tokens_details":{"reasoning_tokens":40}}}}"#;
    let u = parse_response_usage(f).unwrap();
    assert_eq!(u.input_tokens, Some(8380));
    assert_eq!(u.output_tokens, Some(120));
    assert_eq!(u.cached_input_tokens, Some(6912));
    assert_eq!(u.reasoning_tokens, Some(40));
}
#[test]
fn non_completed_frame_is_none() {
    assert!(parse_response_usage(r#"{"type":"response.output_text.delta","delta":"hi"}"#).is_none());
}
#[test]
fn completed_without_usage_is_none() {
    assert!(parse_response_usage(r#"{"type":"response.completed","response":{"id":"r"}}"#).is_none());
}
```
- [ ] **Step 2: Run** `cargo test -p polyflare-server usage_capture` — Expected: FAIL (module/function missing).
- [ ] **Step 3: Implement** with `serde_json::Value`: parse the frame; require `["type"] == "response.completed"` and a `["response"]["usage"]` object; extract the four fields with `.get(...).and_then(Value::as_i64)` (details nested under `input_tokens_details`/`output_tokens_details`); return `Some(ResponseUsage{..})`. Never read or copy any non-usage field. (SSE note: the wrapper in Task 5 hands this fn the JSON object text; if frames arrive as SSE `data: {...}` lines, strip the `data: ` prefix in the wrapper before calling — document that boundary here.)
- [ ] **Step 4: Run** `cargo test -p polyflare-server usage_capture` — Expected PASS (3/3); clippy `--all-targets` + fmt clean.
- [ ] **Step 5: Commit** — `feat(usage): parse response.completed usage object (content-free)`

---

### Task 3: Store — widen record + `update_usage` + migration

**Files:**
- Create: `crates/polyflare-store/migrations/0012_request_log_request_id_index.sql`
- Modify: `crates/polyflare-store/src/request_log_repo.rs` (struct `RequestLogRecord` ~L33, `RequestLogRow` ~L74, `insert` ~L155)

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `RequestLogRecord` gains `pub request_id: Option<String>` and the 0005 analytics fields `pub input_tokens: Option<i64>, pub output_tokens: Option<i64>, pub cached_input_tokens: Option<i64>, pub reasoning_tokens: Option<i64>, pub cost_usd: Option<f64>, pub latency_first_token_ms: Option<i64>`. (`RequestLogRow` mirrors them.)
  - `pub async fn update_usage(&self, request_id: &str, input_tokens: Option<i64>, output_tokens: Option<i64>, cached_input_tokens: Option<i64>, reasoning_tokens: Option<i64>, cost_usd: Option<f64>, latency_first_token_ms: Option<i64>) -> Result<(), StoreError>` — `UPDATE request_log SET input_tokens=?,output_tokens=?,cached_input_tokens=?,reasoning_tokens=?,cost_usd=?,latency_first_token_ms=? WHERE request_id=?`. Does NOT bump any generation (request_log is not the account cache). No-op if no row matches.

- [ ] **Step 1: Write the failing test** (add to `request_log_repo.rs` tests; mirror the existing insert/list test harness):
```rust
#[tokio::test]
async fn update_usage_fills_row_by_request_id() {
    let dir = tempfile::tempdir().unwrap();
    let store = crate::Store::open(&dir.path().join("s.db")).await.unwrap();
    let repo = store.request_log();
    let mut rec = sample_record(); // helper building a RequestLogRecord with usage=None
    rec.request_id = Some("req-xyz".into());
    repo.insert(&rec).await.unwrap();
    repo.update_usage("req-xyz", Some(8380), Some(120), Some(6912), Some(40), Some(0.089), Some(3510)).await.unwrap();
    let row = repo.list(10, 0).await.unwrap().into_iter().find(|r| r.request_id.as_deref()==Some("req-xyz")).unwrap();
    assert_eq!(row.input_tokens, Some(8380));
    assert_eq!(row.cost_usd, Some(0.089));
    assert_eq!(row.latency_first_token_ms, Some(3510));
    // no-op on unknown id returns Ok
    repo.update_usage("nope", Some(1), None, None, None, None, None).await.unwrap();
}
```
(If no `sample_record` helper exists, build a full `RequestLogRecord{..}` inline with the new fields `None`.)
- [ ] **Step 2: Run** `cargo test -p polyflare-store request_log` — Expected FAIL (field/method missing).
- [ ] **Step 3: Implement** — add the fields to both structs; extend the `insert` SQL column list + binds to include `request_id` and the six 0005 columns (they already exist in the schema — 0005; `request_id` at `0005:17`); add `update_usage`; write the migration:
```sql
-- 0012_request_log_request_id_index.sql
-- request_id correlates the stream-end usage UPDATE to its row (see RequestLogRepo::update_usage).
CREATE INDEX IF NOT EXISTS idx_request_log_request_id ON request_log (request_id);
```
- [ ] **Step 4: Run** `cargo test -p polyflare-store request_log` (PASS) + full `cargo test -p polyflare-store`; clippy `--all-targets` + fmt clean.
- [ ] **Step 5: Commit** — `feat(store): request_log carries request_id + 0005 usage/cost cols + update_usage`

---

### Task 4: Ingress — generate + thread `request_id`

**Files:** Modify `crates/polyflare-server/src/ingress.rs` (the `RequestLog`/`RequestLogRecord` build sites ~L1540 and ~L2180; whatever builds the record for `insert`). **Do NOT touch `watchdog.rs`.**

**Interfaces:**
- Consumes: `RequestLogRecord.request_id` (Task 3).
- Produces: a `request_id: String` (a `uuid::Uuid::new_v4()` string — verify `uuid` is a dep; the WS-relay/session code already uses uuids) generated once per request, written into the persisted record's `request_id`. Returned/threaded so Task 6's wrapper can reference the same value.

- [ ] **Step 1: Write/extend a test** — an ingress test that drives a request to the log-write path and asserts the persisted row's `request_id` is `Some(non-empty)`. (Reuse the crate's existing ingress test harness; if the log write isn't reachable in a unit test, add a focused test on the small helper that builds the record, asserting it sets `request_id`.)
- [ ] **Step 2: Run** — Expected FAIL (`request_id` is `None`).
- [ ] **Step 3: Implement** — generate `let request_id = uuid::Uuid::new_v4().to_string();` at the point the record is built (before the sync insert AND before the stream is wrapped, so both share it), set `request_id: Some(request_id.clone())` on the record. Leave the six usage fields `None` here (filled later by `update_usage`). Do not change `duration_ms`, the `log_bus.publish`, or any timing.
- [ ] **Step 4: Run** the test (PASS) + full `cargo test -p polyflare-server`; clippy/fmt clean.
- [ ] **Step 5: Commit** — `feat(ingress): stamp a request_id on every request_log row`

---

### Task 5: Usage-capturing stream wrapper (`usage_capture::UsageCapturingStream`)

**Files:** Modify `crates/polyflare-server/src/usage_capture.rs` (add the wrapper beside the parser).

**Interfaces:**
- Consumes: `parse_response_usage` (Task 2).
- Produces: `pub struct UsageCapturingStream<S>` wrapping an inner `S: Stream<Item = Result<Bytes, E>>`, constructed via `UsageCapturingStream::new(inner, on_done)` where `on_done: impl FnOnce(CapturedUsage) + Send + 'static` fires exactly once when the inner stream ends (normally or on drop). `pub struct CapturedUsage { pub usage: Option<ResponseUsage>, pub ttft_ms: Option<i64> }`. The wrapper yields the inner items UNCHANGED (byte-for-byte passthrough); it records the elapsed-to-first-item as `ttft_ms`, and scans each yielded text chunk for a `response.completed` frame (strip an optional `data: ` SSE prefix, then `parse_response_usage`), keeping the last successful parse.

- [ ] **Step 1: Write the failing tests**
```rust
#[tokio::test]
async fn passes_bytes_through_and_captures_usage() {
    use futures::StreamExt;
    let frames = vec![
        Ok::<_, std::io::Error>(Bytes::from("data: {\"type\":\"response.created\"}\n\n")),
        Ok(Bytes::from("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"usage\":{\"input_tokens\":8380,\"output_tokens\":120,\"input_tokens_details\":{\"cached_tokens\":6912},\"output_tokens_details\":{\"reasoning_tokens\":40}}}}\n\n")),
    ];
    let captured = std::sync::Arc::new(std::sync::Mutex::new(None));
    let c2 = captured.clone();
    let s = UsageCapturingStream::new(futures::stream::iter(frames), move |cu| *c2.lock().unwrap() = Some(cu));
    let out: Vec<_> = s.map(|r| r.unwrap()).collect().await;
    // passthrough: exact bytes preserved
    assert_eq!(out[0], Bytes::from("data: {\"type\":\"response.created\"}\n\n"));
    let cu = captured.lock().unwrap().take().unwrap();
    assert_eq!(cu.usage.unwrap().input_tokens, Some(8380));
    assert!(cu.ttft_ms.is_some());
}
```
- [ ] **Step 2: Run** `cargo test -p polyflare-server usage_capture` — Expected FAIL (`UsageCapturingStream` missing).
- [ ] **Step 3: Implement** the `Stream` adapter (`impl Stream for UsageCapturingStream`), storing `start: Instant`, `ttft: Option<i64>`, `usage: Option<ResponseUsage>`, and `on_done: Option<F>`. In `poll_next`: on the first `Ready(Some(Ok(bytes)))` set `ttft` from `start.elapsed()`; for each `Ok(bytes)` try `str::from_utf8` → for each `data: `-stripped line, `parse_response_usage` → if `Some`, store as `usage`; ALWAYS return the original `bytes` unchanged. On `Ready(None)` (end), take `on_done` and call it with `CapturedUsage{usage, ttft_ms: ttft}`. Also fire `on_done` in a `Drop` impl if it hasn't fired (client disconnect), guarding double-fire with the `Option::take`. Never mutate or drop client bytes; never block.
- [ ] **Step 4: Run** `cargo test -p polyflare-server usage_capture` (PASS); clippy `--all-targets` + fmt clean.
- [ ] **Step 5: Commit** — `feat(usage): passthrough stream wrapper capturing TTFT + completed-frame usage`

---

### Task 6: Ingress — wire the wrapper + persist usage

**Files:** Modify `crates/polyflare-server/src/ingress.rs` (the `stream_response(...)` call sites: main route ~L1817, failover ~L1443, layer2-wait ~L814, native-messages ~L2324; aliased `/v1/messages`: wrap the INNER pre-translation stream ~L2521). **Do NOT touch `watchdog.rs`/`continuity.rs`/`select.rs`.**

**Interfaces:**
- Consumes: `UsageCapturingStream` (Task 5), `CapturedUsage`/`ResponseUsage` (Tasks 2/5), `pricing::pricing_for_model`+`cost_usd` (Task 1), `RequestLogRepo::update_usage` (Task 3), the `request_id` (Task 4), and the resolved `model`/`service_tier` from the route outcome.

- [ ] **Step 1: Write the failing test** — an ingress-level test with a mock upstream stream ending in a `response.completed` with a known `usage`: assert the persisted `request_log` row for the request's `request_id` ends up with the expected `input_tokens`/`cost_usd`/`latency_first_token_ms`; and a second case where the stream has NO completed frame → the row's usage stays `None` and the request still logs. (Model on the crate's existing ingress integration tests; if a full ingress test is too heavy, assert on a small extracted helper `persist_captured_usage(repo, request_id, model, tier, captured)` that maps `CapturedUsage`→`update_usage` args via the pricing module.)
- [ ] **Step 2: Run** — Expected FAIL (usage stays `None`; helper/wiring absent).
- [ ] **Step 3: Implement** — wrap each `stream_response(stream)` as `stream_response(UsageCapturingStream::new(stream, on_done))` where `on_done` is a `move` closure capturing `request_id`, `model`, `service_tier`, and a `store`/`RequestLogRepo` handle (clone the `Arc`), and does: compute `cost = model.and_then(pricing_for_model).map(|p| cost_usd(p, input, output, cached, tier))` from `captured.usage`; then `tokio::spawn(async move { let _ = repo.update_usage(&request_id, input, output, cached, reasoning, cost, captured.ttft_ms).await; });` (fire-and-forget; a failed update is logged content-free and dropped, never propagated to the client). Extract the closure body into the `persist_captured_usage` helper for testability + DRY across the call sites. For the aliased path, wrap the INNER stream (pre-translation) so it sees Codex `response.completed`. Confirm no change to the bytes reaching the client (the wrapper is passthrough).
- [ ] **Step 4: Run** the new tests (PASS) + full `cargo test -p polyflare-server`; clippy `--all-targets -D warnings` + fmt clean; then run the latency gate: `cargo test -p polyflare-server --test latency_regression` (Expected: PASS — the passthrough hop is negligible).
- [ ] **Step 5: Commit** — `feat(ingress): capture live usage/TTFT/cost into request_log via stream wrapper`

---

### Task 7: Migrate the Overview token/TPS reads to the `0005` family

**Files:** Modify `crates/polyflare-server/src/read_api.rs` (`RequestTotalsView` token sum ~L325; `derive_tps` ~L594-628).

**Interfaces:**
- Consumes: the `0005` columns now populated on live rows (Tasks 3/6) and already on historical rows.

- [ ] **Step 1: Write/extend the failing test** — seed `request_log` rows that have the `0005` columns set but `total_tokens` NULL (i.e. imported-shaped) and assert the account-detail `request_totals.total_tokens` reflects `input_tokens+output_tokens+reasoning_tokens` (currently it would be 0 because it sums the always-NULL `total_tokens`). Reuse the existing `read_api` test harness + `RequestLogRecord` seeding.
- [ ] **Step 2: Run** `cargo test -p polyflare-server --test read_api` — Expected FAIL (sum is 0 / uses `total_tokens`).
- [ ] **Step 3: Implement** — change the token total to `COALESCE(total_tokens, input_tokens + output_tokens + reasoning_tokens)` semantics (in Rust: `r.total_tokens.or_else(|| some_sum_of(r.input_tokens, r.output_tokens, r.reasoning_tokens))`), and `derive_tps`'s token/ttft sources to `r.total_tokens.or(...)` / `r.ttft_ms.or(r.latency_first_token_ms)`. Keep it content-free (numbers only). Add `input_tokens/output_tokens/reasoning_tokens/latency_first_token_ms` to the `RequestLogRow` selection if not already read.
- [ ] **Step 4: Run** the test (PASS) + full `cargo test -p polyflare-server`; clippy `--all-targets` + fmt clean.
- [ ] **Step 5: Commit** — `fix(read-api): token/TPS KPIs read the 0005 family (COALESCE) so history + live count`

---

### Task 8: Live verification (controller-run)

**Files:** none (verification only).

- [ ] **Step 1: Capture a real `response.completed` frame** from live traffic (run the server against a healthy account, issue one real `/responses` request, and confirm from the server-side the exact `usage` JSON field names match Task 2's parser — `input_tokens`, `output_tokens`, `input_tokens_details.cached_tokens`, `output_tokens_details.reasoning_tokens`). If any field name differs, fix Task 2's parser + re-run its tests.
- [ ] **Step 2: End-to-end** — against a disposable store copy, issue a real request and query the just-written `request_log` row: confirm `input_tokens`/`output_tokens`/`cached_input_tokens`/`reasoning_tokens`/`cost_usd`/`latency_first_token_ms` are all populated with plausible live values, and `cost_usd` is within the expected range for the token counts (cross-check against `pricing.py` for that model).
- [ ] **Step 3: Content-safety** — grep the server log for any frame/content text; confirm only numeric usage + model slugs appear.
- [ ] **Step 4: Wedge + latency** — confirm the diff touched none of `watchdog.rs`/`continuity.rs`/`select.rs`; confirm the `latency_regression` gate is green.

---

## Notes for the executor

- **`0007` legacy columns** (`total_tokens`/`cached_tokens`/`ttft_ms`) are intentionally left untouched/NULL on native rows — do NOT also write them; the COALESCE in Task 7 is the single reconciliation point.
- **WS-relay capture** (`ws_relay/sniff.rs`) is explicitly OUT of scope (flag-off by default) — a clean follow-up once this HTTP path is proven.
- If any task requires editing a wedge-sacred file to proceed, STOP and escalate — the design guarantees it does not.
