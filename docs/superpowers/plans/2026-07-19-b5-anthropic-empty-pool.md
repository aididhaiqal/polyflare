# B5-anthropic — Layer-2 recovery-wait for `/v1/messages` empty-pool Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend PolyFlare's B5 anti-starvation Layer-1/Layer-2 recovery-wait (today Codex `/responses`-only) to BOTH Anthropic `/v1/messages` empty-pool paths — the native Anthropic path and the aliased (Anthropic-client → Codex-pool) path — so an exhausted pool holds the client with SSE keepalives and serves on recovery instead of a fail-fast 503.

**Architecture:** The Layer-1/Layer-2 machinery (`try_layer1_serve_now`, `try_layer2_recovery_wait`, `layer2_wait_stream`) is already provider-agnostic except for (a) one hard-coded `Provider::Codex` account filter and (b) Codex-shaped SSE frames (the `response.failed` in-band error frame + the `: keepalive` comment) plus (c) verbatim forwarding of the recovered upstream stream. This feature introduces **two orthogonal axes**: a `pool_provider: Provider` param (which accounts to wait on) and a `WaitClient` enum (which SSE dialect the *client* speaks — Codex `response.failed` vs Anthropic `error`/`ping`, and whether the recovered stream must be Codex→Anthropic translated). `WaitClient::AnthropicTranslated` carries a **translator factory** (`AnthropicToResponses` is response-side stateless, so a fresh translator per serve is correct), letting BOTH mutually-exclusive layers wrap their recovered stream without threading a single moved instance.

**Tech Stack:** Rust, axum 0.8 SSE, `async_stream`, tokio. Touches `crates/polyflare-server/src/{ingress.rs, starvation.rs}` and reuses `crates/polyflare-anthropic/src/translate.rs` (`AnthropicToResponses`) + `crates/polyflare-server/src/translate_stream.rs` (`wrap_translating_stream`).

## Global Constraints

- **The wedge fix is sacred.** Do NOT touch `ObservingStream::poll_next` (`watchdog.rs`) or its logic. The empty-pool paths are wedge-neutral BY CONSTRUCTION: both Anthropic handlers prepare via `NoopContinuity` (always `Disarmed`), and the empty-pool branch is strictly *pre-selection* — no `ObservingStream` is ever constructed until a splice succeeds. NOTE (whole-branch review correction): the recovery splice itself (`try_layer1_serve_now` / `layer2_wait_stream`) passes `state.continuity.clone()` — i.e. `CodexContinuity`, NOT `NoopContinuity` — into `execute_recovery_tracked`. Wedge-neutrality still holds, but via a DIFFERENT mechanism than the handler prepare: `session_key` is `None` on both Anthropic paths, and `CodexContinuity::observe` guards every repo write behind `if let Some(sk) = session_key`, so with `None` it is a total no-op (no owner/anchor row is ever written); additionally the recovered splice reaches `wrap_stream` with `OutcomeKind::Recovered` and never goes through the Armed peek path at all. So the inertness comes from `session_key = None`, NOT from NoopContinuity — do not "fix" this by forcing NoopContinuity into the splice, and do not assume NoopContinuity is what guarantees it. Preserve `session_key = None` on the Anthropic empty-pool paths; do not arm continuity anywhere.
- **Content-free forever.** The new Anthropic in-band error frame must embed ONLY fixed text + the compile-time-fixed `StarvationOutcome::code()` label — NEVER upstream error text, conversation content, tokens, or emails. Do NOT reuse the translator's `build_error_event` (`translate.rs:635`), which copies upstream `message` verbatim.
- **No Codex behavior change from T1–T4.** Every existing `/responses` starvation path must remain byte-identical: the Codex keepalive stays `: keepalive\n\n`, the Codex error frame stays `response.failed`, the recovered Codex stream stays verbatim-forwarded. `WaitClient::Codex` reproduces today's behavior exactly; the Codex call sites pass it. Existing starvation regression tests are the guard.
- **Security floor inviolable.** `sel_ctx` (carrying `require_security_work_authorized`) is cloned UNCHANGED into every layer; the post-wait re-select only refreshes `now`. Do not mutate it.
- **Disable lever preserved.** `POLYFLARE_STARVATION_WAIT_BUDGET_SECS=0` ⇒ `budget.is_zero()` ⇒ Layer 2 returns `None` before committing any 200 or emitting any keepalive, for the Anthropic paths too.
- **Workspace stays fmt-clean + clippy clean UNDER `-D warnings`** (CI setting). Avoid raw multi-element tuples in any `query_as`; use `f64::total_cmp` for float sorts; watch `too_many_arguments` (the layer fns already carry `#[allow(clippy::too_many_arguments)]` — keep it).
- **Never log tokens/bearers.**

---

## File Structure

- **Modify** `crates/polyflare-server/src/starvation.rs` — add `anthropic_in_band_error_frame` + `anthropic_ping_frame` (T2), and a content-safety test.
- **Modify** `crates/polyflare-server/src/ingress.rs` — add the `pool_provider` param (T1), the `WaitClient` enum + dialect methods + thread it through the three layer fns (T3), the `AnthropicTranslated` variant + wire both Anthropic handlers (T3 native, T4 aliased).
- **Test** — extend the existing starvation e2e suite (find it: `crates/polyflare-server/tests/starvation*.rs` or `grep -rl starvation crates/polyflare-server/tests`) with a native-Anthropic case (T3) and an aliased case (T4).

---

## Task 1: Parameterize the pool provider on the Layer-2 combinator

**Files:**
- Modify: `crates/polyflare-server/src/ingress.rs` (`layer2_wait_stream` ~line 839, `try_layer2_recovery_wait` ~line 652, and the `/responses` call sites of `try_layer2_recovery_wait`)

**Interfaces:**
- Consumes: nothing new.
- Produces: `layer2_wait_stream(..., pool_provider: Provider, ...)` and `try_layer2_recovery_wait(..., pool_provider: Provider, ...)` — the hard-coded `Provider::Codex` at the in-combinator filter (currently `ingress.rs:930`) becomes the passed param. Pure refactor, ZERO behavior change (all call sites pass `Provider::Codex`).

- [ ] **Step 1: Establish the RED guard (no new test — existing starvation regressions ARE the guard)**

Run the existing starvation suite first to capture the green baseline you must preserve:
```
grep -rl "starvation\|layer2\|keepalive" crates/polyflare-server/tests | head
cargo test -p polyflare-server layer2 2>&1 | tail -15
cargo test -p polyflare-server starvation 2>&1 | tail -15
```
Expected: all green. These prove T1 is behavior-preserving.

- [ ] **Step 2: Add the `pool_provider` param**

In `layer2_wait_stream` (signature ~`ingress.rs:839-852`), add a `pool_provider: Provider` parameter (place it right after `pool: Option<String>`). Replace the hard-code at `ingress.rs:930`:
```rust
        let mut fresh_snapshots =
            filter_by_provider_and_pool(&fresh_snapshots, pool_provider, pool.as_deref());
```
In `try_layer2_recovery_wait` (signature ~`ingress.rs:652-664`), add `pool_provider: Provider` (after `pool: Option<String>`) and forward it into the `layer2_wait_stream(...)` call (~`ingress.rs:693-706`).

- [ ] **Step 3: Update the Codex `/responses` call sites**

Find every call to `try_layer2_recovery_wait` (there are three: the `RouteDecision::NoEligibleAccount` arm ~`ingress.rs:1963`, and inside `run_failover_loop` ~`ingress.rs:1770` and ~`ingress.rs:1915` — verify with `grep -n try_layer2_recovery_wait crates/polyflare-server/src/ingress.rs`). Each passes `Provider::Codex` for the new param. No other change.

- [ ] **Step 4: Verify no behavior change**

Run: `cargo test -p polyflare-server layer2 2>&1 | tail -15` and `cargo test -p polyflare-server starvation 2>&1 | tail -15`
Expected: identical green to Step 1. Then `cargo clippy -p polyflare-server --all-targets -- -D warnings` clean + `cargo fmt --all`.

- [ ] **Step 5: Commit**

```bash
git add crates/polyflare-server/src/ingress.rs
git commit -m "refactor(server): parameterize pool_provider on layer2 recovery-wait (B5-anthropic T1)"
```

---

## Task 2: Anthropic-format in-band error + ping frames (content-safety crux)

**Files:**
- Modify: `crates/polyflare-server/src/starvation.rs` (add two builders + a content-safety test)

**Interfaces:**
- Consumes: the existing `StarvationOutcome` enum + its `code()` (`starvation.rs:58-64`, three fixed labels).
- Produces:
  - `pub fn anthropic_in_band_error_frame(outcome: StarvationOutcome) -> Bytes` — the Anthropic `error` SSE event.
  - `pub fn anthropic_ping_frame() -> Bytes` — the Anthropic `ping` keepalive event.

- [ ] **Step 1: Write the failing content-safety tests**

Add to `starvation.rs`'s `#[cfg(test)] mod tests` (mirror the existing `in_band_error_frame_carries_only_the_fixed_reason_code_and_message` test):

```rust
#[test]
fn anthropic_error_frame_is_the_anthropic_error_event_shape_and_content_free() {
    let frame = anthropic_in_band_error_frame(StarvationOutcome::BudgetExceeded);
    let s = std::str::from_utf8(&frame).unwrap();
    // Anthropic SSE error event shape: `event: error\ndata: {"type":"error","error":{...}}\n\n`
    assert!(s.starts_with("event: error\n"));
    assert!(s.contains("\"type\":\"error\""));
    assert!(s.contains("\"error\":{"));
    // A valid, fixed Anthropic error type — never upstream text.
    assert!(s.contains("\"type\":\"overloaded_error\""));
    // The message is a FIXED sentence carrying only our own fixed outcome code label.
    assert!(s.contains(StarvationOutcome::BudgetExceeded.code()));
    assert!(s.ends_with("\n\n"));
    // The three fixed outcome codes are the ONLY variable content.
    for oc in [
        StarvationOutcome::BudgetExceeded,
        StarvationOutcome::StillNothing,
        StarvationOutcome::ExecutorError,
    ] {
        let f = anthropic_in_band_error_frame(oc);
        let t = std::str::from_utf8(&f).unwrap();
        assert!(t.contains(oc.code()));
    }
}

#[test]
fn anthropic_ping_frame_is_a_typed_ping_event() {
    let frame = anthropic_ping_frame();
    let s = std::str::from_utf8(&frame).unwrap();
    assert_eq!(s, "event: ping\ndata: {\"type\":\"ping\"}\n\n");
}
```

- [ ] **Step 2: Run to verify fail**

Run: `cargo test -p polyflare-server --lib starvation::tests::anthropic 2>&1 | tail -15` (or `cargo test -p polyflare-server anthropic_error_frame 2>&1`)
Expected: FAIL — functions not defined.

- [ ] **Step 3: Implement**

Add to `starvation.rs` (near `in_band_error_frame`, ~line 78):

```rust
/// The Anthropic `/v1/messages` streaming equivalent of [`in_band_error_frame`]: a single
/// `event: error` SSE frame in Anthropic's shape (`{"type":"error","error":{"type":..,"message":..}}`,
/// see `polyflare_anthropic::translate::AnthropicToResponses`'s `build_error_event`), for the
/// Anthropic empty-pool Layer-2 paths (native + aliased). **Content-free by construction**: a FIXED
/// Anthropic error `type` (`overloaded_error` — the closest match for "no capacity available") plus a
/// FIXED sentence carrying ONLY the compile-time [`StarvationOutcome::code`] label — never upstream
/// error text, unlike the translator's `build_error_event`.
pub fn anthropic_in_band_error_frame(outcome: StarvationOutcome) -> Bytes {
    Bytes::from(format!(
        "event: error\ndata: {{\"type\":\"error\",\"error\":{{\"type\":\"overloaded_error\",\
         \"message\":\"starvation recovery wait ended without a servable account ({})\"}}}}\n\n",
        outcome.code()
    ))
}

/// The Anthropic keepalive: a typed `ping` event (what the real Anthropic streaming API emits),
/// used in place of the Codex `: keepalive` SSE comment on the Anthropic Layer-2 wait paths.
pub fn anthropic_ping_frame() -> Bytes {
    Bytes::from_static(b"event: ping\ndata: {\"type\":\"ping\"}\n\n")
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p polyflare-server anthropic_error_frame 2>&1 | tail -8` and `cargo test -p polyflare-server anthropic_ping 2>&1 | tail -8`
Expected: PASS. Then clippy `-D warnings` + fmt.

- [ ] **Step 5: Commit**

```bash
git add crates/polyflare-server/src/starvation.rs
git commit -m "feat(server): content-free Anthropic in-band error + ping frames for Layer-2 (B5-anthropic T2)"
```

---

## Task 3: `WaitClient` dialect seam + wire native Anthropic Layer-1/Layer-2

**Files:**
- Modify: `crates/polyflare-server/src/ingress.rs` (add `WaitClient`; thread it through `try_layer1_serve_now`, `try_layer2_recovery_wait`, `layer2_wait_stream`; wire `messages_handler_native`; update `/responses` + failover call sites to pass `WaitClient::Codex`)
- Test: extend the starvation e2e suite with a native-Anthropic case

**Interfaces:**
- Consumes: T1's `pool_provider` param, T2's `starvation::{anthropic_in_band_error_frame, anthropic_ping_frame}`, existing `starvation::keepalive_item`/`in_band_error_frame`, `ResponseStream`.
- Produces:
  - `enum WaitClient { Codex, Anthropic }` (T4 adds a third variant), deriving `Clone`.
  - `impl WaitClient { fn error_frame(&self, outcome: StarvationOutcome) -> Bytes; fn keepalive_item(&self) -> Result<Bytes, std::convert::Infallible> /* match the existing item type */; fn wrap_recovered(&self, stream: ResponseStream) -> ResponseStream }`
  - All three layer fns gain a `client: WaitClient` (or `&WaitClient` for Layer-1) parameter.

**Note on the keepalive item type:** the wait loop currently `yield starvation::keepalive_item()`. Check `keepalive_item`'s exact return type (`starvation.rs:90-92`) and make `WaitClient::keepalive_item()` return the SAME `Item` type the stream yields, so `Codex` returns exactly today's value (`: keepalive`) and `Anthropic` returns `Ok(starvation::anthropic_ping_frame())`. If matching the item type is awkward, instead add `WaitClient::keepalive_bytes(&self) -> Bytes` and change the yield site to `yield Ok(client.keepalive_bytes());` — but then verify the Codex bytes are byte-identical to today's `KEEPALIVE_FRAME` (`: keepalive\n\n`).

- [ ] **Step 1: Write the failing native-Anthropic e2e**

Find the existing starvation e2e harness (`grep -rl "starvation\|layer2_wait\|keepalive" crates/polyflare-server/tests`) and read its Codex empty-pool→recovery test to copy the pattern (seed accounts in the test store, rate-limit them so the pool is empty, drive a request, assert keepalive-then-serve). Write a native-Anthropic analogue:
- Seed ONE `provider="anthropic"` account directly in the test store (bypass OAuth — the Codex test's seeding helper, adapted to provider "anthropic"), with a mock Anthropic upstream that returns a valid Anthropic SSE stream.
- Rate-limit it (or set a cooldown recover_at a short time out) so the first `/v1/messages` select finds an empty pool but `soonest_recover` has a `Cooldown` target.
- Drive a native `/v1/messages` request (a model that does NOT alias to Codex → native path).
- Assert: HTTP 200; at least one `event: ping` keepalive frame arrives during the wait; after recovery the real Anthropic stream is spliced through; and (separately, forcing budget-exceeded via `POLYFLARE_STARVATION_WAIT_BUDGET_SECS` small) the `event: error` Anthropic frame is emitted with the fixed message — and a seeded sentinel email/token never appears in the response bytes.

Write it fully against the real harness; do not invent helper names — read the sibling Codex test first.

- [ ] **Step 2: Run to verify fail**

Run: `cargo test -p polyflare-server --test <starvation_test_file> native_anthropic 2>&1 | tail -20`
Expected: FAIL — the native `/v1/messages` empty-pool branch returns a fast 503 (no keepalive, no `event: ping`).

- [ ] **Step 3: Implement the `WaitClient` seam**

Add near `layer2_wait_stream` in `ingress.rs`:
```rust
/// Which SSE dialect the *client* of a Layer-1/Layer-2 recovery-wait speaks — orthogonal to
/// `pool_provider` (which accounts we wait ON). Owns every dialect-specific frame the wait emits
/// (keepalive, in-band error) plus how the recovered upstream stream reaches the client (verbatim,
/// or Codex→Anthropic translated). `Clone` is cheap (unit variants; T4's variant holds an `Arc`).
#[derive(Clone)]
enum WaitClient {
    /// Codex `/responses` client: `response.failed` error frames, `: keepalive` comments, recovered
    /// Codex stream forwarded verbatim. Byte-identical to pre-B5-anthropic behavior.
    Codex,
    /// Native Anthropic `/v1/messages` client: `event: error` frames, `event: ping` keepalives,
    /// recovered Anthropic stream forwarded verbatim.
    Anthropic,
    // T4 adds: AnthropicTranslated(Arc<dyn Fn() -> Box<dyn Translator> + Send + Sync>)
}

impl WaitClient {
    fn error_frame(&self, outcome: starvation::StarvationOutcome) -> Bytes {
        match self {
            WaitClient::Codex => starvation::in_band_error_frame(outcome),
            WaitClient::Anthropic => starvation::anthropic_in_band_error_frame(outcome),
        }
    }
    /// The keepalive frame bytes for this dialect.
    fn keepalive_bytes(&self) -> Bytes {
        match self {
            WaitClient::Codex => Bytes::from_static(starvation::KEEPALIVE_FRAME),
            WaitClient::Anthropic => starvation::anthropic_ping_frame(),
        }
    }
    /// How the recovered upstream stream reaches the client. Verbatim for Codex + native Anthropic.
    /// (T4's `AnthropicTranslated` wraps it in a fresh translator.)
    fn wrap_recovered(&self, stream: ResponseStream) -> ResponseStream {
        match self {
            WaitClient::Codex | WaitClient::Anthropic => stream,
        }
    }
}
```
(If `KEEPALIVE_FRAME` is private, make it `pub(crate)` in `starvation.rs`, or expose the bytes via a helper.)

Thread it:
- `layer2_wait_stream(..., client: WaitClient, ...)`: at the loop keepalive yield (~`ingress.rs:888`) → `yield Ok(client.keepalive_bytes());`; at each of the FIVE `in_band_error_frame` yields (~904, 925, 944, 961, 1011) → `yield Ok(client.error_frame(<outcome>));`; at the recovered forward (~997-999) → wrap first: `let mut real_stream = client.wrap_recovered(real_stream);` then the existing `while let Some(item) = real_stream.next().await { yield item; }`.
- `try_layer2_recovery_wait(..., client: WaitClient, ...)`: forward `client` into `layer2_wait_stream`.
- `try_layer1_serve_now(..., client: &WaitClient, ...)`: at its served-stream site (~`ingress.rs:581`) → `Ok(stream) => stream_response(client.wrap_recovered(stream)),`.

Update the Codex `/responses` + `run_failover_loop` call sites of `try_layer1_serve_now`/`try_layer2_recovery_wait` to pass `WaitClient::Codex` (`&WaitClient::Codex` for Layer-1). No Codex behavior change.

- [ ] **Step 4: Wire `messages_handler_native`**

In `messages_handler_native` (~`ingress.rs:2077-2165`), the empty-pool branch (~2127-2130) currently:
```rust
let picked = match selector.pick(&snapshots, &sel_ctx) {
    Some(id) => id,
    None => return (no_eligible(), outcome),
};
```
Replace the `None` arm with the Layer-1→Layer-2→`no_eligible` fallthrough (mirror the `/responses` `NoEligibleAccount` arm, ~`ingress.rs:1939-1973`), reading `state.starvation_wait_budget`/`state.starvation_heartbeat` directly from `state`, passing `pool_provider = Provider::Anthropic` and `client = WaitClient::Anthropic`. Because `try_layer1_serve_now`/`try_layer2_recovery_wait` return `Option<Response>` and this handler returns `(Response, RouteOutcome)`, wrap: on `Some(resp)` return `(resp, outcome)`, else fall through to `(no_eligible(), outcome)`. Thread `req`/`ctx`/`session_key`/`snapshots`/`selector`/`sel_ctx`/`now`/`&mut outcome` exactly as the native handler already has them (note native builds `req`/`ctx` earlier in the handler — reuse those; `session_key` is `None` on the native path since `NoopContinuity` never keys one — pass `None`).

- [ ] **Step 5: Run to verify pass**

Run: `cargo test -p polyflare-server --test <starvation_test_file> native_anthropic 2>&1 | tail -20`
Expected: PASS. Then the full guard: `cargo test -p polyflare-server layer2 2>&1 | tail`, `cargo test -p polyflare-server starvation 2>&1 | tail` (Codex regressions still green), `cargo clippy -p polyflare-server --all-targets -- -D warnings`, `cargo fmt --all`.

- [ ] **Step 6: Commit**

```bash
git add crates/polyflare-server/src/ingress.rs crates/polyflare-server/src/starvation.rs crates/polyflare-server/tests/
git commit -m "feat(server): WaitClient dialect seam + native Anthropic /v1/messages Layer-2 recovery-wait (B5-anthropic T3)"
```

---

## Task 4: Translator-aware recovered stream + wire aliased path

**Files:**
- Modify: `crates/polyflare-server/src/ingress.rs` (add the `AnthropicTranslated` `WaitClient` variant; wire `messages_handler_codex_aliased`)
- Test: extend the starvation e2e with an aliased case

**Interfaces:**
- Consumes: T3's `WaitClient` + `wrap_recovered` dispatch, `polyflare_core::translate::Translator`, `polyflare_anthropic::translate::AnthropicToResponses`, `crate::translate_stream::wrap_translating_stream`.
- Produces: `WaitClient::AnthropicTranslated(Arc<dyn Fn() -> Box<dyn Translator> + Send + Sync>)` + its `error_frame`/`keepalive_bytes`/`wrap_recovered` arms.

**Why a factory (design rationale — verify before implementing):** `AnthropicToResponses`'s state (`message_start_emitted`, `next_block_index`, `blocks` — `translate.rs:325-341`) is built entirely from the RESPONSE stream inside `translate_response_event`; `translate_request` does not set it. So a FRESH `AnthropicToResponses::new()` translates a recovered Codex response correctly. Confirm this by reading `translate_request` (`translate.rs:674`) — it must not mutate those three fields. Because Layer-1 and Layer-2 are mutually exclusive but the wait fn moves its state into an `async_stream` generator, a factory (`Fn() -> Box<dyn Translator>`, called on the serve path) is cleaner than threading one moved instance across the try-layer1-then-layer2 sequence.

- [ ] **Step 1: Write the failing aliased e2e**

Mirror T3's e2e for the aliased path:
- Seed ONE `provider="codex"` account in the test store with a mock Codex upstream returning a valid Codex `/responses` SSE stream, and set a short cooldown so the pool is momentarily empty.
- Drive a `/v1/messages` request whose model ALIASES to Codex (e.g. `opus→gpt-5.6-sol` — use whatever alias the test config registers; check `alias::lookup_alias` / the test's alias setup).
- Assert: HTTP 200; `event: ping` keepalives arrive during the wait (NOT swallowed — this is the crux the `TranslatingStream` comment-drop would break); after recovery the recovered Codex stream reaches the client TRANSLATED to Anthropic SSE (assert `event: message_start` … `event: message_stop` frames appear, i.e. Anthropic event names, NOT raw Codex `response.*`); and on forced budget-exceeded, the `event: error` Anthropic frame appears. Content-safety: seeded sentinel email/token absent from the response bytes.

- [ ] **Step 2: Run to verify fail**

Run: `cargo test -p polyflare-server --test <starvation_test_file> aliased 2>&1 | tail -20`
Expected: FAIL — the aliased `/v1/messages` empty-pool returns a fast 503.

- [ ] **Step 3: Implement the `AnthropicTranslated` variant**

Add the variant + arms to `WaitClient`:
```rust
    /// Anthropic client served from a Codex pool (the `/v1/messages`→Codex alias path): `event:
    /// error`/`event: ping` frames like `Anthropic`, but the recovered CODEX stream is wrapped in a
    /// FRESH translator (response-side state is built from the stream, so a fresh instance is
    /// correct — see this task's rationale) and emitted as Anthropic SSE.
    AnthropicTranslated(std::sync::Arc<dyn Fn() -> Box<dyn Translator> + Send + Sync>),
```
`error_frame`: `AnthropicTranslated(_) => starvation::anthropic_in_band_error_frame(outcome)` (same as `Anthropic`).
`keepalive_bytes`: `AnthropicTranslated(_) => starvation::anthropic_ping_frame()` (same as `Anthropic`).
`wrap_recovered`:
```rust
    WaitClient::AnthropicTranslated(factory) => {
        crate::translate_stream::wrap_translating_stream(stream, factory())
    }
```
(Add `use polyflare_core::translate::Translator;` if not already imported.) The keepalives and error frames are emitted by the OUTER wait stream (already Anthropic-native bytes) and are NEVER routed through the translator — only `wrap_recovered` touches the translator, and only the recovered real stream — so the `event: ping` keepalives survive (the `TranslatingStream` comment-drop at `translate_stream.rs:60` never sees them).

- [ ] **Step 4: Wire `messages_handler_codex_aliased`**

In `messages_handler_codex_aliased` (~`ingress.rs:2258-2261`), replace the empty-pool `None` arm with the same Layer-1→Layer-2→`no_eligible` fallthrough as T3, but passing `pool_provider = Provider::Codex` and:
```rust
let client = WaitClient::AnthropicTranslated(std::sync::Arc::new(|| {
    Box::new(AnthropicToResponses::new()) as Box<dyn Translator>
}));
```
Use the same `req` (`prepared.req`), `ctx`, `snapshots`, `selector`, `sel_ctx`, `now` the handler already built; `session_key = None`. Return `(resp, outcome)` on `Some`, else `(no_eligible(), outcome)`.

**Note:** the normal (non-empty-pool) aliased success path keeps using its own `translator` instance built at `ingress.rs:2193` — do NOT change that. The factory here is ONLY for the recovery-wait's fresh serve.

- [ ] **Step 5: Run to verify pass**

Run: `cargo test -p polyflare-server --test <starvation_test_file> aliased 2>&1 | tail -20`
Expected: PASS (ping keepalives present + Anthropic-translated recovered stream). Then the full guard: `cargo test -p polyflare-server 2>&1 | tail -15` (no regressions), `cargo clippy -p polyflare-server --all-targets -- -D warnings`, `cargo fmt --all`.

- [ ] **Step 6: Commit**

```bash
git add crates/polyflare-server/src/ingress.rs crates/polyflare-server/tests/
git commit -m "feat(server): translator-aware Layer-2 recovery-wait for aliased /v1/messages (B5-anthropic T4)"
```

---

## Self-Review

**1. Spec coverage:**
- Parameterize the hard-coded `Provider::Codex` → **T1** ✓.
- New Anthropic-format in-band error frame (content-free, not the translator's leaky `build_error_event`) → **T2** ✓.
- Native Anthropic empty-pool Layer-2 → **T3** ✓ (WaitClient::Anthropic + wiring).
- Aliased empty-pool with translator-aware keepalive-survival + recovered-stream translation → **T4** ✓ (AnthropicTranslated factory; keepalives emitted outside the translator so they survive the comment-drop; recovered stream translated).
- Wedge-neutral (NoopContinuity, no ObservingStream pre-splice) → asserted as a Global Constraint; no task arms continuity.
- Layer-1 aliased translation (the easy-to-miss second serve site at `ingress.rs:581`) → covered because T3 routes Layer-1's served stream through `client.wrap_recovered`, which T4's variant translates.

**2. Placeholder scan:** The e2e steps say "find the existing starvation harness / read the sibling Codex test" and "use whatever alias the test config registers" — these are explicit directions to read named, existing test infrastructure rather than invent helpers, with the exact assertions spelled out. The keepalive-item-type note (T3 Step 3) gives a concrete fallback (`keepalive_bytes` + `yield Ok(...)`) if the item type is awkward. No TBD/"handle errors"/vague steps remain.

**3. Type consistency:** `WaitClient` (T3) gains one variant in T4 (additive enum; all three `impl` methods get the new arm). `pool_provider: Provider` (T1) and `client: WaitClient` (T3) are threaded through the SAME three fns consistently. `wrap_recovered(&self, ResponseStream) -> ResponseStream` signature is stable T3→T4. The translator factory type `Arc<dyn Fn() -> Box<dyn Translator> + Send + Sync>` matches `wrap_translating_stream(stream, Box<dyn Translator>)`'s second arg.

**Adversarial-review crux (flag for reviewers):**
- **T2 content-safety** — the Anthropic error frame must embed ONLY fixed text + the fixed `outcome.code()`; never upstream text. The e2e sentinel assertion (T3/T4) is the end-to-end guard.
- **T4 keepalive survival** — the make-or-break property is that `event: ping` keepalives reach the aliased client (they're emitted by the outer wait stream, NOT wrapped in `TranslatingStream`; only the recovered real stream is). The aliased e2e MUST assert a ping frame actually arrives, or it's the "green but vacuous" trap. And the recovered stream must show Anthropic event names (`message_start`…`message_stop`), proving translation happened.
- **T4 fresh-translator correctness** — confirm `translate_request` doesn't set the response-side state fields, so a fresh translator per serve is faithful.
- **No Codex regression** — `WaitClient::Codex` must reproduce today's exact bytes (`: keepalive`, `response.failed`, verbatim recovered stream); existing starvation regressions are the guard on every task.
