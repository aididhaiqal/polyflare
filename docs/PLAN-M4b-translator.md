# PolyFlare M4b — Translator Crux Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build PolyFlare's Anthropic→OpenAI-Responses translation machinery as a self-contained, independently-tested unit: reshape the `Translator` trait to a per-turn stateful 1→N seam (SPEC-M4 §3.4), implement the concrete `AnthropicToResponses` translator — request mapping plus the full streaming response-event state machine (SPEC-M4 §3.5) — and prove it correct with golden replay tests over synthetic, doc-verified fixtures. Routing integration and the model-alias config are **explicitly out of scope** (see "Deferred to M4b-wiring" below).

**Architecture:** `Translator` becomes `&mut self`-based, with `translate_response_event` returning `Vec<Value>` instead of `Value`. `TranslatorRegistry` stores a `TranslatorFactory` (`Fn() -> Box<dyn Translator>`) per `(Format, Format)` pair instead of a shared instance, so every turn gets a fresh, independent stateful translator. This reshape lives entirely inside `polyflare-core/src/translate.rs` — a workspace-wide grep confirms no other crate constructs or calls the registry yet (M4a's `ingress.rs` routes by `Provider` alone, with no translator seam wired in), so the ripple is contained to one file. The concrete `AnthropicToResponses` translator lives in `polyflare-anthropic` (which already depends on `polyflare-core`, keeping the dependency direction intact — `polyflare-core` must never depend on a provider crate). It is built in two layers: a stateless `map_request` function for the request-body mechanical mapping (SPEC §3.6's "mechanical direction"), and a stateful struct that assembles synthesized ids/indices/sequence-numbers and buffers accumulated text/arguments per SPEC §3.5's event table for the response-stream side. Golden tests feed synthetic, doc-verified Anthropic SSE event sequences through a fresh instance and assert the emitted OpenAI-Responses sequence.

**Tech Stack:** Rust 2021, `serde_json::Value` as the event representation (see decision below), `rand` 0.9 (new dependency in `polyflare-anthropic`, for synthesizing ids — already a workspace dependency, used the same way in `polyflare-core/src/select.rs`).

## Design decision: `Value` over a typed `SseEvent`

Both trait methods keep `serde_json::Value` — SPEC-M4 §3.4's sketch offered a typed `SseEvent` as an alternative; this plan does **not** introduce one. Rationale:

1. **The codebase already treats every wire event as `Value` end-to-end**, with no separate SSE `event:` line ever parsed into a discriminant. `crates/polyflare-server/src/watchdog.rs` strips the `data:` line prefix (`line.strip_prefix("data:")`, line 304) and parses straight into `serde_json::Value` (line 310), then dispatches on `event["type"].as_str()`. `PreparedRequest.body` (`polyflare-core/src/types.rs`) is `Value` too. Introducing `SseEvent` here would be the only typed-event representation in the whole request path — a new pattern, not a reuse of an existing one.
2. **Both wire formats are open-ended, evolving JSON shapes.** A hand-typed enum covering every Anthropic + OpenAI-Responses event `type` would need re-verification against docs on every SPEC revision; `Value` degrades gracefully (`.get(...)` → `None`) for fields not yet modeled, which matches how this codebase already treats request/response bodies.
3. **One JSON representation flows through the whole path** — no conversion boundary to keep in sync between the executor's raw bytes, the continuity layer's sniffing (`watchdog.rs`), and the translator.

Trade-off accepted: no compile-time exhaustiveness over event `type` strings. Mitigated by Task 4's golden tests asserting the exact set of types this translator dispatches on, and by every `match` in Task 3 having an explicit fallback arm (`_ => vec![]`) so an unrecognized event type is dropped, not a panic.

## Global Constraints

*Every task's requirements implicitly include this section. Values are copied verbatim from SPEC-M4 + the standing project gates.*

- **Rust edition 2021**, workspace resolver `2`. New code compiles clean under `cargo clippy --workspace --all-targets -- -D warnings`.
- **Streaming stays non-buffering.** Each incoming event's outputs are emitted immediately as `translate_response_event` returns — never held back to be flushed later. The *only* buffering is per-turn **state** (accumulated block text/arguments, held so the eventual `.done`/`response.completed` event can carry the full accumulated string per SPEC §3.5) — this is a property of the mapping (some Anthropic events genuinely produce zero output events, e.g. `signature_delta`, `message_delta`), not of delayed delivery.
- **Redacting `Debug` + a redaction test on every secret-bearing type.** `AnthropicToResponses` buffers accumulated block text (assistant output text, tool-call arguments, and — most sensitive — extended-thinking content) as per-turn state; its `Debug` impl must never print that content in clear (mirrors `PreparedRequest`/`ReasoningItems`/`RecoveryPlan` in `polyflare-core/src/types.rs`, each of which has exactly this kind of manual redacting `Debug` + a dedicated test).
- **Real-wire-shape fixtures.** Golden fixtures (Task 4) are the SPEC-M4 §3.5 doc-verified event shapes — synthesized from the mapping table, **not** real captures. Real-capture validation (U4) is a later, separate refinement; this plan's fixtures are flagged as synthetic in the test file's module doc comment.
- **The reshape (Task 1) must keep the identity path + all existing tests green.** Zero behavior change for same-format pairs: `IdentityTranslator` still passes bodies/events through unchanged, just under the new signatures.
- **Model-alias remap and payload-override are OUT of scope here** (SPEC-M4 U2 — the exact `opus→sol` pairs are pending user confirmation). `map_request` does the *mechanical* field mapping only; `model` passes through unchanged, unmapped.
- **No routing/wiring in this plan.** `AnthropicToResponses` is never registered into a `TranslatorRegistry` instance here — that requires `AppState`-level wiring that would need `polyflare-server` to construct a registry spanning both `polyflare-core` and `polyflare-anthropic` types, which is exactly the deferred M4b-wiring phase (see below). This plan proves `AnthropicToResponses` correct as a standalone unit, constructed directly (`AnthropicToResponses::new()`) and driven through the `Translator` trait in tests.
- **Gates before EVERY commit:** `cargo fmt --all -- --check` → `cargo clippy --workspace --all-targets -- -D warnings` → `cargo test --workspace`, all green.

## Deferred to M4b-wiring (explicitly out of scope)

Per the task framing, this plan builds the translator as a **self-contained, independently-tested unit**. The following are follow-on work, gated on user input, and are *not* touched by any task below:

1. **Routing integration** — wiring `TranslatorRegistry`/`AnthropicToResponses` into `ingress.rs` so a Claude/`/v1/messages` client's request can actually reach the Codex pool (SPEC-M4 §3.1's full dispatch pipeline: picking a Codex-provider account for an Anthropic-format request, threading the translator through `execute_with_watchdog`, translating the response stream back). Today `messages_handler` and `responses_handler` each hard-filter to their own provider (`filter_by_provider(&snapshots, Provider::Anthropic \| Codex)`) precisely because no cross-format translator exists yet (see the comments at `crates/polyflare-server/src/ingress.rs:140-142` and `:292-294`) — this plan does not change that filter.
2. **The model-alias mapping pairs/config** (SPEC-M4 U2: `opus→sol`/`sonnet→terra`/`haiku→luna`, effort injection, config source). `map_request` leaves `model` untouched.

## Spec gaps hit while planning (flagging, not silently resolving)

1. **`content_block_stop`'s three "done" events aren't uniformly applicable across block kinds.** SPEC §3.5's table lists `content_block_stop` → `.output_text.done` / `.function_call_arguments.done` / `.reasoning_summary_text.done` **+ `.content_part.done` + `.output_item.done`** as if every stop emits all three unconditionally — but `content_block_start` only opens a `content_part` for **text** blocks (tool_use and thinking blocks get only `output_item.added`, no `content_part.added`, per the same table's start-side rows). This plan resolves the inconsistency by pairing `.done` events with whatever was `.added`: text blocks emit `output_text.done` + `content_part.done` + `output_item.done`; tool_use blocks emit `function_call_arguments.done` + `output_item.done` (no content-part pair, since none was opened); thinking blocks emit `reasoning_summary_text.done` + `output_item.done` (same reasoning). Task 3 documents this inline.
2. **No `response.reasoning_summary_part.added`/`.done` companion event for thinking blocks.** SPEC §3.5's table gives thinking blocks only `output_item.added`/`.done` (type `reasoning`), never a summary-part open/close pair, even though `thinking_delta` targets a `summary_index`. This plan does not synthesize one (nothing in the table calls for it) — flagged as a candidate gap for the live-capture pass (§7) to confirm against a real OpenAI-Responses reasoning-item stream.
3. **`output_index` vs `content_index` split (SPEC §7's own open risk).** This plan resolves it: every Anthropic content block (regardless of kind) becomes its own top-level OpenAI output item — `output_index` is Anthropic's own flat `index` field reused verbatim (not a separately-minted counter), `content_index`/`summary_index` are always `0` (single part per item). This matches §3.5's table structure (each block-kind row mints its own `item.id`) more directly than an alternative "parts within one message item" reading would. Still VERIFY-gated per §7 against a real 2-block capture.
4. **`error.type` → `code` mapping** (mid-stream `error` SSE event) has no canonical table (SPEC admits this in §3.5's row and again in §7). This plan passes the Anthropic `error.type` string through verbatim as `code` — a real, tested, working simplification, not a placeholder — flagged as VERIFY-gated per §7.
5. **`stop_reason` → `status` mapping** only has two documented cases (`end_turn`→`completed`, `max_tokens`→`incomplete`). This plan defaults every other `stop_reason` (`stop_sequence`, `tool_use`, etc.) to `completed` — the safe fallback, and the same gap SPEC §7 already flags ("no canonical mapping exists... pin them with a captured example each").
6. **Usage-field provenance across events isn't pinned by SPEC.** SPEC's usage-mapping table (§3.5) describes cumulative-at-completion values without saying which of `message_start`'s or `message_delta`'s `usage` object carries which subfield (real Anthropic API `message_delta.usage` often carries only `output_tokens`, not `input_tokens`). This plan **merges** `usage` objects across `message_start` and every `message_delta` (each new object's keys overwrite prior ones, additively) so a partial `message_delta.usage` never drops an `input_tokens` value seen only at `message_start`. Documented and unit-tested in Task 3.

---

## File Structure

**New files**
- `crates/polyflare-anthropic/src/translate.rs` — `map_request` (Task 2) + `AnthropicToResponses` (Task 3).
- `crates/polyflare-anthropic/tests/golden_translate.rs` — golden replay tests. (Task 4)

**Modified files**
- `crates/polyflare-core/src/translate.rs` — `Translator` trait reshape, `TranslatorFactory`, `TranslatorRegistry::create`. (Task 1)
- `crates/polyflare-anthropic/src/lib.rs` — register + export the new `translate` module. (Tasks 2–3)
- `crates/polyflare-anthropic/Cargo.toml` — add `rand` dependency. (Task 3)

No other file in the workspace references `Translator`/`TranslatorRegistry` today (verified by workspace-wide grep during planning), so Task 1 touches no call site beyond `polyflare-core/src/translate.rs` itself — `polyflare-core/src/lib.rs`'s re-export list (`pub use translate::{IdentityTranslator, Translator, TranslatorRegistry};`) needs no edit since all three names are unchanged.

---

## Task 1: Reshape `Translator` to stateful 1→N + factory registry

**Files:**
- Modify: `crates/polyflare-core/src/translate.rs`

**Interfaces:**
- Produces:
  - `pub trait Translator: Send + Sync { fn translate_request(&mut self, body: Value) -> Value; fn translate_response_event(&mut self, event: Value) -> Vec<Value>; }`
  - `pub struct IdentityTranslator;` — trivial impl (`translate_request` returns input; `translate_response_event` returns `vec![event]`).
  - `pub type TranslatorFactory = Box<dyn Fn() -> Box<dyn Translator> + Send + Sync>;`
  - `pub struct TranslatorRegistry` with `pub fn new() -> Self`, `pub fn with_defaults() -> Self`, `pub fn register(&mut self, from: Format, to: Format, factory: TranslatorFactory)`, `pub fn create(&self, from: Format, to: Format) -> Option<Box<dyn Translator>>` (renamed from `get` — the old name implied borrowing a shared instance; `create` makes the "fresh instance per call" contract explicit at the call site).
- Consumes: `crate::format::Format` (unchanged).

- [ ] **Step 1: Update the existing tests to the new shape (this is the failing test)**

Replace the `#[cfg(test)] mod tests` block in `crates/polyflare-core/src/translate.rs` with:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::Format;
    use serde_json::json;

    #[test]
    fn identity_registered_for_same_format_pairs() {
        let reg = TranslatorRegistry::with_defaults();
        assert!(reg
            .create(Format::OpenAIResponses, Format::OpenAIResponses)
            .is_some());
        assert!(reg
            .create(Format::AnthropicMessages, Format::AnthropicMessages)
            .is_some());
    }

    #[test]
    fn cross_format_pair_absent_in_m1() {
        let reg = TranslatorRegistry::with_defaults();
        assert!(reg
            .create(Format::AnthropicMessages, Format::OpenAIResponses)
            .is_none());
    }

    #[test]
    fn identity_translator_passes_through() {
        let mut t = IdentityTranslator;
        let body = json!({"model": "gpt-5.6-sol", "input": "hi"});
        assert_eq!(t.translate_request(body.clone()), body);
        let ev = json!({"type": "response.completed"});
        assert_eq!(t.translate_response_event(ev.clone()), vec![ev]);
    }

    struct CountingTranslator {
        count: u32,
    }

    impl Translator for CountingTranslator {
        fn translate_request(&mut self, body: Value) -> Value {
            body
        }
        fn translate_response_event(&mut self, _event: Value) -> Vec<Value> {
            self.count += 1;
            vec![json!({"count": self.count})]
        }
    }

    #[test]
    fn factory_produces_independent_stateful_instances() {
        let mut reg = TranslatorRegistry::new();
        reg.register(
            Format::OpenAIResponses,
            Format::AnthropicMessages,
            Box::new(|| Box::new(CountingTranslator { count: 0 }) as Box<dyn Translator>),
        );

        let mut a = reg
            .create(Format::OpenAIResponses, Format::AnthropicMessages)
            .unwrap();
        assert_eq!(
            a.translate_response_event(json!({})),
            vec![json!({"count": 1})]
        );
        assert_eq!(
            a.translate_response_event(json!({})),
            vec![json!({"count": 2})]
        );

        // A second `create` for the same pair must yield a FRESH instance — `a`'s state must
        // never leak into `b`.
        let mut b = reg
            .create(Format::OpenAIResponses, Format::AnthropicMessages)
            .unwrap();
        assert_eq!(
            b.translate_response_event(json!({})),
            vec![json!({"count": 1})]
        );
    }
}
```

- [ ] **Step 2: Run tests to verify they fail to compile**

Run: `cargo test -p polyflare-core translate`
Expected: FAIL to compile — `no method named \`create\` found`, `expected \`&mut self\` in the method signature`, `expected \`Vec<Value>\`, found \`Value\`` (the trait/impl above `mod tests` is still the old shape).

- [ ] **Step 3: Reshape the trait, `IdentityTranslator`, and the registry**

Replace everything above `#[cfg(test)]` in `crates/polyflare-core/src/translate.rs` with:

```rust
//! Translator registry: the multi-provider spine. Same-format pairs are identity (zero cost).
//! `Translator` is a per-turn STATEFUL 1→N seam (SPEC-M4 §3.4): a cross-format translator (e.g.
//! Anthropic SSE → OpenAI-Responses SSE) needs per-turn state (message id, content-block index,
//! accumulated usage) and one incoming event can produce zero, one, or many outgoing events. The
//! registry stores a FACTORY per `(from, to)` pair so every turn gets its own fresh translator
//! instance — state never leaks across turns or requests.

use std::collections::HashMap;

use serde_json::Value;

use crate::format::Format;

/// Translates request and streaming-response JSON between two wire formats. Stateful: an
/// instance is scoped to a single turn (one request + its response stream), never shared across
/// turns.
pub trait Translator: Send + Sync {
    /// Rewrite an outgoing request body (e.g. model-alias remap + reasoning-effort injection —
    /// both deferred past this trait's concrete M4b translator; see SPEC-M4 §3.6/U2).
    fn translate_request(&mut self, body: Value) -> Value;
    /// Translate one incoming response-stream event into zero, one, or many outgoing events.
    fn translate_response_event(&mut self, event: Value) -> Vec<Value>;
}

/// Pass-through translator used for same-format `(F, F)` pairs. Stateless — a fresh instance per
/// turn costs nothing.
pub struct IdentityTranslator;

impl Translator for IdentityTranslator {
    fn translate_request(&mut self, body: Value) -> Value {
        body
    }
    fn translate_response_event(&mut self, event: Value) -> Vec<Value> {
        vec![event]
    }
}

/// Builds a fresh `Box<dyn Translator>` for one turn. Stored per `(from, to)` pair in the
/// registry; call `TranslatorRegistry::create` once per turn so per-turn state never leaks
/// across requests.
pub type TranslatorFactory = Box<dyn Fn() -> Box<dyn Translator> + Send + Sync>;

/// Registry keyed by `(from, to)` format, storing a translator FACTORY (not a shared instance)
/// per pair.
pub struct TranslatorRegistry {
    map: HashMap<(Format, Format), TranslatorFactory>,
}

impl TranslatorRegistry {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// Registry with the M1 defaults: identity for the two native same-format pairs.
    pub fn with_defaults() -> Self {
        let mut reg = Self::new();
        reg.register(
            Format::OpenAIResponses,
            Format::OpenAIResponses,
            Box::new(|| Box::new(IdentityTranslator) as Box<dyn Translator>),
        );
        reg.register(
            Format::AnthropicMessages,
            Format::AnthropicMessages,
            Box::new(|| Box::new(IdentityTranslator) as Box<dyn Translator>),
        );
        reg
    }

    pub fn register(&mut self, from: Format, to: Format, factory: TranslatorFactory) {
        self.map.insert((from, to), factory);
    }

    /// Build a fresh translator instance for this `(from, to)` pair, or `None` if unregistered.
    /// Call once per turn — the returned instance carries state for that turn only.
    pub fn create(&self, from: Format, to: Format) -> Option<Box<dyn Translator>> {
        self.map.get(&(from, to)).map(|factory| factory())
    }
}

impl Default for TranslatorRegistry {
    fn default() -> Self {
        Self::with_defaults()
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p polyflare-core translate`
Expected: PASS — 4 tests (`identity_registered_for_same_format_pairs`, `cross_format_pair_absent_in_m1`, `identity_translator_passes_through`, `factory_produces_independent_stateful_instances`).

- [ ] **Step 5: Run the full workspace gate**

Run: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace`
Expected: all green — this reshape touches only `polyflare-core/src/translate.rs`, and no other file in the workspace references `Translator`/`TranslatorRegistry`, so nothing else should even recompile differently.

- [ ] **Step 6: Commit**

```bash
git add crates/polyflare-core/src/translate.rs
git commit -m "feat(m4b): reshape Translator trait to stateful 1->N + factory registry"
```

---

## Task 2: `map_request` — Anthropic Messages → OpenAI-Responses request mapping

**Files:**
- Create: `crates/polyflare-anthropic/src/translate.rs`
- Modify: `crates/polyflare-anthropic/src/lib.rs`

**Interfaces:**
- Produces: `fn map_request(body: serde_json::Value) -> serde_json::Value` (crate-private to `polyflare-anthropic`; Task 3 calls it from the same module). Maps `model` (passthrough, **no alias remap** — SPEC-M4 U2 is deferred), `system`→`instructions`, `messages`→`input`, `stream` (passthrough, defaults to `false` if absent), `max_tokens`→`max_output_tokens` (omitted if absent), `tools`→`tools` (omitted if absent).
- Consumes: nothing new (`serde_json::Value` only).

- [ ] **Step 1: Write the failing tests**

Create `crates/polyflare-anthropic/src/translate.rs`:

```rust
//! Anthropic Messages → OpenAI-Responses translator (SPEC-M4 §3.4 stateful 1→N seam). This file
//! builds the mapping in two layers: `map_request` (this task) does the doc-verified *mechanical*
//! request-body field mapping (SPEC-M4 §3.6's "mechanical direction") — model-alias remap and
//! reasoning-effort payload-override are explicitly deferred (SPEC-M4 U2, M4b-wiring), so `model`
//! passes through unchanged here. `AnthropicToResponses` (added on top of this module) is the
//! stateful streaming response-event translator (SPEC-M4 §3.5).

use serde_json::{json, Value};

/// Map an Anthropic Messages request body to an OpenAI-Responses request body. Mechanical only —
/// no model-alias remap, no payload-override (SPEC-M4 U2, deferred to M4b-wiring).
fn map_request(body: Value) -> Value {
    let model = body.get("model").cloned().unwrap_or(Value::Null);
    let system = body.get("system").cloned();
    let messages = body
        .get("messages")
        .cloned()
        .unwrap_or_else(|| Value::Array(vec![]));
    let stream = body
        .get("stream")
        .cloned()
        .unwrap_or(Value::Bool(false));
    let max_tokens = body.get("max_tokens").cloned();
    let tools = body.get("tools").cloned();

    let mut out = json!({
        "model": model,
        "input": messages,
        "stream": stream,
    });
    let map = out.as_object_mut().expect("json! object literal");
    if let Some(sys) = system {
        map.insert("instructions".to_string(), sys);
    }
    if let Some(mt) = max_tokens {
        map.insert("max_output_tokens".to_string(), mt);
    }
    if let Some(t) = tools {
        map.insert("tools".to_string(), t);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_model_messages_stream_and_max_tokens() {
        let body = json!({
            "model": "claude-opus-4-1-20250805",
            "messages": [{"role": "user", "content": [{"type": "text", "text": "hi"}]}],
            "stream": true,
            "max_tokens": 1024
        });
        let out = map_request(body);
        assert_eq!(out["model"], json!("claude-opus-4-1-20250805"));
        assert_eq!(out["stream"], json!(true));
        assert_eq!(out["max_output_tokens"], json!(1024));
        assert_eq!(
            out["input"],
            json!([{"role": "user", "content": [{"type": "text", "text": "hi"}]}])
        );
    }

    #[test]
    fn maps_system_prompt_to_instructions() {
        let body = json!({
            "model": "claude-sonnet-4-5-20250929",
            "system": "You are a helpful assistant.",
            "messages": [],
            "stream": true
        });
        let out = map_request(body);
        assert_eq!(out["instructions"], json!("You are a helpful assistant."));
    }

    #[test]
    fn passes_tools_through_when_present() {
        let tools = json!([{"name": "get_weather", "input_schema": {"type": "object"}}]);
        let body = json!({
            "model": "claude-opus-4-1-20250805",
            "messages": [],
            "stream": true,
            "tools": tools.clone()
        });
        let out = map_request(body);
        assert_eq!(out["tools"], tools);
    }

    #[test]
    fn omits_optional_fields_when_absent() {
        let body = json!({"model": "claude-haiku-4-5-20251001", "messages": [], "stream": false});
        let out = map_request(body);
        assert!(out.get("instructions").is_none());
        assert!(out.get("max_output_tokens").is_none());
        assert!(out.get("tools").is_none());
    }

    #[test]
    fn defaults_stream_false_when_absent() {
        let body = json!({"model": "claude-opus-4-1-20250805", "messages": []});
        let out = map_request(body);
        assert_eq!(out["stream"], json!(false));
    }

    #[test]
    fn does_not_remap_model_alias() {
        // SPEC-M4 U2: the exact opus/sonnet/haiku -> sol/terra/luna pairs are pending user
        // confirmation. `map_request` must never guess at a remap.
        let body = json!({"model": "claude-opus-4-1-20250805", "messages": []});
        let out = map_request(body);
        assert_eq!(out["model"], json!("claude-opus-4-1-20250805"));
    }
}
```

Register the module in `crates/polyflare-anthropic/src/lib.rs` — modify the existing file:

```rust
//! Anthropic backend: HTTP executor (M4a), rate-limit/error classification (M4a), OAuth (M4a,
//! VERIFY-gated), the cross-format translator (M4b). Byte-parity fingerprinting is M5.

pub mod errors;
pub mod executor;
pub mod translate;

pub use errors::{
    classify_status, parse_retry_after_secs, AnthropicErrorBody, AnthropicErrorDetail,
    AnthropicErrorType, StatusClass,
};
pub use executor::AnthropicExecutor;
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p polyflare-anthropic translate`
Expected: FAIL — either a compile error (if `map_request` weren't yet defined) or, since it's defined above, this step should already compile; run it once to confirm the tests actually execute the intended assertions before moving on. (If for some reason the crate fails to compile — e.g. a typo — fix it here before Step 3, since Step 3 is deliberately a no-op for this task.)

- [ ] **Step 3: (No implementation step needed — Step 1 already wrote the real implementation.)**

This task's TDD loop collapses Steps 1 and 3 because `map_request` has no per-turn state to build incrementally — it is a single pure mapping function. Confirm the tests defined in Step 1 pass as-is.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p polyflare-anthropic translate`
Expected: PASS — 6 tests.

- [ ] **Step 5: Run the full workspace gate**

Run: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace`
Expected: all green. Clippy may flag `map_request` as dead code (`never used`) since nothing calls it yet outside its own tests — Task 3 consumes it in the next commit, so if clippy fails on `-D warnings` here, add `#[allow(dead_code)]` temporarily above `fn map_request` and remove the attribute in Task 3 once it's called from `AnthropicToResponses::translate_request`.

- [ ] **Step 6: Commit**

```bash
git add crates/polyflare-anthropic/src/translate.rs crates/polyflare-anthropic/src/lib.rs
git commit -m "feat(m4b): AnthropicToResponses request mapping (map_request)"
```

---

## Task 3: `AnthropicToResponses` — the stateful streaming response translator

**Files:**
- Modify: `crates/polyflare-anthropic/src/translate.rs`
- Modify: `crates/polyflare-anthropic/src/lib.rs`
- Modify: `crates/polyflare-anthropic/Cargo.toml`

**Interfaces:**
- Consumes: `polyflare_core::Translator` (exact trait from Task 1: `fn translate_request(&mut self, body: Value) -> Value; fn translate_response_event(&mut self, event: Value) -> Vec<Value>;`); this task's own `map_request(body: Value) -> Value` (Task 2, same module).
- Produces: `pub struct AnthropicToResponses { .. }` with `pub fn new() -> Self`, implementing `Translator`. Exported as `polyflare_anthropic::AnthropicToResponses`. A manual, redacting `impl std::fmt::Debug for AnthropicToResponses`.

### Step group A — struct skeleton + `message_start`

- [ ] **Step 1: Add the `rand` dependency**

Modify `crates/polyflare-anthropic/Cargo.toml`, adding to `[dependencies]`:

```toml
rand = { workspace = true }
```

- [ ] **Step 2: Write the failing test**

Add to the bottom of the `#[cfg(test)] mod tests` block in `crates/polyflare-anthropic/src/translate.rs` (keep all Task 2 tests above it):

```rust
    use polyflare_core::Translator;

    #[test]
    fn message_start_emits_created_then_in_progress_with_synthesized_response_id() {
        let mut t = AnthropicToResponses::new();
        let events = t.translate_response_event(json!({
            "type": "message_start",
            "message": {
                "id": "msg_01XYZ",
                "model": "claude-opus-4-1-20250805",
                "role": "assistant",
                "content": [],
                "usage": {"input_tokens": 25, "output_tokens": 1}
            }
        }));

        assert_eq!(events.len(), 2, "message_start must emit exactly 2 events immediately");
        assert_eq!(events[0]["type"], json!("response.created"));
        assert_eq!(events[1]["type"], json!("response.in_progress"));

        let seq0 = events[0]["sequence_number"].as_u64().unwrap();
        let seq1 = events[1]["sequence_number"].as_u64().unwrap();
        assert!(seq1 > seq0, "sequence_number must be monotonically increasing");

        let resp_id = events[0]["response"]["id"].as_str().unwrap().to_string();
        assert!(!resp_id.is_empty());
        assert_eq!(events[1]["response"]["id"], json!(resp_id));
        assert_eq!(events[0]["response"]["model"], json!("claude-opus-4-1-20250805"));
        assert_eq!(events[0]["response"]["status"], json!("in_progress"));
        assert_eq!(events[0]["response"]["usage"], Value::Null);
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test -p polyflare-anthropic message_start_emits`
Expected: FAIL to compile — `cannot find type \`AnthropicToResponses\` in this scope`.

- [ ] **Step 4: Implement the struct skeleton + `message_start` handling**

Insert above `#[cfg(test)]` in `crates/polyflare-anthropic/src/translate.rs` (after `map_request`):

```rust
use std::collections::HashMap;

use polyflare_core::Translator;
use rand::Rng;

/// The kind of an open Anthropic content block, tracked so `content_block_delta`/`_stop` know
/// which OpenAI-Responses event family to emit.
#[derive(Clone, Debug, PartialEq, Eq)]
enum BlockKind {
    Text,
    ToolUse,
    Thinking,
}

/// Per-block per-turn state: the synthesized OpenAI item id, the tool call_id/name (tool_use
/// only), and the buffered accumulated text/arguments (SPEC-M4 §3.5: "full accumulated
/// text/arguments [S] (buffered across deltas)").
#[derive(Clone, Debug)]
struct BlockState {
    kind: BlockKind,
    item_id: String,
    call_id: Option<String>,
    name: Option<String>,
    buffer: String,
}

/// Stateful per-turn Anthropic→OpenAI-Responses translator (SPEC-M4 §3.4/§3.5). Construct a
/// fresh instance per turn via `AnthropicToResponses::new()` — never reuse one across requests.
#[derive(Default)]
pub struct AnthropicToResponses {
    seq: u64,
    response_id: Option<String>,
    model: Option<Value>,
    blocks: HashMap<u64, BlockState>,
    order: Vec<u64>,
    usage: Option<Value>,
    stop_reason: Option<String>,
}

impl AnthropicToResponses {
    pub fn new() -> Self {
        Self::default()
    }

    fn next_seq(&mut self) -> u64 {
        let n = self.seq;
        self.seq += 1;
        n
    }

    /// Shallow-merge an incoming Anthropic `usage` object into accumulated per-turn usage.
    /// Anthropic splits usage across `message_start` (typically `input_tokens`) and each
    /// `message_delta` (typically `output_tokens`, updated cumulatively) — merging (rather than
    /// overwriting) means a partial `message_delta.usage` never drops a field only seen at
    /// `message_start` (see "Spec gaps hit while planning", item 6).
    fn merge_usage(&mut self, incoming: &Value) {
        let entry = self.usage.get_or_insert_with(|| json!({}));
        if let (Some(obj), Some(inc_obj)) = (entry.as_object_mut(), incoming.as_object()) {
            for (k, v) in inc_obj {
                obj.insert(k.clone(), v.clone());
            }
        }
    }

    fn on_message_start(&mut self, event: &Value) -> Vec<Value> {
        let message = event.get("message").cloned().unwrap_or(Value::Null);
        let response_id = synth_id("resp");
        let model = message.get("model").cloned().unwrap_or(Value::Null);
        self.response_id = Some(response_id.clone());
        self.model = Some(model.clone());
        if let Some(usage) = message.get("usage") {
            self.merge_usage(usage);
        }

        let response = json!({
            "id": response_id,
            "object": "response",
            "status": "in_progress",
            "model": model,
            "output": [],
            "usage": Value::Null,
        });

        let created_seq = self.next_seq();
        let created = json!({
            "type": "response.created",
            "sequence_number": created_seq,
            "response": response.clone(),
        });
        let in_progress_seq = self.next_seq();
        let in_progress = json!({
            "type": "response.in_progress",
            "sequence_number": in_progress_seq,
            "response": response,
        });
        vec![created, in_progress]
    }
}

/// Mint a fresh synthesized id (`resp_...`, `msg_...`, `fc_...`, `rs_...`) — Anthropic's stream
/// carries none of `response.id`/`item.id`/`call_id` (SPEC-M4 §3.5), so these must be minted.
fn synth_id(prefix: &str) -> String {
    let mut rng = rand::rng();
    let bytes: [u8; 12] = rng.random();
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    format!("{prefix}_{hex}")
}

/// Read the flat Anthropic content-block `index` off a `content_block_start`/`_delta`/`_stop`
/// event.
fn block_index(event: &Value) -> Option<u64> {
    event.get("index").and_then(|v| v.as_u64())
}

impl Translator for AnthropicToResponses {
    fn translate_request(&mut self, body: Value) -> Value {
        map_request(body)
    }

    fn translate_response_event(&mut self, event: Value) -> Vec<Value> {
        let ty = event.get("type").and_then(|v| v.as_str()).unwrap_or("");
        match ty {
            "message_start" => self.on_message_start(&event),
            _ => vec![],
        }
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cargo test -p polyflare-anthropic message_start_emits`
Expected: PASS.

### Step group B — `content_block_start` (text / tool_use / thinking)

- [ ] **Step 6: Write the failing tests**

Add to the test module:

```rust
    #[test]
    fn content_block_start_text_emits_item_added_then_part_added() {
        let mut t = AnthropicToResponses::new();
        t.translate_response_event(json!({
            "type": "message_start",
            "message": {"id": "msg_1", "model": "claude-opus-4-1-20250805", "usage": {"input_tokens": 10, "output_tokens": 0}}
        }));
        let events = t.translate_response_event(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": ""}
        }));

        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["type"], json!("response.output_item.added"));
        assert_eq!(events[0]["output_index"], json!(0));
        assert_eq!(events[0]["item"]["type"], json!("message"));
        assert_eq!(events[0]["item"]["status"], json!("in_progress"));
        let item_id = events[0]["item"]["id"].as_str().unwrap().to_string();
        assert!(!item_id.is_empty());

        assert_eq!(events[1]["type"], json!("response.content_part.added"));
        assert_eq!(events[1]["item_id"], json!(item_id));
        assert_eq!(events[1]["output_index"], json!(0));
        assert_eq!(events[1]["content_index"], json!(0));
        assert_eq!(events[1]["part"]["type"], json!("output_text"));
    }

    #[test]
    fn content_block_start_tool_use_emits_only_item_added_with_call_id_from_anthropic() {
        let mut t = AnthropicToResponses::new();
        t.translate_response_event(json!({
            "type": "message_start",
            "message": {"id": "msg_2", "model": "claude-opus-4-1-20250805", "usage": {"input_tokens": 10, "output_tokens": 0}}
        }));
        let events = t.translate_response_event(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "tool_use", "id": "toolu_01AAA", "name": "get_weather", "input": {}}
        }));

        assert_eq!(events.len(), 1, "tool_use opens no content_part — only output_item.added");
        assert_eq!(events[0]["type"], json!("response.output_item.added"));
        assert_eq!(events[0]["output_index"], json!(0));
        assert_eq!(events[0]["item"]["type"], json!("function_call"));
        assert_eq!(events[0]["item"]["call_id"], json!("toolu_01AAA"));
        assert_eq!(events[0]["item"]["name"], json!("get_weather"));
        assert_eq!(events[0]["item"]["arguments"], json!(""));
    }

    #[test]
    fn content_block_start_thinking_emits_only_item_added_reasoning() {
        let mut t = AnthropicToResponses::new();
        t.translate_response_event(json!({
            "type": "message_start",
            "message": {"id": "msg_3", "model": "claude-opus-4-1-20250805", "usage": {"input_tokens": 10, "output_tokens": 0}}
        }));
        let events = t.translate_response_event(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "thinking", "thinking": "", "signature": ""}
        }));

        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], json!("response.output_item.added"));
        assert_eq!(events[0]["item"]["type"], json!("reasoning"));
        assert_eq!(events[0]["item"]["status"], json!("in_progress"));
    }
```

- [ ] **Step 7: Run tests to verify they fail**

Run: `cargo test -p polyflare-anthropic content_block_start`
Expected: FAIL — all three assert `events.len()` against an actual length of `0` (the `_ => vec![]` fallback currently swallows `content_block_start`).

- [ ] **Step 8: Implement `on_content_block_start`**

Add to `impl AnthropicToResponses` (below `on_message_start`):

```rust
    fn on_content_block_start(&mut self, event: &Value) -> Vec<Value> {
        let Some(idx) = block_index(event) else {
            return vec![];
        };
        let block = event.get("content_block").cloned().unwrap_or(Value::Null);
        let kind_str = block.get("type").and_then(|v| v.as_str()).unwrap_or("");

        match kind_str {
            "text" => {
                let item_id = synth_id("msg");
                self.blocks.insert(
                    idx,
                    BlockState {
                        kind: BlockKind::Text,
                        item_id: item_id.clone(),
                        call_id: None,
                        name: None,
                        buffer: String::new(),
                    },
                );
                self.order.push(idx);

                let item = json!({
                    "id": item_id,
                    "type": "message",
                    "status": "in_progress",
                    "role": "assistant",
                    "content": [],
                });
                let added_seq = self.next_seq();
                let item_added = json!({
                    "type": "response.output_item.added",
                    "sequence_number": added_seq,
                    "output_index": idx,
                    "item": item,
                });

                let part = json!({"type": "output_text", "text": "", "annotations": []});
                let part_seq = self.next_seq();
                let part_added = json!({
                    "type": "response.content_part.added",
                    "sequence_number": part_seq,
                    "item_id": item_id,
                    "output_index": idx,
                    "content_index": 0,
                    "part": part,
                });

                vec![item_added, part_added]
            }
            "tool_use" => {
                let call_id = block
                    .get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let name = block
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let item_id = synth_id("fc");
                self.blocks.insert(
                    idx,
                    BlockState {
                        kind: BlockKind::ToolUse,
                        item_id: item_id.clone(),
                        call_id: Some(call_id.clone()),
                        name: Some(name.clone()),
                        buffer: String::new(),
                    },
                );
                self.order.push(idx);

                let item = json!({
                    "id": item_id,
                    "type": "function_call",
                    "status": "in_progress",
                    "call_id": call_id,
                    "name": name,
                    "arguments": "",
                });
                let seq = self.next_seq();
                vec![json!({
                    "type": "response.output_item.added",
                    "sequence_number": seq,
                    "output_index": idx,
                    "item": item,
                })]
            }
            "thinking" => {
                let item_id = synth_id("rs");
                self.blocks.insert(
                    idx,
                    BlockState {
                        kind: BlockKind::Thinking,
                        item_id: item_id.clone(),
                        call_id: None,
                        name: None,
                        buffer: String::new(),
                    },
                );
                self.order.push(idx);

                let item = json!({
                    "id": item_id,
                    "type": "reasoning",
                    "status": "in_progress",
                    "summary": [],
                });
                let seq = self.next_seq();
                vec![json!({
                    "type": "response.output_item.added",
                    "sequence_number": seq,
                    "output_index": idx,
                    "item": item,
                })]
            }
            _ => vec![],
        }
    }
```

Wire it into the dispatch `match` in `translate_response_event`:

```rust
    fn translate_response_event(&mut self, event: Value) -> Vec<Value> {
        let ty = event.get("type").and_then(|v| v.as_str()).unwrap_or("");
        match ty {
            "message_start" => self.on_message_start(&event),
            "content_block_start" => self.on_content_block_start(&event),
            _ => vec![],
        }
    }
```

- [ ] **Step 9: Run tests to verify they pass**

Run: `cargo test -p polyflare-anthropic content_block_start`
Expected: PASS — 3 tests.

### Step group C — `content_block_delta` (text / tool args / thinking / signature)

- [ ] **Step 10: Write the failing tests**

```rust
    fn started_text_translator() -> AnthropicToResponses {
        let mut t = AnthropicToResponses::new();
        t.translate_response_event(json!({
            "type": "message_start",
            "message": {"id": "msg_1", "model": "claude-opus-4-1-20250805", "usage": {"input_tokens": 10, "output_tokens": 0}}
        }));
        t.translate_response_event(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": ""}
        }));
        t
    }

    #[test]
    fn text_delta_emits_output_text_delta_immediately_per_event() {
        let mut t = started_text_translator();
        let e1 = t.translate_response_event(json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": "Hello"}
        }));
        assert_eq!(e1.len(), 1);
        assert_eq!(e1[0]["type"], json!("response.output_text.delta"));
        assert_eq!(e1[0]["delta"], json!("Hello"));
        assert_eq!(e1[0]["content_index"], json!(0));
        assert_eq!(e1[0]["logprobs"], json!([]));

        let e2 = t.translate_response_event(json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": " world"}
        }));
        assert_eq!(e2.len(), 1);
        assert_eq!(e2[0]["delta"], json!(" world"));
        assert!(
            e2[0]["sequence_number"].as_u64().unwrap() > e1[0]["sequence_number"].as_u64().unwrap()
        );
    }

    #[test]
    fn input_json_delta_emits_function_call_arguments_delta() {
        let mut t = AnthropicToResponses::new();
        t.translate_response_event(json!({
            "type": "message_start",
            "message": {"id": "msg_2", "model": "claude-opus-4-1-20250805", "usage": {"input_tokens": 10, "output_tokens": 0}}
        }));
        t.translate_response_event(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "tool_use", "id": "toolu_01AAA", "name": "get_weather", "input": {}}
        }));
        let events = t.translate_response_event(json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "input_json_delta", "partial_json": "{\"location\":\"SF\"}"}
        }));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], json!("response.function_call_arguments.delta"));
        assert_eq!(events[0]["delta"], json!("{\"location\":\"SF\"}"));
    }

    #[test]
    fn thinking_delta_emits_reasoning_summary_text_delta_with_summary_index_zero() {
        let mut t = AnthropicToResponses::new();
        t.translate_response_event(json!({
            "type": "message_start",
            "message": {"id": "msg_3", "model": "claude-opus-4-1-20250805", "usage": {"input_tokens": 10, "output_tokens": 0}}
        }));
        t.translate_response_event(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "thinking", "thinking": "", "signature": ""}
        }));
        let events = t.translate_response_event(json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "thinking_delta", "thinking": "Let me think..."}
        }));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], json!("response.reasoning_summary_text.delta"));
        assert_eq!(events[0]["summary_index"], json!(0));
        assert_eq!(events[0]["delta"], json!("Let me think..."));
    }

    #[test]
    fn signature_delta_emits_nothing() {
        let mut t = AnthropicToResponses::new();
        t.translate_response_event(json!({
            "type": "message_start",
            "message": {"id": "msg_4", "model": "claude-opus-4-1-20250805", "usage": {"input_tokens": 10, "output_tokens": 0}}
        }));
        t.translate_response_event(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "thinking", "thinking": "", "signature": ""}
        }));
        let events = t.translate_response_event(json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "signature_delta", "signature": "abc123sig"}
        }));
        assert_eq!(events, Vec::<Value>::new(), "signature_delta is one-to-zero");
    }
```

- [ ] **Step 11: Run tests to verify they fail**

Run: `cargo test -p polyflare-anthropic delta`
Expected: FAIL — every non-signature test asserts a non-empty `Vec` against the current `_ => vec![]` fallback for `content_block_delta`.

- [ ] **Step 12: Implement `on_content_block_delta`**

Add to `impl AnthropicToResponses`:

```rust
    fn on_content_block_delta(&mut self, event: &Value) -> Vec<Value> {
        let Some(idx) = block_index(event) else {
            return vec![];
        };
        let delta = event.get("delta").cloned().unwrap_or(Value::Null);
        let delta_type = delta.get("type").and_then(|v| v.as_str()).unwrap_or("");
        let Some(block) = self.blocks.get_mut(&idx) else {
            return vec![];
        };

        match delta_type {
            "text_delta" => {
                let text = delta
                    .get("text")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                block.buffer.push_str(&text);
                let item_id = block.item_id.clone();
                let seq = self.next_seq();
                vec![json!({
                    "type": "response.output_text.delta",
                    "sequence_number": seq,
                    "item_id": item_id,
                    "output_index": idx,
                    "content_index": 0,
                    "delta": text,
                    "logprobs": [],
                })]
            }
            "input_json_delta" => {
                let partial = delta
                    .get("partial_json")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                block.buffer.push_str(&partial);
                let item_id = block.item_id.clone();
                let seq = self.next_seq();
                vec![json!({
                    "type": "response.function_call_arguments.delta",
                    "sequence_number": seq,
                    "item_id": item_id,
                    "output_index": idx,
                    "delta": partial,
                })]
            }
            "thinking_delta" => {
                let text = delta
                    .get("thinking")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                block.buffer.push_str(&text);
                let item_id = block.item_id.clone();
                let seq = self.next_seq();
                vec![json!({
                    "type": "response.reasoning_summary_text.delta",
                    "sequence_number": seq,
                    "item_id": item_id,
                    "output_index": idx,
                    "summary_index": 0,
                    "delta": text,
                })]
            }
            // signature_delta (one-to-zero, SPEC-M4 §3.5: no OpenAI event carries a reasoning
            // signature) and any unrecognized delta type both emit nothing.
            _ => vec![],
        }
    }
```

Wire it into the dispatch `match`:

```rust
            "content_block_start" => self.on_content_block_start(&event),
            "content_block_delta" => self.on_content_block_delta(&event),
            _ => vec![],
```

- [ ] **Step 13: Run tests to verify they pass**

Run: `cargo test -p polyflare-anthropic delta`
Expected: PASS — 4 tests.

### Step group D — `content_block_stop` (the `.done` triads)

- [ ] **Step 14: Write the failing tests**

```rust
    #[test]
    fn content_block_stop_text_emits_done_triad_with_full_accumulated_text() {
        let mut t = started_text_translator();
        t.translate_response_event(json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": "Hello"}
        }));
        t.translate_response_event(json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": " world"}
        }));
        let events = t.translate_response_event(json!({"type": "content_block_stop", "index": 0}));

        assert_eq!(events.len(), 3);
        assert_eq!(events[0]["type"], json!("response.output_text.done"));
        assert_eq!(events[0]["text"], json!("Hello world"));
        assert_eq!(events[1]["type"], json!("response.content_part.done"));
        assert_eq!(events[1]["part"]["text"], json!("Hello world"));
        assert_eq!(events[2]["type"], json!("response.output_item.done"));
        assert_eq!(events[2]["item"]["status"], json!("completed"));
        assert_eq!(
            events[2]["item"]["content"][0]["text"],
            json!("Hello world")
        );
    }

    #[test]
    fn content_block_stop_tool_use_emits_only_two_done_events() {
        let mut t = AnthropicToResponses::new();
        t.translate_response_event(json!({
            "type": "message_start",
            "message": {"id": "msg_2", "model": "claude-opus-4-1-20250805", "usage": {"input_tokens": 10, "output_tokens": 0}}
        }));
        t.translate_response_event(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "tool_use", "id": "toolu_01AAA", "name": "get_weather", "input": {}}
        }));
        t.translate_response_event(json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "input_json_delta", "partial_json": "{\"location\":\"SF\"}"}
        }));
        let events = t.translate_response_event(json!({"type": "content_block_stop", "index": 0}));

        assert_eq!(events.len(), 2, "tool_use has no content_part to close");
        assert_eq!(events[0]["type"], json!("response.function_call_arguments.done"));
        assert_eq!(events[0]["arguments"], json!("{\"location\":\"SF\"}"));
        assert_eq!(events[1]["type"], json!("response.output_item.done"));
        assert_eq!(events[1]["item"]["call_id"], json!("toolu_01AAA"));
        assert_eq!(events[1]["item"]["arguments"], json!("{\"location\":\"SF\"}"));
    }

    #[test]
    fn content_block_stop_thinking_emits_only_two_done_events() {
        let mut t = AnthropicToResponses::new();
        t.translate_response_event(json!({
            "type": "message_start",
            "message": {"id": "msg_3", "model": "claude-opus-4-1-20250805", "usage": {"input_tokens": 10, "output_tokens": 0}}
        }));
        t.translate_response_event(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "thinking", "thinking": "", "signature": ""}
        }));
        t.translate_response_event(json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "thinking_delta", "thinking": "Let me think..."}
        }));
        let events = t.translate_response_event(json!({"type": "content_block_stop", "index": 0}));

        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["type"], json!("response.reasoning_summary_text.done"));
        assert_eq!(events[0]["text"], json!("Let me think..."));
        assert_eq!(events[1]["type"], json!("response.output_item.done"));
        assert_eq!(events[1]["item"]["summary"][0]["text"], json!("Let me think..."));
    }
```

- [ ] **Step 15: Run tests to verify they fail**

Run: `cargo test -p polyflare-anthropic content_block_stop`
Expected: FAIL — `content_block_stop` currently falls through `_ => vec![]`, so all three assert non-zero lengths against `0`.

- [ ] **Step 16: Implement `on_content_block_stop`**

Add to `impl AnthropicToResponses`:

```rust
    fn on_content_block_stop(&mut self, event: &Value) -> Vec<Value> {
        let Some(idx) = block_index(event) else {
            return vec![];
        };
        let Some(block) = self.blocks.get(&idx).cloned() else {
            return vec![];
        };

        match block.kind {
            BlockKind::Text => {
                let text_done_seq = self.next_seq();
                let text_done = json!({
                    "type": "response.output_text.done",
                    "sequence_number": text_done_seq,
                    "item_id": block.item_id,
                    "output_index": idx,
                    "content_index": 0,
                    "text": block.buffer,
                });
                let part = json!({"type": "output_text", "text": block.buffer, "annotations": []});
                let part_done_seq = self.next_seq();
                let part_done = json!({
                    "type": "response.content_part.done",
                    "sequence_number": part_done_seq,
                    "item_id": block.item_id,
                    "output_index": idx,
                    "content_index": 0,
                    "part": part,
                });
                let item = json!({
                    "id": block.item_id,
                    "type": "message",
                    "status": "completed",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": block.buffer, "annotations": []}],
                });
                let item_done_seq = self.next_seq();
                let item_done = json!({
                    "type": "response.output_item.done",
                    "sequence_number": item_done_seq,
                    "output_index": idx,
                    "item": item,
                });
                vec![text_done, part_done, item_done]
            }
            BlockKind::ToolUse => {
                let args_done_seq = self.next_seq();
                let args_done = json!({
                    "type": "response.function_call_arguments.done",
                    "sequence_number": args_done_seq,
                    "item_id": block.item_id,
                    "output_index": idx,
                    "arguments": block.buffer,
                });
                let item = json!({
                    "id": block.item_id,
                    "type": "function_call",
                    "status": "completed",
                    "call_id": block.call_id.clone().unwrap_or_default(),
                    "name": block.name.clone().unwrap_or_default(),
                    "arguments": block.buffer,
                });
                let item_done_seq = self.next_seq();
                let item_done = json!({
                    "type": "response.output_item.done",
                    "sequence_number": item_done_seq,
                    "output_index": idx,
                    "item": item,
                });
                vec![args_done, item_done]
            }
            BlockKind::Thinking => {
                let summary_done_seq = self.next_seq();
                let summary_done = json!({
                    "type": "response.reasoning_summary_text.done",
                    "sequence_number": summary_done_seq,
                    "item_id": block.item_id,
                    "output_index": idx,
                    "summary_index": 0,
                    "text": block.buffer,
                });
                let item = json!({
                    "id": block.item_id,
                    "type": "reasoning",
                    "status": "completed",
                    "summary": [{"type": "summary_text", "text": block.buffer}],
                });
                let item_done_seq = self.next_seq();
                let item_done = json!({
                    "type": "response.output_item.done",
                    "sequence_number": item_done_seq,
                    "output_index": idx,
                    "item": item,
                });
                vec![summary_done, item_done]
            }
        }
    }
```

Wire it into the dispatch `match`:

```rust
            "content_block_delta" => self.on_content_block_delta(&event),
            "content_block_stop" => self.on_content_block_stop(&event),
            _ => vec![],
```

- [ ] **Step 17: Run tests to verify they pass**

Run: `cargo test -p polyflare-anthropic content_block_stop`
Expected: PASS — 3 tests.

### Step group E — `message_delta` (buffers only) + `message_stop` (`response.completed` + usage)

- [ ] **Step 18: Write the failing tests**

```rust
    #[test]
    fn message_delta_emits_nothing_but_buffers_stop_reason_and_usage() {
        let mut t = started_text_translator();
        let events = t.translate_response_event(json!({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn", "stop_sequence": null},
            "usage": {"output_tokens": 8}
        }));
        assert_eq!(events, Vec::<Value>::new(), "message_delta folds into the terminal event only");
    }

    #[test]
    fn message_stop_emits_completed_with_merged_usage_and_assembled_output() {
        let mut t = started_text_translator();
        t.translate_response_event(json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": "42"}
        }));
        t.translate_response_event(json!({"type": "content_block_stop", "index": 0}));
        t.translate_response_event(json!({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn"},
            "usage": {"output_tokens": 8}
        }));
        let events = t.translate_response_event(json!({"type": "message_stop"}));

        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], json!("response.completed"));
        let response = &events[0]["response"];
        assert_eq!(response["status"], json!("completed"));
        // input_tokens came from message_start (10), never overwritten by message_delta's
        // output_tokens-only usage object -- proves the merge strategy (gap 6).
        assert_eq!(response["usage"]["input_tokens"], json!(10));
        assert_eq!(response["usage"]["output_tokens"], json!(8));
        assert_eq!(response["usage"]["total_tokens"], json!(18));
        assert_eq!(response["output"][0]["type"], json!("message"));
        assert_eq!(response["output"][0]["content"][0]["text"], json!("42"));
    }

    #[test]
    fn message_stop_maps_max_tokens_to_incomplete() {
        let mut t = started_text_translator();
        t.translate_response_event(json!({"type": "content_block_stop", "index": 0}));
        t.translate_response_event(json!({
            "type": "message_delta",
            "delta": {"stop_reason": "max_tokens"},
            "usage": {"output_tokens": 5}
        }));
        let events = t.translate_response_event(json!({"type": "message_stop"}));

        assert_eq!(events[0]["type"], json!("response.incomplete"));
        assert_eq!(events[0]["response"]["status"], json!("incomplete"));
        assert_eq!(
            events[0]["response"]["incomplete_details"]["reason"],
            json!("max_output_tokens")
        );
    }

    #[test]
    fn usage_maps_cache_read_and_thinking_tokens() {
        let mut t = started_text_translator();
        t.translate_response_event(json!({"type": "content_block_stop", "index": 0}));
        t.translate_response_event(json!({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn"},
            "usage": {"output_tokens": 15, "cache_read_input_tokens": 5, "thinking_tokens": 3}
        }));
        let events = t.translate_response_event(json!({"type": "message_stop"}));
        let usage = &events[0]["response"]["usage"];
        assert_eq!(usage["input_tokens_details"]["cached_tokens"], json!(5));
        assert_eq!(usage["output_tokens_details"]["reasoning_tokens"], json!(3));
    }
```

- [ ] **Step 19: Run tests to verify they fail**

Run: `cargo test -p polyflare-anthropic message_delta message_stop usage_maps`
Expected: FAIL — `message_delta` and `message_stop` both fall through `_ => vec![]` today (the `message_stop` tests fail on `events.len()`/index-out-of-bounds against an empty `Vec`).

- [ ] **Step 20: Implement `on_message_delta`, `map_usage`, and `on_message_stop`**

Add to `impl AnthropicToResponses`:

```rust
    fn on_message_delta(&mut self, event: &Value) -> Vec<Value> {
        if let Some(sr) = event
            .get("delta")
            .and_then(|d| d.get("stop_reason"))
            .and_then(|v| v.as_str())
        {
            self.stop_reason = Some(sr.to_string());
        }
        if let Some(usage) = event.get("usage") {
            self.merge_usage(usage);
        }
        // Folds into the terminal `response.completed`/`.incomplete` at `message_stop` (SPEC-M4
        // §3.5) -- no immediate client-visible event.
        vec![]
    }

    fn on_message_stop(&mut self, _event: &Value) -> Vec<Value> {
        let status = match self.stop_reason.as_deref() {
            Some("max_tokens") => "incomplete",
            _ => "completed",
        };

        let mut output = Vec::new();
        for idx in &self.order {
            if let Some(block) = self.blocks.get(idx) {
                let item = match block.kind {
                    BlockKind::Text => json!({
                        "id": block.item_id,
                        "type": "message",
                        "status": "completed",
                        "role": "assistant",
                        "content": [{"type": "output_text", "text": block.buffer, "annotations": []}],
                    }),
                    BlockKind::ToolUse => json!({
                        "id": block.item_id,
                        "type": "function_call",
                        "status": "completed",
                        "call_id": block.call_id.clone().unwrap_or_default(),
                        "name": block.name.clone().unwrap_or_default(),
                        "arguments": block.buffer,
                    }),
                    BlockKind::Thinking => json!({
                        "id": block.item_id,
                        "type": "reasoning",
                        "status": "completed",
                        "summary": [{"type": "summary_text", "text": block.buffer}],
                    }),
                };
                output.push(item);
            }
        }

        let usage = self
            .usage
            .as_ref()
            .map(map_usage)
            .unwrap_or(Value::Null);

        let mut response = json!({
            "id": self.response_id.clone().unwrap_or_default(),
            "object": "response",
            "status": status,
            "model": self.model.clone().unwrap_or(Value::Null),
            "output": output,
            "usage": usage,
        });
        if status == "incomplete" {
            response["incomplete_details"] = json!({"reason": "max_output_tokens"});
        }

        let event_type = if status == "incomplete" {
            "response.incomplete"
        } else {
            "response.completed"
        };
        let seq = self.next_seq();
        vec![json!({"type": event_type, "sequence_number": seq, "response": response})]
    }
```

Add the free function `map_usage` (below `synth_id`/`block_index`):

```rust
/// Map accumulated Anthropic usage to OpenAI-Responses usage (SPEC-M4 §3.5's usage table).
/// `total_tokens` has no Anthropic equivalent and is synthesized as `input + output`.
fn map_usage(anthropic: &Value) -> Value {
    let input_tokens = anthropic
        .get("input_tokens")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let output_tokens = anthropic
        .get("output_tokens")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let cached_tokens = anthropic
        .get("cache_read_input_tokens")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let reasoning_tokens = anthropic
        .get("thinking_tokens")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    json!({
        "input_tokens": input_tokens,
        "input_tokens_details": {"cached_tokens": cached_tokens},
        "output_tokens": output_tokens,
        "output_tokens_details": {"reasoning_tokens": reasoning_tokens},
        "total_tokens": input_tokens + output_tokens,
    })
}
```

Wire both handlers into the dispatch `match`:

```rust
            "content_block_stop" => self.on_content_block_stop(&event),
            "message_delta" => self.on_message_delta(&event),
            "message_stop" => self.on_message_stop(&event),
            _ => vec![],
```

- [ ] **Step 21: Run tests to verify they pass**

Run: `cargo test -p polyflare-anthropic message_delta message_stop usage_maps`
Expected: PASS — 4 tests.

### Step group F — mid-stream `error` + redacting `Debug`

- [ ] **Step 22: Write the failing tests**

```rust
    #[test]
    fn mid_stream_error_passes_through_type_as_code() {
        let mut t = AnthropicToResponses::new();
        let events = t.translate_response_event(json!({
            "type": "error",
            "error": {"type": "overloaded_error", "message": "Overloaded"}
        }));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], json!("error"));
        assert_eq!(events[0]["code"], json!("overloaded_error"));
        assert_eq!(events[0]["message"], json!("Overloaded"));
    }

    #[test]
    fn ping_emits_nothing() {
        let mut t = AnthropicToResponses::new();
        let events = t.translate_response_event(json!({"type": "ping"}));
        assert_eq!(events, Vec::<Value>::new());
    }

    #[test]
    fn debug_redacts_accumulated_block_text() {
        let mut t = AnthropicToResponses::new();
        t.translate_response_event(json!({
            "type": "message_start",
            "message": {"id": "msg_1", "model": "claude-opus-4-1-20250805", "usage": {"input_tokens": 1, "output_tokens": 1}}
        }));
        t.translate_response_event(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": ""}
        }));
        t.translate_response_event(json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": "super-secret-user-conversation"}
        }));

        let s = format!("{t:?}");
        assert!(
            !s.contains("super-secret-user-conversation"),
            "Debug must never leak accumulated block text: {s}"
        );
        assert!(
            s.contains("redacted"),
            "Debug should mark blocks redacted: {s}"
        );
    }
```

- [ ] **Step 23: Run tests to verify they fail**

Run: `cargo test -p polyflare-anthropic error_passes ping_emits debug_redacts`
Expected: FAIL — `mid_stream_error_passes_through_type_as_code` fails on `events.len()` (currently `0` via the `_ => vec![]` fallback); `debug_redacts_accumulated_block_text` fails to compile (`AnthropicToResponses` derives no `Debug` today, so `format!("{t:?}")` doesn't compile). `ping_emits_nothing` already passes (both `ping` and every other unhandled type fall through the same `_ => vec![]` arm) — keep it as a regression pin, not a new failure.

- [ ] **Step 24: Implement `on_error` + the manual redacting `Debug`**

Add to `impl AnthropicToResponses`:

```rust
    fn on_error(&mut self, event: &Value) -> Vec<Value> {
        let error = event.get("error").cloned().unwrap_or(Value::Null);
        let code = error
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("api_error");
        let message = error.get("message").and_then(|v| v.as_str()).unwrap_or("");
        let seq = self.next_seq();
        vec![json!({
            "type": "error",
            "sequence_number": seq,
            "code": code,
            "message": message,
        })]
    }
```

Wire it into the dispatch `match` (the final shape of `translate_response_event`):

```rust
    fn translate_response_event(&mut self, event: Value) -> Vec<Value> {
        let ty = event.get("type").and_then(|v| v.as_str()).unwrap_or("");
        match ty {
            "message_start" => self.on_message_start(&event),
            "content_block_start" => self.on_content_block_start(&event),
            "content_block_delta" => self.on_content_block_delta(&event),
            "content_block_stop" => self.on_content_block_stop(&event),
            "message_delta" => self.on_message_delta(&event),
            "message_stop" => self.on_message_stop(&event),
            "error" => self.on_error(&event),
            // `ping` (keepalive) and any unrecognized event type: no client-visible mapping.
            _ => vec![],
        }
    }
```

Add the manual redacting `Debug` impl (below the `impl Translator for AnthropicToResponses` block):

```rust
// `blocks` buffers accumulated assistant text / tool-call arguments / extended-thinking content
// per turn and must never be printed in clear via `{:?}` (mirrors `PreparedRequest`/
// `ReasoningItems` in `polyflare-core::types`).
impl std::fmt::Debug for AnthropicToResponses {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnthropicToResponses")
            .field("seq", &self.seq)
            .field("response_id", &self.response_id)
            .field("model", &self.model)
            .field(
                "blocks",
                &format!("[{} block(s) redacted]", self.blocks.len()),
            )
            .field("stop_reason", &self.stop_reason)
            .field("usage", &self.usage)
            .finish()
    }
}
```

Note: `#[derive(Default)]` on `AnthropicToResponses` is unaffected by adding a manual `Debug` impl (they're independent derives/impls) — do not add `Debug` to the `#[derive(...)]` list.

Finally, export `AnthropicToResponses` from `crates/polyflare-anthropic/src/lib.rs`:

```rust
pub use translate::AnthropicToResponses;
```

- [ ] **Step 25: Run tests to verify they pass**

Run: `cargo test -p polyflare-anthropic error_passes ping_emits debug_redacts`
Expected: PASS — 3 tests.

- [ ] **Step 26: Run the full workspace gate**

Run: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace`
Expected: all green. If clippy flags the now-unused `#[allow(dead_code)]` left over from Task 2 (since `map_request` is now called from `translate_request`), remove that attribute.

- [ ] **Step 27: Commit**

```bash
git add crates/polyflare-anthropic/src/translate.rs crates/polyflare-anthropic/src/lib.rs crates/polyflare-anthropic/Cargo.toml
git commit -m "feat(m4b): AnthropicToResponses stateful streaming response translator"
```

---

## Task 4: Golden replay tests

**Files:**
- Create: `crates/polyflare-anthropic/tests/golden_translate.rs`

**Interfaces:**
- Consumes: `polyflare_anthropic::AnthropicToResponses::new() -> Self` (Task 3); `polyflare_core::Translator::{translate_request, translate_response_event}` (Task 1).
- Produces: nothing new — this task only adds tests.

- [ ] **Step 1: Write the golden replay tests**

Create `crates/polyflare-anthropic/tests/golden_translate.rs`:

```rust
//! Golden replay tests for `AnthropicToResponses` (SPEC-M4 §3.4/§3.5). Fixtures below are
//! SYNTHETIC — built directly from SPEC-M4 §3.5's doc-verified event-mapping table, not captured
//! from a real Claude/Codex request. Real-capture validation is a later, separate refinement
//! (SPEC-M4 U4) — these fixtures only prove the mapping matches the documented table.

use polyflare_anthropic::AnthropicToResponses;
use polyflare_core::Translator;
use serde_json::{json, Value};

/// Feed a full Anthropic SSE event sequence through a FRESH translator instance (never reused
/// across turns, per SPEC-M4 §3.4) and flatten every emitted `Vec<Value>` into one ordered
/// sequence, in the order the events were produced.
fn replay(events: Vec<Value>) -> Vec<Value> {
    let mut t = AnthropicToResponses::new();
    let mut out = Vec::new();
    for event in events {
        out.extend(t.translate_response_event(event));
    }
    out
}

/// Every emitted event must carry a `sequence_number`, and the full sequence must be strictly
/// increasing -- this is the property golden replay must hold regardless of fixture shape.
fn assert_sequence_numbers_monotonic(events: &[Value]) {
    let mut prev: Option<u64> = None;
    for (i, e) in events.iter().enumerate() {
        let seq = e["sequence_number"]
            .as_u64()
            .unwrap_or_else(|| panic!("event {i} ({:?}) missing sequence_number", e["type"]));
        if let Some(p) = prev {
            assert!(
                seq > p,
                "sequence_number must strictly increase: event {i} ({:?}) has {seq} <= previous {p}",
                e["type"]
            );
        }
        prev = Some(seq);
    }
}

#[test]
fn text_only_turn_reassembles_and_maps_usage() {
    let events = vec![
        json!({
            "type": "message_start",
            "message": {
                "id": "msg_01XYZ",
                "model": "claude-opus-4-1-20250805",
                "role": "assistant",
                "content": [],
                "usage": {"input_tokens": 25, "output_tokens": 1}
            }
        }),
        json!({"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}}),
        json!({"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": "Hello"}}),
        json!({"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": " world"}}),
        json!({"type": "content_block_stop", "index": 0}),
        json!({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn", "stop_sequence": null},
            "usage": {"output_tokens": 8}
        }),
        json!({"type": "message_stop"}),
    ];

    let out = replay(events);
    assert_sequence_numbers_monotonic(&out);

    let types: Vec<&str> = out.iter().map(|e| e["type"].as_str().unwrap()).collect();
    assert_eq!(
        types,
        vec![
            "response.created",
            "response.in_progress",
            "response.output_item.added",
            "response.content_part.added",
            "response.output_text.delta",
            "response.output_text.delta",
            "response.output_text.done",
            "response.content_part.done",
            "response.output_item.done",
            "response.completed",
        ]
    );

    // response.id is minted once and identical across every event that carries a `response`.
    let resp_id = out[0]["response"]["id"].as_str().unwrap().to_string();
    assert_eq!(out[1]["response"]["id"], json!(resp_id));
    assert_eq!(out.last().unwrap()["response"]["id"], json!(resp_id));

    // item.id is minted once (at output_item.added) and identical everywhere it recurs.
    let item_id = out[2]["item"]["id"].as_str().unwrap().to_string();
    assert_eq!(out[3]["item_id"], json!(item_id));
    assert_eq!(out[4]["item_id"], json!(item_id));
    assert_eq!(out[6]["item_id"], json!(item_id));
    assert_eq!(out[8]["item"]["id"], json!(item_id));

    // reassembled text: no buffering across the network boundary -- each delta emitted
    // immediately -- but the FINAL accumulated string is correct at .done/.completed.
    assert_eq!(out[6]["text"], json!("Hello world"));
    assert_eq!(
        out.last().unwrap()["response"]["output"][0]["content"][0]["text"],
        json!("Hello world")
    );

    let usage = &out.last().unwrap()["response"]["usage"];
    assert_eq!(usage["input_tokens"], json!(25));
    assert_eq!(usage["output_tokens"], json!(8));
    assert_eq!(usage["total_tokens"], json!(33));
    assert_eq!(out.last().unwrap()["response"]["status"], json!("completed"));
}

#[test]
fn tool_use_turn_reassembles_arguments_and_call_id() {
    let events = vec![
        json!({
            "type": "message_start",
            "message": {"id": "msg_02ABC", "model": "claude-opus-4-1-20250805", "role": "assistant", "content": [], "usage": {"input_tokens": 40, "output_tokens": 1}}
        }),
        json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "tool_use", "id": "toolu_01AAA", "name": "get_weather", "input": {}}
        }),
        json!({"type": "content_block_delta", "index": 0, "delta": {"type": "input_json_delta", "partial_json": "{\"loc"}}),
        json!({"type": "content_block_delta", "index": 0, "delta": {"type": "input_json_delta", "partial_json": "ation\":\"SF\"}"}}),
        json!({"type": "content_block_stop", "index": 0}),
        json!({"type": "message_delta", "delta": {"stop_reason": "tool_use"}, "usage": {"output_tokens": 12}}),
        json!({"type": "message_stop"}),
    ];

    let out = replay(events);
    assert_sequence_numbers_monotonic(&out);

    let types: Vec<&str> = out.iter().map(|e| e["type"].as_str().unwrap()).collect();
    assert_eq!(
        types,
        vec![
            "response.created",
            "response.in_progress",
            "response.output_item.added",
            "response.function_call_arguments.delta",
            "response.function_call_arguments.delta",
            "response.function_call_arguments.done",
            "response.output_item.done",
            "response.completed",
        ]
    );

    // No content_part.added/.done anywhere -- tool_use blocks never open one.
    assert!(!types.contains(&"response.content_part.added"));
    assert!(!types.contains(&"response.content_part.done"));

    assert_eq!(out[2]["item"]["call_id"], json!("toolu_01AAA"));
    assert_eq!(out[2]["item"]["name"], json!("get_weather"));
    assert_eq!(out[5]["arguments"], json!("{\"location\":\"SF\"}"));
    assert_eq!(out[6]["item"]["arguments"], json!("{\"location\":\"SF\"}"));

    let final_output = &out.last().unwrap()["response"]["output"];
    assert_eq!(final_output[0]["type"], json!("function_call"));
    assert_eq!(final_output[0]["call_id"], json!("toolu_01AAA"));
    assert_eq!(final_output[0]["arguments"], json!("{\"location\":\"SF\"}"));
}

#[test]
fn thinking_then_text_turn_separates_output_indices_and_drops_signature() {
    let events = vec![
        json!({
            "type": "message_start",
            "message": {"id": "msg_03DEF", "model": "claude-opus-4-1-20250805", "role": "assistant", "content": [], "usage": {"input_tokens": 30, "output_tokens": 1}}
        }),
        json!({"type": "content_block_start", "index": 0, "content_block": {"type": "thinking", "thinking": "", "signature": ""}}),
        json!({"type": "content_block_delta", "index": 0, "delta": {"type": "thinking_delta", "thinking": "Let me "}}),
        json!({"type": "content_block_delta", "index": 0, "delta": {"type": "thinking_delta", "thinking": "think..."}}),
        json!({"type": "content_block_delta", "index": 0, "delta": {"type": "signature_delta", "signature": "abc123sig"}}),
        json!({"type": "content_block_stop", "index": 0}),
        json!({"type": "content_block_start", "index": 1, "content_block": {"type": "text", "text": ""}}),
        json!({"type": "content_block_delta", "index": 1, "delta": {"type": "text_delta", "text": "42"}}),
        json!({"type": "content_block_stop", "index": 1}),
        json!({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn"},
            "usage": {"output_tokens": 15, "cache_read_input_tokens": 5}
        }),
        json!({"type": "message_stop"}),
    ];

    let out = replay(events);
    assert_sequence_numbers_monotonic(&out);

    let types: Vec<&str> = out.iter().map(|e| e["type"].as_str().unwrap()).collect();
    // signature_delta produced ZERO events -- confirm the one-to-zero mapping by absence, not
    // just by counting: no event in the whole sequence carries a "signature" field.
    assert!(out.iter().all(|e| e.get("signature").is_none()));
    assert_eq!(
        types,
        vec![
            "response.created",
            "response.in_progress",
            "response.output_item.added",       // thinking block opens (index 0)
            "response.reasoning_summary_text.delta",
            "response.reasoning_summary_text.delta",
            "response.reasoning_summary_text.done",
            "response.output_item.done",
            "response.output_item.added",       // text block opens (index 1)
            "response.content_part.added",
            "response.output_text.delta",
            "response.output_text.done",
            "response.content_part.done",
            "response.output_item.done",
            "response.completed",
        ]
    );

    // the thinking block and the text block get DISTINCT output_index values, matching
    // Anthropic's distinct flat `index` values (0 and 1) -- each content block is its own item.
    assert_eq!(out[2]["output_index"], json!(0));
    assert_eq!(out[7]["output_index"], json!(1));
    assert_ne!(out[2]["item"]["id"], out[7]["item"]["id"]);

    assert_eq!(out[5]["text"], json!("Let me think..."));
    assert_eq!(out[10]["text"], json!("42"));

    let final_output = &out.last().unwrap()["response"]["output"];
    assert_eq!(final_output[0]["type"], json!("reasoning"));
    assert_eq!(final_output[0]["summary"][0]["text"], json!("Let me think..."));
    assert_eq!(final_output[1]["type"], json!("message"));
    assert_eq!(final_output[1]["content"][0]["text"], json!("42"));

    // usage merge: input_tokens from message_start (30) survives message_delta's partial usage
    // object (output_tokens + cache_read_input_tokens only); cache_read maps to cached_tokens.
    let usage = &out.last().unwrap()["response"]["usage"];
    assert_eq!(usage["input_tokens"], json!(30));
    assert_eq!(usage["output_tokens"], json!(15));
    assert_eq!(usage["input_tokens_details"]["cached_tokens"], json!(5));
    assert_eq!(usage["total_tokens"], json!(45));
}

#[test]
fn each_turn_gets_a_fresh_translator_with_no_cross_turn_state() {
    let base_events = |msg_id: &str| {
        vec![
            json!({"type": "message_start", "message": {"id": msg_id, "model": "claude-opus-4-1-20250805", "usage": {"input_tokens": 5, "output_tokens": 0}}}),
            json!({"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}}),
            json!({"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": "hi"}}),
            json!({"type": "content_block_stop", "index": 0}),
            json!({"type": "message_delta", "delta": {"stop_reason": "end_turn"}, "usage": {"output_tokens": 1}}),
            json!({"type": "message_stop"}),
        ]
    };

    let first = replay(base_events("msg_a"));
    let second = replay(base_events("msg_b"));

    // Both turns start their own sequence_number counter at the same point (a fresh instance
    // per turn, per SPEC-M4 §3.4) -- the two turns' sequence_numbers are independent, not a
    // continuation of one shared counter.
    assert_eq!(
        first[0]["sequence_number"], second[0]["sequence_number"],
        "each turn's translator must start its own sequence_number counter from scratch"
    );

    // response.id and item.id are freshly minted per turn -- never reused across turns.
    assert_ne!(first[0]["response"]["id"], second[0]["response"]["id"]);
    assert_ne!(first[2]["item"]["id"], second[2]["item"]["id"]);
}

#[test]
fn request_translation_does_not_remap_model_alias() {
    let mut t = AnthropicToResponses::new();
    let body = json!({
        "model": "claude-opus-4-1-20250805",
        "system": "Be concise.",
        "messages": [{"role": "user", "content": [{"type": "text", "text": "2+2?"}]}],
        "stream": true,
        "max_tokens": 512
    });
    let out = t.translate_request(body);
    // Model-alias remap (opus -> sol, SPEC-M4 U2) is deferred to M4b-wiring; the standalone
    // translator must never guess at it.
    assert_eq!(out["model"], json!("claude-opus-4-1-20250805"));
    assert_eq!(out["instructions"], json!("Be concise."));
    assert_eq!(out["max_output_tokens"], json!(512));
    assert_eq!(out["stream"], json!(true));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p polyflare-anthropic --test golden_translate`
Expected: FAIL to compile if Task 3 weren't complete (it is, by this point) — since Tasks 1–3 are already committed, this step should actually PASS immediately. Run it anyway to lock in the golden sequence as a regression pin: if any assertion fails, it means Task 3's implementation drifted from what this task expects (fix Task 3's code, not this test, unless the test itself has a bug — re-derive the expected sequence from SPEC-M4 §3.5's table if so).

- [ ] **Step 3: Run tests to verify they pass**

Run: `cargo test -p polyflare-anthropic --test golden_translate`
Expected: PASS — 5 tests.

- [ ] **Step 4: Run the full workspace gate**

Run: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace`
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add crates/polyflare-anthropic/tests/golden_translate.rs
git commit -m "test(m4b): golden replay tests for Anthropic->Responses streaming translator"
```

---

## Self-Review

**1. Spec coverage** (against SPEC-M4 §3.4/§3.5, the task's explicit scope):
- §3.4 the `Translator` trait reshape (stateless 1-in-1-out → stateful 1→N, registry → factory) → Task 1. Identity path unchanged, all pre-existing tests updated to the new signature and still green.
- §3.6 "mechanical direction" of request mapping (model passthrough, `messages`→`input`, `system`→`instructions`, `max_tokens`→`max_output_tokens`, `stream`, `tools`) → Task 2's `map_request`. Model-alias remap/payload-override correctly excluded (SPEC-M4 U2, not yet resolved).
- §3.5's full event-mapping table → Task 3: `message_start` (row 1), `content_block_start` ×3 kinds (rows 2–4), `content_block_delta` ×4 kinds including the 1→0 `signature_delta` (rows 5–8), `content_block_stop`'s `.done` triad (row 9), `message_delta` folding into the terminal event (row 10), `message_stop` → `response.completed`/`.incomplete` (row 11), mid-stream `error` (row 12). §3.5's usage-mapping paragraph → Task 3's `map_usage` + the merge strategy.
- §6 testing strategy's "Golden replay — captured Anthropic SSE → assert semantically-equivalent OpenAI-Responses SSE... asserting no buffering" → Task 4, with the fixture-provenance caveat (synthetic, not captured — U4 is separate follow-on work) stated in the test file's own doc comment and in Global Constraints.
- §3.2/§3.3/§3.5a/§3.7 (routing dispatch, store, rate-limits, TA6) are M4a, already built — correctly untouched here.
- §3.6's alias pairs + payload-override and §3.1's full routing pipeline are correctly excluded — see "Deferred to M4b-wiring."

**2. Placeholder scan:** No `TBD`/`later`/`add appropriate handling` patterns anywhere in the four tasks — every step contains complete, real, runnable code. The six items simplified relative to a literal reading of SPEC-M4 (content_block_stop's done-event pairing, the missing reasoning-summary-part companion event, the item-per-block output_index resolution, the identity `error.type`→`code` passthrough, the two-case `stop_reason`→`status` mapping, and the usage-merge strategy) are each a real, tested, working implementation choice — not a stub — and each is called out by name in "Spec gaps hit while planning" together with the concrete SPEC row it simplifies and why.

**3. Type consistency across tasks:**
- `Translator` (Task 1: `fn translate_request(&mut self, body: Value) -> Value; fn translate_response_event(&mut self, event: Value) -> Vec<Value>;`) is the exact signature `AnthropicToResponses` implements in Task 3 and the exact signature Task 4's `replay` helper calls.
- `TranslatorRegistry::create(&self, from: Format, to: Format) -> Option<Box<dyn Translator>>` (Task 1) is used identically in Task 1's own tests; no other task calls the registry (by design — see "No routing/wiring" constraint), so there's no drift risk from a second call site.
- `map_request(body: Value) -> Value` (Task 2, private to the module) is called with the exact same signature from `AnthropicToResponses::translate_request` in Task 3 — no rename, no wrapper needed, since it was designed stateless from the start specifically to avoid a same-named inherent-vs-trait-method collision.
- `AnthropicToResponses::new() -> Self` (Task 3) is the exact constructor Task 4's `replay` helper and every one of Task 4's direct-construction tests call.
- `BlockKind`/`BlockState` (Task 3, private) are used identically across `on_content_block_start`, `on_content_block_delta`, `on_content_block_stop`, and `on_message_stop` — one definition, four consumers, no duplication.
- Synthesized-field types are consistent from Task 3 through Task 4: `sequence_number` is always `u64` via `next_seq()`; `output_index`/`content_index`/`summary_index` are always the Anthropic-sourced `u64` `index` (or the literal `0`); `item_id`/`call_id`/`response_id` are always `String`.

**4. Deferred scope (restated for the record):** routing integration (wiring the translator into `ingress.rs` so a `/v1/messages` client can reach the Codex pool) and the model-alias mapping pairs/config (SPEC-M4 U2) are both explicitly out of scope — see "Deferred to M4b-wiring" above. This plan's four tasks leave `AnthropicToResponses` fully built, unit-tested, and golden-replay-tested as a standalone unit, constructible via `AnthropicToResponses::new()` and driven purely through the `Translator` trait — ready to be registered into a `TranslatorRegistry` and wired into `AppState`/`ingress.rs` once U1/U2 are resolved.
